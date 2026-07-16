//! Opt-in resident assembly for bounded goal pursuit.

use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, time::Instant};

use anyhow::{Context as _, Result};
use chrono::Utc;
use vyane_core::{CancellationToken, Sandbox};
use vyane_goal::{
    AcceptanceVerifier, GoalPursuer, GoalQuery, GoalRecord, GoalStatus, GoalStore, GoalStoreError,
    MAX_LEASE_SECONDS, PursuitConfig, SqliteGoalStore,
};
use vyane_service::VyaneService;

use crate::cli::DaemonGoalArgs;
use crate::goal_runtime::DispatchGoalRuntime;
use crate::task::LOCAL_TASK_OWNER;

const DAEMON_GOAL_WORKER: &str = "daemon-goal:auto-v1";
const MAX_PURSUIT_ERRORS_PER_GENERATION: u8 = 5;
const MAX_PURSUIT_ERROR_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy)]
struct PursuitRetry {
    errors: u8,
    eligible_at: Option<Instant>,
}

#[derive(Debug, Clone)]
pub(crate) struct DaemonGoalConfig {
    sandbox: Sandbox,
    pursuit: PursuitConfig,
    verifier_timeout: Duration,
    poll_interval: Duration,
}

impl DaemonGoalConfig {
    pub(crate) fn from_args(service: &VyaneService, args: &DaemonGoalArgs) -> Result<Option<Self>> {
        if !args.goal_auto_pursue {
            return Ok(None);
        }
        let target = args
            .goal_target
            .as_ref()
            .context("--goal-auto-pursue requires --goal-target")?
            .clone();
        service
            .resolve(&target)
            .context("resolve automatic goal pursuit target")?;
        let workdir = std::fs::canonicalize(
            args.goal_workdir
                .as_ref()
                .context("--goal-auto-pursue requires --goal-workdir")?,
        )
        .context("canonicalize automatic goal pursuit workdir")?;
        let pursuit = PursuitConfig {
            workdir: workdir.clone(),
            runtime: target.clone(),
            worker_id: DAEMON_GOAL_WORKER.into(),
            overall_timeout: Duration::from_secs(args.goal_overall_timeout_seconds),
            segment_timeout: Duration::from_secs(args.goal_segment_timeout_seconds),
            max_segments: args.goal_max_segments,
            max_failures: args.goal_max_failures,
        };
        pursuit
            .validate()
            .context("validate automatic goal pursuit")?;
        Ok(Some(Self {
            sandbox: args.goal_sandbox.into(),
            pursuit,
            verifier_timeout: Duration::from_secs(args.goal_verifier_timeout_seconds),
            poll_interval: Duration::from_millis(args.goal_poll_millis),
        }))
    }

    fn lease_seconds(&self) -> u64 {
        self.pursuit
            .overall_timeout
            .as_secs()
            .saturating_add(1)
            .clamp(1, MAX_LEASE_SECONDS)
    }
}

pub(crate) struct DaemonGoalSupervisor {
    service: Arc<VyaneService>,
    store: SqliteGoalStore,
    verifier: AcceptanceVerifier,
    config: DaemonGoalConfig,
    retry_after: HashMap<String, PursuitRetry>,
}

impl DaemonGoalSupervisor {
    pub(crate) fn open(service: Arc<VyaneService>, config: DaemonGoalConfig) -> Result<Self> {
        let path = service.storage_paths().goal_db_path();
        let store = SqliteGoalStore::open(&path)
            .with_context(|| format!("open goal database {}", path.display()))?;
        let verifier = AcceptanceVerifier::new(&config.pursuit.workdir, config.verifier_timeout)
            .context("construct automatic goal verifier")?;
        Ok(Self {
            service,
            store,
            verifier,
            config,
            retry_after: HashMap::new(),
        })
    }

    pub(crate) async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let goal = match self.acquire_next() {
                Ok(Some(goal)) => goal,
                Ok(None) => {
                    self.wait_or_cancel(&cancel).await;
                    continue;
                }
                Err(error) => {
                    tracing::error!(error = %error, "resident goal acquisition failed");
                    self.wait_or_cancel(&cancel).await;
                    continue;
                }
            };

            tracing::info!(goal_id = %goal.id, "starting resident goal pursuit");
            let runtime = DispatchGoalRuntime::new(
                Arc::clone(&self.service),
                self.config.pursuit.runtime.clone(),
                self.config.sandbox,
            );
            let pursuer = GoalPursuer::new(
                &self.store,
                &runtime,
                &self.verifier,
                self.config.pursuit.clone(),
            )
            .context("construct resident goal pursuer")?;
            let result = pursuer
                .pursue_with_cancel_preserving_checkpoint(
                    LOCAL_TASK_OWNER,
                    &goal.id,
                    cancel.child_token(),
                )
                .await;
            let retry_after_error = match result {
                Ok(outcome) => {
                    self.retry_after.remove(&goal.id);
                    tracing::info!(
                        goal_id = %goal.id,
                        status = ?outcome.status,
                        reason = %outcome.reason,
                        "resident goal pursuit settled"
                    );
                    false
                }
                Err(error) => {
                    let retry = schedule_retry(&mut self.retry_after, &goal.id, Instant::now());
                    tracing::warn!(
                        goal_id = %goal.id,
                        error = %error,
                        errors = retry.errors,
                        quarantined = retry.eligible_at.is_none(),
                        "resident goal pursuit stopped with an error"
                    );
                    true
                }
            };
            if retry_after_error {
                self.wait_or_cancel(&cancel).await;
            }
        }
    }

    async fn wait_or_cancel(&self, cancel: &CancellationToken) {
        tokio::select! {
            () = cancel.cancelled() => {}
            () = tokio::time::sleep(self.config.poll_interval) => {}
        }
    }

    fn acquire_next(&self) -> Result<Option<GoalRecord>> {
        acquire_from_store(&self.store, &self.config, &self.retry_after)
    }
}

fn acquire_from_store<S: GoalStore>(
    store: &S,
    config: &DaemonGoalConfig,
    retry_after: &HashMap<String, PursuitRetry>,
) -> Result<Option<GoalRecord>> {
    let query = GoalQuery {
        statuses: vec![GoalStatus::InProgress],
        parent_goal_id: None,
        // Resident recovery must inspect every in-progress goal before it is
        // allowed to claim queued work. GoalStore defines zero as unbounded.
        limit: 0,
    };
    let goals = store.list(LOCAL_TASK_OWNER, &query)?;
    let retry_now = Instant::now();
    let lease_now = Utc::now();

    // A live lease owned by the stable daemon worker is restart continuity,
    // not ordinary competing work. Adopt it before reclaiming any other row.
    if let Some(goal) = goals.iter().find(|goal| {
        retry_eligible(retry_after, &goal.id, retry_now)
            && goal.lease_active(lease_now)
            && goal.claimed_by.as_deref() == Some(DAEMON_GOAL_WORKER)
    }) {
        return Ok(Some(goal.clone()));
    }

    for goal in goals {
        if !retry_eligible(retry_after, &goal.id, retry_now) || goal.lease_active(lease_now) {
            continue;
        }
        let acquired = if goal.claimed_by.is_some() {
            store.reclaim(
                LOCAL_TASK_OWNER,
                &goal.id,
                DAEMON_GOAL_WORKER,
                config.lease_seconds(),
                lease_now,
            )
        } else {
            store.claim(
                LOCAL_TASK_OWNER,
                &goal.id,
                DAEMON_GOAL_WORKER,
                config.lease_seconds(),
                lease_now,
            )
        };
        match acquired {
            Ok(goal) => return Ok(Some(goal)),
            Err(error) if acquisition_raced(&error) => continue,
            Err(error) => return Err(error.into()),
        }
    }

    store
        .claim_next(
            LOCAL_TASK_OWNER,
            DAEMON_GOAL_WORKER,
            config.lease_seconds(),
            Utc::now(),
        )
        .map_err(Into::into)
}

fn retry_eligible(
    retry_after: &HashMap<String, PursuitRetry>,
    goal_id: &str,
    now: Instant,
) -> bool {
    !retry_after.get(goal_id).is_some_and(|retry| {
        retry
            .eligible_at
            .is_none_or(|eligible_at| eligible_at > now)
    })
}

fn schedule_retry(
    retries: &mut HashMap<String, PursuitRetry>,
    goal_id: &str,
    now: Instant,
) -> PursuitRetry {
    let errors = retries
        .get(goal_id)
        .map_or(1, |retry| retry.errors.saturating_add(1));
    let eligible_at = if errors >= MAX_PURSUIT_ERRORS_PER_GENERATION {
        None
    } else {
        let exponent = u32::from(errors.saturating_sub(1));
        let seconds = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
        let delay = Duration::from_secs(seconds).min(MAX_PURSUIT_ERROR_BACKOFF);
        Some(now + delay)
    };
    let retry = PursuitRetry {
        errors,
        eligible_at,
    };
    retries.insert(goal_id.to_string(), retry);
    retry
}

fn acquisition_raced(error: &GoalStoreError) -> bool {
    matches!(
        error,
        GoalStoreError::NotFound { .. }
            | GoalStoreError::InvalidStatus { .. }
            | GoalStoreError::LeaseHeld { .. }
            | GoalStoreError::LeaseExpired { .. }
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use chrono::TimeDelta;
    use std::path::Path;
    use tempfile::TempDir;
    use vyane_goal::{NewGoal, PursuitConfig};

    use crate::cli::{DaemonGoalArgs, SandboxArg};

    fn cli_args(target: &str, workdir: &Path) -> DaemonGoalArgs {
        DaemonGoalArgs {
            goal_auto_pursue: true,
            goal_target: Some(target.into()),
            goal_workdir: Some(workdir.to_path_buf()),
            goal_sandbox: SandboxArg::ReadOnly,
            goal_overall_timeout_seconds: 60,
            goal_segment_timeout_seconds: 10,
            goal_verifier_timeout_seconds: 5,
            goal_max_segments: 3,
            goal_max_failures: 2,
            goal_poll_millis: 50,
        }
    }

    fn service(directory: &TempDir) -> VyaneService {
        let path = directory.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
            [providers.native]
            base_url = "https://unused.invalid"
            auth_style = "x_api_key"
            protocol = "anthropic_messages"
            default_model = "test-model"

            [profiles.builder]
            provider = "native"
            protocol = "anthropic_messages"
            harness = "claude-code"
            model = "test-model"
            "#,
        )
        .unwrap();
        VyaneService::load(Some(&path)).unwrap()
    }

    fn config(directory: &TempDir) -> DaemonGoalConfig {
        let pursuit = PursuitConfig {
            workdir: directory.path().to_path_buf(),
            runtime: "builder".into(),
            worker_id: DAEMON_GOAL_WORKER.into(),
            overall_timeout: Duration::from_secs(60),
            segment_timeout: Duration::from_secs(10),
            max_segments: 3,
            max_failures: 2,
        };
        DaemonGoalConfig {
            sandbox: Sandbox::ReadOnly,
            pursuit,
            verifier_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(50),
        }
    }

    fn supervisor_store(directory: &TempDir) -> (SqliteGoalStore, DaemonGoalConfig) {
        (
            SqliteGoalStore::open(directory.path().join("goals.sqlite3")).unwrap(),
            config(directory),
        )
    }

    #[test]
    fn config_from_args_rejects_unknown_target_and_missing_workdir() {
        let directory = TempDir::new().unwrap();
        let runtime = service(&directory);
        let unknown = cli_args("missing", directory.path());
        assert!(DaemonGoalConfig::from_args(&runtime, &unknown).is_err());

        let missing = cli_args("builder", &directory.path().join("missing-workdir"));
        assert!(DaemonGoalConfig::from_args(&runtime, &missing).is_err());
    }

    #[test]
    fn config_from_args_rejects_invalid_pursuit_bounds() {
        let directory = TempDir::new().unwrap();
        let runtime = service(&directory);
        let mut args = cli_args("builder", directory.path());
        args.goal_overall_timeout_seconds = 0;
        assert!(DaemonGoalConfig::from_args(&runtime, &args).is_err());
    }

    fn create(store: &SqliteGoalStore, id: &str, priority: u8) -> GoalRecord {
        let mut goal = NewGoal::new(id, Utc::now());
        goal.id = Some(id.into());
        goal.priority = priority;
        store.create(LOCAL_TASK_OWNER, goal).unwrap()
    }

    fn acquire(store: &SqliteGoalStore, config: &DaemonGoalConfig) -> Option<GoalRecord> {
        acquire_from_store(store, config, &HashMap::new()).unwrap()
    }

    #[test]
    fn queued_goal_is_atomically_claimed_by_the_stable_worker() {
        let directory = TempDir::new().unwrap();
        let (store, config) = supervisor_store(&directory);
        create(&store, "queued", 1);

        let goal = acquire(&store, &config).unwrap();
        assert_eq!(goal.id, "queued");
        assert_eq!(goal.status, GoalStatus::InProgress);
        assert_eq!(goal.claimed_by.as_deref(), Some(DAEMON_GOAL_WORKER));
        assert_eq!(goal.claim_generation, 1);
    }

    #[test]
    fn unleased_and_expired_in_progress_goals_are_recovered() {
        let directory = TempDir::new().unwrap();
        let (store, config) = supervisor_store(&directory);
        create(&store, "unleased", 0);
        store
            .start(LOCAL_TASK_OWNER, "unleased", Utc::now())
            .unwrap();
        let unleased = acquire(&store, &config).unwrap();
        assert_eq!(unleased.id, "unleased");
        assert_eq!(unleased.claim_generation, 1);

        store
            .pause(
                LOCAL_TASK_OWNER,
                "unleased",
                Some(DAEMON_GOAL_WORKER),
                Some("test setup"),
                Utc::now(),
            )
            .unwrap();
        let past = Utc::now() - TimeDelta::seconds(10);
        let mut expired_goal = NewGoal::new("expired", past - TimeDelta::seconds(1));
        expired_goal.id = Some("expired".into());
        expired_goal.priority = 1;
        store.create(LOCAL_TASK_OWNER, expired_goal).unwrap();
        store
            .claim(LOCAL_TASK_OWNER, "expired", "foreign", 1, past)
            .unwrap();

        let expired = acquire(&store, &config).unwrap();
        assert_eq!(expired.id, "expired");
        assert_eq!(expired.claimed_by.as_deref(), Some(DAEMON_GOAL_WORKER));
        assert_eq!(expired.claim_generation, 2);
    }

    #[test]
    fn live_stable_lease_is_adopted_without_generation_churn() {
        let directory = TempDir::new().unwrap();
        let (store, config) = supervisor_store(&directory);
        create(&store, "restart", 1);
        let claimed = store
            .claim(
                LOCAL_TASK_OWNER,
                "restart",
                DAEMON_GOAL_WORKER,
                60,
                Utc::now(),
            )
            .unwrap();

        let adopted = acquire(&store, &config).unwrap();
        assert_eq!(adopted.id, "restart");
        assert_eq!(adopted.claim_generation, claimed.claim_generation);
        assert_eq!(adopted.revision, claimed.revision);
    }

    #[test]
    fn live_stable_lease_precedes_earlier_reclaimable_work() {
        let directory = TempDir::new().unwrap();
        let (store, config) = supervisor_store(&directory);
        let past = Utc::now() - TimeDelta::seconds(10);
        create(&store, "reclaimable", 0);
        store
            .claim(LOCAL_TASK_OWNER, "reclaimable", "old-worker", 1, past)
            .unwrap();
        create(&store, "restart", 1);
        let restart = store
            .claim(
                LOCAL_TASK_OWNER,
                "restart",
                DAEMON_GOAL_WORKER,
                60,
                Utc::now(),
            )
            .unwrap();

        let adopted = acquire(&store, &config).unwrap();
        assert_eq!(adopted.id, "restart");
        assert_eq!(adopted.claim_generation, restart.claim_generation);
        let untouched = store.get(LOCAL_TASK_OWNER, "reclaimable").unwrap().unwrap();
        assert_eq!(untouched.claimed_by.as_deref(), Some("old-worker"));
        assert_eq!(untouched.claim_generation, 1);
    }

    #[test]
    fn recovery_scans_beyond_one_thousand_in_progress_goals() {
        let directory = TempDir::new().unwrap();
        let (store, config) = supervisor_store(&directory);
        let now = Utc::now();
        for index in 0..1_000 {
            let id = format!("foreign-{index:04}");
            create(&store, &id, 0);
            store
                .claim(LOCAL_TASK_OWNER, &id, "foreign-worker", 60, now)
                .unwrap();
        }
        create(&store, "eligible-after-page", 1);
        store
            .start(LOCAL_TASK_OWNER, "eligible-after-page", now)
            .unwrap();
        create(&store, "queued", 2);

        let recovered = acquire(&store, &config).unwrap();
        assert_eq!(recovered.id, "eligible-after-page");
        let queued = store.get(LOCAL_TASK_OWNER, "queued").unwrap().unwrap();
        assert_eq!(queued.status, GoalStatus::Queued);
        assert_eq!(queued.claim_generation, 0);
    }

    #[test]
    fn paused_and_live_foreign_work_are_not_auto_resumed_or_stolen() {
        let directory = TempDir::new().unwrap();
        let (store, config) = supervisor_store(&directory);
        create(&store, "paused", 0);
        store.start(LOCAL_TASK_OWNER, "paused", Utc::now()).unwrap();
        store
            .pause(
                LOCAL_TASK_OWNER,
                "paused",
                None,
                Some("operator pause"),
                Utc::now(),
            )
            .unwrap();
        create(&store, "foreign", 1);
        store
            .claim(
                LOCAL_TASK_OWNER,
                "foreign",
                "foreign-worker",
                60,
                Utc::now(),
            )
            .unwrap();

        assert!(acquire(&store, &config).is_none());
        let paused = store.get(LOCAL_TASK_OWNER, "paused").unwrap().unwrap();
        let foreign = store.get(LOCAL_TASK_OWNER, "foreign").unwrap().unwrap();
        assert_eq!(paused.status, GoalStatus::Paused);
        assert_eq!(foreign.claimed_by.as_deref(), Some("foreign-worker"));
        assert_eq!(foreign.claim_generation, 1);
    }

    #[test]
    fn cooldown_skips_failed_goal_then_expiry_makes_it_eligible() {
        let directory = TempDir::new().unwrap();
        let (store, config) = supervisor_store(&directory);
        create(&store, "cooling", 0);
        store
            .claim(
                LOCAL_TASK_OWNER,
                "cooling",
                DAEMON_GOAL_WORKER,
                60,
                Utc::now(),
            )
            .unwrap();
        create(&store, "next", 1);

        let mut retries = HashMap::new();
        retries.insert(
            "cooling".into(),
            PursuitRetry {
                errors: 1,
                eligible_at: Some(Instant::now() + Duration::from_secs(60)),
            },
        );
        let next = acquire_from_store(&store, &config, &retries)
            .unwrap()
            .unwrap();
        assert_eq!(next.id, "next");

        store
            .pause(
                LOCAL_TASK_OWNER,
                "next",
                Some(DAEMON_GOAL_WORKER),
                Some("test setup"),
                Utc::now(),
            )
            .unwrap();
        retries.insert(
            "cooling".into(),
            PursuitRetry {
                errors: 1,
                eligible_at: Some(Instant::now() - Duration::from_secs(1)),
            },
        );
        let retried = acquire_from_store(&store, &config, &retries)
            .unwrap()
            .unwrap();
        assert_eq!(retried.id, "cooling");
    }

    #[test]
    fn pursuit_error_retry_is_exponential_and_bounded_per_generation() {
        let now = Instant::now();
        let mut retries = HashMap::new();
        for (attempt, expected_delay) in [(1, 1), (2, 2), (3, 4), (4, 8)] {
            let retry = schedule_retry(&mut retries, "failing", now);
            assert_eq!(retry.errors, attempt);
            assert_eq!(
                retry.eligible_at,
                Some(now + Duration::from_secs(expected_delay))
            );
        }
        let quarantined = schedule_retry(&mut retries, "failing", now);
        assert_eq!(quarantined.errors, MAX_PURSUIT_ERRORS_PER_GENERATION);
        assert_eq!(quarantined.eligible_at, None);
        assert_eq!(retries.len(), 1);
    }

    #[test]
    fn quarantined_goal_stays_skipped() {
        let directory = TempDir::new().unwrap();
        let (store, config) = supervisor_store(&directory);
        create(&store, "quarantined", 0);
        store
            .claim(
                LOCAL_TASK_OWNER,
                "quarantined",
                DAEMON_GOAL_WORKER,
                60,
                Utc::now(),
            )
            .unwrap();
        create(&store, "next", 1);

        let retries = HashMap::from([(
            "quarantined".into(),
            PursuitRetry {
                errors: MAX_PURSUIT_ERRORS_PER_GENERATION,
                eligible_at: None,
            },
        )]);
        let next = acquire_from_store(&store, &config, &retries)
            .unwrap()
            .unwrap();
        assert_eq!(next.id, "next");
    }

    #[test]
    fn lease_duration_clamps_at_maximum_pursuit_timeout() {
        let directory = TempDir::new().unwrap();
        let mut config = config(&directory);
        config.pursuit.overall_timeout = vyane_goal::MAX_PURSUIT_TIMEOUT;
        assert_eq!(config.lease_seconds(), MAX_LEASE_SECONDS);
    }

    #[test]
    fn only_expected_claim_races_are_treated_as_retryable() {
        for error in [
            GoalStoreError::NotFound { id: "g".into() },
            GoalStoreError::InvalidStatus {
                id: "g".into(),
                operation: "claim",
                status: GoalStatus::Paused,
            },
            GoalStoreError::LeaseHeld {
                id: "g".into(),
                held_by: "other".into(),
            },
            GoalStoreError::LeaseExpired { id: "g".into() },
        ] {
            assert!(acquisition_raced(&error));
        }
        assert!(!acquisition_raced(&GoalStoreError::InvalidInput(
            "corrupt request".into()
        )));
    }
}
