//! Opt-in resident assembly for bounded goal pursuit.

use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, time::Instant};

use anyhow::{Context as _, Result};
use chrono::Utc;
use vyane_core::{CancellationToken, Sandbox};
use vyane_goal::{
    AcceptanceVerifier, GoalPursuer, GoalRecord, GoalRecoveryCursor, GoalRecoveryFilter, GoalStore,
    GoalStoreError, MAX_LEASE_SECONDS, PursuitConfig, SqliteGoalStore,
};
use vyane_service::VyaneService;

use crate::cli::DaemonGoalArgs;
use crate::goal_runtime::DispatchGoalRuntime;
use crate::task::LOCAL_TASK_OWNER;

const DAEMON_GOAL_WORKER: &str = "daemon-goal:auto-v1";
const MAX_PURSUIT_ERRORS_PER_GENERATION: u8 = 5;
const MAX_PURSUIT_ERROR_BACKOFF: Duration = Duration::from_secs(60);
const MAX_TRACKED_PURSUIT_RETRIES: usize = 256;
const RECOVERY_PAGE_SIZE: usize = 256;
const RECOVERY_PAGES_PER_CYCLE: usize = 4;

#[derive(Debug, Clone, Copy)]
struct PursuitRetry {
    errors: u8,
    eligible_at: Option<Instant>,
    claim_generation: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum RecoveryPhase {
    #[default]
    StableLease,
    Available,
}

#[derive(Debug, Default)]
struct RecoveryScan {
    phase: RecoveryPhase,
    after: Option<GoalRecoveryCursor>,
    at: Option<chrono::DateTime<Utc>>,
    confirming_queue: bool,
}

impl RecoveryScan {
    fn reset(&mut self) {
        self.phase = RecoveryPhase::StableLease;
        self.after = None;
        self.at = None;
        self.confirming_queue = false;
    }

    fn restart_for_queue_confirmation(&mut self) {
        self.phase = RecoveryPhase::StableLease;
        self.after = None;
        self.at = None;
        self.confirming_queue = true;
    }
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
        if !target.eq_ignore_ascii_case("auto") {
            service
                .resolve(&target)
                .context("resolve automatic goal pursuit target")?;
        }
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
    recovery_scan: RecoveryScan,
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
            recovery_scan: RecoveryScan::default(),
        })
    }

    pub(crate) async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            if let Err(error) = self.config.pursuit.validate() {
                tracing::error!(error = %error, "resident goal configuration is temporarily unavailable");
                self.wait_or_cancel(&cancel).await;
                continue;
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
            let pursuer = match GoalPursuer::new(
                &self.store,
                &runtime,
                &self.verifier,
                self.config.pursuit.clone(),
            ) {
                Ok(pursuer) => pursuer,
                Err(error) => {
                    let retry = schedule_retry(&mut self.retry_after, &goal, Instant::now());
                    self.pause_quarantined(&goal, retry)?;
                    tracing::warn!(
                        goal_id = %goal.id,
                        error = %error,
                        errors = retry.errors,
                        quarantined = retry.eligible_at.is_none(),
                        "resident goal pursuer construction failed"
                    );
                    self.wait_or_cancel(&cancel).await;
                    continue;
                }
            };
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
                    let retry = schedule_retry(&mut self.retry_after, &goal, Instant::now());
                    self.pause_quarantined(&goal, retry)?;
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

    fn acquire_next(&mut self) -> Result<Option<GoalRecord>> {
        acquire_from_store(
            &self.store,
            &self.config,
            &mut self.retry_after,
            &mut self.recovery_scan,
        )
    }

    fn pause_quarantined(&mut self, goal: &GoalRecord, retry: PursuitRetry) -> Result<()> {
        if retry.eligible_at.is_some() {
            return Ok(());
        }
        match self.store.pause(
            LOCAL_TASK_OWNER,
            &goal.id,
            Some(DAEMON_GOAL_WORKER),
            Some("resident pursuit quarantined after repeated internal errors"),
            Utc::now(),
        ) {
            Ok(_) => {
                self.retry_after.remove(&goal.id);
                Ok(())
            }
            Err(error) if self.retry_after.contains_key(&goal.id) => {
                tracing::error!(goal_id = %goal.id, error = %error, "failed to persist resident goal quarantine");
                Ok(())
            }
            Err(error) => Err(error)
                .context("persist resident goal quarantine before retry capacity overflow"),
        }
    }
}

fn acquire_from_store(
    store: &SqliteGoalStore,
    config: &DaemonGoalConfig,
    retry_after: &mut HashMap<String, PursuitRetry>,
    scan: &mut RecoveryScan,
) -> Result<Option<GoalRecord>> {
    let retry_now = Instant::now();
    if let Some(goal) = acquire_due_retry(store, config, retry_after, retry_now)? {
        scan.reset();
        return Ok(Some(goal));
    }

    for _ in 0..RECOVERY_PAGES_PER_CYCLE {
        let lease_now = *scan.at.get_or_insert_with(Utc::now);
        let filter = match scan.phase {
            RecoveryPhase::StableLease => GoalRecoveryFilter::ActiveWorker {
                worker_id: DAEMON_GOAL_WORKER.into(),
                at: lease_now,
            },
            RecoveryPhase::Available => GoalRecoveryFilter::Available { at: lease_now },
        };
        let page = store.list_recovery_page(
            LOCAL_TASK_OWNER,
            &filter,
            scan.after.as_ref(),
            RECOVERY_PAGE_SIZE,
        )?;
        for goal in &page.candidates {
            if !retry_eligible(retry_after, &goal.id, retry_now) {
                continue;
            }
            match scan.phase {
                RecoveryPhase::StableLease => {
                    scan.reset();
                    return Ok(Some(goal.clone()));
                }
                RecoveryPhase::Available => {
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
                        Ok(goal) => {
                            scan.reset();
                            return Ok(Some(goal));
                        }
                        Err(error) if acquisition_raced(&error) => continue,
                        Err(error) => return Err(error.into()),
                    }
                }
            }
        }
        if let Some(next) = page.next {
            scan.after = Some(next);
            continue;
        }
        match scan.phase {
            RecoveryPhase::StableLease => {
                scan.phase = RecoveryPhase::Available;
                scan.after = None;
            }
            RecoveryPhase::Available => {
                if !scan.confirming_queue {
                    scan.restart_for_queue_confirmation();
                    continue;
                }
                scan.reset();
                let retry_exclusions = retry_after
                    .iter()
                    .filter(|(_, retry)| {
                        retry
                            .eligible_at
                            .is_none_or(|eligible_at| eligible_at > Instant::now())
                    })
                    .map(|(id, _)| id.clone())
                    .collect::<Vec<_>>();
                return store
                    .claim_next_if_no_recovery(
                        LOCAL_TASK_OWNER,
                        DAEMON_GOAL_WORKER,
                        config.lease_seconds(),
                        &retry_exclusions,
                        Utc::now(),
                    )
                    .map_err(Into::into);
            }
        }
    }

    Ok(None)
}

fn acquire_due_retry(
    store: &SqliteGoalStore,
    config: &DaemonGoalConfig,
    retries: &mut HashMap<String, PursuitRetry>,
    now: Instant,
) -> Result<Option<GoalRecord>> {
    let retry_snapshot = retries
        .iter()
        .map(|(id, retry)| (id.clone(), *retry))
        .collect::<Vec<_>>();
    let lease_now = Utc::now();
    let mut due = Vec::new();
    for (id, retry) in retry_snapshot {
        let goal = store.get(LOCAL_TASK_OWNER, &id)?;
        let Some(goal) = goal else {
            retries.remove(&id);
            continue;
        };
        if goal.status != vyane_goal::GoalStatus::InProgress
            || goal.claim_generation != retry.claim_generation
        {
            retries.remove(&id);
            continue;
        }
        if retry_eligible(retries, &id, now) {
            due.push(goal);
        }
    }
    due.sort_by(|left, right| {
        (left.priority, left.created_at, &left.id).cmp(&(
            right.priority,
            right.created_at,
            &right.id,
        ))
    });
    for goal in due {
        if goal.lease_active(lease_now) {
            if goal.claimed_by.as_deref() == Some(DAEMON_GOAL_WORKER) {
                return Ok(Some(goal));
            }
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
    Ok(None)
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
    goal: &GoalRecord,
    now: Instant,
) -> PursuitRetry {
    if !retries.contains_key(&goal.id) && retries.len() >= MAX_TRACKED_PURSUIT_RETRIES {
        return PursuitRetry {
            errors: MAX_PURSUIT_ERRORS_PER_GENERATION,
            eligible_at: None,
            claim_generation: goal.claim_generation,
        };
    }
    let errors = retries
        .get(&goal.id)
        .filter(|retry| retry.claim_generation == goal.claim_generation)
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
        claim_generation: goal.claim_generation,
    };
    retries.insert(goal.id.clone(), retry);
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
    use vyane_goal::{GoalStatus, NewGoal, PursuitConfig};

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

        let auto = cli_args("auto", directory.path());
        let auto = DaemonGoalConfig::from_args(&runtime, &auto)
            .unwrap()
            .expect("auto pursuit config");
        assert_eq!(auto.pursuit.runtime, "auto");
    }

    #[test]
    fn config_from_args_rejects_invalid_pursuit_bounds() {
        let directory = TempDir::new().unwrap();
        let runtime = service(&directory);
        let mut args = cli_args("builder", directory.path());
        args.goal_overall_timeout_seconds = 0;
        assert!(DaemonGoalConfig::from_args(&runtime, &args).is_err());
    }

    #[tokio::test]
    async fn unavailable_workdir_does_not_claim_or_terminate_the_supervisor() {
        let fixture = TempDir::new().unwrap();
        let data = TempDir::new().unwrap();
        let workdir = TempDir::new().unwrap();
        let config = config(&workdir);
        let store = SqliteGoalStore::open(data.path().join("goals.sqlite3")).unwrap();
        create(&store, "waiting", 1);
        let verifier = AcceptanceVerifier::new(workdir.path(), Duration::from_secs(1)).unwrap();
        std::fs::remove_dir_all(workdir.path()).unwrap();
        let supervisor = DaemonGoalSupervisor {
            service: Arc::new(service(&fixture)),
            store: store.clone(),
            verifier,
            config,
            retry_after: HashMap::new(),
            recovery_scan: RecoveryScan::default(),
        };
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let canceller = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(120)).await;
            stop.cancel();
        });

        supervisor.run(cancel).await.unwrap();
        canceller.await.unwrap();
        let waiting = store.get(LOCAL_TASK_OWNER, "waiting").unwrap().unwrap();
        assert_eq!(waiting.status, GoalStatus::Queued);
        assert_eq!(waiting.claim_generation, 0);
    }

    fn create(store: &SqliteGoalStore, id: &str, priority: u8) -> GoalRecord {
        let mut goal = NewGoal::new(id, Utc::now());
        goal.id = Some(id.into());
        goal.priority = priority;
        store.create(LOCAL_TASK_OWNER, goal).unwrap()
    }

    fn acquire(store: &SqliteGoalStore, config: &DaemonGoalConfig) -> Option<GoalRecord> {
        let mut retries = HashMap::new();
        let mut scan = RecoveryScan::default();
        for _ in 0..16 {
            if let Some(goal) = acquire_from_store(store, config, &mut retries, &mut scan).unwrap()
            {
                return Some(goal);
            }
        }
        None
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
    fn recovery_scan_has_a_fixed_per_cycle_budget_and_immutable_cursor() {
        const { assert!(RECOVERY_PAGE_SIZE > 0) };
        const { assert!(RECOVERY_PAGES_PER_CYCLE > 0) };
        assert_eq!(RECOVERY_PAGE_SIZE * RECOVERY_PAGES_PER_CYCLE, 1_024);
        let cursor = GoalRecoveryCursor {
            priority: 2,
            created_at: Utc::now(),
            id: "cursor".into(),
        };
        let mut scan = RecoveryScan {
            phase: RecoveryPhase::Available,
            after: Some(cursor.clone()),
            at: Some(Utc::now()),
            confirming_queue: false,
        };
        assert_eq!(scan.after, Some(cursor));
        scan.reset();
        assert_eq!(scan.phase, RecoveryPhase::StableLease);
        assert!(scan.after.is_none());
        assert!(scan.at.is_none());
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
    fn queued_claim_requires_a_fresh_recovery_confirmation() {
        let directory = TempDir::new().unwrap();
        let (store, config) = supervisor_store(&directory);
        let past = Utc::now() - TimeDelta::seconds(10);
        let mut expiring = NewGoal::new("expired-during-scan", past);
        expiring.id = Some("expired-during-scan".into());
        expiring.priority = 0;
        store.create(LOCAL_TASK_OWNER, expiring).unwrap();
        store
            .claim(
                LOCAL_TASK_OWNER,
                "expired-during-scan",
                "foreign-worker",
                1,
                past,
            )
            .unwrap();
        create(&store, "queued", 1);
        let mut scan = RecoveryScan {
            phase: RecoveryPhase::Available,
            after: None,
            // The lease was active at the original scan boundary but is
            // expired by the fresh confirmation boundary.
            at: Some(past),
            confirming_queue: false,
        };

        let recovered = acquire_from_store(&store, &config, &mut HashMap::new(), &mut scan)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.id, "expired-during-scan");
        let queued = store.get(LOCAL_TASK_OWNER, "queued").unwrap().unwrap();
        assert_eq!(queued.status, GoalStatus::Queued);
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
        let cooling = store.get(LOCAL_TASK_OWNER, "cooling").unwrap().unwrap();
        retries.insert(
            "cooling".into(),
            PursuitRetry {
                errors: 1,
                eligible_at: Some(Instant::now() + Duration::from_secs(60)),
                claim_generation: cooling.claim_generation,
            },
        );
        let next = acquire_from_store(&store, &config, &mut retries, &mut RecoveryScan::default())
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
                claim_generation: cooling.claim_generation,
            },
        );
        let retried =
            acquire_from_store(&store, &config, &mut retries, &mut RecoveryScan::default())
                .unwrap()
                .unwrap();
        assert_eq!(retried.id, "cooling");
    }

    #[test]
    fn due_retry_is_checked_before_a_persisted_recovery_cursor() {
        let directory = TempDir::new().unwrap();
        let (store, config) = supervisor_store(&directory);
        create(&store, "due-before-cursor", 0);
        let due = store
            .claim(
                LOCAL_TASK_OWNER,
                "due-before-cursor",
                DAEMON_GOAL_WORKER,
                60,
                Utc::now(),
            )
            .unwrap();
        create(&store, "later-stable", 1);
        store
            .claim(
                LOCAL_TASK_OWNER,
                "later-stable",
                DAEMON_GOAL_WORKER,
                60,
                Utc::now(),
            )
            .unwrap();
        let mut retries = HashMap::from([(
            due.id.clone(),
            PursuitRetry {
                errors: 1,
                eligible_at: Some(Instant::now() - Duration::from_secs(1)),
                claim_generation: due.claim_generation,
            },
        )]);
        let mut scan = RecoveryScan {
            phase: RecoveryPhase::StableLease,
            after: Some(GoalRecoveryCursor {
                priority: 0,
                created_at: Utc::now(),
                id: "cursor-after-due".into(),
            }),
            at: Some(Utc::now()),
            confirming_queue: false,
        };

        let retried = acquire_from_store(&store, &config, &mut retries, &mut scan)
            .unwrap()
            .unwrap();
        assert_eq!(retried.id, "due-before-cursor");
        assert!(scan.after.is_none());
    }

    #[test]
    fn pursuit_error_retry_is_exponential_and_bounded_per_generation() {
        let now = Instant::now();
        let mut retries = HashMap::new();
        let directory = TempDir::new().unwrap();
        let (store, _config) = supervisor_store(&directory);
        create(&store, "failing", 0);
        let failing = store
            .claim(
                LOCAL_TASK_OWNER,
                "failing",
                DAEMON_GOAL_WORKER,
                60,
                Utc::now(),
            )
            .unwrap();
        for (attempt, expected_delay) in [(1, 1), (2, 2), (3, 4), (4, 8)] {
            let retry = schedule_retry(&mut retries, &failing, now);
            assert_eq!(retry.errors, attempt);
            assert_eq!(
                retry.eligible_at,
                Some(now + Duration::from_secs(expected_delay))
            );
        }
        let quarantined = schedule_retry(&mut retries, &failing, now);
        assert_eq!(quarantined.errors, MAX_PURSUIT_ERRORS_PER_GENERATION);
        assert_eq!(quarantined.eligible_at, None);
        assert_eq!(retries.len(), 1);

        let sample = PursuitRetry {
            errors: 1,
            eligible_at: Some(now + Duration::from_secs(1)),
            claim_generation: 1,
        };
        let mut saturated = (0..MAX_TRACKED_PURSUIT_RETRIES)
            .map(|index| (format!("tracked-{index}"), sample))
            .collect::<HashMap<_, _>>();
        let overflow = schedule_retry(&mut saturated, &failing, now);
        assert!(overflow.eligible_at.is_none());
        assert_eq!(saturated.len(), MAX_TRACKED_PURSUIT_RETRIES);
    }

    #[test]
    fn quarantine_is_persisted_and_removed_from_resident_memory() {
        let fixture = TempDir::new().unwrap();
        let data = TempDir::new().unwrap();
        let workdir = TempDir::new().unwrap();
        let config = config(&workdir);
        let store = SqliteGoalStore::open(data.path().join("goals.sqlite3")).unwrap();
        create(&store, "quarantine", 0);
        let goal = store
            .claim(
                LOCAL_TASK_OWNER,
                "quarantine",
                DAEMON_GOAL_WORKER,
                60,
                Utc::now(),
            )
            .unwrap();
        let mut retries = HashMap::new();
        let mut retry = schedule_retry(&mut retries, &goal, Instant::now());
        for _ in 1..MAX_PURSUIT_ERRORS_PER_GENERATION {
            retry = schedule_retry(&mut retries, &goal, Instant::now());
        }
        let verifier = AcceptanceVerifier::new(workdir.path(), Duration::from_secs(1)).unwrap();
        let mut supervisor = DaemonGoalSupervisor {
            service: Arc::new(service(&fixture)),
            store: store.clone(),
            verifier,
            config,
            retry_after: retries,
            recovery_scan: RecoveryScan::default(),
        };

        supervisor.pause_quarantined(&goal, retry).unwrap();

        assert!(supervisor.retry_after.is_empty());
        let paused = store.get(LOCAL_TASK_OWNER, "quarantine").unwrap().unwrap();
        assert_eq!(paused.status, GoalStatus::Paused);
        assert!(paused.claimed_by.is_none());
    }

    #[test]
    fn retry_capacity_overflow_fails_closed_when_quarantine_cannot_persist() {
        let fixture = TempDir::new().unwrap();
        let data = TempDir::new().unwrap();
        let workdir = TempDir::new().unwrap();
        let config = config(&workdir);
        let store = SqliteGoalStore::open(data.path().join("goals.sqlite3")).unwrap();
        create(&store, "overflow", 0);
        let goal = store
            .claim(
                LOCAL_TASK_OWNER,
                "overflow",
                DAEMON_GOAL_WORKER,
                60,
                Utc::now(),
            )
            .unwrap();
        let sample = PursuitRetry {
            errors: 1,
            eligible_at: Some(Instant::now() + Duration::from_secs(60)),
            claim_generation: 1,
        };
        let mut retries = (0..MAX_TRACKED_PURSUIT_RETRIES)
            .map(|index| (format!("tracked-{index}"), sample))
            .collect::<HashMap<_, _>>();
        let overflow = schedule_retry(&mut retries, &goal, Instant::now());
        assert!(overflow.eligible_at.is_none());
        assert!(!retries.contains_key(&goal.id));
        let connection = rusqlite::Connection::open(store.path()).unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER reject_overflow_quarantine
                 BEFORE INSERT ON goal_events WHEN NEW.kind = 'paused'
                 BEGIN
                   SELECT RAISE(ABORT, 'injected quarantine failure');
                 END;",
            )
            .unwrap();
        drop(connection);
        let verifier = AcceptanceVerifier::new(workdir.path(), Duration::from_secs(1)).unwrap();
        let mut supervisor = DaemonGoalSupervisor {
            service: Arc::new(service(&fixture)),
            store: store.clone(),
            verifier,
            config,
            retry_after: retries,
            recovery_scan: RecoveryScan::default(),
        };

        assert!(supervisor.pause_quarantined(&goal, overflow).is_err());
        assert_eq!(supervisor.retry_after.len(), MAX_TRACKED_PURSUIT_RETRIES);
        let unchanged = store.get(LOCAL_TASK_OWNER, &goal.id).unwrap().unwrap();
        assert_eq!(unchanged.status, GoalStatus::InProgress);
        assert_eq!(unchanged.claimed_by.as_deref(), Some(DAEMON_GOAL_WORKER));
    }

    #[test]
    fn terminal_retry_entries_are_pruned_during_acquisition() {
        let directory = TempDir::new().unwrap();
        let (store, config) = supervisor_store(&directory);
        create(&store, "externally-paused", 0);
        let claimed = store
            .claim(
                LOCAL_TASK_OWNER,
                "externally-paused",
                DAEMON_GOAL_WORKER,
                60,
                Utc::now(),
            )
            .unwrap();
        let mut retries = HashMap::from([(
            claimed.id.clone(),
            PursuitRetry {
                errors: 1,
                eligible_at: Some(Instant::now() + Duration::from_secs(60)),
                claim_generation: claimed.claim_generation,
            },
        )]);
        store
            .pause(
                LOCAL_TASK_OWNER,
                &claimed.id,
                Some(DAEMON_GOAL_WORKER),
                Some("external transition"),
                Utc::now(),
            )
            .unwrap();

        let _ = acquire_from_store(&store, &config, &mut retries, &mut RecoveryScan::default())
            .unwrap();
        assert!(retries.is_empty());
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

        let quarantined = store.get(LOCAL_TASK_OWNER, "quarantined").unwrap().unwrap();
        let mut retries = HashMap::from([(
            "quarantined".into(),
            PursuitRetry {
                errors: MAX_PURSUIT_ERRORS_PER_GENERATION,
                eligible_at: None,
                claim_generation: quarantined.claim_generation,
            },
        )]);
        let next = acquire_from_store(&store, &config, &mut retries, &mut RecoveryScan::default())
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
