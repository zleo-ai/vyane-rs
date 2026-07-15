//! Opt-in resident assembly for bounded goal pursuit.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use chrono::Utc;
use vyane_core::{CancellationToken, Sandbox};
use vyane_goal::{
    AcceptanceVerifier, GoalPursuer, GoalQuery, GoalRecord, GoalStatus, GoalStore, GoalStoreError,
    MAX_LEASE_SECONDS, PursuitConfig, SqliteGoalStore,
};
use vyane_service::VyaneService;

use crate::cli::DaemonGoalArgs;
use crate::goal::DispatchGoalRuntime;
use crate::task::LOCAL_TASK_OWNER;

const DAEMON_GOAL_WORKER: &str = "daemon-goal:auto-v1";

#[derive(Debug, Clone)]
pub(crate) struct DaemonGoalConfig {
    target: String,
    workdir: PathBuf,
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
            target,
            workdir,
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
}

impl DaemonGoalSupervisor {
    pub(crate) fn open(service: Arc<VyaneService>, config: DaemonGoalConfig) -> Result<Self> {
        let path = service.storage_paths().goal_db_path();
        let store = SqliteGoalStore::open(&path)
            .with_context(|| format!("open goal database {}", path.display()))?;
        let verifier = AcceptanceVerifier::new(&config.workdir, config.verifier_timeout)
            .context("construct automatic goal verifier")?;
        Ok(Self {
            service,
            store,
            verifier,
            config,
        })
    }

    pub(crate) async fn run(self, cancel: CancellationToken) -> Result<()> {
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
            let runtime_cancel = cancel.child_token();
            let shutdown_cancel = runtime_cancel.clone();
            let runtime = DispatchGoalRuntime::new(
                Arc::clone(&self.service),
                self.config.target.clone(),
                self.config.sandbox,
                runtime_cancel,
            );
            let pursuer = GoalPursuer::new(
                &self.store,
                &runtime,
                &self.verifier,
                self.config.pursuit.clone(),
            )
            .context("construct resident goal pursuer")?;
            let retry_after_error = tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    shutdown_cancel.cancel();
                    return Ok(());
                }
                result = pursuer.pursue(LOCAL_TASK_OWNER, &goal.id) => {
                    match result {
                        Ok(outcome) => {
                            tracing::info!(
                                goal_id = %goal.id,
                                status = ?outcome.status,
                                reason = %outcome.reason,
                                "resident goal pursuit settled"
                            );
                            false
                        }
                        Err(error) => {
                            tracing::warn!(
                                goal_id = %goal.id,
                                error = %error,
                                "resident goal pursuit stopped with an error"
                            );
                            true
                        }
                    }
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
        acquire_from_store(&self.store, &self.config)
    }
}

fn acquire_from_store<S: GoalStore>(
    store: &S,
    config: &DaemonGoalConfig,
) -> Result<Option<GoalRecord>> {
    let query = GoalQuery {
        statuses: vec![GoalStatus::InProgress],
        parent_goal_id: None,
        limit: 1_000,
    };
    for goal in store.list(LOCAL_TASK_OWNER, &query)? {
        let now = Utc::now();
        if goal.lease_active(now) {
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
                now,
            )
        } else {
            store.claim(
                LOCAL_TASK_OWNER,
                &goal.id,
                DAEMON_GOAL_WORKER,
                config.lease_seconds(),
                now,
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
    use tempfile::TempDir;
    use vyane_goal::{NewGoal, PursuitConfig};

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
            target: "builder".into(),
            workdir: directory.path().to_path_buf(),
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

    fn create(store: &SqliteGoalStore, id: &str, priority: u8) -> GoalRecord {
        let mut goal = NewGoal::new(id, Utc::now());
        goal.id = Some(id.into());
        goal.priority = priority;
        store.create(LOCAL_TASK_OWNER, goal).unwrap()
    }

    fn acquire(store: &SqliteGoalStore, config: &DaemonGoalConfig) -> Option<GoalRecord> {
        acquire_from_store(store, config).unwrap()
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
}
