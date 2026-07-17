//! Owner-frozen, bounded ingestion of typed external goal observations.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{StreamExt as _, stream};
use serde::Serialize;
use vyane_goal::{
    GoalContinuityReviewCheck, GoalContinuitySignal, GoalContinuitySignalKind, GoalStore,
    GoalStoreError, SqliteGoalStore,
};

use crate::StoragePaths;

pub const MAX_GOAL_OBSERVATION_WATCHERS: usize = 32;
pub const MAX_GOAL_OBSERVATION_CONCURRENCY: usize = 16;
pub const MAX_GOAL_OBSERVATIONS_PER_WATCHER: usize = 64;
const MAX_GOAL_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_WATCHER_ID_BYTES: usize = 128;

/// Exact primary target boundary observed by an external source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalObservationTarget {
    pub provider: String,
    pub harness: String,
    pub model: String,
}

/// Closed external fact types accepted by the continuity store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalObservationKind {
    QuotaReset,
    ReviewChecksPassed {
        repository: String,
        pull_request: u64,
        observation_id: String,
        observation_sequence: u64,
    },
    ReviewChecksFailed {
        repository: String,
        pull_request: u64,
        observation_id: String,
        observation_sequence: u64,
    },
}

/// One typed fact. Owner and source are deliberately absent: the ingestion
/// authority freezes the owner and binds a trusted watcher identity separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalObservation {
    pub goal_id: String,
    pub quota_event_id: String,
    pub target: GoalObservationTarget,
    pub observed_at: DateTime<Utc>,
    pub kind: GoalObservationKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalObservationSignalKind {
    QuotaReset,
    ReviewChecksPassed,
    ReviewChecksFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalObservationStatus {
    Recorded,
    Unchanged,
    Absent,
    Rejected,
    Unavailable,
}

/// Allowlisted receipt. It contains no owner, target, repository, source,
/// persisted state, event detail or free-form failure text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GoalObservationReceipt {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
    pub kind: GoalObservationSignalKind,
    pub status: GoalObservationStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalObservationIngressError {
    InvalidSource,
    Unavailable,
}

impl fmt::Display for GoalObservationIngressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidSource => "goal observation source is invalid",
            Self::Unavailable => "goal observation ingress is unavailable",
        })
    }
}

impl std::error::Error for GoalObservationIngressError {}

/// Explicitly opened mutation port with one durable owner frozen by authority.
#[derive(Clone)]
pub struct GoalObservationIngress {
    store: Arc<dyn GoalStore>,
    owner: Arc<str>,
}

impl GoalObservationIngress {
    pub(crate) fn open(
        paths: &StoragePaths,
        owner: Arc<str>,
    ) -> Result<Self, GoalObservationIngressError> {
        let store = SqliteGoalStore::open(paths.goal_db_path())
            .map_err(|_| GoalObservationIngressError::Unavailable)?;
        Ok(Self {
            store: Arc::new(store),
            owner,
        })
    }

    /// Bind a trusted, non-secret source identity before accepting facts.
    pub fn bind_source(
        &self,
        source: impl Into<String>,
    ) -> Result<GoalObservationSink, GoalObservationIngressError> {
        let source = source.into();
        validate_source(&source)?;
        Ok(GoalObservationSink {
            ingress: self.clone(),
            source: Arc::from(source),
        })
    }
}

impl fmt::Debug for GoalObservationIngress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GoalObservationIngress")
            .finish_non_exhaustive()
    }
}

/// A mutation capability bound to one frozen owner and one trusted source.
#[derive(Clone)]
pub struct GoalObservationSink {
    ingress: GoalObservationIngress,
    source: Arc<str>,
}

impl GoalObservationSink {
    /// Record exactly one typed observation. This may release a continuity
    /// dependency, but cannot queue/decide/consume approval or dispatch work.
    pub fn ingest(
        &self,
        observation: GoalObservation,
        recorded_at: DateTime<Utc>,
    ) -> GoalObservationReceipt {
        let kind = observation.signal_kind();
        if observation.goal_id.trim().is_empty() || observation.goal_id.len() > 256 {
            return GoalObservationReceipt {
                goal_id: None,
                kind,
                status: GoalObservationStatus::Rejected,
            };
        }
        let goal_id = observation.goal_id.clone();
        let signal = observation.into_signal(self.source.to_string());
        let status = match self.ingress.store.record_continuity_signal(
            &self.ingress.owner,
            &goal_id,
            &signal,
            recorded_at,
        ) {
            Ok(result) if result.changed => GoalObservationStatus::Recorded,
            Ok(_) => GoalObservationStatus::Unchanged,
            Err(GoalStoreError::NotFound { .. }) => GoalObservationStatus::Absent,
            Err(
                GoalStoreError::InvalidInput(_)
                | GoalStoreError::InvalidStatus { .. }
                | GoalStoreError::TakeoverBoundaryChanged { .. },
            ) => GoalObservationStatus::Rejected,
            Err(_) => GoalObservationStatus::Unavailable,
        };
        GoalObservationReceipt {
            goal_id: Some(goal_id),
            kind,
            status,
        }
    }
}

impl fmt::Debug for GoalObservationSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GoalObservationSink")
            .finish_non_exhaustive()
    }
}

impl GoalObservation {
    fn signal_kind(&self) -> GoalObservationSignalKind {
        match self.kind {
            GoalObservationKind::QuotaReset => GoalObservationSignalKind::QuotaReset,
            GoalObservationKind::ReviewChecksPassed { .. } => {
                GoalObservationSignalKind::ReviewChecksPassed
            }
            GoalObservationKind::ReviewChecksFailed { .. } => {
                GoalObservationSignalKind::ReviewChecksFailed
            }
        }
    }

    fn into_signal(self, source: String) -> GoalContinuitySignal {
        let (kind, review_check) = match self.kind {
            GoalObservationKind::QuotaReset => (GoalContinuitySignalKind::QuotaReset, None),
            GoalObservationKind::ReviewChecksPassed {
                repository,
                pull_request,
                observation_id,
                observation_sequence,
            } => (
                GoalContinuitySignalKind::ReviewChecksPassed,
                Some(GoalContinuityReviewCheck {
                    repository,
                    pull_request,
                    observation_id,
                    observation_sequence,
                }),
            ),
            GoalObservationKind::ReviewChecksFailed {
                repository,
                pull_request,
                observation_id,
                observation_sequence,
            } => (
                GoalContinuitySignalKind::ReviewChecksFailed,
                Some(GoalContinuityReviewCheck {
                    repository,
                    pull_request,
                    observation_id,
                    observation_sequence,
                }),
            ),
        };
        GoalContinuitySignal {
            kind,
            quota_event_id: self.quota_event_id,
            provider: self.target.provider,
            harness: self.target.harness,
            model: self.target.model,
            observed_at: self.observed_at,
            source,
            review_check,
        }
    }
}

fn validate_source(source: &str) -> Result<(), GoalObservationIngressError> {
    if source.is_empty()
        || source.len() > MAX_WATCHER_ID_BYTES
        || source.trim() != source
        || source.chars().any(char::is_control)
    {
        return Err(GoalObservationIngressError::InvalidSource);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GoalObservationWatchPolicy {
    pub timeout: Duration,
    pub max_observations_per_watcher: usize,
}

impl Default for GoalObservationWatchPolicy {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            max_observations_per_watcher: 32,
        }
    }
}

impl GoalObservationWatchPolicy {
    fn validate(self) -> Result<Self, GoalObservationRunnerError> {
        if self.timeout.is_zero()
            || self.timeout > MAX_GOAL_OBSERVATION_TIMEOUT
            || self.max_observations_per_watcher == 0
            || self.max_observations_per_watcher > MAX_GOAL_OBSERVATIONS_PER_WATCHER
        {
            return Err(GoalObservationRunnerError::InvalidConfiguration);
        }
        Ok(self)
    }
}

/// Limits supplied to a watcher; the runner enforces them independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GoalObservationWatchContext {
    pub timeout: Duration,
    pub max_observations: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalObservationWatcherErrorCode {
    Authentication,
    RateLimited,
    Unavailable,
    InvalidResponse,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GoalObservationWatcherError {
    pub code: GoalObservationWatcherErrorCode,
}

impl GoalObservationWatcherError {
    #[must_use]
    pub const fn new(code: GoalObservationWatcherErrorCode) -> Self {
        Self { code }
    }
}

#[async_trait]
pub trait GoalObservationWatcher: Send + Sync {
    /// Stable, non-secret source identity fixed by trusted assembly.
    fn id(&self) -> &str;

    /// Return a finite typed batch. Implementations own their endpoints and
    /// credentials; neither may be supplied by an observation.
    async fn observe(
        &self,
        context: GoalObservationWatchContext,
    ) -> Result<Vec<GoalObservation>, GoalObservationWatcherError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalObservationWatchStatus {
    Complete,
    Error,
    Timeout,
    InvalidBatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GoalObservationWatchReport {
    pub watcher_id: String,
    pub status: GoalObservationWatchStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub receipts: Vec<GoalObservationReceipt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<GoalObservationWatcherErrorCode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalObservationRunnerError {
    InvalidConfiguration,
    DuplicateWatcher,
}

impl fmt::Display for GoalObservationRunnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "goal observation runner configuration is invalid",
            Self::DuplicateWatcher => "goal observation watcher id is duplicated",
        })
    }
}

impl std::error::Error for GoalObservationRunnerError {}

struct BoundWatcher {
    watcher: Arc<dyn GoalObservationWatcher>,
    sink: GoalObservationSink,
}

/// One-shot bounded watcher poller. It has no periodic loop and no execution
/// authority. Fetching is concurrent; mutation is deterministic by watcher ID
/// and watcher-returned batch order.
pub struct GoalObservationRunner {
    watchers: Vec<BoundWatcher>,
    concurrency: usize,
    policy: GoalObservationWatchPolicy,
}

impl GoalObservationRunner {
    pub fn new(
        ingress: &GoalObservationIngress,
        watchers: Vec<Arc<dyn GoalObservationWatcher>>,
        concurrency: usize,
        policy: GoalObservationWatchPolicy,
    ) -> Result<Self, GoalObservationRunnerError> {
        if watchers.len() > MAX_GOAL_OBSERVATION_WATCHERS
            || concurrency == 0
            || concurrency > MAX_GOAL_OBSERVATION_CONCURRENCY
        {
            return Err(GoalObservationRunnerError::InvalidConfiguration);
        }
        let policy = policy.validate()?;
        let mut ids = HashSet::with_capacity(watchers.len());
        let mut bound = Vec::with_capacity(watchers.len());
        for watcher in watchers {
            let id = watcher.id();
            let sink = ingress
                .bind_source(id)
                .map_err(|_| GoalObservationRunnerError::InvalidConfiguration)?;
            if !ids.insert(id.to_string()) {
                return Err(GoalObservationRunnerError::DuplicateWatcher);
            }
            bound.push(BoundWatcher { watcher, sink });
        }
        Ok(Self {
            watchers: bound,
            concurrency,
            policy,
        })
    }

    pub async fn poll_once(&self) -> Vec<GoalObservationWatchReport> {
        let context = GoalObservationWatchContext {
            timeout: self.policy.timeout,
            max_observations: self.policy.max_observations_per_watcher,
        };
        let timeout = self.policy.timeout;
        let mut fetched = stream::iter(self.watchers.iter())
            .map(|bound| async move {
                let result = tokio::time::timeout(timeout, bound.watcher.observe(context)).await;
                (bound, result)
            })
            .buffer_unordered(self.concurrency)
            .collect::<Vec<_>>()
            .await;
        fetched.sort_by(|(left, _), (right, _)| left.watcher.id().cmp(right.watcher.id()));

        fetched
            .into_iter()
            .map(|(bound, result)| match result {
                Err(_) => GoalObservationWatchReport {
                    watcher_id: bound.watcher.id().to_string(),
                    status: GoalObservationWatchStatus::Timeout,
                    receipts: Vec::new(),
                    error: None,
                },
                Ok(Err(error)) => GoalObservationWatchReport {
                    watcher_id: bound.watcher.id().to_string(),
                    status: GoalObservationWatchStatus::Error,
                    receipts: Vec::new(),
                    error: Some(error.code),
                },
                Ok(Ok(observations))
                    if observations.len() > self.policy.max_observations_per_watcher =>
                {
                    GoalObservationWatchReport {
                        watcher_id: bound.watcher.id().to_string(),
                        status: GoalObservationWatchStatus::InvalidBatch,
                        receipts: Vec::new(),
                        error: Some(GoalObservationWatcherErrorCode::InvalidResponse),
                    }
                }
                Ok(Ok(observations)) => GoalObservationWatchReport {
                    watcher_id: bound.watcher.id().to_string(),
                    status: GoalObservationWatchStatus::Complete,
                    receipts: observations
                        .into_iter()
                        .map(|observation| bound.sink.ingest(observation, Utc::now()))
                        .collect(),
                    error: None,
                },
            })
            .collect()
    }
}

impl fmt::Debug for GoalObservationRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GoalObservationRunner")
            .field("watchers", &self.watchers.len())
            .field("concurrency", &self.concurrency)
            .field("policy", &self.policy)
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use chrono::TimeZone as _;
    use tempfile::TempDir;
    use vyane_config::ResolvedConfig;
    use vyane_goal::{
        GoalContinuityMode, GoalContinuityPolicy, GoalExecutionTarget, GoalQuotaEvent, NewGoal,
        apply_quota_handoff_events,
    };

    use super::*;
    use crate::{LoadedConfig, OwnerContext, VyaneService};

    fn service(directory: &TempDir) -> VyaneService {
        VyaneService::from_loaded_with_paths(
            LoadedConfig {
                config: ResolvedConfig::default(),
                files: Vec::new(),
                secrets: BTreeMap::new(),
            },
            StoragePaths::from_data_dir(directory.path()),
        )
        .unwrap()
    }

    fn target(role: &str) -> GoalExecutionTarget {
        GoalExecutionTarget {
            provider: "provider".into(),
            protocol: "openai_chat".into(),
            harness: "harness".into(),
            model: "model".into(),
            profile: None,
            role: role.into(),
        }
    }

    fn create_goal(store: &SqliteGoalStore, owner: &str, id: &str) {
        let mut goal = NewGoal::new("private title", Utc.timestamp_opt(1_000, 0).unwrap());
        goal.id = Some(id.into());
        goal.continuity_policy = Some(GoalContinuityPolicy {
            mode: GoalContinuityMode::QuotaHandoff,
            primary: target("primary"),
            takeover: vec![target("takeover")],
            reviewer: None,
            resume_primary_after_reset: true,
            require_review_before_resume: false,
            wait_for_review_checks_before_resume: false,
        });
        store.create(owner, goal).unwrap();
        store
            .start(owner, id, Utc.timestamp_opt(1_001, 0).unwrap())
            .unwrap();
        apply_quota_handoff_events(
            store,
            owner,
            &[GoalQuotaEvent {
                event_id: "quota".into(),
                goal_id: Some(id.into()),
                provider: "provider".into(),
                harness: "harness".into(),
                model: "model".into(),
                session_id: None,
                observed_at: Utc.timestamp_opt(1_002, 0).unwrap(),
                estimated_reset_at: None,
            }],
            Utc.timestamp_opt(1_003, 0).unwrap(),
        )
        .unwrap();
    }

    fn create_review_goal(store: &SqliteGoalStore, owner: &str, id: &str) {
        let mut goal = NewGoal::new("private review title", Utc.timestamp_opt(1_000, 0).unwrap());
        goal.id = Some(id.into());
        goal.continuity_policy = Some(GoalContinuityPolicy {
            mode: GoalContinuityMode::QuotaHandoff,
            primary: target("primary"),
            takeover: vec![target("takeover")],
            reviewer: Some(target("reviewer")),
            resume_primary_after_reset: true,
            require_review_before_resume: true,
            wait_for_review_checks_before_resume: true,
        });
        store.create(owner, goal).unwrap();
        store
            .start(owner, id, Utc.timestamp_opt(1_001, 0).unwrap())
            .unwrap();
        apply_quota_handoff_events(
            store,
            owner,
            &[GoalQuotaEvent {
                event_id: "quota".into(),
                goal_id: Some(id.into()),
                provider: "provider".into(),
                harness: "harness".into(),
                model: "model".into(),
                session_id: None,
                observed_at: Utc.timestamp_opt(1_002, 0).unwrap(),
                estimated_reset_at: None,
            }],
            Utc.timestamp_opt(1_003, 0).unwrap(),
        )
        .unwrap();
    }

    fn reset(goal_id: &str) -> GoalObservation {
        GoalObservation {
            goal_id: goal_id.into(),
            quota_event_id: "quota".into(),
            target: GoalObservationTarget {
                provider: "provider".into(),
                harness: "harness".into(),
                model: "model".into(),
            },
            observed_at: Utc.timestamp_opt(2_000, 0).unwrap(),
            kind: GoalObservationKind::QuotaReset,
        }
    }

    #[test]
    fn goal_database_is_opened_only_after_explicit_ingress_construction() {
        let directory = TempDir::new().unwrap();
        let service = service(&directory);
        assert!(!service.storage_paths().goal_db_path().exists());

        let _ingress = service
            .goal_observation_ingress(OwnerContext::single_user_local())
            .unwrap();

        assert!(service.storage_paths().goal_db_path().is_file());
    }

    #[test]
    fn owner_and_source_are_frozen_and_replay_is_idempotent() {
        let directory = TempDir::new().unwrap();
        let service = service(&directory);
        let store = SqliteGoalStore::open(service.storage_paths().goal_db_path()).unwrap();
        create_goal(&store, "local", "shared");
        create_goal(&store, "foreign", "shared");
        let local_before = store.events("local", "shared").unwrap().len();
        let foreign_before = store.get("foreign", "shared").unwrap().unwrap();

        let ingress = service
            .goal_observation_ingress(OwnerContext::single_user_local())
            .unwrap();
        let sink = ingress.bind_source("quota-watcher").unwrap();
        let first = sink.ingest(reset("shared"), Utc.timestamp_opt(2_001, 0).unwrap());
        let second = sink.ingest(reset("shared"), Utc.timestamp_opt(2_002, 0).unwrap());

        assert_eq!(first.status, GoalObservationStatus::Recorded);
        assert_eq!(second.status, GoalObservationStatus::Unchanged);
        assert_eq!(
            store.events("local", "shared").unwrap().len(),
            local_before + 1
        );
        assert_eq!(
            store.get("foreign", "shared").unwrap().unwrap(),
            foreign_before
        );
        let local = store.get("local", "shared").unwrap().unwrap();
        assert_eq!(
            local.continuity_state.unwrap().ready_signals[0].source,
            "quota-watcher"
        );
    }

    #[test]
    fn absent_and_invalid_facts_use_closed_receipts_without_writes() {
        let directory = TempDir::new().unwrap();
        let service = service(&directory);
        let store = SqliteGoalStore::open(service.storage_paths().goal_db_path()).unwrap();
        create_goal(&store, "local", "goal");
        let before = store.get("local", "goal").unwrap().unwrap();
        let sink = service
            .goal_observation_ingress(OwnerContext::single_user_local())
            .unwrap()
            .bind_source("watcher")
            .unwrap();

        let absent = sink.ingest(reset("missing"), Utc.timestamp_opt(2_001, 0).unwrap());
        let mut mismatch = reset("goal");
        mismatch.target.model = "other".into();
        let rejected = sink.ingest(mismatch, Utc.timestamp_opt(2_002, 0).unwrap());
        let invalid_id = sink.ingest(
            reset(&"SENSITIVE".repeat(40)),
            Utc.timestamp_opt(2_003, 0).unwrap(),
        );

        assert_eq!(absent.status, GoalObservationStatus::Absent);
        assert_eq!(rejected.status, GoalObservationStatus::Rejected);
        assert_eq!(invalid_id.status, GoalObservationStatus::Rejected);
        assert_eq!(invalid_id.goal_id, None);
        assert_eq!(store.get("local", "goal").unwrap().unwrap(), before);
        assert!(!serde_json::to_string(&rejected).unwrap().contains("other"));
        assert!(
            !serde_json::to_string(&invalid_id)
                .unwrap()
                .contains("SENSITIVE")
        );
    }

    #[test]
    fn typed_review_observation_preserves_sequence_evidence_but_receipt_redacts_it() {
        let directory = TempDir::new().unwrap();
        let service = service(&directory);
        let store = SqliteGoalStore::open(service.storage_paths().goal_db_path()).unwrap();
        create_review_goal(&store, "local", "review-goal");
        let sink = service
            .goal_observation_ingress(OwnerContext::single_user_local())
            .unwrap()
            .bind_source("review-watcher")
            .unwrap();
        let mut observation = reset("review-goal");
        observation.kind = GoalObservationKind::ReviewChecksFailed {
            repository: "public/example".into(),
            pull_request: 42,
            observation_id: "check-run-7".into(),
            observation_sequence: 7,
        };

        let receipt = sink.ingest(observation, Utc.timestamp_opt(2_001, 0).unwrap());

        assert_eq!(receipt.status, GoalObservationStatus::Recorded);
        assert_eq!(receipt.kind, GoalObservationSignalKind::ReviewChecksFailed);
        let state = store
            .get("local", "review-goal")
            .unwrap()
            .unwrap()
            .continuity_state
            .unwrap();
        assert_eq!(state.review_observation_high_water, 7);
        assert_eq!(state.ready_signals[0].source, "review-watcher");
        let encoded = serde_json::to_string(&receipt).unwrap();
        assert!(!encoded.contains("public/example"));
        assert!(!encoded.contains("check-run-7"));
        assert!(!encoded.contains("review-watcher"));
    }

    enum FakeResult {
        Batch(Vec<GoalObservation>),
        Error,
        Delay,
    }

    struct FakeWatcher {
        id: String,
        result: Mutex<Option<FakeResult>>,
    }

    #[async_trait]
    impl GoalObservationWatcher for FakeWatcher {
        fn id(&self) -> &str {
            &self.id
        }

        async fn observe(
            &self,
            _context: GoalObservationWatchContext,
        ) -> Result<Vec<GoalObservation>, GoalObservationWatcherError> {
            let result = {
                let mut result = self.result.lock().unwrap();
                result.take().unwrap()
            };
            match result {
                FakeResult::Batch(batch) => Ok(batch),
                FakeResult::Error => Err(GoalObservationWatcherError::new(
                    GoalObservationWatcherErrorCode::Unavailable,
                )),
                FakeResult::Delay => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(Vec::new())
                }
            }
        }
    }

    fn watcher(id: &str, result: FakeResult) -> Arc<dyn GoalObservationWatcher> {
        Arc::new(FakeWatcher {
            id: id.into(),
            result: Mutex::new(Some(result)),
        })
    }

    #[tokio::test]
    async fn runner_is_bounded_sorted_and_failure_isolated() {
        let directory = TempDir::new().unwrap();
        let service = service(&directory);
        let store = SqliteGoalStore::open(service.storage_paths().goal_db_path()).unwrap();
        create_goal(&store, "local", "goal");
        let ingress = service
            .goal_observation_ingress(OwnerContext::single_user_local())
            .unwrap();
        let runner = GoalObservationRunner::new(
            &ingress,
            vec![
                watcher("z-timeout", FakeResult::Delay),
                watcher("a-good", FakeResult::Batch(vec![reset("goal")])),
                watcher("m-error", FakeResult::Error),
            ],
            2,
            GoalObservationWatchPolicy {
                timeout: Duration::from_millis(10),
                max_observations_per_watcher: 1,
            },
        )
        .unwrap();

        let reports = runner.poll_once().await;
        assert_eq!(
            reports
                .iter()
                .map(|report| report.watcher_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a-good", "m-error", "z-timeout"]
        );
        assert_eq!(reports[0].status, GoalObservationWatchStatus::Complete);
        assert_eq!(
            reports[0].receipts[0].status,
            GoalObservationStatus::Recorded
        );
        assert_eq!(reports[1].status, GoalObservationWatchStatus::Error);
        assert_eq!(reports[2].status, GoalObservationWatchStatus::Timeout);
    }

    #[tokio::test]
    async fn oversized_batch_is_rejected_before_any_mutation() {
        let directory = TempDir::new().unwrap();
        let service = service(&directory);
        let store = SqliteGoalStore::open(service.storage_paths().goal_db_path()).unwrap();
        create_goal(&store, "local", "goal");
        let before = store.get("local", "goal").unwrap().unwrap();
        let ingress = service
            .goal_observation_ingress(OwnerContext::single_user_local())
            .unwrap();
        let runner = GoalObservationRunner::new(
            &ingress,
            vec![watcher(
                "oversized",
                FakeResult::Batch(vec![reset("goal"), reset("goal")]),
            )],
            1,
            GoalObservationWatchPolicy {
                timeout: Duration::from_secs(1),
                max_observations_per_watcher: 1,
            },
        )
        .unwrap();

        let reports = runner.poll_once().await;
        assert_eq!(reports[0].status, GoalObservationWatchStatus::InvalidBatch);
        assert!(reports[0].receipts.is_empty());
        assert_eq!(store.get("local", "goal").unwrap().unwrap(), before);
    }

    #[test]
    fn runner_rejects_duplicate_or_invalid_watcher_identity() {
        let directory = TempDir::new().unwrap();
        let service = service(&directory);
        let ingress = service
            .goal_observation_ingress(OwnerContext::single_user_local())
            .unwrap();
        let duplicates = GoalObservationRunner::new(
            &ingress,
            vec![
                watcher("same", FakeResult::Batch(Vec::new())),
                watcher("same", FakeResult::Batch(Vec::new())),
            ],
            1,
            GoalObservationWatchPolicy::default(),
        );
        assert!(matches!(
            duplicates,
            Err(GoalObservationRunnerError::DuplicateWatcher)
        ));
        assert!(matches!(
            ingress.bind_source(" bad"),
            Err(GoalObservationIngressError::InvalidSource)
        ));
    }
}
