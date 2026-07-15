//! Bounded projection of durable AgentRun completion decisions into result sinks.
//!
//! The AgentRun store remains the lifecycle truth while result bytes remain in
//! an [`AgentCompletionSink`]. A committed completion is published and an
//! abandoned completion is discarded before its outbox event is acknowledged.
//! Repeating a pass after a crash between those two operations is safe because
//! sink mutations and `mark_projected` are both required to be idempotent.

use std::collections::BTreeSet;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt as _;
use tokio::time::timeout;
use vyane_agent::{
    AgentEvent, AgentEventKind, AgentStore, ProjectionDeferReason, ProjectionQuarantineReason,
    RunCompletionRecord, RunCompletionStatus, RunState,
};

use crate::{AgentCompletionSink, AgentCompletionSinkTransition};

const MAX_COMPLETION_SINKS: usize = 16;
const MAX_PUBLISH_BATCH: usize = 64;
const MAX_IDENTITY_BYTES: usize = 256;
const MAX_SINK_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_DEFER_DELAY: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCompletionPublisherOptions {
    pub batch_limit: usize,
    pub sink_timeout: Duration,
    pub defer_delay: Duration,
}

impl Default for AgentCompletionPublisherOptions {
    fn default() -> Self {
        Self {
            batch_limit: 16,
            sink_timeout: Duration::from_secs(10),
            defer_delay: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentCompletionProjectionStatus {
    /// The event does not describe a terminal completion and was acknowledged.
    Unrelated,
    Published,
    Discarded,
    CompletionMissing,
    CompletionConflict,
    SinkConflict,
    SinkMissing,
    SinkUnavailable,
    SinkPanicked,
    StoreFailed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCompletionProjectionReport {
    pub scanned: usize,
    pub acknowledged: usize,
    pub deferred: usize,
    pub quarantined: usize,
    pub has_more: bool,
    pub items: Vec<AgentCompletionProjectionStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentCompletionPublisherError {
    InvalidOwner,
    InvalidProjector,
    InvalidOptions,
    InvalidSink,
    DuplicateSinkKind,
    SinkMetadataPanicked,
    RuntimeUnavailable,
    StoreUnavailable,
}

impl fmt::Display for AgentCompletionPublisherError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidOwner => "completion publisher owner is invalid",
            Self::InvalidProjector => "completion publisher identity is invalid",
            Self::InvalidOptions => "completion publisher options are invalid",
            Self::InvalidSink => "completion publisher sink is invalid",
            Self::DuplicateSinkKind => "completion publisher sink kind is duplicated",
            Self::SinkMetadataPanicked => "completion publisher sink metadata panicked",
            Self::RuntimeUnavailable => "completion publisher requires a Tokio runtime",
            Self::StoreUnavailable => "completion publisher store is unavailable",
        })
    }
}

impl std::error::Error for AgentCompletionPublisherError {}

struct RegisteredSink {
    kind: String,
    sink: Arc<dyn AgentCompletionSink>,
}

/// Owner-bound one-shot completion outbox projector.
pub struct AgentCompletionPublisher {
    owner: String,
    projector: String,
    store: Arc<dyn AgentStore>,
    sinks: Arc<Vec<RegisteredSink>>,
    options: AgentCompletionPublisherOptions,
}

impl AgentCompletionPublisher {
    pub fn new(
        owner: impl Into<String>,
        projector: impl Into<String>,
        store: Arc<dyn AgentStore>,
        sinks: Vec<Arc<dyn AgentCompletionSink>>,
        options: AgentCompletionPublisherOptions,
    ) -> Result<Self, AgentCompletionPublisherError> {
        let owner = owner.into();
        let projector = projector.into();
        if !valid_identity(&owner) {
            return Err(AgentCompletionPublisherError::InvalidOwner);
        }
        if !valid_identity(&projector) {
            return Err(AgentCompletionPublisherError::InvalidProjector);
        }
        if options.batch_limit == 0
            || options.batch_limit > MAX_PUBLISH_BATCH
            || options.sink_timeout.is_zero()
            || options.sink_timeout > MAX_SINK_TIMEOUT
            || options.defer_delay.is_zero()
            || options.defer_delay > MAX_DEFER_DELAY
        {
            return Err(AgentCompletionPublisherError::InvalidOptions);
        }
        if sinks.len() > MAX_COMPLETION_SINKS {
            return Err(AgentCompletionPublisherError::InvalidSink);
        }
        let mut kinds = BTreeSet::new();
        let mut registered = Vec::with_capacity(sinks.len());
        for sink in sinks {
            let first = catch_unwind(AssertUnwindSafe(|| sink.kind().to_owned()))
                .map_err(|_| AgentCompletionPublisherError::SinkMetadataPanicked)?;
            let second = catch_unwind(AssertUnwindSafe(|| sink.kind().to_owned()))
                .map_err(|_| AgentCompletionPublisherError::SinkMetadataPanicked)?;
            if first != second || !valid_identity(&first) {
                return Err(AgentCompletionPublisherError::InvalidSink);
            }
            if !kinds.insert(first.clone()) {
                return Err(AgentCompletionPublisherError::DuplicateSinkKind);
            }
            registered.push(RegisteredSink { kind: first, sink });
        }
        Ok(Self {
            owner,
            projector,
            store,
            sinks: Arc::new(registered),
            options,
        })
    }

    pub async fn project_once(
        &self,
    ) -> Result<AgentCompletionProjectionReport, AgentCompletionPublisherError> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(AgentCompletionPublisherError::RuntimeUnavailable);
        }
        let store = Arc::clone(&self.store);
        let owner = self.owner.clone();
        let projector = self.projector.clone();
        let limit = self.options.batch_limit;
        let page = tokio::task::spawn_blocking(move || {
            store.unprojected_events(&owner, &projector, limit)
        })
        .await
        .map_err(|_| AgentCompletionPublisherError::StoreUnavailable)?
        .map_err(|_| AgentCompletionPublisherError::StoreUnavailable)?;

        let scanned = page.items.len();
        let mut acknowledged = 0;
        let mut deferred = 0;
        let mut quarantined = 0;
        let mut items = Vec::with_capacity(scanned);
        for event in page.items {
            let status = self.project_event(event.clone()).await;
            if matches!(
                status,
                AgentCompletionProjectionStatus::Unrelated
                    | AgentCompletionProjectionStatus::Published
                    | AgentCompletionProjectionStatus::Discarded
            ) {
                if self.acknowledge(event.event_id).await {
                    acknowledged += 1;
                } else {
                    items.push(AgentCompletionProjectionStatus::StoreFailed);
                    continue;
                }
            } else {
                let disposition = match status {
                    AgentCompletionProjectionStatus::CompletionMissing
                    | AgentCompletionProjectionStatus::CompletionConflict => Some(
                        ProjectionDisposition::Quarantine(ProjectionQuarantineReason::InvalidEvent),
                    ),
                    AgentCompletionProjectionStatus::SinkConflict => Some(
                        ProjectionDisposition::Quarantine(ProjectionQuarantineReason::SinkConflict),
                    ),
                    AgentCompletionProjectionStatus::SinkMissing => Some(
                        ProjectionDisposition::Defer(ProjectionDeferReason::MissingSink),
                    ),
                    AgentCompletionProjectionStatus::SinkUnavailable
                    | AgentCompletionProjectionStatus::SinkPanicked => Some(
                        ProjectionDisposition::Defer(ProjectionDeferReason::SinkUnavailable),
                    ),
                    AgentCompletionProjectionStatus::StoreFailed => None,
                    AgentCompletionProjectionStatus::Unrelated
                    | AgentCompletionProjectionStatus::Published
                    | AgentCompletionProjectionStatus::Discarded => None,
                };
                if let Some(disposition) = disposition {
                    if self
                        .record_disposition(event.event_id.clone(), disposition)
                        .await
                    {
                        match disposition {
                            ProjectionDisposition::Defer(_) => deferred += 1,
                            ProjectionDisposition::Quarantine(_) => quarantined += 1,
                        }
                    } else {
                        items.push(AgentCompletionProjectionStatus::StoreFailed);
                        continue;
                    }
                }
            }
            items.push(status);
        }
        Ok(AgentCompletionProjectionReport {
            scanned,
            acknowledged,
            deferred,
            quarantined,
            has_more: page.has_more,
            items,
        })
    }

    async fn project_event(&self, event: AgentEvent) -> AgentCompletionProjectionStatus {
        let action = match event.kind {
            AgentEventKind::CompletionCommitted => CompletionAction::Publish,
            AgentEventKind::CompletionAbandoned => CompletionAction::Discard,
            _ => return AgentCompletionProjectionStatus::Unrelated,
        };
        let Some(run_id) = event.run_id.as_deref() else {
            return AgentCompletionProjectionStatus::CompletionConflict;
        };
        let store = Arc::clone(&self.store);
        let owner = self.owner.clone();
        let run_id = run_id.to_owned();
        let completion = match tokio::task::spawn_blocking(move || {
            store.get_completion(&owner, &run_id)
        })
        .await
        {
            Ok(Ok(Some(completion))) => completion,
            Ok(Ok(None)) => return AgentCompletionProjectionStatus::CompletionMissing,
            Ok(Err(_)) | Err(_) => return AgentCompletionProjectionStatus::StoreFailed,
        };
        if !completion_matches_event(&self.owner, &completion, &event, action) {
            return AgentCompletionProjectionStatus::CompletionConflict;
        }
        let Some(sink) = self
            .sinks
            .iter()
            .find(|registered| registered.kind == completion.sink_kind)
            .map(|registered| Arc::clone(&registered.sink))
        else {
            return AgentCompletionProjectionStatus::SinkMissing;
        };
        let future = match catch_unwind(AssertUnwindSafe(|| match action {
            CompletionAction::Publish => sink.publish_transition(completion),
            CompletionAction::Discard => sink.discard_transition(completion),
        })) {
            Ok(future) => future,
            Err(_) => return AgentCompletionProjectionStatus::SinkPanicked,
        };
        let observation = timeout(
            self.options.sink_timeout,
            AssertUnwindSafe(future).catch_unwind(),
        )
        .await;
        match observation {
            Ok(Ok(AgentCompletionSinkTransition::Complete)) => match action {
                CompletionAction::Publish => AgentCompletionProjectionStatus::Published,
                CompletionAction::Discard => AgentCompletionProjectionStatus::Discarded,
            },
            Ok(Ok(AgentCompletionSinkTransition::Absent))
                if matches!(action, CompletionAction::Discard) =>
            {
                AgentCompletionProjectionStatus::Discarded
            }
            Ok(Ok(AgentCompletionSinkTransition::Conflict)) => {
                AgentCompletionProjectionStatus::SinkConflict
            }
            Ok(Ok(
                AgentCompletionSinkTransition::Absent | AgentCompletionSinkTransition::Unavailable,
            ))
            | Err(_) => AgentCompletionProjectionStatus::SinkUnavailable,
            Ok(Err(_)) => AgentCompletionProjectionStatus::SinkPanicked,
        }
    }

    async fn acknowledge(&self, event_id: String) -> bool {
        let store = Arc::clone(&self.store);
        let owner = self.owner.clone();
        let projector = self.projector.clone();
        matches!(
            tokio::task::spawn_blocking(move || {
                store.mark_projected(&owner, &projector, &event_id)
            })
            .await,
            Ok(Ok(()))
        )
    }

    async fn record_disposition(
        &self,
        event_id: String,
        disposition: ProjectionDisposition,
    ) -> bool {
        let store = Arc::clone(&self.store);
        let owner = self.owner.clone();
        let projector = self.projector.clone();
        let defer_delay = self.options.defer_delay;
        matches!(
            tokio::task::spawn_blocking(move || match disposition {
                ProjectionDisposition::Defer(reason) => {
                    store.defer_projection(&owner, &projector, &event_id, reason, defer_delay)
                }
                ProjectionDisposition::Quarantine(reason) => {
                    store.quarantine_projection(&owner, &projector, &event_id, reason)
                }
            })
            .await,
            Ok(Ok(()))
        )
    }
}

impl fmt::Debug for AgentCompletionPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentCompletionPublisher")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy)]
enum CompletionAction {
    Publish,
    Discard,
}

#[derive(Clone, Copy)]
enum ProjectionDisposition {
    Defer(ProjectionDeferReason),
    Quarantine(ProjectionQuarantineReason),
}

fn completion_matches_event(
    owner: &str,
    completion: &RunCompletionRecord,
    event: &AgentEvent,
    action: CompletionAction,
) -> bool {
    if completion.owner != owner
        || event.owner != owner
        || event.run_id.as_deref() != Some(completion.run_id.as_str())
        || event.worker_id != completion.worker_id
    {
        return false;
    }
    match action {
        CompletionAction::Publish => {
            completion.status == RunCompletionStatus::Committed
                && event.run_state == Some(RunState::Succeeded)
                && completion.committed_at == Some(event.occurred_at)
                && completion.committed_run_revision == event.run_revision
                && completion.abandoned_at.is_none()
                && completion.abandoned_run_revision.is_none()
        }
        CompletionAction::Discard => {
            completion.status == RunCompletionStatus::Abandoned
                && event.run_state.is_some_and(|state| {
                    matches!(
                        state,
                        RunState::Failed
                            | RunState::TimedOut
                            | RunState::Cancelled
                            | RunState::Interrupted
                    )
                })
                && completion.abandoned_at == Some(event.occurred_at)
                && completion.abandoned_run_revision == event.run_revision
                && completion.committed_at.is_none()
                && completion.committed_run_revision.is_none()
        }
    }
}

fn valid_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTITY_BYTES
        && value.trim() == value
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/' | b':')
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use chrono::{DateTime, TimeZone as _, Utc};
    use vyane_agent::{
        AgentClock, ControllerKind, ControllerRef, NewAgentRun, NewRunCompletion, NewWorker,
        RunFailureCode, RunMode, RunSettlement, SqliteAgentStore,
    };

    use super::*;
    use crate::AgentCompletionSinkObservation;

    fn digest(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    #[derive(Debug)]
    struct FixedClock(DateTime<Utc>);

    impl AgentClock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    struct RecordingSink {
        kind: &'static str,
        transition: AgentCompletionSinkTransition,
        publishes: AtomicUsize,
        discards: AtomicUsize,
    }

    impl RecordingSink {
        fn new(kind: &'static str, transition: AgentCompletionSinkTransition) -> Self {
            Self {
                kind,
                transition,
                publishes: AtomicUsize::new(0),
                discards: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl AgentCompletionSink for RecordingSink {
        fn kind(&self) -> &str {
            self.kind
        }

        async fn inspect(&self, _: RunCompletionRecord) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Unavailable
        }

        async fn publish(&self, _: RunCompletionRecord) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Unavailable
        }

        async fn discard(&self, _: RunCompletionRecord) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Unavailable
        }

        async fn publish_transition(
            &self,
            _: RunCompletionRecord,
        ) -> AgentCompletionSinkTransition {
            self.publishes.fetch_add(1, Ordering::SeqCst);
            self.transition
        }

        async fn discard_transition(
            &self,
            _: RunCompletionRecord,
        ) -> AgentCompletionSinkTransition {
            self.discards.fetch_add(1, Ordering::SeqCst);
            self.transition
        }
    }

    fn prepare(
        store: &SqliteAgentStore,
        suffix: &str,
        sink_kind: &str,
    ) -> (
        vyane_agent::PreparedRunCompletion,
        vyane_agent::RunLeaseReceipt,
    ) {
        let worker_id = format!("worker-{suffix}");
        let run_id = format!("run-{suffix}");
        store
            .create_root(
                "owner-a",
                &NewWorker {
                    id: worker_id.clone(),
                    logical_session_id: None,
                },
                &NewAgentRun {
                    id: run_id.clone(),
                    worker_id,
                    task_id: None,
                    trace_id: None,
                    parent_run_id: None,
                    execution_backend: vyane_agent::ExecutionBackend::NativeInProcess,
                    mode: RunMode::Autonomous,
                    target_key: "test/default".into(),
                    prompt_digest: digest('a'),
                    policy_digest: digest('b'),
                    available_at: Utc::now(),
                    timeout_seconds: 60,
                    max_resume_attempts: 0,
                },
            )
            .unwrap();
        let claimed = store
            .claim_due(
                "owner-a",
                vyane_agent::ExecutionBackend::NativeInProcess,
                "lease-a",
                30,
                1,
            )
            .unwrap()
            .remove(0);
        let started = store
            .start(
                "owner-a",
                &claimed.receipt,
                &ControllerRef {
                    kind: ControllerKind::InProcess,
                    id: format!("controller-{suffix}"),
                    fingerprint: Some(format!("fingerprint-{suffix}")),
                },
            )
            .unwrap();
        let permit = store
            .issue_execution_permit("owner-a", &started.receipt, &started.run.policy_digest)
            .unwrap();
        let prepared = store
            .prepare_completion(
                "owner-a",
                &permit,
                &NewRunCompletion {
                    id: format!("completion-{suffix}"),
                    sink_kind: sink_kind.into(),
                    publication_key: format!("publication-{suffix}"),
                    content_digest: digest('c'),
                    content_bytes: 7,
                },
            )
            .unwrap();
        (prepared, started.receipt)
    }

    fn publisher(
        store: Arc<dyn AgentStore>,
        projector: &str,
        sinks: Vec<Arc<dyn AgentCompletionSink>>,
    ) -> AgentCompletionPublisher {
        AgentCompletionPublisher::new(
            "owner-a",
            projector,
            store,
            sinks,
            AgentCompletionPublisherOptions {
                batch_limit: 64,
                sink_timeout: Duration::from_secs(1),
                defer_delay: Duration::from_secs(30),
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn sink_success_before_ack_replays_on_the_same_projector() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            Arc::new(SqliteAgentStore::open(directory.path().join("agent.sqlite3")).unwrap());
        let (prepared_publish, _) = prepare(&store, "publish", "recording-v1");
        store
            .commit_completion("owner-a", &prepared_publish.permit)
            .unwrap();
        let (prepared_discard, receipt) = prepare(&store, "discard", "recording-v1");
        store
            .settle(
                "owner-a",
                &receipt,
                RunSettlement::Failed {
                    code: RunFailureCode::Internal,
                },
            )
            .unwrap();
        drop(prepared_discard);

        let sink = Arc::new(RecordingSink::new(
            "recording-v1",
            AgentCompletionSinkTransition::Complete,
        ));
        let terminal_events = store
            .unprojected_events("owner-a", "test-observer", 64)
            .unwrap()
            .items
            .into_iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    AgentEventKind::CompletionCommitted | AgentEventKind::CompletionAbandoned
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(terminal_events.len(), 2);
        let erased_store: Arc<dyn AgentStore> = store.clone();
        let erased_sink: Arc<dyn AgentCompletionSink> = sink.clone();
        let projector = publisher(erased_store, "completion-replay", vec![erased_sink]);

        // Simulate a process crash after each idempotent sink transition but
        // before mark_projected: project_event performs no outbox ack.
        assert_eq!(
            projector.project_event(terminal_events[0].clone()).await,
            AgentCompletionProjectionStatus::Published
        );
        assert_eq!(
            projector.project_event(terminal_events[1].clone()).await,
            AgentCompletionProjectionStatus::Discarded
        );
        let report = projector.project_once().await.unwrap();

        assert!(
            report
                .items
                .contains(&AgentCompletionProjectionStatus::Published)
        );
        assert!(
            report
                .items
                .contains(&AgentCompletionProjectionStatus::Discarded)
        );
        assert_eq!(sink.publishes.load(Ordering::SeqCst), 2);
        assert_eq!(sink.discards.load(Ordering::SeqCst), 2);
        assert_eq!(projector.project_once().await.unwrap().scanned, 0);
    }

    #[tokio::test]
    async fn missing_and_conflicting_sinks_are_durable_and_do_not_block_later_success() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            Arc::new(SqliteAgentStore::open(directory.path().join("agent.sqlite3")).unwrap());
        let (missing, _) = prepare(&store, "missing", "missing-v1");
        store.commit_completion("owner-a", &missing.permit).unwrap();
        let (conflict, _) = prepare(&store, "conflict", "conflict-v1");
        store
            .commit_completion("owner-a", &conflict.permit)
            .unwrap();
        let (exact, _) = prepare(&store, "exact", "exact-v1");
        store.commit_completion("owner-a", &exact.permit).unwrap();

        let conflict_sink: Arc<dyn AgentCompletionSink> = Arc::new(RecordingSink::new(
            "conflict-v1",
            AgentCompletionSinkTransition::Conflict,
        ));
        let exact_sink = Arc::new(RecordingSink::new(
            "exact-v1",
            AgentCompletionSinkTransition::Complete,
        ));
        let erased_exact: Arc<dyn AgentCompletionSink> = exact_sink.clone();
        let erased_store: Arc<dyn AgentStore> = store.clone();
        let projector = publisher(
            erased_store,
            "completion-disposition",
            vec![conflict_sink, erased_exact],
        );
        let report = projector.project_once().await.unwrap();

        assert!(
            report
                .items
                .contains(&AgentCompletionProjectionStatus::SinkMissing)
        );
        assert!(
            report
                .items
                .contains(&AgentCompletionProjectionStatus::SinkConflict)
        );
        assert!(
            report
                .items
                .contains(&AgentCompletionProjectionStatus::Published)
        );
        assert_eq!(report.deferred, 1);
        assert_eq!(report.quarantined, 1);
        assert_eq!(exact_sink.publishes.load(Ordering::SeqCst), 1);
        assert_eq!(projector.project_once().await.unwrap().scanned, 0);
    }

    #[tokio::test]
    async fn defer_delay_is_derived_from_the_store_authoritative_clock() {
        let directory = tempfile::tempdir().unwrap();
        let clock = Arc::new(FixedClock(
            Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0).single().unwrap(),
        ));
        let store = Arc::new(
            SqliteAgentStore::open_with_clock(directory.path().join("agent.sqlite3"), clock)
                .unwrap(),
        );
        let (prepared, _) = prepare(&store, "future-clock", "missing-v1");
        store
            .commit_completion("owner-a", &prepared.permit)
            .unwrap();
        let erased_store: Arc<dyn AgentStore> = store;
        let projector = publisher(erased_store, "future-clock-projector", Vec::new());

        let report = projector.project_once().await.unwrap();
        assert_eq!(report.deferred, 1);
        assert!(
            report
                .items
                .contains(&AgentCompletionProjectionStatus::SinkMissing)
        );
    }
}
