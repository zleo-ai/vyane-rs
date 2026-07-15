//! Body-free completion-sink reconciliation boundary.
//!
//! Result bodies remain in a domain sink such as `vyane-message`. AgentRun
//! recovery receives only the frozen completion descriptor and may settle
//! success after exact controller loss only when the matching sink proves that
//! descriptor was staged or already published.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use vyane_agent::{NewRunCompletion, RunCompletionRecord};
use vyane_message::{
    IdempotencyKey, MessagePublicationStatus, MessageRequestDigest, MessageStore,
    MessageStoreError, NewMessage,
};

/// Stable completion-sink identity for the `vyane-message` publication gate.
pub const MESSAGE_COMPLETION_SINK_KIND: &str = "vyane-message-v1";
/// Stable producer namespace reserved for AgentRun completion messages.
pub const MESSAGE_COMPLETION_PRODUCER: &str = "vyane-agent-completion-v1";

/// Body-free observation of one exact frozen completion descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentCompletionSinkObservation {
    /// The requested operation proved exact sink truth. For `inspect`, the
    /// exact digest/key is staged or already published.
    Exact,
    /// No publication exists under the frozen identity.
    Absent,
    /// The identity exists with different immutable metadata or was discarded.
    Conflict,
    /// The sink could not prove any of the other states.
    Unavailable,
}

/// Result of one idempotent completion-sink mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentCompletionSinkTransition {
    /// The target terminal state is now exact, including an exact replay.
    Complete,
    /// No publication exists under the frozen identity.
    Absent,
    /// The identity exists with conflicting metadata or terminal state.
    Conflict,
    /// The sink could not prove any of the other states.
    Unavailable,
}

/// Trusted, idempotent sink boundary for completion recovery and projection.
///
/// Implementations receive no controller ticket, execution permit, raw
/// AgentStore, or settlement authority. `inspect`, `publish`, and `discard`
/// must bind every lookup to the supplied owner, sink kind, publication key,
/// and content digest. Dropped futures may be retried, so external mutations
/// must be idempotent and reconciliation-safe.
#[async_trait]
pub trait AgentCompletionSink: Send + Sync {
    /// Stable lowercase identity matched to `RunCompletionRecord::sink_kind`.
    fn kind(&self) -> &str;

    async fn inspect(&self, completion: RunCompletionRecord) -> AgentCompletionSinkObservation;

    async fn publish(&self, completion: RunCompletionRecord) -> AgentCompletionSinkObservation;

    async fn discard(&self, completion: RunCompletionRecord) -> AgentCompletionSinkObservation;

    /// Precise mutation result for publishing. New projectors should use this
    /// surface; the observation-returning method remains source compatible.
    async fn publish_transition(
        &self,
        completion: RunCompletionRecord,
    ) -> AgentCompletionSinkTransition {
        observation_transition(self.publish(completion).await)
    }

    /// Precise mutation result for discarding. `Absent` is distinct from an
    /// exact discarded replay so callers can choose their acknowledgement rule.
    async fn discard_transition(
        &self,
        completion: RunCompletionRecord,
    ) -> AgentCompletionSinkTransition {
        observation_transition(self.discard(completion).await)
    }
}

const fn observation_transition(
    observation: AgentCompletionSinkObservation,
) -> AgentCompletionSinkTransition {
    match observation {
        AgentCompletionSinkObservation::Exact => AgentCompletionSinkTransition::Complete,
        AgentCompletionSinkObservation::Absent => AgentCompletionSinkTransition::Absent,
        AgentCompletionSinkObservation::Conflict => AgentCompletionSinkTransition::Conflict,
        AgentCompletionSinkObservation::Unavailable => AgentCompletionSinkTransition::Unavailable,
    }
}

/// Build the frozen AgentRun completion descriptor for one exact message.
///
/// The message must use [`MESSAGE_COMPLETION_PRODUCER`]. Its idempotency key is
/// the reconstructable publication key, while the complete canonical message
/// request digest (including body, payload, routing, and metadata) is frozen as
/// `content_digest`.
pub fn message_run_completion(
    completion_id: impl Into<String>,
    message: &NewMessage,
) -> vyane_message::Result<NewRunCompletion> {
    let completion_id = completion_id.into();
    if completion_id.is_empty()
        || completion_id.len() > 64
        || completion_id.trim() != completion_id
        || completion_id.contains('\0')
        || completion_id.chars().any(char::is_control)
    {
        return Err(MessageStoreError::InvalidInput(
            "completion id is invalid".into(),
        ));
    }
    if message.idempotency.producer != MESSAGE_COMPLETION_PRODUCER {
        return Err(MessageStoreError::InvalidInput(
            "message does not use the completion producer namespace".into(),
        ));
    }
    let digest = message.request_digest()?;
    let content_bytes = u64::try_from(
        serde_json::to_vec(message)
            .map_err(|_| MessageStoreError::InvalidInput("message cannot be serialized".into()))?
            .len(),
    )
    .map_err(|_| MessageStoreError::InvalidInput("message is oversized".into()))?;
    Ok(NewRunCompletion {
        id: completion_id,
        sink_kind: MESSAGE_COMPLETION_SINK_KIND.into(),
        publication_key: message.idempotency.key.clone(),
        content_digest: digest.as_str().into(),
        content_bytes,
    })
}

/// Body-free configuration failure for a message completion sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageAgentCompletionSinkConfigError;

impl fmt::Display for MessageAgentCompletionSinkConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("message completion sink owner is invalid")
    }
}

impl std::error::Error for MessageAgentCompletionSinkConfigError {}

/// Owner-bound AgentRun completion sink backed by `vyane-message`.
#[derive(Clone)]
pub struct MessageAgentCompletionSink {
    owner: String,
    store: Arc<dyn MessageStore>,
}

impl MessageAgentCompletionSink {
    pub fn new(
        owner: impl Into<String>,
        store: Arc<dyn MessageStore>,
    ) -> Result<Self, MessageAgentCompletionSinkConfigError> {
        let owner = owner.into();
        if owner.is_empty()
            || owner.len() > 256
            || owner.trim() != owner
            || owner.contains('\0')
            || owner.chars().any(char::is_control)
        {
            return Err(MessageAgentCompletionSinkConfigError);
        }
        Ok(Self { owner, store })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use static_assertions::{assert_impl_all, assert_not_impl_any};
    use vyane_agent::{RunCompletionRecord, RunCompletionStatus};
    use vyane_message::{
        EndpointKind, EndpointRef, IdempotencyKey, MessageDirection, MessageStore, NewDelivery,
        NewMessage, SqliteMessageStore,
    };

    use super::*;

    assert_impl_all!(MessageAgentCompletionSink: Send, Sync, Clone);
    assert_not_impl_any!(MessageAgentCompletionSink: serde::Serialize, serde::de::DeserializeOwned);

    fn message(key: &str, body: &str) -> NewMessage {
        NewMessage {
            conversation_id: "conversation-1".into(),
            session_id: None,
            direction: MessageDirection::Internal,
            kind: "completion".into(),
            sender: EndpointRef {
                kind: EndpointKind::Agent,
                id: "agent-1".into(),
            },
            body: body.into(),
            payload: serde_json::json!({"result": true}),
            reply_to: None,
            trace_id: Some("trace-1".into()),
            correlation_id: Some("run-1".into()),
            idempotency: IdempotencyKey {
                producer: MESSAGE_COMPLETION_PRODUCER.into(),
                key: key.into(),
            },
            deliveries: vec![NewDelivery {
                route: "worker".into(),
                target: EndpointRef {
                    kind: EndpointKind::Worker,
                    id: "worker-1".into(),
                },
                available_at: None,
                expires_at: None,
                max_attempts: 3,
            }],
        }
    }

    fn record(owner: &str, completion: NewRunCompletion) -> RunCompletionRecord {
        RunCompletionRecord {
            owner: owner.into(),
            run_id: "run-1".into(),
            worker_id: "worker-1".into(),
            worker_generation: 1,
            execution_backend: vyane_agent::ExecutionBackend::NativeInProcess,
            completion_id: completion.id,
            sink_kind: completion.sink_kind,
            publication_key: completion.publication_key,
            content_digest: completion.content_digest,
            content_bytes: completion.content_bytes,
            status: RunCompletionStatus::Prepared,
            prepared_at: Utc::now(),
            prepared_run_revision: 1,
            committed_at: None,
            committed_run_revision: None,
            abandoned_at: None,
            abandoned_run_revision: None,
            committed_by_operation_id: None,
            revision: 0,
        }
    }

    fn fixture() -> (
        tempfile::TempDir,
        Arc<SqliteMessageStore>,
        MessageAgentCompletionSink,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let store =
            Arc::new(SqliteMessageStore::open(directory.path().join("messages.sqlite3")).unwrap());
        let erased: Arc<dyn MessageStore> = store.clone();
        let sink = MessageAgentCompletionSink::new("owner-a", erased).unwrap();
        (directory, store, sink)
    }

    #[test]
    fn helper_freezes_the_complete_request_digest_and_reconstructable_key() {
        let canary = "RESULT-BODY-CANARY";
        let request = message("result.run-1", canary);
        let completion = message_run_completion("completion-1", &request).unwrap();
        assert_eq!(completion.sink_kind, MESSAGE_COMPLETION_SINK_KIND);
        assert_eq!(completion.publication_key, "result.run-1");
        assert_eq!(
            completion.content_digest,
            request.request_digest().unwrap().as_str()
        );
        assert!(completion.content_bytes > u64::try_from(canary.len()).unwrap());
        assert!(!format!("{completion:?}").contains(canary));

        let mut drift = request.clone();
        drift.payload = serde_json::json!({"result": false});
        assert_ne!(
            message_run_completion("completion-1", &drift)
                .unwrap()
                .content_digest,
            completion.content_digest
        );
        let mut wrong_producer = request;
        wrong_producer.idempotency.producer = "other".into();
        assert!(message_run_completion("completion-1", &wrong_producer).is_err());
        assert!(message_run_completion("", &wrong_producer).is_err());
    }

    #[tokio::test]
    async fn exact_staged_publication_inspects_and_publishes_idempotently() {
        let (_directory, store, sink) = fixture();
        let request = message("publish", "body");
        let completion = record(
            "owner-a",
            message_run_completion("completion-publish", &request).unwrap(),
        );
        store.stage("owner-a", &request).unwrap();

        assert_eq!(
            sink.inspect(completion.clone()).await,
            AgentCompletionSinkObservation::Exact
        );
        assert_eq!(
            sink.publish_transition(completion.clone()).await,
            AgentCompletionSinkTransition::Complete
        );
        assert_eq!(
            sink.publish_transition(completion.clone()).await,
            AgentCompletionSinkTransition::Complete
        );
        assert_eq!(
            sink.inspect(completion.clone()).await,
            AgentCompletionSinkObservation::Exact
        );
        assert_eq!(
            sink.discard_transition(completion).await,
            AgentCompletionSinkTransition::Conflict
        );

        let already_published = message("already-published", "body");
        let already_published_completion = record(
            "owner-a",
            message_run_completion("completion-already-published", &already_published).unwrap(),
        );
        store.enqueue("owner-a", &already_published).unwrap();
        assert_eq!(
            sink.publish_transition(already_published_completion.clone())
                .await,
            AgentCompletionSinkTransition::Complete
        );
        assert_eq!(
            sink.discard_transition(already_published_completion).await,
            AgentCompletionSinkTransition::Conflict
        );
    }

    #[tokio::test]
    async fn discard_absence_conflict_and_owner_scope_are_distinct() {
        let (_directory, store, sink) = fixture();
        let request = message("discard", "body");
        let completion = record(
            "owner-a",
            message_run_completion("completion-discard", &request).unwrap(),
        );
        assert_eq!(
            sink.inspect(completion.clone()).await,
            AgentCompletionSinkObservation::Absent
        );
        assert_eq!(
            sink.publish_transition(completion.clone()).await,
            AgentCompletionSinkTransition::Absent
        );
        assert_eq!(
            sink.discard_transition(completion.clone()).await,
            AgentCompletionSinkTransition::Absent
        );

        store.stage("owner-a", &request).unwrap();
        assert_eq!(
            sink.discard_transition(completion.clone()).await,
            AgentCompletionSinkTransition::Complete
        );
        assert_eq!(
            sink.discard_transition(completion.clone()).await,
            AgentCompletionSinkTransition::Complete
        );
        assert_eq!(
            sink.inspect(completion.clone()).await,
            AgentCompletionSinkObservation::Conflict
        );
        assert_eq!(
            sink.publish_transition(completion.clone()).await,
            AgentCompletionSinkTransition::Conflict
        );

        let mut drift = completion.clone();
        drift.content_digest = "d".repeat(64);
        assert_eq!(
            sink.inspect(drift).await,
            AgentCompletionSinkObservation::Conflict
        );
        let mut foreign = completion;
        foreign.owner = "owner-b".into();
        assert_eq!(
            sink.inspect(foreign).await,
            AgentCompletionSinkObservation::Conflict
        );
    }

    #[tokio::test]
    async fn unavailable_store_is_not_misreported_as_absent_or_conflict() {
        let (directory, _store, sink) = fixture();
        let request = message("unavailable", "body");
        let completion = record(
            "owner-a",
            message_run_completion("completion-unavailable", &request).unwrap(),
        );
        std::fs::remove_file(directory.path().join("messages.sqlite3")).unwrap();
        assert_eq!(
            sink.inspect(completion).await,
            AgentCompletionSinkObservation::Unavailable
        );
        assert!(!format!("{sink:?}").contains("owner-a"));
    }

    #[test]
    fn constructor_rejects_invalid_owner_without_store_access() {
        let directory = tempfile::tempdir().unwrap();
        let store: Arc<dyn MessageStore> =
            Arc::new(SqliteMessageStore::open(directory.path().join("messages.sqlite3")).unwrap());
        assert!(MessageAgentCompletionSink::new(" bad", store).is_err());
    }
}

impl fmt::Debug for MessageAgentCompletionSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MessageAgentCompletionSink")
            .field("kind", &MESSAGE_COMPLETION_SINK_KIND)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy)]
enum MessageCompletionOperation {
    Inspect,
    Publish,
    Discard,
}

#[async_trait]
impl AgentCompletionSink for MessageAgentCompletionSink {
    fn kind(&self) -> &str {
        MESSAGE_COMPLETION_SINK_KIND
    }

    async fn inspect(&self, completion: RunCompletionRecord) -> AgentCompletionSinkObservation {
        self.operate(completion, MessageCompletionOperation::Inspect)
            .await
    }

    async fn publish(&self, completion: RunCompletionRecord) -> AgentCompletionSinkObservation {
        self.operate(completion, MessageCompletionOperation::Publish)
            .await
    }

    async fn discard(&self, completion: RunCompletionRecord) -> AgentCompletionSinkObservation {
        self.operate(completion, MessageCompletionOperation::Discard)
            .await
    }

    async fn publish_transition(
        &self,
        completion: RunCompletionRecord,
    ) -> AgentCompletionSinkTransition {
        observation_transition(
            self.operate(completion, MessageCompletionOperation::Publish)
                .await,
        )
    }

    async fn discard_transition(
        &self,
        completion: RunCompletionRecord,
    ) -> AgentCompletionSinkTransition {
        observation_transition(
            self.operate(completion, MessageCompletionOperation::Discard)
                .await,
        )
    }
}

impl MessageAgentCompletionSink {
    async fn operate(
        &self,
        completion: RunCompletionRecord,
        operation: MessageCompletionOperation,
    ) -> AgentCompletionSinkObservation {
        if completion.owner != self.owner || completion.sink_kind != MESSAGE_COMPLETION_SINK_KIND {
            return AgentCompletionSinkObservation::Conflict;
        }
        if completion.publication_key.is_empty()
            || completion.publication_key.len() > 256
            || completion.publication_key.trim() != completion.publication_key
            || completion.publication_key.contains('\0')
            || completion.publication_key.chars().any(char::is_control)
        {
            return AgentCompletionSinkObservation::Conflict;
        }
        let digest = match MessageRequestDigest::parse(completion.content_digest) {
            Ok(digest) => digest,
            Err(_) => return AgentCompletionSinkObservation::Conflict,
        };
        let idempotency = IdempotencyKey {
            producer: MESSAGE_COMPLETION_PRODUCER.into(),
            key: completion.publication_key,
        };
        let store = Arc::clone(&self.store);
        let owner = self.owner.clone();
        match tokio::task::spawn_blocking(move || match operation {
            MessageCompletionOperation::Inspect => store
                .resolve_publication(&owner, &idempotency, &digest)
                .map(|resolution| resolution.map(|resolution| resolution.status)),
            MessageCompletionOperation::Publish => store
                .publish_staged(&owner, &idempotency, &digest)
                .map(|outcome| outcome.map(|outcome| outcome.resolution.status))
                .or_else(|error| {
                    if matches!(error, MessageStoreError::IdempotencyConflict) {
                        store
                            .resolve_publication(&owner, &idempotency, &digest)
                            .map(|resolution| resolution.map(|resolution| resolution.status))
                    } else {
                        Err(error)
                    }
                }),
            MessageCompletionOperation::Discard => store
                .discard_staged(&owner, &idempotency, &digest)
                .map(|outcome| outcome.map(|outcome| outcome.resolution.status)),
        })
        .await
        {
            Err(_) => AgentCompletionSinkObservation::Unavailable,
            Ok(Err(
                MessageStoreError::IdempotencyConflict | MessageStoreError::PublicationConflict,
            )) => AgentCompletionSinkObservation::Conflict,
            Ok(Err(_)) => AgentCompletionSinkObservation::Unavailable,
            Ok(Ok(None)) => AgentCompletionSinkObservation::Absent,
            Ok(Ok(Some(status))) => match (operation, status) {
                (
                    MessageCompletionOperation::Inspect,
                    MessagePublicationStatus::Staged | MessagePublicationStatus::Published,
                )
                | (MessageCompletionOperation::Publish, MessagePublicationStatus::Published)
                | (MessageCompletionOperation::Discard, MessagePublicationStatus::Discarded) => {
                    AgentCompletionSinkObservation::Exact
                }
                _ => AgentCompletionSinkObservation::Conflict,
            },
        }
    }
}
