use std::sync::Arc;

use anyhow::{Context, Result};
use vyane_agent::{ActiveExecutionPermit, AgentStore, RunCompletionStatus};
use vyane_broker::{BrokerScope, MessageBroker, MessageEventProjector};
use vyane_ledger::EventLog;
use vyane_message::{
    IdempotencyKey, MessagePublicationStatus, MessageRequestDigest, MessageStore, NewMessage,
    SqliteMessageStore,
};

use crate::{
    AgentCompletionSink, InProcessCompletionStageError, InProcessPreparedCompletion,
    MessageAgentCompletionSink, StagedRunCompletion, StoragePaths, message_run_completion,
};

/// Explicit message/broker construction for front-ends that need it.
///
/// Ordinary dispatch does not open the message database or start background
/// polling. A daemon or adapter must opt in and own its supervisor lifecycle.
#[derive(Clone)]
pub struct MessageComponents {
    broker: MessageBroker,
    projector: MessageEventProjector,
    completion_sink: Arc<MessageAgentCompletionSink>,
    store: Arc<dyn MessageStore>,
}

impl MessageComponents {
    pub fn open(paths: &StoragePaths, owner: impl Into<String>) -> Result<Self> {
        // Validate authority before creating any persistent state.
        let scope = BrokerScope::new(owner.into())?;
        let store: Arc<dyn MessageStore> = Arc::new(
            SqliteMessageStore::open(paths.message_db_path()).with_context(|| {
                format!(
                    "open message database {}",
                    paths.message_db_path().display()
                )
            })?,
        );
        let event_log = EventLog::new(paths.event_log_dir());
        let broker = MessageBroker::new(scope.clone(), Arc::clone(&store));
        let projector = MessageEventProjector::new(scope, Arc::clone(&store), event_log);
        let completion_sink = Arc::new(
            MessageAgentCompletionSink::new(broker.scope().owner(), Arc::clone(&store))
                .map_err(anyhow::Error::new)?,
        );
        Ok(Self {
            broker,
            projector,
            completion_sink,
            store,
        })
    }

    #[must_use]
    pub fn broker(&self) -> &MessageBroker {
        &self.broker
    }

    #[must_use]
    pub fn projector(&self) -> &MessageEventProjector {
        &self.projector
    }

    /// Clone the owner-bound sink used by AgentRun recovery and completion
    /// projection. The raw message store remains encapsulated.
    #[must_use]
    pub fn completion_sink(&self) -> Arc<dyn AgentCompletionSink> {
        self.completion_sink.clone()
    }

    /// Stage the exact message bound to a prepared AgentRun completion.
    ///
    /// The latest durable completion snapshot is compared with the complete
    /// canonical message digest in the same blocking closure immediately
    /// before the message-store transaction. A caller cannot accidentally
    /// stage a different result under a valid completion permit.
    pub async fn stage_completion(
        &self,
        prepared: InProcessPreparedCompletion<'_>,
        message: NewMessage,
    ) -> Result<StagedRunCompletion, InProcessCompletionStageError> {
        let store = Arc::clone(&self.store);
        let owner = self.broker.scope().owner().to_owned();
        prepared
            .stage_blocking(move |snapshot| {
                let Ok(expected) =
                    message_run_completion(snapshot.record.completion_id.clone(), &message)
                else {
                    return false;
                };
                if snapshot.record.owner != owner
                    || snapshot.record.completion_id != expected.id
                    || snapshot.record.sink_kind != expected.sink_kind
                    || snapshot.record.publication_key != expected.publication_key
                    || snapshot.record.content_digest != expected.content_digest
                    || snapshot.record.content_bytes != expected.content_bytes
                {
                    return false;
                }
                store.stage(&owner, &message).is_ok_and(|outcome| {
                    outcome.resolution.owner == owner
                        && outcome.resolution.request_digest.as_str()
                            == snapshot.record.content_digest
                        && matches!(
                            outcome.resolution.status,
                            MessagePublicationStatus::Staged | MessagePublicationStatus::Published
                        )
                })
            })
            .await
    }

    /// Prepare and stage one exact message completion for a non-in-process
    /// executor such as a lifecycle-gated CLI harness.
    ///
    /// Preparation, final permit validation, and message staging run in one
    /// blocking closure. Dropping the async waiter therefore cannot expose a
    /// prepared AgentRun completion while the matching sink transaction is
    /// still executing. The caller receives commit authority only when the
    /// complete descriptor and owner remain exact.
    pub async fn prepare_and_stage_completion(
        &self,
        agent_store: Arc<dyn AgentStore>,
        permit: ActiveExecutionPermit,
        completion_id: impl Into<String>,
        message: NewMessage,
    ) -> Result<StagedRunCompletion, AgentMessageCompletionStageError> {
        let completion = message_run_completion(completion_id, &message)
            .map_err(|_| AgentMessageCompletionStageError::InvalidMessage)?;
        let owner = self.broker.scope().owner().to_owned();
        if permit.owner() != owner {
            return Err(AgentMessageCompletionStageError::Rejected);
        }
        let message_store = Arc::clone(&self.store);
        tokio::runtime::Handle::try_current()
            .map_err(|_| AgentMessageCompletionStageError::RuntimeUnavailable)?
            .spawn_blocking(move || {
                let prepared = agent_store
                    .prepare_completion(&owner, &permit, &completion)
                    .map_err(|_| AgentMessageCompletionStageError::Rejected)?;
                if prepared.record.owner != owner
                    || prepared.record.completion_id != completion.id
                    || prepared.record.sink_kind != completion.sink_kind
                    || prepared.record.publication_key != completion.publication_key
                    || prepared.record.content_digest != completion.content_digest
                    || prepared.record.content_bytes != completion.content_bytes
                {
                    return Err(AgentMessageCompletionStageError::Rejected);
                }
                let snapshot = agent_store
                    .validate_completion_permit(&owner, &prepared.permit)
                    .map_err(|_| AgentMessageCompletionStageError::Rejected)?;
                if snapshot.record != prepared.record {
                    return Err(AgentMessageCompletionStageError::Rejected);
                }
                let outcome = message_store
                    .stage(&owner, &message)
                    .map_err(|_| AgentMessageCompletionStageError::SinkUnavailable)?;
                if outcome.resolution.owner != owner
                    || outcome.resolution.request_digest.as_str() != completion.content_digest
                    || !matches!(
                        outcome.resolution.status,
                        MessagePublicationStatus::Staged | MessagePublicationStatus::Published
                    )
                {
                    return Err(AgentMessageCompletionStageError::Rejected);
                }
                Ok(StagedRunCompletion::new(prepared.permit))
            })
            .await
            .map_err(|_| AgentMessageCompletionStageError::TaskFailed)?
    }

    /// Read one exact published AgentRun completion body. Prepared, discarded,
    /// foreign, drifted, and missing publications are all absent-shaped.
    pub async fn published_completion_body(
        &self,
        agent_store: Arc<dyn AgentStore>,
        run_id: impl Into<String>,
    ) -> Result<Option<String>, AgentMessageCompletionReadError> {
        let owner = self.broker.scope().owner().to_owned();
        let run_id = run_id.into();
        let message_store = Arc::clone(&self.store);
        tokio::runtime::Handle::try_current()
            .map_err(|_| AgentMessageCompletionReadError::RuntimeUnavailable)?
            .spawn_blocking(move || {
                let Some(completion) = agent_store
                    .get_completion(&owner, &run_id)
                    .map_err(|_| AgentMessageCompletionReadError::Unavailable)?
                else {
                    return Ok(None);
                };
                if completion.owner != owner
                    || completion.run_id != run_id
                    || completion.status != RunCompletionStatus::Committed
                    || completion.sink_kind != crate::MESSAGE_COMPLETION_SINK_KIND
                {
                    return Ok(None);
                }
                let digest = MessageRequestDigest::parse(completion.content_digest)
                    .map_err(|_| AgentMessageCompletionReadError::Unavailable)?;
                let idempotency = IdempotencyKey {
                    producer: crate::MESSAGE_COMPLETION_PRODUCER.into(),
                    key: completion.publication_key,
                };
                let Some(resolution) = message_store
                    .resolve_idempotency(&owner, &idempotency, &digest)
                    .map_err(|_| AgentMessageCompletionReadError::Unavailable)?
                else {
                    return Ok(None);
                };
                if resolution.owner != owner
                    || resolution.status != MessagePublicationStatus::Published
                    || resolution.request_digest != digest
                {
                    return Ok(None);
                }
                let Some(bundle) = message_store
                    .get(&owner, &resolution.message_id)
                    .map_err(|_| AgentMessageCompletionReadError::Unavailable)?
                else {
                    return Ok(None);
                };
                if bundle.message.owner != owner
                    || bundle.message.idempotency != idempotency
                    || bundle.message.request_digest != digest.as_str()
                {
                    return Ok(None);
                }
                Ok(Some(bundle.message.body))
            })
            .await
            .map_err(|_| AgentMessageCompletionReadError::TaskFailed)?
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentMessageCompletionReadError {
    RuntimeUnavailable,
    TaskFailed,
    Unavailable,
}

impl std::fmt::Display for AgentMessageCompletionReadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::RuntimeUnavailable => "AgentRun completion read requires a Tokio runtime",
            Self::TaskFailed => "AgentRun completion read task failed",
            Self::Unavailable => "AgentRun completion output is unavailable",
        })
    }
}

impl std::error::Error for AgentMessageCompletionReadError {}

/// Closed failure surface for generic AgentRun message completion staging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentMessageCompletionStageError {
    InvalidMessage,
    RuntimeUnavailable,
    TaskFailed,
    SinkUnavailable,
    Rejected,
}

impl std::fmt::Display for AgentMessageCompletionStageError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidMessage => "AgentRun completion message is invalid",
            Self::RuntimeUnavailable => "AgentRun completion staging requires a Tokio runtime",
            Self::TaskFailed => "AgentRun completion staging task failed",
            Self::SinkUnavailable => "AgentRun completion sink is unavailable",
            Self::Rejected => "AgentRun completion staging was rejected",
        })
    }
}

impl std::error::Error for AgentMessageCompletionStageError {}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use chrono::Utc;
    use vyane_agent::{
        AgentStore, ControllerKind, ControllerRef, NewAgentRun, NewWorker, RunCompletionStatus,
        RunMode, SqliteAgentStore,
    };
    use vyane_message::{
        EndpointKind, EndpointRef, IdempotencyKey, MessageDirection, MessagePublicationStatus,
        NewDelivery,
    };

    #[test]
    fn construction_is_explicit_owner_bound_and_validates_before_writing() {
        let directory = tempfile::tempdir().unwrap();
        let paths = StoragePaths::from_data_dir(directory.path().join("data"));

        assert!(MessageComponents::open(&paths, "   ").is_err());
        assert!(!paths.message_db_path().exists());

        let components = MessageComponents::open(&paths, "alice").unwrap();
        assert_eq!(components.broker().scope().owner(), "alice");
        assert!(paths.message_db_path().exists());
        assert!(!paths.event_log_dir().exists());
    }

    fn completion_message(key: &str, body: &str) -> NewMessage {
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
            payload: serde_json::json!({"status": "completed"}),
            reply_to: None,
            trace_id: None,
            correlation_id: Some("run-1".into()),
            idempotency: IdempotencyKey {
                producer: crate::MESSAGE_COMPLETION_PRODUCER.into(),
                key: key.into(),
            },
            deliveries: vec![NewDelivery {
                route: "local".into(),
                target: EndpointRef {
                    kind: EndpointKind::User,
                    id: "requester-1".into(),
                },
                available_at: None,
                expires_at: None,
                max_attempts: 3,
            }],
        }
    }

    #[tokio::test]
    async fn process_style_completion_prepares_and_stages_exact_message() {
        let directory = tempfile::tempdir().unwrap();
        let paths = StoragePaths::from_data_dir(directory.path().join("data"));
        let messages = MessageComponents::open(&paths, "owner-a").unwrap();
        let agent_store = Arc::new(SqliteAgentStore::open(paths.agent_metadata_db_path()).unwrap());
        let now = Utc::now();
        agent_store
            .create_root(
                "owner-a",
                &NewWorker {
                    id: "worker-1".into(),
                    logical_session_id: None,
                },
                &NewAgentRun {
                    id: "run-1".into(),
                    worker_id: "worker-1".into(),
                    task_id: None,
                    trace_id: None,
                    parent_run_id: None,
                    execution_backend: vyane_agent::ExecutionBackend::CliHarnessProcess,
                    mode: RunMode::Autonomous,
                    target_key: "profile-1".into(),
                    prompt_digest: "a".repeat(64),
                    policy_digest: "b".repeat(64),
                    available_at: now,
                    timeout_seconds: 60,
                    max_resume_attempts: 0,
                },
            )
            .unwrap();
        let claim = agent_store
            .claim_due(
                "owner-a",
                vyane_agent::ExecutionBackend::CliHarnessProcess,
                "host-1",
                30,
                1,
            )
            .unwrap()
            .remove(0);
        let started = agent_store
            .start(
                "owner-a",
                &claim.receipt,
                &ControllerRef {
                    kind: ControllerKind::Process,
                    id: "controller-1".into(),
                    fingerprint: Some("fingerprint-1".into()),
                },
            )
            .unwrap();
        let permit = agent_store
            .issue_execution_permit("owner-a", &started.receipt, &started.run.policy_digest)
            .unwrap();
        let message = completion_message("result.run-1", "completed body");
        let staged = messages
            .prepare_and_stage_completion(
                agent_store.clone(),
                permit,
                "completion-1",
                message.clone(),
            )
            .await
            .unwrap();

        assert_eq!(staged.completion_id(), "completion-1");
        assert_eq!(
            agent_store
                .get_completion("owner-a", "run-1")
                .unwrap()
                .unwrap()
                .status,
            RunCompletionStatus::Prepared
        );
        let publication = messages
            .store
            .resolve_publication(
                "owner-a",
                &message.idempotency,
                &message.request_digest().unwrap(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(publication.status, MessagePublicationStatus::Staged);
    }

    #[tokio::test]
    async fn completion_body_is_absent_until_exact_commit_and_publication() {
        let directory = tempfile::tempdir().unwrap();
        let paths = StoragePaths::from_data_dir(directory.path().join("data"));
        let messages = MessageComponents::open(&paths, "owner-a").unwrap();
        let agent_store = Arc::new(SqliteAgentStore::open(paths.agent_metadata_db_path()).unwrap());
        let now = Utc::now();
        agent_store
            .create_root(
                "owner-a",
                &NewWorker {
                    id: "worker-output".into(),
                    logical_session_id: None,
                },
                &NewAgentRun {
                    id: "run-output".into(),
                    worker_id: "worker-output".into(),
                    task_id: None,
                    trace_id: None,
                    parent_run_id: None,
                    execution_backend: vyane_agent::ExecutionBackend::CliHarnessProcess,
                    mode: RunMode::Autonomous,
                    target_key: "profile-output".into(),
                    prompt_digest: "c".repeat(64),
                    policy_digest: "d".repeat(64),
                    available_at: now,
                    timeout_seconds: 60,
                    max_resume_attempts: 0,
                },
            )
            .unwrap();
        let claim = agent_store
            .claim_due(
                "owner-a",
                vyane_agent::ExecutionBackend::CliHarnessProcess,
                "host-output",
                30,
                1,
            )
            .unwrap()
            .remove(0);
        let started = agent_store
            .start(
                "owner-a",
                &claim.receipt,
                &ControllerRef {
                    kind: ControllerKind::Process,
                    id: "controller-output".into(),
                    fingerprint: Some("fingerprint-output".into()),
                },
            )
            .unwrap();
        let permit = agent_store
            .issue_execution_permit("owner-a", &started.receipt, &started.run.policy_digest)
            .unwrap();
        let message = completion_message("result.run-output", "published body");
        let completion = message_run_completion("completion-output", &message).unwrap();
        let prepared = agent_store
            .prepare_completion("owner-a", &permit, &completion)
            .unwrap();
        messages.store.stage("owner-a", &message).unwrap();

        assert_eq!(
            messages
                .published_completion_body(agent_store.clone(), "run-output")
                .await
                .unwrap(),
            None
        );

        agent_store
            .commit_completion("owner-a", &prepared.permit)
            .unwrap();
        let digest = message.request_digest().unwrap();
        messages
            .store
            .publish_staged("owner-a", &message.idempotency, &digest)
            .unwrap()
            .unwrap();

        assert_eq!(
            messages
                .published_completion_body(agent_store, "run-output")
                .await
                .unwrap()
                .as_deref(),
            Some("published body")
        );
    }
}
