use std::sync::Arc;

use anyhow::{Context, Result};
use vyane_broker::{BrokerScope, MessageBroker, MessageEventProjector};
use vyane_ledger::EventLog;
use vyane_message::{MessagePublicationStatus, MessageStore, NewMessage, SqliteMessageStore};

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
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

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
}
