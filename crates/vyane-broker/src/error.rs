use thiserror::Error;
use vyane_agent::AgentStoreError;
use vyane_ledger::EventLogError;
use vyane_message::MessageStoreError;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("invalid broker configuration: {0}")]
    InvalidConfig(String),
    #[error("adapter `{adapter}` is not safe to replay after an uncertain result")]
    UnsafeAdapter { adapter: String },
    #[error("message store operation failed: {0}")]
    Store(#[from] MessageStoreError),
    #[error("agent store operation failed: {0}")]
    AgentStore(#[from] AgentStoreError),
    #[error("event projection failed: {0}")]
    Event(#[from] EventLogError),
    #[error("blocking storage worker failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

impl BrokerError {
    /// Stable snake_case *kind* token; nested store payloads stay out.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidConfig(_) => "invalid_config",
            Self::UnsafeAdapter { .. } => "unsafe_adapter",
            Self::Store(_) => "store",
            Self::AgentStore(_) => "agent_store",
            Self::Event(_) => "event",
            Self::Join(_) => "join",
        }
    }
}

pub type Result<T> = std::result::Result<T, BrokerError>;

#[cfg(test)]
mod tests {
    use super::BrokerError;
    use vyane_message::MessageStoreError;

    #[test]
    fn broker_error_kind_tokens_are_snake_case_without_payload() {
        assert_eq!(
            BrokerError::InvalidConfig("secret-config".into()).as_str(),
            "invalid_config"
        );
        assert!(
            !BrokerError::InvalidConfig("secret-config".into())
                .as_str()
                .contains("secret")
        );
        assert_eq!(
            BrokerError::UnsafeAdapter {
                adapter: "secret-adapter".into()
            }
            .as_str(),
            "unsafe_adapter"
        );
        assert_eq!(
            BrokerError::Store(MessageStoreError::NotFound).as_str(),
            "store"
        );
        assert_eq!(
            BrokerError::AgentStore(vyane_agent::AgentStoreError::NotFound {
                id: "secret-id".into()
            })
            .as_str(),
            "agent_store"
        );
        assert_eq!(
            BrokerError::Event(vyane_ledger::EventLogError::CorruptRecord).as_str(),
            "event"
        );
    }
}
