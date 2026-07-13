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

pub type Result<T> = std::result::Result<T, BrokerError>;
