use thiserror::Error;

pub type Result<T> = std::result::Result<T, AgentStoreError>;

#[derive(Debug, Error)]
pub enum AgentStoreError {
    #[error("invalid agent metadata: {0}")]
    InvalidInput(String),
    #[error("agent record `{id}` was not found")]
    NotFound { id: String },
    #[error("agent record `{id}` already exists")]
    AlreadyExists { id: String },
    #[error(
        "agent record `{id}` changed concurrently: expected revision {expected}, found {actual}"
    )]
    Conflict {
        id: String,
        expected: u64,
        actual: u64,
    },
    #[error("invalid state transition for `{id}`: {from} -> {to}")]
    InvalidTransition {
        id: String,
        from: String,
        to: String,
    },
    #[error("run lease receipt for `{id}` is stale or invalid")]
    InvalidReceipt { id: String },
    #[error("active execution permit for `{id}` is stale or invalid")]
    InvalidExecutionPermit { id: String },
    #[error("completion permit for `{id}` is stale or invalid")]
    InvalidCompletionPermit { id: String },
    #[error("completion for `{id}` conflicts with durable state")]
    CompletionConflict { id: String },
    #[error("cancel ticket for `{id}` is stale or invalid")]
    InvalidCancelTicket { id: String },
    #[error("recovery ticket for `{id}` is stale or invalid")]
    InvalidRecoveryTicket { id: String },
    #[error("run `{id}` already has an active control operation")]
    ControlBusy { id: String },
    #[error("run `{id}` cannot be resumed: {reason}")]
    ResumeRejected { id: String, reason: String },
    #[error("database schema {found} is newer than supported schema {supported}")]
    UnsupportedSchema { found: u32, supported: u32 },
    #[error("agent database integrity check failed: {0}")]
    CorruptData(String),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
