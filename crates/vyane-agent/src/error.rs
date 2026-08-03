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

impl AgentStoreError {
    /// Stable snake_case *kind* token; ids/reasons/IO payloads stay out.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidInput(_) => "invalid_input",
            Self::NotFound { .. } => "not_found",
            Self::AlreadyExists { .. } => "already_exists",
            Self::Conflict { .. } => "conflict",
            Self::InvalidTransition { .. } => "invalid_transition",
            Self::InvalidReceipt { .. } => "invalid_receipt",
            Self::InvalidExecutionPermit { .. } => "invalid_execution_permit",
            Self::InvalidCompletionPermit { .. } => "invalid_completion_permit",
            Self::CompletionConflict { .. } => "completion_conflict",
            Self::InvalidCancelTicket { .. } => "invalid_cancel_ticket",
            Self::InvalidRecoveryTicket { .. } => "invalid_recovery_ticket",
            Self::ControlBusy { .. } => "control_busy",
            Self::ResumeRejected { .. } => "resume_rejected",
            Self::UnsupportedSchema { .. } => "unsupported_schema",
            Self::CorruptData(_) => "corrupt_data",
            Self::Sqlite(_) => "sqlite",
            Self::Io(_) => "io",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AgentStoreError;

    #[test]
    fn agent_store_error_kind_tokens_are_snake_case_without_payload() {
        assert_eq!(
            AgentStoreError::InvalidInput("secret".into()).as_str(),
            "invalid_input"
        );
        assert_eq!(
            AgentStoreError::NotFound {
                id: "secret-id".into()
            }
            .as_str(),
            "not_found"
        );
        assert_eq!(
            AgentStoreError::InvalidExecutionPermit {
                id: "secret-id".into()
            }
            .as_str(),
            "invalid_execution_permit"
        );
        assert_eq!(
            AgentStoreError::ResumeRejected {
                id: "secret-id".into(),
                reason: "secret-reason".into(),
            }
            .as_str(),
            "resume_rejected"
        );
        assert!(
            !AgentStoreError::CorruptData("secret-blob".into())
                .as_str()
                .contains("secret")
        );
        assert_eq!(
            AgentStoreError::UnsupportedSchema {
                found: 9,
                supported: 3
            }
            .as_str(),
            "unsupported_schema"
        );
        assert_eq!(
            AgentStoreError::CompletionConflict { id: "x".into() }.as_str(),
            "completion_conflict"
        );
    }
}
