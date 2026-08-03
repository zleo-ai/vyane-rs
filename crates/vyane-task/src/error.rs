use thiserror::Error;

use crate::TaskState;

pub type Result<T> = std::result::Result<T, TaskStoreError>;

/// Failures surfaced by a durable task store.
#[derive(Debug, Error)]
pub enum TaskStoreError {
    #[error("task `{id}` already exists")]
    AlreadyExists { id: String },

    #[error("task `{id}` was not found")]
    NotFound { id: String },

    #[error(
        "task `{id}` changed concurrently: expected revision {expected_revision} and executor epoch {expected_executor_epoch}, found revision {actual_revision} and executor epoch {actual_executor_epoch}"
    )]
    Conflict {
        id: String,
        expected_revision: u64,
        actual_revision: u64,
        expected_executor_epoch: u64,
        actual_executor_epoch: u64,
    },

    #[error("cannot {operation} task `{id}` while it is {state}")]
    InvalidState {
        id: String,
        operation: &'static str,
        state: TaskState,
    },

    #[error("invalid task metadata: {0}")]
    InvalidInput(String),

    #[error("task `{id}` has no expired lease to claim")]
    LeaseNotExpired { id: String },

    #[error("task `{id}` lease has expired and must be claimed before renewal")]
    LeaseAlreadyExpired { id: String },

    #[error("task `{id}` lease is owned by `{actual}`, not `{expected}`")]
    LeaseOwnerMismatch {
        id: String,
        expected: String,
        actual: String,
    },

    #[error("task database schema {found} is newer than supported schema {supported}")]
    UnsupportedSchema { found: u32, supported: u32 },

    #[error("task database contains invalid data: {0}")]
    CorruptData(String),

    #[error(transparent)]
    Sqlite(rusqlite::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl TaskStoreError {
    /// Stable snake_case *kind* token; task ids/owners/payloads stay out.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AlreadyExists { .. } => "already_exists",
            Self::NotFound { .. } => "not_found",
            Self::Conflict { .. } => "conflict",
            Self::InvalidState { .. } => "invalid_state",
            Self::InvalidInput(_) => "invalid_input",
            Self::LeaseNotExpired { .. } => "lease_not_expired",
            Self::LeaseAlreadyExpired { .. } => "lease_already_expired",
            Self::LeaseOwnerMismatch { .. } => "lease_owner_mismatch",
            Self::UnsupportedSchema { .. } => "unsupported_schema",
            Self::CorruptData(_) => "corrupt_data",
            Self::Sqlite(_) => "sqlite",
            Self::Io(_) => "io",
        }
    }
}

impl From<rusqlite::Error> for TaskStoreError {
    fn from(error: rusqlite::Error) -> Self {
        match error {
            rusqlite::Error::FromSqlConversionFailure(index, value_type, source) => {
                match source.downcast::<Self>() {
                    Ok(task_error) => *task_error,
                    Err(source) => Self::CorruptData(format!(
                        "column {index} contains invalid {value_type:?} data: {source}"
                    )),
                }
            }
            rusqlite::Error::IntegralValueOutOfRange(index, value) => Self::CorruptData(format!(
                "column {index} contains out-of-range integer {value}"
            )),
            rusqlite::Error::Utf8Error(error) => {
                Self::CorruptData(format!("database contains invalid UTF-8: {error}"))
            }
            rusqlite::Error::InvalidColumnType(index, name, value_type) => Self::CorruptData(
                format!("column {index} (`{name}`) contains incompatible {value_type:?} data"),
            ),
            other => Self::Sqlite(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TaskStoreError;
    use crate::TaskState;

    #[test]
    fn task_store_error_kind_tokens_are_snake_case_without_payload() {
        assert_eq!(
            TaskStoreError::AlreadyExists {
                id: "secret-task".into()
            }
            .as_str(),
            "already_exists"
        );
        assert_eq!(
            TaskStoreError::NotFound {
                id: "secret-task".into()
            }
            .as_str(),
            "not_found"
        );
        assert_eq!(
            TaskStoreError::Conflict {
                id: "secret-task".into(),
                expected_revision: 1,
                actual_revision: 2,
                expected_executor_epoch: 3,
                actual_executor_epoch: 4,
            }
            .as_str(),
            "conflict"
        );
        assert_eq!(
            TaskStoreError::InvalidState {
                id: "secret-task".into(),
                operation: "cancel",
                state: TaskState::Succeeded,
            }
            .as_str(),
            "invalid_state"
        );
        assert_eq!(
            TaskStoreError::InvalidInput("secret-meta".into()).as_str(),
            "invalid_input"
        );
        assert_eq!(
            TaskStoreError::LeaseNotExpired {
                id: "secret-task".into()
            }
            .as_str(),
            "lease_not_expired"
        );
        assert_eq!(
            TaskStoreError::LeaseAlreadyExpired {
                id: "secret-task".into()
            }
            .as_str(),
            "lease_already_expired"
        );
        assert_eq!(
            TaskStoreError::LeaseOwnerMismatch {
                id: "secret-task".into(),
                expected: "worker-a".into(),
                actual: "worker-b".into(),
            }
            .as_str(),
            "lease_owner_mismatch"
        );
        assert_eq!(
            TaskStoreError::UnsupportedSchema {
                found: 9,
                supported: 3
            }
            .as_str(),
            "unsupported_schema"
        );
        assert_eq!(
            TaskStoreError::CorruptData("secret-blob".into()).as_str(),
            "corrupt_data"
        );
        assert!(
            !TaskStoreError::CorruptData("secret-blob".into())
                .as_str()
                .contains("secret")
        );
        assert_eq!(
            TaskStoreError::Io(std::io::Error::other("secret-io")).as_str(),
            "io"
        );
    }
}
