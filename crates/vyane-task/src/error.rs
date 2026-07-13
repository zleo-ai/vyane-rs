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
