use thiserror::Error;

use crate::GoalStatus;

pub type Result<T> = std::result::Result<T, GoalStoreError>;

#[derive(Debug, Error)]
pub enum GoalStoreError {
    #[error("goal `{id}` already exists")]
    AlreadyExists { id: String },

    #[error("goal `{id}` was not found")]
    NotFound { id: String },

    #[error("cannot {operation} goal `{id}` while it is {status}")]
    InvalidStatus {
        id: String,
        operation: &'static str,
        status: GoalStatus,
    },

    #[error("goal `{id}` is claimed by `{held_by}` under an active lease")]
    LeaseHeld { id: String, held_by: String },

    #[error("lease on goal `{id}` has expired; reclaim it before continuing")]
    LeaseExpired { id: String },

    #[error("goal `{id}` still has {remaining} unsatisfied acceptance criteria")]
    CriteriaUnsatisfied { id: String, remaining: usize },

    #[error("invalid goal metadata: {0}")]
    InvalidInput(String),

    #[error("goal database schema {found} is newer than supported schema {supported}")]
    UnsupportedSchema { found: u32, supported: u32 },

    #[error("goal database contains invalid data: {0}")]
    CorruptData(String),

    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
