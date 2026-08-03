use thiserror::Error;

use crate::{GoalStatus, TakeoverApprovalStatus};

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

    #[error("pursuit checkpoint for goal `{id}` changed concurrently")]
    CheckpointConflict { id: String },

    #[error("takeover approval `{id}` was not found")]
    TakeoverApprovalNotFound { id: String },

    #[error("takeover approval `{id}` is {status} and cannot be executed")]
    TakeoverApprovalNotExecutable {
        id: String,
        status: TakeoverApprovalStatus,
    },

    #[error("takeover approval `{id}` has already been decided and is immutable")]
    TakeoverApprovalAlreadyDecided { id: String },

    #[error("takeover approval `{id}` boundary no longer matches the current ready step")]
    TakeoverBoundaryChanged { id: String },

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

impl GoalStoreError {
    /// Stable snake_case *kind* token; ids/holders/payloads stay out.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AlreadyExists { .. } => "already_exists",
            Self::NotFound { .. } => "not_found",
            Self::InvalidStatus { .. } => "invalid_status",
            Self::LeaseHeld { .. } => "lease_held",
            Self::LeaseExpired { .. } => "lease_expired",
            Self::CriteriaUnsatisfied { .. } => "criteria_unsatisfied",
            Self::CheckpointConflict { .. } => "checkpoint_conflict",
            Self::TakeoverApprovalNotFound { .. } => "takeover_approval_not_found",
            Self::TakeoverApprovalNotExecutable { .. } => "takeover_approval_not_executable",
            Self::TakeoverApprovalAlreadyDecided { .. } => "takeover_approval_already_decided",
            Self::TakeoverBoundaryChanged { .. } => "takeover_boundary_changed",
            Self::InvalidInput(_) => "invalid_input",
            Self::UnsupportedSchema { .. } => "unsupported_schema",
            Self::CorruptData(_) => "corrupt_data",
            Self::Sqlite(_) => "sqlite",
            Self::Io(_) => "io",
            Self::Json(_) => "json",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::GoalStoreError;
    use crate::{GoalStatus, TakeoverApprovalStatus};

    #[test]
    fn goal_store_error_kind_tokens_are_snake_case_without_payload() {
        assert_eq!(
            GoalStoreError::AlreadyExists {
                id: "secret-goal".into()
            }
            .as_str(),
            "already_exists"
        );
        assert_eq!(
            GoalStoreError::NotFound {
                id: "secret-goal".into()
            }
            .as_str(),
            "not_found"
        );
        assert_eq!(
            GoalStoreError::InvalidStatus {
                id: "secret-goal".into(),
                operation: "pause",
                status: GoalStatus::Completed,
            }
            .as_str(),
            "invalid_status"
        );
        assert_eq!(
            GoalStoreError::LeaseHeld {
                id: "secret-goal".into(),
                held_by: "secret-worker".into(),
            }
            .as_str(),
            "lease_held"
        );
        assert_eq!(
            GoalStoreError::LeaseExpired {
                id: "secret-goal".into()
            }
            .as_str(),
            "lease_expired"
        );
        assert_eq!(
            GoalStoreError::CriteriaUnsatisfied {
                id: "secret-goal".into(),
                remaining: 3,
            }
            .as_str(),
            "criteria_unsatisfied"
        );
        assert_eq!(
            GoalStoreError::CheckpointConflict {
                id: "secret-goal".into()
            }
            .as_str(),
            "checkpoint_conflict"
        );
        assert_eq!(
            GoalStoreError::TakeoverApprovalNotFound {
                id: "secret-approval".into()
            }
            .as_str(),
            "takeover_approval_not_found"
        );
        assert_eq!(
            GoalStoreError::TakeoverApprovalNotExecutable {
                id: "secret-approval".into(),
                status: TakeoverApprovalStatus::Pending,
            }
            .as_str(),
            "takeover_approval_not_executable"
        );
        assert_eq!(
            GoalStoreError::TakeoverApprovalAlreadyDecided {
                id: "secret-approval".into()
            }
            .as_str(),
            "takeover_approval_already_decided"
        );
        assert_eq!(
            GoalStoreError::TakeoverBoundaryChanged {
                id: "secret-approval".into()
            }
            .as_str(),
            "takeover_boundary_changed"
        );
        assert_eq!(
            GoalStoreError::InvalidInput("secret-meta".into()).as_str(),
            "invalid_input"
        );
        assert_eq!(
            GoalStoreError::UnsupportedSchema {
                found: 9,
                supported: 3
            }
            .as_str(),
            "unsupported_schema"
        );
        assert_eq!(
            GoalStoreError::CorruptData("secret-blob".into()).as_str(),
            "corrupt_data"
        );
        assert!(
            !GoalStoreError::CorruptData("secret-blob".into())
                .as_str()
                .contains("secret")
        );
        assert_eq!(
            GoalStoreError::Io(std::io::Error::other("secret-io")).as_str(),
            "io"
        );
        let json_err = serde_json::from_str::<()>("not-json")
            .expect_err("invalid JSON must fail parse for token fixture");
        assert_eq!(GoalStoreError::Json(json_err).as_str(), "json");
    }
}
