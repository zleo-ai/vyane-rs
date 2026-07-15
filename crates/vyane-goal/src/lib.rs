//! Owner-scoped durable goals backed by one SQLite source of truth.
//!
//! Every lifecycle or progress mutation updates the current goal snapshot and
//! appends an immutable event in the same transaction. Acceptance criteria are
//! persisted as descriptors; executing them belongs to a later verifier layer.

mod acceptance;
mod error;
mod model;
mod sqlite;
mod store;

pub use acceptance::{
    AcceptanceVerification, AcceptanceVerifier, CriterionResult, CriterionStatus,
    MAX_OUTPUT_TAIL_BYTES, MAX_VERIFIER_TIMEOUT, criterion_key,
};
pub use error::{GoalStoreError, Result};
pub use model::{
    AcceptanceCriterion, GoalEvent, GoalEventKind, GoalQuery, GoalRecord, GoalStatus,
    MAX_LEASE_SECONDS, NewGoal,
};
pub use sqlite::{SCHEMA_VERSION, SqliteGoalStore};
pub use store::GoalStore;
