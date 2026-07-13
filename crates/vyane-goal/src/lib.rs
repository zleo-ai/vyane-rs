//! Owner-scoped durable goals backed by one SQLite source of truth.
//!
//! Every lifecycle or progress mutation updates the current goal snapshot and
//! appends an immutable event in the same transaction. Acceptance criteria are
//! persisted as descriptors; executing them belongs to a later verifier layer.

mod error;
mod model;
mod sqlite;
mod store;

pub use error::{GoalStoreError, Result};
pub use model::{
    AcceptanceCriterion, GoalEvent, GoalEventKind, GoalQuery, GoalRecord, GoalStatus, NewGoal,
};
pub use sqlite::{SCHEMA_VERSION, SqliteGoalStore};
pub use store::GoalStore;
