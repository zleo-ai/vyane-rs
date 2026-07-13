//! Durable task metadata for Vyane.
//!
//! This crate deliberately models only control-plane metadata. It has no field
//! for a prompt, system instruction, provider credential, arbitrary label, raw
//! error, or model output. Callers keep execution payloads outside this store.
//! This is a structural boundary, not content inspection: callers must derive
//! identifiers and digests instead of relabelling private content as metadata.
//! The SQLite backend provides transactional lifecycle transitions that are
//! safe across processes and survive a process restart.

mod error;
mod model;
mod sqlite;
mod store;

pub use error::{Result, TaskStoreError};
pub use model::{
    ControllerRef, FailureCode, Lease, NewTask, TaskCursor, TaskEvent, TaskEventKind, TaskKind,
    TaskOrigin, TaskPage, TaskQuery, TaskRecord, TaskSettlement, TaskState,
};
pub use sqlite::{SCHEMA_VERSION, SqliteTaskStore};
pub use store::TaskStore;
