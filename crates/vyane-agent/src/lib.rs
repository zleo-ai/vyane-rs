//! Durable owner-scoped AgentRun and worker-topology truth.
//!
//! This crate deliberately stores control metadata only. Prompt text, raw
//! errors, model output, credentials, and native harness session identifiers
//! have no representable field. Messages remain exclusively in
//! `vyane-message`; logical/native session content remains in the session
//! store; `EventLog` is a downstream projection of this crate's body-free
//! transactional outbox.

mod error;
mod model;
mod sqlite;
mod store;

pub use error::{AgentStoreError, Result};
pub use model::{
    ActiveCompletionPermit, ActiveExecutionPermit, AgentEvent, AgentEventKind, AgentRunRecord,
    CancelOutcome, CancelPlan, CancelRequest, CancelTicket, ClaimedRun, CompletionPermitSnapshot,
    ControllerKind, ControllerRef, EnqueueResume, ExecutionPermitSnapshot, NativeExecutionScope,
    NewAgentRun, NewRunCompletion, NewWorker, OutboxPage, PreparedRunCompletion,
    ProjectionDeferReason, ProjectionQuarantineReason, RecoveryReason, RecoveryTicket, ResumeProof,
    ResumeSessionProof, RunCompletionRecord, RunCompletionStatus, RunFailureCode, RunLease,
    RunLeaseReceipt, RunMode, RunSettlement, RunState, WorkerLifecycle, WorkerRecord,
    WorkerTopology,
};
pub use model::{MAX_TOPOLOGY_NODES, MAX_TREE_CANCEL_RUNS};
pub use sqlite::{AgentClock, SCHEMA_VERSION, SqliteAgentStore, SystemAgentClock};
pub use store::AgentStore;
