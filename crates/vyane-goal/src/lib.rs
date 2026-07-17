//! Owner-scoped durable goals backed by one SQLite source of truth.
//!
//! Every lifecycle or progress mutation updates the current goal snapshot and
//! appends an immutable event in the same transaction. Acceptance criteria are
//! persisted as descriptors; executing them belongs to a later verifier layer.

mod acceptance;
mod approval;
mod continuity;
mod error;
mod model;
mod pursuit;
mod sqlite;
mod store;

pub use acceptance::{
    AcceptanceVerifier, MAX_OUTPUT_TAIL_BYTES, MAX_VERIFIER_TIMEOUT, criterion_key,
};
pub use approval::{
    MAX_TAKEOVER_TIMEOUT, TakeoverApproval, TakeoverApprovalRequest, TakeoverApprovalStatus,
    TakeoverBoundTarget, TakeoverDecision, TakeoverFinish, TakeoverRunStatus, TakeoverSandbox,
};
pub use continuity::{
    GoalContinuityAction, GoalContinuityMode, GoalContinuityPlan, GoalContinuityPolicy,
    GoalContinuityReviewCheck, GoalContinuitySignal, GoalContinuitySignalKind,
    GoalContinuitySignalResult, GoalContinuityState, GoalContinuityStatus, GoalContinuityStep,
    GoalContinuityStepStatus, GoalExecutionTarget, GoalQuotaEvent, apply_quota_handoff_events,
};
pub use error::{GoalStoreError, Result};
pub use model::{
    AcceptanceCriterion, AcceptanceVerification, CriterionResult, CriterionStatus, GoalEvent,
    GoalEventKind, GoalQuery, GoalRecord, GoalRecoveryCursor, GoalRecoveryFilter, GoalRecoveryPage,
    GoalStatus, GoalVerificationArtifact, MAX_LEASE_SECONDS, NewGoal,
};
pub use pursuit::{
    GoalPursuer, GoalPursuitCheckpoint, GoalSegmentRuntime, MAX_PURSUIT_FAILURES,
    MAX_PURSUIT_SEGMENTS, MAX_PURSUIT_TIMEOUT, MAX_SEGMENT_TIMEOUT, PursuitCheckpointStatus,
    PursuitConfig, PursuitOutcome, PursuitSegmentRequest, PursuitSegmentResult,
    PursuitSegmentStatus, PursuitStatus,
};
pub use sqlite::{SCHEMA_VERSION, SqliteGoalStore};
pub use store::GoalStore;
