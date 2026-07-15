use std::time::Duration;

use crate::{
    ActiveCompletionPermit, ActiveExecutionPermit, AgentRunRecord, CancelOutcome, CancelPlan,
    CancelRequest, CancelTicket, ClaimedRun, CompletionPermitSnapshot, ControllerRef,
    EnqueueResume, ExecutionBackend, ExecutionPermitSnapshot, NativeExecutionScope, NewAgentRun,
    NewRunCompletion, NewWorker, OutboxPage, PreparedRunCompletion, ProjectionDeferReason,
    ProjectionQuarantineReason, RecoveryTicket, Result, ResumeSessionProof, RunCompletionRecord,
    RunLeaseReceipt, RunSettlement, WorkerRecord, WorkerTopology,
};

/// Synchronous owner-scoped source of truth for AgentRuns and worker topology.
///
/// Every identifier lookup is scoped by explicit `owner`; a record belonging
/// to another owner is indistinguishable from an absent record.
pub trait AgentStore: Send + Sync {
    fn create_root(
        &self,
        owner: &str,
        worker: &NewWorker,
        run: &NewAgentRun,
    ) -> Result<(WorkerRecord, AgentRunRecord)>;

    fn spawn_child(
        &self,
        owner: &str,
        parent_worker_id: &str,
        expected_parent_revision: u64,
        child: &NewWorker,
        run: &NewAgentRun,
    ) -> Result<(WorkerRecord, AgentRunRecord)>;

    fn enqueue_run(&self, owner: &str, run: &NewAgentRun) -> Result<AgentRunRecord>;

    fn get_worker(&self, owner: &str, worker_id: &str) -> Result<Option<WorkerRecord>>;

    fn get_run(&self, owner: &str, run_id: &str) -> Result<Option<AgentRunRecord>>;

    fn claim_due(
        &self,
        owner: &str,
        execution_backend: ExecutionBackend,
        lease_owner: &str,
        lease_seconds: u64,
        limit: usize,
    ) -> Result<Vec<ClaimedRun>>;

    fn start(
        &self,
        owner: &str,
        receipt: &RunLeaseReceipt,
        controller: &ControllerRef,
    ) -> Result<ClaimedRun>;

    /// Exchange an exact, current running-run receipt for revision-independent
    /// in-memory execution authority scoped to the frozen policy digest.
    fn issue_execution_permit(
        &self,
        owner: &str,
        receipt: &RunLeaseReceipt,
        expected_policy_digest: &str,
    ) -> Result<ActiveExecutionPermit>;

    /// Revalidate every authority component against current durable state.
    /// The returned value is audit identity only and grants no authority.
    fn validate_execution_permit(
        &self,
        owner: &str,
        permit: &ActiveExecutionPermit,
        expected_policy_digest: &str,
    ) -> Result<ExecutionPermitSnapshot>;

    /// Revalidate a permit and its complete frozen native execution identity
    /// in one store snapshot. Stores that do not implement this atomic check
    /// fail closed instead of inheriting the weaker permit-only validation.
    fn validate_native_execution_permit(
        &self,
        _owner: &str,
        permit: &ActiveExecutionPermit,
        _scope: &NativeExecutionScope,
    ) -> Result<ExecutionPermitSnapshot> {
        Err(crate::AgentStoreError::InvalidExecutionPermit {
            id: permit.run_id().to_string(),
        })
    }

    fn heartbeat(
        &self,
        owner: &str,
        receipt: &RunLeaseReceipt,
        lease_seconds: u64,
    ) -> Result<ClaimedRun>;

    fn record_activity(&self, owner: &str, receipt: &RunLeaseReceipt) -> Result<ClaimedRun>;

    fn bind_resume_session(
        &self,
        owner: &str,
        receipt: &RunLeaseReceipt,
        proof: &ResumeSessionProof,
    ) -> Result<ClaimedRun>;

    /// Freeze one body-free result descriptor and revoke generic execution effects.
    fn prepare_completion(
        &self,
        owner: &str,
        permit: &ActiveExecutionPermit,
        completion: &NewRunCompletion,
    ) -> Result<PreparedRunCompletion>;

    /// Revalidate exact authority immediately before staging the result externally.
    fn validate_completion_permit(
        &self,
        owner: &str,
        permit: &ActiveCompletionPermit,
    ) -> Result<CompletionPermitSnapshot>;

    /// Atomically make a prepared result and its run successful.
    fn commit_completion(
        &self,
        owner: &str,
        permit: &ActiveCompletionPermit,
    ) -> Result<(AgentRunRecord, RunCompletionRecord)>;

    fn get_completion(&self, owner: &str, run_id: &str) -> Result<Option<RunCompletionRecord>>;

    /// Read a prepared descriptor only under one exact active recovery ticket.
    fn completion_for_recovery(
        &self,
        owner: &str,
        ticket: &RecoveryTicket,
    ) -> Result<Option<RunCompletionRecord>>;

    /// Reconcile a staged result after exact controller loss. Lease loss only.
    fn commit_recovered_completion(
        &self,
        owner: &str,
        ticket: &RecoveryTicket,
        completion_id: &str,
    ) -> Result<(AgentRunRecord, RunCompletionRecord)>;

    fn settle(
        &self,
        owner: &str,
        receipt: &RunLeaseReceipt,
        settlement: RunSettlement,
    ) -> Result<AgentRunRecord>;

    fn topology(&self, owner: &str, root_worker_id: &str) -> Result<WorkerTopology>;

    fn request_cancel_tree(
        &self,
        owner: &str,
        root_worker_id: &str,
        request: &CancelRequest,
    ) -> Result<CancelPlan>;

    fn settle_cancel(
        &self,
        owner: &str,
        ticket: &CancelTicket,
        outcome: CancelOutcome,
    ) -> Result<AgentRunRecord>;

    fn claim_recovery_due(
        &self,
        owner: &str,
        reconciler: &str,
        lease_seconds: u64,
        limit: usize,
    ) -> Result<Vec<RecoveryTicket>>;

    /// Settle only after the controller adapter affirmatively proved the old
    /// controller is gone (or synchronously stopped it and observed exit).
    fn confirm_controller_gone(
        &self,
        owner: &str,
        ticket: &RecoveryTicket,
    ) -> Result<AgentRunRecord>;

    fn release_worker(
        &self,
        owner: &str,
        worker_id: &str,
        expected_revision: u64,
    ) -> Result<WorkerRecord>;

    fn enqueue_resume(&self, owner: &str, request: &EnqueueResume) -> Result<AgentRunRecord>;

    fn unprojected_events(&self, owner: &str, projector: &str, limit: usize) -> Result<OutboxPage>;

    fn mark_projected(&self, owner: &str, projector: &str, event_id: &str) -> Result<()>;

    /// Persist a body-free retry fence for one exact source event.
    ///
    /// Stores without durable disposition support fail closed. A successful
    /// projection recorded concurrently or earlier always wins.
    fn defer_projection(
        &self,
        _owner: &str,
        _projector: &str,
        _event_id: &str,
        _reason: ProjectionDeferReason,
        _delay: Duration,
    ) -> Result<()> {
        Err(crate::AgentStoreError::InvalidInput(
            "durable projection deferral is unsupported".into(),
        ))
    }

    /// Permanently isolate one exact bad projection without discarding its
    /// immutable source event. Successful projection progress always wins.
    fn quarantine_projection(
        &self,
        _owner: &str,
        _projector: &str,
        _event_id: &str,
        _reason: ProjectionQuarantineReason,
    ) -> Result<()> {
        Err(crate::AgentStoreError::InvalidInput(
            "durable projection quarantine is unsupported".into(),
        ))
    }
}
