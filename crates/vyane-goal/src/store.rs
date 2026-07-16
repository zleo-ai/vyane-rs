use chrono::{DateTime, Utc};

use crate::{
    AcceptanceVerification, GoalContinuityProjectionSnapshot, GoalContinuitySignal,
    GoalContinuitySignalResult, GoalContinuityState, GoalEvent, GoalPursuitCheckpoint, GoalQuery,
    GoalQuotaEvent, GoalRecord, GoalVerificationArtifact, NewGoal, Result, TakeoverApproval,
    TakeoverApprovalRequest, TakeoverDecision, TakeoverFinish,
};

pub trait GoalStore: Send + Sync {
    fn create(&self, owner: &str, goal: NewGoal) -> Result<GoalRecord>;

    fn get(&self, owner: &str, id: &str) -> Result<Option<GoalRecord>>;

    /// Read one goal and all of its continuity approvals for projection.
    ///
    /// The source-compatible default fails closed because composing `get` and
    /// `list_takeover_approvals` would permit a torn concurrent read. Stores
    /// that expose projection must override this with one storage-native read
    /// transaction.
    fn continuity_projection_snapshot(
        &self,
        _owner: &str,
        _id: &str,
    ) -> Result<Option<GoalContinuityProjectionSnapshot>> {
        Err(crate::GoalStoreError::InvalidInput(
            "goal store does not provide an atomic continuity projection snapshot".into(),
        ))
    }

    fn list(&self, owner: &str, query: &GoalQuery) -> Result<Vec<GoalRecord>>;

    fn next_queued(&self, owner: &str) -> Result<Option<GoalRecord>>;

    /// Atomically claim a specific goal for `worker_id` under a lease of
    /// `lease_seconds`. Succeeds from `queued`; a goal already claimed under an
    /// unexpired lease is rejected with [`crate::GoalStoreError::LeaseHeld`].
    fn claim(
        &self,
        owner: &str,
        id: &str,
        worker_id: &str,
        lease_seconds: u64,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord>;

    /// Atomically select and claim the highest-priority queued goal in one
    /// transaction (the safe replacement for `next_queued` + `start`).
    fn claim_next(
        &self,
        owner: &str,
        worker_id: &str,
        lease_seconds: u64,
        at: DateTime<Utc>,
    ) -> Result<Option<GoalRecord>>;

    /// Heartbeat: extend the lease held by `worker_id`. Rejected when the lease
    /// is held by another worker or has already expired.
    fn renew_lease(
        &self,
        owner: &str,
        id: &str,
        worker_id: &str,
        lease_seconds: u64,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord>;

    /// Take over a goal whose lease has expired. Rejected while the current
    /// lease is still active.
    fn reclaim(
        &self,
        owner: &str,
        id: &str,
        worker_id: &str,
        lease_seconds: u64,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord>;

    /// Record that acceptance criterion `index` was actually verified: the only
    /// code path that writes `satisfied_at`. While the goal is under an active
    /// lease, `worker_id` must match the lease holder.
    fn satisfy_criterion(
        &self,
        owner: &str,
        id: &str,
        worker_id: Option<&str>,
        index: usize,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord>;

    /// Append one immutable, owner-scoped verification artifact. The goal must
    /// be in progress and an active lease, if present, must belong to
    /// `worker_id`.
    fn record_verification(
        &self,
        owner: &str,
        id: &str,
        worker_id: Option<&str>,
        verification: &AcceptanceVerification,
        at: DateTime<Utc>,
    ) -> Result<GoalVerificationArtifact>;

    fn verifications(&self, owner: &str, id: &str) -> Result<Vec<GoalVerificationArtifact>>;

    fn events(&self, owner: &str, id: &str) -> Result<Vec<GoalEvent>>;

    fn pursuit_checkpoint(&self, owner: &str, id: &str) -> Result<Option<GoalPursuitCheckpoint>>;

    /// Idempotently turn one normalized quota fact into visible continuity
    /// state. This records policy intent only and never starts a runtime.
    fn record_quota_handoff(
        &self,
        owner: &str,
        id: &str,
        event: &GoalQuotaEvent,
        at: DateTime<Utc>,
    ) -> Result<Option<GoalContinuityState>>;

    /// Idempotently record one exact external readiness fact. This may release
    /// a visible plan dependency but never consumes approval or dispatches.
    fn record_continuity_signal(
        &self,
        owner: &str,
        id: &str,
        signal: &GoalContinuitySignal,
        at: DateTime<Utc>,
    ) -> Result<GoalContinuitySignalResult>;

    /// CAS-write one lease-fenced checkpoint and append its event in the same
    /// transaction. `Paused` and `Achieved` also perform the matching goal
    /// lifecycle transition atomically. A checkpoint from an older lease may
    /// be adopted only with the current goal revision and claim generation.
    #[allow(clippy::too_many_arguments)]
    fn record_pursuit_checkpoint(
        &self,
        owner: &str,
        id: &str,
        worker_id: &str,
        checkpoint: &GoalPursuitCheckpoint,
        stage: &str,
        detail: &str,
        at: DateTime<Utc>,
    ) -> Result<(GoalPursuitCheckpoint, GoalEvent)>;

    fn start(&self, owner: &str, id: &str, at: DateTime<Utc>) -> Result<GoalRecord>;

    fn progress(
        &self,
        owner: &str,
        id: &str,
        stage: &str,
        detail: &str,
        at: DateTime<Utc>,
    ) -> Result<GoalEvent>;

    /// Pause an in-progress goal. While an active lease is held, only the
    /// holder (matching `worker_id`) may pause; pausing releases the lease.
    fn pause(
        &self,
        owner: &str,
        id: &str,
        worker_id: Option<&str>,
        reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord>;

    /// Resume a paused goal. Pausing already released any lease, so a resumed
    /// goal is always unleased; any stale lease fields are cleared defensively.
    fn resume(
        &self,
        owner: &str,
        id: &str,
        worker_id: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord>;

    /// Complete a goal. Every acceptance criterion must carry `satisfied_at`,
    /// unless `waive_reason` explicitly waives the unsatisfied remainder, which
    /// appends an auditable `criteria_waived` event before completion. While an
    /// active lease is held, only the holder (matching `worker_id`) may
    /// complete; terminal states clear the lease.
    fn done(
        &self,
        owner: &str,
        id: &str,
        worker_id: Option<&str>,
        summary: Option<&str>,
        waive_reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord>;

    /// Fail a goal. While an active lease is held, only the holder (matching
    /// `worker_id`) may fail it; terminal states clear the lease.
    fn fail(
        &self,
        owner: &str,
        id: &str,
        worker_id: Option<&str>,
        reason: &str,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord>;

    /// Cancel a goal. While an active lease is held, only the holder (matching
    /// `worker_id`) may cancel it; terminal states clear the lease.
    fn cancel(
        &self,
        owner: &str,
        id: &str,
        worker_id: Option<&str>,
        reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<GoalRecord>;

    /// Idempotently queue one approval for the current supported, ready
    /// continuity step. The same bound snapshot returns the existing approval.
    /// The queue never dispatches.
    fn queue_takeover_approval(
        &self,
        owner: &str,
        request: &TakeoverApprovalRequest,
        at: DateTime<Utc>,
    ) -> Result<TakeoverApproval>;

    /// Record an explicit approve/reject decision on a pending approval. The
    /// decision is immutable once recorded.
    fn decide_takeover_approval(
        &self,
        owner: &str,
        approval_id: &str,
        decision: TakeoverDecision,
        decided_by: &str,
        reason: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<TakeoverApproval>;

    /// Atomically consume an approved, unconsumed approval and mark its
    /// takeover step `in_flight` in the same transaction. Re-validates owner,
    /// status, goal readiness and the bound boundary; a changed boundary or a
    /// non-ready/terminal goal fails closed. A crash after this point leaves
    /// the approval visibly `in_flight` and unrecoverable by a second consume.
    fn consume_takeover_approval(
        &self,
        owner: &str,
        approval_id: &str,
        at: DateTime<Utc>,
    ) -> Result<TakeoverApproval>;

    /// Settle an in-flight approval done or blocked with run evidence, and mark
    /// its takeover step correspondingly, in one transaction.
    fn finish_takeover_approval(
        &self,
        owner: &str,
        approval_id: &str,
        finish: &TakeoverFinish,
        at: DateTime<Utc>,
    ) -> Result<TakeoverApproval>;

    fn get_takeover_approval(
        &self,
        owner: &str,
        approval_id: &str,
    ) -> Result<Option<TakeoverApproval>>;

    fn list_takeover_approvals(
        &self,
        owner: &str,
        goal_id: Option<&str>,
    ) -> Result<Vec<TakeoverApproval>>;
}
