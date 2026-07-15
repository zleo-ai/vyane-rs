use chrono::{DateTime, Utc};

use crate::{
    AcceptanceVerification, GoalEvent, GoalPursuitCheckpoint, GoalQuery, GoalRecord,
    GoalVerificationArtifact, NewGoal, Result,
};

pub trait GoalStore: Send + Sync {
    fn create(&self, owner: &str, goal: NewGoal) -> Result<GoalRecord>;

    fn get(&self, owner: &str, id: &str) -> Result<Option<GoalRecord>>;

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

    /// CAS-write one lease-fenced checkpoint and append its progress event in
    /// the same transaction. A checkpoint from an older lease may be adopted
    /// only by presenting the current goal revision and claim generation.
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
}
