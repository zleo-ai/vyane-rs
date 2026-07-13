use chrono::{DateTime, Utc};

use crate::{
    ControllerRef, FailureCode, Lease, NewTask, Result, TaskEvent, TaskPage, TaskQuery, TaskRecord,
    TaskSettlement,
};

/// Synchronous durable metadata store.
///
/// Methods that mutate an existing task require both the expected snapshot
/// revision and executor epoch. Callers therefore cannot let a stale worker or
/// lease holder overwrite a newer owner.
pub trait TaskStore: Send + Sync {
    fn create(&self, owner: &str, task: NewTask) -> Result<TaskRecord>;

    fn get(&self, owner: &str, id: &str) -> Result<Option<TaskRecord>>;

    fn list(&self, owner: &str, query: &TaskQuery) -> Result<TaskPage>;

    fn events(&self, owner: &str, id: &str) -> Result<Vec<TaskEvent>>;

    #[allow(clippy::too_many_arguments)]
    fn attach_controller(
        &self,
        owner: &str,
        id: &str,
        expected_revision: u64,
        expected_executor_epoch: u64,
        controller: ControllerRef,
        lease: Option<Lease>,
        at: DateTime<Utc>,
    ) -> Result<TaskRecord>;

    fn request_cancel(
        &self,
        owner: &str,
        id: &str,
        expected_revision: u64,
        expected_executor_epoch: u64,
        at: DateTime<Utc>,
    ) -> Result<TaskRecord>;

    fn settle(
        &self,
        owner: &str,
        id: &str,
        expected_revision: u64,
        expected_executor_epoch: u64,
        settlement: TaskSettlement,
        at: DateTime<Utc>,
    ) -> Result<TaskRecord>;

    fn interrupt(
        &self,
        owner: &str,
        id: &str,
        expected_revision: u64,
        expected_executor_epoch: u64,
        code: FailureCode,
        at: DateTime<Utc>,
    ) -> Result<TaskRecord>;

    #[allow(clippy::too_many_arguments)]
    fn claim_expired(
        &self,
        owner: &str,
        id: &str,
        expected_revision: u64,
        expected_executor_epoch: u64,
        controller: ControllerRef,
        lease: Lease,
        now: DateTime<Utc>,
    ) -> Result<TaskRecord>;

    #[allow(clippy::too_many_arguments)]
    fn renew_lease(
        &self,
        owner: &str,
        id: &str,
        expected_revision: u64,
        expected_executor_epoch: u64,
        lease_owner: &str,
        expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<TaskRecord>;
}
