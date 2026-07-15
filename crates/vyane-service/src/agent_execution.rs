//! Bounded, owner-bound execution of newly due AgentRuns.
//!
//! This is deliberately a consuming, one-shot integration seam. It does not
//! own a resident scheduler, runtime, queue, or recovery loop. A controller
//! that has been durably registered is never guessed to be gone here: every
//! uncertain exit remains `Running` for the recovery driver to reconcile.

use std::collections::BTreeSet;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{FutureExt as _, StreamExt as _, stream};
use tokio::time::{Instant, sleep_until};
use vyane_agent::{
    ActiveCompletionPermit, ActiveExecutionPermit, AgentRunRecord, AgentStore, ClaimedRun,
    ControllerKind, ControllerRef, ExecutionBackend, RunCompletionStatus, RunFailureCode,
    RunLeaseReceipt, RunSettlement, RunState,
};
use vyane_core::CancellationToken;

/// Maximum runs claimed by one invocation.
pub const MAX_EXECUTION_BATCH: usize = 64;
/// Maximum run futures concurrently polled by one invocation.
pub const MAX_EXECUTION_CONCURRENCY: usize = 16;
/// Maximum lease renewal requested by this adapter.
pub const MAX_EXECUTION_LEASE_SECONDS: u64 = 5 * 60;
/// Maximum heartbeat interval.
pub const MAX_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);
/// Minimum heartbeat interval, preventing accidental store hammering.
pub const MIN_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

const MAX_OWNER_BYTES: usize = 256;
const MAX_IDENTITY_BYTES: usize = 64;
const CONTROLLER_PREFIX: &str = "vyane-exec-v1:";

/// Immutable, body-free input available while admitting a prospective handle.
#[derive(Clone, PartialEq, Eq)]
pub struct AgentExecutionIdentity {
    run_id: String,
    worker_id: String,
    generation: u64,
    target_key: String,
    prompt_digest: String,
    policy_digest: String,
    timeout_seconds: u64,
}

impl AgentExecutionIdentity {
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }
    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
    #[must_use]
    pub fn target_key(&self) -> &str {
        &self.target_key
    }
    #[must_use]
    pub fn prompt_digest(&self) -> &str {
        &self.prompt_digest
    }
    #[must_use]
    pub fn policy_digest(&self) -> &str {
        &self.policy_digest
    }
    /// Frozen duration used to derive this run's durable claim deadline.
    #[must_use]
    pub const fn timeout_seconds(&self) -> u64 {
        self.timeout_seconds
    }
}

impl fmt::Debug for AgentExecutionIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentExecutionIdentity")
            .finish_non_exhaustive()
    }
}

/// Context for one executor call.
pub struct AgentExecutionContext {
    deadline: Instant,
    controller: ControllerRef,
    cancellation: CancellationToken,
}

impl AgentExecutionContext {
    #[must_use]
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    #[must_use]
    pub fn controller(&self) -> &ControllerRef {
        &self.controller
    }

    #[must_use]
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

impl fmt::Debug for AgentExecutionContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentExecutionContext")
            .finish_non_exhaustive()
    }
}

/// Proof that a quiesced controller staged one exact completion externally.
///
/// Executors may construct this value only after revalidating the completion
/// permit at the sink's staging linearization point and observing an exact,
/// idempotent stage. The execution driver consumes the permit to atomically
/// commit the completion and the AgentRun success state.
pub struct StagedRunCompletion {
    permit: ActiveCompletionPermit,
}

impl StagedRunCompletion {
    #[must_use]
    pub(crate) fn new(permit: ActiveCompletionPermit) -> Self {
        Self { permit }
    }

    #[must_use]
    pub fn completion_id(&self) -> &str {
        self.permit.completion_id()
    }

    fn into_permit(self) -> ActiveCompletionPermit {
        self.permit
    }
}

impl fmt::Debug for StagedRunCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StagedRunCompletion")
            .field("completion_id", &self.completion_id())
            .finish_non_exhaustive()
    }
}

/// A quiesced controller's terminal, body-free disposition.
#[derive(Debug)]
pub enum AgentExecutionSettlement {
    CompletionStaged(StagedRunCompletion),
    Failed { code: RunFailureCode },
    TimedOut,
}

/// Result of polling a controller to an observation boundary.
#[derive(Debug)]
pub enum AgentExecutorOutcome {
    /// All effects for the prospective controller have stopped. Only this
    /// observation grants this driver settlement authority.
    Quiesced(AgentExecutionSettlement),
    /// The controller's state cannot be proved. It remains recovery-owned.
    Unknown,
}

/// Trusted execution boundary for one controller implementation.
///
/// `admit_controller` must be pure, bounded, and non-blocking: no process,
/// request, task, file, socket, lock wait, or other externally visible effect
/// may be created. It is run on Tokio's blocking pool only to isolate a broken
/// implementation from the async worker; the whole operation is still fully
/// awaited. The driver supplies a fresh
/// 256-bit domain-separated identifier plus an independent 256-bit
/// fingerprint; raw PIDs and server-assigned identifiers are never durable
/// identities. The method returns whether this adapter can create and later
/// recover that exact identity. For `Process` and `Remote`, `execute` may create an effect
/// only when it can idempotently create it *under that exact pre-generated
/// handle* and the recovery adapter can later query/cancel the same identity
/// without ambiguity. Otherwise it must return `Unknown` before creating an
/// effect. Receiving [`ActiveExecutionPermit`] is not by itself permission to
/// perform an effect: immediately at every external-effect linearization point
/// the implementation must revalidate it with
/// [`AgentStore::validate_execution_permit`] (or an authority bridge with an
/// equally strong atomic check). A stale or revoked permit must produce no
/// effect and return `Unknown`. A dropped, cancelled, timed-out, or panicking call may be polled
/// again only through recovery; implementations must not detach unowned work.
/// Panic payloads must remain body-free because panic hooks run before this
/// driver can map an unwind to a redacted status.
#[async_trait]
pub trait AgentRunExecutor: Send + Sync {
    fn kind(&self) -> ControllerKind;

    fn admit_controller(
        &self,
        identity: &AgentExecutionIdentity,
        controller: &ControllerRef,
    ) -> bool;

    /// Durably reserve recovery evidence for the prospective controller.
    ///
    /// This bounded, synchronous hook is invoked on Tokio's blocking pool and
    /// fully awaited before the store may publish `Running`. The default is a
    /// no-op for controller kinds whose identity is intrinsically recoverable.
    fn reserve_controller(
        &self,
        _identity: &AgentExecutionIdentity,
        _controller: &ControllerRef,
    ) -> bool {
        true
    }

    /// Release executor-local recovery evidence after durable settlement.
    /// The driver invokes this bounded synchronous hook on its blocking pool.
    fn confirmed_controller_gone(&self, _controller: &ControllerRef) {}

    async fn execute(
        &self,
        context: AgentExecutionContext,
        identity: AgentExecutionIdentity,
        permit: ActiveExecutionPermit,
    ) -> AgentExecutorOutcome;
}

/// Hard bounds for one execution pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExecutionOptions {
    pub batch_limit: usize,
    pub max_in_flight: usize,
    pub lease_seconds: u64,
    pub heartbeat_interval: Duration,
}

impl Default for AgentExecutionOptions {
    fn default() -> Self {
        Self {
            batch_limit: 16,
            max_in_flight: 4,
            lease_seconds: 30,
            heartbeat_interval: Duration::from_secs(10),
        }
    }
}

/// Construction or claim failure, intentionally stripped of store details.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentExecutionError {
    InvalidOwner,
    InvalidLeaseOwner,
    InvalidOptions,
    InvalidExecutorMetadata,
    ExecutorMetadataPanicked,
    RandomUnavailable,
    RuntimeUnavailable,
    ClaimTaskFailed,
    ClaimStoreFailed,
    InvalidStoreResult,
}

impl fmt::Display for AgentExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidOwner => "execution owner is invalid",
            Self::InvalidLeaseOwner => "execution lease owner is invalid",
            Self::InvalidOptions => "execution options are invalid",
            Self::InvalidExecutorMetadata => "executor metadata is invalid",
            Self::ExecutorMetadataPanicked => "executor metadata panicked",
            Self::RandomUnavailable => "controller identity generation failed",
            Self::RuntimeUnavailable => "execution requires a Tokio runtime",
            Self::ClaimTaskFailed => "execution claim task failed",
            Self::ClaimStoreFailed => "execution claim store operation failed",
            Self::InvalidStoreResult => "execution store returned an invalid result",
        })
    }
}

impl std::error::Error for AgentExecutionError {}

/// Body-free status for one claimed item, in completion order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentExecutionItemStatus {
    Settled,
    InvalidClaim,
    ControllerReservationPanicked,
    InvalidController,
    StartFailed,
    PermitFailed,
    Cancelled,
    TimedOut,
    ExecutorPanicked,
    ControllerUnknown,
    HeartbeatFailed,
    SettlementFailed,
}

/// Bounded report with no durable identity or executor body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExecutionReport {
    pub claimed: usize,
    pub items: Vec<AgentExecutionItemStatus>,
    pub cancelled_before_claim: bool,
}

/// Fixed-owner, non-clone, consuming AgentRun execution driver.
pub struct AgentRunExecutionDriver {
    owner: String,
    store: Arc<dyn AgentStore>,
    lease_owner: String,
    options: AgentExecutionOptions,
    executor: Arc<dyn AgentRunExecutor>,
    executor_kind: ControllerKind,
    execution_backend: ExecutionBackend,
}

impl AgentRunExecutionDriver {
    pub fn new(
        owner: impl Into<String>,
        store: Arc<dyn AgentStore>,
        lease_owner: impl Into<String>,
        options: AgentExecutionOptions,
        executor: Arc<dyn AgentRunExecutor>,
    ) -> Result<Self, AgentExecutionError> {
        let owner = owner.into();
        let lease_owner = lease_owner.into();
        validate_text(&owner, MAX_OWNER_BYTES).map_err(|()| AgentExecutionError::InvalidOwner)?;
        validate_identity(&lease_owner).map_err(|()| AgentExecutionError::InvalidLeaseOwner)?;
        validate_options(&options)?;
        let executor_kind = catch_unwind(AssertUnwindSafe(|| executor.kind()))
            .map_err(|_| AgentExecutionError::ExecutorMetadataPanicked)?;
        Self::new_with_executor_kind(owner, store, lease_owner, options, executor, executor_kind)
    }

    pub(crate) fn new_with_executor_kind(
        owner: impl Into<String>,
        store: Arc<dyn AgentStore>,
        lease_owner: impl Into<String>,
        options: AgentExecutionOptions,
        executor: Arc<dyn AgentRunExecutor>,
        executor_kind: ControllerKind,
    ) -> Result<Self, AgentExecutionError> {
        let owner = owner.into();
        let lease_owner = lease_owner.into();
        validate_text(&owner, MAX_OWNER_BYTES).map_err(|()| AgentExecutionError::InvalidOwner)?;
        validate_identity(&lease_owner).map_err(|()| AgentExecutionError::InvalidLeaseOwner)?;
        validate_options(&options)?;
        let execution_backend = ExecutionBackend::for_controller_kind(executor_kind);
        if execution_backend.controller_kind() != Some(executor_kind) {
            return Err(AgentExecutionError::InvalidExecutorMetadata);
        }
        Ok(Self {
            owner,
            store,
            lease_owner,
            options,
            executor,
            executor_kind,
            execution_backend,
        })
    }

    /// Claim and drive one bounded batch. The monotonic base is captured
    /// before the blocking claim, so claim and setup consume every run's fixed
    /// timeout budget. Blocking store calls, once started, are always awaited.
    pub async fn execute_once(
        self,
        cancellation: CancellationToken,
    ) -> Result<AgentExecutionReport, AgentExecutionError> {
        self.execute_once_with_cancellation(cancellation, ActiveCancellation::DropExecutor)
            .await
    }

    /// Claim and drive one bounded resident-host batch while cooperatively
    /// draining an executor that has already been polled.
    ///
    /// Cancellation still prevents a claim or executor start when observed at
    /// the existing pre-effect fences. Once an executor future has been
    /// created, however, the same token is delivered through
    /// [`AgentExecutionContext`] and the future remains owned and polled until
    /// it returns. This is the shutdown path for process-owning executors:
    /// dropping their future on host cancellation would bypass their
    /// terminate-and-reap and lifecycle-reporting guards.
    pub(crate) async fn execute_once_cooperative_shutdown(
        self,
        cancellation: CancellationToken,
    ) -> Result<AgentExecutionReport, AgentExecutionError> {
        self.execute_once_with_cancellation(cancellation, ActiveCancellation::AwaitExecutor)
            .await
    }

    async fn execute_once_with_cancellation(
        self,
        cancellation: CancellationToken,
        active_cancellation: ActiveCancellation,
    ) -> Result<AgentExecutionReport, AgentExecutionError> {
        if cancellation.is_cancelled() {
            return Ok(AgentExecutionReport {
                claimed: 0,
                items: Vec::new(),
                cancelled_before_claim: true,
            });
        }
        tokio::runtime::Handle::try_current()
            .map_err(|_| AgentExecutionError::RuntimeUnavailable)?;
        let base = Instant::now();
        let store = Arc::clone(&self.store);
        let owner = self.owner.clone();
        let lease_owner = self.lease_owner.clone();
        let lease_seconds = self.options.lease_seconds;
        let limit = self.options.batch_limit;
        let execution_backend = self.execution_backend;
        let claimed = tokio::task::spawn_blocking(move || {
            store.claim_due(
                &owner,
                execution_backend,
                &lease_owner,
                lease_seconds,
                limit,
            )
        })
        .await
        .map_err(|_| AgentExecutionError::ClaimTaskFailed)?
        .map_err(|_| AgentExecutionError::ClaimStoreFailed)?;
        validate_claims(
            &claimed,
            &self.owner,
            self.execution_backend,
            &self.lease_owner,
            limit,
        )?;

        let executor = Arc::clone(&self.executor);
        let executor_kind = self.executor_kind;
        let prepared =
            tokio::task::spawn_blocking(move || preflight_claims(claimed, executor, executor_kind))
                .await
                .map_err(|_| AgentExecutionError::ExecutorMetadataPanicked)??;

        let count = prepared.len();
        let owner = self.owner;
        let store = self.store;
        let executor = self.executor;
        let options = self.options;
        let execution_cancellation = ExecutionCancellation {
            token: cancellation,
            active: active_cancellation,
        };
        let items = stream::iter(prepared.into_iter().map(|prepared| {
            execute_one(
                owner.clone(),
                Arc::clone(&store),
                Arc::clone(&executor),
                prepared,
                execution_cancellation.clone(),
                options.clone(),
                base,
            )
        }))
        .buffer_unordered(options.max_in_flight)
        .collect()
        .await;
        Ok(AgentExecutionReport {
            claimed: count,
            items,
            cancelled_before_claim: false,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveCancellation {
    DropExecutor,
    AwaitExecutor,
}

#[derive(Clone)]
struct ExecutionCancellation {
    token: CancellationToken,
    active: ActiveCancellation,
}

struct PreparedRun {
    claim: ClaimedRun,
    identity: AgentExecutionIdentity,
    controller: ControllerRef,
}

fn preflight_claims(
    claims: Vec<ClaimedRun>,
    executor: Arc<dyn AgentRunExecutor>,
    executor_kind: ControllerKind,
) -> Result<Vec<PreparedRun>, AgentExecutionError> {
    let mut identities = BTreeSet::new();
    let mut prepared = Vec::with_capacity(claims.len());
    for claim in claims {
        let identity = execution_identity(&claim);
        let controller = random_controller(executor_kind)?;
        let admitted = catch_unwind(AssertUnwindSafe(|| {
            executor.admit_controller(&identity, &controller)
        }))
        .map_err(|_| AgentExecutionError::ExecutorMetadataPanicked)?;
        let key = (
            controller.id.clone(),
            controller.fingerprint.clone().unwrap_or_default(),
        );
        if !admitted
            || controller.kind != executor_kind
            || !valid_prospective_controller(&controller)
            || !identities.insert(key)
        {
            return Err(AgentExecutionError::InvalidExecutorMetadata);
        }
        prepared.push(PreparedRun {
            claim,
            identity,
            controller,
        });
    }
    Ok(prepared)
}

impl fmt::Debug for AgentRunExecutionDriver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentRunExecutionDriver")
            .finish_non_exhaustive()
    }
}

async fn execute_one(
    owner: String,
    store: Arc<dyn AgentStore>,
    executor: Arc<dyn AgentRunExecutor>,
    prepared: PreparedRun,
    cancellation: ExecutionCancellation,
    options: AgentExecutionOptions,
    base: Instant,
) -> AgentExecutionItemStatus {
    let PreparedRun {
        claim,
        identity,
        controller,
    } = prepared;
    let Some(deadline) = base.checked_add(Duration::from_secs(claim.run.timeout_seconds)) else {
        return AgentExecutionItemStatus::InvalidClaim;
    };
    if Instant::now() >= deadline {
        return AgentExecutionItemStatus::TimedOut;
    }
    if cancellation.token.is_cancelled() {
        return AgentExecutionItemStatus::Cancelled;
    }
    let reserved = {
        let executor = Arc::clone(&executor);
        let identity = identity.clone();
        let controller = controller.clone();
        tokio::task::spawn_blocking(move || {
            catch_unwind(AssertUnwindSafe(|| {
                executor.reserve_controller(&identity, &controller)
            }))
        })
        .await
    };
    match reserved {
        Ok(Ok(true)) => {}
        Ok(Ok(false)) => return AgentExecutionItemStatus::InvalidController,
        Ok(Err(_)) | Err(_) => return AgentExecutionItemStatus::ControllerReservationPanicked,
    }
    if cancellation.token.is_cancelled() || Instant::now() >= deadline {
        blocking_confirmed_controller_gone(Arc::clone(&executor), controller.clone()).await;
        return if cancellation.token.is_cancelled() {
            AgentExecutionItemStatus::Cancelled
        } else {
            AgentExecutionItemStatus::TimedOut
        };
    }
    let started = match blocking_start(
        Arc::clone(&store),
        owner.clone(),
        claim.receipt.clone(),
        controller.clone(),
    )
    .await
    {
        Some(value) => value,
        None => return AgentExecutionItemStatus::StartFailed,
    };
    if !valid_start_transition(&claim, &started, &owner, &controller) {
        return AgentExecutionItemStatus::StartFailed;
    }
    if cancellation.token.is_cancelled() {
        return AgentExecutionItemStatus::Cancelled;
    }
    if Instant::now() >= deadline {
        return AgentExecutionItemStatus::TimedOut;
    }
    let policy_digest = started.run.policy_digest.clone();
    let permit = match blocking_permit(
        Arc::clone(&store),
        owner.clone(),
        started.receipt.clone(),
        policy_digest,
    )
    .await
    {
        Some(value) => value,
        None => return AgentExecutionItemStatus::PermitFailed,
    };
    if !valid_permit(&permit, &started, &owner) {
        return AgentExecutionItemStatus::PermitFailed;
    }
    if cancellation.token.is_cancelled() {
        return AgentExecutionItemStatus::Cancelled;
    }
    if Instant::now() >= deadline {
        return AgentExecutionItemStatus::TimedOut;
    }
    // Renew once immediately before any executor poll. Claim, reservation,
    // start, and permit issuance may have consumed most of the original lease;
    // allowing the first external effect before this fence would race recovery.
    let Some(heartbeat) = blocking_heartbeat(
        Arc::clone(&store),
        owner.clone(),
        started.receipt.clone(),
        options.lease_seconds,
    )
    .await
    else {
        return AgentExecutionItemStatus::HeartbeatFailed;
    };
    if !valid_heartbeat_transition(&started, &heartbeat, &owner, &controller) {
        return AgentExecutionItemStatus::HeartbeatFailed;
    }
    let mut current = heartbeat;
    if cancellation.token.is_cancelled() {
        return AgentExecutionItemStatus::Cancelled;
    }
    if Instant::now() >= deadline {
        return AgentExecutionItemStatus::TimedOut;
    }
    let call = match catch_unwind(AssertUnwindSafe(|| {
        executor.execute(
            AgentExecutionContext {
                deadline,
                controller: controller.clone(),
                cancellation: cancellation.token.clone(),
            },
            identity,
            permit,
        )
    })) {
        Ok(value) => value,
        Err(_) => return AgentExecutionItemStatus::ExecutorPanicked,
    };
    let mut call = Box::pin(AssertUnwindSafe(call).catch_unwind());
    let mut receipt = current.receipt.clone();
    let mut next_heartbeat = Instant::now() + options.heartbeat_interval;
    loop {
        tokio::select! {
            biased;
            () = cancellation.token.cancelled(), if cancellation.active == ActiveCancellation::AwaitExecutor => {
                // The executor owns controller quiescence from its first poll.
                // Keep polling it directly after cooperative shutdown instead
                // of letting a heartbeat or deadline branch drop the future.
                let outcome = call.await;
                return match outcome {
                    Err(_) => AgentExecutionItemStatus::ExecutorPanicked,
                    Ok(AgentExecutorOutcome::Unknown) => AgentExecutionItemStatus::ControllerUnknown,
                    Ok(AgentExecutorOutcome::Quiesced(settlement)) => {
                        let status = settle_run(store, owner, receipt, current.run.clone(), settlement).await;
                        if status == AgentExecutionItemStatus::Settled {
                            blocking_confirmed_controller_gone(Arc::clone(&executor), controller.clone()).await;
                        }
                        status
                    }
                };
            }
            () = cancellation.token.cancelled(), if cancellation.active == ActiveCancellation::DropExecutor => {
                return AgentExecutionItemStatus::Cancelled;
            }
            () = sleep_until(deadline) => return AgentExecutionItemStatus::TimedOut,
            outcome = &mut call => {
                return match outcome {
                    Err(_) => AgentExecutionItemStatus::ExecutorPanicked,
                    Ok(AgentExecutorOutcome::Unknown) => AgentExecutionItemStatus::ControllerUnknown,
                    Ok(AgentExecutorOutcome::Quiesced(settlement)) => {
                        let status = settle_run(store, owner, receipt, current.run.clone(), settlement).await;
                        if status == AgentExecutionItemStatus::Settled {
                            blocking_confirmed_controller_gone(Arc::clone(&executor), controller.clone()).await;
                        }
                        status
                    }
                };
            }
            () = sleep_until(next_heartbeat) => {
                let Some(next) = blocking_heartbeat(
                    Arc::clone(&store), owner.clone(), receipt.clone(), options.lease_seconds,
                ).await else { return AgentExecutionItemStatus::HeartbeatFailed; };
                if !valid_heartbeat_transition(&current, &next, &owner, &controller) {
                    return AgentExecutionItemStatus::HeartbeatFailed;
                }
                receipt = next.receipt.clone();
                current = next;
                next_heartbeat = Instant::now() + options.heartbeat_interval;
            }
        }
    }
}

async fn blocking_start(
    store: Arc<dyn AgentStore>,
    owner: String,
    receipt: RunLeaseReceipt,
    controller: ControllerRef,
) -> Option<ClaimedRun> {
    tokio::task::spawn_blocking(move || store.start(&owner, &receipt, &controller))
        .await
        .ok()?
        .ok()
}

async fn blocking_confirmed_controller_gone(
    executor: Arc<dyn AgentRunExecutor>,
    controller: ControllerRef,
) {
    let _ = tokio::task::spawn_blocking(move || {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            executor.confirmed_controller_gone(&controller);
        }));
    })
    .await;
}

async fn blocking_permit(
    store: Arc<dyn AgentStore>,
    owner: String,
    receipt: RunLeaseReceipt,
    digest: String,
) -> Option<ActiveExecutionPermit> {
    tokio::task::spawn_blocking(move || store.issue_execution_permit(&owner, &receipt, &digest))
        .await
        .ok()?
        .ok()
}

async fn blocking_heartbeat(
    store: Arc<dyn AgentStore>,
    owner: String,
    receipt: RunLeaseReceipt,
    lease_seconds: u64,
) -> Option<ClaimedRun> {
    tokio::task::spawn_blocking(move || store.heartbeat(&owner, &receipt, lease_seconds))
        .await
        .ok()?
        .ok()
}

async fn settle_run(
    store: Arc<dyn AgentStore>,
    owner: String,
    receipt: RunLeaseReceipt,
    before: AgentRunRecord,
    settlement: AgentExecutionSettlement,
) -> AgentExecutionItemStatus {
    if let AgentExecutionSettlement::CompletionStaged(staged) = settlement {
        return commit_staged_completion(store, owner, before, staged).await;
    }
    let (settlement, expected_state, expected_failure) = match settlement {
        AgentExecutionSettlement::CompletionStaged(_) => unreachable!(),
        AgentExecutionSettlement::Failed { code } => {
            if code.is_resume_eligible()
                || matches!(code, RunFailureCode::TimedOut | RunFailureCode::Cancelled)
            {
                return AgentExecutionItemStatus::SettlementFailed;
            }
            (RunSettlement::Failed { code }, RunState::Failed, Some(code))
        }
        AgentExecutionSettlement::TimedOut => (
            RunSettlement::TimedOut,
            RunState::TimedOut,
            Some(RunFailureCode::TimedOut),
        ),
    };
    match tokio::task::spawn_blocking(move || store.settle(&owner, &receipt, settlement)).await {
        Ok(Ok(after))
            if valid_settle_transition(&before, &after, expected_state, expected_failure) =>
        {
            AgentExecutionItemStatus::Settled
        }
        Ok(Err(_)) | Err(_) => AgentExecutionItemStatus::SettlementFailed,
        Ok(Ok(_)) => AgentExecutionItemStatus::SettlementFailed,
    }
}

async fn commit_staged_completion(
    store: Arc<dyn AgentStore>,
    owner: String,
    before: AgentRunRecord,
    staged: StagedRunCompletion,
) -> AgentExecutionItemStatus {
    let expected_completion_id = staged.completion_id().to_string();
    let permit = staged.into_permit();
    let result =
        tokio::task::spawn_blocking(move || store.commit_completion(&owner, &permit)).await;
    match result {
        Ok(Ok((after, completion)))
            if valid_settle_transition(&before, &after, RunState::Succeeded, None)
                && completion.owner == before.owner
                && completion.run_id == before.id
                && completion.worker_id == before.worker_id
                && completion.worker_generation == before.worker_generation
                && completion.completion_id == expected_completion_id
                && completion.status == RunCompletionStatus::Committed
                && completion.revision > 0
                && completion.prepared_run_revision > 0
                && completion.prepared_run_revision <= before.revision
                && completion.committed_run_revision == Some(after.revision)
                && completion.abandoned_run_revision.is_none()
                && completion.committed_at == after.finished_at
                && completion.abandoned_at.is_none()
                && completion.committed_by_operation_id.is_none() =>
        {
            AgentExecutionItemStatus::Settled
        }
        Ok(Ok(_)) | Ok(Err(_)) | Err(_) => AgentExecutionItemStatus::SettlementFailed,
    }
}

fn validate_options(options: &AgentExecutionOptions) -> Result<(), AgentExecutionError> {
    if !(1..=MAX_EXECUTION_BATCH).contains(&options.batch_limit)
        || !(1..=MAX_EXECUTION_CONCURRENCY).contains(&options.max_in_flight)
        || options.max_in_flight > options.batch_limit
        || !(1..=MAX_EXECUTION_LEASE_SECONDS).contains(&options.lease_seconds)
        || options.heartbeat_interval.is_zero()
        || options.heartbeat_interval < MIN_HEARTBEAT_INTERVAL
        || options.heartbeat_interval > MAX_HEARTBEAT_INTERVAL
        || options.heartbeat_interval >= Duration::from_secs(options.lease_seconds)
    {
        return Err(AgentExecutionError::InvalidOptions);
    }
    Ok(())
}

fn validate_claims(
    claims: &[ClaimedRun],
    owner: &str,
    execution_backend: ExecutionBackend,
    lease_owner: &str,
    limit: usize,
) -> Result<(), AgentExecutionError> {
    if claims.len() > limit {
        return Err(AgentExecutionError::InvalidStoreResult);
    }
    let mut runs = BTreeSet::new();
    let mut workers = BTreeSet::new();
    if claims.iter().any(|claim| {
        !valid_claim(claim, owner, execution_backend)
            || claim.receipt.lease_owner != lease_owner
            || !runs.insert(claim.run.id.as_str())
            || !workers.insert(claim.run.worker_id.as_str())
    }) {
        return Err(AgentExecutionError::InvalidStoreResult);
    }
    Ok(())
}

fn valid_claim(claim: &ClaimedRun, owner: &str, execution_backend: ExecutionBackend) -> bool {
    let receipt = &claim.receipt;
    claim.run.owner == owner
        && claim.run.execution_backend == execution_backend
        && claim.run.state == RunState::Starting
        && claim.run.controller.is_none()
        && claim.run.deadline_at.is_some()
        && claim
            .run
            .lease
            .as_ref()
            .is_some_and(|lease| lease.owner == receipt.lease_owner)
        && claim.run.id == receipt.run_id
        && claim.run.worker_id == receipt.worker_id
        && claim.run.worker_generation == receipt.generation
        && claim.run.revision == receipt.revision
        && validate_text(&claim.run.target_key, 512).is_ok()
        && claim.run.timeout_seconds > 0
        && claim.run.timeout_seconds <= 7 * 24 * 60 * 60
        && claim.run.prompt_digest.len() == 64
        && is_lower_hex(&claim.run.prompt_digest)
        && claim.run.policy_digest.len() == 64
        && is_lower_hex(&claim.run.policy_digest)
        && receipt.generation > 0
        && validate_text(&receipt.run_id, MAX_OWNER_BYTES).is_ok()
        && validate_text(&receipt.worker_id, MAX_OWNER_BYTES).is_ok()
        && validate_identity(&receipt.lease_owner).is_ok()
        && receipt.token.len() == 64
        && is_lower_hex(&receipt.token)
}

fn execution_identity(claim: &ClaimedRun) -> AgentExecutionIdentity {
    AgentExecutionIdentity {
        run_id: claim.run.id.clone(),
        worker_id: claim.run.worker_id.clone(),
        generation: claim.run.worker_generation,
        target_key: claim.run.target_key.clone(),
        prompt_digest: claim.run.prompt_digest.clone(),
        policy_digest: claim.run.policy_digest.clone(),
        timeout_seconds: claim.run.timeout_seconds,
    }
}

fn valid_start_transition(
    before: &ClaimedRun,
    after: &ClaimedRun,
    owner: &str,
    controller: &ControllerRef,
) -> bool {
    same_frozen_run(&before.run, &after.run)
        && after.run.owner == owner
        && before.run.state == RunState::Starting
        && after.run.state == RunState::Running
        && after.run.controller.as_ref() == Some(controller)
        && after.run.started_at.is_some()
        && after.run.last_heartbeat_at.is_some()
        && after.run.last_activity_at.is_some()
        && after.run.lease == before.run.lease
        && after.run.failure_code.is_none()
        && next_revision(before.run.revision, after.run.revision)
        && valid_receipt_transition(&before.receipt, &after.receipt, &after.run)
}

fn valid_heartbeat_transition(
    before: &ClaimedRun,
    after: &ClaimedRun,
    owner: &str,
    controller: &ControllerRef,
) -> bool {
    same_frozen_run(&before.run, &after.run)
        && after.run.owner == owner
        && before.run.state == RunState::Running
        && after.run.state == RunState::Running
        && before.run.controller.as_ref() == Some(controller)
        && after.run.controller.as_ref() == Some(controller)
        && after.run.started_at == before.run.started_at
        && after.run.last_heartbeat_at.is_some()
        && after.run.last_activity_at == before.run.last_activity_at
        && after.run.failure_code.is_none()
        && next_revision(before.run.revision, after.run.revision)
        && before
            .run
            .lease
            .as_ref()
            .zip(after.run.lease.as_ref())
            .is_some_and(|(old, new)| old.owner == new.owner && new.expires_at >= old.expires_at)
        && valid_receipt_transition(&before.receipt, &after.receipt, &after.run)
}

fn valid_receipt_transition(
    before: &RunLeaseReceipt,
    after: &RunLeaseReceipt,
    run: &AgentRunRecord,
) -> bool {
    after.run_id == before.run_id
        && after.worker_id == before.worker_id
        && after.generation == before.generation
        && after.lease_owner == before.lease_owner
        && after.token == before.token
        && after.revision == run.revision
        && run.id == after.run_id
        && run.worker_id == after.worker_id
        && run.worker_generation == after.generation
        && run
            .lease
            .as_ref()
            .is_some_and(|lease| lease.owner == after.lease_owner)
}

fn valid_permit(permit: &ActiveExecutionPermit, run: &ClaimedRun, owner: &str) -> bool {
    permit.owner() == owner
        && permit.run_id() == run.run.id
        && permit.worker_id() == run.run.worker_id
        && permit.generation() == run.run.worker_generation
        && permit.lease_owner() == run.receipt.lease_owner
        && permit.policy_digest() == run.run.policy_digest
}

fn valid_settle_transition(
    before: &AgentRunRecord,
    after: &AgentRunRecord,
    expected_state: RunState,
    expected_failure: Option<RunFailureCode>,
) -> bool {
    same_frozen_run(before, after)
        && before.state == RunState::Running
        && after.state == expected_state
        && after.failure_code == expected_failure
        && after.controller.is_none()
        && after.lease.is_none()
        && after.finished_at.is_some()
        && after.finished_at == Some(after.updated_at)
        && after.started_at == before.started_at
        && next_revision(before.revision, after.revision)
}

fn same_frozen_run(left: &AgentRunRecord, right: &AgentRunRecord) -> bool {
    left.owner == right.owner
        && left.id == right.id
        && left.worker_id == right.worker_id
        && left.task_id == right.task_id
        && left.trace_id == right.trace_id
        && left.parent_run_id == right.parent_run_id
        && left.resume_of_run_id == right.resume_of_run_id
        && left.execution_backend == right.execution_backend
        && left.mode == right.mode
        && left.target_key == right.target_key
        && left.prompt_digest == right.prompt_digest
        && left.policy_digest == right.policy_digest
        && left.resume_binding_digest == right.resume_binding_digest
        && left.available_at == right.available_at
        && left.deadline_at == right.deadline_at
        && left.timeout_seconds == right.timeout_seconds
        && left.max_resume_attempts == right.max_resume_attempts
        && left.resume_attempt == right.resume_attempt
        && left.created_at == right.created_at
        && left.worker_generation == right.worker_generation
}

fn next_revision(before: u64, after: u64) -> bool {
    before.checked_add(1) == Some(after)
}

fn valid_prospective_controller(controller: &ControllerRef) -> bool {
    let Some(suffix) = controller.id.strip_prefix(CONTROLLER_PREFIX) else {
        return false;
    };
    suffix.len() == 64
        && is_lower_hex(suffix)
        && controller
            .fingerprint
            .as_deref()
            .is_some_and(|value| value.len() == 64 && is_lower_hex(value))
}

fn random_controller(kind: ControllerKind) -> Result<ControllerRef, AgentExecutionError> {
    let mut id = [0_u8; 32];
    let mut fingerprint = [0_u8; 32];
    getrandom::fill(&mut id).map_err(|_| AgentExecutionError::RandomUnavailable)?;
    getrandom::fill(&mut fingerprint).map_err(|_| AgentExecutionError::RandomUnavailable)?;
    Ok(ControllerRef {
        kind,
        id: format!("{CONTROLLER_PREFIX}{}", lower_hex(&id)),
        fingerprint: Some(lower_hex(&fingerprint)),
    })
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn validate_text(value: &str, max: usize) -> Result<(), ()> {
    if value.is_empty()
        || value.len() > max
        || value.trim() != value
        || value.contains('\0')
        || value.chars().any(char::is_control)
    {
        return Err(());
    }
    Ok(())
}

fn validate_identity(value: &str) -> Result<(), ()> {
    if value.is_empty()
        || value.len() > MAX_IDENTITY_BYTES
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(());
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::{DateTime, Utc};
    use static_assertions::{assert_impl_all, assert_not_impl_any};
    use tempfile::TempDir;
    use vyane_agent::{
        AgentClock, NewAgentRun, NewRunCompletion, NewWorker, RunMode, SqliteAgentStore,
    };

    use super::*;

    const OWNER: &str = "public-test-owner";
    const LEASE_OWNER: &str = "execution-test";

    struct FixedClock(Mutex<DateTime<Utc>>);

    impl AgentClock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            *self.0.lock().unwrap()
        }
    }

    impl FixedClock {
        fn advance(&self, duration: chrono::TimeDelta) {
            let mut now = self.0.lock().unwrap();
            *now += duration;
        }
    }

    struct Fixture {
        _temp: TempDir,
        store: Arc<SqliteAgentStore>,
        clock: Arc<FixedClock>,
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let clock = Arc::new(FixedClock(Mutex::new(Utc::now())));
            let store = Arc::new(
                SqliteAgentStore::open_with_clock(temp.path().join("agent.db"), clock.clone())
                    .unwrap(),
            );
            Self {
                _temp: temp,
                store,
                clock,
            }
        }

        fn enqueue(&self, suffix: &str, timeout_seconds: u64) {
            self.enqueue_for_backend(suffix, timeout_seconds, ExecutionBackend::NativeInProcess);
        }

        fn enqueue_for_backend(
            &self,
            suffix: &str,
            timeout_seconds: u64,
            execution_backend: ExecutionBackend,
        ) {
            let worker = NewWorker {
                id: format!("worker-{suffix}"),
                logical_session_id: None,
            };
            let run = NewAgentRun {
                id: format!("run-{suffix}"),
                worker_id: worker.id.clone(),
                task_id: None,
                trace_id: None,
                parent_run_id: None,
                execution_backend,
                mode: RunMode::Autonomous,
                target_key: "http:test/model".into(),
                prompt_digest: "a".repeat(64),
                policy_digest: "b".repeat(64),
                available_at: Utc::now() - chrono::TimeDelta::seconds(1),
                timeout_seconds,
                max_resume_attempts: 0,
            };
            self.store.create_root(OWNER, &worker, &run).unwrap();
        }

        fn driver(
            &self,
            executor: Arc<dyn AgentRunExecutor>,
            options: AgentExecutionOptions,
        ) -> AgentRunExecutionDriver {
            AgentRunExecutionDriver::new(OWNER, self.store.clone(), LEASE_OWNER, options, executor)
                .unwrap()
        }
    }

    struct TestExecutor {
        outcome: Mutex<Option<AgentExecutorOutcome>>,
        success_store: Option<Arc<SqliteAgentStore>>,
        calls: AtomicUsize,
        observed_running: Mutex<bool>,
        admissions: Mutex<Vec<ControllerRef>>,
    }

    impl TestExecutor {
        fn new(outcome: AgentExecutorOutcome) -> Self {
            Self {
                outcome: Mutex::new(Some(outcome)),
                success_store: None,
                calls: AtomicUsize::new(0),
                observed_running: Mutex::new(false),
                admissions: Mutex::new(Vec::new()),
            }
        }

        fn success(store: Arc<SqliteAgentStore>) -> Self {
            Self {
                outcome: Mutex::new(None),
                success_store: Some(store),
                calls: AtomicUsize::new(0),
                observed_running: Mutex::new(false),
                admissions: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl AgentRunExecutor for TestExecutor {
        fn kind(&self) -> ControllerKind {
            ControllerKind::InProcess
        }

        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            controller: &ControllerRef,
        ) -> bool {
            self.admissions.lock().unwrap().push(controller.clone());
            true
        }

        async fn execute(
            &self,
            _context: AgentExecutionContext,
            identity: AgentExecutionIdentity,
            permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.observed_running.lock().unwrap() = permit.run_id() == identity.run_id;
            if let Some(store) = &self.success_store {
                return staged_success(store, &permit, identity.run_id());
            }
            self.outcome
                .lock()
                .unwrap()
                .take()
                .unwrap_or(AgentExecutorOutcome::Unknown)
        }
    }

    fn staged_success(
        store: &SqliteAgentStore,
        permit: &ActiveExecutionPermit,
        run_id: &str,
    ) -> AgentExecutorOutcome {
        let prepared = store
            .prepare_completion(
                OWNER,
                permit,
                &NewRunCompletion {
                    id: format!("completion-{run_id}"),
                    sink_kind: "test-sink".into(),
                    publication_key: format!("result.{run_id}"),
                    content_digest: "c".repeat(64),
                    content_bytes: 1,
                },
            )
            .unwrap();
        store
            .validate_completion_permit(OWNER, &prepared.permit)
            .unwrap();
        AgentExecutorOutcome::Quiesced(AgentExecutionSettlement::CompletionStaged(
            StagedRunCompletion::new(prepared.permit),
        ))
    }

    fn options() -> AgentExecutionOptions {
        AgentExecutionOptions {
            batch_limit: 4,
            max_in_flight: 2,
            lease_seconds: 10,
            heartbeat_interval: Duration::from_secs(1),
        }
    }

    assert_impl_all!(AgentRunExecutionDriver: Send, Sync);
    assert_not_impl_any!(AgentRunExecutionDriver: Clone, serde::Serialize, serde::de::DeserializeOwned);

    #[tokio::test]
    async fn start_and_permit_precede_first_executor_poll_and_quiesced_settles() {
        let fixture = Fixture::new();
        fixture.enqueue("success", 30);
        let executor = Arc::new(TestExecutor::success(fixture.store.clone()));
        let report = fixture
            .driver(executor.clone(), options())
            .execute_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.items, [AgentExecutionItemStatus::Settled]);
        assert!(*executor.observed_running.lock().unwrap());
        assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture
                .store
                .get_run(OWNER, "run-success")
                .unwrap()
                .unwrap()
                .state,
            RunState::Succeeded
        );
    }

    #[tokio::test]
    async fn unknown_never_settles_and_leaves_exact_controller_for_recovery() {
        let fixture = Fixture::new();
        fixture.enqueue("unknown", 30);
        let executor = Arc::new(TestExecutor::new(AgentExecutorOutcome::Unknown));
        let report = fixture
            .driver(executor, options())
            .execute_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.items, [AgentExecutionItemStatus::ControllerUnknown]);
        let run = fixture
            .store
            .get_run(OWNER, "run-unknown")
            .unwrap()
            .unwrap();
        assert_eq!(run.state, RunState::Running);
        let controller = run.controller.unwrap();
        assert!(valid_prospective_controller(&controller));
    }

    struct NeverExecutor;

    #[async_trait]
    impl AgentRunExecutor for NeverExecutor {
        fn kind(&self) -> ControllerKind {
            ControllerKind::Remote
        }
        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            _controller: &ControllerRef,
        ) -> bool {
            true
        }
        async fn execute(
            &self,
            _context: AgentExecutionContext,
            _identity: AgentExecutionIdentity,
            _permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn cancellation_drops_executor_without_settlement() {
        let fixture = Fixture::new();
        fixture.enqueue("cancel", 30);
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let report = fixture
            .driver(Arc::new(NeverExecutor), options())
            .execute_once(cancellation)
            .await
            .unwrap();
        assert!(report.cancelled_before_claim);
        assert_eq!(
            fixture
                .store
                .get_run(OWNER, "run-cancel")
                .unwrap()
                .unwrap()
                .state,
            RunState::Queued
        );
    }

    struct InvalidReserve;

    #[async_trait]
    impl AgentRunExecutor for InvalidReserve {
        fn kind(&self) -> ControllerKind {
            ControllerKind::Process
        }
        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            _controller: &ControllerRef,
        ) -> bool {
            false
        }
        async fn execute(
            &self,
            _context: AgentExecutionContext,
            _identity: AgentExecutionIdentity,
            _permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            panic!("must not execute")
        }
    }

    #[tokio::test]
    async fn invalid_prospective_handle_has_zero_external_execution_effect() {
        let fixture = Fixture::new();
        fixture.enqueue_for_backend("invalid", 30, ExecutionBackend::CliHarnessProcess);
        let error = fixture
            .driver(Arc::new(InvalidReserve), options())
            .execute_once(CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error, AgentExecutionError::InvalidExecutorMetadata);
        let run = fixture
            .store
            .get_run(OWNER, "run-invalid")
            .unwrap()
            .unwrap();
        assert_eq!(run.state, RunState::Starting);
        assert!(run.controller.is_none());
    }

    struct CancelOnPoll {
        cancellation: CancellationToken,
    }

    #[async_trait]
    impl AgentRunExecutor for CancelOnPoll {
        fn kind(&self) -> ControllerKind {
            ControllerKind::InProcess
        }
        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            _controller: &ControllerRef,
        ) -> bool {
            true
        }
        async fn execute(
            &self,
            _context: AgentExecutionContext,
            _identity: AgentExecutionIdentity,
            _permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            self.cancellation.cancel();
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn cancellation_after_claim_and_first_poll_never_settles() {
        let fixture = Fixture::new();
        fixture.enqueue("live-cancel", 30);
        let cancellation = CancellationToken::new();
        let executor = Arc::new(CancelOnPoll {
            cancellation: cancellation.clone(),
        });
        let report = fixture
            .driver(executor, options())
            .execute_once(cancellation)
            .await
            .unwrap();
        assert_eq!(report.items, [AgentExecutionItemStatus::Cancelled]);
        let run = fixture
            .store
            .get_run(OWNER, "run-live-cancel")
            .unwrap()
            .unwrap();
        assert_eq!(run.state, RunState::Running);
        assert!(run.controller.is_some());
    }

    struct PanicExecutor;

    #[async_trait]
    impl AgentRunExecutor for PanicExecutor {
        fn kind(&self) -> ControllerKind {
            ControllerKind::InProcess
        }
        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            _controller: &ControllerRef,
        ) -> bool {
            true
        }
        async fn execute(
            &self,
            _context: AgentExecutionContext,
            _identity: AgentExecutionIdentity,
            _permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            panic!("body-free test panic")
        }
    }

    #[tokio::test]
    async fn executor_panic_never_settles() {
        let fixture = Fixture::new();
        fixture.enqueue("panic", 30);
        let report = fixture
            .driver(Arc::new(PanicExecutor), options())
            .execute_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.items, [AgentExecutionItemStatus::ExecutorPanicked]);
        assert_eq!(
            fixture
                .store
                .get_run(OWNER, "run-panic")
                .unwrap()
                .unwrap()
                .state,
            RunState::Running
        );
    }

    #[tokio::test]
    async fn timeout_drops_executor_and_preserves_running_controller() {
        let fixture = Fixture::new();
        fixture.enqueue_for_backend("timeout", 1, ExecutionBackend::Remote);
        let task = tokio::spawn(
            fixture
                .driver(Arc::new(NeverExecutor), options())
                .execute_once(CancellationToken::new()),
        );
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        let report = task.await.unwrap().unwrap();
        assert_eq!(report.items, [AgentExecutionItemStatus::TimedOut]);
        assert_eq!(
            fixture
                .store
                .get_run(OWNER, "run-timeout")
                .unwrap()
                .unwrap()
                .state,
            RunState::Running
        );
    }

    #[tokio::test]
    async fn driver_generates_unique_prospective_handles_for_the_whole_batch() {
        let fixture = Fixture::new();
        fixture.enqueue("duplicate-a", 30);
        fixture.enqueue("duplicate-b", 30);
        let executor = Arc::new(TestExecutor::new(AgentExecutorOutcome::Unknown));
        let report = fixture
            .driver(executor.clone(), options())
            .execute_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.items.len(), 2);
        assert!(
            report
                .items
                .iter()
                .all(|status| *status == AgentExecutionItemStatus::ControllerUnknown)
        );
        assert_eq!(executor.calls.load(Ordering::SeqCst), 2);
        let admissions = executor.admissions.lock().unwrap();
        assert_eq!(admissions.len(), 2);
        assert_ne!(admissions[0].id, admissions[1].id);
        assert_ne!(admissions[0].fingerprint, admissions[1].fingerprint);
        for id in ["run-duplicate-a", "run-duplicate-b"] {
            let run = fixture.store.get_run(OWNER, id).unwrap().unwrap();
            assert_eq!(run.state, RunState::Running);
            assert!(run.controller.is_some());
        }
    }

    struct KindPanic;

    #[async_trait]
    impl AgentRunExecutor for KindPanic {
        fn kind(&self) -> ControllerKind {
            panic!("body-free metadata panic")
        }
        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            _controller: &ControllerRef,
        ) -> bool {
            panic!("must not admit")
        }
        async fn execute(
            &self,
            _context: AgentExecutionContext,
            _identity: AgentExecutionIdentity,
            _permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            panic!("must not execute")
        }
    }

    #[test]
    fn executor_kind_panic_is_caught_at_construction() {
        let fixture = Fixture::new();
        let error = AgentRunExecutionDriver::new(
            OWNER,
            fixture.store.clone(),
            LEASE_OWNER,
            options(),
            Arc::new(KindPanic),
        )
        .unwrap_err();
        assert_eq!(error, AgentExecutionError::ExecutorMetadataPanicked);
    }

    struct ReservePanic;

    #[async_trait]
    impl AgentRunExecutor for ReservePanic {
        fn kind(&self) -> ControllerKind {
            ControllerKind::InProcess
        }
        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            _controller: &ControllerRef,
        ) -> bool {
            true
        }
        fn reserve_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            _controller: &ControllerRef,
        ) -> bool {
            panic!("body-free reservation panic")
        }
        async fn execute(
            &self,
            _context: AgentExecutionContext,
            _identity: AgentExecutionIdentity,
            _permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            panic!("must not execute")
        }
    }

    #[tokio::test]
    async fn reservation_panic_is_caught_before_start() {
        let fixture = Fixture::new();
        fixture.enqueue("reserve-panic", 30);
        let report = fixture
            .driver(Arc::new(ReservePanic), options())
            .execute_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            report.items,
            [AgentExecutionItemStatus::ControllerReservationPanicked]
        );
        let run = fixture
            .store
            .get_run(OWNER, "run-reserve-panic")
            .unwrap()
            .unwrap();
        assert_eq!(run.state, RunState::Starting);
        assert!(run.controller.is_none());
    }

    #[derive(Clone, Copy)]
    enum StoreAttack {
        ForeignClaim,
        WrongBackend,
        SwapStart,
    }

    struct ForeignClaimStore {
        inner: Arc<SqliteAgentStore>,
        attack: StoreAttack,
        claimed: Mutex<Vec<ClaimedRun>>,
    }

    impl AgentStore for ForeignClaimStore {
        fn create_root(
            &self,
            owner: &str,
            worker: &NewWorker,
            run: &NewAgentRun,
        ) -> vyane_agent::Result<(vyane_agent::WorkerRecord, vyane_agent::AgentRunRecord)> {
            self.inner.create_root(owner, worker, run)
        }
        fn spawn_child(
            &self,
            owner: &str,
            parent_worker_id: &str,
            expected_parent_revision: u64,
            child: &NewWorker,
            run: &NewAgentRun,
        ) -> vyane_agent::Result<(vyane_agent::WorkerRecord, vyane_agent::AgentRunRecord)> {
            self.inner.spawn_child(
                owner,
                parent_worker_id,
                expected_parent_revision,
                child,
                run,
            )
        }
        fn enqueue_run(
            &self,
            owner: &str,
            run: &NewAgentRun,
        ) -> vyane_agent::Result<vyane_agent::AgentRunRecord> {
            self.inner.enqueue_run(owner, run)
        }
        fn get_worker(
            &self,
            owner: &str,
            worker_id: &str,
        ) -> vyane_agent::Result<Option<vyane_agent::WorkerRecord>> {
            self.inner.get_worker(owner, worker_id)
        }
        fn get_run(
            &self,
            owner: &str,
            run_id: &str,
        ) -> vyane_agent::Result<Option<vyane_agent::AgentRunRecord>> {
            self.inner.get_run(owner, run_id)
        }
        fn claim_due(
            &self,
            owner: &str,
            execution_backend: ExecutionBackend,
            lease_owner: &str,
            lease_seconds: u64,
            limit: usize,
        ) -> vyane_agent::Result<Vec<ClaimedRun>> {
            let mut claims = self.inner.claim_due(
                owner,
                execution_backend,
                lease_owner,
                lease_seconds,
                limit,
            )?;
            *self.claimed.lock().unwrap() = claims.clone();
            if matches!(self.attack, StoreAttack::ForeignClaim) {
                if let Some(claim) = claims.first_mut() {
                    claim.run.owner = "foreign-owner-canary".into();
                }
            }
            if matches!(self.attack, StoreAttack::WrongBackend) {
                if let Some(claim) = claims.first_mut() {
                    claim.run.execution_backend = ExecutionBackend::Remote;
                }
            }
            Ok(claims)
        }
        fn start(
            &self,
            owner: &str,
            receipt: &RunLeaseReceipt,
            controller: &ControllerRef,
        ) -> vyane_agent::Result<ClaimedRun> {
            if matches!(self.attack, StoreAttack::SwapStart) {
                if let Some(other) = self
                    .claimed
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|claim| claim.receipt.run_id != receipt.run_id)
                    .cloned()
                {
                    return self.inner.start(owner, &other.receipt, controller);
                }
            }
            self.inner.start(owner, receipt, controller)
        }
        fn issue_execution_permit(
            &self,
            owner: &str,
            receipt: &RunLeaseReceipt,
            expected_policy_digest: &str,
        ) -> vyane_agent::Result<ActiveExecutionPermit> {
            self.inner
                .issue_execution_permit(owner, receipt, expected_policy_digest)
        }
        fn validate_execution_permit(
            &self,
            owner: &str,
            permit: &ActiveExecutionPermit,
            expected_policy_digest: &str,
        ) -> vyane_agent::Result<vyane_agent::ExecutionPermitSnapshot> {
            self.inner
                .validate_execution_permit(owner, permit, expected_policy_digest)
        }
        fn validate_native_execution_permit(
            &self,
            owner: &str,
            permit: &ActiveExecutionPermit,
            scope: &vyane_agent::NativeExecutionScope,
        ) -> vyane_agent::Result<vyane_agent::ExecutionPermitSnapshot> {
            self.inner
                .validate_native_execution_permit(owner, permit, scope)
        }
        fn heartbeat(
            &self,
            owner: &str,
            receipt: &RunLeaseReceipt,
            lease_seconds: u64,
        ) -> vyane_agent::Result<ClaimedRun> {
            self.inner.heartbeat(owner, receipt, lease_seconds)
        }
        fn record_activity(
            &self,
            owner: &str,
            receipt: &RunLeaseReceipt,
        ) -> vyane_agent::Result<ClaimedRun> {
            self.inner.record_activity(owner, receipt)
        }
        fn bind_resume_session(
            &self,
            owner: &str,
            receipt: &RunLeaseReceipt,
            proof: &vyane_agent::ResumeSessionProof,
        ) -> vyane_agent::Result<ClaimedRun> {
            self.inner.bind_resume_session(owner, receipt, proof)
        }
        fn prepare_completion(
            &self,
            owner: &str,
            permit: &ActiveExecutionPermit,
            completion: &vyane_agent::NewRunCompletion,
        ) -> vyane_agent::Result<vyane_agent::PreparedRunCompletion> {
            self.inner.prepare_completion(owner, permit, completion)
        }
        fn validate_completion_permit(
            &self,
            owner: &str,
            permit: &vyane_agent::ActiveCompletionPermit,
        ) -> vyane_agent::Result<vyane_agent::CompletionPermitSnapshot> {
            self.inner.validate_completion_permit(owner, permit)
        }
        fn commit_completion(
            &self,
            owner: &str,
            permit: &vyane_agent::ActiveCompletionPermit,
        ) -> vyane_agent::Result<(
            vyane_agent::AgentRunRecord,
            vyane_agent::RunCompletionRecord,
        )> {
            self.inner.commit_completion(owner, permit)
        }
        fn get_completion(
            &self,
            owner: &str,
            run_id: &str,
        ) -> vyane_agent::Result<Option<vyane_agent::RunCompletionRecord>> {
            self.inner.get_completion(owner, run_id)
        }
        fn completion_for_recovery(
            &self,
            owner: &str,
            ticket: &vyane_agent::RecoveryTicket,
        ) -> vyane_agent::Result<Option<vyane_agent::RunCompletionRecord>> {
            self.inner.completion_for_recovery(owner, ticket)
        }
        fn commit_recovered_completion(
            &self,
            owner: &str,
            ticket: &vyane_agent::RecoveryTicket,
            completion_id: &str,
        ) -> vyane_agent::Result<(
            vyane_agent::AgentRunRecord,
            vyane_agent::RunCompletionRecord,
        )> {
            self.inner
                .commit_recovered_completion(owner, ticket, completion_id)
        }
        fn settle(
            &self,
            owner: &str,
            receipt: &RunLeaseReceipt,
            settlement: RunSettlement,
        ) -> vyane_agent::Result<vyane_agent::AgentRunRecord> {
            self.inner.settle(owner, receipt, settlement)
        }
        fn topology(
            &self,
            owner: &str,
            root_worker_id: &str,
        ) -> vyane_agent::Result<vyane_agent::WorkerTopology> {
            self.inner.topology(owner, root_worker_id)
        }
        fn request_cancel_tree(
            &self,
            owner: &str,
            root_worker_id: &str,
            request: &vyane_agent::CancelRequest,
        ) -> vyane_agent::Result<vyane_agent::CancelPlan> {
            self.inner
                .request_cancel_tree(owner, root_worker_id, request)
        }
        fn settle_cancel(
            &self,
            owner: &str,
            ticket: &vyane_agent::CancelTicket,
            outcome: vyane_agent::CancelOutcome,
        ) -> vyane_agent::Result<vyane_agent::AgentRunRecord> {
            self.inner.settle_cancel(owner, ticket, outcome)
        }
        fn claim_recovery_due(
            &self,
            owner: &str,
            reconciler: &str,
            lease_seconds: u64,
            limit: usize,
        ) -> vyane_agent::Result<Vec<vyane_agent::RecoveryTicket>> {
            self.inner
                .claim_recovery_due(owner, reconciler, lease_seconds, limit)
        }
        fn confirm_controller_gone(
            &self,
            owner: &str,
            ticket: &vyane_agent::RecoveryTicket,
        ) -> vyane_agent::Result<vyane_agent::AgentRunRecord> {
            self.inner.confirm_controller_gone(owner, ticket)
        }
        fn release_worker(
            &self,
            owner: &str,
            worker_id: &str,
            expected_revision: u64,
        ) -> vyane_agent::Result<vyane_agent::WorkerRecord> {
            self.inner
                .release_worker(owner, worker_id, expected_revision)
        }
        fn enqueue_resume(
            &self,
            owner: &str,
            request: &vyane_agent::EnqueueResume,
        ) -> vyane_agent::Result<vyane_agent::AgentRunRecord> {
            self.inner.enqueue_resume(owner, request)
        }
        fn unprojected_events(
            &self,
            owner: &str,
            projector: &str,
            limit: usize,
        ) -> vyane_agent::Result<vyane_agent::OutboxPage> {
            self.inner.unprojected_events(owner, projector, limit)
        }
        fn mark_projected(
            &self,
            owner: &str,
            projector: &str,
            event_id: &str,
        ) -> vyane_agent::Result<()> {
            self.inner.mark_projected(owner, projector, event_id)
        }
    }

    #[tokio::test]
    async fn malformed_foreign_claim_rejects_whole_batch_before_executor_effect() {
        let fixture = Fixture::new();
        fixture.enqueue("foreign", 30);
        let executor = Arc::new(TestExecutor::new(AgentExecutorOutcome::Unknown));
        let store: Arc<dyn AgentStore> = Arc::new(ForeignClaimStore {
            inner: fixture.store.clone(),
            attack: StoreAttack::ForeignClaim,
            claimed: Mutex::new(Vec::new()),
        });
        let driver =
            AgentRunExecutionDriver::new(OWNER, store, LEASE_OWNER, options(), executor.clone())
                .unwrap();
        let error = driver
            .execute_once(CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error, AgentExecutionError::InvalidStoreResult);
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
        assert!(executor.admissions.lock().unwrap().is_empty());
        let run = fixture
            .store
            .get_run(OWNER, "run-foreign")
            .unwrap()
            .unwrap();
        assert_eq!(run.state, RunState::Starting);
        assert!(run.controller.is_none());
    }

    #[tokio::test]
    async fn mismatched_backend_claim_rejects_before_controller_reservation() {
        let fixture = Fixture::new();
        fixture.enqueue("wrong-backend", 30);
        let executor = Arc::new(TestExecutor::new(AgentExecutorOutcome::Unknown));
        let store: Arc<dyn AgentStore> = Arc::new(ForeignClaimStore {
            inner: fixture.store.clone(),
            attack: StoreAttack::WrongBackend,
            claimed: Mutex::new(Vec::new()),
        });
        let driver =
            AgentRunExecutionDriver::new(OWNER, store, LEASE_OWNER, options(), executor.clone())
                .unwrap();
        let error = driver
            .execute_once(CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error, AgentExecutionError::InvalidStoreResult);
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
        assert!(executor.admissions.lock().unwrap().is_empty());
        let run = fixture
            .store
            .get_run(OWNER, "run-wrong-backend")
            .unwrap()
            .unwrap();
        assert_eq!(run.state, RunState::Starting);
        assert!(run.controller.is_none());
    }

    #[tokio::test]
    async fn swapped_start_result_never_reaches_executor_or_settlement() {
        let fixture = Fixture::new();
        fixture.enqueue("swap-start-a", 30);
        fixture.enqueue("swap-start-b", 30);
        let executor = Arc::new(TestExecutor::success(fixture.store.clone()));
        let store: Arc<dyn AgentStore> = Arc::new(ForeignClaimStore {
            inner: fixture.store.clone(),
            attack: StoreAttack::SwapStart,
            claimed: Mutex::new(Vec::new()),
        });
        let driver =
            AgentRunExecutionDriver::new(OWNER, store, LEASE_OWNER, options(), executor.clone())
                .unwrap();
        let report = driver.execute_once(CancellationToken::new()).await.unwrap();
        assert!(
            report
                .items
                .iter()
                .all(|status| *status == AgentExecutionItemStatus::StartFailed)
        );
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
        for id in ["run-swap-start-a", "run-swap-start-b"] {
            assert_ne!(
                fixture.store.get_run(OWNER, id).unwrap().unwrap().state,
                RunState::Succeeded
            );
        }
    }

    #[test]
    fn transition_validation_rejects_swapped_or_malformed_store_results() {
        let fixture = Fixture::new();
        fixture.enqueue("transition-a", 30);
        fixture.enqueue("transition-b", 30);
        let claims = fixture
            .store
            .claim_due(OWNER, ExecutionBackend::NativeInProcess, LEASE_OWNER, 10, 2)
            .unwrap();
        assert_eq!(claims.len(), 2);
        let controller_a = random_controller(ControllerKind::InProcess).unwrap();
        let controller_b = random_controller(ControllerKind::InProcess).unwrap();
        let started_a = fixture
            .store
            .start(OWNER, &claims[0].receipt, &controller_a)
            .unwrap();
        let started_b = fixture
            .store
            .start(OWNER, &claims[1].receipt, &controller_b)
            .unwrap();
        assert!(valid_start_transition(
            &claims[0],
            &started_a,
            OWNER,
            &controller_a
        ));
        assert!(!valid_start_transition(
            &claims[0],
            &started_b,
            OWNER,
            &controller_a
        ));

        let permit_b = fixture
            .store
            .issue_execution_permit(OWNER, &started_b.receipt, &started_b.run.policy_digest)
            .unwrap();
        assert!(!valid_permit(&permit_b, &started_a, OWNER));

        let heartbeat_a = fixture
            .store
            .heartbeat(OWNER, &started_a.receipt, 10)
            .unwrap();
        assert!(valid_heartbeat_transition(
            &started_a,
            &heartbeat_a,
            OWNER,
            &controller_a
        ));
        let mut swapped_heartbeat = heartbeat_a.clone();
        swapped_heartbeat.receipt.run_id = started_b.run.id.clone();
        assert!(!valid_heartbeat_transition(
            &started_a,
            &swapped_heartbeat,
            OWNER,
            &controller_a
        ));

        let permit_a = fixture
            .store
            .issue_execution_permit(OWNER, &heartbeat_a.receipt, &heartbeat_a.run.policy_digest)
            .unwrap();
        let prepared_a = fixture
            .store
            .prepare_completion(
                OWNER,
                &permit_a,
                &NewRunCompletion {
                    id: "completion-transition-a".into(),
                    sink_kind: "test-sink".into(),
                    publication_key: "result.transition-a".into(),
                    content_digest: "c".repeat(64),
                    content_bytes: 1,
                },
            )
            .unwrap();
        let (settled, _) = fixture
            .store
            .commit_completion(OWNER, &prepared_a.permit)
            .unwrap();
        assert!(valid_settle_transition(
            &heartbeat_a.run,
            &settled,
            RunState::Succeeded,
            None
        ));
        let mut malformed_settle = settled;
        malformed_settle.controller = Some(controller_a);
        assert!(!valid_settle_transition(
            &heartbeat_a.run,
            &malformed_settle,
            RunState::Succeeded,
            None
        ));
    }

    struct DelayedQuiesce {
        store: Arc<SqliteAgentStore>,
    }

    #[async_trait]
    impl AgentRunExecutor for DelayedQuiesce {
        fn kind(&self) -> ControllerKind {
            ControllerKind::InProcess
        }
        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            _controller: &ControllerRef,
        ) -> bool {
            true
        }
        async fn execute(
            &self,
            _context: AgentExecutionContext,
            identity: AgentExecutionIdentity,
            permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            tokio::time::sleep(Duration::from_millis(1_200)).await;
            staged_success(&self.store, &permit, identity.run_id())
        }
    }

    #[tokio::test]
    async fn heartbeat_updates_receipt_used_by_settlement() {
        let fixture = Fixture::new();
        fixture.enqueue("heartbeat", 30);
        let report = fixture
            .driver(
                Arc::new(DelayedQuiesce {
                    store: fixture.store.clone(),
                }),
                options(),
            )
            .execute_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.items, [AgentExecutionItemStatus::Settled]);
        let run = fixture
            .store
            .get_run(OWNER, "run-heartbeat")
            .unwrap()
            .unwrap();
        assert_eq!(run.state, RunState::Succeeded);
        assert!(run.revision >= 4);
    }

    struct ExpireLease {
        clock: Arc<FixedClock>,
    }

    #[async_trait]
    impl AgentRunExecutor for ExpireLease {
        fn kind(&self) -> ControllerKind {
            ControllerKind::InProcess
        }
        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            _controller: &ControllerRef,
        ) -> bool {
            true
        }
        async fn execute(
            &self,
            _context: AgentExecutionContext,
            _identity: AgentExecutionIdentity,
            _permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            self.clock.advance(chrono::TimeDelta::seconds(20));
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn heartbeat_failure_never_settles() {
        let fixture = Fixture::new();
        fixture.enqueue("heartbeat-fail", 30);
        let executor = Arc::new(ExpireLease {
            clock: fixture.clock.clone(),
        });
        let report = fixture
            .driver(executor, options())
            .execute_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.items, [AgentExecutionItemStatus::HeartbeatFailed]);
        assert_eq!(
            fixture
                .store
                .get_run(OWNER, "run-heartbeat-fail")
                .unwrap()
                .unwrap()
                .state,
            RunState::Running
        );
    }

    struct SlowReserve;

    #[async_trait]
    impl AgentRunExecutor for SlowReserve {
        fn kind(&self) -> ControllerKind {
            ControllerKind::InProcess
        }
        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            _controller: &ControllerRef,
        ) -> bool {
            true
        }
        fn reserve_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            _controller: &ControllerRef,
        ) -> bool {
            std::thread::sleep(Duration::from_millis(1_100));
            true
        }
        async fn execute(
            &self,
            _context: AgentExecutionContext,
            _identity: AgentExecutionIdentity,
            _permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            panic!("must not poll")
        }
    }

    #[tokio::test]
    async fn reservation_setup_consumes_monotonic_run_timeout() {
        let fixture = Fixture::new();
        fixture.enqueue("slow-reserve", 1);
        let report = fixture
            .driver(Arc::new(SlowReserve), options())
            .execute_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.items, [AgentExecutionItemStatus::TimedOut]);
        let run = fixture
            .store
            .get_run(OWNER, "run-slow-reserve")
            .unwrap()
            .unwrap();
        assert_eq!(run.state, RunState::Starting);
        assert!(run.controller.is_none());
    }

    #[tokio::test]
    async fn invalid_quiesced_failure_code_does_not_settle() {
        let fixture = Fixture::new();
        fixture.enqueue("bad-code", 30);
        let executor = Arc::new(TestExecutor::new(AgentExecutorOutcome::Quiesced(
            AgentExecutionSettlement::Failed {
                code: RunFailureCode::ControllerLost,
            },
        )));
        let report = fixture
            .driver(executor, options())
            .execute_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.items, [AgentExecutionItemStatus::SettlementFailed]);
        assert_eq!(
            fixture
                .store
                .get_run(OWNER, "run-bad-code")
                .unwrap()
                .unwrap()
                .state,
            RunState::Running
        );
    }

    struct RevalidatingExecutor {
        store: Arc<SqliteAgentStore>,
        clock: Arc<FixedClock>,
        effects: AtomicUsize,
    }

    #[async_trait]
    impl AgentRunExecutor for RevalidatingExecutor {
        fn kind(&self) -> ControllerKind {
            ControllerKind::InProcess
        }
        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            _controller: &ControllerRef,
        ) -> bool {
            true
        }
        async fn execute(
            &self,
            _context: AgentExecutionContext,
            _identity: AgentExecutionIdentity,
            permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            self.clock.advance(chrono::TimeDelta::seconds(20));
            if self
                .store
                .validate_execution_permit(OWNER, &permit, permit.policy_digest())
                .is_err()
            {
                return AgentExecutorOutcome::Unknown;
            }
            self.effects.fetch_add(1, Ordering::SeqCst);
            staged_success(&self.store, &permit, _identity.run_id())
        }
    }

    #[tokio::test]
    async fn revoked_permit_is_revalidated_before_first_effect() {
        let fixture = Fixture::new();
        fixture.enqueue("revoked", 30);
        let executor = Arc::new(RevalidatingExecutor {
            store: fixture.store.clone(),
            clock: fixture.clock.clone(),
            effects: AtomicUsize::new(0),
        });
        let report = fixture
            .driver(executor.clone(), options())
            .execute_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.items, [AgentExecutionItemStatus::ControllerUnknown]);
        assert_eq!(executor.effects.load(Ordering::SeqCst), 0);
        assert_eq!(
            fixture
                .store
                .get_run(OWNER, "run-revoked")
                .unwrap()
                .unwrap()
                .state,
            RunState::Running
        );
    }

    #[test]
    fn debug_and_errors_are_body_free() {
        let fixture = Fixture::new();
        let debug = format!("{:?}", fixture.driver(Arc::new(NeverExecutor), options()));
        assert_eq!(debug, "AgentRunExecutionDriver { .. }");
        assert!(
            !AgentExecutionError::ClaimStoreFailed
                .to_string()
                .contains("canary")
        );
    }
}
