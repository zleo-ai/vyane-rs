//! Bounded, owner-bound recovery of stale AgentRun controllers.
//!
//! This module is deliberately a one-shot service adapter. It neither starts a
//! resident loop nor executes an AgentRun. A caller assembles trusted
//! controller adapters, consumes the driver with [`AgentRunRecoveryDriver::recover_once`],
//! and decides if or when another pass should be constructed.

use std::collections::BTreeSet;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{FutureExt as _, StreamExt as _, stream};
use tokio::time::{Instant, timeout_at};
use vyane_agent::{
    AgentStore, ControllerKind, ControllerRef, RecoveryReason, RecoveryTicket, RunCompletionRecord,
};
use vyane_core::CancellationToken;

use crate::{AgentCompletionSink, AgentCompletionSinkObservation};

/// Maximum number of recovery tickets claimed in one pass.
pub const MAX_RECOVERY_BATCH: usize = 64;
/// Maximum controller calls concurrently polled by one pass.
pub const MAX_RECOVERY_CONCURRENCY: usize = 16;
/// Hard upper bound for one controller-adapter call.
pub const MAX_CONTROLLER_TIMEOUT: Duration = Duration::from_secs(60);
/// Hard upper bound for the configured minimum lease window reserved before an
/// adapter starts. Blocking settlement itself cannot be forcibly time-limited.
pub const MAX_SETTLEMENT_MARGIN: Duration = Duration::from_secs(60);
/// Hard upper bound accepted for a durable recovery-operation lease.
pub const MAX_RECOVERY_LEASE_SECONDS: u64 = 5 * 60;

const MAX_OWNER_BYTES: usize = 256;
const MAX_IDENTITY_BYTES: usize = 64;
const MAX_REFERENCE_BYTES: usize = 512;

/// Trusted controller-specific proof boundary.
///
/// Implementations must return [`ControllerRecoveryObservation::Gone`] only
/// when the *exact* supplied controller no longer exists, or after they have
/// synchronously requested that exact controller to stop and observed its
/// exit. Merely sending a stop request, losing connectivity, failing a probe,
/// or timing out is not proof. Adapters receive no recovery ticket, bearer
/// token, owner, store handle, or settlement authority. Because a process panic
/// hook runs before the driver can catch an unwind, implementations must keep
/// panic payloads free of controller identities, credentials, request or
/// response bodies, and other secrets.
///
/// Every external control effect must immediately revalidate the complete
/// supplied identity, including its fingerprint when present. If the
/// [`ControllerRef`] cannot exclude identity reuse, the adapter must return
/// [`ControllerRecoveryObservation::Unavailable`] without attempting an
/// effect. Stop/observe behavior must be safely repeatable or reconcilable after
/// an adapter timeout, caller drop, or durable settlement failure, because any
/// of those can cause a later pass to present the same exact controller again.
/// Implementations must not start unowned detached control work. If a platform
/// requires a non-abortable blocking operation, that operation may continue
/// after the adapter future times out or is dropped; it must therefore be
/// independently bounded, exact-identity-safe, and retry-safe. The driver's
/// future timeout bounds polling only and is never proof that an external
/// effect stopped.
#[async_trait]
pub trait AgentControllerAdapter: Send + Sync {
    /// Stable, non-secret adapter identity. The driver freezes and validates it
    /// during construction and never calls it during recovery.
    fn name(&self) -> &str;

    /// The one durable controller kind this adapter can prove.
    fn kind(&self) -> ControllerKind;

    /// Inspect or synchronously stop the exact controller, within `context`'s
    /// deadline, and return a body-free observation.
    async fn observe_gone(
        &self,
        context: ControllerRecoveryContext,
        controller: ControllerRef,
    ) -> ControllerRecoveryObservation;

    /// Release adapter-local observation state after durable confirmation.
    ///
    /// This callback has no settlement authority and its default is a no-op.
    /// Implementations must keep it bounded, non-blocking, idempotent and
    /// body-free. It is never called when confirmation fails or is uncertain.
    fn confirmed_gone(&self, _controller: &ControllerRef) {}
}

/// Bounded call context supplied to a controller adapter.
pub struct ControllerRecoveryContext {
    deadline: Instant,
}

impl ControllerRecoveryContext {
    /// Monotonic deadline also enforced by the driver.
    #[must_use]
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
}

impl fmt::Debug for ControllerRecoveryContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControllerRecoveryContext")
            .finish_non_exhaustive()
    }
}

/// A trusted adapter's body-free result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerRecoveryObservation {
    /// The exact controller is absent, or was stopped and its exit observed.
    Gone,
    /// The exact controller is still present.
    StillPresent,
    /// Its state could not be proved. This never authorizes settlement.
    Unavailable,
}

/// Hard bounds for one recovery pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRecoveryOptions {
    pub batch_limit: usize,
    pub max_in_flight: usize,
    pub adapter_timeout: Duration,
    pub settlement_margin: Duration,
    pub operation_lease_seconds: u64,
}

impl Default for AgentRecoveryOptions {
    fn default() -> Self {
        Self {
            batch_limit: 32,
            max_in_flight: 8,
            adapter_timeout: Duration::from_secs(20),
            settlement_margin: Duration::from_secs(5),
            operation_lease_seconds: 30,
        }
    }
}

/// Static construction or claim failure. No store diagnostic or identifier is
/// retained in this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentRecoveryError {
    InvalidOwner,
    InvalidReconciler,
    InvalidOptions,
    InvalidAdapter,
    DuplicateAdapterKind,
    DuplicateAdapterName,
    AdapterMetadataPanicked,
    InvalidCompletionSink,
    DuplicateCompletionSinkKind,
    CompletionSinkMetadataPanicked,
    RuntimeUnavailable,
    ClaimTaskFailed,
    ClaimStoreFailed,
    InvalidStoreResult,
}

impl fmt::Display for AgentRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidOwner => "recovery owner is invalid",
            Self::InvalidReconciler => "recovery reconciler identity is invalid",
            Self::InvalidOptions => "recovery options are invalid",
            Self::InvalidAdapter => "controller adapter metadata is invalid",
            Self::DuplicateAdapterKind => "controller adapter kind is duplicated",
            Self::DuplicateAdapterName => "controller adapter name is duplicated",
            Self::AdapterMetadataPanicked => "controller adapter metadata panicked",
            Self::InvalidCompletionSink => "completion sink metadata is invalid",
            Self::DuplicateCompletionSinkKind => "completion sink kind is duplicated",
            Self::CompletionSinkMetadataPanicked => "completion sink metadata panicked",
            Self::RuntimeUnavailable => "recovery requires a Tokio runtime",
            Self::ClaimTaskFailed => "recovery claim task failed",
            Self::ClaimStoreFailed => "recovery claim store operation failed",
            Self::InvalidStoreResult => "recovery store returned an invalid result",
        })
    }
}

impl std::error::Error for AgentRecoveryError {}

/// Body-free outcome for one claimed ticket. Item ordering is completion order
/// and deliberately carries no run, worker, controller, operation, or token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentRecoveryItemStatus {
    RecoveredWithoutController,
    RecoveredAfterControllerGone,
    ControllerStillPresent,
    ControllerUnavailable,
    MissingAdapter,
    InvalidController,
    /// The caller-local operation-lease window cannot still fit one adapter
    /// timeout plus the configured settlement margin.
    InsufficientLeaseWindow,
    CancelledBeforeAdapter,
    AdapterTimedOut,
    AdapterPanicked,
    SettlementFailed,
    CompletionRecovered,
    CompletionAbsent,
    CompletionConflict,
    CompletionUnavailable,
}

/// Bounded, body-free report for one pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRecoveryReport {
    pub claimed: usize,
    pub items: Vec<AgentRecoveryItemStatus>,
    /// True only when cancellation was already visible before the claim began.
    pub cancelled_before_claim: bool,
}

struct RegisteredAdapter {
    kind: ControllerKind,
    _name: String,
    adapter: Arc<dyn AgentControllerAdapter>,
}

struct RegisteredCompletionSink {
    kind: String,
    sink: Arc<dyn AgentCompletionSink>,
}

#[derive(Clone, Copy)]
struct RecoveryWindow {
    adapter_timeout: Duration,
    settlement_margin: Duration,
    required_window: Duration,
    pass_deadline: Instant,
}

/// Owner-bound one-shot stale-controller recovery driver.
///
/// The raw store and recovery tickets remain encapsulated. This type is not
/// `Clone`, and [`Self::recover_once`] consumes it. Cancellation observed
/// before the durable claim prevents all mutation. Once claiming starts,
/// already-started blocking claim/settlement work is awaited and controller
/// calls already being polled are allowed to reach their configured timeout;
/// cancellation only suppresses buffered adapter calls that have not started.
/// Dropping the recovery future is not graceful: Tokio cannot abort a running
/// blocking call, a custom store may block indefinitely, and an adapter future
/// may be dropped at an uncertain point. A malformed post-claim store result
/// fails closed without adapter calls; its persisted claims remain fenced until
/// their operation leases expire. The durable lease is the retry fence for a
/// later, freshly constructed pass.
pub struct AgentRunRecoveryDriver {
    owner: String,
    store: Arc<dyn AgentStore>,
    reconciler: String,
    options: AgentRecoveryOptions,
    adapters: Vec<RegisteredAdapter>,
    completion_sinks: Arc<Vec<RegisteredCompletionSink>>,
}

impl AgentRunRecoveryDriver {
    pub(crate) fn registered_adapter_kinds(&self) -> Vec<ControllerKind> {
        self.adapters.iter().map(|adapter| adapter.kind).collect()
    }

    /// Freeze an owner, non-null trait-object store, bounds, and at most one
    /// trusted adapter per controller kind. `Arc<dyn AgentStore>` is non-null by
    /// construction; the driver intentionally performs no probing store call
    /// here, so rejected configuration cannot mutate durable state. Injected
    /// stores remain trusted to honor the [`AgentStore`] limit and keep panic
    /// payloads body-free because panic hooks run before a blocking-task join
    /// is mapped. Adapter admission uses a caller-local monotonic deadline;
    /// ticket wall-clock expiry remains solely a final store settlement fence.
    pub fn new(
        owner: impl Into<String>,
        store: Arc<dyn AgentStore>,
        reconciler: impl Into<String>,
        options: AgentRecoveryOptions,
        adapters: Vec<Arc<dyn AgentControllerAdapter>>,
    ) -> Result<Self, AgentRecoveryError> {
        Self::new_with_completion_sinks(owner, store, reconciler, options, adapters, Vec::new())
    }

    pub fn new_with_completion_sinks(
        owner: impl Into<String>,
        store: Arc<dyn AgentStore>,
        reconciler: impl Into<String>,
        options: AgentRecoveryOptions,
        adapters: Vec<Arc<dyn AgentControllerAdapter>>,
        completion_sinks: Vec<Arc<dyn AgentCompletionSink>>,
    ) -> Result<Self, AgentRecoveryError> {
        let owner = owner.into();
        let reconciler = reconciler.into();
        validate_canonical_text(&owner, MAX_OWNER_BYTES)
            .map_err(|()| AgentRecoveryError::InvalidOwner)?;
        validate_identity(&reconciler).map_err(|()| AgentRecoveryError::InvalidReconciler)?;
        validate_options(&options)?;
        if adapters.len() > 3 {
            return Err(AgentRecoveryError::DuplicateAdapterKind);
        }

        let mut registered = Vec::with_capacity(adapters.len());
        let mut names = BTreeSet::new();
        for adapter in adapters {
            let first = catch_unwind(AssertUnwindSafe(|| {
                (adapter.name().to_owned(), adapter.kind())
            }))
            .map_err(|_| AgentRecoveryError::AdapterMetadataPanicked)?;
            let second = catch_unwind(AssertUnwindSafe(|| {
                (adapter.name().to_owned(), adapter.kind())
            }))
            .map_err(|_| AgentRecoveryError::AdapterMetadataPanicked)?;
            if first != second || validate_identity(&first.0).is_err() {
                return Err(AgentRecoveryError::InvalidAdapter);
            }
            if registered
                .iter()
                .any(|existing: &RegisteredAdapter| existing.kind == first.1)
            {
                return Err(AgentRecoveryError::DuplicateAdapterKind);
            }
            if !names.insert(first.0.clone()) {
                return Err(AgentRecoveryError::DuplicateAdapterName);
            }
            registered.push(RegisteredAdapter {
                kind: first.1,
                _name: first.0,
                adapter,
            });
        }

        if completion_sinks.len() > 16 {
            return Err(AgentRecoveryError::InvalidCompletionSink);
        }
        let mut registered_sinks = Vec::with_capacity(completion_sinks.len());
        let mut sink_kinds = BTreeSet::new();
        for sink in completion_sinks {
            let first = catch_unwind(AssertUnwindSafe(|| sink.kind().to_owned()))
                .map_err(|_| AgentRecoveryError::CompletionSinkMetadataPanicked)?;
            let second = catch_unwind(AssertUnwindSafe(|| sink.kind().to_owned()))
                .map_err(|_| AgentRecoveryError::CompletionSinkMetadataPanicked)?;
            if first != second || validate_identity(&first).is_err() {
                return Err(AgentRecoveryError::InvalidCompletionSink);
            }
            if !sink_kinds.insert(first.clone()) {
                return Err(AgentRecoveryError::DuplicateCompletionSinkKind);
            }
            registered_sinks.push(RegisteredCompletionSink { kind: first, sink });
        }

        Ok(Self {
            owner,
            store,
            reconciler,
            options,
            adapters: registered,
            completion_sinks: Arc::new(registered_sinks),
        })
    }

    /// Claim and reconcile one bounded batch.
    pub async fn recover_once(
        self,
        cancellation: CancellationToken,
    ) -> Result<AgentRecoveryReport, AgentRecoveryError> {
        if cancellation.is_cancelled() {
            return Ok(AgentRecoveryReport {
                claimed: 0,
                items: Vec::new(),
                cancelled_before_claim: true,
            });
        }
        tokio::runtime::Handle::try_current()
            .map_err(|_| AgentRecoveryError::RuntimeUnavailable)?;

        let store = Arc::clone(&self.store);
        let owner = self.owner.clone();
        let reconciler = self.reconciler.clone();
        let lease_seconds = self.options.operation_lease_seconds;
        let limit = self.options.batch_limit;
        // Start the conservative local lease window before entering the
        // blocking pool. The durable claim can only begin later, so this
        // monotonic deadline never grants more controller-call time than the
        // configured operation lease even when a custom store's wall clock is
        // offset from the caller's clock.
        let pass_deadline = Instant::now()
            .checked_add(Duration::from_secs(lease_seconds))
            .ok_or(AgentRecoveryError::InvalidOptions)?;
        let tickets = tokio::task::spawn_blocking(move || {
            store.claim_recovery_due(&owner, &reconciler, lease_seconds, limit)
        })
        .await
        .map_err(|_| AgentRecoveryError::ClaimTaskFailed)?
        .map_err(|_| AgentRecoveryError::ClaimStoreFailed)?;
        validate_claimed_tickets(&tickets, &self.reconciler, self.options.batch_limit)?;

        let claimed = tickets.len();
        let owner = self.owner;
        let store = self.store;
        let timeout_window = self.options.adapter_timeout;
        let required_window = timeout_window
            .checked_add(self.options.settlement_margin)
            .ok_or(AgentRecoveryError::InvalidOptions)?;
        let window = RecoveryWindow {
            adapter_timeout: timeout_window,
            settlement_margin: self.options.settlement_margin,
            required_window,
            pass_deadline,
        };
        let max_in_flight = self.options.max_in_flight;
        let adapters = self.adapters;
        let completion_sinks = self.completion_sinks;

        let items = stream::iter(tickets.into_iter().map(|ticket| {
            let store = Arc::clone(&store);
            let owner = owner.clone();
            let cancellation = cancellation.clone();
            let adapter = ticket.controller.as_ref().and_then(|controller| {
                adapters
                    .iter()
                    .find(|entry| entry.kind == controller.kind)
                    .map(|entry| Arc::clone(&entry.adapter))
            });
            let completion_sinks = Arc::clone(&completion_sinks);
            async move {
                recover_one(
                    owner,
                    store,
                    adapter,
                    completion_sinks,
                    ticket,
                    cancellation,
                    window,
                )
                .await
            }
        }))
        .buffer_unordered(max_in_flight)
        .collect::<Vec<_>>()
        .await;

        Ok(AgentRecoveryReport {
            claimed,
            items,
            cancelled_before_claim: false,
        })
    }
}

impl fmt::Debug for AgentRunRecoveryDriver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentRunRecoveryDriver")
            .finish_non_exhaustive()
    }
}

fn validate_options(options: &AgentRecoveryOptions) -> Result<(), AgentRecoveryError> {
    if !(1..=MAX_RECOVERY_BATCH).contains(&options.batch_limit)
        || !(1..=MAX_RECOVERY_CONCURRENCY).contains(&options.max_in_flight)
        || options.max_in_flight > options.batch_limit
        || options.adapter_timeout.is_zero()
        || options.adapter_timeout > MAX_CONTROLLER_TIMEOUT
        || options.settlement_margin.is_zero()
        || options.settlement_margin > MAX_SETTLEMENT_MARGIN
        || !(1..=MAX_RECOVERY_LEASE_SECONDS).contains(&options.operation_lease_seconds)
    {
        return Err(AgentRecoveryError::InvalidOptions);
    }
    let required = options
        .adapter_timeout
        .checked_add(options.settlement_margin)
        .ok_or(AgentRecoveryError::InvalidOptions)?;
    if Duration::from_secs(options.operation_lease_seconds) <= required {
        return Err(AgentRecoveryError::InvalidOptions);
    }
    Ok(())
}

fn validate_claimed_tickets(
    tickets: &[RecoveryTicket],
    reconciler: &str,
    limit: usize,
) -> Result<(), AgentRecoveryError> {
    if tickets.len() > limit {
        return Err(AgentRecoveryError::InvalidStoreResult);
    }
    let mut operations = BTreeSet::new();
    let mut runs = BTreeSet::new();
    for ticket in tickets {
        if !validate_ticket(ticket, reconciler)
            || !operations.insert(ticket.operation_id.as_str())
            || !runs.insert(ticket.run_id.as_str())
        {
            return Err(AgentRecoveryError::InvalidStoreResult);
        }
    }
    Ok(())
}

fn validate_ticket(ticket: &RecoveryTicket, reconciler: &str) -> bool {
    validate_canonical_text(&ticket.operation_id, MAX_OWNER_BYTES).is_ok()
        && validate_canonical_text(&ticket.worker_id, MAX_OWNER_BYTES).is_ok()
        && validate_canonical_text(&ticket.run_id, MAX_OWNER_BYTES).is_ok()
        && ticket.generation > 0
        && ticket.lease_owner == reconciler
        && ticket.token.len() == 64
        && ticket
            .token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        && ticket.controller.as_ref().is_none_or(validate_controller)
}

fn validate_controller(controller: &ControllerRef) -> bool {
    validate_canonical_text(&controller.id, MAX_REFERENCE_BYTES).is_ok()
        && controller
            .fingerprint
            .as_deref()
            .is_none_or(|value| validate_canonical_text(value, MAX_REFERENCE_BYTES).is_ok())
}

fn validate_canonical_text(value: &str, max: usize) -> Result<(), ()> {
    if value.is_empty()
        || value.len() > max
        || value.contains('\0')
        || value.trim() != value
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

fn adapter_deadline(window: RecoveryWindow, call_started: Instant) -> Option<Instant> {
    let remaining = window.pass_deadline.checked_duration_since(call_started)?;
    if remaining <= window.required_window {
        return None;
    }
    let deadline = call_started.checked_add(window.adapter_timeout)?;
    (deadline < window.pass_deadline).then_some(deadline)
}

async fn recover_one(
    owner: String,
    store: Arc<dyn AgentStore>,
    adapter: Option<Arc<dyn AgentControllerAdapter>>,
    completion_sinks: Arc<Vec<RegisteredCompletionSink>>,
    ticket: RecoveryTicket,
    cancellation: CancellationToken,
    window: RecoveryWindow,
) -> AgentRecoveryItemStatus {
    let Some(controller) = ticket.controller.clone() else {
        return reconcile_completion_after_gone(
            owner,
            store,
            ticket,
            completion_sinks,
            AgentRecoveryItemStatus::RecoveredWithoutController,
            None,
            window,
        )
        .await;
    };
    if !validate_controller(&controller) {
        return AgentRecoveryItemStatus::InvalidController;
    }
    let Some(adapter) = adapter else {
        return AgentRecoveryItemStatus::MissingAdapter;
    };
    if cancellation.is_cancelled() {
        return AgentRecoveryItemStatus::CancelledBeforeAdapter;
    }
    let call_started = Instant::now();
    let Some(deadline) = adapter_deadline(window, call_started) else {
        return AgentRecoveryItemStatus::InsufficientLeaseWindow;
    };

    let observing_adapter = Arc::clone(&adapter);
    let created = catch_unwind(AssertUnwindSafe(|| {
        observing_adapter.observe_gone(ControllerRecoveryContext { deadline }, controller.clone())
    }));
    let observation = match created {
        Err(_) => return AgentRecoveryItemStatus::AdapterPanicked,
        Ok(call) => match timeout_at(deadline, AssertUnwindSafe(call).catch_unwind()).await {
            Err(_) => return AgentRecoveryItemStatus::AdapterTimedOut,
            Ok(Err(_)) => return AgentRecoveryItemStatus::AdapterPanicked,
            Ok(Ok(observation)) => observation,
        },
    };
    match observation {
        ControllerRecoveryObservation::Gone => {
            reconcile_completion_after_gone(
                owner,
                store,
                ticket,
                completion_sinks,
                AgentRecoveryItemStatus::RecoveredAfterControllerGone,
                Some((adapter, controller)),
                window,
            )
            .await
        }
        ControllerRecoveryObservation::StillPresent => {
            AgentRecoveryItemStatus::ControllerStillPresent
        }
        ControllerRecoveryObservation::Unavailable => {
            AgentRecoveryItemStatus::ControllerUnavailable
        }
    }
}

async fn reconcile_completion_after_gone(
    owner: String,
    store: Arc<dyn AgentStore>,
    ticket: RecoveryTicket,
    completion_sinks: Arc<Vec<RegisteredCompletionSink>>,
    ordinary_success: AgentRecoveryItemStatus,
    confirmation: Option<(Arc<dyn AgentControllerAdapter>, ControllerRef)>,
    window: RecoveryWindow,
) -> AgentRecoveryItemStatus {
    if ticket.reason != RecoveryReason::LeaseExpired {
        return settle(owner, store, ticket, ordinary_success, confirmation).await;
    }
    let lookup_store = Arc::clone(&store);
    let lookup_owner = owner.clone();
    let lookup_ticket = ticket.clone();
    let completion = match tokio::task::spawn_blocking(move || {
        lookup_store.completion_for_recovery(&lookup_owner, &lookup_ticket)
    })
    .await
    {
        Ok(Ok(value)) => value,
        Ok(Err(_)) | Err(_) => return AgentRecoveryItemStatus::SettlementFailed,
    };
    let Some(completion) = completion else {
        return settle(owner, store, ticket, ordinary_success, confirmation).await;
    };
    if !completion_matches_ticket(&owner, &completion, &ticket) {
        return AgentRecoveryItemStatus::CompletionConflict;
    }
    let Some(sink) = completion_sinks
        .iter()
        .find(|entry| entry.kind == completion.sink_kind)
        .map(|entry| Arc::clone(&entry.sink))
    else {
        return AgentRecoveryItemStatus::CompletionUnavailable;
    };
    let Some(sink_deadline) = window.pass_deadline.checked_sub(window.settlement_margin) else {
        return AgentRecoveryItemStatus::CompletionUnavailable;
    };
    if Instant::now() >= sink_deadline {
        return AgentRecoveryItemStatus::CompletionUnavailable;
    }
    let inspection = match catch_unwind(AssertUnwindSafe(|| sink.inspect(completion.clone()))) {
        Ok(inspection) => inspection,
        Err(_) => return AgentRecoveryItemStatus::CompletionUnavailable,
    };
    let observation =
        match timeout_at(sink_deadline, AssertUnwindSafe(inspection).catch_unwind()).await {
            Ok(Ok(observation)) => observation,
            Ok(Err(_)) | Err(_) => AgentCompletionSinkObservation::Unavailable,
        };
    match observation {
        AgentCompletionSinkObservation::Exact => {
            commit_recovered(owner, store, ticket, completion, confirmation).await
        }
        AgentCompletionSinkObservation::Absent => {
            let settled = settle(owner, store, ticket, ordinary_success, confirmation).await;
            if matches!(settled, AgentRecoveryItemStatus::SettlementFailed) {
                settled
            } else {
                AgentRecoveryItemStatus::CompletionAbsent
            }
        }
        AgentCompletionSinkObservation::Conflict => AgentRecoveryItemStatus::CompletionConflict,
        AgentCompletionSinkObservation::Unavailable => {
            AgentRecoveryItemStatus::CompletionUnavailable
        }
    }
}

fn completion_matches_ticket(
    owner: &str,
    completion: &RunCompletionRecord,
    ticket: &RecoveryTicket,
) -> bool {
    completion.owner == owner
        && completion.run_id == ticket.run_id
        && completion.worker_id == ticket.worker_id
        && completion.worker_generation == ticket.generation
        && completion.status == vyane_agent::RunCompletionStatus::Prepared
        && completion.prepared_run_revision <= ticket.revision
        && completion.committed_at.is_none()
        && completion.committed_run_revision.is_none()
        && completion.abandoned_at.is_none()
        && completion.abandoned_run_revision.is_none()
        && completion.committed_by_operation_id.is_none()
        && completion.revision == 0
}

async fn commit_recovered(
    owner: String,
    store: Arc<dyn AgentStore>,
    ticket: RecoveryTicket,
    completion: RunCompletionRecord,
    confirmation: Option<(Arc<dyn AgentControllerAdapter>, ControllerRef)>,
) -> AgentRecoveryItemStatus {
    let expected_owner = completion.owner.clone();
    let expected_run_id = completion.run_id.clone();
    let expected_worker_id = completion.worker_id.clone();
    let expected_generation = completion.worker_generation;
    let completion_id = completion.completion_id.clone();
    let expected_operation_id = ticket.operation_id.clone();
    let task = tokio::task::spawn_blocking(move || {
        match store.commit_recovered_completion(&owner, &ticket, &completion_id) {
            Ok((run, committed))
                if run.owner == expected_owner
                    && run.id == expected_run_id
                    && run.worker_id == expected_worker_id
                    && run.worker_generation == expected_generation
                    && run.state == vyane_agent::RunState::Succeeded
                    && run.failure_code.is_none()
                    && run.finished_at.is_some()
                    && run.controller.is_none()
                    && run.lease.is_none()
                    && committed.owner == run.owner
                    && committed.run_id == run.id
                    && committed.worker_id == run.worker_id
                    && committed.worker_generation == run.worker_generation
                    && committed.completion_id == completion.completion_id
                    && committed.sink_kind == completion.sink_kind
                    && committed.publication_key == completion.publication_key
                    && committed.content_digest == completion.content_digest
                    && committed.content_bytes == completion.content_bytes
                    && committed.status == vyane_agent::RunCompletionStatus::Committed
                    && committed.committed_at == run.finished_at
                    && committed.committed_run_revision == Some(run.revision)
                    && committed.abandoned_at.is_none()
                    && committed.abandoned_run_revision.is_none()
                    && committed.committed_by_operation_id.as_deref()
                        == Some(expected_operation_id.as_str()) =>
            {
                // Keep durable settlement and adapter tombstone release in the
                // same non-cancellable blocking task. Dropping the async
                // waiter cannot strand a retired exact controller after the
                // recovery ticket has already been consumed.
                if let Some((adapter, controller)) = confirmation {
                    let _ = catch_unwind(AssertUnwindSafe(|| adapter.confirmed_gone(&controller)));
                }
                AgentRecoveryItemStatus::CompletionRecovered
            }
            Ok(_) | Err(_) => AgentRecoveryItemStatus::SettlementFailed,
        }
    });
    match task.await {
        Ok(status) => status,
        Err(_) => AgentRecoveryItemStatus::SettlementFailed,
    }
}

async fn settle(
    owner: String,
    store: Arc<dyn AgentStore>,
    ticket: RecoveryTicket,
    success: AgentRecoveryItemStatus,
    confirmation: Option<(Arc<dyn AgentControllerAdapter>, ControllerRef)>,
) -> AgentRecoveryItemStatus {
    let expected_owner = owner.clone();
    let expected_run_id = ticket.run_id.clone();
    let expected_worker_id = ticket.worker_id.clone();
    let expected_generation = ticket.generation;
    let task =
        tokio::task::spawn_blocking(
            move || match store.confirm_controller_gone(&owner, &ticket) {
                Ok(run)
                    if run.owner == expected_owner
                        && run.id == expected_run_id
                        && run.worker_id == expected_worker_id
                        && run.worker_generation == expected_generation
                        && run.finished_at.is_some()
                        && run.controller.is_none()
                        && run.lease.is_none() =>
                {
                    if let Some((adapter, controller)) = confirmation {
                        let _ =
                            catch_unwind(AssertUnwindSafe(|| adapter.confirmed_gone(&controller)));
                    }
                    success
                }
                Ok(_) | Err(_) => AgentRecoveryItemStatus::SettlementFailed,
            },
        );
    match task.await {
        Ok(status) => status,
        Err(_) => AgentRecoveryItemStatus::SettlementFailed,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};

    use tokio::sync::Notify;

    use chrono::{DateTime, TimeDelta, Utc};
    use static_assertions::{assert_impl_all, assert_not_impl_any};
    use vyane_agent::{
        AgentClock, NewAgentRun, NewRunCompletion, NewWorker, RunCompletionStatus, RunMode,
        RunState, SqliteAgentStore,
    };

    use super::*;

    assert_impl_all!(AgentRunRecoveryDriver: Send, Sync);
    assert_not_impl_any!(AgentRunRecoveryDriver: Clone, serde::Serialize, serde::de::DeserializeOwned);
    assert_not_impl_any!(ControllerRecoveryContext: Clone, serde::Serialize, serde::de::DeserializeOwned);

    #[derive(Debug)]
    struct TestClock(Mutex<DateTime<Utc>>);

    impl TestClock {
        fn at(now: DateTime<Utc>) -> Self {
            Self(Mutex::new(now))
        }

        fn advance(&self, seconds: i64) {
            let mut now = self.0.lock().unwrap();
            *now = now.checked_add_signed(TimeDelta::seconds(seconds)).unwrap();
        }
    }

    impl AgentClock for TestClock {
        fn now(&self) -> DateTime<Utc> {
            *self.0.lock().unwrap()
        }
    }

    #[derive(Debug)]
    struct BlockingSettlementClock {
        now: Mutex<DateTime<Utc>>,
        block_next: AtomicBool,
        entered: Notify,
        released: Mutex<bool>,
        changed: Condvar,
    }

    impl BlockingSettlementClock {
        fn new(now: DateTime<Utc>) -> Self {
            Self {
                now: Mutex::new(now),
                block_next: AtomicBool::new(false),
                entered: Notify::new(),
                released: Mutex::new(false),
                changed: Condvar::new(),
            }
        }

        fn advance(&self, seconds: i64) {
            *self.now.lock().unwrap() += TimeDelta::seconds(seconds);
        }

        fn arm(&self) {
            *self.released.lock().unwrap() = false;
            self.block_next.store(true, Ordering::SeqCst);
        }

        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.changed.notify_all();
        }
    }

    impl AgentClock for BlockingSettlementClock {
        fn now(&self) -> DateTime<Utc> {
            if self.block_next.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                let mut released = self.released.lock().unwrap();
                while !*released {
                    released = self.changed.wait(released).unwrap();
                }
            }
            *self.now.lock().unwrap()
        }
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        clock: Arc<TestClock>,
        store: Arc<SqliteAgentStore>,
    }

    impl Fixture {
        fn new() -> Self {
            Self::at(Utc::now())
        }

        fn at(now: DateTime<Utc>) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let clock = Arc::new(TestClock::at(now));
            let store = Arc::new(
                SqliteAgentStore::open_with_clock(
                    directory.path().join("agent.sqlite"),
                    clock.clone(),
                )
                .unwrap(),
            );
            Self {
                _directory: directory,
                clock,
                store,
            }
        }

        fn create_claimed(&self, owner: &str, suffix: &str, controller: Option<ControllerRef>) {
            let worker_id = format!("worker-{suffix}");
            let run_id = format!("run-{suffix}");
            let execution_backend = controller.as_ref().map_or(
                vyane_agent::ExecutionBackend::NativeInProcess,
                |controller| vyane_agent::ExecutionBackend::for_controller_kind(controller.kind),
            );
            self.store
                .create_root(
                    owner,
                    &NewWorker {
                        id: worker_id.clone(),
                        logical_session_id: None,
                    },
                    &NewAgentRun {
                        id: run_id.clone(),
                        worker_id,
                        task_id: None,
                        trace_id: None,
                        parent_run_id: None,
                        execution_backend,
                        mode: RunMode::Autonomous,
                        target_key: "provider/model".into(),
                        prompt_digest: digest('a'),
                        policy_digest: digest('b'),
                        available_at: self.clock.now(),
                        timeout_seconds: 600,
                        max_resume_attempts: 0,
                    },
                )
                .unwrap();
            let claimed = self
                .store
                .claim_due(owner, execution_backend, "run-supervisor", 1, 1)
                .unwrap()
                .remove(0);
            if let Some(controller) = controller {
                self.store
                    .start(owner, &claimed.receipt, &controller)
                    .unwrap();
            }
        }

        fn make_due(&self) {
            self.clock.advance(2);
        }

        fn state(&self, owner: &str, suffix: &str) -> RunState {
            self.store
                .get_run(owner, &format!("run-{suffix}"))
                .unwrap()
                .unwrap()
                .state
        }

        fn driver(
            &self,
            owner: &str,
            options: AgentRecoveryOptions,
            adapters: Vec<Arc<dyn AgentControllerAdapter>>,
        ) -> Result<AgentRunRecoveryDriver, AgentRecoveryError> {
            let store: Arc<dyn AgentStore> = self.store.clone();
            AgentRunRecoveryDriver::new(owner, store, "recovery-test", options, adapters)
        }

        fn driver_with_sinks(
            &self,
            owner: &str,
            options: AgentRecoveryOptions,
            adapters: Vec<Arc<dyn AgentControllerAdapter>>,
            sinks: Vec<Arc<dyn AgentCompletionSink>>,
        ) -> Result<AgentRunRecoveryDriver, AgentRecoveryError> {
            let store: Arc<dyn AgentStore> = self.store.clone();
            AgentRunRecoveryDriver::new_with_completion_sinks(
                owner,
                store,
                "recovery-test",
                options,
                adapters,
                sinks,
            )
        }

        fn create_prepared(&self, owner: &str, suffix: &str) -> ControllerRef {
            let worker_id = format!("worker-{suffix}");
            let run_id = format!("run-{suffix}");
            self.store
                .create_root(
                    owner,
                    &NewWorker {
                        id: worker_id.clone(),
                        logical_session_id: None,
                    },
                    &NewAgentRun {
                        id: run_id.clone(),
                        worker_id,
                        task_id: None,
                        trace_id: None,
                        parent_run_id: None,
                        execution_backend: vyane_agent::ExecutionBackend::NativeInProcess,
                        mode: RunMode::Autonomous,
                        target_key: "provider/model".into(),
                        prompt_digest: digest('a'),
                        policy_digest: digest('b'),
                        available_at: self.clock.now(),
                        timeout_seconds: 600,
                        max_resume_attempts: 0,
                    },
                )
                .unwrap();
            let claimed = self
                .store
                .claim_due(
                    owner,
                    vyane_agent::ExecutionBackend::NativeInProcess,
                    "run-supervisor",
                    1,
                    1,
                )
                .unwrap()
                .remove(0);
            let controller = ControllerRef {
                kind: ControllerKind::InProcess,
                id: format!("controller-{suffix}"),
                fingerprint: Some(format!("fingerprint-{suffix}")),
            };
            let started = self
                .store
                .start(owner, &claimed.receipt, &controller)
                .unwrap();
            let permit = self
                .store
                .issue_execution_permit(owner, &started.receipt, &started.run.policy_digest)
                .unwrap();
            self.store
                .prepare_completion(
                    owner,
                    &permit,
                    &NewRunCompletion {
                        id: format!("completion-{suffix}"),
                        sink_kind: "test-sink".into(),
                        publication_key: format!("result.{suffix}"),
                        content_digest: digest('c'),
                        content_bytes: 1,
                    },
                )
                .unwrap();
            controller
        }
    }

    #[derive(Clone, Copy)]
    enum Behavior {
        Gone,
        StillPresent,
        Unavailable,
        Sleep(Duration, ControllerRecoveryObservation),
        Panic,
    }

    struct ConcurrencyProbe {
        active: AtomicUsize,
        maximum: AtomicUsize,
    }

    impl ConcurrencyProbe {
        fn new() -> Self {
            Self {
                active: AtomicUsize::new(0),
                maximum: AtomicUsize::new(0),
            }
        }

        fn enter(&self) {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
        }

        fn leave(&self) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct TestAdapter {
        name: &'static str,
        kind: ControllerKind,
        behavior: Behavior,
        probe: Option<Arc<ConcurrencyProbe>>,
    }

    struct TestCompletionSink {
        observation: AgentCompletionSinkObservation,
        inspections: AtomicUsize,
    }

    struct BlockingConfirmationAdapter {
        clock: Arc<BlockingSettlementClock>,
        confirmed: Arc<AtomicUsize>,
        arm_on_gone: bool,
    }

    #[async_trait]
    impl AgentControllerAdapter for BlockingConfirmationAdapter {
        fn name(&self) -> &str {
            "blocking-confirmation-adapter"
        }

        fn kind(&self) -> ControllerKind {
            ControllerKind::InProcess
        }

        async fn observe_gone(
            &self,
            _: ControllerRecoveryContext,
            _: ControllerRef,
        ) -> ControllerRecoveryObservation {
            if self.arm_on_gone {
                self.clock.arm();
            }
            ControllerRecoveryObservation::Gone
        }

        fn confirmed_gone(&self, _: &ControllerRef) {
            self.confirmed.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct BlockingInspectSink {
        clock: Arc<BlockingSettlementClock>,
        observation: AgentCompletionSinkObservation,
    }

    #[async_trait]
    impl AgentCompletionSink for BlockingInspectSink {
        fn kind(&self) -> &str {
            "test-sink"
        }

        async fn inspect(&self, _: RunCompletionRecord) -> AgentCompletionSinkObservation {
            self.clock.arm();
            self.observation
        }

        async fn publish(&self, _: RunCompletionRecord) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Unavailable
        }

        async fn discard(&self, _: RunCompletionRecord) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Unavailable
        }
    }

    struct SelectivePanicSink;

    #[async_trait]
    impl AgentCompletionSink for SelectivePanicSink {
        fn kind(&self) -> &str {
            "test-sink"
        }

        async fn inspect(&self, completion: RunCompletionRecord) -> AgentCompletionSinkObservation {
            assert_ne!(completion.run_id, "run-sink-panic", "sink inspection panic");
            AgentCompletionSinkObservation::Exact
        }

        async fn publish(&self, _: RunCompletionRecord) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Unavailable
        }

        async fn discard(&self, _: RunCompletionRecord) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Unavailable
        }
    }

    #[async_trait]
    impl AgentCompletionSink for TestCompletionSink {
        fn kind(&self) -> &str {
            "test-sink"
        }

        async fn inspect(
            &self,
            _completion: RunCompletionRecord,
        ) -> AgentCompletionSinkObservation {
            self.inspections.fetch_add(1, Ordering::SeqCst);
            self.observation
        }

        async fn publish(
            &self,
            _completion: RunCompletionRecord,
        ) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Unavailable
        }

        async fn discard(
            &self,
            _completion: RunCompletionRecord,
        ) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Unavailable
        }
    }

    #[async_trait]
    impl AgentControllerAdapter for TestAdapter {
        fn name(&self) -> &str {
            self.name
        }

        fn kind(&self) -> ControllerKind {
            self.kind
        }

        async fn observe_gone(
            &self,
            context: ControllerRecoveryContext,
            controller: ControllerRef,
        ) -> ControllerRecoveryObservation {
            assert!(context.deadline() >= Instant::now());
            assert_eq!(controller.kind, self.kind);
            if let Some(probe) = &self.probe {
                probe.enter();
            }
            let result = match self.behavior {
                Behavior::Gone => ControllerRecoveryObservation::Gone,
                Behavior::StillPresent => ControllerRecoveryObservation::StillPresent,
                Behavior::Unavailable => ControllerRecoveryObservation::Unavailable,
                Behavior::Sleep(duration, result) => {
                    tokio::time::sleep(duration).await;
                    result
                }
                Behavior::Panic => panic!("controller adapter panicked"),
            };
            if let Some(probe) = &self.probe {
                probe.leave();
            }
            result
        }
    }

    fn adapter(kind: ControllerKind, behavior: Behavior) -> Arc<dyn AgentControllerAdapter> {
        Arc::new(TestAdapter {
            name: match kind {
                ControllerKind::InProcess => "test.in-process",
                ControllerKind::Process => "test.process",
                ControllerKind::Remote => "test.remote",
            },
            kind,
            behavior,
            probe: None,
        })
    }

    fn controller(kind: ControllerKind, suffix: &str) -> ControllerRef {
        ControllerRef {
            kind,
            id: format!("controller-{suffix}"),
            fingerprint: Some(format!("fingerprint-{suffix}")),
        }
    }

    fn digest(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn blocking_settlement_store(
        suffix: &str,
        prepare: bool,
    ) -> (
        tempfile::TempDir,
        Arc<BlockingSettlementClock>,
        Arc<SqliteAgentStore>,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let clock = Arc::new(BlockingSettlementClock::new(Utc::now()));
        let store = Arc::new(
            SqliteAgentStore::open_with_clock(directory.path().join("agent.sqlite"), clock.clone())
                .unwrap(),
        );
        let worker_id = format!("worker-{suffix}");
        let run_id = format!("run-{suffix}");
        store
            .create_root(
                "alice",
                &NewWorker {
                    id: worker_id.clone(),
                    logical_session_id: None,
                },
                &NewAgentRun {
                    id: run_id.clone(),
                    worker_id,
                    task_id: None,
                    trace_id: None,
                    parent_run_id: None,
                    execution_backend: vyane_agent::ExecutionBackend::NativeInProcess,
                    mode: RunMode::Autonomous,
                    target_key: "provider/model".into(),
                    prompt_digest: digest('a'),
                    policy_digest: digest('b'),
                    available_at: clock.now(),
                    timeout_seconds: 600,
                    max_resume_attempts: 0,
                },
            )
            .unwrap();
        let claim = store
            .claim_due(
                "alice",
                vyane_agent::ExecutionBackend::NativeInProcess,
                "executor",
                1,
                1,
            )
            .unwrap()
            .remove(0);
        let started = store
            .start(
                "alice",
                &claim.receipt,
                &controller(ControllerKind::InProcess, suffix),
            )
            .unwrap();
        if prepare {
            let permit = store
                .issue_execution_permit("alice", &started.receipt, &started.run.policy_digest)
                .unwrap();
            store
                .prepare_completion(
                    "alice",
                    &permit,
                    &NewRunCompletion {
                        id: format!("completion-{suffix}"),
                        sink_kind: "test-sink".into(),
                        publication_key: format!("result.{suffix}"),
                        content_digest: digest('c'),
                        content_bytes: 1,
                    },
                )
                .unwrap();
        }
        clock.advance(2);
        (directory, clock, store)
    }

    fn options(
        batch_limit: usize,
        max_in_flight: usize,
        adapter_timeout: Duration,
    ) -> AgentRecoveryOptions {
        AgentRecoveryOptions {
            batch_limit,
            max_in_flight,
            adapter_timeout,
            settlement_margin: Duration::from_millis(10),
            operation_lease_seconds: 2,
        }
    }

    #[test]
    fn minimal_margin_never_extends_adapter_deadline_to_pass_deadline() {
        let started = Instant::now();
        let adapter_timeout = Duration::from_millis(5);
        let margin = Duration::from_nanos(1);
        let required_window = adapter_timeout.checked_add(margin).unwrap();
        let exact = RecoveryWindow {
            adapter_timeout,
            settlement_margin: margin,
            required_window,
            pass_deadline: started.checked_add(required_window).unwrap(),
        };
        assert_eq!(adapter_deadline(exact, started), None);

        let admitted = RecoveryWindow {
            pass_deadline: exact
                .pass_deadline
                .checked_add(Duration::from_nanos(1))
                .unwrap(),
            ..exact
        };
        let deadline = adapter_deadline(admitted, started).unwrap();
        assert_eq!(deadline, started.checked_add(adapter_timeout).unwrap());
        assert!(deadline < admitted.pass_deadline);
    }

    #[test]
    fn constructor_rejects_all_invalid_inputs_before_store_mutation() {
        let fixture = Fixture::new();
        fixture.create_claimed(
            "alice",
            "unchanged",
            Some(controller(ControllerKind::InProcess, "unchanged")),
        );
        fixture.make_due();
        let bad_options = AgentRecoveryOptions {
            adapter_timeout: Duration::from_secs(2),
            settlement_margin: Duration::from_secs(1),
            operation_lease_seconds: 3,
            ..options(1, 1, Duration::from_millis(10))
        };
        assert_eq!(
            fixture
                .driver("alice", bad_options, Vec::new())
                .unwrap_err(),
            AgentRecoveryError::InvalidOptions
        );
        assert_eq!(
            fixture
                .driver(
                    " alice",
                    options(1, 1, Duration::from_millis(10)),
                    Vec::new()
                )
                .unwrap_err(),
            AgentRecoveryError::InvalidOwner
        );
        let store: Arc<dyn AgentStore> = fixture.store.clone();
        assert_eq!(
            AgentRunRecoveryDriver::new(
                "alice",
                store,
                "Invalid Identity",
                options(1, 1, Duration::from_millis(10)),
                Vec::new(),
            )
            .unwrap_err(),
            AgentRecoveryError::InvalidReconciler
        );
        for invalid in [
            AgentRecoveryOptions {
                batch_limit: MAX_RECOVERY_BATCH + 1,
                ..options(1, 1, Duration::from_millis(10))
            },
            AgentRecoveryOptions {
                max_in_flight: MAX_RECOVERY_CONCURRENCY + 1,
                batch_limit: MAX_RECOVERY_BATCH,
                ..options(1, 1, Duration::from_millis(10))
            },
            AgentRecoveryOptions {
                adapter_timeout: MAX_CONTROLLER_TIMEOUT + Duration::from_nanos(1),
                operation_lease_seconds: MAX_RECOVERY_LEASE_SECONDS,
                ..options(1, 1, Duration::from_millis(10))
            },
            AgentRecoveryOptions {
                operation_lease_seconds: MAX_RECOVERY_LEASE_SECONDS + 1,
                ..options(1, 1, Duration::from_millis(10))
            },
        ] {
            assert_eq!(
                fixture.driver("alice", invalid, Vec::new()).unwrap_err(),
                AgentRecoveryError::InvalidOptions
            );
        }
        assert_eq!(fixture.state("alice", "unchanged"), RunState::Running);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_before_claim_is_non_mutating() {
        let fixture = Fixture::new();
        fixture.create_claimed(
            "alice",
            "cancelled",
            Some(controller(ControllerKind::InProcess, "cancelled")),
        );
        fixture.make_due();
        let driver = fixture
            .driver(
                "alice",
                options(1, 1, Duration::from_millis(10)),
                vec![adapter(ControllerKind::InProcess, Behavior::Gone)],
            )
            .unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let report = driver.recover_once(cancellation).await.unwrap();
        assert!(report.cancelled_before_claim);
        assert_eq!(report.claimed, 0);
        assert!(report.items.is_empty());
        assert_eq!(fixture.state("alice", "cancelled"), RunState::Running);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owner_scope_is_frozen_and_other_owner_is_untouched() {
        let fixture = Fixture::new();
        for owner in ["alice", "bob"] {
            fixture.create_claimed(
                owner,
                owner,
                Some(controller(ControllerKind::InProcess, owner)),
            );
        }
        fixture.make_due();
        let report = fixture
            .driver(
                "alice",
                options(4, 2, Duration::from_millis(50)),
                vec![adapter(ControllerKind::InProcess, Behavior::Gone)],
            )
            .unwrap()
            .recover_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.claimed, 1);
        assert_eq!(
            report.items,
            vec![AgentRecoveryItemStatus::RecoveredAfterControllerGone]
        );
        assert_eq!(fixture.state("alice", "alice"), RunState::Interrupted);
        assert_eq!(fixture.state("bob", "bob"), RunState::Running);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_confirmation_notifies_the_exact_adapter() {
        struct ConfirmationAdapter {
            confirmed: Arc<Mutex<Vec<ControllerRef>>>,
        }

        #[async_trait]
        impl AgentControllerAdapter for ConfirmationAdapter {
            fn name(&self) -> &str {
                "confirmation-adapter"
            }

            fn kind(&self) -> ControllerKind {
                ControllerKind::InProcess
            }

            async fn observe_gone(
                &self,
                _: ControllerRecoveryContext,
                _: ControllerRef,
            ) -> ControllerRecoveryObservation {
                ControllerRecoveryObservation::Gone
            }

            fn confirmed_gone(&self, controller: &ControllerRef) {
                self.confirmed.lock().unwrap().push(controller.clone());
            }
        }

        let fixture = Fixture::new();
        let expected = controller(ControllerKind::InProcess, "confirmed");
        fixture.create_claimed("alice", "confirmed", Some(expected.clone()));
        fixture.make_due();
        let confirmed = Arc::new(Mutex::new(Vec::new()));
        let registered: Arc<dyn AgentControllerAdapter> = Arc::new(ConfirmationAdapter {
            confirmed: Arc::clone(&confirmed),
        });
        let report = fixture
            .driver(
                "alice",
                options(1, 1, Duration::from_millis(50)),
                vec![registered],
            )
            .unwrap()
            .recover_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            report.items,
            vec![AgentRecoveryItemStatus::RecoveredAfterControllerGone]
        );
        assert_eq!(*confirmed.lock().unwrap(), vec![expected]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborted_waiter_still_confirms_gone_after_blocking_settlement_commits() {
        let (_directory, clock, store) = blocking_settlement_store("abort-confirm", true);
        let confirmed = Arc::new(AtomicUsize::new(0));
        let adapter: Arc<dyn AgentControllerAdapter> = Arc::new(BlockingConfirmationAdapter {
            clock: Arc::clone(&clock),
            confirmed: Arc::clone(&confirmed),
            arm_on_gone: false,
        });
        let sink: Arc<dyn AgentCompletionSink> = Arc::new(BlockingInspectSink {
            clock: Arc::clone(&clock),
            observation: AgentCompletionSinkObservation::Absent,
        });
        let driver = AgentRunRecoveryDriver::new_with_completion_sinks(
            "alice",
            store.clone() as Arc<dyn AgentStore>,
            "recovery-test",
            options(1, 1, Duration::from_millis(50)),
            vec![adapter],
            vec![sink],
        )
        .unwrap();
        let entered = clock.entered.notified();
        tokio::pin!(entered);
        entered.as_mut().enable();
        let task = tokio::spawn(driver.recover_once(CancellationToken::new()));
        entered.await;
        task.abort();
        clock.release();
        tokio::time::timeout(Duration::from_secs(2), async {
            while confirmed.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            store
                .get_run("alice", "run-abort-confirm")
                .unwrap()
                .unwrap()
                .state,
            RunState::Interrupted
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborted_waiter_still_confirms_gone_after_recovered_commit() {
        let (_directory, clock, store) = blocking_settlement_store("abort-commit", true);
        let confirmed = Arc::new(AtomicUsize::new(0));
        let adapter: Arc<dyn AgentControllerAdapter> = Arc::new(BlockingConfirmationAdapter {
            clock: Arc::clone(&clock),
            confirmed: Arc::clone(&confirmed),
            arm_on_gone: false,
        });
        let sink: Arc<dyn AgentCompletionSink> = Arc::new(BlockingInspectSink {
            clock: Arc::clone(&clock),
            observation: AgentCompletionSinkObservation::Exact,
        });
        let driver = AgentRunRecoveryDriver::new_with_completion_sinks(
            "alice",
            store.clone() as Arc<dyn AgentStore>,
            "recovery-test",
            options(1, 1, Duration::from_millis(50)),
            vec![adapter],
            vec![sink],
        )
        .unwrap();
        let entered = clock.entered.notified();
        tokio::pin!(entered);
        entered.as_mut().enable();
        let task = tokio::spawn(driver.recover_once(CancellationToken::new()));
        entered.await;
        task.abort();
        clock.release();
        tokio::time::timeout(Duration::from_secs(2), async {
            while confirmed.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            store
                .get_run("alice", "run-abort-commit")
                .unwrap()
                .unwrap()
                .state,
            RunState::Succeeded
        );
        assert_eq!(
            store
                .get_completion("alice", "run-abort-commit")
                .unwrap()
                .unwrap()
                .status,
            RunCompletionStatus::Committed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn only_affirmative_gone_observation_settles_controller_ticket() {
        for (suffix, behavior, expected) in [
            (
                "gone",
                Behavior::Gone,
                AgentRecoveryItemStatus::RecoveredAfterControllerGone,
            ),
            (
                "still",
                Behavior::StillPresent,
                AgentRecoveryItemStatus::ControllerStillPresent,
            ),
            (
                "unavailable",
                Behavior::Unavailable,
                AgentRecoveryItemStatus::ControllerUnavailable,
            ),
        ] {
            let fixture = Fixture::new();
            fixture.create_claimed(
                "alice",
                suffix,
                Some(controller(ControllerKind::Process, suffix)),
            );
            fixture.make_due();
            let report = fixture
                .driver(
                    "alice",
                    options(1, 1, Duration::from_millis(50)),
                    vec![adapter(ControllerKind::Process, behavior)],
                )
                .unwrap()
                .recover_once(CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(report.items, vec![expected]);
            let expected_state = if behavior_matches_gone(behavior) {
                RunState::Interrupted
            } else {
                RunState::Cancelling
            };
            assert_eq!(fixture.state("alice", suffix), expected_state);
        }
    }

    const fn behavior_matches_gone(behavior: Behavior) -> bool {
        matches!(behavior, Behavior::Gone)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timeout_panic_and_missing_adapter_never_settle() {
        for (suffix, adapters, expected) in [
            (
                "timeout",
                vec![adapter(
                    ControllerKind::Remote,
                    Behavior::Sleep(
                        Duration::from_millis(100),
                        ControllerRecoveryObservation::Gone,
                    ),
                )],
                AgentRecoveryItemStatus::AdapterTimedOut,
            ),
            (
                "panic",
                vec![adapter(ControllerKind::Remote, Behavior::Panic)],
                AgentRecoveryItemStatus::AdapterPanicked,
            ),
            (
                "missing",
                Vec::new(),
                AgentRecoveryItemStatus::MissingAdapter,
            ),
        ] {
            let fixture = Fixture::new();
            fixture.create_claimed(
                "alice",
                suffix,
                Some(controller(ControllerKind::Remote, suffix)),
            );
            fixture.make_due();
            let report = fixture
                .driver("alice", options(1, 1, Duration::from_millis(15)), adapters)
                .unwrap()
                .recover_once(CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(report.items, vec![expected]);
            assert_eq!(fixture.state("alice", suffix), RunState::Cancelling);
            let rendered = format!("{report:?}");
            assert!(!rendered.contains("controller adapter panicked"));
            assert!(!rendered.contains(suffix));
        }
    }

    struct CancelAfterFirstAdapter {
        cancellation: CancellationToken,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl AgentControllerAdapter for CancelAfterFirstAdapter {
        fn name(&self) -> &str {
            "test.cancel-after-first"
        }

        fn kind(&self) -> ControllerKind {
            ControllerKind::Process
        }

        async fn observe_gone(
            &self,
            _context: ControllerRecoveryContext,
            _controller: ControllerRef,
        ) -> ControllerRecoveryObservation {
            assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
            self.cancellation.cancel();
            ControllerRecoveryObservation::StillPresent
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_after_claim_suppresses_buffered_adapter_calls() {
        let fixture = Fixture::new();
        for suffix in ["first", "second"] {
            fixture.create_claimed(
                "alice",
                suffix,
                Some(controller(ControllerKind::Process, suffix)),
            );
        }
        fixture.make_due();
        let cancellation = CancellationToken::new();
        let calls = Arc::new(CancelAfterFirstAdapter {
            cancellation: cancellation.clone(),
            calls: AtomicUsize::new(0),
        });
        let registered: Arc<dyn AgentControllerAdapter> = calls.clone();
        let report = fixture
            .driver(
                "alice",
                options(2, 1, Duration::from_millis(50)),
                vec![registered],
            )
            .unwrap()
            .recover_once(cancellation)
            .await
            .unwrap();
        assert_eq!(calls.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            report.items,
            vec![
                AgentRecoveryItemStatus::ControllerStillPresent,
                AgentRecoveryItemStatus::CancelledBeforeAdapter,
            ]
        );
        assert_eq!(fixture.state("alice", "first"), RunState::Cancelling);
        assert_eq!(fixture.state("alice", "second"), RunState::Cancelling);
    }

    struct ReclaimWindowAdapter {
        entered: Arc<tokio::sync::Notify>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl AgentControllerAdapter for ReclaimWindowAdapter {
        fn name(&self) -> &str {
            "test.reclaim-window"
        }

        fn kind(&self) -> ControllerKind {
            ControllerKind::Remote
        }

        async fn observe_gone(
            &self,
            _context: ControllerRecoveryContext,
            _controller: ControllerRef,
        ) -> ControllerRecoveryObservation {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            tokio::time::sleep(Duration::from_millis(700)).await;
            ControllerRecoveryObservation::StillPresent
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn monotonic_window_blocks_old_adapter_after_custom_clock_reclaim() {
        let far_future = Utc::now().checked_add_signed(TimeDelta::days(365)).unwrap();
        let fixture = Fixture::at(far_future);
        for suffix in ["old-first", "old-second"] {
            fixture.create_claimed(
                "alice",
                suffix,
                Some(controller(ControllerKind::Remote, suffix)),
            );
        }
        fixture.make_due();
        let entered = Arc::new(tokio::sync::Notify::new());
        let adapter = Arc::new(ReclaimWindowAdapter {
            entered: Arc::clone(&entered),
            calls: AtomicUsize::new(0),
        });
        let registered: Arc<dyn AgentControllerAdapter> = adapter.clone();
        let recovery = fixture
            .driver(
                "alice",
                AgentRecoveryOptions {
                    batch_limit: 2,
                    max_in_flight: 1,
                    adapter_timeout: Duration::from_secs(1),
                    settlement_margin: Duration::from_millis(500),
                    operation_lease_seconds: 2,
                },
                vec![registered],
            )
            .unwrap()
            .recover_once(CancellationToken::new());
        let store = Arc::clone(&fixture.store);
        let clock = Arc::clone(&fixture.clock);
        let reclaim = async move {
            entered.notified().await;
            clock.advance(3);
            tokio::task::spawn_blocking(move || {
                store.claim_recovery_due("alice", "second-reconciler", 2, 2)
            })
            .await
            .unwrap()
            .unwrap()
        };
        let (report, reclaimed) = tokio::join!(recovery, reclaim);
        let report = report.unwrap();
        assert_eq!(reclaimed.len(), 2);
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            report.items,
            vec![
                AgentRecoveryItemStatus::ControllerStillPresent,
                AgentRecoveryItemStatus::InsufficientLeaseWindow,
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controllerless_ticket_is_confirmed_without_an_adapter() {
        let fixture = Fixture::new();
        fixture.create_claimed("alice", "controllerless", None);
        fixture.make_due();
        let report = fixture
            .driver(
                "alice",
                options(1, 1, Duration::from_millis(10)),
                Vec::new(),
            )
            .unwrap()
            .recover_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            report.items,
            vec![AgentRecoveryItemStatus::RecoveredWithoutController]
        );
        assert_eq!(
            fixture.state("alice", "controllerless"),
            RunState::Interrupted
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adapter_polling_never_exceeds_frozen_concurrency_bound() {
        let fixture = Fixture::new();
        for index in 0..6 {
            let suffix = format!("bounded-{index}");
            fixture.create_claimed(
                "alice",
                &suffix,
                Some(controller(ControllerKind::Process, &suffix)),
            );
        }
        fixture.make_due();
        let probe = Arc::new(ConcurrencyProbe::new());
        let bounded_adapter: Arc<dyn AgentControllerAdapter> = Arc::new(TestAdapter {
            name: "test.bounded",
            kind: ControllerKind::Process,
            behavior: Behavior::Sleep(
                Duration::from_millis(20),
                ControllerRecoveryObservation::StillPresent,
            ),
            probe: Some(Arc::clone(&probe)),
        });
        let report = fixture
            .driver(
                "alice",
                options(6, 2, Duration::from_millis(100)),
                vec![bounded_adapter],
            )
            .unwrap()
            .recover_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.claimed, 6);
        assert_eq!(report.items.len(), 6);
        assert!(
            report
                .items
                .iter()
                .all(|status| *status == AgentRecoveryItemStatus::ControllerStillPresent)
        );
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(probe.maximum.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn completion_descriptor_must_match_the_exact_recovery_ticket_before_sink_use() {
        let fixture = Fixture::new();
        fixture.create_prepared("alice", "binding");
        fixture.make_due();
        let ticket = fixture
            .store
            .claim_recovery_due("alice", "recovery-test", 30, 1)
            .unwrap()
            .remove(0);
        let completion = fixture
            .store
            .completion_for_recovery("alice", &ticket)
            .unwrap()
            .unwrap();
        assert!(completion_matches_ticket("alice", &completion, &ticket));

        let mut foreign_owner = completion.clone();
        foreign_owner.owner = "bob".into();
        assert!(!completion_matches_ticket("alice", &foreign_owner, &ticket));
        let mut foreign_run = completion.clone();
        foreign_run.run_id = "other-run".into();
        assert!(!completion_matches_ticket("alice", &foreign_run, &ticket));
        let mut foreign_generation = completion.clone();
        foreign_generation.worker_generation += 1;
        assert!(!completion_matches_ticket(
            "alice",
            &foreign_generation,
            &ticket
        ));
        let mut terminal = completion;
        terminal.status = vyane_agent::RunCompletionStatus::Committed;
        assert!(!completion_matches_ticket("alice", &terminal, &ticket));
    }

    #[tokio::test]
    async fn exact_staged_completion_is_committed_after_controller_gone() {
        let fixture = Fixture::new();
        fixture.create_prepared("alice", "completion-exact");
        fixture.make_due();
        let sink = Arc::new(TestCompletionSink {
            observation: AgentCompletionSinkObservation::Exact,
            inspections: AtomicUsize::new(0),
        });
        let report = fixture
            .driver_with_sinks(
                "alice",
                options(1, 1, Duration::from_millis(10)),
                vec![Arc::new(TestAdapter {
                    name: "in-process-test",
                    kind: ControllerKind::InProcess,
                    behavior: Behavior::Gone,
                    probe: None,
                })],
                vec![sink.clone()],
            )
            .unwrap()
            .recover_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.items, [AgentRecoveryItemStatus::CompletionRecovered]);
        assert_eq!(
            fixture.state("alice", "completion-exact"),
            RunState::Succeeded
        );
        assert_eq!(sink.inspections.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture
                .store
                .get_completion("alice", "run-completion-exact")
                .unwrap()
                .unwrap()
                .status,
            RunCompletionStatus::Committed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn panicking_completion_sink_isolated_to_its_item() {
        let fixture = Fixture::new();
        fixture.create_prepared("alice", "sink-panic");
        fixture.create_prepared("alice", "sink-exact");
        fixture.make_due();
        let sink: Arc<dyn AgentCompletionSink> = Arc::new(SelectivePanicSink);
        let report = fixture
            .driver_with_sinks(
                "alice",
                options(2, 2, Duration::from_millis(50)),
                vec![adapter(ControllerKind::InProcess, Behavior::Gone)],
                vec![sink],
            )
            .unwrap()
            .recover_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.claimed, 2);
        assert_eq!(
            report
                .items
                .iter()
                .filter(|status| **status == AgentRecoveryItemStatus::CompletionUnavailable)
                .count(),
            1
        );
        assert_eq!(
            report
                .items
                .iter()
                .filter(|status| **status == AgentRecoveryItemStatus::CompletionRecovered)
                .count(),
            1
        );
        assert_eq!(fixture.state("alice", "sink-panic"), RunState::Cancelling);
        assert_eq!(fixture.state("alice", "sink-exact"), RunState::Succeeded);
    }

    #[tokio::test]
    async fn proven_absent_completion_is_abandoned_after_controller_gone() {
        let fixture = Fixture::new();
        fixture.create_prepared("alice", "completion-absent");
        fixture.make_due();
        let report = fixture
            .driver_with_sinks(
                "alice",
                options(1, 1, Duration::from_millis(10)),
                vec![Arc::new(TestAdapter {
                    name: "in-process-test",
                    kind: ControllerKind::InProcess,
                    behavior: Behavior::Gone,
                    probe: None,
                })],
                vec![Arc::new(TestCompletionSink {
                    observation: AgentCompletionSinkObservation::Absent,
                    inspections: AtomicUsize::new(0),
                })],
            )
            .unwrap()
            .recover_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.items, [AgentRecoveryItemStatus::CompletionAbsent]);
        assert_eq!(
            fixture.state("alice", "completion-absent"),
            RunState::Interrupted
        );
        assert_eq!(
            fixture
                .store
                .get_completion("alice", "run-completion-absent")
                .unwrap()
                .unwrap()
                .status,
            RunCompletionStatus::Abandoned
        );
    }

    #[tokio::test]
    async fn unavailable_completion_truth_keeps_recovery_ticket_unsettled() {
        let fixture = Fixture::new();
        fixture.create_prepared("alice", "completion-unavailable");
        fixture.make_due();
        let report = fixture
            .driver_with_sinks(
                "alice",
                options(1, 1, Duration::from_millis(10)),
                vec![Arc::new(TestAdapter {
                    name: "in-process-test",
                    kind: ControllerKind::InProcess,
                    behavior: Behavior::Gone,
                    probe: None,
                })],
                vec![Arc::new(TestCompletionSink {
                    observation: AgentCompletionSinkObservation::Unavailable,
                    inspections: AtomicUsize::new(0),
                })],
            )
            .unwrap()
            .recover_once(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            report.items,
            [AgentRecoveryItemStatus::CompletionUnavailable]
        );
        assert_eq!(
            fixture.state("alice", "completion-unavailable"),
            RunState::Cancelling
        );
        assert_eq!(
            fixture
                .store
                .get_completion("alice", "run-completion-unavailable")
                .unwrap()
                .unwrap()
                .status,
            RunCompletionStatus::Prepared
        );
    }

    #[test]
    fn adapters_are_frozen_unique_and_debug_is_redacted() {
        let fixture = Fixture::new();
        let secret_adapter: Arc<dyn AgentControllerAdapter> = Arc::new(TestAdapter {
            name: "private-secret-adapter",
            kind: ControllerKind::InProcess,
            behavior: Behavior::Gone,
            probe: None,
        });
        let driver = fixture
            .driver(
                "sensitive-owner",
                options(1, 1, Duration::from_millis(10)),
                vec![Arc::clone(&secret_adapter)],
            )
            .unwrap();
        let rendered = format!("{driver:?}");
        assert!(!rendered.contains("sensitive-owner"));
        assert!(!rendered.contains("private-secret-adapter"));

        let duplicate_kind = fixture.driver(
            "alice",
            options(1, 1, Duration::from_millis(10)),
            vec![
                secret_adapter,
                Arc::new(TestAdapter {
                    name: "another-adapter",
                    kind: ControllerKind::InProcess,
                    behavior: Behavior::Gone,
                    probe: None,
                }),
            ],
        );
        let error = duplicate_kind.unwrap_err();
        assert_eq!(error, AgentRecoveryError::DuplicateAdapterKind);
        assert!(!format!("{error:?}").contains("secret"));

        let invalid_name: Arc<dyn AgentControllerAdapter> = Arc::new(TestAdapter {
            name: "Invalid Adapter",
            kind: ControllerKind::Process,
            behavior: Behavior::Gone,
            probe: None,
        });
        assert_eq!(
            fixture
                .driver(
                    "alice",
                    options(1, 1, Duration::from_millis(10)),
                    vec![invalid_name],
                )
                .unwrap_err(),
            AgentRecoveryError::InvalidAdapter
        );

        let duplicate_name = [ControllerKind::Process, ControllerKind::Remote]
            .into_iter()
            .map(|kind| {
                Arc::new(TestAdapter {
                    name: "same-adapter",
                    kind,
                    behavior: Behavior::Gone,
                    probe: None,
                }) as Arc<dyn AgentControllerAdapter>
            })
            .collect();
        assert_eq!(
            fixture
                .driver(
                    "alice",
                    options(2, 1, Duration::from_millis(10)),
                    duplicate_name,
                )
                .unwrap_err(),
            AgentRecoveryError::DuplicateAdapterName
        );
    }
}
