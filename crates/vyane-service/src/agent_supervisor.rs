//! Explicit resident polling over one paired AgentRun execution/recovery backend.
//!
//! The supervisor owns no runtime, task, channel, payload queue, or resume
//! policy. Host cancellation stops scheduling new passes, interrupts waits,
//! and cooperatively signals a pass that already owns controller work. The
//! pass remains awaited so its executor can quiesce before host exit.

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered};
use vyane_agent::{AgentStore, ControllerKind, ExecutionBackend};
use vyane_core::CancellationToken;

use crate::{
    AgentCompletionProjectionStatus, AgentCompletionPublisher, AgentCompletionPublisherOptions,
    AgentCompletionSink, AgentControllerAdapter, AgentExecutionItemStatus, AgentExecutionOptions,
    AgentRecoveryItemStatus, AgentRecoveryOptions, AgentRunExecutionDriver, AgentRunExecutor,
    AgentRunRecoveryDriver, InProcessAgentComponents,
};

const MAX_SCHEDULE_DELAY: Duration = Duration::from_secs(24 * 60 * 60);
const COMPLETION_PROJECTOR_ID: &str = "vyane-agent-completion-v1";

/// Polling and retry bounds for one resident AgentRun supervisor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSupervisorOptions {
    pub execution_poll_interval: Duration,
    pub recovery_poll_interval: Duration,
    pub initial_error_backoff: Duration,
    pub max_error_backoff: Duration,
}

impl Default for AgentSupervisorOptions {
    fn default() -> Self {
        Self {
            execution_poll_interval: Duration::from_millis(250),
            recovery_poll_interval: Duration::from_secs(1),
            initial_error_backoff: Duration::from_millis(250),
            max_error_backoff: Duration::from_secs(30),
        }
    }
}

/// Closed construction failure with no owner, controller, or store detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentSupervisorError {
    InvalidSchedule,
    InvalidExecution,
    InvalidRecovery,
    InvalidCompletion,
    InvalidLaneCount,
    DuplicateExecutionBackend,
    DuplicateLeaseOwner,
    MissingRecoveryAdapter,
}

impl fmt::Display for AgentSupervisorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidSchedule => "AgentRun supervisor schedule is invalid",
            Self::InvalidExecution => "AgentRun execution configuration is invalid",
            Self::InvalidRecovery => "AgentRun recovery configuration is invalid",
            Self::InvalidCompletion => "AgentRun completion configuration is invalid",
            Self::InvalidLaneCount => "AgentRun execution lane count is invalid",
            Self::DuplicateExecutionBackend => "AgentRun execution backend is duplicated",
            Self::DuplicateLeaseOwner => "AgentRun execution lease owner is duplicated",
            Self::MissingRecoveryAdapter => {
                "AgentRun execution lane has no matching recovery adapter"
            }
        })
    }
}

impl std::error::Error for AgentSupervisorError {}

/// Body-free counters for one resident loop at graceful exit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentSupervisorLoopExit {
    pub cycles: u64,
    pub successful_cycles: u64,
    pub failed_cycles: u64,
    pub panicked_cycles: u64,
    pub claimed: u64,
    pub degraded_items: u64,
}

/// Body-free summary returned after all three loops have drained.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentSupervisorExit {
    pub execution: AgentSupervisorLoopExit,
    pub recovery: AgentSupervisorLoopExit,
    pub completion: AgentSupervisorLoopExit,
}

/// Body-free counters for one exact durable execution backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentExecutionLaneExit {
    pub backend: ExecutionBackend,
    pub execution: AgentSupervisorLoopExit,
}

/// Body-free summary returned after every lane and both shared loops drain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResidentAgentHostExit {
    pub lanes: Vec<AgentExecutionLaneExit>,
    pub recovery: AgentSupervisorLoopExit,
    pub completion: AgentSupervisorLoopExit,
}

/// One closed exact-backend execution lane.
///
/// Metadata is frozen by [`ResidentAgentHost::new`]. The value starts no work
/// and performs no store operation.
pub struct ResidentAgentExecutionLane {
    executor: Arc<dyn AgentRunExecutor>,
    lease_owner: String,
    execution: AgentExecutionOptions,
}

/// Shared owner/store and recovery/publication ports for a multi-lane host.
pub struct ResidentAgentHostBackend {
    owner: String,
    store: Arc<dyn AgentStore>,
    adapters: Vec<Arc<dyn AgentControllerAdapter>>,
    completion_sinks: Vec<Arc<dyn AgentCompletionSink>>,
}

impl ResidentAgentHostBackend {
    #[must_use]
    pub fn new(
        owner: impl Into<String>,
        store: Arc<dyn AgentStore>,
        adapters: Vec<Arc<dyn AgentControllerAdapter>>,
        completion_sinks: Vec<Arc<dyn AgentCompletionSink>>,
    ) -> Self {
        Self {
            owner: owner.into(),
            store,
            adapters,
            completion_sinks,
        }
    }
}

impl fmt::Debug for ResidentAgentHostBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResidentAgentHostBackend")
            .finish_non_exhaustive()
    }
}

impl ResidentAgentExecutionLane {
    #[must_use]
    pub fn new(
        executor: Arc<dyn AgentRunExecutor>,
        lease_owner: impl Into<String>,
        execution: AgentExecutionOptions,
    ) -> Self {
        Self {
            executor,
            lease_owner: lease_owner.into(),
            execution,
        }
    }
}

impl fmt::Debug for ResidentAgentExecutionLane {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResidentAgentExecutionLane")
            .finish_non_exhaustive()
    }
}

/// Non-cloneable multi-lane resident substrate over one owner and store.
///
/// Each exact backend receives one isolated execution loop. Recovery is a
/// single union loop over all lane adapters, and completion projection is a
/// single shared loop. This is a service substrate: constructing it does not
/// install a native or remote production runtime.
pub struct ResidentAgentHost {
    owner: String,
    store: Arc<dyn AgentStore>,
    lanes: Vec<FrozenExecutionLane>,
    recovery_template: AgentRunRecoveryDriver,
    completion: AgentCompletionPublisher,
    schedule: AgentSupervisorOptions,
}

struct FrozenExecutionLane {
    executor: Arc<dyn AgentRunExecutor>,
    executor_kind: ControllerKind,
    backend: ExecutionBackend,
    lease_owner: String,
    execution: AgentExecutionOptions,
}

impl ResidentAgentHost {
    /// Assemble complete paired backends without exposing their internal
    /// ports to a frontend crate. Shared owner/store and backend uniqueness
    /// are validated by this service boundary.
    pub fn from_backends(
        backends: Vec<(ResidentAgentBackend, String, AgentExecutionOptions)>,
        reconciler: impl Into<String>,
        recovery: AgentRecoveryOptions,
        schedule: AgentSupervisorOptions,
    ) -> Result<Self, AgentSupervisorError> {
        if backends.is_empty() || backends.len() > 3 {
            return Err(AgentSupervisorError::InvalidLaneCount);
        }
        let mut backends = backends.into_iter();
        let (first, first_lease, first_execution) = backends.next().expect("non-empty validated");
        let ResidentAgentBackend {
            owner,
            store,
            executor,
            adapters,
            completion_sinks,
        } = first;
        let mut shared_adapters = adapters;
        let mut shared_sinks = completion_sinks;
        let mut lanes = vec![ResidentAgentExecutionLane::new(
            executor,
            first_lease,
            first_execution,
        )];
        for (backend, lease_owner, execution) in backends {
            let ResidentAgentBackend {
                owner: lane_owner,
                store: lane_store,
                executor,
                adapters,
                completion_sinks,
            } = backend;
            if lane_owner != owner || !Arc::ptr_eq(&lane_store, &store) {
                return Err(AgentSupervisorError::InvalidExecution);
            }
            shared_adapters.extend(adapters);
            shared_sinks.extend(completion_sinks);
            lanes.push(ResidentAgentExecutionLane::new(
                executor,
                lease_owner,
                execution,
            ));
        }
        Self::new(
            ResidentAgentHostBackend::new(owner, store, shared_adapters, shared_sinks),
            lanes,
            reconciler,
            recovery,
            schedule,
        )
    }

    pub fn new(
        shared: ResidentAgentHostBackend,
        lanes: Vec<ResidentAgentExecutionLane>,
        reconciler: impl Into<String>,
        recovery: AgentRecoveryOptions,
        schedule: AgentSupervisorOptions,
    ) -> Result<Self, AgentSupervisorError> {
        validate_schedule(&schedule)?;
        if lanes.is_empty() || lanes.len() > 3 {
            return Err(AgentSupervisorError::InvalidLaneCount);
        }

        let ResidentAgentHostBackend {
            owner,
            store,
            adapters,
            completion_sinks,
        } = shared;
        let reconciler = reconciler.into();
        let mut frozen = Vec::with_capacity(lanes.len());
        let mut backends = Vec::with_capacity(lanes.len());
        let mut lease_owners = Vec::with_capacity(lanes.len());
        for lane in lanes {
            let executor_kind = catch_unwind(AssertUnwindSafe(|| lane.executor.kind()))
                .map_err(|_| AgentSupervisorError::InvalidExecution)?;
            let backend = ExecutionBackend::for_controller_kind(executor_kind);
            if backends.contains(&backend) {
                return Err(AgentSupervisorError::DuplicateExecutionBackend);
            }
            if lease_owners.contains(&lane.lease_owner) {
                return Err(AgentSupervisorError::DuplicateLeaseOwner);
            }
            AgentRunExecutionDriver::new_with_executor_kind(
                owner.clone(),
                Arc::clone(&store),
                lane.lease_owner.clone(),
                lane.execution.clone(),
                Arc::clone(&lane.executor),
                executor_kind,
            )
            .map_err(|_| AgentSupervisorError::InvalidExecution)?;
            backends.push(backend);
            lease_owners.push(lane.lease_owner.clone());
            frozen.push(FrozenExecutionLane {
                executor: lane.executor,
                executor_kind,
                backend,
                lease_owner: lane.lease_owner,
                execution: lane.execution,
            });
        }

        // Validate the complete adapter union only after every lane has been
        // frozen. The constructor performs no store operation.
        let recovery_validation = AgentRunRecoveryDriver::new_with_completion_sinks(
            owner.clone(),
            Arc::clone(&store),
            reconciler.clone(),
            recovery.clone(),
            adapters.clone(),
            completion_sinks.clone(),
        )
        .map_err(|_| AgentSupervisorError::InvalidRecovery)?;
        let adapter_kinds = recovery_validation.registered_adapter_kinds();
        if frozen
            .iter()
            .any(|lane| !adapter_kinds.contains(&lane.executor_kind))
        {
            return Err(AgentSupervisorError::MissingRecoveryAdapter);
        }
        let completion = AgentCompletionPublisher::new(
            owner.clone(),
            COMPLETION_PROJECTOR_ID,
            Arc::clone(&store),
            completion_sinks.clone(),
            AgentCompletionPublisherOptions::default(),
        )
        .map_err(|_| AgentSupervisorError::InvalidCompletion)?;

        Ok(Self {
            owner,
            store,
            lanes: frozen,
            recovery_template: recovery_validation,
            completion,
            schedule,
        })
    }

    /// Run all exact-backend lanes with one union recovery loop and one
    /// completion projector. Shutdown first stops claims and drains every
    /// execution lane, then stops recovery, then performs completion's final
    /// durable outbox pass.
    pub async fn run(self, cancel: CancellationToken) -> ResidentAgentHostExit {
        let Self {
            owner,
            store,
            lanes,
            recovery_template,
            completion,
            schedule,
        } = self;

        let lane_futures = FuturesUnordered::new();
        for lane in lanes {
            let backend = lane.backend;
            let lane_owner = owner.clone();
            let lane_store = Arc::clone(&store);
            let lane_schedule = schedule.clone();
            let lane_cancel = cancel.clone();
            lane_futures.push(async move {
                let execution = run_execution_loop(
                    ExecutionLoopBackend {
                        owner: lane_owner,
                        store: lane_store,
                        executor: lane.executor,
                        executor_kind: lane.executor_kind,
                    },
                    lane.lease_owner,
                    lane.execution,
                    lane_schedule,
                    lane_cancel,
                )
                .await;
                AgentExecutionLaneExit { backend, execution }
            });
        }

        let recovery_stop = CancellationToken::new();
        let recovery_loop =
            run_frozen_recovery_loop(recovery_template, schedule.clone(), recovery_stop.clone());
        let lanes_then_stop_recovery = async move {
            let lanes = lane_futures.collect::<Vec<_>>().await;
            recovery_stop.cancel();
            lanes
        };
        let completion_stop = CancellationToken::new();
        let completion_loop =
            run_completion_loop(&completion, schedule, completion_stop.clone(), true);
        let work_loops = async move {
            let (mut lanes, recovery) = tokio::join!(lanes_then_stop_recovery, recovery_loop);
            lanes.sort_by_key(|lane| match lane.backend {
                ExecutionBackend::LegacyUnassigned => 0,
                ExecutionBackend::CliHarnessProcess => 1,
                ExecutionBackend::NativeInProcess => 2,
                ExecutionBackend::Remote => 3,
            });
            completion_stop.cancel();
            (lanes, recovery)
        };
        let ((lanes, recovery), completion) = tokio::join!(work_loops, completion_loop);
        ResidentAgentHostExit {
            lanes,
            recovery,
            completion,
        }
    }
}

impl fmt::Debug for ResidentAgentHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResidentAgentHost")
            .field("lanes", &self.lanes.len())
            .finish_non_exhaustive()
    }
}

/// Non-cloneable resident driver over one exact paired execution/recovery backend.
///
/// Cancelling the host token prevents a new pass, interrupts a scheduling
/// delay, and is propagated into a pass already in progress. Once an executor
/// has been polled, the execution driver continues to own and await it so a
/// process executor can terminate and reap its group and publish lifecycle
/// evidence before returning. Dropping [`Self::run`] forfeits that
/// graceful-drain guarantee, and a blocking custom store call can outlive its
/// async waiter.
pub struct ResidentAgentSupervisor {
    owner: String,
    store: Arc<dyn AgentStore>,
    executor: Arc<dyn AgentRunExecutor>,
    executor_kind: ControllerKind,
    adapters: Vec<Arc<dyn AgentControllerAdapter>>,
    completion_sinks: Vec<Arc<dyn AgentCompletionSink>>,
    lease_owner: String,
    reconciler: String,
    execution: AgentExecutionOptions,
    recovery: AgentRecoveryOptions,
    completion: AgentCompletionPublisher,
    schedule: AgentSupervisorOptions,
}

/// Exact execution/recovery ports retained by a resident AgentRun host.
///
/// The value owns no runtime and starts no work. Keeping the complete paired
/// backend in one value makes it difficult for a process host to accidentally
/// construct execution and recovery over different stores or owners.
pub struct ResidentAgentBackend {
    owner: String,
    store: Arc<dyn AgentStore>,
    executor: Arc<dyn AgentRunExecutor>,
    adapters: Vec<Arc<dyn AgentControllerAdapter>>,
    completion_sinks: Vec<Arc<dyn AgentCompletionSink>>,
}

struct RecoveryLoopBackend {
    owner: String,
    store: Arc<dyn AgentStore>,
    adapters: Vec<Arc<dyn AgentControllerAdapter>>,
    completion_sinks: Vec<Arc<dyn AgentCompletionSink>>,
}

struct ExecutionLoopBackend {
    owner: String,
    store: Arc<dyn AgentStore>,
    executor: Arc<dyn AgentRunExecutor>,
    executor_kind: ControllerKind,
}

impl ResidentAgentBackend {
    #[must_use]
    pub fn new(
        owner: impl Into<String>,
        store: Arc<dyn AgentStore>,
        executor: Arc<dyn AgentRunExecutor>,
        adapters: Vec<Arc<dyn AgentControllerAdapter>>,
        completion_sinks: Vec<Arc<dyn AgentCompletionSink>>,
    ) -> Self {
        Self {
            owner: owner.into(),
            store,
            executor,
            adapters,
            completion_sinks,
        }
    }

    /// Return one exact adapter for host-owned cancellation or confirmation.
    #[must_use]
    pub fn adapter(&self, kind: ControllerKind) -> Option<Arc<dyn AgentControllerAdapter>> {
        self.adapters
            .iter()
            .find(|adapter| adapter.kind() == kind)
            .cloned()
    }
}

impl fmt::Debug for ResidentAgentBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResidentAgentBackend")
            .finish_non_exhaustive()
    }
}

impl ResidentAgentSupervisor {
    pub fn new(
        backend: ResidentAgentBackend,
        lease_owner: impl Into<String>,
        reconciler: impl Into<String>,
        execution: AgentExecutionOptions,
        recovery: AgentRecoveryOptions,
        schedule: AgentSupervisorOptions,
    ) -> Result<Self, AgentSupervisorError> {
        let ResidentAgentBackend {
            owner,
            store,
            executor,
            adapters,
            completion_sinks,
        } = backend;
        let lease_owner = lease_owner.into();
        let reconciler = reconciler.into();
        validate_schedule(&schedule)?;
        let executor_kind = catch_unwind(AssertUnwindSafe(|| executor.kind()))
            .map_err(|_| AgentSupervisorError::InvalidExecution)?;
        AgentRunExecutionDriver::new_with_executor_kind(
            owner.clone(),
            Arc::clone(&store),
            lease_owner.clone(),
            execution.clone(),
            Arc::clone(&executor),
            executor_kind,
        )
        .map_err(|_| AgentSupervisorError::InvalidExecution)?;
        AgentRunRecoveryDriver::new_with_completion_sinks(
            owner.clone(),
            Arc::clone(&store),
            reconciler.clone(),
            recovery.clone(),
            adapters.clone(),
            completion_sinks.clone(),
        )
        .map_err(|_| AgentSupervisorError::InvalidRecovery)?;
        let matching_adapter = adapters.iter().any(|adapter| {
            catch_unwind(AssertUnwindSafe(|| adapter.kind()))
                .is_ok_and(|kind| kind == executor_kind)
        });
        if !matching_adapter {
            return Err(AgentSupervisorError::InvalidRecovery);
        }
        let completion = AgentCompletionPublisher::new(
            owner.clone(),
            COMPLETION_PROJECTOR_ID,
            Arc::clone(&store),
            completion_sinks.clone(),
            AgentCompletionPublisherOptions::default(),
        )
        .map_err(|_| AgentSupervisorError::InvalidCompletion)?;
        Ok(Self {
            owner,
            store,
            executor,
            executor_kind,
            adapters,
            completion_sinks,
            lease_owner,
            reconciler,
            execution,
            recovery,
            completion,
            schedule,
        })
    }

    /// Poll execution, recovery, and completion publication independently
    /// until host cancellation. Completion publication remains live while the
    /// execution and recovery loops drain, then receives one final durable
    /// outbox pass so a completion committed by the last active execution is
    /// not skipped merely because host shutdown arrived first.
    ///
    /// This method creates no task or runtime. Cancellation is a host-drain
    /// signal, not a durable AgentRun tree-cancel request, and never requests
    /// automatic resume.
    pub async fn run(self, cancel: CancellationToken) -> AgentSupervisorExit {
        let Self {
            owner,
            store,
            executor,
            executor_kind,
            adapters,
            completion_sinks,
            lease_owner,
            reconciler,
            execution,
            recovery,
            completion,
            schedule,
        } = self;
        let execution_loop = run_execution_loop(
            ExecutionLoopBackend {
                owner: owner.clone(),
                store: Arc::clone(&store),
                executor,
                executor_kind,
            },
            lease_owner,
            execution,
            schedule.clone(),
            cancel.clone(),
        );
        let recovery_loop = run_recovery_loop(
            RecoveryLoopBackend {
                owner,
                store,
                adapters,
                completion_sinks,
            },
            reconciler,
            recovery,
            schedule.clone(),
            cancel,
        );
        let completion_stop = CancellationToken::new();
        let completion_loop =
            run_completion_loop(&completion, schedule, completion_stop.clone(), true);
        let work_loops = async move {
            let (execution, recovery) = tokio::join!(execution_loop, recovery_loop);
            completion_stop.cancel();
            (execution, recovery)
        };
        let ((execution, recovery), completion) = tokio::join!(work_loops, completion_loop);
        AgentSupervisorExit {
            execution,
            recovery,
            completion,
        }
    }
}

impl fmt::Debug for ResidentAgentSupervisor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResidentAgentSupervisor")
            .finish_non_exhaustive()
    }
}

/// Backwards-compatible in-process facade over the generic resident host.
pub struct ResidentInProcessAgentSupervisor {
    inner: ResidentAgentSupervisor,
}

impl ResidentInProcessAgentSupervisor {
    pub(crate) fn new(
        components: InProcessAgentComponents,
        lease_owner: String,
        reconciler: String,
        execution: AgentExecutionOptions,
        recovery: AgentRecoveryOptions,
        schedule: AgentSupervisorOptions,
    ) -> Result<Self, AgentSupervisorError> {
        ResidentAgentSupervisor::new(
            components.into_resident_backend(),
            lease_owner,
            reconciler,
            execution,
            recovery,
            schedule,
        )
        .map(|inner| Self { inner })
    }

    pub async fn run(self, cancel: CancellationToken) -> AgentSupervisorExit {
        self.inner.run(cancel).await
    }
}

async fn run_completion_loop(
    publisher: &AgentCompletionPublisher,
    schedule: AgentSupervisorOptions,
    cancel: CancellationToken,
    final_drain: bool,
) -> AgentSupervisorLoopExit {
    let mut stats = AgentSupervisorLoopExit::default();
    let mut backoff = Backoff::new(schedule.initial_error_backoff, schedule.max_error_backoff);
    loop {
        if cancel.is_cancelled() {
            if final_drain {
                drain_completion_once(publisher, &mut stats).await;
            }
            break;
        }
        stats.cycles = stats.cycles.saturating_add(1);
        let cycle = AssertUnwindSafe(publisher.project_once())
            .catch_unwind()
            .await;
        let next = match cycle {
            Ok(Ok(report)) => {
                stats.successful_cycles = stats.successful_cycles.saturating_add(1);
                stats.claimed = add_usize(stats.claimed, report.scanned);
                let degraded = report
                    .items
                    .iter()
                    .filter(|status| completion_degraded(**status))
                    .count();
                stats.degraded_items = add_usize(stats.degraded_items, degraded);
                if degraded == 0 {
                    backoff.reset();
                    if report.has_more {
                        NextStep::Yield
                    } else {
                        NextStep::Delay(schedule.recovery_poll_interval)
                    }
                } else {
                    NextStep::Delay(backoff.failure_delay())
                }
            }
            Ok(Err(_)) => {
                stats.failed_cycles = stats.failed_cycles.saturating_add(1);
                NextStep::Delay(backoff.failure_delay())
            }
            Err(_) => {
                stats.panicked_cycles = stats.panicked_cycles.saturating_add(1);
                NextStep::Delay(backoff.failure_delay())
            }
        };
        if cancel.is_cancelled() {
            continue;
        }
        if !perform_next(next, &cancel).await {
            continue;
        }
    }
    stats
}

async fn drain_completion_once(
    publisher: &AgentCompletionPublisher,
    stats: &mut AgentSupervisorLoopExit,
) {
    stats.cycles = stats.cycles.saturating_add(1);
    match AssertUnwindSafe(publisher.project_once())
        .catch_unwind()
        .await
    {
        Ok(Ok(report)) => {
            stats.successful_cycles = stats.successful_cycles.saturating_add(1);
            stats.claimed = add_usize(stats.claimed, report.scanned);
            stats.degraded_items = add_usize(
                stats.degraded_items,
                report
                    .items
                    .iter()
                    .filter(|status| completion_degraded(**status))
                    .count(),
            );
        }
        Ok(Err(_)) => stats.failed_cycles = stats.failed_cycles.saturating_add(1),
        Err(_) => stats.panicked_cycles = stats.panicked_cycles.saturating_add(1),
    }
}

impl fmt::Debug for ResidentInProcessAgentSupervisor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResidentInProcessAgentSupervisor")
            .finish_non_exhaustive()
    }
}

async fn run_execution_loop(
    backend: ExecutionLoopBackend,
    lease_owner: String,
    options: AgentExecutionOptions,
    schedule: AgentSupervisorOptions,
    cancel: CancellationToken,
) -> AgentSupervisorLoopExit {
    let ExecutionLoopBackend {
        owner,
        store,
        executor,
        executor_kind,
    } = backend;
    let mut stats = AgentSupervisorLoopExit::default();
    let mut backoff = Backoff::new(schedule.initial_error_backoff, schedule.max_error_backoff);
    loop {
        if cancel.is_cancelled() {
            break;
        }
        stats.cycles = stats.cycles.saturating_add(1);
        let driver = AgentRunExecutionDriver::new_with_executor_kind(
            owner.clone(),
            Arc::clone(&store),
            lease_owner.clone(),
            options.clone(),
            Arc::clone(&executor),
            executor_kind,
        );
        let next = match driver {
            Ok(driver) => {
                let cycle =
                    AssertUnwindSafe(driver.execute_once_cooperative_shutdown(cancel.clone()))
                        .catch_unwind()
                        .await;
                match cycle {
                    Ok(Ok(report)) => {
                        stats.successful_cycles = stats.successful_cycles.saturating_add(1);
                        stats.claimed = add_usize(stats.claimed, report.claimed);
                        let degraded = report
                            .items
                            .iter()
                            .filter(|status| execution_degraded(**status))
                            .count();
                        stats.degraded_items = add_usize(stats.degraded_items, degraded);
                        if degraded == 0 {
                            backoff.reset();
                            if report.claimed == options.batch_limit {
                                NextStep::Yield
                            } else {
                                NextStep::Delay(schedule.execution_poll_interval)
                            }
                        } else {
                            NextStep::Delay(backoff.failure_delay())
                        }
                    }
                    Ok(Err(_)) => {
                        stats.failed_cycles = stats.failed_cycles.saturating_add(1);
                        NextStep::Delay(backoff.failure_delay())
                    }
                    Err(_) => {
                        stats.panicked_cycles = stats.panicked_cycles.saturating_add(1);
                        NextStep::Delay(backoff.failure_delay())
                    }
                }
            }
            Err(_) => {
                stats.failed_cycles = stats.failed_cycles.saturating_add(1);
                NextStep::Delay(backoff.failure_delay())
            }
        };
        if cancel.is_cancelled() || !perform_next(next, &cancel).await {
            break;
        }
    }
    stats
}

async fn run_recovery_loop(
    backend: RecoveryLoopBackend,
    reconciler: String,
    options: AgentRecoveryOptions,
    schedule: AgentSupervisorOptions,
    cancel: CancellationToken,
) -> AgentSupervisorLoopExit {
    run_recovery_loop_source(
        RecoveryLoopSource::Raw {
            backend,
            reconciler,
            options,
        },
        schedule,
        cancel,
    )
    .await
}

async fn run_frozen_recovery_loop(
    template: AgentRunRecoveryDriver,
    schedule: AgentSupervisorOptions,
    cancel: CancellationToken,
) -> AgentSupervisorLoopExit {
    run_recovery_loop_source(RecoveryLoopSource::Frozen(template), schedule, cancel).await
}

enum RecoveryLoopSource {
    Raw {
        backend: RecoveryLoopBackend,
        reconciler: String,
        options: AgentRecoveryOptions,
    },
    Frozen(AgentRunRecoveryDriver),
}

async fn run_recovery_loop_source(
    source: RecoveryLoopSource,
    schedule: AgentSupervisorOptions,
    cancel: CancellationToken,
) -> AgentSupervisorLoopExit {
    let options = match &source {
        RecoveryLoopSource::Raw { options, .. } => options.clone(),
        RecoveryLoopSource::Frozen(template) => template.options_for_supervisor(),
    };
    let mut stats = AgentSupervisorLoopExit::default();
    let mut backoff = Backoff::new(schedule.initial_error_backoff, schedule.max_error_backoff);
    loop {
        if cancel.is_cancelled() {
            break;
        }
        stats.cycles = stats.cycles.saturating_add(1);
        let driver = match &source {
            RecoveryLoopSource::Raw {
                backend,
                reconciler,
                options,
            } => AgentRunRecoveryDriver::new_with_completion_sinks(
                backend.owner.clone(),
                Arc::clone(&backend.store),
                reconciler.clone(),
                options.clone(),
                backend.adapters.clone(),
                backend.completion_sinks.clone(),
            ),
            RecoveryLoopSource::Frozen(template) => Ok(template.clone_frozen()),
        };
        let next = match driver {
            Ok(driver) => {
                let cycle = AssertUnwindSafe(driver.recover_once(CancellationToken::new()))
                    .catch_unwind()
                    .await;
                match cycle {
                    Ok(Ok(report)) => {
                        stats.successful_cycles = stats.successful_cycles.saturating_add(1);
                        stats.claimed = add_usize(stats.claimed, report.claimed);
                        let degraded = report
                            .items
                            .iter()
                            .filter(|status| recovery_degraded(**status))
                            .count();
                        stats.degraded_items = add_usize(stats.degraded_items, degraded);
                        if degraded == 0 {
                            backoff.reset();
                            if report.claimed == options.batch_limit {
                                NextStep::Yield
                            } else {
                                NextStep::Delay(schedule.recovery_poll_interval)
                            }
                        } else {
                            NextStep::Delay(backoff.failure_delay())
                        }
                    }
                    Ok(Err(_)) => {
                        stats.failed_cycles = stats.failed_cycles.saturating_add(1);
                        NextStep::Delay(backoff.failure_delay())
                    }
                    Err(_) => {
                        stats.panicked_cycles = stats.panicked_cycles.saturating_add(1);
                        NextStep::Delay(backoff.failure_delay())
                    }
                }
            }
            Err(_) => {
                stats.failed_cycles = stats.failed_cycles.saturating_add(1);
                NextStep::Delay(backoff.failure_delay())
            }
        };
        if cancel.is_cancelled() || !perform_next(next, &cancel).await {
            break;
        }
    }
    stats
}

fn validate_schedule(options: &AgentSupervisorOptions) -> Result<(), AgentSupervisorError> {
    let values = [
        options.execution_poll_interval,
        options.recovery_poll_interval,
        options.initial_error_backoff,
        options.max_error_backoff,
    ];
    if values
        .iter()
        .any(|value| value.is_zero() || *value > MAX_SCHEDULE_DELAY)
        || options.initial_error_backoff > options.max_error_backoff
    {
        return Err(AgentSupervisorError::InvalidSchedule);
    }
    Ok(())
}

fn execution_degraded(status: AgentExecutionItemStatus) -> bool {
    status != AgentExecutionItemStatus::Settled
}

fn recovery_degraded(status: AgentRecoveryItemStatus) -> bool {
    !matches!(
        status,
        AgentRecoveryItemStatus::RecoveredWithoutController
            | AgentRecoveryItemStatus::RecoveredAfterControllerGone
            | AgentRecoveryItemStatus::CompletionRecovered
            | AgentRecoveryItemStatus::CompletionAbsent
    )
}

fn completion_degraded(status: AgentCompletionProjectionStatus) -> bool {
    !matches!(
        status,
        AgentCompletionProjectionStatus::Unrelated
            | AgentCompletionProjectionStatus::Published
            | AgentCompletionProjectionStatus::Discarded
    )
}

fn add_usize(current: u64, value: usize) -> u64 {
    current.saturating_add(u64::try_from(value).unwrap_or(u64::MAX))
}

enum NextStep {
    Yield,
    Delay(Duration),
}

async fn perform_next(step: NextStep, cancel: &CancellationToken) -> bool {
    match step {
        NextStep::Yield => tokio::select! {
            biased;
            () = cancel.cancelled() => false,
            () = tokio::task::yield_now() => true,
        },
        NextStep::Delay(delay) => tokio::select! {
            biased;
            () = cancel.cancelled() => false,
            () = tokio::time::sleep(delay) => true,
        },
    }
}

struct Backoff {
    initial: Duration,
    maximum: Duration,
    current: Duration,
}

impl Backoff {
    fn new(initial: Duration, maximum: Duration) -> Self {
        Self {
            initial,
            maximum,
            current: initial,
        }
    }

    fn reset(&mut self) {
        self.current = self.initial;
    }

    fn failure_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = self
            .current
            .checked_mul(2)
            .unwrap_or(self.maximum)
            .min(self.maximum);
        delay
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
    use static_assertions::{assert_impl_all, assert_not_impl_any};
    use tokio::sync::Notify;
    use vyane_agent::{
        ActiveExecutionPermit, AgentClock, AgentStore, ControllerKind, ControllerRef, NewAgentRun,
        NewRunCompletion, NewWorker, RunFailureCode, RunMode, RunState, SqliteAgentStore,
    };

    use super::*;
    use crate::{
        AgentCompletionSink, AgentCompletionSinkObservation, AgentCompletionSinkTransition,
        AgentExecutionIdentity, AgentExecutionSettlement, AgentExecutorOutcome,
        InProcessAgentOperation, InProcessAgentOperationContext, InProcessEffectAuthority,
    };

    const OWNER_BOUNDS: &str = "supervisor-bounds";
    const OWNER_DRAIN: &str = "supervisor-drain";
    const OWNER_RECOVERY: &str = "supervisor-recovery";
    const OWNER_COMPLETION: &str = "supervisor-completion";
    const OWNER_PROCESS: &str = "supervisor-process";
    const OWNER_PROCESS_SHUTDOWN: &str = "supervisor-process-shutdown";

    #[test]
    fn completed_reconciliation_outcomes_are_not_degraded() {
        assert!(!recovery_degraded(
            AgentRecoveryItemStatus::CompletionRecovered
        ));
        assert!(!recovery_degraded(
            AgentRecoveryItemStatus::CompletionAbsent
        ));
        assert!(recovery_degraded(
            AgentRecoveryItemStatus::CompletionUnavailable
        ));
    }

    #[derive(Debug)]
    struct TestClock(Mutex<DateTime<Utc>>);

    impl TestClock {
        fn new() -> Self {
            Self(Mutex::new(
                Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 0)
                    .single()
                    .unwrap(),
            ))
        }

        fn advance(&self, seconds: i64) {
            *self.0.lock().unwrap() += TimeDelta::seconds(seconds);
        }
    }

    impl AgentClock for TestClock {
        fn now(&self) -> DateTime<Utc> {
            *self.0.lock().unwrap()
        }
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        clock: Arc<TestClock>,
        store: Arc<SqliteAgentStore>,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let clock = Arc::new(TestClock::new());
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

        fn enqueue(&self, owner: &str, suffix: &str, max_resume_attempts: u32) {
            self.enqueue_for_backend(
                owner,
                suffix,
                max_resume_attempts,
                vyane_agent::ExecutionBackend::CliHarnessProcess,
            );
        }

        fn enqueue_in_process(&self, owner: &str, suffix: &str, max_resume_attempts: u32) {
            self.enqueue_for_backend(
                owner,
                suffix,
                max_resume_attempts,
                vyane_agent::ExecutionBackend::NativeInProcess,
            );
        }

        fn enqueue_for_backend(
            &self,
            owner: &str,
            suffix: &str,
            max_resume_attempts: u32,
            execution_backend: vyane_agent::ExecutionBackend,
        ) {
            let worker = NewWorker {
                id: format!("worker-{suffix}"),
                logical_session_id: None,
            };
            self.store
                .create_root(
                    owner,
                    &worker,
                    &NewAgentRun {
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
                        available_at: self.clock.now(),
                        timeout_seconds: 60,
                        max_resume_attempts,
                    },
                )
                .unwrap();
        }
    }

    struct BlockingOperation {
        owner: &'static str,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl InProcessAgentOperation for BlockingOperation {
        fn name(&self) -> &str {
            "supervisor-blocking"
        }

        fn owner(&self) -> &str {
            self.owner
        }

        fn admit(
            &self,
            _identity: &AgentExecutionIdentity,
            _controller: &vyane_agent::ControllerRef,
        ) -> bool {
            true
        }

        async fn execute(
            &self,
            _context: InProcessAgentOperationContext,
            identity: AgentExecutionIdentity,
            authority: InProcessEffectAuthority<'_>,
        ) -> AgentExecutorOutcome {
            if authority
                .authorize(crate::InProcessAgentEffect::Other)
                .await
                .is_err()
            {
                return AgentExecutorOutcome::Unknown;
            }
            self.entered.notify_one();
            self.release.notified().await;
            let prepared = match authority
                .prepare_completion(NewRunCompletion {
                    id: format!("completion-{}", identity.run_id()),
                    sink_kind: "test-sink".into(),
                    publication_key: format!("result.{}", identity.run_id()),
                    content_digest: "c".repeat(64),
                    content_bytes: 1,
                })
                .await
            {
                Ok(prepared) => prepared,
                Err(_) => return AgentExecutorOutcome::Unknown,
            };
            match prepared.stage_blocking(|_| true).await {
                Ok(staged) => AgentExecutorOutcome::Quiesced(
                    AgentExecutionSettlement::CompletionStaged(staged),
                ),
                Err(_) => AgentExecutorOutcome::Unknown,
            }
        }
    }

    struct UnknownOperation {
        owner: &'static str,
        entered: Arc<Notify>,
    }

    struct ProcessStyleExecutor;

    struct StaticExecutor {
        kind: ControllerKind,
    }

    struct StaticAdapter {
        name: &'static str,
        kind: ControllerKind,
    }

    struct PanickingExecutor;

    struct PanickingRuntimeExecutor;

    struct PanickingAdapter;

    struct CountingAdapter {
        name: &'static str,
        kind: ControllerKind,
        observations: Arc<AtomicUsize>,
    }

    struct FlappingAdapter {
        calls: Arc<AtomicUsize>,
    }

    struct LateCompletionExecutor {
        owner: &'static str,
        store: Arc<dyn AgentStore>,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl AgentRunExecutor for StaticExecutor {
        fn kind(&self) -> ControllerKind {
            self.kind
        }

        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            controller: &ControllerRef,
        ) -> bool {
            controller.kind == self.kind
        }

        async fn execute(
            &self,
            _context: crate::AgentExecutionContext,
            _identity: AgentExecutionIdentity,
            _permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            AgentExecutorOutcome::Quiesced(AgentExecutionSettlement::Failed {
                code: RunFailureCode::DispatchFailed,
            })
        }
    }

    #[async_trait]
    impl AgentRunExecutor for PanickingExecutor {
        fn kind(&self) -> ControllerKind {
            panic!("metadata panic")
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
            _context: crate::AgentExecutionContext,
            _identity: AgentExecutionIdentity,
            _permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            AgentExecutorOutcome::Unknown
        }
    }

    #[async_trait]
    impl AgentRunExecutor for PanickingRuntimeExecutor {
        fn kind(&self) -> ControllerKind {
            ControllerKind::Process
        }

        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            controller: &ControllerRef,
        ) -> bool {
            controller.kind == ControllerKind::Process
        }

        async fn execute(
            &self,
            _context: crate::AgentExecutionContext,
            _identity: AgentExecutionIdentity,
            _permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            panic!("runtime panic")
        }
    }

    #[async_trait]
    impl AgentControllerAdapter for StaticAdapter {
        fn name(&self) -> &str {
            self.name
        }

        fn kind(&self) -> ControllerKind {
            self.kind
        }

        async fn observe_gone(
            &self,
            _context: crate::ControllerRecoveryContext,
            _controller: ControllerRef,
        ) -> crate::ControllerRecoveryObservation {
            crate::ControllerRecoveryObservation::Gone
        }
    }

    #[async_trait]
    impl AgentControllerAdapter for PanickingAdapter {
        fn name(&self) -> &str {
            "panicking"
        }

        fn kind(&self) -> ControllerKind {
            panic!("adapter metadata panic")
        }

        async fn observe_gone(
            &self,
            _context: crate::ControllerRecoveryContext,
            _controller: ControllerRef,
        ) -> crate::ControllerRecoveryObservation {
            crate::ControllerRecoveryObservation::Unavailable
        }
    }

    #[async_trait]
    impl AgentControllerAdapter for CountingAdapter {
        fn name(&self) -> &str {
            self.name
        }

        fn kind(&self) -> ControllerKind {
            self.kind
        }

        async fn observe_gone(
            &self,
            _context: crate::ControllerRecoveryContext,
            _controller: ControllerRef,
        ) -> crate::ControllerRecoveryObservation {
            self.observations.fetch_add(1, Ordering::SeqCst);
            crate::ControllerRecoveryObservation::Gone
        }
    }

    #[async_trait]
    impl AgentControllerAdapter for FlappingAdapter {
        fn name(&self) -> &str {
            "flapping"
        }

        fn kind(&self) -> ControllerKind {
            if self.calls.fetch_add(1, Ordering::SeqCst) < 2 {
                ControllerKind::Process
            } else {
                ControllerKind::InProcess
            }
        }

        async fn observe_gone(
            &self,
            _context: crate::ControllerRecoveryContext,
            _controller: ControllerRef,
        ) -> crate::ControllerRecoveryObservation {
            crate::ControllerRecoveryObservation::Gone
        }
    }

    #[async_trait]
    impl AgentRunExecutor for LateCompletionExecutor {
        fn kind(&self) -> ControllerKind {
            ControllerKind::InProcess
        }

        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            controller: &ControllerRef,
        ) -> bool {
            controller.kind == ControllerKind::InProcess
        }

        async fn execute(
            &self,
            _context: crate::AgentExecutionContext,
            identity: AgentExecutionIdentity,
            permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            self.entered.notify_one();
            self.release.notified().await;
            let prepared = match self.store.prepare_completion(
                self.owner,
                &permit,
                &NewRunCompletion {
                    id: format!("completion-{}", identity.run_id()),
                    sink_kind: "test-sink".into(),
                    publication_key: format!("result.{}", identity.run_id()),
                    content_digest: "c".repeat(64),
                    content_bytes: 1,
                },
            ) {
                Ok(prepared) => prepared,
                Err(_) => return AgentExecutorOutcome::Unknown,
            };
            if self
                .store
                .validate_completion_permit(self.owner, &prepared.permit)
                .is_err()
            {
                return AgentExecutorOutcome::Unknown;
            }
            AgentExecutorOutcome::Quiesced(AgentExecutionSettlement::CompletionStaged(
                crate::StagedRunCompletion::new(prepared.permit),
            ))
        }
    }

    struct ShutdownAwareProcessExecutor {
        entered: Arc<Notify>,
        cancellation_seen: Arc<Notify>,
        returned: Arc<AtomicBool>,
        dropped_before_return: Arc<AtomicBool>,
    }

    struct ExecutorReturnGuard {
        returned: Arc<AtomicBool>,
        dropped_before_return: Arc<AtomicBool>,
    }

    impl Drop for ExecutorReturnGuard {
        fn drop(&mut self) {
            if !self.returned.load(Ordering::SeqCst) {
                self.dropped_before_return.store(true, Ordering::SeqCst);
            }
        }
    }

    #[async_trait]
    impl AgentRunExecutor for ProcessStyleExecutor {
        fn kind(&self) -> ControllerKind {
            ControllerKind::Process
        }

        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            controller: &ControllerRef,
        ) -> bool {
            controller.kind == ControllerKind::Process && controller.fingerprint.is_some()
        }

        async fn execute(
            &self,
            _context: crate::AgentExecutionContext,
            _identity: AgentExecutionIdentity,
            _permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            AgentExecutorOutcome::Quiesced(AgentExecutionSettlement::Failed {
                code: RunFailureCode::DispatchFailed,
            })
        }
    }

    #[async_trait]
    impl AgentRunExecutor for ShutdownAwareProcessExecutor {
        fn kind(&self) -> ControllerKind {
            ControllerKind::Process
        }

        fn admit_controller(
            &self,
            _identity: &AgentExecutionIdentity,
            controller: &ControllerRef,
        ) -> bool {
            controller.kind == ControllerKind::Process && controller.fingerprint.is_some()
        }

        async fn execute(
            &self,
            context: crate::AgentExecutionContext,
            _identity: AgentExecutionIdentity,
            _permit: ActiveExecutionPermit,
        ) -> AgentExecutorOutcome {
            let _guard = ExecutorReturnGuard {
                returned: Arc::clone(&self.returned),
                dropped_before_return: Arc::clone(&self.dropped_before_return),
            };
            self.entered.notify_one();
            context.cancellation().cancelled().await;
            self.cancellation_seen.notify_one();
            self.returned.store(true, Ordering::SeqCst);
            AgentExecutorOutcome::Quiesced(AgentExecutionSettlement::Failed {
                code: RunFailureCode::DispatchFailed,
            })
        }
    }

    struct ProcessStyleAdapter;

    #[async_trait]
    impl AgentControllerAdapter for ProcessStyleAdapter {
        fn name(&self) -> &str {
            "process-style-test"
        }

        fn kind(&self) -> ControllerKind {
            ControllerKind::Process
        }

        async fn observe_gone(
            &self,
            _context: crate::ControllerRecoveryContext,
            _controller: ControllerRef,
        ) -> crate::ControllerRecoveryObservation {
            crate::ControllerRecoveryObservation::Gone
        }
    }

    struct PublishingSink {
        published: AtomicUsize,
        notification: Notify,
    }

    #[async_trait]
    impl AgentCompletionSink for PublishingSink {
        fn kind(&self) -> &str {
            "test-sink"
        }

        async fn inspect(
            &self,
            _: vyane_agent::RunCompletionRecord,
        ) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Exact
        }

        async fn publish(
            &self,
            _: vyane_agent::RunCompletionRecord,
        ) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Exact
        }

        async fn discard(
            &self,
            _: vyane_agent::RunCompletionRecord,
        ) -> AgentCompletionSinkObservation {
            AgentCompletionSinkObservation::Exact
        }

        async fn publish_transition(
            &self,
            _: vyane_agent::RunCompletionRecord,
        ) -> AgentCompletionSinkTransition {
            self.published.fetch_add(1, Ordering::SeqCst);
            self.notification.notify_one();
            AgentCompletionSinkTransition::Complete
        }
    }

    #[async_trait]
    impl InProcessAgentOperation for UnknownOperation {
        fn name(&self) -> &str {
            "supervisor-unknown"
        }

        fn owner(&self) -> &str {
            self.owner
        }

        fn admit(
            &self,
            _identity: &AgentExecutionIdentity,
            _controller: &vyane_agent::ControllerRef,
        ) -> bool {
            true
        }

        async fn execute(
            &self,
            _context: InProcessAgentOperationContext,
            _identity: AgentExecutionIdentity,
            _authority: InProcessEffectAuthority<'_>,
        ) -> AgentExecutorOutcome {
            self.entered.notify_one();
            AgentExecutorOutcome::Unknown
        }
    }

    fn schedule() -> AgentSupervisorOptions {
        AgentSupervisorOptions {
            execution_poll_interval: Duration::from_millis(5),
            recovery_poll_interval: Duration::from_millis(5),
            initial_error_backoff: Duration::from_millis(5),
            max_error_backoff: Duration::from_millis(20),
        }
    }

    fn execution_options() -> AgentExecutionOptions {
        AgentExecutionOptions {
            batch_limit: 1,
            max_in_flight: 1,
            lease_seconds: 1,
            heartbeat_interval: Duration::from_millis(100),
        }
    }

    fn recovery_options() -> AgentRecoveryOptions {
        AgentRecoveryOptions {
            batch_limit: 1,
            max_in_flight: 1,
            adapter_timeout: Duration::from_millis(100),
            settlement_margin: Duration::from_millis(100),
            operation_lease_seconds: 1,
        }
    }

    fn static_lane(kind: ControllerKind, lease_owner: &str) -> ResidentAgentExecutionLane {
        ResidentAgentExecutionLane::new(
            Arc::new(StaticExecutor { kind }),
            lease_owner,
            execution_options(),
        )
    }

    fn static_adapter(name: &'static str, kind: ControllerKind) -> Arc<dyn AgentControllerAdapter> {
        Arc::new(StaticAdapter { name, kind })
    }

    #[test]
    fn multi_lane_construction_is_closed_and_side_effect_free() {
        assert_impl_all!(ResidentAgentExecutionLane: Send, Sync);
        assert_impl_all!(ResidentAgentHost: Send, Sync);
        assert_not_impl_any!(ResidentAgentExecutionLane: Clone);
        assert_not_impl_any!(ResidentAgentHost: Clone);

        let fixture = Fixture::new();
        fixture.enqueue(OWNER_BOUNDS, "construction", 0);
        let store: Arc<dyn AgentStore> = fixture.store.clone();
        let make = |lanes, adapters| {
            ResidentAgentHost::new(
                ResidentAgentHostBackend::new(
                    OWNER_BOUNDS,
                    Arc::clone(&store),
                    adapters,
                    Vec::new(),
                ),
                lanes,
                "reconciler",
                recovery_options(),
                schedule(),
            )
        };

        assert!(matches!(
            make(Vec::new(), Vec::new()),
            Err(AgentSupervisorError::InvalidLaneCount)
        ));
        assert!(matches!(
            make(
                vec![
                    static_lane(ControllerKind::Process, "lease-a"),
                    static_lane(ControllerKind::InProcess, "lease-b"),
                    static_lane(ControllerKind::Remote, "lease-c"),
                    static_lane(ControllerKind::Process, "lease-d"),
                ],
                Vec::new(),
            ),
            Err(AgentSupervisorError::InvalidLaneCount)
        ));
        assert!(matches!(
            make(
                vec![
                    static_lane(ControllerKind::Process, "lease-a"),
                    static_lane(ControllerKind::Process, "lease-b"),
                ],
                vec![static_adapter("process", ControllerKind::Process)],
            ),
            Err(AgentSupervisorError::DuplicateExecutionBackend)
        ));
        assert!(matches!(
            make(
                vec![
                    static_lane(ControllerKind::Process, "lease"),
                    static_lane(ControllerKind::InProcess, "lease"),
                ],
                vec![
                    static_adapter("process", ControllerKind::Process),
                    static_adapter("native", ControllerKind::InProcess),
                ],
            ),
            Err(AgentSupervisorError::DuplicateLeaseOwner)
        ));
        assert!(matches!(
            make(
                vec![static_lane(ControllerKind::InProcess, "lease")],
                vec![static_adapter("process", ControllerKind::Process)],
            ),
            Err(AgentSupervisorError::MissingRecoveryAdapter)
        ));
        assert!(matches!(
            make(
                vec![static_lane(ControllerKind::Process, "lease")],
                vec![
                    static_adapter("process-a", ControllerKind::Process),
                    static_adapter("process-b", ControllerKind::Process),
                ],
            ),
            Err(AgentSupervisorError::InvalidRecovery)
        ));
        assert!(matches!(
            make(
                vec![static_lane(ControllerKind::Process, "lease")],
                vec![Arc::new(PanickingAdapter)],
            ),
            Err(AgentSupervisorError::InvalidRecovery)
        ));
        assert!(matches!(
            make(
                vec![ResidentAgentExecutionLane::new(
                    Arc::new(PanickingExecutor),
                    "lease",
                    execution_options(),
                )],
                vec![static_adapter("process", ControllerKind::Process)],
            ),
            Err(AgentSupervisorError::InvalidExecution)
        ));

        // Every rejected constructor leaves the queued run untouched.
        assert_eq!(
            fixture
                .store
                .get_run(OWNER_BOUNDS, "run-construction")
                .unwrap()
                .unwrap()
                .state,
            RunState::Queued
        );
    }

    #[tokio::test]
    async fn multi_lane_host_drives_exact_backends_with_shared_loops() {
        let fixture = Fixture::new();
        fixture.enqueue_for_backend(
            OWNER_PROCESS,
            "multi-process",
            0,
            vyane_agent::ExecutionBackend::CliHarnessProcess,
        );
        fixture.enqueue_for_backend(
            OWNER_PROCESS,
            "multi-native",
            0,
            vyane_agent::ExecutionBackend::NativeInProcess,
        );
        let store: Arc<dyn AgentStore> = fixture.store.clone();
        let host = ResidentAgentHost::new(
            ResidentAgentHostBackend::new(
                OWNER_PROCESS,
                store,
                vec![
                    static_adapter("process", ControllerKind::Process),
                    static_adapter("native", ControllerKind::InProcess),
                    // Historical adapters remain valid even without an active lane.
                    static_adapter("remote-history", ControllerKind::Remote),
                ],
                Vec::new(),
            ),
            vec![
                static_lane(ControllerKind::Process, "process-lease"),
                static_lane(ControllerKind::InProcess, "native-lease"),
            ],
            "reconciler",
            recovery_options(),
            schedule(),
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(host.run(cancel.clone()));

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let process = fixture
                    .store
                    .get_run(OWNER_PROCESS, "run-multi-process")
                    .unwrap()
                    .unwrap();
                let native = fixture
                    .store
                    .get_run(OWNER_PROCESS, "run-multi-native")
                    .unwrap()
                    .unwrap();
                if process.state == RunState::Failed && native.state == RunState::Failed {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        let exit = task.await.unwrap();

        assert_eq!(exit.lanes.len(), 2);
        assert_eq!(exit.lanes[0].backend, ExecutionBackend::CliHarnessProcess);
        assert_eq!(exit.lanes[0].execution.claimed, 1);
        assert_eq!(exit.lanes[1].backend, ExecutionBackend::NativeInProcess);
        assert_eq!(exit.lanes[1].execution.claimed, 1);
        assert!(exit.recovery.cycles > 0);
        assert!(exit.completion.cycles > 0);
    }

    #[tokio::test]
    async fn recovery_uses_frozen_adapter_metadata_without_runtime_rereads() {
        let fixture = Fixture::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let store: Arc<dyn AgentStore> = fixture.store.clone();
        let host = ResidentAgentHost::new(
            ResidentAgentHostBackend::new(
                OWNER_PROCESS,
                store,
                vec![Arc::new(FlappingAdapter {
                    calls: Arc::clone(&calls),
                })],
                Vec::new(),
            ),
            vec![static_lane(ControllerKind::Process, "process-lease")],
            "reconciler",
            recovery_options(),
            schedule(),
        )
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let cancel = CancellationToken::new();
        let task = tokio::spawn(host.run(cancel.clone()));
        tokio::time::sleep(Duration::from_millis(30)).await;
        cancel.cancel();
        let exit = task.await.unwrap();

        assert!(exit.recovery.successful_cycles > 0);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn one_union_recovery_loop_routes_both_controller_kinds_once() {
        let fixture = Fixture::new();
        for (suffix, backend, kind, lease) in [
            (
                "union-process",
                ExecutionBackend::CliHarnessProcess,
                ControllerKind::Process,
                "seed-process",
            ),
            (
                "union-native",
                ExecutionBackend::NativeInProcess,
                ControllerKind::InProcess,
                "seed-native",
            ),
        ] {
            fixture.enqueue_for_backend(OWNER_RECOVERY, suffix, 0, backend);
            let claimed = fixture
                .store
                .claim_due(OWNER_RECOVERY, backend, lease, 1, 1)
                .unwrap()
                .remove(0);
            fixture
                .store
                .start(
                    OWNER_RECOVERY,
                    &claimed.receipt,
                    &ControllerRef {
                        kind,
                        id: format!("controller-{suffix}"),
                        fingerprint: Some(format!("fingerprint-{suffix}")),
                    },
                )
                .unwrap();
        }
        fixture.clock.advance(2);
        let process_observations = Arc::new(AtomicUsize::new(0));
        let native_observations = Arc::new(AtomicUsize::new(0));
        let store: Arc<dyn AgentStore> = fixture.store.clone();
        let host = ResidentAgentHost::new(
            ResidentAgentHostBackend::new(
                OWNER_RECOVERY,
                store,
                vec![
                    Arc::new(CountingAdapter {
                        name: "count-process",
                        kind: ControllerKind::Process,
                        observations: Arc::clone(&process_observations),
                    }),
                    Arc::new(CountingAdapter {
                        name: "count-native",
                        kind: ControllerKind::InProcess,
                        observations: Arc::clone(&native_observations),
                    }),
                ],
                Vec::new(),
            ),
            vec![
                static_lane(ControllerKind::Process, "process-lease"),
                static_lane(ControllerKind::InProcess, "native-lease"),
            ],
            "reconciler",
            AgentRecoveryOptions {
                batch_limit: 2,
                max_in_flight: 2,
                ..recovery_options()
            },
            schedule(),
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(host.run(cancel.clone()));

        tokio::time::timeout(Duration::from_secs(2), async {
            while process_observations.load(Ordering::SeqCst) != 1
                || native_observations.load(Ordering::SeqCst) != 1
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel.cancel();
        let exit = task.await.unwrap();

        assert_eq!(process_observations.load(Ordering::SeqCst), 1);
        assert_eq!(native_observations.load(Ordering::SeqCst), 1);
        assert_eq!(exit.recovery.claimed, 2);
    }

    #[tokio::test]
    async fn multi_lane_shutdown_final_pass_publishes_late_completion_once() {
        let fixture = Fixture::new();
        fixture.enqueue_in_process(OWNER_COMPLETION, "host-final", 0);
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let sink = Arc::new(PublishingSink {
            published: AtomicUsize::new(0),
            notification: Notify::new(),
        });
        let store: Arc<dyn AgentStore> = fixture.store.clone();
        let sink_port: Arc<dyn AgentCompletionSink> = sink.clone();
        let executor: Arc<dyn AgentRunExecutor> = Arc::new(LateCompletionExecutor {
            owner: OWNER_COMPLETION,
            store: Arc::clone(&store),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        let host = ResidentAgentHost::new(
            ResidentAgentHostBackend::new(
                OWNER_COMPLETION,
                store,
                vec![
                    static_adapter("native", ControllerKind::InProcess),
                    Arc::new(ProcessStyleAdapter),
                ],
                vec![sink_port],
            ),
            vec![
                ResidentAgentExecutionLane::new(executor, "native-lease", execution_options()),
                static_lane(ControllerKind::Process, "process-lease"),
            ],
            "reconciler",
            recovery_options(),
            AgentSupervisorOptions {
                recovery_poll_interval: Duration::from_secs(1),
                ..schedule()
            },
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(host.run(cancel.clone()));

        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        // The shared projector has completed its empty startup pass and is
        // sleeping well beyond the remainder of this test.
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel.cancel();
        release.notify_one();
        let exit = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();

        let final_run = fixture
            .store
            .get_run(OWNER_COMPLETION, "run-host-final")
            .unwrap()
            .unwrap();
        assert_eq!(sink.published.load(Ordering::SeqCst), 1);
        assert_eq!(exit.completion.cycles, 2);
        assert!(exit.completion.claimed > 0);
        assert_eq!(exit.completion.degraded_items, 0);
        assert_eq!(final_run.state, RunState::Succeeded);
    }

    #[tokio::test]
    async fn one_degraded_lane_does_not_block_another_backend() {
        let fixture = Fixture::new();
        fixture.enqueue_for_backend(
            OWNER_PROCESS,
            "isolated-process",
            0,
            ExecutionBackend::CliHarnessProcess,
        );
        fixture.enqueue_for_backend(
            OWNER_PROCESS,
            "isolated-native",
            0,
            ExecutionBackend::NativeInProcess,
        );
        let store: Arc<dyn AgentStore> = fixture.store.clone();
        let host = ResidentAgentHost::new(
            ResidentAgentHostBackend::new(
                OWNER_PROCESS,
                store,
                vec![
                    static_adapter("process", ControllerKind::Process),
                    static_adapter("native", ControllerKind::InProcess),
                ],
                Vec::new(),
            ),
            vec![
                ResidentAgentExecutionLane::new(
                    Arc::new(PanickingRuntimeExecutor),
                    "process-lease",
                    execution_options(),
                ),
                static_lane(ControllerKind::InProcess, "native-lease"),
            ],
            "reconciler",
            recovery_options(),
            schedule(),
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(host.run(cancel.clone()));

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let native = fixture
                    .store
                    .get_run(OWNER_PROCESS, "run-isolated-native")
                    .unwrap()
                    .unwrap();
                if native.state == RunState::Failed {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        let exit = task.await.unwrap();

        let process = exit
            .lanes
            .iter()
            .find(|lane| lane.backend == ExecutionBackend::CliHarnessProcess)
            .unwrap();
        let native = exit
            .lanes
            .iter()
            .find(|lane| lane.backend == ExecutionBackend::NativeInProcess)
            .unwrap();
        assert_eq!(process.execution.claimed, 1);
        assert_eq!(process.execution.degraded_items, 1);
        assert_eq!(native.execution.claimed, 1);
        assert_eq!(native.execution.degraded_items, 0);
    }

    #[tokio::test]
    async fn exact_lane_cannot_skip_earlier_other_backend_on_same_worker() {
        let fixture = Fixture::new();
        fixture.enqueue_for_backend(
            OWNER_PROCESS,
            "fifo",
            0,
            vyane_agent::ExecutionBackend::CliHarnessProcess,
        );
        let second = NewAgentRun {
            id: "run-fifo-native".into(),
            worker_id: "worker-fifo".into(),
            task_id: None,
            trace_id: None,
            parent_run_id: None,
            execution_backend: ExecutionBackend::NativeInProcess,
            mode: RunMode::Autonomous,
            target_key: "http:test/model".into(),
            prompt_digest: "a".repeat(64),
            policy_digest: "b".repeat(64),
            available_at: fixture.clock.now(),
            timeout_seconds: 60,
            max_resume_attempts: 0,
        };
        fixture.store.enqueue_run(OWNER_PROCESS, &second).unwrap();
        let store: Arc<dyn AgentStore> = fixture.store.clone();
        let host = ResidentAgentHost::new(
            ResidentAgentHostBackend::new(
                OWNER_PROCESS,
                store,
                vec![static_adapter("native", ControllerKind::InProcess)],
                Vec::new(),
            ),
            vec![static_lane(ControllerKind::InProcess, "native-lease")],
            "reconciler",
            recovery_options(),
            schedule(),
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(host.run(cancel.clone()));
        tokio::time::sleep(Duration::from_millis(40)).await;
        cancel.cancel();
        let exit = task.await.unwrap();

        assert_eq!(exit.lanes[0].execution.claimed, 0);
        assert_eq!(
            fixture
                .store
                .get_run(OWNER_PROCESS, "run-fifo-native")
                .unwrap()
                .unwrap()
                .state,
            RunState::Queued
        );
    }

    #[test]
    fn surface_is_non_clone_and_rejects_invalid_bounds_without_store_work() {
        assert_impl_all!(ResidentAgentBackend: Send, Sync);
        assert_impl_all!(ResidentAgentSupervisor: Send, Sync);
        assert_not_impl_any!(ResidentAgentBackend: Clone);
        assert_not_impl_any!(ResidentAgentSupervisor: Clone);
        assert_impl_all!(ResidentInProcessAgentSupervisor: Send, Sync);
        assert_not_impl_any!(ResidentInProcessAgentSupervisor: Clone);

        let fixture = Fixture::new();
        let operation = Arc::new(UnknownOperation {
            owner: OWNER_BOUNDS,
            entered: Arc::new(Notify::new()),
        });
        let store: Arc<dyn AgentStore> = fixture.store.clone();
        let components = InProcessAgentComponents::new(OWNER_BOUNDS, store, operation).unwrap();
        let result = components.into_resident_supervisor(
            "lease",
            "reconciler",
            execution_options(),
            recovery_options(),
            AgentSupervisorOptions {
                execution_poll_interval: Duration::ZERO,
                ..schedule()
            },
        );
        assert!(matches!(result, Err(AgentSupervisorError::InvalidSchedule)));
        assert!(
            fixture
                .store
                .claim_due(
                    OWNER_BOUNDS,
                    vyane_agent::ExecutionBackend::CliHarnessProcess,
                    "probe",
                    1,
                    1,
                )
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn generic_surface_drives_process_style_executor_and_adapter() {
        let fixture = Fixture::new();
        fixture.enqueue(OWNER_PROCESS, "process", 0);
        let store: Arc<dyn AgentStore> = fixture.store.clone();
        let executor: Arc<dyn AgentRunExecutor> = Arc::new(ProcessStyleExecutor);
        let adapter: Arc<dyn AgentControllerAdapter> = Arc::new(ProcessStyleAdapter);
        let supervisor = ResidentAgentSupervisor::new(
            ResidentAgentBackend::new(OWNER_PROCESS, store, executor, vec![adapter], Vec::new()),
            "lease",
            "reconciler",
            execution_options(),
            recovery_options(),
            schedule(),
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(supervisor.run(cancel.clone()));

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let run = fixture
                    .store
                    .get_run(OWNER_PROCESS, "run-process")
                    .unwrap()
                    .unwrap();
                if run.state == RunState::Failed {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        let exit = task.await.unwrap();

        assert_eq!(exit.execution.claimed, 1);
        assert_eq!(exit.execution.degraded_items, 0);
        assert_eq!(
            fixture
                .store
                .get_run(OWNER_PROCESS, "run-process")
                .unwrap()
                .unwrap()
                .failure_code,
            Some(RunFailureCode::DispatchFailed)
        );
    }

    #[tokio::test]
    async fn host_cancellation_is_cooperative_for_an_active_process_executor() {
        let fixture = Fixture::new();
        fixture.enqueue(OWNER_PROCESS_SHUTDOWN, "shutdown", 0);
        let entered = Arc::new(Notify::new());
        let cancellation_seen = Arc::new(Notify::new());
        let returned = Arc::new(AtomicBool::new(false));
        let dropped_before_return = Arc::new(AtomicBool::new(false));
        let executor: Arc<dyn AgentRunExecutor> = Arc::new(ShutdownAwareProcessExecutor {
            entered: Arc::clone(&entered),
            cancellation_seen: Arc::clone(&cancellation_seen),
            returned: Arc::clone(&returned),
            dropped_before_return: Arc::clone(&dropped_before_return),
        });
        let store: Arc<dyn AgentStore> = fixture.store.clone();
        let adapter: Arc<dyn AgentControllerAdapter> = Arc::new(ProcessStyleAdapter);
        let supervisor = ResidentAgentSupervisor::new(
            ResidentAgentBackend::new(
                OWNER_PROCESS_SHUTDOWN,
                store,
                executor,
                vec![adapter],
                Vec::new(),
            ),
            "lease",
            "reconciler",
            execution_options(),
            recovery_options(),
            schedule(),
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(supervisor.run(cancel.clone()));

        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), cancellation_seen.notified())
            .await
            .unwrap();
        let exit = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();

        assert!(returned.load(Ordering::SeqCst));
        assert!(!dropped_before_return.load(Ordering::SeqCst));
        assert_eq!(exit.execution.claimed, 1);
        assert_eq!(
            fixture
                .store
                .get_run(OWNER_PROCESS_SHUTDOWN, "run-shutdown")
                .unwrap()
                .unwrap()
                .failure_code,
            Some(RunFailureCode::DispatchFailed)
        );
    }

    #[tokio::test]
    async fn host_cancellation_reaches_an_active_in_process_executor() {
        let fixture = Fixture::new();
        fixture.enqueue_in_process(OWNER_DRAIN, "drain", 0);
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let operation = Arc::new(BlockingOperation {
            owner: OWNER_DRAIN,
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        let store: Arc<dyn AgentStore> = fixture.store.clone();
        let components = InProcessAgentComponents::new(OWNER_DRAIN, store, operation).unwrap();
        let supervisor = components
            .into_resident_supervisor(
                "lease",
                "reconciler",
                execution_options(),
                recovery_options(),
                schedule(),
            )
            .unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(supervisor.run(cancel.clone()));

        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        cancel.cancel();

        let exit = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exit.execution.claimed, 1);
        assert_eq!(exit.execution.degraded_items, 1);
        assert_eq!(
            fixture
                .store
                .get_run(OWNER_DRAIN, "run-drain")
                .unwrap()
                .unwrap()
                .state,
            RunState::Running
        );
        release.notify_one();
    }

    #[tokio::test]
    async fn resident_supervisor_publishes_committed_completion() {
        let fixture = Fixture::new();
        fixture.enqueue_in_process(OWNER_COMPLETION, "publish", 0);
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let operation = Arc::new(BlockingOperation {
            owner: OWNER_COMPLETION,
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        let sink = Arc::new(PublishingSink {
            published: AtomicUsize::new(0),
            notification: Notify::new(),
        });
        let erased_sink: Arc<dyn AgentCompletionSink> = sink.clone();
        let store: Arc<dyn AgentStore> = fixture.store.clone();
        let components = InProcessAgentComponents::new_with_completion_sinks(
            OWNER_COMPLETION,
            store,
            operation,
            vec![erased_sink],
        )
        .unwrap();
        let supervisor = components
            .into_resident_supervisor(
                "lease",
                "reconciler",
                execution_options(),
                recovery_options(),
                schedule(),
            )
            .unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(supervisor.run(cancel.clone()));

        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        let published = sink.notification.notified();
        tokio::pin!(published);
        published.as_mut().enable();
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), &mut published)
            .await
            .unwrap();
        cancel.cancel();
        let exit = task.await.unwrap();

        assert_eq!(sink.published.load(Ordering::SeqCst), 1);
        assert!(exit.completion.claimed >= 1);
        assert_eq!(exit.completion.degraded_items, 0);
        assert_eq!(
            fixture
                .store
                .get_run(OWNER_COMPLETION, "run-publish")
                .unwrap()
                .unwrap()
                .state,
            RunState::Succeeded
        );
    }

    #[tokio::test]
    async fn uncertain_execution_is_recovered_without_automatic_resume() {
        let fixture = Fixture::new();
        fixture.enqueue_in_process(OWNER_RECOVERY, "recover", 1);
        let entered = Arc::new(Notify::new());
        let operation = Arc::new(UnknownOperation {
            owner: OWNER_RECOVERY,
            entered: Arc::clone(&entered),
        });
        let store: Arc<dyn AgentStore> = fixture.store.clone();
        let components = InProcessAgentComponents::new(OWNER_RECOVERY, store, operation).unwrap();
        let supervisor = components
            .into_resident_supervisor(
                "lease",
                "reconciler",
                execution_options(),
                recovery_options(),
                schedule(),
            )
            .unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(supervisor.run(cancel.clone()));

        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        fixture.clock.advance(2);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let run = fixture
                    .store
                    .get_run(OWNER_RECOVERY, "run-recover")
                    .unwrap()
                    .unwrap();
                if run.state == RunState::Interrupted {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        let exit = task.await.unwrap();

        assert!(exit.execution.degraded_items >= 1);
        assert!(exit.recovery.claimed >= 1);
        assert_eq!(
            fixture
                .store
                .get_run(OWNER_RECOVERY, "run-recover")
                .unwrap()
                .unwrap()
                .state,
            RunState::Interrupted
        );
        assert!(
            fixture
                .store
                .claim_due(
                    OWNER_RECOVERY,
                    vyane_agent::ExecutionBackend::NativeInProcess,
                    "probe",
                    1,
                    10,
                )
                .unwrap()
                .is_empty()
        );
    }
}
