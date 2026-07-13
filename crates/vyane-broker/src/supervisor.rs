use std::collections::HashSet;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt as _, future::join_all};
use vyane_core::CancellationToken;
use vyane_message::{ClaimQuery, DeliveryMailbox, LeaseRequest};

use crate::broker::{validate_adapter, validate_pump_options};
use crate::{
    AgentEventProjector, BrokerError, DeliveryAdapter, MessageBroker, MessageEventProjector,
    PumpItemStatus, PumpOptions, Result,
};

const MAX_LANE_ID_BYTES: usize = 64;
const MAX_MESSAGE_PROJECTOR_ID_BYTES: usize = 128;
const MAX_AGENT_PROJECTOR_ID_BYTES: usize = 256;
const MAX_STREAM_ID_BYTES: usize = 128;
const MAX_SUPERVISOR_BATCH: usize = 128;
const MAX_TOTAL_IN_FLIGHT: usize = 256;
const MAX_SCHEDULE_DELAY: Duration = Duration::from_secs(24 * 60 * 60);

/// One immutable delivery-polling lane.
///
/// A lane owns a disjoint mailbox set, one replay-safe adapter and the exact
/// lease/pump policy used for every bounded cycle. It stores no claimed
/// delivery or message body and is deliberately not cloneable.
pub struct DeliveryLane {
    id: String,
    query: ClaimQuery,
    lease: LeaseRequest,
    adapter: Arc<dyn DeliveryAdapter>,
    adapter_name: String,
    pump: PumpOptions,
}

impl DeliveryLane {
    pub fn new(
        id: impl Into<String>,
        query: ClaimQuery,
        lease: LeaseRequest,
        adapter: Arc<dyn DeliveryAdapter>,
        pump: PumpOptions,
    ) -> Result<Self> {
        let id = id.into();
        validate_lane_id(&id)?;
        query.validate()?;
        lease.validate()?;
        let adapter_name =
            std::panic::catch_unwind(AssertUnwindSafe(|| validate_adapter(adapter.as_ref())))
                .map_err(|_| {
                    BrokerError::InvalidConfig("adapter identity validation panicked".into())
                })??;
        validate_pump_options(&lease, &pump)?;
        if pump.max_in_flight > MAX_TOTAL_IN_FLIGHT {
            return Err(BrokerError::InvalidConfig(format!(
                "lane concurrency must not exceed {MAX_TOTAL_IN_FLIGHT}"
            )));
        }
        Ok(Self {
            id,
            query,
            lease,
            adapter,
            adapter_name,
            pump,
        })
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    fn effective_in_flight(&self) -> usize {
        self.query.limit.min(self.pump.max_in_flight)
    }
}

impl fmt::Debug for DeliveryLane {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryLane")
            .field("id", &self.id)
            .field("mailbox_count", &self.query.mailboxes.len())
            .field("claim_limit", &self.query.limit)
            .field("adapter", &self.adapter_name)
            .field("adapter_timeout", &self.pump.adapter_timeout)
            .field("settlement_margin", &self.pump.settlement_margin)
            .field("max_in_flight", &self.pump.max_in_flight)
            .finish_non_exhaustive()
    }
}

/// Scheduling and resource bounds for an explicit resident broker run.
#[derive(Debug, Clone)]
pub struct SupervisorOptions {
    pub idle_poll_interval: Duration,
    pub maintenance_interval: Duration,
    pub initial_error_backoff: Duration,
    pub max_error_backoff: Duration,
    pub maintenance_limit: usize,
    pub projection_limit: usize,
    pub max_total_in_flight: usize,
}

impl Default for SupervisorOptions {
    fn default() -> Self {
        Self {
            idle_poll_interval: Duration::from_millis(250),
            maintenance_interval: Duration::from_secs(30),
            initial_error_backoff: Duration::from_millis(250),
            max_error_backoff: Duration::from_secs(30),
            maintenance_limit: 128,
            projection_limit: 16,
            max_total_in_flight: 32,
        }
    }
}

/// Safe, body-free counters for one resident loop at graceful exit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LoopExit {
    pub cycles: u64,
    pub successful_cycles: u64,
    pub failed_cycles: u64,
    pub panicked_cycles: u64,
    /// Claimed deliveries, maintenance transitions, or projected events.
    pub work_items: u64,
    /// Delivery items that timed out, panicked, were uncertain, lacked a safe
    /// lease window, or could not be settled.
    pub degraded_items: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryLoopExit {
    pub lane_id: String,
    pub stats: LoopExit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorExit {
    pub deliveries: Vec<DeliveryLoopExit>,
    pub maintenance: LoopExit,
    pub message_projection: LoopExit,
    pub agent_projection: LoopExit,
}

/// Explicit owner-bound resident driver over bounded broker primitives.
///
/// This value is deliberately not cloneable. [`Self::run`] does not spawn or
/// detach tasks, create a channel, discover a runtime, or keep a second queue.
/// The caller must poll the returned future on its own Tokio runtime, cancel
/// the supplied token, and await the future for graceful drain.
pub struct ResidentBrokerSupervisor {
    broker: MessageBroker,
    message_projector: MessageEventProjector,
    agent_projector: AgentEventProjector,
    lanes: Vec<DeliveryLane>,
    options: SupervisorOptions,
}

impl ResidentBrokerSupervisor {
    pub fn new(
        broker: MessageBroker,
        message_projector: MessageEventProjector,
        agent_projector: AgentEventProjector,
        lanes: Vec<DeliveryLane>,
        options: SupervisorOptions,
    ) -> Result<Self> {
        validate_options(&options)?;
        validate_projector(
            "message",
            message_projector.projector_id(),
            message_projector.stream_id(),
            MAX_MESSAGE_PROJECTOR_ID_BYTES,
        )?;
        validate_projector(
            "agent",
            agent_projector.projector_id(),
            agent_projector.stream_id(),
            MAX_AGENT_PROJECTOR_ID_BYTES,
        )?;
        if broker.scope() != message_projector.scope() || broker.scope() != agent_projector.scope()
        {
            return Err(BrokerError::InvalidConfig(
                "broker and projector owner scopes must match".into(),
            ));
        }
        if !Arc::ptr_eq(broker.store(), message_projector.store()) {
            return Err(BrokerError::InvalidConfig(
                "broker and message projector must share one message store".into(),
            ));
        }

        let mut lane_ids = HashSet::with_capacity(lanes.len());
        let mut mailboxes = HashSet::<DeliveryMailbox>::new();
        let mut total_in_flight = 0usize;
        for lane in &lanes {
            if !lane_ids.insert(lane.id.as_str()) {
                return Err(BrokerError::InvalidConfig(
                    "delivery lane ids must be unique".into(),
                ));
            }
            for mailbox in &lane.query.mailboxes {
                if !mailboxes.insert(mailbox.clone()) {
                    return Err(BrokerError::InvalidConfig(
                        "delivery mailboxes must belong to exactly one lane".into(),
                    ));
                }
            }
            total_in_flight = total_in_flight
                .checked_add(lane.effective_in_flight())
                .ok_or_else(|| {
                    BrokerError::InvalidConfig("delivery concurrency overflow".into())
                })?;
        }
        if total_in_flight > options.max_total_in_flight {
            return Err(BrokerError::InvalidConfig(
                "delivery lane concurrency exceeds the supervisor bound".into(),
            ));
        }

        Ok(Self {
            broker,
            message_projector,
            agent_projector,
            lanes,
            options,
        })
    }

    /// Run every loop concurrently until cancellation, then drain the exact
    /// operation already in progress in each loop before returning.
    ///
    /// No new cycle begins after a loop observes cancellation. The one-shot
    /// operations contain bounded batches and adapter deadlines. A caller may
    /// apply a wider outer timeout, but dropping this future forfeits the
    /// graceful-drain guarantee because an already-running blocking store call
    /// can outlive its async waiter.
    pub async fn run(self, cancel: CancellationToken) -> SupervisorExit {
        let Self {
            broker,
            message_projector,
            agent_projector,
            lanes,
            options,
        } = self;
        let delivery_runs = join_all(lanes.into_iter().map(|lane| {
            run_delivery_loop(
                broker.clone(),
                lane,
                options.idle_poll_interval,
                options.initial_error_backoff,
                options.max_error_backoff,
                cancel.clone(),
            )
        }));
        let maintenance_run = run_maintenance_loop(
            broker,
            options.maintenance_interval,
            options.maintenance_limit,
            options.initial_error_backoff,
            options.max_error_backoff,
            cancel.clone(),
        );
        let message_projection_run = run_message_projection_loop(
            message_projector,
            options.idle_poll_interval,
            options.projection_limit,
            options.initial_error_backoff,
            options.max_error_backoff,
            cancel.clone(),
        );
        let agent_projection_run = run_agent_projection_loop(
            agent_projector,
            options.idle_poll_interval,
            options.projection_limit,
            options.initial_error_backoff,
            options.max_error_backoff,
            cancel,
        );

        let (deliveries, maintenance, message_projection, agent_projection) = tokio::join!(
            delivery_runs,
            maintenance_run,
            message_projection_run,
            agent_projection_run
        );
        SupervisorExit {
            deliveries,
            maintenance,
            message_projection,
            agent_projection,
        }
    }
}

async fn run_delivery_loop(
    broker: MessageBroker,
    lane: DeliveryLane,
    idle_poll_interval: Duration,
    initial_error_backoff: Duration,
    max_error_backoff: Duration,
    cancel: CancellationToken,
) -> DeliveryLoopExit {
    let mut stats = LoopExit::default();
    let mut backoff = Backoff::new(initial_error_backoff, max_error_backoff);
    loop {
        if cancel.is_cancelled() {
            break;
        }
        stats.cycles = stats.cycles.saturating_add(1);
        let cycle = AssertUnwindSafe(broker.pump_once(
            lane.query.clone(),
            lane.lease.clone(),
            Arc::clone(&lane.adapter),
            lane.pump.clone(),
        ))
        .catch_unwind()
        .await;
        let next = match cycle {
            Ok(Ok(report)) => {
                stats.successful_cycles = stats.successful_cycles.saturating_add(1);
                stats.work_items = add_usize(stats.work_items, report.claimed);
                let degraded = report
                    .items
                    .iter()
                    .filter(|item| is_degraded(&item.status))
                    .count();
                stats.degraded_items = add_usize(stats.degraded_items, degraded);
                if degraded == 0 {
                    backoff.reset();
                    if report.claimed == 0 {
                        NextStep::Delay(idle_poll_interval)
                    } else {
                        NextStep::Yield
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
        if cancel.is_cancelled() || !perform_next(next, &cancel).await {
            break;
        }
    }
    DeliveryLoopExit {
        lane_id: lane.id,
        stats,
    }
}

async fn run_maintenance_loop(
    broker: MessageBroker,
    maintenance_interval: Duration,
    limit: usize,
    initial_error_backoff: Duration,
    max_error_backoff: Duration,
    cancel: CancellationToken,
) -> LoopExit {
    let mut stats = LoopExit::default();
    let mut backoff = Backoff::new(initial_error_backoff, max_error_backoff);
    loop {
        if cancel.is_cancelled() {
            break;
        }
        stats.cycles = stats.cycles.saturating_add(1);
        let cycle = AssertUnwindSafe(broker.maintenance_once(limit))
            .catch_unwind()
            .await;
        let next = match cycle {
            Ok(Ok(report)) => {
                stats.successful_cycles = stats.successful_cycles.saturating_add(1);
                stats.work_items = add_usize(stats.work_items, report.expired);
                stats.work_items = add_usize(stats.work_items, report.reclaimed);
                backoff.reset();
                if report.expired == limit || report.reclaimed == limit {
                    NextStep::Yield
                } else {
                    NextStep::Delay(maintenance_interval)
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
        if cancel.is_cancelled() || !perform_next(next, &cancel).await {
            break;
        }
    }
    stats
}

async fn run_message_projection_loop(
    projector: MessageEventProjector,
    idle_poll_interval: Duration,
    limit: usize,
    initial_error_backoff: Duration,
    max_error_backoff: Duration,
    cancel: CancellationToken,
) -> LoopExit {
    let mut stats = LoopExit::default();
    let mut backoff = Backoff::new(initial_error_backoff, max_error_backoff);
    loop {
        if cancel.is_cancelled() {
            break;
        }
        stats.cycles = stats.cycles.saturating_add(1);
        let cycle = AssertUnwindSafe(projector.project_once(limit))
            .catch_unwind()
            .await;
        let next = match cycle {
            Ok(Ok(report)) => {
                stats.successful_cycles = stats.successful_cycles.saturating_add(1);
                stats.work_items = add_usize(stats.work_items, report.projected);
                backoff.reset();
                if report.has_more {
                    NextStep::Yield
                } else {
                    NextStep::Delay(idle_poll_interval)
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
        if cancel.is_cancelled() || !perform_next(next, &cancel).await {
            break;
        }
    }
    stats
}

async fn run_agent_projection_loop(
    projector: AgentEventProjector,
    idle_poll_interval: Duration,
    limit: usize,
    initial_error_backoff: Duration,
    max_error_backoff: Duration,
    cancel: CancellationToken,
) -> LoopExit {
    let mut stats = LoopExit::default();
    let mut backoff = Backoff::new(initial_error_backoff, max_error_backoff);
    loop {
        if cancel.is_cancelled() {
            break;
        }
        stats.cycles = stats.cycles.saturating_add(1);
        let cycle = AssertUnwindSafe(projector.project_once(limit))
            .catch_unwind()
            .await;
        let next = match cycle {
            Ok(Ok(report)) => {
                stats.successful_cycles = stats.successful_cycles.saturating_add(1);
                stats.work_items = add_usize(stats.work_items, report.projected);
                backoff.reset();
                if report.has_more {
                    NextStep::Yield
                } else {
                    NextStep::Delay(idle_poll_interval)
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
        if cancel.is_cancelled() || !perform_next(next, &cancel).await {
            break;
        }
    }
    stats
}

fn validate_lane_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_LANE_ID_BYTES
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(BrokerError::InvalidConfig(
            "lane id must use lowercase ASCII identity characters".into(),
        ));
    }
    Ok(())
}

fn validate_options(options: &SupervisorOptions) -> Result<()> {
    if options.idle_poll_interval.is_zero()
        || options.maintenance_interval.is_zero()
        || options.initial_error_backoff.is_zero()
        || options.max_error_backoff.is_zero()
    {
        return Err(BrokerError::InvalidConfig(
            "supervisor durations must be greater than zero".into(),
        ));
    }
    if options.idle_poll_interval > MAX_SCHEDULE_DELAY
        || options.maintenance_interval > MAX_SCHEDULE_DELAY
        || options.initial_error_backoff > MAX_SCHEDULE_DELAY
        || options.max_error_backoff > MAX_SCHEDULE_DELAY
    {
        return Err(BrokerError::InvalidConfig(
            "supervisor durations must not exceed 24 hours".into(),
        ));
    }
    if options.initial_error_backoff > options.max_error_backoff {
        return Err(BrokerError::InvalidConfig(
            "initial error backoff must not exceed the maximum".into(),
        ));
    }
    if !(1..=MAX_SUPERVISOR_BATCH).contains(&options.maintenance_limit)
        || !(1..=MAX_SUPERVISOR_BATCH).contains(&options.projection_limit)
    {
        return Err(BrokerError::InvalidConfig(format!(
            "supervisor batch limits must be between 1 and {MAX_SUPERVISOR_BATCH}"
        )));
    }
    if !(1..=MAX_TOTAL_IN_FLIGHT).contains(&options.max_total_in_flight) {
        return Err(BrokerError::InvalidConfig(format!(
            "total delivery concurrency must be between 1 and {MAX_TOTAL_IN_FLIGHT}"
        )));
    }
    Ok(())
}

fn validate_projector(
    kind: &str,
    projector_id: &str,
    stream_id: &str,
    max_projector_id_bytes: usize,
) -> Result<()> {
    if projector_id.is_empty()
        || projector_id.len() > max_projector_id_bytes
        || projector_id.contains('\0')
        || projector_id.trim() != projector_id
        || projector_id.chars().any(char::is_control)
    {
        return Err(BrokerError::InvalidConfig(format!(
            "{kind} projector identity is invalid"
        )));
    }
    if stream_id.is_empty()
        || stream_id.len() > MAX_STREAM_ID_BYTES
        || !stream_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(BrokerError::InvalidConfig(format!(
            "{kind} projector stream identity is invalid"
        )));
    }
    Ok(())
}

fn is_degraded(status: &PumpItemStatus) -> bool {
    matches!(
        status,
        PumpItemStatus::InsufficientLeaseWindow
            | PumpItemStatus::Uncertain
            | PumpItemStatus::TimedOut
            | PumpItemStatus::AdapterPanicked
            | PumpItemStatus::SettlementFailed
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
