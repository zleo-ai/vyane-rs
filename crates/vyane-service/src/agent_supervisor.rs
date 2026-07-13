//! Explicit resident polling over one paired in-process AgentRun backend.
//!
//! The supervisor owns no runtime, task, channel, payload queue, or resume
//! policy. Host cancellation stops scheduling new passes and interrupts waits;
//! it deliberately does not cancel a pass that already owns controller work.

use std::fmt;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures::FutureExt as _;
use vyane_core::CancellationToken;

use crate::{
    AgentCompletionProjectionStatus, AgentCompletionPublisher, AgentCompletionPublisherOptions,
    AgentExecutionItemStatus, AgentExecutionOptions, AgentRecoveryItemStatus, AgentRecoveryOptions,
    InProcessAgentComponents,
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
}

impl fmt::Display for AgentSupervisorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidSchedule => "AgentRun supervisor schedule is invalid",
            Self::InvalidExecution => "AgentRun execution configuration is invalid",
            Self::InvalidRecovery => "AgentRun recovery configuration is invalid",
            Self::InvalidCompletion => "AgentRun completion configuration is invalid",
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

/// Non-cloneable resident driver over one exact paired in-process backend.
///
/// Cancelling the host token prevents a new pass and interrupts a scheduling
/// delay. A pass already in progress receives its own uncancelled token and is
/// awaited to completion. Dropping [`Self::run`] forfeits that graceful-drain
/// guarantee, and a blocking custom store call can outlive its async waiter.
pub struct ResidentInProcessAgentSupervisor {
    components: InProcessAgentComponents,
    lease_owner: String,
    reconciler: String,
    execution: AgentExecutionOptions,
    recovery: AgentRecoveryOptions,
    completion: AgentCompletionPublisher,
    schedule: AgentSupervisorOptions,
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
        validate_schedule(&schedule)?;
        components
            .execution_driver(lease_owner.clone(), execution.clone())
            .map_err(|_| AgentSupervisorError::InvalidExecution)?;
        components
            .recovery_driver(reconciler.clone(), recovery.clone())
            .map_err(|_| AgentSupervisorError::InvalidRecovery)?;
        let completion = components
            .completion_publisher(
                COMPLETION_PROJECTOR_ID,
                AgentCompletionPublisherOptions::default(),
            )
            .map_err(|_| AgentSupervisorError::InvalidCompletion)?;
        Ok(Self {
            components,
            lease_owner,
            reconciler,
            execution,
            recovery,
            completion,
            schedule,
        })
    }

    /// Poll execution, recovery, and completion publication independently
    /// until host cancellation.
    ///
    /// This method creates no task or runtime. Cancellation is a host-drain
    /// signal, not AgentRun cancellation, and never requests automatic resume.
    pub async fn run(self, cancel: CancellationToken) -> AgentSupervisorExit {
        let Self {
            components,
            lease_owner,
            reconciler,
            execution,
            recovery,
            completion,
            schedule,
        } = self;
        let execution_loop = run_execution_loop(
            &components,
            lease_owner,
            execution,
            schedule.clone(),
            cancel.clone(),
        );
        let completion_loop = run_completion_loop(&completion, schedule.clone(), cancel.clone());
        let recovery_loop = run_recovery_loop(&components, reconciler, recovery, schedule, cancel);
        let (execution, recovery, completion) =
            tokio::join!(execution_loop, recovery_loop, completion_loop);
        AgentSupervisorExit {
            execution,
            recovery,
            completion,
        }
    }
}

async fn run_completion_loop(
    publisher: &AgentCompletionPublisher,
    schedule: AgentSupervisorOptions,
    cancel: CancellationToken,
) -> AgentSupervisorLoopExit {
    let mut stats = AgentSupervisorLoopExit::default();
    let mut backoff = Backoff::new(schedule.initial_error_backoff, schedule.max_error_backoff);
    loop {
        if cancel.is_cancelled() {
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
        if cancel.is_cancelled() || !perform_next(next, &cancel).await {
            break;
        }
    }
    stats
}

impl fmt::Debug for ResidentInProcessAgentSupervisor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResidentInProcessAgentSupervisor")
            .finish_non_exhaustive()
    }
}

async fn run_execution_loop(
    components: &InProcessAgentComponents,
    lease_owner: String,
    options: AgentExecutionOptions,
    schedule: AgentSupervisorOptions,
    cancel: CancellationToken,
) -> AgentSupervisorLoopExit {
    let mut stats = AgentSupervisorLoopExit::default();
    let mut backoff = Backoff::new(schedule.initial_error_backoff, schedule.max_error_backoff);
    loop {
        if cancel.is_cancelled() {
            break;
        }
        stats.cycles = stats.cycles.saturating_add(1);
        let driver = components.execution_driver(lease_owner.clone(), options.clone());
        let next = match driver {
            Ok(driver) => {
                let cycle = AssertUnwindSafe(driver.execute_once(CancellationToken::new()))
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
    components: &InProcessAgentComponents,
    reconciler: String,
    options: AgentRecoveryOptions,
    schedule: AgentSupervisorOptions,
    cancel: CancellationToken,
) -> AgentSupervisorLoopExit {
    let mut stats = AgentSupervisorLoopExit::default();
    let mut backoff = Backoff::new(schedule.initial_error_backoff, schedule.max_error_backoff);
    loop {
        if cancel.is_cancelled() {
            break;
        }
        stats.cycles = stats.cycles.saturating_add(1);
        let driver = components.recovery_driver(reconciler.clone(), options.clone());
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
    use static_assertions::{assert_impl_all, assert_not_impl_any};
    use tokio::sync::Notify;
    use vyane_agent::{
        AgentClock, AgentStore, NewAgentRun, NewRunCompletion, NewWorker, RunMode, RunState,
        SqliteAgentStore,
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

    #[test]
    fn surface_is_non_clone_and_rejects_invalid_bounds_without_store_work() {
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
                .claim_due(OWNER_BOUNDS, "probe", 1, 1)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn host_cancellation_drains_active_pass_without_cancelling_the_run() {
        let fixture = Fixture::new();
        fixture.enqueue(OWNER_DRAIN, "drain", 0);
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
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!task.is_finished());
        release.notify_one();

        let exit = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exit.execution.claimed, 1);
        assert_eq!(exit.execution.degraded_items, 0);
        assert_eq!(
            fixture
                .store
                .get_run(OWNER_DRAIN, "run-drain")
                .unwrap()
                .unwrap()
                .state,
            RunState::Succeeded
        );
    }

    #[tokio::test]
    async fn resident_supervisor_publishes_committed_completion() {
        let fixture = Fixture::new();
        fixture.enqueue(OWNER_COMPLETION, "publish", 0);
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
        fixture.enqueue(OWNER_RECOVERY, "recover", 1);
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
                .claim_due(OWNER_RECOVERY, "probe", 1, 10)
                .unwrap()
                .is_empty()
        );
    }
}
