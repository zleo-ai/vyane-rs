#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
use static_assertions::assert_not_impl_any;
use tempfile::TempDir;
use tokio::sync::Notify;
use vyane_agent::{AgentStore, NewAgentRun, NewWorker, RunMode, SqliteAgentStore};
use vyane_broker::{
    AdapterContext, AdapterFailure, AdapterOutcome, AgentEventProjector, BrokerScope,
    DeliveryAdapter, DeliveryEnvelope, DeliveryLane, MessageBroker, MessageEventProjector,
    PumpOptions, ReplaySafety, ResidentBrokerSupervisor, SupervisorOptions,
};
use vyane_core::CancellationToken;
use vyane_ledger::EventLog;
use vyane_message::{
    ClaimQuery, DeliveryMailbox, DeliveryStatus, EndpointKind, EndpointRef, IdempotencyKey,
    LeaseRequest, MessageClock, MessageDirection, MessageStore, NewDelivery, NewMessage,
    SqliteMessageStore,
};

assert_not_impl_any!(DeliveryLane: Clone);
assert_not_impl_any!(ResidentBrokerSupervisor: Clone);

const OWNER: &str = "owner";

struct Fixture {
    _directory: TempDir,
    message_concrete: Arc<SqliteMessageStore>,
    message_store: Arc<dyn MessageStore>,
    agent_store: Arc<dyn AgentStore>,
    broker: MessageBroker,
    message_projector: MessageEventProjector,
    agent_projector: AgentEventProjector,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let message_concrete =
            Arc::new(SqliteMessageStore::open(directory.path().join("messages.sqlite3")).unwrap());
        let message_store: Arc<dyn MessageStore> = message_concrete.clone();
        let agent_concrete =
            Arc::new(SqliteAgentStore::open(directory.path().join("agent.sqlite3")).unwrap());
        let agent_store: Arc<dyn AgentStore> = agent_concrete;
        let scope = BrokerScope::new(OWNER).unwrap();
        let event_root = directory.path().join("events");
        Self {
            _directory: directory,
            message_concrete,
            message_store: Arc::clone(&message_store),
            agent_store: Arc::clone(&agent_store),
            broker: MessageBroker::new(scope.clone(), Arc::clone(&message_store)),
            message_projector: MessageEventProjector::new(
                scope.clone(),
                message_store,
                EventLog::new(&event_root),
            ),
            agent_projector: AgentEventProjector::new(
                scope,
                agent_store,
                EventLog::new(event_root),
            ),
        }
    }

    fn supervisor(
        &self,
        lanes: Vec<DeliveryLane>,
        options: SupervisorOptions,
    ) -> ResidentBrokerSupervisor {
        ResidentBrokerSupervisor::new(
            self.broker.clone(),
            self.message_projector.clone(),
            self.agent_projector.clone(),
            lanes,
            options,
        )
        .unwrap()
    }

    async fn publish(&self, mailbox: DeliveryMailbox, key: &str) -> String {
        self.broker
            .publish(message(mailbox, key))
            .await
            .unwrap()
            .bundle
            .message
            .id
    }

    fn add_agent_root(&self) {
        add_agent_root(self.agent_store.as_ref(), OWNER);
    }
}

struct ImmediateAdapter {
    name: &'static str,
    calls: AtomicUsize,
    entered: Option<Arc<Notify>>,
}

impl ImmediateAdapter {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            calls: AtomicUsize::new(0),
            entered: None,
        }
    }

    fn notifying(name: &'static str, entered: Arc<Notify>) -> Self {
        Self {
            name,
            calls: AtomicUsize::new(0),
            entered: Some(entered),
        }
    }
}

#[async_trait]
impl DeliveryAdapter for ImmediateAdapter {
    fn name(&self) -> &str {
        self.name
    }

    fn replay_safety(&self) -> ReplaySafety {
        ReplaySafety::Idempotent
    }

    async fn deliver(
        &self,
        _context: AdapterContext,
        _delivery: DeliveryEnvelope,
    ) -> Result<AdapterOutcome, AdapterFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(entered) = &self.entered {
            entered.notify_one();
        }
        Ok(AdapterOutcome::LocalHandled)
    }
}

struct BlockingAdapter {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl DeliveryAdapter for BlockingAdapter {
    fn name(&self) -> &str {
        "blocking"
    }

    fn replay_safety(&self) -> ReplaySafety {
        ReplaySafety::Idempotent
    }

    async fn deliver(
        &self,
        _context: AdapterContext,
        _delivery: DeliveryEnvelope,
    ) -> Result<AdapterOutcome, AdapterFailure> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(AdapterOutcome::LocalHandled)
    }
}

struct RuntimeNamePanicAdapter {
    name_calls: AtomicUsize,
}

impl RuntimeNamePanicAdapter {
    fn new() -> Self {
        Self {
            name_calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl DeliveryAdapter for RuntimeNamePanicAdapter {
    fn name(&self) -> &str {
        if self.name_calls.fetch_add(1, Ordering::SeqCst) > 0 {
            panic!("synthetic adapter identity panic");
        }
        "panic-later"
    }

    fn replay_safety(&self) -> ReplaySafety {
        ReplaySafety::Idempotent
    }

    async fn deliver(
        &self,
        _context: AdapterContext,
        _delivery: DeliveryEnvelope,
    ) -> Result<AdapterOutcome, AdapterFailure> {
        panic!("identity panic must happen before delivery")
    }
}

#[derive(Debug)]
struct TestMessageClock(Mutex<DateTime<Utc>>);

impl TestMessageClock {
    fn new() -> Self {
        Self(Mutex::new(
            Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0)
                .single()
                .unwrap(),
        ))
    }

    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }

    fn advance(&self, seconds: i64) {
        let mut now = self.0.lock().unwrap();
        *now = now.checked_add_signed(TimeDelta::seconds(seconds)).unwrap();
    }
}

impl MessageClock for TestMessageClock {
    fn now(&self) -> DateTime<Utc> {
        self.now()
    }
}

fn endpoint(id: &str) -> EndpointRef {
    EndpointRef {
        kind: EndpointKind::Worker,
        id: id.into(),
    }
}

fn mailbox(id: &str) -> DeliveryMailbox {
    DeliveryMailbox {
        route: "local".into(),
        target: endpoint(id),
    }
}

fn message(mailbox: DeliveryMailbox, key: &str) -> NewMessage {
    NewMessage {
        conversation_id: "conversation".into(),
        session_id: None,
        direction: MessageDirection::Internal,
        kind: "message".into(),
        sender: EndpointRef {
            kind: EndpointKind::Agent,
            id: "sender".into(),
        },
        body: "body".into(),
        payload: serde_json::Value::Null,
        reply_to: None,
        trace_id: None,
        correlation_id: None,
        idempotency: IdempotencyKey {
            producer: "supervisor-test".into(),
            key: key.into(),
        },
        deliveries: vec![NewDelivery {
            route: mailbox.route,
            target: mailbox.target,
            available_at: None,
            expires_at: None,
            max_attempts: 3,
        }],
    }
}

fn query(mailbox: DeliveryMailbox) -> ClaimQuery {
    ClaimQuery {
        mailboxes: vec![mailbox],
        limit: 1,
    }
}

fn lease(consumer: &str) -> LeaseRequest {
    LeaseRequest {
        consumer: consumer.into(),
        lease_seconds: 30,
    }
}

fn pump() -> PumpOptions {
    PumpOptions {
        adapter_timeout: Duration::from_secs(5),
        settlement_margin: Duration::from_secs(1),
        max_in_flight: 1,
    }
}

fn slow_options() -> SupervisorOptions {
    SupervisorOptions {
        idle_poll_interval: Duration::from_secs(3_600),
        maintenance_interval: Duration::from_secs(3_600),
        initial_error_backoff: Duration::from_secs(10),
        max_error_backoff: Duration::from_secs(40),
        maintenance_limit: 8,
        projection_limit: 8,
        max_total_in_flight: 8,
    }
}

fn lane(id: &str, mailbox: DeliveryMailbox, adapter: Arc<dyn DeliveryAdapter>) -> DeliveryLane {
    DeliveryLane::new(id, query(mailbox), lease(id), adapter, pump()).unwrap()
}

fn digest(byte: char) -> String {
    std::iter::repeat_n(byte, 64).collect()
}

fn add_agent_root(store: &dyn AgentStore, owner: &str) {
    store
        .create_root(
            owner,
            &NewWorker {
                id: "agent-worker".into(),
                logical_session_id: None,
            },
            &NewAgentRun {
                id: "agent-run".into(),
                worker_id: "agent-worker".into(),
                task_id: None,
                trace_id: None,
                parent_run_id: None,
                execution_backend: vyane_agent::ExecutionBackend::NativeInProcess,
                mode: RunMode::Autonomous,
                target_key: "provider/model".into(),
                prompt_digest: digest('a'),
                policy_digest: digest('b'),
                available_at: Utc::now(),
                timeout_seconds: 60,
                max_resume_attempts: 0,
            },
        )
        .unwrap();
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if predicate() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition did not become true");
}

#[tokio::test(start_paused = true)]
async fn configuration_rejects_invalid_or_overlapping_lanes_before_running() {
    assert!(BrokerScope::new(" owner").is_err());
    let inspected = Arc::new(RuntimeNamePanicAdapter::new());
    let inspected_trait: Arc<dyn DeliveryAdapter> = inspected.clone();
    assert!(
        DeliveryLane::new(
            "invalid",
            ClaimQuery {
                mailboxes: Vec::new(),
                limit: 1,
            },
            lease("invalid"),
            inspected_trait,
            pump(),
        )
        .is_err()
    );
    assert_eq!(inspected.name_calls.load(Ordering::SeqCst), 0);

    let fixture = Fixture::new();
    let shared = mailbox("shared");
    let first: Arc<dyn DeliveryAdapter> = Arc::new(ImmediateAdapter::new("first"));
    let second: Arc<dyn DeliveryAdapter> = Arc::new(ImmediateAdapter::new("second"));
    assert!(
        ResidentBrokerSupervisor::new(
            fixture.broker.clone(),
            fixture.message_projector.clone(),
            fixture.agent_projector.clone(),
            vec![
                lane("first", shared.clone(), first),
                lane("second", shared, second),
            ],
            slow_options(),
        )
        .is_err()
    );

    let wrong_scope = BrokerScope::new("other-owner").unwrap();
    let wrong_agent = AgentEventProjector::new(
        wrong_scope,
        Arc::clone(&fixture.agent_store),
        EventLog::new(fixture._directory.path().join("other-events")),
    );
    assert!(
        ResidentBrokerSupervisor::new(
            fixture.broker.clone(),
            fixture.message_projector.clone(),
            wrong_agent,
            Vec::new(),
            slow_options(),
        )
        .is_err()
    );

    let mut invalid_options = slow_options();
    invalid_options.projection_limit = 0;
    assert!(
        ResidentBrokerSupervisor::new(
            fixture.broker.clone(),
            fixture.message_projector.clone(),
            fixture.agent_projector.clone(),
            Vec::new(),
            invalid_options,
        )
        .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn pre_cancelled_run_starts_no_cycle_and_claims_nothing() {
    let fixture = Fixture::new();
    let target = mailbox("pre-cancelled-secret-target");
    let message_id = fixture.publish(target.clone(), "pre-cancelled").await;
    let adapter = Arc::new(ImmediateAdapter::new("pre-cancelled"));
    let adapter_trait: Arc<dyn DeliveryAdapter> = adapter.clone();
    let lane = lane("pre-cancelled", target, adapter_trait);
    let lane_debug = format!("{lane:?}");
    assert!(!lane_debug.contains("pre-cancelled-secret-target"));
    let supervisor = fixture.supervisor(vec![lane], slow_options());
    let cancel = CancellationToken::new();
    cancel.cancel();

    let exit = supervisor.run(cancel).await;

    assert_eq!(exit.deliveries[0].stats.cycles, 0);
    assert_eq!(exit.maintenance.cycles, 0);
    assert_eq!(exit.message_projection.cycles, 0);
    assert_eq!(exit.agent_projection.cycles, 0);
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    assert!(!format!("{exit:?}").contains("body"));
    assert_eq!(
        fixture
            .message_concrete
            .get(OWNER, &message_id)
            .unwrap()
            .unwrap()
            .deliveries[0]
            .status,
        DeliveryStatus::Pending
    );
}

#[tokio::test(start_paused = true)]
async fn cancellation_waits_for_the_current_pump_to_settle() {
    let fixture = Fixture::new();
    let target = mailbox("drain");
    let message_id = fixture.publish(target.clone(), "drain").await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let adapter: Arc<dyn DeliveryAdapter> = Arc::new(BlockingAdapter {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    });
    let supervisor = fixture.supervisor(vec![lane("drain", target, adapter)], slow_options());
    let cancel = CancellationToken::new();
    let run = tokio::spawn(supervisor.run(cancel.clone()));

    entered.notified().await;
    cancel.cancel();
    tokio::task::yield_now().await;
    assert!(!run.is_finished());
    release.notify_one();
    let exit = run.await.unwrap();

    assert_eq!(exit.deliveries[0].stats.cycles, 1);
    assert_eq!(exit.deliveries[0].stats.work_items, 1);
    assert_eq!(
        fixture
            .message_concrete
            .get(OWNER, &message_id)
            .unwrap()
            .unwrap()
            .deliveries[0]
            .status,
        DeliveryStatus::Acknowledged
    );
}

#[tokio::test(start_paused = true)]
async fn panics_use_bounded_exponential_backoff() {
    let fixture = Fixture::new();
    let adapter = Arc::new(RuntimeNamePanicAdapter::new());
    let adapter_trait: Arc<dyn DeliveryAdapter> = adapter.clone();
    let supervisor = fixture.supervisor(
        vec![lane("panic-lane", mailbox("panic"), adapter_trait)],
        slow_options(),
    );
    assert_eq!(adapter.name_calls.load(Ordering::SeqCst), 1);
    let cancel = CancellationToken::new();
    let run = tokio::spawn(supervisor.run(cancel.clone()));

    wait_until(|| adapter.name_calls.load(Ordering::SeqCst) == 2).await;
    tokio::time::advance(Duration::from_secs(9)).await;
    tokio::task::yield_now().await;
    assert_eq!(adapter.name_calls.load(Ordering::SeqCst), 2);
    tokio::time::advance(Duration::from_secs(1)).await;
    wait_until(|| adapter.name_calls.load(Ordering::SeqCst) == 3).await;
    tokio::time::advance(Duration::from_secs(19)).await;
    tokio::task::yield_now().await;
    assert_eq!(adapter.name_calls.load(Ordering::SeqCst), 3);
    tokio::time::advance(Duration::from_secs(1)).await;
    wait_until(|| adapter.name_calls.load(Ordering::SeqCst) == 4).await;

    cancel.cancel();
    let exit = run.await.unwrap();
    assert_eq!(exit.deliveries[0].stats.panicked_cycles, 3);
    assert_eq!(exit.deliveries[0].stats.failed_cycles, 0);
}

#[tokio::test(start_paused = true)]
async fn lane_and_projector_failures_do_not_stop_unrelated_loops() {
    let fixture = Fixture::new();
    fixture.add_agent_root();
    let invalid_event_root = fixture._directory.path().join("not-a-directory");
    std::fs::write(&invalid_event_root, b"file").unwrap();
    let failing_message_projector = MessageEventProjector::new(
        BrokerScope::new(OWNER).unwrap(),
        Arc::clone(&fixture.message_store),
        EventLog::new(invalid_event_root),
    );
    let healthy_mailbox = mailbox("healthy");
    let message_id = fixture
        .publish(healthy_mailbox.clone(), "healthy-isolation")
        .await;
    let entered = Arc::new(Notify::new());
    let healthy = Arc::new(ImmediateAdapter::notifying("healthy", Arc::clone(&entered)));
    let healthy_trait: Arc<dyn DeliveryAdapter> = healthy;
    let panicking: Arc<dyn DeliveryAdapter> = Arc::new(RuntimeNamePanicAdapter::new());
    let supervisor = ResidentBrokerSupervisor::new(
        fixture.broker.clone(),
        failing_message_projector,
        fixture.agent_projector.clone(),
        vec![
            lane("broken", mailbox("broken"), panicking),
            lane("healthy", healthy_mailbox, healthy_trait),
        ],
        slow_options(),
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let run = tokio::spawn(supervisor.run(cancel.clone()));

    entered.notified().await;
    wait_until(|| {
        fixture
            .message_concrete
            .get(OWNER, &message_id)
            .ok()
            .flatten()
            .is_some_and(|bundle| bundle.deliveries[0].status == DeliveryStatus::Acknowledged)
    })
    .await;
    wait_until(|| {
        fixture
            .agent_store
            .unprojected_events(OWNER, fixture.agent_projector.projector_id(), 10)
            .is_ok_and(|page| page.items.is_empty())
    })
    .await;
    cancel.cancel();
    let exit = run.await.unwrap();

    assert!(exit.deliveries[0].stats.panicked_cycles >= 1);
    assert_eq!(exit.deliveries[1].stats.work_items, 1);
    assert!(exit.message_projection.failed_cycles >= 1);
    assert_eq!(exit.agent_projection.work_items, 2);
}

#[tokio::test(start_paused = true)]
async fn maintenance_runs_without_delivery_lanes() {
    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(TestMessageClock::new());
    let message_concrete = Arc::new(
        SqliteMessageStore::open_with_clock(
            directory.path().join("messages.sqlite3"),
            clock.clone(),
        )
        .unwrap(),
    );
    let message_store: Arc<dyn MessageStore> = message_concrete.clone();
    let agent_store: Arc<dyn AgentStore> =
        Arc::new(SqliteAgentStore::open(directory.path().join("agent.sqlite3")).unwrap());
    let scope = BrokerScope::new(OWNER).unwrap();
    let broker = MessageBroker::new(scope.clone(), Arc::clone(&message_store));
    let mut request = message(mailbox("expiry"), "expiry");
    request.deliveries[0].expires_at = Some(clock.now() + TimeDelta::seconds(5));
    let message_id = broker.publish(request).await.unwrap().bundle.message.id;
    clock.advance(10);
    let event_root = directory.path().join("events");
    let supervisor = ResidentBrokerSupervisor::new(
        broker,
        MessageEventProjector::new(
            scope.clone(),
            Arc::clone(&message_store),
            EventLog::new(&event_root),
        ),
        AgentEventProjector::new(scope, agent_store, EventLog::new(event_root)),
        Vec::new(),
        slow_options(),
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let run = tokio::spawn(supervisor.run(cancel.clone()));

    wait_until(|| {
        message_concrete
            .get(OWNER, &message_id)
            .ok()
            .flatten()
            .is_some_and(|bundle| bundle.deliveries[0].status == DeliveryStatus::Expired)
    })
    .await;
    cancel.cancel();
    let exit = run.await.unwrap();

    assert_eq!(exit.maintenance.work_items, 1);
    assert!(exit.maintenance.successful_cycles >= 1);
}
