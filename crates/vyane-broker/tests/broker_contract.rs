#![allow(clippy::unwrap_used)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tempfile::TempDir;
use vyane_broker::{
    AdapterContext, AdapterFailure, AdapterOutcome, BrokerError, BrokerScope, DeliveryAdapter,
    DeliveryEnvelope, MessageBroker, PumpItemStatus, PumpOptions, ReplaySafety,
};
use vyane_message::{
    ClaimQuery, DeliveryMailbox, DeliveryStatus, EndpointKind, EndpointRef, IdempotencyKey,
    LeaseRequest, MessageDirection, MessagePublicationStatus, MessageStore, NackDisposition,
    NewDelivery, NewMessage, NewTransportReceipt, SqliteMessageStore,
};

#[derive(Clone)]
enum Action {
    Outcome(AdapterOutcome),
    Failure(AdapterFailure),
    Panic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Seen {
    owner: String,
    key: String,
    delivery_id: String,
}

struct FakeAdapter {
    name: &'static str,
    safety: ReplaySafety,
    actions: Mutex<VecDeque<Action>>,
    seen: Mutex<Vec<Seen>>,
    delay: Duration,
    active: AtomicUsize,
    maximum_active: AtomicUsize,
}

impl FakeAdapter {
    fn new(actions: Vec<Action>) -> Self {
        Self {
            name: "fake",
            safety: ReplaySafety::Idempotent,
            actions: Mutex::new(actions.into()),
            seen: Mutex::new(Vec::new()),
            delay: Duration::ZERO,
            active: AtomicUsize::new(0),
            maximum_active: AtomicUsize::new(0),
        }
    }

    fn unsafe_adapter() -> Self {
        Self {
            safety: ReplaySafety::Unsupported,
            ..Self::new(vec![Action::Outcome(AdapterOutcome::LocalHandled)])
        }
    }

    fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    fn maximum_active(&self) -> usize {
        self.maximum_active.load(Ordering::SeqCst)
    }
}

struct ActiveGuard<'a>(&'a AtomicUsize);

impl Drop for ActiveGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl DeliveryAdapter for FakeAdapter {
    fn name(&self) -> &str {
        self.name
    }

    fn replay_safety(&self) -> ReplaySafety {
        self.safety
    }

    async fn deliver(
        &self,
        context: AdapterContext,
        delivery: DeliveryEnvelope,
    ) -> Result<AdapterOutcome, AdapterFailure> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum_active.fetch_max(active, Ordering::SeqCst);
        let _guard = ActiveGuard(&self.active);
        self.seen.lock().unwrap().push(Seen {
            owner: context.owner().to_string(),
            key: context.transport_idempotency_key().to_string(),
            delivery_id: delivery.delivery_id,
        });
        tokio::time::sleep(self.delay).await;
        match self.actions.lock().unwrap().pop_front().unwrap() {
            Action::Outcome(outcome) => Ok(outcome),
            Action::Failure(failure) => Err(failure),
            Action::Panic => panic!("fake adapter panic"),
        }
    }
}

struct Fixture {
    _directory: TempDir,
    concrete: Arc<SqliteMessageStore>,
    store: Arc<dyn MessageStore>,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let concrete =
            Arc::new(SqliteMessageStore::open(directory.path().join("messages.sqlite3")).unwrap());
        let store: Arc<dyn MessageStore> = concrete.clone();
        Self {
            _directory: directory,
            concrete,
            store,
        }
    }

    fn broker(&self, owner: &str) -> MessageBroker {
        MessageBroker::new(BrokerScope::new(owner).unwrap(), Arc::clone(&self.store))
    }
}

fn endpoint(kind: EndpointKind, id: &str) -> EndpointRef {
    EndpointRef {
        kind,
        id: id.to_string(),
    }
}

fn mailbox() -> DeliveryMailbox {
    DeliveryMailbox {
        route: "local".into(),
        target: endpoint(EndpointKind::Worker, "worker-1"),
    }
}

fn message(key: &str, body: &str) -> NewMessage {
    NewMessage {
        conversation_id: "conversation-1".into(),
        session_id: Some("session-1".into()),
        direction: MessageDirection::Internal,
        kind: "message".into(),
        sender: endpoint(EndpointKind::Agent, "agent-1"),
        body: body.into(),
        payload: serde_json::json!({"private": format!("payload-{body}")}),
        reply_to: None,
        trace_id: Some("trace-1".into()),
        correlation_id: Some("correlation-1".into()),
        idempotency: IdempotencyKey {
            producer: "test".into(),
            key: key.into(),
        },
        deliveries: vec![NewDelivery {
            route: mailbox().route,
            target: mailbox().target,
            available_at: None,
            expires_at: None,
            max_attempts: 3,
        }],
    }
}

fn query(limit: usize) -> ClaimQuery {
    ClaimQuery {
        mailboxes: vec![mailbox()],
        limit,
    }
}

fn lease() -> LeaseRequest {
    LeaseRequest {
        consumer: "broker-test".into(),
        lease_seconds: 30,
    }
}

fn options(max_in_flight: usize) -> PumpOptions {
    PumpOptions {
        adapter_timeout: Duration::from_secs(5),
        settlement_margin: Duration::from_secs(1),
        max_in_flight,
    }
}

#[tokio::test]
async fn broker_resolves_exact_publication_without_returning_body_or_crossing_owner() {
    let fixture = Fixture::new();
    let owner_a = fixture.broker("owner-a");
    let owner_b = fixture.broker("owner-b");
    let request = message("completion", "BROKER-RESULT-BODY-CANARY");
    let key = request.idempotency.clone();
    let digest = request.request_digest().unwrap();
    let published = owner_a.publish(request.clone()).await.unwrap();

    let resolved = owner_a
        .resolve_idempotency(key.clone(), digest.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resolved.message_id, published.bundle.message.id);
    assert_eq!(resolved.owner, "owner-a");
    assert_eq!(resolved.request_digest, digest);
    assert_eq!(resolved.status, MessagePublicationStatus::Published);
    assert!(!format!("{resolved:?}").contains("BROKER-RESULT-BODY-CANARY"));
    assert!(
        owner_b
            .resolve_idempotency(key.clone(), request.request_digest().unwrap())
            .await
            .unwrap()
            .is_none()
    );

    let mut drift = request;
    drift.body = "different".into();
    let error = owner_a
        .resolve_idempotency(key, drift.request_digest().unwrap())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        BrokerError::Store(vyane_message::MessageStoreError::IdempotencyConflict)
    ));
}

#[tokio::test]
async fn broker_publication_gate_is_owner_bound_and_releases_only_once() {
    let fixture = Fixture::new();
    let owner_a = fixture.broker("owner-a");
    let owner_b = fixture.broker("owner-b");
    let request = message("staged", "BROKER-STAGED-CANARY");
    let key = request.idempotency.clone();
    let digest = request.request_digest().unwrap();
    let staged = owner_a.stage(request.clone()).await.unwrap();
    assert_eq!(staged.resolution.status, MessagePublicationStatus::Staged);
    assert!(!format!("{staged:?}").contains("BROKER-STAGED-CANARY"));
    assert!(
        fixture
            .concrete
            .get("owner-a", &staged.resolution.message_id)
            .unwrap()
            .is_none()
    );
    assert!(
        owner_a
            .resolve_idempotency(key.clone(), digest.clone())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        owner_a
            .resolve_publication(key.clone(), digest.clone())
            .await
            .unwrap()
            .unwrap()
            .status,
        MessagePublicationStatus::Staged
    );
    assert!(
        owner_b
            .publish_staged(key.clone(), digest.clone())
            .await
            .unwrap()
            .is_none()
    );

    let adapter = Arc::new(FakeAdapter::new(vec![Action::Outcome(
        AdapterOutcome::LocalHandled,
    )]));
    let hidden = owner_a
        .pump_once(query(1), lease(), adapter.clone(), options(1))
        .await
        .unwrap();
    assert_eq!(hidden.claimed, 0);
    let published = owner_a
        .publish_staged(key.clone(), digest.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(!published.existing);
    assert_eq!(
        published.resolution.status,
        MessagePublicationStatus::Published
    );
    assert!(
        owner_a
            .publish_staged(key, digest)
            .await
            .unwrap()
            .unwrap()
            .existing
    );
    let delivered = owner_a
        .pump_once(query(1), lease(), adapter, options(1))
        .await
        .unwrap();
    assert_eq!(delivered.claimed, 1);
    assert_eq!(delivered.items[0].status, PumpItemStatus::Acknowledged);

    let discarded_request = message("discarded", "discarded-body");
    let discarded_key = discarded_request.idempotency.clone();
    let discarded_digest = discarded_request.request_digest().unwrap();
    owner_a.stage(discarded_request).await.unwrap();
    let discarded = owner_a
        .discard_staged(discarded_key, discarded_digest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        discarded.resolution.status,
        MessagePublicationStatus::Discarded
    );
    fixture.concrete.audit_integrity().unwrap();
}

#[tokio::test]
async fn owner_scope_and_max_in_flight_bound_claim_and_execution() {
    let fixture = Fixture::new();
    let owner_a = fixture.broker("owner-a");
    let owner_b = fixture.broker("owner-b");
    for index in 0..3 {
        owner_a
            .publish(message(&format!("a-{index}"), "owner-a-secret"))
            .await
            .unwrap();
    }
    let b = owner_b
        .publish(message("b-1", "owner-b-secret"))
        .await
        .unwrap();
    let adapter = Arc::new(
        FakeAdapter::new(vec![
            Action::Outcome(AdapterOutcome::LocalHandled),
            Action::Outcome(AdapterOutcome::LocalHandled),
        ])
        .with_delay(Duration::from_millis(20)),
    );

    let report = owner_a
        .pump_once(query(100), lease(), adapter.clone(), options(2))
        .await
        .unwrap();

    assert_eq!(report.claimed, 2);
    assert_eq!(report.items.len(), 2);
    assert!(
        report
            .items
            .iter()
            .all(|item| item.status == PumpItemStatus::Acknowledged)
    );
    assert_eq!(adapter.maximum_active(), 2);
    assert_eq!(
        fixture
            .concrete
            .get("owner-b", &b.bundle.message.id)
            .unwrap()
            .unwrap()
            .deliveries[0]
            .status,
        DeliveryStatus::Pending
    );
    assert!(
        fixture
            .concrete
            .get("owner-a", &b.bundle.message.id)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn retry_then_permanent_failure_uses_fenced_nacks() {
    let fixture = Fixture::new();
    let broker = fixture.broker("owner-a");
    let enqueued = broker.publish(message("retry", "body")).await.unwrap();
    let adapter = Arc::new(FakeAdapter::new(vec![
        Action::Failure(AdapterFailure::Retry {
            reason_code: "temporary".into(),
            delay_seconds: 0,
        }),
        Action::Failure(AdapterFailure::Permanent {
            failure_code: "invalid_destination".into(),
        }),
    ]));

    let first = broker
        .pump_once(query(1), lease(), adapter.clone(), options(1))
        .await
        .unwrap();
    assert_eq!(first.items[0].status, PumpItemStatus::RetryScheduled);
    let second = broker
        .pump_once(query(1), lease(), adapter, options(1))
        .await
        .unwrap();
    assert_eq!(second.items[0].status, PumpItemStatus::DeadLettered);
    assert_eq!(
        fixture
            .concrete
            .get("owner-a", &enqueued.bundle.message.id)
            .unwrap()
            .unwrap()
            .deliveries[0]
            .status,
        DeliveryStatus::DeadLettered
    );
}

#[tokio::test]
async fn retry_exhaustion_reports_the_persisted_dead_letter_truth() {
    let fixture = Fixture::new();
    let broker = fixture.broker("owner-a");
    let mut request = message("exhausted", "body");
    request.deliveries[0].max_attempts = 1;
    let enqueued = broker.publish(request).await.unwrap();
    let adapter = Arc::new(FakeAdapter::new(vec![Action::Failure(
        AdapterFailure::Retry {
            reason_code: "temporary".into(),
            delay_seconds: 0,
        },
    )]));

    let report = broker
        .pump_once(query(1), lease(), adapter, options(1))
        .await
        .unwrap();

    assert_eq!(report.items[0].status, PumpItemStatus::DeadLettered);
    assert_eq!(
        fixture
            .concrete
            .get("owner-a", &enqueued.bundle.message.id)
            .unwrap()
            .unwrap()
            .deliveries[0]
            .status,
        DeliveryStatus::DeadLettered
    );
}

#[tokio::test]
async fn ttl_shortened_lease_never_calls_a_long_running_remote_adapter() {
    let fixture = Fixture::new();
    let broker = fixture.broker("owner-a");
    let mut request = message("short-ttl", "body");
    request.deliveries[0].expires_at =
        Some(chrono::Utc::now() + chrono::Duration::milliseconds(1_200));
    let enqueued = broker.publish(request).await.unwrap();
    let delivery_id = enqueued.bundle.deliveries[0].id.clone();
    let adapter = Arc::new(FakeAdapter::new(vec![Action::Outcome(
        AdapterOutcome::TransportDelivered(NewTransportReceipt {
            transport: "fake".into(),
            account_scope: "account".into(),
            destination_scope: "destination".into(),
            external_ids: vec!["must-not-be-created".into()],
        }),
    )]));
    let options = PumpOptions {
        adapter_timeout: Duration::from_secs(2),
        settlement_margin: Duration::from_secs(1),
        max_in_flight: 1,
    };

    let report = broker
        .pump_once(query(1), lease(), adapter.clone(), options)
        .await
        .unwrap();
    assert_eq!(
        report.items[0].status,
        PumpItemStatus::InsufficientLeaseWindow
    );
    assert!(adapter.seen.lock().unwrap().is_empty());
    assert!(
        fixture
            .concrete
            .transport_receipts("owner-a", &delivery_id)
            .unwrap()
            .is_empty()
    );

    tokio::time::sleep(Duration::from_millis(1_300)).await;
    let maintenance = broker.maintenance_once(10).await.unwrap();
    assert_eq!(maintenance.expired, 1);
    assert_eq!(
        fixture
            .concrete
            .get("owner-a", &enqueued.bundle.message.id)
            .unwrap()
            .unwrap()
            .deliveries[0]
            .status,
        DeliveryStatus::Expired
    );
}

#[tokio::test]
async fn reply_is_enqueued_with_input_acknowledgement_atomically() {
    let fixture = Fixture::new();
    let broker = fixture.broker("owner-a");
    let input = broker.publish(message("input", "question")).await.unwrap();
    let mut reply = message("reply", "answer");
    reply.reply_to = Some(input.bundle.message.id.clone());
    let adapter = Arc::new(FakeAdapter::new(vec![Action::Outcome(
        AdapterOutcome::Reply(Box::new(reply)),
    )]));

    let report = broker
        .pump_once(query(1), lease(), adapter, options(1))
        .await
        .unwrap();
    let PumpItemStatus::ReplyEnqueued { message_id } = &report.items[0].status else {
        panic!("expected reply outcome, got {:?}", report.items[0].status)
    };
    assert!(
        fixture
            .concrete
            .get("owner-a", message_id)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        fixture
            .concrete
            .get("owner-a", &input.bundle.message.id)
            .unwrap()
            .unwrap()
            .deliveries[0]
            .status,
        DeliveryStatus::Acknowledged
    );
}

#[tokio::test]
async fn transport_receipt_uses_stable_key_and_reaches_acknowledged() {
    let fixture = Fixture::new();
    let broker = fixture.broker("owner-a");
    let enqueued = broker.publish(message("external", "body")).await.unwrap();
    let delivery_id = enqueued.bundle.deliveries[0].id.clone();
    let adapter = Arc::new(FakeAdapter::new(vec![Action::Outcome(
        AdapterOutcome::TransportDelivered(NewTransportReceipt {
            transport: "fake".into(),
            account_scope: "account".into(),
            destination_scope: "destination".into(),
            external_ids: vec!["external-1".into()],
        }),
    )]));

    let report = broker
        .pump_once(query(1), lease(), adapter.clone(), options(1))
        .await
        .unwrap();
    assert_eq!(report.items[0].status, PumpItemStatus::Acknowledged);
    assert_eq!(
        adapter.seen.lock().unwrap()[0].key,
        format!("vyane:{delivery_id}")
    );
    assert_eq!(
        fixture
            .concrete
            .get("owner-a", &enqueued.bundle.message.id)
            .unwrap()
            .unwrap()
            .deliveries[0]
            .status,
        DeliveryStatus::Acknowledged
    );
    assert_eq!(
        fixture
            .concrete
            .transport_receipts("owner-a", &delivery_id)
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn unsafe_adapter_is_rejected_before_claim() {
    let fixture = Fixture::new();
    let broker = fixture.broker("owner-a");
    broker.publish(message("unsafe", "body")).await.unwrap();
    let adapter = Arc::new(FakeAdapter::unsafe_adapter());

    let error = broker
        .pump_once(query(1), lease(), adapter, options(1))
        .await
        .unwrap_err();
    assert!(matches!(error, BrokerError::UnsafeAdapter { .. }));
    let claimed = fixture
        .concrete
        .claim("owner-a", &query(1), &lease())
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].receipt.generation, 1);
}

#[tokio::test]
async fn panic_and_uncertain_result_leave_the_lease_for_recovery() {
    let fixture = Fixture::new();
    let broker = fixture.broker("owner-a");
    let panic_message = broker.publish(message("panic", "body")).await.unwrap();
    let panic_adapter = Arc::new(FakeAdapter::new(vec![Action::Panic]));
    let panic_report = broker
        .pump_once(query(1), lease(), panic_adapter, options(1))
        .await
        .unwrap();
    assert_eq!(
        panic_report.items[0].status,
        PumpItemStatus::AdapterPanicked
    );
    assert_eq!(
        fixture
            .concrete
            .get("owner-a", &panic_message.bundle.message.id)
            .unwrap()
            .unwrap()
            .deliveries[0]
            .status,
        DeliveryStatus::Leased
    );

    // A separate owner proves an uncertain result is treated the same way.
    let other = fixture.broker("owner-b");
    let uncertain_message = other.publish(message("uncertain", "body")).await.unwrap();
    let uncertain = Arc::new(FakeAdapter::new(vec![Action::Failure(
        AdapterFailure::Uncertain {
            reason_code: "connection_lost".into(),
        },
    )]));
    let uncertain_report = other
        .pump_once(query(1), lease(), uncertain, options(1))
        .await
        .unwrap();
    assert_eq!(uncertain_report.items[0].status, PumpItemStatus::Uncertain);
    assert_eq!(
        fixture
            .concrete
            .get("owner-b", &uncertain_message.bundle.message.id)
            .unwrap()
            .unwrap()
            .deliveries[0]
            .status,
        DeliveryStatus::Leased
    );
}

#[test]
fn nack_disposition_stays_part_of_the_public_message_contract() {
    // Compile-time guard that the broker's retry mapping remains lossless.
    assert_eq!(
        NackDisposition::RetryAfter { delay_seconds: 3 },
        NackDisposition::RetryAfter { delay_seconds: 3 }
    );
}
