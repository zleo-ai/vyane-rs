#![allow(clippy::unwrap_used)]

use std::process::Command;
use std::sync::{Arc, Barrier, Mutex};

use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
use rusqlite::Connection;
use serde_json::json;
use tempfile::TempDir;
use vyane_message::{
    ClaimQuery, DeliveryMailbox, DeliveryStatus, EndpointKind, EndpointRef, IdempotencyKey,
    LeaseRequest, MessageClock, MessageDirection, MessagePublicationStatus, MessageStore,
    MessageStoreError, NackDisposition, NewDelivery, NewMessage, NewTransportReceipt,
    SqliteMessageStore,
};

const CHILD_ENV: &str = "VYANE_MESSAGE_CONTRACT_CHILD";
const CHILD_DB_ENV: &str = "VYANE_MESSAGE_CONTRACT_DB";

fn timestamp(seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0)
        .single()
        .unwrap()
        + TimeDelta::seconds(seconds)
}

#[derive(Debug, Clone)]
struct TestClock(Arc<Mutex<DateTime<Utc>>>);

impl TestClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self(Arc::new(Mutex::new(now)))
    }

    fn set(&self, now: DateTime<Utc>) {
        *self.0.lock().unwrap() = now;
    }
}

impl MessageClock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

fn endpoint(kind: EndpointKind, id: &str) -> EndpointRef {
    EndpointRef {
        kind,
        id: id.into(),
    }
}

fn mailbox(id: &str) -> DeliveryMailbox {
    DeliveryMailbox {
        route: "worker".into(),
        target: endpoint(EndpointKind::Worker, id),
    }
}

fn message(key: &str, body: &str, target: &str) -> NewMessage {
    NewMessage {
        conversation_id: "conversation-1".into(),
        session_id: Some("session-1".into()),
        direction: MessageDirection::Internal,
        kind: "message".into(),
        sender: endpoint(EndpointKind::Agent, "sender"),
        body: body.into(),
        payload: json!({"safe": "shape"}),
        reply_to: None,
        trace_id: Some("trace-1".into()),
        correlation_id: Some("correlation-1".into()),
        idempotency: IdempotencyKey {
            producer: "test-producer".into(),
            key: key.into(),
        },
        deliveries: vec![NewDelivery {
            route: "worker".into(),
            target: endpoint(EndpointKind::Worker, target),
            available_at: None,
            expires_at: None,
            max_attempts: 3,
        }],
    }
}

fn transport_receipt(external_id: &str) -> NewTransportReceipt {
    NewTransportReceipt {
        transport: "test-adapter".into(),
        account_scope: "account-secret".into(),
        destination_scope: "destination-secret".into(),
        external_ids: vec![external_id.into()],
    }
}

fn test_store() -> (TempDir, TestClock, SqliteMessageStore) {
    let directory = TempDir::new().unwrap();
    let clock = TestClock::new(timestamp(0));
    let store = SqliteMessageStore::open_with_clock(
        directory.path().join("messages.sqlite3"),
        Arc::new(clock.clone()),
    )
    .unwrap();
    (directory, clock, store)
}

fn claim_one(
    store: &SqliteMessageStore,
    owner: &str,
    target: &str,
    consumer: &str,
    seconds: u64,
) -> vyane_message::LeasedDelivery {
    store
        .claim(
            owner,
            &ClaimQuery {
                mailboxes: vec![mailbox(target)],
                limit: 10,
            },
            &LeaseRequest {
                consumer: consumer.into(),
                lease_seconds: seconds,
            },
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}

#[test]
fn enqueue_restart_dedupe_and_body_free_events() {
    let (directory, _clock, store) = test_store();
    let canary = "PRIVATE-BODY-CANARY";
    let first = store
        .enqueue("alice", &message("same", canary, "worker-a"))
        .unwrap();
    assert!(!first.existing);
    assert_eq!(first.bundle.message.conversation_sequence, 1);
    assert_eq!(first.bundle.deliveries[0].status, DeliveryStatus::Pending);
    assert!(!format!("{first:?}").contains(canary));

    let duplicate = store
        .enqueue("alice", &message("same", canary, "worker-a"))
        .unwrap();
    assert!(duplicate.existing);
    assert_eq!(duplicate.bundle.message.id, first.bundle.message.id);

    let reopened = SqliteMessageStore::open(directory.path().join("messages.sqlite3")).unwrap();
    assert_eq!(
        reopened
            .get("alice", &first.bundle.message.id)
            .unwrap()
            .unwrap()
            .message
            .body,
        canary
    );
    let events = reopened.events("alice", &first.bundle.message.id).unwrap();
    assert_eq!(events.len(), 1);
    assert!(!serde_json::to_string(&events).unwrap().contains(canary));
    let raw = std::fs::read(directory.path().join("messages.sqlite3")).unwrap();
    assert!(String::from_utf8_lossy(&raw).contains(canary));
}

#[test]
fn idempotency_drift_conflicts_without_echoing_body() {
    let (_directory, _clock, store) = test_store();
    store
        .enqueue("alice", &message("same", "first-secret", "worker-a"))
        .unwrap();
    let error = store
        .enqueue("alice", &message("same", "second-secret", "worker-a"))
        .unwrap_err();
    assert!(matches!(error, MessageStoreError::IdempotencyConflict));
    let rendered = error.to_string();
    assert!(!rendered.contains("first-secret"));
    assert!(!rendered.contains("second-secret"));
}

#[test]
fn public_request_digest_is_canonical_typed_and_validated() {
    let mut first = message("digest", "body", "worker-a");
    first.payload = serde_json::from_str(r#"{"z":1,"a":{"y":2,"b":3}}"#).unwrap();
    let mut reordered = first.clone();
    reordered.payload = serde_json::from_str(r#"{"a":{"b":3,"y":2},"z":1}"#).unwrap();

    let digest = first.request_digest().unwrap();
    assert_eq!(digest, reordered.request_digest().unwrap());
    assert_eq!(digest.as_str().len(), 64);
    assert!(
        digest
            .as_str()
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_eq!(
        serde_json::from_str::<vyane_message::MessageRequestDigest>(
            &serde_json::to_string(&digest).unwrap()
        )
        .unwrap(),
        digest
    );
    assert!(vyane_message::MessageRequestDigest::parse("A".repeat(64)).is_err());
    assert!(serde_json::from_str::<vyane_message::MessageRequestDigest>(r#""short""#).is_err());

    let mut drift = first.clone();
    drift.body = "different".into();
    assert_ne!(digest, drift.request_digest().unwrap());

    let mut invalid = first;
    invalid.deliveries.clear();
    assert!(invalid.request_digest().is_err());
}

#[test]
fn idempotency_resolution_is_exact_body_free_owner_scoped_and_restart_safe() {
    let (directory, _clock, store) = test_store();
    let canary = "RESULT-BODY-MUST-NOT-REACH-RESOLUTION";
    let request = message("completion", canary, "worker-a");
    let expected = request.request_digest().unwrap();
    let key = request.idempotency.clone();
    let published = store.enqueue("alice", &request).unwrap();

    let resolved = store
        .resolve_idempotency("alice", &key, &expected)
        .unwrap()
        .unwrap();
    assert_eq!(resolved.owner, "alice");
    assert_eq!(resolved.message_id, published.bundle.message.id);
    assert_eq!(resolved.request_digest, expected);
    assert_eq!(resolved.status, MessagePublicationStatus::Published);
    let rendered = format!("{resolved:?}");
    let serialized = serde_json::to_string(&resolved).unwrap();
    assert!(!rendered.contains(canary));
    assert!(!serialized.contains(canary));

    assert!(
        store
            .resolve_idempotency("bob", &key, &expected)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .resolve_idempotency(
                "alice",
                &IdempotencyKey {
                    producer: key.producer.clone(),
                    key: "absent".into(),
                },
                &expected,
            )
            .unwrap()
            .is_none()
    );

    let mut changed = request.clone();
    changed.body = "DRIFT-BODY-MUST-NOT-REACH-ERROR".into();
    let error = store
        .resolve_idempotency("alice", &key, &changed.request_digest().unwrap())
        .unwrap_err();
    assert!(matches!(error, MessageStoreError::IdempotencyConflict));
    assert!(!error.to_string().contains(canary));
    assert!(!error.to_string().contains("DRIFT-BODY"));

    let leased = claim_one(&store, "alice", "worker-a", "completion-consumer", 30);
    store
        .mark_delivered("alice", &mailbox("worker-a"), &leased.receipt)
        .unwrap();
    store
        .acknowledge("alice", &mailbox("worker-a"), &leased.receipt)
        .unwrap();
    assert_eq!(
        store
            .resolve_idempotency("alice", &key, &expected)
            .unwrap()
            .unwrap(),
        resolved
    );

    let reopened = SqliteMessageStore::open(directory.path().join("messages.sqlite3")).unwrap();
    assert_eq!(
        reopened
            .resolve_idempotency("alice", &key, &expected)
            .unwrap()
            .unwrap(),
        resolved
    );
}

#[test]
fn committed_enqueue_and_reply_replay_after_request_ttl() {
    let (_directory, clock, store) = test_store();
    let mut expiring = message("expiring", "body", "worker-a");
    expiring.deliveries[0].expires_at = Some(timestamp(10));
    let first = store.enqueue("alice", &expiring).unwrap();
    clock.set(timestamp(20));
    let replay = store.enqueue("alice", &expiring).unwrap();
    assert!(replay.existing);
    assert_eq!(replay.bundle.message.id, first.bundle.message.id);

    let mut new_expired = expiring.clone();
    new_expired.idempotency.key = "new-expired".into();
    assert!(matches!(
        store.enqueue("alice", &new_expired),
        Err(MessageStoreError::InvalidInput(_))
    ));

    clock.set(timestamp(0));
    store
        .enqueue("alice", &message("original", "request", "worker-b"))
        .unwrap();
    let leased = claim_one(&store, "alice", "worker-b", "consumer-b", 30);
    store
        .mark_delivered("alice", &mailbox("worker-b"), &leased.receipt)
        .unwrap();
    let mut reply = message("reply", "response", "worker-a");
    reply.reply_to = Some(leased.message.id.clone());
    reply.deliveries[0].expires_at = Some(timestamp(10));
    let first_reply = store
        .reply_and_ack("alice", &mailbox("worker-b"), &leased.receipt, &reply)
        .unwrap();
    clock.set(timestamp(20));
    let replayed_reply = store
        .reply_and_ack("alice", &mailbox("worker-b"), &leased.receipt, &reply)
        .unwrap();
    assert!(replayed_reply.reply.existing);
    assert_eq!(
        replayed_reply.reply.bundle.message.id,
        first_reply.reply.bundle.message.id
    );
}

#[test]
fn replies_cannot_cross_logical_conversations() {
    let (_directory, _clock, store) = test_store();
    let original = store
        .enqueue("alice", &message("original", "request", "worker-a"))
        .unwrap();
    let mut cross_conversation = message("cross", "response", "worker-a");
    cross_conversation.conversation_id = "conversation-2".into();
    cross_conversation.reply_to = Some(original.bundle.message.id.clone());
    assert!(matches!(
        store.enqueue("alice", &cross_conversation),
        Err(MessageStoreError::InvalidInput(_))
    ));

    let mut same_conversation = cross_conversation;
    same_conversation.conversation_id = "conversation-1".into();
    same_conversation.idempotency.key = "same-conversation".into();
    assert!(
        store
            .enqueue("alice", &same_conversation)
            .is_ok_and(|outcome| !outcome.existing)
    );
}

#[test]
fn owner_and_mailbox_are_authority_boundaries() {
    let (_directory, _clock, store) = test_store();
    let queued = store
        .enqueue("alice", &message("one", "body", "worker-a"))
        .unwrap();
    assert!(
        store
            .get("bob", &queued.bundle.message.id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .claim(
                "alice",
                &ClaimQuery {
                    mailboxes: vec![mailbox("worker-b")],
                    limit: 1,
                },
                &LeaseRequest {
                    consumer: "consumer-b".into(),
                    lease_seconds: 30,
                },
            )
            .unwrap()
            .is_empty()
    );
    let leased = claim_one(&store, "alice", "worker-a", "consumer-a", 30);
    assert!(matches!(
        store.mark_delivered("bob", &mailbox("worker-a"), &leased.receipt),
        Err(MessageStoreError::NotFound)
    ));
    assert!(matches!(
        store.mark_delivered("alice", &mailbox("worker-b"), &leased.receipt),
        Err(MessageStoreError::InvalidReceipt { .. })
    ));
}

#[test]
fn delivery_ack_is_fenced_and_response_loss_is_idempotent() {
    let (_directory, _clock, store) = test_store();
    store
        .enqueue("alice", &message("one", "body", "worker-a"))
        .unwrap();
    let leased = claim_one(&store, "alice", "worker-a", "consumer-a", 30);
    let delivered = store
        .mark_delivered("alice", &mailbox("worker-a"), &leased.receipt)
        .unwrap();
    assert_eq!(delivered.status, DeliveryStatus::Delivered);
    assert_eq!(
        store
            .mark_delivered("alice", &mailbox("worker-a"), &leased.receipt)
            .unwrap(),
        delivered
    );
    let acked = store
        .acknowledge("alice", &mailbox("worker-a"), &leased.receipt)
        .unwrap();
    assert_eq!(acked.status, DeliveryStatus::Acknowledged);
    assert_eq!(
        store
            .acknowledge("alice", &mailbox("worker-a"), &leased.receipt)
            .unwrap(),
        acked
    );
    assert!(matches!(
        store.nack(
            "alice",
            &mailbox("worker-a"),
            &leased.receipt,
            &NackDisposition::RetryAfter { delay_seconds: 1 }
        ),
        Err(MessageStoreError::InvalidState { .. })
    ));
}

#[test]
fn expired_lease_reclaims_and_rejects_stale_receipt() {
    let (_directory, clock, store) = test_store();
    store
        .enqueue("alice", &message("one", "body", "worker-a"))
        .unwrap();
    let first = claim_one(&store, "alice", "worker-a", "consumer-a", 10);
    clock.set(timestamp(11));
    assert_eq!(store.reclaim_expired("alice", 10).unwrap(), 1);
    let second = claim_one(&store, "alice", "worker-a", "consumer-b", 10);
    assert!(second.receipt.generation > first.receipt.generation);
    assert_eq!(
        second.delivery.transport_idempotency_key(),
        first.delivery.transport_idempotency_key()
    );
    assert!(matches!(
        store.mark_delivered("alice", &mailbox("worker-a"), &first.receipt),
        Err(MessageStoreError::InvalidReceipt { .. })
    ));
}

#[test]
fn historical_nack_replay_does_not_mutate_a_new_lease_generation() {
    let (_directory, _clock, store) = test_store();
    store
        .enqueue("alice", &message("one", "body", "worker-a"))
        .unwrap();
    let first = claim_one(&store, "alice", "worker-a", "consumer-a", 30);
    let original_result = store
        .nack(
            "alice",
            &mailbox("worker-a"),
            &first.receipt,
            &NackDisposition::RetryAfter { delay_seconds: 0 },
        )
        .unwrap();
    let second = claim_one(&store, "alice", "worker-a", "consumer-b", 30);
    assert!(second.receipt.generation > first.receipt.generation);

    let replay = store
        .nack(
            "alice",
            &mailbox("worker-a"),
            &first.receipt,
            &NackDisposition::RetryAfter { delay_seconds: 0 },
        )
        .unwrap();
    assert_eq!(replay, original_result);
    let current = store
        .get("alice", &second.message.id)
        .unwrap()
        .unwrap()
        .deliveries
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(current.status, DeliveryStatus::Leased);
    assert_eq!(current.lease_generation, second.receipt.generation);
}

#[test]
fn renew_is_monotonic_and_supports_periodic_heartbeats() {
    let (_directory, clock, store) = test_store();
    store
        .enqueue("alice", &message("one", "body", "worker-a"))
        .unwrap();
    let leased = claim_one(&store, "alice", "worker-a", "consumer-a", 30);
    let unchanged = store
        .renew(
            "alice",
            &mailbox("worker-a"),
            &leased.receipt,
            "shrink-check",
            1,
        )
        .unwrap();
    assert_eq!(unchanged.revision, leased.delivery.revision);
    assert_eq!(unchanged.lease_expires_at, Some(timestamp(30)));

    clock.set(timestamp(20));
    let first_renewal = store
        .renew(
            "alice",
            &mailbox("worker-a"),
            &leased.receipt,
            "heartbeat-1",
            30,
        )
        .unwrap();
    assert_eq!(first_renewal.lease_expires_at, Some(timestamp(50)));
    clock.set(timestamp(25));
    let replay = store
        .renew(
            "alice",
            &mailbox("worker-a"),
            &leased.receipt,
            "heartbeat-1",
            30,
        )
        .unwrap();
    assert_eq!(replay, first_renewal);
    assert!(matches!(
        store.renew(
            "alice",
            &mailbox("worker-a"),
            &leased.receipt,
            "heartbeat-1",
            31,
        ),
        Err(MessageStoreError::ReceiptOperationConflict { .. })
    ));
    clock.set(timestamp(30));
    let second_renewal = store
        .renew(
            "alice",
            &mailbox("worker-a"),
            &leased.receipt,
            "heartbeat-2",
            30,
        )
        .unwrap();
    assert_eq!(second_renewal.lease_expires_at, Some(timestamp(60)));
    assert!(second_renewal.revision > first_renewal.revision);
}

#[test]
fn nack_retry_then_permanent_dead_letters() {
    let (_directory, clock, store) = test_store();
    store
        .enqueue("alice", &message("one", "body", "worker-a"))
        .unwrap();
    let first = claim_one(&store, "alice", "worker-a", "consumer-a", 30);
    let pending = store
        .nack(
            "alice",
            &mailbox("worker-a"),
            &first.receipt,
            &NackDisposition::RetryAfter { delay_seconds: 5 },
        )
        .unwrap();
    assert_eq!(pending.status, DeliveryStatus::Pending);
    assert!(
        store
            .claim(
                "alice",
                &ClaimQuery {
                    mailboxes: vec![mailbox("worker-a")],
                    limit: 1,
                },
                &LeaseRequest {
                    consumer: "consumer-b".into(),
                    lease_seconds: 30,
                }
            )
            .unwrap()
            .is_empty()
    );
    clock.set(timestamp(5));
    let second = claim_one(&store, "alice", "worker-a", "consumer-b", 30);
    let dead = store
        .nack(
            "alice",
            &mailbox("worker-a"),
            &second.receipt,
            &NackDisposition::Permanent {
                failure_code: "rejected".into(),
            },
        )
        .unwrap();
    assert_eq!(dead.status, DeliveryStatus::DeadLettered);
}

#[test]
fn ttl_expiry_is_store_clock_driven() {
    let (_directory, clock, store) = test_store();
    let mut request = message("ttl", "body", "worker-a");
    request.deliveries[0].expires_at = Some(timestamp(10));
    let queued = store.enqueue("alice", &request).unwrap();
    clock.set(timestamp(10));
    assert_eq!(store.expire_due("alice", 10).unwrap(), 1);
    let loaded = store
        .get("alice", &queued.bundle.message.id)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.deliveries[0].status, DeliveryStatus::Expired);
}

#[test]
fn reply_and_ack_commits_atomically_and_retries_by_reference() {
    let (_directory, _clock, store) = test_store();
    let original = store
        .enqueue("alice", &message("one", "question", "worker-a"))
        .unwrap();
    let leased = claim_one(&store, "alice", "worker-a", "consumer-a", 30);
    store
        .mark_delivered("alice", &mailbox("worker-a"), &leased.receipt)
        .unwrap();
    let mut wrong = message("reply-wrong", "answer", "worker-a");
    wrong.reply_to = Some("missing".into());
    assert!(
        store
            .reply_and_ack("alice", &mailbox("worker-a"), &leased.receipt, &wrong)
            .is_err()
    );
    assert!(
        store
            .get("alice", &original.bundle.message.id)
            .unwrap()
            .unwrap()
            .deliveries[0]
            .status
            == DeliveryStatus::Delivered
    );

    let mut reply = message("reply", "answer", "worker-a");
    reply.reply_to = Some(original.bundle.message.id.clone());
    let first = store
        .reply_and_ack("alice", &mailbox("worker-a"), &leased.receipt, &reply)
        .unwrap();
    assert_eq!(first.acknowledged.status, DeliveryStatus::Acknowledged);
    let replay = store
        .reply_and_ack("alice", &mailbox("worker-a"), &leased.receipt, &reply)
        .unwrap();
    assert_eq!(
        replay.reply.bundle.message.id,
        first.reply.bundle.message.id
    );
}

#[test]
fn transport_delivery_is_transactional_idempotent_and_never_requeued() {
    let (_directory, clock, store) = test_store();
    let queued = store
        .enqueue("alice", &message("external", "body", "worker-a"))
        .unwrap();
    let leased = claim_one(&store, "alice", "worker-a", "consumer-a", 30);
    let mut receipt = transport_receipt("external-secret-id");
    receipt.external_ids.push("external-secret-id-2".into());
    let first = store
        .mark_transport_delivered("alice", &mailbox("worker-a"), &leased.receipt, &receipt)
        .unwrap();
    assert!(!first.existing);
    assert_eq!(first.delivery.status, DeliveryStatus::Delivered);
    assert_eq!(first.receipts.len(), 2);
    assert_eq!(first.receipts[0].ordinal, 0);
    assert_eq!(first.receipts[1].ordinal, 1);
    let rendered = format!("{first:?}");
    assert!(!rendered.contains("external-secret-id"));
    assert!(!rendered.contains("external-secret-id-2"));
    assert!(!rendered.contains("account-secret"));
    assert!(!rendered.contains("destination-secret"));
    assert!(
        store
            .transport_receipts("bob", &leased.delivery.id)
            .unwrap()
            .is_empty()
    );
    let resolved = store
        .resolve_transport_receipt(
            "alice",
            "test-adapter",
            "account-secret",
            "destination-secret",
            "external-secret-id-2",
        )
        .unwrap()
        .unwrap();
    assert_eq!(resolved.delivery.id, leased.delivery.id);
    assert_eq!(resolved.message.id, queued.bundle.message.id);
    assert_eq!(resolved.receipt.ordinal, 1);
    assert!(
        store
            .resolve_transport_receipt(
                "bob",
                "test-adapter",
                "account-secret",
                "destination-secret",
                "external-secret-id-2",
            )
            .unwrap()
            .is_none()
    );

    let replay = store
        .mark_transport_delivered("alice", &mailbox("worker-a"), &leased.receipt, &receipt)
        .unwrap();
    assert!(replay.existing);
    assert_eq!(replay.receipts, first.receipts);
    assert!(matches!(
        store.nack(
            "alice",
            &mailbox("worker-a"),
            &leased.receipt,
            &NackDisposition::RetryAfter { delay_seconds: 0 }
        ),
        Err(MessageStoreError::InvalidState { .. })
    ));
    assert!(matches!(
        store.cancel("alice", &leased.delivery.id),
        Err(MessageStoreError::InvalidState { .. })
    ));

    clock.set(timestamp(31));
    assert_eq!(store.reclaim_expired("alice", 10).unwrap(), 1);
    let current = store
        .get("alice", &queued.bundle.message.id)
        .unwrap()
        .unwrap()
        .deliveries
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(current.status, DeliveryStatus::Acknowledged);
    assert_eq!(
        store
            .acknowledge("alice", &mailbox("worker-a"), &leased.receipt)
            .unwrap()
            .status,
        DeliveryStatus::Acknowledged
    );
    assert!(
        store
            .claim(
                "alice",
                &ClaimQuery {
                    mailboxes: vec![mailbox("worker-a")],
                    limit: 10,
                },
                &LeaseRequest {
                    consumer: "other".into(),
                    lease_seconds: 30,
                },
            )
            .unwrap()
            .is_empty()
    );
    let post_ack_replay = store
        .mark_transport_delivered("alice", &mailbox("worker-a"), &leased.receipt, &receipt)
        .unwrap();
    assert!(post_ack_replay.existing);
    assert_eq!(
        post_ack_replay.delivery.status,
        DeliveryStatus::Acknowledged
    );
    assert_eq!(
        store
            .events("alice", &queued.bundle.message.id)
            .unwrap()
            .len(),
        4
    );
}

#[test]
fn transport_receipt_conflicts_leave_delivery_state_unchanged() {
    let (_directory, _clock, store) = test_store();
    store
        .enqueue("alice", &message("first", "first", "worker-a"))
        .unwrap();
    let first = claim_one(&store, "alice", "worker-a", "consumer-a", 30);
    let shared = transport_receipt("shared-external-id");
    store
        .mark_transport_delivered("alice", &mailbox("worker-a"), &first.receipt, &shared)
        .unwrap();
    let mut drift = shared.clone();
    drift.external_ids = vec!["different-external-id".into()];
    assert!(matches!(
        store.mark_transport_delivered("alice", &mailbox("worker-a"), &first.receipt, &drift,),
        Err(MessageStoreError::TransportReceiptConflict { .. })
    ));

    store
        .enqueue("alice", &message("second", "second", "worker-b"))
        .unwrap();
    let second = claim_one(&store, "alice", "worker-b", "consumer-b", 30);
    assert!(matches!(
        store.mark_transport_delivered("alice", &mailbox("worker-b"), &second.receipt, &shared,),
        Err(MessageStoreError::TransportReceiptConflict { .. })
    ));
    let second_current = store
        .get("alice", &second.message.id)
        .unwrap()
        .unwrap()
        .deliveries
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(second_current.status, DeliveryStatus::Leased);
    assert!(
        store
            .events("alice", &second.message.id)
            .unwrap()
            .iter()
            .all(|event| event.kind != vyane_message::MessageEventKind::Delivered)
    );

    store
        .enqueue("alice", &message("plain", "plain", "worker-c"))
        .unwrap();
    let plain = claim_one(&store, "alice", "worker-c", "consumer-c", 30);
    store
        .mark_delivered("alice", &mailbox("worker-c"), &plain.receipt)
        .unwrap();
    assert!(matches!(
        store.mark_transport_delivered(
            "alice",
            &mailbox("worker-c"),
            &plain.receipt,
            &transport_receipt("late-receipt"),
        ),
        Err(MessageStoreError::InvalidState { .. })
    ));
}

#[test]
fn transport_delivery_survives_ttl_expiry_as_acknowledged() {
    let (_directory, clock, store) = test_store();
    let mut expiring = message("external-ttl", "body", "worker-a");
    expiring.deliveries[0].expires_at = Some(timestamp(10));
    let queued = store.enqueue("alice", &expiring).unwrap();
    let leased = claim_one(&store, "alice", "worker-a", "consumer-a", 30);
    assert_eq!(leased.delivery.lease_expires_at, Some(timestamp(10)));
    clock.set(timestamp(11));
    store
        .mark_transport_delivered(
            "alice",
            &mailbox("worker-a"),
            &leased.receipt,
            &transport_receipt("ttl-external-id"),
        )
        .unwrap();
    assert_eq!(store.expire_due("alice", 10).unwrap(), 1);
    let current = store
        .get("alice", &queued.bundle.message.id)
        .unwrap()
        .unwrap()
        .deliveries
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(current.status, DeliveryStatus::Acknowledged);
    assert!(
        store
            .transport_receipts("alice", &leased.delivery.id)
            .unwrap()
            .len()
            == 1
    );
}

#[test]
fn late_external_receipt_after_reclaim_prevents_duplicate_delivery() {
    let (_directory, clock, store) = test_store();
    store
        .enqueue("alice", &message("external-reclaim", "body", "worker-a"))
        .unwrap();
    let leased = claim_one(&store, "alice", "worker-a", "consumer-a", 10);
    clock.set(timestamp(11));
    assert_eq!(store.reclaim_expired("alice", 10).unwrap(), 1);
    let recorded = store
        .mark_transport_delivered(
            "alice",
            &mailbox("worker-a"),
            &leased.receipt,
            &transport_receipt("late-after-reclaim"),
        )
        .unwrap();
    assert_eq!(recorded.delivery.status, DeliveryStatus::Delivered);
    assert_eq!(
        store
            .acknowledge("alice", &mailbox("worker-a"), &leased.receipt)
            .unwrap()
            .status,
        DeliveryStatus::Acknowledged
    );
    assert!(
        store
            .transport_receipts("alice", &leased.delivery.id)
            .unwrap()
            .len()
            == 1
    );
    assert!(
        store
            .claim(
                "alice",
                &ClaimQuery {
                    mailboxes: vec![mailbox("worker-a")],
                    limit: 1,
                },
                &LeaseRequest {
                    consumer: "duplicate-sender".into(),
                    lease_seconds: 10,
                },
            )
            .unwrap()
            .is_empty()
    );
    let current = store
        .get("alice", &leased.message.id)
        .unwrap()
        .unwrap()
        .deliveries
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(current.status, DeliveryStatus::Acknowledged);
}

#[test]
fn a_new_claim_generation_fences_the_old_external_receipt() {
    let (_directory, clock, store) = test_store();
    store
        .enqueue("alice", &message("external-new-claim", "body", "worker-a"))
        .unwrap();
    let first = claim_one(&store, "alice", "worker-a", "consumer-a", 10);
    clock.set(timestamp(11));
    assert_eq!(store.reclaim_expired("alice", 10).unwrap(), 1);
    let second = claim_one(&store, "alice", "worker-a", "consumer-b", 10);
    assert!(second.receipt.generation > first.receipt.generation);
    assert!(matches!(
        store.mark_transport_delivered(
            "alice",
            &mailbox("worker-a"),
            &first.receipt,
            &transport_receipt("stale-after-new-claim"),
        ),
        Err(MessageStoreError::InvalidReceipt { .. })
    ));
}

#[test]
fn concurrent_transport_receipt_replay_has_one_state_transition() {
    let (_directory, _clock, store) = test_store();
    store
        .enqueue("alice", &message("external-race", "body", "worker-a"))
        .unwrap();
    let leased = claim_one(&store, "alice", "worker-a", "consumer-a", 30);
    let barrier = Arc::new(Barrier::new(3));
    let spawn = || {
        let barrier = Arc::clone(&barrier);
        let store = store.clone();
        let lease_receipt = leased.receipt.clone();
        std::thread::spawn(move || {
            barrier.wait();
            store
                .mark_transport_delivered(
                    "alice",
                    &mailbox("worker-a"),
                    &lease_receipt,
                    &transport_receipt("race-external-id"),
                )
                .unwrap()
                .existing
        })
    };
    let first = spawn();
    let second = spawn();
    barrier.wait();
    let mut outcomes = [first.join().unwrap(), second.join().unwrap()];
    outcomes.sort_unstable();
    assert_eq!(outcomes, [false, true]);
    assert_eq!(
        store
            .events("alice", &leased.message.id)
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == vyane_message::MessageEventKind::Delivered)
            .count(),
        1
    );
}

#[test]
fn conversation_cursor_and_outbox_retry_are_stable() {
    let (_directory, _clock, store) = test_store();
    for key in ["one", "two", "three"] {
        store
            .enqueue("alice", &message(key, key, "worker-a"))
            .unwrap();
    }
    let first = store
        .list_conversation("alice", "conversation-1", None, 2)
        .unwrap();
    assert_eq!(first.items.len(), 2);
    let second = store
        .list_conversation("alice", "conversation-1", first.next_cursor.as_ref(), 2)
        .unwrap();
    assert_eq!(second.items.len(), 1);

    let outbox = store.unprojected_events("alice", "event-log", 2).unwrap();
    assert_eq!(outbox.items.len(), 2);
    assert!(outbox.has_more);
    let failed_event_id = outbox.items[0].event_id.clone();
    let successful_event_id = outbox.items[1].event_id.clone();
    store
        .mark_projected("alice", "event-log", &successful_event_id)
        .unwrap();
    let retry = store.unprojected_events("alice", "event-log", 10).unwrap();
    assert_eq!(retry.items[0].event_id, failed_event_id);
    assert!(
        retry
            .items
            .iter()
            .all(|event| event.event_id != successful_event_id)
    );
    store
        .mark_projected("alice", "event-log", &failed_event_id)
        .unwrap();
    store
        .mark_projected("alice", "event-log", &failed_event_id)
        .unwrap();
    assert!(
        store
            .unprojected_events("alice", "event-log", 10)
            .unwrap()
            .items
            .iter()
            .all(|event| event.event_id != failed_event_id)
    );
    let independent_sink = store
        .unprojected_events("alice", "message-broker", 10)
        .unwrap();
    assert!(
        independent_sink
            .items
            .iter()
            .any(|event| event.event_id == failed_event_id)
    );
    assert!(
        independent_sink
            .items
            .iter()
            .any(|event| event.event_id == successful_event_id)
    );
}

#[test]
fn claim_merges_multiple_mailboxes_in_one_global_fifo() {
    let (_directory, clock, store) = test_store();
    let mut later = message("later", "later", "worker-a");
    later.deliveries[0].available_at = Some(timestamp(5));
    store.enqueue("alice", &later).unwrap();
    store
        .enqueue("alice", &message("earlier", "earlier", "worker-b"))
        .unwrap();
    clock.set(timestamp(5));

    let claimed = store
        .claim(
            "alice",
            &ClaimQuery {
                mailboxes: vec![mailbox("worker-a"), mailbox("worker-b")],
                limit: 2,
            },
            &LeaseRequest {
                consumer: "consumer".into(),
                lease_seconds: 30,
            },
        )
        .unwrap();
    assert_eq!(claimed.len(), 2);
    assert_eq!(claimed[0].message.body, "earlier");
    assert_eq!(claimed[0].receipt.mailbox, mailbox("worker-b"));
    assert_eq!(claimed[1].message.body, "later");
    assert_eq!(claimed[1].receipt.mailbox, mailbox("worker-a"));

    assert!(matches!(
        store.claim(
            "alice",
            &ClaimQuery {
                mailboxes: vec![mailbox("worker-a"), mailbox("worker-a")],
                limit: 1,
            },
            &LeaseRequest {
                consumer: "consumer".into(),
                lease_seconds: 30,
            },
        ),
        Err(MessageStoreError::InvalidInput(_))
    ));
}

#[test]
fn concurrent_threads_claim_once() {
    let (directory, _clock, store) = test_store();
    store
        .enqueue("alice", &message("one", "body", "worker-a"))
        .unwrap();
    let path = directory.path().join("messages.sqlite3");
    let barrier = Arc::new(Barrier::new(3));
    let spawn = |consumer: &'static str| {
        let barrier = Arc::clone(&barrier);
        let path = path.clone();
        std::thread::spawn(move || {
            let store = SqliteMessageStore::open(path).unwrap();
            barrier.wait();
            store
                .claim(
                    "alice",
                    &ClaimQuery {
                        mailboxes: vec![mailbox("worker-a")],
                        limit: 1,
                    },
                    &LeaseRequest {
                        consumer: consumer.into(),
                        lease_seconds: 30,
                    },
                )
                .unwrap()
                .len()
        })
    };
    let first = spawn("a");
    let second = spawn("b");
    barrier.wait();
    assert_eq!(first.join().unwrap() + second.join().unwrap(), 1);
}

#[test]
fn concurrent_child_processes_claim_once() {
    let (directory, _clock, store) = test_store();
    store
        .enqueue("alice", &message("one", "body", "worker-a"))
        .unwrap();
    let executable = std::env::current_exe().unwrap();
    let path = directory.path().join("messages.sqlite3");
    let run = || {
        let executable = executable.clone();
        let path = path.clone();
        std::thread::spawn(move || {
            Command::new(executable)
                .args(["--exact", "contract_child_process", "--nocapture"])
                .env(CHILD_ENV, "1")
                .env(CHILD_DB_ENV, path)
                .output()
                .unwrap()
        })
    };
    let first = run();
    let second = run();
    let outputs = [first.join().unwrap(), second.join().unwrap()];
    assert!(outputs.iter().all(|output| output.status.success()));
    let total = outputs
        .iter()
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .matches("CLAIMED=1")
                .count()
        })
        .sum::<usize>();
    let final_delivery = store
        .get(
            "alice",
            &store
                .list_conversation("alice", "conversation-1", None, 1)
                .unwrap()
                .items[0]
                .message
                .id,
        )
        .unwrap()
        .unwrap()
        .deliveries[0]
        .clone();
    assert_eq!(
        total,
        1,
        "path={} final={final_delivery:?} child outputs: {} | {}",
        path.display(),
        String::from_utf8_lossy(&outputs[0].stdout),
        String::from_utf8_lossy(&outputs[1].stdout)
    );
}

#[test]
fn contract_child_process() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }
    let path = std::env::var_os(CHILD_DB_ENV).unwrap();
    let store = SqliteMessageStore::open(path).unwrap();
    let claimed = store
        .claim(
            "alice",
            &ClaimQuery {
                mailboxes: vec![mailbox("worker-a")],
                limit: 1,
            },
            &LeaseRequest {
                consumer: format!("child-{}", std::process::id()),
                lease_seconds: 30,
            },
        )
        .unwrap();
    if let Some(delivery) = claimed.first() {
        println!(
            "CLAIMED=1 generation={} updated={} expires={} now={}",
            delivery.delivery.lease_generation,
            delivery.delivery.updated_at.timestamp_millis(),
            delivery
                .delivery
                .lease_expires_at
                .unwrap()
                .timestamp_millis(),
            Utc::now().timestamp_millis()
        );
    } else {
        println!("CLAIMED=0 now={}", Utc::now().timestamp_millis());
    }
}

#[test]
fn newer_schema_and_corrupt_rows_fail_closed() {
    let (directory, _clock, store) = test_store();
    let path = directory.path().join("messages.sqlite3");
    drop(store);
    let connection = Connection::open(&path).unwrap();
    connection.pragma_update(None, "user_version", 99).unwrap();
    drop(connection);
    assert!(matches!(
        SqliteMessageStore::open(&path),
        Err(MessageStoreError::UnsupportedSchema { found: 99, .. })
    ));
}

#[test]
fn altered_or_extended_schema_fails_closed() {
    let trigger_directory = TempDir::new().unwrap();
    let trigger_path = trigger_directory.path().join("messages.sqlite3");
    drop(SqliteMessageStore::open(&trigger_path).unwrap());
    let connection = Connection::open(&trigger_path).unwrap();
    connection
        .execute_batch(
            "DROP TRIGGER messages_immutable_update; \
             CREATE TRIGGER messages_immutable_update BEFORE UPDATE ON messages \
             BEGIN SELECT 1; END;",
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteMessageStore::open(&trigger_path),
        Err(MessageStoreError::CorruptData(_))
    ));

    let extra_directory = TempDir::new().unwrap();
    let extra_path = extra_directory.path().join("messages.sqlite3");
    drop(SqliteMessageStore::open(&extra_path).unwrap());
    let connection = Connection::open(&extra_path).unwrap();
    connection
        .execute("CREATE TABLE sqliteXevil (value TEXT)", [])
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteMessageStore::open(&extra_path),
        Err(MessageStoreError::CorruptData(_))
    ));
}

#[test]
fn relational_corruption_fails_the_explicit_and_open_audits() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("messages.sqlite3");
    let store = SqliteMessageStore::open(&path).unwrap();
    store
        .enqueue("alice", &message("relational", "body", "worker-a"))
        .unwrap();
    let _leased = claim_one(&store, "alice", "worker-a", "consumer-a", 30);
    drop(store);

    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "DROP TRIGGER delivery_attempts_immutable_delete; \
             DELETE FROM delivery_attempts; \
             CREATE TRIGGER delivery_attempts_immutable_delete \
             BEFORE DELETE ON delivery_attempts \
             BEGIN SELECT RAISE(ABORT, 'delivery attempts are immutable'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteMessageStore::open(&path),
        Err(MessageStoreError::CorruptData(_))
    ));
}

#[test]
fn publication_audit_binds_release_time_to_each_initial_enqueue_event() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("messages.sqlite3");
    let store = SqliteMessageStore::open(&path).unwrap();
    let request = message("publication-time-corruption", "body", "worker-a");
    let key = request.idempotency.clone();
    let digest = request.request_digest().unwrap();
    store.stage("alice", &request).unwrap();
    store
        .publish_staged("alice", &key, &digest)
        .unwrap()
        .unwrap();

    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "DROP TRIGGER message_publications_guard_update; \
             UPDATE message_publications SET published_at_ms = published_at_ms + 1 \
             WHERE owner = 'alice'; \
             CREATE TRIGGER message_publications_guard_update \
             BEFORE UPDATE ON message_publications \
             WHEN NOT ( \
                 OLD.origin = 'staged' AND OLD.status = 'staged' \
                 AND NEW.owner = OLD.owner AND NEW.message_id = OLD.message_id \
                 AND NEW.conversation_id = OLD.conversation_id \
                 AND NEW.origin = OLD.origin AND NEW.status IN ('published', 'discarded') \
                 AND NEW.revision = OLD.revision + 1 \
                 AND NEW.record_schema = OLD.record_schema \
             ) BEGIN SELECT RAISE(ABORT, 'invalid message publication transition'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        store.audit_integrity(),
        Err(MessageStoreError::CorruptData(_))
    ));
    drop(store);
    assert!(matches!(
        SqliteMessageStore::open(&path),
        Err(MessageStoreError::CorruptData(_))
    ));
}

#[test]
fn publication_audit_rejects_initial_enqueue_time_drift() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("messages.sqlite3");
    let store = SqliteMessageStore::open(&path).unwrap();
    store
        .enqueue(
            "alice",
            &message("event-time-corruption", "body", "worker-a"),
        )
        .unwrap();

    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "DROP TRIGGER message_events_immutable_update; \
             UPDATE message_events SET occurred_at_ms = occurred_at_ms + 1 \
             WHERE owner = 'alice' AND delivery_revision = 0; \
             CREATE TRIGGER message_events_immutable_update \
             BEFORE UPDATE ON message_events \
             BEGIN SELECT RAISE(ABORT, 'message events are immutable'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        store.audit_integrity(),
        Err(MessageStoreError::CorruptData(_))
    ));
}

#[cfg(unix)]
#[test]
fn database_files_are_private_without_mutating_existing_parent() {
    use std::os::unix::fs::PermissionsExt as _;

    let directory = TempDir::new().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o750)).unwrap();
    let path = directory.path().join("messages.sqlite3");
    let store = SqliteMessageStore::open(&path).unwrap();
    store
        .enqueue("alice", &message("one", "body", "worker-a"))
        .unwrap();
    assert_eq!(
        std::fs::metadata(directory.path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o750
    );
    for entry in std::fs::read_dir(directory.path()).unwrap() {
        assert_eq!(
            entry.unwrap().metadata().unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    let private_parent = directory.path().join("private-store");
    SqliteMessageStore::open(private_parent.join("messages.sqlite3")).unwrap();
    assert_eq!(
        std::fs::metadata(private_parent)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
}

#[cfg(unix)]
#[test]
fn database_and_live_wal_files_with_broad_permissions_fail_closed() {
    use std::os::unix::fs::PermissionsExt as _;

    let directory = TempDir::new().unwrap();
    let path = directory.path().join("messages.sqlite3");
    let store = SqliteMessageStore::open(&path).unwrap();
    let message_id = store
        .enqueue("alice", &message("one", "body", "worker-a"))
        .unwrap()
        .bundle
        .message
        .id;

    // Keep one SQLite connection alive so WAL and SHM remain present while a
    // second store validates them. Permission checks must use metadata only:
    // raw reopening either inode here would cancel this process's POSIX locks.
    let live_connection = Connection::open(&path).unwrap();
    live_connection
        .pragma_update(None, "journal_mode", "WAL")
        .unwrap();
    let _: i64 = live_connection
        .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
        .unwrap();
    let wal = directory.path().join("messages.sqlite3-wal");
    let shm = directory.path().join("messages.sqlite3-shm");
    assert!(wal.is_file());
    assert!(shm.is_file());

    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    assert!(matches!(
        SqliteMessageStore::open(&path),
        Err(MessageStoreError::InvalidInput(_))
    ));
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

    for sidecar in [&wal, &shm] {
        std::fs::set_permissions(sidecar, std::fs::Permissions::from_mode(0o640)).unwrap();
        let result = store.get("alice", &message_id);
        assert!(
            matches!(result, Err(MessageStoreError::InvalidInput(_))),
            "sidecar={} mode={:o} result={result:?}",
            sidecar.display(),
            std::fs::metadata(sidecar).unwrap().permissions().mode() & 0o7777
        );
        std::fs::set_permissions(sidecar, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

#[cfg(unix)]
#[test]
fn database_and_sidecar_symlinks_fail_closed() {
    use std::os::unix::fs::{PermissionsExt as _, symlink};

    let directory = TempDir::new().unwrap();
    let victim = directory.path().join("victim");
    std::fs::write(&victim, b"unchanged").unwrap();
    let database_link = directory.path().join("linked.sqlite3");
    symlink(&victim, &database_link).unwrap();
    assert!(SqliteMessageStore::open(&database_link).is_err());
    assert_eq!(std::fs::read(&victim).unwrap(), b"unchanged");

    let database = directory.path().join("messages.sqlite3");
    drop(SqliteMessageStore::open(&database).unwrap());
    let wal = directory.path().join("messages.sqlite3-wal");
    if wal.exists() {
        std::fs::remove_file(&wal).unwrap();
    }
    symlink(&victim, &wal).unwrap();
    assert!(SqliteMessageStore::open(&database).is_err());
    assert_eq!(std::fs::read(&victim).unwrap(), b"unchanged");

    let writable_parent = directory.path().join("shared");
    std::fs::create_dir(&writable_parent).unwrap();
    std::fs::set_permissions(&writable_parent, std::fs::Permissions::from_mode(0o770)).unwrap();
    assert!(matches!(
        SqliteMessageStore::open(writable_parent.join("messages.sqlite3")),
        Err(MessageStoreError::InvalidInput(_))
    ));
    assert_eq!(
        std::fs::metadata(writable_parent)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o770
    );
}

#[test]
fn bounds_fail_before_persistence() {
    let (_directory, _clock, store) = test_store();
    let mut oversized = message("big", "body", "worker-a");
    oversized.body = "x".repeat(256 * 1024 + 1);
    assert!(matches!(
        store.enqueue("alice", &oversized),
        Err(MessageStoreError::InvalidInput(_))
    ));
    let mut sub_millisecond_ttl = message("sub-ms", "body", "worker-a");
    sub_millisecond_ttl.deliveries[0].expires_at =
        Some(timestamp(0) + TimeDelta::microseconds(500));
    assert!(matches!(
        store.enqueue("alice", &sub_millisecond_ttl),
        Err(MessageStoreError::InvalidInput(_))
    ));
    assert!(
        store
            .list_conversation("alice", "conversation-1", None, 1)
            .unwrap()
            .items
            .is_empty()
    );
}

#[test]
fn staged_message_is_hidden_from_all_ordinary_surfaces() {
    let (directory, clock, store) = test_store();
    let canary = "STAGED-BODY-MUST-STAY-HIDDEN";
    let mut request = message("staged-hidden", canary, "worker-a");
    request.deliveries[0].expires_at = Some(timestamp(5));
    let key = request.idempotency.clone();
    let digest = request.request_digest().unwrap();

    let staged = store.stage("alice", &request).unwrap();
    assert!(!staged.existing);
    assert_eq!(staged.resolution.status, MessagePublicationStatus::Staged);
    assert!(!format!("{staged:?}").contains(canary));
    assert!(!serde_json::to_string(&staged).unwrap().contains(canary));

    let replay = store.stage("alice", &request).unwrap();
    assert!(replay.existing);
    assert_eq!(replay.resolution.status, MessagePublicationStatus::Staged);
    assert_eq!(replay.resolution.message_id, staged.resolution.message_id);
    assert!(
        store
            .resolve_idempotency("alice", &key, &digest)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .resolve_publication("alice", &key, &digest)
            .unwrap()
            .unwrap()
            .status,
        MessagePublicationStatus::Staged
    );
    assert!(
        store
            .get("alice", &staged.resolution.message_id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .list_conversation("alice", "conversation-1", None, 10)
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        store
            .events("alice", &staged.resolution.message_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .unprojected_events("alice", "projector", 10)
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        store
            .claim(
                "alice",
                &ClaimQuery {
                    mailboxes: vec![mailbox("worker-a")],
                    limit: 10,
                },
                &LeaseRequest {
                    consumer: "consumer-a".into(),
                    lease_seconds: 30,
                },
            )
            .unwrap()
            .is_empty()
    );

    let connection = Connection::open(directory.path().join("messages.sqlite3")).unwrap();
    let delivery_id: String = connection
        .query_row(
            "SELECT id FROM deliveries WHERE owner = 'alice' AND message_id = ?1",
            [&staged.resolution.message_id],
            |row| row.get(0),
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        store.cancel("alice", &delivery_id),
        Err(MessageStoreError::NotFound)
    ));
    clock.set(timestamp(10));
    assert_eq!(store.expire_due("alice", 10).unwrap(), 0);

    assert!(
        store
            .resolve_idempotency("bob", &key, &digest)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .publish_staged("bob", &key, &digest)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .discard_staged("bob", &key, &digest)
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        store.enqueue("alice", &request),
        Err(MessageStoreError::IdempotencyConflict)
    ));
    let mut drift = request.clone();
    drift.body = "different".into();
    assert!(
        store
            .resolve_idempotency("alice", &key, &drift.request_digest().unwrap())
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        store.resolve_publication("alice", &key, &drift.request_digest().unwrap()),
        Err(MessageStoreError::IdempotencyConflict)
    ));
    assert!(matches!(
        store.stage("alice", &drift),
        Err(MessageStoreError::IdempotencyConflict)
    ));
    store.audit_integrity().unwrap();
}

#[test]
fn publish_staged_releases_exactly_once_across_restart() {
    let (directory, _clock, store) = test_store();
    let mut request = message("release", "released-body", "worker-a");
    request.deliveries.push(NewDelivery {
        route: "worker".into(),
        target: endpoint(EndpointKind::Worker, "worker-b"),
        available_at: None,
        expires_at: None,
        max_attempts: 3,
    });
    let key = request.idempotency.clone();
    let digest = request.request_digest().unwrap();
    let staged = store.stage("alice", &request).unwrap();
    drop(store);

    let store = SqliteMessageStore::open(directory.path().join("messages.sqlite3")).unwrap();
    assert_eq!(
        store
            .resolve_publication("alice", &key, &digest)
            .unwrap()
            .unwrap()
            .status,
        MessagePublicationStatus::Staged
    );
    let mut drift = request.clone();
    drift.body = "wrong".into();
    assert!(matches!(
        store.publish_staged("alice", &key, &drift.request_digest().unwrap()),
        Err(MessageStoreError::IdempotencyConflict)
    ));
    assert!(
        store
            .events("alice", &staged.resolution.message_id)
            .unwrap()
            .is_empty()
    );

    let published = store
        .publish_staged("alice", &key, &digest)
        .unwrap()
        .unwrap();
    assert!(!published.existing);
    assert_eq!(
        published.resolution.status,
        MessagePublicationStatus::Published
    );
    assert_eq!(
        store
            .get("alice", &published.resolution.message_id)
            .unwrap()
            .unwrap()
            .message
            .body,
        "released-body"
    );
    assert_eq!(
        store
            .events("alice", &published.resolution.message_id)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        store
            .unprojected_events("alice", "projector", 10)
            .unwrap()
            .items
            .len(),
        2
    );
    let replay = store
        .publish_staged("alice", &key, &digest)
        .unwrap()
        .unwrap();
    assert!(replay.existing);
    assert_eq!(
        store
            .events("alice", &published.resolution.message_id)
            .unwrap()
            .len(),
        2
    );
    assert!(matches!(
        store.enqueue("alice", &request),
        Err(MessageStoreError::IdempotencyConflict)
    ));
    let staged_replay = store.stage("alice", &request).unwrap();
    assert!(staged_replay.existing);
    assert_eq!(
        staged_replay.resolution.status,
        MessagePublicationStatus::Published
    );
    let leased = claim_one(&store, "alice", "worker-a", "consumer-a", 30);
    assert_eq!(leased.message.id, published.resolution.message_id);
    store.audit_integrity().unwrap();
}

#[test]
fn discard_staged_is_terminal_hidden_and_idempotent() {
    let (_directory, _clock, store) = test_store();
    let request = message("discard", "discarded-body", "worker-a");
    let key = request.idempotency.clone();
    let digest = request.request_digest().unwrap();
    let staged = store.stage("alice", &request).unwrap();

    let discarded = store
        .discard_staged("alice", &key, &digest)
        .unwrap()
        .unwrap();
    assert!(!discarded.existing);
    assert_eq!(
        discarded.resolution.status,
        MessagePublicationStatus::Discarded
    );
    assert!(
        store
            .discard_staged("alice", &key, &digest)
            .unwrap()
            .unwrap()
            .existing
    );
    assert!(matches!(
        store.publish_staged("alice", &key, &digest),
        Err(MessageStoreError::PublicationConflict)
    ));
    let replay = store.stage("alice", &request).unwrap();
    assert!(replay.existing);
    assert_eq!(
        replay.resolution.status,
        MessagePublicationStatus::Discarded
    );
    assert!(matches!(
        store.enqueue("alice", &request),
        Err(MessageStoreError::IdempotencyConflict)
    ));
    assert!(
        store
            .get("alice", &staged.resolution.message_id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .events("alice", &staged.resolution.message_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .unprojected_events("alice", "projector", 10)
            .unwrap()
            .items
            .is_empty()
    );
    store.audit_integrity().unwrap();
}

#[test]
fn publish_and_discard_race_has_one_terminal_truth() {
    let (_directory, _clock, store) = test_store();
    let store = Arc::new(store);
    let request = message("race-publication", "body", "worker-a");
    let key = request.idempotency.clone();
    let digest = request.request_digest().unwrap();
    let message_id = store
        .stage("alice", &request)
        .unwrap()
        .resolution
        .message_id;
    let barrier = Arc::new(Barrier::new(3));

    let publish_store = Arc::clone(&store);
    let publish_barrier = Arc::clone(&barrier);
    let publish_key = key.clone();
    let publish_digest = digest.clone();
    let publish = std::thread::spawn(move || {
        publish_barrier.wait();
        publish_store.publish_staged("alice", &publish_key, &publish_digest)
    });
    let discard_store = Arc::clone(&store);
    let discard_barrier = Arc::clone(&barrier);
    let discard_key = key.clone();
    let discard_digest = digest.clone();
    let discard = std::thread::spawn(move || {
        discard_barrier.wait();
        discard_store.discard_staged("alice", &discard_key, &discard_digest)
    });
    barrier.wait();
    let publish = publish.join().unwrap();
    let discard = discard.join().unwrap();
    let loser = match (publish, discard) {
        (Ok(Some(winner)), Err(loser)) | (Err(loser), Ok(Some(winner))) => {
            assert!(!winner.existing);
            loser
        }
        results => panic!("unexpected publication race results: {results:?}"),
    };
    assert!(matches!(loser, MessageStoreError::PublicationConflict));

    let status = store
        .resolve_publication("alice", &key, &digest)
        .unwrap()
        .unwrap()
        .status;
    match status {
        MessagePublicationStatus::Published => {
            assert!(store.get("alice", &message_id).unwrap().is_some());
            assert_eq!(store.events("alice", &message_id).unwrap().len(), 1);
        }
        MessagePublicationStatus::Discarded => {
            assert!(store.get("alice", &message_id).unwrap().is_none());
            assert!(store.events("alice", &message_id).unwrap().is_empty());
        }
        MessagePublicationStatus::Staged => panic!("race did not reach a terminal state"),
    }
    store.audit_integrity().unwrap();
}

#[test]
fn ordinary_publication_cannot_be_reclassified_as_staged() {
    let (_directory, _clock, store) = test_store();
    let request = message("ordinary-first", "body", "worker-a");
    let key = request.idempotency.clone();
    let digest = request.request_digest().unwrap();
    store.enqueue("alice", &request).unwrap();
    assert!(matches!(
        store.stage("alice", &request),
        Err(MessageStoreError::IdempotencyConflict)
    ));
    assert!(matches!(
        store.publish_staged("alice", &key, &digest),
        Err(MessageStoreError::IdempotencyConflict)
    ));
    assert!(matches!(
        store.discard_staged("alice", &key, &digest),
        Err(MessageStoreError::IdempotencyConflict)
    ));
    assert_eq!(
        store
            .resolve_idempotency("alice", &key, &digest)
            .unwrap()
            .unwrap()
            .status,
        MessagePublicationStatus::Published
    );
}

#[test]
fn staged_messages_enter_conversation_order_only_when_published() {
    let (_directory, _clock, store) = test_store();
    let staged_request = message("ordered-stage", "staged", "worker-a");
    let staged_key = staged_request.idempotency.clone();
    let staged_digest = staged_request.request_digest().unwrap();
    let staged_id = store
        .stage("alice", &staged_request)
        .unwrap()
        .resolution
        .message_id;
    let ordinary = store
        .enqueue(
            "alice",
            &message("ordered-ordinary", "ordinary", "worker-a"),
        )
        .unwrap();

    let before = store
        .list_conversation("alice", "conversation-1", None, 1)
        .unwrap();
    assert_eq!(before.items.len(), 1);
    assert_eq!(before.items[0].message.id, ordinary.bundle.message.id);
    assert_eq!(before.items[0].message.conversation_sequence, 1);
    let cursor = vyane_message::MessageCursor {
        conversation_id: before.items[0].message.conversation_id.clone(),
        sequence: before.items[0].message.conversation_sequence,
        id: before.items[0].message.id.clone(),
    };

    store
        .publish_staged("alice", &staged_key, &staged_digest)
        .unwrap()
        .unwrap();
    let after_cursor = store
        .list_conversation("alice", "conversation-1", Some(&cursor), 10)
        .unwrap();
    assert_eq!(after_cursor.items.len(), 1);
    assert_eq!(after_cursor.items[0].message.id, staged_id);
    assert_eq!(after_cursor.items[0].message.conversation_sequence, 2);
    let all = store
        .list_conversation("alice", "conversation-1", None, 10)
        .unwrap();
    assert_eq!(
        all.items
            .iter()
            .map(|item| (item.message.id.as_str(), item.message.conversation_sequence))
            .collect::<Vec<_>>(),
        vec![
            (ordinary.bundle.message.id.as_str(), 1),
            (staged_id.as_str(), 2)
        ]
    );
    store.audit_integrity().unwrap();
}

#[cfg(unix)]
fn create_v1_database(path: &std::path::Path, with_message: bool) -> Option<(NewMessage, String)> {
    use std::os::unix::fs::PermissionsExt as _;

    let mut connection = Connection::open(path).unwrap();
    connection
        .execute_batch(include_str!("../migrations/0001_messages.sql"))
        .unwrap();
    let result = if with_message {
        let request = message("legacy", "legacy-body", "worker-a");
        let digest = request.request_digest().unwrap();
        let message_id = uuid::Uuid::now_v7().to_string();
        let delivery_id = uuid::Uuid::now_v7().to_string();
        let event_id = uuid::Uuid::now_v7().to_string();
        let now = timestamp(0).timestamp_millis();
        let transaction = connection.transaction().unwrap();
        transaction
            .execute(
                "INSERT INTO conversation_sequences(owner, conversation_id, next_sequence) \
                 VALUES ('alice', 'conversation-1', 2)",
                [],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO owner_event_sequences(owner, next_sequence) VALUES ('alice', 2)",
                [],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO messages( \
                    id, record_schema, owner, conversation_id, conversation_sequence, session_id, \
                    direction, kind, sender_kind, sender_id, body, payload_json, reply_to, trace_id, \
                    correlation_id, producer, idempotency_key, request_digest, created_at_ms \
                 ) VALUES ( \
                    ?1, 1, 'alice', 'conversation-1', 1, 'session-1', 'internal', 'message', \
                    'agent', 'sender', 'legacy-body', '{\"safe\":\"shape\"}', NULL, 'trace-1', \
                    'correlation-1', 'test-producer', 'legacy', ?2, ?3 \
                 )",
                rusqlite::params![message_id, digest.as_str(), now],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO deliveries( \
                    id, record_schema, owner, message_id, route, target_kind, target_id, status, \
                    available_at_ms, expires_at_ms, attempt_count, max_attempts, revision, \
                    lease_generation, lease_owner, lease_token_hash, lease_expires_at_ms, \
                    first_delivered_at_ms, acknowledged_at_ms, dead_lettered_at_ms, failure_code, \
                    created_at_ms, updated_at_ms \
                 ) VALUES ( \
                    ?1, 1, 'alice', ?2, 'worker', 'worker', 'worker-a', 'pending', ?3, NULL, 0, 3, \
                    0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?3, ?3 \
                 )",
                rusqlite::params![delivery_id, message_id, now],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO message_events( \
                    sequence, event_id, owner, message_id, delivery_id, delivery_revision, \
                    conversation_id, conversation_sequence, occurred_at_ms, event_type, \
                    from_status, to_status, lease_generation, route, target_kind, target_id, \
                    direction, reply_to \
                 ) VALUES ( \
                    1, ?1, 'alice', ?2, ?3, 0, 'conversation-1', 1, ?4, 'enqueued', NULL, \
                    'pending', 0, 'worker', 'worker', 'worker-a', 'internal', NULL \
                 )",
                rusqlite::params![event_id, message_id, delivery_id, now],
            )
            .unwrap();
        transaction.commit().unwrap();
        Some((request, message_id))
    } else {
        None
    };
    connection.pragma_update(None, "user_version", 1).unwrap();
    drop(connection);
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    result
}

#[cfg(unix)]
#[test]
fn schema_v1_migrates_existing_messages_to_ordinary_published_truth() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("messages.sqlite3");
    let (request, message_id) = create_v1_database(&path, true).unwrap();
    let digest = request.request_digest().unwrap();

    let store = SqliteMessageStore::open(&path).unwrap();
    assert_eq!(
        store
            .resolve_idempotency("alice", &request.idempotency, &digest)
            .unwrap()
            .unwrap()
            .status,
        MessagePublicationStatus::Published
    );
    assert_eq!(
        store
            .get("alice", &message_id)
            .unwrap()
            .unwrap()
            .message
            .body,
        "legacy-body"
    );
    store.audit_integrity().unwrap();
    drop(store);

    let connection = Connection::open(&path).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 2);
    let publication: (String, String, i64) = connection
        .query_row(
            "SELECT origin, status, revision FROM message_publications \
             WHERE owner = 'alice' AND message_id = ?1",
            [&message_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(publication, ("ordinary".into(), "published".into(), 0));
}

#[cfg(unix)]
#[test]
fn schema_v1_upgrade_rolls_back_atomically_when_final_manifest_is_invalid() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("messages.sqlite3");
    create_v1_database(&path, false);
    let connection = Connection::open(&path).unwrap();
    connection
        .execute("CREATE TABLE unexpected_private_state(value TEXT)", [])
        .unwrap();
    drop(connection);

    assert!(matches!(
        SqliteMessageStore::open(&path),
        Err(MessageStoreError::CorruptData(_))
    ));
    let connection = Connection::open(&path).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 1);
    let migrated_table_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema \
             WHERE type = 'table' AND name = 'message_publications'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(migrated_table_count, 0);
}
