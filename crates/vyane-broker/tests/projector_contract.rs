#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use tempfile::TempDir;
use vyane_broker::{BrokerScope, MessageBroker, MessageEventProjector};
use vyane_ledger::EventLog;
use vyane_message::{
    EndpointKind, EndpointRef, IdempotencyKey, MessageDirection, MessageStore, NewDelivery,
    NewMessage, SqliteMessageStore,
};

struct Fixture {
    directory: TempDir,
    store: Arc<dyn MessageStore>,
    scope: BrokerScope,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let concrete =
            Arc::new(SqliteMessageStore::open(directory.path().join("messages.sqlite3")).unwrap());
        let store: Arc<dyn MessageStore> = concrete.clone();
        Self {
            directory,
            store,
            scope: BrokerScope::new("owner-a").unwrap(),
        }
    }

    async fn enqueue_secret(&self) {
        MessageBroker::new(self.scope.clone(), Arc::clone(&self.store))
            .publish(NewMessage {
                conversation_id: "CONVERSATION_MUST_NOT_REACH_EVENT_LOG".into(),
                session_id: Some("session-secret".into()),
                direction: MessageDirection::Internal,
                kind: "message".into(),
                sender: EndpointRef {
                    kind: EndpointKind::Agent,
                    id: "agent-1".into(),
                },
                body: "BODY_MUST_NOT_REACH_EVENT_LOG".into(),
                payload: serde_json::json!({"secret": "PAYLOAD_MUST_NOT_REACH_EVENT_LOG"}),
                reply_to: None,
                trace_id: Some("trace-private".into()),
                correlation_id: Some("correlation-private".into()),
                idempotency: IdempotencyKey {
                    producer: "test".into(),
                    key: "project".into(),
                },
                deliveries: vec![NewDelivery {
                    route: "ROUTE_MUST_NOT_REACH_EVENT_LOG".into(),
                    target: EndpointRef {
                        kind: EndpointKind::Worker,
                        id: "TARGET_MUST_NOT_REACH_EVENT_LOG".into(),
                    },
                    available_at: None,
                    expires_at: None,
                    max_attempts: 3,
                }],
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn projection_is_body_free_and_each_projector_has_independent_progress() {
    let fixture = Fixture::new();
    fixture.enqueue_secret().await;
    let events = fixture.directory.path().join("events");
    let first = MessageEventProjector::with_identity(
        fixture.scope.clone(),
        Arc::clone(&fixture.store),
        EventLog::new(&events),
        "projector-one",
        "stream-one",
    );
    let second = MessageEventProjector::with_identity(
        fixture.scope.clone(),
        Arc::clone(&fixture.store),
        EventLog::new(&events),
        "projector-two",
        "stream-two",
    );

    assert_eq!(first.project_once(100).await.unwrap().projected, 1);
    assert_eq!(first.project_once(100).await.unwrap().projected, 0);
    assert_eq!(second.project_once(100).await.unwrap().projected, 1);
    let first_page = EventLog::new(&events)
        .read_after("owner-a", "stream-one", 0, 100)
        .await
        .unwrap();
    let second_page = EventLog::new(&events)
        .read_after("owner-a", "stream-two", 0, 100)
        .await
        .unwrap();
    assert_eq!(first_page.events.len(), 1);
    assert_eq!(second_page.events.len(), 1);
    assert_eq!(
        first_page.events[0].event_id,
        second_page.events[0].event_id
    );

    let all_files = walk_text(&events);
    assert!(!all_files.contains("BODY_MUST_NOT_REACH_EVENT_LOG"));
    assert!(!all_files.contains("PAYLOAD_MUST_NOT_REACH_EVENT_LOG"));
    assert!(!all_files.contains("TARGET_MUST_NOT_REACH_EVENT_LOG"));
    assert!(!all_files.contains("ROUTE_MUST_NOT_REACH_EVENT_LOG"));
    assert!(!all_files.contains("CONVERSATION_MUST_NOT_REACH_EVENT_LOG"));
    assert!(!all_files.contains("trace-private"));
    assert!(!all_files.contains("correlation-private"));
}

#[tokio::test]
async fn failed_append_does_not_advance_outbox_progress() {
    let fixture = Fixture::new();
    fixture.enqueue_secret().await;
    let invalid_root = fixture.directory.path().join("not-a-directory");
    std::fs::write(&invalid_root, b"file").unwrap();
    let failing = MessageEventProjector::with_identity(
        fixture.scope.clone(),
        Arc::clone(&fixture.store),
        EventLog::new(invalid_root),
        "retry-projector",
        "message-lifecycle",
    );
    assert!(failing.project_once(100).await.is_err());

    let events = fixture.directory.path().join("retry-events");
    let retry = MessageEventProjector::with_identity(
        fixture.scope.clone(),
        Arc::clone(&fixture.store),
        EventLog::new(&events),
        "retry-projector",
        "message-lifecycle",
    );
    assert_eq!(retry.project_once(100).await.unwrap().projected, 1);
    assert_eq!(
        EventLog::new(events)
            .read_after("owner-a", "message-lifecycle", 0, 100)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
}

fn walk_text(root: &std::path::Path) -> String {
    let mut output = String::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            for entry in std::fs::read_dir(path).unwrap() {
                pending.push(entry.unwrap().path());
            }
        } else if let Ok(text) = std::fs::read_to_string(path) {
            output.push_str(&text);
        }
    }
    output
}
