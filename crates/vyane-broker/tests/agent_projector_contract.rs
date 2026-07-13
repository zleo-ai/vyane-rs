#![allow(clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::Utc;
use tempfile::TempDir;
use vyane_agent::{AgentStore, NewAgentRun, NewWorker, RunMode, SqliteAgentStore};
use vyane_broker::{AgentEventProjector, BrokerScope};
use vyane_ledger::{EventCategory, EventLog, EventSource};

fn digest(byte: char) -> String {
    std::iter::repeat_n(byte, 64).collect()
}

fn add_root(store: &dyn AgentStore, owner: &str, suffix: &str, include_canaries: bool) {
    let logical_session_id = include_canaries.then(|| "LOGICAL_SESSION_MUST_NOT_REACH_LOG".into());
    let task_id = include_canaries.then(|| "TASK_ID_MUST_NOT_REACH_LOG".into());
    let trace_id = include_canaries.then(|| "TRACE_ID_MUST_NOT_REACH_LOG".into());
    let target_key = if include_canaries {
        "TARGET_KEY_MUST_NOT_REACH_LOG"
    } else {
        "native/default"
    };
    store
        .create_root(
            owner,
            &NewWorker {
                id: format!("worker-{suffix}"),
                logical_session_id,
            },
            &NewAgentRun {
                id: format!("run-{suffix}"),
                worker_id: format!("worker-{suffix}"),
                task_id,
                trace_id,
                parent_run_id: None,
                mode: RunMode::Autonomous,
                target_key: target_key.into(),
                prompt_digest: digest('a'),
                policy_digest: digest('b'),
                available_at: Utc::now(),
                timeout_seconds: 60,
                max_resume_attempts: 1,
            },
        )
        .unwrap();
}

#[tokio::test]
async fn projection_is_bounded_body_free_and_owner_projector_scoped() {
    let directory = TempDir::new().unwrap();
    let concrete =
        Arc::new(SqliteAgentStore::open(directory.path().join("agent.sqlite3")).unwrap());
    add_root(concrete.as_ref(), "owner-a", "a", true);
    add_root(concrete.as_ref(), "owner-b", "b", false);
    let store: Arc<dyn AgentStore> = concrete;
    let event_root = directory.path().join("events");

    let first = AgentEventProjector::with_identity(
        BrokerScope::new("owner-a").unwrap(),
        Arc::clone(&store),
        EventLog::new(&event_root),
        "projector-one",
        "stream-one",
    );
    let report = first.project_once(1).await.unwrap();
    assert_eq!(report.read, 1);
    assert_eq!(report.projected, 1);
    assert!(report.has_more);
    assert_eq!(
        EventLog::new(&event_root)
            .read_after("owner-a", "stream-one", 0, 10)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
    assert!(
        EventLog::new(&event_root)
            .read_after("owner-b", "stream-one", 0, 10)
            .await
            .unwrap()
            .events
            .is_empty()
    );
    assert_eq!(first.project_once(10).await.unwrap().projected, 1);
    assert_eq!(first.project_once(10).await.unwrap().projected, 0);

    let independent = AgentEventProjector::with_identity(
        BrokerScope::new("owner-a").unwrap(),
        Arc::clone(&store),
        EventLog::new(&event_root),
        "projector-two",
        "stream-two",
    );
    assert_eq!(independent.project_once(10).await.unwrap().projected, 2);

    let owner_b = AgentEventProjector::with_identity(
        BrokerScope::new("owner-b").unwrap(),
        Arc::clone(&store),
        EventLog::new(&event_root),
        "projector-one",
        "stream-one",
    );
    assert_eq!(owner_b.project_once(10).await.unwrap().projected, 2);

    let first_events = EventLog::new(&event_root)
        .read_after("owner-a", "stream-one", 0, 10)
        .await
        .unwrap()
        .events;
    let independent_events = EventLog::new(&event_root)
        .read_after("owner-a", "stream-two", 0, 10)
        .await
        .unwrap()
        .events;
    assert_eq!(first_events.len(), 2);
    assert_eq!(independent_events.len(), 2);
    assert_eq!(
        first_events
            .iter()
            .map(|event| event.event_id.as_str())
            .collect::<Vec<_>>(),
        independent_events
            .iter()
            .map(|event| event.event_id.as_str())
            .collect::<Vec<_>>()
    );
    assert!(first_events.iter().all(|event| {
        event.owner == "owner-a"
            && event.category == EventCategory::Lifecycle
            && event.source == EventSource::Daemon
            && event.event_type.starts_with("agent.")
            && event.trace_id.is_none()
            && event.summary.is_none()
    }));
    let payload_keys = first_events
        .iter()
        .flat_map(|event| event.payload.keys().map(String::as_str))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        payload_keys,
        BTreeSet::from([
            "agent_event_sequence",
            "run_id",
            "run_revision",
            "run_state",
            "worker_id",
            "worker_lifecycle",
            "worker_revision",
        ])
    );

    let all_files = walk_text(&event_root);
    for forbidden in [
        "LOGICAL_SESSION_MUST_NOT_REACH_LOG",
        "TASK_ID_MUST_NOT_REACH_LOG",
        "TRACE_ID_MUST_NOT_REACH_LOG",
        "TARGET_KEY_MUST_NOT_REACH_LOG",
    ] {
        assert!(!all_files.contains(forbidden));
    }
}

#[tokio::test]
async fn failed_append_does_not_acknowledge_the_source_event() {
    let directory = TempDir::new().unwrap();
    let concrete =
        Arc::new(SqliteAgentStore::open(directory.path().join("agent.sqlite3")).unwrap());
    add_root(concrete.as_ref(), "owner", "retry", false);
    let store: Arc<dyn AgentStore> = concrete;
    let source_event_id = store
        .unprojected_events("owner", "retry-projector", 1)
        .unwrap()
        .items
        .remove(0)
        .event_id;
    let invalid_root = directory.path().join("not-a-directory");
    std::fs::write(&invalid_root, b"file").unwrap();
    let failing = AgentEventProjector::with_identity(
        BrokerScope::new("owner").unwrap(),
        Arc::clone(&store),
        EventLog::new(invalid_root),
        "retry-projector",
        "retry-stream",
    );
    assert!(failing.project_once(1).await.is_err());
    assert_eq!(
        store
            .unprojected_events("owner", "retry-projector", 1)
            .unwrap()
            .items[0]
            .event_id,
        source_event_id
    );

    let retry = AgentEventProjector::with_identity(
        BrokerScope::new("owner").unwrap(),
        Arc::clone(&store),
        EventLog::new(directory.path().join("retry-events")),
        "retry-projector",
        "retry-stream",
    );
    assert_eq!(retry.project_once(1).await.unwrap().projected, 1);
    assert_ne!(
        store
            .unprojected_events("owner", "retry-projector", 1)
            .unwrap()
            .items[0]
            .event_id,
        source_event_id
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
