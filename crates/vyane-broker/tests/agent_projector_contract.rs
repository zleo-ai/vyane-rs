#![allow(clippy::unwrap_used)]

mod common;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone as _, Utc};
use vyane_agent::{
    AgentClock, AgentEventKind, AgentStore, ControllerKind, ControllerRef, ExecutionBackend,
    NewAgentRun, NewWorker, ProjectionDeferReason, RunFailureCode, RunMode, RunSettlement,
    RunState, SqliteAgentStore,
};
use vyane_broker::{AgentEventProjector, BrokerScope};
use vyane_ledger::{EventCategory, EventLog, EventRecord, EventSource};

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
                execution_backend: ExecutionBackend::NativeInProcess,
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
    let directory = common::private_tempdir();
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
    let directory = common::private_tempdir();
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

struct ManualClock(Mutex<DateTime<Utc>>);

impl AgentClock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

fn at(origin: DateTime<Utc>, seconds: i64) -> DateTime<Utc> {
    origin + chrono::Duration::seconds(seconds)
}

fn queued_run(id: &str, worker_id: &str, available_at: DateTime<Utc>) -> NewAgentRun {
    NewAgentRun {
        id: id.into(),
        worker_id: worker_id.into(),
        task_id: None,
        trace_id: None,
        parent_run_id: None,
        execution_backend: ExecutionBackend::NativeInProcess,
        mode: RunMode::Autonomous,
        target_key: "native/default".into(),
        prompt_digest: digest('a'),
        policy_digest: digest('b'),
        available_at,
        timeout_seconds: 60,
        max_resume_attempts: 1,
    }
}

fn settle_next(store: &dyn AgentStore, owner: &str) -> String {
    let claimed = store
        .claim_due(
            owner,
            ExecutionBackend::NativeInProcess,
            "supervisor",
            30,
            1,
        )
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let claimed = claimed.into_iter().next().unwrap();
    let run_id = claimed.run.id.clone();
    let started = store
        .start(
            owner,
            &claimed.receipt,
            &ControllerRef {
                kind: ControllerKind::InProcess,
                id: "controller".into(),
                fingerprint: None,
            },
        )
        .unwrap();
    store
        .settle(
            owner,
            &started.receipt,
            RunSettlement::Failed {
                code: RunFailureCode::Internal,
            },
        )
        .unwrap();
    assert_eq!(
        store.get_run(owner, &run_id).unwrap().unwrap().state,
        RunState::Failed
    );
    run_id
}

fn payload_str<'a>(event: &'a EventRecord, key: &str) -> &'a str {
    event
        .payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap()
}

fn source_sequence(event: &EventRecord) -> u64 {
    event
        .payload
        .get("agent_event_sequence")
        .and_then(serde_json::Value::as_u64)
        .unwrap()
}

#[tokio::test]
async fn two_terminal_events_for_the_same_worker_project_in_append_order() {
    let origin = Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0).unwrap();
    let clock = Arc::new(ManualClock(Mutex::new(origin)));
    let directory = common::private_tempdir();
    let concrete = Arc::new(
        SqliteAgentStore::open_with_clock(
            directory.path().join("agent.sqlite3"),
            Arc::clone(&clock) as Arc<dyn AgentClock>,
        )
        .unwrap(),
    );
    concrete
        .create_root(
            "owner",
            &NewWorker {
                id: "worker-a".into(),
                logical_session_id: None,
            },
            &queued_run("run-a1", "worker-a", at(origin, -30)),
        )
        .unwrap();
    concrete
        .enqueue_run("owner", &queued_run("run-a2", "worker-a", at(origin, -10)))
        .unwrap();
    concrete
        .create_root(
            "owner",
            &NewWorker {
                id: "worker-b".into(),
                logical_session_id: None,
            },
            &queued_run("run-b1", "worker-b", at(origin, -20)),
        )
        .unwrap();
    assert_eq!(settle_next(concrete.as_ref(), "owner"), "run-a1");
    assert_eq!(settle_next(concrete.as_ref(), "owner"), "run-b1");
    assert_eq!(settle_next(concrete.as_ref(), "owner"), "run-a2");

    let store: Arc<dyn AgentStore> = concrete;
    let mut settled = store
        .unprojected_events("owner", "order-projector", 100)
        .unwrap()
        .items
        .into_iter()
        .filter(|event| event.kind == AgentEventKind::RunSettled)
        .collect::<Vec<_>>();
    settled.sort_by_key(|event| event.sequence);
    assert_eq!(
        settled
            .iter()
            .map(|event| event.run_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("run-a1"), Some("run-b1"), Some("run-a2")]
    );
    assert!(settled[0].sequence < settled[2].sequence);
    assert_eq!(settled[0].worker_id, "worker-a");
    assert_eq!(settled[2].worker_id, "worker-a");
    assert!(settled[0].run_state.is_some_and(RunState::is_terminal));
    assert!(settled[2].run_state.is_some_and(RunState::is_terminal));
    store
        .defer_projection(
            "owner",
            "order-projector",
            &settled[0].event_id,
            ProjectionDeferReason::SinkUnavailable,
            Duration::from_secs(30),
        )
        .unwrap();

    let event_root = directory.path().join("events");
    let projector = AgentEventProjector::with_identity(
        BrokerScope::new("owner").unwrap(),
        Arc::clone(&store),
        EventLog::new(&event_root),
        "order-projector",
        "order-stream",
    );
    let mut blocked = None;
    for _ in 0..100 {
        let report = projector.project_once(1).await.unwrap();
        if report.projected == 0 {
            blocked = Some(report);
            break;
        }
    }
    let blocked = blocked.expect("projection should stop while the earlier event is hidden");
    assert!(blocked.read >= 1);
    assert!(!blocked.has_more);
    let early = EventLog::new(&event_root)
        .read_after("owner", "order-stream", 0, 100)
        .await
        .unwrap()
        .events;
    assert!(
        early
            .iter()
            .all(|event| event.event_type != "agent.run_settled")
    );

    *clock.0.lock().unwrap() = at(origin, 120);
    for _ in 0..100 {
        let report = projector.project_once(1).await.unwrap();
        if report.projected == 0 {
            assert_eq!(report.read, 0);
            break;
        }
    }
    let events = EventLog::new(&event_root)
        .read_after("owner", "order-stream", 0, 100)
        .await
        .unwrap()
        .events;
    let terminal: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == "agent.run_settled")
        .collect();
    assert_eq!(
        terminal
            .iter()
            .map(|event| payload_str(event, "run_id"))
            .collect::<Vec<_>>(),
        vec!["run-a1", "run-b1", "run-a2"]
    );
    let same_worker: Vec<_> = terminal
        .iter()
        .copied()
        .filter(|event| payload_str(event, "worker_id") == "worker-a")
        .collect();
    assert_eq!(same_worker.len(), 2);
    assert!(source_sequence(same_worker[0]) < source_sequence(same_worker[1]));
    assert!(same_worker[0].sequence < same_worker[1].sequence);
    assert_eq!(same_worker[0].event_id, settled[0].event_id);
    assert_eq!(same_worker[1].event_id, settled[2].event_id);
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
