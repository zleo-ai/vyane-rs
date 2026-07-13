#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
use tempfile::TempDir;
use vyane_agent::{
    AgentClock, AgentEventKind, AgentStore, AgentStoreError, CancelOutcome, CancelRequest,
    CancelTicket, ControllerKind, ControllerRef, EnqueueResume, MAX_TOPOLOGY_NODES, NewAgentRun,
    NewRunCompletion, NewWorker, ProjectionDeferReason, ProjectionQuarantineReason, RecoveryReason,
    ResumeProof, ResumeSessionProof, RunFailureCode, RunMode, RunSettlement, RunState,
    SqliteAgentStore, WorkerLifecycle,
};

#[derive(Debug)]
struct TestClock(Mutex<DateTime<Utc>>);

impl TestClock {
    fn new() -> Self {
        Self(Mutex::new(
            Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0)
                .single()
                .unwrap(),
        ))
    }

    fn advance(&self, seconds: i64) {
        let mut now = self.0.lock().unwrap();
        *now = now.checked_add_signed(TimeDelta::seconds(seconds)).unwrap();
    }

    fn rewind(&self, seconds: i64) {
        self.advance(-seconds);
    }
}

impl AgentClock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

fn digest(byte: char) -> String {
    std::iter::repeat_n(byte, 64).collect()
}

fn private_tempdir() -> TempDir {
    TempDir::new_in(std::env::temp_dir().canonicalize().unwrap()).unwrap()
}

fn worker(id: &str, logical_session_id: Option<&str>) -> NewWorker {
    NewWorker {
        id: id.into(),
        logical_session_id: logical_session_id.map(str::to_string),
    }
}

fn run(id: &str, worker_id: &str, now: DateTime<Utc>) -> NewAgentRun {
    NewAgentRun {
        id: id.into(),
        worker_id: worker_id.into(),
        task_id: Some(format!("task-{id}")),
        trace_id: Some(format!("trace-{id}")),
        parent_run_id: None,
        mode: RunMode::Autonomous,
        target_key: "codex/default".into(),
        prompt_digest: digest('a'),
        policy_digest: digest('b'),
        available_at: now,
        timeout_seconds: 600,
        max_resume_attempts: 2,
    }
}

fn controller(id: &str) -> ControllerRef {
    ControllerRef {
        kind: ControllerKind::InProcess,
        id: id.into(),
        fingerprint: Some(format!("fingerprint-{id}")),
    }
}

fn cancel_request(operation_id: &str, retry_tickets: Vec<CancelTicket>) -> CancelRequest {
    CancelRequest {
        operation_id: operation_id.into(),
        lease_owner: "cancel-supervisor".into(),
        lease_seconds: 30,
        retry_tickets,
    }
}

fn complete(store: &dyn AgentStore, owner: &str, claimed: &vyane_agent::ClaimedRun) {
    let permit = store
        .issue_execution_permit(owner, &claimed.receipt, &claimed.run.policy_digest)
        .unwrap();
    let prepared = store
        .prepare_completion(
            owner,
            &permit,
            &NewRunCompletion {
                id: format!("completion-{}", claimed.run.id),
                sink_kind: "test".into(),
                publication_key: format!("publication-{}", claimed.run.id),
                content_digest: digest('c'),
                content_bytes: 1,
            },
        )
        .unwrap();
    store.commit_completion(owner, &prepared.permit).unwrap();
}

fn store() -> (TempDir, Arc<TestClock>, SqliteAgentStore) {
    let directory = private_tempdir();
    let clock = Arc::new(TestClock::new());
    let store =
        SqliteAgentStore::open_with_clock(directory.path().join("agent.sqlite"), clock.clone())
            .unwrap();
    (directory, clock, store)
}

#[test]
fn owner_isolation_is_absent_semantics_and_ids_may_repeat() {
    let (_directory, clock, store) = store();
    let now = clock.now();
    store
        .create_root("alice", &worker("worker", None), &run("run", "worker", now))
        .unwrap();
    store
        .create_root("bob", &worker("worker", None), &run("run", "worker", now))
        .unwrap();

    assert!(store.get_worker("mallory", "worker").unwrap().is_none());
    assert!(store.get_run("mallory", "run").unwrap().is_none());
    assert!(matches!(
        store.spawn_child(
            "mallory",
            "worker",
            0,
            &worker("child", None),
            &run("child-run", "child", now),
        ),
        Err(AgentStoreError::NotFound { .. })
    ));
    assert_eq!(
        store.get_worker("alice", "worker").unwrap().unwrap().owner,
        "alice"
    );
    assert_eq!(
        store.get_worker("bob", "worker").unwrap().unwrap().owner,
        "bob"
    );
    store.audit_integrity().unwrap();
}

#[test]
fn restart_preserves_run_topology_and_body_free_events() {
    let (directory, clock, store) = store();
    let path = directory.path().join("agent.sqlite");
    let now = clock.now();
    store
        .create_root(
            "alice",
            &worker("root", Some("logical")),
            &run("root-run", "root", now),
        )
        .unwrap();
    let mut child_run = run("child-run", "child", now);
    child_run.parent_run_id = Some("root-run".into());
    store
        .spawn_child(
            "alice",
            "root",
            0,
            &worker("child", Some("logical")),
            &child_run,
        )
        .unwrap();
    drop(store);

    let reopened = SqliteAgentStore::open_with_clock(path.clone(), clock).unwrap();
    let topology = reopened.topology("alice", "root").unwrap();
    assert_eq!(
        topology
            .workers
            .iter()
            .map(|worker| worker.id.as_str())
            .collect::<Vec<_>>(),
        ["root", "child"]
    );
    assert_eq!(
        reopened
            .get_worker("alice", "child")
            .unwrap()
            .unwrap()
            .parent_id
            .as_deref(),
        Some("root")
    );
    let events = reopened
        .unprojected_events("alice", "audit", 100)
        .unwrap()
        .items;
    assert!(events.iter().all(|event| event.owner == "alice"));

    // The outbox schema cannot represent prompt, native-session, token, raw
    // error, output, payload, or credential content.
    let connection = rusqlite::Connection::open(path).unwrap();
    let mut statement = connection
        .prepare("SELECT name FROM pragma_table_info('agent_events') ORDER BY cid")
        .unwrap();
    let columns = statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for forbidden in [
        "prompt",
        "native_session_id",
        "token",
        "raw_error",
        "output",
        "payload",
        "credential",
    ] {
        assert!(!columns.iter().any(|column| column.contains(forbidden)));
    }
}

#[test]
fn concurrent_child_spawn_uses_parent_revision_as_single_winner() {
    let (_directory, clock, store) = store();
    let now = clock.now();
    store
        .create_root(
            "alice",
            &worker("root", None),
            &run("root-run", "root", now),
        )
        .unwrap();
    let store = Arc::new(store);
    let barrier = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for suffix in ["a", "b"] {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            store.spawn_child(
                "alice",
                "root",
                0,
                &worker(&format!("child-{suffix}"), None),
                &run(
                    &format!("child-run-{suffix}"),
                    &format!("child-{suffix}"),
                    now,
                ),
            )
        }));
    }
    barrier.wait();
    let results = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(AgentStoreError::Conflict { .. })))
            .count(),
        1
    );
    assert_eq!(store.topology("alice", "root").unwrap().workers.len(), 2);
    store.audit_integrity().unwrap();
}

#[test]
fn concurrent_claim_is_single_winner_and_fifo_per_worker() {
    let (_directory, clock, store) = store();
    let now = clock.now();
    store
        .create_root(
            "alice",
            &worker("worker", None),
            &run("first", "worker", now),
        )
        .unwrap();
    store
        .enqueue_run("alice", &run("second", "worker", now))
        .unwrap();
    let store = Arc::new(store);
    let barrier = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for consumer in ["supervisor-a", "supervisor-b"] {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            store.claim_due("alice", consumer, 30, 1).unwrap()
        }));
    }
    barrier.wait();
    let claims = threads
        .into_iter()
        .flat_map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].run.id, "first");

    let claimed = &claims[0];
    let started = store
        .start("alice", &claimed.receipt, &controller("one"))
        .unwrap();
    complete(store.as_ref(), "alice", &started);
    let next = store.claim_due("alice", "supervisor-a", 30, 1).unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].run.id, "second");
    assert_eq!(next[0].run.worker_generation, 2);
}

#[test]
fn stale_revision_generation_token_and_expiry_all_fail_closed() {
    let (_directory, clock, store) = store();
    let now = clock.now();
    store
        .create_root("alice", &worker("worker", None), &run("run", "worker", now))
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 30, 1)
        .unwrap()
        .remove(0);
    let started = store
        .start("alice", &claimed.receipt, &controller("one"))
        .unwrap();
    assert!(matches!(
        store.record_activity("alice", &claimed.receipt),
        Err(AgentStoreError::InvalidReceipt { .. })
    ));

    let mut wrong_generation = started.receipt.clone();
    wrong_generation.generation += 1;
    assert!(matches!(
        store.record_activity("alice", &wrong_generation),
        Err(AgentStoreError::InvalidReceipt { .. })
    ));
    let mut wrong_token = started.receipt.clone();
    wrong_token.token = "0".repeat(64);
    assert!(matches!(
        store.record_activity("alice", &wrong_token),
        Err(AgentStoreError::InvalidReceipt { .. })
    ));

    clock.advance(31);
    assert!(matches!(
        store.record_activity("alice", &started.receipt),
        Err(AgentStoreError::InvalidReceipt { .. })
    ));
    assert_eq!(
        store.get_run("alice", "run").unwrap().unwrap().state,
        RunState::Running
    );
}

#[test]
fn heartbeat_activity_and_terminal_state_are_revision_fenced() {
    let (_directory, clock, store) = store();
    let now = clock.now();
    store
        .create_root("alice", &worker("worker", None), &run("run", "worker", now))
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 30, 1)
        .unwrap()
        .remove(0);
    let started = store
        .start("alice", &claimed.receipt, &controller("one"))
        .unwrap();
    clock.advance(10);
    let heartbeat = store.heartbeat("alice", &started.receipt, 30).unwrap();
    let activity = store.record_activity("alice", &heartbeat.receipt).unwrap();
    assert!(activity.run.last_heartbeat_at.is_some());
    assert!(activity.run.last_activity_at.is_some());
    complete(&store, "alice", &activity);
    let terminal = store.get_run("alice", "run").unwrap().unwrap();
    assert_eq!(terminal.state, RunState::Succeeded);
    assert!(terminal.controller.is_none());
    assert!(terminal.lease.is_none());
    assert!(matches!(
        store.start("alice", &activity.receipt, &controller("again")),
        Err(AgentStoreError::InvalidReceipt { .. })
    ));
    assert_eq!(
        store.get_run("alice", "run").unwrap().unwrap().state,
        RunState::Succeeded
    );
    store.audit_integrity().unwrap();
}

#[test]
fn topology_parent_is_immutable_and_cycles_are_detected_on_reopen() {
    let (directory, clock, store) = store();
    let path = directory.path().join("agent.sqlite");
    let now = clock.now();
    store
        .create_root(
            "alice",
            &worker("root", None),
            &run("root-run", "root", now),
        )
        .unwrap();
    assert!(matches!(
        store.spawn_child(
            "alice",
            "root",
            0,
            &worker("root", None),
            &run("bad", "root", now),
        ),
        Err(AgentStoreError::InvalidInput(_))
    ));
    store
        .spawn_child(
            "alice",
            "root",
            0,
            &worker("child", None),
            &run("child-run", "child", now),
        )
        .unwrap();
    assert_eq!(
        store
            .get_worker("alice", "child")
            .unwrap()
            .unwrap()
            .parent_id
            .as_deref(),
        Some("root")
    );
    drop(store);

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE workers SET parent_id = 'child' WHERE owner = 'alice' AND id = 'root'",
            [],
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteAgentStore::open_with_clock(path, clock),
        Err(AgentStoreError::CorruptData(_))
    ));
}

#[test]
fn oversized_topology_fails_before_tree_mutation() {
    let (_directory, clock, store) = store();
    let now = clock.now();
    store
        .create_root(
            "alice",
            &worker("root", None),
            &run("root-run", "root", now),
        )
        .unwrap();
    for index in 0..MAX_TOPOLOGY_NODES {
        let worker_id = format!("child-{index:04}");
        let run_id = format!("run-{index:04}");
        store
            .spawn_child(
                "alice",
                "root",
                u64::try_from(index).unwrap(),
                &worker(&worker_id, None),
                &run(&run_id, &worker_id, now),
            )
            .unwrap();
    }
    assert!(matches!(
        store.topology("alice", "root"),
        Err(AgentStoreError::InvalidInput(_))
    ));
    assert!(matches!(
        store.request_cancel_tree("alice", "root", &cancel_request("oversized", Vec::new())),
        Err(AgentStoreError::InvalidInput(_))
    ));
    assert_eq!(
        store
            .get_worker("alice", "root")
            .unwrap()
            .unwrap()
            .lifecycle,
        WorkerLifecycle::Open
    );
    assert_eq!(
        store.get_run("alice", "root-run").unwrap().unwrap().state,
        RunState::Queued
    );
}

#[test]
fn tree_cancel_is_children_first_two_phase_idempotent_and_lease_fenced() {
    let (_directory, clock, store) = store();
    let now = clock.now();
    store
        .create_root(
            "alice",
            &worker("root", None),
            &run("root-run", "root", now),
        )
        .unwrap();
    let mut child_run = run("child-run", "child", now);
    child_run.parent_run_id = Some("root-run".into());
    store
        .spawn_child("alice", "root", 0, &worker("child", None), &child_run)
        .unwrap();
    store
        .enqueue_run("alice", &run("queued-root", "root", now))
        .unwrap();
    let claims = store.claim_due("alice", "supervisor", 60, 10).unwrap();
    assert_eq!(claims.len(), 2);
    for claim in &claims {
        store
            .start("alice", &claim.receipt, &controller(&claim.run.id))
            .unwrap();
    }

    let first_request = cancel_request("cancel-one", Vec::new());
    let first = store
        .request_cancel_tree("alice", "root", &first_request)
        .unwrap();
    assert_eq!(
        first
            .tickets
            .iter()
            .map(|ticket| ticket.worker_id.as_str())
            .collect::<Vec<_>>(),
        ["child", "root"]
    );
    assert_eq!(
        store
            .get_run("alice", "queued-root")
            .unwrap()
            .unwrap()
            .state,
        RunState::Cancelled
    );
    assert!(first.tickets.iter().all(|ticket| {
        store
            .get_run("alice", &ticket.run_id)
            .unwrap()
            .unwrap()
            .state
            == RunState::Cancelling
    }));

    let revisions = first
        .tickets
        .iter()
        .map(|ticket| (ticket.run_id.clone(), ticket.revision))
        .collect::<Vec<_>>();
    let retry = cancel_request("cancel-one", first.tickets.clone());
    let same = store.request_cancel_tree("alice", "root", &retry).unwrap();
    assert_eq!(same.tickets, first.tickets);
    for (run_id, revision) in revisions {
        assert_eq!(
            store.get_run("alice", &run_id).unwrap().unwrap().revision,
            revision
        );
    }
    assert!(matches!(
        store.request_cancel_tree("alice", "root", &cancel_request("cancel-busy", Vec::new())),
        Err(AgentStoreError::ControlBusy { .. })
    ));

    clock.advance(31);
    let second = store
        .request_cancel_tree("alice", "root", &cancel_request("cancel-two", Vec::new()))
        .unwrap();
    assert_eq!(second.tickets.len(), 2);
    assert!(matches!(
        store.settle_cancel("alice", &first.tickets[0], CancelOutcome::Cancelled),
        Err(AgentStoreError::InvalidCancelTicket { .. })
    ));
    for ticket in &second.tickets {
        store
            .settle_cancel("alice", ticket, CancelOutcome::Cancelled)
            .unwrap();
    }
    assert_eq!(
        store
            .get_worker("alice", "root")
            .unwrap()
            .unwrap()
            .lifecycle,
        WorkerLifecycle::Draining
    );
    let child = store.get_worker("alice", "child").unwrap().unwrap();
    store
        .release_worker("alice", "child", child.revision)
        .unwrap();
    let root = store.get_worker("alice", "root").unwrap().unwrap();
    store
        .release_worker("alice", "root", root.revision)
        .unwrap();
    store.audit_integrity().unwrap();
}

#[test]
fn cancel_operation_id_cannot_expand_shrink_or_move_its_frozen_tree_scope() {
    let (_directory, clock, store) = store();
    let now = clock.now();
    store
        .create_root(
            "alice",
            &worker("root", None),
            &run("root-run", "root", now),
        )
        .unwrap();
    let mut child_run = run("child-run", "child", now);
    child_run.parent_run_id = Some("root-run".into());
    store
        .spawn_child("alice", "root", 0, &worker("child", None), &child_run)
        .unwrap();
    store
        .create_root(
            "alice",
            &worker("disjoint", None),
            &run("disjoint-run", "disjoint", now),
        )
        .unwrap();
    let claims = store.claim_due("alice", "supervisor", 60, 10).unwrap();
    for claim in claims {
        store
            .start("alice", &claim.receipt, &controller(&claim.run.id))
            .unwrap();
    }

    let child_plan = store
        .request_cancel_tree("alice", "child", &cancel_request("scope-child", Vec::new()))
        .unwrap();
    assert!(matches!(
        store.request_cancel_tree(
            "alice",
            "root",
            &cancel_request("scope-child", child_plan.tickets.clone()),
        ),
        Err(AgentStoreError::InvalidInput(_))
    ));
    assert_eq!(
        store
            .get_worker("alice", "root")
            .unwrap()
            .unwrap()
            .lifecycle,
        WorkerLifecycle::Open
    );
    store
        .settle_cancel("alice", &child_plan.tickets[0], CancelOutcome::Cancelled)
        .unwrap();

    let parent_plan = store
        .request_cancel_tree("alice", "root", &cancel_request("scope-parent", Vec::new()))
        .unwrap();
    assert!(matches!(
        store.request_cancel_tree(
            "alice",
            "child",
            &cancel_request("scope-parent", parent_plan.tickets.clone()),
        ),
        Err(AgentStoreError::InvalidInput(_))
    ));
    assert!(matches!(
        store.request_cancel_tree(
            "alice",
            "disjoint",
            &cancel_request("scope-parent", parent_plan.tickets.clone()),
        ),
        Err(AgentStoreError::InvalidInput(_))
    ));
    assert_eq!(
        store
            .get_worker("alice", "disjoint")
            .unwrap()
            .unwrap()
            .lifecycle,
        WorkerLifecycle::Open
    );
    store
        .settle_cancel("alice", &parent_plan.tickets[0], CancelOutcome::Cancelled)
        .unwrap();
    store.audit_integrity().unwrap();
}

#[test]
fn strict_resume_binding_rejects_mismatch_and_owner_then_survives_restart() {
    let (directory, clock, store) = store();
    let path = directory.path().join("agent.sqlite");
    let now = clock.now();
    let mut initial = run("run", "worker", now);
    initial.max_resume_attempts = 1;
    store
        .create_root("alice", &worker("worker", Some("logical")), &initial)
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 60, 1)
        .unwrap()
        .remove(0);
    let started = store
        .start("alice", &claimed.receipt, &controller("one"))
        .unwrap();
    let exact_session = ResumeSessionProof::derive("alice", "logical", "native-secret").unwrap();
    let bound = store
        .bind_resume_session("alice", &started.receipt, &exact_session)
        .unwrap();
    let interrupted = store
        .settle(
            "alice",
            &bound.receipt,
            RunSettlement::Interrupted {
                code: RunFailureCode::ControllerLost,
            },
        )
        .unwrap();
    assert!(interrupted.is_resume_eligible());

    let mismatched = ResumeProof::new(
        ResumeSessionProof::derive("alice", "logical", "different-native").unwrap(),
        digest('b'),
    )
    .unwrap();
    assert!(matches!(
        store.enqueue_resume(
            "alice",
            &EnqueueResume {
                new_run_id: "resume-mismatch".into(),
                source_run_id: "run".into(),
                available_at: now,
                proof: mismatched,
            },
        ),
        Err(AgentStoreError::ResumeRejected { .. })
    ));
    let wrong_owner = ResumeProof::new(
        ResumeSessionProof::derive("bob", "logical", "native-secret").unwrap(),
        digest('b'),
    )
    .unwrap();
    assert!(matches!(
        store.enqueue_resume(
            "alice",
            &EnqueueResume {
                new_run_id: "resume-owner".into(),
                source_run_id: "run".into(),
                available_at: now,
                proof: wrong_owner,
            },
        ),
        Err(AgentStoreError::ResumeRejected { .. })
    ));
    drop(store);

    let reopened = SqliteAgentStore::open_with_clock(path, clock).unwrap();
    let exact = ResumeProof::new(exact_session, digest('b')).unwrap();
    let resumed = reopened
        .enqueue_resume(
            "alice",
            &EnqueueResume {
                new_run_id: "resume-exact".into(),
                source_run_id: "run".into(),
                available_at: now,
                proof: exact,
            },
        )
        .unwrap();
    assert_eq!(resumed.resume_attempt, 1);
    assert_eq!(resumed.resume_of_run_id.as_deref(), Some("run"));
    assert!(resumed.resume_binding_digest.is_some());

    let claimed = reopened
        .claim_due("alice", "supervisor", 60, 1)
        .unwrap()
        .remove(0);
    let started = reopened
        .start("alice", &claimed.receipt, &controller("two"))
        .unwrap();
    let second_interruption = reopened
        .settle(
            "alice",
            &started.receipt,
            RunSettlement::Interrupted {
                code: RunFailureCode::ControllerLost,
            },
        )
        .unwrap();
    assert!(!second_interruption.is_resume_eligible());
    let proof = ResumeProof::new(
        ResumeSessionProof::derive("alice", "logical", "native-secret").unwrap(),
        digest('b'),
    )
    .unwrap();
    assert!(matches!(
        reopened.enqueue_resume(
            "alice",
            &EnqueueResume {
                new_run_id: "over-budget".into(),
                source_run_id: "resume-exact".into(),
                available_at: now,
                proof,
            },
        ),
        Err(AgentStoreError::ResumeRejected { .. })
    ));
    reopened.audit_integrity().unwrap();
}

#[test]
fn only_exact_resumable_interruption_can_resume() {
    let (_directory, clock, store) = store();
    let now = clock.now();
    store
        .create_root(
            "alice",
            &worker("worker", Some("logical")),
            &run("run", "worker", now),
        )
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 60, 1)
        .unwrap()
        .remove(0);
    let started = store
        .start("alice", &claimed.receipt, &controller("one"))
        .unwrap();
    let session = ResumeSessionProof::derive("alice", "logical", "native").unwrap();
    let bound = store
        .bind_resume_session("alice", &started.receipt, &session)
        .unwrap();
    store
        .settle(
            "alice",
            &bound.receipt,
            RunSettlement::Failed {
                code: RunFailureCode::DispatchFailed,
            },
        )
        .unwrap();
    let proof = ResumeProof::new(session, digest('b')).unwrap();
    assert!(matches!(
        store.enqueue_resume(
            "alice",
            &EnqueueResume {
                new_run_id: "not-allowed".into(),
                source_run_id: "run".into(),
                available_at: now,
                proof,
            },
        ),
        Err(AgentStoreError::ResumeRejected { .. })
    ));
}

#[test]
fn outbox_progress_is_owner_and_projector_scoped_and_idempotent() {
    let (_directory, clock, store) = store();
    let now = clock.now();
    store
        .create_root("alice", &worker("worker", None), &run("run", "worker", now))
        .unwrap();
    let first_a = store.unprojected_events("alice", "projector-a", 1).unwrap();
    assert_eq!(first_a.items.len(), 1);
    assert!(first_a.has_more);
    let event_id = first_a.items[0].event_id.clone();
    assert!(matches!(
        store.mark_projected("bob", "projector-a", &event_id),
        Err(AgentStoreError::NotFound { .. })
    ));
    store
        .mark_projected("alice", "projector-a", &event_id)
        .unwrap();
    store
        .mark_projected("alice", "projector-a", &event_id)
        .unwrap();
    assert_ne!(
        store
            .unprojected_events("alice", "projector-a", 100)
            .unwrap()
            .items[0]
            .event_id,
        event_id
    );
    assert_eq!(
        store
            .unprojected_events("alice", "projector-b", 100)
            .unwrap()
            .items[0]
            .event_id,
        event_id
    );
    assert!(
        store
            .unprojected_events("alice", "projector-b", 100)
            .unwrap()
            .items
            .iter()
            .any(|event| event.kind == AgentEventKind::RunQueued)
    );
    store.audit_integrity().unwrap();
}

#[test]
fn projection_deferral_survives_restart_expires_and_does_not_block() {
    let (directory, clock, store) = store();
    let path = directory.path().join("agent.sqlite");
    store
        .create_root(
            "alice",
            &worker("worker", None),
            &run("run", "worker", clock.now()),
        )
        .unwrap();
    let first = store
        .unprojected_events("alice", "completion-publisher", 1)
        .unwrap()
        .items
        .remove(0);
    store
        .defer_projection(
            "alice",
            "completion-publisher",
            &first.event_id,
            ProjectionDeferReason::MissingSink,
            Duration::from_secs(60),
        )
        .unwrap();
    store
        .defer_projection(
            "alice",
            "completion-publisher",
            &first.event_id,
            ProjectionDeferReason::MissingSink,
            Duration::from_secs(60),
        )
        .unwrap();
    assert_ne!(
        store
            .unprojected_events("alice", "completion-publisher", 1)
            .unwrap()
            .items[0]
            .event_id,
        first.event_id
    );
    drop(store);

    let reopened = SqliteAgentStore::open_with_clock(&path, clock.clone()).unwrap();
    assert!(
        reopened
            .unprojected_events("alice", "completion-publisher", 100)
            .unwrap()
            .items
            .iter()
            .all(|event| event.event_id != first.event_id)
    );
    clock.advance(60);
    assert_eq!(
        reopened
            .unprojected_events("alice", "completion-publisher", 1)
            .unwrap()
            .items[0]
            .event_id,
        first.event_id
    );
    reopened.audit_integrity().unwrap();
}

#[test]
fn projection_deferral_rejects_sub_millisecond_delay() {
    let (_directory, clock, store) = store();
    store
        .create_root(
            "alice",
            &worker("worker", None),
            &run("run", "worker", clock.now()),
        )
        .unwrap();
    let event = store
        .unprojected_events("alice", "completion-publisher", 1)
        .unwrap()
        .items
        .remove(0);
    assert!(matches!(
        store.defer_projection(
            "alice",
            "completion-publisher",
            &event.event_id,
            ProjectionDeferReason::SinkUnavailable,
            Duration::from_nanos(1),
        ),
        Err(AgentStoreError::InvalidInput(_))
    ));
}

#[test]
fn quarantined_projection_is_durable_owner_scoped_and_non_blocking() {
    let (directory, clock, store) = store();
    let path = directory.path().join("agent.sqlite");
    store
        .create_root(
            "alice",
            &worker("worker", None),
            &run("run", "worker", clock.now()),
        )
        .unwrap();
    let page = store
        .unprojected_events("alice", "completion-publisher", 100)
        .unwrap();
    let first = &page.items[0];
    let second = &page.items[1];
    assert!(matches!(
        store.quarantine_projection(
            "bob",
            "completion-publisher",
            &first.event_id,
            ProjectionQuarantineReason::InvalidEvent,
        ),
        Err(AgentStoreError::NotFound { .. })
    ));
    store
        .quarantine_projection(
            "alice",
            "completion-publisher",
            &first.event_id,
            ProjectionQuarantineReason::InvalidEvent,
        )
        .unwrap();
    store
        .quarantine_projection(
            "alice",
            "completion-publisher",
            &first.event_id,
            ProjectionQuarantineReason::InvalidEvent,
        )
        .unwrap();
    assert_eq!(
        store
            .unprojected_events("alice", "completion-publisher", 1)
            .unwrap()
            .items[0]
            .event_id,
        second.event_id
    );
    assert_eq!(
        store
            .unprojected_events("alice", "different-projector", 1)
            .unwrap()
            .items[0]
            .event_id,
        first.event_id
    );
    drop(store);
    let reopened = SqliteAgentStore::open_with_clock(&path, clock).unwrap();
    assert!(
        reopened
            .unprojected_events("alice", "completion-publisher", 100)
            .unwrap()
            .items
            .iter()
            .all(|event| event.event_id != first.event_id)
    );
    reopened.audit_integrity().unwrap();
}

#[test]
fn successful_projection_wins_concurrently_over_deferral() {
    let (_directory, clock, store) = store();
    store
        .create_root(
            "alice",
            &worker("worker", None),
            &run("run", "worker", clock.now()),
        )
        .unwrap();
    let store = Arc::new(store);
    let event_id = store
        .unprojected_events("alice", "completion-publisher", 1)
        .unwrap()
        .items
        .remove(0)
        .event_id;
    let barrier = Arc::new(Barrier::new(3));
    let success_store = Arc::clone(&store);
    let success_barrier = Arc::clone(&barrier);
    let success_event = event_id.clone();
    let success = std::thread::spawn(move || {
        success_barrier.wait();
        success_store.mark_projected("alice", "completion-publisher", &success_event)
    });
    let defer_store = Arc::clone(&store);
    let defer_barrier = Arc::clone(&barrier);
    let defer_event = event_id.clone();
    let deferred = std::thread::spawn(move || {
        defer_barrier.wait();
        defer_store.defer_projection(
            "alice",
            "completion-publisher",
            &defer_event,
            ProjectionDeferReason::SinkUnavailable,
            Duration::from_secs(60),
        )
    });
    barrier.wait();
    success.join().unwrap().unwrap();
    deferred.join().unwrap().unwrap();
    assert!(
        store
            .unprojected_events("alice", "completion-publisher", 100)
            .unwrap()
            .items
            .iter()
            .all(|event| event.event_id != event_id)
    );
    let connection = rusqlite::Connection::open(store.path()).unwrap();
    let dispositions: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM agent_projector_dispositions d \
             JOIN agent_events e ON e.owner = d.owner AND e.sequence = d.event_sequence \
             WHERE d.owner = 'alice' AND d.projector = 'completion-publisher' \
               AND e.event_id = ?1",
            [&event_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(dispositions, 0);
    drop(connection);
    store.audit_integrity().unwrap();
}

#[test]
fn schema_v2_migrates_projection_dispositions_atomically() {
    let (directory, clock, store) = store();
    let path = directory.path().join("agent.sqlite");
    store
        .create_root(
            "alice",
            &worker("worker", None),
            &run("run", "worker", clock.now()),
        )
        .unwrap();
    drop(store);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "DROP INDEX agent_projector_dispositions_due_idx; \
             DROP TABLE agent_projector_dispositions; \
             PRAGMA user_version = 2;",
        )
        .unwrap();
    drop(connection);

    let migrated = SqliteAgentStore::open_with_clock(&path, clock).unwrap();
    let event = migrated
        .unprojected_events("alice", "completion-publisher", 1)
        .unwrap()
        .items
        .remove(0);
    migrated
        .quarantine_projection(
            "alice",
            "completion-publisher",
            &event.event_id,
            ProjectionQuarantineReason::SinkConflict,
        )
        .unwrap();
    migrated.audit_integrity().unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 3);
}

#[test]
fn projection_disposition_corruption_fails_integrity_audit() {
    let (directory, clock, store) = store();
    let path = directory.path().join("agent.sqlite");
    store
        .create_root(
            "alice",
            &worker("worker", None),
            &run("run", "worker", clock.now()),
        )
        .unwrap();
    let event = store
        .unprojected_events("alice", "completion-publisher", 1)
        .unwrap()
        .items
        .remove(0);
    store
        .quarantine_projection(
            "alice",
            "completion-publisher",
            &event.event_id,
            ProjectionQuarantineReason::InvalidEvent,
        )
        .unwrap();
    let connection = rusqlite::Connection::open(path).unwrap();
    connection
        .execute(
            "UPDATE agent_projector_dispositions SET recorded_at_ms = 0 \
             WHERE owner = 'alice' AND projector = 'completion-publisher'",
            [],
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        store.audit_integrity(),
        Err(AgentStoreError::CorruptData(_))
    ));
}

#[test]
fn expired_active_run_recovery_is_single_winner_and_unblocks_fifo() {
    let (_directory, clock, store) = store();
    let now = clock.now();
    store
        .create_root(
            "alice",
            &worker("worker", None),
            &run("first", "worker", now),
        )
        .unwrap();
    store
        .enqueue_run("alice", &run("second", "worker", now))
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 5, 1)
        .unwrap()
        .remove(0);
    assert_eq!(claimed.run.state, RunState::Starting);
    assert!(
        store
            .claim_recovery_due("alice", "early", 30, 1)
            .unwrap()
            .is_empty()
    );
    clock.advance(6);

    let store = Arc::new(store);
    let barrier = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for reconciler in ["reconciler-a", "reconciler-b"] {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            store
                .claim_recovery_due("alice", reconciler, 30, 1)
                .unwrap()
        }));
    }
    barrier.wait();
    let tickets = threads
        .into_iter()
        .flat_map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(tickets.len(), 1);
    assert_eq!(tickets[0].reason, RecoveryReason::LeaseExpired);
    let recovered = store.confirm_controller_gone("alice", &tickets[0]).unwrap();
    assert_eq!(recovered.state, RunState::Interrupted);
    assert_eq!(recovered.failure_code, Some(RunFailureCode::ControllerLost));

    let next = store.claim_due("alice", "supervisor", 30, 1).unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].run.id, "second");
    store.audit_integrity().unwrap();
}

#[test]
fn fixed_execution_deadline_wins_over_renewed_heartbeat_lease() {
    let (_directory, clock, store) = store();
    let now = clock.now();
    let mut timed = run("timed", "worker", now);
    timed.timeout_seconds = 5;
    store
        .create_root("alice", &worker("worker", None), &timed)
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 30, 1)
        .unwrap()
        .remove(0);
    let fixed_deadline = claimed.run.deadline_at.unwrap();
    let started = store
        .start("alice", &claimed.receipt, &controller("controller"))
        .unwrap();
    clock.advance(4);
    let heartbeat = store.heartbeat("alice", &started.receipt, 30).unwrap();
    assert_eq!(heartbeat.run.deadline_at, Some(fixed_deadline));
    clock.advance(2);
    assert!(matches!(
        store.heartbeat("alice", &heartbeat.receipt, 30),
        Err(AgentStoreError::InvalidReceipt { .. })
    ));
    assert!(matches!(
        store.record_activity("alice", &heartbeat.receipt),
        Err(AgentStoreError::InvalidReceipt { .. })
    ));
    let late_binding = ResumeSessionProof::derive("alice", "logical", "native").unwrap();
    assert!(matches!(
        store.bind_resume_session("alice", &heartbeat.receipt, &late_binding),
        Err(AgentStoreError::InvalidReceipt { .. })
    ));
    assert!(matches!(
        store.settle(
            "alice",
            &heartbeat.receipt,
            RunSettlement::Failed {
                code: RunFailureCode::Internal,
            },
        ),
        Err(AgentStoreError::InvalidReceipt { .. })
    ));
    let ticket = store
        .claim_recovery_due("alice", "reconciler", 30, 1)
        .unwrap()
        .remove(0);
    assert_eq!(ticket.reason, RecoveryReason::ExecutionTimedOut);
    let terminal = store.confirm_controller_gone("alice", &ticket).unwrap();
    assert_eq!(terminal.state, RunState::TimedOut);
    assert_eq!(terminal.failure_code, Some(RunFailureCode::TimedOut));
    store.audit_integrity().unwrap();
}

#[test]
fn late_start_fails_closed_then_starting_crash_recovers_as_timeout() {
    let (_directory, clock, store) = store();
    let now = clock.now();
    let mut timed = run("timed", "worker", now);
    timed.timeout_seconds = 1;
    store
        .create_root("alice", &worker("worker", None), &timed)
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 30, 1)
        .unwrap()
        .remove(0);
    clock.advance(2);
    assert!(matches!(
        store.start("alice", &claimed.receipt, &controller("late")),
        Err(AgentStoreError::InvalidReceipt { .. })
    ));
    let ticket = store
        .claim_recovery_due("alice", "reconciler", 30, 1)
        .unwrap()
        .remove(0);
    assert_eq!(ticket.reason, RecoveryReason::ExecutionTimedOut);
    let terminal = store.confirm_controller_gone("alice", &ticket).unwrap();
    assert_eq!(terminal.state, RunState::TimedOut);
    store.audit_integrity().unwrap();
}

#[test]
fn expired_recovery_ticket_is_reclaimed_and_survives_restart_before_confirm() {
    let (directory, clock, store) = store();
    let path = directory.path().join("agent.sqlite");
    let now = clock.now();
    store
        .create_root("alice", &worker("worker", None), &run("run", "worker", now))
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 5, 1)
        .unwrap()
        .remove(0);
    store
        .start("alice", &claimed.receipt, &controller("controller"))
        .unwrap();
    clock.advance(6);
    let first = store
        .claim_recovery_due("alice", "reconciler-one", 5, 1)
        .unwrap()
        .remove(0);
    clock.advance(6);
    let second = store
        .claim_recovery_due("alice", "reconciler-two", 30, 1)
        .unwrap()
        .remove(0);
    assert_eq!(second.reason, RecoveryReason::LeaseExpired);
    assert!(matches!(
        store.confirm_controller_gone("alice", &first),
        Err(AgentStoreError::InvalidRecoveryTicket { .. })
    ));
    drop(store);

    let reopened = SqliteAgentStore::open_with_clock(path, clock).unwrap();
    let terminal = reopened.confirm_controller_gone("alice", &second).unwrap();
    assert_eq!(terminal.state, RunState::Interrupted);
    assert_eq!(terminal.failure_code, Some(RunFailureCode::ControllerLost));
    reopened.audit_integrity().unwrap();
}

#[test]
fn abandoned_cancel_is_recovered_without_persisting_plaintext_capability() {
    let (directory, clock, store) = store();
    let now = clock.now();
    store
        .create_root("alice", &worker("worker", None), &run("run", "worker", now))
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 60, 1)
        .unwrap()
        .remove(0);
    store
        .start("alice", &claimed.receipt, &controller("controller"))
        .unwrap();
    let plan = store
        .request_cancel_tree(
            "alice",
            "worker",
            &cancel_request("abandoned-cancel", Vec::new()),
        )
        .unwrap();
    let cancel = &plan.tickets[0];
    assert!(!format!("{cancel:?}").contains(&cancel.token));
    let connection = rusqlite::Connection::open(directory.path().join("agent.sqlite")).unwrap();
    let stored_hash: String = connection
        .query_row(
            "SELECT token_hash FROM run_control_operations \
             WHERE owner = 'alice' AND run_id = 'run' AND status = 'active'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_ne!(stored_hash, cancel.token);
    assert_eq!(stored_hash.len(), 64);
    drop(connection);

    clock.advance(31);
    let recovery = store
        .claim_recovery_due("alice", "reconciler", 30, 1)
        .unwrap()
        .remove(0);
    assert_eq!(recovery.reason, RecoveryReason::CancellationAbandoned);
    assert!(!format!("{recovery:?}").contains(&recovery.token));
    assert!(matches!(
        store.settle_cancel("alice", cancel, CancelOutcome::Cancelled),
        Err(AgentStoreError::InvalidCancelTicket { .. })
    ));
    let terminal = store.confirm_controller_gone("alice", &recovery).unwrap();
    assert_eq!(terminal.state, RunState::Cancelled);
    store.audit_integrity().unwrap();
}

#[test]
fn owner_event_sequences_are_independently_contiguous() {
    let (_directory, clock, store) = store();
    let now = clock.now();
    store
        .create_root("alice", &worker("worker", None), &run("run", "worker", now))
        .unwrap();
    store
        .create_root("bob", &worker("worker", None), &run("run", "worker", now))
        .unwrap();
    store
        .enqueue_run("bob", &run("second", "worker", now))
        .unwrap();
    store
        .enqueue_run("alice", &run("second", "worker", now))
        .unwrap();

    for owner in ["alice", "bob"] {
        let sequences = store
            .unprojected_events(owner, "sequence-audit", 100)
            .unwrap()
            .items
            .into_iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>();
        assert_eq!(sequences, vec![1, 2, 3]);
    }
    store.audit_integrity().unwrap();
}

#[test]
fn clock_rollback_never_regresses_records_events_or_projection_time() {
    let (directory, clock, store) = store();
    let path = directory.path().join("agent.sqlite");
    let now = clock.now();
    store
        .create_root("alice", &worker("worker", None), &run("run", "worker", now))
        .unwrap();
    clock.advance(10);
    let claimed = store
        .claim_due("alice", "supervisor", 60, 1)
        .unwrap()
        .remove(0);
    let started = store
        .start("alice", &claimed.receipt, &controller("controller"))
        .unwrap();
    let before = started.run.updated_at;
    clock.rewind(100);
    let heartbeat = store.heartbeat("alice", &started.receipt, 30).unwrap();
    assert!(heartbeat.run.updated_at >= before);
    assert!(heartbeat.run.last_heartbeat_at.unwrap() >= before);
    let activity = store.record_activity("alice", &heartbeat.receipt).unwrap();
    assert!(activity.run.updated_at >= heartbeat.run.updated_at);
    assert!(activity.run.last_activity_at.unwrap() >= heartbeat.run.updated_at);
    let event = store
        .unprojected_events("alice", "rollback", 100)
        .unwrap()
        .items
        .pop()
        .unwrap();
    assert_eq!(event.kind, AgentEventKind::RunActivity);
    assert_eq!(event.occurred_at, activity.run.updated_at);
    store
        .mark_projected("alice", "rollback", &event.event_id)
        .unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    let projected_at_ms: i64 = connection
        .query_row(
            "SELECT p.projected_at_ms FROM agent_projector_progress p \
             JOIN agent_events e ON e.owner = p.owner AND e.sequence = p.event_sequence \
             WHERE p.owner = 'alice' AND p.projector = 'rollback' AND e.event_id = ?1",
            [&event.event_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(projected_at_ms >= event.occurred_at.timestamp_millis());
    drop(connection);
    store.audit_integrity().unwrap();
    drop(store);

    let reopened = SqliteAgentStore::open_with_clock(path, clock).unwrap();
    let events = reopened
        .unprojected_events("alice", "fresh-projector", 100)
        .unwrap()
        .items;
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].occurred_at <= pair[1].occurred_at)
    );
    reopened.audit_integrity().unwrap();
}

#[cfg(unix)]
#[test]
fn database_files_are_private_without_mutating_existing_parent() {
    use std::os::unix::fs::PermissionsExt as _;

    let directory = private_tempdir();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o750)).unwrap();
    let path = directory.path().join("agent.sqlite");
    let store = SqliteAgentStore::open(&path).unwrap();
    store
        .create_root(
            "alice",
            &worker("worker", None),
            &run("run", "worker", Utc::now()),
        )
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
    SqliteAgentStore::open(private_parent.join("agent.sqlite")).unwrap();
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
fn database_and_sidecar_symlinks_fail_closed() {
    use std::os::unix::fs::{PermissionsExt as _, symlink};

    let directory = private_tempdir();
    let victim = directory.path().join("victim");
    std::fs::write(&victim, b"unchanged").unwrap();
    let database_link = directory.path().join("linked.sqlite");
    symlink(&victim, &database_link).unwrap();
    assert!(SqliteAgentStore::open(&database_link).is_err());
    assert_eq!(std::fs::read(&victim).unwrap(), b"unchanged");

    let database = directory.path().join("agent.sqlite");
    drop(SqliteAgentStore::open(&database).unwrap());
    for suffix in ["-wal", "-shm"] {
        let sidecar = directory.path().join(format!("agent.sqlite{suffix}"));
        if sidecar.exists() {
            std::fs::remove_file(&sidecar).unwrap();
        }
        symlink(&victim, &sidecar).unwrap();
        assert!(SqliteAgentStore::open(&database).is_err());
        assert_eq!(std::fs::read(&victim).unwrap(), b"unchanged");
        std::fs::remove_file(sidecar).unwrap();
    }

    let writable_parent = directory.path().join("shared");
    std::fs::create_dir(&writable_parent).unwrap();
    std::fs::set_permissions(&writable_parent, std::fs::Permissions::from_mode(0o770)).unwrap();
    assert!(matches!(
        SqliteAgentStore::open(writable_parent.join("agent.sqlite")),
        Err(AgentStoreError::InvalidInput(_))
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

#[cfg(unix)]
#[test]
fn existing_database_files_with_permissive_modes_fail_closed() {
    use std::os::unix::fs::PermissionsExt as _;

    let directory = private_tempdir();
    let database = directory.path().join("agent.sqlite");
    drop(SqliteAgentStore::open(&database).unwrap());

    std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o640)).unwrap();
    assert!(matches!(
        SqliteAgentStore::open(&database),
        Err(AgentStoreError::InvalidInput(_))
    ));
    std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o600)).unwrap();

    for suffix in ["-wal", "-shm"] {
        let sidecar = directory.path().join(format!("agent.sqlite{suffix}"));
        std::fs::write(&sidecar, b"insecure sidecar").unwrap();
        std::fs::set_permissions(&sidecar, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(matches!(
            SqliteAgentStore::open(&database),
            Err(AgentStoreError::InvalidInput(_))
        ));
        std::fs::remove_file(sidecar).unwrap();
    }

    let sidecar = directory.path().join("agent.sqlite-shm");
    std::fs::create_dir(&sidecar).unwrap();
    std::fs::set_permissions(&sidecar, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(matches!(
        SqliteAgentStore::open(&database),
        Err(AgentStoreError::InvalidInput(_))
    ));
}
