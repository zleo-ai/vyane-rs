#![allow(clippy::unwrap_used)]

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::sync::{Arc, Barrier, Condvar, Mutex, mpsc};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
use static_assertions::assert_not_impl_any;
use tempfile::TempDir;
use vyane_agent::{
    ActiveCompletionPermit, AgentClock, AgentEventKind, AgentStore, AgentStoreError, CancelOutcome,
    CancelRequest, ControllerKind, ControllerRef, NativeExecutionScope, NewAgentRun,
    NewRunCompletion, NewWorker, RecoveryReason, RunCompletionStatus, RunFailureCode, RunMode,
    RunSettlement, RunState, SqliteAgentStore,
};

assert_not_impl_any!(ActiveCompletionPermit: Clone);

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

#[derive(Debug)]
struct ReaderBlockingClock {
    now: Mutex<DateTime<Utc>>,
    gate: Mutex<ReaderGate>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct ReaderGate {
    enabled: bool,
    entered: bool,
    released: bool,
}

impl ReaderBlockingClock {
    fn new() -> Self {
        Self {
            now: Mutex::new(
                Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 0)
                    .single()
                    .unwrap(),
            ),
            gate: Mutex::new(ReaderGate::default()),
            changed: Condvar::new(),
        }
    }

    fn advance(&self, seconds: i64) {
        *self.now.lock().unwrap() += TimeDelta::seconds(seconds);
    }

    fn block_reader(&self) {
        let mut gate = self.gate.lock().unwrap();
        gate.enabled = true;
        gate.entered = false;
        gate.released = false;
    }

    fn wait_until_reader_entered(&self) {
        let mut gate = self.gate.lock().unwrap();
        while !gate.entered {
            gate = self.changed.wait(gate).unwrap();
        }
    }

    fn release_reader(&self) {
        let mut gate = self.gate.lock().unwrap();
        gate.released = true;
        self.changed.notify_all();
    }
}

impl AgentClock for ReaderBlockingClock {
    fn now(&self) -> DateTime<Utc> {
        let is_reader = std::thread::current().name() == Some("completion-reader");
        if is_reader {
            let mut gate = self.gate.lock().unwrap();
            if gate.enabled {
                gate.entered = true;
                self.changed.notify_all();
                while !gate.released {
                    gate = self.changed.wait(gate).unwrap();
                }
                gate.enabled = false;
            }
        }
        *self.now.lock().unwrap()
    }
}

fn digest(byte: char) -> String {
    std::iter::repeat_n(byte, 64).collect()
}

fn private_tempdir() -> TempDir {
    TempDir::new_in(std::env::temp_dir().canonicalize().unwrap()).unwrap()
}

struct Fixture {
    _directory: TempDir,
    clock: Arc<TestClock>,
    store: SqliteAgentStore,
}

impl Fixture {
    fn new() -> Self {
        let directory = private_tempdir();
        let clock = Arc::new(TestClock::new());
        let store =
            SqliteAgentStore::open_with_clock(directory.path().join("agent.sqlite"), clock.clone())
                .unwrap();
        Self {
            _directory: directory,
            clock,
            store,
        }
    }

    fn start(
        &self,
        owner: &str,
        id: &str,
        lease_seconds: u64,
        timeout_seconds: u64,
    ) -> vyane_agent::ClaimedRun {
        let worker_id = format!("worker-{id}");
        self.store
            .create_root(
                owner,
                &NewWorker {
                    id: worker_id.clone(),
                    logical_session_id: None,
                },
                &NewAgentRun {
                    id: id.into(),
                    worker_id,
                    task_id: None,
                    trace_id: None,
                    parent_run_id: None,
                    mode: RunMode::Autonomous,
                    target_key: "test/default".into(),
                    prompt_digest: digest('a'),
                    policy_digest: digest('b'),
                    available_at: self.clock.now(),
                    timeout_seconds,
                    max_resume_attempts: 1,
                },
            )
            .unwrap();
        let claim = self
            .store
            .claim_due(owner, "executor", lease_seconds, 1)
            .unwrap()
            .remove(0);
        self.store
            .start(
                owner,
                &claim.receipt,
                &ControllerRef {
                    kind: ControllerKind::InProcess,
                    id: format!("controller-{id}"),
                    fingerprint: Some(format!("fingerprint-{id}")),
                },
            )
            .unwrap()
    }
}

fn proposal(id: &str) -> NewRunCompletion {
    NewRunCompletion {
        id: format!("completion-{id}"),
        sink_kind: "test-sink-v1".into(),
        publication_key: format!("opaque-{id}"),
        content_digest: digest('c'),
        content_bytes: 42,
    }
}

#[test]
fn prepare_revokes_generic_effects_but_keeps_exact_completion_live() {
    let fixture = Fixture::new();
    let started = fixture.start("alice", "run", 30, 600);
    let permit = fixture
        .store
        .issue_execution_permit("alice", &started.receipt, &started.run.policy_digest)
        .unwrap();
    let prepared = fixture
        .store
        .prepare_completion("alice", &permit, &proposal("run"))
        .unwrap();

    assert!(matches!(
        fixture
            .store
            .validate_execution_permit("alice", &permit, &started.run.policy_digest,),
        Err(AgentStoreError::InvalidExecutionPermit { .. })
    ));
    let scope = NativeExecutionScope::fresh(
        &started.run.target_key,
        &started.run.prompt_digest,
        &started.run.policy_digest,
        None,
    )
    .unwrap();
    assert!(matches!(
        fixture
            .store
            .validate_native_execution_permit("alice", &permit, &scope),
        Err(AgentStoreError::InvalidExecutionPermit { .. })
    ));
    assert!(matches!(
        fixture.store.record_activity("alice", &started.receipt),
        Err(AgentStoreError::InvalidReceipt { .. })
    ));
    let heartbeat = fixture
        .store
        .heartbeat("alice", &started.receipt, 30)
        .unwrap();
    let snapshot = fixture
        .store
        .validate_completion_permit("alice", &prepared.permit)
        .unwrap();
    assert_eq!(snapshot.run_revision, heartbeat.run.revision);
    assert_eq!(snapshot.record.status, RunCompletionStatus::Prepared);

    let (run, committed) = fixture
        .store
        .commit_completion("alice", &prepared.permit)
        .unwrap();
    assert_eq!(run.state, RunState::Succeeded);
    assert_eq!(committed.status, RunCompletionStatus::Committed);
    assert!(run.controller.is_none());
    assert!(run.lease.is_none());
    fixture.store.audit_integrity().unwrap();
}

#[test]
fn prepare_and_commit_replay_are_idempotent_and_drift_conflicts() {
    let fixture = Fixture::new();
    let started = fixture.start("alice", "run", 30, 600);
    let permit = fixture
        .store
        .issue_execution_permit("alice", &started.receipt, &started.run.policy_digest)
        .unwrap();
    let first = fixture
        .store
        .prepare_completion("alice", &permit, &proposal("run"))
        .unwrap();
    let replay = fixture
        .store
        .prepare_completion("alice", &permit, &proposal("run"))
        .unwrap();
    assert_eq!(first.record, replay.record);
    fixture
        .store
        .validate_completion_permit("alice", &replay.permit)
        .unwrap();

    let mut drift = proposal("run");
    drift.content_digest = digest('d');
    assert!(matches!(
        fixture.store.prepare_completion("alice", &permit, &drift),
        Err(AgentStoreError::CompletionConflict { .. })
    ));

    let (first_run, first_record) = fixture
        .store
        .commit_completion("alice", &first.permit)
        .unwrap();
    let events_before = fixture
        .store
        .unprojected_events("alice", "audit", 100)
        .unwrap()
        .items
        .len();
    let (replayed_run, replayed_record) = fixture
        .store
        .commit_completion("alice", &replay.permit)
        .unwrap();
    assert_eq!(first_run, replayed_run);
    assert_eq!(first_record, replayed_record);
    assert_eq!(
        events_before,
        fixture
            .store
            .unprojected_events("alice", "audit", 100)
            .unwrap()
            .items
            .len()
    );
}

#[test]
fn owner_scope_and_non_success_settlement_abandon_prepared_results() {
    let fixture = Fixture::new();
    for owner in ["alice", "bob"] {
        let started = fixture.start(owner, "same", 30, 600);
        let permit = fixture
            .store
            .issue_execution_permit(owner, &started.receipt, &started.run.policy_digest)
            .unwrap();
        let prepared = fixture
            .store
            .prepare_completion(owner, &permit, &proposal("same"))
            .unwrap();
        if owner == "alice" {
            assert!(matches!(
                fixture.store.commit_completion("bob", &prepared.permit),
                Err(AgentStoreError::InvalidCompletionPermit { .. })
            ));
        }
        fixture
            .store
            .settle(
                owner,
                &started.receipt,
                RunSettlement::Failed {
                    code: RunFailureCode::Internal,
                },
            )
            .unwrap();
    }
    assert!(
        fixture
            .store
            .get_completion("mallory", "same")
            .unwrap()
            .is_none()
    );
    for owner in ["alice", "bob"] {
        let completion = fixture
            .store
            .get_completion(owner, "same")
            .unwrap()
            .unwrap();
        assert_eq!(completion.owner, owner);
        assert_eq!(completion.status, RunCompletionStatus::Abandoned);
    }
    fixture.store.audit_integrity().unwrap();
}

#[test]
fn lease_loss_recovery_can_commit_exact_generation_once() {
    let fixture = Fixture::new();
    let started = fixture.start("alice", "recover", 5, 600);
    let permit = fixture
        .store
        .issue_execution_permit("alice", &started.receipt, &started.run.policy_digest)
        .unwrap();
    let prepared = fixture
        .store
        .prepare_completion("alice", &permit, &proposal("recover"))
        .unwrap();
    fixture.clock.advance(6);
    let ticket = fixture
        .store
        .claim_recovery_due("alice", "reconciler", 30, 1)
        .unwrap()
        .remove(0);
    assert_eq!(ticket.reason, RecoveryReason::LeaseExpired);
    assert_eq!(
        fixture
            .store
            .completion_for_recovery("alice", &ticket)
            .unwrap()
            .unwrap(),
        prepared.record
    );
    let mut wrong_generation = ticket.clone();
    wrong_generation.generation += 1;
    assert!(matches!(
        fixture
            .store
            .completion_for_recovery("alice", &wrong_generation),
        Err(AgentStoreError::InvalidRecoveryTicket { .. })
    ));
    let (run, completion) = fixture
        .store
        .commit_recovered_completion("alice", &ticket, prepared.record.completion_id.as_str())
        .unwrap();
    assert_eq!(run.state, RunState::Succeeded);
    assert_eq!(completion.status, RunCompletionStatus::Committed);
    assert_eq!(
        completion.committed_by_operation_id.as_deref(),
        Some(ticket.operation_id.as_str())
    );
    let events_before = fixture
        .store
        .unprojected_events("alice", "audit", 100)
        .unwrap()
        .items
        .len();
    let replay = fixture
        .store
        .commit_recovered_completion("alice", &ticket, prepared.record.completion_id.as_str())
        .unwrap();
    assert_eq!(replay, (run, completion));
    assert_eq!(
        events_before,
        fixture
            .store
            .unprojected_events("alice", "audit", 100)
            .unwrap()
            .items
            .len()
    );
    fixture.store.audit_integrity().unwrap();
}

#[test]
fn timeout_recovery_cannot_upgrade_a_staged_result() {
    let fixture = Fixture::new();
    let started = fixture.start("alice", "timeout", 30, 5);
    let permit = fixture
        .store
        .issue_execution_permit("alice", &started.receipt, &started.run.policy_digest)
        .unwrap();
    let prepared = fixture
        .store
        .prepare_completion("alice", &permit, &proposal("timeout"))
        .unwrap();
    fixture.clock.advance(6);
    let ticket = fixture
        .store
        .claim_recovery_due("alice", "reconciler", 30, 1)
        .unwrap()
        .remove(0);
    assert_eq!(ticket.reason, RecoveryReason::ExecutionTimedOut);
    assert!(matches!(
        fixture.store.commit_recovered_completion(
            "alice",
            &ticket,
            prepared.record.completion_id.as_str(),
        ),
        Err(AgentStoreError::InvalidRecoveryTicket { .. })
    ));
    let run = fixture
        .store
        .confirm_controller_gone("alice", &ticket)
        .unwrap();
    assert_eq!(run.state, RunState::TimedOut);
    assert_eq!(
        fixture
            .store
            .get_completion("alice", "timeout")
            .unwrap()
            .unwrap()
            .status,
        RunCompletionStatus::Abandoned
    );
    fixture.store.audit_integrity().unwrap();
}

fn run_event_kinds(store: &dyn AgentStore, owner: &str, run_id: &str) -> Vec<AgentEventKind> {
    store
        .unprojected_events(owner, "completion-test", 100)
        .unwrap()
        .items
        .into_iter()
        .filter(|event| event.run_id.as_deref() == Some(run_id))
        .map(|event| event.kind)
        .collect()
}

#[test]
fn completion_outbox_is_complete_ordered_and_replay_safe_for_every_abandon_path() {
    let fixture = Fixture::new();

    let settled = fixture.start("alice", "settled", 30, 600);
    let settled_permit = fixture
        .store
        .issue_execution_permit("alice", &settled.receipt, &settled.run.policy_digest)
        .unwrap();
    fixture
        .store
        .prepare_completion("alice", &settled_permit, &proposal("settled"))
        .unwrap();
    fixture
        .store
        .prepare_completion("alice", &settled_permit, &proposal("settled"))
        .unwrap();
    assert_eq!(
        run_event_kinds(&fixture.store, "alice", "settled")
            .into_iter()
            .filter(|kind| *kind == AgentEventKind::CompletionPrepared)
            .count(),
        1
    );
    fixture
        .store
        .settle(
            "alice",
            &settled.receipt,
            RunSettlement::Failed {
                code: RunFailureCode::Internal,
            },
        )
        .unwrap();
    let settled_events = run_event_kinds(&fixture.store, "alice", "settled");
    assert_eq!(
        &settled_events[settled_events.len() - 2..],
        [
            AgentEventKind::RunSettled,
            AgentEventKind::CompletionAbandoned,
        ]
    );

    let cancelled = fixture.start("alice", "cancelled", 30, 600);
    let cancelled_permit = fixture
        .store
        .issue_execution_permit("alice", &cancelled.receipt, &cancelled.run.policy_digest)
        .unwrap();
    fixture
        .store
        .prepare_completion("alice", &cancelled_permit, &proposal("cancelled"))
        .unwrap();
    let cancel_ticket = fixture
        .store
        .request_cancel_tree(
            "alice",
            "worker-cancelled",
            &CancelRequest {
                operation_id: "cancel-completion".into(),
                lease_owner: "canceller".into(),
                lease_seconds: 30,
                retry_tickets: Vec::new(),
            },
        )
        .unwrap()
        .tickets
        .remove(0);
    fixture
        .store
        .settle_cancel("alice", &cancel_ticket, CancelOutcome::Cancelled)
        .unwrap();
    let cancel_events_before = run_event_kinds(&fixture.store, "alice", "cancelled");
    assert_eq!(
        &cancel_events_before[cancel_events_before.len() - 2..],
        [
            AgentEventKind::CancelSettled,
            AgentEventKind::CompletionAbandoned,
        ]
    );
    assert!(matches!(
        fixture
            .store
            .settle_cancel("alice", &cancel_ticket, CancelOutcome::Cancelled),
        Err(AgentStoreError::InvalidCancelTicket { .. })
    ));
    assert_eq!(
        cancel_events_before,
        run_event_kinds(&fixture.store, "alice", "cancelled")
    );

    let recovered = fixture.start("alice", "recovered", 1, 600);
    let recovered_permit = fixture
        .store
        .issue_execution_permit("alice", &recovered.receipt, &recovered.run.policy_digest)
        .unwrap();
    fixture
        .store
        .prepare_completion("alice", &recovered_permit, &proposal("recovered"))
        .unwrap();
    fixture.clock.advance(2);
    let recovery_ticket = fixture
        .store
        .claim_recovery_due("alice", "reconciler", 30, 1)
        .unwrap()
        .remove(0);
    fixture
        .store
        .confirm_controller_gone("alice", &recovery_ticket)
        .unwrap();
    let recovery_events_before = run_event_kinds(&fixture.store, "alice", "recovered");
    assert_eq!(
        &recovery_events_before[recovery_events_before.len() - 2..],
        [
            AgentEventKind::RecoverySettled,
            AgentEventKind::CompletionAbandoned,
        ]
    );
    assert!(matches!(
        fixture
            .store
            .confirm_controller_gone("alice", &recovery_ticket),
        Err(AgentStoreError::InvalidRecoveryTicket { .. })
    ));
    assert_eq!(
        recovery_events_before,
        run_event_kinds(&fixture.store, "alice", "recovered")
    );

    let all_events = fixture
        .store
        .unprojected_events("alice", "completion-order", 100)
        .unwrap()
        .items;
    assert!(
        all_events
            .windows(2)
            .all(|pair| pair[0].sequence + 1 == pair[1].sequence)
    );
    fixture.store.audit_integrity().unwrap();
}

#[test]
fn tampered_completion_abandon_order_fails_integrity_audit() {
    let fixture = Fixture::new();
    let started = fixture.start("alice", "order-tamper", 30, 600);
    let permit = fixture
        .store
        .issue_execution_permit("alice", &started.receipt, &started.run.policy_digest)
        .unwrap();
    fixture
        .store
        .prepare_completion("alice", &permit, &proposal("order-tamper"))
        .unwrap();
    fixture
        .store
        .settle(
            "alice",
            &started.receipt,
            RunSettlement::Failed {
                code: RunFailureCode::Internal,
            },
        )
        .unwrap();
    let connection = rusqlite::Connection::open(fixture.store.path()).unwrap();
    let prepared_sequence: i64 = connection
        .query_row(
            "SELECT sequence FROM agent_events WHERE owner = 'alice' AND run_id = 'order-tamper' AND event_type = 'completion_prepared'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let terminal_sequence: i64 = connection
        .query_row(
            "SELECT sequence FROM agent_events WHERE owner = 'alice' AND run_id = 'order-tamper' AND event_type = 'run_settled'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let transaction = connection.unchecked_transaction().unwrap();
    transaction
        .execute(
            "UPDATE agent_events SET sequence = -1 WHERE owner = 'alice' AND sequence = ?1",
            [prepared_sequence],
        )
        .unwrap();
    transaction
        .execute(
            "UPDATE agent_events SET sequence = ?1 WHERE owner = 'alice' AND sequence = ?2",
            rusqlite::params![prepared_sequence, terminal_sequence],
        )
        .unwrap();
    transaction
        .execute(
            "UPDATE agent_events SET sequence = ?1 WHERE owner = 'alice' AND sequence = -1",
            [terminal_sequence],
        )
        .unwrap();
    transaction.commit().unwrap();
    assert!(matches!(
        fixture.store.audit_integrity(),
        Err(AgentStoreError::CorruptData(_))
    ));
}

#[test]
fn tampered_completion_commit_snapshot_fails_integrity_audit() {
    let fixture = Fixture::new();
    let started = fixture.start("alice", "commit-tamper", 30, 600);
    let permit = fixture
        .store
        .issue_execution_permit("alice", &started.receipt, &started.run.policy_digest)
        .unwrap();
    let prepared = fixture
        .store
        .prepare_completion("alice", &permit, &proposal("commit-tamper"))
        .unwrap();
    let (run, completion) = fixture
        .store
        .commit_completion("alice", &prepared.permit)
        .unwrap();
    assert_eq!(completion.committed_run_revision, Some(run.revision));
    let connection = rusqlite::Connection::open(fixture.store.path()).unwrap();
    connection
        .execute(
            "UPDATE agent_events SET run_state = 'failed' WHERE owner = 'alice' AND run_id = 'commit-tamper' AND event_type = 'completion_committed'",
            [],
        )
        .unwrap();
    assert!(matches!(
        fixture.store.audit_integrity(),
        Err(AgentStoreError::CorruptData(_))
    ));
}

#[test]
fn recovery_descriptor_read_linearizes_before_ticket_settlement() {
    let directory = private_tempdir();
    let clock = Arc::new(ReaderBlockingClock::new());
    let store = Arc::new(
        SqliteAgentStore::open_with_clock(directory.path().join("agent.sqlite"), clock.clone())
            .unwrap(),
    );
    let now = clock.now();
    store
        .create_root(
            "alice",
            &NewWorker {
                id: "worker-linearization".into(),
                logical_session_id: None,
            },
            &NewAgentRun {
                id: "linearization".into(),
                worker_id: "worker-linearization".into(),
                task_id: None,
                trace_id: None,
                parent_run_id: None,
                mode: RunMode::Autonomous,
                target_key: "test/default".into(),
                prompt_digest: digest('a'),
                policy_digest: digest('b'),
                available_at: now,
                timeout_seconds: 600,
                max_resume_attempts: 1,
            },
        )
        .unwrap();
    let claim = store
        .claim_due("alice", "executor", 1, 1)
        .unwrap()
        .remove(0);
    let started = store
        .start(
            "alice",
            &claim.receipt,
            &ControllerRef {
                kind: ControllerKind::InProcess,
                id: "controller-linearization".into(),
                fingerprint: Some("fingerprint-linearization".into()),
            },
        )
        .unwrap();
    let permit = store
        .issue_execution_permit("alice", &started.receipt, &started.run.policy_digest)
        .unwrap();
    let prepared = store
        .prepare_completion("alice", &permit, &proposal("linearization"))
        .unwrap();
    clock.advance(2);
    let ticket = store
        .claim_recovery_due("alice", "reconciler", 30, 1)
        .unwrap()
        .remove(0);

    clock.block_reader();
    let reader_store = Arc::clone(&store);
    let reader_ticket = ticket.clone();
    let reader = std::thread::Builder::new()
        .name("completion-reader".into())
        .spawn(move || reader_store.completion_for_recovery("alice", &reader_ticket))
        .unwrap();
    clock.wait_until_reader_entered();

    let settlement_store = Arc::clone(&store);
    let settlement_ticket = ticket.clone();
    let (started_tx, started_rx) = mpsc::sync_channel(0);
    let (done_tx, done_rx) = mpsc::sync_channel(0);
    let settlement = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = settlement_store.confirm_controller_gone("alice", &settlement_ticket);
        done_tx.send(result).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(matches!(
        done_rx.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));

    clock.release_reader();
    assert_eq!(reader.join().unwrap().unwrap(), Some(prepared.record));
    assert_eq!(
        done_rx.recv().unwrap().unwrap().state,
        RunState::Interrupted
    );
    settlement.join().unwrap();
    assert_eq!(
        store
            .get_completion("alice", "linearization")
            .unwrap()
            .unwrap()
            .status,
        RunCompletionStatus::Abandoned
    );
    store.audit_integrity().unwrap();
}

#[test]
fn concurrent_commit_and_cancel_have_one_terminal_truth() {
    let fixture = Fixture::new();
    let started = fixture.start("alice", "race", 30, 600);
    let permit = fixture
        .store
        .issue_execution_permit("alice", &started.receipt, &started.run.policy_digest)
        .unwrap();
    let prepared = fixture
        .store
        .prepare_completion("alice", &permit, &proposal("race"))
        .unwrap();
    let store = Arc::new(fixture.store);
    let barrier = Arc::new(Barrier::new(3));
    let commit_store = Arc::clone(&store);
    let commit_barrier = Arc::clone(&barrier);
    let commit = std::thread::spawn(move || {
        commit_barrier.wait();
        commit_store.commit_completion("alice", &prepared.permit)
    });
    let cancel_store = Arc::clone(&store);
    let cancel_barrier = Arc::clone(&barrier);
    let cancel = std::thread::spawn(move || {
        cancel_barrier.wait();
        cancel_store.request_cancel_tree(
            "alice",
            "worker-race",
            &CancelRequest {
                operation_id: "cancel-race".into(),
                lease_owner: "canceller".into(),
                lease_seconds: 30,
                retry_tickets: Vec::new(),
            },
        )
    });
    barrier.wait();
    let commit_result = commit.join().unwrap();
    let plan = cancel.join().unwrap().unwrap();
    for ticket in &plan.tickets {
        store
            .settle_cancel("alice", ticket, CancelOutcome::Cancelled)
            .unwrap();
    }
    let run = store.get_run("alice", "race").unwrap().unwrap();
    let completion = store.get_completion("alice", "race").unwrap().unwrap();
    match run.state {
        RunState::Succeeded => {
            assert!(commit_result.is_ok());
            assert_eq!(completion.status, RunCompletionStatus::Committed);
            assert!(plan.tickets.is_empty());
        }
        RunState::Cancelled => {
            assert!(matches!(
                commit_result,
                Err(AgentStoreError::InvalidCompletionPermit { .. })
            ));
            assert_eq!(completion.status, RunCompletionStatus::Abandoned);
            assert_eq!(plan.tickets.len(), 1);
        }
        state => panic!("unexpected terminal state: {state:?}"),
    }
    store.audit_integrity().unwrap();
}

#[test]
fn version_one_database_migrates_atomically_with_existing_truth() {
    let directory = private_tempdir();
    let path = directory.path().join("agent.sqlite");
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(include_str!("../migrations/0001_agent.sql"))
        .unwrap();
    let now = Utc
        .with_ymd_and_hms(2026, 7, 13, 12, 0, 0)
        .single()
        .unwrap()
        .timestamp_millis();
    connection
        .execute(
            "INSERT INTO workers (owner,id,parent_id,logical_session_id,lifecycle,created_at_ms,updated_at_ms,released_at_ms,revision,record_schema) VALUES ('alice','worker',NULL,NULL,'open',?1,?1,NULL,0,1)",
            [now],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO agent_runs (owner,id,queue_sequence,worker_id,task_id,trace_id,parent_run_id,resume_of_run_id,state,mode,target_key,prompt_digest,policy_digest,resume_binding_digest,available_at_ms,deadline_at_ms,timeout_ms,max_resume_attempts,resume_attempt,created_at_ms,started_at_ms,updated_at_ms,finished_at_ms,revision,worker_generation,controller_kind,controller_id,controller_fingerprint,lease_owner,lease_expires_at_ms,lease_token_hash,last_heartbeat_at_ms,last_activity_at_ms,failure_code,record_schema) VALUES ('alice','run',1,'worker',NULL,NULL,NULL,NULL,'queued','autonomous','test/default',?1,?2,NULL,?3,NULL,600000,1,0,?3,NULL,?3,NULL,0,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1)",
            rusqlite::params![digest('a'), digest('b'), now],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO owner_agent_event_sequences (owner,next_sequence) VALUES ('alice',3)",
            [],
        )
        .unwrap();
    connection
        .execute("INSERT INTO agent_events (owner,sequence,event_id,worker_id,run_id,occurred_at_ms,event_type,worker_revision,run_revision,run_state,worker_lifecycle) VALUES ('alice',1,'event-worker','worker',NULL,?1,'worker_created',0,NULL,NULL,'open')", [now])
        .unwrap();
    connection
        .execute("INSERT INTO agent_events (owner,sequence,event_id,worker_id,run_id,occurred_at_ms,event_type,worker_revision,run_revision,run_state,worker_lifecycle) VALUES ('alice',2,'event-run','worker','run',?1,'run_queued',0,0,'queued','open')", [now])
        .unwrap();
    connection.pragma_update(None, "user_version", 1).unwrap();
    drop(connection);
    #[cfg(unix)]
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

    let store = SqliteAgentStore::open(&path).unwrap();
    assert_eq!(
        store.get_run("alice", "run").unwrap().unwrap().state,
        RunState::Queued
    );
    assert!(store.get_completion("alice", "run").unwrap().is_none());
    store.audit_integrity().unwrap();
    let connection = rusqlite::Connection::open(path).unwrap();
    let version: u32 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, vyane_agent::SCHEMA_VERSION);
}

#[test]
fn drifted_version_one_schema_is_rejected_without_partial_migration() {
    let directory = private_tempdir();
    let path = directory.path().join("agent.sqlite");
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(include_str!("../migrations/0001_agent.sql"))
        .unwrap();
    connection
        .execute("CREATE TABLE unexpected_schema_drift (value TEXT)", [])
        .unwrap();
    connection.pragma_update(None, "user_version", 1).unwrap();
    drop(connection);
    #[cfg(unix)]
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

    assert!(matches!(
        SqliteAgentStore::open(&path),
        Err(AgentStoreError::CorruptData(_))
    ));
    let connection = rusqlite::Connection::open(path).unwrap();
    let version: u32 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 1);
    let completion_table: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'agent_run_completions')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!completion_table);
}
