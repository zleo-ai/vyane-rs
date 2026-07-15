#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
use static_assertions::{assert_impl_all, assert_not_impl_any};
use tempfile::TempDir;
use vyane_agent::{
    ActiveExecutionPermit, AgentClock, AgentStore, AgentStoreError, CancelOutcome, CancelRequest,
    ControllerKind, ControllerRef, ExecutionPermitSnapshot, NativeExecutionScope, NewAgentRun,
    NewWorker, ResumeSessionProof, RunMode, SqliteAgentStore,
};

assert_not_impl_any!(ActiveExecutionPermit: serde::Serialize, serde::de::DeserializeOwned, Clone);
assert_not_impl_any!(NativeExecutionScope: serde::Serialize, serde::de::DeserializeOwned);
assert_impl_all!(ExecutionPermitSnapshot: serde::Serialize, serde::de::DeserializeOwned, Clone);

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
}

impl AgentClock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

fn digest(byte: char) -> String {
    std::iter::repeat_n(byte, 64).collect()
}

fn new_run(id: &str, worker_id: &str, now: DateTime<Utc>, timeout_seconds: u64) -> NewAgentRun {
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
        timeout_seconds,
        max_resume_attempts: 2,
    }
}

fn controller() -> ControllerRef {
    ControllerRef {
        kind: ControllerKind::InProcess,
        id: "controller".into(),
        fingerprint: Some("controller-fingerprint".into()),
    }
}

fn store() -> (TempDir, Arc<TestClock>, SqliteAgentStore) {
    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(TestClock::new());
    let store =
        SqliteAgentStore::open_with_clock(directory.path().join("agent.sqlite"), clock.clone())
            .unwrap();
    (directory, clock, store)
}

fn assert_invalid<T>(result: Result<T, AgentStoreError>) {
    assert!(matches!(
        result,
        Err(AgentStoreError::InvalidExecutionPermit { .. })
    ));
}

#[test]
fn issuance_requires_exact_running_receipt_owner_and_policy() {
    let (_directory, clock, store) = store();
    let policy = digest('b');
    store
        .create_root(
            "alice",
            &NewWorker {
                id: "worker".into(),
                logical_session_id: None,
            },
            &new_run("run", "worker", clock.now(), 600),
        )
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 30, 1)
        .unwrap()
        .remove(0);

    assert_invalid(store.issue_execution_permit("alice", &claimed.receipt, &policy));
    let started = store
        .start("alice", &claimed.receipt, &controller())
        .unwrap();
    let mut wrong_revision = started.receipt.clone();
    wrong_revision.revision += 1;
    assert_invalid(store.issue_execution_permit("alice", &wrong_revision, &policy));
    let mut wrong_generation = started.receipt.clone();
    wrong_generation.generation += 1;
    assert_invalid(store.issue_execution_permit("alice", &wrong_generation, &policy));
    let mut wrong_lease_owner = started.receipt.clone();
    wrong_lease_owner.lease_owner = "other-supervisor".into();
    assert_invalid(store.issue_execution_permit("alice", &wrong_lease_owner, &policy));
    let mut wrong_token = started.receipt.clone();
    wrong_token.token = "0".repeat(64);
    assert_invalid(store.issue_execution_permit("alice", &wrong_token, &policy));
    assert_invalid(store.issue_execution_permit("bob", &started.receipt, &policy));
    assert_invalid(store.issue_execution_permit("alice", &started.receipt, &digest('c')));

    let token = started.receipt.token.clone();
    let permit = store
        .issue_execution_permit("alice", &started.receipt, &policy)
        .unwrap();
    assert_eq!(permit.owner(), "alice");
    assert_eq!(permit.run_id(), "run");
    assert_eq!(permit.worker_id(), "worker");
    assert_eq!(permit.generation(), started.run.worker_generation);
    assert_eq!(permit.lease_owner(), "supervisor");
    assert_eq!(permit.policy_digest(), policy);
    let debug = format!("{permit:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains(&token));
    assert!(!debug.contains(&policy));

    assert_invalid(store.validate_execution_permit("bob", &permit, &policy));
    assert_invalid(store.validate_execution_permit("alice", &permit, &digest('c')));
}

#[test]
fn permit_survives_heartbeat_and_activity_revision_changes() {
    let (_directory, clock, store) = store();
    let policy = digest('b');
    store
        .create_root(
            "alice",
            &NewWorker {
                id: "worker".into(),
                logical_session_id: None,
            },
            &new_run("run", "worker", clock.now(), 600),
        )
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 30, 1)
        .unwrap()
        .remove(0);
    let started = store
        .start("alice", &claimed.receipt, &controller())
        .unwrap();
    let permit = store
        .issue_execution_permit("alice", &started.receipt, &policy)
        .unwrap();

    clock.advance(5);
    let heartbeat = store.heartbeat("alice", &started.receipt, 30).unwrap();
    let after_heartbeat = store
        .validate_execution_permit("alice", &permit, &policy)
        .unwrap();
    assert_eq!(after_heartbeat.run_revision(), heartbeat.run.revision);
    assert_eq!(after_heartbeat.owner(), "alice");
    assert_eq!(after_heartbeat.run_id(), "run");
    assert_eq!(after_heartbeat.worker_id(), "worker");
    assert_eq!(
        after_heartbeat.generation(),
        heartbeat.run.worker_generation
    );
    assert_eq!(after_heartbeat.lease_owner(), "supervisor");
    assert_eq!(after_heartbeat.target_key(), "codex/default");
    assert_eq!(after_heartbeat.prompt_digest(), digest('a'));
    assert_eq!(after_heartbeat.policy_digest(), policy);
    assert_eq!(
        after_heartbeat.lease_expires_at(),
        heartbeat.run.lease.as_ref().unwrap().expires_at
    );
    assert_eq!(
        after_heartbeat.deadline_at(),
        heartbeat.run.deadline_at.unwrap()
    );
    assert!(after_heartbeat.validated_at() >= heartbeat.run.updated_at);

    clock.advance(1);
    let activity = store.record_activity("alice", &heartbeat.receipt).unwrap();
    let after_activity = store
        .validate_execution_permit("alice", &permit, &policy)
        .unwrap();
    assert_eq!(after_activity.run_revision(), activity.run.revision);
    assert_eq!(after_activity.target_key(), "codex/default");
    assert_eq!(after_activity.prompt_digest(), digest('a'));
}

#[test]
fn cancelling_and_terminal_runs_revoke_execution_permit() {
    let (_directory, clock, store) = store();
    let policy = digest('b');
    store
        .create_root(
            "alice",
            &NewWorker {
                id: "worker".into(),
                logical_session_id: None,
            },
            &new_run("run", "worker", clock.now(), 600),
        )
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 60, 1)
        .unwrap()
        .remove(0);
    let started = store
        .start("alice", &claimed.receipt, &controller())
        .unwrap();
    let permit = store
        .issue_execution_permit("alice", &started.receipt, &policy)
        .unwrap();
    let plan = store
        .request_cancel_tree(
            "alice",
            "worker",
            &CancelRequest {
                operation_id: "cancel".into(),
                lease_owner: "cancel-supervisor".into(),
                lease_seconds: 30,
                retry_tickets: Vec::new(),
            },
        )
        .unwrap();
    assert_invalid(store.validate_execution_permit("alice", &permit, &policy));

    store
        .settle_cancel("alice", &plan.tickets[0], CancelOutcome::Cancelled)
        .unwrap();
    assert_invalid(store.validate_execution_permit("alice", &permit, &policy));
}

#[test]
fn lease_and_fixed_deadline_expiry_revoke_execution_permits() {
    let (_directory, clock, store) = store();
    let policy = digest('b');
    for (worker_id, run_id, timeout, lease) in [
        ("lease-worker", "lease-run", 600, 5),
        ("deadline-worker", "deadline-run", 5, 30),
    ] {
        store
            .create_root(
                "alice",
                &NewWorker {
                    id: worker_id.into(),
                    logical_session_id: None,
                },
                &new_run(run_id, worker_id, clock.now(), timeout),
            )
            .unwrap();
        let claimed = store
            .claim_due("alice", "supervisor", lease, 1)
            .unwrap()
            .remove(0);
        let started = store
            .start("alice", &claimed.receipt, &controller())
            .unwrap();
        let permit = store
            .issue_execution_permit("alice", &started.receipt, &policy)
            .unwrap();
        clock.advance(6);
        assert_invalid(store.validate_execution_permit("alice", &permit, &policy));
    }
}

#[test]
fn in_memory_permit_can_be_revalidated_after_store_restart() {
    let (directory, clock, store) = store();
    let path = directory.path().join("agent.sqlite");
    let policy = digest('b');
    store
        .create_root(
            "alice",
            &NewWorker {
                id: "worker".into(),
                logical_session_id: None,
            },
            &new_run("run", "worker", clock.now(), 600),
        )
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 30, 1)
        .unwrap()
        .remove(0);
    let started = store
        .start("alice", &claimed.receipt, &controller())
        .unwrap();
    let permit = store
        .issue_execution_permit("alice", &started.receipt, &policy)
        .unwrap();
    drop(store);

    let reopened = SqliteAgentStore::open_with_clock(path, clock).unwrap();
    let snapshot = reopened
        .validate_execution_permit("alice", &permit, &policy)
        .unwrap();
    assert_eq!(snapshot.run_revision(), started.run.revision);
    reopened.audit_integrity().unwrap();
}

#[test]
fn native_scope_construction_is_strict_and_debug_is_opaque() {
    let prompt = digest('a');
    let policy = digest('b');
    let native_session_id = "native-session-secret";
    let proof = ResumeSessionProof::derive("alice", "logical", native_session_id).unwrap();

    assert!(NativeExecutionScope::fresh("", &prompt, &policy, None).is_err());
    assert!(NativeExecutionScope::fresh("codex/default", "short", &policy, None).is_err());
    assert!(
        NativeExecutionScope::fresh("codex/default", &prompt, &policy, Some(String::new()),)
            .is_err()
    );
    assert!(
        NativeExecutionScope::resumed(
            "codex/default",
            &prompt,
            &policy,
            "different-logical",
            proof.clone(),
        )
        .is_err()
    );

    let scope =
        NativeExecutionScope::resumed("codex/default", &prompt, &policy, "logical", proof).unwrap();
    assert_eq!(scope.target_key(), "codex/default");
    assert_eq!(scope.prompt_digest(), prompt);
    assert_eq!(scope.policy_digest(), policy);
    assert_eq!(scope.logical_session_id(), Some("logical"));
    assert!(scope.resume_session_proof().is_some());
    let debug = format!("{scope:?}");
    assert!(debug.contains("[OPAQUE]"));
    assert!(!debug.contains(&prompt));
    assert!(!debug.contains(&policy));
    assert!(!debug.contains(native_session_id));
}

#[test]
fn native_permit_binds_target_prompt_policy_and_logical_session() {
    let (_directory, clock, store) = store();
    let prompt = digest('a');
    let policy = digest('b');
    store
        .create_root(
            "alice",
            &NewWorker {
                id: "worker".into(),
                logical_session_id: Some("logical".into()),
            },
            &new_run("run", "worker", clock.now(), 600),
        )
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 30, 1)
        .unwrap()
        .remove(0);
    let started = store
        .start("alice", &claimed.receipt, &controller())
        .unwrap();
    let permit = store
        .issue_execution_permit("alice", &started.receipt, &policy)
        .unwrap();
    let exact =
        NativeExecutionScope::fresh("codex/default", &prompt, &policy, Some("logical".into()))
            .unwrap();
    assert!(exact.resume_session_proof().is_none());
    store
        .validate_native_execution_permit("alice", &permit, &exact)
        .unwrap();

    for mismatched in [
        NativeExecutionScope::fresh("other/default", &prompt, &policy, Some("logical".into()))
            .unwrap(),
        NativeExecutionScope::fresh(
            "codex/default",
            digest('c'),
            &policy,
            Some("logical".into()),
        )
        .unwrap(),
        NativeExecutionScope::fresh(
            "codex/default",
            &prompt,
            digest('c'),
            Some("logical".into()),
        )
        .unwrap(),
        NativeExecutionScope::fresh(
            "codex/default",
            &prompt,
            &policy,
            Some("other-logical".into()),
        )
        .unwrap(),
    ] {
        assert_invalid(store.validate_native_execution_permit("alice", &permit, &mismatched));
    }

    clock.advance(5);
    let heartbeat = store.heartbeat("alice", &started.receipt, 30).unwrap();
    let after_heartbeat = store
        .validate_native_execution_permit("alice", &permit, &exact)
        .unwrap();
    assert_eq!(after_heartbeat.run_revision(), heartbeat.run.revision);
    clock.advance(1);
    let activity = store.record_activity("alice", &heartbeat.receipt).unwrap();
    let after_activity = store
        .validate_native_execution_permit("alice", &permit, &exact)
        .unwrap();
    assert_eq!(after_activity.run_revision(), activity.run.revision);
}

#[test]
fn native_permit_requires_the_exact_frozen_resume_binding() {
    let (_directory, clock, store) = store();
    let prompt = digest('a');
    let policy = digest('b');
    store
        .create_root(
            "alice",
            &NewWorker {
                id: "worker".into(),
                logical_session_id: Some("logical".into()),
            },
            &new_run("run", "worker", clock.now(), 600),
        )
        .unwrap();
    let claimed = store
        .claim_due("alice", "supervisor", 30, 1)
        .unwrap()
        .remove(0);
    let started = store
        .start("alice", &claimed.receipt, &controller())
        .unwrap();
    let permit = store
        .issue_execution_permit("alice", &started.receipt, &policy)
        .unwrap();
    let fresh =
        NativeExecutionScope::fresh("codex/default", &prompt, &policy, Some("logical".into()))
            .unwrap();
    store
        .validate_native_execution_permit("alice", &permit, &fresh)
        .unwrap();

    let exact_proof = ResumeSessionProof::derive("alice", "logical", "native-session").unwrap();
    store
        .bind_resume_session("alice", &started.receipt, &exact_proof)
        .unwrap();
    assert_invalid(store.validate_native_execution_permit("alice", &permit, &fresh));

    let exact =
        NativeExecutionScope::resumed("codex/default", &prompt, &policy, "logical", exact_proof)
            .unwrap();
    store
        .validate_native_execution_permit("alice", &permit, &exact)
        .unwrap();

    let wrong_proof =
        ResumeSessionProof::derive("alice", "logical", "different-native-session").unwrap();
    let wrong =
        NativeExecutionScope::resumed("codex/default", &prompt, &policy, "logical", wrong_proof)
            .unwrap();
    assert_invalid(store.validate_native_execution_permit("alice", &permit, &wrong));
}
