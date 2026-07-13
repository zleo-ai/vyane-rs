#![allow(clippy::unwrap_used)]

use std::fs;
use std::sync::{Arc, Barrier};

use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
use rusqlite::Connection;
use tempfile::TempDir;
use vyane_task::{
    ControllerRef, FailureCode, Lease, NewTask, SqliteTaskStore, TaskEventKind, TaskKind,
    TaskOrigin, TaskQuery, TaskSettlement, TaskState, TaskStore, TaskStoreError,
};

const OWNER: &str = "local";

const CHILD_MODE_ENV: &str = "VYANE_TASK_STORE_CONTRACT_CHILD";
const CHILD_DB_ENV: &str = "VYANE_TASK_STORE_CONTRACT_DB";
const CHILD_INSTANCE_ENV: &str = "VYANE_TASK_STORE_CONTRACT_INSTANCE";

fn timestamp(offset_seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0)
        .single()
        .unwrap()
        + TimeDelta::seconds(offset_seconds)
}

fn new_task(id: &str, offset_seconds: i64) -> NewTask {
    scoped_task(
        id,
        "local",
        TaskKind::Dispatch,
        TaskOrigin::RestAsync,
        offset_seconds,
    )
}

fn scoped_task(
    id: &str,
    _owner: &str,
    kind: TaskKind,
    origin: TaskOrigin,
    offset_seconds: i64,
) -> NewTask {
    NewTask {
        id: id.to_string(),
        kind,
        origin,
        task_digest: "0123456789abcdef".to_string(),
        target_key: "test/model".to_string(),
        created_at: timestamp(offset_seconds),
    }
}

fn in_process(instance_id: &str) -> ControllerRef {
    ControllerRef::InProcess {
        instance_id: instance_id.to_string(),
    }
}

fn lease(owner: &str, expires_offset: i64) -> Lease {
    Lease {
        owner: owner.to_string(),
        expires_at: timestamp(expires_offset),
    }
}

fn test_store() -> (TempDir, SqliteTaskStore) {
    let directory = TempDir::new().unwrap();
    let store = SqliteTaskStore::open(directory.path().join("tasks.sqlite3")).unwrap();
    (directory, store)
}

fn create_v1_database(path: &std::path::Path) {
    let connection = Connection::open(path).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .unwrap();
    connection
        .execute_batch(include_str!("../migrations/0001_tasks.sql"))
        .unwrap();
    connection.pragma_update(None, "user_version", 1).unwrap();
    drop(connection);
    make_database_owner_only(path);
}

#[test]
fn create_attach_and_reopen_preserve_snapshot_and_events() {
    let (directory, store) = test_store();
    let queued = store.create(OWNER, new_task("reopen", 0)).unwrap();
    assert_eq!(queued.state, TaskState::Queued);
    assert_eq!(queued.revision, 0);
    assert_eq!(queued.executor_epoch, 0);
    assert!(queued.controller.is_none());

    let running = store
        .attach_controller(
            OWNER,
            &queued.id,
            queued.revision,
            queued.executor_epoch,
            in_process("server-a"),
            Some(lease("server-a", 30)),
            timestamp(1),
        )
        .unwrap();
    assert_eq!(running.state, TaskState::Running);
    assert_eq!(running.revision, 1);
    assert_eq!(running.executor_epoch, 1);
    assert_eq!(running.started_at, Some(timestamp(1)));

    let settled = store
        .settle(
            OWNER,
            &running.id,
            running.revision,
            running.executor_epoch,
            TaskSettlement::Succeeded {
                ledger_run_id: Some("run-reopen".into()),
            },
            timestamp(2),
        )
        .unwrap();
    assert_eq!(settled.state, TaskState::Succeeded);
    assert_eq!(settled.revision, 2);
    drop(store);

    let reopened = SqliteTaskStore::open(directory.path().join("tasks.sqlite3")).unwrap();
    assert_eq!(reopened.get(OWNER, "reopen").unwrap(), Some(settled));
    let page = reopened.list(OWNER, &TaskQuery::default()).unwrap();
    assert_eq!(page.items.len(), 1);
    let events = reopened.events(OWNER, "reopen").unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].kind, TaskEventKind::Created);
    assert_eq!(events[1].kind, TaskEventKind::ControllerAttached);
    assert_eq!(events[2].kind, TaskEventKind::Settled);
    assert_eq!(events[2].revision, page.items[0].revision);
}

#[test]
fn illegal_transitions_are_atomic_and_terminal_settlement_is_exactly_idempotent() {
    let (_directory, store) = test_store();
    let queued = store.create(OWNER, new_task("states", 0)).unwrap();

    let illegal = store.settle(
        OWNER,
        &queued.id,
        queued.revision,
        queued.executor_epoch,
        TaskSettlement::Succeeded {
            ledger_run_id: Some("too-early".into()),
        },
        timestamp(1),
    );
    assert!(matches!(illegal, Err(TaskStoreError::InvalidState { .. })));
    assert_eq!(store.get(OWNER, "states").unwrap(), Some(queued.clone()));
    assert_eq!(store.events(OWNER, "states").unwrap().len(), 1);

    let running = store
        .attach_controller(
            OWNER,
            "states",
            queued.revision,
            queued.executor_epoch,
            in_process("server-a"),
            None,
            timestamp(2),
        )
        .unwrap();
    let settlement = TaskSettlement::Succeeded {
        ledger_run_id: Some("run-states".into()),
    };
    let terminal = store
        .settle(
            OWNER,
            "states",
            running.revision,
            running.executor_epoch,
            settlement.clone(),
            timestamp(3),
        )
        .unwrap();

    // Retry with the pre-settlement revision models a successful commit whose
    // response was lost before the caller observed the terminal snapshot.
    let replay = store
        .settle(
            OWNER,
            "states",
            running.revision,
            terminal.executor_epoch,
            settlement,
            timestamp(4),
        )
        .unwrap();
    assert_eq!(replay, terminal);
    assert_eq!(store.events(OWNER, "states").unwrap().len(), 3);

    let conflicting = store.settle(
        OWNER,
        "states",
        terminal.revision,
        terminal.executor_epoch,
        TaskSettlement::Failed {
            code: FailureCode::DispatchFailed,
            ledger_run_id: Some("run-states".into()),
        },
        timestamp(4),
    );
    assert!(matches!(
        conflicting,
        Err(TaskStoreError::InvalidState { .. })
    ));
    assert_eq!(store.get(OWNER, "states").unwrap(), Some(terminal));
    assert_eq!(store.events(OWNER, "states").unwrap().len(), 3);
}

#[test]
fn concurrent_identical_settlements_are_exactly_idempotent_inside_writer_lock() {
    let (_directory, store) = test_store();
    let queued = store.create(OWNER, new_task("settle-race", 0)).unwrap();
    let running = store
        .attach_controller(
            OWNER,
            "settle-race",
            queued.revision,
            queued.executor_epoch,
            in_process("worker"),
            None,
            timestamp(1),
        )
        .unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let handles = (0..2)
        .map(|_| {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            let running = running.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store.settle(
                    OWNER,
                    "settle-race",
                    running.revision,
                    running.executor_epoch,
                    TaskSettlement::Succeeded {
                        ledger_run_id: Some("run-settle-race".into()),
                    },
                    timestamp(2),
                )
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results[0], results[1]);
    assert_eq!(results[0].state, TaskState::Succeeded);
    assert_eq!(store.events(OWNER, "settle-race").unwrap().len(), 3);
}

#[test]
fn every_write_checks_revision_and_executor_epoch() {
    let (_directory, store) = test_store();
    let queued = store.create(OWNER, new_task("cas", 0)).unwrap();

    let wrong_revision = store.attach_controller(
        OWNER,
        "cas",
        queued.revision + 1,
        queued.executor_epoch,
        in_process("server-a"),
        None,
        timestamp(1),
    );
    assert!(matches!(
        wrong_revision,
        Err(TaskStoreError::Conflict { .. })
    ));

    let wrong_epoch = store.attach_controller(
        OWNER,
        "cas",
        queued.revision,
        queued.executor_epoch + 1,
        in_process("server-a"),
        None,
        timestamp(1),
    );
    assert!(matches!(wrong_epoch, Err(TaskStoreError::Conflict { .. })));
    assert_eq!(store.get(OWNER, "cas").unwrap(), Some(queued));
    assert_eq!(store.events(OWNER, "cas").unwrap().len(), 1);
}

#[test]
fn cancel_and_settle_race_has_one_cas_winner_then_authoritative_settlement() {
    let (_directory, store) = test_store();
    let queued = store.create(OWNER, new_task("race", 0)).unwrap();
    let running = store
        .attach_controller(
            OWNER,
            "race",
            queued.revision,
            queued.executor_epoch,
            in_process("server-a"),
            None,
            timestamp(1),
        )
        .unwrap();
    let race_revision = running.revision;
    let race_epoch = running.executor_epoch;

    let barrier = Arc::new(Barrier::new(3));
    let cancel_store = store.clone();
    let cancel_barrier = Arc::clone(&barrier);
    let cancel = std::thread::spawn(move || {
        cancel_barrier.wait();
        cancel_store.request_cancel(OWNER, "race", race_revision, race_epoch, timestamp(2))
    });
    let settle_store = store.clone();
    let settle_barrier = Arc::clone(&barrier);
    let settle = std::thread::spawn(move || {
        settle_barrier.wait();
        settle_store.settle(
            OWNER,
            "race",
            race_revision,
            race_epoch,
            TaskSettlement::Succeeded {
                ledger_run_id: Some("run-race".into()),
            },
            timestamp(2),
        )
    });
    barrier.wait();

    let cancel_result = cancel.join().unwrap();
    let settle_result = settle.join().unwrap();
    assert_ne!(cancel_result.is_ok(), settle_result.is_ok());
    assert!(cancel_result.is_ok() || matches!(cancel_result, Err(TaskStoreError::Conflict { .. })));
    assert!(settle_result.is_ok() || matches!(settle_result, Err(TaskStoreError::Conflict { .. })));

    let current = store.get(OWNER, "race").unwrap().unwrap();
    let terminal = if current.state == TaskState::Cancelling {
        store
            .settle(
                OWNER,
                "race",
                current.revision,
                current.executor_epoch,
                TaskSettlement::Succeeded {
                    ledger_run_id: Some("run-race".into()),
                },
                timestamp(3),
            )
            .unwrap()
    } else {
        current
    };
    assert_eq!(terminal.state, TaskState::Succeeded);
    assert_eq!(terminal.ledger_run_id.as_deref(), Some("run-race"));
    let events = store.events(OWNER, "race").unwrap();
    assert_eq!(events.last().unwrap().revision, terminal.revision);
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].revision < pair[1].revision)
    );
}

#[test]
fn snapshot_update_rolls_back_when_event_append_fails() {
    let (directory, store) = test_store();
    let queued = store.create(OWNER, new_task("atomic", 0)).unwrap();
    let connection = Connection::open(directory.path().join("tasks.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_controller_event \
             BEFORE INSERT ON task_events \
             WHEN NEW.kind = 'controller_attached' \
             BEGIN SELECT RAISE(ABORT, 'injected event failure'); END;",
        )
        .unwrap();

    let result = store.attach_controller(
        OWNER,
        "atomic",
        queued.revision,
        queued.executor_epoch,
        in_process("server-a"),
        None,
        timestamp(1),
    );
    assert!(matches!(result, Err(TaskStoreError::Sqlite(_))));
    assert_eq!(store.get(OWNER, "atomic").unwrap(), Some(queued));
    assert_eq!(store.events(OWNER, "atomic").unwrap().len(), 1);
}

#[test]
fn lease_claim_requires_expiry_and_invalidates_the_old_executor_epoch() {
    let (_directory, store) = test_store();
    let queued = store.create(OWNER, new_task("lease", 0)).unwrap();
    let running = store
        .attach_controller(
            OWNER,
            "lease",
            queued.revision,
            queued.executor_epoch,
            in_process("server-a"),
            Some(lease("server-a", 10)),
            timestamp(1),
        )
        .unwrap();

    let early_claim = store.claim_expired(
        OWNER,
        "lease",
        running.revision,
        running.executor_epoch,
        in_process("server-b"),
        lease("server-b", 20),
        timestamp(5),
    );
    assert!(matches!(
        early_claim,
        Err(TaskStoreError::LeaseNotExpired { .. })
    ));
    assert_eq!(store.get(OWNER, "lease").unwrap(), Some(running.clone()));

    let shortening = store.renew_lease(
        OWNER,
        "lease",
        running.revision,
        running.executor_epoch,
        "server-a",
        timestamp(9),
        timestamp(5),
    );
    assert!(matches!(shortening, Err(TaskStoreError::InvalidInput(_))));
    assert_eq!(store.get(OWNER, "lease").unwrap(), Some(running.clone()));

    let renewed = store
        .renew_lease(
            OWNER,
            "lease",
            running.revision,
            running.executor_epoch,
            "server-a",
            timestamp(15),
            timestamp(5),
        )
        .unwrap();
    assert_eq!(renewed.revision, 2);
    assert_eq!(renewed.executor_epoch, 1);

    let claimed = store
        .claim_expired(
            OWNER,
            "lease",
            renewed.revision,
            renewed.executor_epoch,
            in_process("server-b"),
            lease("server-b", 30),
            timestamp(16),
        )
        .unwrap();
    assert_eq!(claimed.revision, 3);
    assert_eq!(claimed.executor_epoch, 2);
    assert_eq!(claimed.lease.as_ref().unwrap().owner, "server-b");

    let stale_settle = store.settle(
        OWNER,
        "lease",
        claimed.revision,
        running.executor_epoch,
        TaskSettlement::Succeeded {
            ledger_run_id: Some("stale".into()),
        },
        timestamp(17),
    );
    assert!(matches!(stale_settle, Err(TaskStoreError::Conflict { .. })));
    assert_eq!(store.get(OWNER, "lease").unwrap(), Some(claimed));
}

#[test]
fn queued_tasks_can_be_discovered_and_interrupted_for_startup_recovery() {
    let (_directory, store) = test_store();
    let queued = store
        .create(OWNER, new_task("queued-crash-window", 0))
        .unwrap();
    let page = store
        .list(
            OWNER,
            &TaskQuery {
                states: vec![TaskState::Queued],
                ..TaskQuery::default()
            },
        )
        .unwrap();
    assert_eq!(page.items, vec![queued.clone()]);

    let interrupted = store
        .interrupt(
            OWNER,
            &queued.id,
            queued.revision,
            queued.executor_epoch,
            FailureCode::WorkerLost,
            timestamp(1),
        )
        .unwrap();
    assert_eq!(interrupted.state, TaskState::Interrupted);
    assert_eq!(interrupted.failure_code, Some(FailureCode::WorkerLost));
}

#[test]
fn list_uses_stable_descending_cursor_and_filters() {
    let (_directory, store) = test_store();
    store.create(OWNER, new_task("a", 1)).unwrap();
    store.create(OWNER, new_task("b", 2)).unwrap();
    store.create(OWNER, new_task("c", 2)).unwrap();
    store.create(OWNER, new_task("d", 3)).unwrap();

    let first = store
        .list(
            OWNER,
            &TaskQuery {
                limit: 2,
                ..TaskQuery::default()
            },
        )
        .unwrap();
    assert_eq!(
        first
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        vec!["d", "c"]
    );
    assert!(first.next_cursor.is_some());

    let second = store
        .list(
            OWNER,
            &TaskQuery {
                limit: 2,
                cursor: first.next_cursor,
                ..TaskQuery::default()
            },
        )
        .unwrap();
    assert_eq!(
        second
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        vec!["b", "a"]
    );
    assert!(second.next_cursor.is_none());

    let filtered = store
        .list(
            "nobody",
            &TaskQuery {
                ..TaskQuery::default()
            },
        )
        .unwrap();
    assert!(filtered.items.is_empty());
}

#[test]
fn daemon_origin_roundtrips_and_scope_matching_requires_all_dimensions() {
    let (directory, store) = test_store();
    let record = store
        .create(
            OWNER,
            scoped_task(
                "daemon-roundtrip",
                "local",
                TaskKind::Workflow,
                TaskOrigin::Daemon,
                0,
            ),
        )
        .unwrap();

    assert!(record.matches_scope("local", TaskKind::Workflow, TaskOrigin::Daemon));
    assert!(!record.matches_scope("other", TaskKind::Workflow, TaskOrigin::Daemon));
    assert!(!record.matches_scope("local", TaskKind::Dispatch, TaskOrigin::Daemon));
    assert!(!record.matches_scope("local", TaskKind::Workflow, TaskOrigin::RestAsync));
    assert_eq!(store.get(OWNER, &record.id).unwrap(), Some(record.clone()));
    drop(store);

    let reopened = SqliteTaskStore::open(directory.path().join("tasks.sqlite3")).unwrap();
    assert_eq!(reopened.get(OWNER, &record.id).unwrap(), Some(record));
}

#[test]
fn list_filters_kind_and_origin_individually_and_in_combination_across_pages() {
    let (_directory, store) = test_store();
    for task in [
        scoped_task(
            "daemon-dispatch",
            "local",
            TaskKind::Dispatch,
            TaskOrigin::Daemon,
            7,
        ),
        scoped_task(
            "other-daemon-workflow",
            "other",
            TaskKind::Workflow,
            TaskOrigin::Daemon,
            6,
        ),
        scoped_task(
            "rest-dispatch",
            "local",
            TaskKind::Dispatch,
            TaskOrigin::RestAsync,
            5,
        ),
        scoped_task(
            "daemon-workflow-c",
            "local",
            TaskKind::Workflow,
            TaskOrigin::Daemon,
            4,
        ),
        scoped_task(
            "daemon-workflow-b",
            "local",
            TaskKind::Workflow,
            TaskOrigin::Daemon,
            3,
        ),
        scoped_task(
            "daemon-workflow-a",
            "local",
            TaskKind::Workflow,
            TaskOrigin::Daemon,
            2,
        ),
    ] {
        let owner = if task.id == "other-daemon-workflow" {
            "other"
        } else {
            OWNER
        };
        store.create(owner, task).unwrap();
    }

    let workflow_only = store
        .list(
            OWNER,
            &TaskQuery {
                kinds: vec![TaskKind::Workflow],
                ..TaskQuery::default()
            },
        )
        .unwrap();
    assert_eq!(
        workflow_only
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        vec![
            "daemon-workflow-c",
            "daemon-workflow-b",
            "daemon-workflow-a"
        ]
    );

    let daemon_only = store
        .list(
            OWNER,
            &TaskQuery {
                origins: vec![TaskOrigin::Daemon],
                ..TaskQuery::default()
            },
        )
        .unwrap();
    assert_eq!(
        daemon_only
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        vec![
            "daemon-dispatch",
            "daemon-workflow-c",
            "daemon-workflow-b",
            "daemon-workflow-a"
        ]
    );

    let first = store
        .list(
            OWNER,
            &TaskQuery {
                kinds: vec![TaskKind::Workflow],
                origins: vec![TaskOrigin::Daemon],
                limit: 2,
                ..TaskQuery::default()
            },
        )
        .unwrap();
    assert_eq!(
        first
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        vec!["daemon-workflow-c", "daemon-workflow-b"]
    );
    assert!(first.next_cursor.is_some());

    let second = store
        .list(
            OWNER,
            &TaskQuery {
                kinds: vec![TaskKind::Workflow],
                origins: vec![TaskOrigin::Daemon],
                limit: 2,
                cursor: first.next_cursor,
                ..TaskQuery::default()
            },
        )
        .unwrap();
    assert_eq!(
        second
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        vec!["daemon-workflow-a"]
    );
    assert!(second.next_cursor.is_none());
}

#[test]
fn identical_task_ids_are_isolated_by_owner_across_snapshots_events_and_cas() {
    let (_directory, store) = test_store();
    let alpha = store.create("alpha", new_task("shared", 0)).unwrap();
    let beta = store.create("beta", new_task("shared", 1)).unwrap();

    let alpha = store
        .attach_controller(
            "alpha",
            "shared",
            alpha.revision,
            alpha.executor_epoch,
            in_process("alpha-worker"),
            Some(lease("alpha-lease", 5)),
            timestamp(2),
        )
        .unwrap();
    let beta = store
        .attach_controller(
            "beta",
            "shared",
            beta.revision,
            beta.executor_epoch,
            in_process("beta-worker"),
            Some(lease("beta-lease", 20)),
            timestamp(3),
        )
        .unwrap();
    let alpha = store
        .request_cancel(
            "alpha",
            "shared",
            alpha.revision,
            alpha.executor_epoch,
            timestamp(4),
        )
        .unwrap();
    let alpha = store
        .claim_expired(
            "alpha",
            "shared",
            alpha.revision,
            alpha.executor_epoch,
            in_process("alpha-recovery"),
            lease("alpha-recovery-lease", 30),
            timestamp(10),
        )
        .unwrap();
    let beta_before_renew = beta.clone();
    let beta = store
        .renew_lease(
            "beta",
            "shared",
            beta.revision,
            beta.executor_epoch,
            "beta-lease",
            timestamp(30),
            timestamp(4),
        )
        .unwrap();

    assert_eq!(store.get("alpha", "shared").unwrap(), Some(alpha.clone()));
    assert_eq!(store.get("beta", "shared").unwrap(), Some(beta.clone()));
    assert_eq!(alpha.revision, 3);
    assert_eq!(beta.revision, 2);
    assert_eq!(alpha.executor_epoch, 2);
    assert_eq!(beta.executor_epoch, 1);
    assert_eq!(alpha.lease.as_ref().unwrap().owner, "alpha-recovery-lease");
    assert_eq!(beta.lease.as_ref().unwrap().owner, "beta-lease");
    assert_eq!(beta_before_renew.executor_epoch, beta.executor_epoch);
    assert_eq!(store.events("alpha", "shared").unwrap().len(), 4);
    assert_eq!(store.events("beta", "shared").unwrap().len(), 3);
    assert!(
        store
            .events("alpha", "shared")
            .unwrap()
            .iter()
            .all(|event| event.owner == "alpha")
    );
    assert_eq!(
        store.list("alpha", &TaskQuery::default()).unwrap().items,
        vec![alpha]
    );
    assert_eq!(
        store.list("beta", &TaskQuery::default()).unwrap().items,
        vec![beta]
    );
}

#[test]
fn foreign_owner_mutations_are_not_found_and_never_change_snapshot_or_events() {
    let (_directory, store) = test_store();
    let queued = store.create("beta", new_task("protected", 0)).unwrap();
    let running = store
        .attach_controller(
            "beta",
            "protected",
            queued.revision,
            queued.executor_epoch,
            in_process("beta-worker"),
            Some(lease("beta-lease", 5)),
            timestamp(1),
        )
        .unwrap();
    let baseline_events = store.events("beta", "protected").unwrap();

    let errors = [
        store.attach_controller(
            "alpha",
            "protected",
            running.revision,
            running.executor_epoch,
            in_process("alpha-worker"),
            None,
            timestamp(10),
        ),
        store.request_cancel(
            "alpha",
            "protected",
            running.revision,
            running.executor_epoch,
            timestamp(10),
        ),
        store.settle(
            "alpha",
            "protected",
            running.revision,
            running.executor_epoch,
            TaskSettlement::Succeeded {
                ledger_run_id: None,
            },
            timestamp(10),
        ),
        store.interrupt(
            "alpha",
            "protected",
            running.revision,
            running.executor_epoch,
            FailureCode::WorkerLost,
            timestamp(10),
        ),
        store.claim_expired(
            "alpha",
            "protected",
            running.revision,
            running.executor_epoch,
            in_process("alpha-worker"),
            lease("alpha-lease", 20),
            timestamp(10),
        ),
        store.renew_lease(
            "alpha",
            "protected",
            running.revision,
            running.executor_epoch,
            "beta-lease",
            timestamp(20),
            timestamp(2),
        ),
    ];
    assert!(errors.into_iter().all(|result| matches!(
        result,
        Err(TaskStoreError::NotFound { ref id }) if id == "protected"
    )));
    assert_eq!(store.get("beta", "protected").unwrap(), Some(running));
    assert_eq!(store.events("beta", "protected").unwrap(), baseline_events);
    assert!(matches!(
        store.events("alpha", "protected"),
        Err(TaskStoreError::NotFound { .. })
    ));
}

#[test]
fn owner_scoped_pagination_and_recovery_filters_never_cross_tenants() {
    let (_directory, store) = test_store();
    for index in 0..5 {
        store
            .create("alpha", new_task(&format!("alpha-{index}"), index))
            .unwrap();
        store
            .create("beta", new_task(&format!("beta-{index}"), index))
            .unwrap();
    }
    let query = TaskQuery {
        states: vec![TaskState::Queued],
        limit: 2,
        ..TaskQuery::default()
    };
    let first = store.list("alpha", &query).unwrap();
    let second = store
        .list(
            "alpha",
            &TaskQuery {
                cursor: first.next_cursor.clone(),
                ..query.clone()
            },
        )
        .unwrap();
    let third = store
        .list(
            "alpha",
            &TaskQuery {
                cursor: second.next_cursor.clone(),
                ..query
            },
        )
        .unwrap();
    let all = first
        .items
        .into_iter()
        .chain(second.items)
        .chain(third.items)
        .collect::<Vec<_>>();
    assert_eq!(all.len(), 5);
    assert!(all.iter().all(|record| record.owner == "alpha"));
    assert!(all.iter().all(|record| record.state == TaskState::Queued));
}

#[test]
fn sqlite_rejects_cross_owner_event_foreign_keys() {
    let (directory, store) = test_store();
    store.create("alpha", new_task("fk-bound", 0)).unwrap();
    let connection = Connection::open(directory.path().join("tasks.sqlite3")).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .unwrap();
    let error = connection
        .execute(
            "INSERT INTO task_events (owner, task_id, revision, occurred_at_ms, kind, \
             from_state, to_state, actor_instance, executor_epoch) \
             VALUES ('beta', 'fk-bound', 1, 0, 'cancel_requested', 'queued', \
                     'cancelled', NULL, 0)",
            [],
        )
        .unwrap_err();
    assert!(matches!(error, rusqlite::Error::SqliteFailure(_, _)));
    assert_eq!(store.events("alpha", "fk-bound").unwrap().len(), 1);
}

#[test]
fn query_rejects_more_kind_or_origin_filters_than_the_canonical_sets() {
    let (_directory, store) = test_store();
    let too_many_kinds = store.list(
        OWNER,
        &TaskQuery {
            kinds: vec![TaskKind::Dispatch, TaskKind::Workflow, TaskKind::Dispatch],
            ..TaskQuery::default()
        },
    );
    assert!(matches!(
        too_many_kinds,
        Err(TaskStoreError::InvalidInput(_))
    ));

    let too_many_origins = store.list(
        OWNER,
        &TaskQuery {
            origins: vec![
                TaskOrigin::RestAsync,
                TaskOrigin::CliDetached,
                TaskOrigin::Daemon,
                TaskOrigin::RestAsync,
            ],
            ..TaskQuery::default()
        },
    );
    assert!(matches!(
        too_many_origins,
        Err(TaskStoreError::InvalidInput(_))
    ));
}

#[test]
fn process_group_controller_roundtrips_without_runtime_handles() {
    let (_directory, store) = test_store();
    let queued = store.create(OWNER, new_task("process", 0)).unwrap();
    let controller = ControllerRef::ProcessGroup {
        pid: 1234,
        pgid: 1234,
        started_at: timestamp(1),
        birth_fingerprint: Some("boot-id:start-ticks".into()),
    };
    let running = store
        .attach_controller(
            OWNER,
            "process",
            queued.revision,
            queued.executor_epoch,
            controller.clone(),
            None,
            timestamp(1),
        )
        .unwrap();
    assert_eq!(running.controller, Some(controller));
    assert_eq!(store.get(OWNER, "process").unwrap(), Some(running));
}

#[test]
fn task_database_and_wal_never_receive_prompt_or_secret_canaries() {
    const PROMPT_CANARY: &str = "PROMPT-CANARY-must-never-reach-task-storage";
    const SECRET_CANARY: &str = "SECRET-CANARY-must-never-reach-task-storage";
    const OUTPUT_CANARY: &str = "OUTPUT-CANARY-must-never-reach-task-storage";
    const RAW_ERROR_CANARY: &str = "RAW-ERROR-CANARY-must-never-reach-task-storage";

    let (directory, store) = test_store();
    let path = directory.path().join("tasks.sqlite3");
    // Keep a connection open so SQLite retains a WAL file while the store uses
    // its own short-lived writer connections.
    let keeper = Connection::open(&path).unwrap();
    keeper.pragma_update(None, "journal_mode", "WAL").unwrap();

    let columns = {
        let mut statement = keeper.prepare("PRAGMA table_info(tasks)").unwrap();
        statement
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    for forbidden in [
        "prompt",
        "system",
        "secret",
        "output",
        "raw_error",
        "error_message",
        "labels",
        "endpoint",
        "credential",
    ] {
        assert!(!columns.iter().any(|column| column == forbidden));
    }

    for (index, canary) in [
        PROMPT_CANARY,
        SECRET_CANARY,
        OUTPUT_CANARY,
        RAW_ERROR_CANARY,
    ]
    .into_iter()
    .enumerate()
    {
        let mut invalid = new_task(&format!("private-content-{index}"), 0);
        invalid.task_digest = canary.to_string();
        assert!(matches!(
            store.create(OWNER, invalid),
            Err(TaskStoreError::InvalidInput(_))
        ));
    }

    let queued = store.create(OWNER, new_task("privacy", 0)).unwrap();
    store
        .attach_controller(
            OWNER,
            "privacy",
            queued.revision,
            queued.executor_epoch,
            in_process("server-a"),
            None,
            timestamp(1),
        )
        .unwrap();

    for candidate in [path.clone(), append_suffix(&path, "-wal")] {
        if let Ok(bytes) = fs::read(&candidate) {
            assert!(!contains_bytes(&bytes, PROMPT_CANARY.as_bytes()));
            assert!(!contains_bytes(&bytes, SECRET_CANARY.as_bytes()));
            assert!(!contains_bytes(&bytes, OUTPUT_CANARY.as_bytes()));
            assert!(!contains_bytes(&bytes, RAW_ERROR_CANARY.as_bytes()));
        }
    }

    drop(keeper);
}

#[test]
fn newer_user_version_is_rejected_without_downgrade() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("future.sqlite3");
    let connection = Connection::open(&path).unwrap();
    connection.pragma_update(None, "user_version", 3).unwrap();
    drop(connection);
    make_database_owner_only(&path);

    let error = SqliteTaskStore::open(&path).unwrap_err();
    assert!(matches!(
        error,
        TaskStoreError::UnsupportedSchema {
            found: 3,
            supported: 2
        }
    ));
    let connection = Connection::open(&path).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 3);
}

#[test]
fn v1_migration_preserves_rich_rows_events_and_autoincrement_high_water() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("v1-rich.sqlite3");
    create_v1_database(&path);
    let connection = Connection::open(&path).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .unwrap();
    connection
        .execute_batch(
            "INSERT INTO tasks (
                id, record_schema, owner, kind, origin, state, task_digest, target_key,
                created_at_ms, started_at_ms, updated_at_ms, finished_at_ms, revision,
                executor_epoch, controller_kind, controller_instance_id, controller_pid,
                controller_pgid, controller_started_at_ms, controller_birth_fingerprint,
                lease_owner, lease_expires_at_ms, ledger_run_id, failure_code
             ) VALUES (
                'shared', 1, 'alpha', 'dispatch', 'rest_async', 'running',
                '0123456789abcdef', 'test/model', 0, 1000, 1000, NULL, 1, 1,
                'process_group', NULL, 123, 123, 500, 'birth-123',
                'lease-alpha', 20000, NULL, NULL
             );
             INSERT INTO task_events (
                sequence, task_id, revision, occurred_at_ms, kind, from_state, to_state,
                actor_instance, executor_epoch
             ) VALUES
                (10, 'shared', 0, 0, 'created', NULL, 'queued', NULL, 0),
                (20, 'shared', 1, 1000, 'controller_attached', 'queued', 'running',
                 'process:123', 1);
             INSERT INTO tasks (
                id, record_schema, owner, kind, origin, state, task_digest, target_key,
                created_at_ms, updated_at_ms, revision, executor_epoch
             ) VALUES (
                'deleted-tail', 1, 'beta', 'dispatch', 'rest_async', 'queued',
                'fedcba9876543210', 'test/model', 0, 0, 0, 0
             );
             INSERT INTO task_events (
                sequence, task_id, revision, occurred_at_ms, kind, from_state, to_state,
                actor_instance, executor_epoch
             ) VALUES (100, 'deleted-tail', 0, 0, 'created', NULL, 'queued', NULL, 0);
             DELETE FROM tasks WHERE id = 'deleted-tail';",
        )
        .unwrap();
    let old_high_water: i64 = connection
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name = 'task_events'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(old_high_water, 100);
    drop(connection);

    let store = SqliteTaskStore::open(&path).unwrap();
    let migrated = store.get("alpha", "shared").unwrap().unwrap();
    assert_eq!(migrated.owner, "alpha");
    assert_eq!(migrated.state, TaskState::Running);
    assert_eq!(migrated.revision, 1);
    assert_eq!(migrated.executor_epoch, 1);
    assert_eq!(
        migrated.controller,
        Some(ControllerRef::ProcessGroup {
            pid: 123,
            pgid: 123,
            started_at: DateTime::from_timestamp_millis(500).unwrap(),
            birth_fingerprint: Some("birth-123".into()),
        })
    );
    assert_eq!(
        migrated.lease,
        Some(Lease {
            owner: "lease-alpha".into(),
            expires_at: DateTime::from_timestamp_millis(20_000).unwrap(),
        })
    );
    let migrated_events = store.events("alpha", "shared").unwrap();
    assert_eq!(
        migrated_events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![10, 20]
    );
    assert!(migrated_events.iter().all(|event| event.owner == "alpha"));

    let duplicate_id = store.create("beta", new_task("shared", 2)).unwrap();
    assert_eq!(duplicate_id.owner, "beta");
    let renewed = store
        .renew_lease(
            "alpha",
            "shared",
            migrated.revision,
            migrated.executor_epoch,
            "lease-alpha",
            DateTime::from_timestamp_millis(30_000).unwrap(),
            DateTime::from_timestamp_millis(2_000).unwrap(),
        )
        .unwrap();
    assert_eq!(renewed.revision, 2);
    let newest = store.events("alpha", "shared").unwrap().pop().unwrap();
    assert!(newest.sequence > u64::try_from(old_high_water).unwrap());

    drop(store);
    let reopened = SqliteTaskStore::open(&path).unwrap();
    assert!(reopened.get("alpha", "shared").unwrap().is_some());
    assert!(reopened.get("beta", "shared").unwrap().is_some());
}

#[test]
fn failed_v1_migration_rolls_back_version_objects_and_rows() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("v1-rollback.sqlite3");
    create_v1_database(&path);
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "PRAGMA ignore_check_constraints = ON;
             INSERT INTO tasks (
                id, record_schema, owner, kind, origin, state, task_digest, target_key,
                created_at_ms, updated_at_ms, revision, executor_epoch
             ) VALUES (
                'invalid-revision', 1, 'alpha', 'dispatch', 'rest_async', 'queued',
                '0123456789abcdef', 'test/model', 0, 0, -1, 0
             );
             INSERT INTO task_events (
                sequence, task_id, revision, occurred_at_ms, kind, from_state, to_state,
                actor_instance, executor_epoch
             ) VALUES (7, 'invalid-revision', -1, 0, 'created', NULL, 'queued', NULL, 0);
             PRAGMA ignore_check_constraints = OFF;",
        )
        .unwrap();
    drop(connection);

    assert!(matches!(
        SqliteTaskStore::open(&path),
        Err(TaskStoreError::CorruptData(_))
    ));
    let connection = Connection::open(&path).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 1);
    let preserved_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM tasks WHERE id = 'invalid-revision' AND revision = -1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(preserved_count, 1);
    let owner_column_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('task_events') WHERE name = 'owner'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(owner_column_count, 0);
    let temporary_tables: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name IN \
             ('tasks_v1', 'task_events_v1')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(temporary_tables, 0);
    let v1_indexes: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'index' AND name IN (
                'tasks_owner_created_idx', 'tasks_state_lease_idx',
                'tasks_ledger_run_idx', 'task_events_task_idx'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(v1_indexes, 4);
    let v2_only_indexes: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'index' AND name IN (
                'tasks_owner_state_lease_idx', 'tasks_owner_ledger_run_idx',
                'task_events_owner_task_idx'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(v2_only_indexes, 0);
}

#[test]
fn concurrent_v1_open_migrates_once_and_both_openers_observe_v2() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("v1-concurrent.sqlite3");
    create_v1_database(&path);
    let barrier = Arc::new(Barrier::new(3));
    let handles = (0..2)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let path = path.clone();
            std::thread::spawn(move || {
                barrier.wait();
                SqliteTaskStore::open(path)
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    for handle in handles {
        let result = handle.join().unwrap();
        assert!(result.is_ok(), "concurrent open failed: {result:?}");
    }
    let connection = Connection::open(&path).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 2);
}

#[test]
fn unversioned_database_with_user_objects_fails_closed() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("unversioned-objects.sqlite3");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch("CREATE TABLE unrelated(value TEXT);")
        .unwrap();
    drop(connection);
    make_database_owner_only(&path);
    assert!(matches!(
        SqliteTaskStore::open(path),
        Err(TaskStoreError::CorruptData(_))
    ));
}

#[test]
fn timestamps_are_normalized_to_milliseconds_before_return_and_reopen() {
    let (directory, store) = test_store();
    let created_at = timestamp(0) + TimeDelta::nanoseconds(123_456);
    let operation_at = timestamp(1) + TimeDelta::nanoseconds(654_321);
    let process_started_at = timestamp(1) + TimeDelta::nanoseconds(222_333);
    let lease_expires_at = timestamp(30) + TimeDelta::nanoseconds(987_654);
    assert_ne!(created_at.timestamp_subsec_nanos() % 1_000_000, 0);

    let mut task = new_task("nanoseconds", 0);
    task.created_at = created_at;
    let queued = store.create(OWNER, task).unwrap();
    assert_millisecond_precision(queued.created_at);
    assert_eq!(
        store.get(OWNER, "nanoseconds").unwrap(),
        Some(queued.clone())
    );

    let running = store
        .attach_controller(
            OWNER,
            "nanoseconds",
            queued.revision,
            queued.executor_epoch,
            ControllerRef::ProcessGroup {
                pid: 123,
                pgid: 123,
                started_at: process_started_at,
                birth_fingerprint: None,
            },
            Some(Lease {
                owner: "worker-a".into(),
                expires_at: lease_expires_at,
            }),
            operation_at,
        )
        .unwrap();
    assert_millisecond_precision(running.updated_at);
    assert_millisecond_precision(running.started_at.unwrap());
    let ControllerRef::ProcessGroup { started_at, .. } = running.controller.as_ref().unwrap()
    else {
        panic!("expected process-group controller");
    };
    assert_millisecond_precision(*started_at);
    assert_millisecond_precision(running.lease.as_ref().unwrap().expires_at);
    assert_eq!(
        store.get(OWNER, "nanoseconds").unwrap(),
        Some(running.clone())
    );
    drop(store);

    let reopened = SqliteTaskStore::open(directory.path().join("tasks.sqlite3")).unwrap();
    assert_eq!(reopened.get(OWNER, "nanoseconds").unwrap(), Some(running));
}

#[test]
fn backward_wall_clock_is_clamped_and_lease_checks_use_effective_time() {
    let (_directory, store) = test_store();

    let queued = store.create(OWNER, new_task("clock-settle", 10)).unwrap();
    let running = store
        .attach_controller(
            OWNER,
            "clock-settle",
            queued.revision,
            queued.executor_epoch,
            in_process("server-a"),
            None,
            timestamp(5),
        )
        .unwrap();
    assert_eq!(running.started_at, Some(timestamp(10)));
    assert_eq!(running.updated_at, timestamp(10));
    let settled = store
        .settle(
            OWNER,
            "clock-settle",
            running.revision,
            running.executor_epoch,
            TaskSettlement::Succeeded {
                ledger_run_id: Some("run-clock".into()),
            },
            timestamp(1),
        )
        .unwrap();
    assert_eq!(settled.updated_at, timestamp(10));
    assert_eq!(settled.finished_at, Some(timestamp(10)));
    assert_eq!(
        store
            .events(OWNER, "clock-settle")
            .unwrap()
            .last()
            .unwrap()
            .occurred_at,
        timestamp(10)
    );

    let queued = store
        .create(OWNER, new_task("clock-attach-lease", 10))
        .unwrap();
    let invalid_attach = store.attach_controller(
        OWNER,
        "clock-attach-lease",
        queued.revision,
        queued.executor_epoch,
        in_process("server-a"),
        Some(lease("server-a", 7)),
        timestamp(5),
    );
    assert!(matches!(
        invalid_attach,
        Err(TaskStoreError::InvalidInput(_))
    ));
    assert_eq!(
        store.get(OWNER, "clock-attach-lease").unwrap(),
        Some(queued)
    );

    let queued = store
        .create(OWNER, new_task("clock-renew-lease", 0))
        .unwrap();
    let running = store
        .attach_controller(
            OWNER,
            "clock-renew-lease",
            queued.revision,
            queued.executor_epoch,
            in_process("server-a"),
            Some(lease("server-a", 20)),
            timestamp(10),
        )
        .unwrap();
    let invalid_renewal = store.renew_lease(
        OWNER,
        "clock-renew-lease",
        running.revision,
        running.executor_epoch,
        "server-a",
        timestamp(8),
        timestamp(5),
    );
    assert!(matches!(
        invalid_renewal,
        Err(TaskStoreError::InvalidInput(_))
    ));
    assert_eq!(
        store.get(OWNER, "clock-renew-lease").unwrap(),
        Some(running)
    );

    let queued = store
        .create(OWNER, new_task("clock-claim-lease", 0))
        .unwrap();
    let running = store
        .attach_controller(
            OWNER,
            "clock-claim-lease",
            queued.revision,
            queued.executor_epoch,
            in_process("server-a"),
            Some(lease("server-a", 20)),
            timestamp(10),
        )
        .unwrap();
    let cancelling = store
        .request_cancel(
            OWNER,
            "clock-claim-lease",
            running.revision,
            running.executor_epoch,
            timestamp(30),
        )
        .unwrap();
    let invalid_claim = store.claim_expired(
        OWNER,
        "clock-claim-lease",
        cancelling.revision,
        cancelling.executor_epoch,
        in_process("server-b"),
        lease("server-b", 25),
        timestamp(5),
    );
    assert!(matches!(
        invalid_claim,
        Err(TaskStoreError::InvalidInput(_))
    ));
    assert_eq!(
        store.get(OWNER, "clock-claim-lease").unwrap(),
        Some(cancelling)
    );
}

#[test]
fn state_specific_failure_codes_reject_semantic_mismatches() {
    let (_directory, store) = test_store();
    let queued = store.create(OWNER, new_task("failure-codes", 0)).unwrap();

    for code in [FailureCode::Cancelled, FailureCode::TimedOut] {
        let result = store.settle(
            OWNER,
            "failure-codes",
            queued.revision,
            queued.executor_epoch,
            TaskSettlement::Failed {
                code,
                ledger_run_id: None,
            },
            timestamp(1),
        );
        assert!(matches!(result, Err(TaskStoreError::InvalidInput(_))));

        let result = store.interrupt(
            OWNER,
            "failure-codes",
            queued.revision,
            queued.executor_epoch,
            code,
            timestamp(1),
        );
        assert!(matches!(result, Err(TaskStoreError::InvalidInput(_))));
    }

    assert_eq!(store.get(OWNER, "failure-codes").unwrap(), Some(queued));
    assert_eq!(store.events(OWNER, "failure-codes").unwrap().len(), 1);
}

#[cfg(unix)]
#[test]
fn database_wal_and_shared_memory_files_are_owner_only() {
    use std::os::unix::fs::PermissionsExt as _;

    let (directory, store) = test_store();
    let path = directory.path().join("tasks.sqlite3");
    let keeper = Connection::open(&path).unwrap();
    keeper.pragma_update(None, "journal_mode", "WAL").unwrap();
    let queued = store.create(OWNER, new_task("permissions", 0)).unwrap();
    store
        .attach_controller(
            OWNER,
            "permissions",
            queued.revision,
            queued.executor_epoch,
            in_process("server-a"),
            None,
            timestamp(1),
        )
        .unwrap();
    keeper
        .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get::<_, i64>(0))
        .unwrap();

    let wal = append_suffix(&path, "-wal");
    let shared_memory = append_suffix(&path, "-shm");
    for candidate in [&path, &wal, &shared_memory] {
        let metadata = fs::metadata(candidate)
            .unwrap_or_else(|error| panic!("{} must exist: {error}", candidate.display()));
        assert_eq!(
            metadata.permissions().mode() & 0o777,
            0o600,
            "{} must be private",
            candidate.display()
        );
    }
    drop(keeper);
}

#[test]
fn process_group_schema_rejects_null_process_identity_columns() {
    let (directory, store) = test_store();
    store.create(OWNER, new_task("null-controller", 0)).unwrap();
    let connection = Connection::open(directory.path().join("tasks.sqlite3")).unwrap();
    let result = connection.execute(
        "UPDATE tasks SET \
            controller_kind = 'process_group', \
            controller_pid = NULL, controller_pgid = NULL, \
            controller_started_at_ms = ?1 \
         WHERE id = 'null-controller'",
        [timestamp(1).timestamp_millis()],
    );
    assert!(result.is_err());
    assert_eq!(
        store
            .get(OWNER, "null-controller")
            .unwrap()
            .unwrap()
            .controller,
        None
    );
}

#[test]
fn corrupt_row_values_surface_as_corrupt_data_not_sqlite_transport_errors() {
    let (directory, store) = test_store();
    store.create(OWNER, new_task("corrupt-row", 0)).unwrap();
    let connection = Connection::open(directory.path().join("tasks.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE tasks SET state = 'not_a_task_state' WHERE id = 'corrupt-row'",
            [],
        )
        .unwrap();

    assert!(matches!(
        store.get(OWNER, "corrupt-row"),
        Err(TaskStoreError::CorruptData(_))
    ));
}

#[test]
fn unknown_origin_still_fails_closed_as_corrupt_data() {
    let (directory, store) = test_store();
    store.create(OWNER, new_task("unknown-origin", 0)).unwrap();
    let connection = Connection::open(directory.path().join("tasks.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE tasks SET origin = 'future_unknown_origin' WHERE id = 'unknown-origin'",
            [],
        )
        .unwrap();

    assert!(matches!(
        store.get(OWNER, "unknown-origin"),
        Err(TaskStoreError::CorruptData(_))
    ));
}

#[test]
fn non_unique_insert_constraints_are_not_misreported_as_duplicates() {
    let (directory, store) = test_store();
    let connection = Connection::open(directory.path().join("tasks.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_task_insert \
             BEFORE INSERT ON tasks \
             BEGIN SELECT RAISE(ABORT, 'injected task insert failure'); END;",
        )
        .unwrap();

    assert!(matches!(
        store.create(OWNER, new_task("trigger-failure", 0)),
        Err(TaskStoreError::Sqlite(_))
    ));
}

#[test]
fn matching_user_version_with_missing_schema_is_rejected_at_open() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("missing-schema.sqlite3");
    let connection = Connection::open(&path).unwrap();
    connection.pragma_update(None, "user_version", 1).unwrap();
    drop(connection);
    make_database_owner_only(&path);

    assert!(matches!(
        SqliteTaskStore::open(path),
        Err(TaskStoreError::CorruptData(_))
    ));
}

#[test]
fn matching_user_version_rejects_views_and_damaged_integrity_contracts() {
    let directory = TempDir::new().unwrap();
    let views_path = directory.path().join("views.sqlite3");
    let connection = Connection::open(&views_path).unwrap();
    connection
        .execute_batch(
            "CREATE VIEW tasks AS SELECT \
                '' AS id, 1 AS record_schema, '' AS owner, 'dispatch' AS kind, \
                'rest_async' AS origin, 'queued' AS state, '' AS task_digest, \
                '' AS target_key, 0 AS created_at_ms, NULL AS started_at_ms, \
                0 AS updated_at_ms, NULL AS finished_at_ms, 0 AS revision, \
                0 AS executor_epoch, NULL AS controller_kind, \
                NULL AS controller_instance_id, NULL AS controller_pid, \
                NULL AS controller_pgid, NULL AS controller_started_at_ms, \
                NULL AS controller_birth_fingerprint, NULL AS lease_owner, \
                NULL AS lease_expires_at_ms, NULL AS ledger_run_id, \
                NULL AS failure_code WHERE 0; \
             CREATE VIEW task_events AS SELECT \
                1 AS sequence, '' AS task_id, 0 AS revision, 0 AS occurred_at_ms, \
                'created' AS kind, NULL AS from_state, 'queued' AS to_state, \
                NULL AS actor_instance, 0 AS executor_epoch WHERE 0; \
             PRAGMA user_version = 1;",
        )
        .unwrap();
    drop(connection);
    make_database_owner_only(&views_path);
    assert!(matches!(
        SqliteTaskStore::open(&views_path),
        Err(TaskStoreError::CorruptData(_))
    ));

    let migration = include_str!("../migrations/0001_tasks.sql");
    let damaged = [
        (
            "primary-key",
            migration.replacen("TEXT PRIMARY KEY", "TEXT NOT NULL", 1),
        ),
        (
            "event-unique",
            migration.replacen("UNIQUE(task_id, revision)", "CHECK (revision >= 0)", 1),
        ),
        (
            "event-foreign-key",
            migration.replacen(
                "TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE",
                "TEXT NOT NULL",
                1,
            ),
        ),
        (
            "task-check",
            migration.replacen("CHECK (revision >= 0)", "CHECK (revision >= -1)", 1),
        ),
        (
            "controller-check-literal",
            migration.replacen(
                "controller_kind = 'in_process'",
                "controller_kind = 'IN_PROCESS'",
                1,
            ),
        ),
        (
            "required-index",
            migration.replacen(
                "CREATE INDEX tasks_state_lease_idx",
                "CREATE INDEX damaged_tasks_state_lease_idx",
                1,
            ),
        ),
    ];
    for (name, sql) in damaged {
        assert_ne!(sql, migration, "fixture `{name}` must damage the schema");
        let path = directory.path().join(format!("damaged-{name}.sqlite3"));
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(&sql).unwrap();
        connection.pragma_update(None, "user_version", 1).unwrap();
        drop(connection);
        make_database_owner_only(&path);
        assert!(
            matches!(
                SqliteTaskStore::open(&path),
                Err(TaskStoreError::CorruptData(_))
            ),
            "damaged schema `{name}` must fail during open"
        );
    }
}

#[test]
fn current_v2_reopen_rejects_a_replaced_owner_event_index() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("damaged-v2.sqlite3");
    let store = SqliteTaskStore::open(&path).unwrap();
    drop(store);
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "DROP INDEX task_events_owner_task_idx;
             CREATE INDEX task_events_owner_task_idx
             ON task_events(task_id, owner, revision);",
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteTaskStore::open(path),
        Err(TaskStoreError::CorruptData(_))
    ));
}

#[test]
fn corrupt_storage_types_and_lifecycle_invariants_surface_as_corrupt_data() {
    for (name, mutation) in [
        (
            "storage-type",
            "UPDATE tasks SET created_at_ms = 'broken' WHERE id = 'corrupt'",
        ),
        (
            "digest-domain",
            "UPDATE tasks SET task_digest = 'not-a-digest' WHERE id = 'corrupt'",
        ),
        (
            "lifecycle-time",
            "UPDATE tasks SET state = 'succeeded' WHERE id = 'corrupt'",
        ),
    ] {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join(format!("{name}.sqlite3"));
        let store = SqliteTaskStore::open(&path).unwrap();
        store.create(OWNER, new_task("corrupt", 0)).unwrap();
        let connection = Connection::open(&path).unwrap();
        connection.execute(mutation, []).unwrap();
        drop(connection);
        assert!(
            matches!(
                store.get(OWNER, "corrupt"),
                Err(TaskStoreError::CorruptData(_))
            ),
            "corrupt row `{name}` must be classified as CorruptData"
        );
    }

    let (directory, store) = test_store();
    store.create(OWNER, new_task("corrupt-event", 0)).unwrap();
    let connection = Connection::open(directory.path().join("tasks.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE task_events SET kind = 'settled', to_state = 'succeeded' \
             WHERE task_id = 'corrupt-event'",
            [],
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        store.events(OWNER, "corrupt-event"),
        Err(TaskStoreError::CorruptData(_))
    ));
}

#[test]
fn abrupt_child_process_rolls_back_uncommitted_snapshot_and_event() {
    let (directory, store) = test_store();
    let path = directory.path().join("tasks.sqlite3");
    let queued = store.create(OWNER, new_task("abrupt-crash", 0)).unwrap();
    drop(store);

    let mut child = spawn_contract_child("abrupt", &path, None);
    let status = child.wait().unwrap();
    assert!(!status.success(), "abort helper must exit unsuccessfully");

    let reopened = SqliteTaskStore::open(&path).unwrap();
    assert_eq!(
        reopened.get(OWNER, "abrupt-crash").unwrap(),
        Some(queued.clone())
    );
    assert_eq!(reopened.events(OWNER, "abrupt-crash").unwrap().len(), 1);
    let running = reopened
        .attach_controller(
            OWNER,
            "abrupt-crash",
            queued.revision,
            queued.executor_epoch,
            in_process("recovered-parent"),
            None,
            timestamp(1),
        )
        .unwrap();
    assert_eq!(running.state, TaskState::Running);
    assert_eq!(running.revision, 1);
    assert_eq!(reopened.events(OWNER, "abrupt-crash").unwrap().len(), 2);
}

#[test]
fn two_child_processes_with_the_same_attach_cas_have_one_winner() {
    let (directory, store) = test_store();
    let path = directory.path().join("tasks.sqlite3");
    store.create(OWNER, new_task("process-race", 0)).unwrap();
    drop(store);

    let mut first = spawn_contract_child("attach", &path, Some("child-a"));
    let mut second = spawn_contract_child("attach", &path, Some("child-b"));
    let first_status = first.wait().unwrap();
    let second_status = second.wait().unwrap();
    let statuses = [first_status, second_status];
    assert_eq!(statuses.iter().filter(|status| status.success()).count(), 1);
    assert_eq!(
        statuses
            .iter()
            .filter(|status| status.code() == Some(42))
            .count(),
        1,
        "the losing child must observe a CAS conflict"
    );

    let reopened = SqliteTaskStore::open(&path).unwrap();
    let record = reopened.get(OWNER, "process-race").unwrap().unwrap();
    assert_eq!(record.state, TaskState::Running);
    assert_eq!(record.revision, 1);
    assert_eq!(record.executor_epoch, 1);
    assert_eq!(reopened.events(OWNER, "process-race").unwrap().len(), 2);
}

/// Helper entrypoint selected by parent tests through libtest's `--exact`.
/// It is a no-op during the ordinary in-process test pass.
#[test]
fn contract_child_process() {
    let Some(mode) = std::env::var_os(CHILD_MODE_ENV) else {
        return;
    };
    let path = std::path::PathBuf::from(
        std::env::var_os(CHILD_DB_ENV).expect("child database path must be provided"),
    );
    match mode.to_str().expect("child mode must be UTF-8") {
        "abrupt" => {
            let connection = Connection::open(path).unwrap();
            connection
                .pragma_update(None, "journal_mode", "WAL")
                .unwrap();
            connection.execute_batch("BEGIN IMMEDIATE").unwrap();
            connection
                .execute(
                    "UPDATE tasks SET \
                        state = 'running', started_at_ms = ?1, updated_at_ms = ?1, \
                        revision = 1, executor_epoch = 1, \
                        controller_kind = 'in_process', \
                        controller_instance_id = 'aborting-child' \
                     WHERE id = 'abrupt-crash' AND revision = 0 AND executor_epoch = 0",
                    [timestamp(1).timestamp_millis()],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO task_events (\
                        owner, task_id, revision, occurred_at_ms, kind, from_state, to_state, \
                        actor_instance, executor_epoch\
                     ) VALUES ('local', 'abrupt-crash', 1, ?1, 'controller_attached', \
                               'queued', 'running', 'aborting-child', 1)",
                    [timestamp(1).timestamp_millis()],
                )
                .unwrap();
            std::process::abort();
        }
        "attach" => {
            let instance = std::env::var(CHILD_INSTANCE_ENV)
                .expect("attach child instance id must be provided");
            let store = SqliteTaskStore::open(path).unwrap();
            match store.attach_controller(
                OWNER,
                "process-race",
                0,
                0,
                in_process(&instance),
                None,
                timestamp(1),
            ) {
                Ok(_) => {}
                Err(TaskStoreError::Conflict { .. }) => std::process::exit(42),
                Err(error) => panic!("unexpected attach child error: {error}"),
            }
        }
        other => panic!("unknown contract child mode `{other}`"),
    }
}

fn spawn_contract_child(
    mode: &str,
    database: &std::path::Path,
    instance: Option<&str>,
) -> std::process::Child {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "contract_child_process", "--nocapture"])
        .env(CHILD_MODE_ENV, mode)
        .env(CHILD_DB_ENV, database);
    if let Some(instance) = instance {
        command.env(CHILD_INSTANCE_ENV, instance);
    }
    command.spawn().unwrap()
}

fn assert_millisecond_precision(value: DateTime<Utc>) {
    assert_eq!(value.timestamp_subsec_nanos() % 1_000_000, 0);
}

fn append_suffix(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    value.into()
}

#[cfg(unix)]
fn make_database_owner_only(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

#[cfg(not(unix))]
fn make_database_owner_only(_path: &std::path::Path) {}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
