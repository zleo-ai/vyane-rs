use chrono::{DateTime, TimeDelta, Utc};
use rusqlite::Connection;
use tempfile::TempDir;
use vyane_goal::{
    AcceptanceCriterion, GoalEventKind, GoalQuery, GoalStatus, GoalStore, GoalStoreError, NewGoal,
    SqliteGoalStore,
};

const OWNER_A: &str = "owner-a";
const OWNER_B: &str = "owner-b";

fn timestamp(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).expect("valid test timestamp")
}

fn fixture() -> (TempDir, SqliteGoalStore) {
    let directory = TempDir::new().expect("tempdir");
    let store =
        SqliteGoalStore::open(directory.path().join("goals.sqlite3")).expect("open goal store");
    (directory, store)
}

fn new_goal(id: &str, title: &str, priority: u8, at: DateTime<Utc>) -> NewGoal {
    let mut goal = NewGoal::new(title, at);
    goal.id = Some(id.to_string());
    goal.priority = priority;
    goal
}

#[test]
fn lifecycle_updates_snapshot_and_appends_revision_ordered_events() {
    let (_directory, store) = fixture();
    let base = timestamp(1_700_000_000);
    let mut goal = new_goal("goal-lifecycle", "Ship lifecycle", 1, base);
    goal.description = "A durable goal".into();
    goal.acceptance_criteria = vec![
        AcceptanceCriterion::new("test-passes", "workspace"),
        AcceptanceCriterion::new("manual-confirm", "release owner approves"),
    ];

    let created = store.create(OWNER_A, goal).expect("create");
    assert_eq!(created.status, GoalStatus::Queued);
    assert_eq!(created.revision, 0);
    assert_eq!(created.acceptance_criteria.len(), 2);

    let started = store
        .start(OWNER_A, &created.id, base + TimeDelta::seconds(1))
        .expect("start");
    assert_eq!(started.status, GoalStatus::InProgress);
    assert_eq!(started.revision, 1);

    let progress = store
        .progress(
            OWNER_A,
            &created.id,
            "implementation",
            "store and CLI wired",
            base + TimeDelta::seconds(2),
        )
        .expect("progress");
    assert_eq!(progress.kind, GoalEventKind::Progress);
    assert_eq!(progress.revision, 2);
    assert_eq!(progress.stage.as_deref(), Some("implementation"));

    let paused = store
        .pause(
            OWNER_A,
            &created.id,
            None,
            Some("waiting for review"),
            base + TimeDelta::seconds(3),
        )
        .expect("pause");
    assert_eq!(paused.status, GoalStatus::Paused);

    let resumed = store
        .resume(OWNER_A, &created.id, None, base + TimeDelta::seconds(4))
        .expect("resume");
    assert_eq!(resumed.status, GoalStatus::InProgress);

    for index in 0..2 {
        store
            .satisfy_criterion(
                OWNER_A,
                &created.id,
                None,
                index,
                base + TimeDelta::seconds(5),
            )
            .expect("satisfy criterion");
    }

    let completed = store
        .done(
            OWNER_A,
            &created.id,
            None,
            Some("all checks passed"),
            None,
            base + TimeDelta::seconds(6),
        )
        .expect("complete");
    assert_eq!(completed.status, GoalStatus::Completed);
    assert_eq!(completed.revision, 7);
    assert_eq!(
        completed.completion_summary.as_deref(),
        Some("all checks passed")
    );
    assert_eq!(completed.finished_at, Some(base + TimeDelta::seconds(6)));

    let events = store.events(OWNER_A, &created.id).expect("events");
    assert_eq!(events.len(), 8);
    assert_eq!(
        events
            .iter()
            .map(|event| event.revision)
            .collect::<Vec<_>>(),
        [0, 1, 2, 3, 4, 5, 6, 7]
    );
    assert_eq!(events[0].from_status, None);
    assert_eq!(events[5].kind, GoalEventKind::CriterionSatisfied);
    assert_eq!(events[7].to_status, GoalStatus::Completed);
}

#[test]
fn owner_scope_allows_same_id_and_hides_foreign_records() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("shared-id", "Owner A", 2, at))
        .expect("owner A create");
    store
        .create(OWNER_B, new_goal("shared-id", "Owner B", 2, at))
        .expect("owner B create");

    assert_eq!(
        store
            .get(OWNER_A, "shared-id")
            .expect("owner A get")
            .expect("owner A record")
            .title,
        "Owner A"
    );
    assert_eq!(
        store
            .get(OWNER_B, "shared-id")
            .expect("owner B get")
            .expect("owner B record")
            .title,
        "Owner B"
    );
    assert!(
        store
            .get("foreign", "shared-id")
            .expect("foreign get")
            .is_none()
    );
    assert!(matches!(
        store.start("foreign", "shared-id", at),
        Err(GoalStoreError::NotFound { .. })
    ));
    assert!(matches!(
        store.events("foreign", "shared-id"),
        Err(GoalStoreError::NotFound { .. })
    ));
}

#[test]
fn queue_and_list_order_are_stable_and_owner_scoped() {
    let (_directory, store) = fixture();
    let base = timestamp(1_700_000_000);
    let mut parented = new_goal(
        "later-urgent",
        "Later urgent",
        0,
        base + TimeDelta::seconds(2),
    );
    parented.parent_goal_id = Some("umbrella".into());
    store.create(OWNER_A, parented).expect("later urgent");
    store
        .create(OWNER_A, new_goal("older-urgent", "Older urgent", 0, base))
        .expect("older urgent");
    store
        .create(OWNER_A, new_goal("normal", "Normal", 2, base))
        .expect("normal");
    store
        .create(OWNER_B, new_goal("foreign", "Foreign", 0, base))
        .expect("foreign");

    let next = store
        .next_queued(OWNER_A)
        .expect("next")
        .expect("queued goal");
    assert_eq!(next.id, "older-urgent");

    store.start(OWNER_A, "older-urgent", base).expect("start");
    let queued = store
        .list(
            OWNER_A,
            &GoalQuery {
                statuses: vec![GoalStatus::Queued],
                parent_goal_id: None,
                limit: 50,
            },
        )
        .expect("queued list");
    assert_eq!(
        queued
            .iter()
            .map(|goal| goal.id.as_str())
            .collect::<Vec<_>>(),
        ["later-urgent", "normal"]
    );

    let parented = store
        .list(
            OWNER_A,
            &GoalQuery {
                statuses: Vec::new(),
                parent_goal_id: Some("umbrella".into()),
                limit: 50,
            },
        )
        .expect("parent list");
    assert_eq!(parented.len(), 1);
    assert_eq!(parented[0].id, "later-urgent");
}

#[test]
fn illegal_terminal_transition_is_rejected_without_an_event() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("terminal", "Terminal", 2, at))
        .expect("create");
    store.start(OWNER_A, "terminal", at).expect("start");
    store
        .done(OWNER_A, "terminal", None, None, None, at)
        .expect("complete");

    assert!(matches!(
        store.fail(OWNER_A, "terminal", None, "too late", at),
        Err(GoalStoreError::InvalidStatus {
            status: GoalStatus::Completed,
            ..
        })
    ));
    let record = store
        .get(OWNER_A, "terminal")
        .expect("get")
        .expect("record");
    assert_eq!(record.status, GoalStatus::Completed);
    assert_eq!(record.revision, 2);
    assert_eq!(store.events(OWNER_A, "terminal").expect("events").len(), 3);
}

#[test]
fn failed_and_cancelled_terminal_paths_persist_reasons() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    for id in ["failed", "cancelled", "paused-cancel"] {
        store
            .create(OWNER_A, new_goal(id, id, 2, at))
            .expect("create terminal fixture");
    }
    store
        .start(OWNER_A, "failed", at)
        .expect("start failed fixture");
    let failed = store
        .fail(OWNER_A, "failed", None, "verification failed", at)
        .expect("fail");
    assert_eq!(failed.status, GoalStatus::Failed);
    assert_eq!(
        failed.failure_reason.as_deref(),
        Some("verification failed")
    );

    let cancelled = store
        .cancel(OWNER_A, "cancelled", None, Some("superseded"), at)
        .expect("cancel queued");
    assert_eq!(cancelled.status, GoalStatus::Cancelled);
    assert_eq!(cancelled.cancel_reason.as_deref(), Some("superseded"));

    store
        .start(OWNER_A, "paused-cancel", at)
        .expect("start paused fixture");
    store
        .pause(OWNER_A, "paused-cancel", None, None, at)
        .expect("pause fixture");
    let paused_cancel = store
        .cancel(OWNER_A, "paused-cancel", None, None, at)
        .expect("cancel paused");
    assert_eq!(paused_cancel.status, GoalStatus::Cancelled);
}

#[test]
fn snapshot_and_event_write_roll_back_together() {
    let (directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("atomic", "Atomic", 2, at))
        .expect("create");
    let connection =
        Connection::open(directory.path().join("goals.sqlite3")).expect("open mutation connection");
    connection
        .execute_batch(
            "CREATE TRIGGER reject_selected_progress BEFORE INSERT ON goal_events \
             WHEN NEW.kind = 'progress' AND NEW.stage = 'reject' \
             BEGIN SELECT RAISE(ABORT, 'injected event failure'); END;",
        )
        .expect("install failure trigger");
    drop(connection);

    assert!(
        store
            .progress(OWNER_A, "atomic", "reject", "must roll back", at)
            .is_err()
    );
    let record = store.get(OWNER_A, "atomic").expect("get").expect("record");
    assert_eq!(record.revision, 0);
    assert_eq!(record.updated_at, at);
    assert_eq!(store.events(OWNER_A, "atomic").expect("events").len(), 1);
}

#[test]
fn events_are_immutable_at_the_database_boundary() {
    let (directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("immutable", "Immutable", 2, at))
        .expect("create");
    let connection =
        Connection::open(directory.path().join("goals.sqlite3")).expect("open raw database");
    assert!(connection.execute("DELETE FROM goal_events", []).is_err());
    assert!(
        connection
            .execute("UPDATE goal_events SET detail = 'changed'", [])
            .is_err()
    );
}

#[test]
fn store_reopens_and_rejects_newer_schema() {
    let (directory, store) = fixture();
    let path = directory.path().join("goals.sqlite3");
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("durable", "Durable", 2, at))
        .expect("create");
    drop(store);
    let reopened = SqliteGoalStore::open(&path).expect("reopen");
    assert!(reopened.get(OWNER_A, "durable").expect("get").is_some());
    drop(reopened);

    let connection = Connection::open(&path).expect("open raw database");
    connection
        .pragma_update(None, "user_version", 99_u32)
        .expect("set future schema");
    drop(connection);
    assert!(matches!(
        SqliteGoalStore::open(&path),
        Err(GoalStoreError::UnsupportedSchema {
            found: 99,
            supported: 2
        })
    ));
}

#[test]
fn invalid_metadata_is_rejected_before_persistence() {
    let (_directory, store) = fixture();
    let at = Utc::now();
    assert!(matches!(
        store.create(OWNER_A, NewGoal::new("   ", at)),
        Err(GoalStoreError::InvalidInput(_))
    ));
    let mut invalid_priority = NewGoal::new("valid", at);
    invalid_priority.priority = 5;
    assert!(matches!(
        store.create(OWNER_A, invalid_priority),
        Err(GoalStoreError::InvalidInput(_))
    ));
    assert!(
        store
            .list(
                OWNER_A,
                &GoalQuery {
                    limit: 1_001,
                    ..GoalQuery::default()
                }
            )
            .is_err()
    );
}
