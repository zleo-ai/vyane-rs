use chrono::{DateTime, TimeDelta, Utc};
use rusqlite::Connection;
use sha2::{Digest as _, Sha256};
use tempfile::TempDir;
use vyane_goal::{
    AcceptanceCriterion, AcceptanceVerification, GoalEventKind, GoalPursuitCheckpoint, GoalQuery,
    GoalRecoveryFilter, GoalStatus, GoalStore, GoalStoreError, NewGoal, PursuitCheckpointStatus,
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
fn recovery_pages_filter_leases_and_ignore_mutable_update_order() {
    let (_directory, store) = fixture();
    let base = timestamp(1_700_000_000);
    for (id, priority, offset) in [("available-a", 0, 0), ("available-b", 0, 1)] {
        store
            .create(
                OWNER_A,
                new_goal(id, id, priority, base + TimeDelta::seconds(offset)),
            )
            .expect("create available goal");
        store
            .start(OWNER_A, id, base)
            .expect("start available goal");
    }
    store
        .create(OWNER_A, new_goal("stable", "stable", 1, base))
        .expect("create stable goal");
    store
        .claim(OWNER_A, "stable", "daemon-worker", 60, base)
        .expect("claim stable goal");
    store
        .create(OWNER_A, new_goal("foreign", "foreign", 0, base))
        .expect("create foreign goal");
    store
        .claim(OWNER_A, "foreign", "foreign-worker", 60, base)
        .expect("claim foreign goal");

    let active = store
        .list_recovery_page(
            OWNER_A,
            &GoalRecoveryFilter::ActiveWorker {
                worker_id: "daemon-worker".into(),
                at: base + TimeDelta::seconds(1),
            },
            None,
            10,
        )
        .expect("stable-worker page");
    assert_eq!(
        active
            .candidates
            .iter()
            .map(|goal| goal.id.as_str())
            .collect::<Vec<_>>(),
        ["stable"]
    );

    let filter = GoalRecoveryFilter::Available { at: base };
    let first = store
        .list_recovery_page(OWNER_A, &filter, None, 2)
        .expect("first recovery page");
    assert_eq!(first.candidates[0].id, "available-a");
    let cursor = first.next.expect("raw page cursor");
    store
        .progress(
            OWNER_A,
            "available-b",
            "concurrent",
            "move mutable updated_at",
            base + TimeDelta::seconds(30),
        )
        .expect("update second recovery goal");
    let second = store
        .list_recovery_page(OWNER_A, &filter, Some(&cursor), 2)
        .expect("second recovery page");
    assert_eq!(second.candidates[0].id, "available-b");
}

#[test]
fn recovery_page_bounds_rows_examined_before_lease_filtering() {
    let (directory, store) = fixture();
    let base = timestamp(1_700_000_000);
    for index in 0..20 {
        let id = format!("foreign-{index:02}");
        store
            .create(OWNER_A, new_goal(&id, &id, 0, base))
            .expect("create foreign goal");
        store
            .claim(OWNER_A, &id, "foreign-worker", 60, base)
            .expect("claim foreign goal");
    }

    let page = store
        .list_recovery_page(
            OWNER_A,
            &GoalRecoveryFilter::Available {
                at: base + TimeDelta::seconds(1),
            },
            None,
            5,
        )
        .expect("bounded raw page");
    assert!(page.candidates.is_empty());
    assert!(page.next.is_some(), "five examined rows advance the cursor");

    let connection = Connection::open(directory.path().join("goals.sqlite3"))
        .expect("open query-plan connection");
    let detail: String = connection
        .query_row(
            "EXPLAIN QUERY PLAN SELECT id FROM goals INDEXED BY goals_owner_queue_idx \
             WHERE owner = ?1 AND status = 'in_progress' \
             ORDER BY priority, created_at_ms, id LIMIT 5",
            [OWNER_A],
            |row| row.get(3),
        )
        .expect("recovery query plan");
    assert!(detail.contains("goals_owner_queue_idx"), "{detail}");

    for (index, predicate) in [
        (
            "goals_owner_worker_lease_idx",
            "claimed_by = 'daemon-worker' AND claim_expires_at_ms > 0",
        ),
        ("goals_owner_lease_idx", "claim_expires_at_ms <= 0"),
    ] {
        let sql = format!(
            "EXPLAIN QUERY PLAN SELECT 1 FROM goals INDEXED BY {index} \
             WHERE owner = 'owner-a' AND status = 'in_progress' AND {predicate} LIMIT 1"
        );
        let plan: String = connection
            .query_row(&sql, [], |row| row.get(3))
            .expect("recovery confirmation query plan");
        assert!(plan.contains(index), "{plan}");
    }
}

#[test]
fn queued_claim_is_atomically_gated_by_recovery_candidates() {
    let (_directory, store) = fixture();
    let base = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("expired", "expired", 0, base))
        .expect("create expired goal");
    store
        .claim(OWNER_A, "expired", "foreign-worker", 1, base)
        .expect("claim expired goal");
    store
        .create(OWNER_A, new_goal("queued", "queued", 1, base))
        .expect("create queued goal");
    let after_expiry = base + TimeDelta::seconds(2);

    assert!(
        store
            .claim_next_if_no_recovery(OWNER_A, "daemon-worker", 60, &[], after_expiry)
            .expect("recovery-gated claim")
            .is_none()
    );
    let queued = store
        .get(OWNER_A, "queued")
        .expect("get queued")
        .expect("queued goal remains");
    assert_eq!(queued.status, GoalStatus::Queued);

    let claimed = store
        .claim_next_if_no_recovery(
            OWNER_A,
            "daemon-worker",
            60,
            &["expired".into()],
            after_expiry,
        )
        .expect("cooldown-excluded claim")
        .expect("queued claim");
    assert_eq!(claimed.id, "queued");
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
fn verification_artifacts_are_owner_scoped_immutable_and_durable() {
    let (directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("verified", "Verified", 2, at))
        .expect("create");
    store
        .start(OWNER_A, "verified", at + TimeDelta::seconds(1))
        .expect("start");
    let verification = AcceptanceVerification {
        goal_id: "verified".into(),
        all_satisfied: false,
        results: Vec::new(),
        summary: "0 criteria: satisfied=0, unsatisfied=0, other=0".into(),
    };
    let artifact = store
        .record_verification(
            OWNER_A,
            "verified",
            None,
            &verification,
            at + TimeDelta::seconds(2),
        )
        .expect("record artifact");
    assert_eq!(artifact.goal_id, "verified");
    assert_eq!(artifact.payload_sha256.len(), 64);
    assert_eq!(artifact.verification, verification);
    assert_eq!(
        store
            .verifications(OWNER_A, "verified")
            .expect("read artifacts"),
        vec![artifact]
    );
    assert!(matches!(
        store.verifications(OWNER_B, "verified"),
        Err(GoalStoreError::NotFound { .. })
    ));

    let connection =
        Connection::open(directory.path().join("goals.sqlite3")).expect("open raw database");
    assert!(
        connection
            .execute("DELETE FROM goal_verifications", [])
            .is_err()
    );
    assert!(
        connection
            .execute("UPDATE goal_verifications SET payload_json = '{}'", [])
            .is_err()
    );
}

#[test]
fn verification_artifacts_require_matching_goal_status_and_worker_lease() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("fenced-verification", "Fenced", 2, at))
        .expect("create");
    let verification = AcceptanceVerification {
        goal_id: "fenced-verification".into(),
        all_satisfied: false,
        results: Vec::new(),
        summary: "not complete".into(),
    };
    let missing = AcceptanceVerification {
        goal_id: "missing".into(),
        ..verification.clone()
    };
    assert!(matches!(
        store.record_verification(OWNER_A, "missing", None, &missing, at),
        Err(GoalStoreError::NotFound { .. })
    ));
    assert!(matches!(
        store.record_verification(OWNER_A, "fenced-verification", None, &verification, at),
        Err(GoalStoreError::InvalidStatus { .. })
    ));
    store
        .claim(OWNER_A, "fenced-verification", "worker-a", 60, at)
        .expect("claim");
    assert!(matches!(
        store.record_verification(
            OWNER_A,
            "fenced-verification",
            Some("worker-b"),
            &verification,
            at
        ),
        Err(GoalStoreError::LeaseHeld { .. })
    ));
    let artifact = store
        .record_verification(
            OWNER_A,
            "fenced-verification",
            Some("worker-a"),
            &verification,
            at,
        )
        .expect("record with lease holder");
    assert_eq!(artifact.worker_id.as_deref(), Some("worker-a"));

    let wrong_goal = AcceptanceVerification {
        goal_id: "different".into(),
        ..verification
    };
    assert!(matches!(
        store.record_verification(
            OWNER_A,
            "fenced-verification",
            Some("worker-a"),
            &wrong_goal,
            at
        ),
        Err(GoalStoreError::InvalidInput(_))
    ));

    store
        .reclaim(
            OWNER_A,
            "fenced-verification",
            "worker-b",
            60,
            at + TimeDelta::seconds(61),
        )
        .expect("reclaim after expiry");
    assert!(matches!(
        store.satisfy_criterion(
            OWNER_A,
            "fenced-verification",
            Some("worker-a"),
            0,
            at + TimeDelta::seconds(61)
        ),
        Err(GoalStoreError::LeaseHeld { .. }) | Err(GoalStoreError::InvalidInput(_))
    ));
    assert_eq!(
        store
            .verifications(OWNER_A, "fenced-verification")
            .expect("artifact survives later fence failure")
            .len(),
        1
    );
}

#[test]
fn verification_artifact_size_and_digest_guards_fail_closed() {
    let (directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("bounded-artifact", "Bounded", 2, at))
        .expect("create");
    store.start(OWNER_A, "bounded-artifact", at).expect("start");
    let oversized = AcceptanceVerification {
        goal_id: "bounded-artifact".into(),
        all_satisfied: false,
        results: Vec::new(),
        summary: "x".repeat(1024 * 1024),
    };
    assert!(matches!(
        store.record_verification(OWNER_A, "bounded-artifact", None, &oversized, at),
        Err(GoalStoreError::InvalidInput(_))
    ));

    let payload = serde_json::to_string(&AcceptanceVerification {
        goal_id: "bounded-artifact".into(),
        all_satisfied: false,
        results: Vec::new(),
        summary: "tampered".into(),
    })
    .expect("serialize payload");
    let connection =
        Connection::open(directory.path().join("goals.sqlite3")).expect("open raw database");
    connection
        .execute(
            "INSERT INTO goal_verifications (verification_id, owner, goal_id, recorded_at_ms, \
             worker_id, payload_json, payload_sha256) VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6)",
            rusqlite::params![
                "verification-corrupt",
                OWNER_A,
                "bounded-artifact",
                at.timestamp_millis(),
                payload,
                "00".repeat(32),
            ],
        )
        .expect("inject corrupt artifact");
    assert!(matches!(
        store.verifications(OWNER_A, "bounded-artifact"),
        Err(GoalStoreError::Sqlite(_))
    ));

    store
        .create(OWNER_A, new_goal("goal-mismatch", "Mismatch", 2, at))
        .expect("create mismatch goal");
    let mismatch_payload = serde_json::to_string(&AcceptanceVerification {
        goal_id: "different".into(),
        all_satisfied: false,
        results: Vec::new(),
        summary: "mismatch".into(),
    })
    .expect("serialize mismatch payload");
    let mismatch_digest = Sha256::digest(mismatch_payload.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    connection
        .execute(
            "INSERT INTO goal_verifications (verification_id, owner, goal_id, recorded_at_ms, \
             worker_id, payload_json, payload_sha256) VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6)",
            rusqlite::params![
                "verification-mismatch",
                OWNER_A,
                "goal-mismatch",
                at.timestamp_millis(),
                mismatch_payload,
                mismatch_digest,
            ],
        )
        .expect("inject mismatched artifact");
    assert!(matches!(
        store.verifications(OWNER_A, "goal-mismatch"),
        Err(GoalStoreError::Sqlite(_))
    ));
}

#[test]
fn verification_history_returns_latest_hundred_in_insert_order() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("paged-artifacts", "Paged", 2, at))
        .expect("create");
    store.start(OWNER_A, "paged-artifacts", at).expect("start");
    let verification = AcceptanceVerification {
        goal_id: "paged-artifacts".into(),
        all_satisfied: false,
        results: Vec::new(),
        summary: "attempt".into(),
    };
    for _ in 0..101 {
        store
            .record_verification(OWNER_A, "paged-artifacts", None, &verification, at)
            .expect("record artifact");
    }
    let artifacts = store
        .verifications(OWNER_A, "paged-artifacts")
        .expect("read bounded page");
    assert_eq!(artifacts.len(), 100);
    assert_eq!(artifacts.first().expect("first").sequence, 2);
    assert_eq!(artifacts.last().expect("last").sequence, 101);
}

#[test]
fn pursuit_checkpoint_is_revision_and_lease_generation_fenced() {
    let (directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("checkpoint", "Checkpoint", 2, at))
        .expect("create");
    let claimed = store
        .claim(OWNER_A, "checkpoint", "worker-a", 60, at)
        .expect("claim");
    let checkpoint = GoalPursuitCheckpoint {
        owner: OWNER_A.into(),
        goal_id: "checkpoint".into(),
        checkpoint_revision: 0,
        goal_revision: claimed.revision,
        claim_generation: claimed.claim_generation,
        worker_id: "worker-a".into(),
        runtime: "builder".into(),
        workdir: directory.path().canonicalize().expect("canonical workdir"),
        started_at: at,
        updated_at: at,
        segments_started: 1,
        segments_completed: 0,
        consecutive_failures: 0,
        status: PursuitCheckpointStatus::Running,
        last_run_id: None,
        last_verification_id: Some("verification-test".into()),
    };

    let (first, event) = store
        .record_pursuit_checkpoint(
            OWNER_A,
            "checkpoint",
            "worker-a",
            &checkpoint,
            "pursuit.segment.started",
            "segment 1 started",
            at + TimeDelta::seconds(1),
        )
        .expect("record initial checkpoint");
    assert_eq!(first.checkpoint_revision, 1);
    assert_eq!(first.goal_revision, event.revision);
    assert_eq!(event.kind, GoalEventKind::Progress);
    assert_eq!(
        store
            .pursuit_checkpoint(OWNER_A, "checkpoint")
            .expect("read checkpoint"),
        Some(first.clone())
    );
    assert!(matches!(
        store.record_pursuit_checkpoint(
            OWNER_A,
            "checkpoint",
            "worker-a",
            &checkpoint,
            "pursuit.segment.started",
            "stale write",
            at + TimeDelta::seconds(2),
        ),
        Err(GoalStoreError::CheckpointConflict { .. })
    ));

    let reclaimed = store
        .reclaim(
            OWNER_A,
            "checkpoint",
            "worker-b",
            60,
            at + TimeDelta::seconds(120),
        )
        .expect("reclaim expired lease");
    let mut adopted = first;
    adopted.goal_revision = reclaimed.revision;
    adopted.claim_generation = reclaimed.claim_generation;
    adopted.worker_id = "worker-b".into();
    adopted.segments_completed = 1;
    adopted.last_run_id = Some("run-1".into());
    let (second, _) = store
        .record_pursuit_checkpoint(
            OWNER_A,
            "checkpoint",
            "worker-b",
            &adopted,
            "pursuit.segment.completed",
            "segment 1 completed; run run-1",
            at + TimeDelta::seconds(121),
        )
        .expect("adopt checkpoint under new generation");
    assert_eq!(second.checkpoint_revision, 2);
    assert_eq!(second.claim_generation, reclaimed.claim_generation);
    assert_eq!(second.worker_id, "worker-b");
    let current = store
        .get(OWNER_A, "checkpoint")
        .expect("get current goal")
        .expect("current goal");
    let mut stale_cas = adopted.clone();
    stale_cas.goal_revision = current.revision;
    assert!(matches!(
        store.record_pursuit_checkpoint(
            OWNER_A,
            "checkpoint",
            "worker-b",
            &stale_cas,
            "pursuit.segment.completed",
            "pure checkpoint CAS conflict",
            at + TimeDelta::seconds(122),
        ),
        Err(GoalStoreError::CheckpointConflict { .. })
    ));
    let mut stale_worker = adopted;
    stale_worker.worker_id = "worker-a".into();
    assert!(matches!(
        store.record_pursuit_checkpoint(
            OWNER_A,
            "checkpoint",
            "worker-a",
            &stale_worker,
            "pursuit.segment.completed",
            "stale worker",
            at + TimeDelta::seconds(122),
        ),
        Err(GoalStoreError::LeaseHeld { .. })
    ));
}

#[test]
fn checkpoint_can_read_a_foreign_platform_absolute_workdir() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("foreign-path", "Foreign path", 2, at))
        .expect("create");
    let claimed = store
        .claim(OWNER_A, "foreign-path", "worker-a", 60, at)
        .expect("claim");
    #[cfg(unix)]
    let workdir = std::path::PathBuf::from(r"C:\workspace\vyane");
    #[cfg(windows)]
    let workdir = std::path::PathBuf::from("/workspace/vyane");
    let checkpoint = GoalPursuitCheckpoint {
        owner: OWNER_A.into(),
        goal_id: "foreign-path".into(),
        checkpoint_revision: 0,
        goal_revision: claimed.revision,
        claim_generation: claimed.claim_generation,
        worker_id: "worker-a".into(),
        runtime: "builder".into(),
        workdir: workdir.clone(),
        started_at: at,
        updated_at: at,
        segments_started: 0,
        segments_completed: 0,
        consecutive_failures: 0,
        status: PursuitCheckpointStatus::Running,
        last_run_id: None,
        last_verification_id: None,
    };

    store
        .record_pursuit_checkpoint(
            OWNER_A,
            "foreign-path",
            "worker-a",
            &checkpoint,
            "pursuit.started",
            "foreign path fixture",
            at + TimeDelta::seconds(1),
        )
        .expect("record foreign path checkpoint");

    assert_eq!(
        store
            .pursuit_checkpoint(OWNER_A, "foreign-path")
            .expect("read foreign path checkpoint")
            .expect("checkpoint")
            .workdir,
        workdir
    );
}

#[test]
fn achieved_checkpoint_and_goal_completion_are_one_atomic_transition() {
    let (directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    let mut goal = new_goal("atomic-achieved", "Atomic achieved", 2, at);
    goal.acceptance_criteria = vec![AcceptanceCriterion::new("custom", "cmd:true")];
    store.create(OWNER_A, goal).expect("create");
    let claimed = store
        .claim(OWNER_A, "atomic-achieved", "worker-a", 60, at)
        .expect("claim");
    let mut checkpoint = GoalPursuitCheckpoint {
        owner: OWNER_A.into(),
        goal_id: "atomic-achieved".into(),
        checkpoint_revision: 0,
        goal_revision: claimed.revision,
        claim_generation: claimed.claim_generation,
        worker_id: "worker-a".into(),
        runtime: "builder".into(),
        workdir: directory.path().canonicalize().expect("canonical workdir"),
        started_at: at,
        updated_at: at,
        segments_started: 0,
        segments_completed: 0,
        consecutive_failures: 0,
        status: PursuitCheckpointStatus::Achieved,
        last_run_id: None,
        last_verification_id: Some("verification-test".into()),
    };
    let before_events = store
        .events(OWNER_A, "atomic-achieved")
        .expect("events before");

    assert!(matches!(
        store.record_pursuit_checkpoint(
            OWNER_A,
            "atomic-achieved",
            "worker-a",
            &checkpoint,
            "pursuit.achieved",
            "acceptance satisfied",
            at + TimeDelta::seconds(1),
        ),
        Err(GoalStoreError::CriteriaUnsatisfied { remaining: 1, .. })
    ));
    assert!(
        store
            .pursuit_checkpoint(OWNER_A, "atomic-achieved")
            .expect("checkpoint after rollback")
            .is_none()
    );
    let unchanged = store
        .get(OWNER_A, "atomic-achieved")
        .expect("get unchanged")
        .expect("unchanged goal");
    assert_eq!(unchanged.status, GoalStatus::InProgress);
    assert_eq!(unchanged.revision, claimed.revision);
    assert_eq!(
        store
            .events(OWNER_A, "atomic-achieved")
            .expect("events after rollback"),
        before_events
    );

    let satisfied = store
        .satisfy_criterion(
            OWNER_A,
            "atomic-achieved",
            Some("worker-a"),
            0,
            at + TimeDelta::seconds(2),
        )
        .expect("satisfy criterion");
    checkpoint.goal_revision = satisfied.revision;
    let (recorded, event) = store
        .record_pursuit_checkpoint(
            OWNER_A,
            "atomic-achieved",
            "worker-a",
            &checkpoint,
            "pursuit.achieved",
            "acceptance satisfied",
            at + TimeDelta::seconds(3),
        )
        .expect("complete with checkpoint");
    assert_eq!(recorded.status, PursuitCheckpointStatus::Achieved);
    assert_eq!(event.kind, GoalEventKind::Completed);
    let completed = store
        .get(OWNER_A, "atomic-achieved")
        .expect("get completed")
        .expect("completed goal");
    assert_eq!(completed.status, GoalStatus::Completed);
    assert_eq!(completed.revision, recorded.goal_revision);
    assert_eq!(completed.claimed_by, None);
}

#[test]
fn paused_checkpoint_rolls_back_goal_and_event_when_checkpoint_insert_fails() {
    let (directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("atomic-paused", "Atomic paused", 2, at))
        .expect("create");
    let claimed = store
        .claim(OWNER_A, "atomic-paused", "worker-a", 60, at)
        .expect("claim");
    let checkpoint = GoalPursuitCheckpoint {
        owner: OWNER_A.into(),
        goal_id: "atomic-paused".into(),
        checkpoint_revision: 0,
        goal_revision: claimed.revision,
        claim_generation: claimed.claim_generation,
        worker_id: "worker-a".into(),
        runtime: "builder".into(),
        workdir: directory.path().canonicalize().expect("canonical workdir"),
        started_at: at,
        updated_at: at,
        segments_started: 1,
        segments_completed: 1,
        consecutive_failures: 0,
        status: PursuitCheckpointStatus::Paused,
        last_run_id: Some("run-1".into()),
        last_verification_id: Some("verification-test".into()),
    };
    let before_events = store
        .events(OWNER_A, "atomic-paused")
        .expect("events before");
    let connection =
        Connection::open(directory.path().join("goals.sqlite3")).expect("open raw database");
    connection
        .execute_batch(
            "CREATE TRIGGER reject_paused_checkpoint
             BEFORE INSERT ON goal_pursuit_checkpoints
             BEGIN
               SELECT RAISE(ABORT, 'injected checkpoint failure');
             END;",
        )
        .expect("install failure trigger");
    drop(connection);

    assert!(matches!(
        store.record_pursuit_checkpoint(
            OWNER_A,
            "atomic-paused",
            "worker-a",
            &checkpoint,
            "pursuit.paused",
            "pause atomically",
            at + TimeDelta::seconds(1),
        ),
        Err(GoalStoreError::Sqlite(_))
    ));
    let unchanged = store
        .get(OWNER_A, "atomic-paused")
        .expect("get unchanged")
        .expect("unchanged goal");
    assert_eq!(unchanged.status, GoalStatus::InProgress);
    assert_eq!(unchanged.revision, claimed.revision);
    assert_eq!(unchanged.claimed_by.as_deref(), Some("worker-a"));
    assert!(
        store
            .pursuit_checkpoint(OWNER_A, "atomic-paused")
            .expect("checkpoint after rollback")
            .is_none()
    );
    assert_eq!(
        store
            .events(OWNER_A, "atomic-paused")
            .expect("events after rollback"),
        before_events
    );
}

#[test]
fn v2_database_migrates_current_goal_features_without_losing_goals() {
    let (directory, store) = fixture();
    let path = directory.path().join("goals.sqlite3");
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("v2-existing", "Existing", 2, at))
        .expect("create");
    drop(store);

    let connection = Connection::open(&path).expect("open raw database");
    connection
        .execute_batch(
            "DROP INDEX goal_pursuit_checkpoints_owner_updated_idx;
             DROP INDEX goals_owner_worker_lease_idx;
             DROP INDEX goals_owner_lease_idx;
             DROP TABLE goal_pursuit_checkpoints;
             DROP TRIGGER goal_verifications_immutable_update;
             DROP TRIGGER goal_verifications_immutable_delete;
             DROP INDEX goal_verifications_owner_goal_idx;
             DROP TABLE goal_verifications;
             DROP INDEX goal_takeover_approvals_owner_goal_idx;
             DROP INDEX goal_takeover_approvals_owner_status_idx;
             DROP TABLE goal_takeover_approvals;
             ALTER TABLE goals DROP COLUMN continuity_state_json;
             ALTER TABLE goals DROP COLUMN continuity_policy_json;
             PRAGMA user_version = 2;",
        )
        .expect("restore v2 schema shape");
    drop(connection);

    let migrated = SqliteGoalStore::open(&path).expect("migrate v2 to current");
    assert!(
        migrated
            .get(OWNER_A, "v2-existing")
            .expect("get existing goal")
            .is_some()
    );
    migrated
        .start(OWNER_A, "v2-existing", at)
        .expect("start existing goal");
    let verification = AcceptanceVerification {
        goal_id: "v2-existing".into(),
        all_satisfied: false,
        results: Vec::new(),
        summary: "migrated".into(),
    };
    migrated
        .record_verification(OWNER_A, "v2-existing", None, &verification, at)
        .expect("record after migration");
    assert_eq!(
        migrated
            .verifications(OWNER_A, "v2-existing")
            .expect("read after migration")
            .len(),
        1
    );
}

#[test]
fn v5_database_migrates_continuity_without_losing_recovery_indexes() {
    let (directory, store) = fixture();
    let path = directory.path().join("goals.sqlite3");
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("v5-existing", "Existing", 2, at))
        .expect("create");
    drop(store);

    let connection = Connection::open(&path).expect("open raw database");
    connection
        .execute_batch(
            "DROP INDEX goal_takeover_approvals_owner_goal_idx;
             DROP INDEX goal_takeover_approvals_owner_status_idx;
             DROP TABLE goal_takeover_approvals;
             ALTER TABLE goals DROP COLUMN continuity_state_json;
             ALTER TABLE goals DROP COLUMN continuity_policy_json;
             PRAGMA user_version = 5;",
        )
        .expect("restore v5 schema shape");
    drop(connection);

    let migrated = SqliteGoalStore::open(&path).expect("migrate v5 to current");
    assert!(
        migrated
            .get(OWNER_A, "v5-existing")
            .expect("get existing goal")
            .is_some()
    );
    drop(migrated);

    let connection = Connection::open(&path).expect("inspect migrated database");
    let version: u32 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("read schema version");
    assert_eq!(version, 8);
    for name in [
        "goals_owner_worker_lease_idx",
        "goals_owner_lease_idx",
        "continuity_policy_json",
        "continuity_state_json",
    ] {
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?1 \
                 UNION ALL SELECT 1 FROM pragma_table_info('goals') WHERE name = ?1)",
                [name],
                |row| row.get(0),
            )
            .expect("inspect migrated schema object");
        assert!(exists, "missing migrated schema object {name}");
    }
}

#[test]
fn v6_database_migrates_takeover_approval_without_losing_goal_data() {
    let (directory, store) = fixture();
    let path = directory.path().join("goals.sqlite3");
    let at = timestamp(1_700_000_000);
    store
        .create(OWNER_A, new_goal("v6-existing", "Existing", 2, at))
        .expect("create");
    drop(store);

    let connection = Connection::open(&path).expect("open raw database");
    connection
        .execute_batch(
            "DROP INDEX goal_takeover_approvals_owner_goal_idx;
             DROP INDEX goal_takeover_approvals_owner_status_idx;
             DROP TABLE goal_takeover_approvals;
             PRAGMA user_version = 6;",
        )
        .expect("restore v6 schema shape");
    drop(connection);

    let migrated = SqliteGoalStore::open(&path).expect("migrate v6 to current");
    assert!(
        migrated
            .get(OWNER_A, "v6-existing")
            .expect("get existing goal")
            .is_some()
    );
    drop(migrated);

    let connection = Connection::open(&path).expect("inspect migrated database");
    let version: u32 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("read schema version");
    assert_eq!(version, 8);
    for name in [
        "goal_takeover_approvals",
        "goal_takeover_approvals_owner_goal_idx",
        "goal_takeover_approvals_owner_status_idx",
    ] {
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?1)",
                [name],
                |row| row.get(0),
            )
            .expect("inspect migrated schema object");
        assert!(exists, "missing migrated schema object {name}");
    }
}

#[test]
fn current_schema_without_continuity_column_is_rejected_at_open() {
    let (directory, store) = fixture();
    let path = directory.path().join("goals.sqlite3");
    drop(store);

    let connection = Connection::open(&path).expect("open raw database");
    connection
        .execute_batch("ALTER TABLE goals DROP COLUMN continuity_state_json;")
        .expect("damage current schema");
    drop(connection);

    assert!(matches!(
        SqliteGoalStore::open(&path),
        Err(GoalStoreError::CorruptData(message))
            if message.contains("continuity_state_json")
    ));
}

#[test]
fn current_schema_without_takeover_table_is_rejected_at_open() {
    let (directory, store) = fixture();
    let path = directory.path().join("goals.sqlite3");
    drop(store);

    let connection = Connection::open(&path).expect("open raw database");
    connection
        .execute_batch("DROP TABLE goal_takeover_approvals;")
        .expect("damage current schema");
    drop(connection);

    assert!(matches!(
        SqliteGoalStore::open(&path),
        Err(GoalStoreError::CorruptData(message))
            if message.contains("goal_takeover_approvals")
    ));
}

#[test]
fn current_schema_without_review_handback_column_is_rejected_at_open() {
    let (directory, store) = fixture();
    let path = directory.path().join("goals.sqlite3");
    drop(store);

    let connection = Connection::open(&path).expect("open raw database");
    connection
        .execute_batch("ALTER TABLE goal_takeover_approvals DROP COLUMN upstream_run_id;")
        .expect("damage current schema");
    drop(connection);

    assert!(matches!(
        SqliteGoalStore::open(&path),
        Err(GoalStoreError::CorruptData(message))
            if message.contains("upstream_run_id")
    ));
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
            supported: 8
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
