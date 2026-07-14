//! Behavior contract for EOS-652: queue claim/lease (P1-A) and real
//! acceptance-criteria verification before completion (P1-B).

use std::sync::Barrier;

use chrono::{DateTime, TimeDelta, Utc};
use rusqlite::Connection;
use tempfile::TempDir;
use vyane_goal::{
    AcceptanceCriterion, GoalEventKind, GoalStatus, GoalStore, GoalStoreError, NewGoal,
    SqliteGoalStore,
};

const OWNER: &str = "owner-a";
const TTL: u64 = 60;

fn timestamp(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).expect("valid test timestamp")
}

fn fixture() -> (TempDir, SqliteGoalStore) {
    let directory = TempDir::new().expect("tempdir");
    let store =
        SqliteGoalStore::open(directory.path().join("goals.sqlite3")).expect("open goal store");
    (directory, store)
}

fn queued_goal(store: &SqliteGoalStore, id: &str, at: DateTime<Utc>) {
    let mut goal = NewGoal::new(format!("Goal {id}"), at);
    goal.id = Some(id.to_string());
    store.create(OWNER, goal).expect("create goal");
}

// --- P1-A: claim / lease ---------------------------------------------------

#[test]
fn concurrent_claim_lets_exactly_one_worker_win() {
    let (_directory, store) = fixture();
    let base = timestamp(1_700_000_000);
    queued_goal(&store, "contested", base);

    let workers = 4;
    let barrier = Barrier::new(workers);
    let results: Vec<_> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|index| {
                let store = store.clone();
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    store.claim(
                        OWNER,
                        "contested",
                        &format!("worker-{index}"),
                        TTL,
                        base + TimeDelta::seconds(1),
                    )
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("claim thread"))
            .collect()
    });

    let winners: Vec<_> = results.iter().filter(|result| result.is_ok()).collect();
    assert_eq!(
        winners.len(),
        1,
        "exactly one concurrent claim must win, got {results:?}"
    );
    for result in &results {
        if let Err(error) = result {
            assert!(
                matches!(
                    error,
                    GoalStoreError::LeaseHeld { .. } | GoalStoreError::InvalidStatus { .. }
                ),
                "loser must be rejected by the lease gate, got {error:?}"
            );
        }
    }
    // The winning claim is observable: in_progress, leased, fenced.
    let record = store.get(OWNER, "contested").expect("get").expect("record");
    assert_eq!(record.status, GoalStatus::InProgress);
    assert!(record.claimed_by.is_some());
    assert_eq!(record.claim_generation, 1);
    // Exactly one claimed event was appended.
    let events = store.events(OWNER, "contested").expect("events");
    let claimed_events = events
        .iter()
        .filter(|event| event.kind == GoalEventKind::Claimed)
        .count();
    assert_eq!(claimed_events, 1);
}

#[test]
fn concurrent_claim_next_never_hands_out_the_same_goal() {
    let (_directory, store) = fixture();
    let base = timestamp(1_700_000_000);
    queued_goal(&store, "first", base);
    queued_goal(&store, "second", base + TimeDelta::seconds(1));

    let workers = 4;
    let barrier = Barrier::new(workers);
    let results: Vec<_> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|index| {
                let store = store.clone();
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    store.claim_next(
                        OWNER,
                        &format!("worker-{index}"),
                        TTL,
                        base + TimeDelta::seconds(2),
                    )
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("claim_next thread"))
            .collect()
    });

    let mut claimed_ids: Vec<String> = results
        .into_iter()
        .filter_map(|result| result.expect("claim_next must not error"))
        .map(|record| record.id)
        .collect();
    claimed_ids.sort();
    assert_eq!(
        claimed_ids,
        ["first", "second"],
        "two queued goals must be claimed exactly once each; extra workers get None"
    );
}

#[test]
fn claim_is_rejected_while_lease_is_active_and_reclaim_succeeds_after_expiry() {
    let (_directory, store) = fixture();
    let base = timestamp(1_700_000_000);
    queued_goal(&store, "leased", base);
    let claimed = store
        .claim(OWNER, "leased", "worker-1", TTL, base)
        .expect("initial claim");
    assert_eq!(claimed.claimed_by.as_deref(), Some("worker-1"));
    assert_eq!(
        claimed.claim_expires_at,
        Some(base + TimeDelta::seconds(60))
    );
    assert_eq!(claimed.claim_generation, 1);
    assert_eq!(claimed.status, GoalStatus::InProgress);

    // Active lease: both direct claim and reclaim are refused.
    assert!(matches!(
        store.claim(OWNER, "leased", "worker-2", TTL, base + TimeDelta::seconds(30)),
        Err(GoalStoreError::LeaseHeld { held_by, .. }) if held_by == "worker-1"
    ));
    assert!(matches!(
        store.reclaim(
            OWNER,
            "leased",
            "worker-2",
            TTL,
            base + TimeDelta::seconds(30)
        ),
        Err(GoalStoreError::LeaseHeld { .. })
    ));

    // Expired lease: reclaim takes over with a fresh lease and fencing bump.
    let reclaimed = store
        .reclaim(
            OWNER,
            "leased",
            "worker-2",
            TTL,
            base + TimeDelta::seconds(61),
        )
        .expect("reclaim after expiry");
    assert_eq!(reclaimed.claimed_by.as_deref(), Some("worker-2"));
    assert_eq!(
        reclaimed.claim_expires_at,
        Some(base + TimeDelta::seconds(121))
    );
    assert_eq!(reclaimed.claim_generation, 2);

    let events = store.events(OWNER, "leased").expect("events");
    assert_eq!(
        events.iter().map(|event| event.kind).collect::<Vec<_>>(),
        [
            GoalEventKind::Created,
            GoalEventKind::Claimed,
            GoalEventKind::Reclaimed
        ]
    );
    assert_eq!(events[2].detail.as_deref(), Some("worker-2"));
}

#[test]
fn heartbeat_renewal_extends_the_lease_and_guards_identity_and_expiry() {
    let (_directory, store) = fixture();
    let base = timestamp(1_700_000_000);
    queued_goal(&store, "beating", base);
    store
        .claim(OWNER, "beating", "worker-1", TTL, base)
        .expect("claim");

    // Renewal by another worker is refused.
    assert!(matches!(
        store.renew_lease(OWNER, "beating", "worker-2", TTL, base + TimeDelta::seconds(30)),
        Err(GoalStoreError::LeaseHeld { held_by, .. }) if held_by == "worker-1"
    ));

    // Renewal by the holder extends the lease from the renewal instant.
    let renewed = store
        .renew_lease(
            OWNER,
            "beating",
            "worker-1",
            TTL,
            base + TimeDelta::seconds(30),
        )
        .expect("renew");
    assert_eq!(
        renewed.claim_expires_at,
        Some(base + TimeDelta::seconds(90))
    );
    assert_eq!(
        renewed.claim_generation, 1,
        "renewal must not change fencing"
    );

    // The renewed lease is honored: reclaim at t+70 (past original expiry) fails.
    assert!(matches!(
        store.reclaim(
            OWNER,
            "beating",
            "worker-2",
            TTL,
            base + TimeDelta::seconds(70)
        ),
        Err(GoalStoreError::LeaseHeld { .. })
    ));

    // After the renewed lease lapses, the holder can no longer renew.
    assert!(matches!(
        store.renew_lease(
            OWNER,
            "beating",
            "worker-1",
            TTL,
            base + TimeDelta::seconds(91)
        ),
        Err(GoalStoreError::LeaseExpired { .. })
    ));
}

#[test]
fn double_start_and_claim_over_manual_start_are_rejected() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    queued_goal(&store, "manual", at);
    store.start(OWNER, "manual", at).expect("first start");

    // Self-transition (double start) is no longer allowed.
    assert!(matches!(
        store.start(OWNER, "manual", at),
        Err(GoalStoreError::InvalidStatus {
            status: GoalStatus::InProgress,
            ..
        })
    ));
    // An unleased manual start is not silently claimable either.
    assert!(matches!(
        store.claim(OWNER, "manual", "worker-1", TTL, at),
        Err(GoalStoreError::InvalidStatus { .. })
    ));
}

#[test]
fn claim_validates_worker_and_lease_bounds() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    queued_goal(&store, "bounds", at);
    assert!(matches!(
        store.claim(OWNER, "bounds", "   ", TTL, at),
        Err(GoalStoreError::InvalidInput(_))
    ));
    assert!(matches!(
        store.claim(OWNER, "bounds", "worker-1", 0, at),
        Err(GoalStoreError::InvalidInput(_))
    ));
    assert!(matches!(
        store.claim(OWNER, "bounds", "worker-1", 86_401, at),
        Err(GoalStoreError::InvalidInput(_))
    ));
    assert_eq!(
        store
            .get(OWNER, "bounds")
            .expect("get")
            .expect("record")
            .status,
        GoalStatus::Queued
    );
}

// --- P1-B: acceptance criteria become real ---------------------------------

fn goal_with_criteria(store: &SqliteGoalStore, id: &str, at: DateTime<Utc>) {
    let mut goal = NewGoal::new(format!("Goal {id}"), at);
    goal.id = Some(id.to_string());
    goal.acceptance_criteria = vec![
        AcceptanceCriterion::new("test-passes", "cargo test"),
        AcceptanceCriterion::new("review-approved", "independent reviewer"),
    ];
    store.create(OWNER, goal).expect("create goal");
}

#[test]
fn done_is_rejected_while_acceptance_criteria_are_unsatisfied() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    goal_with_criteria(&store, "gated", at);
    store.start(OWNER, "gated", at).expect("start");

    assert!(matches!(
        store.done(OWNER, "gated", Some("looks done"), None, at),
        Err(GoalStoreError::CriteriaUnsatisfied { remaining: 2, .. })
    ));
    // Rejection leaves no trace: still in_progress, no completed event.
    let record = store.get(OWNER, "gated").expect("get").expect("record");
    assert_eq!(record.status, GoalStatus::InProgress);
    assert_eq!(record.revision, 1);
    assert_eq!(store.events(OWNER, "gated").expect("events").len(), 2);

    // Satisfying one of two is still not enough.
    store
        .satisfy_criterion(OWNER, "gated", 0, at)
        .expect("satisfy first");
    assert!(matches!(
        store.done(OWNER, "gated", None, None, at),
        Err(GoalStoreError::CriteriaUnsatisfied { remaining: 1, .. })
    ));

    // All satisfied: completion goes through.
    store
        .satisfy_criterion(OWNER, "gated", 1, at)
        .expect("satisfy second");
    let completed = store
        .done(OWNER, "gated", Some("verified"), None, at)
        .expect("complete");
    assert_eq!(completed.status, GoalStatus::Completed);
}

#[test]
fn satisfy_criterion_persists_satisfied_at_and_appends_an_event() {
    let (_directory, store) = fixture();
    let base = timestamp(1_700_000_000);
    goal_with_criteria(&store, "verified", base);
    store.start(OWNER, "verified", base).expect("start");

    let updated = store
        .satisfy_criterion(OWNER, "verified", 0, base + TimeDelta::seconds(5))
        .expect("satisfy");
    assert_eq!(
        updated.acceptance_criteria[0].satisfied_at,
        Some(base + TimeDelta::seconds(5))
    );
    assert_eq!(updated.acceptance_criteria[1].satisfied_at, None);

    // Persisted, not just in-memory: reread from the database.
    let reread = store.get(OWNER, "verified").expect("get").expect("record");
    assert_eq!(
        reread.acceptance_criteria[0].satisfied_at,
        Some(base + TimeDelta::seconds(5))
    );

    let events = store.events(OWNER, "verified").expect("events");
    let satisfied = events
        .iter()
        .find(|event| event.kind == GoalEventKind::CriterionSatisfied)
        .expect("criterion_satisfied event");
    assert_eq!(satisfied.stage.as_deref(), Some("test-passes"));
    assert_eq!(satisfied.detail.as_deref(), Some("cargo test"));

    // A criterion cannot be satisfied twice.
    assert!(matches!(
        store.satisfy_criterion(OWNER, "verified", 0, base + TimeDelta::seconds(6)),
        Err(GoalStoreError::InvalidInput(_))
    ));
    // Out-of-range index is rejected.
    assert!(matches!(
        store.satisfy_criterion(OWNER, "verified", 9, base + TimeDelta::seconds(6)),
        Err(GoalStoreError::InvalidInput(_))
    ));
}

#[test]
fn satisfy_criterion_requires_an_in_progress_goal() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    goal_with_criteria(&store, "not-running", at);
    assert!(matches!(
        store.satisfy_criterion(OWNER, "not-running", 0, at),
        Err(GoalStoreError::InvalidStatus {
            status: GoalStatus::Queued,
            ..
        })
    ));
}

#[test]
fn explicit_waiver_records_an_auditable_event_before_completion() {
    let (_directory, store) = fixture();
    let base = timestamp(1_700_000_000);
    goal_with_criteria(&store, "waived", base);
    store.start(OWNER, "waived", base).expect("start");
    store
        .satisfy_criterion(OWNER, "waived", 0, base + TimeDelta::seconds(1))
        .expect("satisfy first");

    let completed = store
        .done(
            OWNER,
            "waived",
            Some("shipping anyway"),
            Some("reviewer unavailable before deadline"),
            base + TimeDelta::seconds(2),
        )
        .expect("complete with waiver");
    assert_eq!(completed.status, GoalStatus::Completed);
    // Waiver never forges verification data.
    assert_eq!(completed.acceptance_criteria[1].satisfied_at, None);

    let events = store.events(OWNER, "waived").expect("events");
    let kinds: Vec<_> = events.iter().map(|event| event.kind).collect();
    assert_eq!(
        kinds,
        [
            GoalEventKind::Created,
            GoalEventKind::Started,
            GoalEventKind::CriterionSatisfied,
            GoalEventKind::CriteriaWaived,
            GoalEventKind::Completed
        ]
    );
    let waive_event = &events[3];
    assert_eq!(waive_event.to_status, GoalStatus::InProgress);
    let detail = waive_event.detail.as_deref().expect("waive detail");
    assert!(detail.contains("1:review-approved"));
    assert!(detail.contains("reviewer unavailable before deadline"));
    assert_eq!(
        events
            .iter()
            .map(|event| event.revision)
            .collect::<Vec<_>>(),
        [0, 1, 2, 3, 4]
    );
}

#[test]
fn waiver_and_completion_commit_atomically() {
    let (directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    goal_with_criteria(&store, "atomic-waive", at);
    store.start(OWNER, "atomic-waive", at).expect("start");

    // Force the completed event insert to fail after the waive event succeeded.
    let connection =
        Connection::open(directory.path().join("goals.sqlite3")).expect("open mutation connection");
    connection
        .execute_batch(
            "CREATE TRIGGER reject_completed BEFORE INSERT ON goal_events \
             WHEN NEW.kind = 'completed' \
             BEGIN SELECT RAISE(ABORT, 'injected completion failure'); END;",
        )
        .expect("install failure trigger");
    drop(connection);

    assert!(
        store
            .done(OWNER, "atomic-waive", None, Some("waive it"), at)
            .is_err()
    );
    // The half-applied waiver must have rolled back with the completion.
    let record = store
        .get(OWNER, "atomic-waive")
        .expect("get")
        .expect("record");
    assert_eq!(record.status, GoalStatus::InProgress);
    assert_eq!(record.revision, 1);
    assert_eq!(
        store.events(OWNER, "atomic-waive").expect("events").len(),
        2
    );
}

// --- schema v1 -> v2 migration ---------------------------------------------

const MIGRATION_0001: &str = include_str!("../migrations/0001_goals.sql");

#[test]
fn v1_database_upgrades_in_place_and_supports_claims() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("goals.sqlite3");
    {
        let connection = Connection::open(&path).expect("create v1 database");
        connection
            .execute_batch(MIGRATION_0001)
            .expect("apply v1 schema");
        connection
            .pragma_update(None, "user_version", 1_u32)
            .expect("stamp v1");
        connection
            .execute(
                "INSERT INTO goals (owner, id, record_schema, title, description, status, \
                 priority, parent_goal_id, acceptance_json, created_at_ms, started_at_ms, \
                 updated_at_ms, finished_at_ms, revision, completion_summary, failure_reason, \
                 pause_reason, cancel_reason) VALUES ('owner-a', 'legacy', 1, 'Legacy goal', \
                 '', 'queued', 2, NULL, '[]', 1700000000000, NULL, 1700000000000, NULL, 0, \
                 NULL, NULL, NULL, NULL)",
                [],
            )
            .expect("insert legacy goal");
        connection
            .execute(
                "INSERT INTO goal_events (event_id, owner, goal_id, revision, occurred_at_ms, \
                 kind, from_status, to_status, stage, detail) VALUES ('legacy-event', \
                 'owner-a', 'legacy', 0, 1700000000000, 'created', NULL, 'queued', NULL, NULL)",
                [],
            )
            .expect("insert legacy event");
    }
    // 0600 is enforced on open.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("restrict permissions");
    }

    let store = SqliteGoalStore::open(&path).expect("upgrade to v2");
    let record = store
        .get(OWNER, "legacy")
        .expect("get")
        .expect("legacy record");
    assert_eq!(record.claimed_by, None);
    assert_eq!(record.claim_generation, 0);
    // Pre-existing events survive the goal_events rebuild.
    assert_eq!(store.events(OWNER, "legacy").expect("events").len(), 1);
    // The upgraded database supports the new lease semantics.
    let at = timestamp(1_700_000_100);
    let claimed = store
        .claim(OWNER, "legacy", "worker-1", TTL, at)
        .expect("claim on upgraded database");
    assert_eq!(claimed.claim_generation, 1);
}
