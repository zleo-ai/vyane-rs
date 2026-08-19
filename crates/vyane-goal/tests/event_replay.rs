use chrono::{DateTime, TimeDelta, Utc};
use tempfile::TempDir;
use vyane_goal::{
    AcceptanceCriterion, AcceptanceVerification, CriterionResult, CriterionStatus, GoalEvent,
    GoalEventKind, GoalStatus, GoalStore, NewGoal, SqliteGoalStore, criterion_key,
};

const OWNER: &str = "owner-a";
const WORKER: &str = "worker-a";
const LEASE_SECONDS: u64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayedLeaseExpiry {
    KnownNone,
    UnknownBecauseDurationIsNotRecorded,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ReplayedCriterionSatisfaction {
    kind: String,
    target: String,
    satisfied_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayedGoal {
    owner: String,
    goal_id: String,
    status: GoalStatus,
    revision: u64,
    created_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    completion_summary: Option<String>,
    failure_reason: Option<String>,
    pause_reason: Option<String>,
    cancel_reason: Option<String>,
    claimed_by: Option<String>,
    claim_expires_at: ReplayedLeaseExpiry,
    claim_generation: u64,
    satisfied_criteria: Vec<ReplayedCriterionSatisfaction>,
    waived_criterion_indices: Vec<usize>,
}

fn timestamp(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).expect("valid test timestamp")
}

fn fixture() -> (TempDir, SqliteGoalStore) {
    let directory = TempDir::new().expect("tempdir");
    #[cfg(unix)]
    {
        use std::fs;
        use std::os::unix::fs::PermissionsExt as _;

        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("restrict tempdir permissions");
    }
    let store =
        SqliteGoalStore::open(directory.path().join("goals.sqlite3")).expect("open goal store");
    (directory, store)
}

fn create_goal(
    store: &SqliteGoalStore,
    id: &str,
    criteria: Vec<AcceptanceCriterion>,
    at: DateTime<Utc>,
) {
    let mut goal = NewGoal::new(format!("Goal {id}"), at);
    goal.id = Some(id.to_string());
    goal.description = "event replay contract".into();
    goal.acceptance_criteria = criteria;
    store.create(OWNER, goal).expect("create goal");
}

fn satisfied_result(index: usize, criterion: &AcceptanceCriterion) -> CriterionResult {
    CriterionResult {
        criterion_index: index,
        criterion_key: criterion_key(index, criterion),
        kind: criterion.kind.clone(),
        target: criterion.target.clone(),
        status: CriterionStatus::Satisfied,
        command: vec!["true".into()],
        cwd: "/tmp".into(),
        exit_code: Some(0),
        duration_ms: 1,
        stdout_tail: String::new(),
        stderr_tail: String::new(),
        detail: "independently verified in replay test".into(),
    }
}

fn record_independent_verification(
    store: &SqliteGoalStore,
    id: &str,
    worker_id: Option<&str>,
    indices: &[usize],
    at: DateTime<Utc>,
) {
    let record = store.get(OWNER, id).expect("get").expect("record");
    let results = indices
        .iter()
        .map(|index| satisfied_result(*index, &record.acceptance_criteria[*index]))
        .collect::<Vec<_>>();
    let verification = AcceptanceVerification {
        goal_id: id.into(),
        all_satisfied: results.len() == record.acceptance_criteria.len()
            && results
                .iter()
                .all(|result| result.status == CriterionStatus::Satisfied),
        results,
        summary: format!("independent verification for {id}"),
    };
    store
        .record_verification(OWNER, id, worker_id, &verification, at)
        .expect("record independent verification");
}

fn parse_waived_indices(detail: &str) -> Vec<usize> {
    let Some((entries, _reason)) = detail
        .strip_prefix("waived [")
        .and_then(|value| value.split_once("]:"))
    else {
        panic!("criteria_waived detail has unexpected format: {detail}");
    };
    entries
        .split(',')
        .map(|entry| {
            entry
                .trim()
                .split_once(':')
                .expect("waived criterion includes kind")
                .0
                .parse()
                .expect("waived criterion index")
        })
        .collect()
}

fn replay_goal_events(owner: &str, goal_id: &str, events: &[GoalEvent]) -> ReplayedGoal {
    let created = events.first().expect("created event");
    assert_eq!(created.kind, GoalEventKind::Created);
    assert_eq!(created.owner, owner);
    assert_eq!(created.goal_id, goal_id);
    assert_eq!(created.revision, 0);
    assert_eq!(created.from_status, None);
    assert_eq!(created.to_status, GoalStatus::Queued);

    let mut replayed = ReplayedGoal {
        owner: owner.to_string(),
        goal_id: goal_id.to_string(),
        status: created.to_status,
        revision: created.revision,
        created_at: created.occurred_at,
        started_at: None,
        updated_at: created.occurred_at,
        finished_at: None,
        completion_summary: None,
        failure_reason: None,
        pause_reason: None,
        cancel_reason: None,
        claimed_by: None,
        claim_expires_at: ReplayedLeaseExpiry::KnownNone,
        claim_generation: 0,
        satisfied_criteria: Vec::new(),
        waived_criterion_indices: Vec::new(),
    };

    for event in &events[1..] {
        assert_eq!(event.owner, owner);
        assert_eq!(event.goal_id, goal_id);
        assert_eq!(event.revision, replayed.revision + 1);
        assert_eq!(event.from_status, Some(replayed.status));

        replayed.status = event.to_status;
        replayed.revision = event.revision;
        replayed.updated_at = event.occurred_at;
        match event.kind {
            GoalEventKind::Created => panic!("created may only be the first event"),
            GoalEventKind::Started => {
                replayed.started_at.get_or_insert(event.occurred_at);
            }
            GoalEventKind::Claimed | GoalEventKind::Reclaimed => {
                replayed.started_at.get_or_insert(event.occurred_at);
                replayed.claimed_by = Some(event.detail.clone().expect("claim worker detail"));
                // The event records the holder but not the lease duration, so an
                // absolute expiry cannot be reconstructed until a later event
                // definitively clears the lease.
                replayed.claim_expires_at =
                    ReplayedLeaseExpiry::UnknownBecauseDurationIsNotRecorded;
                replayed.claim_generation += 1;
            }
            GoalEventKind::LeaseRenewed => {
                replayed.claimed_by = Some(event.detail.clone().expect("renewed worker detail"));
                replayed.claim_expires_at =
                    ReplayedLeaseExpiry::UnknownBecauseDurationIsNotRecorded;
            }
            GoalEventKind::Progress => {}
            GoalEventKind::CriterionSatisfied => {
                replayed
                    .satisfied_criteria
                    .push(ReplayedCriterionSatisfaction {
                        kind: event.stage.clone().expect("criterion kind"),
                        target: event.detail.clone().expect("criterion target"),
                        satisfied_at: event.occurred_at,
                    });
            }
            GoalEventKind::CriteriaWaived => {
                replayed
                    .waived_criterion_indices
                    .extend(parse_waived_indices(
                        event.detail.as_deref().expect("criteria waiver detail"),
                    ));
            }
            GoalEventKind::Paused => {
                replayed.pause_reason.clone_from(&event.detail);
                replayed.claimed_by = None;
                replayed.claim_expires_at = ReplayedLeaseExpiry::KnownNone;
            }
            GoalEventKind::Resumed => {
                replayed.claimed_by = None;
                replayed.claim_expires_at = ReplayedLeaseExpiry::KnownNone;
            }
            GoalEventKind::Completed => {
                replayed.finished_at = Some(event.occurred_at);
                replayed.completion_summary.clone_from(&event.detail);
                replayed.claimed_by = None;
                replayed.claim_expires_at = ReplayedLeaseExpiry::KnownNone;
            }
            GoalEventKind::Failed => {
                replayed.finished_at = Some(event.occurred_at);
                replayed.failure_reason.clone_from(&event.detail);
                replayed.claimed_by = None;
                replayed.claim_expires_at = ReplayedLeaseExpiry::KnownNone;
            }
            GoalEventKind::Cancelled => {
                replayed.finished_at = Some(event.occurred_at);
                replayed.cancel_reason.clone_from(&event.detail);
                replayed.claimed_by = None;
                replayed.claim_expires_at = ReplayedLeaseExpiry::KnownNone;
            }
        }
    }
    replayed.satisfied_criteria.sort();
    replayed.waived_criterion_indices.sort_unstable();
    replayed
}

fn assert_replay_matches_snapshot(
    store: &SqliteGoalStore,
    owner: &str,
    goal_id: &str,
) -> ReplayedGoal {
    let snapshot = store.get(owner, goal_id).expect("get").expect("snapshot");
    let events = store.events(owner, goal_id).expect("events");
    let replayed = replay_goal_events(owner, goal_id, &events);

    assert_eq!(replayed.owner, snapshot.owner);
    assert_eq!(replayed.goal_id, snapshot.id);
    assert_eq!(replayed.status, snapshot.status);
    assert_eq!(replayed.revision, snapshot.revision);
    assert_eq!(replayed.created_at, snapshot.created_at);
    assert_eq!(replayed.started_at, snapshot.started_at);
    assert_eq!(replayed.updated_at, snapshot.updated_at);
    assert_eq!(replayed.finished_at, snapshot.finished_at);
    assert_eq!(replayed.completion_summary, snapshot.completion_summary);
    assert_eq!(replayed.failure_reason, snapshot.failure_reason);
    assert_eq!(replayed.pause_reason, snapshot.pause_reason);
    assert_eq!(replayed.cancel_reason, snapshot.cancel_reason);
    assert_eq!(replayed.claimed_by, snapshot.claimed_by);
    assert_eq!(replayed.claim_generation, snapshot.claim_generation);
    if replayed.claim_expires_at == ReplayedLeaseExpiry::KnownNone {
        assert_eq!(snapshot.claim_expires_at, None);
    }

    let mut snapshot_satisfied = snapshot
        .acceptance_criteria
        .iter()
        .filter_map(|criterion| {
            criterion
                .satisfied_at
                .map(|satisfied_at| ReplayedCriterionSatisfaction {
                    kind: criterion.kind.clone(),
                    target: criterion.target.clone(),
                    satisfied_at,
                })
        })
        .collect::<Vec<_>>();
    snapshot_satisfied.sort();
    assert_eq!(replayed.satisfied_criteria, snapshot_satisfied);

    // Created events do not carry title, description, priority, parent, the
    // full static criterion list, or continuity fields. Verification artifacts
    // are a separate durable stream. Those fields are intentionally excluded
    // instead of being copied from the stored snapshot into the replay result.
    replayed
}

#[test]
fn replay_matches_snapshot_after_claim_pause_resume_and_verified_done() {
    let (_directory, store) = fixture();
    let base = timestamp(1_700_100_000);
    create_goal(
        &store,
        "replay-completed",
        vec![AcceptanceCriterion::new("custom", "cmd:true")],
        base,
    );
    store
        .claim(
            OWNER,
            "replay-completed",
            WORKER,
            LEASE_SECONDS,
            base + TimeDelta::seconds(1),
        )
        .expect("claim and start");
    store
        .progress(
            OWNER,
            "replay-completed",
            Some(WORKER),
            "implementation",
            "durable progress",
            base + TimeDelta::seconds(2),
        )
        .expect("progress");
    store
        .pause(
            OWNER,
            "replay-completed",
            Some(WORKER),
            Some("awaiting review"),
            base + TimeDelta::seconds(3),
        )
        .expect("pause");
    store
        .resume(
            OWNER,
            "replay-completed",
            None,
            base + TimeDelta::seconds(4),
        )
        .expect("resume");
    record_independent_verification(
        &store,
        "replay-completed",
        None,
        &[0],
        base + TimeDelta::seconds(5),
    );
    store
        .satisfy_criterion(
            OWNER,
            "replay-completed",
            None,
            0,
            base + TimeDelta::seconds(6),
        )
        .expect("satisfy criterion");
    store
        .done(
            OWNER,
            "replay-completed",
            None,
            Some("verified and complete"),
            None,
            base + TimeDelta::seconds(7),
        )
        .expect("complete");

    let replayed = assert_replay_matches_snapshot(&store, OWNER, "replay-completed");
    assert!(replayed.waived_criterion_indices.is_empty());
}

#[test]
fn replay_matches_snapshot_after_failure() {
    let (_directory, store) = fixture();
    let base = timestamp(1_700_200_000);
    create_goal(&store, "replay-failed", Vec::new(), base);
    store
        .start(OWNER, "replay-failed", base + TimeDelta::seconds(1))
        .expect("start");
    store
        .progress(
            OWNER,
            "replay-failed",
            None,
            "verification",
            "failure discovered",
            base + TimeDelta::seconds(2),
        )
        .expect("progress");
    store
        .fail(
            OWNER,
            "replay-failed",
            None,
            "independent check failed",
            base + TimeDelta::seconds(3),
        )
        .expect("fail");

    let replayed = assert_replay_matches_snapshot(&store, OWNER, "replay-failed");
    assert_eq!(
        replayed.failure_reason.as_deref(),
        Some("independent check failed")
    );
}

#[test]
fn replay_matches_snapshot_after_explicit_waiver() {
    let (_directory, store) = fixture();
    let base = timestamp(1_700_300_000);
    create_goal(
        &store,
        "replay-waived",
        vec![
            AcceptanceCriterion::new("custom", "cmd:true"),
            AcceptanceCriterion::new("manual-confirm", "release owner approves"),
        ],
        base,
    );
    store
        .claim(
            OWNER,
            "replay-waived",
            WORKER,
            LEASE_SECONDS,
            base + TimeDelta::seconds(1),
        )
        .expect("claim and start");
    record_independent_verification(
        &store,
        "replay-waived",
        Some(WORKER),
        &[0],
        base + TimeDelta::seconds(2),
    );
    store
        .satisfy_criterion(
            OWNER,
            "replay-waived",
            Some(WORKER),
            0,
            base + TimeDelta::seconds(3),
        )
        .expect("satisfy verified criterion");
    let snapshot = store
        .done(
            OWNER,
            "replay-waived",
            Some(WORKER),
            Some("complete with explicit waiver"),
            Some("manual approval unavailable"),
            base + TimeDelta::seconds(4),
        )
        .expect("complete with waiver");

    let replayed = assert_replay_matches_snapshot(&store, OWNER, "replay-waived");
    assert_eq!(replayed.waived_criterion_indices, vec![1]);
    assert_eq!(snapshot.acceptance_criteria[1].satisfied_at, None);
}
