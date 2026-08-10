//! Residual EOS-652 P2 regressions:
//! (a) completed requires independent verifier products (not bare self-report)
//! (b) progress is lease-fenced and status-guarded
//! (c) future caller timestamps must not false-kill an active lease via
//!     the store's monotonic updated_at clamp
//!
//! Signature note: while `progress` gains an optional `worker_id` fence argument
//! during implementation, these probes drive the real GoalStore entry points.

use std::fs;
use std::os::unix::fs::PermissionsExt as _;

use chrono::{DateTime, TimeDelta, Utc};
use tempfile::TempDir;
use vyane_goal::{
    AcceptanceCriterion, AcceptanceVerification, CriterionResult, CriterionStatus, GoalEventKind,
    GoalStatus, GoalStore, GoalStoreError, NewGoal, SqliteGoalStore, criterion_key,
};

const OWNER: &str = "owner-a";
const TTL: u64 = 60;

fn timestamp(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).expect("valid test timestamp")
}

fn fixture() -> (TempDir, SqliteGoalStore) {
    let directory = TempDir::new().expect("tempdir");
    // GoalStore refuses group/world-writable parents; neutralize umask 002 temps.
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
        .expect("restrict tempdir permissions");
    let store =
        SqliteGoalStore::open(directory.path().join("goals.sqlite3")).expect("open goal store");
    (directory, store)
}

fn goal_with_criteria(store: &SqliteGoalStore, id: &str, at: DateTime<Utc>) {
    let mut goal = NewGoal::new(format!("Goal {id}"), at);
    goal.id = Some(id.to_string());
    goal.acceptance_criteria = vec![
        AcceptanceCriterion::new("custom", "cmd:true"),
        AcceptanceCriterion::new("custom", "cmd:true"),
    ];
    store.create(OWNER, goal).expect("create goal");
}

fn goal_with_manual(store: &SqliteGoalStore, id: &str, at: DateTime<Utc>) {
    let mut goal = NewGoal::new(format!("Goal {id}"), at);
    goal.id = Some(id.to_string());
    goal.acceptance_criteria = vec![
        AcceptanceCriterion::new("custom", "cmd:true"),
        AcceptanceCriterion::new("manual-confirm", "release owner approves"),
    ];
    store.create(OWNER, goal).expect("create goal");
}

fn satisfied_result(index: usize, criterion: &AcceptanceCriterion) -> CriterionResult {
    assert_ne!(criterion.kind, "manual-confirm", "manual-confirm must be waived, not forged");
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
        detail: "verified in test".into(),
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
        .map(|index| {
            let criterion = &record.acceptance_criteria[*index];
            satisfied_result(*index, criterion)
        })
        .collect::<Vec<_>>();
    let verification = AcceptanceVerification {
        goal_id: id.into(),
        all_satisfied: !results.is_empty()
            && results.len() == record.acceptance_criteria.len()
            && results
                .iter()
                .all(|result| result.status == CriterionStatus::Satisfied),
        summary: format!("independent verification for {id}"),
        results,
    };
    store
        .record_verification(OWNER, id, worker_id, &verification, at)
        .expect("record independent verification");
}

// --- (a) completed ↔ independent verifier -----------------------------------

#[test]
fn done_rejects_bare_self_report_without_independent_verifier_results() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    goal_with_criteria(&store, "self-report", at);
    store.start(OWNER, "self-report", at).expect("start");

    // Pure self-report: satisfy_criterion alone is not independent verification.
    store
        .satisfy_criterion(OWNER, "self-report", None, 0, at + TimeDelta::seconds(1))
        .expect("self-report first criterion");
    store
        .satisfy_criterion(OWNER, "self-report", None, 1, at + TimeDelta::seconds(2))
        .expect("self-report second criterion");

    assert!(
        matches!(
            store.done(
                OWNER,
                "self-report",
                None,
                Some("I verified it myself"),
                None,
                at + TimeDelta::seconds(3),
            ),
            Err(GoalStoreError::CriteriaUnsatisfied { remaining: 2, .. })
        ),
        "bare self-report must not reach completed"
    );
    let record = store
        .get(OWNER, "self-report")
        .expect("get")
        .expect("record");
    assert_eq!(record.status, GoalStatus::InProgress);
    assert!(
        store
            .verifications(OWNER, "self-report")
            .expect("v")
            .is_empty()
    );
}

#[test]
fn done_accepts_path_that_records_real_verifier_results_then_completes() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    goal_with_criteria(&store, "verified-done", at);
    store.start(OWNER, "verified-done", at).expect("start");

    record_independent_verification(
        &store,
        "verified-done",
        None,
        &[0, 1],
        at + TimeDelta::seconds(1),
    );
    // Optional enrichment still allowed; completion is gated on durable artifacts.
    store
        .satisfy_criterion(OWNER, "verified-done", None, 0, at + TimeDelta::seconds(2))
        .expect("persist first satisfied_at");
    store
        .satisfy_criterion(OWNER, "verified-done", None, 1, at + TimeDelta::seconds(3))
        .expect("persist second satisfied_at");

    let completed = store
        .done(
            OWNER,
            "verified-done",
            None,
            Some("verifier-backed"),
            None,
            at + TimeDelta::seconds(4),
        )
        .expect("complete after independent verification");
    assert_eq!(completed.status, GoalStatus::Completed);
    assert_eq!(
        completed.completion_summary.as_deref(),
        Some("verifier-backed")
    );
    assert_eq!(
        store
            .verifications(OWNER, "verified-done")
            .expect("artifacts")
            .len(),
        1
    );
}

#[test]
fn waiver_still_completes_without_forging_satisfied_at() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    goal_with_manual(&store, "waive-path", at);
    store.start(OWNER, "waive-path", at).expect("start");

    // One criterion independently verified; the other is waived.
    record_independent_verification(&store, "waive-path", None, &[0], at + TimeDelta::seconds(1));
    store
        .satisfy_criterion(OWNER, "waive-path", None, 0, at + TimeDelta::seconds(2))
        .expect("persist verified criterion");

    let completed = store
        .done(
            OWNER,
            "waive-path",
            None,
            Some("ship with waiver"),
            Some("manual confirm blocked on reviewer"),
            at + TimeDelta::seconds(3),
        )
        .expect("complete with waiver of unverified remainder");
    assert_eq!(completed.status, GoalStatus::Completed);
    assert_eq!(
        completed.acceptance_criteria[0].satisfied_at,
        Some(at + TimeDelta::seconds(2))
    );
    // Waiver never forges verification data on the waived criterion.
    assert_eq!(completed.acceptance_criteria[1].satisfied_at, None);

    let events = store.events(OWNER, "waive-path").expect("events");
    let waive = events
        .iter()
        .find(|event| event.kind == GoalEventKind::CriteriaWaived)
        .expect("criteria_waived audit event");
    let detail = waive.detail.as_deref().expect("waive detail");
    assert!(detail.contains("1:manual-confirm"), "{detail}");
    assert!(
        detail.contains("manual confirm blocked on reviewer"),
        "{detail}"
    );
    assert_eq!(waive.to_status, GoalStatus::InProgress);
}

#[test]
fn satisfy_then_empty_satisfied_artifact_does_not_unlock_done() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    goal_with_criteria(&store, "pre-satisfy", at);
    store.start(OWNER, "pre-satisfy", at).expect("start");
    store
        .satisfy_criterion(OWNER, "pre-satisfy", None, 0, at + TimeDelta::seconds(1))
        .expect("self-report first");
    let record = store.get(OWNER, "pre-satisfy").expect("get").expect("record");
    let criterion = &record.acceptance_criteria[0];
    let forged = CriterionResult {
        criterion_index: 0,
        criterion_key: criterion_key(0, criterion),
        kind: criterion.kind.clone(),
        target: criterion.target.clone(),
        status: CriterionStatus::Satisfied,
        command: Vec::new(),
        cwd: String::new(),
        exit_code: None,
        duration_ms: 0,
        stdout_tail: String::new(),
        stderr_tail: String::new(),
        detail: "criterion already satisfied".into(),
    };
    let verification = AcceptanceVerification {
        goal_id: "pre-satisfy".into(),
        all_satisfied: false,
        summary: "forged after satisfy".into(),
        results: vec![forged],
    };
    // Bookkeeping re-report may be accepted after satisfy_at is set, but must not unlock done.
    let _ = store.record_verification(
        OWNER,
        "pre-satisfy",
        None,
        &verification,
        at + TimeDelta::seconds(2),
    );
    assert!(
        matches!(
            store.done(
                OWNER,
                "pre-satisfy",
                None,
                Some("should stay open"),
                None,
                at + TimeDelta::seconds(3),
            ),
            Err(GoalStoreError::CriteriaUnsatisfied { remaining: 2, .. })
        ),
        "empty already-satisfied re-report must not unlock completion"
    );
}

#[test]
fn mismatched_command_payload_is_rejected_even_with_exit_zero() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    goal_with_criteria(&store, "mismatch", at);
    store.start(OWNER, "mismatch", at).expect("start");
    let record = store.get(OWNER, "mismatch").expect("get").expect("record");
    let criterion = &record.acceptance_criteria[0];
    let forged = CriterionResult {
        criterion_index: 0,
        criterion_key: criterion_key(0, criterion),
        kind: criterion.kind.clone(),
        target: criterion.target.clone(),
        status: CriterionStatus::Satisfied,
        command: vec!["false".into()],
        cwd: "/tmp".into(),
        exit_code: Some(0),
        duration_ms: 1,
        stdout_tail: String::new(),
        stderr_tail: String::new(),
        detail: "forged command".into(),
    };
    let verification = AcceptanceVerification {
        goal_id: "mismatch".into(),
        all_satisfied: false,
        summary: "forged".into(),
        results: vec![forged],
    };
    assert!(matches!(
        store.record_verification(OWNER, "mismatch", None, &verification, at + TimeDelta::seconds(1)),
        Err(GoalStoreError::InvalidInput(_))
    ));
}

#[test]
fn forged_satisfied_artifact_without_command_evidence_is_rejected() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    goal_with_criteria(&store, "forged", at);
    store.start(OWNER, "forged", at).expect("start");
    let record = store.get(OWNER, "forged").expect("get").expect("record");
    let criterion = &record.acceptance_criteria[0];
    let forged = CriterionResult {
        criterion_index: 0,
        criterion_key: criterion_key(0, criterion),
        kind: criterion.kind.clone(),
        target: criterion.target.clone(),
        status: CriterionStatus::Satisfied,
        command: Vec::new(),
        cwd: String::new(),
        exit_code: None,
        duration_ms: 0,
        stdout_tail: String::new(),
        stderr_tail: String::new(),
        detail: "hand-written".into(),
    };
    let verification = AcceptanceVerification {
        goal_id: "forged".into(),
        all_satisfied: false,
        summary: "forged".into(),
        results: vec![forged],
    };
    assert!(
        matches!(
            store.record_verification(
                OWNER,
                "forged",
                None,
                &verification,
                at + TimeDelta::seconds(1)
            ),
            Err(GoalStoreError::InvalidInput(_))
        ),
        "store must reject forged satisfied command results"
    );
    assert!(
        matches!(
            store.done(
                OWNER,
                "forged",
                None,
                Some("should stay open"),
                None,
                at + TimeDelta::seconds(2),
            ),
            Err(GoalStoreError::CriteriaUnsatisfied { remaining: 2, .. })
        ),
        "forged artifact must not unlock completion"
    );
}

// --- (b) progress lease fence + status guard --------------------------------

#[test]
fn progress_is_status_guarded_against_queued_and_terminal() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    let mut goal = NewGoal::new("Progress status", at);
    goal.id = Some("progress-status".into());
    store.create(OWNER, goal).expect("create");

    // Queued: no progress events (status guard).
    assert!(matches!(
        store.progress(OWNER, "progress-status", None, "build", "too early", at,),
        Err(GoalStoreError::InvalidStatus {
            status: GoalStatus::Queued,
            ..
        })
    ));

    store
        .start(OWNER, "progress-status", at)
        .expect("start without lease");
    store
        .progress(
            OWNER,
            "progress-status",
            None,
            "build",
            "while in progress",
            at + TimeDelta::seconds(1),
        )
        .expect("unleased in_progress progress is allowed");

    store
        .done(
            OWNER,
            "progress-status",
            None,
            Some("done"),
            Some("no criteria"),
            at + TimeDelta::seconds(2),
        )
        .expect("complete empty-criteria goal");
    let before_events = store
        .events(OWNER, "progress-status")
        .expect("events")
        .len();
    assert!(matches!(
        store.progress(
            OWNER,
            "progress-status",
            None,
            "post",
            "after terminal",
            at + TimeDelta::seconds(3),
        ),
        Err(GoalStoreError::InvalidStatus {
            status: GoalStatus::Completed,
            ..
        })
    ));
    assert_eq!(
        store
            .events(OWNER, "progress-status")
            .expect("events")
            .len(),
        before_events,
        "progress must not append after terminal status"
    );
}

#[test]
fn progress_is_lease_fenced_for_non_holder_and_anonymous() {
    let (_directory, store) = fixture();
    let at = timestamp(1_700_000_000);
    let mut goal = NewGoal::new("Progress fence", at);
    goal.id = Some("progress-fence".into());
    store.create(OWNER, goal).expect("create");

    let claimed = store
        .claim(OWNER, "progress-fence", "worker-a", TTL, at)
        .expect("claim");
    assert_eq!(claimed.status, GoalStatus::InProgress);

    // Non-holder and anonymous progress are LeaseHeld.
    assert!(matches!(
        store.progress(
            OWNER,
            "progress-fence",
            Some("worker-b"),
            "build",
            "stolen",
            at + TimeDelta::seconds(1),
        ),
        Err(GoalStoreError::LeaseHeld { held_by, .. }) if held_by == "worker-a"
    ));
    assert!(matches!(
        store.progress(
            OWNER,
            "progress-fence",
            None,
            "build",
            "anonymous",
            at + TimeDelta::seconds(1),
        ),
        Err(GoalStoreError::LeaseHeld { held_by, .. }) if held_by == "worker-a"
    ));

    // Holder succeeds.
    let event = store
        .progress(
            OWNER,
            "progress-fence",
            Some("worker-a"),
            "build",
            "real progress",
            at + TimeDelta::seconds(2),
        )
        .expect("holder progress");
    assert_eq!(event.kind, GoalEventKind::Progress);
    assert_eq!(event.stage.as_deref(), Some("build"));
    assert_eq!(event.detail.as_deref(), Some("real progress"));
}

// --- (c) future-ts × monotonic clamp must not kill active lease -------------

#[test]
fn future_progress_timestamp_does_not_false_kill_active_lease() {
    let (_directory, store) = fixture();
    let now = timestamp(1_700_000_000);
    let mut goal = NewGoal::new("Future ts lease", now);
    goal.id = Some("future-ts".into());
    store.create(OWNER, goal).expect("create");

    let claimed = store
        .claim(OWNER, "future-ts", "worker-a", TTL, now)
        .expect("claim");
    assert_eq!(claimed.claimed_by.as_deref(), Some("worker-a"));
    let expires = claimed.claim_expires_at.expect("lease expiry");
    assert!(expires > now);
    assert!(expires <= now + TimeDelta::seconds(TTL as i64));

    // Holder records progress with a far-future caller timestamp (clock skew).
    let far_future = now + TimeDelta::days(30);
    store
        .progress(
            OWNER,
            "future-ts",
            Some("worker-a"),
            "skew",
            "future dated progress",
            far_future,
        )
        .expect("future-dated progress by holder");
    let after_progress = store.get(OWNER, "future-ts").expect("get").expect("record");
    assert!(
        after_progress.updated_at >= far_future,
        "monotonic clamp advances updated_at to the future stamp"
    );
    // Lease wall-clock fields must remain the original tenure.
    assert_eq!(after_progress.claimed_by.as_deref(), Some("worker-a"));
    assert_eq!(after_progress.claim_expires_at, Some(expires));

    // Real "now" shortly after claim: lease still active.
    let real_now = now + TimeDelta::seconds(10);
    assert!(
        after_progress.lease_active(real_now),
        "wall-clock lease must still be active at real_now"
    );

    // Reclaim at real now must fail (lease not actually expired).
    assert!(matches!(
        store.reclaim(OWNER, "future-ts", "worker-b", TTL, real_now),
        Err(GoalStoreError::LeaseHeld { held_by, .. }) if held_by == "worker-a"
    ));

    // Anonymous / non-holder writes must still be LeaseHeld (no anonymous window).
    assert!(matches!(
        store.progress(
            OWNER,
            "future-ts",
            None,
            "intrusion",
            "anonymous after future stamp",
            real_now,
        ),
        Err(GoalStoreError::LeaseHeld { held_by, .. }) if held_by == "worker-a"
    ));
    assert!(matches!(
        store.done(
            OWNER,
            "future-ts",
            Some("worker-b"),
            Some("stolen"),
            Some("bypass"),
            real_now,
        ),
        Err(GoalStoreError::LeaseHeld { held_by, .. }) if held_by == "worker-a"
    ));

    // Holder can still renew / progress at real now.
    let renewed = store
        .renew_lease(OWNER, "future-ts", "worker-a", TTL, real_now)
        .expect("holder renew after future-dated progress");
    assert_eq!(renewed.claimed_by.as_deref(), Some("worker-a"));
    assert!(renewed.claim_expires_at.expect("expiry") > real_now);

    store
        .progress(
            OWNER,
            "future-ts",
            Some("worker-a"),
            "ok",
            "still holder",
            real_now + TimeDelta::seconds(1),
        )
        .expect("holder progress at real now");
}
