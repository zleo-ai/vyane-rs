#![allow(clippy::unwrap_used)]

use std::time::Duration;

use chrono::{TimeZone as _, Utc};
use rusqlite::Connection;
use tempfile::TempDir;
use vyane_goal::{
    GoalContinuityMode, GoalContinuityPolicy, GoalContinuitySignal, GoalContinuitySignalKind,
    GoalContinuityStepStatus, GoalExecutionTarget, GoalQuotaEvent, GoalStore, GoalStoreError,
    NewGoal, SCHEMA_VERSION, SqliteGoalStore, TakeoverApproval, TakeoverApprovalRequest,
    TakeoverApprovalStatus, TakeoverBoundTarget, TakeoverDecision, TakeoverFinish,
    TakeoverRunStatus, TakeoverSandbox, apply_quota_handoff_events,
};

const OWNER: &str = "local";

fn at(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).unwrap()
}

fn target(role: &str, provider: &str, harness: &str, model: &str) -> GoalExecutionTarget {
    GoalExecutionTarget {
        provider: provider.into(),
        protocol: "openai_responses".into(),
        harness: harness.into(),
        model: model.into(),
        profile: None,
        role: role.into(),
    }
}

fn policy() -> GoalContinuityPolicy {
    GoalContinuityPolicy {
        mode: GoalContinuityMode::QuotaHandoff,
        primary: target("primary", "primary-provider", "codex-cli", "primary-model"),
        takeover: vec![target(
            "takeover",
            "backup-provider",
            "claude-code",
            "backup-model",
        )],
        reviewer: Some(target(
            "reviewer",
            "primary-provider",
            "codex-cli",
            "primary-model",
        )),
        resume_primary_after_reset: true,
        require_review_before_resume: true,
    }
}

fn setup() -> (TempDir, SqliteGoalStore) {
    let dir = TempDir::new().unwrap();
    let store = SqliteGoalStore::open(dir.path().join("goals.sqlite3")).unwrap();
    let mut goal = NewGoal::new("controlled takeover", at(1_000));
    goal.id = Some("goal-a".into());
    goal.continuity_policy = Some(policy());
    store.create(OWNER, goal).unwrap();
    store.start(OWNER, "goal-a", at(1_001)).unwrap();
    let event = GoalQuotaEvent {
        event_id: "quota-a".into(),
        goal_id: Some("goal-a".into()),
        provider: "primary-provider".into(),
        harness: "codex-cli".into(),
        model: "primary-model".into(),
        session_id: None,
        observed_at: at(1_100),
        estimated_reset_at: Some(at(2_000)),
    };
    apply_quota_handoff_events(&store, OWNER, &[event], at(1_101)).unwrap();
    (dir, store)
}

fn request(store: &SqliteGoalStore, dir: &TempDir) -> TakeoverApprovalRequest {
    let goal = store.get(OWNER, "goal-a").unwrap().unwrap();
    let state = goal.continuity_state.clone().unwrap();
    let step = state.handoff_plan.steps.first().unwrap();
    TakeoverApprovalRequest {
        goal_id: goal.id,
        step_id: step.id.clone(),
        step_kind: step.kind.clone(),
        quota_event_id: state.quota_event_id.clone(),
        target: TakeoverBoundTarget::from_execution(step.target.as_ref().unwrap()),
        workdir: std::fs::canonicalize(dir.path()).unwrap(),
        sandbox: TakeoverSandbox::Write,
        timeout: Duration::from_secs(300),
        goal_revision: goal.revision,
        plan_snapshot: state,
        upstream_approval_id: None,
        upstream_run_id: None,
        upstream_run_status: None,
    }
}

fn complete_takeover(store: &SqliteGoalStore, dir: &TempDir) -> TakeoverApproval {
    let approval = store
        .queue_takeover_approval(OWNER, &request(store, dir), at(1_200))
        .unwrap();
    store
        .decide_takeover_approval(
            OWNER,
            &approval.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(1_201),
        )
        .unwrap();
    store
        .consume_takeover_approval(OWNER, &approval.approval_id, at(1_202))
        .unwrap();
    store
        .finish_takeover_approval(
            OWNER,
            &approval.approval_id,
            &TakeoverFinish {
                run_id: Some("takeover-run".into()),
                run_status: TakeoverRunStatus::Success,
                detail: "takeover completed".into(),
            },
            at(1_203),
        )
        .unwrap()
}

fn review_request(
    store: &SqliteGoalStore,
    dir: &TempDir,
    takeover: &TakeoverApproval,
) -> TakeoverApprovalRequest {
    let goal = store.get(OWNER, "goal-a").unwrap().unwrap();
    let state = goal.continuity_state.clone().unwrap();
    let review = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.id == "review_takeover")
        .unwrap();
    TakeoverApprovalRequest {
        goal_id: goal.id,
        step_id: review.id.clone(),
        step_kind: review.kind.clone(),
        quota_event_id: state.quota_event_id.clone(),
        target: TakeoverBoundTarget::from_execution(review.target.as_ref().unwrap()),
        workdir: std::fs::canonicalize(dir.path()).unwrap(),
        sandbox: TakeoverSandbox::ReadOnly,
        timeout: Duration::from_secs(300),
        goal_revision: goal.revision,
        plan_snapshot: state,
        upstream_approval_id: Some(takeover.approval_id.clone()),
        upstream_run_id: takeover.run_id.clone(),
        upstream_run_status: takeover.run_status,
    }
}

fn quota_reset_signal() -> GoalContinuitySignal {
    GoalContinuitySignal {
        kind: GoalContinuitySignalKind::QuotaReset,
        quota_event_id: "quota-a".into(),
        provider: "primary-provider".into(),
        harness: "codex-cli".into(),
        model: "primary-model".into(),
        observed_at: at(2_001),
        source: "quota-reader".into(),
    }
}

fn complete_review(store: &SqliteGoalStore, dir: &TempDir) {
    let takeover = complete_takeover(store, dir);
    let approval = store
        .queue_takeover_approval(OWNER, &review_request(store, dir, &takeover), at(1_204))
        .unwrap();
    store
        .decide_takeover_approval(
            OWNER,
            &approval.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(1_205),
        )
        .unwrap();
    store
        .consume_takeover_approval(OWNER, &approval.approval_id, at(1_206))
        .unwrap();
    store
        .finish_takeover_approval(
            OWNER,
            &approval.approval_id,
            &TakeoverFinish {
                run_id: Some("review-run".into()),
                run_status: TakeoverRunStatus::Success,
                detail: "review completed".into(),
            },
            at(1_207),
        )
        .unwrap();
}

#[test]
fn schema_v7_contains_durable_continuity_approval_table() {
    let (dir, _store) = setup();
    assert_eq!(SCHEMA_VERSION, 7);
    let connection = Connection::open(dir.path().join("goals.sqlite3")).unwrap();
    let exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'goal_takeover_approvals')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(exists);
}

#[test]
fn schema_v6_takeover_rows_upgrade_without_digest_drift() {
    let (dir, store) = setup();
    let approval = store
        .queue_takeover_approval(OWNER, &request(&store, &dir), at(1_200))
        .unwrap();
    let original_digest = approval.snapshot_digest.clone();
    drop(store);
    let connection = Connection::open(dir.path().join("goals.sqlite3")).unwrap();
    connection
        .execute_batch(
            "DROP INDEX goal_takeover_approvals_upstream_idx;
             ALTER TABLE goal_takeover_approvals DROP COLUMN upstream_run_status;
             ALTER TABLE goal_takeover_approvals DROP COLUMN upstream_run_id;
             ALTER TABLE goal_takeover_approvals DROP COLUMN upstream_approval_id;
             PRAGMA user_version = 6;",
        )
        .unwrap();
    drop(connection);

    let upgraded = SqliteGoalStore::open(dir.path().join("goals.sqlite3")).unwrap();
    let approval = upgraded
        .get_takeover_approval(OWNER, &approval.approval_id)
        .unwrap()
        .unwrap();
    assert_eq!(approval.status, TakeoverApprovalStatus::Pending);
    assert_eq!(approval.snapshot_digest, original_digest);
    assert!(approval.upstream_approval_id.is_none());
    assert!(approval.upstream_run_id.is_none());
    assert!(approval.upstream_run_status.is_none());
}

#[test]
fn queue_is_idempotent_and_decision_is_explicit_and_immutable() {
    let (dir, store) = setup();
    let request = request(&store, &dir);
    let first = store
        .queue_takeover_approval(OWNER, &request, at(1_200))
        .unwrap();
    let repeated = store
        .queue_takeover_approval(OWNER, &request, at(1_201))
        .unwrap();
    assert_eq!(first.approval_id, repeated.approval_id);
    assert_eq!(first.status, TakeoverApprovalStatus::Pending);
    assert!(matches!(
        store.consume_takeover_approval(OWNER, &first.approval_id, at(1_201)),
        Err(GoalStoreError::TakeoverApprovalNotExecutable {
            status: TakeoverApprovalStatus::Pending,
            ..
        })
    ));

    let approved = store
        .decide_takeover_approval(
            OWNER,
            &first.approval_id,
            TakeoverDecision::Approve,
            "operator",
            Some("explicit takeover approval"),
            at(1_202),
        )
        .unwrap();
    assert_eq!(approved.status, TakeoverApprovalStatus::Approved);
    assert_eq!(approved.decided_by.as_deref(), Some("operator"));
    assert!(matches!(
        store.decide_takeover_approval(
            OWNER,
            &first.approval_id,
            TakeoverDecision::Reject,
            "operator",
            None,
            at(1_203),
        ),
        Err(GoalStoreError::TakeoverApprovalAlreadyDecided { .. })
    ));
}

#[test]
fn reject_is_durable_and_never_executable() {
    let (dir, store) = setup();
    let request = request(&store, &dir);
    let approval = store
        .queue_takeover_approval(OWNER, &request, at(1_200))
        .unwrap();
    let rejected = store
        .decide_takeover_approval(
            OWNER,
            &approval.approval_id,
            TakeoverDecision::Reject,
            "operator",
            Some("boundary rejected"),
            at(1_201),
        )
        .unwrap();
    assert_eq!(rejected.status, TakeoverApprovalStatus::Rejected);
    assert_eq!(rejected.decided_by.as_deref(), Some("operator"));
    assert_eq!(
        rejected.decision_reason.as_deref(),
        Some("boundary rejected")
    );
    assert!(matches!(
        store.consume_takeover_approval(OWNER, &approval.approval_id, at(1_202)),
        Err(GoalStoreError::TakeoverApprovalNotExecutable {
            status: TakeoverApprovalStatus::Rejected,
            ..
        })
    ));
}

#[test]
fn consume_and_finish_are_one_shot_and_visible() {
    let (dir, store) = setup();
    let request = request(&store, &dir);
    let approval = store
        .queue_takeover_approval(OWNER, &request, at(1_200))
        .unwrap();
    store
        .decide_takeover_approval(
            OWNER,
            &approval.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(1_201),
        )
        .unwrap();
    let consumed = store
        .consume_takeover_approval(OWNER, &approval.approval_id, at(1_202))
        .unwrap();
    assert_eq!(consumed.status, TakeoverApprovalStatus::InFlight);
    assert_eq!(
        store
            .get(OWNER, "goal-a")
            .unwrap()
            .unwrap()
            .continuity_state
            .unwrap()
            .handoff_plan
            .steps[0]
            .status,
        GoalContinuityStepStatus::InFlight
    );
    assert!(matches!(
        store.consume_takeover_approval(OWNER, &approval.approval_id, at(1_203)),
        Err(GoalStoreError::TakeoverApprovalNotExecutable {
            status: TakeoverApprovalStatus::InFlight,
            ..
        })
    ));

    let finished = store
        .finish_takeover_approval(
            OWNER,
            &approval.approval_id,
            &TakeoverFinish {
                run_id: Some("run-a".into()),
                run_status: TakeoverRunStatus::Success,
                detail: "completed".into(),
            },
            at(1_204),
        )
        .unwrap();
    assert_eq!(finished.status, TakeoverApprovalStatus::Done);
    assert_eq!(finished.run_id.as_deref(), Some("run-a"));
    assert_eq!(
        store
            .get(OWNER, "goal-a")
            .unwrap()
            .unwrap()
            .continuity_state
            .unwrap()
            .handoff_plan
            .steps[0]
            .status,
        GoalContinuityStepStatus::Done
    );
}

#[test]
fn stale_goal_revision_cannot_consume_approval() {
    let (dir, store) = setup();
    let request = request(&store, &dir);
    let approval = store
        .queue_takeover_approval(OWNER, &request, at(1_200))
        .unwrap();
    store
        .decide_takeover_approval(
            OWNER,
            &approval.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(1_201),
        )
        .unwrap();
    store
        .progress(OWNER, "goal-a", "unrelated", "revision changed", at(1_202))
        .unwrap();
    assert!(matches!(
        store.consume_takeover_approval(OWNER, &approval.approval_id, at(1_203)),
        Err(GoalStoreError::TakeoverBoundaryChanged { .. })
    ));
}

#[test]
fn blocked_finish_and_owner_scope_are_persisted() {
    let (dir, store) = setup();
    let request = request(&store, &dir);
    let approval = store
        .queue_takeover_approval(OWNER, &request, at(1_200))
        .unwrap();
    assert!(
        store
            .get_takeover_approval("other", &approval.approval_id)
            .unwrap()
            .is_none()
    );
    store
        .decide_takeover_approval(
            OWNER,
            &approval.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(1_201),
        )
        .unwrap();
    assert!(matches!(
        store.finish_takeover_approval(
            OWNER,
            &approval.approval_id,
            &TakeoverFinish {
                run_id: None,
                run_status: TakeoverRunStatus::Error,
                detail: "not started".into(),
            },
            at(1_202),
        ),
        Err(GoalStoreError::TakeoverApprovalNotExecutable {
            status: TakeoverApprovalStatus::Approved,
            ..
        })
    ));
    store
        .consume_takeover_approval(OWNER, &approval.approval_id, at(1_203))
        .unwrap();
    let blocked = store
        .finish_takeover_approval(
            OWNER,
            &approval.approval_id,
            &TakeoverFinish {
                run_id: Some("run-failed".into()),
                run_status: TakeoverRunStatus::Error,
                detail: "dispatch failed".into(),
            },
            at(1_204),
        )
        .unwrap();
    assert_eq!(blocked.status, TakeoverApprovalStatus::Blocked);
    assert_eq!(blocked.blocker_reason.as_deref(), Some("dispatch failed"));
    let state = store
        .get(OWNER, "goal-a")
        .unwrap()
        .unwrap()
        .continuity_state
        .unwrap();
    assert_eq!(
        state.handoff_plan.steps[0].status,
        GoalContinuityStepStatus::Blocked
    );
    assert_eq!(
        state
            .handoff_plan
            .steps
            .iter()
            .find(|step| step.id == "review_takeover")
            .unwrap()
            .status,
        GoalContinuityStepStatus::WaitingForTakeover
    );
    assert!(state.handoff_plan.next_ready_step.is_empty());
}

#[test]
fn store_rejects_noncanonical_workdir_boundary() {
    let (dir, store) = setup();
    let nested = dir.path().join("nested");
    std::fs::create_dir(&nested).unwrap();
    let mut request = request(&store, &dir);
    request.workdir = nested.join("..");
    assert!(matches!(
        store.queue_takeover_approval(OWNER, &request, at(1_200)),
        Err(GoalStoreError::InvalidInput(_))
    ));
}

#[test]
fn tampered_approval_boundary_fails_integrity_read() {
    let (dir, store) = setup();
    let request = request(&store, &dir);
    let approval = store
        .queue_takeover_approval(OWNER, &request, at(1_200))
        .unwrap();
    let connection = Connection::open(dir.path().join("goals.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE goal_takeover_approvals SET timeout_seconds = timeout_seconds + 1 WHERE approval_id = ?1",
            [&approval.approval_id],
        )
        .unwrap();
    assert!(matches!(
        store.get_takeover_approval(OWNER, &approval.approval_id),
        Err(GoalStoreError::Sqlite(_))
    ));
}

#[test]
fn removed_workdir_does_not_make_approval_unreadable() {
    let (dir, store) = setup();
    let workdir = TempDir::new().unwrap();
    let mut request = request(&store, &dir);
    request.workdir = std::fs::canonicalize(workdir.path()).unwrap();
    let approval = store
        .queue_takeover_approval(OWNER, &request, at(1_200))
        .unwrap();
    workdir.close().unwrap();
    assert_eq!(
        store
            .get_takeover_approval(OWNER, &approval.approval_id)
            .unwrap()
            .unwrap()
            .status,
        TakeoverApprovalStatus::Pending
    );
}

#[test]
fn successful_takeover_releases_bound_review_and_review_hands_back_safely() {
    let (dir, store) = setup();
    let takeover_request = request(&store, &dir);
    let takeover = store
        .queue_takeover_approval(OWNER, &takeover_request, at(1_200))
        .unwrap();
    store
        .decide_takeover_approval(
            OWNER,
            &takeover.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(1_201),
        )
        .unwrap();
    store
        .consume_takeover_approval(OWNER, &takeover.approval_id, at(1_202))
        .unwrap();
    let takeover = store
        .finish_takeover_approval(
            OWNER,
            &takeover.approval_id,
            &TakeoverFinish {
                run_id: Some("takeover-run".into()),
                run_status: TakeoverRunStatus::Success,
                detail: "takeover completed".into(),
            },
            at(1_203),
        )
        .unwrap();

    let goal = store.get(OWNER, "goal-a").unwrap().unwrap();
    let state = goal.continuity_state.clone().unwrap();
    let review = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.id == "review_takeover")
        .unwrap();
    assert_eq!(review.status, GoalContinuityStepStatus::Ready);
    assert_eq!(state.handoff_plan.next_ready_step, "review_takeover");
    let review_request = TakeoverApprovalRequest {
        goal_id: goal.id,
        step_id: review.id.clone(),
        step_kind: review.kind.clone(),
        quota_event_id: state.quota_event_id.clone(),
        target: TakeoverBoundTarget::from_execution(review.target.as_ref().unwrap()),
        workdir: std::fs::canonicalize(dir.path()).unwrap(),
        sandbox: TakeoverSandbox::ReadOnly,
        timeout: Duration::from_secs(300),
        goal_revision: goal.revision,
        plan_snapshot: state,
        upstream_approval_id: Some(takeover.approval_id.clone()),
        upstream_run_id: takeover.run_id.clone(),
        upstream_run_status: takeover.run_status,
    };
    let review_approval = store
        .queue_takeover_approval(OWNER, &review_request, at(1_204))
        .unwrap();
    assert_eq!(
        review_approval.upstream_approval_id.as_deref(),
        Some(takeover.approval_id.as_str())
    );
    assert_eq!(
        review_approval.upstream_run_id.as_deref(),
        Some("takeover-run")
    );
    store
        .decide_takeover_approval(
            OWNER,
            &review_approval.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(1_205),
        )
        .unwrap();
    store
        .consume_takeover_approval(OWNER, &review_approval.approval_id, at(1_206))
        .unwrap();
    store
        .finish_takeover_approval(
            OWNER,
            &review_approval.approval_id,
            &TakeoverFinish {
                run_id: Some("review-run".into()),
                run_status: TakeoverRunStatus::Success,
                detail: "review completed".into(),
            },
            at(1_207),
        )
        .unwrap();

    let state = store
        .get(OWNER, "goal-a")
        .unwrap()
        .unwrap()
        .continuity_state
        .unwrap();
    let review = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.id == "review_takeover")
        .unwrap();
    let resume = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.id == "resume_primary")
        .unwrap();
    assert_eq!(review.status, GoalContinuityStepStatus::Done);
    assert_eq!(
        resume.status,
        GoalContinuityStepStatus::WaitingForQuotaReset
    );
    assert!(state.handoff_plan.next_ready_step.is_empty());
}

#[test]
fn review_queue_rejects_forged_upstream_run_evidence() {
    let (dir, store) = setup();
    let mut forged = request(&store, &dir);
    forged.step_id = "review_takeover".into();
    forged.step_kind = "review_takeover_work".into();
    forged.upstream_approval_id = Some("continuity-forged".into());
    forged.upstream_run_id = Some("run-forged".into());
    forged.upstream_run_status = Some(TakeoverRunStatus::Success);
    assert!(matches!(
        store.queue_takeover_approval(OWNER, &forged, at(1_200)),
        Err(GoalStoreError::InvalidInput(_))
    ));
}

#[test]
fn review_queue_rejects_mismatched_existing_upstream_run() {
    let (dir, store) = setup();
    let takeover = complete_takeover(&store, &dir);
    let mut request = review_request(&store, &dir, &takeover);
    request.upstream_run_id = Some("different-run".into());
    let error = store
        .queue_takeover_approval(OWNER, &request, at(1_204))
        .unwrap_err();
    assert!(matches!(
        error,
        GoalStoreError::InvalidInput(ref message)
            if message == "review approval is not bound to the exact successful takeover run"
    ));
}

#[test]
fn quota_reset_before_review_waits_then_review_releases_resume() {
    let (dir, store) = setup();
    let result = store
        .record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002))
        .unwrap();
    assert!(result.changed);
    let resume = result
        .state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.id == "resume_primary")
        .unwrap();
    assert_eq!(resume.status, GoalContinuityStepStatus::WaitingForReview);

    complete_review(&store, &dir);
    let state = store
        .get(OWNER, "goal-a")
        .unwrap()
        .unwrap()
        .continuity_state
        .unwrap();
    let resume = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.id == "resume_primary")
        .unwrap();
    assert_eq!(resume.status, GoalContinuityStepStatus::Ready);
    assert_eq!(state.handoff_plan.next_ready_step, "resume_primary");
}

#[test]
fn review_before_quota_reset_releases_resume_without_dispatching() {
    let (dir, store) = setup();
    complete_review(&store, &dir);

    let result = store
        .record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002))
        .unwrap();

    let resume = result
        .state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.id == "resume_primary")
        .unwrap();
    assert_eq!(resume.status, GoalContinuityStepStatus::Ready);
    assert_eq!(result.state.handoff_plan.next_ready_step, "resume_primary");
    assert_eq!(result.state.ready_signals, vec![quota_reset_signal()]);
}

#[test]
fn repeated_signal_evidence_is_idempotent_without_revision_or_event_drift() {
    let (_dir, store) = setup();
    let signal = quota_reset_signal();
    let first = store
        .record_continuity_signal(OWNER, "goal-a", &signal, at(2_002))
        .unwrap();
    let revision = store.get(OWNER, "goal-a").unwrap().unwrap().revision;
    let event_count = store.events(OWNER, "goal-a").unwrap().len();

    let mut retried = signal.clone();
    retried.observed_at = at(2_100);
    let repeated = store
        .record_continuity_signal(OWNER, "goal-a", &retried, at(2_003))
        .unwrap();

    assert!(first.changed);
    assert!(!repeated.changed);
    assert_eq!(repeated.signal, signal);
    assert_eq!(
        store.get(OWNER, "goal-a").unwrap().unwrap().revision,
        revision
    );
    assert_eq!(store.events(OWNER, "goal-a").unwrap().len(), event_count);
}

#[test]
fn continuity_signal_conflicts_and_wrong_primary_boundaries_fail_closed() {
    let (_dir, store) = setup();
    let signal = quota_reset_signal();
    store
        .record_continuity_signal(OWNER, "goal-a", &signal, at(2_002))
        .unwrap();
    let mut conflicting = signal.clone();
    conflicting.source = "different-reader".into();
    assert!(matches!(
        store.record_continuity_signal(OWNER, "goal-a", &conflicting, at(2_003)),
        Err(GoalStoreError::InvalidInput(ref message))
            if message == "continuity signal kind was already recorded with different evidence"
    ));

    for wrong in ["quota", "provider", "harness", "model"] {
        let (_dir, store) = setup();
        let mut signal = quota_reset_signal();
        match wrong {
            "quota" => signal.quota_event_id = "quota-other".into(),
            "provider" => signal.provider = "provider-other".into(),
            "harness" => signal.harness = "harness-other".into(),
            "model" => signal.model = "model-other".into(),
            _ => unreachable!(),
        }
        assert!(matches!(
            store.record_continuity_signal(OWNER, "goal-a", &signal, at(2_002)),
            Err(GoalStoreError::InvalidInput(ref message))
                if message == "continuity signal does not match the current primary quota boundary"
        ));
    }
}

#[test]
fn continuity_signal_rejects_foreign_terminal_and_missing_state_goals() {
    let (_dir, store) = setup();
    assert!(matches!(
        store.record_continuity_signal("foreign", "goal-a", &quota_reset_signal(), at(2_002)),
        Err(GoalStoreError::NotFound { .. })
    ));
    store
        .fail(OWNER, "goal-a", None, "stopped", at(2_001))
        .unwrap();
    assert!(matches!(
        store.record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002)),
        Err(GoalStoreError::InvalidStatus { .. })
    ));

    let mut plain = NewGoal::new("plain goal", at(3_000));
    plain.id = Some("goal-plain".into());
    store.create(OWNER, plain).unwrap();
    store.start(OWNER, "goal-plain", at(3_001)).unwrap();
    assert!(matches!(
        store.record_continuity_signal(OWNER, "goal-plain", &quota_reset_signal(), at(3_002)),
        Err(GoalStoreError::InvalidInput(ref message))
            if message == "goal has no visible continuity state"
    ));
}

#[test]
fn empty_ready_signals_preserve_legacy_snapshot_serialization() {
    let (_dir, store) = setup();
    let state = store
        .get(OWNER, "goal-a")
        .unwrap()
        .unwrap()
        .continuity_state
        .unwrap();
    let value = serde_json::to_value(state).unwrap();
    assert!(value.get("ready_signals").is_none());
}
