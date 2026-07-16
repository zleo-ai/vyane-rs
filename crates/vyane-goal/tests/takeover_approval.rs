#![allow(clippy::unwrap_used)]

use std::time::Duration;

use chrono::{TimeZone as _, Utc};
use rusqlite::Connection;
use tempfile::TempDir;
use vyane_goal::{
    GoalContinuityMode, GoalContinuityPolicy, GoalContinuityReviewCheck, GoalContinuitySignal,
    GoalContinuitySignalKind, GoalContinuityStepStatus, GoalExecutionTarget, GoalQuotaEvent,
    GoalStore, GoalStoreError, NewGoal, SCHEMA_VERSION, SqliteGoalStore, TakeoverApproval,
    TakeoverApprovalRequest, TakeoverApprovalStatus, TakeoverBoundTarget, TakeoverDecision,
    TakeoverFinish, TakeoverRunStatus, TakeoverSandbox, apply_quota_handoff_events,
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
        wait_for_review_checks_before_resume: false,
    }
}

fn setup() -> (TempDir, SqliteGoalStore) {
    setup_with_policy(policy())
}

fn setup_with_policy(policy: GoalContinuityPolicy) -> (TempDir, SqliteGoalStore) {
    let dir = TempDir::new().unwrap();
    let store = SqliteGoalStore::open(dir.path().join("goals.sqlite3")).unwrap();
    let mut goal = NewGoal::new("controlled takeover", at(1_000));
    goal.id = Some("goal-a".into());
    goal.continuity_policy = Some(policy);
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

fn review_check_signal(kind: GoalContinuitySignalKind, pull_request: u64) -> GoalContinuitySignal {
    let observation_id = match kind {
        GoalContinuitySignalKind::ReviewChecksPassed => "checks-passed-v1",
        GoalContinuitySignalKind::ReviewChecksFailed => "checks-failed-v1",
        GoalContinuitySignalKind::QuotaReset => unreachable!(),
    };
    let observation_sequence = match kind {
        GoalContinuitySignalKind::ReviewChecksFailed => 1,
        GoalContinuitySignalKind::ReviewChecksPassed => 2,
        GoalContinuitySignalKind::QuotaReset => unreachable!(),
    };
    review_check_signal_with_observation(kind, pull_request, observation_id, observation_sequence)
}

fn review_check_signal_with_observation(
    kind: GoalContinuitySignalKind,
    pull_request: u64,
    observation_id: &str,
    observation_sequence: u64,
) -> GoalContinuitySignal {
    GoalContinuitySignal {
        kind,
        quota_event_id: "quota-a".into(),
        provider: "primary-provider".into(),
        harness: "codex-cli".into(),
        model: "primary-model".into(),
        observed_at: at(2_100),
        source: "github-check-reader".into(),
        review_check: Some(GoalContinuityReviewCheck {
            repository: "example/vyane-rs".into(),
            pull_request,
            observation_id: observation_id.into(),
            observation_sequence,
        }),
    }
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
        review_check: None,
    }
}

fn complete_review(store: &SqliteGoalStore, dir: &TempDir) -> TakeoverApproval {
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
        .unwrap()
}

fn resume_request(
    store: &SqliteGoalStore,
    dir: &TempDir,
    review: &TakeoverApproval,
) -> TakeoverApprovalRequest {
    let goal = store.get(OWNER, "goal-a").unwrap().unwrap();
    let state = goal.continuity_state.clone().unwrap();
    let resume = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.id == "resume_primary")
        .unwrap();
    TakeoverApprovalRequest {
        goal_id: goal.id,
        step_id: resume.id.clone(),
        step_kind: resume.kind.clone(),
        quota_event_id: state.quota_event_id.clone(),
        target: TakeoverBoundTarget::from_execution(resume.target.as_ref().unwrap()),
        workdir: std::fs::canonicalize(dir.path()).unwrap(),
        sandbox: TakeoverSandbox::Write,
        timeout: Duration::from_secs(300),
        goal_revision: goal.revision,
        plan_snapshot: state,
        upstream_approval_id: Some(review.approval_id.clone()),
        upstream_run_id: review.run_id.clone(),
        upstream_run_status: review.run_status,
    }
}

fn downstream_request(
    store: &SqliteGoalStore,
    dir: &TempDir,
    step_id: &str,
    upstream: &TakeoverApproval,
) -> TakeoverApprovalRequest {
    let goal = store.get(OWNER, "goal-a").unwrap().unwrap();
    let state = goal.continuity_state.clone().unwrap();
    let step = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.id == step_id)
        .unwrap();
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
        upstream_approval_id: Some(upstream.approval_id.clone()),
        upstream_run_id: upstream.run_id.clone(),
        upstream_run_status: upstream.run_status,
    }
}

fn complete_downstream_step(
    store: &SqliteGoalStore,
    dir: &TempDir,
    step_id: &str,
    upstream: &TakeoverApproval,
    run_id: &str,
    seconds: i64,
) -> TakeoverApproval {
    let approval = store
        .queue_takeover_approval(
            OWNER,
            &downstream_request(store, dir, step_id, upstream),
            at(seconds),
        )
        .unwrap();
    store
        .decide_takeover_approval(
            OWNER,
            &approval.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(seconds + 1),
        )
        .unwrap();
    store
        .consume_takeover_approval(OWNER, &approval.approval_id, at(seconds + 2))
        .unwrap();
    store
        .finish_takeover_approval(
            OWNER,
            &approval.approval_id,
            &TakeoverFinish {
                run_id: Some(run_id.into()),
                run_status: TakeoverRunStatus::Success,
                detail: format!("{step_id} completed"),
            },
            at(seconds + 3),
        )
        .unwrap()
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
fn primary_resume_approval_binds_reviewed_chain_and_executes_once() {
    let (dir, store) = setup();
    let review = complete_review(&store, &dir);
    store
        .record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002))
        .unwrap();
    let request = resume_request(&store, &dir, &review);

    let approval = store
        .queue_takeover_approval(OWNER, &request, at(2_003))
        .unwrap();
    assert_eq!(approval.step_id, "resume_primary");
    assert_eq!(
        approval.upstream_approval_id.as_deref(),
        Some(review.approval_id.as_str())
    );
    store
        .decide_takeover_approval(
            OWNER,
            &approval.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(2_004),
        )
        .unwrap();
    let consumed = store
        .consume_takeover_approval(OWNER, &approval.approval_id, at(2_005))
        .unwrap();
    assert_eq!(consumed.status, TakeoverApprovalStatus::InFlight);
    assert!(matches!(
        store.consume_takeover_approval(OWNER, &approval.approval_id, at(2_006)),
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
                run_id: Some("primary-resume-run".into()),
                run_status: TakeoverRunStatus::Success,
                detail: "primary resume completed".into(),
            },
            at(2_007),
        )
        .unwrap();
    assert_eq!(finished.status, TakeoverApprovalStatus::Done);
    let goal = store.get(OWNER, "goal-a").unwrap().unwrap();
    assert_eq!(goal.status, vyane_goal::GoalStatus::InProgress);
    let resume = goal
        .continuity_state
        .unwrap()
        .handoff_plan
        .steps
        .into_iter()
        .find(|step| step.id == "resume_primary")
        .unwrap();
    assert_eq!(resume.status, GoalContinuityStepStatus::Done);
}

#[test]
fn primary_resume_queue_rejects_missing_or_drifted_review_chain() {
    let (dir, store) = setup();
    let review = complete_review(&store, &dir);
    store
        .record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002))
        .unwrap();

    let mut missing = resume_request(&store, &dir, &review);
    missing.upstream_approval_id = None;
    missing.upstream_run_id = None;
    missing.upstream_run_status = None;
    assert!(matches!(
        store.queue_takeover_approval(OWNER, &missing, at(2_003)),
        Err(GoalStoreError::InvalidInput(ref message))
            if message == "continuity approval requires exact successful predecessor evidence"
    ));

    let mut drifted = resume_request(&store, &dir, &review);
    drifted.upstream_run_id = Some("different-review-run".into());
    assert!(matches!(
        store.queue_takeover_approval(OWNER, &drifted, at(2_003)),
        Err(GoalStoreError::InvalidInput(ref message))
            if message == "primary resume approval is not bound to the exact successful review run"
    ));
}

#[test]
fn primary_resume_without_review_requires_no_predecessor_review() {
    let dir = TempDir::new().unwrap();
    let store = SqliteGoalStore::open(dir.path().join("goals.sqlite3")).unwrap();
    let mut no_review = policy();
    no_review.reviewer = None;
    no_review.require_review_before_resume = false;
    let mut goal = NewGoal::new("resume without review", at(1_000));
    goal.id = Some("goal-a".into());
    goal.continuity_policy = Some(no_review);
    store.create(OWNER, goal).unwrap();
    store.start(OWNER, "goal-a", at(1_001)).unwrap();
    apply_quota_handoff_events(
        &store,
        OWNER,
        &[GoalQuotaEvent {
            event_id: "quota-a".into(),
            goal_id: Some("goal-a".into()),
            provider: "primary-provider".into(),
            harness: "codex-cli".into(),
            model: "primary-model".into(),
            session_id: None,
            observed_at: at(1_100),
            estimated_reset_at: None,
        }],
        at(1_101),
    )
    .unwrap();
    complete_takeover(&store, &dir);
    store
        .record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002))
        .unwrap();
    let goal = store.get(OWNER, "goal-a").unwrap().unwrap();
    let state = goal.continuity_state.clone().unwrap();
    let resume = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.id == "resume_primary")
        .unwrap();
    let request = TakeoverApprovalRequest {
        goal_id: goal.id,
        step_id: resume.id.clone(),
        step_kind: resume.kind.clone(),
        quota_event_id: state.quota_event_id.clone(),
        target: TakeoverBoundTarget::from_execution(resume.target.as_ref().unwrap()),
        workdir: std::fs::canonicalize(dir.path()).unwrap(),
        sandbox: TakeoverSandbox::Write,
        timeout: Duration::from_secs(300),
        goal_revision: goal.revision,
        plan_snapshot: state,
        upstream_approval_id: None,
        upstream_run_id: None,
        upstream_run_status: None,
    };

    let approval = store
        .queue_takeover_approval(OWNER, &request, at(2_003))
        .unwrap();

    assert_eq!(approval.step_id, "resume_primary");
    assert!(approval.upstream_approval_id.is_none());
}

#[test]
fn primary_resume_consume_rejects_tampered_review_chain() {
    let (dir, store) = setup();
    let review = complete_review(&store, &dir);
    store
        .record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002))
        .unwrap();
    let approval = store
        .queue_takeover_approval(OWNER, &resume_request(&store, &dir, &review), at(2_003))
        .unwrap();
    store
        .decide_takeover_approval(
            OWNER,
            &approval.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(2_004),
        )
        .unwrap();
    Connection::open(dir.path().join("goals.sqlite3"))
        .unwrap()
        .execute(
            "UPDATE goal_takeover_approvals SET run_id = 'tampered-review-run' \
             WHERE approval_id = ?1",
            [&review.approval_id],
        )
        .unwrap();

    assert!(matches!(
        store.consume_takeover_approval(OWNER, &approval.approval_id, at(2_005)),
        Err(GoalStoreError::TakeoverBoundaryChanged { .. })
    ));
    let persisted = store
        .get_takeover_approval(OWNER, &approval.approval_id)
        .unwrap()
        .unwrap();
    assert_eq!(persisted.status, TakeoverApprovalStatus::Approved);
    let resume = store
        .get(OWNER, "goal-a")
        .unwrap()
        .unwrap()
        .continuity_state
        .unwrap()
        .handoff_plan
        .steps
        .into_iter()
        .find(|step| step.id == "resume_primary")
        .unwrap();
    assert_eq!(resume.status, GoalContinuityStepStatus::Ready);
}

#[test]
fn primary_resume_consume_rejects_tampered_takeover_ancestor() {
    let (dir, store) = setup();
    let review = complete_review(&store, &dir);
    store
        .record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002))
        .unwrap();
    let approval = store
        .queue_takeover_approval(OWNER, &resume_request(&store, &dir, &review), at(2_003))
        .unwrap();
    store
        .decide_takeover_approval(
            OWNER,
            &approval.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(2_004),
        )
        .unwrap();
    Connection::open(dir.path().join("goals.sqlite3"))
        .unwrap()
        .execute(
            "UPDATE goal_takeover_approvals SET run_id = 'tampered-takeover-run' \
             WHERE approval_id = ?1",
            [review.upstream_approval_id.as_ref().unwrap()],
        )
        .unwrap();

    assert!(matches!(
        store.consume_takeover_approval(OWNER, &approval.approval_id, at(2_005)),
        Err(GoalStoreError::TakeoverBoundaryChanged { .. })
    ));
    let persisted = store
        .get_takeover_approval(OWNER, &approval.approval_id)
        .unwrap()
        .unwrap();
    assert_eq!(persisted.status, TakeoverApprovalStatus::Approved);
    let resume = store
        .get(OWNER, "goal-a")
        .unwrap()
        .unwrap()
        .continuity_state
        .unwrap()
        .handoff_plan
        .steps
        .into_iter()
        .find(|step| step.id == "resume_primary")
        .unwrap();
    assert_eq!(resume.status, GoalContinuityStepStatus::Ready);
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
    assert!(value.get("review_observation_high_water").is_none());
    assert!(value.get("wait_for_review_checks_before_resume").is_none());
    for step in value["handoff_plan"]["steps"].as_array().unwrap() {
        assert!(step.get("failure_wait_for").is_none());
        assert!(step.get("failure_step").is_none());
    }
    let policy = serde_json::to_value(policy()).unwrap();
    assert!(policy.get("wait_for_review_checks_before_resume").is_none());
}

#[test]
fn passed_review_checks_release_primary_without_repair() {
    let mut gated = policy();
    gated.wait_for_review_checks_before_resume = true;
    let (dir, store) = setup_with_policy(gated);
    complete_review(&store, &dir);
    store
        .record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002))
        .unwrap();
    let passed = review_check_signal(GoalContinuitySignalKind::ReviewChecksPassed, 27);
    let result = store
        .record_continuity_signal(OWNER, "goal-a", &passed, at(2_101))
        .unwrap();

    let step = |id: &str| {
        result
            .state
            .handoff_plan
            .steps
            .iter()
            .find(|step| step.id == id)
            .unwrap()
            .status
    };
    assert_eq!(step("wait_review_checks"), GoalContinuityStepStatus::Done);
    assert_eq!(
        step("repair_failed_review"),
        GoalContinuityStepStatus::WaitingForReviewChecks
    );
    assert_eq!(step("resume_primary"), GoalContinuityStepStatus::Ready);
    assert_eq!(result.state.handoff_plan.next_ready_step, "resume_primary");
}

#[test]
fn failed_review_checks_require_approved_repair_before_primary_resume() {
    let mut gated = policy();
    gated.wait_for_review_checks_before_resume = true;
    let (dir, store) = setup_with_policy(gated);
    let review = complete_review(&store, &dir);
    store
        .record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002))
        .unwrap();
    store
        .record_continuity_signal(
            OWNER,
            "goal-a",
            &review_check_signal(GoalContinuitySignalKind::ReviewChecksFailed, 27),
            at(2_101),
        )
        .unwrap();
    let before_repair = store
        .record_continuity_signal(
            OWNER,
            "goal-a",
            &review_check_signal(GoalContinuitySignalKind::ReviewChecksPassed, 27),
            at(2_102),
        )
        .unwrap();
    let status = |id: &str| {
        before_repair
            .state
            .handoff_plan
            .steps
            .iter()
            .find(|step| step.id == id)
            .unwrap()
            .status
    };
    assert_eq!(
        status("repair_failed_review"),
        GoalContinuityStepStatus::Ready
    );
    assert_eq!(
        status("wait_review_checks"),
        GoalContinuityStepStatus::WaitingForReview
    );
    assert_ne!(status("resume_primary"), GoalContinuityStepStatus::Ready);

    let request = downstream_request(&store, &dir, "repair_failed_review", &review);
    let repair = store
        .queue_takeover_approval(OWNER, &request, at(2_103))
        .unwrap();
    store
        .decide_takeover_approval(
            OWNER,
            &repair.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(2_104),
        )
        .unwrap();
    store
        .consume_takeover_approval(OWNER, &repair.approval_id, at(2_105))
        .unwrap();
    let repair = store
        .finish_takeover_approval(
            OWNER,
            &repair.approval_id,
            &TakeoverFinish {
                run_id: Some("repair-run".into()),
                run_status: TakeoverRunStatus::Success,
                detail: "review checks repaired".into(),
            },
            at(2_106),
        )
        .unwrap();

    let goal = store.get(OWNER, "goal-a").unwrap().unwrap();
    let state = goal.continuity_state.unwrap();
    assert_eq!(
        state
            .handoff_plan
            .steps
            .iter()
            .find(|step| step.id == "wait_review_checks")
            .unwrap()
            .status,
        GoalContinuityStepStatus::Done
    );
    assert_eq!(state.handoff_plan.next_ready_step, "resume_primary");

    let resume = store
        .queue_takeover_approval(
            OWNER,
            &downstream_request(&store, &dir, "resume_primary", &repair),
            at(2_107),
        )
        .unwrap();
    assert_eq!(
        resume.upstream_approval_id.as_deref(),
        Some(repair.approval_id.as_str())
    );
    assert_eq!(resume.upstream_run_id.as_deref(), Some("repair-run"));
    store
        .decide_takeover_approval(
            OWNER,
            &resume.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(2_108),
        )
        .unwrap();
    Connection::open(dir.path().join("goals.sqlite3"))
        .unwrap()
        .execute(
            "UPDATE goal_takeover_approvals SET run_id = 'tampered-review-run' \
             WHERE approval_id = ?1",
            [&review.approval_id],
        )
        .unwrap();
    assert!(matches!(
        store.consume_takeover_approval(OWNER, &resume.approval_id, at(2_109)),
        Err(GoalStoreError::TakeoverBoundaryChanged { .. })
    ));
    assert_eq!(
        store
            .get_takeover_approval(OWNER, &resume.approval_id)
            .unwrap()
            .unwrap()
            .status,
        TakeoverApprovalStatus::Approved
    );
}

#[test]
fn review_check_signals_fail_closed_without_gate_or_exact_coordinates() {
    let (_dir, store) = setup();
    let signal = review_check_signal(GoalContinuitySignalKind::ReviewChecksPassed, 27);
    assert!(matches!(
        store.record_continuity_signal(OWNER, "goal-a", &signal, at(2_101)),
        Err(GoalStoreError::InvalidInput(ref message))
            if message == "review-check signal is not enabled for the current continuity plan"
    ));

    let mut gated = policy();
    gated.wait_for_review_checks_before_resume = true;
    let (_dir, store) = setup_with_policy(gated);
    let mut missing = signal;
    missing.review_check = None;
    assert!(matches!(
        store.record_continuity_signal(OWNER, "goal-a", &missing, at(2_101)),
        Err(GoalStoreError::InvalidInput(ref message))
            if message.contains("requires repository and pull request")
    ));
}

#[test]
fn failed_review_checks_wait_for_review_before_repair() {
    let mut gated = policy();
    gated.wait_for_review_checks_before_resume = true;
    let (dir, store) = setup_with_policy(gated);
    let takeover = complete_takeover(&store, &dir);
    let failed = store
        .record_continuity_signal(
            OWNER,
            "goal-a",
            &review_check_signal(GoalContinuitySignalKind::ReviewChecksFailed, 27),
            at(2_101),
        )
        .unwrap();
    assert_eq!(failed.state.handoff_plan.next_ready_step, "review_takeover");
    assert_eq!(
        failed
            .state
            .handoff_plan
            .steps
            .iter()
            .find(|step| step.id == "repair_failed_review")
            .unwrap()
            .status,
        GoalContinuityStepStatus::WaitingForReviewChecks
    );

    let review = store
        .queue_takeover_approval(OWNER, &review_request(&store, &dir, &takeover), at(2_102))
        .unwrap();
    store
        .decide_takeover_approval(
            OWNER,
            &review.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(2_103),
        )
        .unwrap();
    store
        .consume_takeover_approval(OWNER, &review.approval_id, at(2_104))
        .unwrap();
    store
        .finish_takeover_approval(
            OWNER,
            &review.approval_id,
            &TakeoverFinish {
                run_id: Some("review-run".into()),
                run_status: TakeoverRunStatus::Success,
                detail: "review completed".into(),
            },
            at(2_105),
        )
        .unwrap();
    let state = store
        .get(OWNER, "goal-a")
        .unwrap()
        .unwrap()
        .continuity_state
        .unwrap();
    assert_eq!(state.handoff_plan.next_ready_step, "repair_failed_review");
    assert_eq!(
        state
            .handoff_plan
            .steps
            .iter()
            .find(|step| step.id == "repair_failed_review")
            .unwrap()
            .status,
        GoalContinuityStepStatus::Ready
    );
}

#[test]
fn late_review_failure_reblocks_ready_resume_and_invalidates_approval() {
    let mut gated = policy();
    gated.wait_for_review_checks_before_resume = true;
    let (dir, store) = setup_with_policy(gated);
    let review = complete_review(&store, &dir);
    store
        .record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002))
        .unwrap();
    store
        .record_continuity_signal(
            OWNER,
            "goal-a",
            &review_check_signal(GoalContinuitySignalKind::ReviewChecksPassed, 27),
            at(2_101),
        )
        .unwrap();
    let resume = store
        .queue_takeover_approval(
            OWNER,
            &downstream_request(&store, &dir, "resume_primary", &review),
            at(2_102),
        )
        .unwrap();
    store
        .decide_takeover_approval(
            OWNER,
            &resume.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(2_103),
        )
        .unwrap();

    let failed = store
        .record_continuity_signal(
            OWNER,
            "goal-a",
            &review_check_signal_with_observation(
                GoalContinuitySignalKind::ReviewChecksFailed,
                27,
                "checks-failed-after-pass",
                3,
            ),
            at(2_104),
        )
        .unwrap();
    assert_eq!(
        failed.state.handoff_plan.next_ready_step,
        "repair_failed_review"
    );
    assert_eq!(
        failed
            .state
            .handoff_plan
            .steps
            .iter()
            .find(|step| step.id == "resume_primary")
            .unwrap()
            .status,
        GoalContinuityStepStatus::WaitingForReview
    );
    assert!(matches!(
        store.consume_takeover_approval(OWNER, &resume.approval_id, at(2_105)),
        Err(GoalStoreError::TakeoverBoundaryChanged { .. })
    ));
}

#[test]
fn passed_and_failed_signals_must_bind_the_same_pull_request() {
    let mut gated = policy();
    gated.wait_for_review_checks_before_resume = true;
    let (_dir, store) = setup_with_policy(gated);
    store
        .record_continuity_signal(
            OWNER,
            "goal-a",
            &review_check_signal(GoalContinuitySignalKind::ReviewChecksPassed, 27),
            at(2_101),
        )
        .unwrap();
    assert!(matches!(
        store.record_continuity_signal(
            OWNER,
            "goal-a",
            &review_check_signal(GoalContinuitySignalKind::ReviewChecksFailed, 28),
            at(2_102),
        ),
        Err(GoalStoreError::InvalidInput(ref message))
            if message == "review-check signals do not describe the same pull request"
    ));
}

#[test]
fn late_review_failure_is_rejected_after_primary_resume_starts() {
    for finish_resume in [false, true] {
        let mut gated = policy();
        gated.wait_for_review_checks_before_resume = true;
        let (dir, store) = setup_with_policy(gated);
        let review = complete_review(&store, &dir);
        store
            .record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002))
            .unwrap();
        store
            .record_continuity_signal(
                OWNER,
                "goal-a",
                &review_check_signal(GoalContinuitySignalKind::ReviewChecksPassed, 27),
                at(2_101),
            )
            .unwrap();
        let resume = store
            .queue_takeover_approval(
                OWNER,
                &downstream_request(&store, &dir, "resume_primary", &review),
                at(2_102),
            )
            .unwrap();
        store
            .decide_takeover_approval(
                OWNER,
                &resume.approval_id,
                TakeoverDecision::Approve,
                "operator",
                None,
                at(2_103),
            )
            .unwrap();
        store
            .consume_takeover_approval(OWNER, &resume.approval_id, at(2_104))
            .unwrap();
        if finish_resume {
            store
                .finish_takeover_approval(
                    OWNER,
                    &resume.approval_id,
                    &TakeoverFinish {
                        run_id: Some("resume-run".into()),
                        run_status: TakeoverRunStatus::Success,
                        detail: "primary resumed".into(),
                    },
                    at(2_105),
                )
                .unwrap();
        }

        let before = store.get(OWNER, "goal-a").unwrap().unwrap();
        assert!(matches!(
            store.record_continuity_signal(
                OWNER,
                "goal-a",
                &review_check_signal_with_observation(
                    GoalContinuitySignalKind::ReviewChecksFailed,
                    27,
                    "checks-failed-after-resume",
                    3,
                ),
                at(2_106),
            ),
            Err(GoalStoreError::InvalidInput(ref message))
                if message == "review-check failure arrived after primary resume started"
        ));
        let after = store.get(OWNER, "goal-a").unwrap().unwrap();
        assert_eq!(after.revision, before.revision);
        assert_eq!(after.continuity_state, before.continuity_state);
    }
}

#[test]
fn a_new_failure_after_repair_requires_another_repair_and_newer_pass() {
    let mut gated = policy();
    gated.wait_for_review_checks_before_resume = true;
    let (dir, store) = setup_with_policy(gated);
    let review = complete_review(&store, &dir);
    store
        .record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002))
        .unwrap();
    store
        .record_continuity_signal(
            OWNER,
            "goal-a",
            &review_check_signal(GoalContinuitySignalKind::ReviewChecksFailed, 27),
            at(2_101),
        )
        .unwrap();
    store
        .record_continuity_signal(
            OWNER,
            "goal-a",
            &review_check_signal(GoalContinuitySignalKind::ReviewChecksPassed, 27),
            at(2_102),
        )
        .unwrap();
    let first_repair = complete_downstream_step(
        &store,
        &dir,
        "repair_failed_review",
        &review,
        "repair-run-v1",
        2_103,
    );
    let state = store
        .get(OWNER, "goal-a")
        .unwrap()
        .unwrap()
        .continuity_state
        .unwrap();
    assert_eq!(state.handoff_plan.next_ready_step, "resume_primary");

    let second_failure = review_check_signal_with_observation(
        GoalContinuitySignalKind::ReviewChecksFailed,
        27,
        "checks-failed-v2",
        3,
    );
    let failed = store
        .record_continuity_signal(OWNER, "goal-a", &second_failure, at(2_107))
        .unwrap();
    let status = |id: &str| {
        failed
            .state
            .handoff_plan
            .steps
            .iter()
            .find(|step| step.id == id)
            .unwrap()
            .status
    };
    assert_eq!(
        status("repair_failed_review"),
        GoalContinuityStepStatus::Ready
    );
    assert_eq!(
        status("wait_review_checks"),
        GoalContinuityStepStatus::WaitingForReview
    );
    assert_eq!(
        status("resume_primary"),
        GoalContinuityStepStatus::WaitingForReview
    );
    assert_eq!(
        failed.state.handoff_plan.next_ready_step,
        "repair_failed_review"
    );

    let before_repeat = store.get(OWNER, "goal-a").unwrap().unwrap();
    let repeated = store
        .record_continuity_signal(OWNER, "goal-a", &second_failure, at(2_108))
        .unwrap();
    assert!(!repeated.changed);
    assert_eq!(
        store.get(OWNER, "goal-a").unwrap().unwrap().revision,
        before_repeat.revision
    );

    let second_repair = complete_downstream_step(
        &store,
        &dir,
        "repair_failed_review",
        &review,
        "repair-run-v2",
        2_109,
    );
    assert_ne!(second_repair.approval_id, first_repair.approval_id);
    let after_repair = store
        .get(OWNER, "goal-a")
        .unwrap()
        .unwrap()
        .continuity_state
        .unwrap();
    assert!(after_repair.handoff_plan.next_ready_step.is_empty());
    assert_eq!(
        after_repair
            .handoff_plan
            .steps
            .iter()
            .find(|step| step.id == "wait_review_checks")
            .unwrap()
            .status,
        GoalContinuityStepStatus::WaitingForReview
    );

    let second_pass = review_check_signal_with_observation(
        GoalContinuitySignalKind::ReviewChecksPassed,
        27,
        "checks-passed-v2",
        4,
    );
    let passed = store
        .record_continuity_signal(OWNER, "goal-a", &second_pass, at(2_113))
        .unwrap();
    assert_eq!(passed.state.handoff_plan.next_ready_step, "resume_primary");
    assert!(matches!(
        store.queue_takeover_approval(
            OWNER,
            &downstream_request(&store, &dir, "resume_primary", &first_repair),
            at(2_114),
        ),
        Err(GoalStoreError::InvalidInput(ref message))
            if message.contains("latest review-check failure")
    ));
    let resume = store
        .queue_takeover_approval(
            OWNER,
            &downstream_request(&store, &dir, "resume_primary", &second_repair),
            at(2_115),
        )
        .unwrap();
    assert_eq!(
        resume.upstream_approval_id.as_deref(),
        Some(second_repair.approval_id.as_str())
    );
}

#[test]
fn failure_during_inflight_repair_rearms_after_the_stale_repair_finishes() {
    let mut gated = policy();
    gated.wait_for_review_checks_before_resume = true;
    let (dir, store) = setup_with_policy(gated);
    let review = complete_review(&store, &dir);
    store
        .record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002))
        .unwrap();
    store
        .record_continuity_signal(
            OWNER,
            "goal-a",
            &review_check_signal(GoalContinuitySignalKind::ReviewChecksFailed, 27),
            at(2_101),
        )
        .unwrap();
    let stale_repair = store
        .queue_takeover_approval(
            OWNER,
            &downstream_request(&store, &dir, "repair_failed_review", &review),
            at(2_102),
        )
        .unwrap();
    store
        .decide_takeover_approval(
            OWNER,
            &stale_repair.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            at(2_103),
        )
        .unwrap();
    store
        .consume_takeover_approval(OWNER, &stale_repair.approval_id, at(2_104))
        .unwrap();

    store
        .record_continuity_signal(
            OWNER,
            "goal-a",
            &review_check_signal_with_observation(
                GoalContinuitySignalKind::ReviewChecksFailed,
                27,
                "checks-failed-v2",
                2,
            ),
            at(2_105),
        )
        .unwrap();
    store
        .finish_takeover_approval(
            OWNER,
            &stale_repair.approval_id,
            &TakeoverFinish {
                run_id: Some("stale-repair-run".into()),
                run_status: TakeoverRunStatus::Success,
                detail: "stale repair completed".into(),
            },
            at(2_106),
        )
        .unwrap();
    let state = store
        .get(OWNER, "goal-a")
        .unwrap()
        .unwrap()
        .continuity_state
        .unwrap();
    assert_eq!(state.handoff_plan.next_ready_step, "repair_failed_review");
    assert_eq!(
        state
            .handoff_plan
            .steps
            .iter()
            .find(|step| step.id == "repair_failed_review")
            .unwrap()
            .status,
        GoalContinuityStepStatus::Ready
    );

    store
        .record_continuity_signal(
            OWNER,
            "goal-a",
            &review_check_signal_with_observation(
                GoalContinuitySignalKind::ReviewChecksPassed,
                27,
                "checks-passed-v2",
                3,
            ),
            at(2_107),
        )
        .unwrap();
    let before_fresh_repair = store
        .get(OWNER, "goal-a")
        .unwrap()
        .unwrap()
        .continuity_state
        .unwrap();
    assert_eq!(
        before_fresh_repair.handoff_plan.next_ready_step,
        "repair_failed_review"
    );
    let fresh_repair = complete_downstream_step(
        &store,
        &dir,
        "repair_failed_review",
        &review,
        "fresh-repair-run",
        2_108,
    );
    let final_state = store
        .get(OWNER, "goal-a")
        .unwrap()
        .unwrap()
        .continuity_state
        .unwrap();
    assert_eq!(final_state.handoff_plan.next_ready_step, "resume_primary");
    let resume = store
        .queue_takeover_approval(
            OWNER,
            &downstream_request(&store, &dir, "resume_primary", &fresh_repair),
            at(2_112),
        )
        .unwrap();
    assert_eq!(
        resume.upstream_approval_id.as_deref(),
        Some(fresh_repair.approval_id.as_str())
    );
}

#[test]
fn superseded_review_observations_do_not_exhaust_continuity_state() {
    let mut gated = policy();
    gated.wait_for_review_checks_before_resume = true;
    let (_dir, store) = setup_with_policy(gated);
    for index in 1..=100 {
        let kind = if index % 2 == 0 {
            GoalContinuitySignalKind::ReviewChecksPassed
        } else {
            GoalContinuitySignalKind::ReviewChecksFailed
        };
        let signal = review_check_signal_with_observation(
            kind,
            27,
            &format!("checks-observation-{index}"),
            index as u64,
        );
        store
            .record_continuity_signal(OWNER, "goal-a", &signal, at(2_100 + index))
            .unwrap();
    }
    let state = store
        .get(OWNER, "goal-a")
        .unwrap()
        .unwrap()
        .continuity_state
        .unwrap();
    assert!(state.ready_signals.len() <= 2);
    assert_eq!(state.review_observation_high_water, 100);
    assert_eq!(
        state.ready_signals.last().unwrap().kind,
        GoalContinuitySignalKind::ReviewChecksPassed
    );
    assert_eq!(
        state
            .ready_signals
            .iter()
            .find(|signal| signal.kind == GoalContinuitySignalKind::ReviewChecksFailed)
            .unwrap()
            .review_check
            .as_ref()
            .unwrap()
            .observation_id,
        "checks-observation-99"
    );
    let events = store.events(OWNER, "goal-a").unwrap();
    let detail = events.last().unwrap().detail.as_deref().unwrap();
    assert!(detail.contains("review checks passed"));
    assert!(detail.contains("checks-observation-100"));
}

#[test]
fn replayed_pass_below_observation_high_water_cannot_reopen_the_gate() {
    let mut gated = policy();
    gated.wait_for_review_checks_before_resume = true;
    let (dir, store) = setup_with_policy(gated);
    complete_review(&store, &dir);
    store
        .record_continuity_signal(OWNER, "goal-a", &quota_reset_signal(), at(2_002))
        .unwrap();
    let old_pass = review_check_signal(GoalContinuitySignalKind::ReviewChecksPassed, 27);
    store
        .record_continuity_signal(OWNER, "goal-a", &old_pass, at(2_101))
        .unwrap();
    store
        .record_continuity_signal(
            OWNER,
            "goal-a",
            &review_check_signal_with_observation(
                GoalContinuitySignalKind::ReviewChecksFailed,
                27,
                "checks-failed-v2",
                3,
            ),
            at(2_102),
        )
        .unwrap();
    let before = store.get(OWNER, "goal-a").unwrap().unwrap();

    let replay = store
        .record_continuity_signal(OWNER, "goal-a", &old_pass, at(2_103))
        .unwrap();
    assert!(!replay.changed);
    assert_eq!(
        replay.state.handoff_plan.next_ready_step,
        "repair_failed_review"
    );
    let after = store.get(OWNER, "goal-a").unwrap().unwrap();
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.continuity_state, before.continuity_state);
}

#[test]
fn corrupt_continuity_plan_dependencies_fail_closed_on_read() {
    for mutation in ["duplicate", "missing_dependency", "missing_failure_step"] {
        let mut gated = policy();
        gated.wait_for_review_checks_before_resume = true;
        let (dir, store) = setup_with_policy(gated);
        let state = store
            .get(OWNER, "goal-a")
            .unwrap()
            .unwrap()
            .continuity_state
            .unwrap();
        let mut value = serde_json::to_value(state).unwrap();
        let steps = value["handoff_plan"]["steps"].as_array_mut().unwrap();
        match mutation {
            "duplicate" => steps.push(steps[0].clone()),
            "missing_dependency" => {
                steps[2]["wait_for"] = serde_json::json!(["missing-step"]);
            }
            "missing_failure_step" => {
                steps[2]["failure_step"] = serde_json::json!("missing-step");
            }
            _ => unreachable!(),
        }
        Connection::open(dir.path().join("goals.sqlite3"))
            .unwrap()
            .execute(
                "UPDATE goals SET continuity_state_json = ?1 WHERE owner = ?2 AND id = ?3",
                (serde_json::to_string(&value).unwrap(), OWNER, "goal-a"),
            )
            .unwrap();

        let error = store
            .get(OWNER, "goal-a")
            .expect_err("corrupt continuity plan must be rejected");
        let message = error.to_string();
        assert!(
            message.contains("duplicate step ids") || message.contains("not in the plan"),
            "unexpected error for {mutation}: {message}"
        );
    }
}
