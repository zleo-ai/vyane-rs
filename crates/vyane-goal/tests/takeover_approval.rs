#![allow(clippy::unwrap_used)]

use std::time::Duration;

use chrono::{TimeZone as _, Utc};
use rusqlite::Connection;
use tempfile::TempDir;
use vyane_goal::{
    GoalContinuityMode, GoalContinuityPolicy, GoalContinuityStepStatus, GoalExecutionTarget,
    GoalQuotaEvent, GoalStore, GoalStoreError, NewGoal, SCHEMA_VERSION, SqliteGoalStore,
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
    }
}

#[test]
fn schema_v6_contains_durable_takeover_approval_table() {
    let (dir, _store) = setup();
    assert_eq!(SCHEMA_VERSION, 6);
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
        Err(GoalStoreError::TakeoverApprovalNotExecutable { .. })
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
        Err(GoalStoreError::TakeoverApprovalNotExecutable { .. })
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
        Err(GoalStoreError::TakeoverApprovalNotExecutable { .. })
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
    store
        .consume_takeover_approval(OWNER, &approval.approval_id, at(1_202))
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
            at(1_203),
        )
        .unwrap();
    assert_eq!(blocked.status, TakeoverApprovalStatus::Blocked);
    assert_eq!(blocked.blocker_reason.as_deref(), Some("dispatch failed"));
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
        GoalContinuityStepStatus::Blocked
    );
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
