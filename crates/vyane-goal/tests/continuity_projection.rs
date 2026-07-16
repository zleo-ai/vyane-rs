#![allow(clippy::unwrap_used)]

use std::time::Duration;

use chrono::{TimeZone as _, Utc};
use tempfile::TempDir;
use vyane_goal::{
    GoalContinuityMode, GoalContinuityNextActionKind, GoalContinuityOperatorCommand,
    GoalContinuityPolicy, GoalContinuityReviewCheck, GoalContinuitySignal,
    GoalContinuitySignalKind, GoalExecutionTarget, GoalQuotaEvent, GoalStore, NewGoal,
    SqliteGoalStore, TakeoverApprovalRequest, TakeoverBoundTarget, TakeoverDecision,
    TakeoverFinish, TakeoverRunStatus, TakeoverSandbox, apply_quota_handoff_events,
    project_continuity_next_action,
};

fn target(role: &str) -> GoalExecutionTarget {
    GoalExecutionTarget {
        provider: "provider".into(),
        protocol: "openai_chat".into(),
        harness: "harness".into(),
        model: "model".into(),
        profile: None,
        role: role.into(),
    }
}

fn create_goal(store: &SqliteGoalStore, id: &str, with_takeover: bool) -> vyane_goal::GoalRecord {
    let mut goal = NewGoal::new("projection", Utc.timestamp_opt(1_000, 0).unwrap());
    goal.id = Some(id.into());
    goal.continuity_policy = Some(GoalContinuityPolicy {
        mode: GoalContinuityMode::QuotaHandoff,
        primary: target("primary"),
        takeover: if with_takeover {
            vec![target("takeover")]
        } else {
            Vec::new()
        },
        reviewer: Some(target("reviewer")),
        resume_primary_after_reset: true,
        require_review_before_resume: true,
        wait_for_review_checks_before_resume: true,
    });
    store.create("local", goal).unwrap();
    store
        .start("local", id, Utc.timestamp_opt(1_001, 0).unwrap())
        .unwrap();
    apply_quota_handoff_events(
        store,
        "local",
        &[GoalQuotaEvent {
            event_id: format!("quota-{id}"),
            goal_id: Some(id.into()),
            provider: "provider".into(),
            harness: "harness".into(),
            model: "model".into(),
            session_id: None,
            observed_at: Utc.timestamp_opt(1_002, 0).unwrap(),
            estimated_reset_at: None,
        }],
        Utc.timestamp_opt(1_003, 0).unwrap(),
    )
    .unwrap();
    store.get("local", id).unwrap().unwrap()
}

fn queue_current(
    store: &SqliteGoalStore,
    goal: &vyane_goal::GoalRecord,
    workdir: &TempDir,
) -> vyane_goal::TakeoverApproval {
    queue_current_with_upstream(store, goal, workdir, None)
}

fn queue_current_with_upstream(
    store: &SqliteGoalStore,
    goal: &vyane_goal::GoalRecord,
    workdir: &TempDir,
    upstream: Option<&vyane_goal::TakeoverApproval>,
) -> vyane_goal::TakeoverApproval {
    let state = goal.continuity_state.as_ref().unwrap();
    let step = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.status == vyane_goal::GoalContinuityStepStatus::Ready)
        .unwrap();
    store
        .queue_takeover_approval(
            "local",
            &TakeoverApprovalRequest {
                goal_id: goal.id.clone(),
                step_id: step.id.clone(),
                step_kind: step.kind.clone(),
                quota_event_id: state.quota_event_id.clone(),
                target: TakeoverBoundTarget::from_execution(step.target.as_ref().unwrap()),
                workdir: workdir.path().canonicalize().unwrap(),
                sandbox: TakeoverSandbox::Write,
                timeout: Duration::from_secs(30),
                goal_revision: goal.revision,
                plan_snapshot: state.clone(),
                upstream_approval_id: upstream.map(|value| value.approval_id.clone()),
                upstream_run_id: upstream.and_then(|value| value.run_id.clone()),
                upstream_run_status: upstream.and_then(|value| value.run_status),
            },
            Utc.timestamp_opt(1_004, 0).unwrap(),
        )
        .unwrap()
}

fn approve_and_finish(
    store: &SqliteGoalStore,
    approval: &vyane_goal::TakeoverApproval,
    sequence: i64,
) -> vyane_goal::TakeoverApproval {
    store
        .decide_takeover_approval(
            "local",
            &approval.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            Utc.timestamp_opt(sequence, 0).unwrap(),
        )
        .unwrap();
    store
        .consume_takeover_approval(
            "local",
            &approval.approval_id,
            Utc.timestamp_opt(sequence + 1, 0).unwrap(),
        )
        .unwrap();
    store
        .finish_takeover_approval(
            "local",
            &approval.approval_id,
            &TakeoverFinish {
                run_id: Some(format!("run-{}", approval.step_id)),
                run_status: TakeoverRunStatus::Success,
                detail: "fixture success".into(),
            },
            Utc.timestamp_opt(sequence + 2, 0).unwrap(),
        )
        .unwrap()
}

#[test]
fn projection_moves_from_queue_to_decide_to_execute_without_mutation() {
    let directory = TempDir::new().unwrap();
    let workdir = TempDir::new().unwrap();
    let store = SqliteGoalStore::open(directory.path().join("goals.sqlite3")).unwrap();
    let goal = create_goal(&store, "approval-flow", true);

    let first = project_continuity_next_action(&goal, &[]).unwrap();
    assert_eq!(first.action, GoalContinuityNextActionKind::QueueApproval);
    assert_eq!(
        first.command,
        Some(GoalContinuityOperatorCommand::ContinuityQueue)
    );
    assert_eq!(
        first.required_inputs,
        ["workdir", "sandbox", "timeout_seconds"]
    );

    let queued = queue_current(&store, &goal, &workdir);
    let approvals = store
        .list_takeover_approvals("local", Some(&goal.id))
        .unwrap();
    let pending = project_continuity_next_action(&goal, &approvals).unwrap();
    assert_eq!(pending.action, GoalContinuityNextActionKind::DecideApproval);
    assert_eq!(
        pending.approval_id.as_deref(),
        Some(queued.approval_id.as_str())
    );

    store
        .decide_takeover_approval(
            "local",
            &queued.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            Utc.timestamp_opt(1_005, 0).unwrap(),
        )
        .unwrap();
    let approvals = store
        .list_takeover_approvals("local", Some(&goal.id))
        .unwrap();
    let approved = project_continuity_next_action(&goal, &approvals).unwrap();
    assert_eq!(
        approved.action,
        GoalContinuityNextActionKind::ExecuteApproval
    );
    assert_eq!(
        store.get("local", &goal.id).unwrap().unwrap(),
        goal,
        "projection must not mutate the goal"
    );
}

#[test]
fn projection_reports_in_flight_and_blocked_execution_from_durable_evidence() {
    let directory = TempDir::new().unwrap();
    let workdir = TempDir::new().unwrap();
    let store = SqliteGoalStore::open(directory.path().join("goals.sqlite3")).unwrap();
    let goal = create_goal(&store, "execution-flow", true);
    let queued = queue_current(&store, &goal, &workdir);
    store
        .decide_takeover_approval(
            "local",
            &queued.approval_id,
            TakeoverDecision::Approve,
            "operator",
            None,
            Utc.timestamp_opt(1_005, 0).unwrap(),
        )
        .unwrap();
    store
        .consume_takeover_approval(
            "local",
            &queued.approval_id,
            Utc.timestamp_opt(1_006, 0).unwrap(),
        )
        .unwrap();

    let current = store.get("local", &goal.id).unwrap().unwrap();
    let approvals = store
        .list_takeover_approvals("local", Some(&goal.id))
        .unwrap();
    let in_flight = project_continuity_next_action(&current, &approvals).unwrap();
    assert_eq!(
        in_flight.action,
        GoalContinuityNextActionKind::WaitForExecution
    );
    assert_eq!(
        in_flight.approval_id.as_deref(),
        Some(queued.approval_id.as_str())
    );

    store
        .finish_takeover_approval(
            "local",
            &queued.approval_id,
            &TakeoverFinish {
                run_id: Some("run-1".into()),
                run_status: TakeoverRunStatus::Error,
                detail: "fixture failure".into(),
            },
            Utc.timestamp_opt(1_007, 0).unwrap(),
        )
        .unwrap();
    let current = store.get("local", &goal.id).unwrap().unwrap();
    let approvals = store
        .list_takeover_approvals("local", Some(&goal.id))
        .unwrap();
    let blocked = project_continuity_next_action(&current, &approvals).unwrap();
    assert_eq!(
        blocked.action,
        GoalContinuityNextActionKind::ResolveBlockedExecution
    );
    assert_eq!(blocked.reason, "fixture failure");
}

#[test]
fn projection_exposes_manual_decision_without_inventing_a_command() {
    let directory = TempDir::new().unwrap();
    let store = SqliteGoalStore::open(directory.path().join("goals.sqlite3")).unwrap();
    let goal = create_goal(&store, "manual", false);

    let action = project_continuity_next_action(&goal, &[]).unwrap();
    assert_eq!(action.action, GoalContinuityNextActionKind::ManualDecision);
    assert_eq!(action.command, None);
    assert!(action.required_inputs.is_empty());
}

#[test]
fn projection_lists_only_signals_accepted_by_the_current_boundary() {
    let directory = TempDir::new().unwrap();
    let workdir = TempDir::new().unwrap();
    let store = SqliteGoalStore::open(directory.path().join("goals.sqlite3")).unwrap();
    let goal = create_goal(&store, "signal-flow", true);
    let takeover = queue_current(&store, &goal, &workdir);
    let takeover = approve_and_finish(&store, &takeover, 1_010);
    let goal = store.get("local", "signal-flow").unwrap().unwrap();
    let review = queue_current_with_upstream(&store, &goal, &workdir, Some(&takeover));
    approve_and_finish(&store, &review, 1_020);

    let goal = store.get("local", "signal-flow").unwrap().unwrap();
    let approvals = store
        .list_takeover_approvals("local", Some("signal-flow"))
        .unwrap();
    let waiting = project_continuity_next_action(&goal, &approvals).unwrap();
    assert_eq!(waiting.action, GoalContinuityNextActionKind::RecordSignal);
    assert_eq!(
        waiting.accepted_signals,
        [
            GoalContinuitySignalKind::QuotaReset,
            GoalContinuitySignalKind::ReviewChecksPassed,
            GoalContinuitySignalKind::ReviewChecksFailed,
        ]
    );

    let state = goal.continuity_state.as_ref().unwrap();
    store
        .record_continuity_signal(
            "local",
            "signal-flow",
            &GoalContinuitySignal {
                kind: GoalContinuitySignalKind::QuotaReset,
                quota_event_id: state.quota_event_id.clone(),
                provider: state.primary.provider.clone(),
                harness: state.primary.harness.clone(),
                model: state.primary.model.clone(),
                observed_at: Utc.timestamp_opt(1_030, 0).unwrap(),
                source: "fixture".into(),
                review_check: None,
            },
            Utc.timestamp_opt(1_030, 0).unwrap(),
        )
        .unwrap();
    let goal = store.get("local", "signal-flow").unwrap().unwrap();
    let waiting = project_continuity_next_action(&goal, &approvals).unwrap();
    assert_eq!(
        waiting.accepted_signals,
        [
            GoalContinuitySignalKind::ReviewChecksPassed,
            GoalContinuitySignalKind::ReviewChecksFailed,
        ]
    );

    let state = goal.continuity_state.as_ref().unwrap();
    store
        .record_continuity_signal(
            "local",
            "signal-flow",
            &GoalContinuitySignal {
                kind: GoalContinuitySignalKind::ReviewChecksPassed,
                quota_event_id: state.quota_event_id.clone(),
                provider: state.primary.provider.clone(),
                harness: state.primary.harness.clone(),
                model: state.primary.model.clone(),
                observed_at: Utc.timestamp_opt(1_031, 0).unwrap(),
                source: "fixture".into(),
                review_check: Some(GoalContinuityReviewCheck {
                    repository: "example/project".into(),
                    pull_request: 1,
                    observation_id: "checks-1".into(),
                    observation_sequence: 1,
                }),
            },
            Utc.timestamp_opt(1_031, 0).unwrap(),
        )
        .unwrap();
    let goal = store.get("local", "signal-flow").unwrap().unwrap();
    let approvals = store
        .list_takeover_approvals("local", Some("signal-flow"))
        .unwrap();
    let resume = project_continuity_next_action(&goal, &approvals).unwrap();
    assert_eq!(resume.action, GoalContinuityNextActionKind::QueueApproval);
    assert_eq!(resume.step_id.as_deref(), Some("resume_primary"));

    let review = approvals
        .iter()
        .find(|approval| approval.step_id == "review_takeover")
        .unwrap();
    let resume = queue_current_with_upstream(&store, &goal, &workdir, Some(review));
    approve_and_finish(&store, &resume, 1_040);
    let goal = store.get("local", "signal-flow").unwrap().unwrap();
    let approvals = store
        .list_takeover_approvals("local", Some("signal-flow"))
        .unwrap();
    let complete = project_continuity_next_action(&goal, &approvals).unwrap();
    assert_eq!(
        complete.action,
        GoalContinuityNextActionKind::ContinuityComplete
    );
    assert_eq!(goal.status, vyane_goal::GoalStatus::InProgress);
}

#[test]
fn projection_rejects_approvals_from_another_goal() {
    let directory = TempDir::new().unwrap();
    let workdir = TempDir::new().unwrap();
    let store = SqliteGoalStore::open(directory.path().join("goals.sqlite3")).unwrap();
    let first = create_goal(&store, "first", true);
    let second = create_goal(&store, "second", true);
    let approval = queue_current(&store, &second, &workdir);

    let error = project_continuity_next_action(&first, &[approval]).unwrap_err();
    assert!(error.to_string().contains("exact goal owner and id"));
}

#[test]
fn signal_kinds_are_closed_and_serializable() {
    assert_eq!(
        serde_json::to_string(&GoalContinuitySignalKind::ReviewChecksPassed).unwrap(),
        "\"review_checks_passed\""
    );
}
