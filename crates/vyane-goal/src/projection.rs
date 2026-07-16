use serde::{Deserialize, Serialize};

use crate::{
    GoalContinuitySignalKind, GoalContinuityState, GoalContinuityStep, GoalContinuityStepStatus,
    GoalRecord, GoalStatus, GoalStoreError, Result, TakeoverApproval, TakeoverApprovalStatus,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalContinuityNextActionKind {
    QueueApproval,
    DecideApproval,
    ExecuteApproval,
    RecordSignal,
    WaitForDependency,
    WaitForExecution,
    ResolveBlockedExecution,
    ManualDecision,
    ContinuityComplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalContinuityOperatorCommand {
    ContinuityQueue,
    ContinuityDecide,
    ContinuityExecute,
    ContinuitySignal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalContinuityNextAction {
    pub goal_id: String,
    pub goal_revision: u64,
    pub quota_event_id: String,
    pub action: GoalContinuityNextActionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<GoalContinuityOperatorCommand>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub accepted_signals: Vec<GoalContinuitySignalKind>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_inputs: Vec<String>,
    pub reason: String,
}

/// Project the single operator-visible next action for the current continuity
/// boundary. This is a pure read: it never queues, decides, consumes or
/// dispatches anything.
pub fn project_continuity_next_action(
    goal: &GoalRecord,
    approvals: &[TakeoverApproval],
) -> Result<GoalContinuityNextAction> {
    if goal.status != GoalStatus::InProgress {
        return Err(GoalStoreError::InvalidInput(
            "continuity next action requires an in-progress goal".into(),
        ));
    }
    let state = goal.continuity_state.as_ref().ok_or_else(|| {
        GoalStoreError::InvalidInput("goal has no visible continuity state".into())
    })?;
    state.validate()?;
    for approval in approvals {
        if approval.owner != goal.owner || approval.goal_id != goal.id {
            return Err(GoalStoreError::InvalidInput(
                "continuity projection approvals must belong to the exact goal owner and id".into(),
            ));
        }
    }

    if let Some(step) = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.status == GoalContinuityStepStatus::InFlight)
    {
        let approval = latest_approval(approvals, state, step, TakeoverApprovalStatus::InFlight)?;
        return Ok(action_for_step(
            goal,
            state,
            step,
            GoalContinuityNextActionKind::WaitForExecution,
            None,
            Some(approval.approval_id.clone()),
            Vec::new(),
            Vec::new(),
            "the approved continuity execution is still in flight",
        ));
    }

    if let Some(step) = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.status == GoalContinuityStepStatus::Blocked)
    {
        let approval = latest_approval(approvals, state, step, TakeoverApprovalStatus::Blocked)?;
        return Ok(action_for_step(
            goal,
            state,
            step,
            GoalContinuityNextActionKind::ResolveBlockedExecution,
            None,
            Some(approval.approval_id.clone()),
            Vec::new(),
            Vec::new(),
            approval
                .blocker_reason
                .as_deref()
                .unwrap_or("the one-shot continuity execution is blocked"),
        ));
    }

    if let Some(step) = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.status == GoalContinuityStepStatus::Ready)
    {
        if !step.requires_approval || step.target.is_none() {
            return Ok(action_for_step(
                goal,
                state,
                step,
                GoalContinuityNextActionKind::ManualDecision,
                None,
                None,
                Vec::new(),
                Vec::new(),
                &step.reason,
            ));
        }
        let current = approvals.iter().rev().find(|approval| {
            approval.quota_event_id == state.quota_event_id
                && approval.step_id == step.id
                && approval.step_kind == step.kind
                && approval.goal_revision == goal.revision
                && approval.plan_snapshot == *state
        });
        return Ok(match current.map(|approval| approval.status) {
            None => action_for_step(
                goal,
                state,
                step,
                GoalContinuityNextActionKind::QueueApproval,
                Some(GoalContinuityOperatorCommand::ContinuityQueue),
                None,
                Vec::new(),
                vec!["workdir".into(), "sandbox".into(), "timeout_seconds".into()],
                "the ready continuity step has not been queued for approval",
            ),
            Some(TakeoverApprovalStatus::Pending) => {
                let approval = current.expect("matched approval exists");
                action_for_step(
                    goal,
                    state,
                    step,
                    GoalContinuityNextActionKind::DecideApproval,
                    Some(GoalContinuityOperatorCommand::ContinuityDecide),
                    Some(approval.approval_id.clone()),
                    Vec::new(),
                    vec!["decision".into(), "decided_by".into()],
                    "the queued continuity approval needs an explicit decision",
                )
            }
            Some(TakeoverApprovalStatus::Approved) => {
                let approval = current.expect("matched approval exists");
                action_for_step(
                    goal,
                    state,
                    step,
                    GoalContinuityNextActionKind::ExecuteApproval,
                    Some(GoalContinuityOperatorCommand::ContinuityExecute),
                    Some(approval.approval_id.clone()),
                    Vec::new(),
                    Vec::new(),
                    "the approved continuity step is ready for one-shot execution",
                )
            }
            Some(TakeoverApprovalStatus::Rejected) => {
                let approval = current.expect("matched approval exists");
                action_for_step(
                    goal,
                    state,
                    step,
                    GoalContinuityNextActionKind::ManualDecision,
                    None,
                    Some(approval.approval_id.clone()),
                    Vec::new(),
                    Vec::new(),
                    approval
                        .decision_reason
                        .as_deref()
                        .unwrap_or("the continuity approval was rejected"),
                )
            }
            Some(
                TakeoverApprovalStatus::InFlight
                | TakeoverApprovalStatus::Done
                | TakeoverApprovalStatus::Blocked,
            ) => {
                return Err(GoalStoreError::CorruptData(
                    "ready continuity step has a terminal or consumed current approval".into(),
                ));
            }
        });
    }

    let waits_for_quota = state.handoff_plan.steps.iter().any(|step| {
        matches!(
            step.status,
            GoalContinuityStepStatus::WaitingForQuotaReset
                | GoalContinuityStepStatus::WaitingForQuotaResetAndReview
        )
    });
    let quota_recorded = state
        .ready_signals
        .iter()
        .any(|signal| signal.kind == GoalContinuitySignalKind::QuotaReset);
    let waits_for_review_checks = state.handoff_plan.steps.iter().any(|step| {
        step.kind == "wait_for_review_checks"
            && matches!(
                step.status,
                GoalContinuityStepStatus::WaitingForReview
                    | GoalContinuityStepStatus::WaitingForReviewChecks
            )
    });
    let mut accepted_signals = Vec::new();
    if waits_for_quota && !quota_recorded {
        accepted_signals.push(GoalContinuitySignalKind::QuotaReset);
    }
    if waits_for_review_checks {
        accepted_signals.extend([
            GoalContinuitySignalKind::ReviewChecksPassed,
            GoalContinuitySignalKind::ReviewChecksFailed,
        ]);
    }
    if !accepted_signals.is_empty() {
        return Ok(base_action(
            goal,
            state,
            GoalContinuityNextActionKind::RecordSignal,
            Some(GoalContinuityOperatorCommand::ContinuitySignal),
            None,
            None,
            None,
            accepted_signals,
            vec!["signal_evidence".into()],
            "continuity is waiting for exact external readiness evidence",
        ));
    }

    let latest_review_signal = state
        .ready_signals
        .iter()
        .rev()
        .find(|signal| {
            matches!(
                signal.kind,
                GoalContinuitySignalKind::ReviewChecksPassed
                    | GoalContinuitySignalKind::ReviewChecksFailed
            )
        })
        .map(|signal| signal.kind);
    let review_failure_observed = state
        .ready_signals
        .iter()
        .any(|signal| signal.kind == GoalContinuitySignalKind::ReviewChecksFailed);
    if state.handoff_plan.steps.iter().all(|step| {
        step.status == GoalContinuityStepStatus::Done
            || (step.kind == "repair_review_failure"
                && step.status == GoalContinuityStepStatus::WaitingForReviewChecks
                && !review_failure_observed
                && latest_review_signal == Some(GoalContinuitySignalKind::ReviewChecksPassed))
    }) {
        return Ok(base_action(
            goal,
            state,
            GoalContinuityNextActionKind::ContinuityComplete,
            None,
            None,
            None,
            None,
            Vec::new(),
            Vec::new(),
            "the continuity handoff plan is complete; the goal remains in progress",
        ));
    }

    Ok(base_action(
        goal,
        state,
        GoalContinuityNextActionKind::WaitForDependency,
        None,
        None,
        None,
        None,
        Vec::new(),
        Vec::new(),
        "continuity is waiting for an internal predecessor step",
    ))
}

fn latest_approval<'a>(
    approvals: &'a [TakeoverApproval],
    state: &GoalContinuityState,
    step: &GoalContinuityStep,
    status: TakeoverApprovalStatus,
) -> Result<&'a TakeoverApproval> {
    let latest = approvals
        .iter()
        .filter(|approval| {
            approval.quota_event_id == state.quota_event_id
                && approval.step_id == step.id
                && approval.step_kind == step.kind
        })
        .max_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.approval_id.cmp(&right.approval_id))
        })
        .ok_or_else(|| {
            GoalStoreError::CorruptData(format!(
                "{} continuity step has no matching durable approval",
                step.id
            ))
        })?;
    if latest.status != status {
        return Err(GoalStoreError::CorruptData(format!(
            "{} continuity step is {} but its latest durable approval is {}",
            step.id,
            status.as_str(),
            latest.status.as_str()
        )));
    }
    Ok(latest)
}

#[allow(clippy::too_many_arguments)]
fn action_for_step(
    goal: &GoalRecord,
    state: &GoalContinuityState,
    step: &GoalContinuityStep,
    action: GoalContinuityNextActionKind,
    command: Option<GoalContinuityOperatorCommand>,
    approval_id: Option<String>,
    accepted_signals: Vec<GoalContinuitySignalKind>,
    required_inputs: Vec<String>,
    reason: &str,
) -> GoalContinuityNextAction {
    base_action(
        goal,
        state,
        action,
        command,
        Some(step.id.clone()),
        Some(step.kind.clone()),
        approval_id,
        accepted_signals,
        required_inputs,
        reason,
    )
}

#[allow(clippy::too_many_arguments)]
fn base_action(
    goal: &GoalRecord,
    state: &GoalContinuityState,
    action: GoalContinuityNextActionKind,
    command: Option<GoalContinuityOperatorCommand>,
    step_id: Option<String>,
    step_kind: Option<String>,
    approval_id: Option<String>,
    accepted_signals: Vec<GoalContinuitySignalKind>,
    required_inputs: Vec<String>,
    reason: &str,
) -> GoalContinuityNextAction {
    GoalContinuityNextAction {
        goal_id: goal.id.clone(),
        goal_revision: goal.revision,
        quota_event_id: state.quota_event_id.clone(),
        action,
        command,
        step_id,
        step_kind,
        approval_id,
        accepted_signals,
        required_inputs,
        reason: reason.into(),
    }
}
