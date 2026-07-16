use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    GoalQuery, GoalRecord, GoalStatus, GoalStore, GoalStoreError, Result, model::validate_text,
};

const MAX_TARGETS: usize = 8;
const MAX_APPLIED_EVENTS: usize = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalContinuityMode {
    QuotaHandoff,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalExecutionTarget {
    pub provider: String,
    pub protocol: String,
    pub harness: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    pub role: String,
}

impl GoalExecutionTarget {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("continuity target provider", &self.provider, 128)?;
        validate_text("continuity target protocol", &self.protocol, 128)?;
        validate_text("continuity target harness", &self.harness, 128)?;
        validate_text("continuity target model", &self.model, 256)?;
        if let Some(profile) = &self.profile {
            validate_text("continuity target profile", profile, 128)?;
        }
        validate_text("continuity target role", &self.role, 64)
    }

    fn matches(&self, event: &GoalQuotaEvent) -> bool {
        (self.provider == event.provider || self.harness == event.harness)
            && (event.model.is_empty() || self.model == event.model)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalContinuityPolicy {
    pub mode: GoalContinuityMode,
    pub primary: GoalExecutionTarget,
    #[serde(default)]
    pub takeover: Vec<GoalExecutionTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer: Option<GoalExecutionTarget>,
    #[serde(default = "default_true")]
    pub resume_primary_after_reset: bool,
    #[serde(default = "default_true")]
    pub require_review_before_resume: bool,
}

impl GoalContinuityPolicy {
    pub(crate) fn validate(&self) -> Result<()> {
        self.primary.validate()?;
        if self.primary.role != "primary" {
            return Err(GoalStoreError::InvalidInput(
                "continuity primary target role must be `primary`".into(),
            ));
        }
        if self.takeover.len() > MAX_TARGETS {
            return Err(GoalStoreError::InvalidInput(
                "at most 8 continuity takeover targets are allowed".into(),
            ));
        }
        for target in &self.takeover {
            target.validate()?;
            if target.role != "takeover" {
                return Err(GoalStoreError::InvalidInput(
                    "continuity takeover target role must be `takeover`".into(),
                ));
            }
        }
        if let Some(reviewer) = &self.reviewer {
            reviewer.validate()?;
            if reviewer.role != "reviewer" {
                return Err(GoalStoreError::InvalidInput(
                    "continuity reviewer target role must be `reviewer`".into(),
                ));
            }
        }
        if self.require_review_before_resume && self.reviewer.is_none() {
            return Err(GoalStoreError::InvalidInput(
                "continuity reviewer is required before primary resume".into(),
            ));
        }
        Ok(())
    }
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalQuotaEvent {
    pub event_id: String,
    pub goal_id: Option<String>,
    pub provider: String,
    pub harness: String,
    pub model: String,
    pub session_id: Option<String>,
    pub observed_at: DateTime<Utc>,
    pub estimated_reset_at: Option<DateTime<Utc>>,
}

impl GoalQuotaEvent {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("quota event id", &self.event_id, 256)?;
        if let Some(goal_id) = &self.goal_id {
            validate_text("quota event goal id", goal_id, 256)?;
        }
        validate_text("quota event provider", &self.provider, 128)?;
        validate_text("quota event harness", &self.harness, 128)?;
        if !self.model.is_empty() {
            validate_text("quota event model", &self.model, 256)?;
        }
        if let Some(session_id) = &self.session_id {
            validate_text("quota event session id", session_id, 256)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalContinuityStatus {
    TakeoverReady,
    BlockedNoTakeover,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalContinuityStepStatus {
    Ready,
    WaitingForTakeover,
    WaitingForQuotaReset,
    WaitingForQuotaResetAndReview,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalContinuityStep {
    pub id: String,
    pub kind: String,
    pub status: GoalContinuityStepStatus,
    pub target: Option<GoalExecutionTarget>,
    pub requires_approval: bool,
    pub wait_for: Vec<String>,
    pub reason: String,
    pub estimated_ready_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalContinuityState {
    pub state: GoalContinuityStatus,
    pub quota_event_id: String,
    pub observed_at: DateTime<Utc>,
    pub session_id: Option<String>,
    pub primary: GoalExecutionTarget,
    pub takeover: Option<GoalExecutionTarget>,
    pub reviewer: Option<GoalExecutionTarget>,
    pub resume_primary_after_reset: bool,
    pub require_review_before_resume: bool,
    pub handoff_plan: GoalContinuityPlan,
    pub applied_quota_event_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalContinuityPlan {
    pub version: u32,
    pub state: GoalContinuityStatus,
    pub quota_event_id: String,
    pub next_ready_step: String,
    pub steps: Vec<GoalContinuityStep>,
}

impl GoalContinuityState {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("continuity quota event id", &self.quota_event_id, 256)?;
        self.primary.validate()?;
        if let Some(takeover) = &self.takeover {
            takeover.validate()?;
        }
        if let Some(reviewer) = &self.reviewer {
            reviewer.validate()?;
        }
        if self.applied_quota_event_ids.len() > MAX_APPLIED_EVENTS {
            return Err(GoalStoreError::InvalidInput(
                "continuity applied quota event history is too large".into(),
            ));
        }
        for event_id in &self.applied_quota_event_ids {
            validate_text("continuity applied quota event id", event_id, 256)?;
        }
        if self.handoff_plan.version != 1
            || self.handoff_plan.state != self.state
            || self.handoff_plan.quota_event_id != self.quota_event_id
            || self.handoff_plan.steps.len() > MAX_TARGETS
        {
            return Err(GoalStoreError::InvalidInput(
                "continuity handoff plan envelope is invalid".into(),
            ));
        }
        if !self.handoff_plan.next_ready_step.is_empty()
            && !self
                .handoff_plan
                .steps
                .iter()
                .any(|step| step.id == self.handoff_plan.next_ready_step)
        {
            return Err(GoalStoreError::InvalidInput(
                "continuity next ready step is not in the plan".into(),
            ));
        }
        for step in &self.handoff_plan.steps {
            validate_text("continuity step id", &step.id, 128)?;
            validate_text("continuity step kind", &step.kind, 128)?;
            validate_text("continuity step reason", &step.reason, 4_096)?;
            if let Some(target) = &step.target {
                target.validate()?;
            }
            if step.wait_for.len() > MAX_TARGETS {
                return Err(GoalStoreError::InvalidInput(
                    "continuity step dependencies are too large".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalContinuityAction {
    pub goal_id: String,
    pub state: GoalContinuityState,
}

pub fn apply_quota_handoff_events<S: GoalStore>(
    store: &S,
    owner: &str,
    events: &[GoalQuotaEvent],
    at: DateTime<Utc>,
) -> Result<Vec<GoalContinuityAction>> {
    let mut actions = Vec::new();
    for event in events {
        event.validate()?;
        let candidates = if let Some(goal_id) = &event.goal_id {
            store.get(owner, goal_id)?.into_iter().collect()
        } else {
            store.list(
                owner,
                &GoalQuery {
                    statuses: vec![GoalStatus::InProgress],
                    parent_goal_id: None,
                    limit: 1_000,
                },
            )?
        };
        for candidate in candidates {
            if !event_matches(&candidate, event) {
                continue;
            }
            if let Some(state) = store.record_quota_handoff(owner, &candidate.id, event, at)? {
                actions.push(GoalContinuityAction {
                    goal_id: candidate.id,
                    state,
                });
            }
        }
    }
    Ok(actions)
}

pub(crate) fn event_matches(record: &GoalRecord, event: &GoalQuotaEvent) -> bool {
    record.status == GoalStatus::InProgress
        && record.continuity_policy.as_ref().is_some_and(|policy| {
            event
                .goal_id
                .as_ref()
                .map_or_else(|| policy.primary.matches(event), |id| id == &record.id)
        })
}

pub(crate) fn state_for_event(
    record: &GoalRecord,
    event: &GoalQuotaEvent,
) -> Result<Option<GoalContinuityState>> {
    event.validate()?;
    if !event_matches(record, event) {
        return Ok(None);
    }
    let policy = record
        .continuity_policy
        .as_ref()
        .ok_or_else(|| GoalStoreError::CorruptData("continuity policy disappeared".into()))?;
    policy.validate()?;
    if record.continuity_state.as_ref().is_some_and(|state| {
        state
            .applied_quota_event_ids
            .iter()
            .any(|id| id == &event.event_id)
    }) {
        return Ok(None);
    }

    let takeover = policy.takeover.first().cloned();
    let (status, next_ready_step, steps) = if let Some(target) = takeover.clone() {
        let review_wait = policy.require_review_before_resume;
        let mut steps = vec![step(
            "takeover",
            "start_takeover",
            GoalContinuityStepStatus::Ready,
            Some(target),
            true,
            &[],
            "primary quota blocked",
            None,
        )];
        if review_wait {
            steps.push(step(
                "review_takeover",
                "review_takeover_work",
                GoalContinuityStepStatus::WaitingForTakeover,
                policy.reviewer.clone(),
                true,
                &["takeover"],
                "primary resume requires reviewer approval",
                None,
            ));
        }
        if policy.resume_primary_after_reset {
            steps.push(step(
                "resume_primary",
                "resume_primary_after_reset",
                if review_wait {
                    GoalContinuityStepStatus::WaitingForQuotaResetAndReview
                } else {
                    GoalContinuityStepStatus::WaitingForQuotaReset
                },
                Some(policy.primary.clone()),
                true,
                if review_wait {
                    &["quota_reset", "review_takeover"]
                } else {
                    &["quota_reset"]
                },
                "primary quota reset is expected",
                event.estimated_reset_at,
            ));
        }
        (
            GoalContinuityStatus::TakeoverReady,
            "takeover".to_string(),
            steps,
        )
    } else {
        (
            GoalContinuityStatus::BlockedNoTakeover,
            "manual_decision".to_string(),
            vec![step(
                "manual_decision",
                "manual_decision",
                GoalContinuityStepStatus::Ready,
                None,
                true,
                &[],
                "primary quota blocked and no takeover target is configured",
                None,
            )],
        )
    };
    let mut applied = record
        .continuity_state
        .as_ref()
        .map_or_else(Vec::new, |state| state.applied_quota_event_ids.clone());
    applied.push(event.event_id.clone());
    if applied.len() > MAX_APPLIED_EVENTS {
        applied.drain(..applied.len() - MAX_APPLIED_EVENTS);
    }
    let state = GoalContinuityState {
        state: status,
        quota_event_id: event.event_id.clone(),
        observed_at: event.observed_at,
        session_id: event.session_id.clone(),
        primary: policy.primary.clone(),
        takeover,
        reviewer: policy.reviewer.clone(),
        resume_primary_after_reset: policy.resume_primary_after_reset,
        require_review_before_resume: policy.require_review_before_resume,
        handoff_plan: GoalContinuityPlan {
            version: 1,
            state: status,
            quota_event_id: event.event_id.clone(),
            next_ready_step,
            steps,
        },
        applied_quota_event_ids: applied,
    };
    Ok(Some(state))
}

#[allow(clippy::too_many_arguments)]
fn step(
    id: &str,
    kind: &str,
    status: GoalContinuityStepStatus,
    target: Option<GoalExecutionTarget>,
    requires_approval: bool,
    wait_for: &[&str],
    reason: &str,
    estimated_ready_at: Option<DateTime<Utc>>,
) -> GoalContinuityStep {
    GoalContinuityStep {
        id: id.into(),
        kind: kind.into(),
        status,
        target,
        requires_approval,
        wait_for: wait_for.iter().map(|value| (*value).into()).collect(),
        reason: reason.into(),
        estimated_ready_at,
    }
}
