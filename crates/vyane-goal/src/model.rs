use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{GoalContinuityPolicy, GoalContinuityState, GoalStoreError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalStatus {
    Queued,
    InProgress,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl GoalStatus {
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::InProgress => "in_progress",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for GoalStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for GoalStatus {
    type Err = GoalStoreError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "in_progress" => Ok(Self::InProgress),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(GoalStoreError::CorruptData(format!(
                "unknown GoalStatus value `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalEventKind {
    Created,
    Started,
    Claimed,
    LeaseRenewed,
    Reclaimed,
    Progress,
    CriterionSatisfied,
    CriteriaWaived,
    Paused,
    Resumed,
    Completed,
    Failed,
    Cancelled,
}

impl GoalEventKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Started => "started",
            Self::Claimed => "claimed",
            Self::LeaseRenewed => "lease_renewed",
            Self::Reclaimed => "reclaimed",
            Self::Progress => "progress",
            Self::CriterionSatisfied => "criterion_satisfied",
            Self::CriteriaWaived => "criteria_waived",
            Self::Paused => "paused",
            Self::Resumed => "resumed",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl FromStr for GoalEventKind {
    type Err = GoalStoreError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "created" => Ok(Self::Created),
            "started" => Ok(Self::Started),
            "claimed" => Ok(Self::Claimed),
            "lease_renewed" => Ok(Self::LeaseRenewed),
            "reclaimed" => Ok(Self::Reclaimed),
            "progress" => Ok(Self::Progress),
            "criterion_satisfied" => Ok(Self::CriterionSatisfied),
            "criteria_waived" => Ok(Self::CriteriaWaived),
            "paused" => Ok(Self::Paused),
            "resumed" => Ok(Self::Resumed),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(GoalStoreError::CorruptData(format!(
                "unknown GoalEventKind value `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptanceCriterion {
    pub kind: String,
    pub target: String,
    pub satisfied_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CriterionStatus {
    Satisfied,
    Unsatisfied,
    Inconclusive,
    ManualRequired,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CriterionResult {
    pub criterion_index: usize,
    pub criterion_key: String,
    pub kind: String,
    pub target: String,
    pub status: CriterionStatus,
    pub command: Vec<String>,
    pub cwd: String,
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    pub stdout_tail: String,
    pub stderr_tail: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptanceVerification {
    pub goal_id: String,
    pub all_satisfied: bool,
    pub results: Vec<CriterionResult>,
    pub summary: String,
}

impl AcceptanceCriterion {
    pub fn new(kind: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            target: target.into(),
            satisfied_at: None,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("acceptance kind", &self.kind, 128)?;
        validate_text("acceptance target", &self.target, 4_096)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewGoal {
    pub id: Option<String>,
    pub title: String,
    pub description: String,
    pub priority: u8,
    pub parent_goal_id: Option<String>,
    pub acceptance_criteria: Vec<AcceptanceCriterion>,
    pub continuity_policy: Option<GoalContinuityPolicy>,
    pub created_at: DateTime<Utc>,
}

impl NewGoal {
    #[must_use]
    pub fn new(title: impl Into<String>, created_at: DateTime<Utc>) -> Self {
        Self {
            id: None,
            title: title.into(),
            description: String::new(),
            priority: 2,
            parent_goal_id: None,
            acceptance_criteria: Vec::new(),
            continuity_policy: None,
            created_at,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(id) = &self.id {
            validate_text("goal id", id, 256)?;
        }
        validate_text("title", &self.title, 1_024)?;
        validate_optional_text("description", &self.description, 65_536)?;
        if self.priority > 4 {
            return Err(GoalStoreError::InvalidInput(
                "priority must be between 0 and 4".into(),
            ));
        }
        if let Some(parent) = &self.parent_goal_id {
            validate_text("parent goal id", parent, 256)?;
        }
        if self.acceptance_criteria.len() > 64 {
            return Err(GoalStoreError::InvalidInput(
                "at most 64 acceptance criteria are allowed".into(),
            ));
        }
        for criterion in &self.acceptance_criteria {
            criterion.validate()?;
        }
        if let Some(policy) = &self.continuity_policy {
            policy.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalRecord {
    pub id: String,
    pub owner: String,
    pub title: String,
    pub description: String,
    pub status: GoalStatus,
    pub priority: u8,
    pub parent_goal_id: Option<String>,
    pub acceptance_criteria: Vec<AcceptanceCriterion>,
    pub continuity_policy: Option<GoalContinuityPolicy>,
    pub continuity_state: Option<GoalContinuityState>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub revision: u64,
    pub completion_summary: Option<String>,
    pub failure_reason: Option<String>,
    pub pause_reason: Option<String>,
    pub cancel_reason: Option<String>,
    /// Worker currently holding the execution lease, if any.
    pub claimed_by: Option<String>,
    /// Instant the current lease lapses; meaningful only while `claimed_by` is set.
    pub claim_expires_at: Option<DateTime<Utc>>,
    /// Fencing token: increments on every successful claim or reclaim.
    pub claim_generation: u64,
}

impl GoalRecord {
    /// Whether an unexpired execution lease is held at instant `now`.
    #[must_use]
    pub fn lease_active(&self, now: DateTime<Utc>) -> bool {
        self.claimed_by.is_some() && self.claim_expires_at.is_some_and(|expiry| expiry > now)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalEvent {
    pub sequence: u64,
    pub event_id: String,
    pub owner: String,
    pub goal_id: String,
    pub revision: u64,
    pub occurred_at: DateTime<Utc>,
    pub kind: GoalEventKind,
    pub from_status: Option<GoalStatus>,
    pub to_status: GoalStatus,
    pub stage: Option<String>,
    pub detail: Option<String>,
}

/// Immutable evidence captured for one acceptance-verification attempt.
///
/// The verification is stored as canonical JSON separately from the mutable
/// goal snapshot so repeated attempts remain auditable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalVerificationArtifact {
    pub sequence: u64,
    pub verification_id: String,
    pub owner: String,
    pub goal_id: String,
    pub recorded_at: DateTime<Utc>,
    pub worker_id: Option<String>,
    pub verification: AcceptanceVerification,
    pub payload_sha256: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GoalQuery {
    pub statuses: Vec<GoalStatus>,
    pub parent_goal_id: Option<String>,
    pub limit: usize,
}

/// Exclusive cursor for immutable resident-recovery ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalRecoveryCursor {
    pub priority: u8,
    pub created_at: DateTime<Utc>,
    pub id: String,
}

/// One bounded slice of the immutable recovery order. `next` is the cursor
/// after the last row examined, even when none of those rows matched the
/// requested recovery filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalRecoveryPage {
    pub candidates: Vec<GoalRecord>,
    pub next: Option<GoalRecoveryCursor>,
}

impl From<&GoalRecord> for GoalRecoveryCursor {
    fn from(goal: &GoalRecord) -> Self {
        Self {
            priority: goal.priority,
            created_at: goal.created_at,
            id: goal.id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalRecoveryFilter {
    ActiveWorker {
        worker_id: String,
        at: DateTime<Utc>,
    },
    Available {
        at: DateTime<Utc>,
    },
}

impl GoalQuery {
    #[must_use]
    pub fn with_default_limit() -> Self {
        Self {
            limit: 50,
            ..Self::default()
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.limit > 1_000 {
            return Err(GoalStoreError::InvalidInput(
                "limit must be between 0 and 1000".into(),
            ));
        }
        if let Some(parent) = &self.parent_goal_id {
            validate_text("parent goal id", parent, 256)?;
        }
        Ok(())
    }
}

impl GoalRecoveryCursor {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.priority > 4 {
            return Err(GoalStoreError::InvalidInput(
                "goal recovery cursor priority must be between 0 and 4".into(),
            ));
        }
        validate_goal_id(&self.id)
    }
}

/// Longest lease a worker may request, in seconds (one day).
pub const MAX_LEASE_SECONDS: u64 = 86_400;

pub(crate) fn validate_owner(owner: &str) -> Result<()> {
    validate_text("owner", owner, 256)
}

pub(crate) fn validate_worker(worker: &str) -> Result<()> {
    validate_text("worker id", worker, 256)
}

pub(crate) fn validate_lease_seconds(lease_seconds: u64) -> Result<()> {
    if lease_seconds == 0 || lease_seconds > MAX_LEASE_SECONDS {
        return Err(GoalStoreError::InvalidInput(format!(
            "lease seconds must be between 1 and {MAX_LEASE_SECONDS}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_goal_id(id: &str) -> Result<()> {
    validate_text("goal id", id, 256)
}

pub(crate) fn validate_stage(stage: &str) -> Result<()> {
    validate_text("progress stage", stage, 256)
}

pub(crate) fn validate_detail(detail: &str) -> Result<()> {
    validate_text("progress detail", detail, 16_384)
}

pub(crate) fn validate_optional_reason(field: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        validate_text(field, value, 16_384)?;
    }
    Ok(())
}

fn validate_optional_text(field: &str, value: &str, maximum: usize) -> Result<()> {
    if value.len() > maximum {
        return Err(GoalStoreError::InvalidInput(format!(
            "{field} must be at most {maximum} bytes"
        )));
    }
    Ok(())
}

pub(crate) fn validate_text(field: &str, value: &str, maximum: usize) -> Result<()> {
    if value.trim().is_empty() {
        return Err(GoalStoreError::InvalidInput(format!(
            "{field} must not be empty"
        )));
    }
    validate_optional_text(field, value, maximum)
}
