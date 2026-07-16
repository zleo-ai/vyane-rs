use std::fmt;
use std::path::{Component, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    GoalContinuityState, GoalContinuityStepStatus, GoalExecutionTarget, GoalStoreError, Result,
    model::validate_text,
};

pub const MAX_TAKEOVER_TIMEOUT: Duration = Duration::from_secs(3_600);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TakeoverBoundTarget {
    pub profile: Option<String>,
    pub provider: String,
    pub protocol: String,
    pub harness: String,
    pub model: String,
}

impl TakeoverBoundTarget {
    #[must_use]
    pub fn from_execution(target: &GoalExecutionTarget) -> Self {
        Self {
            profile: target.profile.clone(),
            provider: target.provider.clone(),
            protocol: target.protocol.clone(),
            harness: target.harness.clone(),
            model: target.model.clone(),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(profile) = &self.profile {
            validate_text("takeover target profile", profile, 128)?;
        }
        validate_text("takeover target provider", &self.provider, 128)?;
        validate_text("takeover target protocol", &self.protocol, 128)?;
        validate_text("takeover target harness", &self.harness, 128)?;
        validate_text("takeover target model", &self.model, 256)
    }

    #[must_use]
    pub fn selector(&self) -> String {
        self.profile.as_ref().map_or_else(
            || format!("target:{}/{}", self.provider, self.model),
            |profile| format!("profile:{profile}"),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TakeoverSandbox {
    ReadOnly,
    Write,
    Full,
}

impl TakeoverSandbox {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::Write => "write",
            Self::Full => "full",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "read_only" => Ok(Self::ReadOnly),
            "write" => Ok(Self::Write),
            "full" => Ok(Self::Full),
            other => Err(GoalStoreError::CorruptData(format!(
                "unknown takeover sandbox `{other}`"
            ))),
        }
    }
}

impl fmt::Display for TakeoverSandbox {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TakeoverApprovalStatus {
    Pending,
    Approved,
    Rejected,
    InFlight,
    Done,
    Blocked,
}

impl TakeoverApprovalStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::InFlight => "in_flight",
            Self::Done => "done",
            Self::Blocked => "blocked",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "approved" => Ok(Self::Approved),
            "rejected" => Ok(Self::Rejected),
            "in_flight" => Ok(Self::InFlight),
            "done" => Ok(Self::Done),
            "blocked" => Ok(Self::Blocked),
            other => Err(GoalStoreError::CorruptData(format!(
                "unknown takeover approval status `{other}`"
            ))),
        }
    }
}

impl fmt::Display for TakeoverApprovalStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakeoverDecision {
    Approve,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TakeoverRunStatus {
    Success,
    Error,
    Timeout,
    Cancelled,
}

impl TakeoverRunStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn is_success(self) -> bool {
        matches!(self, Self::Success)
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "success" => Ok(Self::Success),
            "error" => Ok(Self::Error),
            "timeout" => Ok(Self::Timeout),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(GoalStoreError::CorruptData(format!(
                "unknown takeover run status `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TakeoverApprovalRequest {
    pub goal_id: String,
    pub step_id: String,
    pub step_kind: String,
    pub quota_event_id: String,
    pub target: TakeoverBoundTarget,
    pub workdir: PathBuf,
    pub sandbox: TakeoverSandbox,
    pub timeout: Duration,
    pub goal_revision: u64,
    pub plan_snapshot: GoalContinuityState,
}

impl TakeoverApprovalRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("takeover goal id", &self.goal_id, 256)?;
        validate_text("takeover step id", &self.step_id, 128)?;
        validate_text("takeover step kind", &self.step_kind, 128)?;
        validate_text("takeover quota event id", &self.quota_event_id, 256)?;
        self.target.validate()?;
        self.plan_snapshot.validate()?;
        if self.step_id != "takeover" || self.step_kind != "start_takeover" {
            return Err(GoalStoreError::InvalidInput(
                "only takeover/start_takeover can be approved in this layer".into(),
            ));
        }
        let workdir_text = self.workdir.to_str().ok_or_else(|| {
            GoalStoreError::InvalidInput("takeover workdir must be valid UTF-8".into())
        })?;
        validate_text("takeover workdir", workdir_text, 4_096)?;
        if !self.workdir.is_absolute() {
            return Err(GoalStoreError::InvalidInput(
                "takeover workdir must be canonical and absolute".into(),
            ));
        }
        if self
            .workdir
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(GoalStoreError::InvalidInput(
                "takeover workdir must be canonical and absolute".into(),
            ));
        }
        if self.timeout.is_zero() || self.timeout > MAX_TAKEOVER_TIMEOUT {
            return Err(GoalStoreError::InvalidInput(format!(
                "takeover timeout must be between 1 and {} seconds",
                MAX_TAKEOVER_TIMEOUT.as_secs()
            )));
        }
        Ok(())
    }

    pub(crate) fn validate_live_workdir(&self) -> Result<()> {
        let canonical = std::fs::canonicalize(&self.workdir).map_err(|error| {
            GoalStoreError::InvalidInput(format!(
                "takeover workdir cannot be canonicalized: {error}"
            ))
        })?;
        if canonical != self.workdir {
            return Err(GoalStoreError::InvalidInput(
                "takeover workdir must be canonical and absolute".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn snapshot_payload(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Snapshot<'a> {
            goal_id: &'a str,
            step_id: &'a str,
            step_kind: &'a str,
            quota_event_id: &'a str,
            target: &'a TakeoverBoundTarget,
            workdir: &'a PathBuf,
            sandbox: TakeoverSandbox,
            timeout_seconds: u64,
            goal_revision: u64,
            plan_snapshot: &'a GoalContinuityState,
        }
        Ok(serde_json::to_string(&Snapshot {
            goal_id: &self.goal_id,
            step_id: &self.step_id,
            step_kind: &self.step_kind,
            quota_event_id: &self.quota_event_id,
            target: &self.target,
            workdir: &self.workdir,
            sandbox: self.sandbox,
            timeout_seconds: self.timeout.as_secs(),
            goal_revision: self.goal_revision,
            plan_snapshot: &self.plan_snapshot,
        })?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TakeoverApproval {
    pub approval_id: String,
    pub owner: String,
    pub goal_id: String,
    pub step_id: String,
    pub step_kind: String,
    pub quota_event_id: String,
    pub snapshot_digest: String,
    pub target: TakeoverBoundTarget,
    pub workdir: PathBuf,
    pub sandbox: TakeoverSandbox,
    pub timeout_secs: u64,
    pub goal_revision: u64,
    pub plan_snapshot: GoalContinuityState,
    pub status: TakeoverApprovalStatus,
    pub decided_by: Option<String>,
    pub decision_reason: Option<String>,
    pub run_id: Option<String>,
    pub run_status: Option<TakeoverRunStatus>,
    pub blocker_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub decided_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TakeoverFinish {
    pub run_id: Option<String>,
    pub run_status: TakeoverRunStatus,
    pub detail: String,
}

impl TakeoverFinish {
    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(run_id) = &self.run_id {
            validate_text("takeover run id", run_id, 256)?;
        }
        validate_text("takeover finish detail", &self.detail, 4_096)
    }

    pub(crate) const fn terminal_approval_status(&self) -> TakeoverApprovalStatus {
        if self.run_status.is_success() {
            TakeoverApprovalStatus::Done
        } else {
            TakeoverApprovalStatus::Blocked
        }
    }

    pub(crate) const fn terminal_step_status(&self) -> GoalContinuityStepStatus {
        if self.run_status.is_success() {
            GoalContinuityStepStatus::Done
        } else {
            GoalContinuityStepStatus::Blocked
        }
    }
}
