use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{Result, TaskStoreError};

macro_rules! impl_string_enum {
    ($name:ident { $($variant:ident => $value:literal,)+ }) => {
        impl $name {
            pub(crate) const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value,)+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = TaskStoreError;

            fn from_str(value: &str) -> Result<Self> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    other => Err(TaskStoreError::CorruptData(format!(
                        "unknown {} value `{other}`",
                        stringify!($name)
                    ))),
                }
            }
        }
    };
}

/// Canonical lifecycle state shared by in-process and detached tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Queued,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
    Interrupted,
}

impl TaskState {
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::TimedOut | Self::Cancelled | Self::Interrupted
        )
    }
}

impl_string_enum!(TaskState {
    Queued => "queued",
    Running => "running",
    Cancelling => "cancelling",
    Succeeded => "succeeded",
    Failed => "failed",
    TimedOut => "timed_out",
    Cancelled => "cancelled",
    Interrupted => "interrupted",
});

/// What submitted the task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskOrigin {
    RestAsync,
    CliDetached,
    Daemon,
}

impl_string_enum!(TaskOrigin {
    RestAsync => "rest_async",
    CliDetached => "cli_detached",
    Daemon => "daemon",
});

/// The durable task class. Workflow is reserved for the daemon integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Dispatch,
    Workflow,
}

impl_string_enum!(TaskKind {
    Dispatch => "dispatch",
    Workflow => "workflow",
});

/// A bounded, non-secret failure classification. Raw errors are not persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureCode {
    DispatchFailed,
    SpawnFailed,
    Configuration,
    ControlUnavailable,
    WorkerLost,
    LeaseExpired,
    Cancelled,
    TimedOut,
    Internal,
}

impl_string_enum!(FailureCode {
    DispatchFailed => "dispatch_failed",
    SpawnFailed => "spawn_failed",
    Configuration => "configuration",
    ControlUnavailable => "control_unavailable",
    WorkerLost => "worker_lost",
    LeaseExpired => "lease_expired",
    Cancelled => "cancelled",
    TimedOut => "timed_out",
    Internal => "internal",
});

/// Persistable identity used to control a live task.
///
/// An in-process cancellation token is intentionally absent. The owner process
/// keeps that token in a live map keyed by `(task_id, executor_epoch)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ControllerRef {
    InProcess {
        instance_id: String,
    },
    ProcessGroup {
        pid: i32,
        pgid: i32,
        started_at: DateTime<Utc>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        birth_fingerprint: Option<String>,
    },
}

impl ControllerRef {
    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::InProcess { instance_id } => validate_text("instance_id", instance_id, 256),
            Self::ProcessGroup {
                pid,
                pgid,
                birth_fingerprint,
                ..
            } => {
                if *pid <= 0 || *pgid <= 0 {
                    return Err(TaskStoreError::InvalidInput(
                        "process pid and pgid must be positive".into(),
                    ));
                }
                if let Some(value) = birth_fingerprint {
                    validate_text("birth_fingerprint", value, 512)?;
                }
                Ok(())
            }
        }
    }

    pub(crate) fn actor(&self) -> String {
        match self {
            Self::InProcess { instance_id } => instance_id.clone(),
            Self::ProcessGroup { pid, .. } => format!("process:{pid}"),
        }
    }
}

/// Optional ownership lease for a future resident daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub owner: String,
    pub expires_at: DateTime<Utc>,
}

impl Lease {
    pub(crate) fn validate_after(&self, now: DateTime<Utc>) -> Result<()> {
        validate_text("lease owner", &self.owner, 256)?;
        if self.expires_at <= now {
            return Err(TaskStoreError::InvalidInput(
                "lease expiry must be later than the operation timestamp".into(),
            ));
        }
        Ok(())
    }
}

/// Secret-free metadata accepted when a task is created.
///
/// There is deliberately no prompt, system instruction, session transcript,
/// arbitrary labels, raw error, provider endpoint, credential, or output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewTask {
    pub id: String,
    pub kind: TaskKind,
    pub origin: TaskOrigin,
    pub task_digest: String,
    pub target_key: String,
    pub created_at: DateTime<Utc>,
}

impl NewTask {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("task id", &self.id, 256)?;
        validate_task_digest(&self.task_digest)?;
        validate_text("target key", &self.target_key, 512)
    }
}

/// Durable task snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: String,
    pub owner: String,
    pub kind: TaskKind,
    pub origin: TaskOrigin,
    pub state: TaskState,
    pub task_digest: String,
    pub target_key: String,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub revision: u64,
    pub executor_epoch: u64,
    pub controller: Option<ControllerRef>,
    pub lease: Option<Lease>,
    pub ledger_run_id: Option<String>,
    pub failure_code: Option<FailureCode>,
}

impl TaskRecord {
    pub(crate) fn from_new(owner: String, value: NewTask) -> Self {
        Self {
            id: value.id,
            owner,
            kind: value.kind,
            origin: value.origin,
            state: TaskState::Queued,
            task_digest: value.task_digest,
            target_key: value.target_key,
            created_at: value.created_at,
            started_at: None,
            updated_at: value.created_at,
            finished_at: None,
            revision: 0,
            executor_epoch: 0,
            controller: None,
            lease: None,
            ledger_run_id: None,
            failure_code: None,
        }
    }
}

impl TaskRecord {
    /// Return whether this row belongs to one exact frontend scope.
    ///
    /// Frontends must compare all three dimensions before listing, recovering,
    /// attaching, or cancelling a row. `origin` alone is not an ownership
    /// boundary because multiple task kinds share the same database.
    #[must_use]
    pub fn matches_scope(&self, owner: &str, kind: TaskKind, origin: TaskOrigin) -> bool {
        self.owner == owner && self.kind == kind && self.origin == origin
    }
}

/// Stable cursor for descending `(created_at, id)` ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCursor {
    pub created_at: DateTime<Utc>,
    pub id: String,
}

/// Task listing filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskQuery {
    /// Exact task kinds to include. Empty means every kind.
    pub kinds: Vec<TaskKind>,
    /// Exact submission origins to include. Empty means every origin.
    pub origins: Vec<TaskOrigin>,
    /// Exact lifecycle states to include. Empty means every state.
    pub states: Vec<TaskState>,
    pub limit: usize,
    pub cursor: Option<TaskCursor>,
}

impl Default for TaskQuery {
    fn default() -> Self {
        Self {
            kinds: Vec::new(),
            origins: Vec::new(),
            states: Vec::new(),
            limit: 100,
            cursor: None,
        }
    }
}

impl TaskQuery {
    pub(crate) fn validate(&self) -> Result<()> {
        if !(1..=1_000).contains(&self.limit) {
            return Err(TaskStoreError::InvalidInput(
                "task query limit must be between 1 and 1000".into(),
            ));
        }
        if self.states.len() > 8 {
            return Err(TaskStoreError::InvalidInput(
                "task query contains more states than the canonical state set".into(),
            ));
        }
        if self.kinds.len() > 2 {
            return Err(TaskStoreError::InvalidInput(
                "task query contains more kinds than the canonical kind set".into(),
            ));
        }
        if self.origins.len() > 3 {
            return Err(TaskStoreError::InvalidInput(
                "task query contains more origins than the canonical origin set".into(),
            ));
        }
        if let Some(cursor) = &self.cursor {
            validate_text("cursor task id", &cursor.id, 256)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskPage {
    pub items: Vec<TaskRecord>,
    pub next_cursor: Option<TaskCursor>,
}

/// Terminal result metadata. Output content is intentionally not part of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskSettlement {
    Succeeded {
        ledger_run_id: Option<String>,
    },
    Failed {
        code: FailureCode,
        ledger_run_id: Option<String>,
    },
    TimedOut {
        ledger_run_id: Option<String>,
    },
    Cancelled {
        ledger_run_id: Option<String>,
    },
}

impl TaskSettlement {
    pub(crate) fn validate(&self) -> Result<()> {
        if matches!(
            self,
            Self::Failed {
                code: FailureCode::Cancelled | FailureCode::TimedOut,
                ..
            }
        ) {
            return Err(TaskStoreError::InvalidInput(
                "failed settlement cannot use the cancelled or timed_out failure code".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn parts(&self) -> (TaskState, Option<FailureCode>, Option<&str>) {
        match self {
            Self::Succeeded { ledger_run_id } => {
                (TaskState::Succeeded, None, ledger_run_id.as_deref())
            }
            Self::Failed {
                code,
                ledger_run_id,
            } => (TaskState::Failed, Some(*code), ledger_run_id.as_deref()),
            Self::TimedOut { ledger_run_id } => (
                TaskState::TimedOut,
                Some(FailureCode::TimedOut),
                ledger_run_id.as_deref(),
            ),
            Self::Cancelled { ledger_run_id } => (
                TaskState::Cancelled,
                Some(FailureCode::Cancelled),
                ledger_run_id.as_deref(),
            ),
        }
    }
}

/// Append-only metadata event stored in the same transaction as its snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEvent {
    pub sequence: u64,
    pub owner: String,
    pub task_id: String,
    pub revision: u64,
    pub occurred_at: DateTime<Utc>,
    pub kind: TaskEventKind,
    pub from_state: Option<TaskState>,
    pub to_state: TaskState,
    pub actor_instance: Option<String>,
    pub executor_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskEventKind {
    Created,
    ControllerAttached,
    CancelRequested,
    Settled,
    Interrupted,
    LeaseClaimed,
    LeaseRenewed,
}

impl_string_enum!(TaskEventKind {
    Created => "created",
    ControllerAttached => "controller_attached",
    CancelRequested => "cancel_requested",
    Settled => "settled",
    Interrupted => "interrupted",
    LeaseClaimed => "lease_claimed",
    LeaseRenewed => "lease_renewed",
});

pub(crate) fn validate_text(field: &str, value: &str, max_len: usize) -> Result<()> {
    if value.trim().is_empty() {
        return Err(TaskStoreError::InvalidInput(format!(
            "{field} must not be empty"
        )));
    }
    if value.len() > max_len {
        return Err(TaskStoreError::InvalidInput(format!(
            "{field} exceeds {max_len} bytes"
        )));
    }
    if value.contains('\0') {
        return Err(TaskStoreError::InvalidInput(format!(
            "{field} contains a NUL byte"
        )));
    }
    Ok(())
}

pub(crate) fn validate_task_digest(value: &str) -> Result<()> {
    if !matches!(value.len(), 16 | 64)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(TaskStoreError::InvalidInput(
            "task digest must be 16 or 64 lowercase hexadecimal characters".into(),
        ));
    }
    Ok(())
}
