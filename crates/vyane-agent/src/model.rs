use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::{AgentStoreError, Result};

pub(crate) const MAX_OWNER_BYTES: usize = 256;
pub(crate) const MAX_ID_BYTES: usize = 256;
pub(crate) const MAX_REFERENCE_BYTES: usize = 512;
pub(crate) const MAX_TARGET_KEY_BYTES: usize = 512;
pub(crate) const MAX_PROJECTOR_BYTES: usize = 256;
pub(crate) const MAX_COMPLETION_KIND_BYTES: usize = 128;
pub(crate) const MAX_PUBLICATION_KEY_BYTES: usize = 512;
pub(crate) const MAX_PAGE_SIZE: usize = 1_000;
pub(crate) const MAX_RESUME_ATTEMPTS: u32 = 100;
pub(crate) const MAX_TIMEOUT_SECONDS: u64 = 7 * 24 * 60 * 60;
/// Hard bound for one topology snapshot or tree mutation.
pub const MAX_TOPOLOGY_NODES: usize = 256;
/// Maximum nonterminal runs one atomic tree-cancel request may transition.
pub const MAX_TREE_CANCEL_RUNS: usize = 256;

macro_rules! string_enum {
    ($name:ident { $($variant:ident => $value:literal,)+ }) => {
        impl $name {
            pub(crate) const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $value,)+ }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = AgentStoreError;

            fn from_str(value: &str) -> Result<Self> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(AgentStoreError::CorruptData(format!(
                        "unknown stored {} value",
                        stringify!($name)
                    ))),
                }
            }
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerLifecycle {
    Open,
    Draining,
    Released,
}

string_enum!(WorkerLifecycle {
    Open => "open",
    Draining => "draining",
    Released => "released",
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Queued,
    Starting,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
    Interrupted,
}

impl RunState {
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::TimedOut | Self::Cancelled | Self::Interrupted
        )
    }

    #[must_use]
    pub fn is_controller_active(self) -> bool {
        matches!(self, Self::Starting | Self::Running | Self::Cancelling)
    }
}

string_enum!(RunState {
    Queued => "queued",
    Starting => "starting",
    Running => "running",
    Cancelling => "cancelling",
    Succeeded => "succeeded",
    Failed => "failed",
    TimedOut => "timed_out",
    Cancelled => "cancelled",
    Interrupted => "interrupted",
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunMode {
    Interactive,
    Autonomous,
}

string_enum!(RunMode {
    Interactive => "interactive",
    Autonomous => "autonomous",
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunFailureCode {
    DispatchFailed,
    SpawnFailed,
    ControllerLost,
    HeartbeatStale,
    ActivityStalled,
    TransportInterrupted,
    ControlUnavailable,
    PolicyDenied,
    TimedOut,
    Cancelled,
    Internal,
}

impl RunFailureCode {
    #[must_use]
    pub fn is_resume_eligible(self) -> bool {
        matches!(
            self,
            Self::ControllerLost
                | Self::HeartbeatStale
                | Self::ActivityStalled
                | Self::TransportInterrupted
        )
    }
}

string_enum!(RunFailureCode {
    DispatchFailed => "dispatch_failed",
    SpawnFailed => "spawn_failed",
    ControllerLost => "controller_lost",
    HeartbeatStale => "heartbeat_stale",
    ActivityStalled => "activity_stalled",
    TransportInterrupted => "transport_interrupted",
    ControlUnavailable => "control_unavailable",
    PolicyDenied => "policy_denied",
    TimedOut => "timed_out",
    Cancelled => "cancelled",
    Internal => "internal",
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControllerKind {
    InProcess,
    Process,
    Remote,
}

string_enum!(ControllerKind {
    InProcess => "in_process",
    Process => "process",
    Remote => "remote",
});

/// Secret-free stable controller identity. OS-specific control details remain
/// in the service/CLI adapter, outside this persistence boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerRef {
    pub kind: ControllerKind,
    pub id: String,
    pub fingerprint: Option<String>,
}

impl ControllerRef {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("controller id", &self.id, MAX_REFERENCE_BYTES)?;
        validate_optional_text(
            "controller fingerprint",
            self.fingerprint.as_deref(),
            MAX_REFERENCE_BYTES,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLease {
    pub owner: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewWorker {
    pub id: String,
    pub logical_session_id: Option<String>,
}

impl NewWorker {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("worker id", &self.id, MAX_ID_BYTES)?;
        validate_optional_text(
            "logical session id",
            self.logical_session_id.as_deref(),
            MAX_REFERENCE_BYTES,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRecord {
    pub owner: String,
    pub id: String,
    pub parent_id: Option<String>,
    pub logical_session_id: Option<String>,
    pub lifecycle: WorkerLifecycle,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub released_at: Option<DateTime<Utc>>,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewAgentRun {
    pub id: String,
    pub worker_id: String,
    pub task_id: Option<String>,
    pub trace_id: Option<String>,
    pub parent_run_id: Option<String>,
    pub mode: RunMode,
    pub target_key: String,
    /// Domain-separated digest of the transient prompt; never the prompt.
    pub prompt_digest: String,
    /// Digest of the frozen effective execution/resume policy.
    pub policy_digest: String,
    pub available_at: DateTime<Utc>,
    pub timeout_seconds: u64,
    pub max_resume_attempts: u32,
}

impl NewAgentRun {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("run id", &self.id, MAX_ID_BYTES)?;
        validate_text("run worker id", &self.worker_id, MAX_ID_BYTES)?;
        validate_optional_text("task id", self.task_id.as_deref(), MAX_REFERENCE_BYTES)?;
        validate_optional_text("trace id", self.trace_id.as_deref(), MAX_REFERENCE_BYTES)?;
        validate_optional_text(
            "parent run id",
            self.parent_run_id.as_deref(),
            MAX_REFERENCE_BYTES,
        )?;
        validate_text("target key", &self.target_key, MAX_TARGET_KEY_BYTES)?;
        validate_digest("prompt digest", &self.prompt_digest)?;
        validate_digest("policy digest", &self.policy_digest)?;
        if self.timeout_seconds == 0 || self.timeout_seconds > MAX_TIMEOUT_SECONDS {
            return Err(AgentStoreError::InvalidInput(format!(
                "run timeout must be between 1 and {MAX_TIMEOUT_SECONDS} seconds"
            )));
        }
        if self.max_resume_attempts > MAX_RESUME_ATTEMPTS {
            return Err(AgentStoreError::InvalidInput(format!(
                "max resume attempts exceeds {MAX_RESUME_ATTEMPTS}"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunRecord {
    pub owner: String,
    pub id: String,
    pub worker_id: String,
    pub task_id: Option<String>,
    pub trace_id: Option<String>,
    pub parent_run_id: Option<String>,
    pub resume_of_run_id: Option<String>,
    pub state: RunState,
    pub mode: RunMode,
    pub target_key: String,
    pub prompt_digest: String,
    pub policy_digest: String,
    /// Owner/logical/native session binding digest; never the native id.
    pub resume_binding_digest: Option<String>,
    pub available_at: DateTime<Utc>,
    /// Fixed wall-clock deadline assigned on claim; heartbeats never extend it.
    pub deadline_at: Option<DateTime<Utc>>,
    pub timeout_seconds: u64,
    pub max_resume_attempts: u32,
    pub resume_attempt: u32,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub revision: u64,
    pub worker_generation: u64,
    pub controller: Option<ControllerRef>,
    pub lease: Option<RunLease>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub last_activity_at: Option<DateTime<Utc>>,
    pub failure_code: Option<RunFailureCode>,
}

impl AgentRunRecord {
    #[must_use]
    pub fn is_resume_eligible(&self) -> bool {
        self.state == RunState::Interrupted
            && self
                .failure_code
                .is_some_and(RunFailureCode::is_resume_eligible)
            && self.resume_attempt < self.max_resume_attempts
    }
}

/// Exact bearer capability for one claimed run generation and revision.
#[derive(Clone, PartialEq, Eq)]
pub struct RunLeaseReceipt {
    pub run_id: String,
    pub worker_id: String,
    pub generation: u64,
    pub revision: u64,
    pub lease_owner: String,
    pub token: String,
}

impl fmt::Debug for RunLeaseReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunLeaseReceipt")
            .field("run_id", &self.run_id)
            .field("worker_id", &self.worker_id)
            .field("generation", &self.generation)
            .field("revision", &self.revision)
            .field("lease_owner", &self.lease_owner)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl RunLeaseReceipt {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("receipt run id", &self.run_id, MAX_ID_BYTES)?;
        validate_text("receipt worker id", &self.worker_id, MAX_ID_BYTES)?;
        validate_text("receipt lease owner", &self.lease_owner, MAX_OWNER_BYTES)?;
        if self.generation == 0 || self.token.len() != 64 || !is_lower_hex(&self.token) {
            return Err(AgentStoreError::InvalidReceipt {
                id: self.run_id.clone(),
            });
        }
        Ok(())
    }
}

/// In-memory bearer authority for one active run generation.
///
/// Unlike [`RunLeaseReceipt`], this permit deliberately does not freeze a run
/// revision: routine heartbeat and activity writes may advance the revision
/// without invalidating already-issued execution authority. The store still
/// revalidates every other authority component, the current policy, lease,
/// deadline, and lifecycle on every use.
///
/// This type has private fields, is not serializable, and is intentionally not
/// cloneable. It is authority, not an audit record; use
/// [`ExecutionPermitSnapshot`] for safe logging or persistence.
pub struct ActiveExecutionPermit {
    owner: String,
    run_id: String,
    worker_id: String,
    generation: u64,
    lease_owner: String,
    policy_digest: String,
    token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunCompletionStatus {
    Prepared,
    Committed,
    Abandoned,
}

string_enum!(RunCompletionStatus {
    Prepared => "prepared",
    Committed => "committed",
    Abandoned => "abandoned",
});

/// Secret-free metadata for one result staged outside this store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewRunCompletion {
    pub id: String,
    pub sink_kind: String,
    /// Stable opaque sink key. It must not be a URL, path, credential, or bearer.
    pub publication_key: String,
    pub content_digest: String,
    pub content_bytes: u64,
}

impl NewRunCompletion {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("completion id", &self.id, MAX_ID_BYTES)?;
        validate_text(
            "completion sink kind",
            &self.sink_kind,
            MAX_COMPLETION_KIND_BYTES,
        )?;
        validate_opaque_key("completion sink kind", &self.sink_kind)?;
        validate_text(
            "completion publication key",
            &self.publication_key,
            MAX_PUBLICATION_KEY_BYTES,
        )?;
        validate_opaque_key("completion publication key", &self.publication_key)?;
        validate_digest("completion content digest", &self.content_digest)
    }
}

/// Durable, body-free completion metadata. This record grants no authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunCompletionRecord {
    pub owner: String,
    pub run_id: String,
    pub worker_id: String,
    pub worker_generation: u64,
    pub completion_id: String,
    pub sink_kind: String,
    pub publication_key: String,
    pub content_digest: String,
    pub content_bytes: u64,
    pub status: RunCompletionStatus,
    pub prepared_at: DateTime<Utc>,
    pub prepared_run_revision: u64,
    pub committed_at: Option<DateTime<Utc>>,
    pub committed_run_revision: Option<u64>,
    pub abandoned_at: Option<DateTime<Utc>>,
    pub abandoned_run_revision: Option<u64>,
    pub committed_by_operation_id: Option<String>,
    pub revision: u64,
}

/// Non-cloneable bearer for the publication and live commit of one prepared result.
pub struct ActiveCompletionPermit {
    owner: String,
    run_id: String,
    worker_id: String,
    generation: u64,
    completion_id: String,
    token: String,
}

impl fmt::Debug for ActiveCompletionPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveCompletionPermit")
            .field("owner", &self.owner)
            .field("run_id", &self.run_id)
            .field("worker_id", &self.worker_id)
            .field("generation", &self.generation)
            .field("completion_id", &self.completion_id)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl ActiveCompletionPermit {
    pub(crate) fn issue(record: &RunCompletionRecord, token: String) -> Self {
        Self {
            owner: record.owner.clone(),
            run_id: record.run_id.clone(),
            worker_id: record.worker_id.clone(),
            generation: record.worker_generation,
            completion_id: record.completion_id.clone(),
            token,
        }
    }

    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn completion_id(&self) -> &str {
        &self.completion_id
    }

    pub(crate) fn token(&self) -> &str {
        &self.token
    }
}

/// Prepared completion metadata paired with its non-cloneable authority.
pub struct PreparedRunCompletion {
    pub record: RunCompletionRecord,
    pub permit: ActiveCompletionPermit,
}

impl fmt::Debug for PreparedRunCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedRunCompletion")
            .field("record", &self.record)
            .field("permit", &self.permit)
            .finish()
    }
}

/// Safe audit snapshot proving a prepared completion is still publishable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionPermitSnapshot {
    pub record: RunCompletionRecord,
    pub run_revision: u64,
    pub lease_expires_at: DateTime<Utc>,
    pub deadline_at: DateTime<Utc>,
    pub validated_at: DateTime<Utc>,
}

impl fmt::Debug for ActiveExecutionPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveExecutionPermit")
            .field("owner", &self.owner)
            .field("run_id", &self.run_id)
            .field("worker_id", &self.worker_id)
            .field("generation", &self.generation)
            .field("lease_owner", &self.lease_owner)
            .field("policy_digest", &"[OPAQUE]")
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl ActiveExecutionPermit {
    pub(crate) fn issue(owner: &str, receipt: &RunLeaseReceipt, policy_digest: &str) -> Self {
        Self {
            owner: owner.to_string(),
            run_id: receipt.run_id.clone(),
            worker_id: receipt.worker_id.clone(),
            generation: receipt.generation,
            lease_owner: receipt.lease_owner.clone(),
            policy_digest: policy_digest.to_string(),
            token: receipt.token.clone(),
        }
    }

    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn lease_owner(&self) -> &str {
        &self.lease_owner
    }

    #[must_use]
    pub fn policy_digest(&self) -> &str {
        &self.policy_digest
    }

    pub(crate) fn token(&self) -> &str {
        &self.token
    }
}

/// Safe audit identity returned after an execution permit is revalidated.
///
/// This snapshot contains no bearer token and grants no authority. It may be
/// serialized or cloned for audit/event correlation independently from the
/// in-memory [`ActiveExecutionPermit`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPermitSnapshot {
    owner: String,
    run_id: String,
    worker_id: String,
    generation: u64,
    run_revision: u64,
    lease_owner: String,
    policy_digest: String,
    lease_expires_at: DateTime<Utc>,
    deadline_at: DateTime<Utc>,
    validated_at: DateTime<Utc>,
}

impl ExecutionPermitSnapshot {
    pub(crate) fn from_validated_run(
        run: &AgentRunRecord,
        lease_owner: &str,
        lease_expires_at: DateTime<Utc>,
        deadline_at: DateTime<Utc>,
        validated_at: DateTime<Utc>,
    ) -> Self {
        Self {
            owner: run.owner.clone(),
            run_id: run.id.clone(),
            worker_id: run.worker_id.clone(),
            generation: run.worker_generation,
            run_revision: run.revision,
            lease_owner: lease_owner.to_string(),
            policy_digest: run.policy_digest.clone(),
            lease_expires_at,
            deadline_at,
            validated_at,
        }
    }

    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn run_revision(&self) -> u64 {
        self.run_revision
    }

    #[must_use]
    pub fn lease_owner(&self) -> &str {
        &self.lease_owner
    }

    #[must_use]
    pub fn policy_digest(&self) -> &str {
        &self.policy_digest
    }

    #[must_use]
    pub const fn lease_expires_at(&self) -> DateTime<Utc> {
        self.lease_expires_at
    }

    #[must_use]
    pub const fn deadline_at(&self) -> DateTime<Utc> {
        self.deadline_at
    }

    #[must_use]
    pub const fn validated_at(&self) -> DateTime<Utc> {
        self.validated_at
    }
}

/// Owned, secret-free identity frozen around one native execution attempt.
///
/// The scope retains digests and, for resumed work, an opaque binding proof;
/// it never retains the native harness session identifier used to derive that
/// proof. Fields are private so a scope can only be created after all target,
/// prompt, policy, logical-session, and resume-binding invariants are checked.
#[derive(Clone, PartialEq, Eq)]
pub struct NativeExecutionScope {
    target_key: String,
    prompt_digest: String,
    policy_digest: String,
    logical_session_id: Option<String>,
    resume_session_proof: Option<ResumeSessionProof>,
}

impl fmt::Debug for NativeExecutionScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeExecutionScope")
            .field("target_key", &self.target_key)
            .field("prompt_digest", &"[OPAQUE]")
            .field("policy_digest", &"[OPAQUE]")
            .field("logical_session_id", &self.logical_session_id)
            .field(
                "resume_session_proof",
                &self.resume_session_proof.as_ref().map(|_| "[OPAQUE]"),
            )
            .finish()
    }
}

impl NativeExecutionScope {
    /// Freeze a fresh native execution scope. A logical session may be
    /// allocated up front, but fresh work cannot carry resume authority.
    pub fn fresh(
        target_key: impl Into<String>,
        prompt_digest: impl Into<String>,
        policy_digest: impl Into<String>,
        logical_session_id: Option<String>,
    ) -> Result<Self> {
        Self::build(
            target_key.into(),
            prompt_digest.into(),
            policy_digest.into(),
            logical_session_id,
            None,
        )
    }

    /// Freeze a resumed native execution scope bound to one exact logical and
    /// native harness session identity.
    pub fn resumed(
        target_key: impl Into<String>,
        prompt_digest: impl Into<String>,
        policy_digest: impl Into<String>,
        logical_session_id: impl Into<String>,
        resume_session_proof: ResumeSessionProof,
    ) -> Result<Self> {
        Self::build(
            target_key.into(),
            prompt_digest.into(),
            policy_digest.into(),
            Some(logical_session_id.into()),
            Some(resume_session_proof),
        )
    }

    fn build(
        target_key: String,
        prompt_digest: String,
        policy_digest: String,
        logical_session_id: Option<String>,
        resume_session_proof: Option<ResumeSessionProof>,
    ) -> Result<Self> {
        let scope = Self {
            target_key,
            prompt_digest,
            policy_digest,
            logical_session_id,
            resume_session_proof,
        };
        scope.validate()?;
        Ok(scope)
    }

    fn validate(&self) -> Result<()> {
        validate_text(
            "native execution target key",
            &self.target_key,
            MAX_TARGET_KEY_BYTES,
        )?;
        validate_digest("native execution prompt digest", &self.prompt_digest)?;
        validate_digest("native execution policy digest", &self.policy_digest)?;
        validate_optional_text(
            "native execution logical session id",
            self.logical_session_id.as_deref(),
            MAX_REFERENCE_BYTES,
        )?;
        if let Some(proof) = &self.resume_session_proof {
            proof.validate()?;
            if self.logical_session_id.as_deref() != Some(proof.logical_session_id()) {
                return Err(AgentStoreError::InvalidInput(
                    "native execution resume proof does not match the logical session".into(),
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn target_key(&self) -> &str {
        &self.target_key
    }

    #[must_use]
    pub fn prompt_digest(&self) -> &str {
        &self.prompt_digest
    }

    #[must_use]
    pub fn policy_digest(&self) -> &str {
        &self.policy_digest
    }

    #[must_use]
    pub fn logical_session_id(&self) -> Option<&str> {
        self.logical_session_id.as_deref()
    }

    #[must_use]
    pub fn resume_session_proof(&self) -> Option<&ResumeSessionProof> {
        self.resume_session_proof.as_ref()
    }

    pub(crate) fn resume_binding_digest(&self) -> Option<&str> {
        self.resume_session_proof
            .as_ref()
            .map(ResumeSessionProof::binding_digest)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ClaimedRun {
    pub run: AgentRunRecord,
    pub receipt: RunLeaseReceipt,
}

impl fmt::Debug for ClaimedRun {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClaimedRun")
            .field("run", &self.run)
            .field("receipt", &self.receipt)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunSettlement {
    Failed { code: RunFailureCode },
    TimedOut,
    Interrupted { code: RunFailureCode },
}

impl RunSettlement {
    pub(crate) fn parts(self) -> Result<(RunState, Option<RunFailureCode>)> {
        match self {
            Self::Failed { code } => {
                if matches!(code, RunFailureCode::TimedOut | RunFailureCode::Cancelled) {
                    return Err(AgentStoreError::InvalidInput(
                        "failed settlement cannot use timed_out or cancelled".into(),
                    ));
                }
                Ok((RunState::Failed, Some(code)))
            }
            Self::TimedOut => Ok((RunState::TimedOut, Some(RunFailureCode::TimedOut))),
            Self::Interrupted { code } => {
                if !code.is_resume_eligible() {
                    return Err(AgentStoreError::InvalidInput(
                        "interrupted settlement requires a resumable interruption code".into(),
                    ));
                }
                Ok((RunState::Interrupted, Some(code)))
            }
        }
    }
}

/// Exact opaque binding to one `(owner, logical session, native session)`.
///
/// Fields are private so callers cannot turn an arbitrary 64-hex string into
/// proof. The native session identifier is hashed transiently and is never
/// retained by this value or persisted by the store.
#[derive(Clone, PartialEq, Eq)]
pub struct ResumeSessionProof {
    logical_session_id: String,
    binding_digest: String,
}

impl fmt::Debug for ResumeSessionProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResumeSessionProof")
            .field("logical_session_id", &self.logical_session_id)
            .field("binding_digest", &"[OPAQUE]")
            .finish()
    }
}

impl ResumeSessionProof {
    pub fn derive(owner: &str, logical_session_id: &str, native_session_id: &str) -> Result<Self> {
        validate_owner(owner)?;
        validate_text(
            "resume logical session id",
            logical_session_id,
            MAX_REFERENCE_BYTES,
        )?;
        validate_text("native session id", native_session_id, MAX_REFERENCE_BYTES)?;
        let mut hasher = Sha256::new();
        hasher.update(b"vyane-agent-resume-binding-v1\0");
        for value in [owner, logical_session_id, native_session_id] {
            let length = u64::try_from(value.len()).map_err(|_| {
                AgentStoreError::InvalidInput("resume binding field is too large".into())
            })?;
            hasher.update(length.to_be_bytes());
            hasher.update(value.as_bytes());
        }
        Ok(Self {
            logical_session_id: logical_session_id.to_string(),
            binding_digest: hex_lower(&hasher.finalize()),
        })
    }

    #[must_use]
    pub fn logical_session_id(&self) -> &str {
        &self.logical_session_id
    }

    pub(crate) fn binding_digest(&self) -> &str {
        &self.binding_digest
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_text(
            "resume logical session id",
            &self.logical_session_id,
            MAX_REFERENCE_BYTES,
        )?;
        validate_digest("resume binding digest", &self.binding_digest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeProof {
    session: ResumeSessionProof,
    policy_digest: String,
}

impl ResumeProof {
    pub fn new(session: ResumeSessionProof, policy_digest: impl Into<String>) -> Result<Self> {
        let proof = Self {
            session,
            policy_digest: policy_digest.into(),
        };
        proof.validate()?;
        Ok(proof)
    }

    #[must_use]
    pub fn session(&self) -> &ResumeSessionProof {
        &self.session
    }

    #[must_use]
    pub fn policy_digest(&self) -> &str {
        &self.policy_digest
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.session.validate()?;
        validate_digest("resume policy digest", &self.policy_digest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueResume {
    pub new_run_id: String,
    pub source_run_id: String,
    pub available_at: DateTime<Utc>,
    pub proof: ResumeProof,
}

#[derive(Clone, PartialEq, Eq)]
pub struct CancelTicket {
    pub operation_id: String,
    pub worker_id: String,
    pub run_id: String,
    pub generation: u64,
    pub revision: u64,
    pub controller: Option<ControllerRef>,
    pub lease_owner: String,
    pub expires_at: DateTime<Utc>,
    pub token: String,
}

impl fmt::Debug for CancelTicket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancelTicket")
            .field("operation_id", &self.operation_id)
            .field("worker_id", &self.worker_id)
            .field("run_id", &self.run_id)
            .field("generation", &self.generation)
            .field("revision", &self.revision)
            .field("controller", &self.controller)
            .field("lease_owner", &self.lease_owner)
            .field("expires_at", &self.expires_at)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl CancelTicket {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("cancel operation id", &self.operation_id, MAX_ID_BYTES)?;
        validate_text("cancel worker id", &self.worker_id, MAX_ID_BYTES)?;
        validate_text("cancel run id", &self.run_id, MAX_ID_BYTES)?;
        validate_text("cancel lease owner", &self.lease_owner, MAX_OWNER_BYTES)?;
        if self.token.len() != 64 || !is_lower_hex(&self.token) {
            return Err(AgentStoreError::InvalidCancelTicket {
                id: self.run_id.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelRequest {
    pub operation_id: String,
    pub lease_owner: String,
    pub lease_seconds: u64,
    /// Exact tickets returned by an earlier attempt of this operation.
    pub retry_tickets: Vec<CancelTicket>,
}

impl CancelRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("cancel operation id", &self.operation_id, MAX_ID_BYTES)?;
        validate_text("cancel lease owner", &self.lease_owner, MAX_OWNER_BYTES)?;
        if self.lease_seconds == 0 || self.lease_seconds > 24 * 60 * 60 {
            return Err(AgentStoreError::InvalidInput(
                "cancel operation lease must be between 1 second and 24 hours".into(),
            ));
        }
        if self.retry_tickets.len() > MAX_TREE_CANCEL_RUNS {
            return Err(AgentStoreError::InvalidInput(
                "cancel retry contains too many tickets".into(),
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for ticket in &self.retry_tickets {
            ticket.validate()?;
            if ticket.operation_id != self.operation_id
                || ticket.lease_owner != self.lease_owner
                || !seen.insert(ticket.run_id.as_str())
            {
                return Err(AgentStoreError::InvalidInput(
                    "cancel retry tickets do not match the operation".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelPlan {
    pub root_worker_id: String,
    /// Exact controller work in children-first order.
    pub tickets: Vec<CancelTicket>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    Cancelled,
    ControllerUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryReason {
    LeaseExpired,
    ExecutionTimedOut,
    CancellationAbandoned,
}

string_enum!(RecoveryReason {
    LeaseExpired => "lease_expired",
    ExecutionTimedOut => "execution_timed_out",
    CancellationAbandoned => "cancellation_abandoned",
});

#[derive(Clone, PartialEq, Eq)]
pub struct RecoveryTicket {
    pub operation_id: String,
    pub worker_id: String,
    pub run_id: String,
    pub generation: u64,
    pub revision: u64,
    pub controller: Option<ControllerRef>,
    pub reason: RecoveryReason,
    pub lease_owner: String,
    pub expires_at: DateTime<Utc>,
    pub token: String,
}

impl fmt::Debug for RecoveryTicket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecoveryTicket")
            .field("operation_id", &self.operation_id)
            .field("worker_id", &self.worker_id)
            .field("run_id", &self.run_id)
            .field("generation", &self.generation)
            .field("revision", &self.revision)
            .field("controller", &self.controller)
            .field("reason", &self.reason)
            .field("lease_owner", &self.lease_owner)
            .field("expires_at", &self.expires_at)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl RecoveryTicket {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_text("recovery operation id", &self.operation_id, MAX_ID_BYTES)?;
        validate_text("recovery worker id", &self.worker_id, MAX_ID_BYTES)?;
        validate_text("recovery run id", &self.run_id, MAX_ID_BYTES)?;
        validate_text("recovery lease owner", &self.lease_owner, MAX_OWNER_BYTES)?;
        if self.generation == 0 || self.token.len() != 64 || !is_lower_hex(&self.token) {
            return Err(AgentStoreError::InvalidRecoveryTicket {
                id: self.run_id.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerTopology {
    pub root_worker_id: String,
    /// Parent-before-child stable order. Parent links are stored only on each row.
    pub workers: Vec<WorkerRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentEventKind {
    WorkerCreated,
    ChildSpawned,
    RunQueued,
    RunClaimed,
    RunStarted,
    RunHeartbeat,
    RunActivity,
    ResumeBound,
    RecoveryRequested,
    RecoverySettled,
    RunSettled,
    CancelRequested,
    CancelSettled,
    WorkerReleased,
    ResumeQueued,
    CompletionPrepared,
    CompletionCommitted,
    CompletionAbandoned,
}

string_enum!(AgentEventKind {
    WorkerCreated => "worker_created",
    ChildSpawned => "child_spawned",
    RunQueued => "run_queued",
    RunClaimed => "run_claimed",
    RunStarted => "run_started",
    RunHeartbeat => "run_heartbeat",
    RunActivity => "run_activity",
    ResumeBound => "resume_bound",
    RecoveryRequested => "recovery_requested",
    RecoverySettled => "recovery_settled",
    RunSettled => "run_settled",
    CancelRequested => "cancel_requested",
    CancelSettled => "cancel_settled",
    WorkerReleased => "worker_released",
    ResumeQueued => "resume_queued",
    CompletionPrepared => "completion_prepared",
    CompletionCommitted => "completion_committed",
    CompletionAbandoned => "completion_abandoned",
});

/// Body-free event committed with its source mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEvent {
    pub sequence: u64,
    pub event_id: String,
    pub owner: String,
    pub worker_id: String,
    pub run_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub kind: AgentEventKind,
    pub worker_revision: u64,
    pub run_revision: Option<u64>,
    pub run_state: Option<RunState>,
    pub worker_lifecycle: WorkerLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxPage {
    pub items: Vec<AgentEvent>,
    pub has_more: bool,
}

/// Bounded, body-free reason for retrying one event projection later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionDeferReason {
    SinkUnavailable,
    MissingSink,
}

string_enum!(ProjectionDeferReason {
    SinkUnavailable => "sink_unavailable",
    MissingSink => "missing_sink",
});

/// Bounded, body-free reason for permanently isolating one bad projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionQuarantineReason {
    InvalidEvent,
    SinkConflict,
}

string_enum!(ProjectionQuarantineReason {
    InvalidEvent => "invalid_event",
    SinkConflict => "sink_conflict",
});

pub(crate) fn validate_owner(owner: &str) -> Result<()> {
    validate_text("owner", owner, MAX_OWNER_BYTES)
}

pub(crate) fn validate_projector(projector: &str) -> Result<()> {
    validate_text("projector", projector, MAX_PROJECTOR_BYTES)
}

pub(crate) fn validate_limit(limit: usize, field: &str) -> Result<()> {
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(AgentStoreError::InvalidInput(format!(
            "{field} must be between 1 and {MAX_PAGE_SIZE}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_text(field: &str, value: &str, max: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max
        || value.contains('\0')
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(AgentStoreError::InvalidInput(format!(
            "{field} must contain between 1 and {max} canonical non-control bytes"
        )));
    }
    Ok(())
}

pub(crate) fn validate_opaque_key(field: &str, value: &str) -> Result<()> {
    if value
        .bytes()
        .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')))
    {
        return Err(AgentStoreError::InvalidInput(format!(
            "{field} must be an opaque ASCII key"
        )));
    }
    Ok(())
}

pub(crate) fn validate_optional_text(field: &str, value: Option<&str>, max: usize) -> Result<()> {
    if let Some(value) = value {
        validate_text(field, value, max)?;
    }
    Ok(())
}

pub(crate) fn validate_digest(field: &str, value: &str) -> Result<()> {
    if value.len() != 64 || !is_lower_hex(value) {
        return Err(AgentStoreError::InvalidInput(format!(
            "{field} must be a lowercase SHA-256 digest"
        )));
    }
    Ok(())
}

pub(crate) fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
