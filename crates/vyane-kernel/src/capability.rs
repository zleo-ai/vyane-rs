//! Capability declarations and pre-execution admission evidence.
//!
//! These types are deliberately serializable audit data. They describe what a
//! trusted executor factory says it can do and why the kernel admitted (or
//! rejected) a target; none of them grants authority to perform a side effect.

use std::fmt;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use vyane_core::{ErrorKind, Sandbox, Target, VyaneError, WorkdirIdentity};

/// Stable, serializable identity allocated before any dispatch-side lookup or
/// executor inspection.
///
/// This is audit context only. It intentionally contains no cancellation,
/// lease, credential, capability token, or other runtime authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionScope {
    /// Also used as the final [`vyane_core::RunRecord::run_id`].
    pub execution_id: String,
    pub owner: String,
    pub started_at: DateTime<Utc>,
    pub requested_sandbox: Sandbox,
    /// Present only after a mutating request's workdir has been canonicalized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_workdir: Option<PathBuf>,
    /// Identity of the opened directory handle used for execution.  This is
    /// evidence only; the non-serializable handle lives in `PreparedDispatch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir_identity: Option<WorkdirIdentity>,
}

impl ExecutionScope {
    pub(crate) fn allocate(owner: &str, requested_sandbox: Sandbox) -> Self {
        Self {
            execution_id: uuid::Uuid::now_v7().to_string(),
            owner: owner.to_string(),
            started_at: Utc::now(),
            requested_sandbox,
            canonical_workdir: None,
            workdir_identity: None,
        }
    }

    pub(crate) fn with_workdir(
        mut self,
        workdir: Option<PathBuf>,
        identity: Option<WorkdirIdentity>,
    ) -> Self {
        self.canonical_workdir = workdir;
        self.workdir_identity = identity;
        self
    }
}

/// Filesystem behavior a trusted factory declares for an executor adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemCapability {
    /// Chat-only: no local workspace editing capability.
    None,
    /// The adapter consumes the caller's canonical local workdir and supports
    /// local editing from that execution root. Confinement strength is stated
    /// separately by [`IsolationStrength`] (and `Sandbox::Full` is explicitly
    /// not workspace-confined).
    CallerWorkdirEditing,
}

/// Strength of isolation provided by the adapter itself.
///
/// This is kept separate from filesystem capability: being able to edit a
/// workdir does not prove how that editing is isolated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationStrength {
    None,
    /// Enforcement is delegated to the known local harness adapter.
    AdapterDelegated,
    /// Reserved for executors backed by an OS-enforced sandbox.
    OsEnforced,
}

/// Trusted declaration returned by [`crate::ExecutorFactory`].
///
/// A manifest is evidence for admission, never the authority to execute.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityManifest {
    pub filesystem: FilesystemCapability,
    pub isolation: IsolationStrength,
}

impl CapabilityManifest {
    /// Conservative default for unknown/custom/remote executors.
    pub const fn chat_only() -> Self {
        Self {
            filesystem: FilesystemCapability::None,
            isolation: IsolationStrength::None,
        }
    }

    /// Declaration used by trusted local harness adapters that edit the
    /// canonical caller workdir under their own sandbox controls.
    pub const fn local_workdir_editing(isolation: IsolationStrength) -> Self {
        Self {
            filesystem: FilesystemCapability::CallerWorkdirEditing,
            isolation,
        }
    }
}

impl Default for CapabilityManifest {
    fn default() -> Self {
        Self::chat_only()
    }
}

/// Stable rejection categories for capability admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityRejectionReason {
    MissingWorkdir,
    WorkdirCanonicalizationFailed,
    WorkdirNotDirectory,
    WorkdirPinningUnavailable,
    LocalEditingUnavailable,
    IsolationUnavailable,
}

impl fmt::Display for CapabilityRejectionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::MissingWorkdir => "missing_workdir",
            Self::WorkdirCanonicalizationFailed => "workdir_canonicalization_failed",
            Self::WorkdirNotDirectory => "workdir_not_directory",
            Self::WorkdirPinningUnavailable => "workdir_pinning_unavailable",
            Self::LocalEditingUnavailable => "local_editing_unavailable",
            Self::IsolationUnavailable => "isolation_unavailable",
        };
        f.write_str(value)
    }
}

/// Admission decision for one target in the original resolved chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision", content = "reason")]
pub enum CapabilityAdmissionDecision {
    Admitted,
    Rejected(CapabilityRejectionReason),
}

/// Serializable explanation of one target's capability decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityAdmissionEvidence {
    pub execution_id: String,
    /// Position in the *original* resolved chain; filtering never renumbers it.
    pub original_chain_ordinal: usize,
    pub target: Target,
    pub requested_sandbox: Sandbox,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_workdir: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir_identity: Option<WorkdirIdentity>,
    pub manifest: CapabilityManifest,
    pub decision: CapabilityAdmissionDecision,
}

/// Per-attempt audit context handed to the scoped factory seam.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptScope {
    pub execution: ExecutionScope,
    pub admission: CapabilityAdmissionEvidence,
}

/// Serializable, execution-id-independent capability plan frozen at a process
/// boundary (for example a detached parent handing work to its worker).
///
/// Rechecking this value proves that the worker resolved the same target
/// capabilities and the same opened directory identity.  It does not contain
/// or recreate the process-local directory handle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityPlanSnapshot {
    pub requested_sandbox: Sandbox,
    /// New detached submissions with a mutating workdir must transfer the
    /// opened directory descriptor; a worker may not recreate authority from
    /// the audit pathname.
    #[serde(default)]
    pub requires_inherited_workdir: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_workdir: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir_identity: Option<WorkdirIdentity>,
    pub targets: Vec<CapabilityTargetSnapshot>,
}

/// One original-chain entry in [`CapabilityPlanSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityTargetSnapshot {
    pub original_chain_ordinal: usize,
    pub target: Target,
    pub manifest: CapabilityManifest,
    pub decision: CapabilityAdmissionDecision,
}

impl AttemptScope {
    pub fn original_chain_ordinal(&self) -> usize {
        self.admission.original_chain_ordinal
    }
}

/// Typed pre-execution error when the primary target cannot satisfy the
/// requested sandbox capability.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityAdmissionError {
    pub evidence: CapabilityAdmissionEvidence,
}

impl fmt::Display for CapabilityAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self.evidence.decision {
            CapabilityAdmissionDecision::Rejected(reason) => reason,
            CapabilityAdmissionDecision::Admitted => {
                return f.write_str("capability admission failed without a rejection reason");
            }
        };
        write!(
            f,
            "execution {} rejected primary target at chain ordinal {} for sandbox {:?}: {}",
            self.evidence.execution_id,
            self.evidence.original_chain_ordinal,
            self.evidence.requested_sandbox,
            reason
        )
    }
}

impl std::error::Error for CapabilityAdmissionError {}

impl CapabilityAdmissionError {
    pub(crate) fn into_vyane_error(self) -> VyaneError {
        VyaneError::with_source(ErrorKind::Unsupported, self.to_string(), self)
    }
}
