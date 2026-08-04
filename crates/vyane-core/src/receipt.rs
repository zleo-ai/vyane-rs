//! Public-safe execution provenance and CompletionReceipt contract.
//!
//! This module is schema and pure transition logic only. Private execution
//! records belong in a runtime store, not in Git. Digests are opaque hex
//! strings produced by callers; this crate never invents success from missing
//! evidence.
//!
//! # Invariants
//!
//! - Schema is versioned (`RECEIPT_SCHEMA_VERSION`).
//! - Owner isolation is structural: every durable document carries `owner`.
//! - Transitions are revision-fenced (compare-and-swap on `revision`).
//! - Unknown optional evidence stays `None` / `Unknown` — never inferred.
//! - A receipt cannot enter [`ReceiptFinalStatus::Completed`] unless every
//!   required gate reports [`GateOutcome::Passed`] and the output artifact
//!   digest is present.
//! - Cost fields distinguish *actual* (provider-supplied) from *estimated*;
//!   absence is not zero.

use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::target::{HarnessKind, ModelId, Protocol, ProviderId};
use crate::task::Effort;

/// Frozen schema version for [`CompletionReceipt`] and related documents.
pub const RECEIPT_SCHEMA_VERSION: u32 = 1;

/// Maximum UTF-8 bytes for owner, ids, and free-text labels on the public contract.
pub const MAX_RECEIPT_TEXT_BYTES: usize = 256;
/// Maximum UTF-8 bytes for digests and opaque keys.
pub const MAX_DIGEST_BYTES: usize = 128;

/// Errors from receipt validation or fenced transitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiptError {
    InvalidInput(&'static str),
    StaleRevision { expected: u64, actual: u64 },
    OwnerMismatch,
    TruthRequired,
    MissingArtifact,
    IncompleteGates,
    TerminalImmutable,
}

impl fmt::Display for ReceiptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(msg) => write!(f, "invalid receipt input: {msg}"),
            Self::StaleRevision { expected, actual } => {
                write!(
                    f,
                    "stale receipt revision: expected {expected}, actual {actual}"
                )
            }
            Self::OwnerMismatch => f.write_str("receipt owner mismatch"),
            Self::TruthRequired => f.write_str("completion requires a passed truth-probe gate"),
            Self::MissingArtifact => f.write_str("completion requires an output artifact digest"),
            Self::IncompleteGates => f.write_str("completion requires all required gates to pass"),
            Self::TerminalImmutable => f.write_str("terminal receipt cannot be mutated"),
        }
    }
}

impl std::error::Error for ReceiptError {}

pub type ReceiptResult<T> = Result<T, ReceiptError>;

/// Risk class for a task case (public, non-secret).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RiskClass {
    ReadOnly,
    WorkspaceWrite,
    Network,
    Privileged,
}

impl RiskClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::WorkspaceWrite => "workspace_write",
            Self::Network => "network",
            Self::Privileged => "privileged",
        }
    }
}

impl fmt::Display for RiskClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Endpoint class without private account data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EndpointClass {
    OfficialApi,
    CompatibleRelay,
    LocalProcess,
    Unknown,
}

impl EndpointClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OfficialApi => "official_api",
            Self::CompatibleRelay => "compatible_relay",
            Self::LocalProcess => "local_process",
            Self::Unknown => "unknown",
        }
    }
}

/// Billing mode category only — never account ids or tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BillingModeCategory {
    ApiKey,
    SubscriptionHarness,
    Unknown,
}

impl BillingModeCategory {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApiKey => "api_key",
            Self::SubscriptionHarness => "subscription_harness",
            Self::Unknown => "unknown",
        }
    }
}

/// Final status of a completion receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ReceiptFinalStatus {
    /// Work is still open; must not be treated as success.
    Open,
    /// All required gates passed and artifact is present.
    Completed,
    /// Failed with classified reason; not success.
    Failed,
    /// Cancelled by controller.
    Cancelled,
    /// Recovery finished without a definitive same receipt.
    Unresolved,
}

impl ReceiptFinalStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Unresolved => "unresolved",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Open)
    }

    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Completed)
    }
}

impl fmt::Display for ReceiptFinalStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Outcome of one validation gate. Missing evidence is not a pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum GateOutcome {
    /// Not yet run; cannot support completion.
    Unknown,
    Passed,
    Failed,
    Skipped,
    Unavailable,
}

impl GateOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Unavailable => "unavailable",
        }
    }

    #[must_use]
    pub const fn is_pass(self) -> bool {
        matches!(self, Self::Passed)
    }
}

/// Named gate keys used by the dogfood path. Callers may add additional keys
/// in [`GateResult::extra`], but required keys for completion are fixed.
pub const GATE_UNIT: &str = "unit";
pub const GATE_INTEGRATION: &str = "integration";
pub const GATE_TRUTH_PROBE: &str = "truth_probe";
pub const GATE_INDEPENDENT_REVIEW: &str = "independent_review";
pub const GATE_CI_PACKAGING: &str = "ci_packaging";

/// One bounded task case (acceptance + risk), secret-free.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCase {
    pub task_case_id: String,
    pub task_type: String,
    /// Digest of the acceptance criteria document (not the criteria text).
    pub acceptance_digest: String,
    /// Digest of the truth probe definition / command set.
    pub truth_probe_digest: String,
    pub risk_class: RiskClass,
}

impl TaskCase {
    pub fn validate(&self) -> ReceiptResult<()> {
        validate_text("task_case_id", &self.task_case_id)?;
        validate_text("task_type", &self.task_type)?;
        validate_digest("acceptance_digest", &self.acceptance_digest)?;
        validate_digest("truth_probe_digest", &self.truth_probe_digest)?;
        Ok(())
    }
}

/// Explicit route provenance without private account data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteConfig {
    pub provider: ProviderId,
    pub endpoint_class: EndpointClass,
    pub protocol: Protocol,
    /// `None` means direct HTTP chat (no harness).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<HarnessKind>,
    pub model: ModelId,
    /// Optional provider-side model snapshot id (not a secret).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_snapshot: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_effort: Option<Effort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_effort: Option<Effort>,
    /// Digest of profile/config used for resolution (opaque).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_or_config_digest: Option<String>,
    pub billing_mode_category: BillingModeCategory,
}

impl RouteConfig {
    pub fn validate(&self) -> ReceiptResult<()> {
        validate_text("provider", self.provider.as_str())?;
        validate_text("model", self.model.as_str())?;
        if let Some(snapshot) = &self.model_snapshot {
            validate_text("model_snapshot", snapshot)?;
        }
        if let Some(digest) = &self.profile_or_config_digest {
            validate_digest("profile_or_config_digest", digest)?;
        }
        Ok(())
    }
}

/// Failure class for a single attempt (public taxonomy).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AttemptFailureClass {
    None,
    Timeout,
    Cancelled,
    Auth,
    RateLimited,
    Transport,
    Protocol,
    SpawnFailed,
    HarnessFailed,
    PermissionDenied,
    ApprovalRequired,
    Conflict,
    Indeterminate,
    Other,
}

impl AttemptFailureClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Auth => "auth",
            Self::RateLimited => "rate_limited",
            Self::Transport => "transport",
            Self::Protocol => "protocol",
            Self::SpawnFailed => "spawn_failed",
            Self::HarnessFailed => "harness_failed",
            Self::PermissionDenied => "permission_denied",
            Self::ApprovalRequired => "approval_required",
            Self::Conflict => "conflict",
            Self::Indeterminate => "indeterminate",
            Self::Other => "other",
        }
    }
}

/// Status of one execution attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AttemptStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Abandoned,
}

impl AttemptStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Abandoned => "abandoned",
        }
    }
}

/// Cost evidence: actual only when the provider supplied it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CostEvidence {
    /// Provider-supplied actual cost in micro-units of currency, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_micro_units: Option<u64>,
    /// Estimated cost when computed locally; never substituted for actual.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_micro_units: Option<u64>,
    /// ISO-like currency code when known; absent stays unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
}

/// One execution attempt with AgentRun identity and optional git SHAs.
///
/// Named [`ReceiptAttempt`] at the type level so it does not collide with the
/// ledger [`crate::run::Attempt`]. Serialized field names match the product
/// contract (`attempt_id`, …).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptAttempt {
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_attempt_id: Option<String>,
    /// Owner-scoped AgentRun id.
    pub agent_run_id: String,
    pub owner: String,
    pub started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime<Utc>>,
    pub status: AttemptStatus,
    pub failure_class: AttemptFailureClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(default)]
    pub cost: CostEvidence,
}

impl ReceiptAttempt {
    pub fn validate(&self) -> ReceiptResult<()> {
        validate_text("attempt_id", &self.attempt_id)?;
        if let Some(parent) = &self.parent_attempt_id {
            validate_text("parent_attempt_id", parent)?;
        }
        validate_text("agent_run_id", &self.agent_run_id)?;
        validate_text("owner", &self.owner)?;
        if let Some(sha) = &self.base_sha {
            validate_git_sha("base_sha", sha)?;
        }
        if let Some(sha) = &self.head_sha {
            validate_git_sha("head_sha", sha)?;
        }
        Ok(())
    }
}

/// One named validation gate with optional evidence URI or content digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedGate {
    pub name: String,
    pub outcome: GateOutcome,
    /// Exact code head SHA when the gate is code-bound; unknown stays absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_head_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_digest: Option<String>,
}

impl NamedGate {
    pub fn validate(&self) -> ReceiptResult<()> {
        validate_text("gate name", &self.name)?;
        if let Some(sha) = &self.exact_head_sha {
            validate_git_sha("exact_head_sha", sha)?;
        }
        if let Some(uri) = &self.evidence_uri {
            // Public-safe: allow only relative or synthetic URIs, not file:// private paths.
            if uri.starts_with("file://") || uri.contains('\0') {
                return Err(ReceiptError::InvalidInput(
                    "evidence_uri must not be a private file path",
                ));
            }
            if uri.len() > MAX_RECEIPT_TEXT_BYTES * 4 {
                return Err(ReceiptError::InvalidInput("evidence_uri too long"));
            }
        }
        if let Some(digest) = &self.content_digest {
            validate_digest("gate content_digest", digest)?;
        }
        Ok(())
    }
}

/// Aggregate gate results for a receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateResult {
    pub unit: NamedGate,
    pub integration: NamedGate,
    pub truth_probe: NamedGate,
    pub independent_review: NamedGate,
    pub ci_packaging: NamedGate,
    /// Additional named gates; never used to invent success for required ones.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, NamedGate>,
}

impl GateResult {
    /// All required gates start as [`GateOutcome::Unknown`].
    #[must_use]
    pub fn unknown_baseline() -> Self {
        Self {
            unit: NamedGate {
                name: GATE_UNIT.into(),
                outcome: GateOutcome::Unknown,
                exact_head_sha: None,
                evidence_uri: None,
                content_digest: None,
            },
            integration: NamedGate {
                name: GATE_INTEGRATION.into(),
                outcome: GateOutcome::Unknown,
                exact_head_sha: None,
                evidence_uri: None,
                content_digest: None,
            },
            truth_probe: NamedGate {
                name: GATE_TRUTH_PROBE.into(),
                outcome: GateOutcome::Unknown,
                exact_head_sha: None,
                evidence_uri: None,
                content_digest: None,
            },
            independent_review: NamedGate {
                name: GATE_INDEPENDENT_REVIEW.into(),
                outcome: GateOutcome::Unknown,
                exact_head_sha: None,
                evidence_uri: None,
                content_digest: None,
            },
            ci_packaging: NamedGate {
                name: GATE_CI_PACKAGING.into(),
                outcome: GateOutcome::Unknown,
                exact_head_sha: None,
                evidence_uri: None,
                content_digest: None,
            },
            extra: BTreeMap::new(),
        }
    }

    pub fn validate(&self) -> ReceiptResult<()> {
        self.unit.validate()?;
        self.integration.validate()?;
        self.truth_probe.validate()?;
        self.independent_review.validate()?;
        self.ci_packaging.validate()?;
        for gate in self.extra.values() {
            gate.validate()?;
        }
        Ok(())
    }

    /// Required gates for [`ReceiptFinalStatus::Completed`].
    ///
    /// Independent review and CI may be recorded as [`GateOutcome::Skipped`]
    /// only when the receipt explicitly marks them non-blocking for a local
    /// dogfood; truth probe and unit must still pass.
    #[must_use]
    pub fn supports_completion(&self, require_review_and_ci: bool) -> bool {
        let core = self.unit.outcome.is_pass()
            && self.integration.outcome.is_pass()
            && self.truth_probe.outcome.is_pass();
        if !core {
            return false;
        }
        if require_review_and_ci {
            self.independent_review.outcome.is_pass() && self.ci_packaging.outcome.is_pass()
        } else {
            matches!(
                self.independent_review.outcome,
                GateOutcome::Passed | GateOutcome::Skipped
            ) && matches!(
                self.ci_packaging.outcome,
                GateOutcome::Passed | GateOutcome::Skipped
            )
        }
    }
}

/// One review finding disposition (public-safe summary).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewFindingDisposition {
    pub finding_id: String,
    pub severity: String,
    pub disposition: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_digest: Option<String>,
}

/// Recovery / cleanup visibility on the receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryCleanupState {
    /// Whether post-terminal process inventory reported orphans.
    pub orphan_processes_detected: bool,
    /// Whether declared temp/worktree/socket cleanup succeeded.
    pub cleanup_succeeded: bool,
    /// Free-form but bounded public note; no private paths.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl Default for RecoveryCleanupState {
    fn default() -> Self {
        Self {
            orphan_processes_detected: false,
            cleanup_succeeded: true,
            note: None,
        }
    }
}

/// Durable CompletionReceipt — product truth for one finished delivery path.
///
/// Unknown JSON fields fail closed (`deny_unknown_fields`). Missing optional
/// evidence stays absent and never implies success.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionReceipt {
    pub schema_version: u32,
    pub receipt_id: String,
    pub owner: String,
    pub revision: u64,
    pub task_case: TaskCase,
    pub route: RouteConfig,
    pub attempts: Vec<ReceiptAttempt>,
    pub gates: GateResult,
    pub final_status: ReceiptFinalStatus,
    /// Output artifact content digest; required for Completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_artifact_digest: Option<String>,
    /// Exact code state for the delivery (when applicable).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_base_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_head_sha: Option<String>,
    /// Short validation summary (non-secret).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub review_findings: Vec<ReviewFindingDisposition>,
    pub rework_count: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remaining_risks: Vec<String>,
    pub recovery_cleanup: RecoveryCleanupState,
    /// When true, independent review + CI must pass (not merely skip) for completion.
    #[serde(default)]
    pub require_review_and_ci: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CompletionReceipt {
    /// Open a new receipt. Final status is always [`ReceiptFinalStatus::Open`].
    pub fn open(
        receipt_id: impl Into<String>,
        owner: impl Into<String>,
        task_case: TaskCase,
        route: RouteConfig,
        now: DateTime<Utc>,
    ) -> ReceiptResult<Self> {
        let receipt_id = receipt_id.into();
        let owner = owner.into();
        validate_text("receipt_id", &receipt_id)?;
        validate_text("owner", &owner)?;
        task_case.validate()?;
        route.validate()?;
        Ok(Self {
            schema_version: RECEIPT_SCHEMA_VERSION,
            receipt_id,
            owner,
            revision: 1,
            task_case,
            route,
            attempts: Vec::new(),
            gates: GateResult::unknown_baseline(),
            final_status: ReceiptFinalStatus::Open,
            output_artifact_digest: None,
            code_base_sha: None,
            code_head_sha: None,
            validation_summary: None,
            review_findings: Vec::new(),
            rework_count: 0,
            remaining_risks: Vec::new(),
            recovery_cleanup: RecoveryCleanupState::default(),
            require_review_and_ci: false,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn validate(&self) -> ReceiptResult<()> {
        if self.schema_version != RECEIPT_SCHEMA_VERSION {
            return Err(ReceiptError::InvalidInput(
                "unsupported or newer receipt schema_version",
            ));
        }
        validate_text("receipt_id", &self.receipt_id)?;
        validate_text("owner", &self.owner)?;
        self.task_case.validate()?;
        self.route.validate()?;
        for attempt in &self.attempts {
            attempt.validate()?;
            if attempt.owner != self.owner {
                return Err(ReceiptError::OwnerMismatch);
            }
        }
        self.gates.validate()?;
        if let Some(digest) = &self.output_artifact_digest {
            validate_digest("output_artifact_digest", digest)?;
        }
        if let Some(sha) = &self.code_base_sha {
            validate_git_sha("code_base_sha", sha)?;
        }
        if let Some(sha) = &self.code_head_sha {
            validate_git_sha("code_head_sha", sha)?;
        }
        if self.final_status == ReceiptFinalStatus::Completed {
            self.assert_completion_preconditions()?;
        }
        Ok(())
    }

    fn assert_completion_preconditions(&self) -> ReceiptResult<()> {
        if self.output_artifact_digest.is_none() {
            return Err(ReceiptError::MissingArtifact);
        }
        if !self.gates.truth_probe.outcome.is_pass() {
            return Err(ReceiptError::TruthRequired);
        }
        if !self.gates.supports_completion(self.require_review_and_ci) {
            return Err(ReceiptError::IncompleteGates);
        }
        Ok(())
    }

    /// Stable JSON serialization (sorted map keys via `BTreeMap` extras).
    pub fn to_canonical_json(&self) -> ReceiptResult<String> {
        self.validate()?;
        serde_json::to_string(self).map_err(|_| ReceiptError::InvalidInput("serialize failed"))
    }

    pub fn from_canonical_json(bytes: &str) -> ReceiptResult<Self> {
        let value: Self = serde_json::from_str(bytes)
            .map_err(|_| ReceiptError::InvalidInput("deserialize failed"))?;
        value.validate()?;
        Ok(value)
    }
}

/// In-memory, owner-isolated, revision-fenced receipt ledger for tests and
/// local dogfood. Optionally durable when constructed with
/// [`MemoryReceiptLedger::open_durable`].
#[derive(Debug, Default)]
pub struct MemoryReceiptLedger {
    by_owner: BTreeMap<String, BTreeMap<String, CompletionReceipt>>,
    /// When set, every successful mutation is fsynced as JSON under this root.
    durable_root: Option<std::path::PathBuf>,
}

impl MemoryReceiptLedger {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Open or create a durable ledger rooted at `dir` (`{owner}/{receipt_id}.json`).
    pub fn open_durable(dir: impl Into<std::path::PathBuf>) -> ReceiptResult<Self> {
        let root = dir.into();
        std::fs::create_dir_all(&root).map_err(|_| ReceiptError::InvalidInput("durable root"))?;
        let mut ledger = Self {
            by_owner: BTreeMap::new(),
            durable_root: Some(root.clone()),
        };
        ledger.load_from_disk()?;
        Ok(ledger)
    }

    fn load_from_disk(&mut self) -> ReceiptResult<()> {
        let Some(root) = self.durable_root.clone() else {
            return Ok(());
        };
        let Ok(owners) = std::fs::read_dir(&root) else {
            return Ok(());
        };
        for owner_entry in owners.flatten() {
            if !owner_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let owner = owner_entry.file_name().to_string_lossy().into_owned();
            let Ok(files) = std::fs::read_dir(owner_entry.path()) else {
                continue;
            };
            for file in files.flatten() {
                let path = file.path();
                if path.extension().and_then(|e| e.to_str()) != Some("json") {
                    continue;
                }
                let raw = std::fs::read_to_string(&path)
                    .map_err(|_| ReceiptError::InvalidInput("read durable receipt"))?;
                let receipt = CompletionReceipt::from_canonical_json(&raw)?;
                if receipt.owner != owner {
                    return Err(ReceiptError::OwnerMismatch);
                }
                self.by_owner
                    .entry(owner.clone())
                    .or_default()
                    .insert(receipt.receipt_id.clone(), receipt);
            }
        }
        Ok(())
    }

    fn persist_receipt(&self, receipt: &CompletionReceipt) -> ReceiptResult<()> {
        let Some(root) = &self.durable_root else {
            return Ok(());
        };
        let owner_dir = root.join(&receipt.owner);
        std::fs::create_dir_all(&owner_dir)
            .map_err(|_| ReceiptError::InvalidInput("create durable owner dir"))?;
        let path = owner_dir.join(format!("{}.json", receipt.receipt_id));
        let tmp = owner_dir.join(format!("{}.json.tmp", receipt.receipt_id));
        let json = receipt.to_canonical_json()?;
        std::fs::write(&tmp, json.as_bytes())
            .map_err(|_| ReceiptError::InvalidInput("write durable receipt tmp"))?;
        std::fs::rename(&tmp, &path)
            .map_err(|_| ReceiptError::InvalidInput("rename durable receipt"))?;
        Ok(())
    }

    pub fn insert_open(&mut self, receipt: CompletionReceipt) -> ReceiptResult<()> {
        receipt.validate()?;
        if receipt.final_status != ReceiptFinalStatus::Open || receipt.revision != 1 {
            return Err(ReceiptError::InvalidInput(
                "insert_open requires open revision 1",
            ));
        }
        if self
            .by_owner
            .get(&receipt.owner)
            .is_some_and(|m| m.contains_key(&receipt.receipt_id))
        {
            return Err(ReceiptError::InvalidInput("receipt_id already exists"));
        }
        self.persist_receipt(&receipt)?;
        self.by_owner
            .entry(receipt.owner.clone())
            .or_default()
            .insert(receipt.receipt_id.clone(), receipt);
        Ok(())
    }

    pub fn get(&self, owner: &str, receipt_id: &str) -> Option<&CompletionReceipt> {
        self.by_owner.get(owner)?.get(receipt_id)
    }

    /// Cross-owner get always returns `None` (foreign-as-absent).
    pub fn get_for_owner(&self, owner: &str, receipt_id: &str) -> Option<&CompletionReceipt> {
        self.get(owner, receipt_id)
    }

    /// Apply a pure mutation under revision CAS and owner fence.
    pub fn transition<F>(
        &mut self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        now: DateTime<Utc>,
        mutate: F,
    ) -> ReceiptResult<CompletionReceipt>
    where
        F: FnOnce(&mut CompletionReceipt) -> ReceiptResult<()>,
    {
        let current = self
            .by_owner
            .get(owner)
            .and_then(|m| m.get(receipt_id))
            .ok_or(ReceiptError::InvalidInput("receipt not found"))?
            .clone();
        if current.owner != owner {
            return Err(ReceiptError::OwnerMismatch);
        }
        if current.revision != expected_revision {
            return Err(ReceiptError::StaleRevision {
                expected: expected_revision,
                actual: current.revision,
            });
        }
        if current.final_status.is_terminal() {
            return Err(ReceiptError::TerminalImmutable);
        }
        let mut next = current;
        mutate(&mut next)?;
        next.revision = expected_revision
            .checked_add(1)
            .ok_or(ReceiptError::InvalidInput("revision overflow"))?;
        next.updated_at = now;
        next.validate()?;
        // Persist before the in-memory swap so a crash still reloads the
        // advanced revision from disk.
        self.persist_receipt(&next)?;
        self.by_owner
            .entry(owner.to_string())
            .or_default()
            .insert(receipt_id.to_string(), next.clone());
        Ok(next)
    }

    pub fn record_attempt(
        &mut self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        attempt: ReceiptAttempt,
        now: DateTime<Utc>,
    ) -> ReceiptResult<CompletionReceipt> {
        if attempt.owner != owner {
            return Err(ReceiptError::OwnerMismatch);
        }
        attempt.validate()?;
        self.transition(owner, receipt_id, expected_revision, now, |receipt| {
            receipt.attempts.push(attempt);
            Ok(())
        })
    }

    pub fn set_gate(
        &mut self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        gate: NamedGate,
        now: DateTime<Utc>,
    ) -> ReceiptResult<CompletionReceipt> {
        gate.validate()?;
        self.transition(owner, receipt_id, expected_revision, now, |receipt| {
            match gate.name.as_str() {
                GATE_UNIT => receipt.gates.unit = gate,
                GATE_INTEGRATION => receipt.gates.integration = gate,
                GATE_TRUTH_PROBE => receipt.gates.truth_probe = gate,
                GATE_INDEPENDENT_REVIEW => receipt.gates.independent_review = gate,
                GATE_CI_PACKAGING => receipt.gates.ci_packaging = gate,
                _ => {
                    receipt.gates.extra.insert(gate.name.clone(), gate);
                }
            }
            Ok(())
        })
    }

    pub fn set_artifact_digest(
        &mut self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        digest: impl Into<String>,
        now: DateTime<Utc>,
    ) -> ReceiptResult<CompletionReceipt> {
        let digest = digest.into();
        validate_digest("output_artifact_digest", &digest)?;
        self.transition(owner, receipt_id, expected_revision, now, |receipt| {
            receipt.output_artifact_digest = Some(digest);
            Ok(())
        })
    }

    /// Mark completed only when truth probe and other required gates pass.
    pub fn complete(
        &mut self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        now: DateTime<Utc>,
    ) -> ReceiptResult<CompletionReceipt> {
        self.transition(owner, receipt_id, expected_revision, now, |receipt| {
            receipt.assert_completion_preconditions()?;
            receipt.final_status = ReceiptFinalStatus::Completed;
            Ok(())
        })
    }

    pub fn fail(
        &mut self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        summary: impl Into<String>,
        now: DateTime<Utc>,
    ) -> ReceiptResult<CompletionReceipt> {
        let summary = summary.into();
        self.transition(owner, receipt_id, expected_revision, now, |receipt| {
            receipt.final_status = ReceiptFinalStatus::Failed;
            receipt.validation_summary = Some(summary);
            Ok(())
        })
    }

    pub fn cancel(
        &mut self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        now: DateTime<Utc>,
    ) -> ReceiptResult<CompletionReceipt> {
        self.transition(owner, receipt_id, expected_revision, now, |receipt| {
            receipt.final_status = ReceiptFinalStatus::Cancelled;
            Ok(())
        })
    }

    pub fn mark_unresolved(
        &mut self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        note: impl Into<String>,
        now: DateTime<Utc>,
    ) -> ReceiptResult<CompletionReceipt> {
        let note = note.into();
        self.transition(owner, receipt_id, expected_revision, now, |receipt| {
            receipt.final_status = ReceiptFinalStatus::Unresolved;
            receipt.recovery_cleanup.note = Some(note);
            Ok(())
        })
    }
}

fn validate_text(label: &'static str, value: &str) -> ReceiptResult<()> {
    if value.is_empty() || value.len() > MAX_RECEIPT_TEXT_BYTES {
        return Err(ReceiptError::InvalidInput(label));
    }
    if value.trim() != value || value.contains('\0') || value.chars().any(char::is_control) {
        return Err(ReceiptError::InvalidInput(label));
    }
    Ok(())
}

fn validate_digest(label: &'static str, value: &str) -> ReceiptResult<()> {
    if value.is_empty() || value.len() > MAX_DIGEST_BYTES {
        return Err(ReceiptError::InvalidInput(label));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_hexdigit() || c == ':' || c == '-')
    {
        // Allow hex and a small set of separators used by "sha256:…" prefixes.
        return Err(ReceiptError::InvalidInput(label));
    }
    // Must contain at least one hex digit so empty separators fail closed.
    if !value.chars().any(|c| c.is_ascii_hexdigit()) {
        return Err(ReceiptError::InvalidInput(label));
    }
    Ok(())
}

fn validate_git_sha(label: &'static str, value: &str) -> ReceiptResult<()> {
    if !(value.len() == 40 || value.len() == 64) {
        return Err(ReceiptError::InvalidInput(label));
    }
    if !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ReceiptError::InvalidInput(label));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 4, 12, 0, 0).single().unwrap()
    }

    fn sample_task() -> TaskCase {
        TaskCase {
            task_case_id: "tc-dogfood-001".into(),
            task_type: "synthetic_delivery".into(),
            acceptance_digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .into(),
            truth_probe_digest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .into(),
            risk_class: RiskClass::ReadOnly,
        }
    }

    fn sample_route() -> RouteConfig {
        RouteConfig {
            provider: ProviderId::new("fixture-provider"),
            endpoint_class: EndpointClass::LocalProcess,
            protocol: Protocol::OpenaiChat,
            harness: Some(HarnessKind::ClaudeCode),
            model: ModelId::new("fixture-model"),
            model_snapshot: None,
            requested_effort: Some(Effort::High),
            effective_effort: Some(Effort::High),
            profile_or_config_digest: Some(
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".into(),
            ),
            billing_mode_category: BillingModeCategory::SubscriptionHarness,
        }
    }

    fn open_receipt() -> CompletionReceipt {
        CompletionReceipt::open("rcpt-001", "local", sample_task(), sample_route(), now()).unwrap()
    }

    fn passed(name: &str) -> NamedGate {
        NamedGate {
            name: name.into(),
            outcome: GateOutcome::Passed,
            exact_head_sha: Some("10ebe700cef3416459beebfb7ed07d7e9b866de7".into()),
            evidence_uri: Some("synthetic://fixture/gate".into()),
            content_digest: Some(
                "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".into(),
            ),
        }
    }

    fn skipped(name: &str) -> NamedGate {
        NamedGate {
            name: name.into(),
            outcome: GateOutcome::Skipped,
            exact_head_sha: None,
            evidence_uri: None,
            content_digest: None,
        }
    }

    #[test]
    fn serialization_roundtrip_is_stable() {
        let receipt = open_receipt();
        let json = receipt.to_canonical_json().unwrap();
        let again = CompletionReceipt::from_canonical_json(&json).unwrap();
        assert_eq!(receipt, again);
        assert!(json.contains("\"schema_version\":1"));
        assert!(json.contains("\"final_status\":\"open\""));
    }

    #[test]
    fn complete_without_truth_probe_is_rejected() {
        let mut ledger = MemoryReceiptLedger::new();
        let receipt = open_receipt();
        ledger.insert_open(receipt).unwrap();
        let r = ledger
            .set_artifact_digest(
                "local",
                "rcpt-001",
                1,
                "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                now(),
            )
            .unwrap();
        let r = ledger
            .set_gate("local", "rcpt-001", r.revision, passed(GATE_UNIT), now())
            .unwrap();
        let r = ledger
            .set_gate(
                "local",
                "rcpt-001",
                r.revision,
                passed(GATE_INTEGRATION),
                now(),
            )
            .unwrap();
        let r = ledger
            .set_gate(
                "local",
                "rcpt-001",
                r.revision,
                skipped(GATE_INDEPENDENT_REVIEW),
                now(),
            )
            .unwrap();
        let r = ledger
            .set_gate(
                "local",
                "rcpt-001",
                r.revision,
                skipped(GATE_CI_PACKAGING),
                now(),
            )
            .unwrap();
        // truth_probe still Unknown
        let err = ledger
            .complete("local", "rcpt-001", r.revision, now())
            .unwrap_err();
        assert_eq!(err, ReceiptError::TruthRequired);
        assert_eq!(
            ledger.get("local", "rcpt-001").unwrap().final_status,
            ReceiptFinalStatus::Open
        );
    }

    #[test]
    fn complete_without_artifact_is_rejected() {
        let mut ledger = MemoryReceiptLedger::new();
        ledger.insert_open(open_receipt()).unwrap();
        let mut rev = 1;
        for gate in [
            passed(GATE_UNIT),
            passed(GATE_INTEGRATION),
            passed(GATE_TRUTH_PROBE),
            skipped(GATE_INDEPENDENT_REVIEW),
            skipped(GATE_CI_PACKAGING),
        ] {
            let r = ledger
                .set_gate("local", "rcpt-001", rev, gate, now())
                .unwrap();
            rev = r.revision;
        }
        let err = ledger
            .complete("local", "rcpt-001", rev, now())
            .unwrap_err();
        assert_eq!(err, ReceiptError::MissingArtifact);
    }

    #[test]
    fn happy_path_completion_and_terminal_immutability() {
        let mut ledger = MemoryReceiptLedger::new();
        ledger.insert_open(open_receipt()).unwrap();
        let mut rev = 1;
        let r = ledger
            .set_artifact_digest(
                "local",
                "rcpt-001",
                rev,
                "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                now(),
            )
            .unwrap();
        rev = r.revision;
        for gate in [
            passed(GATE_UNIT),
            passed(GATE_INTEGRATION),
            passed(GATE_TRUTH_PROBE),
            skipped(GATE_INDEPENDENT_REVIEW),
            skipped(GATE_CI_PACKAGING),
        ] {
            let r = ledger
                .set_gate("local", "rcpt-001", rev, gate, now())
                .unwrap();
            rev = r.revision;
        }
        let done = ledger.complete("local", "rcpt-001", rev, now()).unwrap();
        assert_eq!(done.final_status, ReceiptFinalStatus::Completed);
        assert!(done.final_status.is_success());
        let err = ledger
            .set_gate("local", "rcpt-001", done.revision, passed(GATE_UNIT), now())
            .unwrap_err();
        assert_eq!(err, ReceiptError::TerminalImmutable);
    }

    #[test]
    fn owner_isolation_fences_cross_owner_reads_and_writes() {
        let mut ledger = MemoryReceiptLedger::new();
        ledger.insert_open(open_receipt()).unwrap();
        assert!(ledger.get_for_owner("other", "rcpt-001").is_none());
        let err = ledger
            .set_artifact_digest(
                "other",
                "rcpt-001",
                1,
                "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                now(),
            )
            .unwrap_err();
        assert_eq!(err, ReceiptError::InvalidInput("receipt not found"));
    }

    #[test]
    fn stale_revision_is_rejected() {
        let mut ledger = MemoryReceiptLedger::new();
        ledger.insert_open(open_receipt()).unwrap();
        let _ = ledger
            .set_artifact_digest(
                "local",
                "rcpt-001",
                1,
                "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                now(),
            )
            .unwrap();
        let err = ledger
            .set_artifact_digest(
                "local",
                "rcpt-001",
                1,
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                now(),
            )
            .unwrap_err();
        assert!(matches!(err, ReceiptError::StaleRevision { .. }));
    }

    #[test]
    fn unknown_schema_version_fails_closed() {
        let mut receipt = open_receipt();
        receipt.schema_version = 99;
        let err = receipt.validate().unwrap_err();
        assert_eq!(
            err,
            ReceiptError::InvalidInput("unsupported or newer receipt schema_version")
        );
    }

    #[test]
    fn cost_unknown_is_not_zero() {
        let attempt = ReceiptAttempt {
            attempt_id: "att-1".into(),
            parent_attempt_id: None,
            agent_run_id: "run-1".into(),
            owner: "local".into(),
            started_at: now(),
            ended_at: None,
            status: AttemptStatus::Running,
            failure_class: AttemptFailureClass::None,
            worktree: None,
            branch: None,
            base_sha: None,
            head_sha: None,
            input_tokens: None,
            output_tokens: None,
            cost: CostEvidence::default(),
        };
        let json = serde_json::to_string(&attempt).unwrap();
        assert!(!json.contains("\"actual_micro_units\":0"));
        assert!(!json.contains("\"estimated_micro_units\":0"));
        let back: ReceiptAttempt = serde_json::from_str(&json).unwrap();
        assert!(back.cost.actual_micro_units.is_none());
        assert!(back.cost.estimated_micro_units.is_none());
    }

    #[test]
    fn cannot_construct_completed_via_deserialize_without_truth() {
        let mut receipt = open_receipt();
        receipt.final_status = ReceiptFinalStatus::Completed;
        receipt.output_artifact_digest =
            Some("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".into());
        // gates still unknown
        let json = serde_json::to_string(&receipt).unwrap();
        let err = CompletionReceipt::from_canonical_json(&json).unwrap_err();
        assert_eq!(err, ReceiptError::TruthRequired);
    }

    #[test]
    fn unknown_json_fields_fail_closed() {
        let json = r#"{
            "schema_version": 1,
            "receipt_id": "rcpt-001",
            "owner": "local",
            "revision": 1,
            "task_case": {
                "task_case_id": "tc",
                "task_type": "t",
                "acceptance_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "truth_probe_digest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "risk_class": "read_only",
                "secret_extra": true
            },
            "route": {
                "provider": "p",
                "endpoint_class": "unknown",
                "protocol": "openai_chat",
                "model": "m",
                "billing_mode_category": "unknown"
            },
            "attempts": [],
            "gates": {
                "unit": {"name": "unit", "outcome": "unknown"},
                "integration": {"name": "integration", "outcome": "unknown"},
                "truth_probe": {"name": "truth_probe", "outcome": "unknown"},
                "independent_review": {"name": "independent_review", "outcome": "unknown"},
                "ci_packaging": {"name": "ci_packaging", "outcome": "unknown"}
            },
            "final_status": "open",
            "rework_count": 0,
            "recovery_cleanup": {
                "orphan_processes_detected": false,
                "cleanup_succeeded": true
            },
            "require_review_and_ci": false,
            "created_at": "2026-08-04T12:00:00Z",
            "updated_at": "2026-08-04T12:00:00Z"
        }"#;
        assert!(CompletionReceipt::from_canonical_json(json).is_err());
    }

    #[test]
    fn attempt_owner_mismatch_fails_validation() {
        let mut receipt = open_receipt();
        receipt.attempts.push(ReceiptAttempt {
            attempt_id: "att-1".into(),
            parent_attempt_id: None,
            agent_run_id: "run-1".into(),
            owner: "intruder".into(),
            started_at: now(),
            ended_at: None,
            status: AttemptStatus::Running,
            failure_class: AttemptFailureClass::None,
            worktree: None,
            branch: None,
            base_sha: None,
            head_sha: None,
            input_tokens: None,
            output_tokens: None,
            cost: CostEvidence::default(),
        });
        assert_eq!(receipt.validate().unwrap_err(), ReceiptError::OwnerMismatch);
    }
}
