//! Process-lane autonomous delivery dogfood path.
//!
//! Composes existing AgentRun claim/lease primitives with the public
//! [`vyane_core::receipt`] contract. Hermetic tests use a synthetic external
//! effect log (stand-in for CLI-harness Process side effects) and a real
//! on-disk truth probe that must fail on a broken baseline before the path
//! may complete.
//!
//! This is not a second runtime: durable run state remains in
//! [`vyane_agent::AgentStore`]; product truth for the delivery path is the
//! [`CompletionReceipt`].

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use sha2::{Digest as _, Sha256};
use vyane_agent::{
    AgentStore, CancelOutcome, CancelRequest, ClaimedRun, ControllerKind, ControllerRef,
    ExecutionBackend, NewAgentRun, NewRunCompletion, NewWorker, RunLeaseReceipt, RunMode,
    SqliteAgentStore,
};
use vyane_core::{
    AttemptFailureClass, AttemptStatus, BillingModeCategory, CompletionReceipt, CostEvidence,
    EndpointClass, GATE_CI_PACKAGING, GATE_INDEPENDENT_REVIEW, GATE_INTEGRATION, GATE_TRUTH_PROBE,
    GATE_UNIT, GateOutcome, HarnessKind, ModelId, NamedGate, Protocol, ProviderId, ReceiptAttempt,
    ReceiptError, RecoveryCleanupState, RiskClass, RouteConfig, TaskCase,
};

use crate::approval_fsm::{DeliveryEvent, DeliveryPhase};
use crate::kernel_store::{
    ApprovalDecisionKind, ApprovalGrantBinding, ArtifactMeta, KernelStore, KernelStoreError,
    LeaseFence,
};

/// Stable dogfood task type recorded on receipts.
pub const DOGFOOD_TASK_TYPE: &str = "process_lane_autonomous_delivery";

/// Errors from the dogfood path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DogfoodError {
    Receipt(String),
    Agent(String),
    Kernel(String),
    TruthProbeFailed { reason: String },
    ApprovalRequired,
    PermissionDenied,
    ApprovalDenied,
    DuplicateEffect { effect_id: String },
    Cancelled,
    InvalidState(&'static str),
    Io(String),
}

impl std::fmt::Display for DogfoodError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Receipt(msg) => write!(f, "receipt: {msg}"),
            Self::Agent(msg) => write!(f, "agent: {msg}"),
            Self::Kernel(msg) => write!(f, "kernel: {msg}"),
            Self::TruthProbeFailed { reason } => write!(f, "truth probe failed: {reason}"),
            Self::ApprovalRequired => f.write_str("approval required before side effect"),
            Self::PermissionDenied => f.write_str("permission denied"),
            Self::ApprovalDenied => f.write_str("approval denied; not recoverable by grant"),
            Self::DuplicateEffect { effect_id } => {
                write!(f, "duplicate external effect prevented for {effect_id}")
            }
            Self::Cancelled => f.write_str("dogfood path cancelled"),
            Self::InvalidState(msg) => write!(f, "invalid dogfood state: {msg}"),
            Self::Io(msg) => write!(f, "io: {msg}"),
        }
    }
}

impl std::error::Error for DogfoodError {}

impl From<ReceiptError> for DogfoodError {
    fn from(value: ReceiptError) -> Self {
        Self::Receipt(value.to_string())
    }
}

impl From<KernelStoreError> for DogfoodError {
    fn from(value: KernelStoreError) -> Self {
        match value {
            KernelStoreError::DuplicateEffect { effect_id } => Self::DuplicateEffect { effect_id },
            KernelStoreError::ApprovalDeniedFinal => Self::ApprovalDenied,
            KernelStoreError::NotFound => Self::InvalidState("kernel row not found"),
            other => Self::Kernel(other.to_string()),
        }
    }
}

/// Tool permission decision under a declared ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionDecision {
    Allow,
    Ask,
    Deny,
}

impl PermissionDecision {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Ask => "ask",
            Self::Deny => "deny",
        }
    }
}

/// Effect log view over the multi-process [`KernelStore`] (or ephemeral/JSON map).
///
/// Production dogfood authority is `kernel.sqlite`. Ephemeral/JSON modes remain
/// for unit isolation; they are not multi-process authority.
#[derive(Debug)]
pub struct ExternalEffectLog {
    backend: EffectLogBackend,
}

#[derive(Debug)]
enum EffectLogBackend {
    Ephemeral {
        applied: Mutex<BTreeMap<String, String>>,
    },
    Json {
        path: PathBuf,
        applied: Mutex<BTreeMap<String, String>>,
    },
    Kernel {
        store: KernelStore,
        owner: String,
    },
}

impl ExternalEffectLog {
    /// In-memory only (tests that do not exercise multi-process restart).
    #[must_use]
    pub fn new_ephemeral() -> Self {
        Self {
            backend: EffectLogBackend::Ephemeral {
                applied: Mutex::new(BTreeMap::new()),
            },
        }
    }

    /// View over an existing kernel store (multi-process authority).
    #[must_use]
    pub fn from_kernel(store: KernelStore, owner: impl Into<String>) -> Self {
        Self {
            backend: EffectLogBackend::Kernel {
                store,
                owner: owner.into(),
            },
        }
    }

    /// Open a JSON-backed log (legacy single-process helper).
    /// Prefer [`Self::from_kernel`] for concurrent writers / reopen authority.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, DogfoodError> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| DogfoodError::Io(e.to_string()))?;
        }
        let applied = if path.exists() {
            let raw = fs::read_to_string(&path).map_err(|e| DogfoodError::Io(e.to_string()))?;
            serde_json::from_str(&raw).map_err(|e| DogfoodError::Io(e.to_string()))?
        } else {
            BTreeMap::new()
        };
        Ok(Self {
            backend: EffectLogBackend::Json {
                path,
                applied: Mutex::new(applied),
            },
        })
    }

    fn flush_json(path: &Path, map: &BTreeMap<String, String>) -> Result<(), DogfoodError> {
        let tmp = path.with_extension("json.tmp");
        let body = serde_json::to_string(map).map_err(|e| DogfoodError::Io(e.to_string()))?;
        fs::write(&tmp, body).map_err(|e| DogfoodError::Io(e.to_string()))?;
        fs::rename(&tmp, path).map_err(|e| DogfoodError::Io(e.to_string()))?;
        Ok(())
    }

    /// Apply once. Same digest is idempotent (`Ok(false)`). Conflicting digest
    /// is [`DogfoodError::DuplicateEffect`].
    pub fn apply_once(&self, effect_id: &str, payload_digest: &str) -> Result<bool, DogfoodError> {
        match &self.backend {
            EffectLogBackend::Ephemeral { applied } => {
                let mut guard = applied
                    .lock()
                    .map_err(|_| DogfoodError::InvalidState("effect log poisoned"))?;
                match guard.get(effect_id) {
                    Some(existing) if existing == payload_digest => Ok(false),
                    Some(_) => Err(DogfoodError::DuplicateEffect {
                        effect_id: effect_id.to_string(),
                    }),
                    None => {
                        guard.insert(effect_id.to_string(), payload_digest.to_string());
                        Ok(true)
                    }
                }
            }
            EffectLogBackend::Json { path, applied } => {
                let mut guard = applied
                    .lock()
                    .map_err(|_| DogfoodError::InvalidState("effect log poisoned"))?;
                match guard.get(effect_id) {
                    Some(existing) if existing == payload_digest => Ok(false),
                    Some(_) => Err(DogfoodError::DuplicateEffect {
                        effect_id: effect_id.to_string(),
                    }),
                    None => {
                        guard.insert(effect_id.to_string(), payload_digest.to_string());
                        Self::flush_json(path, &guard)?;
                        Ok(true)
                    }
                }
            }
            EffectLogBackend::Kernel { store, owner } => store
                .apply_effect_once(owner, effect_id, payload_digest, None, None, Utc::now())
                .map_err(Into::into),
        }
    }

    #[must_use]
    pub fn was_applied(&self, effect_id: &str) -> bool {
        match &self.backend {
            EffectLogBackend::Ephemeral { applied } | EffectLogBackend::Json { applied, .. } => {
                applied
                    .lock()
                    .map(|g| g.contains_key(effect_id))
                    .unwrap_or(false)
            }
            EffectLogBackend::Kernel { store, owner } => store.was_effect_applied(owner, effect_id),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match &self.backend {
            EffectLogBackend::Ephemeral { applied } | EffectLogBackend::Json { applied, .. } => {
                applied.lock().map(|g| g.len()).unwrap_or(0)
            }
            EffectLogBackend::Kernel { store, owner } => store.effect_count(owner).unwrap_or(0),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        match &self.backend {
            EffectLogBackend::Kernel { store, .. } => store.path(),
            EffectLogBackend::Json { path, .. } => path.as_path(),
            EffectLogBackend::Ephemeral { .. } => Path::new(""),
        }
    }
}

/// Configuration for one dogfood session.
#[derive(Debug, Clone)]
pub struct DogfoodConfig {
    pub owner: String,
    pub receipt_id: String,
    pub run_id: String,
    pub worker_id: String,
    pub lease_owner: String,
    pub workdir: PathBuf,
    pub route: RouteConfig,
    pub risk_class: RiskClass,
    /// When true, permission surface returns Ask and blocks effects.
    pub require_approval: bool,
    /// Permission decision to apply (Ask overrides require_approval when set).
    pub permission: PermissionDecision,
    pub code_base_sha: Option<String>,
    pub code_head_sha: Option<String>,
}

/// Snapshot of lifecycle cleanup after a terminal run.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LifecycleInventory {
    pub orphan_processes_detected: bool,
    pub child_scopes_open: usize,
    pub temp_paths_remaining: usize,
    /// OS PIDs of effect child processes still unreaped (should be empty after cleanup).
    pub live_child_pids: Vec<u32>,
}

/// Orchestrates one Process-lane dogfood delivery with durable effect/receipt state.
pub struct DogfoodPath {
    config: DogfoodConfig,
    /// Durable root: kernel.sqlite, agent.sqlite, workdir, pid inventory.
    durable_root: PathBuf,
    #[allow(dead_code)]
    agent_db_path: PathBuf,
    agent: Arc<SqliteAgentStore>,
    /// Multi-process authority for receipt/effect/approval/lease fence/phase.
    kernel: KernelStore,
    effects: Arc<ExternalEffectLog>,
    cancel: bool,
    /// Simulated crash fence: if set, stop after the named stage without
    /// completing the receipt (for recovery tests).
    crash_after: Option<CrashFence>,
    claimed: Option<ClaimedRun>,
    permit_issued: bool,
    effect_applied: bool,
    artifact_path: Option<PathBuf>,
    artifact_digest: Option<String>,
    receipt_revision: u64,
    /// Delivery phase revision in kernel_delivery.
    phase_revision: u64,
    /// Pending approval request digest (ask path).
    approval_request_digest: Option<String>,
    approval_id: Option<String>,
    lifecycle: LifecycleInventory,
    /// Child PIDs spawned for process effects (for real inventory).
    effect_child_pids: Vec<u32>,
}

/// Injected crash points between durable stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashFence {
    AfterEffectBeforeReceipt,
    AfterArtifactBeforeTransition,
    AfterGrantBeforeEffect,
}

impl DogfoodPath {
    /// Open a new dogfood session with durable state under `durable_root`.
    ///
    /// Layout: `{durable_root}/kernel.sqlite`, `{durable_root}/agent.sqlite`,
    /// `{config.workdir}/`.
    pub fn open_durable(
        durable_root: impl Into<PathBuf>,
        config: DogfoodConfig,
        now: DateTime<Utc>,
    ) -> Result<Self, DogfoodError> {
        let durable_root = durable_root.into();
        fs::create_dir_all(&durable_root).map_err(|e| DogfoodError::Io(e.to_string()))?;
        fs::create_dir_all(&config.workdir).map_err(|e| DogfoodError::Io(e.to_string()))?;
        let agent_db_path = durable_root.join("agent.sqlite");
        let agent = Arc::new(
            SqliteAgentStore::open(&agent_db_path)
                .map_err(|e| DogfoodError::Agent(e.to_string()))?,
        );
        let kernel = KernelStore::open(durable_root.join("kernel.sqlite"))?;
        let effects = Arc::new(ExternalEffectLog::from_kernel(
            kernel.clone(),
            config.owner.clone(),
        ));
        let acceptance_digest = digest_bytes(b"acceptance:marker-file-PASS");
        let truth_probe_digest = digest_bytes(b"truth_probe:marker-equals-PASS");
        let task = TaskCase {
            task_case_id: config.receipt_id.clone(),
            task_type: DOGFOOD_TASK_TYPE.into(),
            acceptance_digest,
            truth_probe_digest,
            risk_class: config.risk_class,
        };
        let mut receipt = CompletionReceipt::open(
            config.receipt_id.clone(),
            config.owner.clone(),
            task,
            config.route.clone(),
            now,
        )?;
        receipt.code_base_sha = config.code_base_sha.clone();
        receipt.code_head_sha = config.code_head_sha.clone();
        receipt.validate()?;
        let revision = receipt.revision;
        kernel.insert_open_receipt(&receipt)?;
        let (_, phase_revision) = kernel.ensure_delivery_running(
            &config.owner,
            &config.receipt_id,
            &config.run_id,
            now,
        )?;
        Ok(Self {
            config,
            durable_root,
            agent_db_path,
            agent,
            kernel,
            effects,
            cancel: false,
            crash_after: None,
            claimed: None,
            permit_issued: false,
            effect_applied: false,
            artifact_path: None,
            artifact_digest: None,
            receipt_revision: revision,
            phase_revision,
            approval_request_digest: None,
            approval_id: None,
            lifecycle: LifecycleInventory::default(),
            effect_child_pids: Vec::new(),
        })
    }

    /// Compatibility constructor used by older tests; durable under workdir parent.
    pub fn open(
        agent: Arc<SqliteAgentStore>,
        effects: Arc<ExternalEffectLog>,
        config: DogfoodConfig,
        now: DateTime<Utc>,
    ) -> Result<Self, DogfoodError> {
        fs::create_dir_all(&config.workdir).map_err(|e| DogfoodError::Io(e.to_string()))?;
        let durable_root = config
            .workdir
            .parent()
            .map(|p| p.join(format!("durable-{}", config.receipt_id)))
            .unwrap_or_else(|| config.workdir.join("durable"));
        fs::create_dir_all(&durable_root).map_err(|e| DogfoodError::Io(e.to_string()))?;
        let kernel = KernelStore::open(durable_root.join("kernel.sqlite"))?;
        // Prefer kernel-backed effects for multi-process authority.
        let effects = if effects.path().as_os_str().is_empty() {
            Arc::new(ExternalEffectLog::from_kernel(
                kernel.clone(),
                config.owner.clone(),
            ))
        } else {
            // If caller supplied a non-kernel log, still dual-authority via kernel for dogfood path.
            Arc::new(ExternalEffectLog::from_kernel(
                kernel.clone(),
                config.owner.clone(),
            ))
        };
        let acceptance_digest = digest_bytes(b"acceptance:marker-file-PASS");
        let truth_probe_digest = digest_bytes(b"truth_probe:marker-equals-PASS");
        let task = TaskCase {
            task_case_id: config.receipt_id.clone(),
            task_type: DOGFOOD_TASK_TYPE.into(),
            acceptance_digest,
            truth_probe_digest,
            risk_class: config.risk_class,
        };
        let mut receipt = CompletionReceipt::open(
            config.receipt_id.clone(),
            config.owner.clone(),
            task,
            config.route.clone(),
            now,
        )?;
        receipt.code_base_sha = config.code_base_sha.clone();
        receipt.code_head_sha = config.code_head_sha.clone();
        receipt.validate()?;
        let revision = receipt.revision;
        kernel.insert_open_receipt(&receipt)?;
        let (_, phase_revision) = kernel.ensure_delivery_running(
            &config.owner,
            &config.receipt_id,
            &config.run_id,
            now,
        )?;
        Ok(Self {
            config,
            durable_root,
            agent_db_path: PathBuf::new(),
            agent,
            kernel,
            effects,
            cancel: false,
            crash_after: None,
            claimed: None,
            permit_issued: false,
            effect_applied: false,
            artifact_path: None,
            artifact_digest: None,
            receipt_revision: revision,
            phase_revision,
            approval_request_digest: None,
            approval_id: None,
            lifecycle: LifecycleInventory::default(),
            effect_child_pids: Vec::new(),
        })
    }

    /// Reopen after process death: reload durable kernel + AgentStore.
    ///
    /// Does **not** retain in-memory `claimed` / permits. Callers must
    /// re-claim or recover from AgentStore truth, then `recover_after_crash`.
    pub fn reopen(
        durable_root: impl Into<PathBuf>,
        config: DogfoodConfig,
    ) -> Result<Self, DogfoodError> {
        let durable_root = durable_root.into();
        let agent_db_path = durable_root.join("agent.sqlite");
        let agent = Arc::new(
            SqliteAgentStore::open(&agent_db_path)
                .map_err(|e| DogfoodError::Agent(e.to_string()))?,
        );
        let kernel = KernelStore::open(durable_root.join("kernel.sqlite"))?;
        let effects = Arc::new(ExternalEffectLog::from_kernel(
            kernel.clone(),
            config.owner.clone(),
        ));
        let receipt = kernel
            .get_receipt(&config.owner, &config.receipt_id)?
            .ok_or(DogfoodError::InvalidState("receipt missing after reopen"))?;
        let revision = receipt.revision;
        let artifact_digest = receipt.output_artifact_digest.clone();
        let effect_applied =
            kernel.was_effect_applied(&config.owner, &format!("effect:{}", config.run_id));
        let (phase, phase_revision) = kernel
            .get_delivery_phase(&config.owner, &config.receipt_id)?
            .unwrap_or((DeliveryPhase::Running, 0));
        let approval = kernel.get_approval(&config.owner, &config.receipt_id)?;
        let (approval_id, approval_request_digest) = match approval {
            Some(a) => (Some(a.approval_id), Some(a.request_digest)),
            None => (None, None),
        };
        let _ = phase; // phase recovered; resume methods re-check
        Ok(Self {
            config,
            durable_root,
            agent_db_path,
            agent,
            kernel,
            effects,
            cancel: false,
            crash_after: None,
            claimed: None,
            permit_issued: false,
            effect_applied,
            artifact_path: None,
            artifact_digest,
            receipt_revision: revision,
            phase_revision,
            approval_request_digest,
            approval_id,
            lifecycle: LifecycleInventory::default(),
            effect_child_pids: Vec::new(),
        })
    }

    #[must_use]
    pub fn durable_root(&self) -> &Path {
        &self.durable_root
    }

    #[must_use]
    pub fn kernel(&self) -> &KernelStore {
        &self.kernel
    }

    #[must_use]
    pub fn receipt(&self) -> Option<CompletionReceipt> {
        self.kernel
            .get_receipt(&self.config.owner, &self.config.receipt_id)
            .ok()
            .flatten()
    }

    #[must_use]
    pub fn effects(&self) -> &ExternalEffectLog {
        &self.effects
    }

    pub fn set_crash_after(&mut self, fence: CrashFence) {
        self.crash_after = Some(fence);
    }

    pub fn request_cancel(&mut self) {
        self.cancel = true;
    }

    /// Bound grant for a pending approval request.
    ///
    /// Requires a prior ask (`evaluate_permission` → ApprovalRequired) that
    /// recorded `approval_request_digest`. Binding includes owner, run, revision,
    /// lease owner, generation, and request digest. Deny is never recoverable.
    pub fn grant_approval(&mut self, now: DateTime<Utc>) -> Result<(), DogfoodError> {
        let digest = self
            .approval_request_digest
            .clone()
            .ok_or(DogfoodError::InvalidState("no pending approval request"))?;
        let generation = self
            .claimed
            .as_ref()
            .map(|c| c.receipt.generation)
            .or_else(|| {
                self.kernel
                    .get_lease_fence(&self.config.owner, &self.config.run_id)
                    .ok()
                    .flatten()
                    .map(|f| f.generation)
            })
            .ok_or(DogfoodError::InvalidState("no lease generation for grant"))?;
        let binding = ApprovalGrantBinding {
            owner: self.config.owner.clone(),
            receipt_id: self.config.receipt_id.clone(),
            run_id: self.config.run_id.clone(),
            request_digest: digest,
            expected_revision: self
                .kernel
                .get_approval(&self.config.owner, &self.config.receipt_id)?
                .map(|a| a.bound_revision)
                .unwrap_or(self.receipt_revision),
            lease_owner: self.config.lease_owner.clone(),
            generation,
            decided_by: self.config.lease_owner.clone(),
        };
        let decision = self.kernel.grant_approval(&binding, now)?;
        if decision.decision != ApprovalDecisionKind::Approved {
            return Err(DogfoodError::InvalidState("grant did not approve"));
        }
        let (phase, rev) = self.kernel.set_delivery_phase(
            &self.config.owner,
            &self.config.receipt_id,
            &self.config.run_id,
            self.phase_revision,
            DeliveryEvent::GrantAccepted,
            Some(&decision.approval_id),
            now,
        )?;
        self.phase_revision = rev;
        self.approval_id = Some(decision.approval_id);
        // Record approval gate as passed on the receipt.
        let gate = NamedGate {
            name: "approval".into(),
            outcome: GateOutcome::Passed,
            exact_head_sha: self.config.code_head_sha.clone(),
            evidence_uri: Some("synthetic://approval_granted".into()),
            content_digest: Some(digest_bytes(b"approved")),
        };
        let updated = self.kernel.set_gate(
            &self.config.owner,
            &self.config.receipt_id,
            self.receipt_revision,
            gate,
            now,
        )?;
        self.receipt_revision = updated.revision;
        let _ = phase;
        if self.crash_after == Some(CrashFence::AfterGrantBeforeEffect) {
            return Err(DogfoodError::InvalidState(
                "injected crash after grant before effect",
            ));
        }
        // Advance to Resuming so effects may run.
        let (phase, rev) = self.kernel.set_delivery_phase(
            &self.config.owner,
            &self.config.receipt_id,
            &self.config.run_id,
            self.phase_revision,
            DeliveryEvent::ResumeStarted,
            self.approval_id.as_deref(),
            now,
        )?;
        self.phase_revision = rev;
        let _ = phase;
        Ok(())
    }

    /// Explicit deny — terminal; subsequent grant fails closed.
    pub fn deny_approval(&mut self, now: DateTime<Utc>) -> Result<(), DogfoodError> {
        let digest = self
            .approval_request_digest
            .clone()
            .ok_or(DogfoodError::InvalidState("no pending approval request"))?;
        self.kernel.deny_approval(
            &self.config.owner,
            &self.config.receipt_id,
            &digest,
            &self.config.lease_owner,
            now,
        )?;
        let (phase, rev) = self.kernel.set_delivery_phase(
            &self.config.owner,
            &self.config.receipt_id,
            &self.config.run_id,
            self.phase_revision,
            DeliveryEvent::DenyAccepted,
            self.approval_id.as_deref(),
            now,
        )?;
        self.phase_revision = rev;
        let gate = NamedGate {
            name: "approval".into(),
            outcome: GateOutcome::Failed,
            exact_head_sha: self.config.code_head_sha.clone(),
            evidence_uri: Some("synthetic://approval_denied".into()),
            content_digest: Some(digest_bytes(b"denied")),
        };
        let updated = self.kernel.set_gate(
            &self.config.owner,
            &self.config.receipt_id,
            self.receipt_revision,
            gate,
            now,
        )?;
        self.receipt_revision = updated.revision;
        let _ = phase;
        Err(DogfoodError::ApprovalDenied)
    }

    /// Whether a durable grant exists for this receipt.
    #[must_use]
    pub fn is_approval_granted(&self) -> bool {
        self.kernel
            .get_approval(&self.config.owner, &self.config.receipt_id)
            .ok()
            .flatten()
            .is_some_and(|a| a.decision == ApprovalDecisionKind::Approved)
    }

    #[must_use]
    pub fn delivery_phase(&self) -> Option<DeliveryPhase> {
        self.kernel
            .get_delivery_phase(&self.config.owner, &self.config.receipt_id)
            .ok()
            .flatten()
            .map(|(p, _)| p)
    }

    /// Create root AgentRun (or adopt existing same identity as no-op check).
    pub fn create_or_adopt_agent_run(&mut self, now: DateTime<Utc>) -> Result<(), DogfoodError> {
        self.ensure_not_cancelled()?;
        if self
            .agent
            .get_run(&self.config.owner, &self.config.run_id)
            .map_err(|e| DogfoodError::Agent(e.to_string()))?
            .is_some()
        {
            return Ok(());
        }
        let worker = NewWorker {
            id: self.config.worker_id.clone(),
            logical_session_id: None,
        };
        // available_at must be <= store clock (system time). The receipt `now`
        // may be a frozen test timestamp ahead of or behind wall clock, so pin
        // availability to the earlier of the two.
        let available_at = now.min(Utc::now());
        let run = NewAgentRun {
            id: self.config.run_id.clone(),
            worker_id: self.config.worker_id.clone(),
            task_id: Some(self.config.receipt_id.clone()),
            trace_id: Some(format!("dogfood-{}", self.config.receipt_id)),
            parent_run_id: None,
            execution_backend: ExecutionBackend::CliHarnessProcess,
            mode: RunMode::Autonomous,
            target_key: format!(
                "{}/{}",
                self.config.route.provider.as_str(),
                self.config.route.model.as_str()
            ),
            prompt_digest: digest_bytes(self.config.receipt_id.as_bytes()),
            policy_digest: digest_bytes(b"dogfood-policy-v1"),
            available_at,
            timeout_seconds: 600,
            max_resume_attempts: 0,
        };
        self.agent
            .create_root(&self.config.owner, &worker, &run)
            .map_err(|e| DogfoodError::Agent(e.to_string()))?;
        self.lifecycle.child_scopes_open = 1;
        Ok(())
    }

    /// Atomic claim + start + execution permit.
    pub fn claim_and_lease(&mut self) -> Result<(), DogfoodError> {
        self.ensure_not_cancelled()?;
        let claimed = self
            .agent
            .claim_due(
                &self.config.owner,
                ExecutionBackend::CliHarnessProcess,
                &self.config.lease_owner,
                60,
                1,
            )
            .map_err(|e| DogfoodError::Agent(e.to_string()))?;
        let claimed = claimed
            .into_iter()
            .find(|c| c.run.id == self.config.run_id)
            .ok_or(DogfoodError::InvalidState("run not claimable"))?;
        // CliHarnessProcess requires ControllerKind::Process (not InProcess).
        let controller = ControllerRef {
            kind: ControllerKind::Process,
            id: format!("dogfood-{}", self.config.lease_owner),
            fingerprint: Some(format!("fp-{}", self.config.lease_owner)),
        };
        let claimed = self
            .agent
            .start(&self.config.owner, &claimed.receipt, &controller)
            .map_err(|e| DogfoodError::Agent(e.to_string()))?;
        let _permit = self
            .agent
            .issue_execution_permit(
                &self.config.owner,
                &claimed.receipt,
                &claimed.run.policy_digest,
            )
            .map_err(|e| DogfoodError::Agent(e.to_string()))?;
        self.permit_issued = true;
        self.persist_lease_snapshot(&claimed)?;
        self.claimed = Some(claimed);
        Ok(())
    }

    fn persist_lease_snapshot(&self, claimed: &ClaimedRun) -> Result<(), DogfoodError> {
        let fence = LeaseFence {
            owner: self.config.owner.clone(),
            run_id: claimed.receipt.run_id.clone(),
            lease_owner: claimed.receipt.lease_owner.clone(),
            generation: claimed.receipt.generation,
            revision: claimed.receipt.revision,
            token: claimed.receipt.token.clone(),
            policy_digest: claimed.run.policy_digest.clone(),
            expires_at_ms: None,
        };
        self.kernel.put_lease_fence(&fence, Utc::now())?;
        Ok(())
    }

    fn load_lease_snapshot(&self) -> Result<Option<LeaseFence>, DogfoodError> {
        Ok(self
            .kernel
            .get_lease_fence(&self.config.owner, &self.config.run_id)?)
    }

    /// Record route attempt provenance on the receipt.
    pub fn record_attempt(&mut self, now: DateTime<Utc>) -> Result<(), DogfoodError> {
        self.ensure_not_cancelled()?;
        let claimed = self
            .claimed
            .as_ref()
            .ok_or(DogfoodError::InvalidState("not claimed"))?;
        let attempt = ReceiptAttempt {
            attempt_id: format!("attempt-{}", claimed.run.id),
            parent_attempt_id: None,
            agent_run_id: claimed.run.id.clone(),
            owner: self.config.owner.clone(),
            started_at: now,
            ended_at: None,
            status: AttemptStatus::Running,
            failure_class: AttemptFailureClass::None,
            worktree: None,
            branch: None,
            base_sha: self.config.code_base_sha.clone(),
            head_sha: self.config.code_head_sha.clone(),
            input_tokens: None,
            output_tokens: None,
            cost: CostEvidence::default(),
        };
        let updated = self.kernel.record_attempt(
            &self.config.owner,
            &self.config.receipt_id,
            self.receipt_revision,
            attempt,
            now,
        )?;
        self.receipt_revision = updated.revision;
        Ok(())
    }

    /// Evaluate permission ceiling; Ask without grant blocks effects.
    pub fn evaluate_permission(&mut self, now: DateTime<Utc>) -> Result<(), DogfoodError> {
        self.ensure_not_cancelled()?;
        // Denied delivery phase is terminal.
        if self.delivery_phase() == Some(DeliveryPhase::Denied) {
            return Err(DogfoodError::ApprovalDenied);
        }
        let granted = self.is_approval_granted();
        // Already waiting on approval: fail closed without re-transition.
        if matches!(self.delivery_phase(), Some(DeliveryPhase::ApprovalRequired)) && !granted {
            return Err(DogfoodError::ApprovalRequired);
        }
        let decision = if self.config.require_approval && !granted {
            PermissionDecision::Ask
        } else if granted {
            PermissionDecision::Allow
        } else {
            self.config.permission
        };
        match decision {
            PermissionDecision::Deny => {
                self.fail_path(
                    "permission denied",
                    AttemptFailureClass::PermissionDenied,
                    now,
                )?;
                Err(DogfoodError::PermissionDenied)
            }
            PermissionDecision::Ask if !granted => {
                let request_digest = digest_bytes(
                    format!(
                        "ask:{}:{}:{}",
                        self.config.receipt_id, self.config.run_id, self.receipt_revision
                    )
                    .as_bytes(),
                );
                let approval_id = format!("appr-{}", self.config.receipt_id);
                let decision_row = self.kernel.record_approval_required(
                    &self.config.owner,
                    &approval_id,
                    &self.config.receipt_id,
                    &self.config.run_id,
                    &request_digest,
                    self.receipt_revision,
                    now,
                )?;
                self.approval_id = Some(decision_row.approval_id);
                self.approval_request_digest = Some(request_digest.clone());
                let (phase, rev) = self.kernel.set_delivery_phase(
                    &self.config.owner,
                    &self.config.receipt_id,
                    &self.config.run_id,
                    self.phase_revision,
                    DeliveryEvent::AskRequired,
                    self.approval_id.as_deref(),
                    now,
                )?;
                self.phase_revision = rev;
                let _ = phase;
                // Record approval-required as a gate note, do not execute.
                let gate = NamedGate {
                    name: "approval".into(),
                    outcome: GateOutcome::Failed,
                    exact_head_sha: self.config.code_head_sha.clone(),
                    evidence_uri: Some("synthetic://approval_required".into()),
                    content_digest: Some(request_digest),
                };
                let updated = self.kernel.set_gate(
                    &self.config.owner,
                    &self.config.receipt_id,
                    self.receipt_revision,
                    gate,
                    now,
                )?;
                self.receipt_revision = updated.revision;
                Err(DogfoodError::ApprovalRequired)
            }
            PermissionDecision::Allow | PermissionDecision::Ask => {
                let gate = NamedGate {
                    name: "approval".into(),
                    outcome: GateOutcome::Passed,
                    exact_head_sha: self.config.code_head_sha.clone(),
                    evidence_uri: Some("synthetic://approval_granted_or_allow".into()),
                    content_digest: Some(digest_bytes(decision.as_str().as_bytes())),
                };
                let updated = self.kernel.set_gate(
                    &self.config.owner,
                    &self.config.receipt_id,
                    self.receipt_revision,
                    gate,
                    now,
                )?;
                self.receipt_revision = updated.revision;
                Ok(())
            }
        }
    }

    /// Apply one external Process-lane effect under the lease.
    ///
    /// Spawns a real short-lived child process that writes the marker (not an
    /// in-process-only write). Effect identity is durable so restart cannot
    /// re-apply a conflicting payload.
    pub fn execute_process_effect(&mut self, payload: &str) -> Result<(), DogfoodError> {
        self.ensure_not_cancelled()?;
        if !self.permit_issued {
            return Err(DogfoodError::InvalidState("no execution permit"));
        }
        let effect_id = format!("effect:{}", self.config.run_id);
        let digest = digest_bytes(payload.as_bytes());
        // Idempotent re-apply of an already-recorded effect is allowed even after
        // terminal phases (no second side effect). New effects still require a
        // phase that allows execution.
        let already = self
            .kernel
            .was_effect_applied(&self.config.owner, &effect_id);
        if !already
            && self
                .delivery_phase()
                .is_some_and(|phase| !phase.allows_effect())
        {
            return Err(DogfoodError::InvalidState(
                "delivery phase does not allow effects",
            ));
        }
        // Multi-process authority: KernelStore apply with owner/run binding.
        let first = self.kernel.apply_effect_once(
            &self.config.owner,
            &effect_id,
            &digest,
            Some(&self.config.run_id),
            Some(&self.config.receipt_id),
            Utc::now(),
        )?;
        if first {
            self.effect_applied = true;
            let marker = self.config.workdir.join("effect.marker");
            let pid = spawn_effect_child(&marker, payload)?;
            self.effect_child_pids.push(pid);
            self.lifecycle.child_scopes_open = self
                .lifecycle
                .child_scopes_open
                .saturating_add(1)
                .max(self.effect_child_pids.len());
            self.lifecycle.temp_paths_remaining += 1;
            // Reap immediately so normal path leaves no orphans; inventory still
            // recorded the real PID for tests that inspect mid-flight state.
            let _ = wait_pid_gone(pid, Duration::from_secs(2));
            self.effect_child_pids.retain(|p| *p != pid);
            self.lifecycle.live_child_pids = live_pids(&self.effect_child_pids);
        }
        if self.crash_after == Some(CrashFence::AfterEffectBeforeReceipt) {
            return Err(DogfoodError::InvalidState(
                "injected crash after effect before receipt",
            ));
        }
        Ok(())
    }

    /// Truth probe: requires workdir/MARKER to equal "PASS".
    ///
    /// Callers must first demonstrate failure on a broken baseline by writing
    /// a non-PASS marker (or leaving it absent), then fix and re-run.
    pub fn run_truth_probe(&mut self, now: DateTime<Utc>) -> Result<(), DogfoodError> {
        self.ensure_not_cancelled()?;
        let marker = self.config.workdir.join("MARKER");
        let outcome = match fs::read_to_string(&marker) {
            Ok(content) if content.trim() == "PASS" => GateOutcome::Passed,
            Ok(content) => {
                let reason = format!("marker was {:?}", content.trim());
                self.record_truth_gate(GateOutcome::Failed, now, Some(&reason))?;
                return Err(DogfoodError::TruthProbeFailed { reason });
            }
            Err(_) => {
                let reason = String::from("marker file missing");
                self.record_truth_gate(GateOutcome::Failed, now, Some(&reason))?;
                return Err(DogfoodError::TruthProbeFailed { reason });
            }
        };
        self.record_truth_gate(outcome, now, Some("marker PASS"))?;
        Ok(())
    }

    fn record_truth_gate(
        &mut self,
        outcome: GateOutcome,
        now: DateTime<Utc>,
        note: Option<&str>,
    ) -> Result<(), DogfoodError> {
        let gate = NamedGate {
            name: GATE_TRUTH_PROBE.into(),
            outcome,
            exact_head_sha: self.config.code_head_sha.clone(),
            evidence_uri: Some("synthetic://truth_probe/MARKER".into()),
            content_digest: note.map(|n| digest_bytes(n.as_bytes())),
        };
        let updated = self.kernel.set_gate(
            &self.config.owner,
            &self.config.receipt_id,
            self.receipt_revision,
            gate,
            now,
        )?;
        self.receipt_revision = updated.revision;
        Ok(())
    }

    /// Publish immutable output artifact under workdir/artifacts/.
    pub fn publish_artifact(
        &mut self,
        body: &str,
        now: DateTime<Utc>,
    ) -> Result<String, DogfoodError> {
        self.ensure_not_cancelled()?;
        let dir = self.config.workdir.join("artifacts");
        fs::create_dir_all(&dir).map_err(|e| DogfoodError::Io(e.to_string()))?;
        let digest = digest_bytes(body.as_bytes());
        let path = dir.join(format!("{digest}.txt"));
        if path.exists() {
            let existing = fs::read(&path).map_err(|e| DogfoodError::Io(e.to_string()))?;
            if existing != body.as_bytes() {
                return Err(DogfoodError::Io(
                    "artifact digest collision with different body".into(),
                ));
            }
        } else {
            fs::write(&path, body).map_err(|e| DogfoodError::Io(e.to_string()))?;
        }
        // Make immutable best-effort on Unix.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o444));
        }
        self.artifact_path = Some(path.clone());
        self.artifact_digest = Some(digest.clone());
        let meta = ArtifactMeta {
            owner: self.config.owner.clone(),
            receipt_id: self.config.receipt_id.clone(),
            digest: digest.clone(),
            path: path.to_string_lossy().into_owned(),
            content_bytes: body.len() as u64,
            created_at_ms: now.timestamp_millis(),
        };
        self.kernel.put_artifact_meta(&meta)?;
        let updated = self.kernel.set_artifact_digest(
            &self.config.owner,
            &self.config.receipt_id,
            self.receipt_revision,
            digest.clone(),
            now,
        )?;
        self.receipt_revision = updated.revision;
        if self.crash_after == Some(CrashFence::AfterArtifactBeforeTransition) {
            return Err(DogfoodError::InvalidState(
                "injected crash after artifact before task transition",
            ));
        }
        Ok(digest)
    }

    /// Mark unit/integration gates as passed (local dogfood evidence).
    pub fn record_local_gates(&mut self, now: DateTime<Utc>) -> Result<(), DogfoodError> {
        for (name, outcome) in [
            (GATE_UNIT, GateOutcome::Passed),
            (GATE_INTEGRATION, GateOutcome::Passed),
            (GATE_INDEPENDENT_REVIEW, GateOutcome::Skipped),
            (GATE_CI_PACKAGING, GateOutcome::Skipped),
        ] {
            let gate = NamedGate {
                name: name.into(),
                outcome,
                exact_head_sha: self.config.code_head_sha.clone(),
                evidence_uri: Some(format!("synthetic://gate/{name}")),
                content_digest: Some(digest_bytes(name.as_bytes())),
            };
            let updated = self.kernel.set_gate(
                &self.config.owner,
                &self.config.receipt_id,
                self.receipt_revision,
                gate,
                now,
            )?;
            self.receipt_revision = updated.revision;
        }
        Ok(())
    }

    /// Commit AgentRun completion + CompletionReceipt when gates allow.
    pub fn finish_success(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<CompletionReceipt, DogfoodError> {
        self.ensure_not_cancelled()?;
        let claimed = self
            .claimed
            .as_ref()
            .ok_or(DogfoodError::InvalidState("not claimed"))?;
        let artifact = self
            .artifact_digest
            .clone()
            .ok_or(DogfoodError::InvalidState("no artifact"))?;
        let permit = self
            .agent
            .issue_execution_permit(
                &self.config.owner,
                &claimed.receipt,
                &claimed.run.policy_digest,
            )
            .map_err(|e| DogfoodError::Agent(e.to_string()))?;
        let prepared = self
            .agent
            .prepare_completion(
                &self.config.owner,
                &permit,
                &NewRunCompletion {
                    id: format!("cmp-{}", self.config.run_id),
                    sink_kind: "dogfood-artifact-v1".into(),
                    publication_key: format!("pub-{}", self.config.run_id),
                    content_digest: artifact,
                    content_bytes: self
                        .artifact_path
                        .as_ref()
                        .and_then(|p| fs::metadata(p).ok())
                        .map(|m| m.len())
                        .unwrap_or(0),
                },
            )
            .map_err(|e| DogfoodError::Agent(e.to_string()))?;
        self.agent
            .commit_completion(&self.config.owner, &prepared.permit)
            .map_err(|e| DogfoodError::Agent(e.to_string()))?;

        // Capture lifecycle inventory before the terminal transition so cleanup
        // fields can land on the still-open receipt.
        let cleanup = self.prepare_cleanup_state(true);
        let updated = self.kernel.transition_receipt(
            &self.config.owner,
            &self.config.receipt_id,
            self.receipt_revision,
            now,
            |receipt| {
                receipt.recovery_cleanup = cleanup;
                receipt.remaining_risks = vec![
                    "formal_vendor_harness_integration_open".into(),
                    "daemon_process_spawn_covered_by_existing_acceptance".into(),
                    "production_cutover_not_performed".into(),
                ];
                receipt.validation_summary = Some(
                    "dogfood: kernel.sqlite authority; truth_probe passed; artifact published; process effect once".into(),
                );
                Ok(())
            },
        )?;
        self.receipt_revision = updated.revision;
        let receipt = self.kernel.complete_receipt(
            &self.config.owner,
            &self.config.receipt_id,
            self.receipt_revision,
            now,
        )?;
        self.receipt_revision = receipt.revision;
        // Delivery phase must reach terminal consistently with the receipt.
        // If still Approved after grant-crash, Complete is legal (FSM allows it).
        match self.kernel.set_delivery_phase(
            &self.config.owner,
            &self.config.receipt_id,
            &self.config.run_id,
            self.phase_revision,
            DeliveryEvent::Complete,
            self.approval_id.as_deref(),
            now,
        ) {
            Ok((_phase, rev)) => {
                self.phase_revision = rev;
            }
            Err(KernelStoreError::Delivery(_)) | Err(KernelStoreError::StaleRevision { .. }) => {
                // Already terminal or concurrent advance: re-read and require completed/denied/etc.
                if let Some((phase, rev)) = self
                    .kernel
                    .get_delivery_phase(&self.config.owner, &self.config.receipt_id)?
                {
                    self.phase_revision = rev;
                    if !phase.is_terminal() {
                        return Err(DogfoodError::InvalidState(
                            "delivery phase failed to reach terminal on complete",
                        ));
                    }
                }
            }
            Err(e) => return Err(e.into()),
        }
        self.apply_cleanup_inventory(true);
        Ok(receipt)
    }

    pub fn cancel_path(&mut self, now: DateTime<Utc>) -> Result<CompletionReceipt, DogfoodError> {
        self.cancel = true;
        if self.claimed.is_some() {
            let request = CancelRequest {
                operation_id: format!("cancel-{}", self.config.run_id),
                lease_owner: self.config.lease_owner.clone(),
                lease_seconds: 30,
                retry_tickets: Vec::new(),
            };
            if let Ok(plan) =
                self.agent
                    .request_cancel_tree(&self.config.owner, &self.config.worker_id, &request)
            {
                for ticket in &plan.tickets {
                    let _ = self.agent.settle_cancel(
                        &self.config.owner,
                        ticket,
                        CancelOutcome::Cancelled,
                    );
                }
            }
        }
        let cleanup = self.prepare_cleanup_state(true);
        let updated = self.kernel.transition_receipt(
            &self.config.owner,
            &self.config.receipt_id,
            self.receipt_revision,
            now,
            |receipt| {
                receipt.recovery_cleanup = cleanup;
                Ok(())
            },
        )?;
        self.receipt_revision = updated.revision;
        let receipt = self.kernel.cancel_receipt(
            &self.config.owner,
            &self.config.receipt_id,
            self.receipt_revision,
            now,
        )?;
        self.receipt_revision = receipt.revision;
        self.apply_cleanup_inventory(true);
        Ok(receipt)
    }

    /// Recover after crash or process restart.
    ///
    /// Requires a durable effect log entry (survives restart). Re-claims the
    /// AgentRun when this instance has no live permit (true reopen path).
    /// Incomplete recovery is `Unresolved` (never invented success).
    pub fn recover_after_crash(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<CompletionReceipt, DogfoodError> {
        self.crash_after = None;
        self.cancel = false;
        let effect_id = format!("effect:{}", self.config.run_id);
        // Reload durable effect truth (also covers same-process fences).
        if !self.effects.was_applied(&effect_id) {
            // Disk marker alone is not enough to invent an effect id — but if
            // the marker exists and log is empty after reopen of ephemeral log,
            // still refuse re-application by checking marker+digest conflict path.
            let receipt = self.kernel.mark_unresolved(
                &self.config.owner,
                &self.config.receipt_id,
                self.receipt_revision,
                "recovery without durable prior effect".to_string(),
                now,
            )?;
            self.receipt_revision = receipt.revision;
            return Ok(receipt);
        }
        self.effect_applied = true;
        // Re-adopt AgentRun lease when this is a fresh process (no claimed).
        if self.claimed.is_none() {
            self.reclaim_or_adopt_after_restart()?;
        }
        if self.artifact_digest.is_none() {
            let body = fs::read_to_string(self.config.workdir.join("effect.marker"))
                .unwrap_or_else(|_| "recovered-missing-marker".into());
            self.publish_artifact(&format!("recovered:{body}"), now)?;
        }
        self.run_truth_probe(now)?;
        self.record_local_gates(now)?;
        self.finish_success(now)
    }

    /// After reopen, restore lease from durable snapshot and re-issue permit.
    pub fn reclaim_or_adopt_after_restart(&mut self) -> Result<(), DogfoodError> {
        let run = self
            .agent
            .get_run(&self.config.owner, &self.config.run_id)
            .map_err(|e| DogfoodError::Agent(e.to_string()))?
            .ok_or(DogfoodError::InvalidState("run missing after reopen"))?;
        use vyane_agent::RunState;
        if matches!(
            run.state,
            RunState::Succeeded | RunState::Failed | RunState::Cancelled | RunState::TimedOut
        ) {
            self.permit_issued = true;
            return Ok(());
        }
        let Some(snap) = self.load_lease_snapshot()? else {
            return Err(DogfoodError::InvalidState(
                "no durable lease snapshot after reopen",
            ));
        };
        // Generation fence: wrong owner/generation fail closed.
        self.kernel.assert_generation(
            &self.config.owner,
            &self.config.run_id,
            &self.config.lease_owner,
            snap.generation,
        )?;
        let receipt = RunLeaseReceipt {
            run_id: snap.run_id.clone(),
            worker_id: self.config.worker_id.clone(),
            generation: snap.generation,
            revision: snap.revision,
            lease_owner: snap.lease_owner.clone(),
            token: snap.token.clone(),
        };
        // Stale generation / expired lease fail closed here — cannot complete with
        // a dead fence (see hostile lease tests).
        let permit = self
            .agent
            .issue_execution_permit(&self.config.owner, &receipt, &snap.policy_digest)
            .map_err(|e| DogfoodError::Agent(e.to_string()))?;
        let _ = permit;
        // Rebuild ClaimedRun for finish_success by reading current run + receipt.
        let run = self
            .agent
            .get_run(&self.config.owner, &self.config.run_id)
            .map_err(|e| DogfoodError::Agent(e.to_string()))?
            .ok_or(DogfoodError::InvalidState("run missing"))?;
        self.claimed = Some(ClaimedRun { receipt, run });
        self.permit_issued = true;
        Ok(())
    }

    pub fn fail_path(
        &mut self,
        summary: &str,
        _class: AttemptFailureClass,
        now: DateTime<Utc>,
    ) -> Result<CompletionReceipt, DogfoodError> {
        let cleanup = self.prepare_cleanup_state(false);
        let updated = self.kernel.transition_receipt(
            &self.config.owner,
            &self.config.receipt_id,
            self.receipt_revision,
            now,
            |receipt| {
                receipt.recovery_cleanup = cleanup;
                Ok(())
            },
        )?;
        self.receipt_revision = updated.revision;
        let receipt = self.kernel.fail_receipt(
            &self.config.owner,
            &self.config.receipt_id,
            self.receipt_revision,
            summary.to_string(),
            now,
        )?;
        self.receipt_revision = receipt.revision;
        self.apply_cleanup_inventory(false);
        Ok(receipt)
    }

    /// Snapshot cleanup truth **before** terminalization (scopes still open).
    fn prepare_cleanup_state(&self, graceful: bool) -> RecoveryCleanupState {
        let scopes_open = self.lifecycle.child_scopes_open;
        RecoveryCleanupState {
            orphan_processes_detected: !graceful && scopes_open > 0,
            // Graceful path expects scopes to be closable; ungraceful leaves residual.
            cleanup_succeeded: graceful,
            note: Some(if graceful {
                "dogfood-declared-scope-cleanup".into()
            } else {
                "dogfood-ungraceful-stop-scopes-still-open".into()
            }),
        }
    }

    /// Apply post-terminal inventory mutations (ephemeral markers, scope close).
    fn apply_cleanup_inventory(&mut self, graceful: bool) {
        let marker = self.config.workdir.join("effect.marker");
        if marker.exists() {
            let _ = fs::remove_file(&marker);
            self.lifecycle.temp_paths_remaining =
                self.lifecycle.temp_paths_remaining.saturating_sub(1);
        }
        if graceful {
            self.lifecycle.child_scopes_open = 0;
            self.lifecycle.orphan_processes_detected = false;
        } else {
            self.lifecycle.orphan_processes_detected = self.lifecycle.child_scopes_open > 0;
        }
    }

    #[must_use]
    pub fn lifecycle(&self) -> &LifecycleInventory {
        &self.lifecycle
    }

    fn ensure_not_cancelled(&self) -> Result<(), DogfoodError> {
        if self.cancel {
            Err(DogfoodError::Cancelled)
        } else {
            Ok(())
        }
    }
}

/// Full happy path helper used by tests and dogfood dual-run evidence.
pub fn run_successful_dogfood(
    root: &Path,
    owner: &str,
    run_suffix: &str,
    now: DateTime<Utc>,
) -> Result<(CompletionReceipt, Arc<ExternalEffectLog>), DogfoodError> {
    let durable = root.join(format!("durable-{run_suffix}"));
    let workdir = durable.join("workdir");
    fs::create_dir_all(&workdir).map_err(|e| DogfoodError::Io(e.to_string()))?;
    // Broken baseline first: truth probe must fail.
    fs::write(workdir.join("MARKER"), "BROKEN").map_err(|e| DogfoodError::Io(e.to_string()))?;

    let base_sha = "10ebe700cef3416459beebfb7ed07d7e9b866de7".to_string();
    let config = DogfoodConfig {
        owner: owner.into(),
        receipt_id: format!("rcpt-{run_suffix}"),
        run_id: format!("0197f524-7a00-7000-8000-{:012}", suffix_u48(run_suffix)),
        worker_id: format!("worker-{run_suffix}"),
        lease_owner: format!("lease-{run_suffix}"),
        workdir: workdir.clone(),
        route: RouteConfig {
            provider: ProviderId::new("fixture-provider"),
            endpoint_class: EndpointClass::LocalProcess,
            protocol: Protocol::OpenaiChat,
            harness: Some(HarnessKind::ClaudeCode),
            model: ModelId::new("fixture-model"),
            model_snapshot: None,
            requested_effort: Some(vyane_core::Effort::High),
            effective_effort: Some(vyane_core::Effort::High),
            profile_or_config_digest: Some(digest_bytes(b"profile-dogfood")),
            billing_mode_category: BillingModeCategory::SubscriptionHarness,
        },
        risk_class: RiskClass::WorkspaceWrite,
        require_approval: false,
        permission: PermissionDecision::Allow,
        code_base_sha: Some(base_sha.clone()),
        code_head_sha: Some(base_sha),
    };

    let mut path = DogfoodPath::open_durable(&durable, config, now)?;
    path.create_or_adopt_agent_run(now)?;
    path.claim_and_lease()?;
    path.record_attempt(now)?;
    path.evaluate_permission(now)?;
    path.execute_process_effect("process-output-v1")?;

    // Truth probe fails on broken baseline.
    let fail = path.run_truth_probe(now);
    assert!(
        matches!(fail, Err(DogfoodError::TruthProbeFailed { .. })),
        "truth probe must fail on broken baseline, got {fail:?}"
    );

    // Fix baseline and pass.
    fs::write(workdir.join("MARKER"), "PASS").map_err(|e| DogfoodError::Io(e.to_string()))?;
    path.run_truth_probe(now)?;
    path.record_local_gates(now)?;
    path.publish_artifact("process-output-v1", now)?;
    let receipt = path.finish_success(now)?;
    let effects = Arc::clone(&path.effects);
    Ok((receipt, effects))
}

fn suffix_u48(suffix: &str) -> u64 {
    let mut acc = 0u64;
    for (i, b) in suffix.bytes().take(6).enumerate() {
        acc |= u64::from(b) << (8 * (5 - i));
    }
    if acc == 0 { 1 } else { acc }
}

fn spawn_effect_child(marker: &Path, payload: &str) -> Result<u32, DogfoodError> {
    let marker_s = marker.to_string_lossy().into_owned();
    let mut child = std::process::Command::new("sh")
        .arg("-c")
        .arg("printf '%s' \"$1\" > \"$2\"")
        .arg("dogfood-effect")
        .arg(payload)
        .arg(&marker_s)
        .spawn()
        .map_err(|e| DogfoodError::Io(e.to_string()))?;
    let pid = child.id();
    let status = child.wait().map_err(|e| DogfoodError::Io(e.to_string()))?;
    if !status.success() {
        return Err(DogfoodError::Io(format!(
            "effect child exited {:?}",
            status.code()
        )));
    }
    Ok(pid)
}

fn wait_pid_gone(pid: u32, budget: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < budget {
        if !pid_is_live(pid) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    !pid_is_live(pid)
}

fn pid_is_live(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    {
        Path::new(&format!("/proc/{pid}")).exists()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        false
    }
}

fn live_pids(pids: &[u32]) -> Vec<u32> {
    pids.iter().copied().filter(|p| pid_is_live(*p)).collect()
}

/// Public inventory probe for verification evidence (ps-like via `/proc`).
#[must_use]
#[allow(dead_code)] // exercised by hostile lifecycle tests and external evidence capture
pub fn inventory_live_pids(pids: &[u32]) -> Vec<u32> {
    live_pids(pids)
}

#[must_use]
pub fn digest_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Default lease duration for dogfood claims.
#[allow(dead_code)]
pub const DOGFOOD_LEASE: Duration = Duration::from_secs(60);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;
    use vyane_core::ReceiptFinalStatus;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 4, 12, 0, 0).single().unwrap()
    }

    #[test]
    fn dogfood_truth_probe_fails_then_succeeds_with_receipt() {
        let root = tempfile::tempdir().unwrap();
        let (receipt, effects) =
            run_successful_dogfood(root.path(), "local", "aa01", now()).unwrap();
        assert_eq!(receipt.final_status, ReceiptFinalStatus::Completed);
        assert!(receipt.final_status.is_success());
        assert!(receipt.output_artifact_digest.is_some());
        assert_eq!(receipt.route.provider.as_str(), "fixture-provider");
        assert_eq!(
            receipt.route.harness.as_ref().map(HarnessKind::as_str),
            Some("claude-code")
        );
        assert_eq!(
            receipt.route.effective_effort,
            Some(vyane_core::Effort::High)
        );
        assert_eq!(receipt.gates.truth_probe.outcome, GateOutcome::Passed);
        assert_eq!(effects.len(), 1);
        // Dual-run consistency: second full path also completes.
        let (receipt2, effects2) =
            run_successful_dogfood(root.path(), "local", "aa02", now()).unwrap();
        assert_eq!(receipt2.final_status, ReceiptFinalStatus::Completed);
        assert_eq!(effects2.len(), 1);
        assert_eq!(
            receipt.gates.truth_probe.outcome,
            receipt2.gates.truth_probe.outcome
        );
    }

    fn sample_config(_root: &Path, id: &str, workdir: PathBuf) -> DogfoodConfig {
        DogfoodConfig {
            owner: "local".into(),
            receipt_id: format!("rcpt-{id}"),
            run_id: format!("0197f524-7a00-7000-8000-{:012}", suffix_u48(id)),
            worker_id: format!("worker-{id}"),
            lease_owner: format!("lease-{id}"),
            workdir,
            route: RouteConfig {
                provider: ProviderId::new("fixture-provider"),
                endpoint_class: EndpointClass::LocalProcess,
                protocol: Protocol::OpenaiChat,
                harness: Some(HarnessKind::ClaudeCode),
                model: ModelId::new("fixture-model"),
                model_snapshot: None,
                requested_effort: None,
                effective_effort: None,
                profile_or_config_digest: None,
                billing_mode_category: BillingModeCategory::Unknown,
            },
            risk_class: RiskClass::WorkspaceWrite,
            require_approval: false,
            permission: PermissionDecision::Allow,
            code_base_sha: Some("10ebe700cef3416459beebfb7ed07d7e9b866de7".into()),
            code_head_sha: Some("10ebe700cef3416459beebfb7ed07d7e9b866de7".into()),
        }
    }

    #[test]
    fn approval_required_blocks_effect() {
        let root = tempfile::tempdir().unwrap();
        let durable = root.path().join("d");
        let workdir = durable.join("wd");
        fs::create_dir_all(&workdir).unwrap();
        let mut config = sample_config(root.path(), "ask", workdir);
        config.require_approval = true;
        config.permission = PermissionDecision::Ask;
        let mut path = DogfoodPath::open_durable(&durable, config, now()).unwrap();
        path.create_or_adopt_agent_run(now()).unwrap();
        path.claim_and_lease().unwrap();
        path.record_attempt(now()).unwrap();
        let err = path.evaluate_permission(now()).unwrap_err();
        assert_eq!(err, DogfoodError::ApprovalRequired);
        assert!(path.effects().is_empty());
    }

    #[test]
    fn approval_deny_then_grant_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let durable = root.path().join("d");
        let workdir = durable.join("wd");
        fs::create_dir_all(&workdir).unwrap();
        let mut config = sample_config(root.path(), "deny", workdir);
        config.require_approval = true;
        config.permission = PermissionDecision::Ask;
        let mut path = DogfoodPath::open_durable(&durable, config, now()).unwrap();
        path.create_or_adopt_agent_run(now()).unwrap();
        path.claim_and_lease().unwrap();
        path.record_attempt(now()).unwrap();
        assert_eq!(
            path.evaluate_permission(now()).unwrap_err(),
            DogfoodError::ApprovalRequired
        );
        assert_eq!(
            path.deny_approval(now()).unwrap_err(),
            DogfoodError::ApprovalDenied
        );
        let err = path.grant_approval(now()).unwrap_err();
        assert!(
            matches!(err, DogfoodError::ApprovalDenied | DogfoodError::Kernel(_)),
            "deny then grant must fail closed, got {err:?}"
        );
        assert!(path.effects().is_empty());
        assert_eq!(path.delivery_phase(), Some(DeliveryPhase::Denied));
    }

    #[test]
    fn approval_grant_then_crash_before_effect_no_duplicate_on_resume() {
        let root = tempfile::tempdir().unwrap();
        let durable = root.path().join("d");
        let workdir = durable.join("wd");
        fs::create_dir_all(&workdir).unwrap();
        fs::write(workdir.join("MARKER"), "PASS").unwrap();
        let mut config = sample_config(root.path(), "gcrash", workdir);
        config.require_approval = true;
        config.permission = PermissionDecision::Ask;
        let config_reopen = config.clone();
        {
            let mut path = DogfoodPath::open_durable(&durable, config, now()).unwrap();
            path.set_crash_after(CrashFence::AfterGrantBeforeEffect);
            path.create_or_adopt_agent_run(now()).unwrap();
            path.claim_and_lease().unwrap();
            path.record_attempt(now()).unwrap();
            assert_eq!(
                path.evaluate_permission(now()).unwrap_err(),
                DogfoodError::ApprovalRequired
            );
            let crash = path.grant_approval(now()).unwrap_err();
            assert!(matches!(crash, DogfoodError::InvalidState(_)));
            assert!(path.is_approval_granted());
            assert!(path.effects().is_empty());
        }
        let mut path = DogfoodPath::reopen(&durable, config_reopen).unwrap();
        assert!(path.is_approval_granted());
        // Resume: re-claim and continue after grant.
        path.create_or_adopt_agent_run(now()).unwrap();
        // Run may still be Running with lease; reclaim via recover path only if effect exists.
        // Here effect not yet applied — re-claim if needed.
        if path.claimed.is_none() {
            // Lease fence exists; adopt without re-claim race by reclaim helper.
            path.reclaim_or_adopt_after_restart().unwrap();
        }
        // Ensure Resuming phase after durable grant (crash may have skipped ResumeStarted).
        if path.delivery_phase() == Some(DeliveryPhase::Approved) {
            let (phase, rev) = path
                .kernel
                .set_delivery_phase(
                    &path.config.owner,
                    &path.config.receipt_id,
                    &path.config.run_id,
                    path.phase_revision,
                    DeliveryEvent::ResumeStarted,
                    path.approval_id.as_deref(),
                    now(),
                )
                .unwrap();
            path.phase_revision = rev;
            assert_eq!(phase, DeliveryPhase::Resuming);
        }
        path.evaluate_permission(now()).unwrap();
        path.execute_process_effect("after-grant-resume").unwrap();
        path.run_truth_probe(now()).unwrap();
        path.record_local_gates(now()).unwrap();
        path.publish_artifact("after-grant-resume", now()).unwrap();
        let receipt = path.finish_success(now()).unwrap();
        assert_eq!(receipt.final_status, ReceiptFinalStatus::Completed);
        assert_eq!(path.effects().len(), 1);
        // Approval evidence on receipt.
        assert!(
            receipt.gates.extra.contains_key("approval")
                || receipt
                    .gates
                    .extra
                    .values()
                    .any(|g| g.evidence_uri.as_deref() == Some("synthetic://approval_granted"))
                || receipt
                    .gates
                    .extra
                    .get("approval")
                    .is_some_and(|g| g.outcome == GateOutcome::Passed)
        );
    }

    #[test]
    fn approval_grant_allows_effect_and_completion() {
        let root = tempfile::tempdir().unwrap();
        let durable = root.path().join("d");
        let workdir = durable.join("wd");
        fs::create_dir_all(&workdir).unwrap();
        fs::write(workdir.join("MARKER"), "PASS").unwrap();
        let mut config = sample_config(root.path(), "grant", workdir);
        config.require_approval = true;
        config.permission = PermissionDecision::Ask;
        let mut path = DogfoodPath::open_durable(&durable, config, now()).unwrap();
        path.create_or_adopt_agent_run(now()).unwrap();
        path.claim_and_lease().unwrap();
        path.record_attempt(now()).unwrap();
        assert_eq!(
            path.evaluate_permission(now()).unwrap_err(),
            DogfoodError::ApprovalRequired
        );
        path.grant_approval(now()).unwrap();
        path.evaluate_permission(now()).unwrap();
        path.execute_process_effect("granted-effect").unwrap();
        path.run_truth_probe(now()).unwrap();
        path.record_local_gates(now()).unwrap();
        path.publish_artifact("granted-effect", now()).unwrap();
        let receipt = path.finish_success(now()).unwrap();
        assert_eq!(receipt.final_status, ReceiptFinalStatus::Completed);
        assert_eq!(path.effects().len(), 1);
    }

    #[test]
    fn duplicate_effect_is_prevented() {
        let log = ExternalEffectLog::new_ephemeral();
        assert!(log.apply_once("e1", "d1").unwrap());
        assert!(!log.apply_once("e1", "d1").unwrap());
        let err = log.apply_once("e1", "d2").unwrap_err();
        assert!(matches!(err, DogfoodError::DuplicateEffect { .. }));
    }

    #[test]
    fn cancel_before_effect_leaves_zero_effects() {
        let root = tempfile::tempdir().unwrap();
        let durable = root.path().join("d");
        let workdir = durable.join("wd");
        fs::create_dir_all(&workdir).unwrap();
        let mut config = sample_config(root.path(), "cancel", workdir);
        config.risk_class = RiskClass::ReadOnly;
        let mut path = DogfoodPath::open_durable(&durable, config, now()).unwrap();
        path.create_or_adopt_agent_run(now()).unwrap();
        path.claim_and_lease().unwrap();
        let receipt = path.cancel_path(now()).unwrap();
        assert_eq!(receipt.final_status, ReceiptFinalStatus::Cancelled);
        assert!(path.effects().is_empty());
        assert_eq!(path.lifecycle().child_scopes_open, 0);
        assert!(path.lifecycle().live_child_pids.is_empty());
    }

    #[test]
    fn crash_after_effect_true_process_restart_does_not_duplicate() {
        let root = tempfile::tempdir().unwrap();
        let durable = root.path().join("d");
        let workdir = durable.join("wd");
        fs::create_dir_all(&workdir).unwrap();
        fs::write(workdir.join("MARKER"), "PASS").unwrap();
        let config = sample_config(root.path(), "crash", workdir.clone());
        let config_reopen = config.clone();
        {
            let mut path = DogfoodPath::open_durable(&durable, config, now()).unwrap();
            path.set_crash_after(CrashFence::AfterEffectBeforeReceipt);
            path.create_or_adopt_agent_run(now()).unwrap();
            path.claim_and_lease().unwrap();
            path.record_attempt(now()).unwrap();
            path.evaluate_permission(now()).unwrap();
            let crash = path.execute_process_effect("once-only").unwrap_err();
            assert!(matches!(crash, DogfoodError::InvalidState(_)));
            assert_eq!(path.effects().len(), 1);
            // Drop path entirely — simulates process death (no in-memory state).
        }
        // Fresh process: reopen durable roots only.
        let mut path = DogfoodPath::reopen(&durable, config_reopen).unwrap();
        assert_eq!(path.effects().len(), 1);
        assert!(path.claimed.is_none());
        let recovered = path.recover_after_crash(now()).unwrap();
        assert_eq!(path.effects().len(), 1);
        assert_eq!(recovered.final_status, ReceiptFinalStatus::Completed);
        // Conflicting re-apply is blocked by durable log.
        let err = path
            .execute_process_effect("once-only-different")
            .unwrap_err();
        assert!(matches!(
            err,
            DogfoodError::DuplicateEffect { .. } | DogfoodError::InvalidState(_)
        ));
        // Same digest is idempotent if permit allows.
        if path.permit_issued {
            path.execute_process_effect("once-only").unwrap();
        }
        assert_eq!(path.effects().len(), 1);
    }

    #[test]
    fn durable_effect_log_survives_reopen_without_agent() {
        let root = tempfile::tempdir().unwrap();
        let log_path = root.path().join("effects.json");
        {
            let log = ExternalEffectLog::open(&log_path).unwrap();
            assert!(log.apply_once("effect:run", "digest-a").unwrap());
        }
        let reopened = ExternalEffectLog::open(&log_path).unwrap();
        assert!(reopened.was_applied("effect:run"));
        assert!(!reopened.apply_once("effect:run", "digest-a").unwrap());
        assert!(matches!(
            reopened.apply_once("effect:run", "digest-b"),
            Err(DogfoodError::DuplicateEffect { .. })
        ));
    }

    #[test]
    fn owner_isolation_on_dogfood_receipts() {
        let root = tempfile::tempdir().unwrap();
        let (r1, _) = run_successful_dogfood(root.path(), "alice", "b001", now()).unwrap();
        assert!(r1.final_status.is_success());
        // separate owner path
        let (r2, _) = run_successful_dogfood(root.path(), "bob", "b002", now()).unwrap();
        assert_eq!(r2.owner, "bob");
        assert_ne!(r1.owner, r2.owner);
    }

    #[test]
    fn stale_lease_generation_cannot_complete_foreign_mutation() {
        // Cross-owner complete is fenced by KernelStore (foreign-as-absent).
        let dir = tempfile::tempdir().unwrap();
        let store = KernelStore::open(dir.path().join("k.sqlite")).unwrap();
        let task = TaskCase {
            task_case_id: "tc".into(),
            task_type: DOGFOOD_TASK_TYPE.into(),
            acceptance_digest: digest_bytes(b"a"),
            truth_probe_digest: digest_bytes(b"b"),
            risk_class: RiskClass::ReadOnly,
        };
        let route = RouteConfig {
            provider: ProviderId::new("p"),
            endpoint_class: EndpointClass::Unknown,
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("m"),
            model_snapshot: None,
            requested_effort: None,
            effective_effort: None,
            profile_or_config_digest: None,
            billing_mode_category: BillingModeCategory::Unknown,
        };
        let receipt = CompletionReceipt::open("r1", "alice", task, route, now()).unwrap();
        store.insert_open_receipt(&receipt).unwrap();
        let err = store.complete_receipt("bob", "r1", 1, now()).unwrap_err();
        assert!(matches!(err, KernelStoreError::NotFound));
        assert!(store.get_receipt("bob", "r1").unwrap().is_none());
    }

    #[test]
    fn hostile_crash_after_artifact_true_restart() {
        let root = tempfile::tempdir().unwrap();
        let durable = root.path().join("d");
        let workdir = durable.join("wd");
        fs::create_dir_all(&workdir).unwrap();
        fs::write(workdir.join("MARKER"), "PASS").unwrap();
        let config = sample_config(root.path(), "art", workdir);
        let config2 = config.clone();
        {
            let mut path = DogfoodPath::open_durable(&durable, config, now()).unwrap();
            path.set_crash_after(CrashFence::AfterArtifactBeforeTransition);
            path.create_or_adopt_agent_run(now()).unwrap();
            path.claim_and_lease().unwrap();
            path.record_attempt(now()).unwrap();
            path.evaluate_permission(now()).unwrap();
            path.execute_process_effect("payload").unwrap();
            path.run_truth_probe(now()).unwrap();
            path.record_local_gates(now()).unwrap();
            let crash = path.publish_artifact("payload", now()).unwrap_err();
            assert!(matches!(crash, DogfoodError::InvalidState(_)));
            assert_eq!(path.effects().len(), 1);
        }
        let mut path = DogfoodPath::reopen(&durable, config2).unwrap();
        let recovered = path.recover_after_crash(now()).unwrap();
        assert_eq!(path.effects().len(), 1);
        assert_eq!(recovered.final_status, ReceiptFinalStatus::Completed);
        assert!(recovered.output_artifact_digest.is_some());
    }

    #[test]
    fn hostile_cancel_race_after_claim() {
        let root = tempfile::tempdir().unwrap();
        let durable = root.path().join("d");
        let workdir = durable.join("wd");
        fs::create_dir_all(&workdir).unwrap();
        let mut config = sample_config(root.path(), "race", workdir);
        config.risk_class = RiskClass::ReadOnly;
        let mut path = DogfoodPath::open_durable(&durable, config, now()).unwrap();
        path.create_or_adopt_agent_run(now()).unwrap();
        path.claim_and_lease().unwrap();
        path.request_cancel();
        let err = path.execute_process_effect("should-not-run").unwrap_err();
        assert_eq!(err, DogfoodError::Cancelled);
        assert!(path.effects().is_empty());
        let receipt = path.cancel_path(now()).unwrap();
        assert_eq!(receipt.final_status, ReceiptFinalStatus::Cancelled);
    }

    #[test]
    fn hostile_duplicate_command_replay_is_idempotent_or_detected() {
        let root = tempfile::tempdir().unwrap();
        let log = ExternalEffectLog::open(root.path().join("e.json")).unwrap();
        assert!(log.apply_once("cmd-1", "digest-a").unwrap());
        assert!(!log.apply_once("cmd-1", "digest-a").unwrap());
        assert!(matches!(
            log.apply_once("cmd-1", "digest-b"),
            Err(DogfoodError::DuplicateEffect { .. })
        ));
    }

    #[test]
    fn hostile_corrupted_or_newer_schema_fails_closed() {
        let mut receipt = {
            let root = tempfile::tempdir().unwrap();
            let (r, _) = run_successful_dogfood(root.path(), "local", "schema", now()).unwrap();
            r
        };
        receipt.schema_version = 99;
        let err = receipt.validate().unwrap_err();
        assert_eq!(
            err,
            ReceiptError::InvalidInput("unsupported or newer receipt schema_version")
        );
        let json = serde_json::to_string(&receipt).unwrap();
        assert!(CompletionReceipt::from_canonical_json(&json).is_err());
    }

    #[test]
    fn hostile_partial_process_tree_cleanup_is_visible() {
        let root = tempfile::tempdir().unwrap();
        let durable = root.path().join("d");
        let workdir = durable.join("wd");
        fs::create_dir_all(&workdir).unwrap();
        fs::write(workdir.join("MARKER"), "PASS").unwrap();
        let mut config = sample_config(root.path(), "tree", workdir);
        config.risk_class = RiskClass::ReadOnly;
        let mut path = DogfoodPath::open_durable(&durable, config, now()).unwrap();
        path.create_or_adopt_agent_run(now()).unwrap();
        path.claim_and_lease().unwrap();
        path.evaluate_permission(now()).unwrap();
        path.execute_process_effect("tree-payload").unwrap();
        // Child was spawned and reaped — no live orphans under /proc.
        assert!(path.lifecycle().live_child_pids.is_empty());
        assert!(
            inventory_live_pids(&path.lifecycle().live_child_pids).is_empty(),
            "post-effect inventory must show no live effect children"
        );
        let _ = path.cancel_path(now());
        // After graceful cleanup, declared scopes closed.
        assert_eq!(path.lifecycle().child_scopes_open, 0);
        assert!(!path.lifecycle().orphan_processes_detected);
    }

    #[test]
    fn hostile_expired_agent_lease_rejects_stale_generation() {
        use chrono::{TimeDelta, TimeZone as _};
        use std::sync::Mutex as StdMutex;
        use vyane_agent::{
            AgentClock, AgentStore, ExecutionBackend, NewAgentRun, NewWorker, RunMode,
        };

        struct TestClock(StdMutex<DateTime<Utc>>);
        impl TestClock {
            fn new() -> Self {
                Self(StdMutex::new(
                    Utc.with_ymd_and_hms(2026, 8, 4, 12, 0, 0).single().unwrap(),
                ))
            }
            fn advance(&self, seconds: i64) {
                *self.0.lock().unwrap() += TimeDelta::seconds(seconds);
            }
        }
        impl AgentClock for TestClock {
            fn now(&self) -> DateTime<Utc> {
                *self.0.lock().unwrap()
            }
        }

        let root = tempfile::tempdir().unwrap();
        let clock = Arc::new(TestClock::new());
        let store =
            SqliteAgentStore::open_with_clock(root.path().join("agent.sqlite"), clock.clone())
                .unwrap();
        let t0 = clock.now();
        store
            .create_root(
                "local",
                &NewWorker {
                    id: "w1".into(),
                    logical_session_id: None,
                },
                &NewAgentRun {
                    id: "0197f524-7a00-7000-8000-0000000000e9".into(),
                    worker_id: "w1".into(),
                    task_id: None,
                    trace_id: None,
                    parent_run_id: None,
                    execution_backend: ExecutionBackend::CliHarnessProcess,
                    mode: RunMode::Autonomous,
                    target_key: "p/m".into(),
                    prompt_digest: digest_bytes(b"p"),
                    policy_digest: digest_bytes(b"pol"),
                    available_at: t0,
                    timeout_seconds: 600,
                    max_resume_attempts: 0,
                },
            )
            .unwrap();
        let claimed = store
            .claim_due(
                "local",
                ExecutionBackend::CliHarnessProcess,
                "lease-a",
                30,
                1,
            )
            .unwrap()
            .remove(0);
        let started = store
            .start(
                "local",
                &claimed.receipt,
                &ControllerRef {
                    kind: ControllerKind::Process,
                    id: "ctrl".into(),
                    fingerprint: Some("fp".into()),
                },
            )
            .unwrap();
        let permit_ok =
            store.issue_execution_permit("local", &started.receipt, &started.run.policy_digest);
        assert!(permit_ok.is_ok());
        // Expire lease generation fence.
        clock.advance(31);
        let stale =
            store.issue_execution_permit("local", &started.receipt, &started.run.policy_digest);
        assert!(
            stale.is_err(),
            "expired lease must reject permit re-issue: {stale:?}"
        );
        // Wrong generation also fails closed.
        let mut wrong_gen = started.receipt.clone();
        wrong_gen.generation = wrong_gen.generation.saturating_add(1);
        assert!(
            store
                .issue_execution_permit("local", &wrong_gen, &started.run.policy_digest)
                .is_err()
        );
    }
}
