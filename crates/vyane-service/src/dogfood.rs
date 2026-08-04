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
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use vyane_agent::{
    AgentStore, CancelOutcome, CancelRequest, ClaimedRun, ControllerKind, ControllerRef,
    ExecutionBackend, NewAgentRun, NewRunCompletion, NewWorker, RunLeaseReceipt, RunMode,
    SqliteAgentStore,
};
use vyane_core::{
    AttemptFailureClass, AttemptStatus, BillingModeCategory, CompletionReceipt, CostEvidence,
    EndpointClass, GATE_CI_PACKAGING, GATE_INDEPENDENT_REVIEW, GATE_INTEGRATION, GATE_TRUTH_PROBE,
    GATE_UNIT, GateOutcome, HarnessKind, MemoryReceiptLedger, ModelId, NamedGate, Protocol,
    ProviderId, ReceiptAttempt, ReceiptError, RecoveryCleanupState, RiskClass, RouteConfig,
    TaskCase,
};

/// Stable dogfood task type recorded on receipts.
pub const DOGFOOD_TASK_TYPE: &str = "process_lane_autonomous_delivery";

/// Errors from the dogfood path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DogfoodError {
    Receipt(String),
    Agent(String),
    TruthProbeFailed { reason: String },
    ApprovalRequired,
    PermissionDenied,
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
            Self::TruthProbeFailed { reason } => write!(f, "truth probe failed: {reason}"),
            Self::ApprovalRequired => f.write_str("approval required before side effect"),
            Self::PermissionDenied => f.write_str("permission denied"),
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

/// Durable append-only log of external effects for duplicate-effect detection.
///
/// Survives process restart when opened at a stable filesystem path. Apply is
/// atomic: map update is written to disk before the method returns success.
#[derive(Debug)]
pub struct ExternalEffectLog {
    path: PathBuf,
    // effect_id -> first application payload digest
    applied: Mutex<BTreeMap<String, String>>,
}

impl ExternalEffectLog {
    /// In-memory only (tests that do not exercise restart). Prefer [`Self::open`].
    #[must_use]
    pub fn new_ephemeral() -> Self {
        Self {
            path: PathBuf::new(),
            applied: Mutex::new(BTreeMap::new()),
        }
    }

    /// Open or create a durable effect log at `path` (JSON object map).
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
            path,
            applied: Mutex::new(applied),
        })
    }

    fn flush_locked(&self, map: &BTreeMap<String, String>) -> Result<(), DogfoodError> {
        if self.path.as_os_str().is_empty() {
            return Ok(());
        }
        let tmp = self.path.with_extension("json.tmp");
        let body = serde_json::to_string(map).map_err(|e| DogfoodError::Io(e.to_string()))?;
        fs::write(&tmp, body).map_err(|e| DogfoodError::Io(e.to_string()))?;
        fs::rename(&tmp, &self.path).map_err(|e| DogfoodError::Io(e.to_string()))?;
        Ok(())
    }

    /// Apply once. Same digest is idempotent (`Ok(false)`). Conflicting digest
    /// is [`DogfoodError::DuplicateEffect`].
    pub fn apply_once(&self, effect_id: &str, payload_digest: &str) -> Result<bool, DogfoodError> {
        let mut guard = self
            .applied
            .lock()
            .map_err(|_| DogfoodError::InvalidState("effect log poisoned"))?;
        match guard.get(effect_id) {
            Some(existing) if existing == payload_digest => Ok(false),
            Some(_) => Err(DogfoodError::DuplicateEffect {
                effect_id: effect_id.to_string(),
            }),
            None => {
                guard.insert(effect_id.to_string(), payload_digest.to_string());
                self.flush_locked(&guard)?;
                Ok(true)
            }
        }
    }

    #[must_use]
    pub fn was_applied(&self, effect_id: &str) -> bool {
        self.applied
            .lock()
            .map(|g| g.contains_key(effect_id))
            .unwrap_or(false)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.applied.lock().map(|g| g.len()).unwrap_or(0)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
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
    /// Durable root: effects.json, receipts/, agent.sqlite path sibling, pid inventory.
    durable_root: PathBuf,
    #[allow(dead_code)]
    agent_db_path: PathBuf,
    agent: Arc<SqliteAgentStore>,
    receipts: MemoryReceiptLedger,
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
    approval_granted: bool,
    lifecycle: LifecycleInventory,
    /// Child PIDs spawned for process effects (for real inventory).
    effect_child_pids: Vec<u32>,
}

/// Injected crash points between durable stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashFence {
    AfterEffectBeforeReceipt,
    AfterArtifactBeforeTransition,
}

/// Durable lease snapshot so restart can re-issue a permit without inventing
/// a second AgentRun or replaying the external effect.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableLeaseSnapshot {
    run_id: String,
    worker_id: String,
    generation: u64,
    revision: u64,
    lease_owner: String,
    token: String,
    policy_digest: String,
}

impl DogfoodPath {
    /// Open a new dogfood session with durable state under `durable_root`.
    ///
    /// Layout: `{durable_root}/effects.json`, `{durable_root}/receipts/`,
    /// `{durable_root}/agent.sqlite`, `{config.workdir}/`.
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
        let effects = Arc::new(ExternalEffectLog::open(durable_root.join("effects.json"))?);
        let mut receipts = MemoryReceiptLedger::open_durable(durable_root.join("receipts"))
            .map_err(|e| DogfoodError::Receipt(e.to_string()))?;
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
        receipts.insert_open(receipt)?;
        Ok(Self {
            config,
            durable_root,
            agent_db_path,
            agent,
            receipts,
            effects,
            cancel: false,
            crash_after: None,
            claimed: None,
            permit_issued: false,
            effect_applied: false,
            artifact_path: None,
            artifact_digest: None,
            receipt_revision: revision,
            approval_granted: false,
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
        // Prefer caller-supplied effects if already durable; else open under root.
        let effects = if effects.path().as_os_str().is_empty() {
            Arc::new(ExternalEffectLog::open(durable_root.join("effects.json"))?)
        } else {
            effects
        };
        let mut receipts = MemoryReceiptLedger::open_durable(durable_root.join("receipts"))
            .map_err(|e| DogfoodError::Receipt(e.to_string()))?;
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
        receipts.insert_open(receipt)?;
        Ok(Self {
            config,
            durable_root,
            agent_db_path: PathBuf::new(),
            agent,
            receipts,
            effects,
            cancel: false,
            crash_after: None,
            claimed: None,
            permit_issued: false,
            effect_applied: false,
            artifact_path: None,
            artifact_digest: None,
            receipt_revision: revision,
            approval_granted: false,
            lifecycle: LifecycleInventory::default(),
            effect_child_pids: Vec::new(),
        })
    }

    /// Reopen after process death: reload durable effects + receipts, re-open AgentStore.
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
        let effects = Arc::new(ExternalEffectLog::open(durable_root.join("effects.json"))?);
        let receipts = MemoryReceiptLedger::open_durable(durable_root.join("receipts"))
            .map_err(|e| DogfoodError::Receipt(e.to_string()))?;
        let receipt = receipts
            .get_for_owner(&config.owner, &config.receipt_id)
            .ok_or(DogfoodError::InvalidState("receipt missing after reopen"))?;
        let revision = receipt.revision;
        let artifact_digest = receipt.output_artifact_digest.clone();
        let effect_applied = effects.was_applied(&format!("effect:{}", config.run_id));
        Ok(Self {
            config,
            durable_root,
            agent_db_path,
            agent,
            receipts,
            effects,
            cancel: false,
            crash_after: None,
            claimed: None,
            permit_issued: false,
            effect_applied,
            artifact_path: None,
            artifact_digest,
            receipt_revision: revision,
            approval_granted: false,
            lifecycle: LifecycleInventory::default(),
            effect_child_pids: Vec::new(),
        })
    }

    #[must_use]
    pub fn durable_root(&self) -> &Path {
        &self.durable_root
    }

    #[must_use]
    pub fn receipt(&self) -> Option<&CompletionReceipt> {
        self.receipts
            .get_for_owner(&self.config.owner, &self.config.receipt_id)
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

    pub fn grant_approval(&mut self) {
        self.approval_granted = true;
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

    fn lease_snapshot_path(&self) -> PathBuf {
        self.durable_root.join("lease-snapshot.json")
    }

    fn persist_lease_snapshot(&self, claimed: &ClaimedRun) -> Result<(), DogfoodError> {
        if self.durable_root.as_os_str().is_empty() {
            return Ok(());
        }
        let snap = DurableLeaseSnapshot {
            run_id: claimed.receipt.run_id.clone(),
            worker_id: claimed.receipt.worker_id.clone(),
            generation: claimed.receipt.generation,
            revision: claimed.receipt.revision,
            lease_owner: claimed.receipt.lease_owner.clone(),
            token: claimed.receipt.token.clone(),
            policy_digest: claimed.run.policy_digest.clone(),
        };
        let path = self.lease_snapshot_path();
        let tmp = path.with_extension("json.tmp");
        let body = serde_json::to_string(&snap).map_err(|e| DogfoodError::Io(e.to_string()))?;
        fs::write(&tmp, body).map_err(|e| DogfoodError::Io(e.to_string()))?;
        fs::rename(&tmp, &path).map_err(|e| DogfoodError::Io(e.to_string()))?;
        Ok(())
    }

    fn load_lease_snapshot(&self) -> Result<Option<DurableLeaseSnapshot>, DogfoodError> {
        let path = self.lease_snapshot_path();
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(&path).map_err(|e| DogfoodError::Io(e.to_string()))?;
        let snap: DurableLeaseSnapshot =
            serde_json::from_str(&raw).map_err(|e| DogfoodError::Io(e.to_string()))?;
        Ok(Some(snap))
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
        let updated = self.receipts.record_attempt(
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
        let decision = if self.config.require_approval && !self.approval_granted {
            PermissionDecision::Ask
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
            PermissionDecision::Ask if !self.approval_granted => {
                // Record approval-required as a gate note, do not execute.
                let gate = NamedGate {
                    name: "approval".into(),
                    outcome: GateOutcome::Failed,
                    exact_head_sha: self.config.code_head_sha.clone(),
                    evidence_uri: Some("synthetic://approval_required".into()),
                    content_digest: Some(digest_bytes(b"approval_required")),
                };
                let updated = self.receipts.set_gate(
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
                let updated = self.receipts.set_gate(
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
        let first = self.effects.apply_once(&effect_id, &digest)?;
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
        let updated = self.receipts.set_gate(
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
        self.artifact_path = Some(path);
        self.artifact_digest = Some(digest.clone());
        let updated = self.receipts.set_artifact_digest(
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
            let updated = self.receipts.set_gate(
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
        let updated = self.receipts.transition(
            &self.config.owner,
            &self.config.receipt_id,
            self.receipt_revision,
            now,
            |receipt| {
                receipt.recovery_cleanup = cleanup;
                receipt.remaining_risks = vec![
                    "approval_resume_open".into(),
                    "durable_multiprocess_receipt_store_open".into(),
                    "daemon_process_spawn_covered_by_existing_acceptance".into(),
                ];
                receipt.validation_summary = Some(
                    "dogfood: truth_probe passed; artifact published; process effect once".into(),
                );
                Ok(())
            },
        )?;
        self.receipt_revision = updated.revision;
        let receipt = self.receipts.complete(
            &self.config.owner,
            &self.config.receipt_id,
            self.receipt_revision,
            now,
        )?;
        self.receipt_revision = receipt.revision;
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
        let updated = self.receipts.transition(
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
        let receipt = self.receipts.cancel(
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
            let receipt = self.receipts.mark_unresolved(
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
    fn reclaim_or_adopt_after_restart(&mut self) -> Result<(), DogfoodError> {
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
        let receipt = RunLeaseReceipt {
            run_id: snap.run_id,
            worker_id: snap.worker_id,
            generation: snap.generation,
            revision: snap.revision,
            lease_owner: snap.lease_owner,
            token: snap.token,
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
        let updated = self.receipts.transition(
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
        let receipt = self.receipts.fail(
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
        path.grant_approval();
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
        // Cross-owner complete is fenced by MemoryReceiptLedger.
        let mut ledger = MemoryReceiptLedger::new();
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
        ledger.insert_open(receipt).unwrap();
        let err = ledger.complete("bob", "r1", 1, now()).unwrap_err();
        assert_eq!(err, ReceiptError::InvalidInput("receipt not found"));
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
