//! Transport-neutral local kernel boundary for a future Tauri/Horus shell.
//!
//! Versioned commands and events only. No Tauri types, no UI inference of
//! completion/ownership/route from display strings. Authority always comes
//! from typed projections and the receipt contract.
//!
//! # Authority split (honest)
//!
//! - **Durable multi-process facts** live in
//!   [`crate::kernel_store::KernelStore`] (`kernel.sqlite`).
//!   [`KernelCommandKind::DriveDogfood`] writes the Process path;
//!   [`KernelCommandKind::DecideApproval`] / [`KernelCommandKind::DenyApproval`]
//!   persist grant/deny only through that store.
//! - Approve/deny without a `dogfood_root` or registered durable store fail
//!   closed. Deny without a caller digest fails closed. Deny fences
//!   `expected_revision` / `lease_owner` / `generation` the same way grant
//!   does (lease fence checked when present). Deny may omit `agent_run_id`;
//!   the delivery FSM and ownership projection use the durable row's run id.
//!   Grant/deny and the delivery transition commit in one KernelStore
//!   transaction; a missing delivery row fails closed and does not persist
//!   the decision. Events remain rebuildable; they are not a second authority.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use vyane_core::{
    CompletionReceipt, MemoryReceiptLedger, RECEIPT_SCHEMA_VERSION, ReceiptFinalStatus,
    RouteConfig, TaskCase,
};

use crate::dogfood::run_successful_dogfood;
use crate::kernel_store::{
    ApprovalDecisionKind, ApprovalDenyBinding, ApprovalGrantBinding, KernelStore, KernelStoreError,
};

/// Frozen local boundary protocol version.
pub const KERNEL_BOUNDARY_VERSION: u32 = 1;

/// Collect candidate `kernel.sqlite` paths under a dogfood root for receipt rebuild.
fn push_kernel_candidates(root: &Path, receipt_id: &str, out: &mut Vec<PathBuf>) {
    out.push(root.join("kernel.sqlite"));
    if let Some(suffix) = receipt_id.strip_prefix("rcpt-") {
        out.push(root.join(format!("durable-{suffix}")).join("kernel.sqlite"));
    }
    // Scan durable-* children (DriveDogfood layout).
    if let Ok(entries) = fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name.starts_with("durable-") {
                    out.push(path.join("kernel.sqlite"));
                }
            }
        }
    }
}

/// Local principal (not a multi-user production boundary).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelPrincipal {
    pub principal_id: String,
    /// Bound owner scope; request payloads cannot override this.
    pub owner: String,
}

/// Capability discovery tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum KernelCapability {
    SubmitTask,
    CancelTask,
    Approve,
    Deny,
    Status,
    ReadArtifact,
    ReadReceipt,
    SubscribeEvents,
    ProjectTask,
    ProjectAgentRun,
    ProjectRoute,
    ProjectOwnership,
    ProjectReceipt,
    ReplayEvents,
    DriveDogfood,
}

impl KernelCapability {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SubmitTask => "submit_task",
            Self::CancelTask => "cancel_task",
            Self::Approve => "approve",
            Self::Deny => "deny",
            Self::Status => "status",
            Self::ReadArtifact => "read_artifact",
            Self::ReadReceipt => "read_receipt",
            Self::SubscribeEvents => "subscribe_events",
            Self::ProjectTask => "project_task",
            Self::ProjectAgentRun => "project_agent_run",
            Self::ProjectRoute => "project_route",
            Self::ProjectOwnership => "project_ownership",
            Self::ProjectReceipt => "project_receipt",
            Self::ReplayEvents => "replay_events",
            Self::DriveDogfood => "drive_dogfood",
        }
    }
}

/// Versioned command kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum KernelCommandKind {
    DiscoverCapabilities,
    SubmitTask,
    CancelTask,
    DecideApproval,
    /// Explicit deny (never recoverable by a later grant on the same request).
    DenyApproval,
    GetProjection,
    /// Alias of status/read projection for shell adapters.
    Status,
    ReadArtifact,
    ReadReceipt,
    Subscribe,
    Replay,
    /// Drive the Process dogfood path to a truth-verified CompletionReceipt.
    DriveDogfood,
}

/// Versioned local command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KernelCommand {
    pub boundary_version: u32,
    pub command_id: String,
    pub kind: KernelCommandKind,
    pub principal: KernelPrincipal,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_case: Option<TaskCase>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<RouteConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_granted: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribe: Option<SubscribeRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_from: Option<ReplayCursor>,
    /// Durable dogfood root for [`KernelCommandKind::DriveDogfood`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dogfood_root: Option<String>,
    /// Grant/deny binding. Required for granted [`KernelCommandKind::DecideApproval`]
    /// (`approval_granted = true`) and for every [`KernelCommandKind::DenyApproval`].
    /// Not required when DecideApproval has `approval_granted = false` or `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_binding: Option<KernelApprovalBinding>,
}

/// Binding for durable approve/deny. Required on granted DecideApproval and
/// every DenyApproval (missing there fails closed). DecideApproval with
/// `approval_granted = false` or `None` does not need it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelApprovalBinding {
    pub request_digest: String,
    pub expected_revision: u64,
    pub lease_owner: String,
    pub generation: u64,
}

/// Event stream subscription request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeRequest {
    /// Exclusive lower bound sequence; `None` means from latest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_sequence: Option<u64>,
}

/// Replay cursor for reconnect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayCursor {
    pub sequence: u64,
}

/// Versioned event kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum KernelEventKind {
    Capabilities,
    TaskAccepted,
    Claimed,
    Started,
    TaskStateChanged,
    ApprovalRequired,
    Approved,
    Denied,
    Resumed,
    ApprovalDecided,
    EffectRecorded,
    GateResult,
    ArtifactFinalized,
    CompletionReceiptFinalized,
    ReceiptUpdated,
    Cancelled,
    Failed,
    Error,
    Heartbeat,
    Unknown,
}

/// Versioned event. Display text fields are never authoritative.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KernelEvent {
    pub boundary_version: u32,
    pub sequence: u64,
    pub kind: KernelEventKind,
    pub owner: String,
    pub emitted_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_status: Option<ReceiptFinalStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection: Option<KernelProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<KernelErrorCode>,
    /// Non-authoritative human hint only; UI must not infer state from this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_hint: Option<String>,
}

/// Typed projections — the only authority for UI state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "projection", rename_all = "snake_case")]
pub enum KernelProjection {
    Capabilities {
        capabilities: Vec<KernelCapability>,
        receipt_schema_version: u32,
    },
    Task {
        task_case: TaskCase,
        owner: String,
    },
    AgentRun {
        agent_run_id: String,
        owner: String,
        /// Opaque lifecycle token from the store; not display text.
        state: String,
    },
    Route {
        route: RouteConfig,
    },
    Ownership {
        owner: String,
        lease_owner: Option<String>,
        generation: Option<u64>,
    },
    Receipt {
        receipt: Box<CompletionReceipt>,
    },
    /// Explicit unavailable/unknown without inventing success.
    Unavailable {
        reason: KernelErrorCode,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum KernelErrorCode {
    Unauthorized,
    OwnerMismatch,
    NotFound,
    Conflict,
    UnsupportedVersion,
    Unavailable,
    InvalidCommand,
    ApprovalRequired,
}

impl KernelErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unauthorized => "unauthorized",
            Self::OwnerMismatch => "owner_mismatch",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::UnsupportedVersion => "unsupported_version",
            Self::Unavailable => "unavailable",
            Self::InvalidCommand => "invalid_command",
            Self::ApprovalRequired => "approval_required",
        }
    }
}

/// Locates the durable [`KernelStore`] authoritative for a receipt.
///
/// Approve/deny and durable reads ([`KernelCommandKind::Status`],
/// [`KernelCommandKind::ReadReceipt`], [`KernelCommandKind::GetProjection`],
/// [`KernelCommandKind::ReadArtifact`]) consume this seam instead of probing
/// sqlite paths in the adapter. `None` is unknown and maps to
/// [`KernelErrorCode::NotFound`]. An explicit `dogfood_root` does not fall
/// back to the memory ledger when resolution returns `None`.
pub trait KernelStoreResolver: Send + Sync {
    fn resolve(
        &self,
        owner: &str,
        receipt_id: &str,
        dogfood_root: Option<&str>,
    ) -> Option<KernelStore>;
}

struct DurableStoreIndex {
    durable_roots: Mutex<Vec<PathBuf>>,
    receipt_store_index: Mutex<HashMap<String, PathBuf>>,
}

impl DurableStoreIndex {
    fn new() -> Self {
        Self {
            durable_roots: Mutex::new(Vec::new()),
            receipt_store_index: Mutex::new(HashMap::new()),
        }
    }

    fn register_root(&self, root: PathBuf) {
        if let Ok(mut guard) = self.durable_roots.lock()
            && !guard.iter().any(|p| p == &root)
        {
            guard.push(root);
        }
    }

    fn remember_receipt_store(&self, receipt_id: &str, path: PathBuf) {
        if let Ok(mut index) = self.receipt_store_index.lock() {
            index.insert(receipt_id.to_string(), path);
        }
    }

    fn store_for_existing_path(&self, path: &Path) -> Option<KernelStore> {
        // Discovery cannot create, migrate, or change journal mode. Revalidate
        // even remembered paths so a replaced candidate is not trusted by cache.
        KernelStore::probe_existing(path).ok()
    }

    fn indexed_store(&self, receipt_id: &str) -> Option<KernelStore> {
        let path = {
            let Ok(index) = self.receipt_store_index.lock() else {
                return None;
            };
            index.get(receipt_id).cloned()?
        };
        self.store_for_existing_path(&path)
    }
}

/// Default resolver: candidate `kernel.sqlite` paths under registered /
/// command `dogfood_root`s. Named roots stay exclusive.
struct FilesystemKernelStoreResolver {
    index: Arc<DurableStoreIndex>,
}

impl KernelStoreResolver for FilesystemKernelStoreResolver {
    fn resolve(
        &self,
        owner: &str,
        receipt_id: &str,
        dogfood_root: Option<&str>,
    ) -> Option<KernelStore> {
        // Named roots stay exclusive. The index only short-circuits the
        // unbounded registered-root scan so a later decoy cannot steal a
        // receipt this adapter already resolved.
        if dogfood_root.is_none()
            && let Some(store) = self.index.indexed_store(receipt_id)
            && matches!(store.get_approval(owner, receipt_id), Ok(Some(_)))
        {
            return Some(store);
        }

        let mut candidates: Vec<PathBuf> = Vec::new();
        if let Some(root) = dogfood_root.map(PathBuf::from) {
            // A named root is exclusive so a sibling registered sqlite cannot
            // steal the grant/deny write.
            push_kernel_candidates(&root, receipt_id, &mut candidates);
        } else if let Ok(roots) = self.index.durable_roots.lock() {
            for root in roots.iter() {
                push_kernel_candidates(root, receipt_id, &mut candidates);
            }
        }
        let mut seen = HashSet::new();
        candidates.retain(|path| seen.insert(path.clone()));
        let mut receipt_hit = None;
        for path in candidates {
            let Some(store) = self.index.store_for_existing_path(&path) else {
                continue;
            };
            if matches!(store.get_approval(owner, receipt_id), Ok(Some(_))) {
                self.index
                    .remember_receipt_store(receipt_id, store.path().to_path_buf());
                return Some(store);
            }
            if receipt_hit.is_none() && matches!(store.get_receipt(owner, receipt_id), Ok(Some(_)))
            {
                receipt_hit = Some(store);
            }
        }
        if let Some(store) = receipt_hit {
            self.index
                .remember_receipt_store(receipt_id, store.path().to_path_buf());
            return Some(store);
        }
        None
    }
}

/// In-process adapter exercising the transport-neutral contract.
///
/// Process-local event queue is rebuildable. Durable receipt and approval
/// authority is [`KernelStore`] located by [`KernelStoreResolver`]. An
/// explicit `dogfood_root` is exclusive: it never falls back to the memory
/// ledger or to registered siblings. Rootless submit and cancel keep their
/// in-memory behavior; rootless reads try memory before the resolver.
/// Approve/deny fail closed as
/// [`KernelErrorCode::NotFound`], not [`KernelErrorCode::InvalidCommand`].
pub struct LocalKernelAdapter {
    principal: KernelPrincipal,
    events: Mutex<VecDeque<KernelEvent>>,
    next_sequence: Mutex<u64>,
    /// Fenced receipt ledger for pure boundary submit/cancel (non-dogfood).
    receipts: Mutex<MemoryReceiptLedger>,
    index: Arc<DurableStoreIndex>,
    store_resolver: Arc<dyn KernelStoreResolver>,
}

impl LocalKernelAdapter {
    #[must_use]
    pub fn new(principal: KernelPrincipal) -> Self {
        let index = Arc::new(DurableStoreIndex::new());
        let store_resolver = Arc::new(FilesystemKernelStoreResolver {
            index: Arc::clone(&index),
        });
        Self::with_index(principal, index, store_resolver)
    }

    /// Construct with an injected store locator. Approve/deny and durable
    /// reads use this resolver exclusively and do not fall back to filesystem
    /// probing.
    #[must_use]
    pub fn with_store_resolver(
        principal: KernelPrincipal,
        store_resolver: Arc<dyn KernelStoreResolver>,
    ) -> Self {
        Self::with_index(
            principal,
            Arc::new(DurableStoreIndex::new()),
            store_resolver,
        )
    }

    fn with_index(
        principal: KernelPrincipal,
        index: Arc<DurableStoreIndex>,
        store_resolver: Arc<dyn KernelStoreResolver>,
    ) -> Self {
        Self {
            principal,
            events: Mutex::new(VecDeque::new()),
            next_sequence: Mutex::new(1),
            receipts: Mutex::new(MemoryReceiptLedger::new()),
            index,
            store_resolver,
        }
    }

    /// Register a durable root so later Status/ReadReceipt can rebuild without
    /// an in-process receipt cache (discard-and-rebuild).
    pub fn register_durable_root(&self, root: impl Into<PathBuf>) {
        self.index.register_root(root.into());
    }

    /// Drop process-local receipt cache (events kept). Used by rebuild tests.
    pub fn discard_in_memory_receipts(&self) {
        if let Ok(mut guard) = self.receipts.lock() {
            *guard = MemoryReceiptLedger::new();
        }
    }

    /// Receipt for Status / ReadReceipt / GetProjection / ReadArtifact.
    ///
    /// An explicit `dogfood_root` resolves only through [`KernelStoreResolver`]
    /// and [`KernelStore::get_receipt`]. Rootless commands still read the
    /// in-memory ledger before that resolver.
    fn load_receipt(
        &self,
        receipt_id: &str,
        dogfood_root: Option<&str>,
    ) -> Option<CompletionReceipt> {
        if dogfood_root.is_none()
            && let Ok(guard) = self.receipts.lock()
            && let Some(receipt) = guard.get_for_owner(&self.principal.owner, receipt_id)
        {
            return Some(receipt.clone());
        }
        let store = self.open_durable_store(receipt_id, dogfood_root)?;
        store
            .get_receipt(&self.principal.owner, receipt_id)
            .ok()
            .flatten()
    }

    fn open_durable_store(
        &self,
        receipt_id: &str,
        dogfood_root: Option<&str>,
    ) -> Option<KernelStore> {
        self.store_resolver
            .resolve(&self.principal.owner, receipt_id, dogfood_root)
    }

    fn map_store_error(err: KernelStoreError) -> KernelErrorCode {
        match err {
            KernelStoreError::NotFound => KernelErrorCode::NotFound,
            KernelStoreError::OwnerMismatch => KernelErrorCode::OwnerMismatch,
            KernelStoreError::ApprovalDeniedFinal
            | KernelStoreError::ApprovalBindingMismatch
            | KernelStoreError::Conflict(_)
            | KernelStoreError::StaleRevision { .. }
            | KernelStoreError::TerminalImmutable => KernelErrorCode::Conflict,
            KernelStoreError::UnsupportedSchema { .. } => KernelErrorCode::UnsupportedVersion,
            KernelStoreError::InvalidInput(_) => KernelErrorCode::InvalidCommand,
            KernelStoreError::Io(_)
            | KernelStoreError::Sqlite(_)
            | KernelStoreError::Receipt(_)
            | KernelStoreError::Delivery(_)
            | KernelStoreError::DuplicateEffect { .. } => KernelErrorCode::Unavailable,
        }
    }

    fn handle_decide_approval(&self, command: KernelCommand, now: DateTime<Utc>) -> KernelEvent {
        let Some(receipt_id) = command.receipt_id.clone() else {
            return self.error_event(now, KernelErrorCode::InvalidCommand, None);
        };
        // Missing run_id/binding is malformed even when no store is resolvable.
        // InvalidCommand stays for those fields; NotFound is only for a
        // well-formed approve whose receipt is not in any candidate store.
        let granted_binding = if command.approval_granted.unwrap_or(false) {
            let Some(run_id) = command.agent_run_id.clone() else {
                return self.error_event(now, KernelErrorCode::InvalidCommand, Some(receipt_id));
            };
            let Some(binding) = command.approval_binding.clone() else {
                return self.error_event(now, KernelErrorCode::InvalidCommand, Some(receipt_id));
            };
            Some((run_id, binding))
        } else {
            None
        };
        let Some(store) = self.open_durable_store(&receipt_id, command.dogfood_root.as_deref())
        else {
            return self.error_event(now, KernelErrorCode::NotFound, Some(receipt_id));
        };
        let Some((run_id, binding)) = granted_binding else {
            return match store.get_approval(&self.principal.owner, &receipt_id) {
                Ok(Some(row)) if row.decision == ApprovalDecisionKind::Pending => self.push_event(
                    KernelEventKind::ApprovalRequired,
                    now,
                    Some(receipt_id),
                    command.agent_run_id,
                    None,
                    None,
                    Some(KernelErrorCode::ApprovalRequired),
                    Some("approval still required".into()),
                ),
                Ok(Some(_)) => self.error_event(now, KernelErrorCode::Conflict, Some(receipt_id)),
                Ok(None) => self.error_event(now, KernelErrorCode::NotFound, Some(receipt_id)),
                Err(err) => self.error_event(now, Self::map_store_error(err), Some(receipt_id)),
            };
        };
        let grant = ApprovalGrantBinding {
            owner: self.principal.owner.clone(),
            receipt_id: receipt_id.clone(),
            run_id,
            request_digest: binding.request_digest,
            expected_revision: binding.expected_revision,
            lease_owner: binding.lease_owner.clone(),
            generation: binding.generation,
            decided_by: self.principal.principal_id.clone(),
        };
        match store.grant_approval_and_transition(&grant, now) {
            Ok(decision) => {
                if let Some(root) = command.dogfood_root.as_deref() {
                    self.register_durable_root(root);
                }
                let projection = self.ownership_from_store(&store, Some(decision.run_id.as_str()));
                self.push_event(
                    KernelEventKind::Approved,
                    now,
                    Some(receipt_id),
                    Some(decision.run_id),
                    None,
                    Some(projection),
                    None,
                    Some("approval granted".into()),
                )
            }
            Err(err) => self.error_event(now, Self::map_store_error(err), Some(receipt_id)),
        }
    }

    fn handle_deny_approval(&self, command: KernelCommand, now: DateTime<Utc>) -> KernelEvent {
        let Some(receipt_id) = command.receipt_id.clone() else {
            return self.error_event(now, KernelErrorCode::InvalidCommand, None);
        };
        let Some(binding) = command.approval_binding.clone() else {
            return self.error_event(now, KernelErrorCode::InvalidCommand, Some(receipt_id));
        };
        let Some(store) = self.open_durable_store(&receipt_id, command.dogfood_root.as_deref())
        else {
            return self.error_event(now, KernelErrorCode::NotFound, Some(receipt_id));
        };
        let deny = ApprovalDenyBinding {
            owner: self.principal.owner.clone(),
            receipt_id: receipt_id.clone(),
            request_digest: binding.request_digest,
            expected_revision: binding.expected_revision,
            lease_owner: binding.lease_owner,
            generation: binding.generation,
            decided_by: self.principal.principal_id.clone(),
        };
        match store.deny_approval_and_transition(&deny, now) {
            Ok(decision) => {
                if let Some(root) = command.dogfood_root.as_deref() {
                    self.register_durable_root(root);
                }
                // Denied is the approval decision. Receipt status stays whatever
                // KernelStore already recorded (usually Open) — do not invent Failed.
                // Ownership and the delivery FSM use the durable row's run id,
                // not the caller-supplied agent_run_id (which may be omitted).
                self.push_event(
                    KernelEventKind::Denied,
                    now,
                    Some(receipt_id),
                    Some(decision.run_id.clone()),
                    None,
                    Some(self.ownership_from_store(&store, Some(decision.run_id.as_str()))),
                    None,
                    Some("approval denied".into()),
                )
            }
            Err(err) => self.error_event(now, Self::map_store_error(err), Some(receipt_id)),
        }
    }

    pub fn handle(&self, command: KernelCommand, now: DateTime<Utc>) -> KernelEvent {
        if command.boundary_version != KERNEL_BOUNDARY_VERSION {
            return self.error_event(
                now,
                KernelErrorCode::UnsupportedVersion,
                command.receipt_id.clone(),
            );
        }
        if command.principal.owner != self.principal.owner
            || command.principal.principal_id != self.principal.principal_id
        {
            return self.error_event(
                now,
                KernelErrorCode::Unauthorized,
                command.receipt_id.clone(),
            );
        }

        match command.kind {
            KernelCommandKind::DiscoverCapabilities => {
                let projection = KernelProjection::Capabilities {
                    capabilities: vec![
                        KernelCapability::SubmitTask,
                        KernelCapability::CancelTask,
                        KernelCapability::Approve,
                        KernelCapability::Deny,
                        KernelCapability::Status,
                        KernelCapability::ReadArtifact,
                        KernelCapability::ReadReceipt,
                        KernelCapability::SubscribeEvents,
                        KernelCapability::ProjectTask,
                        KernelCapability::ProjectAgentRun,
                        KernelCapability::ProjectRoute,
                        KernelCapability::ProjectOwnership,
                        KernelCapability::ProjectReceipt,
                        KernelCapability::ReplayEvents,
                        KernelCapability::DriveDogfood,
                    ],
                    receipt_schema_version: RECEIPT_SCHEMA_VERSION,
                };
                self.push_event(
                    KernelEventKind::Capabilities,
                    now,
                    None,
                    None,
                    None,
                    Some(projection),
                    None,
                    Some("capabilities".into()),
                )
            }
            KernelCommandKind::SubmitTask => {
                let Some(task) = command.task_case else {
                    return self.error_event(now, KernelErrorCode::InvalidCommand, None);
                };
                let Some(route) = command.route else {
                    return self.error_event(now, KernelErrorCode::InvalidCommand, None);
                };
                let receipt_id = command
                    .receipt_id
                    .clone()
                    .unwrap_or_else(|| format!("rcpt-{}", command.command_id));
                let Ok(receipt) = CompletionReceipt::open(
                    receipt_id.clone(),
                    self.principal.owner.clone(),
                    task.clone(),
                    route.clone(),
                    now,
                ) else {
                    return self.error_event(
                        now,
                        KernelErrorCode::InvalidCommand,
                        Some(receipt_id),
                    );
                };
                let mut guard = self.receipts.lock().expect("receipts");
                if guard.insert_open(receipt.clone()).is_err() {
                    return self.error_event(now, KernelErrorCode::Conflict, Some(receipt_id));
                }
                drop(guard);
                self.push_event(
                    KernelEventKind::TaskAccepted,
                    now,
                    Some(receipt_id),
                    command.agent_run_id,
                    Some(ReceiptFinalStatus::Open),
                    Some(KernelProjection::Receipt {
                        receipt: Box::new(receipt),
                    }),
                    None,
                    Some("task accepted".into()),
                )
            }
            KernelCommandKind::CancelTask => {
                let Some(receipt_id) = command.receipt_id.clone() else {
                    return self.error_event(now, KernelErrorCode::InvalidCommand, None);
                };
                let mut guard = self.receipts.lock().expect("receipts");
                let Some(current) = guard
                    .get_for_owner(&self.principal.owner, &receipt_id)
                    .cloned()
                else {
                    return self.error_event(now, KernelErrorCode::NotFound, Some(receipt_id));
                };
                // Fenced cancel — same CAS path as dogfood / MemoryReceiptLedger.
                let cancelled =
                    match guard.cancel(&self.principal.owner, &receipt_id, current.revision, now) {
                        Ok(r) => r,
                        Err(_) => {
                            return self.error_event(
                                now,
                                KernelErrorCode::Conflict,
                                Some(receipt_id),
                            );
                        }
                    };
                drop(guard);
                self.push_event(
                    KernelEventKind::Cancelled,
                    now,
                    Some(receipt_id),
                    command.agent_run_id,
                    Some(ReceiptFinalStatus::Cancelled),
                    Some(KernelProjection::Receipt {
                        receipt: Box::new(cancelled),
                    }),
                    None,
                    Some("cancelled".into()),
                )
            }
            KernelCommandKind::DriveDogfood => {
                let Some(root) = command.dogfood_root.clone() else {
                    return self.error_event(now, KernelErrorCode::InvalidCommand, None);
                };
                let root = PathBuf::from(root);
                let suffix = command.command_id.clone();
                match run_successful_dogfood(&root, &self.principal.owner, &suffix, now) {
                    Ok((receipt, effects)) => {
                        // Register durable path so Status rebuilds after memory discard.
                        let durable = root.join(format!("durable-{suffix}"));
                        self.register_durable_root(&durable);
                        self.register_durable_root(&root);
                        // Emit lifecycle event kinds for shell adapters (non-authoritative
                        // display_hint only). Durable facts live in kernel.sqlite under root.
                        let _ = self.push_event(
                            KernelEventKind::Started,
                            now,
                            Some(receipt.receipt_id.clone()),
                            command.agent_run_id.clone(),
                            Some(ReceiptFinalStatus::Open),
                            None,
                            None,
                            Some("started".into()),
                        );
                        if !effects.is_empty() {
                            let _ = self.push_event(
                                KernelEventKind::EffectRecorded,
                                now,
                                Some(receipt.receipt_id.clone()),
                                command.agent_run_id.clone(),
                                None,
                                None,
                                None,
                                Some("effect_recorded".into()),
                            );
                        }
                        if receipt.output_artifact_digest.is_some() {
                            let _ = self.push_event(
                                KernelEventKind::ArtifactFinalized,
                                now,
                                Some(receipt.receipt_id.clone()),
                                command.agent_run_id.clone(),
                                None,
                                None,
                                None,
                                Some("artifact_finalized".into()),
                            );
                        }
                        let _ = self.push_event(
                            KernelEventKind::GateResult,
                            now,
                            Some(receipt.receipt_id.clone()),
                            command.agent_run_id.clone(),
                            None,
                            None,
                            None,
                            Some("gate_result".into()),
                        );
                        self.push_event(
                            KernelEventKind::CompletionReceiptFinalized,
                            now,
                            Some(receipt.receipt_id.clone()),
                            command.agent_run_id,
                            Some(receipt.final_status),
                            Some(KernelProjection::Receipt {
                                receipt: Box::new(receipt),
                            }),
                            None,
                            Some("completion_receipt_finalized".into()),
                        )
                    }
                    Err(_) => {
                        self.error_event(now, KernelErrorCode::Unavailable, command.receipt_id)
                    }
                }
            }
            KernelCommandKind::DecideApproval => self.handle_decide_approval(command, now),
            KernelCommandKind::DenyApproval => self.handle_deny_approval(command, now),
            KernelCommandKind::Status
            | KernelCommandKind::ReadReceipt
            | KernelCommandKind::GetProjection => {
                if let Some(receipt_id) = &command.receipt_id {
                    match self.load_receipt(receipt_id, command.dogfood_root.as_deref()) {
                        Some(receipt) => self.push_event(
                            KernelEventKind::ReceiptUpdated,
                            now,
                            Some(receipt_id.clone()),
                            command.agent_run_id,
                            Some(receipt.final_status),
                            Some(KernelProjection::Receipt {
                                receipt: Box::new(receipt),
                            }),
                            None,
                            None,
                        ),
                        None => self.error_event(
                            now,
                            KernelErrorCode::NotFound,
                            Some(receipt_id.clone()),
                        ),
                    }
                } else {
                    self.push_event(
                        KernelEventKind::Error,
                        now,
                        None,
                        None,
                        None,
                        Some(KernelProjection::Unavailable {
                            reason: KernelErrorCode::NotFound,
                        }),
                        Some(KernelErrorCode::NotFound),
                        None,
                    )
                }
            }
            KernelCommandKind::ReadArtifact => {
                let Some(receipt_id) = &command.receipt_id else {
                    return self.error_event(now, KernelErrorCode::InvalidCommand, None);
                };
                match self.load_receipt(receipt_id, command.dogfood_root.as_deref()) {
                    Some(receipt) if receipt.output_artifact_digest.is_some() => self.push_event(
                        KernelEventKind::ArtifactFinalized,
                        now,
                        Some(receipt_id.clone()),
                        command.agent_run_id,
                        Some(receipt.final_status),
                        Some(KernelProjection::Receipt {
                            receipt: Box::new(receipt),
                        }),
                        None,
                        // display_hint must never be treated as authority
                        Some("artifact projection (non-authoritative hint)".into()),
                    ),
                    Some(_) | None => {
                        self.error_event(now, KernelErrorCode::NotFound, Some(receipt_id.clone()))
                    }
                }
            }
            KernelCommandKind::Subscribe => {
                // Subscription is modeled as returning a heartbeat with cursor.
                let after = command
                    .subscribe
                    .as_ref()
                    .and_then(|s| s.after_sequence)
                    .unwrap_or(0);
                self.push_event(
                    KernelEventKind::Heartbeat,
                    now,
                    None,
                    None,
                    None,
                    Some(KernelProjection::Ownership {
                        owner: self.principal.owner.clone(),
                        lease_owner: None,
                        generation: Some(after),
                    }),
                    None,
                    Some("subscribed".into()),
                )
            }
            KernelCommandKind::Replay => {
                let from = command
                    .replay_from
                    .as_ref()
                    .map(|c| c.sequence)
                    .unwrap_or(0);
                let events = self.events.lock().expect("events");
                // Return the first event at-or-after cursor, or unavailable.
                if let Some(event) = events.iter().find(|e| e.sequence > from).cloned() {
                    event
                } else {
                    drop(events);
                    self.push_event(
                        KernelEventKind::Heartbeat,
                        now,
                        None,
                        None,
                        None,
                        Some(KernelProjection::Unavailable {
                            reason: KernelErrorCode::Unavailable,
                        }),
                        None,
                        Some("no events to replay".into()),
                    )
                }
            }
        }
    }

    /// Events after an exclusive sequence cursor (reconnect/replay).
    pub fn events_after(&self, after_sequence: u64) -> Vec<KernelEvent> {
        self.events
            .lock()
            .expect("events")
            .iter()
            .filter(|e| e.sequence > after_sequence)
            .cloned()
            .collect()
    }

    fn ownership_from_store(&self, store: &KernelStore, run_id: Option<&str>) -> KernelProjection {
        let fence = run_id.and_then(|run_id| {
            store
                .get_lease_fence(&self.principal.owner, run_id)
                .ok()
                .flatten()
        });
        KernelProjection::Ownership {
            owner: self.principal.owner.clone(),
            lease_owner: fence.as_ref().map(|f| f.lease_owner.clone()),
            generation: fence.as_ref().map(|f| f.generation),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn push_event(
        &self,
        kind: KernelEventKind,
        now: DateTime<Utc>,
        receipt_id: Option<String>,
        agent_run_id: Option<String>,
        final_status: Option<ReceiptFinalStatus>,
        projection: Option<KernelProjection>,
        error: Option<KernelErrorCode>,
        display_hint: Option<String>,
    ) -> KernelEvent {
        let mut seq = self.next_sequence.lock().expect("seq");
        let sequence = *seq;
        *seq = seq.saturating_add(1);
        let event = KernelEvent {
            boundary_version: KERNEL_BOUNDARY_VERSION,
            sequence,
            kind,
            owner: self.principal.owner.clone(),
            emitted_at: now,
            receipt_id,
            agent_run_id,
            final_status,
            projection,
            error,
            display_hint,
        };
        self.events.lock().expect("events").push_back(event.clone());
        event
    }

    fn error_event(
        &self,
        now: DateTime<Utc>,
        code: KernelErrorCode,
        receipt_id: Option<String>,
    ) -> KernelEvent {
        self.push_event(
            KernelEventKind::Error,
            now,
            receipt_id,
            None,
            None,
            Some(KernelProjection::Unavailable { reason: code }),
            Some(code),
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;
    use vyane_core::{
        BillingModeCategory, EndpointClass, ModelId, Protocol, ProviderId, RiskClass,
    };

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 4, 15, 0, 0).single().unwrap()
    }

    fn principal() -> KernelPrincipal {
        KernelPrincipal {
            principal_id: "ui-local".into(),
            owner: "local".into(),
        }
    }

    fn task() -> TaskCase {
        TaskCase {
            task_case_id: "tc-1".into(),
            task_type: "boundary".into(),
            acceptance_digest: "aa".repeat(32),
            truth_probe_digest: "bb".repeat(32),
            risk_class: RiskClass::ReadOnly,
        }
    }

    fn route() -> RouteConfig {
        RouteConfig {
            provider: ProviderId::new("fixture"),
            endpoint_class: EndpointClass::LocalProcess,
            protocol: Protocol::OpenaiChat,
            harness: None,
            model: ModelId::new("m"),
            model_snapshot: None,
            requested_effort: None,
            effective_effort: None,
            profile_or_config_digest: None,
            billing_mode_category: BillingModeCategory::Unknown,
        }
    }

    #[test]
    fn capability_discovery_and_version_gate() {
        let adapter = LocalKernelAdapter::new(principal());
        let event = adapter.handle(
            KernelCommand {
                boundary_version: KERNEL_BOUNDARY_VERSION,
                command_id: "c1".into(),
                kind: KernelCommandKind::DiscoverCapabilities,
                principal: principal(),
                task_case: None,
                route: None,
                receipt_id: None,
                agent_run_id: None,
                approval_granted: None,
                subscribe: None,
                replay_from: None,
                dogfood_root: None,
                approval_binding: None,
            },
            now(),
        );
        assert_eq!(event.kind, KernelEventKind::Capabilities);
        match event.projection.unwrap() {
            KernelProjection::Capabilities {
                capabilities,
                receipt_schema_version,
            } => {
                assert!(capabilities.contains(&KernelCapability::ProjectReceipt));
                assert_eq!(receipt_schema_version, RECEIPT_SCHEMA_VERSION);
            }
            other => panic!("unexpected {other:?}"),
        }

        let bad = adapter.handle(
            KernelCommand {
                boundary_version: 99,
                command_id: "c2".into(),
                kind: KernelCommandKind::DiscoverCapabilities,
                principal: principal(),
                task_case: None,
                route: None,
                receipt_id: None,
                agent_run_id: None,
                approval_granted: None,
                subscribe: None,
                replay_from: None,
                dogfood_root: None,
                approval_binding: None,
            },
            now(),
        );
        assert_eq!(bad.error, Some(KernelErrorCode::UnsupportedVersion));
    }

    #[test]
    fn submit_cancel_projection_and_auth_fence() {
        let adapter = LocalKernelAdapter::new(principal());
        let accepted = adapter.handle(
            KernelCommand {
                boundary_version: KERNEL_BOUNDARY_VERSION,
                command_id: "s1".into(),
                kind: KernelCommandKind::SubmitTask,
                principal: principal(),
                task_case: Some(task()),
                route: Some(route()),
                receipt_id: Some("rcpt-bound".into()),
                agent_run_id: Some("run-1".into()),
                approval_granted: None,
                subscribe: None,
                replay_from: None,
                dogfood_root: None,
                approval_binding: None,
            },
            now(),
        );
        assert_eq!(accepted.kind, KernelEventKind::TaskAccepted);
        assert_eq!(accepted.final_status, Some(ReceiptFinalStatus::Open));
        // UI must use projection, not display_hint.
        assert!(accepted.display_hint.is_some());
        assert!(matches!(
            accepted.projection,
            Some(KernelProjection::Receipt { .. })
        ));

        let foreign = adapter.handle(
            KernelCommand {
                boundary_version: KERNEL_BOUNDARY_VERSION,
                command_id: "x".into(),
                kind: KernelCommandKind::GetProjection,
                principal: KernelPrincipal {
                    principal_id: "other".into(),
                    owner: "local".into(),
                },
                task_case: None,
                route: None,
                receipt_id: Some("rcpt-bound".into()),
                agent_run_id: None,
                approval_granted: None,
                subscribe: None,
                replay_from: None,
                dogfood_root: None,
                approval_binding: None,
            },
            now(),
        );
        assert_eq!(foreign.error, Some(KernelErrorCode::Unauthorized));

        let cancelled = adapter.handle(
            KernelCommand {
                boundary_version: KERNEL_BOUNDARY_VERSION,
                command_id: "c".into(),
                kind: KernelCommandKind::CancelTask,
                principal: principal(),
                task_case: None,
                route: None,
                receipt_id: Some("rcpt-bound".into()),
                agent_run_id: Some("run-1".into()),
                approval_granted: None,
                subscribe: None,
                replay_from: None,
                dogfood_root: None,
                approval_binding: None,
            },
            now(),
        );
        assert_eq!(cancelled.kind, KernelEventKind::Cancelled);
        assert_eq!(cancelled.final_status, Some(ReceiptFinalStatus::Cancelled));
    }

    #[test]
    fn reconnect_replay_after_cursor() {
        let adapter = LocalKernelAdapter::new(principal());
        let _ = adapter.handle(
            KernelCommand {
                boundary_version: KERNEL_BOUNDARY_VERSION,
                command_id: "d".into(),
                kind: KernelCommandKind::DiscoverCapabilities,
                principal: principal(),
                task_case: None,
                route: None,
                receipt_id: None,
                agent_run_id: None,
                approval_granted: None,
                subscribe: None,
                replay_from: None,
                dogfood_root: None,
                approval_binding: None,
            },
            now(),
        );
        let _ = adapter.handle(
            KernelCommand {
                boundary_version: KERNEL_BOUNDARY_VERSION,
                command_id: "s".into(),
                kind: KernelCommandKind::SubmitTask,
                principal: principal(),
                task_case: Some(task()),
                route: Some(route()),
                receipt_id: Some("r2".into()),
                agent_run_id: None,
                approval_granted: None,
                subscribe: None,
                replay_from: None,
                dogfood_root: None,
                approval_binding: None,
            },
            now(),
        );
        let after_first = adapter.events_after(1);
        assert!(!after_first.is_empty());
        assert!(after_first.iter().all(|e| e.sequence > 1));

        let replay = adapter.handle(
            KernelCommand {
                boundary_version: KERNEL_BOUNDARY_VERSION,
                command_id: "rp".into(),
                kind: KernelCommandKind::Replay,
                principal: principal(),
                task_case: None,
                route: None,
                receipt_id: None,
                agent_run_id: None,
                approval_granted: None,
                subscribe: None,
                replay_from: Some(ReplayCursor { sequence: 1 }),
                dogfood_root: None,
                approval_binding: None,
            },
            now(),
        );
        assert!(replay.sequence > 1 || replay.kind == KernelEventKind::Heartbeat);
    }

    #[test]
    fn display_hint_is_never_required_for_authority() {
        // Structural: final_status and projection are the authority fields.
        let event = KernelEvent {
            boundary_version: KERNEL_BOUNDARY_VERSION,
            sequence: 1,
            kind: KernelEventKind::ReceiptUpdated,
            owner: "local".into(),
            emitted_at: now(),
            receipt_id: Some("r".into()),
            agent_run_id: None,
            final_status: Some(ReceiptFinalStatus::Completed),
            projection: None,
            error: None,
            display_hint: Some("looks done!".into()),
        };
        // Without projection, a careful client treats status as incomplete evidence.
        assert!(event.projection.is_none());
        assert_eq!(event.final_status, Some(ReceiptFinalStatus::Completed));
        // Contract: display_hint must not be the sole signal — documented by
        // requiring projection for receipt authority in GetProjection handler.
        let _ = event.display_hint;
    }

    #[test]
    fn drive_dogfood_yields_truth_verified_receipt_projection() {
        let root = tempfile::tempdir().unwrap();
        let adapter = LocalKernelAdapter::new(principal());
        let event = adapter.handle(
            KernelCommand {
                boundary_version: KERNEL_BOUNDARY_VERSION,
                command_id: "df01".into(),
                kind: KernelCommandKind::DriveDogfood,
                principal: principal(),
                task_case: None,
                route: None,
                receipt_id: None,
                agent_run_id: None,
                approval_granted: None,
                subscribe: None,
                replay_from: None,
                dogfood_root: Some(root.path().to_string_lossy().into_owned()),
                approval_binding: None,
            },
            now(),
        );
        assert_eq!(event.kind, KernelEventKind::CompletionReceiptFinalized);
        assert_eq!(event.final_status, Some(ReceiptFinalStatus::Completed));
        match event.projection.unwrap() {
            KernelProjection::Receipt { receipt } => {
                assert!(receipt.final_status.is_success());
                assert!(receipt.output_artifact_digest.is_some());
                assert_eq!(
                    receipt.gates.truth_probe.outcome,
                    vyane_core::GateOutcome::Passed
                );
            }
            other => panic!("expected receipt projection, got {other:?}"),
        }
    }

    #[test]
    fn status_rebuilds_projection_from_kernel_store_after_discard() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let adapter = LocalKernelAdapter::new(principal());
        let driven = adapter.handle(
            KernelCommand {
                boundary_version: KERNEL_BOUNDARY_VERSION,
                command_id: "rb01".into(),
                kind: KernelCommandKind::DriveDogfood,
                principal: principal(),
                task_case: None,
                route: None,
                receipt_id: None,
                agent_run_id: None,
                approval_granted: None,
                subscribe: None,
                replay_from: None,
                dogfood_root: Some(root_s.clone()),
                approval_binding: None,
            },
            now(),
        );
        assert_eq!(driven.kind, KernelEventKind::CompletionReceiptFinalized);
        let receipt_id = driven.receipt_id.clone().expect("receipt_id on complete");
        let digest = match driven.projection.as_ref() {
            Some(KernelProjection::Receipt { receipt }) => {
                receipt.output_artifact_digest.clone().unwrap()
            }
            _ => panic!("expected receipt projection"),
        };

        // Discard process-local cache — client cache drop.
        adapter.discard_in_memory_receipts();

        // Fresh adapter with only dogfood_root (no registered roots, empty memory)
        // proves durable store is authority.
        let fresh = LocalKernelAdapter::new(principal());
        let status = fresh.handle(
            KernelCommand {
                boundary_version: KERNEL_BOUNDARY_VERSION,
                command_id: "st1".into(),
                kind: KernelCommandKind::Status,
                principal: principal(),
                task_case: None,
                route: None,
                receipt_id: Some(receipt_id.clone()),
                agent_run_id: None,
                approval_granted: None,
                subscribe: None,
                replay_from: None,
                dogfood_root: Some(root_s.clone()),
                approval_binding: None,
            },
            now(),
        );
        assert_eq!(
            status.error, None,
            "Status must rebuild from kernel.sqlite, got {status:?}"
        );
        assert_eq!(status.kind, KernelEventKind::ReceiptUpdated);
        assert_eq!(status.final_status, Some(ReceiptFinalStatus::Completed));
        match status.projection.unwrap() {
            KernelProjection::Receipt { receipt } => {
                assert_eq!(receipt.receipt_id, receipt_id);
                assert_eq!(
                    receipt.output_artifact_digest.as_deref(),
                    Some(digest.as_str())
                );
                assert!(receipt.final_status.is_success());
            }
            other => panic!("expected rebuilt receipt, got {other:?}"),
        }

        // Same on original adapter after discard (uses registered durable roots).
        let status2 = adapter.handle(
            KernelCommand {
                boundary_version: KERNEL_BOUNDARY_VERSION,
                command_id: "st2".into(),
                kind: KernelCommandKind::ReadReceipt,
                principal: principal(),
                task_case: None,
                route: None,
                receipt_id: Some(receipt_id),
                agent_run_id: None,
                approval_granted: None,
                subscribe: None,
                replay_from: None,
                dogfood_root: None,
                approval_binding: None,
            },
            now(),
        );
        assert_eq!(status2.error, None);
        assert_eq!(status2.final_status, Some(ReceiptFinalStatus::Completed));
    }

    fn digest_hex(seed: &str) -> String {
        seed.repeat(32)
    }

    fn approval_cmd(
        command_id: &str,
        kind: KernelCommandKind,
        receipt_id: Option<&str>,
        run_id: Option<&str>,
        granted: Option<bool>,
        root: Option<&str>,
        binding: Option<KernelApprovalBinding>,
    ) -> KernelCommand {
        KernelCommand {
            boundary_version: KERNEL_BOUNDARY_VERSION,
            command_id: command_id.into(),
            kind,
            principal: principal(),
            task_case: None,
            route: None,
            receipt_id: receipt_id.map(str::to_string),
            agent_run_id: run_id.map(str::to_string),
            approval_granted: granted,
            subscribe: None,
            replay_from: None,
            dogfood_root: root.map(str::to_string),
            approval_binding: binding,
        }
    }

    fn binding_for(digest: &str, revision: u64) -> KernelApprovalBinding {
        KernelApprovalBinding {
            request_digest: digest.to_string(),
            expected_revision: revision,
            lease_owner: principal().principal_id,
            generation: 1,
        }
    }

    fn seed_pending_ask(
        root: &std::path::Path,
        receipt_id: &str,
        run_id: &str,
        revision: u64,
    ) -> (KernelStore, String) {
        seed_pending_ask_with_lease(
            root,
            receipt_id,
            run_id,
            revision,
            &principal().principal_id,
            1,
        )
    }

    fn seed_pending_ask_with_lease(
        root: &std::path::Path,
        receipt_id: &str,
        run_id: &str,
        revision: u64,
        lease_owner: &str,
        generation: u64,
    ) -> (KernelStore, String) {
        seed_pending_ask_maybe_lease(
            root,
            receipt_id,
            run_id,
            revision,
            Some((lease_owner, generation)),
        )
    }

    fn seed_pending_ask_without_lease(
        root: &std::path::Path,
        receipt_id: &str,
        run_id: &str,
        revision: u64,
    ) -> (KernelStore, String) {
        seed_pending_ask_maybe_lease(root, receipt_id, run_id, revision, None)
    }

    fn seed_pending_ask_maybe_lease(
        root: &std::path::Path,
        receipt_id: &str,
        run_id: &str,
        revision: u64,
        lease: Option<(&str, u64)>,
    ) -> (KernelStore, String) {
        let store = KernelStore::open(root.join("kernel.sqlite")).unwrap();
        let receipt =
            CompletionReceipt::open(receipt_id, principal().owner, task(), route(), now()).unwrap();
        store.insert_open_receipt(&receipt).unwrap();
        let digest = digest_hex("ab");
        store
            .record_approval_required(
                &principal().owner,
                &format!("appr-{receipt_id}"),
                receipt_id,
                run_id,
                &digest,
                revision,
                now(),
            )
            .unwrap();
        if let Some((lease_owner, generation)) = lease {
            store
                .put_lease_fence(
                    &crate::kernel_store::LeaseFence {
                        owner: principal().owner,
                        run_id: run_id.into(),
                        lease_owner: lease_owner.into(),
                        generation,
                        revision: 1,
                        token: "tok".into(),
                        policy_digest: digest_hex("cd"),
                        expires_at_ms: None,
                    },
                    now(),
                )
                .unwrap();
        }
        let (_, phase_rev) = store
            .ensure_delivery_running(&principal().owner, receipt_id, run_id, now())
            .unwrap();
        store
            .set_delivery_phase(
                &principal().owner,
                receipt_id,
                run_id,
                phase_rev,
                crate::approval_fsm::DeliveryEvent::AskRequired,
                Some(&format!("appr-{receipt_id}")),
                now(),
            )
            .unwrap();
        (store, digest)
    }

    #[test]
    fn decide_approval_without_store_fails_closed() {
        let adapter = LocalKernelAdapter::new(principal());
        let event = adapter.handle(
            approval_cmd(
                "ap-nostore",
                KernelCommandKind::DecideApproval,
                Some("rcpt-nostore"),
                Some("run-nostore"),
                Some(true),
                None,
                Some(binding_for(&digest_hex("ab"), 1)),
            ),
            now(),
        );
        assert_ne!(
            event.kind,
            KernelEventKind::Approved,
            "grant without KernelStore must not emit Approved"
        );
        assert_eq!(event.kind, KernelEventKind::Error);
        assert_eq!(event.error, Some(KernelErrorCode::NotFound));
    }

    #[test]
    fn deny_approval_without_store_fails_closed() {
        let adapter = LocalKernelAdapter::new(principal());
        let event = adapter.handle(
            approval_cmd(
                "dn-nostore",
                KernelCommandKind::DenyApproval,
                Some("rcpt-nostore"),
                Some("run-nostore"),
                None,
                None,
                Some(binding_for(&digest_hex("ab"), 1)),
            ),
            now(),
        );
        assert_ne!(event.kind, KernelEventKind::Denied);
        assert_eq!(event.kind, KernelEventKind::Error);
        assert_eq!(event.error, Some(KernelErrorCode::NotFound));
    }

    #[test]
    fn unresolvable_named_root_is_not_found_not_invalid_command() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let adapter = LocalKernelAdapter::new(principal());
        let grant = adapter.handle(
            approval_cmd(
                "ap-empty-root",
                KernelCommandKind::DecideApproval,
                Some("rcpt-empty-root"),
                Some("run-empty-root"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest_hex("ab"), 1)),
            ),
            now(),
        );
        assert_eq!(grant.kind, KernelEventKind::Error);
        assert_eq!(grant.error, Some(KernelErrorCode::NotFound));
        assert_ne!(grant.kind, KernelEventKind::Approved);

        let deny = adapter.handle(
            approval_cmd(
                "dn-empty-root",
                KernelCommandKind::DenyApproval,
                Some("rcpt-empty-root"),
                Some("run-empty-root"),
                None,
                Some(&root_s),
                Some(binding_for(&digest_hex("ab"), 1)),
            ),
            now(),
        );
        assert_eq!(deny.kind, KernelEventKind::Error);
        assert_eq!(deny.error, Some(KernelErrorCode::NotFound));
        assert_ne!(deny.kind, KernelEventKind::Denied);

        let malformed = adapter.handle(
            approval_cmd(
                "ap-no-rcpt-empty",
                KernelCommandKind::DecideApproval,
                None,
                Some("run-empty-root"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest_hex("ab"), 1)),
            ),
            now(),
        );
        assert_eq!(malformed.kind, KernelEventKind::Error);
        assert_eq!(malformed.error, Some(KernelErrorCode::InvalidCommand));
    }

    #[test]
    fn malformed_approve_deny_is_invalid_even_without_store() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let adapter = LocalKernelAdapter::new(principal());

        let deny = adapter.handle(
            approval_cmd(
                "dn-nobind-empty",
                KernelCommandKind::DenyApproval,
                Some("rcpt-empty-bind"),
                Some("run-empty-bind"),
                None,
                Some(&root_s),
                None,
            ),
            now(),
        );
        assert_eq!(deny.kind, KernelEventKind::Error);
        assert_eq!(deny.error, Some(KernelErrorCode::InvalidCommand));
        assert_ne!(deny.error, Some(KernelErrorCode::NotFound));

        let grant_no_bind = adapter.handle(
            approval_cmd(
                "ap-nobind-empty",
                KernelCommandKind::DecideApproval,
                Some("rcpt-empty-bind"),
                Some("run-empty-bind"),
                Some(true),
                Some(&root_s),
                None,
            ),
            now(),
        );
        assert_eq!(grant_no_bind.kind, KernelEventKind::Error);
        assert_eq!(grant_no_bind.error, Some(KernelErrorCode::InvalidCommand));
        assert_ne!(grant_no_bind.error, Some(KernelErrorCode::NotFound));

        let grant_no_run = adapter.handle(
            approval_cmd(
                "ap-norun-empty",
                KernelCommandKind::DecideApproval,
                Some("rcpt-empty-bind"),
                None,
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest_hex("ab"), 1)),
            ),
            now(),
        );
        assert_eq!(grant_no_run.kind, KernelEventKind::Error);
        assert_eq!(grant_no_run.error, Some(KernelErrorCode::InvalidCommand));
        assert_ne!(grant_no_run.error, Some(KernelErrorCode::NotFound));
    }

    struct FixedStoreResolver {
        store: KernelStore,
    }

    impl KernelStoreResolver for FixedStoreResolver {
        fn resolve(
            &self,
            _owner: &str,
            _receipt_id: &str,
            _dogfood_root: Option<&str>,
        ) -> Option<KernelStore> {
            Some(self.store.clone())
        }
    }

    struct EmptyStoreResolver;

    impl KernelStoreResolver for EmptyStoreResolver {
        fn resolve(
            &self,
            _owner: &str,
            _receipt_id: &str,
            _dogfood_root: Option<&str>,
        ) -> Option<KernelStore> {
            None
        }
    }

    #[test]
    fn injected_store_resolver_grants_without_filesystem_probe() {
        let root = tempfile::tempdir().unwrap();
        let (store, digest) = seed_pending_ask(root.path(), "rcpt-inject", "run-inject", 1);
        let adapter = LocalKernelAdapter::with_store_resolver(
            principal(),
            Arc::new(FixedStoreResolver {
                store: store.clone(),
            }),
        );
        let granted = adapter.handle(
            approval_cmd(
                "ap-inject",
                KernelCommandKind::DecideApproval,
                Some("rcpt-inject"),
                Some("run-inject"),
                Some(true),
                None,
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(granted.kind, KernelEventKind::Approved, "{granted:?}");
        let row = store
            .get_approval(&principal().owner, "rcpt-inject")
            .unwrap()
            .expect("injected store must receive the grant");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Approved
        );
    }

    #[test]
    fn injected_store_resolver_is_exclusive_of_filesystem_probe() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) = seed_pending_ask(root.path(), "rcpt-reject", "run-reject", 1);
        let adapter =
            LocalKernelAdapter::with_store_resolver(principal(), Arc::new(EmptyStoreResolver));
        adapter.register_durable_root(root.path());
        let granted = adapter.handle(
            approval_cmd(
                "ap-reject",
                KernelCommandKind::DecideApproval,
                Some("rcpt-reject"),
                Some("run-reject"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(granted.kind, KernelEventKind::Error, "{granted:?}");
        assert_eq!(granted.error, Some(KernelErrorCode::NotFound));
        let row = store
            .get_approval(&principal().owner, "rcpt-reject")
            .unwrap()
            .expect("pending ask must stay pending");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
    }

    #[test]
    fn map_store_error_maps_every_kernel_store_error_arm() {
        let cases = [
            (KernelStoreError::NotFound, KernelErrorCode::NotFound),
            (
                KernelStoreError::OwnerMismatch,
                KernelErrorCode::OwnerMismatch,
            ),
            (
                KernelStoreError::ApprovalDeniedFinal,
                KernelErrorCode::Conflict,
            ),
            (
                KernelStoreError::ApprovalBindingMismatch,
                KernelErrorCode::Conflict,
            ),
            (
                KernelStoreError::Conflict("cas".into()),
                KernelErrorCode::Conflict,
            ),
            (
                KernelStoreError::StaleRevision {
                    expected: 2,
                    actual: 1,
                },
                KernelErrorCode::Conflict,
            ),
            (
                KernelStoreError::TerminalImmutable,
                KernelErrorCode::Conflict,
            ),
            (
                KernelStoreError::UnsupportedSchema {
                    found: 9,
                    supported: 1,
                },
                KernelErrorCode::UnsupportedVersion,
            ),
            (
                KernelStoreError::InvalidInput("bad field"),
                KernelErrorCode::InvalidCommand,
            ),
            (
                KernelStoreError::Io("disk".into()),
                KernelErrorCode::Unavailable,
            ),
            (
                KernelStoreError::Sqlite("busy".into()),
                KernelErrorCode::Unavailable,
            ),
            (
                KernelStoreError::Receipt("json".into()),
                KernelErrorCode::Unavailable,
            ),
            (
                KernelStoreError::Delivery("phase".into()),
                KernelErrorCode::Unavailable,
            ),
            (
                KernelStoreError::DuplicateEffect {
                    effect_id: "eff-1".into(),
                },
                KernelErrorCode::Unavailable,
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(
                LocalKernelAdapter::map_store_error(err.clone()),
                expected,
                "{err:?} must map to {expected:?}"
            );
        }
    }

    #[test]
    fn decide_approval_persists_grant_visible_to_fresh_adapter() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) = seed_pending_ask(root.path(), "rcpt-grant", "run-grant", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let granted = adapter.handle(
            approval_cmd(
                "ap-grant",
                KernelCommandKind::DecideApproval,
                Some("rcpt-grant"),
                Some("run-grant"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(
            granted.error, None,
            "expected durable grant, got {granted:?}"
        );
        assert_eq!(granted.kind, KernelEventKind::Approved);

        let row = store
            .get_approval(&principal().owner, "rcpt-grant")
            .unwrap()
            .expect("durable approval row");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Approved
        );
        let (phase, _) = store
            .get_delivery_phase(&principal().owner, "rcpt-grant")
            .unwrap()
            .expect("delivery phase after grant");
        assert_eq!(phase, crate::approval_fsm::DeliveryPhase::Approved);

        let fresh = LocalKernelAdapter::new(principal());
        let status = fresh.handle(
            approval_cmd(
                "st-grant",
                KernelCommandKind::Status,
                Some("rcpt-grant"),
                None,
                None,
                Some(&root_s),
                None,
            ),
            now(),
        );
        assert_eq!(
            status.error, None,
            "Status must rebuild receipt after grant: {status:?}"
        );
        assert_eq!(status.kind, KernelEventKind::ReceiptUpdated);
        match status.projection.unwrap() {
            KernelProjection::Receipt { receipt } => {
                assert_eq!(receipt.receipt_id, "rcpt-grant");
                assert_eq!(receipt.final_status, ReceiptFinalStatus::Open);
            }
            other => panic!("expected receipt projection, got {other:?}"),
        }

        let first = store
            .apply_effect_once(
                &principal().owner,
                "eff-grant",
                &digest,
                Some("run-grant"),
                Some("rcpt-grant"),
                now(),
            )
            .unwrap();
        assert!(first, "first effect after grant must apply");
        let restarted = KernelStore::open(root.path().join("kernel.sqlite")).unwrap();
        let second = restarted
            .apply_effect_once(
                &principal().owner,
                "eff-grant",
                &digest,
                Some("run-grant"),
                Some("rcpt-grant"),
                now(),
            )
            .unwrap();
        assert!(!second, "crash-before-duplicate-effect must not re-apply");
    }

    #[test]
    fn deny_then_grant_via_boundary_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) = seed_pending_ask(root.path(), "rcpt-deny", "run-deny", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let denied = adapter.handle(
            approval_cmd(
                "dn-1",
                KernelCommandKind::DenyApproval,
                Some("rcpt-deny"),
                Some("run-deny"),
                None,
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(denied.error, None, "expected durable deny, got {denied:?}");
        assert_eq!(denied.kind, KernelEventKind::Denied);
        assert_eq!(
            denied.final_status, None,
            "deny must not invent receipt Failed"
        );
        match denied.projection.as_ref() {
            Some(KernelProjection::Ownership {
                lease_owner,
                generation,
                ..
            }) => {
                assert_eq!(
                    lease_owner.as_deref(),
                    Some(principal().principal_id.as_str())
                );
                assert_eq!(*generation, Some(1));
            }
            other => panic!("deny must project store ownership, got {other:?}"),
        }
        let row = store
            .get_approval(&principal().owner, "rcpt-deny")
            .unwrap()
            .expect("denied row");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Denied
        );
        let (phase, _) = store
            .get_delivery_phase(&principal().owner, "rcpt-deny")
            .unwrap()
            .expect("delivery phase after deny");
        assert_eq!(phase, crate::approval_fsm::DeliveryPhase::Denied);

        let granted = adapter.handle(
            approval_cmd(
                "ap-after-deny",
                KernelCommandKind::DecideApproval,
                Some("rcpt-deny"),
                Some("run-deny"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_ne!(granted.kind, KernelEventKind::Approved);
        assert_eq!(granted.kind, KernelEventKind::Error);
        assert_eq!(granted.error, Some(KernelErrorCode::Conflict));
        let row = store
            .get_approval(&principal().owner, "rcpt-deny")
            .unwrap()
            .expect("still denied");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Denied
        );
    }

    #[test]
    fn deny_via_boundary_leaves_receipt_open() {
        // Deny is an approval decision. The durable receipt stays Open; the
        // Denied event must not invent Failed. Assert store + Status +
        // ReadReceipt, not only the event's final_status field.
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) = seed_pending_ask(root.path(), "rcpt-open", "run-open", 1);
        let before = store
            .get_receipt(&principal().owner, "rcpt-open")
            .unwrap()
            .expect("seeded receipt");
        assert_eq!(before.final_status, ReceiptFinalStatus::Open);
        let adapter = LocalKernelAdapter::new(principal());
        let denied = adapter.handle(
            approval_cmd(
                "dn-open",
                KernelCommandKind::DenyApproval,
                Some("rcpt-open"),
                Some("run-open"),
                None,
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(denied.error, None, "expected durable deny, got {denied:?}");
        assert_eq!(denied.kind, KernelEventKind::Denied);
        let stored = store
            .get_receipt(&principal().owner, "rcpt-open")
            .unwrap()
            .expect("receipt after deny");
        assert_eq!(stored.final_status, ReceiptFinalStatus::Open);
        let fresh = LocalKernelAdapter::new(principal());
        for (command_id, kind) in [
            ("st-open", KernelCommandKind::Status),
            ("rr-open", KernelCommandKind::ReadReceipt),
        ] {
            let event = fresh.handle(
                approval_cmd(
                    command_id,
                    kind,
                    Some("rcpt-open"),
                    None,
                    None,
                    Some(&root_s),
                    None,
                ),
                now(),
            );
            assert_eq!(
                event.error, None,
                "{kind:?} must rebuild receipt after deny: {event:?}"
            );
            assert_eq!(event.kind, KernelEventKind::ReceiptUpdated);
            match event.projection.unwrap() {
                KernelProjection::Receipt { receipt } => {
                    assert_eq!(receipt.receipt_id, "rcpt-open");
                    assert_eq!(receipt.final_status, ReceiptFinalStatus::Open);
                }
                other => panic!("{kind:?}: expected receipt projection, got {other:?}"),
            }
        }
    }

    #[test]
    fn deny_ownership_projection_reads_store_lease_fence() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (_store, digest) =
            seed_pending_ask_with_lease(root.path(), "rcpt-own", "run-own", 1, "worker-a", 4);
        let adapter = LocalKernelAdapter::new(principal());
        let mut binding = binding_for(&digest, 1);
        binding.lease_owner = "worker-a".into();
        binding.generation = 4;
        let denied = adapter.handle(
            approval_cmd(
                "dn-own",
                KernelCommandKind::DenyApproval,
                Some("rcpt-own"),
                Some("run-own"),
                None,
                Some(&root_s),
                Some(binding),
            ),
            now(),
        );
        assert_eq!(denied.kind, KernelEventKind::Denied, "{denied:?}");
        match denied.projection.unwrap() {
            KernelProjection::Ownership {
                owner,
                lease_owner,
                generation,
            } => {
                assert_eq!(owner, principal().owner);
                assert_eq!(lease_owner.as_deref(), Some("worker-a"));
                assert_eq!(generation, Some(4));
            }
            other => panic!("expected ownership projection, got {other:?}"),
        }

        let (store2, digest2) = seed_pending_ask(root.path(), "rcpt-own-norun", "run-own-norun", 1);
        let no_run = adapter.handle(
            approval_cmd(
                "dn-own-norun",
                KernelCommandKind::DenyApproval,
                Some("rcpt-own-norun"),
                None,
                None,
                Some(&root_s),
                Some(binding_for(&digest2, 1)),
            ),
            now(),
        );
        assert_eq!(no_run.kind, KernelEventKind::Denied, "{no_run:?}");
        assert_eq!(no_run.agent_run_id.as_deref(), Some("run-own-norun"));
        match no_run.projection.unwrap() {
            KernelProjection::Ownership {
                lease_owner,
                generation,
                ..
            } => {
                assert_eq!(
                    lease_owner.as_deref(),
                    Some(principal().principal_id.as_str())
                );
                assert_eq!(generation, Some(1));
            }
            other => panic!("expected ownership from store run id, got {other:?}"),
        }
        let (phase, _) = store2
            .get_delivery_phase(&principal().owner, "rcpt-own-norun")
            .unwrap()
            .expect("delivery phase after deny without command run id");
        assert_eq!(phase, crate::approval_fsm::DeliveryPhase::Denied);
    }

    #[test]
    fn deny_ownership_projection_omits_lease_when_fence_missing() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) =
            seed_pending_ask_without_lease(root.path(), "rcpt-dnofence", "run-dnofence", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let denied = adapter.handle(
            approval_cmd(
                "dn-nofence",
                KernelCommandKind::DenyApproval,
                Some("rcpt-dnofence"),
                Some("run-dnofence"),
                None,
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(denied.kind, KernelEventKind::Denied, "{denied:?}");
        assert_eq!(denied.agent_run_id.as_deref(), Some("run-dnofence"));
        match denied.projection.unwrap() {
            KernelProjection::Ownership {
                owner,
                lease_owner,
                generation,
            } => {
                assert_eq!(owner, principal().owner);
                assert_eq!(lease_owner, None);
                assert_eq!(generation, None);
            }
            other => panic!("expected ownership without lease, got {other:?}"),
        }
        let row = store
            .get_approval(&principal().owner, "rcpt-dnofence")
            .unwrap()
            .expect("denied");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Denied
        );
        let (phase, _) = store
            .get_delivery_phase(&principal().owner, "rcpt-dnofence")
            .unwrap()
            .expect("delivery after deny");
        assert_eq!(phase, crate::approval_fsm::DeliveryPhase::Denied);
    }

    #[test]
    fn deny_ignores_conflicting_caller_run_id() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) = seed_pending_ask_with_lease(
            root.path(),
            "rcpt-deny-run",
            "run-durable",
            1,
            "worker-a",
            4,
        );
        store
            .put_lease_fence(
                &crate::kernel_store::LeaseFence {
                    owner: principal().owner,
                    run_id: "run-spoof".into(),
                    lease_owner: "worker-spoof".into(),
                    generation: 99,
                    revision: 1,
                    token: "tok-spoof".into(),
                    policy_digest: digest_hex("cd"),
                    expires_at_ms: None,
                },
                now(),
            )
            .unwrap();
        let adapter = LocalKernelAdapter::new(principal());
        let mut binding = binding_for(&digest, 1);
        binding.lease_owner = "worker-a".into();
        binding.generation = 4;
        let denied = adapter.handle(
            approval_cmd(
                "dn-spoof-run",
                KernelCommandKind::DenyApproval,
                Some("rcpt-deny-run"),
                Some("run-spoof"),
                None,
                Some(&root_s),
                Some(binding),
            ),
            now(),
        );
        assert_eq!(denied.kind, KernelEventKind::Denied, "{denied:?}");
        assert_ne!(denied.agent_run_id.as_deref(), Some("run-spoof"));
        assert_eq!(denied.agent_run_id.as_deref(), Some("run-durable"));
        match denied.projection.unwrap() {
            KernelProjection::Ownership {
                lease_owner,
                generation,
                ..
            } => {
                assert_eq!(lease_owner.as_deref(), Some("worker-a"));
                assert_eq!(generation, Some(4));
            }
            other => panic!("expected durable-run ownership, got {other:?}"),
        }
        let row = store
            .get_approval(&principal().owner, "rcpt-deny-run")
            .unwrap()
            .expect("denied");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Denied
        );
        assert_eq!(row.run_id, "run-durable");
        let (phase, _) = store
            .get_delivery_phase(&principal().owner, "rcpt-deny-run")
            .unwrap()
            .expect("delivery after deny");
        assert_eq!(phase, crate::approval_fsm::DeliveryPhase::Denied);
    }

    #[test]
    fn deny_revision_and_lease_fence_mismatch_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) =
            seed_pending_ask_with_lease(root.path(), "rcpt-dfence", "run-dfence", 1, "worker-a", 4);
        let adapter = LocalKernelAdapter::new(principal());

        let mut bad_gen = binding_for(&digest, 1);
        bad_gen.lease_owner = "worker-a".into();
        bad_gen.generation = 99;
        let gen_mismatch = adapter.handle(
            approval_cmd(
                "dn-bad-gen",
                KernelCommandKind::DenyApproval,
                Some("rcpt-dfence"),
                Some("run-dfence"),
                None,
                Some(&root_s),
                Some(bad_gen),
            ),
            now(),
        );
        assert_ne!(gen_mismatch.kind, KernelEventKind::Denied);
        assert_eq!(gen_mismatch.kind, KernelEventKind::Error);
        assert_eq!(gen_mismatch.error, Some(KernelErrorCode::Conflict));

        let mut bad_rev = binding_for(&digest, 99);
        bad_rev.lease_owner = "worker-a".into();
        bad_rev.generation = 4;
        let rev_mismatch = adapter.handle(
            approval_cmd(
                "dn-bad-rev",
                KernelCommandKind::DenyApproval,
                Some("rcpt-dfence"),
                Some("run-dfence"),
                None,
                Some(&root_s),
                Some(bad_rev),
            ),
            now(),
        );
        assert_ne!(rev_mismatch.kind, KernelEventKind::Denied);
        assert_eq!(rev_mismatch.kind, KernelEventKind::Error);
        assert_eq!(rev_mismatch.error, Some(KernelErrorCode::Conflict));

        let row = store
            .get_approval(&principal().owner, "rcpt-dfence")
            .unwrap()
            .expect("pending retained");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
    }

    #[test]
    fn lease_owner_mismatch_fails_closed_for_grant_and_deny() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let adapter = LocalKernelAdapter::new(principal());

        let (grant_store, grant_digest) =
            seed_pending_ask_with_lease(root.path(), "rcpt-lo-g", "run-lo-g", 1, "worker-a", 4);
        let mut grant_binding = binding_for(&grant_digest, 1);
        grant_binding.lease_owner = "worker-b".into();
        grant_binding.generation = 4;
        let grant_mismatch = adapter.handle(
            approval_cmd(
                "ap-bad-lease",
                KernelCommandKind::DecideApproval,
                Some("rcpt-lo-g"),
                Some("run-lo-g"),
                Some(true),
                Some(&root_s),
                Some(grant_binding),
            ),
            now(),
        );
        assert_ne!(grant_mismatch.kind, KernelEventKind::Approved);
        assert_eq!(grant_mismatch.kind, KernelEventKind::Error);
        assert_eq!(grant_mismatch.error, Some(KernelErrorCode::Conflict));
        let grant_row = grant_store
            .get_approval(&principal().owner, "rcpt-lo-g")
            .unwrap()
            .expect("grant ask retained");
        assert_eq!(
            grant_row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
        let (grant_phase, _) = grant_store
            .get_delivery_phase(&principal().owner, "rcpt-lo-g")
            .unwrap()
            .expect("grant delivery retained");
        assert_eq!(
            grant_phase,
            crate::approval_fsm::DeliveryPhase::ApprovalRequired
        );

        let (deny_store, deny_digest) =
            seed_pending_ask_with_lease(root.path(), "rcpt-lo-d", "run-lo-d", 1, "worker-a", 4);
        let mut deny_binding = binding_for(&deny_digest, 1);
        deny_binding.lease_owner = "worker-b".into();
        deny_binding.generation = 4;
        let deny_mismatch = adapter.handle(
            approval_cmd(
                "dn-bad-lease",
                KernelCommandKind::DenyApproval,
                Some("rcpt-lo-d"),
                Some("run-lo-d"),
                None,
                Some(&root_s),
                Some(deny_binding),
            ),
            now(),
        );
        assert_ne!(deny_mismatch.kind, KernelEventKind::Denied);
        assert_eq!(deny_mismatch.kind, KernelEventKind::Error);
        assert_eq!(deny_mismatch.error, Some(KernelErrorCode::Conflict));
        let deny_row = deny_store
            .get_approval(&principal().owner, "rcpt-lo-d")
            .unwrap()
            .expect("deny ask retained");
        assert_eq!(
            deny_row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
        let (deny_phase, _) = deny_store
            .get_delivery_phase(&principal().owner, "rcpt-lo-d")
            .unwrap()
            .expect("deny delivery retained");
        assert_eq!(
            deny_phase,
            crate::approval_fsm::DeliveryPhase::ApprovalRequired
        );
    }

    #[test]
    fn wrong_approval_binding_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) = seed_pending_ask(root.path(), "rcpt-bind", "run-bind", 3);
        let adapter = LocalKernelAdapter::new(principal());
        let event = adapter.handle(
            approval_cmd(
                "ap-bad-rev",
                KernelCommandKind::DecideApproval,
                Some("rcpt-bind"),
                Some("run-bind"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 99)),
            ),
            now(),
        );
        assert_ne!(event.kind, KernelEventKind::Approved);
        assert_eq!(event.kind, KernelEventKind::Error);
        assert_eq!(event.error, Some(KernelErrorCode::Conflict));
        let row = store
            .get_approval(&principal().owner, "rcpt-bind")
            .unwrap()
            .expect("pending retained");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
    }

    #[test]
    fn decide_approval_prefers_store_with_approval_row() {
        let root = tempfile::tempdir().unwrap();
        let decoy = root.path().join("durable-decoy");
        std::fs::create_dir_all(&decoy).unwrap();
        let _decoy_store = KernelStore::open(decoy.join("kernel.sqlite")).unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) = seed_pending_ask(root.path(), "rcpt-pick", "run-pick", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let granted = adapter.handle(
            approval_cmd(
                "ap-pick",
                KernelCommandKind::DecideApproval,
                Some("rcpt-pick"),
                Some("run-pick"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(
            granted.error, None,
            "expected grant against pending row, got {granted:?}"
        );
        assert_eq!(granted.kind, KernelEventKind::Approved);
        let row = store
            .get_approval(&principal().owner, "rcpt-pick")
            .unwrap()
            .expect("approval remains on the receipt-bearing store");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Approved
        );
        let decoy_row = KernelStore::open(decoy.join("kernel.sqlite"))
            .unwrap()
            .get_approval(&principal().owner, "rcpt-pick")
            .unwrap();
        assert!(
            decoy_row.is_none(),
            "empty sibling sqlite must not receive the grant"
        );
    }

    #[test]
    fn decide_approval_missing_fields_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) = seed_pending_ask(root.path(), "rcpt-miss", "run-miss", 1);
        let adapter = LocalKernelAdapter::new(principal());

        let no_receipt = adapter.handle(
            approval_cmd(
                "ap-no-rcpt",
                KernelCommandKind::DecideApproval,
                None,
                Some("run-miss"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(no_receipt.kind, KernelEventKind::Error);
        assert_eq!(no_receipt.error, Some(KernelErrorCode::InvalidCommand));

        let no_run = adapter.handle(
            approval_cmd(
                "ap-no-run",
                KernelCommandKind::DecideApproval,
                Some("rcpt-miss"),
                None,
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(no_run.kind, KernelEventKind::Error);
        assert_eq!(no_run.error, Some(KernelErrorCode::InvalidCommand));

        let no_binding = adapter.handle(
            approval_cmd(
                "ap-no-bind",
                KernelCommandKind::DecideApproval,
                Some("rcpt-miss"),
                Some("run-miss"),
                Some(true),
                Some(&root_s),
                None,
            ),
            now(),
        );
        assert_eq!(no_binding.kind, KernelEventKind::Error);
        assert_eq!(no_binding.error, Some(KernelErrorCode::InvalidCommand));

        let row = store
            .get_approval(&principal().owner, "rcpt-miss")
            .unwrap()
            .expect("pending retained");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
    }

    #[test]
    fn decide_approval_mismatch_and_missing_ask_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) = seed_pending_ask(root.path(), "rcpt-mis", "run-mis", 1);
        let adapter = LocalKernelAdapter::new(principal());

        let bad_digest = adapter.handle(
            approval_cmd(
                "ap-digest",
                KernelCommandKind::DecideApproval,
                Some("rcpt-mis"),
                Some("run-mis"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest_hex("cd"), 1)),
            ),
            now(),
        );
        assert_eq!(bad_digest.kind, KernelEventKind::Error);
        assert_eq!(bad_digest.error, Some(KernelErrorCode::NotFound));

        let bad_run = adapter.handle(
            approval_cmd(
                "ap-run",
                KernelCommandKind::DecideApproval,
                Some("rcpt-mis"),
                Some("run-other"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(bad_run.kind, KernelEventKind::Error);
        assert_eq!(bad_run.error, Some(KernelErrorCode::Conflict));

        let mut bad_gen = binding_for(&digest, 1);
        bad_gen.generation = 9;
        let bad_generation = adapter.handle(
            approval_cmd(
                "ap-gen",
                KernelCommandKind::DecideApproval,
                Some("rcpt-mis"),
                Some("run-mis"),
                Some(true),
                Some(&root_s),
                Some(bad_gen),
            ),
            now(),
        );
        assert_eq!(bad_generation.kind, KernelEventKind::Error);
        assert_eq!(bad_generation.error, Some(KernelErrorCode::Conflict));

        let no_ask_id = "rcpt-noask";
        let receipt =
            CompletionReceipt::open(no_ask_id, principal().owner, task(), route(), now()).unwrap();
        store.insert_open_receipt(&receipt).unwrap();
        let no_ask = adapter.handle(
            approval_cmd(
                "ap-noask",
                KernelCommandKind::DecideApproval,
                Some(no_ask_id),
                Some("run-noask"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest_hex("ab"), 1)),
            ),
            now(),
        );
        assert_eq!(no_ask.kind, KernelEventKind::Error);
        assert_eq!(no_ask.error, Some(KernelErrorCode::NotFound));

        let row = store
            .get_approval(&principal().owner, "rcpt-mis")
            .unwrap()
            .expect("pending retained");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
    }

    #[test]
    fn decide_approval_regrant_is_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) = seed_pending_ask(root.path(), "rcpt-idemp", "run-idemp", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let first = adapter.handle(
            approval_cmd(
                "ap-idemp-1",
                KernelCommandKind::DecideApproval,
                Some("rcpt-idemp"),
                Some("run-idemp"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(first.kind, KernelEventKind::Approved);
        let second = adapter.handle(
            approval_cmd(
                "ap-idemp-2",
                KernelCommandKind::DecideApproval,
                Some("rcpt-idemp"),
                Some("run-idemp"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(second.kind, KernelEventKind::Approved);
        let row = store
            .get_approval(&principal().owner, "rcpt-idemp")
            .unwrap()
            .expect("approved row");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Approved
        );
    }

    #[test]
    fn decide_approval_not_granted_retains_pending() {
        // approval_granted None and Some(false) both keep a pending ask.
        // Binding is optional on this path.
        let cases: [(&str, &str, &str, Option<bool>); 2] = [
            (
                "rcpt-hold-false",
                "run-hold-false",
                "ap-hold-false",
                Some(false),
            ),
            ("rcpt-hold-none", "run-hold-none", "ap-hold-none", None),
        ];
        for (receipt, run_id, command_id, granted) in cases {
            let root = tempfile::tempdir().unwrap();
            let root_s = root.path().to_string_lossy().into_owned();
            let (store, digest) = seed_pending_ask(root.path(), receipt, run_id, 1);
            let adapter = LocalKernelAdapter::new(principal());
            let event = adapter.handle(
                approval_cmd(
                    command_id,
                    KernelCommandKind::DecideApproval,
                    Some(receipt),
                    Some(run_id),
                    granted,
                    Some(&root_s),
                    Some(binding_for(&digest, 1)),
                ),
                now(),
            );
            assert_eq!(
                event.kind,
                KernelEventKind::ApprovalRequired,
                "granted={granted:?}"
            );
            assert_eq!(
                event.error,
                Some(KernelErrorCode::ApprovalRequired),
                "granted={granted:?}"
            );
            let row = store
                .get_approval(&principal().owner, receipt)
                .unwrap()
                .expect("pending retained");
            assert_eq!(
                row.decision,
                crate::kernel_store::ApprovalDecisionKind::Pending,
                "granted={granted:?}"
            );
        }
    }

    #[test]
    fn decide_approval_not_granted_after_approve_is_conflict() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) = seed_pending_ask(root.path(), "rcpt-hold-ap", "run-hold-ap", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let granted = adapter.handle(
            approval_cmd(
                "ap-hold-ap",
                KernelCommandKind::DecideApproval,
                Some("rcpt-hold-ap"),
                Some("run-hold-ap"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(granted.kind, KernelEventKind::Approved, "{granted:?}");
        let held = adapter.handle(
            approval_cmd(
                "ap-hold-ap-false",
                KernelCommandKind::DecideApproval,
                Some("rcpt-hold-ap"),
                Some("run-hold-ap"),
                Some(false),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_ne!(held.kind, KernelEventKind::Approved);
        assert_eq!(held.kind, KernelEventKind::Error);
        assert_eq!(held.error, Some(KernelErrorCode::Conflict));
        let row = store
            .get_approval(&principal().owner, "rcpt-hold-ap")
            .unwrap()
            .expect("still approved");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Approved
        );
        let (phase, _) = store
            .get_delivery_phase(&principal().owner, "rcpt-hold-ap")
            .unwrap()
            .expect("delivery retained");
        assert_eq!(phase, crate::approval_fsm::DeliveryPhase::Approved);
    }

    #[test]
    fn decide_approval_not_granted_after_deny_is_conflict() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) = seed_pending_ask(root.path(), "rcpt-hold-dn", "run-hold-dn", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let denied = adapter.handle(
            approval_cmd(
                "dn-hold-dn",
                KernelCommandKind::DenyApproval,
                Some("rcpt-hold-dn"),
                Some("run-hold-dn"),
                None,
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(denied.kind, KernelEventKind::Denied, "{denied:?}");
        let held = adapter.handle(
            approval_cmd(
                "ap-hold-dn-false",
                KernelCommandKind::DecideApproval,
                Some("rcpt-hold-dn"),
                Some("run-hold-dn"),
                Some(false),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_ne!(held.kind, KernelEventKind::Denied);
        assert_ne!(held.kind, KernelEventKind::Approved);
        assert_eq!(held.kind, KernelEventKind::Error);
        assert_eq!(held.error, Some(KernelErrorCode::Conflict));
        let row = store
            .get_approval(&principal().owner, "rcpt-hold-dn")
            .unwrap()
            .expect("still denied");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Denied
        );
        let (phase, _) = store
            .get_delivery_phase(&principal().owner, "rcpt-hold-dn")
            .unwrap()
            .expect("delivery retained");
        assert_eq!(phase, crate::approval_fsm::DeliveryPhase::Denied);
    }

    #[test]
    fn deny_without_binding_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, _digest) = seed_pending_ask(root.path(), "rcpt-nobind", "run-nobind", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let denied = adapter.handle(
            approval_cmd(
                "dn-nobind",
                KernelCommandKind::DenyApproval,
                Some("rcpt-nobind"),
                Some("run-nobind"),
                None,
                Some(&root_s),
                None,
            ),
            now(),
        );
        assert_ne!(denied.kind, KernelEventKind::Denied);
        assert_eq!(denied.kind, KernelEventKind::Error);
        assert_eq!(denied.error, Some(KernelErrorCode::InvalidCommand));
        let row = store
            .get_approval(&principal().owner, "rcpt-nobind")
            .unwrap()
            .expect("pending retained");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
    }

    #[test]
    fn deny_digest_mismatch_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, _digest) = seed_pending_ask(root.path(), "rcpt-baddig", "run-baddig", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let denied = adapter.handle(
            approval_cmd(
                "dn-baddig",
                KernelCommandKind::DenyApproval,
                Some("rcpt-baddig"),
                Some("run-baddig"),
                None,
                Some(&root_s),
                Some(binding_for(&digest_hex("cd"), 1)),
            ),
            now(),
        );
        assert_ne!(denied.kind, KernelEventKind::Denied);
        assert_eq!(denied.kind, KernelEventKind::Error);
        assert_eq!(denied.error, Some(KernelErrorCode::NotFound));
        let row = store
            .get_approval(&principal().owner, "rcpt-baddig")
            .unwrap()
            .expect("pending retained");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
    }

    #[test]
    fn grant_ownership_projection_reads_store_lease_fence() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (_store, digest) =
            seed_pending_ask_with_lease(root.path(), "rcpt-gown", "run-gown", 1, "worker-a", 4);
        let adapter = LocalKernelAdapter::new(principal());
        let mut binding = binding_for(&digest, 1);
        binding.lease_owner = "worker-a".into();
        binding.generation = 4;
        let granted = adapter.handle(
            approval_cmd(
                "ap-gown",
                KernelCommandKind::DecideApproval,
                Some("rcpt-gown"),
                Some("run-gown"),
                Some(true),
                Some(&root_s),
                Some(binding),
            ),
            now(),
        );
        assert_eq!(granted.kind, KernelEventKind::Approved, "{granted:?}");
        match granted.projection.unwrap() {
            KernelProjection::Ownership {
                owner,
                lease_owner,
                generation,
            } => {
                assert_eq!(owner, principal().owner);
                assert_eq!(lease_owner.as_deref(), Some("worker-a"));
                assert_eq!(generation, Some(4));
            }
            other => panic!("expected ownership projection, got {other:?}"),
        }
    }

    #[test]
    fn grant_ownership_projection_omits_lease_when_fence_missing() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (_store, digest) =
            seed_pending_ask_without_lease(root.path(), "rcpt-nofence", "run-nofence", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let granted = adapter.handle(
            approval_cmd(
                "ap-nofence",
                KernelCommandKind::DecideApproval,
                Some("rcpt-nofence"),
                Some("run-nofence"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(granted.kind, KernelEventKind::Approved, "{granted:?}");
        match granted.projection.unwrap() {
            KernelProjection::Ownership {
                lease_owner,
                generation,
                ..
            } => {
                assert_eq!(lease_owner, None);
                assert_eq!(generation, None);
            }
            other => panic!("expected ownership without lease, got {other:?}"),
        }
    }

    #[test]
    fn decide_approval_named_dogfood_root_is_not_stolen_by_sorted_sibling() {
        let parent = tempfile::tempdir().unwrap();
        let early = parent.path().join("aaa");
        let named = parent.path().join("zzz");
        std::fs::create_dir_all(&early).unwrap();
        std::fs::create_dir_all(&named).unwrap();
        let (early_store, _early_digest) =
            seed_pending_ask(&early, "rcpt-collide", "run-collide", 1);
        let (named_store, named_digest) =
            seed_pending_ask(&named, "rcpt-collide", "run-collide", 1);
        let adapter = LocalKernelAdapter::new(principal());
        adapter.register_durable_root(&early);
        let named_s = named.to_string_lossy().into_owned();
        let granted = adapter.handle(
            approval_cmd(
                "ap-named-root",
                KernelCommandKind::DecideApproval,
                Some("rcpt-collide"),
                Some("run-collide"),
                Some(true),
                Some(&named_s),
                Some(binding_for(&named_digest, 1)),
            ),
            now(),
        );
        assert_eq!(granted.kind, KernelEventKind::Approved, "{granted:?}");
        let named_row = named_store
            .get_approval(&principal().owner, "rcpt-collide")
            .unwrap()
            .expect("named store must receive the grant");
        assert_eq!(
            named_row.decision,
            crate::kernel_store::ApprovalDecisionKind::Approved
        );
        let early_row = early_store
            .get_approval(&principal().owner, "rcpt-collide")
            .unwrap()
            .expect("early sibling must stay pending");
        assert_eq!(
            early_row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
    }

    #[test]
    fn deny_after_approve_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) = seed_pending_ask(root.path(), "rcpt-post", "run-post", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let granted = adapter.handle(
            approval_cmd(
                "ap-post",
                KernelCommandKind::DecideApproval,
                Some("rcpt-post"),
                Some("run-post"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(granted.kind, KernelEventKind::Approved);
        let denied = adapter.handle(
            approval_cmd(
                "dn-post",
                KernelCommandKind::DenyApproval,
                Some("rcpt-post"),
                Some("run-post"),
                None,
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_ne!(denied.kind, KernelEventKind::Denied);
        assert_eq!(denied.kind, KernelEventKind::Error);
        assert_eq!(denied.error, Some(KernelErrorCode::Conflict));
        let row = store
            .get_approval(&principal().owner, "rcpt-post")
            .unwrap()
            .expect("still approved");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Approved
        );
    }

    #[test]
    fn decide_approval_does_not_create_missing_candidate_sqlite() {
        let root = tempfile::tempdir().unwrap();
        let ghost = root.path().join("durable-ghost");
        std::fs::create_dir_all(&ghost).unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (_store, digest) = seed_pending_ask(root.path(), "rcpt-ghost", "run-ghost", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let granted = adapter.handle(
            approval_cmd(
                "ap-ghost",
                KernelCommandKind::DecideApproval,
                Some("rcpt-ghost"),
                Some("run-ghost"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(granted.kind, KernelEventKind::Approved, "{granted:?}");
        assert!(
            !ghost.join("kernel.sqlite").exists(),
            "probe must not create missing kernel.sqlite candidates"
        );
    }

    #[test]
    fn decide_approval_indexed_store_skips_later_unbounded_scan() {
        let parent = tempfile::tempdir().unwrap();
        let decoy = parent.path().join("decoy");
        let named = parent.path().join("named");
        std::fs::create_dir_all(&decoy).unwrap();
        std::fs::create_dir_all(&named).unwrap();
        let (named_store, digest) = seed_pending_ask(&named, "rcpt-idx", "run-idx", 1);
        let adapter = LocalKernelAdapter::new(principal());
        adapter.register_durable_root(&decoy);
        let named_s = named.to_string_lossy().into_owned();
        let first = adapter.handle(
            approval_cmd(
                "ap-idx-1",
                KernelCommandKind::DecideApproval,
                Some("rcpt-idx"),
                Some("run-idx"),
                Some(true),
                Some(&named_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(first.kind, KernelEventKind::Approved, "{first:?}");

        let (decoy_store, _decoy_digest) = seed_pending_ask(&decoy, "rcpt-idx", "run-idx", 1);
        let second = adapter.handle(
            approval_cmd(
                "ap-idx-2",
                KernelCommandKind::DecideApproval,
                Some("rcpt-idx"),
                Some("run-idx"),
                Some(true),
                None,
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(second.kind, KernelEventKind::Approved, "{second:?}");
        let named_row = named_store
            .get_approval(&principal().owner, "rcpt-idx")
            .unwrap()
            .expect("named store keeps the grant");
        assert_eq!(
            named_row.decision,
            crate::kernel_store::ApprovalDecisionKind::Approved
        );
        let decoy_row = decoy_store
            .get_approval(&principal().owner, "rcpt-idx")
            .unwrap()
            .expect("decoy must stay pending");
        assert_eq!(
            decoy_row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
    }

    fn seed_pending_ask_without_delivery(
        root: &std::path::Path,
        receipt_id: &str,
        run_id: &str,
        revision: u64,
    ) -> (KernelStore, String) {
        let store = KernelStore::open(root.join("kernel.sqlite")).unwrap();
        let receipt =
            CompletionReceipt::open(receipt_id, principal().owner, task(), route(), now()).unwrap();
        store.insert_open_receipt(&receipt).unwrap();
        let digest = digest_hex("ab");
        store
            .record_approval_required(
                &principal().owner,
                &format!("appr-{receipt_id}"),
                receipt_id,
                run_id,
                &digest,
                revision,
                now(),
            )
            .unwrap();
        (store, digest)
    }

    #[test]
    fn deny_without_delivery_phase_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) =
            seed_pending_ask_without_delivery(root.path(), "rcpt-nophase", "run-nophase", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let denied = adapter.handle(
            approval_cmd(
                "dn-nophase",
                KernelCommandKind::DenyApproval,
                Some("rcpt-nophase"),
                Some("run-nophase"),
                None,
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(denied.kind, KernelEventKind::Error, "{denied:?}");
        assert_eq!(denied.error, Some(KernelErrorCode::NotFound));
        let row = store
            .get_approval(&principal().owner, "rcpt-nophase")
            .unwrap()
            .expect("ask retained");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
        assert!(
            store
                .get_delivery_phase(&principal().owner, "rcpt-nophase")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn grant_without_delivery_phase_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) =
            seed_pending_ask_without_delivery(root.path(), "rcpt-nogrant", "run-nogrant", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let granted = adapter.handle(
            approval_cmd(
                "ap-nogrant",
                KernelCommandKind::DecideApproval,
                Some("rcpt-nogrant"),
                Some("run-nogrant"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(granted.kind, KernelEventKind::Error, "{granted:?}");
        assert_eq!(granted.error, Some(KernelErrorCode::NotFound));
        let row = store
            .get_approval(&principal().owner, "rcpt-nogrant")
            .unwrap()
            .expect("ask retained");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
        assert!(
            store
                .get_delivery_phase(&principal().owner, "rcpt-nogrant")
                .unwrap()
                .is_none()
        );
    }

    fn seed_pending_ask_with_running_delivery(
        root: &std::path::Path,
        receipt_id: &str,
        run_id: &str,
        revision: u64,
    ) -> (KernelStore, String) {
        let (store, digest) = seed_pending_ask_without_delivery(root, receipt_id, run_id, revision);
        store
            .ensure_delivery_running(&principal().owner, receipt_id, run_id, now())
            .unwrap();
        (store, digest)
    }

    #[test]
    fn grant_while_delivery_still_running_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) =
            seed_pending_ask_with_running_delivery(root.path(), "rcpt-running", "run-running", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let granted = adapter.handle(
            approval_cmd(
                "ap-running",
                KernelCommandKind::DecideApproval,
                Some("rcpt-running"),
                Some("run-running"),
                Some(true),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(granted.kind, KernelEventKind::Error, "{granted:?}");
        assert_eq!(granted.error, Some(KernelErrorCode::Unavailable));
        let row = store
            .get_approval(&principal().owner, "rcpt-running")
            .unwrap()
            .expect("ask retained");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
        let (phase, _) = store
            .get_delivery_phase(&principal().owner, "rcpt-running")
            .unwrap()
            .expect("delivery retained");
        assert_eq!(phase, crate::approval_fsm::DeliveryPhase::Running);
    }

    fn read_cmd(
        command_id: &str,
        kind: KernelCommandKind,
        receipt_id: &str,
        root: Option<&str>,
    ) -> KernelCommand {
        KernelCommand {
            boundary_version: KERNEL_BOUNDARY_VERSION,
            command_id: command_id.into(),
            kind,
            principal: principal(),
            task_case: None,
            route: None,
            receipt_id: Some(receipt_id.into()),
            agent_run_id: None,
            approval_granted: None,
            subscribe: None,
            replay_from: None,
            dogfood_root: root.map(str::to_string),
            approval_binding: None,
        }
    }

    fn submit_cmd(command_id: &str, receipt_id: &str, root: Option<&str>) -> KernelCommand {
        KernelCommand {
            boundary_version: KERNEL_BOUNDARY_VERSION,
            command_id: command_id.into(),
            kind: KernelCommandKind::SubmitTask,
            principal: principal(),
            task_case: Some(task()),
            route: Some(route()),
            receipt_id: Some(receipt_id.into()),
            agent_run_id: None,
            approval_granted: None,
            subscribe: None,
            replay_from: None,
            dogfood_root: root.map(str::to_string),
            approval_binding: None,
        }
    }

    fn explicit_read_kinds() -> [KernelCommandKind; 4] {
        [
            KernelCommandKind::Status,
            KernelCommandKind::ReadReceipt,
            KernelCommandKind::GetProjection,
            KernelCommandKind::ReadArtifact,
        ]
    }

    /// Distinguish two same-id rows by task id, revision, status, and digest.
    fn stamp_receipt(
        store: &KernelStore,
        owner: &str,
        receipt_id: &str,
        task_case_id: &str,
        status: ReceiptFinalStatus,
        digest: &str,
    ) -> CompletionReceipt {
        store
            .transition_receipt(owner, receipt_id, 1, now(), |receipt| {
                receipt.task_case.task_case_id = task_case_id.to_string();
                receipt.final_status = status;
                receipt.validation_summary = Some(task_case_id.to_string());
                receipt.output_artifact_digest = Some(digest.to_string());
                Ok(())
            })
            .unwrap()
    }

    fn assert_selected_receipt(
        event: &KernelEvent,
        task_case_id: &str,
        revision: u64,
        status: ReceiptFinalStatus,
        digest: &str,
    ) {
        assert_eq!(event.error, None, "{event:?}");
        let Some(KernelProjection::Receipt { receipt }) = &event.projection else {
            panic!("expected receipt projection, got {event:?}");
        };
        assert_eq!(receipt.task_case.task_case_id, task_case_id, "{receipt:?}");
        assert_eq!(receipt.revision, revision, "{receipt:?}");
        assert_eq!(receipt.final_status, status, "{receipt:?}");
        assert_eq!(
            receipt.output_artifact_digest.as_deref(),
            Some(digest),
            "{receipt:?}"
        );
    }

    fn assert_read_not_found(event: &KernelEvent) {
        assert_eq!(event.kind, KernelEventKind::Error, "{event:?}");
        assert_eq!(event.error, Some(KernelErrorCode::NotFound), "{event:?}");
        assert!(
            !matches!(event.projection, Some(KernelProjection::Receipt { .. })),
            "missing root must not project a receipt: {event:?}"
        );
    }

    #[test]
    fn root_resolution_explicit_root_reads_match_approval_store() {
        let parent = tempfile::tempdir().unwrap();
        let registered = parent.path().join("aaa-registered");
        let named = parent.path().join("zzz-named");
        fs::create_dir_all(&registered).unwrap();
        fs::create_dir_all(&named).unwrap();
        let receipt_id = "rcpt-same";
        let (registered_store, _) = seed_pending_ask(&registered, receipt_id, "run-same", 1);
        let (named_store, named_digest) = seed_pending_ask(&named, receipt_id, "run-same", 1);
        let artifact = digest_hex("ef");
        let named_receipt = stamp_receipt(
            &named_store,
            &principal().owner,
            receipt_id,
            "task-named",
            ReceiptFinalStatus::Failed,
            &artifact,
        );
        assert_eq!(named_receipt.task_case.task_case_id, "task-named");
        assert_eq!(named_receipt.revision, 2);
        assert_eq!(named_receipt.final_status, ReceiptFinalStatus::Failed);
        assert_eq!(
            named_receipt.output_artifact_digest.as_deref(),
            Some(artifact.as_str())
        );
        let registered_receipt = registered_store
            .get_receipt(&principal().owner, receipt_id)
            .unwrap()
            .expect("registered receipt");
        assert_eq!(registered_receipt.task_case.task_case_id, "tc-1");
        assert_eq!(registered_receipt.revision, 1);
        assert_eq!(registered_receipt.final_status, ReceiptFinalStatus::Open);
        assert!(registered_receipt.output_artifact_digest.is_none());

        let adapter = LocalKernelAdapter::new(principal());
        adapter.register_durable_root(&registered);
        let named_s = named.to_string_lossy().into_owned();
        let granted = adapter.handle(
            approval_cmd(
                "ap-root-resolution",
                KernelCommandKind::DecideApproval,
                Some(receipt_id),
                Some("run-same"),
                Some(true),
                Some(&named_s),
                Some(binding_for(&named_digest, 1)),
            ),
            now(),
        );
        assert_eq!(granted.kind, KernelEventKind::Approved, "{granted:?}");
        assert_eq!(
            named_store
                .get_approval(&principal().owner, receipt_id)
                .unwrap()
                .expect("named approval")
                .decision,
            crate::kernel_store::ApprovalDecisionKind::Approved
        );
        assert_eq!(
            registered_store
                .get_approval(&principal().owner, receipt_id)
                .unwrap()
                .expect("registered approval")
                .decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );

        for kind in explicit_read_kinds() {
            let event = adapter.handle(
                read_cmd("rd-named", kind, receipt_id, Some(&named_s)),
                now(),
            );
            assert_selected_receipt(
                &event,
                "task-named",
                2,
                ReceiptFinalStatus::Failed,
                &artifact,
            );
        }
    }

    #[test]
    fn root_resolution_missing_explicit_root_does_not_fall_back() {
        let parent = tempfile::tempdir().unwrap();
        let registered = parent.path().join("aaa-registered");
        let explicit = parent.path().join("zzz-missing");
        fs::create_dir_all(&registered).unwrap();
        fs::create_dir_all(&explicit).unwrap();
        let receipt_id = "rcpt-miss";
        let (registered_store, _) = seed_pending_ask(&registered, receipt_id, "run-miss", 1);
        let artifact = digest_hex("ab");
        stamp_receipt(
            &registered_store,
            &principal().owner,
            receipt_id,
            "task-registered",
            ReceiptFinalStatus::Failed,
            &artifact,
        );
        let adapter = LocalKernelAdapter::new(principal());
        adapter.register_durable_root(&registered);
        let explicit_s = explicit.to_string_lossy().into_owned();
        for kind in explicit_read_kinds() {
            let event = adapter.handle(
                read_cmd("rd-miss", kind, receipt_id, Some(&explicit_s)),
                now(),
            );
            assert_read_not_found(&event);
        }
        assert!(
            !explicit.join("kernel.sqlite").exists(),
            "missing explicit root must not materialize kernel.sqlite"
        );
        assert!(
            !explicit.join("durable-miss").join("kernel.sqlite").exists(),
            "missing explicit root must not materialize a suffix candidate"
        );
    }

    #[test]
    fn root_resolution_memory_receipt_cannot_steal_explicit_durable_read() {
        let named = tempfile::tempdir().unwrap();
        let receipt_id = "rcpt-durable";
        let (store, _) = seed_pending_ask(named.path(), receipt_id, "run-durable", 1);
        let artifact = digest_hex("cd");
        stamp_receipt(
            &store,
            &principal().owner,
            receipt_id,
            "task-durable",
            ReceiptFinalStatus::Failed,
            &artifact,
        );
        let named_s = named.path().to_string_lossy().into_owned();
        let adapter = LocalKernelAdapter::new(principal());
        let _submitted = adapter.handle(submit_cmd("sub-mem", receipt_id, None), now());
        for kind in explicit_read_kinds() {
            let event = adapter.handle(read_cmd("rd-mem", kind, receipt_id, Some(&named_s)), now());
            assert_selected_receipt(
                &event,
                "task-durable",
                2,
                ReceiptFinalStatus::Failed,
                &artifact,
            );
        }
    }

    #[test]
    fn root_resolution_memory_only_submit_without_root_is_supported() {
        let adapter = LocalKernelAdapter::new(principal());
        let submitted = adapter.handle(submit_cmd("sub-noroot", "rcpt-memonly", None), now());
        assert_eq!(
            submitted.kind,
            KernelEventKind::TaskAccepted,
            "{submitted:?}"
        );
        let Some(KernelProjection::Receipt { receipt: accepted }) = submitted.projection.as_ref()
        else {
            panic!("accepted submit must project the memory receipt: {submitted:?}");
        };
        assert!(
            accepted.output_artifact_digest.is_none(),
            "memory submit has no artifact digest: {accepted:?}"
        );
        for kind in [
            KernelCommandKind::Status,
            KernelCommandKind::ReadReceipt,
            KernelCommandKind::GetProjection,
        ] {
            let event = adapter.handle(read_cmd("rd-noroot", kind, "rcpt-memonly", None), now());
            assert_eq!(event.kind, KernelEventKind::ReceiptUpdated, "{event:?}");
            assert_eq!(event.error, None, "{event:?}");
            let Some(KernelProjection::Receipt { receipt }) = event.projection.as_ref() else {
                panic!("rootless read must project the memory receipt: {event:?}");
            };
            assert_eq!(
                receipt, accepted,
                "{kind:?} must return the accepted receipt"
            );
        }
        let artifact = adapter.handle(
            read_cmd(
                "rd-noroot-artifact",
                KernelCommandKind::ReadArtifact,
                "rcpt-memonly",
                None,
            ),
            now(),
        );
        assert_read_not_found(&artifact);
    }

    #[test]
    fn root_resolution_injected_fixed_resolver_serves_durable_reads() {
        let injected = tempfile::tempdir().unwrap();
        let decoy = tempfile::tempdir().unwrap();
        let receipt_id = "rcpt-inject-read";
        let (injected_store, _) = seed_pending_ask(injected.path(), receipt_id, "run-inject", 1);
        let (decoy_store, _) = seed_pending_ask(decoy.path(), receipt_id, "run-decoy", 1);
        let injected_digest = digest_hex("11");
        let decoy_digest = digest_hex("22");
        stamp_receipt(
            &injected_store,
            &principal().owner,
            receipt_id,
            "task-injected",
            ReceiptFinalStatus::Failed,
            &injected_digest,
        );
        stamp_receipt(
            &decoy_store,
            &principal().owner,
            receipt_id,
            "task-decoy",
            ReceiptFinalStatus::Cancelled,
            &decoy_digest,
        );
        let adapter = LocalKernelAdapter::with_store_resolver(
            principal(),
            Arc::new(FixedStoreResolver {
                store: injected_store.clone(),
            }),
        );
        let decoy_s = decoy.path().to_string_lossy().into_owned();
        for kind in explicit_read_kinds() {
            let event = adapter.handle(
                read_cmd("rd-fixed", kind, receipt_id, Some(&decoy_s)),
                now(),
            );
            assert_selected_receipt(
                &event,
                "task-injected",
                2,
                ReceiptFinalStatus::Failed,
                &injected_digest,
            );
        }
    }

    #[test]
    fn root_resolution_empty_resolver_does_not_read_filesystem() {
        let decoy = tempfile::tempdir().unwrap();
        let receipt_id = "rcpt-empty-resolver";
        let (decoy_store, _) = seed_pending_ask(decoy.path(), receipt_id, "run-empty", 1);
        let decoy_digest = digest_hex("33");
        stamp_receipt(
            &decoy_store,
            &principal().owner,
            receipt_id,
            "task-filesystem",
            ReceiptFinalStatus::Failed,
            &decoy_digest,
        );
        let adapter =
            LocalKernelAdapter::with_store_resolver(principal(), Arc::new(EmptyStoreResolver));
        adapter.register_durable_root(decoy.path());
        let decoy_s = decoy.path().to_string_lossy().into_owned();
        for kind in explicit_read_kinds() {
            let event = adapter.handle(
                read_cmd("rd-empty", kind, receipt_id, Some(&decoy_s)),
                now(),
            );
            assert_read_not_found(&event);
        }
    }

    #[test]
    fn root_resolution_missing_storage_does_not_create_or_initialize_sqlite() {
        let parent = tempfile::tempdir().unwrap();
        let explicit = parent.path().join("explicit");
        let registered = parent.path().join("registered");
        fs::create_dir_all(&explicit).unwrap();
        fs::create_dir_all(&registered).unwrap();
        let missing_sqlite = explicit.join("kernel.sqlite");
        let explicit_empty = explicit.join("durable-gap").join("kernel.sqlite");
        fs::create_dir_all(explicit_empty.parent().unwrap()).unwrap();
        fs::write(&explicit_empty, b"").unwrap();
        let registered_empty = registered.join("kernel.sqlite");
        fs::write(&registered_empty, b"").unwrap();
        let adapter = LocalKernelAdapter::new(principal());
        adapter.register_durable_root(&registered);
        let explicit_s = explicit.to_string_lossy().into_owned();
        let event = adapter.handle(
            read_cmd(
                "rd-gap",
                KernelCommandKind::Status,
                "rcpt-gap",
                Some(&explicit_s),
            ),
            now(),
        );
        assert_read_not_found(&event);
        assert!(
            !missing_sqlite.exists(),
            "status read must not create kernel.sqlite"
        );
        assert!(
            !registered
                .join("durable-gap")
                .join("kernel.sqlite")
                .exists(),
            "status read must not create a registered suffix candidate"
        );
        let explicit_len = fs::metadata(&explicit_empty).unwrap().len();
        let registered_len = fs::metadata(&registered_empty).unwrap().len();
        assert_eq!(
            (explicit_len, registered_len),
            (0, 0),
            "unrelated empty sqlite files were initialized"
        );
    }

    #[test]
    fn root_resolution_wrong_owner_unauthorized_and_foreign_not_leaked() {
        let root = tempfile::tempdir().unwrap();
        let store = KernelStore::open(root.path().join("kernel.sqlite")).unwrap();
        let receipt_id = "rcpt-owner";
        let foreign_owner = "other-owner";
        let mut foreign_task = task();
        foreign_task.task_case_id = "task-foreign".into();
        let foreign =
            CompletionReceipt::open(receipt_id, foreign_owner, foreign_task, route(), now())
                .unwrap();
        store.insert_open_receipt(&foreign).unwrap();
        let foreign_digest = digest_hex("ff");
        let foreign_receipt = stamp_receipt(
            &store,
            foreign_owner,
            receipt_id,
            "task-foreign",
            ReceiptFinalStatus::Failed,
            &foreign_digest,
        );
        assert_eq!(foreign_receipt.revision, 2);
        let local =
            CompletionReceipt::open(receipt_id, principal().owner, task(), route(), now()).unwrap();
        store.insert_open_receipt(&local).unwrap();

        let adapter = LocalKernelAdapter::new(principal());
        let root_s = root.path().to_string_lossy().into_owned();
        let mut wrong_owner = read_cmd(
            "rd-wrong-owner",
            KernelCommandKind::ReadReceipt,
            receipt_id,
            Some(&root_s),
        );
        wrong_owner.principal = KernelPrincipal {
            principal_id: principal().principal_id,
            owner: foreign_owner.into(),
        };
        let denied = adapter.handle(wrong_owner, now());
        assert_eq!(denied.kind, KernelEventKind::Error, "{denied:?}");
        assert_eq!(
            denied.error,
            Some(KernelErrorCode::Unauthorized),
            "{denied:?}"
        );
        assert!(
            !matches!(denied.projection, Some(KernelProjection::Receipt { .. })),
            "wrong owner must not receive a receipt: {denied:?}"
        );
        assert_ne!(denied.final_status, Some(ReceiptFinalStatus::Failed));

        let local_read = adapter.handle(
            read_cmd(
                "rd-local",
                KernelCommandKind::ReadReceipt,
                receipt_id,
                Some(&root_s),
            ),
            now(),
        );
        assert_eq!(local_read.error, None, "{local_read:?}");
        let Some(KernelProjection::Receipt { receipt }) = &local_read.projection else {
            panic!("expected local receipt, got {local_read:?}");
        };
        assert_eq!(receipt.owner, principal().owner);
        assert_eq!(receipt.task_case.task_case_id, "tc-1");
        assert_eq!(receipt.revision, 1);
        assert_eq!(receipt.final_status, ReceiptFinalStatus::Open);
        assert!(receipt.output_artifact_digest.is_none());
        assert_ne!(receipt.task_case.task_case_id, "task-foreign");

        let artifact = adapter.handle(
            read_cmd(
                "rd-artifact",
                KernelCommandKind::ReadArtifact,
                receipt_id,
                Some(&root_s),
            ),
            now(),
        );
        assert_read_not_found(&artifact);
        assert_ne!(artifact.final_status, Some(foreign_receipt.final_status));
    }

    #[test]
    fn root_resolution_fresh_adapter_read_receipt_and_artifact_agree() {
        let root = tempfile::tempdir().unwrap();
        let receipt_id = "rcpt-fresh";
        let store = KernelStore::open(root.path().join("kernel.sqlite")).unwrap();
        let opened =
            CompletionReceipt::open(receipt_id, principal().owner, task(), route(), now()).unwrap();
        store.insert_open_receipt(&opened).unwrap();
        let artifact = digest_hex("cd");
        stamp_receipt(
            &store,
            &principal().owner,
            receipt_id,
            "task-fresh",
            ReceiptFinalStatus::Failed,
            &artifact,
        );
        let fresh = LocalKernelAdapter::new(principal());
        let root_s = root.path().to_string_lossy().into_owned();
        let read_receipt = fresh.handle(
            read_cmd(
                "rd-fresh-receipt",
                KernelCommandKind::ReadReceipt,
                receipt_id,
                Some(&root_s),
            ),
            now(),
        );
        let read_artifact = fresh.handle(
            read_cmd(
                "rd-fresh-artifact",
                KernelCommandKind::ReadArtifact,
                receipt_id,
                Some(&root_s),
            ),
            now(),
        );
        assert_eq!(
            read_receipt.kind,
            KernelEventKind::ReceiptUpdated,
            "{read_receipt:?}"
        );
        assert_eq!(
            read_artifact.kind,
            KernelEventKind::ArtifactFinalized,
            "{read_artifact:?}"
        );
        assert_selected_receipt(
            &read_receipt,
            "task-fresh",
            2,
            ReceiptFinalStatus::Failed,
            &artifact,
        );
        assert_selected_receipt(
            &read_artifact,
            "task-fresh",
            2,
            ReceiptFinalStatus::Failed,
            &artifact,
        );
        match (&read_receipt.projection, &read_artifact.projection) {
            (
                Some(KernelProjection::Receipt { receipt: left }),
                Some(KernelProjection::Receipt { receipt: right }),
            ) => {
                assert_eq!(left.receipt_id, right.receipt_id);
                assert_eq!(left.task_case.task_case_id, right.task_case.task_case_id);
                assert_eq!(left.revision, right.revision);
                assert_eq!(left.final_status, right.final_status);
                assert_eq!(left.output_artifact_digest, right.output_artifact_digest);
                assert_eq!(left, right);
            }
            _ => panic!("both reads must project the same receipt"),
        }
    }

    fn seed_unrelated_sqlite(path: &Path, version: u32) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE unrelated(value TEXT); INSERT INTO unrelated VALUES ('keep');",
        )
        .unwrap();
        conn.pragma_update(None, "user_version", version).unwrap();
    }

    #[test]
    fn root_resolution_unrelated_sqlite_is_not_initialized() {
        for version in [0_u32, KernelStore::schema_version()] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("kernel.sqlite");
            seed_unrelated_sqlite(&path, version);
            let before = fs::read(&path).unwrap();
            let adapter = LocalKernelAdapter::new(principal());
            let root_s = root.path().to_string_lossy().into_owned();
            for kind in explicit_read_kinds() {
                assert_read_not_found(&adapter.handle(
                    read_cmd("rd-foreign-db", kind, "rcpt-unknown", Some(&root_s)),
                    now(),
                ));
                assert_eq!(
                    fs::read(&path).unwrap(),
                    before,
                    "{kind:?} changed unrelated SQLite v{version}"
                );
                let conn = rusqlite::Connection::open_with_flags(
                    &path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
                .unwrap();
                let actual: u32 = conn
                    .pragma_query_value(None, "user_version", |row| row.get(0))
                    .unwrap();
                let journal: String = conn
                    .pragma_query_value(None, "journal_mode", |row| row.get(0))
                    .unwrap();
                let tables: Vec<String> = conn
                    .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                    .unwrap()
                    .query_map([], |row| row.get(0))
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap();
                let value: String = conn
                    .query_row("SELECT value FROM unrelated", [], |row| row.get(0))
                    .unwrap();
                assert_eq!(actual, version);
                assert_eq!(journal, "delete");
                assert_eq!(tables, vec!["unrelated".to_string()]);
                assert_eq!(value, "keep");
                assert!(!root.path().join("kernel.sqlite-wal").exists());
                assert!(!root.path().join("kernel.sqlite-shm").exists());
            }
            assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
        }
    }

    #[test]
    fn root_resolution_replaced_cached_sqlite_is_revalidated() {
        let root = tempfile::tempdir().unwrap();
        let receipt_id = "rcpt-replaced-store";
        let (_store, _) = seed_pending_ask(root.path(), receipt_id, "run-replaced", 1);
        let adapter = LocalKernelAdapter::new(principal());
        adapter.register_durable_root(root.path());
        let first = adapter.handle(
            read_cmd("rd-prime", KernelCommandKind::Status, receipt_id, None),
            now(),
        );
        assert_eq!(first.error, None);
        let path = root.path().join("kernel.sqlite");
        fs::remove_file(&path).unwrap();
        seed_unrelated_sqlite(&path, KernelStore::schema_version());
        let before = fs::read(&path).unwrap();
        for kind in explicit_read_kinds() {
            assert_read_not_found(
                &adapter.handle(read_cmd("rd-replaced", kind, receipt_id, None), now()),
            );
        }
        assert_eq!(fs::read(&path).unwrap(), before);
    }
}
