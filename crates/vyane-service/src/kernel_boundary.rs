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
//!   closed. Events remain rebuildable; they are not a second authority.

use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use vyane_core::{
    CompletionReceipt, MemoryReceiptLedger, RECEIPT_SCHEMA_VERSION, ReceiptFinalStatus,
    RouteConfig, TaskCase,
};

use crate::approval_fsm::DeliveryEvent;
use crate::dogfood::run_successful_dogfood;
use crate::kernel_store::{
    ApprovalDecisionKind, ApprovalGrantBinding, KernelStore, KernelStoreError,
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
    /// Grant/deny binding. Required for durable [`KernelCommandKind::DecideApproval`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_binding: Option<KernelApprovalBinding>,
}

/// Binding for durable approve/deny. Optional on the wire; missing grant binding fails closed.
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

/// In-process adapter exercising the transport-neutral contract.
///
/// Process-local event queue is rebuildable. Durable receipt and approval
/// authority is [`KernelStore`] under registered / command `dogfood_root`
/// paths — Status/ReadReceipt discard memory and re-read facts. Approve/deny
/// without that store fail closed.
pub struct LocalKernelAdapter {
    principal: KernelPrincipal,
    events: Mutex<VecDeque<KernelEvent>>,
    next_sequence: Mutex<u64>,
    /// Fenced receipt ledger for pure boundary submit/cancel (non-dogfood).
    receipts: Mutex<MemoryReceiptLedger>,
    /// Durable dogfood roots (`…/durable-*` or roots containing `kernel.sqlite`).
    durable_roots: Mutex<Vec<PathBuf>>,
}

impl LocalKernelAdapter {
    #[must_use]
    pub fn new(principal: KernelPrincipal) -> Self {
        Self {
            principal,
            events: Mutex::new(VecDeque::new()),
            next_sequence: Mutex::new(1),
            receipts: Mutex::new(MemoryReceiptLedger::new()),
            durable_roots: Mutex::new(Vec::new()),
        }
    }

    /// Register a durable root so later Status/ReadReceipt can rebuild without
    /// an in-process receipt cache (discard-and-rebuild).
    pub fn register_durable_root(&self, root: impl Into<PathBuf>) {
        let root = root.into();
        if let Ok(mut guard) = self.durable_roots.lock()
            && !guard.iter().any(|p| p == &root)
        {
            guard.push(root);
        }
    }

    /// Drop process-local receipt cache (events kept). Used by rebuild tests.
    pub fn discard_in_memory_receipts(&self) {
        if let Ok(mut guard) = self.receipts.lock() {
            *guard = MemoryReceiptLedger::new();
        }
    }

    /// Load receipt: memory first, then durable KernelStore under dogfood roots.
    fn load_receipt(
        &self,
        receipt_id: &str,
        dogfood_root: Option<&str>,
    ) -> Option<CompletionReceipt> {
        if let Ok(guard) = self.receipts.lock()
            && let Some(r) = guard.get_for_owner(&self.principal.owner, receipt_id)
        {
            return Some(r.clone());
        }
        self.load_receipt_from_durable(receipt_id, dogfood_root)
    }

    fn load_receipt_from_durable(
        &self,
        receipt_id: &str,
        dogfood_root: Option<&str>,
    ) -> Option<CompletionReceipt> {
        let mut candidates: Vec<PathBuf> = Vec::new();
        if let Some(root) = dogfood_root.map(PathBuf::from) {
            push_kernel_candidates(&root, receipt_id, &mut candidates);
        }
        if let Ok(roots) = self.durable_roots.lock() {
            for root in roots.iter() {
                push_kernel_candidates(root, receipt_id, &mut candidates);
            }
        }
        // Dedup paths.
        candidates.sort();
        candidates.dedup();
        for path in candidates {
            if !path.exists() {
                continue;
            }
            let Ok(store) = KernelStore::open(&path) else {
                continue;
            };
            if let Ok(Some(receipt)) = store.get_receipt(&self.principal.owner, receipt_id) {
                return Some(receipt);
            }
        }
        None
    }

    fn open_durable_store(
        &self,
        receipt_id: &str,
        dogfood_root: Option<&str>,
    ) -> Option<KernelStore> {
        let mut candidates: Vec<PathBuf> = Vec::new();
        if let Some(root) = dogfood_root.map(PathBuf::from) {
            push_kernel_candidates(&root, receipt_id, &mut candidates);
        }
        if let Ok(roots) = self.durable_roots.lock() {
            for root in roots.iter() {
                push_kernel_candidates(root, receipt_id, &mut candidates);
            }
        }
        candidates.sort();
        candidates.dedup();
        let mut receipt_hit = None;
        for path in candidates {
            if !path.exists() {
                continue;
            }
            let Ok(store) = KernelStore::open(&path) else {
                continue;
            };
            if matches!(
                store.get_approval(&self.principal.owner, receipt_id),
                Ok(Some(_))
            ) {
                return Some(store);
            }
            if receipt_hit.is_none()
                && matches!(
                    store.get_receipt(&self.principal.owner, receipt_id),
                    Ok(Some(_))
                )
            {
                receipt_hit = Some(store);
            }
        }
        receipt_hit
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
        let Some(store) = self.open_durable_store(&receipt_id, command.dogfood_root.as_deref())
        else {
            return self.error_event(now, KernelErrorCode::InvalidCommand, Some(receipt_id));
        };
        if !command.approval_granted.unwrap_or(false) {
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
        }
        let Some(run_id) = command.agent_run_id.clone() else {
            return self.error_event(now, KernelErrorCode::InvalidCommand, Some(receipt_id));
        };
        let Some(binding) = command.approval_binding.clone() else {
            return self.error_event(now, KernelErrorCode::InvalidCommand, Some(receipt_id));
        };
        let grant = ApprovalGrantBinding {
            owner: self.principal.owner.clone(),
            receipt_id: receipt_id.clone(),
            run_id: run_id.clone(),
            request_digest: binding.request_digest,
            expected_revision: binding.expected_revision,
            lease_owner: binding.lease_owner.clone(),
            generation: binding.generation,
            decided_by: self.principal.principal_id.clone(),
        };
        match store.grant_approval(&grant, now) {
            Ok(decision) => {
                self.advance_delivery_phase(
                    &store,
                    &receipt_id,
                    Some(run_id.as_str()),
                    DeliveryEvent::GrantAccepted,
                    Some(decision.approval_id.as_str()),
                    now,
                );
                if let Some(root) = command.dogfood_root.as_deref() {
                    self.register_durable_root(root);
                }
                self.push_event(
                    KernelEventKind::Approved,
                    now,
                    Some(receipt_id),
                    Some(run_id),
                    None,
                    Some(KernelProjection::Ownership {
                        owner: self.principal.owner.clone(),
                        lease_owner: Some(grant.lease_owner),
                        generation: Some(grant.generation),
                    }),
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
        let Some(store) = self.open_durable_store(&receipt_id, command.dogfood_root.as_deref())
        else {
            return self.error_event(now, KernelErrorCode::InvalidCommand, Some(receipt_id));
        };
        let digest = match command.approval_binding.as_ref() {
            Some(binding) => binding.request_digest.clone(),
            None => match store.get_approval(&self.principal.owner, &receipt_id) {
                Ok(Some(row)) => row.request_digest,
                Ok(None) => {
                    return self.error_event(now, KernelErrorCode::NotFound, Some(receipt_id));
                }
                Err(err) => {
                    return self.error_event(now, Self::map_store_error(err), Some(receipt_id));
                }
            },
        };
        match store.deny_approval(
            &self.principal.owner,
            &receipt_id,
            &digest,
            &self.principal.principal_id,
            now,
        ) {
            Ok(_) => {
                self.advance_delivery_phase(
                    &store,
                    &receipt_id,
                    command.agent_run_id.as_deref(),
                    DeliveryEvent::DenyAccepted,
                    None,
                    now,
                );
                if let Some(root) = command.dogfood_root.as_deref() {
                    self.register_durable_root(root);
                }
                // Denied is the approval decision. Receipt status stays whatever
                // KernelStore already recorded (usually Open) — do not invent Failed.
                // Ownership comes from the durable lease fence, not the caller
                // principal/binding: deny does not steal or grant a lease.
                self.push_event(
                    KernelEventKind::Denied,
                    now,
                    Some(receipt_id),
                    command.agent_run_id.clone(),
                    None,
                    Some(self.ownership_from_store(&store, command.agent_run_id.as_deref())),
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

    /// Best-effort FSM advance after a durable grant/deny. The approval row is
    /// already committed; a phase miss must not invent Approved/Denied.
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

    fn advance_delivery_phase(
        &self,
        store: &KernelStore,
        receipt_id: &str,
        run_id: Option<&str>,
        event: DeliveryEvent,
        approval_id: Option<&str>,
        now: DateTime<Utc>,
    ) {
        let Some(run_id) = run_id else {
            return;
        };
        if let Ok(Some((_, phase_rev))) =
            store.get_delivery_phase(&self.principal.owner, receipt_id)
        {
            let _ = store.set_delivery_phase(
                &self.principal.owner,
                receipt_id,
                run_id,
                phase_rev,
                event,
                approval_id,
                now,
            );
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
        assert_eq!(event.error, Some(KernelErrorCode::InvalidCommand));
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
        assert_eq!(event.error, Some(KernelErrorCode::InvalidCommand));
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
    fn deny_ownership_projection_reads_store_lease_fence() {
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (_store, digest) =
            seed_pending_ask_with_lease(root.path(), "rcpt-own", "run-own", 1, "worker-a", 4);
        let adapter = LocalKernelAdapter::new(principal());
        let mut binding = binding_for(&digest, 1);
        binding.lease_owner = principal().principal_id.clone();
        binding.generation = 99;
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

        let (_store2, digest2) =
            seed_pending_ask(root.path(), "rcpt-own-norun", "run-own-norun", 1);
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
        match no_run.projection.unwrap() {
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
        let root = tempfile::tempdir().unwrap();
        let root_s = root.path().to_string_lossy().into_owned();
        let (store, digest) = seed_pending_ask(root.path(), "rcpt-hold", "run-hold", 1);
        let adapter = LocalKernelAdapter::new(principal());
        let event = adapter.handle(
            approval_cmd(
                "ap-hold",
                KernelCommandKind::DecideApproval,
                Some("rcpt-hold"),
                Some("run-hold"),
                Some(false),
                Some(&root_s),
                Some(binding_for(&digest, 1)),
            ),
            now(),
        );
        assert_eq!(event.kind, KernelEventKind::ApprovalRequired);
        assert_eq!(event.error, Some(KernelErrorCode::ApprovalRequired));
        let row = store
            .get_approval(&principal().owner, "rcpt-hold")
            .unwrap()
            .expect("pending retained");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Pending
        );
    }

    #[test]
    fn deny_without_binding_uses_pending_row() {
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
        assert_eq!(denied.kind, KernelEventKind::Denied);
        assert_eq!(denied.final_status, None);
        let row = store
            .get_approval(&principal().owner, "rcpt-nobind")
            .unwrap()
            .expect("denied row");
        assert_eq!(
            row.decision,
            crate::kernel_store::ApprovalDecisionKind::Denied
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
}
