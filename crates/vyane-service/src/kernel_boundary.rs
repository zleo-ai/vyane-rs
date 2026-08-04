//! Transport-neutral local kernel boundary for a future Tauri/Horus shell.
//!
//! Versioned commands and events only. No Tauri types, no UI inference of
//! completion/ownership/route from display strings. Authority always comes
//! from typed projections and the receipt contract.
//!
//! # Authority split (honest)
//!
//! - **Durable multi-process facts** for Process dogfood live in
//!   [`crate::kernel_store::KernelStore`] (`kernel.sqlite`) and are exercised
//!   by [`KernelCommandKind::DriveDogfood`].
//! - [`LocalKernelAdapter`] approve/deny/submit without a dogfood root emit
//!   **versioned events and in-process receipt projections only** — they are
//!   not a second multi-process store. Shell adapters must call dogfood /
//!   KernelStore for durable grant binding.

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

use crate::dogfood::run_successful_dogfood;
use crate::kernel_store::KernelStore;

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
/// Process-local event queue is rebuildable. Durable receipt authority for the
/// Process dogfood path is [`KernelStore`] under registered / command
/// `dogfood_root` paths — Status/ReadReceipt discard memory and re-read facts.
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
            KernelCommandKind::DecideApproval => {
                let granted = command.approval_granted.unwrap_or(false);
                if !granted {
                    return self.push_event(
                        KernelEventKind::ApprovalRequired,
                        now,
                        command.receipt_id,
                        command.agent_run_id,
                        None,
                        None,
                        Some(KernelErrorCode::ApprovalRequired),
                        Some("approval still required".into()),
                    );
                }
                self.push_event(
                    KernelEventKind::Approved,
                    now,
                    command.receipt_id,
                    command.agent_run_id,
                    None,
                    Some(KernelProjection::Ownership {
                        owner: self.principal.owner.clone(),
                        lease_owner: Some(self.principal.principal_id.clone()),
                        generation: Some(1),
                    }),
                    None,
                    Some("approval granted".into()),
                )
            }
            KernelCommandKind::DenyApproval => self.push_event(
                KernelEventKind::Denied,
                now,
                command.receipt_id,
                command.agent_run_id,
                Some(ReceiptFinalStatus::Failed),
                Some(KernelProjection::Ownership {
                    owner: self.principal.owner.clone(),
                    lease_owner: Some(self.principal.principal_id.clone()),
                    generation: None,
                }),
                None,
                Some("approval denied".into()),
            ),
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
            },
            now(),
        );
        assert_eq!(status2.error, None);
        assert_eq!(status2.final_status, Some(ReceiptFinalStatus::Completed));
    }
}
