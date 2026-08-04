//! SQLite-backed multi-process kernel durable store.
//!
//! Authority for CompletionReceipt CAS, effect identity, approval decisions,
//! artifact metadata, lease fences, and delivery phase. AgentRun claim/lease
//! remains in [`vyane_agent::SqliteAgentStore`]; this store does not reimplement it.
//!
//! JSON files are **not** the multi-process authority.

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension as _, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use vyane_core::{
    CompletionReceipt, GATE_CI_PACKAGING, GATE_INDEPENDENT_REVIEW, GATE_INTEGRATION,
    GATE_TRUTH_PROBE, GATE_UNIT, NamedGate, ReceiptAttempt, ReceiptError, ReceiptFinalStatus,
};

use crate::approval_fsm::{self, DeliveryEvent, DeliveryPhase, DeliveryTransitionError};

const SCHEMA_VERSION: u32 = 1;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const MIGRATION_0001: &str = include_str!("../migrations/0001_kernel.sql");

/// Errors from the kernel durable store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KernelStoreError {
    Io(String),
    Sqlite(String),
    UnsupportedSchema { found: u32, supported: u32 },
    NotFound,
    OwnerMismatch,
    StaleRevision { expected: u64, actual: u64 },
    Conflict(String),
    TerminalImmutable,
    InvalidInput(&'static str),
    Receipt(String),
    Delivery(String),
    ApprovalDeniedFinal,
    ApprovalBindingMismatch,
    DuplicateEffect { effect_id: String },
}

impl std::fmt::Display for KernelStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(msg) => write!(f, "kernel store io: {msg}"),
            Self::Sqlite(msg) => write!(f, "kernel store sqlite: {msg}"),
            Self::UnsupportedSchema { found, supported } => {
                write!(
                    f,
                    "unsupported kernel schema {found} (supported {supported})"
                )
            }
            Self::NotFound => f.write_str("kernel store row not found"),
            Self::OwnerMismatch => f.write_str("owner mismatch"),
            Self::StaleRevision { expected, actual } => {
                write!(f, "stale revision expected {expected} actual {actual}")
            }
            Self::Conflict(msg) => write!(f, "conflict: {msg}"),
            Self::TerminalImmutable => f.write_str("terminal state is immutable"),
            Self::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
            Self::Receipt(msg) => write!(f, "receipt: {msg}"),
            Self::Delivery(msg) => write!(f, "delivery: {msg}"),
            Self::ApprovalDeniedFinal => f.write_str("denied approval cannot be granted"),
            Self::ApprovalBindingMismatch => f.write_str("approval binding mismatch"),
            Self::DuplicateEffect { effect_id } => {
                write!(f, "duplicate effect for {effect_id}")
            }
        }
    }
}

impl std::error::Error for KernelStoreError {}

impl From<rusqlite::Error> for KernelStoreError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value.to_string())
    }
}

impl From<ReceiptError> for KernelStoreError {
    fn from(value: ReceiptError) -> Self {
        match value {
            ReceiptError::OwnerMismatch => Self::OwnerMismatch,
            ReceiptError::StaleRevision { expected, actual } => {
                Self::StaleRevision { expected, actual }
            }
            ReceiptError::TerminalImmutable => Self::TerminalImmutable,
            other => Self::Receipt(other.to_string()),
        }
    }
}

impl From<DeliveryTransitionError> for KernelStoreError {
    fn from(value: DeliveryTransitionError) -> Self {
        match value {
            DeliveryTransitionError::DenyIsFinal => Self::ApprovalDeniedFinal,
            DeliveryTransitionError::TerminalImmutable => Self::TerminalImmutable,
            other => Self::Delivery(other.to_string()),
        }
    }
}

pub type KernelStoreResult<T> = Result<T, KernelStoreError>;

/// Durable lease fence for dogfood reopen (pairs with AgentStore lease).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseFence {
    pub owner: String,
    pub run_id: String,
    pub lease_owner: String,
    pub generation: u64,
    pub revision: u64,
    pub token: String,
    pub policy_digest: String,
    pub expires_at_ms: Option<i64>,
}

/// Durable approval decision bound to task/run/revision/digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDecision {
    pub owner: String,
    pub approval_id: String,
    pub receipt_id: String,
    pub run_id: String,
    pub request_digest: String,
    pub decision: ApprovalDecisionKind,
    pub decided_by: Option<String>,
    pub bound_revision: u64,
    pub bound_lease_owner: Option<String>,
    pub bound_generation: Option<u64>,
    pub decided_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecisionKind {
    Pending,
    Approved,
    Denied,
}

impl ApprovalDecisionKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Denied => "denied",
        }
    }

    fn parse(value: &str) -> KernelStoreResult<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "approved" => Ok(Self::Approved),
            "denied" => Ok(Self::Denied),
            _ => Err(KernelStoreError::InvalidInput("unknown approval decision")),
        }
    }
}

/// Binding required for a successful grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalGrantBinding {
    pub owner: String,
    pub receipt_id: String,
    pub run_id: String,
    pub request_digest: String,
    pub expected_revision: u64,
    pub lease_owner: String,
    pub generation: u64,
    pub decided_by: String,
}

/// Artifact metadata row (bytes live on disk under workdir).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactMeta {
    pub owner: String,
    pub receipt_id: String,
    pub digest: String,
    pub path: String,
    pub content_bytes: u64,
    pub created_at_ms: i64,
}

/// Multi-process kernel durable store.
#[derive(Debug, Clone)]
pub struct KernelStore {
    path: PathBuf,
}

impl KernelStore {
    /// Open or create `kernel.sqlite` at `path` (file path, not directory).
    pub fn open(path: impl Into<PathBuf>) -> KernelStoreResult<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| KernelStoreError::Io(e.to_string()))?;
        }
        let store = Self { path };
        store.initialize()?;
        Ok(store)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub const fn schema_version() -> u32 {
        SCHEMA_VERSION
    }

    fn connect(&self) -> KernelStoreResult<Connection> {
        let conn = Connection::open(&self.path)?;
        conn.busy_timeout(BUSY_TIMEOUT)?;
        conn.pragma_update(None, "foreign_keys", true)?;
        Ok(conn)
    }

    fn initialize(&self) -> KernelStoreResult<()> {
        // Retry on SQLITE_BUSY so concurrent first-open does not fail closed spuriously.
        let mut last_err = None;
        for attempt in 0..32 {
            match self.initialize_once() {
                Ok(()) => return Ok(()),
                Err(KernelStoreError::Sqlite(msg))
                    if msg.contains("locked") || msg.contains("busy") =>
                {
                    last_err = Some(KernelStoreError::Sqlite(msg));
                    std::thread::sleep(Duration::from_millis(5 + attempt * 2));
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            KernelStoreError::Sqlite("initialize busy after retries".into())
        }))
    }

    fn initialize_once(&self) -> KernelStoreResult<()> {
        let mut conn = self.connect()?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        let found: u32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if found > SCHEMA_VERSION {
            return Err(KernelStoreError::UnsupportedSchema {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        // Re-check user_version inside Immediate txn so concurrent first-open
        // does not double-apply CREATE TABLE.
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let found: u32 = tx.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if found > SCHEMA_VERSION {
            return Err(KernelStoreError::UnsupportedSchema {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        if found == 0 {
            tx.execute_batch(MIGRATION_0001)?;
            tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            tx.execute(
                "INSERT OR REPLACE INTO kernel_meta(key, value) VALUES ('schema_version', ?1)",
                params![SCHEMA_VERSION.to_string()],
            )?;
        } else if found < SCHEMA_VERSION {
            // Future migrations land here; v1 has none yet.
            return Err(KernelStoreError::UnsupportedSchema {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        tx.commit()?;
        Ok(())
    }

    // ── Effects ──────────────────────────────────────────────────────────

    /// Apply an external effect once. Same digest is idempotent (`Ok(false)`).
    /// Conflicting digest is fail-closed [`KernelStoreError::DuplicateEffect`].
    pub fn apply_effect_once(
        &self,
        owner: &str,
        effect_id: &str,
        payload_digest: &str,
        run_id: Option<&str>,
        receipt_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> KernelStoreResult<bool> {
        validate_owner(owner)?;
        validate_id("effect_id", effect_id)?;
        validate_digest(payload_digest)?;
        let mut conn = self.connect()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT payload_digest FROM kernel_effects WHERE owner = ?1 AND effect_id = ?2",
                params![owner, effect_id],
                |row| row.get(0),
            )
            .optional()?;
        match existing {
            Some(d) if d == payload_digest => {
                tx.commit()?;
                Ok(false)
            }
            Some(_) => Err(KernelStoreError::DuplicateEffect {
                effect_id: effect_id.to_string(),
            }),
            None => {
                tx.execute(
                    "INSERT INTO kernel_effects(owner, effect_id, payload_digest, run_id, receipt_id, applied_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        owner,
                        effect_id,
                        payload_digest,
                        run_id,
                        receipt_id,
                        now.timestamp_millis()
                    ],
                )?;
                tx.commit()?;
                Ok(true)
            }
        }
    }

    #[must_use]
    pub fn was_effect_applied(&self, owner: &str, effect_id: &str) -> bool {
        self.connect()
            .ok()
            .and_then(|conn| {
                conn.query_row(
                    "SELECT 1 FROM kernel_effects WHERE owner = ?1 AND effect_id = ?2",
                    params![owner, effect_id],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .ok()
                .flatten()
            })
            .is_some()
    }

    pub fn effect_count(&self, owner: &str) -> KernelStoreResult<usize> {
        let conn = self.connect()?;
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM kernel_effects WHERE owner = ?1",
            params![owner],
            |row| row.get(0),
        )?;
        Ok(usize::try_from(n).unwrap_or(0))
    }

    // ── Receipts ─────────────────────────────────────────────────────────

    pub fn insert_open_receipt(&self, receipt: &CompletionReceipt) -> KernelStoreResult<()> {
        receipt.validate()?;
        if receipt.final_status != ReceiptFinalStatus::Open || receipt.revision != 1 {
            return Err(KernelStoreError::InvalidInput(
                "insert_open requires open revision 1",
            ));
        }
        let body = receipt
            .to_canonical_json()
            .map_err(|e| KernelStoreError::Receipt(e.to_string()))?;
        let mut conn = self.connect()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists: Option<i64> = tx
            .query_row(
                "SELECT 1 FROM kernel_receipts WHERE owner = ?1 AND receipt_id = ?2",
                params![receipt.owner, receipt.receipt_id],
                |row| row.get(0),
            )
            .optional()?;
        if exists.is_some() {
            return Err(KernelStoreError::Conflict(
                "receipt_id already exists".into(),
            ));
        }
        tx.execute(
            "INSERT INTO kernel_receipts(owner, receipt_id, revision, final_status, body_json, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                receipt.owner,
                receipt.receipt_id,
                receipt.revision as i64,
                final_status_str(receipt.final_status),
                body,
                receipt.created_at.timestamp_millis(),
                receipt.updated_at.timestamp_millis()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_receipt(
        &self,
        owner: &str,
        receipt_id: &str,
    ) -> KernelStoreResult<Option<CompletionReceipt>> {
        let conn = self.connect()?;
        let body: Option<String> = conn
            .query_row(
                "SELECT body_json FROM kernel_receipts WHERE owner = ?1 AND receipt_id = ?2",
                params![owner, receipt_id],
                |row| row.get(0),
            )
            .optional()?;
        match body {
            None => Ok(None),
            Some(json) => {
                let receipt = CompletionReceipt::from_canonical_json(&json)
                    .map_err(|e| KernelStoreError::Receipt(e.to_string()))?;
                if receipt.owner != owner {
                    return Err(KernelStoreError::OwnerMismatch);
                }
                Ok(Some(receipt))
            }
        }
    }

    /// CAS transition of a receipt body. Persist only after mutate + validate.
    pub fn transition_receipt<F>(
        &self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        now: DateTime<Utc>,
        mutate: F,
    ) -> KernelStoreResult<CompletionReceipt>
    where
        F: FnOnce(&mut CompletionReceipt) -> Result<(), ReceiptError>,
    {
        let mut conn = self.connect()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<(i64, String, String)> = tx
            .query_row(
                "SELECT revision, final_status, body_json FROM kernel_receipts
                 WHERE owner = ?1 AND receipt_id = ?2",
                params![owner, receipt_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((rev, status, body)) = row else {
            return Err(KernelStoreError::NotFound);
        };
        let actual = u64::try_from(rev).unwrap_or(0);
        if actual != expected_revision {
            return Err(KernelStoreError::StaleRevision {
                expected: expected_revision,
                actual,
            });
        }
        if status != "open" {
            return Err(KernelStoreError::TerminalImmutable);
        }
        let mut next = CompletionReceipt::from_canonical_json(&body)
            .map_err(|e| KernelStoreError::Receipt(e.to_string()))?;
        if next.owner != owner {
            return Err(KernelStoreError::OwnerMismatch);
        }
        mutate(&mut next)?;
        next.revision = expected_revision
            .checked_add(1)
            .ok_or(KernelStoreError::InvalidInput("revision overflow"))?;
        next.updated_at = now;
        next.validate()?;
        let json = next
            .to_canonical_json()
            .map_err(|e| KernelStoreError::Receipt(e.to_string()))?;
        let n = tx.execute(
            "UPDATE kernel_receipts
             SET revision = ?1, final_status = ?2, body_json = ?3, updated_at_ms = ?4
             WHERE owner = ?5 AND receipt_id = ?6 AND revision = ?7",
            params![
                next.revision as i64,
                final_status_str(next.final_status),
                json,
                now.timestamp_millis(),
                owner,
                receipt_id,
                expected_revision as i64
            ],
        )?;
        if n != 1 {
            return Err(KernelStoreError::StaleRevision {
                expected: expected_revision,
                actual,
            });
        }
        tx.commit()?;
        Ok(next)
    }

    pub fn record_attempt(
        &self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        attempt: ReceiptAttempt,
        now: DateTime<Utc>,
    ) -> KernelStoreResult<CompletionReceipt> {
        if attempt.owner != owner {
            return Err(KernelStoreError::OwnerMismatch);
        }
        attempt.validate()?;
        self.transition_receipt(owner, receipt_id, expected_revision, now, |receipt| {
            receipt.attempts.push(attempt);
            Ok(())
        })
    }

    pub fn set_gate(
        &self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        gate: NamedGate,
        now: DateTime<Utc>,
    ) -> KernelStoreResult<CompletionReceipt> {
        gate.validate()?;
        self.transition_receipt(owner, receipt_id, expected_revision, now, |receipt| {
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
        &self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        digest: impl Into<String>,
        now: DateTime<Utc>,
    ) -> KernelStoreResult<CompletionReceipt> {
        let digest = digest.into();
        self.transition_receipt(owner, receipt_id, expected_revision, now, |receipt| {
            receipt.output_artifact_digest = Some(digest);
            Ok(())
        })
    }

    pub fn complete_receipt(
        &self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        now: DateTime<Utc>,
    ) -> KernelStoreResult<CompletionReceipt> {
        // `validate()` enforces truth probe + artifact when status is Completed.
        self.transition_receipt(owner, receipt_id, expected_revision, now, |receipt| {
            receipt.final_status = ReceiptFinalStatus::Completed;
            Ok(())
        })
    }

    pub fn fail_receipt(
        &self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        summary: impl Into<String>,
        now: DateTime<Utc>,
    ) -> KernelStoreResult<CompletionReceipt> {
        let summary = summary.into();
        self.transition_receipt(owner, receipt_id, expected_revision, now, |receipt| {
            receipt.final_status = ReceiptFinalStatus::Failed;
            receipt.validation_summary = Some(summary);
            Ok(())
        })
    }

    pub fn cancel_receipt(
        &self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        now: DateTime<Utc>,
    ) -> KernelStoreResult<CompletionReceipt> {
        self.transition_receipt(owner, receipt_id, expected_revision, now, |receipt| {
            receipt.final_status = ReceiptFinalStatus::Cancelled;
            Ok(())
        })
    }

    pub fn mark_unresolved(
        &self,
        owner: &str,
        receipt_id: &str,
        expected_revision: u64,
        note: impl Into<String>,
        now: DateTime<Utc>,
    ) -> KernelStoreResult<CompletionReceipt> {
        let note = note.into();
        self.transition_receipt(owner, receipt_id, expected_revision, now, |receipt| {
            receipt.final_status = ReceiptFinalStatus::Unresolved;
            receipt.recovery_cleanup.note = Some(note);
            Ok(())
        })
    }

    // ── Approvals ────────────────────────────────────────────────────────

    /// Record a pending approval request (ask). Idempotent for same digest.
    #[allow(clippy::too_many_arguments)]
    pub fn record_approval_required(
        &self,
        owner: &str,
        approval_id: &str,
        receipt_id: &str,
        run_id: &str,
        request_digest: &str,
        bound_revision: u64,
        now: DateTime<Utc>,
    ) -> KernelStoreResult<ApprovalDecision> {
        validate_owner(owner)?;
        validate_digest(request_digest)?;
        let mut conn = self.connect()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<(String, String)> = tx
            .query_row(
                "SELECT approval_id, decision FROM kernel_approvals
                 WHERE owner = ?1 AND receipt_id = ?2 AND request_digest = ?3",
                params![owner, receipt_id, request_digest],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((id, decision)) = existing {
            let kind = ApprovalDecisionKind::parse(&decision)?;
            let row = load_approval(&tx, owner, &id)?;
            if kind == ApprovalDecisionKind::Pending {
                return Ok(row);
            }
            return Ok(row);
        }
        let ms = now.timestamp_millis();
        tx.execute(
            "INSERT INTO kernel_approvals(
                owner, approval_id, receipt_id, run_id, request_digest, decision,
                decided_by, bound_revision, bound_lease_owner, bound_generation,
                decided_at_ms, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, 'pending', NULL, ?6, NULL, NULL, NULL, ?7, ?7)",
            params![
                owner,
                approval_id,
                receipt_id,
                run_id,
                request_digest,
                bound_revision as i64,
                ms
            ],
        )?;
        let row = load_approval(&tx, owner, approval_id)?;
        tx.commit()?;
        Ok(row)
    }

    /// Bound grant. Idempotent if already approved with same binding.
    pub fn grant_approval(
        &self,
        binding: &ApprovalGrantBinding,
        now: DateTime<Utc>,
    ) -> KernelStoreResult<ApprovalDecision> {
        validate_owner(&binding.owner)?;
        validate_digest(&binding.request_digest)?;
        let mut conn = self.connect()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        #[allow(clippy::type_complexity)]
        type ExistingApprovalRow = (String, String, i64, Option<String>, Option<i64>);
        let existing: Option<ExistingApprovalRow> = tx
            .query_row(
                "SELECT approval_id, decision, bound_revision, bound_lease_owner, bound_generation
                 FROM kernel_approvals
                 WHERE owner = ?1 AND receipt_id = ?2 AND request_digest = ?3",
                params![binding.owner, binding.receipt_id, binding.request_digest],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((approval_id, decision, bound_rev, bound_lo, bound_gen)) = existing else {
            return Err(KernelStoreError::NotFound);
        };
        let kind = ApprovalDecisionKind::parse(&decision)?;
        match kind {
            ApprovalDecisionKind::Denied => Err(KernelStoreError::ApprovalDeniedFinal),
            ApprovalDecisionKind::Approved => {
                // Idempotent only when binding matches.
                let rev_ok = u64::try_from(bound_rev).unwrap_or(0) == binding.expected_revision;
                let lo_ok = bound_lo.as_deref() == Some(binding.lease_owner.as_str());
                let gen_ok =
                    bound_gen.map(|g| u64::try_from(g).unwrap_or(0)) == Some(binding.generation);
                if rev_ok && lo_ok && gen_ok {
                    let row = load_approval(&tx, &binding.owner, &approval_id)?;
                    tx.commit()?;
                    Ok(row)
                } else {
                    Err(KernelStoreError::ApprovalBindingMismatch)
                }
            }
            ApprovalDecisionKind::Pending => {
                if u64::try_from(bound_rev).unwrap_or(0) != binding.expected_revision {
                    return Err(KernelStoreError::ApprovalBindingMismatch);
                }
                // Also require run_id match from row.
                let run_id: String = tx.query_row(
                    "SELECT run_id FROM kernel_approvals WHERE owner = ?1 AND approval_id = ?2",
                    params![binding.owner, approval_id],
                    |row| row.get(0),
                )?;
                if run_id != binding.run_id {
                    return Err(KernelStoreError::ApprovalBindingMismatch);
                }
                // First grant must match durable lease fence when present.
                let fence: Option<(String, i64)> = tx
                    .query_row(
                        "SELECT lease_owner, generation FROM kernel_lease_fences
                         WHERE owner = ?1 AND run_id = ?2",
                        params![binding.owner, binding.run_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                if let Some((fence_owner, fence_gen)) = fence
                    && (fence_owner != binding.lease_owner
                        || u64::try_from(fence_gen).unwrap_or(0) != binding.generation)
                {
                    return Err(KernelStoreError::ApprovalBindingMismatch);
                }
                let ms = now.timestamp_millis();
                let n = tx.execute(
                    "UPDATE kernel_approvals
                     SET decision = 'approved', decided_by = ?1, bound_lease_owner = ?2,
                         bound_generation = ?3, decided_at_ms = ?4, updated_at_ms = ?4
                     WHERE owner = ?5 AND approval_id = ?6 AND decision = 'pending'",
                    params![
                        binding.decided_by,
                        binding.lease_owner,
                        binding.generation as i64,
                        ms,
                        binding.owner,
                        approval_id
                    ],
                )?;
                if n != 1 {
                    return Err(KernelStoreError::Conflict(
                        "approval grant CAS failed".into(),
                    ));
                }
                let row = load_approval(&tx, &binding.owner, &approval_id)?;
                tx.commit()?;
                Ok(row)
            }
        }
    }

    pub fn deny_approval(
        &self,
        owner: &str,
        receipt_id: &str,
        request_digest: &str,
        decided_by: &str,
        now: DateTime<Utc>,
    ) -> KernelStoreResult<ApprovalDecision> {
        let mut conn = self.connect()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<(String, String)> = tx
            .query_row(
                "SELECT approval_id, decision FROM kernel_approvals
                 WHERE owner = ?1 AND receipt_id = ?2 AND request_digest = ?3",
                params![owner, receipt_id, request_digest],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((approval_id, decision)) = existing else {
            return Err(KernelStoreError::NotFound);
        };
        let kind = ApprovalDecisionKind::parse(&decision)?;
        if kind == ApprovalDecisionKind::Denied {
            let row = load_approval(&tx, owner, &approval_id)?;
            tx.commit()?;
            return Ok(row);
        }
        if kind == ApprovalDecisionKind::Approved {
            return Err(KernelStoreError::Conflict(
                "cannot deny an already approved decision".into(),
            ));
        }
        let ms = now.timestamp_millis();
        tx.execute(
            "UPDATE kernel_approvals
             SET decision = 'denied', decided_by = ?1, decided_at_ms = ?2, updated_at_ms = ?2
             WHERE owner = ?3 AND approval_id = ?4 AND decision = 'pending'",
            params![decided_by, ms, owner, approval_id],
        )?;
        let row = load_approval(&tx, owner, &approval_id)?;
        tx.commit()?;
        Ok(row)
    }

    pub fn get_approval(
        &self,
        owner: &str,
        receipt_id: &str,
    ) -> KernelStoreResult<Option<ApprovalDecision>> {
        let conn = self.connect()?;
        let id: Option<String> = conn
            .query_row(
                "SELECT approval_id FROM kernel_approvals
                 WHERE owner = ?1 AND receipt_id = ?2
                 ORDER BY updated_at_ms DESC LIMIT 1",
                params![owner, receipt_id],
                |row| row.get(0),
            )
            .optional()?;
        match id {
            None => Ok(None),
            Some(approval_id) => Ok(Some(load_approval(&conn, owner, &approval_id)?)),
        }
    }

    // ── Lease fence ──────────────────────────────────────────────────────

    /// Persist a lease fence. Fail closed on stale generation (cannot downgrade
    /// or clobber a higher generation with a lower one).
    pub fn put_lease_fence(&self, fence: &LeaseFence, now: DateTime<Utc>) -> KernelStoreResult<()> {
        validate_owner(&fence.owner)?;
        let mut conn = self.connect()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<(String, i64)> = tx
            .query_row(
                "SELECT lease_owner, generation FROM kernel_lease_fences
                 WHERE owner = ?1 AND run_id = ?2",
                params![fence.owner, fence.run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((existing_owner, existing_gen_i)) = existing {
            let existing_gen = u64::try_from(existing_gen_i).unwrap_or(0);
            if fence.generation < existing_gen {
                return Err(KernelStoreError::Conflict(
                    "stale lease generation cannot overwrite newer fence".into(),
                ));
            }
            // Same generation must keep the same lease_owner (no silent steal).
            if fence.generation == existing_gen && existing_owner != fence.lease_owner {
                return Err(KernelStoreError::Conflict(
                    "lease_owner mismatch for same generation".into(),
                ));
            }
        }
        tx.execute(
            "INSERT INTO kernel_lease_fences(
                owner, run_id, lease_owner, generation, revision, token, policy_digest,
                expires_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(owner, run_id) DO UPDATE SET
                lease_owner = excluded.lease_owner,
                generation = excluded.generation,
                revision = excluded.revision,
                token = excluded.token,
                policy_digest = excluded.policy_digest,
                expires_at_ms = excluded.expires_at_ms,
                updated_at_ms = excluded.updated_at_ms
             WHERE excluded.generation >= kernel_lease_fences.generation",
            params![
                fence.owner,
                fence.run_id,
                fence.lease_owner,
                fence.generation as i64,
                fence.revision as i64,
                fence.token,
                fence.policy_digest,
                fence.expires_at_ms,
                now.timestamp_millis()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_lease_fence(
        &self,
        owner: &str,
        run_id: &str,
    ) -> KernelStoreResult<Option<LeaseFence>> {
        let conn = self.connect()?;
        conn.query_row(
            "SELECT owner, run_id, lease_owner, generation, revision, token, policy_digest, expires_at_ms
             FROM kernel_lease_fences WHERE owner = ?1 AND run_id = ?2",
            params![owner, run_id],
            |row| {
                Ok(LeaseFence {
                    owner: row.get(0)?,
                    run_id: row.get(1)?,
                    lease_owner: row.get(2)?,
                    generation: row.get::<_, i64>(3)? as u64,
                    revision: row.get::<_, i64>(4)? as u64,
                    token: row.get(5)?,
                    policy_digest: row.get(6)?,
                    expires_at_ms: row.get(7)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    /// Fail closed when caller generation does not match durable fence.
    pub fn assert_generation(
        &self,
        owner: &str,
        run_id: &str,
        lease_owner: &str,
        generation: u64,
    ) -> KernelStoreResult<LeaseFence> {
        let fence = self
            .get_lease_fence(owner, run_id)?
            .ok_or(KernelStoreError::NotFound)?;
        if fence.lease_owner != lease_owner || fence.generation != generation {
            return Err(KernelStoreError::Conflict(
                "lease generation or owner fence mismatch".into(),
            ));
        }
        Ok(fence)
    }

    // ── Artifacts ────────────────────────────────────────────────────────

    pub fn put_artifact_meta(&self, meta: &ArtifactMeta) -> KernelStoreResult<()> {
        validate_digest(&meta.digest)?;
        let mut conn = self.connect()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        // Same digest + same path is idempotent; conflicting path for digest fails.
        let existing: Option<(String, i64)> = tx
            .query_row(
                "SELECT path, content_bytes FROM kernel_artifacts
                 WHERE owner = ?1 AND receipt_id = ?2 AND digest = ?3",
                params![meta.owner, meta.receipt_id, meta.digest],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((path, bytes)) = existing {
            if path != meta.path || bytes as u64 != meta.content_bytes {
                return Err(KernelStoreError::Conflict(
                    "artifact digest collision with different metadata".into(),
                ));
            }
            tx.commit()?;
            return Ok(());
        }
        tx.execute(
            "INSERT INTO kernel_artifacts(owner, receipt_id, digest, path, content_bytes, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                meta.owner,
                meta.receipt_id,
                meta.digest,
                meta.path,
                meta.content_bytes as i64,
                meta.created_at_ms
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_artifact_meta(
        &self,
        owner: &str,
        receipt_id: &str,
    ) -> KernelStoreResult<Option<ArtifactMeta>> {
        let conn = self.connect()?;
        conn.query_row(
            "SELECT owner, receipt_id, digest, path, content_bytes, created_at_ms
             FROM kernel_artifacts WHERE owner = ?1 AND receipt_id = ?2
             ORDER BY created_at_ms DESC LIMIT 1",
            params![owner, receipt_id],
            |row| {
                Ok(ArtifactMeta {
                    owner: row.get(0)?,
                    receipt_id: row.get(1)?,
                    digest: row.get(2)?,
                    path: row.get(3)?,
                    content_bytes: row.get::<_, i64>(4)? as u64,
                    created_at_ms: row.get(5)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    // ── Delivery phase ───────────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub fn set_delivery_phase(
        &self,
        owner: &str,
        receipt_id: &str,
        run_id: &str,
        expected_revision: u64,
        event: DeliveryEvent,
        approval_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> KernelStoreResult<(DeliveryPhase, u64)> {
        let mut conn = self.connect()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<(String, i64)> = tx
            .query_row(
                "SELECT phase, revision FROM kernel_delivery
                 WHERE owner = ?1 AND receipt_id = ?2",
                params![owner, receipt_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let insert_new = row.is_none();
        let (current, rev) = match row {
            None => {
                if expected_revision != 0 {
                    return Err(KernelStoreError::StaleRevision {
                        expected: expected_revision,
                        actual: 0,
                    });
                }
                (DeliveryPhase::Running, 0u64)
            }
            Some((phase_s, rev_i)) => {
                let actual = u64::try_from(rev_i).unwrap_or(0);
                if actual != expected_revision {
                    return Err(KernelStoreError::StaleRevision {
                        expected: expected_revision,
                        actual,
                    });
                }
                let phase = DeliveryPhase::parse(&phase_s).ok_or(
                    KernelStoreError::InvalidInput("unknown delivery phase in store"),
                )?;
                (phase, actual)
            }
        };
        let next = approval_fsm::transition(current, event)?;
        let next_rev = rev
            .checked_add(1)
            .ok_or(KernelStoreError::InvalidInput("phase revision overflow"))?;
        let ms = now.timestamp_millis();
        if insert_new {
            tx.execute(
                "INSERT INTO kernel_delivery(owner, receipt_id, run_id, phase, revision, approval_id, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    owner,
                    receipt_id,
                    run_id,
                    next.as_str(),
                    next_rev as i64,
                    approval_id,
                    ms
                ],
            )?;
        } else {
            let n = tx.execute(
                "UPDATE kernel_delivery
                 SET phase = ?1, revision = ?2, approval_id = COALESCE(?3, approval_id),
                     run_id = ?4, updated_at_ms = ?5
                 WHERE owner = ?6 AND receipt_id = ?7 AND revision = ?8",
                params![
                    next.as_str(),
                    next_rev as i64,
                    approval_id,
                    run_id,
                    ms,
                    owner,
                    receipt_id,
                    expected_revision as i64
                ],
            )?;
            if n != 1 {
                return Err(KernelStoreError::StaleRevision {
                    expected: expected_revision,
                    actual: rev,
                });
            }
        }
        tx.commit()?;
        Ok((next, next_rev))
    }

    pub fn get_delivery_phase(
        &self,
        owner: &str,
        receipt_id: &str,
    ) -> KernelStoreResult<Option<(DeliveryPhase, u64)>> {
        let conn = self.connect()?;
        let row: Option<(String, i64)> = conn
            .query_row(
                "SELECT phase, revision FROM kernel_delivery
                 WHERE owner = ?1 AND receipt_id = ?2",
                params![owner, receipt_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        match row {
            None => Ok(None),
            Some((phase_s, rev)) => {
                let phase = DeliveryPhase::parse(&phase_s).ok_or(
                    KernelStoreError::InvalidInput("unknown delivery phase in store"),
                )?;
                Ok(Some((phase, u64::try_from(rev).unwrap_or(0))))
            }
        }
    }

    /// Ensure delivery row exists at Running revision 0→1 bootstrap when absent.
    pub fn ensure_delivery_running(
        &self,
        owner: &str,
        receipt_id: &str,
        run_id: &str,
        now: DateTime<Utc>,
    ) -> KernelStoreResult<(DeliveryPhase, u64)> {
        if let Some(existing) = self.get_delivery_phase(owner, receipt_id)? {
            return Ok(existing);
        }
        let mut conn = self.connect()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT OR IGNORE INTO kernel_delivery(owner, receipt_id, run_id, phase, revision, approval_id, updated_at_ms)
             VALUES (?1, ?2, ?3, 'running', 1, NULL, ?4)",
            params![owner, receipt_id, run_id, now.timestamp_millis()],
        )?;
        tx.commit()?;
        self.get_delivery_phase(owner, receipt_id)?
            .ok_or(KernelStoreError::NotFound)
    }
}

fn load_approval(
    conn: &Connection,
    owner: &str,
    approval_id: &str,
) -> KernelStoreResult<ApprovalDecision> {
    conn.query_row(
        "SELECT owner, approval_id, receipt_id, run_id, request_digest, decision,
                decided_by, bound_revision, bound_lease_owner, bound_generation,
                decided_at_ms, created_at_ms, updated_at_ms
         FROM kernel_approvals WHERE owner = ?1 AND approval_id = ?2",
        params![owner, approval_id],
        |row| {
            let decision_s: String = row.get(5)?;
            Ok(ApprovalDecision {
                owner: row.get(0)?,
                approval_id: row.get(1)?,
                receipt_id: row.get(2)?,
                run_id: row.get(3)?,
                request_digest: row.get(4)?,
                decision: ApprovalDecisionKind::parse(&decision_s).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        5,
                        rusqlite::types::Type::Text,
                        Box::new(std::io::Error::other(e.to_string())),
                    )
                })?,
                decided_by: row.get(6)?,
                bound_revision: row.get::<_, i64>(7)? as u64,
                bound_lease_owner: row.get(8)?,
                bound_generation: row.get::<_, Option<i64>>(9)?.map(|g| g as u64),
                decided_at_ms: row.get(10)?,
                created_at_ms: row.get(11)?,
                updated_at_ms: row.get(12)?,
            })
        },
    )
    .map_err(Into::into)
}

fn final_status_str(status: ReceiptFinalStatus) -> &'static str {
    match status {
        ReceiptFinalStatus::Open => "open",
        ReceiptFinalStatus::Completed => "completed",
        ReceiptFinalStatus::Failed => "failed",
        ReceiptFinalStatus::Cancelled => "cancelled",
        ReceiptFinalStatus::Unresolved => "unresolved",
        _ => "unresolved",
    }
}

fn validate_owner(owner: &str) -> KernelStoreResult<()> {
    if owner.is_empty() || owner.len() > 128 {
        return Err(KernelStoreError::InvalidInput("owner"));
    }
    Ok(())
}

fn validate_id(label: &'static str, value: &str) -> KernelStoreResult<()> {
    if value.is_empty() || value.len() > 256 {
        return Err(KernelStoreError::InvalidInput(label));
    }
    Ok(())
}

fn validate_digest(digest: &str) -> KernelStoreResult<()> {
    if digest.len() != 64 || !digest.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(KernelStoreError::InvalidInput("digest"));
    }
    // Lowercase hex only (same rule as SQL CHECK).
    if digest.chars().any(|c| c.is_ascii_uppercase()) {
        return Err(KernelStoreError::InvalidInput(
            "digest must be lowercase hex",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use vyane_core::{
        BillingModeCategory, EndpointClass, HarnessKind, ModelId, Protocol, ProviderId, RiskClass,
        RouteConfig, TaskCase,
    };

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 4, 15, 0, 0).single().unwrap()
    }

    fn sample_receipt(id: &str, owner: &str) -> CompletionReceipt {
        let task = TaskCase {
            task_case_id: id.into(),
            task_type: "process_lane_autonomous_delivery".into(),
            acceptance_digest: "a".repeat(64),
            truth_probe_digest: "b".repeat(64),
            risk_class: RiskClass::WorkspaceWrite,
        };
        let route = RouteConfig {
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
        };
        CompletionReceipt::open(id, owner, task, route, now()).unwrap()
    }

    #[test]
    fn effect_once_and_reopen_no_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kernel.sqlite");
        let store = KernelStore::open(&path).unwrap();
        let dig = "c".repeat(64);
        assert!(
            store
                .apply_effect_once("o", "effect:1", &dig, Some("run"), Some("r"), now())
                .unwrap()
        );
        assert!(
            !store
                .apply_effect_once("o", "effect:1", &dig, Some("run"), Some("r"), now())
                .unwrap()
        );
        drop(store);
        let store2 = KernelStore::open(&path).unwrap();
        assert!(store2.was_effect_applied("o", "effect:1"));
        assert_eq!(store2.effect_count("o").unwrap(), 1);
        let err = store2
            .apply_effect_once("o", "effect:1", &"d".repeat(64), None, None, now())
            .unwrap_err();
        assert!(matches!(err, KernelStoreError::DuplicateEffect { .. }));
    }

    #[test]
    fn stale_lease_generation_cannot_overwrite_fence() {
        let dir = tempfile::tempdir().unwrap();
        let store = KernelStore::open(dir.path().join("k.sqlite")).unwrap();
        let high = LeaseFence {
            owner: "o".into(),
            run_id: "run".into(),
            lease_owner: "lease-a".into(),
            generation: 5,
            revision: 3,
            token: "tok-a".into(),
            policy_digest: "p".repeat(64),
            expires_at_ms: None,
        };
        store.put_lease_fence(&high, now()).unwrap();
        let low = LeaseFence {
            generation: 2,
            lease_owner: "lease-stale".into(),
            token: "tok-stale".into(),
            ..high.clone()
        };
        let err = store.put_lease_fence(&low, now()).unwrap_err();
        assert!(matches!(err, KernelStoreError::Conflict(_)));
        let got = store.get_lease_fence("o", "run").unwrap().unwrap();
        assert_eq!(got.generation, 5);
        assert_eq!(got.lease_owner, "lease-a");
        // Same generation, different owner: fail closed.
        let steal = LeaseFence {
            generation: 5,
            lease_owner: "lease-thief".into(),
            ..high
        };
        assert!(matches!(
            store.put_lease_fence(&steal, now()).unwrap_err(),
            KernelStoreError::Conflict(_)
        ));
    }

    #[test]
    fn concurrent_schema_init_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("k.sqlite");
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                KernelStore::open(&path)
            }));
        }
        for h in handles {
            h.join().unwrap().unwrap();
        }
        let store = KernelStore::open(&path).unwrap();
        assert_eq!(KernelStore::schema_version(), 1);
        // Usable after concurrent open.
        let dig = "f".repeat(64);
        assert!(
            store
                .apply_effect_once("o", "e1", &dig, None, None, now())
                .unwrap()
        );
    }

    #[test]
    fn multi_process_effect_race_only_one_first() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kernel.sqlite");
        // Initialize schema once.
        let _ = KernelStore::open(&path).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let dig = "e".repeat(64);
        let mut handles = Vec::new();
        for _ in 0..2 {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            let dig = dig.clone();
            handles.push(thread::spawn(move || {
                let store = KernelStore::open(&path).unwrap();
                barrier.wait();
                store.apply_effect_once("owner", "effect:race", &dig, None, None, now())
            }));
        }
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let firsts = results.iter().filter(|r| matches!(r, Ok(true))).count();
        let seconds = results.iter().filter(|r| matches!(r, Ok(false))).count();
        assert_eq!(firsts, 1, "exactly one first apply: {results:?}");
        assert_eq!(seconds, 1, "exactly one idempotent: {results:?}");
        let store = KernelStore::open(&path).unwrap();
        assert_eq!(store.effect_count("owner").unwrap(), 1);
    }

    #[test]
    fn receipt_cas_and_owner_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let store = KernelStore::open(dir.path().join("k.sqlite")).unwrap();
        let r = sample_receipt("rcpt-1", "alice");
        store.insert_open_receipt(&r).unwrap();
        assert!(store.get_receipt("bob", "rcpt-1").unwrap().is_none());
        let updated = store
            .transition_receipt("alice", "rcpt-1", 1, now(), |rec| {
                rec.validation_summary = Some("ok".into());
                Ok(())
            })
            .unwrap();
        assert_eq!(updated.revision, 2);
        let err = store
            .transition_receipt("alice", "rcpt-1", 1, now(), |_| Ok(()))
            .unwrap_err();
        assert!(matches!(err, KernelStoreError::StaleRevision { .. }));
    }

    #[test]
    fn unknown_schema_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("k.sqlite");
        {
            let _ = KernelStore::open(&path).unwrap();
            let conn = Connection::open(&path).unwrap();
            conn.pragma_update(None, "user_version", 99u32).unwrap();
        }
        let err = KernelStore::open(&path).unwrap_err();
        assert!(matches!(
            err,
            KernelStoreError::UnsupportedSchema {
                found: 99,
                supported: 1
            }
        ));
    }

    #[test]
    fn approval_grant_binding_and_deny_final() {
        let dir = tempfile::tempdir().unwrap();
        let store = KernelStore::open(dir.path().join("k.sqlite")).unwrap();
        let dig = "a".repeat(64);
        store
            .record_approval_required("o", "ap1", "rcpt", "run", &dig, 3, now())
            .unwrap();
        let wrong = ApprovalGrantBinding {
            owner: "o".into(),
            receipt_id: "rcpt".into(),
            run_id: "run".into(),
            request_digest: dig.clone(),
            expected_revision: 99,
            lease_owner: "lease".into(),
            generation: 1,
            decided_by: "principal".into(),
        };
        assert!(matches!(
            store.grant_approval(&wrong, now()).unwrap_err(),
            KernelStoreError::ApprovalBindingMismatch
        ));
        let ok = ApprovalGrantBinding {
            expected_revision: 3,
            ..wrong.clone()
        };
        let d = store.grant_approval(&ok, now()).unwrap();
        assert_eq!(d.decision, ApprovalDecisionKind::Approved);
        // Idempotent
        let d2 = store.grant_approval(&ok, now()).unwrap();
        assert_eq!(d2.decision, ApprovalDecisionKind::Approved);

        // Fresh deny path
        let dig2 = "b".repeat(64);
        store
            .record_approval_required("o", "ap2", "rcpt2", "run2", &dig2, 1, now())
            .unwrap();
        store
            .deny_approval("o", "rcpt2", &dig2, "principal", now())
            .unwrap();
        let grant_after_deny = ApprovalGrantBinding {
            owner: "o".into(),
            receipt_id: "rcpt2".into(),
            run_id: "run2".into(),
            request_digest: dig2,
            expected_revision: 1,
            lease_owner: "lease".into(),
            generation: 1,
            decided_by: "principal".into(),
        };
        assert!(matches!(
            store.grant_approval(&grant_after_deny, now()).unwrap_err(),
            KernelStoreError::ApprovalDeniedFinal
        ));
    }

    #[test]
    fn delivery_phase_ask_grant_resume() {
        let dir = tempfile::tempdir().unwrap();
        let store = KernelStore::open(dir.path().join("k.sqlite")).unwrap();
        let (p, rev) = store
            .ensure_delivery_running("o", "rcpt", "run", now())
            .unwrap();
        assert_eq!(p, DeliveryPhase::Running);
        let (p, rev) = store
            .set_delivery_phase(
                "o",
                "rcpt",
                "run",
                rev,
                DeliveryEvent::AskRequired,
                Some("ap1"),
                now(),
            )
            .unwrap();
        assert_eq!(p, DeliveryPhase::ApprovalRequired);
        let (p, rev) = store
            .set_delivery_phase(
                "o",
                "rcpt",
                "run",
                rev,
                DeliveryEvent::GrantAccepted,
                Some("ap1"),
                now(),
            )
            .unwrap();
        assert_eq!(p, DeliveryPhase::Approved);
        let (p, _) = store
            .set_delivery_phase(
                "o",
                "rcpt",
                "run",
                rev,
                DeliveryEvent::ResumeStarted,
                None,
                now(),
            )
            .unwrap();
        assert_eq!(p, DeliveryPhase::Resuming);
    }

    #[test]
    fn unknown_cost_not_zero_on_receipt_attempt_path() {
        // CostEvidence default keeps actual_micro_units = None (not 0).
        let dir = tempfile::tempdir().unwrap();
        let store = KernelStore::open(dir.path().join("k.sqlite")).unwrap();
        let mut r = sample_receipt("rcpt-cost", "o");
        store.insert_open_receipt(&r).unwrap();
        r = store
            .transition_receipt("o", "rcpt-cost", 1, now(), |rec| {
                use vyane_core::{
                    AttemptFailureClass, AttemptStatus, CostEvidence, ReceiptAttempt,
                };
                rec.attempts.push(ReceiptAttempt {
                    attempt_id: "a1".into(),
                    parent_attempt_id: None,
                    agent_run_id: "run".into(),
                    owner: "o".into(),
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
                Ok(())
            })
            .unwrap();
        assert!(r.attempts[0].cost.actual_micro_units.is_none());
        assert!(r.attempts[0].cost.estimated_micro_units.is_none());
        let json = r.to_canonical_json().unwrap();
        assert!(!json.contains("\"actual_micro_units\":0"));
    }
}
