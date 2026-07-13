//! Session continuity across runs.
//!
//! Two continuity mechanisms exist, and they are different things:
//!
//! * **Native harness sessions** — a CLI harness (Claude Code, Codex CLI…)
//!   keeps its own session state. The additive [`SessionSnapshot`] contract can
//!   carry an exact [`NativeSessionDomain`] without changing the legacy
//!   [`SessionRecord`] shape. A native id stored only on `SessionRecord` is
//!   [`NativeSessionState::LegacyUnbound`] evidence and is not resumable.
//! * **Transcript sessions** — direct-HTTP chat has no native state; Vyane
//!   itself stores the message transcript and replays it as history.
//!
//! These storage types do not authorize execution or resume. Direct transcript
//! continuation works today; native resume remains fail-closed until a runtime
//! consumer also enforces active permits and exact domain drift checks. The
//! regular dispatch path already holds an execution-period session lease.

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::chat::ChatMessage;
use crate::target::{HarnessKind, ModelId, Protocol, ProviderId, Target};
use crate::workdir::WorkdirIdentity;

/// Reference to an existing session, as given by a caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionRef(pub String);

impl SessionRef {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Persisted session state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id: String,
    /// Owner scope; `"local"` for single-user setups.
    #[serde(default = "default_owner")]
    pub owner: String,
    /// The last target this session ran against.
    pub target: Target,
    /// Native session id inside the harness, when one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_session_id: Option<String>,
    /// Message transcript for direct-chat continuity. Empty for pure
    /// harness sessions.
    #[serde(default)]
    pub transcript: Vec<ChatMessage>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Number of runs recorded against this session.
    #[serde(default)]
    pub run_count: u64,
}

/// Exact, secret-free identity boundary for one native harness session.
///
/// A domain is persistence evidence, not permission to resume. In particular,
/// `canonical_workdir` is only an audit identity; a live execution must still
/// use a process-local pinned directory and revalidate its execution permit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSessionDomain {
    /// Versioned runtime/adapter identity that interprets the native id.
    pub runtime: String,
    pub harness: HarnessKind,
    pub provider: ProviderId,
    pub protocol: Protocol,
    pub model: ModelId,
    /// SHA-256 of the canonical endpoint routing identity. The digest contains
    /// no credentials or plaintext endpoint query values.
    pub endpoint_routing_digest: String,
    /// Canonical path used when the session was created.
    pub canonical_workdir: PathBuf,
    /// Stable object identity observed while pinning `canonical_workdir`.
    pub workdir_identity: WorkdirIdentity,
    /// Versioned namespace and schema used for runtime checkpoints.
    pub checkpoint_namespace: String,
    pub checkpoint_schema: u32,
    /// Secret-free digests of the account and runtime state scopes.
    pub account_scope_digest: String,
    pub runtime_scope_digest: String,
}

/// A native session id and the exact domain in which that id is meaningful.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSessionBinding {
    pub native_session_id: String,
    pub domain: NativeSessionDomain,
}

/// Binding status returned by [`crate::SessionStore::load_snapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum NativeSessionState {
    /// The logical session has never been associated with native state, or was
    /// explicitly reset.
    Absent,
    /// A pre-domain id remains readable for migration/audit, but must never be
    /// interpreted as resumable by inferring a domain from the current target.
    LegacyUnbound { native_session_id: String },
    /// The id and domain were committed together by a domain-aware store.
    Bound { binding: Box<NativeSessionBinding> },
}

/// Revisioned view used by domain-aware session control paths.
///
/// `session_revision` covers every store mutation, including legacy
/// [`SessionStore::save`](crate::SessionStore::save) and
/// [`SessionStore::apply_update`](crate::SessionStore::apply_update) calls, so
/// native transitions can use compare-and-swap without racing those paths.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSnapshot {
    pub record: SessionRecord,
    pub session_revision: u64,
    pub native_session: NativeSessionState,
}

impl SessionSnapshot {
    pub(crate) fn from_legacy_record(record: SessionRecord) -> Self {
        let native_session = match record.native_session_id.as_ref() {
            Some(native_session_id) => NativeSessionState::LegacyUnbound {
                native_session_id: native_session_id.clone(),
            },
            None => NativeSessionState::Absent,
        };
        Self {
            record,
            session_revision: 0,
            native_session,
        }
    }
}

/// One completed run's atomic mutation of a logical session.
///
/// Stores apply this under the same per-session lock that protects persistence.
/// Dispatchers additionally retain a [`SessionExecutionLease`](crate::SessionExecutionLease)
/// from the continuity read through this mutation, so two executions cannot
/// branch from the same prior context. Stores that expose direct mutation APIs
/// must coordinate those mutations with the same execution authority; the
/// filesystem store does so internally.
#[derive(Debug, Clone)]
pub struct SessionUpdate {
    pub owner: String,
    pub session_id: String,
    pub target: Target,
    pub native_session_id: Option<String>,
    pub transcript_delta: Vec<ChatMessage>,
    pub occurred_at: DateTime<Utc>,
}

/// Atomic, revision-fenced mutation of native session state.
///
/// `Commit` is for an initial binding or a completion in the exact existing
/// binding. It must reject legacy ids and binding drift. `ForkFresh` is the
/// explicit migration/replacement path after a fresh native run. Both carry
/// the completed run update so the logical record and native binding publish
/// in one atomic store write. `Reset` only removes native state.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum NativeSessionTransition {
    Reset {
        expected_revision: u64,
    },
    ForkFresh {
        expected_revision: u64,
        update: SessionUpdate,
        binding: NativeSessionBinding,
    },
    Commit {
        expected_revision: u64,
        update: SessionUpdate,
        binding: NativeSessionBinding,
    },
}

impl NativeSessionTransition {
    #[must_use]
    pub fn expected_revision(&self) -> u64 {
        match self {
            Self::Reset { expected_revision }
            | Self::ForkFresh {
                expected_revision, ..
            }
            | Self::Commit {
                expected_revision, ..
            } => *expected_revision,
        }
    }
}

impl SessionUpdate {
    #[must_use]
    pub fn apply_to(&self, existing: Option<SessionRecord>) -> SessionRecord {
        let mut record = existing.unwrap_or_else(|| SessionRecord {
            session_id: self.session_id.clone(),
            owner: self.owner.clone(),
            target: self.target.clone(),
            native_session_id: None,
            transcript: Vec::new(),
            created_at: self.occurred_at,
            updated_at: self.occurred_at,
            run_count: 0,
        });
        record.target = self.target.clone();
        record.updated_at = record.updated_at.max(self.occurred_at);
        record.run_count = record.run_count.saturating_add(1);
        if let Some(native_session_id) = self.native_session_id.as_ref() {
            record.native_session_id = Some(native_session_id.clone());
        }
        record.transcript.extend(self.transcript_delta.clone());
        record
    }
}

fn default_owner() -> String {
    "local".to_string()
}
