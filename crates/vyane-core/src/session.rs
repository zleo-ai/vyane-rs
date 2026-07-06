//! Session continuity across runs.
//!
//! Two continuity mechanisms exist, and they are different things:
//!
//! * **Native harness sessions** — a CLI harness (Claude Code, Codex CLI…)
//!   keeps its own session state; Vyane stores the native session id and
//!   passes the appropriate resume flag on the next run.
//! * **Transcript sessions** — direct-HTTP chat has no native state; Vyane
//!   itself stores the message transcript and replays it as history.
//!
//! A [`SessionRecord`] can carry both: a topic session may hop between a
//! harness target and a direct-chat target while keeping one logical id.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::chat::ChatMessage;
use crate::target::Target;

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

fn default_owner() -> String {
    "local".to_string()
}
