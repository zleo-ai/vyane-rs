//! Protocol-neutral chat types for direct-HTTP targets.

use serde::{Deserialize, Serialize};

use crate::run::Usage;
use crate::target::ModelId;
use crate::task::GenParams;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: Role,
    pub content: String,
}

impl ChatMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: Role::System,
            content: content.into(),
        }
    }
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: content.into(),
        }
    }
    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: Role::Assistant,
            content: content.into(),
        }
    }
}

/// A protocol-neutral chat request. Protocol clients translate this into
/// their wire format (system message placement differs per protocol).
#[derive(Debug, Clone)]
pub struct ChatRequest {
    pub model: ModelId,
    pub messages: Vec<ChatMessage>,
    pub params: GenParams,
}

/// The result of a non-streaming chat call.
#[derive(Debug, Clone, Default)]
pub struct ChatOutcome {
    pub text: String,
    pub usage: Option<Usage>,
    /// Model id echoed by the server, when present. Useful for verifying
    /// that a relay actually served the model you asked for.
    pub model_echo: Option<String>,
    pub finish_reason: Option<String>,
}

/// Streaming events, normalized across protocols.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// A fragment of the answer text.
    Delta(String),
    /// A fragment of reasoning/thinking output (not all targets emit these;
    /// some relays emit none even when the model reasons — never rely on
    /// reasoning deltas for liveness).
    ReasoningDelta(String),
    /// Token usage, typically once near the end of the stream.
    Usage(Usage),
    /// Stream finished cleanly.
    Done { finish_reason: Option<String> },
}
