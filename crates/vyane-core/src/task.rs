//! What the caller asks for: a task plus generation parameters.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::session::SessionRef;
use crate::target::Sandbox;

/// Reasoning-effort level, passed through to targets that support it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Effort {
    Low,
    Medium,
    High,
    Xhigh,
}

impl Effort {
    pub fn as_str(&self) -> &str {
        match self {
            Effort::Low => "low",
            Effort::Medium => "medium",
            Effort::High => "high",
            Effort::Xhigh => "xhigh",
        }
    }
}

/// Generation parameters, normalized across protocols.
///
/// Each protocol client maps these onto its own wire fields (for example
/// `max_output_tokens` becomes the appropriate output-limit field per
/// protocol). Reasoning models may count "thinking" tokens against output
/// limits — leaving `max_output_tokens` unset is the safe default.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GenParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<Effort>,
    /// Provider/protocol-specific passthrough values, applied last.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// One unit of work submitted to the kernel.
#[derive(Debug, Clone)]
pub struct TaskSpec {
    /// The task / prompt text.
    pub prompt: String,
    /// Optional system prompt (direct chat) or appended instructions (harness).
    pub system: Option<String>,
    /// Working directory for harness runs. Ignored by direct chat.
    pub workdir: Option<PathBuf>,
    pub sandbox: Sandbox,
    /// Continue an existing session instead of starting fresh.
    pub session: Option<SessionRef>,
    /// `None` = no timeout. Long agentic runs legitimately take hours;
    /// timeouts are opt-in, not a hidden default.
    pub timeout: Option<Duration>,
    /// Free-form labels recorded into the ledger (task tags, ticket ids…).
    pub labels: BTreeMap<String, String>,
}

impl TaskSpec {
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            system: None,
            workdir: None,
            sandbox: Sandbox::default(),
            session: None,
            timeout: None,
            labels: BTreeMap::new(),
        }
    }

    pub fn with_workdir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.workdir = Some(dir.into());
        self
    }

    pub fn with_sandbox(mut self, sandbox: Sandbox) -> Self {
        self.sandbox = sandbox;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}
