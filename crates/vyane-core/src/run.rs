//! Run records — the ledger's unit of truth.
//!
//! Every dispatch produces exactly one [`RunRecord`], whatever happened.
//! Failover attempts are recorded inside the run, so "which target actually
//! served this" is always answerable after the fact. Records deliberately
//! store a prompt *digest* rather than the full prompt by default: the
//! ledger is for accounting and observability, not for silently archiving
//! possibly-sensitive task text.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::ErrorKind;
use crate::target::{AdapterTransport, ProviderId, Sandbox, Target};

/// Token usage, normalized across protocols.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// Reasoning/thinking tokens when reported separately.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_input_tokens: Option<u64>,
}

impl Usage {
    pub fn add(&mut self, other: &Usage) {
        self.input_tokens += other.input_tokens;
        self.output_tokens += other.output_tokens;
        if let Some(r) = other.reasoning_tokens {
            *self.reasoning_tokens.get_or_insert(0) += r;
        }
        if let Some(c) = other.cached_input_tokens {
            *self.cached_input_tokens.get_or_insert(0) += c;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Success,
    Error,
    Timeout,
    Cancelled,
}

/// Outcome of a single attempt against one target.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "result")]
pub enum AttemptOutcome {
    Ok,
    Err {
        kind: ErrorKind,
        message: String,
        /// Whether this error made the kernel move to the next target.
        failed_over: bool,
    },
}

/// One attempt within a run (the failover chain is `Vec<Attempt>`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    pub target: Target,
    pub transport: AdapterTransport,
    pub started_at: DateTime<Utc>,
    pub duration_ms: u64,
    pub outcome: AttemptOutcome,
}

/// The persisted record of one dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    /// UUIDv7 — time-ordered, globally unique.
    pub run_id: String,
    /// Owner scope. Single-user setups use `"local"`. Present from day one
    /// so multi-user isolation never needs a schema retrofit.
    #[serde(default = "default_owner")]
    pub owner: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    /// SHA-256 (hex, first 16 chars) of the prompt text.
    pub task_digest: String,
    /// First ~120 chars of the prompt, for human scanning. Configurable off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    pub sandbox: Sandbox,
    /// The target that produced the final outcome (last attempt).
    pub target: Target,
    pub transport: AdapterTransport,
    /// Full failover chain, in order. Length 1 = no failover.
    pub attempts: Vec<Attempt>,
    pub status: RunStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
    /// Session this run belonged to / created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_chars: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Free-form labels copied from the task spec.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub labels: std::collections::BTreeMap<String, String>,
}

fn default_owner() -> String {
    "local".to_string()
}

/// Filter for querying the ledger.
#[derive(Debug, Clone, Default)]
pub struct RunQuery {
    pub owner: Option<String>,
    pub provider: Option<ProviderId>,
    pub status: Option<RunStatus>,
    pub since: Option<DateTime<Utc>>,
    /// Most-recent-first limit. `None` = implementation default.
    pub limit: Option<usize>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn run_record_json_roundtrip() {
        let rec = RunRecord {
            run_id: "0198c0de-0000-7000-8000-000000000000".into(),
            owner: default_owner(),
            started_at: Utc::now(),
            finished_at: Utc::now(),
            task_digest: "abcd1234abcd1234".into(),
            task_preview: Some("say hi".into()),
            workdir: None,
            sandbox: Sandbox::ReadOnly,
            target: Target {
                provider: ProviderId::new("openai"),
                protocol: crate::target::Protocol::OpenaiChat,
                harness: None,
                model: crate::target::ModelId::new("gpt-x"),
            },
            transport: AdapterTransport::DirectHttp,
            attempts: vec![],
            status: RunStatus::Success,
            usage: Some(Usage {
                input_tokens: 10,
                output_tokens: 5,
                ..Default::default()
            }),
            cost_usd: None,
            session_id: None,
            output_chars: Some(2),
            error: None,
            labels: Default::default(),
        };
        let json = serde_json::to_string(&rec).unwrap();
        let back: RunRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.run_id, rec.run_id);
        assert_eq!(back.status, RunStatus::Success);
    }

    #[test]
    fn missing_owner_defaults_to_local() {
        // Records written before multi-user support must stay readable.
        let json = r#"{
            "run_id":"r1","started_at":"2026-01-01T00:00:00Z",
            "finished_at":"2026-01-01T00:00:01Z","task_digest":"d",
            "sandbox":"read-only",
            "target":{"provider":"p","protocol":"openai_chat","harness":null,"model":"m"},
            "transport":"direct_http","attempts":[],"status":"success"
        }"#;
        let rec: RunRecord = serde_json::from_str(json).unwrap();
        assert_eq!(rec.owner, "local");
    }
}
