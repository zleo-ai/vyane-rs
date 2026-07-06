//! The four-layer target model: provider / protocol / harness / model,
//! plus the resolved forms the kernel executes against.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Who supplies the endpoint, credentials, quota and billing.
///
/// Provider ids are configuration-defined, not a closed set: `"openai"`,
/// `"anthropic"`, `"my-relay"` are all valid. An OpenAI-compatible relay is a
/// provider in its own right — never conflate it with the protocol it speaks.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderId(pub String);

impl ProviderId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProviderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A concrete model id, only meaningful within one provider.
///
/// Failover must never carry a model id from one provider to another unless
/// the configuration explicitly declares them compatible.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ModelId(pub String);

impl ModelId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ModelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The wire format of a chat request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Protocol {
    OpenaiChat,
    OpenaiResponses,
    AnthropicMessages,
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Protocol::OpenaiChat => "openai_chat",
            Protocol::OpenaiResponses => "openai_responses",
            Protocol::AnthropicMessages => "anthropic_messages",
        };
        f.write_str(s)
    }
}

/// The execution shell a run works inside.
///
/// A harness decides workspace capabilities: files, shell, tools, MCP,
/// long-lived sessions, sandboxing. Direct HTTP chat has *no* harness — that
/// is expressed as `Option<HarnessKind>::None` on [`Target`], never as a
/// pseudo-harness value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HarnessKind {
    ClaudeCode,
    CodexCli,
    OpenCode,
    Other(String),
}

impl HarnessKind {
    pub fn as_str(&self) -> &str {
        match self {
            HarnessKind::ClaudeCode => "claude-code",
            HarnessKind::CodexCli => "codex-cli",
            HarnessKind::OpenCode => "opencode",
            HarnessKind::Other(s) => s,
        }
    }
}

impl From<&str> for HarnessKind {
    fn from(s: &str) -> Self {
        match s {
            "claude-code" => HarnessKind::ClaudeCode,
            "codex-cli" => HarnessKind::CodexCli,
            "opencode" => HarnessKind::OpenCode,
            other => HarnessKind::Other(other.to_string()),
        }
    }
}

impl fmt::Display for HarnessKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for HarnessKind {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for HarnessKind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(HarnessKind::from(s.as_str()))
    }
}

/// How the kernel reaches the execution target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AdapterTransport {
    /// Spawn a CLI harness as a subprocess.
    CliWrap,
    /// Speak the protocol directly over HTTP. No workspace capabilities.
    DirectHttp,
}

/// Workspace permission level for a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Sandbox {
    /// May read the workspace, may not modify anything.
    #[default]
    ReadOnly,
    /// May modify files inside the working directory.
    Write,
    /// Unrestricted. Dangerous; reserve for isolated worktrees.
    Full,
}

/// A resolved execution target: the four layers, pinned.
///
/// This is the loggable identity of "where a run went". It carries no
/// credentials and is safe to serialize into the ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    pub provider: ProviderId,
    pub protocol: Protocol,
    /// `None` = direct chat with no workspace capabilities.
    pub harness: Option<HarnessKind>,
    pub model: ModelId,
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.harness {
            Some(h) => write!(
                f,
                "{}/{} via {} ({})",
                self.provider, self.model, h, self.protocol
            ),
            None => write!(f, "{}/{} ({})", self.provider, self.model, self.protocol),
        }
    }
}

/// A secret string with redacted `Debug`/`Display`.
///
/// Deliberately *not* `Serialize` — secrets must never reach the ledger or
/// any other persisted record.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    /// Access the raw value. Call sites should be easy to audit.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

/// How the credential is presented on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthStyle {
    /// `Authorization: Bearer <key>` (OpenAI-style; also what some
    /// Anthropic-compatible endpoints require via auth-token env).
    Bearer,
    /// `x-api-key: <key>` header (Anthropic-style).
    XApiKey,
}

/// Credential material for one endpoint.
#[derive(Debug, Clone)]
pub struct AuthMaterial {
    pub style: AuthStyle,
    pub secret: Secret,
}

/// A reachable endpoint: base URL plus optional credential.
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub base_url: String,
    /// `None` = the harness authenticates natively (its own login/subscription).
    pub auth: Option<AuthMaterial>,
}

/// Everything the kernel needs to execute against one target:
/// identity + endpoint + generation parameters + transport.
#[derive(Debug, Clone)]
pub struct BoundTarget {
    pub target: Target,
    pub transport: AdapterTransport,
    /// `None` only makes sense for `CliWrap` targets whose harness uses its
    /// own native authentication.
    pub endpoint: Option<Endpoint>,
    pub params: crate::task::GenParams,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn secret_debug_is_redacted() {
        let s = Secret::new("sk-super-secret");
        assert_eq!(format!("{s:?}"), "Secret(***)");
    }

    #[test]
    fn harness_kind_roundtrip() {
        for name in ["claude-code", "codex-cli", "opencode", "weird-shell"] {
            let kind = HarnessKind::from(name);
            assert_eq!(kind.as_str(), name);
            let json = serde_json::to_string(&kind).unwrap();
            let back: HarnessKind = serde_json::from_str(&json).unwrap();
            assert_eq!(kind, back);
        }
    }

    #[test]
    fn target_display_reads_naturally() {
        let t = Target {
            provider: ProviderId::new("anthropic"),
            protocol: Protocol::AnthropicMessages,
            harness: Some(HarnessKind::ClaudeCode),
            model: ModelId::new("claude-opus-4-8"),
        };
        assert_eq!(
            t.to_string(),
            "anthropic/claude-opus-4-8 via claude-code (anthropic_messages)"
        );
    }
}
