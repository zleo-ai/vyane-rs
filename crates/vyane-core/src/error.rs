//! Error taxonomy.
//!
//! Every error carries an [`ErrorKind`] that is serializable into the ledger
//! and drives failover decisions. The kind classification is part of the
//! kernel's contract: adapters must map their failures onto it faithfully,
//! because a wrong kind silently changes failover behaviour.

use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, VyaneError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorKind {
    /// Invalid or missing configuration. Not retryable — fix the config.
    Config,
    /// Authentication / authorization failure (401/403, bad key).
    Auth,
    /// Rate limited or quota exhausted (429).
    RateLimited,
    /// The run exceeded its caller-specified timeout.
    Timeout,
    /// Network-level failure (DNS, connect, TLS, broken stream).
    Transport,
    /// The endpoint answered with a protocol-level error (5xx, malformed
    /// response, refused request).
    Protocol,
    /// The harness binary could not be spawned (missing, not executable).
    SpawnFailed,
    /// The harness ran but exited unsuccessfully.
    HarnessFailed,
    /// Cancelled by the caller.
    Cancelled,
    /// The target does not support the requested capability (e.g. streaming).
    Unsupported,
    /// A referenced entity (session, profile, run) does not exist.
    NotFound,
    /// Local I/O failure (ledger, config files).
    Io,
    /// Anything else.
    Other,
}

impl ErrorKind {
    /// Whether an error of this kind should trigger failover to the next
    /// target in the chain.
    ///
    /// Deterministic caller-side mistakes (`Config`, `NotFound`,
    /// `Unsupported`) and explicit cancellation must abort instead of
    /// failing over: retrying them elsewhere either can't succeed or does
    /// something the caller didn't ask for.
    pub fn failover_eligible(&self) -> bool {
        match self {
            ErrorKind::Auth
            | ErrorKind::RateLimited
            | ErrorKind::Timeout
            | ErrorKind::Transport
            | ErrorKind::Protocol
            | ErrorKind::SpawnFailed
            | ErrorKind::HarnessFailed => true,
            ErrorKind::Config
            | ErrorKind::Cancelled
            | ErrorKind::Unsupported
            | ErrorKind::NotFound
            | ErrorKind::Io
            | ErrorKind::Other => false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{kind:?}: {message}")]
pub struct VyaneError {
    pub kind: ErrorKind,
    pub message: String,
    #[source]
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl VyaneError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source(
        kind: ErrorKind,
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    pub fn config(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Config, message)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unsupported, message)
    }

    pub fn cancelled() -> Self {
        Self::new(ErrorKind::Cancelled, "cancelled by caller")
    }

    pub fn failover_eligible(&self) -> bool {
        self.kind.failover_eligible()
    }
}

impl From<std::io::Error> for VyaneError {
    fn from(e: std::io::Error) -> Self {
        Self::with_source(ErrorKind::Io, e.to_string(), e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_errors_do_not_fail_over() {
        assert!(!VyaneError::config("missing key").failover_eligible());
        assert!(VyaneError::new(ErrorKind::RateLimited, "429").failover_eligible());
    }
}
