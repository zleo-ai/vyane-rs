use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{StreamExt as _, stream};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{QuotaCard, model::validate_connector_identity};

pub const MAX_CONNECTORS: usize = 32;
pub const MAX_QUOTA_CONCURRENCY: usize = 16;
const MAX_BODY_BYTES_CEILING: usize = 4 * 1024 * 1024;
const MAX_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaReadPolicy {
    pub timeout: Duration,
    pub max_body_bytes: usize,
}

impl Default for QuotaReadPolicy {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            max_body_bytes: 1024 * 1024,
        }
    }
}

impl QuotaReadPolicy {
    pub fn validate(self) -> Result<Self, QuotaRunnerError> {
        if self.timeout.is_zero() || self.timeout > MAX_TIMEOUT {
            return Err(QuotaRunnerError::InvalidPolicy);
        }
        if self.max_body_bytes == 0 || self.max_body_bytes > MAX_BODY_BYTES_CEILING {
            return Err(QuotaRunnerError::InvalidPolicy);
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaConnectorErrorCode {
    Unavailable,
    Authentication,
    RateLimited,
    InvalidResponse,
    Internal,
}

impl QuotaConnectorErrorCode {
    /// Stable snake_case token matching the serde rename for this error code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::Authentication => "authentication",
            Self::RateLimited => "rate_limited",
            Self::InvalidResponse => "invalid_response",
            Self::Internal => "internal",
        }
    }
}

impl fmt::Display for QuotaConnectorErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("quota connector failed with {code}")]
pub struct QuotaConnectorError {
    pub code: QuotaConnectorErrorCode,
}

impl QuotaConnectorError {
    #[must_use]
    pub const fn new(code: QuotaConnectorErrorCode) -> Self {
        Self { code }
    }
}

#[async_trait]
pub trait QuotaConnector: Send + Sync {
    fn id(&self) -> &str;
    fn provider(&self) -> &str;

    /// Read one current snapshot. Implementations own and trust their fixed
    /// endpoint; they must not pass caller-controlled URLs through to the
    /// transport. They must also treat `policy` as a ceiling;
    /// [`crate::QuotaHttpReader`] is the provided redirect-closed,
    /// body-bounded transport helper.
    async fn snapshot(&self, policy: QuotaReadPolicy) -> Result<QuotaCard, QuotaConnectorError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaSnapshotStatus {
    Ok,
    Error,
    Timeout,
}

impl QuotaSnapshotStatus {
    /// Stable snake_case token matching the serde rename for this snapshot status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Timeout => "timeout",
        }
    }
}

impl fmt::Display for QuotaSnapshotStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaSnapshot {
    pub connector_id: String,
    pub provider: String,
    pub checked_at: DateTime<Utc>,
    pub status: QuotaSnapshotStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub card: Option<QuotaCard>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<QuotaConnectorErrorCode>,
}

pub struct QuotaSnapshotRunner {
    connectors: Vec<Arc<dyn QuotaConnector>>,
    concurrency: usize,
    policy: QuotaReadPolicy,
}

impl QuotaSnapshotRunner {
    pub fn new(
        connectors: Vec<Arc<dyn QuotaConnector>>,
        concurrency: usize,
        policy: QuotaReadPolicy,
    ) -> Result<Self, QuotaRunnerError> {
        if connectors.len() > MAX_CONNECTORS
            || concurrency == 0
            || concurrency > MAX_QUOTA_CONCURRENCY
        {
            return Err(QuotaRunnerError::InvalidConfiguration);
        }
        let policy = policy.validate()?;
        let mut ids = HashSet::with_capacity(connectors.len());
        for connector in &connectors {
            validate_connector_identity(connector.id(), connector.provider())
                .map_err(|_| QuotaRunnerError::InvalidConfiguration)?;
            if !ids.insert(connector.id().to_string()) {
                return Err(QuotaRunnerError::DuplicateConnector);
            }
        }
        Ok(Self {
            connectors,
            concurrency,
            policy,
        })
    }

    pub async fn snapshot(&self) -> Vec<QuotaSnapshot> {
        let policy = self.policy;
        let mut snapshots = stream::iter(self.connectors.iter().cloned())
            .map(|connector| async move {
                let connector_id = connector.id().to_string();
                let provider = connector.provider().to_string();
                let result = tokio::time::timeout(policy.timeout, connector.snapshot(policy)).await;
                let checked_at = Utc::now();
                match result {
                    Err(_) => QuotaSnapshot {
                        connector_id,
                        provider,
                        checked_at,
                        status: QuotaSnapshotStatus::Timeout,
                        card: None,
                        error: None,
                    },
                    Ok(Err(error)) => QuotaSnapshot {
                        connector_id,
                        provider,
                        checked_at,
                        status: QuotaSnapshotStatus::Error,
                        card: None,
                        error: Some(error.code),
                    },
                    Ok(Ok(card)) => {
                        if card.connector_id != connector_id
                            || card.provider != provider
                            || card.validate().is_err()
                        {
                            return QuotaSnapshot {
                                connector_id,
                                provider,
                                checked_at,
                                status: QuotaSnapshotStatus::Error,
                                card: None,
                                error: Some(QuotaConnectorErrorCode::InvalidResponse),
                            };
                        }
                        QuotaSnapshot {
                            connector_id,
                            provider,
                            checked_at,
                            status: QuotaSnapshotStatus::Ok,
                            card: Some(card),
                            error: None,
                        }
                    }
                }
            })
            .buffer_unordered(self.concurrency)
            .collect::<Vec<_>>()
            .await;
        snapshots.sort_by(|left, right| left.connector_id.cmp(&right.connector_id));
        snapshots
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum QuotaRunnerError {
    #[error("invalid quota snapshot runner configuration")]
    InvalidConfiguration,
    #[error("duplicate quota connector id")]
    DuplicateConnector,
    #[error("invalid quota read policy")]
    InvalidPolicy,
}

impl QuotaRunnerError {
    /// Stable snake_case *kind* token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "invalid_configuration",
            Self::DuplicateConnector => "duplicate_connector",
            Self::InvalidPolicy => "invalid_policy",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_runner_error_kind_tokens_are_snake_case() {
        assert_eq!(
            QuotaRunnerError::InvalidConfiguration.as_str(),
            "invalid_configuration"
        );
        assert_eq!(
            QuotaRunnerError::DuplicateConnector.as_str(),
            "duplicate_connector"
        );
        assert_eq!(QuotaRunnerError::InvalidPolicy.as_str(), "invalid_policy");
    }

    #[test]
    fn quota_runner_closed_enum_tokens_match_serde_snake_case() {
        assert_eq!(QuotaConnectorErrorCode::Unavailable.as_str(), "unavailable");
        assert_eq!(
            QuotaConnectorErrorCode::Authentication.to_string(),
            "authentication"
        );
        assert_eq!(
            QuotaConnectorErrorCode::RateLimited.as_str(),
            "rate_limited"
        );
        assert_eq!(
            QuotaConnectorErrorCode::InvalidResponse.to_string(),
            "invalid_response"
        );
        assert_eq!(QuotaConnectorErrorCode::Internal.as_str(), "internal");

        assert_eq!(QuotaSnapshotStatus::Ok.as_str(), "ok");
        assert_eq!(QuotaSnapshotStatus::Error.to_string(), "error");
        assert_eq!(QuotaSnapshotStatus::Timeout.as_str(), "timeout");
    }
}
