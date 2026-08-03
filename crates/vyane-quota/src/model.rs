use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAX_CONNECTOR_ID_BYTES: usize = 128;
pub const MAX_PROVIDER_BYTES: usize = 128;
pub const MAX_QUOTA_WINDOWS: usize = 16;
const MAX_WINDOW_ID_BYTES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaStatus {
    Available,
    Limited,
    Exhausted,
    Unknown,
}

impl QuotaStatus {
    /// Stable snake_case token matching the serde rename for this status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Limited => "limited",
            Self::Exhausted => "exhausted",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for QuotaStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaUnit {
    Requests,
    Tokens,
    Credits,
    UsdMicros,
}

impl QuotaUnit {
    /// Stable snake_case token matching the serde rename for this unit.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::Tokens => "tokens",
            Self::Credits => "credits",
            Self::UsdMicros => "usd_micros",
        }
    }
}

impl fmt::Display for QuotaUnit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaBalance {
    pub unit: QuotaUnit,
    pub remaining: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

impl QuotaBalance {
    fn validate(&self) -> Result<(), QuotaValidationError> {
        if self.limit.is_some_and(|limit| self.remaining > limit) {
            return Err(QuotaValidationError::InvalidBalance);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaWindow {
    pub id: String,
    /// Used portion in basis points, from 0 through 10_000 inclusive.
    pub used_basis_points: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_at: Option<DateTime<Utc>>,
}

impl QuotaWindow {
    fn validate(&self) -> Result<(), QuotaValidationError> {
        validate_identifier("window id", &self.id, MAX_WINDOW_ID_BYTES)?;
        if self.used_basis_points > 10_000 {
            return Err(QuotaValidationError::InvalidWindowUsage);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaCard {
    pub connector_id: String,
    pub provider: String,
    pub status: QuotaStatus,
    pub checked_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub windows: Vec<QuotaWindow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balance: Option<QuotaBalance>,
}

impl QuotaCard {
    pub fn validate(&self) -> Result<(), QuotaValidationError> {
        validate_identifier("connector id", &self.connector_id, MAX_CONNECTOR_ID_BYTES)?;
        validate_identifier("provider", &self.provider, MAX_PROVIDER_BYTES)?;
        if self.windows.len() > MAX_QUOTA_WINDOWS {
            return Err(QuotaValidationError::TooManyWindows);
        }
        for window in &self.windows {
            window.validate()?;
        }
        if let Some(balance) = &self.balance {
            balance.validate()?;
        }
        if self.status == QuotaStatus::Exhausted
            && self
                .balance
                .as_ref()
                .is_some_and(|balance| balance.remaining > 0)
        {
            return Err(QuotaValidationError::StatusContradictsBalance);
        }
        Ok(())
    }
}

pub(crate) fn validate_connector_identity(
    id: &str,
    provider: &str,
) -> Result<(), QuotaValidationError> {
    validate_identifier("connector id", id, MAX_CONNECTOR_ID_BYTES)?;
    validate_identifier("provider", provider, MAX_PROVIDER_BYTES)
}

fn validate_identifier(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), QuotaValidationError> {
    if value.is_empty()
        || value.len() > max_bytes
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(QuotaValidationError::InvalidIdentifier { field });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum QuotaValidationError {
    #[error("invalid {field}")]
    InvalidIdentifier { field: &'static str },
    #[error("quota card has too many windows")]
    TooManyWindows,
    #[error("quota window usage is outside 0..=10000 basis points")]
    InvalidWindowUsage,
    #[error("quota balance remaining exceeds its limit")]
    InvalidBalance,
    #[error("quota status contradicts the normalized balance")]
    StatusContradictsBalance,
}

impl QuotaValidationError {
    /// Stable snake_case *kind* token; field names stay out of the token.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidIdentifier { .. } => "invalid_identifier",
            Self::TooManyWindows => "too_many_windows",
            Self::InvalidWindowUsage => "invalid_window_usage",
            Self::InvalidBalance => "invalid_balance",
            Self::StatusContradictsBalance => "status_contradicts_balance",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_validation_error_kind_tokens_are_snake_case_without_payload() {
        assert_eq!(
            QuotaValidationError::InvalidIdentifier { field: "secret" }.as_str(),
            "invalid_identifier"
        );
        assert!(
            !QuotaValidationError::InvalidIdentifier { field: "secret" }
                .as_str()
                .contains("secret")
        );
        assert_eq!(
            QuotaValidationError::TooManyWindows.as_str(),
            "too_many_windows"
        );
        assert_eq!(
            QuotaValidationError::InvalidWindowUsage.as_str(),
            "invalid_window_usage"
        );
        assert_eq!(
            QuotaValidationError::InvalidBalance.as_str(),
            "invalid_balance"
        );
        assert_eq!(
            QuotaValidationError::StatusContradictsBalance.as_str(),
            "status_contradicts_balance"
        );
    }

    #[test]
    fn quota_status_and_unit_tokens_match_serde_snake_case() {
        assert_eq!(QuotaStatus::Available.as_str(), "available");
        assert_eq!(QuotaStatus::Limited.to_string(), "limited");
        assert_eq!(QuotaStatus::Exhausted.as_str(), "exhausted");
        assert_eq!(QuotaStatus::Unknown.to_string(), "unknown");

        assert_eq!(QuotaUnit::Requests.as_str(), "requests");
        assert_eq!(QuotaUnit::Tokens.to_string(), "tokens");
        assert_eq!(QuotaUnit::Credits.as_str(), "credits");
        assert_eq!(QuotaUnit::UsdMicros.to_string(), "usd_micros");
    }
}
