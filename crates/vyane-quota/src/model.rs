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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaUnit {
    Requests,
    Tokens,
    Credits,
    UsdMicros,
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
