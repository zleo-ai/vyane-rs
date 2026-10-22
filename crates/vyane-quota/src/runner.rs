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
/// Maximum bytes of connector error detail retained for a caller.
pub const MAX_CONNECTOR_ERROR_BYTES: usize = 1024;
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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("quota connector failed with {code}")]
pub struct QuotaConnectorError {
    pub code: QuotaConnectorErrorCode,
    detail: String,
}

impl QuotaConnectorError {
    #[must_use]
    pub const fn new(code: QuotaConnectorErrorCode) -> Self {
        Self {
            code,
            detail: String::new(),
        }
    }

    /// Detail kept for a caller is truncated to [`MAX_CONNECTOR_ERROR_BYTES`]
    /// and omits every line that assigns a token or password. A camelCase
    /// suffix counts. A bare line that only continues an empty or unclosed
    /// value is omitted too.
    #[must_use]
    pub fn with_detail(code: QuotaConnectorErrorCode, detail: &str) -> Self {
        Self {
            code,
            detail: bound_connector_error(detail),
        }
    }

    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

fn bound_connector_error(detail: &str) -> String {
    let mut kept = String::new();
    let mut drop_bare_continuation = false;
    for line in detail.lines() {
        if drop_bare_continuation {
            drop_bare_continuation = false;
            // The previous assignment's value was empty or unclosed, so this
            // line is that value unless it starts a new field.
            if !line_has_assignment_separator(line) {
                continue;
            }
        }
        if line_assigns_token_or_password(line) {
            drop_bare_continuation = secret_value_continues(line);
            continue;
        }
        if !kept.is_empty() {
            kept.push('\n');
        }
        kept.push_str(line);
    }
    truncate_to_bytes(&kept, MAX_CONNECTOR_ERROR_BYTES)
}

fn retained_connector_detail(detail: &str) -> Option<String> {
    let bounded = bound_connector_error(detail);
    (!bounded.is_empty()).then_some(bounded)
}

fn line_assigns_token_or_password(line: &str) -> bool {
    let bytes = line.as_bytes();
    bytes.iter().enumerate().any(|(index, byte)| {
        matches!(byte, b'=' | b':') && key_before_separator_is_token_or_password(bytes, index)
    })
}

fn key_before_separator_is_token_or_password(bytes: &[u8], separator: usize) -> bool {
    let mut index = separator;
    while index > 0 && is_key_padding(bytes[index - 1]) {
        index -= 1;
    }
    let end = index;
    while index > 0 && is_key_byte(bytes[index - 1]) {
        index -= 1;
    }
    if index == end {
        return false;
    }
    let Ok(key) = std::str::from_utf8(&bytes[index..end]) else {
        return false;
    };
    key_names_token_or_password(key)
}

fn is_key_padding(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'"' | b'\'' | b'`')
}

fn is_key_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
}

fn line_has_assignment_separator(line: &str) -> bool {
    line.bytes().any(|byte| byte == b'=' || byte == b':')
}

fn secret_value_continues(line: &str) -> bool {
    let bytes = line.as_bytes();
    bytes.iter().enumerate().any(|(index, byte)| {
        if !matches!(byte, b'=' | b':') || !key_before_separator_is_token_or_password(bytes, index)
        {
            return false;
        }
        let raw = line[index + 1..].trim_start_matches([' ', '\t']);
        if raw.is_empty() {
            return true;
        }
        let quote = raw.as_bytes()[0];
        if quote == b'"' || quote == b'\'' {
            unescaped_quote_is_odd(raw, quote)
        } else {
            false
        }
    })
}

fn unescaped_quote_is_odd(raw: &str, quote: u8) -> bool {
    let mut count = 0;
    let mut escaped = false;
    for byte in raw.bytes() {
        if escaped {
            escaped = false;
            continue;
        }
        if byte == b'\\' {
            escaped = true;
            continue;
        }
        if byte == quote {
            count += 1;
        }
    }
    count % 2 == 1
}

fn key_names_token_or_password(key: &str) -> bool {
    let key = key.trim_matches(|character| matches!(character, '.' | '_' | '-'));
    if key.is_empty() {
        return false;
    }
    let last = key.rsplit(['_', '-', '.']).next().unwrap_or(key);
    is_exact_secret_name(last) || camel_secret_suffix(last)
}

fn is_exact_secret_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("token") || name.eq_ignore_ascii_case("password")
}

fn camel_secret_suffix(name: &str) -> bool {
    for suffix in ["token", "password"] {
        if name.len() <= suffix.len() {
            continue;
        }
        let (stem, tail) = name.split_at(name.len() - suffix.len());
        if !tail.eq_ignore_ascii_case(suffix) {
            continue;
        }
        let Some(boundary) = stem.chars().next_back() else {
            continue;
        };
        let Some(first) = tail.chars().next() else {
            continue;
        };
        if (boundary.is_ascii_lowercase() || boundary.is_ascii_digit())
            && first.is_ascii_uppercase()
        {
            return true;
        }
    }
    false
}

fn truncate_to_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

fn deserialize_error_detail<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let detail = Option::<String>::deserialize(deserializer)?;
    Ok(detail.as_deref().and_then(retained_connector_detail))
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
    /// Bounded detail kept for the caller. Lines that assign a token or
    /// password are removed, then the remainder is truncated to
    /// [`MAX_CONNECTOR_ERROR_BYTES`].
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_error_detail"
    )]
    pub error_detail: Option<String>,
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
                        error_detail: None,
                    },
                    Ok(Err(error)) => {
                        let error_detail = retained_connector_detail(error.detail());
                        QuotaSnapshot {
                            connector_id,
                            provider,
                            checked_at,
                            status: QuotaSnapshotStatus::Error,
                            card: None,
                            error: Some(error.code),
                            error_detail,
                        }
                    }
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
                                error_detail: None,
                            };
                        }
                        QuotaSnapshot {
                            connector_id,
                            provider,
                            checked_at,
                            status: QuotaSnapshotStatus::Ok,
                            card: Some(card),
                            error: None,
                            error_detail: None,
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
    use std::sync::Arc;

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};

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

    #[test]
    fn connector_error_detail_drops_secret_assignments_and_truncates() {
        let cases = [
            ("", ""),
            ("upstream", "upstream"),
            ("token=super-secret-token", ""),
            ("password = super-secret-password", ""),
            ("TOKEN=super-secret-token", ""),
            ("Password: super-secret-password", ""),
            ("api_token=super-secret-token", ""),
            ("user-password=super-secret-password", ""),
            ("accessToken=super-secret-token", ""),
            ("refreshToken: super-secret-token", ""),
            ("userPassword=super-secret-password", ""),
            ("\"refreshToken\":\"super-secret-token\"", ""),
            ("password:\nsuper-secret-password", ""),
            ("\"access_token\": \"super-\nsecret-token\"", ""),
            (
                "password:\nerror: upstream timeout",
                "error: upstream timeout",
            ),
            (
                "accessToken: \"say \\\"hi\\\"\"\nprovider returned 401",
                "provider returned 401",
            ),
            ("userPassword: \"secret\\\"\"\nretry later", "retry later"),
            ("\"access_token\": \"super\\\"\nsecret-token\"", ""),
            ("\"password\": \"super-secret-password\"", ""),
            ("token =", ""),
            ("upstream failed token=super-secret-token", ""),
            ("tokens=12", "tokens=12"),
            ("token_count=3", "token_count=3"),
            ("message=bad password", "message=bad password"),
            ("token bucket exhausted", "token bucket exhausted"),
            (
                "note: token bucket exhausted",
                "note: token bucket exhausted",
            ),
            (
                "Authorization: Bearer not-a-named-token",
                "Authorization: Bearer not-a-named-token",
            ),
            (
                "upstream\ntoken=super-secret-token\npassword=super-secret-password\nretry",
                "upstream\nretry",
            ),
        ];
        for (input, expected) in cases {
            let error = QuotaConnectorError::with_detail(QuotaConnectorErrorCode::Internal, input);
            assert_eq!(error.detail(), expected, "input: {input}");
            assert!(!error.detail().contains("super-secret"));
            assert!(!error.to_string().contains("super-secret"));
            assert!(!format!("{error:?}").contains("super-secret"));
            assert!(error.detail().len() <= MAX_CONNECTOR_ERROR_BYTES);
            assert_eq!(error.to_string(), "quota connector failed with internal");
        }

        let exact = "y".repeat(MAX_CONNECTOR_ERROR_BYTES);
        assert_eq!(
            QuotaConnectorError::with_detail(QuotaConnectorErrorCode::Internal, &exact).detail(),
            exact
        );
        let over = "y".repeat(MAX_CONNECTOR_ERROR_BYTES + 1);
        let truncated = QuotaConnectorError::with_detail(QuotaConnectorErrorCode::Internal, &over);
        assert_eq!(truncated.detail().len(), MAX_CONNECTOR_ERROR_BYTES);
        assert!(truncated.detail().chars().all(|character| character == 'y'));

        let wide = "你".repeat(400);
        let bounded = QuotaConnectorError::with_detail(QuotaConnectorErrorCode::Internal, &wide);
        assert_eq!(bounded.detail().len(), 1023);
        assert!(bounded.detail().chars().all(|character| character == '你'));

        let mut prefixed = String::from("token=super-secret-token\n");
        prefixed.push_str(&"b".repeat(MAX_CONNECTOR_ERROR_BYTES + 50));
        let shifted =
            QuotaConnectorError::with_detail(QuotaConnectorErrorCode::Internal, &prefixed);
        assert_eq!(shifted.detail().len(), MAX_CONNECTOR_ERROR_BYTES);
        assert!(shifted.detail().chars().all(|character| character == 'b'));
        assert!(!shifted.detail().contains("super-secret"));
        assert_eq!(
            QuotaConnectorError::new(QuotaConnectorErrorCode::Internal).detail(),
            ""
        );
    }

    #[test]
    fn snapshot_detail_round_trip_drops_secret_lines_and_omits_empty_detail() {
        let checked_at = "2026-01-02T03:04:05Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp");
        let hostile = format!(
            "token=super-secret-token\nupstream\n{}",
            "z".repeat(MAX_CONNECTOR_ERROR_BYTES + 40)
        );
        let raw = format!(
            r#"{{
                "connector_id": "connector",
                "provider": "provider",
                "checked_at": "2026-01-02T03:04:05Z",
                "status": "error",
                "error": "unavailable",
                "error_detail": {detail}
            }}"#,
            detail = serde_json::to_string(&hostile).expect("detail json")
        );
        let restored: QuotaSnapshot = serde_json::from_str(&raw).expect("snapshot");
        let detail = restored.error_detail.as_deref().expect("bounded detail");
        assert_eq!(restored.error, Some(QuotaConnectorErrorCode::Unavailable));
        assert!(detail.starts_with("upstream\n"));
        assert_eq!(detail.len(), MAX_CONNECTOR_ERROR_BYTES);
        assert!(!detail.contains("super-secret"));
        assert!(!format!("{restored:?}").contains("super-secret"));
        assert_eq!(restored.checked_at, checked_at);

        let legacy = r#"{
            "connector_id": "connector",
            "provider": "provider",
            "checked_at": "2026-01-02T03:04:05Z",
            "status": "error",
            "error": "unavailable"
        }"#;
        let legacy: QuotaSnapshot = serde_json::from_str(legacy).expect("legacy snapshot");
        assert_eq!(legacy.error_detail, None);
        let json = serde_json::to_value(&legacy).expect("legacy json");
        assert!(json.get("error_detail").is_none());

        let only_secret = r#"{
            "connector_id": "connector",
            "provider": "provider",
            "checked_at": "2026-01-02T03:04:05Z",
            "status": "error",
            "error": "authentication",
            "error_detail": "password = super-secret-password"
        }"#;
        let only_secret: QuotaSnapshot =
            serde_json::from_str(only_secret).expect("secret snapshot");
        assert_eq!(only_secret.error_detail, None);
        assert!(!format!("{only_secret:?}").contains("super-secret"));
    }

    struct DetailConnector {
        id: &'static str,
        detail: String,
        code_only: bool,
    }

    #[async_trait]
    impl QuotaConnector for DetailConnector {
        fn id(&self) -> &str {
            self.id
        }

        fn provider(&self) -> &str {
            "provider"
        }

        async fn snapshot(
            &self,
            _policy: QuotaReadPolicy,
        ) -> Result<crate::QuotaCard, QuotaConnectorError> {
            if self.code_only {
                return Err(QuotaConnectorError::new(
                    QuotaConnectorErrorCode::RateLimited,
                ));
            }
            Err(QuotaConnectorError::with_detail(
                QuotaConnectorErrorCode::Unavailable,
                &self.detail,
            ))
        }
    }

    #[tokio::test]
    async fn runner_keeps_bounded_connector_error_for_the_caller() {
        let mut detail = String::from("upstream unavailable\ntoken=super-secret-token\n");
        detail.push_str("password = super-secret-password\n");
        detail.push_str(&"c".repeat(MAX_CONNECTOR_ERROR_BYTES + 80));
        let connectors: Vec<Arc<dyn QuotaConnector>> = vec![
            Arc::new(DetailConnector {
                id: "detail",
                detail,
                code_only: false,
            }),
            Arc::new(DetailConnector {
                id: "code-only",
                detail: String::new(),
                code_only: true,
            }),
        ];
        let runner =
            QuotaSnapshotRunner::new(connectors, 2, QuotaReadPolicy::default()).expect("runner");
        let snapshots = runner.snapshot().await;

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].connector_id, "code-only");
        assert_eq!(
            snapshots[0].error,
            Some(QuotaConnectorErrorCode::RateLimited)
        );
        assert_eq!(snapshots[0].error_detail, None);
        assert_eq!(snapshots[1].connector_id, "detail");
        assert_eq!(
            snapshots[1].error,
            Some(QuotaConnectorErrorCode::Unavailable)
        );
        let kept = snapshots[1].error_detail.as_deref().expect("kept detail");
        let prefix = "upstream unavailable\n";
        assert!(kept.starts_with(prefix));
        assert_eq!(kept.len(), MAX_CONNECTOR_ERROR_BYTES);
        assert!(
            kept[prefix.len()..]
                .chars()
                .all(|character| character == 'c')
        );
        assert!(!kept.contains("super-secret"));
        assert!(!format!("{snapshots:?}").contains("super-secret"));
    }
}
