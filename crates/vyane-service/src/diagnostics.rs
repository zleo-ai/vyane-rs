//! Safe, static diagnostics shared by protocol front-ends.
//!
//! The DTOs in this module are allowlists rather than serialized views of
//! [`LoadedConfig`]. In particular they cannot carry config/storage paths,
//! endpoint URLs, environment-variable names, credential values, prompts, or
//! source-error text. Both operations are local computations: they never
//! probe a provider, inspect a harness binary, make a network request, or
//! spawn a process.

use std::fmt;

use anyhow::Result;
use serde::Serialize;
use vyane_config::{ProfilePatch, RawFailoverElement, ResolvedConfig};
use vyane_core::{AdapterTransport, HarnessKind};
use vyane_protocol::validate_http_base_url;

use crate::config::LoadedConfig;
use crate::factory::validate_assembler_combo;
use crate::routing::{RouteParams, route_task};

/// Maximum prompt bytes accepted by a route preview.
pub const ROUTE_PREVIEW_MAX_TASK_BYTES: usize = 64 * 1024;
/// Maximum bytes in one routing hint, tag, candidate, or config identifier.
pub const ROUTE_PREVIEW_MAX_VALUE_BYTES: usize = 256;
/// Maximum number of explicit tags or candidate profiles in one preview.
pub const ROUTE_PREVIEW_MAX_LIST_ITEMS: usize = 64;
/// Maximum accepted structural counter value.
pub const ROUTE_PREVIEW_MAX_SIGNAL: usize = 1_000_000;
/// Maximum provider + profile rows considered by one diagnostic operation.
pub const DIAGNOSTIC_MAX_CONFIG_ITEMS: usize = 256;
/// Maximum failover legs on one profile during static checking.
pub const DIAGNOSTIC_MAX_FAILOVER_LEGS: usize = 64;
/// Maximum entries in one config metadata/tag map or list.
pub const DIAGNOSTIC_MAX_METADATA_ITEMS: usize = 64;
/// Maximum recursively charged bytes/nodes in one free-form metadata map.
pub const DIAGNOSTIC_MAX_METADATA_BYTES: usize = 16 * 1024;
/// Maximum nesting depth traversed in free-form metadata.
pub const DIAGNOSTIC_MAX_METADATA_DEPTH: usize = 32;
/// Maximum endpoint bytes diagnostics permit the resolver to copy.
pub const DIAGNOSTIC_MAX_ENDPOINT_BYTES: usize = 4 * 1024;
/// Maximum serialized JSON bytes in a successful diagnostics MCP payload.
pub const DIAGNOSTIC_MAX_OUTPUT_BYTES: usize = 256 * 1024;

/// Stable diagnostic failure class. Messages are compile-time constants and
/// never contain caller/config/source text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticErrorKind {
    InvalidInput,
    ConfigInvalid,
    BudgetExceeded,
}

/// Typed static error used by both diagnostics operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiagnosticError {
    pub kind: DiagnosticErrorKind,
    message: &'static str,
}

impl DiagnosticError {
    const fn invalid_input(message: &'static str) -> Self {
        Self {
            kind: DiagnosticErrorKind::InvalidInput,
            message,
        }
    }

    const fn config_invalid(message: &'static str) -> Self {
        Self {
            kind: DiagnosticErrorKind::ConfigInvalid,
            message,
        }
    }

    const fn budget_exceeded(message: &'static str) -> Self {
        Self {
            kind: DiagnosticErrorKind::BudgetExceeded,
            message,
        }
    }
}

impl fmt::Display for DiagnosticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for DiagnosticError {}

/// Bounded inputs for a deterministic route preview.
#[derive(Debug, Clone)]
pub struct RoutePreviewParams {
    pub task: String,
    pub stage: Option<String>,
    pub changed_files: Option<usize>,
    pub dependency_edges: Option<usize>,
    pub retry_count: Option<usize>,
    pub explicit_tier: Option<String>,
    pub extra_tags: Vec<String>,
    pub candidate_profiles: Vec<String>,
    pub allow_frontier: bool,
}

impl Default for RoutePreviewParams {
    fn default() -> Self {
        Self {
            task: String::new(),
            stage: None,
            changed_files: None,
            dependency_edges: None,
            retry_count: None,
            explicit_tier: None,
            extra_tags: Vec::new(),
            candidate_profiles: Vec::new(),
            allow_frontier: true,
        }
    }
}

/// How the deterministic router arrived at its selected profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteSelectionBasis {
    Preference,
    Default,
}

/// Redacted route result. The source task, matched raw tag, and free-form
/// router reason are deliberately absent.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RoutePreview {
    pub profile: String,
    pub provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub tier: String,
    pub effort: String,
    pub intent: String,
    pub complexity_score: f64,
    pub selection_basis: RouteSelectionBasis,
}

/// Overall result of the non-probing configuration check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigCheckStatus {
    Valid,
    Partial,
}

/// Credential readiness without the environment-variable name or value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialStatus {
    NotRequired,
    Present,
    Missing,
}

/// Static resolution status for one configured profile. These names avoid an
/// online/harness-readiness claim: no provider or process is probed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileCheckStatus {
    Resolvable,
    Unresolvable,
}

/// Closed issue taxonomy for configuration diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigIssueCode {
    NoProviders,
    NoProfiles,
    CredentialMissing,
    ProfileUnresolvable,
    TargetUnsupported,
}

/// One issue whose message is selected from compile-time constants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConfigIssue {
    pub code: ConfigIssueCode,
    pub message: &'static str,
}

/// Safe provider summary. Endpoint and credential configuration are never
/// copied into this type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderCheck {
    pub id: String,
    pub protocol: String,
    pub has_default_model: bool,
    pub credential: CredentialStatus,
}

/// Safe profile summary. A failed resolution is represented only by a closed
/// issue code and static text; the raw resolver error is discarded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProfileCheck {
    pub name: String,
    pub status: ProfileCheckStatus,
    pub chain_length: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<ConfigIssue>,
}

/// Static-only config diagnostics. The complete row set is bounded before
/// construction; this report is never a silently truncated readiness claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConfigCheckReport {
    pub status: ConfigCheckStatus,
    /// Makes the intentionally narrow readiness claim explicit to callers.
    pub scope: &'static str,
    pub provider_count: usize,
    pub profile_count: usize,
    pub providers: Vec<ProviderCheck>,
    pub profiles: Vec<ProfileCheck>,
    pub issues: Vec<ConfigIssue>,
}

pub(crate) fn route_preview(
    loaded: &LoadedConfig,
    params: RoutePreviewParams,
) -> Result<RoutePreview> {
    validate_route_preview_params(loaded, &params)?;
    validate_config_budget(&loaded.config)?;

    let result = route_task(
        &loaded.config,
        RouteParams {
            task: params.task,
            stage: params.stage,
            changed_files: params.changed_files,
            dependency_edges: params.dependency_edges,
            retry_count: params.retry_count,
            explicit_tier: params.explicit_tier,
            explicit_effort: None,
            extra_tags: params.extra_tags,
            candidate_profiles: params.candidate_profiles,
            allow_frontier: params.allow_frontier,
        },
    )
    .map_err(|_| static_config_error("routing configuration cannot produce a preview"))?;

    let decision = result.decision;
    Ok(RoutePreview {
        profile: result.profile,
        provider: decision.provider,
        model: (!decision.model.is_empty()).then_some(decision.model),
        tier: decision.tier.as_str().to_string(),
        effort: decision.effort.as_str().to_string(),
        intent: decision.intent,
        complexity_score: decision.complexity_score,
        selection_basis: if decision.reason == "default fallback" {
            RouteSelectionBasis::Default
        } else {
            RouteSelectionBasis::Preference
        },
    })
}

pub(crate) fn check_config(loaded: &LoadedConfig) -> Result<ConfigCheckReport> {
    validate_config_budget(&loaded.config)?;

    let provider_count = loaded.config.providers.providers.len();
    let profile_count = loaded.config.profiles.len();
    let mut partial = provider_count == 0 || profile_count == 0;
    let mut issues = Vec::new();

    if provider_count == 0 {
        issues.push(issue(
            ConfigIssueCode::NoProviders,
            "no providers are configured",
        ));
    }
    if profile_count == 0 {
        issues.push(issue(
            ConfigIssueCode::NoProfiles,
            "no profiles are configured",
        ));
    }

    let providers = loaded
        .config
        .providers
        .providers
        .iter()
        .map(|(id, provider)| {
            let credential = match provider.api_key_env.as_deref() {
                None => CredentialStatus::NotRequired,
                Some(name) if loaded.env_present(name) => CredentialStatus::Present,
                Some(_) => CredentialStatus::Missing,
            };
            if credential == CredentialStatus::Missing {
                partial = true;
            }
            ProviderCheck {
                id: id.clone(),
                protocol: provider.protocol.to_string(),
                has_default_model: provider.default_model.is_some(),
                credential,
            }
        })
        .collect::<Vec<_>>();

    if providers
        .iter()
        .any(|provider| provider.credential == CredentialStatus::Missing)
    {
        issues.push(issue(
            ConfigIssueCode::CredentialMissing,
            "one or more configured credentials are unavailable",
        ));
    }

    let profiles = loaded
        .config
        .profiles
        .keys()
        .map(|name| match static_profile_resolvability(loaded, name) {
            Ok(chain_length) => ProfileCheck {
                name: name.clone(),
                status: ProfileCheckStatus::Resolvable,
                chain_length,
                issue: None,
            },
            Err(StaticProfileFailure::Unsupported) => {
                partial = true;
                ProfileCheck {
                    name: name.clone(),
                    status: ProfileCheckStatus::Unresolvable,
                    chain_length: 0,
                    issue: Some(issue(
                        ConfigIssueCode::TargetUnsupported,
                        "profile uses an unsupported execution target",
                    )),
                }
            }
            Err(StaticProfileFailure::Unresolvable) => {
                partial = true;
                ProfileCheck {
                    name: name.clone(),
                    status: ProfileCheckStatus::Unresolvable,
                    chain_length: 0,
                    issue: Some(issue(
                        ConfigIssueCode::ProfileUnresolvable,
                        "profile could not be resolved",
                    )),
                }
            }
        })
        .collect::<Vec<_>>();

    Ok(ConfigCheckReport {
        status: if partial {
            ConfigCheckStatus::Partial
        } else {
            ConfigCheckStatus::Valid
        },
        scope: "static_config_only",
        provider_count,
        profile_count,
        providers,
        profiles,
        issues,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaticProfileFailure {
    Unresolvable,
    Unsupported,
}

fn static_profile_resolvability(
    loaded: &LoadedConfig,
    profile_name: &str,
) -> std::result::Result<usize, StaticProfileFailure> {
    let profile = loaded
        .config
        .profiles
        .get(profile_name)
        .ok_or(StaticProfileFailure::Unresolvable)?;
    validate_profile_target(loaded, profile)?;
    let mut chain_length = 1;
    if let Some(failover) = profile.failover.as_deref() {
        for leg in failover {
            match leg {
                RawFailoverElement::ProfileName(name) => {
                    let fallback = loaded
                        .config
                        .profiles
                        .get(name)
                        .ok_or(StaticProfileFailure::Unresolvable)?;
                    validate_profile_target(loaded, fallback)?;
                }
                RawFailoverElement::ProviderModel { provider, model } => {
                    let provider = loaded
                        .config
                        .providers
                        .get(provider)
                        .map_err(|_| StaticProfileFailure::Unresolvable)?;
                    if model.as_str().is_empty()
                        || provider
                            .api_key_env
                            .as_deref()
                            .is_some_and(|name| !loaded.env_present(name))
                    {
                        return Err(StaticProfileFailure::Unresolvable);
                    }
                    validate_assembler_combo(AdapterTransport::DirectHttp, provider.protocol, None)
                        .map_err(|_| StaticProfileFailure::Unsupported)?;
                }
            }
            chain_length += 1;
        }
    }
    Ok(chain_length)
}

fn validate_profile_target(
    loaded: &LoadedConfig,
    profile: &ProfilePatch,
) -> std::result::Result<(), StaticProfileFailure> {
    let provider_id = profile
        .provider
        .as_deref()
        .ok_or(StaticProfileFailure::Unresolvable)?;
    let provider = loaded
        .config
        .providers
        .get(provider_id)
        .map_err(|_| StaticProfileFailure::Unresolvable)?;
    if profile
        .model
        .as_ref()
        .or(provider.default_model.as_ref())
        .is_none_or(|model| model.as_str().is_empty())
        || provider
            .api_key_env
            .as_deref()
            .is_some_and(|name| !loaded.env_present(name))
    {
        return Err(StaticProfileFailure::Unresolvable);
    }
    let harness = match profile.harness.as_deref() {
        None | Some("none") => None,
        Some(raw) if raw.chars().any(|character| character.is_ascii_uppercase()) => {
            return Err(StaticProfileFailure::Unresolvable);
        }
        Some(raw) => Some(HarnessKind::from(raw)),
    };
    let transport = if harness.is_some() {
        AdapterTransport::CliWrap
    } else {
        AdapterTransport::DirectHttp
    };
    validate_assembler_combo(
        transport,
        profile.protocol.unwrap_or(provider.protocol),
        harness.as_ref(),
    )
    .map_err(|_| StaticProfileFailure::Unsupported)
}

fn validate_route_preview_params(loaded: &LoadedConfig, params: &RoutePreviewParams) -> Result<()> {
    if params.task.trim().is_empty() {
        return Err(static_input_error("route task must not be empty"));
    }
    if params.task.len() > ROUTE_PREVIEW_MAX_TASK_BYTES {
        return Err(static_input_error("route task exceeds the size limit"));
    }
    if contains_forbidden_task_control(&params.task) {
        return Err(static_input_error("route task contains invalid characters"));
    }
    validate_input_value(params.stage.as_deref(), "route stage is invalid")?;
    validate_input_value(params.explicit_tier.as_deref(), "route tier is invalid")?;
    if let Some(tier) = params.explicit_tier.as_deref() {
        if !matches!(
            tier.trim().to_ascii_lowercase().as_str(),
            "economy" | "mainline" | "frontier"
        ) {
            return Err(static_input_error("route tier is invalid"));
        }
    }
    validate_input_list(&params.extra_tags, "route tags are invalid")?;
    validate_input_list(&params.candidate_profiles, "route candidates are invalid")?;
    if params
        .candidate_profiles
        .iter()
        .any(|name| !loaded.config.profiles.contains_key(name))
    {
        return Err(static_input_error("route candidates are invalid"));
    }
    for signal in [
        params.changed_files,
        params.dependency_edges,
        params.retry_count,
    ]
    .into_iter()
    .flatten()
    {
        if signal > ROUTE_PREVIEW_MAX_SIGNAL {
            return Err(static_input_error("route signal exceeds the limit"));
        }
    }
    Ok(())
}

fn validate_config_budget(config: &ResolvedConfig) -> Result<()> {
    let provider_count = config.providers.providers.len();
    let profile_count = config.profiles.len();
    if provider_count.saturating_add(profile_count) > DIAGNOSTIC_MAX_CONFIG_ITEMS {
        return Err(static_budget_error(
            "configuration exceeds the diagnostic row limit",
        ));
    }

    for (provider_id, provider) in &config.providers.providers {
        validate_config_identifier(provider_id)?;
        validate_config_endpoint(&provider.base_url)?;
        if let Some(env_name) = provider.api_key_env.as_deref() {
            validate_config_metadata(env_name)?;
        }
        if let Some(model) = provider.default_model.as_ref() {
            validate_config_identifier(model.as_str())?;
        }
        if provider.env_inject.len() > DIAGNOSTIC_MAX_METADATA_ITEMS {
            return Err(static_budget_error(
                "provider metadata exceeds the diagnostic item limit",
            ));
        }
        for env_name in provider.env_inject.keys() {
            validate_config_metadata(env_name)?;
        }
        validate_metadata_map(&provider.extra)?;
    }

    for (profile_name, profile) in &config.profiles {
        validate_config_identifier(profile_name)?;
        if let Some(provider_id) = profile.provider.as_deref() {
            validate_config_identifier(provider_id)?;
        }
        if let Some(model) = profile.model.as_ref() {
            validate_config_identifier(model.as_str())?;
        }
        for metadata in [
            profile.harness.as_deref(),
            profile.tier.as_deref(),
            profile.stage.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            validate_config_metadata(metadata)?;
        }
        if let Some(tags) = profile.tags.as_deref() {
            if tags.len() > DIAGNOSTIC_MAX_METADATA_ITEMS {
                return Err(static_budget_error(
                    "profile tags exceed the diagnostic item limit",
                ));
            }
            for tag in tags {
                validate_config_metadata(tag)?;
            }
        }
        if let Some(params) = profile.params.as_ref() {
            validate_metadata_map(&params.extra)?;
        }
        if let Some(failover) = profile.failover.as_deref() {
            if failover.len() > DIAGNOSTIC_MAX_FAILOVER_LEGS {
                return Err(static_budget_error(
                    "profile failover exceeds the diagnostic leg limit",
                ));
            }
            for leg in failover {
                match leg {
                    RawFailoverElement::ProfileName(name) => validate_config_identifier(name)?,
                    RawFailoverElement::ProviderModel { provider, model } => {
                        validate_config_identifier(provider)?;
                        validate_config_identifier(model.as_str())?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_input_value(value: Option<&str>, message: &'static str) -> Result<()> {
    if value.is_some_and(|value| {
        value.trim().is_empty()
            || value.len() > ROUTE_PREVIEW_MAX_VALUE_BYTES
            || value.chars().any(char::is_control)
    }) {
        return Err(static_input_error(message));
    }
    Ok(())
}

fn validate_input_list(values: &[String], message: &'static str) -> Result<()> {
    if values.len() > ROUTE_PREVIEW_MAX_LIST_ITEMS
        || values.iter().any(|value| {
            value.trim().is_empty()
                || value.len() > ROUTE_PREVIEW_MAX_VALUE_BYTES
                || value.chars().any(char::is_control)
        })
    {
        return Err(static_input_error(message));
    }
    Ok(())
}

fn validate_config_identifier(value: &str) -> Result<()> {
    validate_config_text(value, ROUTE_PREVIEW_MAX_VALUE_BYTES, "config identifier")
}

fn validate_config_metadata(value: &str) -> Result<()> {
    validate_config_text(value, ROUTE_PREVIEW_MAX_VALUE_BYTES, "config metadata")
}

fn validate_config_endpoint(value: &str) -> Result<()> {
    validate_config_text(value, DIAGNOSTIC_MAX_ENDPOINT_BYTES, "config endpoint")?;
    validate_http_base_url(value)
        .map_err(|_| static_config_error("configuration contains an invalid endpoint"))
}

fn validate_config_text(value: &str, max_bytes: usize, field: &'static str) -> Result<()> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(static_config_error(match field {
            "config identifier" => "configuration contains an invalid identifier",
            "config metadata" => "configuration contains invalid metadata",
            _ => "configuration contains an invalid endpoint",
        }));
    }
    if value.len() > max_bytes {
        return Err(static_budget_error(match field {
            "config identifier" => "configuration identifier exceeds the diagnostic limit",
            "config metadata" => "configuration metadata exceeds the diagnostic limit",
            _ => "configuration endpoint exceeds the diagnostic limit",
        }));
    }
    Ok(())
}

fn validate_metadata_map(map: &serde_json::Map<String, serde_json::Value>) -> Result<()> {
    if map.len() > DIAGNOSTIC_MAX_METADATA_ITEMS {
        return Err(static_budget_error(
            "configuration metadata exceeds the diagnostic item limit",
        ));
    }
    let mut remaining = DIAGNOSTIC_MAX_METADATA_BYTES;
    for (key, value) in map {
        validate_config_metadata(key)?;
        if !charge_json_value(value, &mut remaining, 0) {
            return Err(static_budget_error(
                "configuration metadata exceeds the diagnostic byte limit",
            ));
        }
    }
    Ok(())
}

fn charge_json_value(value: &serde_json::Value, remaining: &mut usize, depth: usize) -> bool {
    if depth > DIAGNOSTIC_MAX_METADATA_DEPTH || !charge(remaining, 1) {
        return false;
    }
    let fixed_charge = match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) => 1,
        serde_json::Value::Number(number) => number.to_string().len(),
        serde_json::Value::String(text) => text.len(),
        serde_json::Value::Array(values) => {
            if !charge(remaining, values.len()) {
                return false;
            }
            return values
                .iter()
                .all(|value| charge_json_value(value, remaining, depth + 1));
        }
        serde_json::Value::Object(values) => {
            if values.len() > DIAGNOSTIC_MAX_METADATA_ITEMS {
                return false;
            }
            for (key, value) in values {
                if !charge(remaining, key.len()) || !charge_json_value(value, remaining, depth + 1)
                {
                    return false;
                }
            }
            return true;
        }
    };
    charge(remaining, fixed_charge)
}

fn charge(remaining: &mut usize, amount: usize) -> bool {
    if amount > *remaining {
        return false;
    }
    *remaining -= amount;
    true
}

fn contains_forbidden_task_control(value: &str) -> bool {
    value
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
}

fn issue(code: ConfigIssueCode, message: &'static str) -> ConfigIssue {
    ConfigIssue { code, message }
}

fn static_input_error(message: &'static str) -> anyhow::Error {
    DiagnosticError::invalid_input(message).into()
}

fn static_config_error(message: &'static str) -> anyhow::Error {
    DiagnosticError::config_invalid(message).into()
}

fn static_budget_error(message: &'static str) -> anyhow::Error {
    DiagnosticError::budget_exceeded(message).into()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use serde_json::json;
    use vyane_config::{ConfigLayers, RawRoot};
    use vyane_core::ModelId;

    use super::*;

    const PATH_CANARY: &str = "CANARY_CONFIG_FILE";
    const URL_CANARY: &str = "https://CANARY_BASE_URL.invalid/v1";
    const ENV_CANARY: &str = "CANARY_API_KEY_ENV";
    const SECRET_CANARY: &str = "CANARY_SECRET_VALUE";
    const TASK_CANARY: &str = "CANARY_TASK_PROMPT";

    fn loaded_config(include_secret: bool) -> LoadedConfig {
        loaded_from_json(
            json!({
                "providers": {
                    "safe-provider": {
                        "base_url": URL_CANARY,
                        "api_key_env": ENV_CANARY,
                        "auth_style": "bearer",
                        "protocol": "openai_chat",
                        "default_model": "safe-model",
                        "env_inject": {
                            "CANARY_CHILD_ENV": "api_key"
                        }
                    }
                },
                "profiles": {
                    "safe-profile": {
                        "provider": "safe-provider",
                        "model": "safe-model",
                        "tier": "economy",
                        "tags": ["code"]
                    }
                }
            }),
            include_secret,
        )
    }

    fn loaded_from_json(value: serde_json::Value, include_secret: bool) -> LoadedConfig {
        let root: RawRoot = serde_json::from_value(value).unwrap();
        let mut layers = ConfigLayers::new();
        layers.merge(&root).unwrap();
        let secrets = if include_secret {
            BTreeMap::from([(ENV_CANARY.to_string(), SECRET_CANARY.to_string())])
        } else {
            BTreeMap::new()
        };
        LoadedConfig {
            config: ResolvedConfig::from(layers),
            files: vec![PathBuf::from(PATH_CANARY)],
            secrets,
        }
    }

    fn diagnostic_error(error: &anyhow::Error) -> &DiagnosticError {
        error.downcast_ref::<DiagnosticError>().unwrap()
    }

    #[test]
    fn route_preview_is_allowlisted_and_does_not_echo_task_or_config_secrets() {
        let preview = route_preview(
            &loaded_config(true),
            RoutePreviewParams {
                task: TASK_CANARY.to_string(),
                extra_tags: vec!["CANARY_RAW_TAG".into()],
                ..Default::default()
            },
        )
        .unwrap();
        let wire = serde_json::to_string(&preview).unwrap();

        assert_eq!(preview.profile, "safe-profile");
        assert_eq!(preview.provider, "safe-provider");
        for canary in [
            PATH_CANARY,
            URL_CANARY,
            ENV_CANARY,
            SECRET_CANARY,
            TASK_CANARY,
            "CANARY_RAW_TAG",
        ] {
            assert!(!wire.contains(canary), "leaked {canary}");
        }
    }

    #[test]
    fn check_config_reports_static_resolvability_without_sensitive_fields() {
        let report = check_config(&loaded_config(true)).unwrap();
        let wire = serde_json::to_string(&report).unwrap();

        assert_eq!(report.status, ConfigCheckStatus::Valid);
        assert_eq!(report.scope, "static_config_only");
        assert_eq!(report.providers[0].credential, CredentialStatus::Present);
        assert_eq!(report.profiles[0].status, ProfileCheckStatus::Resolvable);
        for canary in [PATH_CANARY, URL_CANARY, ENV_CANARY, SECRET_CANARY] {
            assert!(!wire.contains(canary), "leaked {canary}");
        }
    }

    #[test]
    fn check_config_maps_raw_resolution_failures_to_static_partial_issues() {
        let report = check_config(&loaded_config(false)).unwrap();
        let wire = serde_json::to_string(&report).unwrap();

        assert_eq!(report.status, ConfigCheckStatus::Partial);
        assert_eq!(report.providers[0].credential, CredentialStatus::Missing);
        assert_eq!(report.profiles[0].status, ProfileCheckStatus::Unresolvable);
        assert_eq!(
            report.profiles[0].issue.as_ref().unwrap().code,
            ConfigIssueCode::ProfileUnresolvable
        );
        assert!(!wire.contains(ENV_CANARY));
        assert!(!wire.contains("environment variable"));
    }

    #[test]
    fn check_config_rejects_assembler_unsupported_targets_statically() {
        let loaded = loaded_from_json(
            json!({
                "providers": {
                    "openai": {
                        "base_url": "https://example.invalid/v1",
                        "auth_style": "bearer",
                        "protocol": "openai_chat",
                        "default_model": "model"
                    },
                    "anthropic": {
                        "base_url": "https://example.invalid",
                        "auth_style": "x_api_key",
                        "protocol": "anthropic_messages",
                        "default_model": "model"
                    }
                },
                "profiles": {
                    "opencode": { "provider": "openai", "harness": "opencode" },
                    "custom": { "provider": "openai", "harness": "custom-shell" },
                    "codex-anthropic": {
                        "provider": "anthropic",
                        "harness": "codex-cli"
                    },
                    "claude-openai": {
                        "provider": "openai",
                        "harness": "claude-code"
                    }
                }
            }),
            false,
        );

        let report = check_config(&loaded).unwrap();
        assert_eq!(report.status, ConfigCheckStatus::Partial);
        assert_eq!(report.profiles.len(), 4);
        assert!(report.profiles.iter().all(|profile| {
            profile.status == ProfileCheckStatus::Unresolvable
                && profile.issue.as_ref().is_some_and(|issue| {
                    issue.code == ConfigIssueCode::TargetUnsupported
                        && issue.message == "profile uses an unsupported execution target"
                })
        }));
    }

    #[test]
    fn diagnostics_reject_config_count_identifier_model_metadata_and_failover_overflow() {
        let mut count = loaded_config(false);
        for index in 0..DIAGNOSTIC_MAX_CONFIG_ITEMS {
            count
                .config
                .profiles
                .insert(format!("extra-{index}"), Default::default());
        }

        let mut provider_id = loaded_config(false);
        let provider = provider_id
            .config
            .providers
            .providers
            .remove("safe-provider")
            .unwrap();
        provider_id
            .config
            .providers
            .providers
            .insert("p".repeat(ROUTE_PREVIEW_MAX_VALUE_BYTES + 1), provider);

        let mut model = loaded_config(false);
        model.config.profiles.get_mut("safe-profile").unwrap().model =
            Some(ModelId::new("m".repeat(ROUTE_PREVIEW_MAX_VALUE_BYTES + 1)));

        let mut tags = loaded_config(false);
        tags.config.profiles.get_mut("safe-profile").unwrap().tags =
            Some(vec!["tag".into(); DIAGNOSTIC_MAX_METADATA_ITEMS + 1]);

        let mut failover = loaded_config(false);
        failover
            .config
            .profiles
            .get_mut("safe-profile")
            .unwrap()
            .failover = Some(vec![
            RawFailoverElement::ProfileName("safe-profile".into());
            DIAGNOSTIC_MAX_FAILOVER_LEGS + 1
        ]);

        for loaded in [count, provider_id, model, tags, failover] {
            let error = check_config(&loaded).unwrap_err();
            assert_eq!(
                diagnostic_error(&error).kind,
                DiagnosticErrorKind::BudgetExceeded
            );
            assert!(!error.to_string().contains("safe-provider"));
        }
    }

    #[test]
    fn diagnostics_reject_control_characters_in_config_identifiers() {
        let mut loaded = loaded_config(false);
        loaded.config.profiles.insert(
            "CANARY\nPROFILE".into(),
            loaded.config.profiles.get("safe-profile").unwrap().clone(),
        );

        let error = check_config(&loaded).unwrap_err();
        assert_eq!(
            diagnostic_error(&error).kind,
            DiagnosticErrorKind::ConfigInvalid
        );
        assert!(!error.to_string().contains("CANARY"));
    }

    #[test]
    fn diagnostics_reject_non_http_or_malformed_endpoint_urls_without_echoing_them() {
        for endpoint in [
            "CANARY not a URL",
            "file:///CANARY/redacted/socket",
            "mailto:CANARY@example.invalid",
            "https://CANARY invalid host",
        ] {
            let mut loaded = loaded_config(false);
            loaded
                .config
                .providers
                .providers
                .get_mut("safe-provider")
                .unwrap()
                .base_url = endpoint.into();

            let error = check_config(&loaded).unwrap_err();
            assert_eq!(
                diagnostic_error(&error).kind,
                DiagnosticErrorKind::ConfigInvalid
            );
            assert!(!error.to_string().contains("CANARY"));
        }
    }

    #[test]
    fn worst_case_legal_report_fits_the_shared_output_budget() {
        let mut providers = serde_json::Map::new();
        for index in 0..DIAGNOSTIC_MAX_CONFIG_ITEMS {
            let prefix = format!("p{index:03}-");
            let id = format!(
                "{prefix}{}",
                "x".repeat(ROUTE_PREVIEW_MAX_VALUE_BYTES - prefix.len())
            );
            providers.insert(
                id,
                json!({
                    "base_url": "https://example.invalid",
                    "auth_style": "bearer",
                    "protocol": "openai_chat",
                    "default_model": "model"
                }),
            );
        }
        let loaded = loaded_from_json(json!({ "providers": providers }), false);
        let report = check_config(&loaded).unwrap();
        let wire = serde_json::to_vec_pretty(&report).unwrap();

        assert_eq!(report.provider_count, DIAGNOSTIC_MAX_CONFIG_ITEMS);
        assert!(wire.len() < DIAGNOSTIC_MAX_OUTPUT_BYTES);
    }

    #[test]
    fn route_preview_rejects_empty_oversized_unknown_and_control_inputs_statically() {
        let loaded = loaded_config(true);
        let cases = [
            RoutePreviewParams {
                task: " ".into(),
                ..Default::default()
            },
            RoutePreviewParams {
                task: "x".repeat(ROUTE_PREVIEW_MAX_TASK_BYTES + 1),
                ..Default::default()
            },
            RoutePreviewParams {
                task: "safe".into(),
                explicit_tier: Some("CANARY_UNKNOWN_TIER".into()),
                ..Default::default()
            },
            RoutePreviewParams {
                task: "safe".into(),
                candidate_profiles: vec!["CANARY_UNKNOWN_PROFILE".into()],
                ..Default::default()
            },
            RoutePreviewParams {
                task: "safe".into(),
                retry_count: Some(ROUTE_PREVIEW_MAX_SIGNAL + 1),
                ..Default::default()
            },
            RoutePreviewParams {
                task: "safe".into(),
                stage: Some("x".repeat(ROUTE_PREVIEW_MAX_VALUE_BYTES + 1)),
                ..Default::default()
            },
            RoutePreviewParams {
                task: "safe".into(),
                extra_tags: vec!["tag".into(); ROUTE_PREVIEW_MAX_LIST_ITEMS + 1],
                ..Default::default()
            },
            RoutePreviewParams {
                task: "safe".into(),
                candidate_profiles: vec!["safe-profile".into(); ROUTE_PREVIEW_MAX_LIST_ITEMS + 1],
                ..Default::default()
            },
            RoutePreviewParams {
                task: "CANARY\0TASK".into(),
                ..Default::default()
            },
            RoutePreviewParams {
                task: "safe".into(),
                stage: Some("review\nCANARY".into()),
                ..Default::default()
            },
            RoutePreviewParams {
                task: "safe".into(),
                extra_tags: vec!["security\u{1b}CANARY".into()],
                ..Default::default()
            },
        ];

        for params in cases {
            let error = route_preview(&loaded, params).unwrap_err();
            assert_eq!(
                diagnostic_error(&error).kind,
                DiagnosticErrorKind::InvalidInput
            );
            assert!(!error.to_string().contains("CANARY"));
        }
    }

    #[test]
    fn route_preview_applies_config_budget_before_routing() {
        let mut loaded = loaded_config(false);
        loaded
            .config
            .profiles
            .get_mut("safe-profile")
            .unwrap()
            .model = Some(ModelId::new("m".repeat(ROUTE_PREVIEW_MAX_VALUE_BYTES + 1)));

        let error = route_preview(
            &loaded,
            RoutePreviewParams {
                task: "safe".into(),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(
            diagnostic_error(&error).kind,
            DiagnosticErrorKind::BudgetExceeded
        );
    }

    #[test]
    fn metadata_budget_walk_stops_before_unbounded_nested_values() {
        let mut loaded = loaded_config(false);
        loaded
            .config
            .profiles
            .get_mut("safe-profile")
            .unwrap()
            .params
            .get_or_insert_default()
            .extra
            .insert(
                "bounded-key".into(),
                serde_json::Value::Array(vec![
                    serde_json::Value::Null;
                    DIAGNOSTIC_MAX_METADATA_BYTES + 1
                ]),
            );

        let error = check_config(&loaded).unwrap_err();
        assert_eq!(
            diagnostic_error(&error).kind,
            DiagnosticErrorKind::BudgetExceeded
        );
    }

    #[test]
    fn metadata_budget_rejects_excessive_nesting_before_deep_recursion() {
        let mut loaded = loaded_config(false);
        let mut nested = serde_json::Value::Null;
        for _ in 0..=DIAGNOSTIC_MAX_METADATA_DEPTH {
            nested = serde_json::Value::Array(vec![nested]);
        }
        loaded
            .config
            .profiles
            .get_mut("safe-profile")
            .unwrap()
            .params
            .get_or_insert_default()
            .extra
            .insert("bounded-key".into(), nested);

        let error = check_config(&loaded).unwrap_err();
        assert_eq!(
            diagnostic_error(&error).kind,
            DiagnosticErrorKind::BudgetExceeded
        );
    }
}
