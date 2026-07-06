//! # vyane-provider
//!
//! Provider registry: resolves a [`vyane_core::ProviderId`] into its
//! endpoint, credentials and auth material, and builds the per-provider
//! environment-injection set for harness runs.
//!
//! A [`Provider`] never stores a key value — only `api_key_env`, the name of
//! the environment variable Vyane reads the secret from at resolve time. See
//! `docs/plan/WP-01.md` for the work-package plan.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use vyane_core::{AuthMaterial, AuthStyle, Endpoint, ErrorKind, ModelId, Protocol, Secret,
                 VyaneError};

/// A source of an environment variable's value at env-injection time.
///
/// Maps directly onto the string values used in `[providers.<id>.env_inject]`
/// tables (see `profiles.example.toml`): each key is the variable name to
/// inject into the child process, and the value names which resolved field
/// supplies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvInjectSource {
    /// The provider's `base_url`.
    BaseUrl,
    /// The resolved API key (read from `api_key_env` at resolve time).
    ApiKey,
    /// The resolved model id.
    Model,
}

/// One entry in a provider's `[providers.<id>].env_inject` table: an
/// environment variable name mapped to the resolved field that fills it.
pub type EnvInjectMap = BTreeMap<String, EnvInjectSource>;

/// A configured provider: who supplies the endpoint, key, quota and billing.
///
/// Holds only *configuration* — `api_key_env` is indirection to an
/// environment variable name, never a key value. Mirrors one
/// `[providers.<id>]` TOML table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Provider {
    pub base_url: String,
    /// Name of the environment variable holding the API key. Indirection
    /// only — the config never stores the key itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    pub auth_style: AuthStyle,
    pub protocol: Protocol,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<ModelId>,
    /// Free-form passthrough table (e.g. `max_output_tokens`).
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, serde_json::Value>,
    /// Per-provider env-injection rules for harness runs.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env_inject: EnvInjectMap,
}

impl Provider {
    /// Resolve this provider against an explicit or default model into a
    /// [`vyane_core::Endpoint`], reading the API key from the process
    /// environment variable named by `api_key_env`.
    ///
    /// `api_key_env` absent entirely means the harness authenticates
    /// natively (no `Endpoint::auth`); `api_key_env` present but the named
    /// variable unset is a [`ErrorKind::Config`] error naming the exact
    /// variable, since that configuration explicitly promised a key.
    pub fn resolve_endpoint(&self) -> vyane_core::Result<Endpoint> {
        self.resolve_endpoint_with(|name| std::env::var(name).ok())
    }

    /// Same as [`Self::resolve_endpoint`], but reads the secret through an
    /// injected lookup instead of the real process environment. Lets tests
    /// exercise env-var resolution deterministically without mutating
    /// process-global state (which would need `unsafe` under the 2024
    /// edition and would race across parallel test threads).
    pub fn resolve_endpoint_with(
        &self,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> vyane_core::Result<Endpoint> {
        let auth = match &self.api_key_env {
            None => None,
            Some(var_name) => {
                let value = lookup(var_name).ok_or_else(|| {
                    VyaneError::new(
                        ErrorKind::Config,
                        format!(
                            "provider requires environment variable `{var_name}` for its API \
                             key, but it is not set"
                        ),
                    )
                })?;
                Some(AuthMaterial {
                    style: self.auth_style,
                    secret: Secret::new(value),
                })
            }
        };
        Ok(Endpoint {
            base_url: self.base_url.clone(),
            auth,
        })
    }

    /// Resolve the model to use: an explicit override, or this provider's
    /// `default_model`.
    pub fn resolve_model(&self, explicit: Option<&ModelId>) -> vyane_core::Result<ModelId> {
        explicit
            .cloned()
            .or_else(|| self.default_model.clone())
            .ok_or_else(|| {
                VyaneError::new(
                    ErrorKind::Config,
                    "no model specified and provider has no default_model",
                )
            })
    }

    /// Build the `EnvPolicy.inject` entries this provider contributes for a
    /// harness run, given the endpoint and model already resolved for it.
    ///
    /// Each `env_inject` rule names an environment variable and which
    /// resolved field supplies its value. A rule requesting the API key when
    /// the endpoint carries no auth material is skipped (native-auth
    /// harnesses have nothing to inject there).
    pub fn env_injections(&self, endpoint: &Endpoint, model: &ModelId) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        for (var_name, source) in &self.env_inject {
            let value = match source {
                EnvInjectSource::BaseUrl => Some(endpoint.base_url.clone()),
                EnvInjectSource::ApiKey => endpoint
                    .auth
                    .as_ref()
                    .map(|auth| auth.secret.expose().to_string()),
                EnvInjectSource::Model => Some(model.as_str().to_string()),
            };
            if let Some(value) = value {
                out.insert(var_name.clone(), value);
            }
        }
        out
    }
}

/// Registry of configured providers, keyed by provider id string.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderRegistry {
    pub providers: BTreeMap<String, Provider>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, id: &str) -> vyane_core::Result<&Provider> {
        self.providers.get(id).ok_or_else(|| {
            VyaneError::new(ErrorKind::NotFound, format!("unknown provider `{id}`"))
        })
    }

    pub fn insert(&mut self, id: impl Into<String>, provider: Provider) {
        self.providers.insert(id.into(), provider);
    }

    /// Merge `other` on top of `self`, per provider *and* per field: a
    /// higher-precedence table that only sets one field of an existing
    /// provider must not clear that provider's other fields. Providers
    /// present only in `other` are added wholesale.
    pub fn merge_over(&mut self, other: &ProviderPatchSet) {
        for (id, patch) in &other.providers {
            match self.providers.get_mut(id) {
                Some(existing) => existing.apply_patch(patch),
                None => {
                    if let Some(full) = patch.as_full_provider() {
                        self.providers.insert(id.clone(), full);
                    }
                }
            }
        }
    }
}

/// A partial provider definition as it appears in a layered (user/project)
/// config file: every field optional, so a layer can override just one
/// field without needing to restate the rest.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProviderPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_style: Option<AuthStyle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<Protocol>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<ModelId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_inject: Option<EnvInjectMap>,
}

impl ProviderPatch {
    /// A patch that fully specifies a provider can stand alone as a new
    /// entry (used when a layer introduces a provider id the lower layers
    /// never defined).
    fn as_full_provider(&self) -> Option<Provider> {
        Some(Provider {
            base_url: self.base_url.clone()?,
            api_key_env: self.api_key_env.clone(),
            auth_style: self.auth_style?,
            protocol: self.protocol?,
            default_model: self.default_model.clone(),
            extra: self.extra.clone().unwrap_or_default(),
            env_inject: self.env_inject.clone().unwrap_or_default(),
        })
    }
}

impl From<&Provider> for ProviderPatch {
    fn from(p: &Provider) -> Self {
        Self {
            base_url: Some(p.base_url.clone()),
            api_key_env: p.api_key_env.clone(),
            auth_style: Some(p.auth_style),
            protocol: Some(p.protocol),
            default_model: p.default_model.clone(),
            extra: if p.extra.is_empty() {
                None
            } else {
                Some(p.extra.clone())
            },
            env_inject: if p.env_inject.is_empty() {
                None
            } else {
                Some(p.env_inject.clone())
            },
        }
    }
}

impl Provider {
    /// Apply a patch's `Some` fields over `self`, leaving fields the patch
    /// left `None` untouched.
    fn apply_patch(&mut self, patch: &ProviderPatch) {
        if let Some(v) = &patch.base_url {
            self.base_url = v.clone();
        }
        if patch.api_key_env.is_some() {
            self.api_key_env = patch.api_key_env.clone();
        }
        if let Some(v) = patch.auth_style {
            self.auth_style = v;
        }
        if let Some(v) = patch.protocol {
            self.protocol = v;
        }
        if patch.default_model.is_some() {
            self.default_model = patch.default_model.clone();
        }
        if let Some(v) = &patch.extra {
            self.extra = v.clone();
        }
        if let Some(v) = &patch.env_inject {
            self.env_inject = v.clone();
        }
    }
}

/// A set of provider patches, one per provider id, as parsed from a single
/// config layer (user file, project file, …).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderPatchSet {
    pub providers: BTreeMap<String, ProviderPatch>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn anthropic_provider() -> Provider {
        Provider {
            base_url: "https://api.anthropic.example".to_string(),
            api_key_env: Some("VYANE_TEST_ANTHROPIC_KEY".to_string()),
            auth_style: AuthStyle::XApiKey,
            protocol: Protocol::AnthropicMessages,
            default_model: Some(ModelId::new("a-capable-anthropic-model")),
            extra: serde_json::Map::new(),
            env_inject: BTreeMap::new(),
        }
    }

    #[test]
    fn resolve_endpoint_reads_key_from_named_env_var() {
        let provider = anthropic_provider();
        let endpoint = provider
            .resolve_endpoint_with(|name| {
                (name == "VYANE_TEST_ANTHROPIC_KEY").then(|| "sk-test-value".to_string())
            })
            .unwrap();
        assert_eq!(endpoint.base_url, "https://api.anthropic.example");
        let auth = endpoint.auth.expect("auth material expected");
        assert_eq!(auth.style, AuthStyle::XApiKey);
        assert_eq!(auth.secret.expose(), "sk-test-value");
    }

    #[test]
    fn resolve_endpoint_missing_env_var_names_it_in_error() {
        let mut provider = anthropic_provider();
        provider.api_key_env = Some("VYANE_TEST_DEFINITELY_UNSET_KEY".to_string());
        let err = provider.resolve_endpoint_with(|_| None).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Config);
        assert!(
            err.message.contains("VYANE_TEST_DEFINITELY_UNSET_KEY"),
            "error message must name the exact env var: {}",
            err.message
        );
    }

    #[test]
    fn resolve_endpoint_no_api_key_env_means_native_auth() {
        let mut provider = anthropic_provider();
        provider.api_key_env = None;
        let endpoint = provider.resolve_endpoint_with(|_| None).unwrap();
        assert!(endpoint.auth.is_none());
    }

    #[test]
    fn resolve_model_prefers_explicit_over_default() {
        let provider = anthropic_provider();
        let explicit = ModelId::new("explicit-model");
        assert_eq!(
            provider.resolve_model(Some(&explicit)).unwrap(),
            explicit
        );
        assert_eq!(
            provider.resolve_model(None).unwrap(),
            ModelId::new("a-capable-anthropic-model")
        );
    }

    #[test]
    fn resolve_model_errors_without_explicit_or_default() {
        let mut provider = anthropic_provider();
        provider.default_model = None;
        let err = provider.resolve_model(None).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Config);
    }

    #[test]
    fn env_injections_map_base_url_key_and_model() {
        let mut provider = anthropic_provider();
        provider.env_inject.insert(
            "ANTHROPIC_BASE_URL".to_string(),
            EnvInjectSource::BaseUrl,
        );
        provider
            .env_inject
            .insert("ANTHROPIC_API_KEY".to_string(), EnvInjectSource::ApiKey);
        provider
            .env_inject
            .insert("ANTHROPIC_MODEL".to_string(), EnvInjectSource::Model);

        let endpoint = Endpoint {
            base_url: "https://api.anthropic.example".to_string(),
            auth: Some(AuthMaterial {
                style: AuthStyle::XApiKey,
                secret: Secret::new("sk-test"),
            }),
        };
        let model = ModelId::new("a-capable-anthropic-model");
        let injections = provider.env_injections(&endpoint, &model);

        assert_eq!(
            injections.get("ANTHROPIC_BASE_URL").map(String::as_str),
            Some("https://api.anthropic.example")
        );
        assert_eq!(
            injections.get("ANTHROPIC_API_KEY").map(String::as_str),
            Some("sk-test")
        );
        assert_eq!(
            injections.get("ANTHROPIC_MODEL").map(String::as_str),
            Some("a-capable-anthropic-model")
        );
    }

    #[test]
    fn env_injections_skip_api_key_when_endpoint_has_no_auth() {
        let mut provider = anthropic_provider();
        provider
            .env_inject
            .insert("ANTHROPIC_API_KEY".to_string(), EnvInjectSource::ApiKey);
        let endpoint = Endpoint {
            base_url: "https://api.anthropic.example".to_string(),
            auth: None,
        };
        let model = ModelId::new("a-capable-anthropic-model");
        let injections = provider.env_injections(&endpoint, &model);
        assert!(!injections.contains_key("ANTHROPIC_API_KEY"));
    }

    #[test]
    fn merge_over_overrides_only_patched_field_leaves_siblings() {
        let mut registry = ProviderRegistry::new();
        registry.insert("anthropic", anthropic_provider());

        let mut patch_set = ProviderPatchSet::default();
        patch_set.providers.insert(
            "anthropic".to_string(),
            ProviderPatch {
                base_url: Some("https://overridden.example".to_string()),
                ..Default::default()
            },
        );
        registry.merge_over(&patch_set);

        let merged = registry.get("anthropic").unwrap();
        assert_eq!(merged.base_url, "https://overridden.example");
        // Siblings survive the single-field override untouched.
        assert_eq!(merged.auth_style, AuthStyle::XApiKey);
        assert_eq!(merged.protocol, Protocol::AnthropicMessages);
        assert_eq!(
            merged.default_model,
            Some(ModelId::new("a-capable-anthropic-model"))
        );
        assert_eq!(
            merged.api_key_env.as_deref(),
            Some("VYANE_TEST_ANTHROPIC_KEY")
        );
    }

    #[test]
    fn merge_over_adds_new_provider_from_full_patch() {
        let mut registry = ProviderRegistry::new();
        let mut patch_set = ProviderPatchSet::default();
        patch_set.providers.insert(
            "new-provider".to_string(),
            ProviderPatch::from(&anthropic_provider()),
        );
        registry.merge_over(&patch_set);
        assert!(registry.get("new-provider").is_ok());
    }

    #[test]
    fn provider_roundtrips_through_toml() {
        let provider = anthropic_provider();
        let serialized = toml::to_string(&provider).unwrap();
        let back: Provider = toml::from_str(&serialized).unwrap();
        assert_eq!(provider, back);
    }
}
