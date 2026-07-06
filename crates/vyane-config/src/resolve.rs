//! Profile and failover-chain resolution: turn the merged config state
//! (`vyane_provider::ProviderRegistry` + parsed profile patches) into
//! [`vyane_core::BoundTarget`]s, plus the `EnvPolicy` inject set for harness
//! targets.

use std::collections::BTreeMap;

use vyane_core::{
    AdapterTransport, BoundTarget, EnvPolicy, ErrorKind, GenParams, HarnessKind, ModelId, Result,
    Sandbox, Target, VyaneError,
};
use vyane_provider::ProviderRegistry;

use crate::layer::ConfigLayers;
use crate::model::{ProfilePatch, RawFailoverElement};

/// The fully merged config, ready to resolve profiles and failover chains
/// against. A thin wrapper over [`ConfigLayers`] — kept as a separate type
/// so resolution has a stable, minimal surface independent of how many
/// layers were merged to produce it.
#[derive(Debug, Clone, Default)]
pub struct ResolvedConfig {
    pub providers: ProviderRegistry,
    pub profiles: BTreeMap<String, ProfilePatch>,
}

impl From<ConfigLayers> for ResolvedConfig {
    fn from(layers: ConfigLayers) -> Self {
        Self {
            providers: layers.providers,
            profiles: layers.profiles,
        }
    }
}

impl ResolvedConfig {
    fn profile(&self, name: &str) -> Result<&ProfilePatch> {
        self.profiles.get(name).ok_or_else(|| {
            VyaneError::new(ErrorKind::NotFound, format!("unknown profile `{name}`"))
        })
    }

    /// Resolve one profile by name into a [`BoundTarget`], reading each
    /// target's API key from the real process environment. Does not follow
    /// `failover` — see [`Self::resolve_failover`] for chain resolution.
    pub fn resolve_profile(&self, name: &str) -> Result<BoundTarget> {
        self.resolve_profile_with(name, &real_env_lookup)
    }

    /// Resolve a profile's full failover chain: the profile itself first,
    /// followed by each `failover` element, **in order**, each fully and
    /// independently resolved. A model id is never carried from one element
    /// to the next — every element either names a profile (which resolves
    /// its own provider/model pair from its own config) or pins its own
    /// `provider/model` pair directly.
    pub fn resolve_failover(&self, name: &str) -> Result<Vec<BoundTarget>> {
        self.resolve_failover_with(name, &real_env_lookup)
    }

    /// Same as [`Self::resolve_profile`], but reads API keys through an
    /// injected lookup instead of the real process environment. Lets tests
    /// exercise resolution deterministically without mutating process-global
    /// state, which the 2024 edition makes `unsafe` and which would race
    /// across parallel test threads.
    pub fn resolve_profile_with(
        &self,
        name: &str,
        env_lookup: &dyn Fn(&str) -> Option<String>,
    ) -> Result<BoundTarget> {
        let patch = self.profile(name)?;
        self.resolve_profile_patch(patch, name, env_lookup)
    }

    /// Same as [`Self::resolve_failover`], with an injected env lookup.
    pub fn resolve_failover_with(
        &self,
        name: &str,
        env_lookup: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Vec<BoundTarget>> {
        let patch = self.profile(name)?;
        let primary = self.resolve_profile_patch(patch, name, env_lookup)?;
        let mut chain = vec![primary];

        if let Some(elements) = &patch.failover {
            for element in elements {
                let bound = match element {
                    RawFailoverElement::ProfileName(profile_name) => {
                        self.resolve_profile_with(profile_name, env_lookup)?
                    }
                    RawFailoverElement::ProviderModel { provider, model } => self
                        .resolve_provider_model(
                            provider,
                            model,
                            &gen_params_from_patch(patch),
                            env_lookup,
                        )?,
                };
                chain.push(bound);
            }
        }
        Ok(chain)
    }

    fn resolve_profile_patch(
        &self,
        patch: &ProfilePatch,
        profile_name: &str,
        env_lookup: &dyn Fn(&str) -> Option<String>,
    ) -> Result<BoundTarget> {
        let provider_id = patch.provider.as_deref().ok_or_else(|| {
            VyaneError::new(
                ErrorKind::Config,
                format!("profile `{profile_name}` has no `provider` set"),
            )
        })?;
        let provider = self.providers.get(provider_id)?;

        let protocol = patch.protocol.unwrap_or(provider.protocol);
        let harness = parse_harness(patch.harness.as_deref());
        // Sandbox is validated here so profile authors get immediate
        // feedback on typos, but it is not carried on `Target`/`BoundTarget`
        // — it belongs to `TaskSpec` at dispatch time (`vyane_core::task`).
        let _sandbox: Sandbox = patch.sandbox.unwrap_or_default();
        let model = provider.resolve_model(patch.model.as_ref())?;
        let endpoint = provider.resolve_endpoint_with(env_lookup)?;
        let params = gen_params_from_patch(patch);
        let transport = transport_for(&harness);

        Ok(BoundTarget {
            target: Target {
                provider: vyane_core::ProviderId::new(provider_id.to_string()),
                protocol,
                harness,
                model,
            },
            transport,
            endpoint: Some(endpoint),
            params,
        })
    }

    /// Resolve a `provider/model` pair directly (used for failover elements
    /// that pin a provider/model rather than naming a profile). A bare
    /// `provider/model` string carries no protocol/harness of its own, so it
    /// resolves to the pinned provider's declared protocol with no harness
    /// (direct HTTP) — matching `profiles.example.toml`'s usage
    /// (`"openai/a-fast-openai-model"` as a direct-chat failover leg) — and
    /// inherits the resolving profile's generation params.
    fn resolve_provider_model(
        &self,
        provider_id: &str,
        model: &ModelId,
        params: &GenParams,
        env_lookup: &dyn Fn(&str) -> Option<String>,
    ) -> Result<BoundTarget> {
        let provider = self.providers.get(provider_id)?;
        let resolved_model = provider.resolve_model(Some(model))?;
        let endpoint = provider.resolve_endpoint_with(env_lookup)?;
        Ok(BoundTarget {
            target: Target {
                provider: vyane_core::ProviderId::new(provider_id.to_string()),
                protocol: provider.protocol,
                harness: None,
                model: resolved_model,
            },
            transport: AdapterTransport::DirectHttp,
            endpoint: Some(endpoint),
            params: params.clone(),
        })
    }

    /// Build the `EnvPolicy` for a harness `BoundTarget`: `mode` defaults to
    /// `Scrub` (per `vyane_core::EnvPolicy::default`), and `inject` is
    /// filled from the target's provider's `env_inject` rules. Returns
    /// `None` for direct-HTTP targets, since only harness runs need a child
    /// environment.
    pub fn env_policy_for(&self, bound: &BoundTarget) -> Result<Option<EnvPolicy>> {
        if bound.target.harness.is_none() {
            return Ok(None);
        }
        let provider = self.providers.get(bound.target.provider.as_str())?;
        let endpoint = bound
            .endpoint
            .as_ref()
            .ok_or_else(|| VyaneError::new(ErrorKind::Config, "harness target has no endpoint"))?;
        let inject = provider.env_injections(endpoint, &bound.target.model);
        Ok(Some(EnvPolicy {
            inject,
            ..EnvPolicy::scrubbed()
        }))
    }
}

fn real_env_lookup(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// Parse a `[profiles.<name>].harness` value into `Option<HarnessKind>`.
/// `"none"` or the field being absent both mean direct HTTP with no
/// harness; any other string names a harness kind.
fn parse_harness(raw: Option<&str>) -> Option<HarnessKind> {
    match raw {
        None | Some("none") => None,
        Some(other) => Some(HarnessKind::from(other)),
    }
}

fn transport_for(harness: &Option<HarnessKind>) -> AdapterTransport {
    match harness {
        Some(_) => AdapterTransport::CliWrap,
        None => AdapterTransport::DirectHttp,
    }
}

fn gen_params_from_patch(patch: &ProfilePatch) -> GenParams {
    let mut params = GenParams::default();
    if let Some(p) = &patch.params {
        p.apply_over(&mut params);
    }
    params
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::model::RawRoot;
    use vyane_core::{AdapterTransport, Effort, ProviderId};

    fn config_from_toml(src: &str) -> ResolvedConfig {
        let root: RawRoot = toml::from_str(src).unwrap();
        let mut layers = ConfigLayers::new();
        layers.merge(&root).unwrap();
        layers.into()
    }

    const BASE_TOML: &str = r#"
        [providers.anthropic]
        base_url      = "https://api.anthropic.example"
        api_key_env   = "VYANE_RESOLVE_TEST_ANTHROPIC_KEY"
        auth_style    = "x_api_key"
        protocol      = "anthropic_messages"
        default_model = "a-capable-anthropic-model"

        [providers.openai]
        base_url      = "https://api.openai.example/v1"
        api_key_env   = "VYANE_RESOLVE_TEST_OPENAI_KEY"
        auth_style    = "bearer"
        protocol      = "openai_chat"
        default_model = "a-fast-openai-model"

        [providers.some-relay]
        base_url      = "https://relay.example/v1"
        api_key_env   = "VYANE_RESOLVE_TEST_RELAY_KEY"
        auth_style    = "bearer"
        protocol      = "openai_chat"
        default_model = "relay-hosted-model"

        [providers.anthropic.env_inject]
        ANTHROPIC_BASE_URL = "base_url"
        ANTHROPIC_API_KEY  = "api_key"
        ANTHROPIC_MODEL    = "model"

        [profiles.review]
        provider = "anthropic"
        protocol = "anthropic_messages"
        harness  = "none"
        model    = "a-capable-anthropic-model"

        [profiles.review.params]
        effort = "high"

        [profiles.builder]
        provider = "anthropic"
        protocol = "anthropic_messages"
        harness  = "claude-code"
        model    = "a-capable-anthropic-model"
        sandbox  = "write"

        [profiles.resilient-review]
        provider = "anthropic"
        protocol = "anthropic_messages"
        harness  = "none"
        model    = "a-capable-anthropic-model"
        failover = ["review", "some-relay/relay-hosted-model", "openai/a-fast-openai-model"]
    "#;

    /// A deterministic fake lookup standing in for the process environment
    /// in tests: every `*_KEY` env var named in `BASE_TOML` resolves to a
    /// fixed test secret, so resolution succeeds without touching real
    /// process env.
    fn fake_env(name: &str) -> Option<String> {
        match name {
            "VYANE_RESOLVE_TEST_ANTHROPIC_KEY" => Some("sk-ant-test".to_string()),
            "VYANE_RESOLVE_TEST_OPENAI_KEY" => Some("sk-openai-test".to_string()),
            "VYANE_RESOLVE_TEST_RELAY_KEY" => Some("sk-relay-test".to_string()),
            _ => None,
        }
    }

    #[test]
    fn harness_none_profile_resolves_to_direct_http() {
        let config = config_from_toml(BASE_TOML);
        let bound = config.resolve_profile_with("review", &fake_env).unwrap();
        assert_eq!(bound.transport, AdapterTransport::DirectHttp);
        assert_eq!(bound.target.harness, None);
        assert_eq!(bound.target.provider, ProviderId::new("anthropic"));
        assert_eq!(
            bound.target.model,
            ModelId::new("a-capable-anthropic-model")
        );
        assert_eq!(bound.params.effort, Some(Effort::High));
    }

    #[test]
    fn harness_claude_code_profile_resolves_to_cli_wrap() {
        let config = config_from_toml(BASE_TOML);
        let bound = config.resolve_profile_with("builder", &fake_env).unwrap();
        assert_eq!(bound.transport, AdapterTransport::CliWrap);
        assert_eq!(bound.target.harness, Some(HarnessKind::ClaudeCode));
    }

    #[test]
    fn resolve_profile_unknown_name_is_not_found() {
        let config = config_from_toml(BASE_TOML);
        let err = config
            .resolve_profile_with("does-not-exist", &fake_env)
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::NotFound);
    }

    #[test]
    fn resolve_endpoint_missing_env_var_error_names_it() {
        let config = config_from_toml(BASE_TOML);
        // Lookup that always returns None simulates every env var unset.
        let err = config
            .resolve_profile_with("review", &|_| None)
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::Config);
        assert!(
            err.message.contains("VYANE_RESOLVE_TEST_ANTHROPIC_KEY"),
            "error must name the exact env var: {}",
            err.message
        );
    }

    #[test]
    fn failover_chain_resolves_each_element_fully_and_independently() {
        let config = config_from_toml(BASE_TOML);
        let chain = config
            .resolve_failover_with("resilient-review", &fake_env)
            .unwrap();

        // Primary + 3 failover legs = 4 targets, in order.
        assert_eq!(chain.len(), 4);

        assert_eq!(chain[0].target.provider, ProviderId::new("anthropic"));
        assert_eq!(
            chain[0].target.model,
            ModelId::new("a-capable-anthropic-model")
        );

        // Leg 2 names the "review" profile again — same provider/model.
        assert_eq!(chain[1].target.provider, ProviderId::new("anthropic"));
        assert_eq!(
            chain[1].target.model,
            ModelId::new("a-capable-anthropic-model")
        );

        // Leg 3 pins some-relay/relay-hosted-model directly.
        assert_eq!(chain[2].target.provider, ProviderId::new("some-relay"));
        assert_eq!(chain[2].target.model, ModelId::new("relay-hosted-model"));

        // Leg 4 pins openai/a-fast-openai-model directly.
        assert_eq!(chain[3].target.provider, ProviderId::new("openai"));
        assert_eq!(chain[3].target.model, ModelId::new("a-fast-openai-model"));

        // Model-leak assertion: every element's model is paired only with
        // its own provider — no element carries another element's model.
        for bound in &chain {
            let expected_model = match bound.target.provider.as_str() {
                "anthropic" => "a-capable-anthropic-model",
                "some-relay" => "relay-hosted-model",
                "openai" => "a-fast-openai-model",
                other => panic!("unexpected provider in chain: {other}"),
            };
            assert_eq!(bound.target.model, ModelId::new(expected_model));
        }
    }

    #[test]
    fn env_policy_for_harness_target_fills_inject_from_provider_rules() {
        let config = config_from_toml(BASE_TOML);
        let bound = config.resolve_profile_with("builder", &fake_env).unwrap();
        let policy = config
            .env_policy_for(&bound)
            .unwrap()
            .expect("harness target must produce a policy");
        assert_eq!(
            policy.inject.get("ANTHROPIC_BASE_URL").map(String::as_str),
            Some("https://api.anthropic.example")
        );
        assert_eq!(
            policy.inject.get("ANTHROPIC_API_KEY").map(String::as_str),
            Some("sk-ant-test")
        );
        assert_eq!(
            policy.inject.get("ANTHROPIC_MODEL").map(String::as_str),
            Some("a-capable-anthropic-model")
        );
        assert_eq!(policy.mode, vyane_core::InheritMode::Scrub);
    }

    #[test]
    fn env_policy_for_direct_http_target_is_none() {
        let config = config_from_toml(BASE_TOML);
        let bound = config.resolve_profile_with("review", &fake_env).unwrap();
        assert!(config.env_policy_for(&bound).unwrap().is_none());
    }
}
