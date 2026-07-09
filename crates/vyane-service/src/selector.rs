//! Selector resolution: turning a raw selector string into a failover chain.
//!
//! A selector is either a *profile name* (resolved through the config's
//! failover chain, so a profile with a `failover` list yields multiple targets)
//! or a *provider/model pair* (`provider/model`, always a single direct-HTTP
//! target). This is the single chokepoint the dispatch path, the detached
//! worker, and the workflow engine all share — the env lookup is injected so
//! secrets-file resolution stays identical across every front-end.

use std::collections::BTreeMap;

use anyhow::{Result, bail};
use vyane_config::{ConfigLayers, ProfilePatch, RawRoot, ResolvedConfig};
use vyane_core::{BoundTarget, ModelId};

use crate::config::LoadedConfig;

/// Resolve a raw selector into a `Vec<BoundTarget>` failover chain.
///
/// - `provider/model` → a synthetic single direct-HTTP profile (no failover,
///   no harness — the CLI/API/MCP cannot guess which harness you meant).
/// - anything else → treated as a profile name and resolved through
///   [`ResolvedConfig::resolve_failover_with`], expanding the profile's
///   declared failover legs.
pub fn resolve_target_chain(loaded: &LoadedConfig, raw: &str) -> Result<Vec<BoundTarget>> {
    if let Some((provider, model)) = parse_provider_model(raw) {
        let root = provider_model_config(&loaded.config, provider, model)?;
        return resolve_temp_profile(root, "__cli_target", loaded);
    }
    Ok(loaded
        .config
        .resolve_failover_with(raw, &|key| loaded.env_lookup(key))?)
}

/// Parse a `provider/model` selector, returning `None` when the string is not
/// in that form (so the caller falls back to profile-name resolution).
pub fn parse_provider_model(raw: &str) -> Option<(&str, &str)> {
    let (provider, model) = raw.split_once('/')?;
    (!provider.is_empty() && !model.is_empty()).then_some((provider, model))
}

/// Split a comma-separated target list, trimming whitespace and dropping empties.
/// Errors when the list is empty after cleaning.
pub fn split_targets(raw: &str) -> Result<Vec<String>> {
    let targets = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if targets.is_empty() {
        bail!("targets must include at least one target");
    }
    Ok(targets)
}

fn provider_model_config(config: &ResolvedConfig, provider: &str, model: &str) -> Result<RawRoot> {
    let provider_config = config.providers.get(provider)?;
    let profile = ProfilePatch {
        provider: Some(provider.to_string()),
        protocol: Some(provider_config.protocol),
        harness: Some("none".to_string()),
        model: Some(ModelId::new(model)),
        sandbox: None,
        params: None,
        failover: None,
        tier: None,
        tags: None,
    };
    let mut profiles = BTreeMap::new();
    profiles.insert("__cli_target".to_string(), profile);
    Ok(RawRoot {
        providers: BTreeMap::new(),
        profiles,
    })
}

fn resolve_temp_profile(
    root: RawRoot,
    profile: &str,
    loaded: &LoadedConfig,
) -> Result<Vec<BoundTarget>> {
    let mut layers = ConfigLayers {
        providers: loaded.config.providers.clone(),
        profiles: loaded.config.profiles.clone(),
    };
    layers.merge(&root)?;
    let config: ResolvedConfig = layers.into();
    Ok(vec![config.resolve_profile_with(profile, &|key| {
        loaded.env_lookup(key)
    })?])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_model_split() {
        assert_eq!(
            parse_provider_model("openai/gpt-4o"),
            Some(("openai", "gpt-4o"))
        );
        assert_eq!(
            parse_provider_model("anthropic/claude-4"),
            Some(("anthropic", "claude-4"))
        );
    }

    #[test]
    fn profile_name_is_not_provider_model() {
        assert_eq!(parse_provider_model("default"), None);
        // No slash at all.
        assert_eq!(parse_provider_model("my-profile"), None);
    }

    #[test]
    fn provider_model_rejects_empty_halves() {
        assert_eq!(parse_provider_model("/gpt-4o"), None);
        assert_eq!(parse_provider_model("openai/"), None);
        assert_eq!(parse_provider_model("/"), None);
    }

    #[test]
    fn split_targets_basic() {
        assert_eq!(split_targets("a, b ,c").unwrap(), vec!["a", "b", "c"]);
    }

    #[test]
    fn split_targets_drops_empties_then_errors() {
        assert!(split_targets(" , , ").is_err());
        assert_eq!(split_targets("a,,b").unwrap(), vec!["a", "b"]);
    }
}
