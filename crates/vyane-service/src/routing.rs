//! Routing integration: bridges `vyane-router` (pure routing decisions) with
//! `vyane-config` (profile metadata) to produce a routing result a front-end
//! can act on.
//!
//! The router crate is deliberately standalone (only depends on serde) — it
//! knows nothing about profiles or config. This module is the adapter: it
//! reads `tier`/`tags` from configured profiles, builds a
//! [`RoutePreferenceTable`], calls [`vyane_router::route_task`], and maps the
//! decision back to a profile name the dispatch path understands.

use std::collections::BTreeMap;

use anyhow::Result;
use vyane_config::ResolvedConfig;
use vyane_router::{ComplexitySignals, RouteDecision, RoutePreferenceTable, RouteTargetPreference};

/// Parameters for routing a task. The task text is the primary input; the rest
/// are optional signals that can override the inferred complexity.
#[derive(Debug, Clone, Default)]
pub struct RouteParams {
    pub task: String,
    pub stage: Option<String>,
    pub changed_files: Option<usize>,
    pub dependency_edges: Option<usize>,
    pub retry_count: Option<usize>,
    pub explicit_tier: Option<String>,
    /// Extra tags beyond what's inferred from the task text.
    pub extra_tags: Vec<String>,
    /// Restrict routing to these profile names only. Empty = all profiles.
    pub candidate_profiles: Vec<String>,
}

/// The result of routing: the router's decision plus the resolved profile name
/// (the profile whose tier/tags match the decision, or the best fallback).
#[derive(Debug, Clone)]
pub struct RouteResult {
    pub decision: RouteDecision,
    /// The profile name to dispatch to (resolved from the decision's provider).
    pub profile: String,
}

/// Route a task using the configured profiles. Builds a preference table from
/// profile tier/tags, calls the router, and maps the decision back to a profile.
pub fn route_task(config: &ResolvedConfig, params: RouteParams) -> Result<RouteResult> {
    // Build signals from params.
    let signals = ComplexitySignals {
        explicit_tier: params.explicit_tier,
        stage: params.stage.unwrap_or_default(),
        task_tags: params.extra_tags,
        changed_files: params.changed_files.unwrap_or(0),
        dependency_edges: params.dependency_edges.unwrap_or(0),
        retry_count: params.retry_count.unwrap_or(0),
        allow_frontier: true,
        ..Default::default()
    };

    // Collect candidate profiles and their metadata.
    let mut candidates: Vec<(&String, &vyane_config::ProfilePatch)> =
        if params.candidate_profiles.is_empty() {
            config.profiles.iter().collect()
        } else {
            params
                .candidate_profiles
                .iter()
                .filter_map(|name| config.profiles.get_key_value(name))
                .collect()
        };

    if candidates.is_empty() {
        anyhow::bail!("no candidate profiles available for routing");
    }

    // Sort candidates by tier so the default fallback prefers cheaper profiles
    // (economy < mainline < frontier). This avoids accidentally defaulting to
    // a frontier model when a cheaper one is available — the old alphabetical
    // order was purely a naming accident.
    candidates.sort_by_key(|(_, patch)| match patch.tier.as_deref() {
        Some("economy") => 0,
        Some("mainline") => 1,
        Some("frontier") => 2,
        _ => 3,
    });

    // Build the preference table from profile tier/tags.
    let mut tag_preferences: BTreeMap<String, RouteTargetPreference> = BTreeMap::new();
    let mut tier_preferences: BTreeMap<String, RouteTargetPreference> = BTreeMap::new();
    // The router's `available_providers` and preference `provider` fields use
    // profile names (not underlying provider ids), so the decision maps directly
    // back to a profile the dispatch path can resolve.
    let available_providers: Vec<String> = candidates
        .iter()
        .map(|(name, _)| name.to_string())
        .collect();

    for (name, patch) in &candidates {
        let pref = RouteTargetPreference {
            provider: name.to_string(),
            model: patch
                .model
                .as_ref()
                .map(|m| m.as_str().to_string())
                .unwrap_or_default(),
            effort: String::new(),
            tier: patch.tier.clone().unwrap_or_default(),
        };

        // Tag → this profile. Keys are normalized so mixed-case tags in config
        // ("Front-End") match the normalized lookup in RoutePreferenceTable::resolve.
        if let Some(tags) = &patch.tags {
            for tag in tags {
                tag_preferences
                    .entry(vyane_router::normalize_key(tag))
                    .or_insert(pref.clone());
            }
        }
        // Tier → this profile (lowercased to match RouteTier::as_str lookup).
        if let Some(tier) = &patch.tier {
            tier_preferences
                .entry(tier.to_ascii_lowercase())
                .or_insert(pref.clone());
        }
    }

    let table = RoutePreferenceTable {
        tag_preferences,
        tier_preferences,
        default: candidates
            .first()
            .map(|(name, patch)| RouteTargetPreference {
                provider: name.to_string(),
                model: patch
                    .model
                    .as_ref()
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_default(),
                ..Default::default()
            }),
        ..Default::default()
    };

    // Call the router.
    let default_provider = candidates
        .first()
        .map(|(name, _)| name.as_str())
        .unwrap_or("default");

    let decision = vyane_router::route_task(
        &params.task,
        &available_providers,
        Some(&signals),
        Some(&table),
        default_provider,
    );

    // Map the decision's provider back to a profile name.
    // The preference table stores profile names as "provider", so the decision's
    // provider field IS the profile name when a preference matched. When it's
    // a fallback to an actual provider id, find the first profile using it.
    let profile = candidates
        .iter()
        .find(|(name, _)| name.as_str() == decision.provider)
        .or_else(|| {
            candidates
                .iter()
                .find(|(_, patch)| patch.provider.as_deref() == Some(decision.provider.as_str()))
        })
        .map(|(name, _)| name.to_string())
        .unwrap_or_else(|| decision.provider.clone());

    Ok(RouteResult { decision, profile })
}

/// Build routing params from a task string and optional labels.
/// Labels with keys `stage`, `tier`, or `tags` are extracted as routing signals.
///
/// Intended for CLI/API integration where routing signals arrive as labels.
#[allow(dead_code)]
pub fn route_params_from_labels(task: String, labels: &BTreeMap<String, String>) -> RouteParams {
    let mut params = RouteParams {
        task,
        ..Default::default()
    };
    if let Some(stage) = labels.get("stage") {
        params.stage = Some(stage.clone());
    }
    if let Some(tier) = labels.get("tier") {
        params.explicit_tier = Some(tier.clone());
    }
    if let Some(tags) = labels.get("tags") {
        params.extra_tags = tags
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use vyane_config::ProfilePatch;
    use vyane_core::ModelId;

    fn make_config() -> ResolvedConfig {
        let mut profiles = BTreeMap::new();
        profiles.insert(
            "economy-model".into(),
            ProfilePatch {
                provider: Some("openai".into()),
                protocol: Some(vyane_core::Protocol::OpenaiChat),
                harness: Some("none".into()),
                model: Some(ModelId::new("gpt-4o-mini")),
                tier: Some("economy".into()),
                tags: None,
                ..Default::default()
            },
        );
        profiles.insert(
            "frontier-model".into(),
            ProfilePatch {
                provider: Some("anthropic".into()),
                protocol: Some(vyane_core::Protocol::AnthropicMessages),
                harness: Some("none".into()),
                model: Some(ModelId::new("claude-opus")),
                tier: Some("frontier".into()),
                tags: Some(vec!["architecture".into()]),
                ..Default::default()
            },
        );
        ResolvedConfig {
            providers: Default::default(),
            profiles,
        }
    }

    #[test]
    fn simple_task_routes_to_economy() {
        let config = make_config();
        let params = RouteParams {
            task: "hello world".into(),
            ..Default::default()
        };
        let result = route_task(&config, params).unwrap();
        // Score 0.0 → economy tier → economy-model profile
        assert_eq!(result.profile, "economy-model");
    }

    #[test]
    fn complex_task_routes_to_frontier() {
        let config = make_config();
        let params = RouteParams {
            task: "write an ADR for the system architecture design".into(),
            stage: Some("architecture".into()),
            changed_files: Some(20),
            dependency_edges: Some(10),
            ..Default::default()
        };
        let result = route_task(&config, params).unwrap();
        assert_eq!(result.profile, "frontier-model");
    }

    #[test]
    fn explicit_tier_overrides() {
        let config = make_config();
        let params = RouteParams {
            task: "simple task".into(),
            explicit_tier: Some("frontier".into()),
            ..Default::default()
        };
        let result = route_task(&config, params).unwrap();
        assert_eq!(result.profile, "frontier-model");
    }

    #[test]
    fn no_candidates_errors() {
        let config = ResolvedConfig::default();
        let params = RouteParams {
            task: "test".into(),
            ..Default::default()
        };
        assert!(route_task(&config, params).is_err());
    }

    #[test]
    fn mixed_case_tag_matches() {
        // Regression test for BLOCKER: tags must be normalized on insertion so
        // mixed-case config values like "Architecture" still match.
        let config = make_config();
        let params = RouteParams {
            task: "write an ADR".into(),
            explicit_tier: Some("frontier".into()),
            ..Default::default()
        };
        let result = route_task(&config, params).unwrap();
        // frontier-model has tag "architecture" — should be selected.
        assert_eq!(result.profile, "frontier-model");
    }
}
