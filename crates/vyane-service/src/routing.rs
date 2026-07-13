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

use anyhow::{Result, bail};
use vyane_config::{RawFailoverElement, ResolvedConfig};
use vyane_core::{BoundTarget, Effort, TaskSpec};
use vyane_router::{
    ComplexitySignals, RouteDecision, RouteEffort, RoutePreferenceTable, RouteTargetPreference,
    effort_for_tier,
};

use crate::config::LoadedConfig;
use crate::selector::resolve_target_chain;

/// Parameters for routing a task. The task text is the primary input; the rest
/// are optional signals that can override the inferred complexity.
#[derive(Debug, Clone)]
pub struct RouteParams {
    pub task: String,
    pub stage: Option<String>,
    pub changed_files: Option<usize>,
    pub dependency_edges: Option<usize>,
    pub retry_count: Option<usize>,
    pub explicit_tier: Option<String>,
    /// Internal typed override supplied by trusted deferred-routing surfaces.
    /// Ordinary user labels may not set the reserved `routing.effort` key.
    pub explicit_effort: Option<Effort>,
    /// Extra tags beyond what's inferred from the task text.
    pub extra_tags: Vec<String>,
    /// Restrict routing to these profile names only. Empty = all profiles.
    pub candidate_profiles: Vec<String>,
    /// Whether frontier-tier profiles may be selected. Defaults to true.
    pub allow_frontier: bool,
}

impl Default for RouteParams {
    fn default() -> Self {
        Self {
            task: String::new(),
            stage: None,
            changed_files: None,
            dependency_edges: None,
            retry_count: None,
            explicit_tier: None,
            explicit_effort: None,
            extra_tags: Vec::new(),
            candidate_profiles: Vec::new(),
            allow_frontier: true,
        }
    }
}

/// The result of routing: the router's decision plus the resolved profile name
/// (the profile whose tier/tags match the decision, or the best fallback).
#[derive(Debug, Clone)]
pub struct RouteResult {
    pub decision: RouteDecision,
    /// The profile name to dispatch to. `decision.provider` remains the actual
    /// provider id; profile identity is never overloaded into that field.
    pub profile: String,
}

/// A concrete target chain ready for kernel dispatch, plus the routing
/// decision when the caller selected `auto`.
#[derive(Debug, Clone)]
pub struct DispatchPlan {
    /// The explicit profile or provider/model selector ultimately resolved.
    pub selector: String,
    pub chain: Vec<BoundTarget>,
    pub route: Option<RouteResult>,
}

/// Resolve an explicit selector or execute the complete auto-routing path.
/// The latter applies the decision's effort to every failover leg and writes
/// canonical routing labels onto the task for ledger observability.
pub fn plan_dispatch(
    loaded: &LoadedConfig,
    selector: &str,
    task: &mut TaskSpec,
) -> Result<DispatchPlan> {
    if !selector.eq_ignore_ascii_case("auto") {
        return Ok(DispatchPlan {
            selector: selector.to_string(),
            chain: resolve_target_chain(loaded, selector)?,
            route: None,
        });
    }

    let route_params = route_params_from_labels(task.prompt.clone(), &task.labels)?;
    let allow_frontier = route_params.allow_frontier;
    let route = route_task(&loaded.config, route_params)?;
    let chain = resolve_target_chain(loaded, &route.profile)?;
    let mut chain = guard_route_chain(loaded, &route.profile, chain, allow_frontier)?;
    apply_route_effort(&mut chain, &route.decision);
    record_route_labels(task, &route, &chain);

    Ok(DispatchPlan {
        selector: route.profile.clone(),
        chain,
        route: Some(route),
    })
}

/// Route a task using the configured profiles. Builds a preference table from
/// profile tier/tags, calls the router, and maps the decision back to a profile.
pub fn route_task(config: &ResolvedConfig, params: RouteParams) -> Result<RouteResult> {
    validate_route_param_values(config, &params)?;
    let explicit_effort = params.explicit_effort;

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

    // Treat the frontier guard as an eligibility filter, not merely a routing
    // preference. Otherwise a frontier-only candidate set can have its
    // preference rejected by the core router and then be selected again by the
    // provider fallback below.
    if !params.allow_frontier {
        candidates.retain(|(_, patch)| {
            !patch
                .tier
                .as_deref()
                .is_some_and(|tier| tier.eq_ignore_ascii_case("frontier"))
        });
        if candidates.is_empty() {
            bail!("frontier routing is disabled and no non-frontier candidate profiles remain");
        }
    }

    // A profile without a provider cannot resolve into an executable target
    // and cannot produce a provider-truthful RouteDecision. Exclude it before
    // building the keyed preference table so every possible decision carries
    // an exact, executable profile key.
    candidates.retain(|(_, patch)| patch.provider.is_some());
    if candidates.is_empty() {
        bail!("candidate profiles do not name any providers");
    }

    // Sort candidates by tier so the default fallback prefers cheaper profiles
    // (economy < mainline < frontier). This avoids accidentally defaulting to
    // a frontier model when a cheaper one is available — the old alphabetical
    // order was purely a naming accident.
    candidates.sort_by_key(|(_, patch)| match patch.tier.as_deref() {
        Some(tier) if tier.eq_ignore_ascii_case("economy") => 0,
        Some(tier) if tier.eq_ignore_ascii_case("mainline") => 1,
        Some(tier) if tier.eq_ignore_ascii_case("frontier") => 2,
        _ => 3,
    });

    // Build the preference table from profile tier/tags/stage.
    let mut tag_preferences: BTreeMap<String, RouteTargetPreference> = BTreeMap::new();
    let mut tier_preferences: BTreeMap<String, RouteTargetPreference> = BTreeMap::new();
    let mut stage_preferences: BTreeMap<String, RouteTargetPreference> = BTreeMap::new();
    // RouteDecision.provider is an actual provider id. Keep a separate
    // RouteResult.profile for the config selector used by dispatch.
    let mut available_providers = Vec::<String>::new();
    for (_, patch) in &candidates {
        if let Some(provider) = patch.provider.as_ref() {
            if !available_providers
                .iter()
                .any(|seen| seen.eq_ignore_ascii_case(provider))
            {
                available_providers.push(provider.clone());
            }
        }
    }
    if available_providers.is_empty() {
        bail!("candidate profiles do not name any providers");
    }

    for (name, patch) in &candidates {
        let Some(provider) = patch.provider.as_ref() else {
            continue;
        };
        let pref = RouteTargetPreference {
            selection_key: (*name).clone(),
            provider: provider.clone(),
            model: patch
                .model
                .as_ref()
                .map(|m| m.as_str().to_string())
                .unwrap_or_default(),
            effort: patch
                .params
                .as_ref()
                .and_then(|params| params.effort)
                .map(|effort| effort.as_str().to_string())
                .unwrap_or_default(),
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
        // Stage → this profile (normalized, highest precedence in resolution).
        if let Some(stage) = &patch.stage {
            stage_preferences
                .entry(vyane_router::normalize_key(stage))
                .or_insert(pref.clone());
        }
    }

    let table = RoutePreferenceTable {
        tag_preferences,
        stage_preferences,
        tier_preferences,
        default: candidates.first().and_then(|(name, patch)| {
            let provider = patch.provider.clone()?;
            Some(RouteTargetPreference {
                selection_key: (*name).clone(),
                provider,
                model: patch
                    .model
                    .as_ref()
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_default(),
                effort: patch
                    .params
                    .as_ref()
                    .and_then(|params| params.effort)
                    .map(|effort| effort.as_str().to_string())
                    .unwrap_or_default(),
                tier: patch.tier.clone().unwrap_or_default(),
            })
        }),
    };

    // Frontier eligibility is enforced at profile identity above. Do not feed
    // provider/model-wide frontier lists to the generic router: one provider or
    // model tuple can back both a safe and a frontier profile, so global marking
    // would incorrectly taint the safe profile.
    let requested_stage = params.stage.clone();
    let signals = ComplexitySignals {
        explicit_tier: params.explicit_tier,
        stage: requested_stage.clone().unwrap_or_default(),
        task_tags: params.extra_tags,
        changed_files: params.changed_files.unwrap_or(0),
        dependency_edges: params.dependency_edges.unwrap_or(0),
        retry_count: params.retry_count.unwrap_or(0),
        allow_frontier: params.allow_frontier,
        frontier_providers: Vec::new(),
        frontier_models: Vec::new(),
    };

    // Call the router.
    let default_provider = candidates
        .first()
        .and_then(|(_, patch)| patch.provider.as_deref())
        .unwrap_or("default");

    let mut decision = vyane_router::route_task(
        &params.task,
        &available_providers,
        Some(&signals),
        Some(&table),
        default_provider,
    );

    // The service must never reverse-map provider/model back to a profile: that
    // tuple is not unique across harnesses, protocols, failover chains, or
    // effort policies. The router carries the exact opaque key selected from
    // the preference table instead.
    let profile = candidates
        .iter()
        .find(|(name, _)| name.as_str() == decision.selection_key)
        .map(|(name, _)| (*name).clone())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "route decision did not carry a valid candidate profile key `{}`",
                decision.selection_key
            )
        })?;

    let selected_patch = config
        .profiles
        .get(&profile)
        .ok_or_else(|| anyhow::anyhow!("selected profile `{profile}` disappeared from config"))?;
    if !selected_patch
        .provider
        .as_deref()
        .is_some_and(|provider| provider.eq_ignore_ascii_case(&decision.provider))
    {
        bail!(
            "route decision profile `{profile}` does not belong to provider `{}`",
            decision.provider
        );
    }

    // A blocked preference can make the core router return a provider-only
    // fallback. Once the adapter chooses the concrete profile, make the model
    // and effective effort truthful to what dispatch will actually execute.
    if decision.model.is_empty() {
        if let Some(model) = selected_patch.model.as_ref() {
            decision.model = model.as_str().to_string();
        }
    }
    decision.effort = explicit_effort
        .or_else(|| {
            selected_patch
                .params
                .as_ref()
                .and_then(|params| params.effort)
        })
        .map(route_effort_from_core)
        .unwrap_or_else(|| effort_for_tier(decision.tier));

    Ok(RouteResult { decision, profile })
}

fn validate_route_param_values(config: &ResolvedConfig, params: &RouteParams) -> Result<()> {
    if let Some(tier) = params.explicit_tier.as_deref() {
        if !matches!(
            tier.trim().to_ascii_lowercase().as_str(),
            "economy" | "mainline" | "frontier"
        ) {
            bail!("unknown routing tier `{tier}` (expected economy, mainline, or frontier)");
        }
    }
    let unknown_candidates = params
        .candidate_profiles
        .iter()
        .filter(|name| !config.profiles.contains_key(*name))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown_candidates.is_empty() {
        bail!(
            "unknown routing candidate profiles: {}",
            unknown_candidates.join(", ")
        );
    }
    Ok(())
}

/// Validate every profile that a deferred `auto` selector could choose,
/// independent of the eventual rendered prompt. This catches missing env,
/// malformed failover, and frontier-policy failures before a workflow journal
/// is created instead of validating one arbitrary dummy-prompt decision.
pub fn validate_auto_route_candidates(
    loaded: &LoadedConfig,
    labels: &BTreeMap<String, String>,
) -> Result<()> {
    let params = route_params_from_labels(String::new(), labels)?;
    validate_route_param_values(&loaded.config, &params)?;
    let mut names = if params.candidate_profiles.is_empty() {
        loaded.config.profiles.keys().cloned().collect::<Vec<_>>()
    } else {
        params.candidate_profiles.clone()
    };
    if !params.allow_frontier {
        names.retain(|name| {
            loaded
                .config
                .profiles
                .get(name)
                .is_some_and(|patch| !profile_is_frontier(patch))
        });
    }
    if names.is_empty() {
        bail!("no eligible profiles remain for deferred auto routing");
    }
    for name in names {
        let patch = loaded
            .config
            .profiles
            .get(&name)
            .ok_or_else(|| anyhow::anyhow!("unknown routing candidate profile `{name}`"))?;
        if patch.provider.is_none() {
            bail!("routing candidate profile `{name}` does not name a provider");
        }
        let chain = resolve_target_chain(loaded, &name)
            .map_err(|error| anyhow::anyhow!("routing candidate `{name}`: {error:#}"))?;
        guard_route_chain(loaded, &name, chain, params.allow_frontier)
            .map_err(|error| anyhow::anyhow!("routing candidate `{name}`: {error:#}"))?;
    }
    Ok(())
}

/// Reconstruct a parent-frozen auto route without making a new route decision.
/// The worker replays the same frontier guard and effort transformation before
/// comparing its secret-free target snapshot, so policy filtering is not
/// mistaken for config drift.
pub fn replay_recorded_auto_chain(
    loaded: &LoadedConfig,
    profile: &str,
    labels: &BTreeMap<String, String>,
) -> Result<Vec<BoundTarget>> {
    if labels.get("routing.mode").map(String::as_str) != Some("auto") {
        bail!("recorded auto route is missing routing.mode=auto");
    }
    let params = route_params_from_labels(String::new(), labels)?;
    let chain = resolve_target_chain(loaded, profile)?;
    let mut chain = guard_route_chain(loaded, profile, chain, params.allow_frontier)?;
    let effort = params
        .explicit_effort
        .ok_or_else(|| anyhow::anyhow!("recorded auto route is missing routing.effort"))?;
    for bound in &mut chain {
        bound.params.effort = Some(effort);
    }
    Ok(chain)
}

/// Enforce the frontier guard across the selected profile's resolved failover
/// chain while preserving each leg's provenance.
///
/// Policy when frontier is disabled:
///
/// - the primary and profile-name failover legs are classified by that exact
///   profile's `tier`; frontier profile legs are removed;
/// - a raw `provider/model` failover leg has no profile identity, so it is kept
///   only when every configured profile resolving to that tuple is explicitly
///   `economy` or `mainline`; unknown, un-tiered, or ambiguously frontier tuples
///   fail closed with a configuration error.
///
/// This deliberately avoids provider-wide classification: the same provider
/// (and even the same provider/model tuple) may back profiles with different
/// harnesses, policies, and tiers.
fn guard_route_chain(
    loaded: &LoadedConfig,
    profile: &str,
    chain: Vec<BoundTarget>,
    allow_frontier: bool,
) -> Result<Vec<BoundTarget>> {
    if allow_frontier {
        return Ok(chain);
    }

    let patch = loaded
        .config
        .profiles
        .get(profile)
        .ok_or_else(|| anyhow::anyhow!("selected profile `{profile}` is not configured"))?;
    if profile_is_frontier(patch) {
        bail!("frontier routing is disabled but selected profile `{profile}` is frontier");
    }

    let failover = patch.failover.as_deref().unwrap_or_default();
    if chain.len() != failover.len() + 1 {
        bail!(
            "resolved profile `{profile}` produced {} targets for {} declared failover legs",
            chain.len(),
            failover.len()
        );
    }

    let mut resolved = chain.into_iter();
    let primary = resolved.next().ok_or_else(|| {
        anyhow::anyhow!("selected profile `{profile}` resolved to an empty chain")
    })?;
    let mut guarded = Vec::with_capacity(failover.len() + 1);
    guarded.push(primary);

    for (element, bound) in failover.iter().zip(resolved) {
        match element {
            RawFailoverElement::ProfileName(name) => {
                let failover_patch = loaded.config.profiles.get(name).ok_or_else(|| {
                    anyhow::anyhow!("profile `{profile}` names unknown failover profile `{name}`")
                })?;
                if !profile_is_frontier(failover_patch) {
                    guarded.push(bound);
                }
            }
            RawFailoverElement::ProviderModel { provider, model } => {
                match classify_direct_failover(&loaded.config, provider, model.as_str()) {
                    DirectFailoverSafety::NonFrontier => guarded.push(bound),
                    DirectFailoverSafety::Frontier => bail!(
                        "frontier routing is disabled and direct failover `{provider}/{model}` is classified frontier; name a non-frontier profile instead"
                    ),
                    DirectFailoverSafety::Unclassified => bail!(
                        "frontier routing is disabled and direct failover `{provider}/{model}` cannot be proven non-frontier; name an explicitly tiered profile instead"
                    ),
                }
            }
        }
    }

    Ok(guarded)
}

fn profile_is_frontier(patch: &vyane_config::ProfilePatch) -> bool {
    patch
        .tier
        .as_deref()
        .is_some_and(|tier| tier.eq_ignore_ascii_case("frontier"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectFailoverSafety {
    NonFrontier,
    Frontier,
    Unclassified,
}

fn classify_direct_failover(
    config: &ResolvedConfig,
    provider: &str,
    model: &str,
) -> DirectFailoverSafety {
    let default_model = config
        .providers
        .get(provider)
        .ok()
        .and_then(|provider| provider.default_model.as_ref());
    let mut saw_safe = false;
    let mut saw_frontier = false;
    let mut saw_unclassified = false;

    for patch in config.profiles.values() {
        if !patch
            .provider
            .as_deref()
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(provider))
        {
            continue;
        }
        let candidate_model = patch.model.as_ref().or(default_model);
        if candidate_model.is_none_or(|candidate| candidate.as_str() != model) {
            continue;
        }

        match patch.tier.as_deref() {
            Some(tier) if tier.eq_ignore_ascii_case("economy") => saw_safe = true,
            Some(tier) if tier.eq_ignore_ascii_case("mainline") => saw_safe = true,
            Some(tier) if tier.eq_ignore_ascii_case("frontier") => saw_frontier = true,
            _ => saw_unclassified = true,
        }
    }

    if saw_frontier {
        DirectFailoverSafety::Frontier
    } else if saw_safe && !saw_unclassified {
        DirectFailoverSafety::NonFrontier
    } else {
        DirectFailoverSafety::Unclassified
    }
}

pub fn apply_route_effort(chain: &mut [BoundTarget], decision: &RouteDecision) {
    let effort = match decision.effort {
        RouteEffort::Low => Effort::Low,
        RouteEffort::Medium => Effort::Medium,
        RouteEffort::High => Effort::High,
        RouteEffort::Xhigh => Effort::Xhigh,
    };
    for bound in chain {
        bound.params.effort = Some(effort);
    }
}

fn route_effort_from_core(effort: Effort) -> RouteEffort {
    match effort {
        Effort::Low => RouteEffort::Low,
        Effort::Medium => RouteEffort::Medium,
        Effort::High => RouteEffort::High,
        Effort::Xhigh => RouteEffort::Xhigh,
    }
}

fn record_route_labels(task: &mut TaskSpec, route: &RouteResult, chain: &[BoundTarget]) {
    let decision = &route.decision;
    task.labels.insert("routing.mode".into(), "auto".into());
    task.labels
        .insert("routing.profile".into(), route.profile.clone());
    task.labels
        .insert("routing.provider".into(), decision.provider.clone());
    let selected_model = chain
        .first()
        .map(|bound| bound.target.model.as_str())
        .unwrap_or(decision.model.as_str());
    task.labels
        .insert("routing.model".into(), selected_model.to_string());
    task.labels
        .insert("routing.tier".into(), decision.tier.as_str().into());
    task.labels
        .insert("routing.effort".into(), decision.effort.as_str().into());
    task.labels.insert(
        "routing.score".into(),
        format!("{:.3}", decision.complexity_score),
    );
    task.labels
        .insert("routing.intent".into(), decision.intent.clone());
    if !decision.tag.is_empty() {
        task.labels
            .insert("routing.tag".into(), decision.tag.clone());
    }
}

/// Build routing params from a task string and optional labels.
/// Labels with keys `stage`, `tier`, or `tags` are extracted as routing signals.
/// Explicit effort is accepted only through the canonical internal
/// `routing.effort` key; there is intentionally no generic user-label alias.
///
/// Intended for CLI/API integration where routing signals arrive as labels.
pub fn route_params_from_labels(
    task: String,
    labels: &BTreeMap<String, String>,
) -> Result<RouteParams> {
    let mut params = RouteParams {
        task,
        ..Default::default()
    };
    if let Some(stage) = labels.get("routing.stage").or_else(|| labels.get("stage")) {
        params.stage = Some(stage.clone());
    }
    if let Some(tier) = labels.get("routing.tier").or_else(|| labels.get("tier")) {
        params.explicit_tier = Some(tier.clone());
    }
    if let Some(effort) = labels.get("routing.effort") {
        params.explicit_effort = Some(match effort.as_str() {
            "low" => Effort::Low,
            "medium" => Effort::Medium,
            "high" => Effort::High,
            "xhigh" => Effort::Xhigh,
            _ => bail!("routing effort must be low, medium, high, or xhigh"),
        });
    }
    if let Some(tags) = labels.get("routing.tags").or_else(|| labels.get("tags")) {
        params.extra_tags = tags
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    if let Some(raw) = labels
        .get("routing.allow_frontier")
        .or_else(|| labels.get("allow_frontier"))
    {
        params.allow_frontier = match raw.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => true,
            "false" | "0" | "no" => false,
            _ => bail!("routing allow_frontier label must be true or false, got `{raw}`"),
        };
    }
    if let Some(candidates) = labels
        .get("routing.candidates")
        .or_else(|| labels.get("candidates"))
    {
        params.candidate_profiles = candidates
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .collect();
    }
    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vyane_config::{GenParamsPatch, ProfilePatch};
    use vyane_core::{AdapterTransport, GenParams, ModelId, Protocol, ProviderId, Target};

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

    fn test_bound(provider: &str, model: &str) -> BoundTarget {
        BoundTarget {
            target: Target {
                provider: ProviderId::new(provider),
                protocol: Protocol::OpenaiResponses,
                harness: None,
                model: ModelId::new(model),
            },
            transport: AdapterTransport::DirectHttp,
            endpoint: None,
            params: GenParams::default(),
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
        assert_eq!(result.decision.provider, "openai");
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

    #[test]
    fn stage_routes_to_matching_profile() {
        // A profile with stage="review" should be selected when the task's
        // stage signal is "review", even if it's not the economy default.
        let mut profiles = BTreeMap::new();
        profiles.insert(
            "default-model".into(),
            ProfilePatch {
                provider: Some("openai".into()),
                protocol: Some(vyane_core::Protocol::OpenaiChat),
                harness: Some("none".into()),
                model: Some(ModelId::new("gpt-4o-mini")),
                tier: Some("economy".into()),
                ..Default::default()
            },
        );
        profiles.insert(
            "review-model".into(),
            ProfilePatch {
                provider: Some("anthropic".into()),
                protocol: Some(vyane_core::Protocol::AnthropicMessages),
                harness: Some("none".into()),
                model: Some(ModelId::new("claude-sonnet")),
                tier: Some("mainline".into()),
                stage: Some("review".into()),
                ..Default::default()
            },
        );
        let config = ResolvedConfig {
            providers: Default::default(),
            profiles,
        };
        let params = RouteParams {
            task: "check the code".into(),
            stage: Some("review".into()),
            ..Default::default()
        };
        let result = route_task(&config, params).unwrap();
        // Stage has highest precedence — review-model should win over default.
        assert_eq!(result.profile, "review-model");
    }

    #[test]
    fn frontier_guard_allows_frontier_by_default() {
        // When allow_frontier=false, a frontier-tier preference should be blocked
        // and fall back. This tests that frontier_providers/models are populated
        // from tier="frontier" profiles (MINOR #7 fix).
        let config = make_config();
        let params = RouteParams {
            task: "write an ADR for system architecture".into(),
            explicit_tier: Some("frontier".into()),
            ..Default::default()
        };
        let result = route_task(&config, params).unwrap();
        // With allow_frontier=true (default), frontier-model should be selected.
        assert_eq!(result.profile, "frontier-model");
        // Verify the decision reached frontier tier.
        assert_eq!(result.decision.tier, vyane_router::RouteTier::Frontier);
    }

    #[test]
    fn frontier_guard_blocks_frontier_and_falls_back_to_non_frontier() {
        let config = make_config();
        let params = RouteParams {
            task: "write an ADR for system architecture".into(),
            explicit_tier: Some("frontier".into()),
            allow_frontier: false,
            ..Default::default()
        };
        let result = route_task(&config, params).unwrap();
        assert_eq!(result.profile, "economy-model");
        assert_eq!(result.decision.provider, "openai");
        assert_ne!(result.decision.tier, vyane_router::RouteTier::Frontier);
    }

    #[test]
    fn frontier_guard_rejects_frontier_only_candidates() {
        let config = make_config();
        let error = route_task(
            &config,
            RouteParams {
                task: "write an ADR for system architecture".into(),
                candidate_profiles: vec!["frontier-model".into()],
                allow_frontier: false,
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("no non-frontier candidate"));
    }

    #[test]
    fn exact_profile_key_disambiguates_same_provider_and_model() {
        let mut profiles = BTreeMap::new();
        profiles.insert(
            "economy-direct".into(),
            ProfilePatch {
                provider: Some("shared".into()),
                protocol: Some(Protocol::OpenaiResponses),
                harness: Some("none".into()),
                model: Some(ModelId::new("same-model")),
                tier: Some("economy".into()),
                params: Some(GenParamsPatch {
                    effort: Some(Effort::Low),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        profiles.insert(
            "frontier-cli".into(),
            ProfilePatch {
                provider: Some("shared".into()),
                protocol: Some(Protocol::OpenaiResponses),
                harness: Some("codex-cli".into()),
                model: Some(ModelId::new("same-model")),
                tier: Some("frontier".into()),
                params: Some(GenParamsPatch {
                    effort: Some(Effort::Xhigh),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        let config = ResolvedConfig {
            providers: Default::default(),
            profiles,
        };

        let result = route_task(
            &config,
            RouteParams {
                task: "architecture".into(),
                explicit_tier: Some("frontier".into()),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(result.profile, "frontier-cli");
        assert_eq!(result.decision.selection_key, "frontier-cli");
        assert_eq!(result.decision.provider, "shared");
        assert_eq!(result.decision.effort, RouteEffort::Xhigh);
    }

    #[test]
    fn no_frontier_keeps_safe_profile_on_shared_provider() {
        let mut profiles = BTreeMap::new();
        profiles.insert(
            "reviewer".into(),
            ProfilePatch {
                provider: Some("shared".into()),
                model: Some(ModelId::new("same-model")),
                tier: Some("mainline".into()),
                stage: Some("review".into()),
                ..Default::default()
            },
        );
        profiles.insert(
            "frontier".into(),
            ProfilePatch {
                provider: Some("shared".into()),
                model: Some(ModelId::new("same-model")),
                tier: Some("frontier".into()),
                ..Default::default()
            },
        );
        let config = ResolvedConfig {
            providers: Default::default(),
            profiles,
        };

        let result = route_task(
            &config,
            RouteParams {
                task: "review the patch".into(),
                stage: Some("review".into()),
                allow_frontier: false,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(result.profile, "reviewer");
        assert_eq!(result.decision.selection_key, "reviewer");
        assert_eq!(result.decision.provider, "shared");
    }

    #[test]
    fn no_frontier_filters_profile_named_frontier_failover_leg() {
        let mut profiles = BTreeMap::new();
        profiles.insert(
            "safe".into(),
            ProfilePatch {
                provider: Some("shared".into()),
                model: Some(ModelId::new("safe-model")),
                tier: Some("economy".into()),
                failover: Some(vec![RawFailoverElement::ProfileName("frontier".into())]),
                ..Default::default()
            },
        );
        profiles.insert(
            "frontier".into(),
            ProfilePatch {
                provider: Some("shared".into()),
                model: Some(ModelId::new("frontier-model")),
                tier: Some("frontier".into()),
                ..Default::default()
            },
        );
        let loaded = LoadedConfig {
            config: ResolvedConfig {
                providers: Default::default(),
                profiles,
            },
            files: Vec::new(),
            secrets: BTreeMap::new(),
        };
        let chain = vec![
            test_bound("shared", "safe-model"),
            test_bound("shared", "frontier-model"),
        ];

        let guarded = guard_route_chain(&loaded, "safe", chain, false).unwrap();
        assert_eq!(guarded.len(), 1);
        assert_eq!(guarded[0].target.model.as_str(), "safe-model");
    }

    #[test]
    fn no_frontier_fails_closed_for_unclassified_direct_failover() {
        let mut profiles = BTreeMap::new();
        profiles.insert(
            "safe".into(),
            ProfilePatch {
                provider: Some("shared".into()),
                model: Some(ModelId::new("safe-model")),
                tier: Some("economy".into()),
                failover: Some(vec![RawFailoverElement::ProviderModel {
                    provider: "unknown".into(),
                    model: ModelId::new("unclassified-model"),
                }]),
                ..Default::default()
            },
        );
        let loaded = LoadedConfig {
            config: ResolvedConfig {
                providers: Default::default(),
                profiles,
            },
            files: Vec::new(),
            secrets: BTreeMap::new(),
        };
        let chain = vec![
            test_bound("shared", "safe-model"),
            test_bound("unknown", "unclassified-model"),
        ];

        let error = guard_route_chain(&loaded, "safe", chain, false).unwrap_err();
        assert!(error.to_string().contains("cannot be proven non-frontier"));
    }

    #[test]
    fn no_frontier_keeps_direct_failover_proven_safe_by_profile_metadata() {
        let mut profiles = BTreeMap::new();
        profiles.insert(
            "safe".into(),
            ProfilePatch {
                provider: Some("primary".into()),
                model: Some(ModelId::new("safe-model")),
                tier: Some("economy".into()),
                failover: Some(vec![RawFailoverElement::ProviderModel {
                    provider: "backup".into(),
                    model: ModelId::new("backup-model"),
                }]),
                ..Default::default()
            },
        );
        profiles.insert(
            "classified-backup".into(),
            ProfilePatch {
                provider: Some("backup".into()),
                model: Some(ModelId::new("backup-model")),
                tier: Some("mainline".into()),
                ..Default::default()
            },
        );
        let loaded = LoadedConfig {
            config: ResolvedConfig {
                providers: Default::default(),
                profiles,
            },
            files: Vec::new(),
            secrets: BTreeMap::new(),
        };
        let chain = vec![
            test_bound("primary", "safe-model"),
            test_bound("backup", "backup-model"),
        ];

        let guarded = guard_route_chain(&loaded, "safe", chain, false).unwrap();
        assert_eq!(guarded.len(), 2);
        assert_eq!(guarded[1].target.model.as_str(), "backup-model");
    }

    #[test]
    fn configured_profile_effort_flows_into_decision() {
        let mut config = make_config();
        config.profiles.get_mut("economy-model").unwrap().params = Some(GenParamsPatch {
            effort: Some(Effort::Xhigh),
            ..Default::default()
        });

        let result = route_task(
            &config,
            RouteParams {
                task: "hello".into(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_ne!(result.decision.reason, "default fallback");
        assert_eq!(result.decision.effort, RouteEffort::Xhigh);
    }

    #[test]
    fn tier_default_applies_when_selected_profile_has_no_effort() {
        let result = route_task(
            &make_config(),
            RouteParams {
                task: "hello".into(),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(result.decision.tier, vyane_router::RouteTier::Economy);
        assert_eq!(result.decision.effort, RouteEffort::Low);
    }

    #[test]
    fn explicit_effort_overrides_selected_profile_and_tier_default() {
        let mut config = make_config();
        config.profiles.get_mut("economy-model").unwrap().params = Some(GenParamsPatch {
            effort: Some(Effort::Medium),
            ..Default::default()
        });

        let result = route_task(
            &config,
            RouteParams {
                task: "hello".into(),
                explicit_effort: Some(Effort::Xhigh),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(result.decision.tier, vyane_router::RouteTier::Economy);
        assert_eq!(result.decision.effort, RouteEffort::Xhigh);
    }

    #[test]
    fn recorded_auto_replay_uses_frozen_effective_effort_for_every_leg() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("config.toml");
        std::fs::write(
            &config_path,
            r#"
[providers.primary]
base_url = "https://primary.example.invalid"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "primary-model"

[providers.backup]
base_url = "https://backup.example.invalid"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "backup-model"

[profiles.mainline]
provider = "primary"
protocol = "openai_chat"
harness = "none"
model = "primary-model"
tier = "mainline"
failover = ["backup"]

[profiles.mainline.params]
effort = "low"

[profiles.backup]
provider = "backup"
protocol = "openai_chat"
harness = "none"
model = "backup-model"
tier = "economy"

[profiles.backup.params]
effort = "xhigh"
"#,
        )
        .unwrap();
        let loaded = crate::config::load_config(Some(&config_path)).unwrap();
        let mut task = TaskSpec::new("run");
        task.labels.insert("routing.tier".into(), "mainline".into());
        task.labels.insert("routing.effort".into(), "high".into());

        let plan = plan_dispatch(&loaded, "auto", &mut task).unwrap();
        assert_eq!(plan.selector, "mainline");
        assert!(
            plan.chain
                .iter()
                .all(|target| target.params.effort == Some(Effort::High))
        );
        assert_eq!(
            task.labels.get("routing.effort").map(String::as_str),
            Some("high")
        );

        let replayed = replay_recorded_auto_chain(&loaded, &plan.selector, &task.labels).unwrap();
        assert_eq!(replayed.len(), 2);
        assert!(
            replayed
                .iter()
                .all(|target| target.params.effort == Some(Effort::High))
        );
    }

    #[test]
    fn route_effort_overrides_every_failover_leg() {
        let mut chain = [Effort::Low, Effort::Medium].map(|effort| BoundTarget {
            target: Target {
                provider: ProviderId::new("test"),
                protocol: Protocol::OpenaiChat,
                harness: None,
                model: ModelId::new("model"),
            },
            transport: AdapterTransport::DirectHttp,
            endpoint: None,
            params: GenParams {
                effort: Some(effort),
                ..Default::default()
            },
        });
        let decision = route_task(
            &make_config(),
            RouteParams {
                task: "hello".into(),
                ..Default::default()
            },
        )
        .unwrap()
        .decision;

        apply_route_effort(&mut chain, &decision);
        assert!(
            chain
                .iter()
                .all(|bound| bound.params.effort == Some(Effort::Low))
        );
    }

    #[test]
    fn routing_labels_parse_frontier_guard_and_candidates() {
        let labels = BTreeMap::from([
            ("routing.stage".into(), "review".into()),
            ("routing.tags".into(), "security, test".into()),
            ("routing.allow_frontier".into(), "false".into()),
            ("routing.candidates".into(), "a, b".into()),
            ("routing.effort".into(), "high".into()),
        ]);
        let params = route_params_from_labels("task".into(), &labels).unwrap();
        assert_eq!(params.stage.as_deref(), Some("review"));
        assert_eq!(params.extra_tags, vec!["security", "test"]);
        assert!(!params.allow_frontier);
        assert_eq!(params.candidate_profiles, vec!["a", "b"]);
        assert_eq!(params.explicit_effort, Some(Effort::High));
    }

    #[test]
    fn canonical_frontier_guard_label_wins_over_generic_alias() {
        let labels = BTreeMap::from([
            ("allow_frontier".into(), "true".into()),
            ("routing.allow_frontier".into(), "false".into()),
        ]);
        let params = route_params_from_labels("task".into(), &labels).unwrap();
        assert!(!params.allow_frontier);
    }

    #[test]
    fn canonical_route_hints_win_over_all_generic_aliases() {
        let labels = BTreeMap::from([
            ("stage".into(), "generic-stage".into()),
            ("routing.stage".into(), "canonical-stage".into()),
            ("tier".into(), "economy".into()),
            ("routing.tier".into(), "frontier".into()),
            ("tags".into(), "generic".into()),
            ("routing.tags".into(), "canonical,security".into()),
            ("candidates".into(), "generic".into()),
            ("routing.candidates".into(), "canonical".into()),
        ]);
        let params = route_params_from_labels("task".into(), &labels).unwrap();
        assert_eq!(params.stage.as_deref(), Some("canonical-stage"));
        assert_eq!(params.explicit_tier.as_deref(), Some("frontier"));
        assert_eq!(params.extra_tags, vec!["canonical", "security"]);
        assert_eq!(params.candidate_profiles, vec!["canonical"]);
    }

    #[test]
    fn invalid_frontier_guard_label_is_rejected() {
        let labels = BTreeMap::from([("routing.allow_frontier".into(), "sometimes".into())]);
        assert!(route_params_from_labels("task".into(), &labels).is_err());
    }

    #[test]
    fn invalid_routing_effort_is_rejected_without_echoing_input() {
        let canary = "EFFORT_VALUE_MUST_NOT_BE_ECHOED";
        let labels = BTreeMap::from([("routing.effort".into(), canary.into())]);

        let error = route_params_from_labels("task".into(), &labels)
            .unwrap_err()
            .to_string();

        assert!(!error.contains(canary));
        assert!(error.contains("low, medium, high, or xhigh"));
    }

    #[test]
    fn generic_effort_label_cannot_set_the_reserved_override() {
        let labels = BTreeMap::from([("effort".into(), "xhigh".into())]);

        let params = route_params_from_labels("task".into(), &labels).unwrap();

        assert_eq!(params.explicit_effort, None);
    }

    #[test]
    fn candidate_profiles_filters_to_subset() {
        let config = make_config();
        // Only allow the frontier-model — even a simple task should route there.
        let params = RouteParams {
            task: "hello world".into(),
            candidate_profiles: vec!["frontier-model".into()],
            ..Default::default()
        };
        let result = route_task(&config, params).unwrap();
        assert_eq!(result.profile, "frontier-model");
    }

    #[test]
    fn candidate_profiles_empty_includes_all() {
        let config = make_config();
        let params = RouteParams {
            task: "hello world".into(),
            candidate_profiles: vec![],
            ..Default::default()
        };
        let result = route_task(&config, params).unwrap();
        // With all candidates, economy-model wins for a zero-score task.
        assert_eq!(result.profile, "economy-model");
    }

    #[test]
    fn candidate_profiles_unknown_name_errors() {
        let config = make_config();
        let params = RouteParams {
            task: "test".into(),
            candidate_profiles: vec!["nonexistent".into()],
            ..Default::default()
        };
        assert!(route_task(&config, params).is_err());
    }

    #[test]
    fn one_unknown_candidate_makes_the_whole_constraint_invalid() {
        let config = make_config();
        let params = RouteParams {
            task: "test".into(),
            candidate_profiles: vec!["economy-model".into(), "typo".into()],
            ..Default::default()
        };
        assert!(route_task(&config, params).is_err());
    }

    #[test]
    fn invalid_explicit_tier_is_rejected() {
        let config = make_config();
        let params = RouteParams {
            task: "test".into(),
            explicit_tier: Some("ultra".into()),
            ..Default::default()
        };
        assert!(route_task(&config, params).is_err());
    }

    #[test]
    fn route_decision_carries_complexity_score() {
        let config = make_config();
        let params = RouteParams {
            task: "hello".into(),
            ..Default::default()
        };
        let result = route_task(&config, params).unwrap();
        // A trivial task should have a low complexity score.
        assert!(
            result.decision.complexity_score <= 0.15,
            "trivial task score should be ≤ 0.15, got {}",
            result.decision.complexity_score
        );
    }

    #[test]
    fn route_decision_carries_intent() {
        let config = make_config();
        let params = RouteParams {
            task: "fix the crash bug".into(),
            ..Default::default()
        };
        let result = route_task(&config, params).unwrap();
        // "fix...crash...bug" should classify as debug intent.
        assert!(!result.decision.intent.is_empty());
    }
}
