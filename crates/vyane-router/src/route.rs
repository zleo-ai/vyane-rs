//! The main routing entry point: [`route_task`].
//!
//! Given a task text, the set of available provider ids, and optional signals
//! + preferences, produce a [`RouteDecision`] naming the target to dispatch to.

use crate::decision::RouteDecision;
use crate::intent::classify_intent;
use crate::preference::{RoutePreferenceTable, parse_effort};
use crate::score::{ComplexitySignals, complexity_score, effort_for_tier, tier_for_score};
use crate::tags::infer_route_tags_with_intent;

/// Route a task to a target.
///
/// - `task` — the task/prompt text.
/// - `available_providers` — provider ids that are configured and available.
///   The router only considers these when falling back (a preference pointing
///   to an unavailable provider is skipped).
/// - `signals` — optional structural signals (stage, changed_files, retry_count,
///   explicit_tier, etc.). Defaults to a permissive zero-signal set.
/// - `preferences` — optional preference table (tag/stage/tier → target). When
///   `None`, the router falls back to the first available provider.
/// - `default_provider` — the fallback provider when no preference matches and
///   no preference table is configured.
pub fn route_task(
    task: &str,
    available_providers: &[String],
    signals: Option<&ComplexitySignals>,
    preferences: Option<&RoutePreferenceTable>,
    default_provider: &str,
) -> RouteDecision {
    let signals = signals.cloned().unwrap_or_else(ComplexitySignals::new);
    let intent = classify_intent(task);

    // Merge explicit + inferred tags (explicit first, deduplicated).
    let mut tags = signals.task_tags.clone();
    for inferred in infer_route_tags_with_intent(task, &intent) {
        if !tags.iter().any(|t| t == &inferred) {
            tags.push(inferred);
        }
    }

    let score = complexity_score(task, &signals, &tags);
    let tier = tier_for_score(score, &signals);
    let mut effort = effort_for_tier(tier);

    // Resolve a preference (stage → tag → tier → default).
    if let Some(table) = preferences {
        if let Some(pref) = table.resolve(&signals.stage, &tags, tier) {
            // Check frontier guard: if the preference is frontier-tier but
            // frontier is disallowed, skip it and fall through.
            let is_frontier_pref = pref.tier.trim().eq_ignore_ascii_case("frontier")
                || signals
                    .frontier_providers
                    .iter()
                    .any(|p| p.eq_ignore_ascii_case(&pref.provider))
                || signals
                    .frontier_models
                    .iter()
                    .any(|m| m.eq_ignore_ascii_case(&pref.model));

            let available = available_providers
                .iter()
                .any(|p| p.eq_ignore_ascii_case(&pref.provider));

            if (!is_frontier_pref || signals.allow_frontier) && available {
                if !pref.effort.is_empty() {
                    effort = parse_effort(&pref.effort);
                }
                return RouteDecision {
                    provider: pref.provider.clone(),
                    model: pref.model.clone(),
                    effort,
                    tier,
                    tag: first_matching_tag(&tags, table),
                    intent: intent.primary.as_str().to_string(),
                    complexity_score: score,
                    reason: format!("preference matched (tag/stage/tier): {}", pref.provider),
                };
            }
        }
    }

    // Fallback: use the default provider, or the first available one.
    let provider = available_providers
        .first()
        .map(|s| s.as_str())
        .unwrap_or(default_provider);

    RouteDecision {
        provider: provider.to_string(),
        model: String::new(),
        effort,
        tier,
        tag: String::new(),
        intent: intent.primary.as_str().to_string(),
        complexity_score: score,
        reason: "default fallback".to_string(),
    }
}

/// Find the first tag that has a preference entry, for diagnostic tagging.
fn first_matching_tag(tags: &[String], table: &RoutePreferenceTable) -> String {
    for tag in tags {
        let key = tag.trim().to_ascii_lowercase();
        if table.tag_preferences.contains_key(&key) {
            return tag.clone();
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decision::{RouteEffort, RouteTier};
    use crate::preference::RouteTargetPreference;
    use std::collections::BTreeMap;

    #[test]
    fn default_fallback() {
        let decision = route_task("hello world", &["openai".into()], None, None, "openai");
        assert_eq!(decision.provider, "openai");
        // A zero-complexity task ("hello world", no signals) scores 0.0,
        // which maps to the economy tier — this is by design: a trivial task
        // doesn't need a frontier model.
        assert_eq!(decision.tier, RouteTier::Economy);
        assert_eq!(decision.reason, "default fallback");
    }

    #[test]
    fn preference_overrides_default() {
        let mut tag_prefs = BTreeMap::new();
        tag_prefs.insert(
            "frontend".into(),
            RouteTargetPreference {
                provider: "anthropic".into(),
                model: "claude-sonnet".into(),
                effort: "high".into(),
                tier: String::new(),
            },
        );
        let table = RoutePreferenceTable {
            tag_preferences: tag_prefs,
            ..Default::default()
        };
        let providers = vec!["anthropic".into(), "openai".into()];
        let decision = route_task(
            "build a react frontend component",
            &providers,
            None,
            Some(&table),
            "openai",
        );
        assert_eq!(decision.provider, "anthropic");
        assert_eq!(decision.model, "claude-sonnet");
        assert_eq!(decision.effort, RouteEffort::High);
        assert_eq!(decision.tag, "frontend");
    }

    #[test]
    fn unavailable_preference_falls_back() {
        let mut tag_prefs = BTreeMap::new();
        tag_prefs.insert(
            "code".into(),
            RouteTargetPreference {
                provider: "unavailable".into(),
                ..Default::default()
            },
        );
        let table = RoutePreferenceTable {
            tag_preferences: tag_prefs,
            ..Default::default()
        };
        let providers = vec!["openai".into()];
        let decision = route_task(
            "implement the function",
            &providers,
            None,
            Some(&table),
            "openai",
        );
        // "code" preference points to "unavailable" which isn't in providers,
        // so we fall back to the first available.
        assert_eq!(decision.provider, "openai");
    }

    #[test]
    fn frontier_blocked_when_disallowed() {
        let mut tag_prefs = BTreeMap::new();
        tag_prefs.insert(
            "architecture".into(),
            RouteTargetPreference {
                provider: "frontier-provider".into(),
                tier: "frontier".into(),
                ..Default::default()
            },
        );
        let table = RoutePreferenceTable {
            tag_preferences: tag_prefs,
            ..Default::default()
        };
        let providers = vec!["openai".into(), "frontier-provider".into()];
        let signals = ComplexitySignals {
            allow_frontier: false,
            ..ComplexitySignals::new()
        };
        let decision = route_task(
            "write an ADR for system design",
            &providers,
            Some(&signals),
            Some(&table),
            "openai",
        );
        // Frontier preference is blocked by the guard (allow_frontier=false).
        // The router falls back to the first available provider. Note: the
        // current implementation does NOT filter frontier providers from the
        // fallback candidates (that's the Python `_guarded_fallback_candidates`
        // function, deferred). So the first available wins regardless of tier.
        assert_eq!(decision.provider, "openai");
        assert_eq!(decision.reason, "default fallback");
    }

    #[test]
    fn complexity_drives_tier() {
        let signals = ComplexitySignals {
            stage: "architecture".into(),
            changed_files: 20,
            dependency_edges: 10,
            ..ComplexitySignals::new()
        };
        let decision = route_task(
            "design the system",
            &["openai".into()],
            Some(&signals),
            None,
            "openai",
        );
        assert_eq!(decision.tier, RouteTier::Frontier);
        assert_eq!(decision.effort, RouteEffort::High);
        assert!(decision.complexity_score >= 0.70);
    }

    #[test]
    fn frontier_guard_via_frontier_providers_list() {
        // A preference whose provider is in frontier_providers should be
        // blocked when allow_frontier=false — even without tier="frontier"
        // on the preference itself.
        let mut tag_prefs = BTreeMap::new();
        tag_prefs.insert(
            "code".into(),
            RouteTargetPreference {
                provider: "expensive-model".into(),
                tier: String::new(), // no explicit tier on the pref
                ..Default::default()
            },
        );
        let table = RoutePreferenceTable {
            tag_preferences: tag_prefs,
            ..Default::default()
        };
        let signals = ComplexitySignals {
            allow_frontier: false,
            frontier_providers: vec!["expensive-model".into()],
            ..ComplexitySignals::new()
        };
        let decision = route_task(
            "implement the function",
            &["openai".into(), "expensive-model".into()],
            Some(&signals),
            Some(&table),
            "openai",
        );
        // Frontier provider blocked → falls back to first available.
        assert_eq!(decision.provider, "openai");
        assert_eq!(decision.reason, "default fallback");
    }

    #[test]
    fn frontier_allowed_when_explicitly_enabled() {
        let mut tag_prefs = BTreeMap::new();
        tag_prefs.insert(
            "architecture".into(),
            RouteTargetPreference {
                provider: "frontier-prov".into(),
                tier: "frontier".into(),
                ..Default::default()
            },
        );
        let table = RoutePreferenceTable {
            tag_preferences: tag_prefs,
            ..Default::default()
        };
        let signals = ComplexitySignals {
            allow_frontier: true,
            ..ComplexitySignals::new()
        };
        let decision = route_task(
            "write an ADR for system design",
            &["openai".into(), "frontier-prov".into()],
            Some(&signals),
            Some(&table),
            "openai",
        );
        // allow_frontier=true → preference is honored.
        assert_eq!(decision.provider, "frontier-prov");
        assert_ne!(decision.reason, "default fallback");
    }

    #[test]
    fn preference_effort_override() {
        let mut tier_prefs = BTreeMap::new();
        tier_prefs.insert(
            "mainline".into(),
            RouteTargetPreference {
                provider: "mainline-prov".into(),
                effort: "xhigh".into(),
                tier: "mainline".into(),
                model: String::new(),
            },
        );
        let table = RoutePreferenceTable {
            tier_preferences: tier_prefs,
            ..Default::default()
        };
        // Score must land in mainline range (0.16–0.69).
        // changed_files=5 → +0.15, prompt ≥1200 → +0.10, total = 0.25 → mainline.
        let signals = ComplexitySignals {
            changed_files: 5,
            ..ComplexitySignals::new()
        };
        let long_task = "x".repeat(1200);
        let decision = route_task(
            &long_task,
            &["mainline-prov".into()],
            Some(&signals),
            Some(&table),
            "mainline-prov",
        );
        // Score should be 0.10 (length) + 0.15 (5 files) = 0.25 → mainline.
        assert_eq!(decision.tier, RouteTier::Mainline);
        assert_eq!(decision.provider, "mainline-prov");
        // The preference overrides effort to xhigh.
        assert_eq!(decision.effort, RouteEffort::Xhigh);
    }

    #[test]
    fn stage_preference_highest_precedence() {
        let mut stage_prefs = BTreeMap::new();
        stage_prefs.insert(
            "review".into(),
            RouteTargetPreference {
                provider: "review-prov".into(),
                ..Default::default()
            },
        );
        let mut tag_prefs = BTreeMap::new();
        tag_prefs.insert(
            "code".into(),
            RouteTargetPreference {
                provider: "code-prov".into(),
                ..Default::default()
            },
        );
        let table = RoutePreferenceTable {
            stage_preferences: stage_prefs,
            tag_preferences: tag_prefs,
            ..Default::default()
        };
        let signals = ComplexitySignals {
            stage: "review".into(),
            ..ComplexitySignals::new()
        };
        let decision = route_task(
            "implement and review the code",
            &["review-prov".into(), "code-prov".into()],
            Some(&signals),
            Some(&table),
            "review-prov",
        );
        // Stage wins over tag.
        assert_eq!(decision.provider, "review-prov");
    }

    #[test]
    fn empty_available_providers_uses_default() {
        let decision = route_task("hello", &[], None, None, "fallback");
        assert_eq!(decision.provider, "fallback");
        assert_eq!(decision.reason, "default fallback");
    }
}
