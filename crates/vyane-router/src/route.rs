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
}
