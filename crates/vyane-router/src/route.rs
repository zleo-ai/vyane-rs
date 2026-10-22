//! The main routing entry point: [`route_task`].
//!
//! Given a task text, the set of available provider ids, and optional signals
//! + preferences, produce a [`RouteDecision`] naming the target to dispatch to.

use crate::decision::RouteDecision;
use crate::feedback::{FeedbackNote, FeedbackScoring};
use crate::intent::classify_intent;
use crate::preference::{RoutePreferenceTable, normalize_key, parse_effort};
use crate::score::{
    ComplexitySignals, complexity_score_with_feedback, effort_for_tier, tier_for_score,
};
use crate::tags::infer_route_tags_with_intent;

/// A route decision plus the feedback notes scoring admitted.
///
/// `feedback_notes` is empty unless the caller passed
/// [`FeedbackScoring::Enabled`]. Admitted notes do not change `decision`.
#[derive(Debug, Clone, PartialEq)]
pub struct RoutedTask {
    pub decision: RouteDecision,
    pub feedback_notes: Vec<FeedbackNote>,
}

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
///
/// Feedback notes are not accepted here. Pass them to
/// [`route_task_with_feedback`]; they stay out of the result unless that
/// caller enables feedback scoring.
pub fn route_task(
    task: &str,
    available_providers: &[String],
    signals: Option<&ComplexitySignals>,
    preferences: Option<&RoutePreferenceTable>,
    default_provider: &str,
) -> RouteDecision {
    route_task_with_feedback(
        task,
        available_providers,
        signals,
        preferences,
        default_provider,
        &[],
        FeedbackScoring::Disabled,
    )
    .decision
}

/// Route a task, optionally recording explicit feedback notes.
///
/// [`FeedbackScoring::Disabled`] ignores `feedback` and returns the same
/// decision as [`route_task`]. [`FeedbackScoring::Enabled`] records those
/// notes on [`RoutedTask::feedback_notes`] and does not change the decision.
pub fn route_task_with_feedback(
    task: &str,
    available_providers: &[String],
    signals: Option<&ComplexitySignals>,
    preferences: Option<&RoutePreferenceTable>,
    default_provider: &str,
    feedback: &[FeedbackNote],
    feedback_scoring: FeedbackScoring,
) -> RoutedTask {
    let signals = signals.cloned().unwrap_or_else(ComplexitySignals::new);
    let intent = classify_intent(task);

    // Merge explicit + inferred tags (explicit first, deduplicated).
    let mut tags = signals.task_tags.clone();
    for inferred in infer_route_tags_with_intent(task, &intent) {
        if !tags.iter().any(|t| t == &inferred) {
            tags.push(inferred);
        }
    }

    let scored = complexity_score_with_feedback(task, &signals, &tags, feedback, feedback_scoring);
    let score = scored.score;
    let tier = tier_for_score(score, &signals);
    let mut effort = effort_for_tier(tier);

    // Resolve a preference (stage → tag → tier → default).
    let matched = if let Some(table) = preferences
        && let Some(pref) = table.resolve(&signals.stage, &tags, tier)
    {
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
            Some(RouteDecision {
                selection_key: pref.selection_key.clone(),
                provider: pref.provider.clone(),
                model: pref.model.clone(),
                effort,
                tier,
                tag: first_matching_tag(&tags, table),
                intent: intent.primary.as_str().to_string(),
                complexity_score: score,
                reason: format!("preference matched (tag/stage/tier): {}", pref.provider),
            })
        } else {
            None
        }
    } else {
        None
    };

    let decision = matched.unwrap_or_else(|| {
        // Fallback: use the default provider, or the first available one.
        let provider = available_providers
            .first()
            .map(|s| s.as_str())
            .unwrap_or(default_provider);
        RouteDecision {
            selection_key: String::new(),
            provider: provider.to_string(),
            model: String::new(),
            effort,
            tier,
            tag: String::new(),
            intent: intent.primary.as_str().to_string(),
            complexity_score: score,
            reason: "default fallback".to_string(),
        }
    });

    RoutedTask {
        decision,
        feedback_notes: scored.feedback_notes,
    }
}

/// Find the first tag that has a preference entry, for diagnostic tagging.
///
/// Uses the same key normalization as [`RoutePreferenceTable::resolve`] for
/// tag lookup, while preserving the original matching tag in the diagnostic.
fn first_matching_tag(tags: &[String], table: &RoutePreferenceTable) -> String {
    for tag in tags {
        let key = normalize_key(tag);
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
    use crate::feedback::FeedbackOutcome;
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
                selection_key: "profile:frontend".into(),
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
        assert_eq!(decision.selection_key, "profile:frontend");
    }

    #[test]
    fn diagnostic_tag_agrees_with_normalized_preference_match() {
        // The table is keyed by the normalized form ("front-end") while the
        // task carries the raw tag "front end" (with a space). resolve()
        // matches via normalize_key(), so the diagnostic RouteDecision.tag
        // must report the matching tag instead of coming back empty.
        let mut tag_prefs = BTreeMap::new();
        tag_prefs.insert(
            "front-end".into(),
            RouteTargetPreference {
                selection_key: "profile:front-end".into(),
                provider: "anthropic".into(),
                ..Default::default()
            },
        );
        let table = RoutePreferenceTable {
            tag_preferences: tag_prefs,
            ..Default::default()
        };
        let signals = ComplexitySignals {
            task_tags: vec!["front end".into()],
            ..ComplexitySignals::new()
        };
        let decision = route_task(
            "build a component",
            &["anthropic".into()],
            Some(&signals),
            Some(&table),
            "openai",
        );
        // Routing itself resolves the preference through the normalized key...
        assert_eq!(decision.provider, "anthropic");
        // ...so the diagnostic tag must be reported, not silently emptied.
        assert_eq!(decision.tag, "front end");
    }

    #[test]
    fn diagnostic_tag_normalization_covers_non_space_punctuation() {
        // Same agreement requirement for a separator other than a space: the
        // raw tag "web/frontend" normalizes to the configured "web-frontend".
        let mut tag_prefs = BTreeMap::new();
        tag_prefs.insert(
            "web-frontend".into(),
            RouteTargetPreference {
                provider: "anthropic".into(),
                ..Default::default()
            },
        );
        let table = RoutePreferenceTable {
            tag_preferences: tag_prefs,
            ..Default::default()
        };
        let signals = ComplexitySignals {
            task_tags: vec!["web/frontend".into()],
            ..ComplexitySignals::new()
        };
        let decision = route_task(
            "build a page",
            &["anthropic".into()],
            Some(&signals),
            Some(&table),
            "openai",
        );
        assert_eq!(decision.provider, "anthropic");
        assert_eq!(decision.tag, "web/frontend");
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
                selection_key: String::new(),
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

    #[test]
    fn disabled_feedback_matches_current_route_and_enabled_only_records() {
        // The note names a configured security target and words that would
        // raise the score if scoring treated the note as task text. Neither
        // setting may select that target.
        let mut tag_prefs = BTreeMap::new();
        tag_prefs.insert(
            "security".into(),
            RouteTargetPreference {
                selection_key: "profile:security".into(),
                provider: "anthropic".into(),
                model: "claude-frontier".into(),
                effort: "xhigh".into(),
                tier: "frontier".into(),
            },
        );
        let table = RoutePreferenceTable {
            tag_preferences: tag_prefs,
            ..Default::default()
        };
        let signals = ComplexitySignals {
            changed_files: 5,
            ..ComplexitySignals::new()
        };
        let providers = vec!["openai".into(), "anthropic".into()];
        let notes = vec![FeedbackNote {
            target_key: "security architecture review".into(),
            outcome: FeedbackOutcome::Failed,
        }];
        let current = route_task(
            "hello world",
            &providers,
            Some(&signals),
            Some(&table),
            "openai",
        );
        assert_eq!(current.provider, "openai");
        assert_eq!(current.model, "");
        assert_eq!(current.effort, RouteEffort::Low);
        assert_eq!(current.tier, RouteTier::Economy);
        assert_eq!(current.complexity_score, 0.15);
        assert_eq!(current.selection_key, "");

        let disabled = route_task_with_feedback(
            "hello world",
            &providers,
            Some(&signals),
            Some(&table),
            "openai",
            &notes,
            FeedbackScoring::Disabled,
        );
        assert_eq!(disabled.decision, current);
        assert!(disabled.feedback_notes.is_empty());

        let enabled = route_task_with_feedback(
            "hello world",
            &providers,
            Some(&signals),
            Some(&table),
            "openai",
            &notes,
            FeedbackScoring::Enabled,
        );
        assert_eq!(enabled.decision, current);
        assert_eq!(enabled.decision.as_tuple(), current.as_tuple());
        assert_eq!(enabled.feedback_notes, notes);
    }
}
