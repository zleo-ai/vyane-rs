//! User-configured target preferences for tags, stages, and tiers.

use serde::{Deserialize, Serialize};

use crate::decision::{RouteEffort, RouteTier};

/// A user-configured preference for a tag, stage, or tier. When the router
/// matches a tag/stage/tier, it uses this preference's provider/model/effort
/// instead of the default fallback.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RouteTargetPreference {
    pub provider: String,
    #[serde(default)]
    pub model: String,
    /// `"low"`, `"medium"`, `"high"`, `"xhigh"`. Empty = inherit from tier.
    #[serde(default)]
    pub effort: String,
    #[serde(default)]
    pub tier: String,
}

/// The full preference table. Resolution precedence: **stage → tag → tier →
/// default**. The first match wins.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoutePreferenceTable {
    #[serde(default)]
    pub tag_preferences: std::collections::BTreeMap<String, RouteTargetPreference>,
    #[serde(default)]
    pub stage_preferences: std::collections::BTreeMap<String, RouteTargetPreference>,
    #[serde(default)]
    pub tier_preferences: std::collections::BTreeMap<String, RouteTargetPreference>,
    #[serde(default)]
    pub default: Option<RouteTargetPreference>,
}

impl RoutePreferenceTable {
    /// Resolve a preference for the given stage, tags, and tier, in precedence
    /// order (stage → tag → tier → default). Returns `None` when nothing
    /// matches and no default is configured.
    pub fn resolve(
        &self,
        stage: &str,
        tags: &[String],
        tier: RouteTier,
    ) -> Option<&RouteTargetPreference> {
        // Stage → the highest-precedence signal.
        let stage_key = normalize_key(stage);
        if !stage_key.is_empty() {
            if let Some(pref) = self.stage_preferences.get(&stage_key) {
                return Some(pref);
            }
        }

        // Tags → first matching tag wins (tags are already ordered: explicit
        // tags first, then inferred).
        for tag in tags {
            let tag_key = normalize_key(tag);
            if let Some(pref) = self.tag_preferences.get(&tag_key) {
                return Some(pref);
            }
        }

        // Tier → economy/mainline/frontier.
        if let Some(pref) = self.tier_preferences.get(tier.as_str()) {
            return Some(pref);
        }

        // Default fallback.
        self.default.as_ref()
    }
}

/// Normalize a key for lookup and insertion: lowercase, collapse non-alphanumeric
/// to hyphens, trim. Mirrors the Python `_normalize_route_key`.
///
/// Must be called on BOTH insertion (when building the table from config) and
/// lookup (when resolving) so mixed-case tags like `"Front-End"` match.
pub fn normalize_key(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

/// Parse an effort string into a [`RouteEffort`]. Falls back to `Medium`.
pub fn parse_effort(s: &str) -> RouteEffort {
    match s.trim().to_ascii_lowercase().as_str() {
        "low" => RouteEffort::Low,
        "high" => RouteEffort::High,
        "xhigh" => RouteEffort::Xhigh,
        _ => RouteEffort::Medium,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::decision::RouteTier;
    use std::collections::BTreeMap;

    fn pref(provider: &str) -> RouteTargetPreference {
        RouteTargetPreference {
            provider: provider.into(),
            model: String::new(),
            effort: String::new(),
            tier: String::new(),
        }
    }

    #[test]
    fn stage_wins_over_tag() {
        let mut stage_prefs = BTreeMap::new();
        stage_prefs.insert("review".into(), pref("reviewer"));
        let mut tag_prefs = BTreeMap::new();
        tag_prefs.insert("code".into(), pref("coder"));
        let table = RoutePreferenceTable {
            tag_preferences: tag_prefs,
            stage_preferences: stage_prefs,
            ..Default::default()
        };
        let resolved = table.resolve("review", &["code".into()], RouteTier::Mainline);
        assert_eq!(resolved.unwrap().provider, "reviewer");
    }

    #[test]
    fn tag_wins_over_tier() {
        let mut tag_prefs = BTreeMap::new();
        tag_prefs.insert("frontend".into(), pref("frontend-provider"));
        let mut tier_prefs = BTreeMap::new();
        tier_prefs.insert("mainline".into(), pref("mainline-provider"));
        let table = RoutePreferenceTable {
            tag_preferences: tag_prefs,
            tier_preferences: tier_prefs,
            ..Default::default()
        };
        let resolved = table.resolve("", &["frontend".into()], RouteTier::Mainline);
        assert_eq!(resolved.unwrap().provider, "frontend-provider");
    }

    #[test]
    fn default_fallback() {
        let table = RoutePreferenceTable {
            default: Some(pref("fallback")),
            ..Default::default()
        };
        let resolved = table.resolve("", &[], RouteTier::Mainline);
        assert_eq!(resolved.unwrap().provider, "fallback");
    }

    #[test]
    fn no_match_returns_none() {
        let table = RoutePreferenceTable::default();
        let resolved = table.resolve("", &[], RouteTier::Mainline);
        assert!(resolved.is_none());
    }

    #[test]
    fn key_normalization() {
        assert_eq!(normalize_key("  Front-End  "), "front-end");
        assert_eq!(normalize_key("Code Gen!"), "code-gen");
    }
}
