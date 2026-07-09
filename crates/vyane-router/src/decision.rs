//! Core routing types: tier, effort, and the decision a router produces.

use serde::{Deserialize, Serialize};

/// Cost/quality tier for a route decision. The three tiers map to rough model
/// classes: economy (cheap/fast), mainline (balanced), frontier (strongest,
/// most expensive).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RouteTier {
    Economy,
    Mainline,
    Frontier,
}

impl RouteTier {
    pub fn as_str(self) -> &'static str {
        match self {
            RouteTier::Economy => "economy",
            RouteTier::Mainline => "mainline",
            RouteTier::Frontier => "frontier",
        }
    }

    /// Parse a tier from its string form. Returns `None` for anything else so
    /// an unknown value degrades to "no explicit tier" rather than erroring.
    pub fn from_str_lossy(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "economy" => Some(RouteTier::Economy),
            "mainline" => Some(RouteTier::Mainline),
            "frontier" => Some(RouteTier::Frontier),
            _ => None,
        }
    }
}

/// Generic effort hint. Adapters translate this to harness-specific knobs (e.g.
/// Codex `reasoning_effort`, Claude thinking budget).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RouteEffort {
    Low,
    Medium,
    High,
    Xhigh,
}

impl RouteEffort {
    pub fn as_str(self) -> &'static str {
        match self {
            RouteEffort::Low => "low",
            RouteEffort::Medium => "medium",
            RouteEffort::High => "high",
            RouteEffort::Xhigh => "xhigh",
        }
    }
}

/// The result of a routing decision. The first four fields
/// (`provider`, `model`, `effort`, `tier`) are the stable public contract;
/// the rest are diagnostic metadata for logging and observability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteDecision {
    /// The chosen provider id (e.g. "openai", "anthropic").
    pub provider: String,
    /// The chosen model id within that provider. Empty means "use the
    /// provider's default model".
    pub model: String,
    pub effort: RouteEffort,
    pub tier: RouteTier,
    /// The tag that drove the preference resolution, if any (e.g. "frontend",
    /// "security"). Empty when the decision came from the default fallback.
    pub tag: String,
    /// The inferred intent category, carried through for observability.
    pub intent: String,
    /// The complexity score [0.0, 1.0] that drove tier selection.
    pub complexity_score: f64,
    /// Human-readable reason string for logging (e.g. "tag:frontend preference
    /// selected", "default fallback").
    pub reason: String,
}

impl RouteDecision {
    /// Return the stable route tuple: `(provider, model, effort_str, tier_str)`.
    pub fn as_tuple(&self) -> (&str, &str, &str, &str) {
        (
            &self.provider,
            &self.model,
            self.effort.as_str(),
            self.tier.as_str(),
        )
    }
}
