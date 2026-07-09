//! Complexity scoring and tier mapping.
//!
//! The complexity score is a deterministic additive function of structural
//! signals (changed files, dependency edges, retry count, prompt length, stage,
//! tags). No LLM call, no keyword counting beyond tag inference. The score maps
//! to a three-level tier (economy / mainline / frontier) which in turn maps to
//! a default effort level.

use crate::decision::{RouteEffort, RouteTier};

/// Explicit and structural inputs for route tiering. All fields are optional
/// or defaulted; the router works with a zero-valued `ComplexitySignals` (just
/// scoring the task text length and inferred tags).
#[derive(Debug, Clone, Default)]
pub struct ComplexitySignals {
    /// Override the computed tier directly: `"frontier"` → score 1.0,
    /// `"mainline"` → 0.45, `"economy"` → 0.05.
    pub explicit_tier: Option<String>,
    /// Workflow stage: `"plan"`, `"review"`, `"architecture"`, etc. Stages in
    /// the workflow set add to the score.
    pub stage: String,
    /// Explicit tags supplied by the caller (merged with inferred tags by the
    /// router before scoring).
    pub task_tags: Vec<String>,
    /// Number of files changed in the current work context.
    pub changed_files: usize,
    /// Number of cross-file dependency edges touched.
    pub dependency_edges: usize,
    /// How many times this task has been retried. Retries escalate the tier.
    pub retry_count: usize,
    /// Whether frontier-tier targets are allowed. When `false`, frontier is
    /// demoted to mainline.
    pub allow_frontier: bool,
    /// Provider ids considered "frontier" (expensive). Used by the frontier
    /// guard to filter candidates.
    pub frontier_providers: Vec<String>,
    /// Model ids considered "frontier".
    pub frontier_models: Vec<String>,
}

impl ComplexitySignals {
    /// Create signals with `allow_frontier = true` and everything else zeroed.
    pub fn new() -> Self {
        Self {
            allow_frontier: true,
            ..Default::default()
        }
    }
}

/// Tags that signal frontier-level work (architecture, security, review,
/// research).
const FRONTIER_TAGS: &[&str] = &["architecture", "security", "review", "research"];

/// Stages that add to the complexity score.
const WORKFLOW_STAGES: &[&str] = &["plan", "planning", "review", "verify", "architecture"];

/// Compute a complexity score in [0.0, 1.0] from the task text, signals, and
/// inferred tags.
///
/// The score is additive:
/// - workflow stage → +0.25
/// - any frontier tag → +0.20
/// - any of {debug, refactor, test} tag → +0.10
/// - changed_files ≥ 20 → +0.25, ≥ 5 → +0.15, ≥ 2 → +0.05
/// - dependency_edges ≥ 10 → +0.20, ≥ 3 → +0.10
/// - retry_count ≥ 2 → +0.25, == 1 → +0.10
/// - task length ≥ 4000 → +0.20, ≥ 1200 → +0.10
///
/// An explicit tier short-circuits: frontier → 1.0, mainline → 0.45,
/// economy → 0.05.
pub fn complexity_score(task: &str, signals: &ComplexitySignals, tags: &[String]) -> f64 {
    if let Some(explicit) = signals.explicit_tier.as_deref() {
        match RouteTier::from_str_lossy(explicit) {
            Some(RouteTier::Frontier) => return 1.0,
            Some(RouteTier::Mainline) => return 0.45,
            Some(RouteTier::Economy) => return 0.05,
            None => {}
        }
    }

    let mut score = 0.0_f64;

    let stage = signals.stage.trim().to_ascii_lowercase();
    let stage_normalized = stage.replace(|c: char| !c.is_ascii_alphanumeric() && c != '.', "-");
    if WORKFLOW_STAGES.iter().any(|s| stage_normalized == *s) {
        score += 0.25;
    }

    if tags.iter().any(|t| FRONTIER_TAGS.contains(&t.as_str())) {
        score += 0.20;
    }
    if tags
        .iter()
        .any(|t| matches!(t.as_str(), "debug" | "refactor" | "test"))
    {
        score += 0.10;
    }

    score += match signals.changed_files {
        n if n >= 20 => 0.25,
        n if n >= 5 => 0.15,
        n if n >= 2 => 0.05,
        _ => 0.0,
    };

    score += match signals.dependency_edges {
        n if n >= 10 => 0.20,
        n if n >= 3 => 0.10,
        _ => 0.0,
    };

    score += match signals.retry_count {
        n if n >= 2 => 0.25,
        1 => 0.10,
        _ => 0.0,
    };

    score += match task.len() {
        n if n >= 4000 => 0.20,
        n if n >= 1200 => 0.10,
        _ => 0.0,
    };

    (score.min(1.0) * 1000.0).round() / 1000.0
}

/// Map a complexity score to a tier, honoring explicit overrides and the
/// frontier guard.
///
/// - Explicit tier (if valid) wins, except frontier is demoted to mainline when
///   `allow_frontier` is false.
/// - score ≥ 0.70 and `allow_frontier` → frontier
/// - score ≤ 0.15 → economy
/// - otherwise → mainline
pub fn tier_for_score(score: f64, signals: &ComplexitySignals) -> RouteTier {
    if let Some(explicit) = signals.explicit_tier.as_deref() {
        if let Some(tier) = RouteTier::from_str_lossy(explicit) {
            if tier == RouteTier::Frontier && !signals.allow_frontier {
                return RouteTier::Mainline;
            }
            return tier;
        }
    }
    if score >= 0.70 && signals.allow_frontier {
        RouteTier::Frontier
    } else if score <= 0.15 {
        RouteTier::Economy
    } else {
        RouteTier::Mainline
    }
}

/// Default effort for a tier: frontier → high, economy → low, mainline → medium.
pub fn effort_for_tier(tier: RouteTier) -> RouteEffort {
    match tier {
        RouteTier::Frontier => RouteEffort::High,
        RouteTier::Economy => RouteEffort::Low,
        RouteTier::Mainline => RouteEffort::Medium,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_signals_gives_zero_score() {
        let signals = ComplexitySignals::new();
        let score = complexity_score("hi", &signals, &[]);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn explicit_frontier_short_circuits() {
        let signals = ComplexitySignals {
            explicit_tier: Some("frontier".into()),
            ..ComplexitySignals::new()
        };
        assert_eq!(complexity_score("hi", &signals, &[]), 1.0);
    }

    #[test]
    fn explicit_mainline_short_circuits() {
        let signals = ComplexitySignals {
            explicit_tier: Some("mainline".into()),
            ..ComplexitySignals::new()
        };
        assert_eq!(complexity_score("hi", &signals, &[]), 0.45);
    }

    #[test]
    fn frontier_tag_adds_score() {
        let signals = ComplexitySignals::new();
        let tags = vec!["security".to_string()];
        let score = complexity_score("fix it", &signals, &tags);
        assert!((score - 0.20).abs() < 0.001);
    }

    #[test]
    fn many_signals_reach_frontier() {
        let signals = ComplexitySignals {
            stage: "architecture".into(),
            changed_files: 20,
            dependency_edges: 10,
            ..ComplexitySignals::new()
        };
        let tags = vec!["architecture".to_string()];
        let score = complexity_score("do the thing", &signals, &tags);
        // 0.25 (stage) + 0.20 (frontier tag) + 0.25 (20 files) + 0.20 (10 edges) = 0.90
        assert!((score - 0.90).abs() < 0.001);
        assert_eq!(tier_for_score(score, &signals), RouteTier::Frontier);
    }

    #[test]
    fn tier_boundaries() {
        let signals = ComplexitySignals::new();
        assert_eq!(tier_for_score(0.0, &signals), RouteTier::Economy);
        assert_eq!(tier_for_score(0.15, &signals), RouteTier::Economy);
        assert_eq!(tier_for_score(0.16, &signals), RouteTier::Mainline);
        assert_eq!(tier_for_score(0.69, &signals), RouteTier::Mainline);
        assert_eq!(tier_for_score(0.70, &signals), RouteTier::Frontier);
        assert_eq!(tier_for_score(1.0, &signals), RouteTier::Frontier);
    }

    #[test]
    fn frontier_blocked_when_disallowed() {
        let signals = ComplexitySignals {
            allow_frontier: false,
            ..ComplexitySignals::new()
        };
        assert_eq!(tier_for_score(1.0, &signals), RouteTier::Mainline);
    }

    #[test]
    fn effort_mapping() {
        assert_eq!(effort_for_tier(RouteTier::Frontier), RouteEffort::High);
        assert_eq!(effort_for_tier(RouteTier::Mainline), RouteEffort::Medium);
        assert_eq!(effort_for_tier(RouteTier::Economy), RouteEffort::Low);
    }

    #[test]
    fn long_task_adds_score() {
        let signals = ComplexitySignals::new();
        let long_task = "x".repeat(1200);
        let score = complexity_score(&long_task, &signals, &[]);
        assert!((score - 0.10).abs() < 0.001);
    }

    #[test]
    fn retry_adds_score() {
        let signals = ComplexitySignals {
            retry_count: 2,
            ..ComplexitySignals::new()
        };
        let score = complexity_score("hi", &signals, &[]);
        assert!((score - 0.25).abs() < 0.001);
    }
}
