//! Intent classification: weighted keyword matching to categorize a task.
//!
//! Clean-room port of Vyane's `classify_intent`. Primary keywords score 3,
//! secondary score 1; the highest-scoring category wins, with a confidence
//! derived from the margin over the runner-up. No LLM call — this is a cheap,
//! deterministic first pass that feeds tag inference and complexity scoring.

use serde::{Deserialize, Serialize};

/// The eight intent categories the router distinguishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentCategory {
    CodeGen,
    Review,
    Analysis,
    Debug,
    Docs,
    Research,
    Refactor,
    Test,
}

impl IntentCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            IntentCategory::CodeGen => "code_gen",
            IntentCategory::Review => "review",
            IntentCategory::Analysis => "analysis",
            IntentCategory::Debug => "debug",
            IntentCategory::Docs => "docs",
            IntentCategory::Research => "research",
            IntentCategory::Refactor => "refactor",
            IntentCategory::Test => "test",
        }
    }
}

/// The result of classifying a task: the best-matching category plus diagnostic
/// metadata.
#[derive(Debug, Clone)]
pub struct IntentResult {
    pub primary: IntentCategory,
    /// [0.0, 1.0] — how dominant the top category is over the runner-up.
    pub confidence: f64,
    pub secondary: Option<IntentCategory>,
}

const PRIMARY_WEIGHT: f64 = 3.0;
const SECONDARY_WEIGHT: f64 = 1.0;

/// (primary_keywords, secondary_keywords) for each category.
///
/// Adapted from the Python v5 keyword table (`_INTENT_KEYWORDS` in
/// `routing.py`). The Rust version uses a simplified but representative set;
/// it may diverge from the Python table on edge cases. Keyword matching uses
/// substring containment (not word boundaries), matching the Python v4
/// `keyword_scores` behavior.
#[rustfmt::skip]
const INTENT_KEYWORDS: &[(IntentCategory, &[&str], &[&str])] = &[
    (IntentCategory::CodeGen, &[
        "implement", "write", "create", "build", "add", "generate", "develop", "code",
    ], &[
        "function", "class", "method", "component", "feature", "endpoint", "api",
        "script", "module", "service", "handler", "utility",
    ]),
    (IntentCategory::Review, &[
        "review", "audit", "inspect", "evaluate", "assess", "check",
    ], &[
        "feedback", "comment", "approve", "reject", "merge", "pr review", "pull request",
        "diff", "quality", "lint",
    ]),
    (IntentCategory::Analysis, &[
        "analyze", "analyse", "explain", "understand", "investigate", "trace",
    ], &[
        "why", "how does", "what does", "architecture", "design", "flow",
        "behavior", "performance", "bottleneck", "profiling", "root cause",
    ]),
    (IntentCategory::Debug, &[
        "fix", "debug", "troubleshoot", "diagnose",
    ], &[
        "bug", "error", "crash", "broken", "failing", "not working", "issue",
        "exception", "stack trace", "segfault",
    ]),
    (IntentCategory::Docs, &[
        "document", "readme", "docstring", "changelog", "api docs", "wiki",
        "translate", "summarize",
    ], &[
        "comment", "jsdoc", "typedoc", "description", "annotation", "documentation",
    ]),
    (IntentCategory::Research, &[
        "research", "compare", "explore", "benchmark", "trade-off", "trade off",
    ], &[
        "alternatives", "options", "pros and cons", "survey", "landscape",
        "evaluation", "spike",
    ]),
    (IntentCategory::Refactor, &[
        "refactor", "restructure", "reorganize", "deduplicate",
    ], &[
        "clean up", "simplify", "extract", "optimize", "consolidate", "rename",
        "move", "split",
    ]),
    (IntentCategory::Test, &[
        "unit test", "integration test", "test plan", "test suite",
    ], &[
        "test", "spec", "coverage", "assertion", "mock", "fixture", "expect",
        "pytest", "jest", "vitest",
    ]),
];

/// Classify a task into a structured intent category using weighted keyword
/// matching. Returns `CodeGen` with zero confidence when no keywords match.
pub fn classify_intent(task: &str) -> IntentResult {
    let task_lower = task.to_ascii_lowercase();
    let mut scores: Vec<(IntentCategory, f64)> = Vec::new();

    for (cat, primary_kws, secondary_kws) in INTENT_KEYWORDS {
        let mut total = 0.0;
        for kw in *primary_kws {
            if task_lower.contains(kw) {
                total += PRIMARY_WEIGHT;
            }
        }
        for kw in *secondary_kws {
            if task_lower.contains(kw) {
                total += SECONDARY_WEIGHT;
            }
        }
        if total > 0.0 {
            scores.push((*cat, total));
        }
    }

    if scores.is_empty() {
        return IntentResult {
            primary: IntentCategory::CodeGen,
            confidence: 0.0,
            secondary: None,
        };
    }

    scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let (best_cat, best_score) = scores[0];
    let runner_up_score = scores.get(1).map(|(_, s)| *s).unwrap_or(0.0);

    let confidence = if runner_up_score == 0.0 {
        (best_score / (PRIMARY_WEIGHT * 2.0)).min(1.0)
    } else {
        let margin = (best_score - runner_up_score) / best_score;
        let base = (best_score / (PRIMARY_WEIGHT * 2.0)).min(1.0);
        base * (0.5 + 0.5 * margin)
    };

    IntentResult {
        primary: best_cat,
        confidence: (confidence * 1000.0).round() / 1000.0,
        secondary: scores.get(1).map(|(c, _)| *c),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_gen_default() {
        let result = classify_intent("hello world");
        assert_eq!(result.primary, IntentCategory::CodeGen);
        assert_eq!(result.confidence, 0.0);
    }

    #[test]
    fn primary_keyword_dominates() {
        let result = classify_intent("implement a new function");
        assert_eq!(result.primary, IntentCategory::CodeGen);
        assert!(result.confidence > 0.0);
    }

    #[test]
    fn debug_detection() {
        let result = classify_intent("fix the bug causing crash");
        assert_eq!(result.primary, IntentCategory::Debug);
    }

    #[test]
    fn research_detection() {
        let result = classify_intent("compare alternatives and explore options");
        assert_eq!(result.primary, IntentCategory::Research);
    }

    #[test]
    fn test_detection() {
        let result = classify_intent("add unit test for coverage");
        assert_eq!(result.primary, IntentCategory::Test);
    }

    #[test]
    fn review_detection() {
        let result = classify_intent("review the pull request and leave feedback");
        assert_eq!(result.primary, IntentCategory::Review);
    }

    #[test]
    fn root_cause_is_analysis_secondary_keyword() {
        let result = classify_intent("root cause analysis");
        assert_eq!(result.primary, IntentCategory::Analysis);
        assert_eq!(result.confidence, 0.167);
        assert_eq!(result.secondary, None);
    }

    #[test]
    fn bare_pr_does_not_inflate_review_confidence() {
        let result = classify_intent("Review The PR");
        assert_eq!(result.primary, IntentCategory::Review);
        assert_eq!(result.confidence, 0.5);
        assert_eq!(result.secondary, None);
    }
}
