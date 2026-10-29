//! Pure lexical statistics for completed broadcast results.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use vyane_core::{RunRecord, RunStatus};

/// Bounds pairwise work to at most 2,016 pairs. Checked before text processing.
pub const MAX_BROADCAST_COMPARE_TARGETS: usize = 64;
pub const BROADCAST_COMPARE_INSUFFICIENT_TEXT: &str = "Need at least 2 successful text results";

#[derive(Debug, thiserror::Error)]
#[error(
    "broadcast comparison supports at most {} targets",
    MAX_BROADCAST_COMPARE_TARGETS
)]
pub struct BroadcastCompareLimit;

/// One original result slot. Slice position is its zero-based input identity.
#[derive(Debug, Clone, Copy)]
pub struct BroadcastCompareInput<'a> {
    pub target: &'a str,
    pub record: Option<&'a RunRecord>,
    pub output: Option<&'a str>,
}

#[derive(Debug, Serialize)]
pub struct BroadcastTargetComparison {
    pub index: usize,
    pub target: String,
    pub char_count: usize,
    pub word_count: usize,
    pub top_terms: Vec<String>,
    pub unique_terms: Vec<String>,
    /// Recorded wall time; reversed timestamps remain unknown.
    pub duration_seconds: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct BroadcastPairSimilarity {
    pub left_index: usize,
    pub right_index: usize,
    /// An empty token set on either side cannot be scored.
    pub similarity: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct BroadcastComparison {
    pub comparable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
    pub target_count: usize,
    pub per_target: Vec<BroadcastTargetComparison>,
    pub pairwise_similarity: Vec<BroadcastPairSimilarity>,
    pub average_similarity: Option<f64>,
    /// Input indices, fastest first; unknown timings are excluded.
    pub speed_ranking: Vec<usize>,
}

/// Compare successful, nonblank text only. Tokens are lowercase ASCII
/// alphanumeric runs. Similarity measures vocabulary overlap, not correctness.
/// Count and pairwise allocations are capped; token storage is linear in text.
/// Top terms break frequency ties by first occurrence, unique terms by length
/// then lexical order. Both lists contain at most ten terms per target.
pub fn compare_broadcast_results(
    results: &[BroadcastCompareInput<'_>],
) -> Result<BroadcastComparison, BroadcastCompareLimit> {
    if results.len() > MAX_BROADCAST_COMPARE_TARGETS {
        return Err(BroadcastCompareLimit);
    }

    let mut per_target = Vec::new();
    let mut token_sets = Vec::new();
    let mut document_counts = BTreeMap::<String, usize>::new();
    for (index, result) in results.iter().enumerate() {
        let (Some(record), Some(text)) = (result.record, result.output) else {
            continue;
        };
        if record.status != RunStatus::Success || text.trim().is_empty() {
            continue;
        }

        let mut frequencies = BTreeMap::<String, (usize, usize)>::new();
        let mut word_count = 0;
        for token in text
            .to_lowercase()
            .split(|ch: char| !ch.is_ascii_alphanumeric())
            .filter(|token| !token.is_empty())
        {
            let entry = frequencies
                .entry(token.to_owned())
                .or_insert((0, word_count));
            entry.0 += 1;
            word_count += 1;
        }
        let terms: BTreeSet<String> = frequencies.keys().cloned().collect();
        for term in &terms {
            *document_counts.entry(term.clone()).or_default() += 1;
        }
        let mut ranked: Vec<_> = frequencies.into_iter().collect();
        ranked.sort_by(|a, b| b.1.0.cmp(&a.1.0).then(a.1.1.cmp(&b.1.1)));
        let duration_seconds = record
            .finished_at
            .signed_duration_since(record.started_at)
            .to_std()
            .ok()
            .map(|elapsed| elapsed.as_secs_f64());
        per_target.push(BroadcastTargetComparison {
            index,
            target: result.target.to_owned(),
            char_count: text.chars().count(),
            word_count,
            top_terms: ranked.into_iter().take(10).map(|(term, _)| term).collect(),
            unique_terms: Vec::new(),
            duration_seconds,
        });
        token_sets.push(terms);
    }

    for (metrics, terms) in per_target.iter_mut().zip(&token_sets) {
        let mut unique: Vec<_> = terms
            .iter()
            .filter(|term| document_counts.get(*term) == Some(&1))
            .cloned()
            .collect();
        unique.sort_by(|a, b| b.len().cmp(&a.len()).then(a.cmp(b)));
        unique.truncate(10);
        metrics.unique_terms = unique;
    }

    let mut pairwise_similarity = Vec::new();
    let mut sum = 0.0;
    let mut scored = 0;
    for (i, left) in token_sets.iter().enumerate() {
        for (j, right) in token_sets.iter().enumerate().skip(i + 1) {
            let similarity = if left.is_empty() || right.is_empty() {
                None
            } else {
                let intersection = left.intersection(right).count();
                let union = left.len() + right.len() - intersection;
                let value = intersection as f64 / union as f64;
                sum += value;
                scored += 1;
                Some(round_similarity(value))
            };
            pairwise_similarity.push(BroadcastPairSimilarity {
                left_index: per_target[i].index,
                right_index: per_target[j].index,
                similarity,
            });
        }
    }
    let mut timed: Vec<_> = per_target
        .iter()
        .filter_map(|entry| entry.duration_seconds.map(|time| (entry.index, time)))
        .collect();
    timed.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
    let comparable = per_target.len() >= 2;
    Ok(BroadcastComparison {
        comparable,
        reason: (!comparable).then_some(BROADCAST_COMPARE_INSUFFICIENT_TEXT),
        target_count: per_target.len(),
        per_target,
        pairwise_similarity,
        average_similarity: (scored > 0).then(|| round_similarity(sum / scored as f64)),
        speed_ranking: timed.into_iter().map(|(index, _)| index).collect(),
    })
}

fn round_similarity(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}
