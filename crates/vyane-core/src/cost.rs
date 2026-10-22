//! Explicit per-model rates, in USD per 1,000,000 tokens.
//!
//! Dispatch records a cost only when the caller supplies a rate for that model.
//! An absent rate stays unknown; this module does not invent a price and does
//! not consult any built-in list.

use crate::run::Usage;

/// One model's token rates. Every field is **USD per 1,000,000 tokens**.
///
/// Reasoning tokens are a subset of `Usage::output_tokens`, and cached tokens
/// are a subset of `Usage::input_tokens`. They are not extra tokens.
///
/// - `reasoning_per_1m == None` bills the reasoning subset at `output_per_1m`.
/// - `reasoning_per_1m == Some(rate)` bills that subset at `rate` instead.
/// - `cache_read_per_1m` follows the same rule for the cached subset of input.
///
/// A subset larger than its parent is clamped to the parent count.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ModelTokenRate {
    /// USD per 1,000,000 input tokens.
    pub input_per_1m: f64,
    /// USD per 1,000,000 output tokens.
    pub output_per_1m: f64,
    /// Replacement rate for the reasoning subset of output. `None` keeps
    /// `output_per_1m`.
    pub reasoning_per_1m: Option<f64>,
    /// Replacement rate for the cached subset of input. `None` keeps
    /// `input_per_1m`.
    pub cache_read_per_1m: Option<f64>,
}

impl ModelTokenRate {
    /// Input and output rates, with reasoning folded into output and cache
    /// folded into input.
    #[must_use]
    pub const fn per_1m(input_per_1m: f64, output_per_1m: f64) -> Self {
        Self {
            input_per_1m,
            output_per_1m,
            reasoning_per_1m: None,
            cache_read_per_1m: None,
        }
    }

    /// Bill the reasoning subset of output at `rate` instead of the output rate.
    #[must_use]
    pub const fn with_reasoning(mut self, rate: f64) -> Self {
        self.reasoning_per_1m = Some(rate);
        self
    }

    /// Bill the cached subset of input at `rate` instead of the input rate.
    #[must_use]
    pub const fn with_cache_read(mut self, rate: f64) -> Self {
        self.cache_read_per_1m = Some(rate);
        self
    }
}

/// Cost in USD of `usage` at `rate`.
///
/// Returns `None` when `rate` or `usage` is missing. A present rate can still
/// estimate to `Some(0.0)`; zero is never used as the stand-in for an unknown
/// price. See [`ModelTokenRate`] for the subset convention.
#[must_use]
pub fn estimate_configured_cost(
    rate: Option<&ModelTokenRate>,
    usage: Option<&Usage>,
) -> Option<f64> {
    let rate = rate?;
    let usage = usage?;
    let per_million = 1_000_000_f64;

    let (reasoning_tokens, reasoning_cost) = match (usage.reasoning_tokens, rate.reasoning_per_1m) {
        (Some(tokens), Some(subset_rate)) => {
            let tokens = tokens.min(usage.output_tokens);
            (tokens, (tokens as f64 / per_million) * subset_rate)
        }
        _ => (0, 0.0),
    };
    let plain_output = usage.output_tokens - reasoning_tokens;

    let (cached_tokens, cached_cost) = match (usage.cached_input_tokens, rate.cache_read_per_1m) {
        (Some(tokens), Some(subset_rate)) => {
            let tokens = tokens.min(usage.input_tokens);
            (tokens, (tokens as f64 / per_million) * subset_rate)
        }
        _ => (0, 0.0),
    };
    let plain_input = usage.input_tokens - cached_tokens;

    Some(
        (plain_input as f64 / per_million) * rate.input_per_1m
            + (plain_output as f64 / per_million) * rate.output_per_1m
            + reasoning_cost
            + cached_cost,
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn usage(input: u64, output: u64) -> Usage {
        Usage {
            input_tokens: input,
            output_tokens: output,
            reasoning_tokens: None,
            cached_input_tokens: None,
        }
    }

    fn assert_cost(actual: Option<f64>, expected: f64) {
        let actual = actual.expect("priced usage should estimate");
        assert!(
            (actual - expected).abs() < 1e-9,
            "estimate {actual} ~= {expected}"
        );
    }

    #[test]
    fn absent_rate_or_usage_stays_unset() {
        let rate = ModelTokenRate::per_1m(1.0, 2.0);
        let usage = usage(1_000, 1_000);
        assert_eq!(estimate_configured_cost(None, Some(&usage)), None);
        assert_eq!(estimate_configured_cost(Some(&rate), None), None);
        assert_eq!(estimate_configured_cost(None, None), None);
    }

    #[test]
    fn known_rate_is_usd_per_million_tokens() {
        let rate = ModelTokenRate::per_1m(2.5, 10.0);
        // 1M input * 2.5 + 0.5M output * 10 = 7.5
        let usage = usage(1_000_000, 500_000);
        assert_cost(estimate_configured_cost(Some(&rate), Some(&usage)), 7.5);
    }

    #[test]
    fn zero_usage_of_a_known_rate_is_zero() {
        let rate = ModelTokenRate::per_1m(3.0, 15.0);
        let usage = usage(0, 0);
        assert_cost(estimate_configured_cost(Some(&rate), Some(&usage)), 0.0);
    }

    #[test]
    fn reasoning_and_cached_subsets_replace_parent_rates() {
        let rate = ModelTokenRate::per_1m(1.0, 2.0)
            .with_cache_read(0.1)
            .with_reasoning(8.0);
        let usage = Usage {
            input_tokens: 1_000,
            output_tokens: 1_000,
            cached_input_tokens: Some(900),
            reasoning_tokens: Some(400),
        };
        // 100*1 + 900*0.1 + 600*2 + 400*8 = 4590, per million.
        assert_cost(
            estimate_configured_cost(Some(&rate), Some(&usage)),
            0.004_59,
        );
    }

    #[test]
    fn missing_subset_rates_bill_the_parent_count_once() {
        let rate = ModelTokenRate::per_1m(1.0, 2.0);
        let usage = Usage {
            input_tokens: 1_000,
            output_tokens: 1_000,
            cached_input_tokens: Some(900),
            reasoning_tokens: Some(400),
        };
        assert_cost(estimate_configured_cost(Some(&rate), Some(&usage)), 0.003);
    }

    #[test]
    fn subset_counts_larger_than_the_parent_are_clamped() {
        let rate = ModelTokenRate::per_1m(1.0, 2.0)
            .with_cache_read(0.1)
            .with_reasoning(8.0);
        let usage = Usage {
            input_tokens: 100,
            output_tokens: 100,
            cached_input_tokens: Some(500),
            reasoning_tokens: Some(u64::MAX),
        };
        // All 100 input at 0.1 and all 100 output at 8.
        assert_cost(
            estimate_configured_cost(Some(&rate), Some(&usage)),
            0.000_81,
        );
    }
}
