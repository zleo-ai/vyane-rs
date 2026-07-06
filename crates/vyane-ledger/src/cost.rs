//! Cost estimation from a price table.
//!
//! The ledger never archives enough state to recompute a bill itself; instead it
//! records [`Usage`] and a price table turns that into a `cost_usd`. The
//! defining rule of this module is **never guess**: an unknown model yields
//! `None`, not zero, so downstream tooling can distinguish "we don't know the
//! price" from "this run was free".
//!
//! All rates are expressed as **USD per 1,000,000 tokens**. Keeping the unit
//! fixed everywhere avoids the order-of-magnitude mistakes (per-token vs.
//! per-1k vs. per-1M) that quietly inflate or shrink estimates.

use std::collections::BTreeMap;

use vyane_core::{ModelId, Usage};

/// Per-model token pricing. Every field is **USD per 1,000,000 tokens**.
///
/// # The reasoning / cache convention
///
/// Whether reasoning and cached tokens are *separate* from the main input /
/// output counts or *already included* in them depends on the provider's usage
/// reporting, which this crate does not control. The table encodes that as an
/// explicit convention rather than a guess:
///
/// - [`ModelPricing::reasoning_per_1m`] is `None` ⇒ reasoning tokens are assumed
///   to be **already counted** in `Usage::output_tokens` (the default — e.g.
///   OpenAI `o`-series folds reasoning into completion tokens). When `Some`,
///   reasoning tokens are billed **in addition** at the given rate.
/// - [`ModelPricing::cache_read_per_1m`] is `None` ⇒ cached tokens are assumed
///   to be **already counted** in `Usage::input_tokens`. When `Some`, cached
///   tokens are billed **in addition** at the given rate.
///
/// A caller that sets a separate rate is asserting their `Usage` reports those
/// tokens as distinct. This keeps the estimate explicit and reproducible.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ModelPricing {
    /// USD per 1,000,000 prompt / input tokens.
    pub input_per_1m: f64,
    /// USD per 1,000,000 completion / output tokens.
    pub output_per_1m: f64,
    /// Separate rate for reasoning / thinking tokens. `None` ⇒ folded into
    /// output (see type docs).
    pub reasoning_per_1m: Option<f64>,
    /// Separate rate for cached input tokens. `None` ⇒ folded into input
    /// (see type docs).
    pub cache_read_per_1m: Option<f64>,
}

impl ModelPricing {
    /// Build pricing for the common case: distinct input and output rates,
    /// reasoning folded into output, cache folded into input.
    #[must_use]
    pub const fn per_1m(input_per_1m: f64, output_per_1m: f64) -> Self {
        Self {
            input_per_1m,
            output_per_1m,
            reasoning_per_1m: None,
            cache_read_per_1m: None,
        }
    }

    /// Mark reasoning tokens as billed separately at `rate` (USD / 1M).
    #[must_use]
    pub const fn with_reasoning(mut self, rate: f64) -> Self {
        self.reasoning_per_1m = Some(rate);
        self
    }

    /// Mark cached input tokens as billed separately at `rate` (USD / 1M).
    #[must_use]
    pub const fn with_cache(mut self, rate: f64) -> Self {
        self.cache_read_per_1m = Some(rate);
        self
    }
}

/// A map from model id to [`ModelPricing`].
///
/// The builtin table ([`PriceTable::builtins`]) holds best-effort public list
/// prices for a few well-known models and is **illustrative**: vendor prices
/// change, so production deployments should override it from configuration via
/// [`PriceTable::with_overrides`], where caller-supplied entries win.
#[derive(Debug, Clone, Default)]
pub struct PriceTable {
    entries: BTreeMap<String, ModelPricing>,
}

impl PriceTable {
    /// An empty table — every model is unknown.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Best-effort public list prices (USD per 1M tokens) for a handful of
    /// well-known public models.
    ///
    /// These values are **illustrative and will go stale**: provider pricing
    /// changes over time and varies by region/plan. They exist so a fresh
    /// install produces plausible estimates without configuration; anything
    /// billing-sensitive must override them. Reasoning and cache are folded
    /// into output/input respectively (the table carries no separate rates).
    #[must_use]
    pub fn builtins() -> Self {
        let mut t = Self::new();
        // OpenAI — public API list prices, best-effort.
        t.insert("gpt-4o", ModelPricing::per_1m(2.50, 10.00));
        t.insert("gpt-4o-mini", ModelPricing::per_1m(0.15, 0.60));
        t.insert("gpt-4.1", ModelPricing::per_1m(2.00, 8.00));
        t.insert("gpt-4.1-mini", ModelPricing::per_1m(0.40, 1.60));
        t.insert("o1", ModelPricing::per_1m(15.00, 60.00));
        t.insert("o3-mini", ModelPricing::per_1m(1.10, 4.40));
        // Anthropic — public API list prices, best-effort.
        t.insert(
            "claude-3-5-sonnet-20241022",
            ModelPricing::per_1m(3.00, 15.00),
        );
        t.insert(
            "claude-3-5-haiku-20241022",
            ModelPricing::per_1m(0.80, 4.00),
        );
        t.insert(
            "claude-sonnet-4-20250514",
            ModelPricing::per_1m(3.00, 15.00),
        );
        t.insert(
            "claude-opus-4-1-20250805",
            ModelPricing::per_1m(15.00, 75.00),
        );
        // Google — public API list prices, best-effort.
        t.insert("gemini-2.0-flash", ModelPricing::per_1m(0.10, 0.40));
        t.insert("gemini-1.5-pro", ModelPricing::per_1m(1.25, 5.00));
        t
    }

    /// Insert or replace one model's pricing.
    pub fn insert(&mut self, model: impl Into<String>, pricing: ModelPricing) {
        self.entries.insert(model.into(), pricing);
    }

    /// Apply `overrides` on top of `self`; entries in `overrides` **win** over
    /// any existing entry for the same model id. This is how a config-supplied
    /// table overrides the builtins.
    #[must_use]
    pub fn with_overrides(
        mut self,
        overrides: impl IntoIterator<Item = (String, ModelPricing)>,
    ) -> Self {
        for (model, pricing) in overrides {
            self.entries.insert(model, pricing);
        }
        self
    }

    /// Pricing for a model id, if known.
    #[must_use]
    pub fn get(&self, model: &str) -> Option<ModelPricing> {
        self.entries.get(model).copied()
    }

    /// Whether `model` has a known price.
    #[must_use]
    pub fn contains(&self, model: &str) -> bool {
        self.entries.contains_key(model)
    }

    /// Number of priced models.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the table prices no models.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Estimate the cost in USD of `usage` on `model`.
    ///
    /// Returns `None` when the model is unknown — **never a zero or guessed
    /// value** — so callers can tell "free" apart from "unpriced".
    ///
    /// See [`ModelPricing`] for the reasoning / cached-token convention.
    #[must_use]
    pub fn estimate(&self, model: &ModelId, usage: &Usage) -> Option<f64> {
        let pricing = self.entries.get(model.as_str()).copied()?;

        let per_million = 1_000_000_f64;
        let mut cost = (usage.input_tokens as f64 / per_million) * pricing.input_per_1m
            + (usage.output_tokens as f64 / per_million) * pricing.output_per_1m;

        // Reasoning is billed separately only when a distinct rate is declared;
        // otherwise it is assumed already counted in output (see ModelPricing).
        if let (Some(tokens), Some(rate)) = (usage.reasoning_tokens, pricing.reasoning_per_1m) {
            cost += (tokens as f64 / per_million) * rate;
        }
        // Likewise, cached input is billed separately only with a distinct rate.
        if let (Some(tokens), Some(rate)) = (usage.cached_input_tokens, pricing.cache_read_per_1m) {
            cost += (tokens as f64 / per_million) * rate;
        }

        Some(cost)
    }
}

impl PriceTable {
    /// Iterate over `(model_id, pricing)` entries.
    pub fn iter(&self) -> impl Iterator<Item = (&str, ModelPricing)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), *v))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn model(s: &str) -> ModelId {
        ModelId::new(s)
    }

    fn usage(input: u64, output: u64) -> Usage {
        Usage {
            input_tokens: input,
            output_tokens: output,
            reasoning_tokens: None,
            cached_input_tokens: None,
        }
    }

    /// Compare an estimate to an expected value within one tenth of a cent —
    /// USD prices are decimal but float arithmetic is not, so exact `==` would
    /// be flaky. `None` stays exactly `None`.
    fn assert_cost(actual: Option<f64>, expected: f64) {
        let actual = actual.expect("expected a priced estimate, got None");
        assert!(
            (actual - expected).abs() < 1e-9,
            "estimate {actual} ~= {expected}"
        );
    }

    #[test]
    fn known_model_yields_expected_cost() {
        // gpt-4o-mini is priced at $0.15 / $0.60 per 1M. 1M input + 0.5M output
        // = 0.15 + 0.30 = $0.45.
        let table = PriceTable::builtins();
        let cost = table.estimate(&model("gpt-4o-mini"), &usage(1_000_000, 500_000));
        assert_cost(cost, 0.45);
    }

    #[test]
    fn unknown_model_is_none_not_zero() {
        let table = PriceTable::builtins();
        let cost = table.estimate(&model("totally-fake-model-xyz"), &usage(1_000, 1_000));
        assert_eq!(cost, None);
    }

    #[test]
    fn unknown_model_with_zero_usage_is_still_none() {
        let table = PriceTable::builtins();
        // Even free usage of an unknown model stays None — never a guessed zero.
        assert_eq!(table.estimate(&model("nope"), &usage(0, 0)), None);
    }

    #[test]
    fn override_changes_known_model_price() {
        let table = PriceTable::builtins()
            .with_overrides([("gpt-4o-mini".to_string(), ModelPricing::per_1m(1.00, 2.00))]);
        // With the override: 1M input + 0.5M output = 1.00 + 1.00 = $2.00.
        assert_cost(
            table.estimate(&model("gpt-4o-mini"), &usage(1_000_000, 500_000)),
            2.00,
        );
    }

    #[test]
    fn override_can_add_an_unknown_model() {
        let table = PriceTable::builtins()
            .with_overrides([("custom-model".to_string(), ModelPricing::per_1m(5.0, 5.0))]);
        assert_cost(
            table.estimate(&model("custom-model"), &usage(200_000, 0)),
            1.0,
        );
    }

    #[test]
    fn override_wins_over_builtin() {
        // A config override must beat the builtin for the same model id.
        let table = PriceTable::builtins()
            .with_overrides([("gpt-4o".to_string(), ModelPricing::per_1m(7.0, 7.0))]);
        assert_cost(table.estimate(&model("gpt-4o"), &usage(1_000_000, 0)), 7.0);
    }

    #[test]
    fn reasoning_billed_separately_when_rate_set() {
        // Declaring a reasoning rate asserts reasoning tokens are NOT in output.
        let pricing = ModelPricing::per_1m(1.0, 2.0).with_reasoning(3.0);
        let table = PriceTable::new().with_overrides([("r".to_string(), pricing)]);
        let usage = Usage {
            input_tokens: 1_000_000,
            output_tokens: 1_000_000,
            reasoning_tokens: Some(1_000_000),
            cached_input_tokens: None,
        };
        // 1*1 + 1*2 + 1*3 = 6
        assert_cost(table.estimate(&model("r"), &usage), 6.0);
    }

    #[test]
    fn reasoning_folded_into_output_when_no_rate() {
        // Without a reasoning rate, reasoning tokens are ignored (assumed in output).
        let pricing = ModelPricing::per_1m(1.0, 2.0);
        let table = PriceTable::new().with_overrides([("r".to_string(), pricing)]);
        let usage = Usage {
            input_tokens: 1_000_000,
            output_tokens: 1_000_000,
            reasoning_tokens: Some(1_000_000),
            cached_input_tokens: None,
        };
        // 1*1 + 1*2 = 3 (reasoning ignored)
        assert_cost(table.estimate(&model("r"), &usage), 3.0);
    }

    #[test]
    fn cache_billed_separately_when_rate_set() {
        let pricing = ModelPricing::per_1m(1.0, 2.0).with_cache(0.1);
        let table = PriceTable::new().with_overrides([("c".to_string(), pricing)]);
        let usage = Usage {
            input_tokens: 1_000_000,
            output_tokens: 0,
            reasoning_tokens: None,
            cached_input_tokens: Some(1_000_000),
        };
        // 1*1 + 1*0.1 = 1.1
        assert_cost(table.estimate(&model("c"), &usage), 1.1);
    }

    #[test]
    fn builtin_table_is_nonempty() {
        let table = PriceTable::builtins();
        assert!(!table.is_empty());
        assert!(table.contains("gpt-4o"));
        assert!(table.contains("claude-3-5-sonnet-20241022"));
        assert!(table.contains("gemini-2.0-flash"));
    }
}
