//! Read-only run statistics, derived from the existing ledger records.

use std::collections::BTreeMap;

use serde::{Serialize, Serializer, ser::SerializeStruct};
use vyane_core::{RunRecord, RunStatus};

/// Counts keep timeout and cancellation distinct from an execution error.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RunCounts {
    pub total: usize,
    pub success: usize,
    pub error: usize,
    pub timeout: usize,
    pub cancelled: usize,
}

impl RunCounts {
    /// Successful runs as a percentage, rounded to one decimal.
    /// No matching runs means no measured rate.
    #[must_use]
    pub fn success_rate(&self) -> Option<f64> {
        if self.total == 0 {
            return None;
        }
        let rate = self.success as f64 / self.total as f64 * 100.0;
        // Round the percentage itself in decimal. Scaling before rounding
        // can change the result for values such as 111/2000 * 100.
        format!("{rate:.1}").parse().ok()
    }

    fn record(&mut self, status: RunStatus) {
        self.total += 1;
        match status {
            RunStatus::Success => self.success += 1,
            RunStatus::Error => self.error += 1,
            RunStatus::Timeout => self.timeout += 1,
            RunStatus::Cancelled => self.cancelled += 1,
        }
    }
}

impl Serialize for RunCounts {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = serializer.serialize_struct("RunCounts", 6)?;
        fields.serialize_field("total", &self.total)?;
        fields.serialize_field("success", &self.success)?;
        fields.serialize_field("error", &self.error)?;
        fields.serialize_field("timeout", &self.timeout)?;
        fields.serialize_field("cancelled", &self.cancelled)?;
        fields.serialize_field("success_rate", &self.success_rate())?;
        fields.end()
    }
}

/// Whole-run wall-clock timings derived from saved start/finish timestamps.
/// Reversed timestamps are counted separately instead of becoming zero samples.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct HistoryDurations {
    pub samples: usize,
    pub invalid: usize,
    total_ms: u128,
}

impl HistoryDurations {
    /// Sum of measured millisecond samples. No sample, or a sum outside u64,
    /// has no representable total in this projection.
    #[must_use]
    pub fn total_ms(&self) -> Option<u64> {
        if self.samples == 0 {
            return None;
        }
        u64::try_from(self.total_ms).ok()
    }

    /// Mean of valid samples, rounded to one decimal millisecond.
    #[must_use]
    pub fn mean_ms(&self) -> Option<f64> {
        if self.samples == 0 {
            return None;
        }
        let mean = self.total_ms as f64 / self.samples as f64;
        format!("{mean:.1}").parse().ok()
    }

    fn record(&mut self, record: &RunRecord) {
        let elapsed = record.finished_at.signed_duration_since(record.started_at);
        let Ok(duration) = elapsed.to_std() else {
            self.invalid += 1;
            return;
        };
        self.samples += 1;
        // A representable chrono timestamp span fits comfortably in u64 ms;
        // a u128 sum also covers every possible in-memory slice on supported
        // platforms without wrapping when many long samples are combined.
        self.total_ms += duration.as_millis();
    }
}

impl Serialize for HistoryDurations {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = serializer.serialize_struct("HistoryDurations", 4)?;
        fields.serialize_field("samples", &self.samples)?;
        fields.serialize_field("invalid", &self.invalid)?;
        fields.serialize_field("total_ms", &self.total_ms())?;
        fields.serialize_field("mean_ms", &self.mean_ms())?;
        fields.end()
    }
}

/// One reported token field, aggregated independently across saved runs.
/// Missing fields remain unknown; a reported zero is a known observation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct HistoryTokenTotal {
    pub known: usize,
    pub unknown: usize,
    total: u128,
}

impl HistoryTokenTotal {
    /// Known partial total. None means no observations or a sum outside u64.
    #[must_use]
    pub fn total_tokens(&self) -> Option<u64> {
        if self.known == 0 {
            return None;
        }
        u64::try_from(self.total).ok()
    }

    #[must_use]
    pub fn overflowed(&self) -> bool {
        self.total > u128::from(u64::MAX)
    }

    fn record(&mut self, tokens: Option<u64>) {
        match tokens {
            Some(value) => {
                self.known += 1;
                // A slice contains at most usize::MAX records; on supported
                // platforms, that many u64 values fit in a u128 accumulator.
                self.total += u128::from(value);
            }
            None => self.unknown += 1,
        }
    }
}

impl Serialize for HistoryTokenTotal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = serializer.serialize_struct("HistoryTokenTotal", 4)?;
        fields.serialize_field("known", &self.known)?;
        fields.serialize_field("unknown", &self.unknown)?;
        fields.serialize_field("total_tokens", &self.total_tokens())?;
        fields.serialize_field("overflowed", &self.overflowed())?;
        fields.end()
    }
}

/// Observed input/output and optional detail fields, without estimating usage.
/// Detail fields are not added to their parent fields or used to infer prices.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct HistoryUsage {
    pub input_tokens: HistoryTokenTotal,
    pub output_tokens: HistoryTokenTotal,
    pub reasoning_tokens: HistoryTokenTotal,
    pub cached_input_tokens: HistoryTokenTotal,
}

impl HistoryUsage {
    fn record(&mut self, record: &RunRecord) {
        let usage = record.usage.as_ref();
        self.input_tokens.record(usage.map(|u| u.input_tokens));
        self.output_tokens.record(usage.map(|u| u.output_tokens));
        self.reasoning_tokens
            .record(usage.and_then(|u| u.reasoning_tokens));
        self.cached_input_tokens
            .record(usage.and_then(|u| u.cached_input_tokens));
    }
}

/// Allowlisted aggregates only: no prompts, paths, labels, raw errors or costs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct HistoryStats {
    #[serde(flatten)]
    pub counts: RunCounts,
    pub duration: HistoryDurations,
    /// Per-field known totals and missing-observation counts from stored usage.
    pub usage: HistoryUsage,
    pub by_provider: BTreeMap<String, RunCounts>,
    /// Whole recorded run spans grouped by their final provider.
    pub by_provider_duration: BTreeMap<String, HistoryDurations>,
}

impl HistoryStats {
    #[must_use]
    pub fn from_records(records: &[RunRecord]) -> Self {
        let mut stats = Self::default();
        for record in records {
            stats.counts.record(record.status);
            stats.duration.record(record);
            stats.usage.record(record);
            let provider = record.target.provider.to_string();
            stats
                .by_provider
                .entry(provider.clone())
                .or_default()
                .record(record.status);
            stats
                .by_provider_duration
                .entry(provider)
                .or_default()
                .record(record);
        }
        stats
    }
}

#[cfg(test)]
mod duration_tests {
    use super::*;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn history_duration_large_total_is_unknown_without_wrapping_or_losing_the_mean() {
        let mut record: RunRecord = serde_json::from_str(
            r#"{
            "run_id":"duration-overflow","owner":"local",
            "started_at":"2026-01-01T00:00:00Z","finished_at":"2026-01-01T00:00:01Z",
            "task_digest":"d","sandbox":"read-only",
            "target":{"provider":"p","protocol":"openai_chat","model":"m"},
            "transport":"direct_http","attempts":[],"status":"success"
        }"#,
        )
        .unwrap();
        record.started_at = chrono::DateTime::<chrono::Utc>::MIN_UTC;
        record.finished_at = chrono::DateTime::<chrono::Utc>::MAX_UTC;
        let stats = HistoryStats::from_records(&vec![record; 2048]);
        assert_eq!(stats.duration.samples, 2048);
        assert_eq!(stats.duration.invalid, 0);
        assert_eq!(stats.duration.total_ms(), None);
        assert!(
            stats
                .duration
                .mean_ms()
                .is_some_and(|value| value.is_finite() && value > 1.0e16)
        );
        let body = serde_json::to_value(stats).unwrap();
        assert!(body["duration"]["total_ms"].is_null());
        assert!(
            body["duration"]["mean_ms"]
                .as_f64()
                .is_some_and(|value| value.is_finite())
        );
    }
}
