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

/// Allowlisted aggregates only: no prompts, paths, labels, raw errors or costs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct HistoryStats {
    #[serde(flatten)]
    pub counts: RunCounts,
    pub by_provider: BTreeMap<String, RunCounts>,
}

impl HistoryStats {
    #[must_use]
    pub fn from_records(records: &[RunRecord]) -> Self {
        let mut stats = Self::default();
        for record in records {
            stats.counts.record(record.status);
            stats
                .by_provider
                .entry(record.target.provider.to_string())
                .or_default()
                .record(record.status);
        }
        stats
    }
}
