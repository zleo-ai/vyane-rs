//! Read-only run counts, derived from the existing ledger records.

use std::collections::BTreeMap;

use serde::Serialize;
use vyane_core::{RunRecord, RunStatus};

/// Counts keep timeout and cancellation distinct from an execution error.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct RunCounts {
    pub total: usize,
    pub success: usize,
    pub error: usize,
    pub timeout: usize,
    pub cancelled: usize,
}

impl RunCounts {
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

/// Allowlisted counts only: no prompts, paths, labels, raw errors or costs.
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
