//! Owner-scoped run ratings and read-only outcome statistics.
//!
//! Ratings live in [`vyane_ledger::JsonlFeedbackStore`]. They are joined to
//! run records by `(owner, run_id)` only when outcome statistics are read, so
//! the run ledger stays the single source of provider, protocol, harness and
//! model facts.
//!
//! Nothing in this module is read by routing. Route selection stays
//! deterministic (ADR 0001); a stateful feedback signal in route scoring is a
//! separate, explicitly enabled feature.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};
use vyane_core::{ErrorKind, RunRecord, RunStatus, VyaneError};
use vyane_ledger::{FeedbackRecord, Rating};

use crate::history_stats::{HistoryCosts, HistoryDurations, HistoryUsage, RunCounts};

/// Wire schema of [`FeedbackView`].
pub const FEEDBACK_VIEW_SCHEMA: u32 = 1;

/// Most dimensions one outcome query may group by.
pub const MAX_OUTCOME_DIMENSIONS: usize = 4;

/// Grouping used when the caller names none.
pub const DEFAULT_OUTCOME_GROUP_BY: [OutcomeDimension; 4] = [
    OutcomeDimension::Provider,
    OutcomeDimension::Harness,
    OutcomeDimension::Model,
    OutcomeDimension::Effort,
];

/// Value used for a grouping dimension that a run did not record.
pub const OUTCOME_UNKNOWN: &str = "unknown";

/// Harness value for a direct HTTP run, which has no harness.
pub const OUTCOME_DIRECT_HARNESS: &str = "direct";

/// Allowlisted rating view for CLI, REST and MCP.
///
/// It omits the owner: every read is already scoped to one owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FeedbackView {
    pub view_schema: u32,
    pub feedback_id: String,
    pub run_id: String,
    pub rating: u8,
    pub source: String,
    pub recorded_at: DateTime<Utc>,
}

impl From<&FeedbackRecord> for FeedbackView {
    fn from(record: &FeedbackRecord) -> Self {
        Self {
            view_schema: FEEDBACK_VIEW_SCHEMA,
            feedback_id: record.feedback_id.clone(),
            run_id: record.run_id.clone(),
            rating: record.rating.get(),
            source: record.source.to_string(),
            recorded_at: record.recorded_at,
        }
    }
}

/// Folded ratings for one owner, most recently recorded first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FeedbackList {
    pub items: Vec<FeedbackView>,
    /// Feedback lines skipped as corrupt, truncated or of an unknown schema.
    pub skipped_lines: u64,
}

impl FeedbackList {
    #[must_use]
    pub(crate) fn new(items: Vec<FeedbackView>, skipped_lines: u64) -> Self {
        Self {
            items,
            skipped_lines,
        }
    }
}

/// One field an outcome query can group by. Provider, protocol, harness and
/// model stay separate dimensions; they are never combined into one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OutcomeDimension {
    Provider,
    Protocol,
    Harness,
    Model,
    Effort,
    Profile,
    Status,
}

impl OutcomeDimension {
    /// Every dimension, in canonical order.
    pub const ALL: [Self; 7] = [
        Self::Provider,
        Self::Protocol,
        Self::Harness,
        Self::Model,
        Self::Effort,
        Self::Profile,
        Self::Status,
    ];

    /// Stable lowercase token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Provider => "provider",
            Self::Protocol => "protocol",
            Self::Harness => "harness",
            Self::Model => "model",
            Self::Effort => "effort",
            Self::Profile => "profile",
            Self::Status => "status",
        }
    }

    /// The value this dimension takes for one run record.
    ///
    /// A missing or empty value is [`OUTCOME_UNKNOWN`]; a run without a
    /// harness reports [`OUTCOME_DIRECT_HARNESS`]. Effort and profile come
    /// from the reserved `routing.effort` and `routing.profile` labels.
    #[must_use]
    pub fn value_of(self, record: &RunRecord) -> String {
        let value = match self {
            Self::Provider => record.target.provider.to_string(),
            Self::Protocol => record.target.protocol.as_str().to_owned(),
            Self::Harness => record
                .target
                .harness
                .as_ref()
                .map_or_else(|| OUTCOME_DIRECT_HARNESS.to_owned(), |h| h.to_string()),
            Self::Model => record.target.model.to_string(),
            Self::Effort => record
                .labels
                .get("routing.effort")
                .cloned()
                .unwrap_or_default(),
            Self::Profile => record
                .labels
                .get("routing.profile")
                .cloned()
                .unwrap_or_default(),
            Self::Status => record.status.as_str().to_owned(),
        };
        if value.is_empty() {
            OUTCOME_UNKNOWN.to_owned()
        } else {
            value
        }
    }
}

impl fmt::Display for OutcomeDimension {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for OutcomeDimension {
    type Err = VyaneError;

    fn from_str(value: &str) -> Result<Self, VyaneError> {
        Self::ALL
            .into_iter()
            .find(|dimension| dimension.as_str() == value)
            .ok_or_else(|| {
                VyaneError::new(
                    ErrorKind::Config,
                    "group_by accepts provider, protocol, harness, model, effort, profile or status",
                )
            })
    }
}

impl Serialize for OutcomeDimension {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// Check that a grouping names 1 to [`MAX_OUTCOME_DIMENSIONS`] distinct
/// dimensions.
pub fn validate_outcome_group_by(group_by: &[OutcomeDimension]) -> Result<(), VyaneError> {
    if group_by.is_empty() || group_by.len() > MAX_OUTCOME_DIMENSIONS {
        return Err(VyaneError::new(
            ErrorKind::Config,
            "group_by must name 1 to 4 dimensions",
        ));
    }
    for (index, dimension) in group_by.iter().enumerate() {
        if group_by[..index].contains(dimension) {
            return Err(VyaneError::new(
                ErrorKind::Config,
                "group_by must not repeat a dimension",
            ));
        }
    }
    Ok(())
}

/// Parse a comma-separated grouping such as `provider,model` and validate it.
pub fn parse_outcome_group_by(raw: &str) -> Result<Vec<OutcomeDimension>, VyaneError> {
    let group_by = raw
        .split(',')
        .map(|part| part.trim().parse())
        .collect::<Result<Vec<OutcomeDimension>, _>>()?;
    validate_outcome_group_by(&group_by)?;
    Ok(group_by)
}

/// Fixed message for a rejected run-status filter.
pub const RUN_STATUS_FILTER_ERROR: &str =
    "status must be one of success, error, timeout, or cancelled";

/// Parse a run-status filter shared by the CLI, REST and MCP surfaces.
///
/// Accepts `success`, `error`, `timeout` and `cancelled` (also spelled
/// `canceled`). The error message is fixed and never echoes the input.
pub fn parse_run_status_filter(raw: &str) -> Result<RunStatus, VyaneError> {
    match raw {
        "success" => Ok(RunStatus::Success),
        "error" => Ok(RunStatus::Error),
        "timeout" => Ok(RunStatus::Timeout),
        "cancelled" | "canceled" => Ok(RunStatus::Cancelled),
        _ => Err(VyaneError::new(ErrorKind::Config, RUN_STATUS_FILTER_ERROR)),
    }
}

/// Statistics for the runs that share one grouping key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutcomeGroup {
    /// Dimension name to value, in canonical dimension order.
    pub key: BTreeMap<OutcomeDimension, String>,
    pub counts: RunCounts,
    /// Runs with more than one recorded attempt.
    pub failover_count: usize,
    pub duration: HistoryDurations,
    pub cost: HistoryCosts,
    pub usage: HistoryUsage,
    /// Runs in this group with a counted rating.
    pub rating_count: usize,
    rating_sum: u64,
}

impl OutcomeGroup {
    /// Runs that failed over, as a percentage rounded to one decimal.
    /// An empty group has no rate.
    #[must_use]
    pub fn failover_rate(&self) -> Option<f64> {
        if self.counts.total == 0 {
            return None;
        }
        let rate = self.failover_count as f64 / self.counts.total as f64 * 100.0;
        format!("{rate:.1}").parse().ok()
    }

    /// Mean counted rating, rounded to two decimals. No rating, no mean.
    #[must_use]
    pub fn average_rating(&self) -> Option<f64> {
        if self.rating_count == 0 {
            return None;
        }
        let mean = self.rating_sum as f64 / self.rating_count as f64;
        format!("{mean:.2}").parse().ok()
    }

    fn record(&mut self, record: &RunRecord, rating: Option<Rating>) {
        self.counts.record(record.status);
        if record.attempts.len() > 1 {
            self.failover_count += 1;
        }
        self.duration.record(record);
        self.cost.record(record.cost_usd);
        self.usage.record(record);
        if let Some(rating) = rating {
            self.rating_count += 1;
            self.rating_sum += u64::from(rating.get());
        }
    }
}

// Serialize computed rates next to the counters they come from.
#[derive(Serialize)]
struct OutcomeGroupWire<'a> {
    key: &'a BTreeMap<OutcomeDimension, String>,
    #[serde(flatten)]
    counts: &'a RunCounts,
    failover_count: usize,
    failover_rate: Option<f64>,
    duration: &'a HistoryDurations,
    cost: &'a HistoryCosts,
    usage: &'a HistoryUsage,
    rating_count: usize,
    average_rating: Option<f64>,
}

impl Serialize for OutcomeGroup {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.wire().serialize(serializer)
    }
}

impl OutcomeGroup {
    fn wire(&self) -> OutcomeGroupWire<'_> {
        OutcomeGroupWire {
            key: &self.key,
            counts: &self.counts,
            failover_count: self.failover_count,
            failover_rate: self.failover_rate(),
            duration: &self.duration,
            cost: &self.cost,
            usage: &self.usage,
            rating_count: self.rating_count,
            average_rating: self.average_rating(),
        }
    }
}

/// Result of one outcome query.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutcomeReport {
    pub group_by: Vec<OutcomeDimension>,
    pub include_auto_ratings: bool,
    /// Runs that matched the filter, across all groups.
    pub total_runs: usize,
    pub groups: Vec<OutcomeGroup>,
    /// Feedback lines skipped as corrupt, truncated or of an unknown schema.
    pub feedback_skipped_lines: u64,
}

impl OutcomeReport {
    #[must_use]
    pub(crate) fn new(
        group_by: Vec<OutcomeDimension>,
        include_auto_ratings: bool,
        total_runs: usize,
        groups: Vec<OutcomeGroup>,
        feedback_skipped_lines: u64,
    ) -> Self {
        Self {
            group_by,
            include_auto_ratings,
            total_runs,
            groups,
            feedback_skipped_lines,
        }
    }
}

impl Serialize for OutcomeReport {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Wire<'a> {
            group_by: &'a [OutcomeDimension],
            include_auto_ratings: bool,
            total_runs: usize,
            groups: &'a [OutcomeGroup],
            feedback_skipped_lines: u64,
        }
        Wire {
            group_by: &self.group_by,
            include_auto_ratings: self.include_auto_ratings,
            total_runs: self.total_runs,
            groups: &self.groups,
            feedback_skipped_lines: self.feedback_skipped_lines,
        }
        .serialize(serializer)
    }
}

/// Group `records` by `group_by` and aggregate each group.
///
/// `ratings` maps a run id to its one counted rating; callers build it for
/// the same owner as `records`. Groups are sorted by their key values in
/// `group_by` order, so the output is deterministic.
#[must_use]
pub fn summarize_outcomes(
    records: &[RunRecord],
    ratings: &HashMap<String, Rating>,
    group_by: &[OutcomeDimension],
) -> Vec<OutcomeGroup> {
    let mut groups: BTreeMap<Vec<String>, OutcomeGroup> = BTreeMap::new();
    for record in records {
        let values: Vec<String> = group_by
            .iter()
            .map(|dimension| dimension.value_of(record))
            .collect();
        let group = groups
            .entry(values.clone())
            .or_insert_with(|| OutcomeGroup {
                key: group_by.iter().copied().zip(values).collect(),
                ..OutcomeGroup::default()
            });
        group.record(record, ratings.get(&record.run_id).copied());
    }
    groups.into_values().collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run(id: &str, provider: &str, harness: Option<&str>) -> RunRecord {
        let mut value = json!({
            "run_id": id, "owner": "local",
            "started_at": "2026-01-01T00:00:00Z", "finished_at": "2026-01-01T00:00:02Z",
            "task_digest": "d", "sandbox": "read-only",
            "target": {"provider": provider, "protocol": "openai_chat", "model": "m1"},
            "transport": "direct_http",
            "attempts": [{"target": {"provider": provider, "protocol": "openai_chat", "model": "m1"},
                          "transport": "direct_http",
                          "started_at": "2026-01-01T00:00:00Z", "duration_ms": 2000,
                          "outcome": {"result": "ok"}}],
            "status": "success", "cost_usd": 0.5,
            "usage": {"input_tokens": 10, "output_tokens": 5},
            "labels": {"routing.effort": "medium", "routing.profile": "fast"}
        });
        if let Some(harness) = harness {
            value["target"]["harness"] = json!(harness);
            value["transport"] = json!("cli_wrap");
        }
        serde_json::from_value(value).unwrap()
    }

    fn rating(value: u8) -> Rating {
        Rating::try_from(value).unwrap()
    }

    fn six_runs() -> Vec<RunRecord> {
        let mut runs = vec![
            run("r1", "alpha", None),
            run("r2", "alpha", None),
            run("r3", "alpha", None),
            run("r4", "beta", None),
            run("r5", "beta", None),
            run("r6", "beta", None),
        ];
        // r2 failed over: two attempts.
        let first = runs[1].attempts[0].clone();
        runs[1].attempts.insert(0, first);
        // r3 timed out.
        runs[2].status = vyane_core::RunStatus::Timeout;
        // r5 has no known cost.
        runs[4].cost_usd = None;
        runs[5].status = vyane_core::RunStatus::Error;
        runs
    }

    #[test]
    fn summarize_outcomes_reports_rates_ratings_and_cost_per_group_in_stable_order() {
        let runs = six_runs();
        let ratings = HashMap::from([
            ("r1".to_owned(), rating(5)),
            ("r2".to_owned(), rating(2)),
            ("r4".to_owned(), rating(4)),
            // A rating for a run outside `records` is never counted.
            ("foreign".to_owned(), rating(1)),
        ]);
        let groups = summarize_outcomes(&runs, &ratings, &[OutcomeDimension::Provider]);
        assert_eq!(groups.len(), 2);
        let alpha = &groups[0];
        assert_eq!(alpha.key[&OutcomeDimension::Provider], "alpha");
        assert_eq!(alpha.counts.total, 3);
        assert_eq!(alpha.counts.success, 2);
        assert_eq!(alpha.counts.timeout, 1);
        assert_eq!(alpha.counts.success_rate(), Some(66.7));
        assert_eq!(alpha.failover_count, 1);
        assert_eq!(alpha.failover_rate(), Some(33.3));
        assert_eq!(alpha.rating_count, 2);
        assert_eq!(alpha.average_rating(), Some(3.5));
        assert_eq!(alpha.cost.known, 3);
        assert_eq!(alpha.cost.known_total_usd(), Some(1.5));
        let beta = &groups[1];
        assert_eq!(beta.key[&OutcomeDimension::Provider], "beta");
        assert_eq!(beta.counts.total, 3);
        assert_eq!(beta.counts.error, 1);
        assert_eq!(beta.failover_rate(), Some(0.0));
        assert_eq!(beta.rating_count, 1);
        assert_eq!(beta.average_rating(), Some(4.0));
        assert_eq!(beta.cost.known, 2);
        assert_eq!(beta.cost.unknown, 1);
        assert_eq!(beta.cost.known_total_usd(), Some(1.0));

        let report = OutcomeReport::new(vec![OutcomeDimension::Provider], false, 6, groups, 0);
        let body = serde_json::to_value(&report).unwrap();
        let expected_alpha = json!({
            "key": {"provider": "alpha"},
            "total": 3, "success": 2, "error": 0, "timeout": 1, "cancelled": 0,
            "success_rate": 66.7,
            "failover_count": 1, "failover_rate": 33.3,
            "duration": {"samples": 3, "invalid": 0, "total_ms": 6000, "mean_ms": 2000.0},
            "cost": {"known": 3, "unknown": 0, "invalid": 0, "known_total_usd": 1.5, "overflowed": false},
            "usage": {
                "input_tokens": {"known": 3, "unknown": 0, "total_tokens": 30, "overflowed": false},
                "output_tokens": {"known": 3, "unknown": 0, "total_tokens": 15, "overflowed": false},
                "reasoning_tokens": {"known": 0, "unknown": 3, "total_tokens": null, "overflowed": false},
                "cached_input_tokens": {"known": 0, "unknown": 3, "total_tokens": null, "overflowed": false}
            },
            "rating_count": 2, "average_rating": 3.5
        });
        assert_eq!(body["groups"][0], expected_alpha);
        assert_eq!(body["group_by"], json!(["provider"]));
        assert_eq!(body["include_auto_ratings"], false);
        assert_eq!(body["total_runs"], 6);
        assert_eq!(body["feedback_skipped_lines"], 0);
        assert_eq!(body["groups"][1]["key"], json!({"provider": "beta"}));
        assert_eq!(body["groups"][1]["average_rating"], 4.0);

        // Input order never changes the output order.
        let mut reversed = runs.clone();
        reversed.reverse();
        let again = summarize_outcomes(&reversed, &ratings, &[OutcomeDimension::Provider]);
        assert_eq!(
            again
                .iter()
                .map(|group| group.key[&OutcomeDimension::Provider].clone())
                .collect::<Vec<_>>(),
            ["alpha", "beta"]
        );
        let unrated = summarize_outcomes(&runs, &HashMap::new(), &[OutcomeDimension::Status]);
        assert!(unrated.iter().all(|group| group.average_rating().is_none()));
        let body = serde_json::to_value(unrated[0].wire()).unwrap();
        assert!(body["average_rating"].is_null());
    }

    #[test]
    fn dimensions_keep_the_four_layers_apart_and_mark_missing_values() {
        let mut direct = run("d", "acme", None);
        direct.labels.clear();
        let harness = run("h", "acme", Some("codex-cli"));
        let group_by = [
            OutcomeDimension::Provider,
            OutcomeDimension::Protocol,
            OutcomeDimension::Harness,
            OutcomeDimension::Effort,
        ];
        let groups = summarize_outcomes(&[direct, harness], &HashMap::new(), &group_by);
        assert_eq!(groups.len(), 2);
        let keys: Vec<_> = groups.iter().map(|group| group.key.clone()).collect();
        let direct_key = keys
            .iter()
            .find(|key| key[&OutcomeDimension::Harness] == OUTCOME_DIRECT_HARNESS)
            .unwrap();
        assert_eq!(direct_key[&OutcomeDimension::Provider], "acme");
        assert_eq!(direct_key[&OutcomeDimension::Protocol], "openai_chat");
        assert_eq!(direct_key[&OutcomeDimension::Effort], OUTCOME_UNKNOWN);
        let harness_key = keys
            .iter()
            .find(|key| key[&OutcomeDimension::Harness] == "codex-cli")
            .unwrap();
        assert_eq!(harness_key[&OutcomeDimension::Effort], "medium");
        assert_eq!(
            serde_json::to_value(harness_key).unwrap(),
            json!({"provider": "acme", "protocol": "openai_chat", "harness": "codex-cli", "effort": "medium"})
        );
        let profile = OutcomeDimension::Profile.value_of(&run("p", "acme", None));
        assert_eq!(profile, "fast");
        assert_eq!(
            OutcomeDimension::Model.value_of(&run("p", "acme", None)),
            "m1"
        );
    }

    #[test]
    fn group_by_parsing_rejects_unknown_repeated_empty_and_oversized_lists() {
        assert_eq!(
            parse_outcome_group_by("provider, model").unwrap(),
            [OutcomeDimension::Provider, OutcomeDimension::Model]
        );
        for raw in [
            "",
            "provider,",
            "vendor",
            "Provider",
            "provider,provider",
            "provider,protocol,harness,model,effort",
            "provider\u{7}",
        ] {
            let error = parse_outcome_group_by(raw).unwrap_err();
            assert_eq!(error.kind, ErrorKind::Config, "{raw:?}");
            // Only fixed messages: the rejected input is never echoed.
            assert!(
                [
                    "group_by accepts provider, protocol, harness, model, effort, profile or status",
                    "group_by must name 1 to 4 dimensions",
                    "group_by must not repeat a dimension",
                ]
                .contains(&error.message.as_str()),
                "{raw:?}"
            );
        }
        for dimension in OutcomeDimension::ALL {
            assert_eq!(
                dimension.as_str().parse::<OutcomeDimension>().unwrap(),
                dimension
            );
            assert_eq!(dimension.to_string(), dimension.as_str());
        }
        validate_outcome_group_by(&DEFAULT_OUTCOME_GROUP_BY).unwrap();
    }

    #[test]
    fn run_status_filter_accepts_known_tokens_and_never_echoes() {
        for status in [
            RunStatus::Success,
            RunStatus::Error,
            RunStatus::Timeout,
            RunStatus::Cancelled,
        ] {
            assert_eq!(parse_run_status_filter(status.as_str()).unwrap(), status);
        }
        assert_eq!(
            parse_run_status_filter("canceled").unwrap(),
            RunStatus::Cancelled
        );
        for raw in ["", "nope", "Success", "STATUS_CANARY", "success\u{7}"] {
            let error = parse_run_status_filter(raw).unwrap_err();
            assert_eq!(error.kind, ErrorKind::Config);
            assert_eq!(error.message, RUN_STATUS_FILTER_ERROR);
        }
    }
}
