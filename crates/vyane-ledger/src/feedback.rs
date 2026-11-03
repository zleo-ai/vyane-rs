//! Append-only, owner-scoped run ratings.
//!
//! [`JsonlFeedbackStore`] keeps one [`FeedbackRecord`] per line in a single
//! JSONL file. It follows the same write discipline as
//! [`crate::JsonlLedger`]: the line is serialized outside the lock, an
//! advisory exclusive `fs4` lock serializes writers across processes, and the
//! whole line is written with one `write_all`. Reads take no lock.
//!
//! Each line carries `"schema": 1`. A line with another schema value, a line
//! that does not parse, or a truncated tail is skipped and counted. It is
//! never reinterpreted with a newer meaning.
//!
//! The file is append-only. For one `(owner, run_id, source)` triple the last
//! valid line in file order wins; reads fold earlier lines away. There is no
//! edit or delete operation: appending a new rating replaces the old one.
//!
//! A rating holds no free-text comment, no prompt, and no copy of the run's
//! provider or model. Consumers join a rating to its run record by
//! `(owner, run_id)` so the run ledger stays the single source of those facts.

use std::collections::HashMap;
use std::fmt;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use chrono::{DateTime, Utc};
use fs4::fs_std::FileExt;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use vyane_core::error::{ErrorKind, Result, VyaneError};

/// Line schema version written by this module.
pub const FEEDBACK_SCHEMA: u32 = 1;

/// Longest accepted automatic rule name, in bytes.
pub const AUTO_RULE_MAX_BYTES: usize = 32;

/// A run rating from 1 (worst) to 5 (best).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Rating(u8);

impl Rating {
    /// Lowest accepted rating.
    pub const MIN: u8 = 1;
    /// Highest accepted rating.
    pub const MAX: u8 = 5;

    /// The rating value, always in `1..=5`.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

impl TryFrom<u8> for Rating {
    type Error = VyaneError;

    fn try_from(value: u8) -> Result<Self> {
        if (Self::MIN..=Self::MAX).contains(&value) {
            Ok(Self(value))
        } else {
            Err(VyaneError::new(
                ErrorKind::Config,
                "rating must be an integer from 1 to 5",
            ))
        }
    }
}

impl Serialize for Rating {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_u8(self.0)
    }
}

impl<'de> Deserialize<'de> for Rating {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = u8::deserialize(deserializer)?;
        Self::try_from(value)
            .map_err(|_| serde::de::Error::custom("rating must be an integer from 1 to 5"))
    }
}

/// Name of a machine rule that produced a rating.
///
/// Accepted names match `^[a-z0-9][a-z0-9_-]{0,31}$`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AutoRule(String);

impl AutoRule {
    /// The validated rule name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for AutoRule {
    type Err = VyaneError;

    fn from_str(value: &str) -> Result<Self> {
        let bytes = value.as_bytes();
        let valid = !bytes.is_empty()
            && bytes.len() <= AUTO_RULE_MAX_BYTES
            && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
            && bytes.iter().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
            });
        if valid {
            Ok(Self(value.to_owned()))
        } else {
            // Never echo the rejected value: it is caller input.
            Err(invalid_source())
        }
    }
}

/// Who produced a rating.
///
/// Serialized as `"human"` or `"auto:<rule>"`. Only human ratings count as a
/// quality signal by default; automatic ratings must be requested explicitly.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FeedbackSource {
    Human,
    Auto(AutoRule),
}

impl FeedbackSource {
    /// Whether this rating came from a person.
    #[must_use]
    pub const fn is_human(&self) -> bool {
        matches!(self, Self::Human)
    }
}

fn invalid_source() -> VyaneError {
    VyaneError::new(
        ErrorKind::Config,
        "feedback source must be `human`, or `auto` then a colon and a lowercase rule name",
    )
}

impl FromStr for FeedbackSource {
    type Err = VyaneError;

    fn from_str(value: &str) -> Result<Self> {
        if value == "human" {
            return Ok(Self::Human);
        }
        match value.strip_prefix("auto:") {
            Some(rule) => rule.parse().map(Self::Auto),
            None => Err(invalid_source()),
        }
    }
}

impl fmt::Display for FeedbackSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Human => formatter.write_str("human"),
            Self::Auto(rule) => write!(formatter, "auto:{}", rule.as_str()),
        }
    }
}

impl Serialize for FeedbackSource {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for FeedbackSource {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        value
            .parse()
            .map_err(|_| serde::de::Error::custom("invalid feedback source"))
    }
}

/// One stored rating for one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackRecord {
    /// UUIDv7 assigned when the rating was recorded.
    pub feedback_id: String,
    /// Owner scope; matched exactly on every read.
    pub owner: String,
    /// The rated run. The service checks that it exists for `owner`.
    pub run_id: String,
    pub rating: Rating,
    pub source: FeedbackSource,
    pub recorded_at: DateTime<Utc>,
}

/// The exact schema-1 line shape. Unknown fields are rejected so a line from
/// a different writer is skipped instead of being partially understood.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedbackLineV1 {
    schema: u32,
    feedback_id: String,
    owner: String,
    run_id: String,
    rating: Rating,
    source: FeedbackSource,
    recorded_at: DateTime<Utc>,
}

impl From<&FeedbackRecord> for FeedbackLineV1 {
    fn from(record: &FeedbackRecord) -> Self {
        Self {
            schema: FEEDBACK_SCHEMA,
            feedback_id: record.feedback_id.clone(),
            owner: record.owner.clone(),
            run_id: record.run_id.clone(),
            rating: record.rating,
            source: record.source.clone(),
            recorded_at: record.recorded_at,
        }
    }
}

impl From<FeedbackLineV1> for FeedbackRecord {
    fn from(line: FeedbackLineV1) -> Self {
        Self {
            feedback_id: line.feedback_id,
            owner: line.owner,
            run_id: line.run_id,
            rating: line.rating,
            source: line.source,
            recorded_at: line.recorded_at,
        }
    }
}

/// Read filter for [`JsonlFeedbackStore::query`]. `owner` is required.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FeedbackQuery {
    pub owner: String,
    pub run_id: Option<String>,
    /// Maximum folded records to return; `None` returns every match.
    pub limit: Option<usize>,
}

/// Result of [`JsonlFeedbackStore::query`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FeedbackPage {
    /// Folded records, most recently recorded first.
    pub records: Vec<FeedbackRecord>,
    /// Non-empty lines skipped as corrupt, truncated, or of an unknown schema.
    pub skipped_lines: u64,
}

/// Result of [`JsonlFeedbackStore::latest_by_run`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RatingIndex {
    /// One counted rating per run id.
    pub ratings: HashMap<String, Rating>,
    /// Non-empty lines skipped as corrupt, truncated, or of an unknown schema.
    pub skipped_lines: u64,
}

/// Append-only JSONL store of run ratings.
pub struct JsonlFeedbackStore {
    path: PathBuf,
}

impl JsonlFeedbackStore {
    /// Create a store backed at `path`. The file is created on first append.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Path of the backing JSONL file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one rating as one locked line.
    pub async fn append(&self, record: &FeedbackRecord) -> Result<()> {
        let path = self.path.clone();
        let line = FeedbackLineV1::from(record);
        tokio::task::spawn_blocking(move || append_blocking(&path, &line))
            .await
            .map_err(|_| VyaneError::new(ErrorKind::Io, "feedback append task failed"))?
    }

    /// Return the folded ratings for one owner, most recently recorded first.
    ///
    /// For each `(owner, run_id, source)` only the last valid line in file
    /// order is returned.
    pub async fn query(&self, query: FeedbackQuery) -> Result<FeedbackPage> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let (mut records, skipped_lines) = fold_blocking(&path, &query.owner)?;
            if let Some(run_id) = &query.run_id {
                records.retain(|record| &record.run_id == run_id);
            }
            records.reverse();
            if let Some(limit) = query.limit {
                records.truncate(limit);
            }
            Ok(FeedbackPage {
                records,
                skipped_lines,
            })
        })
        .await
        .map_err(|_| VyaneError::new(ErrorKind::Io, "feedback query task failed"))?
    }

    /// Return the one counted rating per run for `owner`.
    ///
    /// A human rating always wins. When `include_auto` is set, a run with no
    /// human rating uses its most recently recorded automatic rating. Without
    /// it, automatic ratings are ignored.
    pub async fn latest_by_run(&self, owner: &str, include_auto: bool) -> Result<RatingIndex> {
        let path = self.path.clone();
        let owner = owner.to_owned();
        tokio::task::spawn_blocking(move || {
            let (records, skipped_lines) = fold_blocking(&path, &owner)?;
            let mut human: HashMap<String, Rating> = HashMap::new();
            let mut auto: HashMap<String, Rating> = HashMap::new();
            // `records` is in fold order: the latest surviving line per key is
            // last, so a later insert is the more recent rating.
            for record in records {
                if record.source.is_human() {
                    human.insert(record.run_id, record.rating);
                } else if include_auto {
                    auto.insert(record.run_id, record.rating);
                }
            }
            for (run_id, rating) in auto {
                human.entry(run_id).or_insert(rating);
            }
            Ok(RatingIndex {
                ratings: human,
                skipped_lines,
            })
        })
        .await
        .map_err(|_| VyaneError::new(ErrorKind::Io, "feedback query task failed"))?
    }
}

fn io_error(context: &'static str) -> impl FnOnce(std::io::Error) -> VyaneError {
    // Keep the path out of the message: it can reach a protocol boundary.
    move |error| VyaneError::with_source(ErrorKind::Io, context, error)
}

fn append_blocking(path: &Path, line: &FeedbackLineV1) -> Result<()> {
    let mut bytes = serde_json::to_vec(line).map_err(|error| {
        VyaneError::with_source(ErrorKind::Io, "serialize feedback record", error)
    })?;
    bytes.push(b'\n');
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(io_error("create feedback directory"))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(io_error("open feedback store"))?;
    file.lock_exclusive()
        .map_err(io_error("lock feedback store"))?;
    file.write_all(&bytes)
        .and_then(|()| file.flush())
        .map_err(io_error("write feedback store"))?;
    Ok(())
}

/// Read every valid line for `owner` and fold it by `(run_id, source)`.
///
/// Returns the surviving records ordered by the file position of their winning
/// line (oldest first), plus the skipped-line count for the whole file.
fn fold_blocking(path: &Path, owner: &str) -> Result<(Vec<FeedbackRecord>, u64)> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), 0));
        }
        Err(error) => return Err(io_error("read feedback store")(error)),
    };
    let mut skipped = 0u64;
    let mut position = 0usize;
    let mut latest: HashMap<(String, FeedbackSource), (usize, FeedbackRecord)> = HashMap::new();
    for line in bytes.split(|&byte| byte == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let (records, bad) = records_on_line(line);
        skipped += bad;
        for record in records {
            position += 1;
            if record.owner != owner {
                continue;
            }
            latest.insert(
                (record.run_id.clone(), record.source.clone()),
                (position, record),
            );
        }
    }
    let mut ordered: Vec<(usize, FeedbackRecord)> = latest.into_values().collect();
    ordered.sort_by_key(|(position, _)| *position);
    Ok((
        ordered.into_iter().map(|(_, record)| record).collect(),
        skipped,
    ))
}

/// Valid schema-1 records on one physical line, plus how many values (or
/// unparseable tails) on it were skipped.
fn records_on_line(line: &[u8]) -> (Vec<FeedbackRecord>, u64) {
    let mut records = Vec::new();
    let mut skipped = 0u64;
    let mut parsed_any = false;
    for item in serde_json::Deserializer::from_slice(line).into_iter::<serde_json::Value>() {
        let Ok(value) = item else {
            skipped += 1;
            break;
        };
        parsed_any = true;
        let schema = value.get("schema").and_then(serde_json::Value::as_u64);
        if schema != Some(u64::from(FEEDBACK_SCHEMA)) {
            skipped += 1;
            continue;
        }
        match serde_json::from_value::<FeedbackLineV1>(value) {
            Ok(line) => records.push(line.into()),
            Err(_) => skipped += 1,
        }
    }
    if !parsed_any && skipped == 0 {
        skipped = 1;
    }
    (records, skipped)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn record(owner: &str, run_id: &str, rating: u8, source: &str) -> FeedbackRecord {
        FeedbackRecord {
            feedback_id: uuid::Uuid::now_v7().to_string(),
            owner: owner.into(),
            run_id: run_id.into(),
            rating: Rating::try_from(rating).unwrap(),
            source: source.parse().unwrap(),
            recorded_at: Utc::now(),
        }
    }

    fn store() -> (tempfile::TempDir, JsonlFeedbackStore) {
        let directory = tempfile::tempdir().unwrap();
        let store = JsonlFeedbackStore::new(directory.path().join("feedback.jsonl"));
        (directory, store)
    }

    #[test]
    fn rating_accepts_one_through_five_only() {
        for value in [0u8, 6, 9, u8::MAX] {
            let error = Rating::try_from(value).unwrap_err();
            assert_eq!(error.kind, ErrorKind::Config);
        }
        for value in 1u8..=5 {
            assert_eq!(Rating::try_from(value).unwrap().get(), value);
        }
        assert!(serde_json::from_str::<Rating>("0").is_err());
        assert!(serde_json::from_str::<Rating>("6").is_err());
        assert_eq!(serde_json::from_str::<Rating>("3").unwrap().get(), 3);
    }

    #[test]
    fn source_parses_human_and_lowercase_auto_rules_only() {
        assert_eq!(
            "human".parse::<FeedbackSource>().unwrap(),
            FeedbackSource::Human
        );
        let auto: FeedbackSource = "auto:tests".parse().unwrap();
        assert_eq!(auto.to_string(), "auto:tests");
        assert!(!auto.is_human());
        assert!("auto:a_b-9".parse::<FeedbackSource>().is_ok());
        let max_rule = format!("auto:{}", "a".repeat(AUTO_RULE_MAX_BYTES));
        assert!(max_rule.parse::<FeedbackSource>().is_ok());
        let long_rule = format!("auto:{}", "a".repeat(AUTO_RULE_MAX_BYTES + 1));
        for rejected in [
            "auto:",
            "auto:Tests",
            "robot",
            "Human",
            "auto:-x",
            "auto:_x",
            "auto:te\nsts",
            "auto:te\u{7}sts",
            "human\u{0}",
            "auto:tést",
            long_rule.as_str(),
            "",
        ] {
            let error = rejected.parse::<FeedbackSource>().unwrap_err();
            assert_eq!(error.kind, ErrorKind::Config, "{rejected:?}");
            assert!(
                rejected.is_empty() || !error.to_string().contains(rejected),
                "error echoed the rejected source"
            );
        }
        assert_eq!(
            serde_json::to_string(&FeedbackSource::Human).unwrap(),
            "\"human\""
        );
        assert!(serde_json::from_str::<FeedbackSource>("\"robot\"").is_err());
    }

    #[tokio::test]
    async fn latest_line_per_owner_run_and_source_wins_and_sources_coexist() {
        let (_directory, store) = store();
        store
            .append(&record("local", "r1", 2, "human"))
            .await
            .unwrap();
        store
            .append(&record("local", "r1", 4, "human"))
            .await
            .unwrap();
        store
            .append(&record("local", "r1", 1, "auto:tests"))
            .await
            .unwrap();
        store
            .append(&record("other", "r1", 5, "human"))
            .await
            .unwrap();

        let page = store
            .query(FeedbackQuery {
                owner: "local".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.skipped_lines, 0);
        assert_eq!(page.records.len(), 2);
        // Most recent first.
        assert_eq!(page.records[0].source.to_string(), "auto:tests");
        assert_eq!(page.records[0].rating.get(), 1);
        assert_eq!(page.records[1].source, FeedbackSource::Human);
        assert_eq!(page.records[1].rating.get(), 4);
        assert!(page.records.iter().all(|record| record.owner == "local"));

        let limited = store
            .query(FeedbackQuery {
                owner: "local".into(),
                run_id: Some("r1".into()),
                limit: Some(1),
            })
            .await
            .unwrap();
        assert_eq!(limited.records.len(), 1);
        let absent = store
            .query(FeedbackQuery {
                owner: "local".into(),
                run_id: Some("r2".into()),
                limit: None,
            })
            .await
            .unwrap();
        assert!(absent.records.is_empty());
        let foreign = store
            .query(FeedbackQuery {
                owner: "LOCAL".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(foreign.records.is_empty());

        let text = std::fs::read_to_string(store.path()).unwrap();
        assert_eq!(text.lines().count(), 4);
        let first: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        let keys: Vec<&str> = first
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys.len(),
            7,
            "schema-1 lines carry exactly the documented fields: {keys:?}"
        );
        assert_eq!(first["schema"], 1);
        assert_eq!(first["source"], "human");
    }

    #[tokio::test]
    async fn latest_by_run_prefers_human_and_includes_auto_only_on_request() {
        let (_directory, store) = store();
        store
            .append(&record("local", "r1", 2, "human"))
            .await
            .unwrap();
        store
            .append(&record("local", "r1", 5, "auto:tests"))
            .await
            .unwrap();
        store
            .append(&record("local", "r2", 1, "auto:tests"))
            .await
            .unwrap();
        store
            .append(&record("local", "r2", 3, "auto:lint"))
            .await
            .unwrap();

        let human_only = store.latest_by_run("local", false).await.unwrap();
        assert_eq!(human_only.ratings.len(), 1);
        assert_eq!(human_only.ratings["r1"].get(), 2);

        let with_auto = store.latest_by_run("local", true).await.unwrap();
        assert_eq!(with_auto.ratings.len(), 2);
        assert_eq!(with_auto.ratings["r1"].get(), 2);
        assert_eq!(with_auto.ratings["r2"].get(), 3);
        assert!(
            store
                .latest_by_run("other", true)
                .await
                .unwrap()
                .ratings
                .is_empty()
        );
    }

    #[tokio::test]
    async fn corrupt_truncated_and_unknown_schema_lines_are_skipped_and_counted() {
        let (_directory, store) = store();
        store
            .append(&record("local", "r1", 3, "human"))
            .await
            .unwrap();
        let valid = std::fs::read_to_string(store.path()).unwrap();
        let mut future: serde_json::Value = serde_json::from_str(valid.trim()).unwrap();
        future["schema"] = serde_json::json!(2);
        future["rating"] = serde_json::json!(1);
        let mut extra: serde_json::Value = serde_json::from_str(valid.trim()).unwrap();
        extra["comment"] = serde_json::json!("not stored");
        let mut out_of_range: serde_json::Value = serde_json::from_str(valid.trim()).unwrap();
        out_of_range["rating"] = serde_json::json!(9);
        let truncated = &valid.trim()[..valid.trim().len() / 2];
        let contents =
            format!("{valid}not json\n{future}\n{extra}\n{out_of_range}\n\n   \n{truncated}");
        std::fs::write(store.path(), contents).unwrap();

        let page = store
            .query(FeedbackQuery {
                owner: "local".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.skipped_lines, 5);
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].rating.get(), 3);
        let index = store.latest_by_run("local", true).await.unwrap();
        assert_eq!(index.skipped_lines, 5);
        assert_eq!(index.ratings["r1"].get(), 3);

        // A later valid append after a truncated tail still parses because
        // the tail stays on its own (corrupt) line.
        std::fs::write(
            store.path(),
            format!("{}\n", std::fs::read_to_string(store.path()).unwrap()),
        )
        .unwrap();
        store
            .append(&record("local", "r1", 5, "human"))
            .await
            .unwrap();
        let page = store
            .query(FeedbackQuery {
                owner: "local".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.records[0].rating.get(), 5);
        assert_eq!(page.skipped_lines, 5);
    }

    #[tokio::test]
    async fn missing_file_is_empty() {
        let (_directory, store) = store();
        let page = store
            .query(FeedbackQuery {
                owner: "local".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page, FeedbackPage::default());
        assert!(!store.path().exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_appends_never_interleave_lines() {
        let (_directory, store) = store();
        let store = Arc::new(store);
        let mut tasks = Vec::new();
        for index in 0..16u8 {
            let store = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                let run_id = format!("run-{index}");
                store
                    .append(&record("local", &run_id, index % 5 + 1, "human"))
                    .await
            }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        let text = std::fs::read_to_string(store.path()).unwrap();
        assert_eq!(text.lines().count(), 16);
        for line in text.lines() {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(value["schema"], 1);
        }
        let page = store
            .query(FeedbackQuery {
                owner: "local".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.records.len(), 16);
        assert_eq!(page.skipped_lines, 0);
    }
}
