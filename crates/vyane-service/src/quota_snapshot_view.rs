//! Read-only freshness view of a local quota snapshot.
//!
//! The view reports `fresh`, `stale`, or `missing` and echoes a schema v1
//! object when the file can be read. It does not probe a provider, write the
//! file, or change automatic routing. A stale or unreadable snapshot stays
//! visible here, and automatic routing still leaves that route unfiltered.

use std::fs::File;
use std::io::{ErrorKind, Read};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{Number, Value};

/// Document name for the read-only snapshot view.
pub const QUOTA_SNAPSHOT_READ_SCHEMA: &str = "quota-snapshot-read.v1";

/// Age after which a successful probe is `too_old`. Three 15-minute probe
/// rounds with no newer success.
pub const QUOTA_SNAPSHOT_STALE_AFTER_MS: i64 = 45 * 60 * 1000;

/// A `generated_at_ms` further ahead than this is `future_timestamp`.
pub const QUOTA_SNAPSHOT_FUTURE_TOLERANCE_MS: i64 = 60 * 1000;

/// Largest snapshot this view will read. A longer file is `unreadable`.
pub const QUOTA_SNAPSHOT_MAX_BYTES: u64 = 1024 * 1024;

/// Largest epoch millisecond a JSON number can hold exactly.
const MAX_EPOCH_MS: i64 = (1_i64 << 53) - 1;

/// Stand-in written only while parsing an integer serde_json cannot hold.
/// It is never returned in `snapshot`.
const OVERSIZED_SENTINEL: &str = "\u{0}vyane-oversized-number";
const OVERSIZED_SENTINEL_JSON: &[u8] = br#""\u0000vyane-oversized-number""#;

/// `fresh`, `stale`, or `missing`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaSnapshotReadState {
    /// The snapshot is usable and inside the fresh window.
    Fresh,
    /// The file parsed, and at least one reason says it should not be treated
    /// as current.
    Stale,
    /// No usable snapshot is available.
    Missing,
}

impl QuotaSnapshotReadState {
    /// Stable wire token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Stale => "stale",
            Self::Missing => "missing",
        }
    }
}

/// Why a snapshot is stale or missing.
///
/// `fresh` carries an empty list. The names are fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaSnapshotReadReason {
    /// The path does not exist.
    FileMissing,
    /// The path is not a regular file, is too large, or is not strict JSON.
    Unreadable,
    /// The JSON is not schema v1, or a timestamp is present but not a usable
    /// epoch millisecond.
    SchemaMismatch,
    /// Schema v1 parsed, and `generated_at_ms` has never been recorded.
    NeverSucceeded,
    /// `generated_at_ms` is more than the future tolerance ahead of the read.
    FutureTimestamp,
    /// The successful probe is older than the stale window.
    TooOld,
    /// `probe_status` is not `ok`.
    ProbeFailed,
    /// `invalidated_at_ms` is present and at least as new as `generated_at_ms`.
    Invalidated,
}

impl QuotaSnapshotReadReason {
    /// Stable wire token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FileMissing => "file_missing",
            Self::Unreadable => "unreadable",
            Self::SchemaMismatch => "schema_mismatch",
            Self::NeverSucceeded => "never_succeeded",
            Self::FutureTimestamp => "future_timestamp",
            Self::TooOld => "too_old",
            Self::ProbeFailed => "probe_failed",
            Self::Invalidated => "invalidated",
        }
    }
}

/// Envelope returned by [`quota_snapshot_read_view`].
///
/// `snapshot` is the parsed schema v1 object. It is present for `fresh`,
/// `stale`, and `never_succeeded`, and absent for the other missing reasons.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QuotaSnapshotReadView {
    /// Always [`QUOTA_SNAPSHOT_READ_SCHEMA`].
    pub schema: &'static str,
    /// Freshness of the file.
    pub state: QuotaSnapshotReadState,
    /// Empty when `state` is `fresh`.
    pub reasons: Vec<QuotaSnapshotReadReason>,
    /// Clock used for this read, in Unix milliseconds.
    pub read_at_ms: i64,
    /// Stale window used for this read, in milliseconds.
    pub stale_after_ms: i64,
    /// Usable `generated_at_ms`, when the file has one.
    pub generated_at_ms: Option<i64>,
    /// `read_at_ms - generated_at_ms`. Negative when the probe is ahead of the
    /// read. Absent when the file has no usable generation time.
    pub age_ms: Option<i64>,
    /// Parsed snapshot object, or `null` when it cannot be echoed.
    pub snapshot: Option<Value>,
}

/// Read `path`, or the default snapshot path when `path` is `None`.
///
/// `now_ms` fixes the clock. `None` uses the current Unix time, or `0` when
/// that clock is before the Unix epoch. The default path is
/// `$XDG_STATE_HOME/aios/quota-snapshot.json`, or
/// `~/.local/state/aios/quota-snapshot.json` when `XDG_STATE_HOME` is unset
/// or empty, including the Windows home lookup used by automatic routing.
/// Nothing in this function creates, truncates, or replaces the file.
#[must_use]
pub fn quota_snapshot_read_view(path: Option<&Path>, now_ms: Option<i64>) -> QuotaSnapshotReadView {
    let read_at_ms = now_ms.unwrap_or_else(unix_now_ms);
    if let Some(path) = path {
        return view_at_path(path, read_at_ms);
    }
    match crate::routing::default_quota_snapshot_path() {
        Some(path) => view_at_path(&path, read_at_ms),
        None => missing(read_at_ms, QuotaSnapshotReadReason::FileMissing),
    }
}

fn view_at_path(path: &Path, read_at_ms: i64) -> QuotaSnapshotReadView {
    match read_regular_prefix(path) {
        FileRead::Missing => missing(read_at_ms, QuotaSnapshotReadReason::FileMissing),
        FileRead::Unreadable => missing(read_at_ms, QuotaSnapshotReadReason::Unreadable),
        FileRead::Bytes(bytes) => view_from_bytes(&bytes, read_at_ms),
    }
}

fn view_from_bytes(bytes: &[u8], read_at_ms: i64) -> QuotaSnapshotReadView {
    let Some(parsed) = parse_document(bytes) else {
        return missing(read_at_ms, QuotaSnapshotReadReason::Unreadable);
    };
    if !is_schema_v1(&parsed.value) {
        return missing(read_at_ms, QuotaSnapshotReadReason::SchemaMismatch);
    }
    let generated = epoch_field(parsed.value.get("generated_at_ms"));
    let invalidated = epoch_field(parsed.value.get("invalidated_at_ms"));
    if generated == EpochField::Unusable || invalidated == EpochField::Unusable {
        return missing(read_at_ms, QuotaSnapshotReadReason::SchemaMismatch);
    }
    // An integer outside serde_json's range cannot be echoed unchanged.
    // Timestamp fields are the schema mismatch above. Any other such number
    // stays unreadable so this view does not invent a payload.
    if parsed.rewritten && contains_sentinel(&parsed.value) {
        return missing(read_at_ms, QuotaSnapshotReadReason::Unreadable);
    }
    let Some(generated_at_ms) = generated.usable() else {
        return QuotaSnapshotReadView {
            schema: QUOTA_SNAPSHOT_READ_SCHEMA,
            state: QuotaSnapshotReadState::Missing,
            reasons: vec![QuotaSnapshotReadReason::NeverSucceeded],
            read_at_ms,
            stale_after_ms: QUOTA_SNAPSHOT_STALE_AFTER_MS,
            generated_at_ms: None,
            age_ms: None,
            snapshot: Some(parsed.value),
        };
    };

    let age_ms = read_at_ms.saturating_sub(generated_at_ms);
    let mut reasons = Vec::new();
    if age_ms < -QUOTA_SNAPSHOT_FUTURE_TOLERANCE_MS {
        reasons.push(QuotaSnapshotReadReason::FutureTimestamp);
    }
    if age_ms > QUOTA_SNAPSHOT_STALE_AFTER_MS {
        reasons.push(QuotaSnapshotReadReason::TooOld);
    }
    if parsed.value.get("probe_status").and_then(Value::as_str) != Some("ok") {
        reasons.push(QuotaSnapshotReadReason::ProbeFailed);
    }
    if invalidated
        .usable()
        .is_some_and(|mark| mark >= generated_at_ms)
    {
        reasons.push(QuotaSnapshotReadReason::Invalidated);
    }
    let state = if reasons.is_empty() {
        QuotaSnapshotReadState::Fresh
    } else {
        QuotaSnapshotReadState::Stale
    };
    QuotaSnapshotReadView {
        schema: QUOTA_SNAPSHOT_READ_SCHEMA,
        state,
        reasons,
        read_at_ms,
        stale_after_ms: QUOTA_SNAPSHOT_STALE_AFTER_MS,
        generated_at_ms: Some(generated_at_ms),
        age_ms: Some(age_ms),
        snapshot: Some(parsed.value),
    }
}

fn missing(read_at_ms: i64, reason: QuotaSnapshotReadReason) -> QuotaSnapshotReadView {
    QuotaSnapshotReadView {
        schema: QUOTA_SNAPSHOT_READ_SCHEMA,
        state: QuotaSnapshotReadState::Missing,
        reasons: vec![reason],
        read_at_ms,
        stale_after_ms: QUOTA_SNAPSHOT_STALE_AFTER_MS,
        generated_at_ms: None,
        age_ms: None,
        snapshot: None,
    }
}

struct ParsedDoc {
    value: Value,
    rewritten: bool,
}

fn parse_document(bytes: &[u8]) -> Option<ParsedDoc> {
    match serde_json::from_slice::<Value>(bytes) {
        Ok(value) => {
            serde_json::to_vec(&value).ok()?;
            Some(ParsedDoc {
                value,
                rewritten: false,
            })
        }
        Err(error) if is_oversized_number_error(&error) => {
            let rewritten = rewrite_oversized_integers(bytes)?;
            let value = serde_json::from_slice::<Value>(&rewritten).ok()?;
            serde_json::to_vec(&value).ok()?;
            Some(ParsedDoc {
                value,
                rewritten: true,
            })
        }
        Err(_) => None,
    }
}

fn is_oversized_number_error(error: &serde_json::Error) -> bool {
    error.to_string().contains("number out of range")
}

fn is_schema_v1(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    let version_ok = object
        .get("schema_version")
        .and_then(Value::as_number)
        .is_some_and(number_is_one);
    version_ok && object.get("providers").is_some_and(Value::is_array)
}

fn number_is_one(number: &Number) -> bool {
    if let Some(value) = number.as_u64() {
        return value == 1;
    }
    if let Some(value) = number.as_i64() {
        return value == 1;
    }
    number.as_f64().is_some_and(|value| value == 1.0)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EpochField {
    Absent,
    Usable(i64),
    Unusable,
}

impl EpochField {
    const fn usable(self) -> Option<i64> {
        match self {
            Self::Usable(value) => Some(value),
            Self::Absent | Self::Unusable => None,
        }
    }
}

fn epoch_field(value: Option<&Value>) -> EpochField {
    match value {
        None | Some(Value::Null) => EpochField::Absent,
        Some(Value::Number(number)) => match epoch_from_number(number) {
            Some(epoch) => EpochField::Usable(epoch),
            None => EpochField::Unusable,
        },
        Some(_) => EpochField::Unusable,
    }
}

fn epoch_from_number(number: &Number) -> Option<i64> {
    if let Some(value) = number.as_i64() {
        return (0..=MAX_EPOCH_MS).contains(&value).then_some(value);
    }
    if number.as_u64().is_some() {
        return None;
    }
    let value = number.as_f64()?;
    if !value.is_finite() || value < 0.0 || value > max_epoch_as_f64() {
        return None;
    }
    let truncated = value.trunc();
    if truncated > max_epoch_as_f64() {
        return None;
    }
    Some(truncated as i64)
}

fn max_epoch_as_f64() -> f64 {
    // 2^53 - 1 is exactly representable.
    MAX_EPOCH_MS as f64
}

fn contains_sentinel(value: &Value) -> bool {
    match value {
        Value::String(text) => text == OVERSIZED_SENTINEL,
        Value::Array(items) => items.iter().any(contains_sentinel),
        Value::Object(map) => map.values().any(contains_sentinel),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn unix_now_ms() -> i64 {
    let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

enum FileRead {
    Missing,
    Unreadable,
    Bytes(Vec<u8>),
}

fn read_regular_prefix(path: &Path) -> FileRead {
    let file = match open_snapshot(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return FileRead::Missing,
        Err(_) => return FileRead::Unreadable,
    };
    let metadata = match file.metadata() {
        Ok(metadata) => metadata,
        Err(_) => return FileRead::Unreadable,
    };
    if !metadata.is_file() {
        return FileRead::Unreadable;
    }
    let mut buffer = Vec::new();
    if file
        .take(QUOTA_SNAPSHOT_MAX_BYTES.saturating_add(1))
        .read_to_end(&mut buffer)
        .is_err()
    {
        return FileRead::Unreadable;
    }
    if u64::try_from(buffer.len()).unwrap_or(u64::MAX) > QUOTA_SNAPSHOT_MAX_BYTES {
        return FileRead::Unreadable;
    }
    FileRead::Bytes(buffer)
}

fn open_snapshot(path: &Path) -> std::io::Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(unix_nonblock_flag())
            .open(path)
    }
    #[cfg(not(unix))]
    {
        File::open(path)
    }
}

#[cfg(unix)]
const fn unix_nonblock_flag() -> i32 {
    // Linux and Android use 0x800. Other Unix targets this crate builds use 0x4.
    if cfg!(any(target_os = "linux", target_os = "android")) {
        0x800
    } else {
        0x4
    }
}

struct ScannedNumber {
    end: usize,
    oversized_integer: bool,
}

/// Replace pure integers outside serde_json's range with a sentinel string.
/// Returns `None` when the text has no such integer. Fractional and exponent
/// forms are left in place so non-finite values still fail to parse.
fn rewrite_oversized_integers(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut rewritten = Vec::with_capacity(bytes.len());
    let mut index = 0;
    let mut replaced = false;
    let mut in_string = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            rewritten.push(byte);
            index += 1;
            if byte == b'\\' && index < bytes.len() {
                rewritten.push(bytes[index]);
                index += 1;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        if byte == b'"' {
            in_string = true;
            rewritten.push(byte);
            index += 1;
            continue;
        }
        if (byte == b'-' || byte.is_ascii_digit())
            && let Some(scanned) = scan_number(bytes, index)
            && scanned.end > index
        {
            if scanned.oversized_integer {
                rewritten.extend_from_slice(OVERSIZED_SENTINEL_JSON);
                replaced = true;
            } else {
                rewritten.extend_from_slice(&bytes[index..scanned.end]);
            }
            index = scanned.end;
            continue;
        }
        rewritten.push(byte);
        index += 1;
    }
    replaced.then_some(rewritten)
}

fn scan_number(bytes: &[u8], start: usize) -> Option<ScannedNumber> {
    let mut index = start;
    let negative = bytes.get(index) == Some(&b'-');
    if negative {
        index += 1;
    }
    let digits_start = index;
    let first = *bytes.get(index)?;
    if !first.is_ascii_digit() {
        return None;
    }
    if first == b'0' {
        index += 1;
    } else {
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
    }
    let digits_end = index;
    let mut fractional = false;
    if bytes.get(index) == Some(&b'.') {
        fractional = true;
        index += 1;
        let fraction_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == fraction_start {
            return None;
        }
    }
    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        fractional = true;
        index += 1;
        if matches!(bytes.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        let exponent_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == exponent_start {
            return None;
        }
    }
    let oversized_integer =
        !fractional && integer_exceeds_serde(negative, &bytes[digits_start..digits_end]);
    Some(ScannedNumber {
        end: index,
        oversized_integer,
    })
}

fn integer_exceeds_serde(negative: bool, digits: &[u8]) -> bool {
    let limit: &[u8] = if negative {
        b"9223372036854775808"
    } else {
        b"18446744073709551615"
    };
    let digits = trim_ascii_zeros(digits);
    if digits.is_empty() || digits.len() < limit.len() {
        return false;
    }
    digits.len() > limit.len() || digits > limit
}

fn trim_ascii_zeros(digits: &[u8]) -> &[u8] {
    let offset = digits
        .iter()
        .position(|byte| *byte != b'0')
        .unwrap_or(digits.len());
    &digits[offset..]
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::fs;
    use std::path::Path;

    use serde_json::{Value, json};

    use super::*;

    const NOW_MS: i64 = 1_790_754_013_249;
    const MINUTE_MS: i64 = 60_000;

    fn snapshot(overrides: Value) -> Value {
        let mut value = json!({
            "schema_version": 1,
            "generated_at_ms": NOW_MS - 5 * MINUTE_MS,
            "probe_status": "ok",
            "probe_error": null,
            "providers": [{
                "provider": "example-provider",
                "account_label": "subscriber",
                "kind": "subscription",
                "windows": [{
                    "id": "example-provider:5h",
                    "label": "Example 5 Hour",
                    "used_pct": 7.0,
                    "resets_at_ms": NOW_MS + 60 * MINUTE_MS,
                    "duration_ms": 18_000_000,
                    "scope": null,
                    "pace_ratio": 0.349,
                    "probe_status": "ok"
                }]
            }]
        });
        let Some(extra) = overrides.as_object() else {
            return value;
        };
        let Some(object) = value.as_object_mut() else {
            return value;
        };
        for (key, item) in extra {
            object.insert(key.clone(), item.clone());
        }
        value
    }

    fn write_value(path: &Path, value: &Value) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
    }

    fn write_raw(path: &Path, raw: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, raw).unwrap();
    }

    fn view_at(path: &Path) -> QuotaSnapshotReadView {
        quota_snapshot_read_view(Some(path), Some(NOW_MS))
    }

    fn names(directory: &Path) -> Vec<String> {
        let mut found = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        found.sort();
        found
    }

    fn assert_common(view: &QuotaSnapshotReadView) {
        assert_eq!(view.schema, QUOTA_SNAPSHOT_READ_SCHEMA);
        assert_eq!(view.read_at_ms, NOW_MS);
        assert_eq!(view.stale_after_ms, QUOTA_SNAPSHOT_STALE_AFTER_MS);
        serde_json::to_vec(view).unwrap();
    }

    fn assert_missing(view: &QuotaSnapshotReadView, reason: QuotaSnapshotReadReason) {
        assert_common(view);
        assert_eq!(view.state, QuotaSnapshotReadState::Missing);
        assert_eq!(view.reasons, vec![reason]);
        assert_eq!(view.generated_at_ms, None);
        assert_eq!(view.age_ms, None);
        assert!(view.snapshot.is_none());
    }

    #[test]
    fn constants_and_tokens_match_the_read_contract() {
        assert_eq!(QUOTA_SNAPSHOT_READ_SCHEMA, "quota-snapshot-read.v1");
        assert_eq!(QUOTA_SNAPSHOT_STALE_AFTER_MS, 2_700_000);
        assert_eq!(QUOTA_SNAPSHOT_FUTURE_TOLERANCE_MS, 60_000);
        assert_eq!(QUOTA_SNAPSHOT_MAX_BYTES, 1_048_576);

        let states = [
            (QuotaSnapshotReadState::Fresh, "fresh"),
            (QuotaSnapshotReadState::Stale, "stale"),
            (QuotaSnapshotReadState::Missing, "missing"),
        ];
        for (state, token) in states {
            assert_eq!(state.as_str(), token);
            assert_eq!(serde_json::to_value(state).unwrap(), token);
        }
        let reasons = [
            (QuotaSnapshotReadReason::FileMissing, "file_missing"),
            (QuotaSnapshotReadReason::Unreadable, "unreadable"),
            (QuotaSnapshotReadReason::SchemaMismatch, "schema_mismatch"),
            (QuotaSnapshotReadReason::NeverSucceeded, "never_succeeded"),
            (QuotaSnapshotReadReason::FutureTimestamp, "future_timestamp"),
            (QuotaSnapshotReadReason::TooOld, "too_old"),
            (QuotaSnapshotReadReason::ProbeFailed, "probe_failed"),
            (QuotaSnapshotReadReason::Invalidated, "invalidated"),
        ];
        for (reason, token) in reasons {
            assert_eq!(reason.as_str(), token);
            assert_eq!(serde_json::to_value(reason).unwrap(), token);
        }
    }

    #[test]
    fn fresh_snapshot_is_returned_verbatim_and_the_file_is_not_written() {
        let directory = crate::test_support::private_tempdir();
        let path = directory.path().join("quota-snapshot.json");
        let body = snapshot(json!({}));
        write_value(&path, &body);
        let before = fs::read(&path).unwrap();
        let metadata = fs::metadata(&path).unwrap();
        let modified = metadata.modified().unwrap();
        let listing = names(directory.path());

        let view = view_at(&path);

        assert_common(&view);
        assert_eq!(view.state, QuotaSnapshotReadState::Fresh);
        assert!(view.reasons.is_empty());
        assert_eq!(view.snapshot.as_ref(), Some(&body));
        assert_eq!(view.generated_at_ms, Some(NOW_MS - 5 * MINUTE_MS));
        assert_eq!(view.age_ms, Some(5 * MINUTE_MS));
        assert_eq!(fs::read(&path).unwrap(), before);
        assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), modified);
        assert_eq!(fs::metadata(&path).unwrap().len(), metadata.len());
        assert_eq!(names(directory.path()), listing);
    }

    #[test]
    fn stale_boundary_is_strictly_after_the_threshold() {
        let directory = crate::test_support::private_tempdir();
        let at_edge = directory.path().join("edge.json");
        let past_edge = directory.path().join("past.json");
        write_value(
            &at_edge,
            &snapshot(json!({"generated_at_ms": NOW_MS - QUOTA_SNAPSHOT_STALE_AFTER_MS})),
        );
        write_value(
            &past_edge,
            &snapshot(json!({"generated_at_ms": NOW_MS - QUOTA_SNAPSHOT_STALE_AFTER_MS - 1})),
        );

        assert_eq!(view_at(&at_edge).state, QuotaSnapshotReadState::Fresh);
        let past = view_at(&past_edge);
        assert_eq!(past.state, QuotaSnapshotReadState::Stale);
        assert_eq!(past.reasons, vec![QuotaSnapshotReadReason::TooOld]);
        assert!(
            !past.snapshot.unwrap()["providers"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn probe_failure_keeps_the_payload_and_is_stale() {
        let directory = crate::test_support::private_tempdir();
        let path = directory.path().join("quota-snapshot.json");
        let body = snapshot(json!({
            "probe_status": "error",
            "probe_error": "omp usage timed out"
        }));
        write_value(&path, &body);

        let view = view_at(&path);

        assert_eq!(view.state, QuotaSnapshotReadState::Stale);
        assert_eq!(view.reasons, vec![QuotaSnapshotReadReason::ProbeFailed]);
        assert_eq!(
            view.snapshot.unwrap()["probe_error"],
            json!("omp usage timed out")
        );
    }

    #[test]
    fn invalidation_at_or_after_generation_is_stale() {
        let directory = crate::test_support::private_tempdir();
        let generated = NOW_MS - 5 * MINUTE_MS;
        let same = directory.path().join("same.json");
        let older = directory.path().join("older.json");
        write_value(
            &same,
            &snapshot(json!({
                "generated_at_ms": generated,
                "invalidated_at_ms": generated,
                "invalidate_reason": "quota_exhausted",
                "invalidate_window": "example-provider:5h"
            })),
        );
        write_value(
            &older,
            &snapshot(json!({
                "generated_at_ms": generated,
                "invalidated_at_ms": generated - 1
            })),
        );

        let view = view_at(&same);
        assert_eq!(view.state, QuotaSnapshotReadState::Stale);
        assert_eq!(view.reasons, vec![QuotaSnapshotReadReason::Invalidated]);
        assert_eq!(
            view.snapshot.unwrap()["invalidate_window"],
            json!("example-provider:5h")
        );
        assert_eq!(view_at(&older).state, QuotaSnapshotReadState::Fresh);
    }

    #[test]
    fn stale_reasons_accumulate_in_contract_order() {
        let directory = crate::test_support::private_tempdir();
        let path = directory.path().join("quota-snapshot.json");
        let generated = NOW_MS - 2 * QUOTA_SNAPSHOT_STALE_AFTER_MS;
        write_value(
            &path,
            &snapshot(json!({
                "generated_at_ms": generated,
                "probe_status": "error",
                "invalidated_at_ms": generated + 1
            })),
        );

        let view = view_at(&path);

        assert_eq!(view.state, QuotaSnapshotReadState::Stale);
        assert_eq!(
            view.reasons,
            vec![
                QuotaSnapshotReadReason::TooOld,
                QuotaSnapshotReadReason::ProbeFailed,
                QuotaSnapshotReadReason::Invalidated
            ]
        );
    }

    #[test]
    fn missing_states_carry_no_snapshot_and_do_not_create_a_file() {
        let directory = crate::test_support::private_tempdir();
        let cases = [
            ("absent", None, QuotaSnapshotReadReason::FileMissing),
            (
                "not-json",
                Some("not json"),
                QuotaSnapshotReadReason::Unreadable,
            ),
            (
                "schema-2",
                Some(r#"{"schema_version":2,"providers":[]}"#),
                QuotaSnapshotReadReason::SchemaMismatch,
            ),
            (
                "providers-object",
                Some(r#"{"schema_version":1,"providers":{}}"#),
                QuotaSnapshotReadReason::SchemaMismatch,
            ),
            ("array", Some("[]"), QuotaSnapshotReadReason::SchemaMismatch),
        ];
        for (name, contents, reason) in cases {
            let path = directory.path().join(format!("{name}.json"));
            if let Some(contents) = contents {
                write_raw(&path, contents);
            }
            let listing = names(directory.path());
            let view = view_at(&path);
            assert_missing(&view, reason);
            assert_eq!(names(directory.path()), listing, "{name}");
        }

        let nested = directory.path().join("missing/quota-snapshot.json");
        let listing = names(directory.path());
        assert_missing(&view_at(&nested), QuotaSnapshotReadReason::FileMissing);
        assert_eq!(names(directory.path()), listing);
        assert!(!directory.path().join("missing").exists());
    }

    #[test]
    fn never_succeeded_is_missing_and_keeps_the_probe_error() {
        let directory = crate::test_support::private_tempdir();
        let path = directory.path().join("quota-snapshot.json");
        let body = snapshot(json!({
            "generated_at_ms": null,
            "probe_status": "error",
            "probe_error": "omp not found",
            "providers": []
        }));
        write_value(&path, &body);

        let view = view_at(&path);

        assert_common(&view);
        assert_eq!(view.state, QuotaSnapshotReadState::Missing);
        assert_eq!(view.reasons, vec![QuotaSnapshotReadReason::NeverSucceeded]);
        assert_eq!(view.age_ms, None);
        assert_eq!(view.generated_at_ms, None);
        assert_eq!(
            view.snapshot.unwrap()["probe_error"],
            json!("omp not found")
        );
    }

    #[test]
    fn unusable_timestamps_are_schema_mismatch() {
        let directory = crate::test_support::private_tempdir();
        let overrides = [
            json!({"generated_at_ms": true}),
            json!({"generated_at_ms": "1790754013249"}),
            json!({"generated_at_ms": -1}),
            json!({"generated_at_ms": 9_007_199_254_740_992_i64}),
            json!({"invalidated_at_ms": "soon"}),
        ];
        for (index, override_value) in overrides.iter().enumerate() {
            let path = directory.path().join(format!("{index}.json"));
            write_value(&path, &snapshot(override_value.clone()));
            assert_missing(&view_at(&path), QuotaSnapshotReadReason::SchemaMismatch);
        }
    }

    #[test]
    fn fractional_timestamps_truncate_toward_zero() {
        let directory = crate::test_support::private_tempdir();
        for (index, (raw, expected)) in [(0.9, 0_i64), (1.9, 1)].into_iter().enumerate() {
            let path = directory.path().join(format!("{index}.json"));
            write_value(&path, &snapshot(json!({"generated_at_ms": raw})));
            let view = view_at(&path);
            assert_eq!(view.state, QuotaSnapshotReadState::Stale);
            assert_eq!(view.reasons, vec![QuotaSnapshotReadReason::TooOld]);
            assert_eq!(view.generated_at_ms, Some(expected));
            assert!(view.snapshot.is_some());
        }
    }

    #[test]
    fn huge_integer_timestamps_are_schema_mismatch() {
        let directory = crate::test_support::private_tempdir();
        let mut huge = String::from("1");
        huge.extend(std::iter::repeat_n('0', 400));
        for field in ["generated_at_ms", "invalidated_at_ms"] {
            let raw = format!(
                r#"{{"schema_version": 1, "generated_at_ms": 1790754013249, "probe_status": "ok", "providers": [], "{field}": {huge}}}"#
            );
            let path = directory.path().join(format!("{field}.json"));
            write_raw(&path, &raw);
            assert_missing(&view_at(&path), QuotaSnapshotReadReason::SchemaMismatch);
        }
    }

    #[test]
    fn ordinary_numbers_round_trip_unchanged() {
        let directory = crate::test_support::private_tempdir();
        let path = directory.path().join("quota-snapshot.json");
        let raw = concat!(
            r#"{"schema_version": 1, "generated_at_ms": 1790754013249.0,"#,
            r#" "probe_status": "ok", "providers": [{"provider": "cursor", "windows": ["#,
            r#"{"used_pct": 10.74, "pace_ratio": 1e-3, "duration_ms": 604800000,"#,
            r#" "resets_at_ms": 1791815698000}]}]}"#
        );
        write_raw(&path, raw);

        let view = view_at(&path);

        assert_eq!(view.state, QuotaSnapshotReadState::Fresh);
        assert_eq!(view.generated_at_ms, Some(1_790_754_013_249));
        let window = &view.snapshot.unwrap()["providers"][0]["windows"][0];
        assert_eq!(window["used_pct"], json!(10.74));
        assert_eq!(window["pace_ratio"], json!(0.001));
        assert_eq!(window["duration_ms"], json!(604_800_000));
        assert_eq!(window["resets_at_ms"], json!(1_791_815_698_000_i64));
    }

    #[test]
    fn future_success_time_is_stale_inside_the_tolerance_stays_fresh() {
        let directory = crate::test_support::private_tempdir();
        let far = directory.path().join("far.json");
        let near = directory.path().join("near.json");
        write_value(
            &far,
            &snapshot(json!({
                "generated_at_ms": NOW_MS + QUOTA_SNAPSHOT_FUTURE_TOLERANCE_MS + 1
            })),
        );
        write_value(
            &near,
            &snapshot(json!({
                "generated_at_ms": NOW_MS + QUOTA_SNAPSHOT_FUTURE_TOLERANCE_MS
            })),
        );

        let view = view_at(&far);
        assert_eq!(view.state, QuotaSnapshotReadState::Stale);
        assert_eq!(view.reasons, vec![QuotaSnapshotReadReason::FutureTimestamp]);
        assert!(view.age_ms.unwrap() < 0);
        assert_eq!(view_at(&near).state, QuotaSnapshotReadState::Fresh);
        assert_eq!(
            view_at(&near).age_ms,
            Some(-QUOTA_SNAPSHOT_FUTURE_TOLERANCE_MS)
        );
    }

    #[test]
    fn non_finite_numbers_and_lone_surrogates_are_unreadable() {
        let directory = crate::test_support::private_tempdir();
        let samples = [
            concat!(
                r#"{"schema_version": 1, "generated_at_ms": 1, "probe_status": "ok","#,
                r#" "providers": [{"windows": [{"used_pct": NaN}]}]}"#
            ),
            concat!(
                r#"{"schema_version": 1, "generated_at_ms": 1, "probe_status": "ok","#,
                r#" "providers": [{"windows": [{"pace_ratio": Infinity}]}]}"#
            ),
            concat!(
                r#"{"schema_version": 1, "generated_at_ms": 1, "probe_status": "ok","#,
                r#" "providers": [{"windows": [{"used_pct": 1e999}]}]}"#
            ),
            concat!(
                r#"{"schema_version": 1, "probe_status": "error", "providers": [],"#,
                r#" "generated_at_ms": 1790754013249, "probe_error": "\ud800"}"#
            ),
        ];
        for (index, raw) in samples.iter().enumerate() {
            let path = directory.path().join(format!("{index}.json"));
            write_raw(&path, raw);
            assert_missing(&view_at(&path), QuotaSnapshotReadReason::Unreadable);
        }
    }

    #[test]
    fn oversized_file_and_directory_are_unreadable() {
        let directory = crate::test_support::private_tempdir();
        let big = directory.path().join("big.json");
        let file = fs::File::create(&big).unwrap();
        file.set_len(QUOTA_SNAPSHOT_MAX_BYTES + 1).unwrap();
        drop(file);
        let folder = directory.path().join("folder.json");
        fs::create_dir(&folder).unwrap();

        for path in [&big, &folder] {
            assert_missing(&view_at(path), QuotaSnapshotReadReason::Unreadable);
        }
    }

    #[cfg(unix)]
    #[test]
    fn fifo_without_a_writer_is_unreadable() {
        let directory = crate::test_support::private_tempdir();
        let path = directory.path().join("quota-snapshot.json");
        let status = std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap();
        assert!(status.success());
        assert_missing(&view_at(&path), QuotaSnapshotReadReason::Unreadable);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_to_a_regular_file_is_read_and_a_directory_link_is_unreadable() {
        let directory = crate::test_support::private_tempdir();
        let real = directory.path().join("real.json");
        let link = directory.path().join("link.json");
        let body = snapshot(json!({}));
        write_value(&real, &body);
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert_eq!(view_at(&link).snapshot.as_ref(), Some(&body));

        let folder = directory.path().join("folder");
        let folder_link = directory.path().join("folder-link.json");
        fs::create_dir(&folder).unwrap();
        std::os::unix::fs::symlink(&folder, &folder_link).unwrap();
        assert_missing(&view_at(&folder_link), QuotaSnapshotReadReason::Unreadable);
    }

    #[test]
    fn explicit_read_does_not_touch_file_times_of_a_sibling() {
        let directory = crate::test_support::private_tempdir();
        let sibling = directory.path().join("other.json");
        write_raw(&sibling, "untouched");
        let modified = fs::metadata(&sibling).unwrap().modified().unwrap();
        let missing = directory.path().join("missing.json");
        assert_missing(&view_at(&missing), QuotaSnapshotReadReason::FileMissing);
        assert_eq!(fs::read(&sibling).unwrap(), b"untouched");
        assert_eq!(
            fs::metadata(&sibling).unwrap().modified().unwrap(),
            modified
        );
    }
}
