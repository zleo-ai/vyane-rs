//! Monthly archives for one JSONL ledger.
//!
//! [`crate::JsonlLedger`] appends to the live file and does not call this
//! module. `vyane history` does not read it either. A caller constructs the
//! archive by invoking [`maybe_rotate`] (or the smaller pieces below) on a
//! path it chooses.
//!
//! When the live file is larger than [`ROTATE_MAX_BYTES`], the older half of
//! its records moves into:
//!
//! ```text
//! <ledger directory>/history-archive/YYYY-MM.jsonl.gz
//! <ledger directory>/history-archive/undated.jsonl.gz
//! ```
//!
//! `YYYY-MM` is the UTC month of the record's `started_at` field (RFC 3339).
//! A line with no usable `started_at`, including a line that is not a JSON
//! object, goes to `undated`. New rotations write gzip only. A plain
//! `YYYY-MM.jsonl` file already in that directory is still read. Each rotation
//! appends one gzip member for the new segment and does not recompress the
//! month.
//!
//! Readers walk committed archive files in month order, then the live file.
//! `history-archive/.pending/` is the crash log for one rotation, not a set of
//! ledger rows. After every monthly gzip matches the hash stored before it was
//! published, `history-archive/.pending/committed.json` records the sha256 and
//! byte length of the live prefix those members came from. A live file that
//! still starts with that prefix drops those bytes on read. A live row that
//! merely has the same text as some archived row is kept.
//!
//! The live file is replaced only after that commit. A later cleanup failure
//! leaves `.pending` in place; the next [`maybe_rotate`] finishes it instead
//! of appending the same segment again.
//!
//! This performs blocking file I/O. It does not take the ledger's append lock.
//! Do not run it at the same time as a writer on the same live file.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Datelike, TimeZone, Utc};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use vyane_core::error::{ErrorKind, Result, VyaneError};

/// Directory name, next to the live ledger, that holds monthly archives.
pub const ARCHIVE_DIR_NAME: &str = "history-archive";

/// Crash-recovery directory inside [`ARCHIVE_DIR_NAME`]. Not ledger data.
pub const PENDING_DIR_NAME: &str = ".pending";

/// Live ledger size, in bytes, above which [`maybe_rotate`] archives the older half.
///
/// This is `10 * 1024 * 1024`, the value of Python `vyane.history._ROTATE_MAX_BYTES`.
pub const ROTATE_MAX_BYTES: u64 = 10 * 1024 * 1024;

const COMMITTED_NAME: &str = "committed.json";
const UNDATED: &str = "undated";
const GZIP_LEVEL: u32 = 6;
const MANIFEST_VERSION: u64 = 1;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// How to stitch archive files and the live ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordRead {
    /// When false, only the live file is read.
    pub include_archives: bool,
    /// Skip month files that end at or before this instant. `undated` is still
    /// read. The live file is still read in full.
    pub since: Option<DateTime<Utc>>,
    /// Drop a committed rotation prefix from the live file. `None` drops it
    /// only when [`Self::include_archives`] is true.
    pub drop_committed_prefix: Option<bool>,
    /// Reject a record that does not end in a newline or is not valid UTF-8.
    pub strict: bool,
}

impl Default for RecordRead {
    fn default() -> Self {
        Self {
            include_archives: true,
            since: None,
            drop_committed_prefix: None,
            strict: false,
        }
    }
}

/// A newest-first read that opens only the archive files it needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecentQuery {
    /// Newest matches to keep. `None` keeps every match. `Some(0)` keeps none
    /// and does not open an archive file.
    pub limit: Option<usize>,
    /// Skip month files that end at or before this instant, and drop objects
    /// whose `started_at` is missing, unusable, or earlier. The live file is
    /// still read.
    pub since: Option<DateTime<Utc>>,
}

/// Lines from [`query_recent`], newest first, plus the archives actually read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentPage {
    /// Matching JSON objects, newest first.
    pub lines: Vec<String>,
    /// Archive files whose bytes were read, in the order they were opened.
    pub opened_archives: Vec<PathBuf>,
}

/// Older half of a live ledger, split by UTC month.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivePlan {
    /// Exact live-file prefix to remove after the segments are committed.
    pub prefix: Vec<u8>,
    /// Segment name (`YYYY-MM` or `undated`) to original record bytes plus `\n`.
    pub segments: BTreeMap<String, Vec<u8>>,
}

/// One rotation whose live prefix may not have been removed yet.
#[derive(Debug)]
pub struct PendingRotation {
    directory: PathBuf,
    prefix_path: PathBuf,
    segments: BTreeMap<String, PathBuf>,
}

impl PendingRotation {
    /// `history-archive/.pending` for this rotation.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Bytes of `prefix.bin`.
    pub fn prefix_bytes(&self) -> Result<Vec<u8>> {
        fs::read(&self.prefix_path).map_err(|err| {
            VyaneError::with_source(
                ErrorKind::Io,
                format!("unreadable archive rotation prefix: {err}"),
                err,
            )
        })
    }
}

/// `<ledger directory>/history-archive`.
#[must_use]
pub fn archive_directory(history_path: &Path) -> PathBuf {
    match history_path.parent() {
        Some(parent) => parent.join(ARCHIVE_DIR_NAME),
        None => PathBuf::from(ARCHIVE_DIR_NAME),
    }
}

/// `<ledger directory>/history-archive/.pending`.
#[must_use]
pub fn pending_directory(history_path: &Path) -> PathBuf {
    archive_directory(history_path).join(PENDING_DIR_NAME)
}

/// Whether any committed archive file is present. `.pending` does not count.
pub fn has_archives(history_path: &Path) -> Result<bool> {
    Ok(!iter_archive_files(history_path, None)?.is_empty())
}

/// Archive the older half when `history_path` is larger than [`ROTATE_MAX_BYTES`].
///
/// A size at or below the cap leaves the file untouched. A `.pending` directory
/// from an earlier attempt is finished first, even when the live file is now
/// small. [`crate::JsonlLedger::append`] does not call this.
pub fn maybe_rotate(history_path: &Path) -> Result<()> {
    maybe_rotate_with_cap(history_path, ROTATE_MAX_BYTES)
}

/// [`maybe_rotate`] with an explicit byte cap. The cap is exclusive: a file of
/// `max_bytes` is left untouched, and a larger file can rotate.
pub fn maybe_rotate_with_cap(history_path: &Path, max_bytes: u64) -> Result<()> {
    if let Some(pending) = load_complete_pending(history_path)? {
        return finish_pending(history_path, &pending);
    }
    discard_incomplete_pending(history_path)?;
    let metadata = match fs::metadata(history_path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(VyaneError::with_source(
                ErrorKind::Io,
                format!("stat {}", history_path.display()),
                err,
            ));
        }
    };
    if metadata.len() <= max_bytes {
        return Ok(());
    }
    let raw = fs::read(history_path)?;
    let Some(plan) = plan_archive(&raw) else {
        return Ok(());
    };
    let pending = create_pending(history_path, &plan.prefix, &plan.segments)?;
    finish_pending(history_path, &pending)
}

fn finish_pending(history_path: &Path, pending: &PendingRotation) -> Result<()> {
    let mode = file_mode(history_path).unwrap_or(0o644);
    commit_pending(pending, mode)?;
    install_kept_suffix(history_path, pending)?;
    clear_pending(pending)?;
    Ok(())
}

/// UTC `YYYY-MM` for a ledger line, or `undated` when `started_at` is unusable.
///
/// `started_at` must be an RFC 3339 timestamp. A numeric timestamp is not a
/// month. The month is the UTC year and month, so an offset can land in the
/// previous or next calendar month.
#[must_use]
pub fn month_of_record(text: &str) -> String {
    let Ok(Value::Object(object)) = serde_json::from_str::<Value>(text) else {
        return UNDATED.to_string();
    };
    let Some(started) = object.get("started_at").and_then(Value::as_str) else {
        return UNDATED.to_string();
    };
    let Ok(parsed) = DateTime::parse_from_rfc3339(started) else {
        return UNDATED.to_string();
    };
    let utc = parsed.with_timezone(&Utc);
    let year = utc.year();
    if !(1970..=9999).contains(&year) {
        return UNDATED.to_string();
    }
    format!("{year:04}-{:02}", utc.month())
}

/// Split the older half of `raw` into monthly segments.
///
/// Returns `None` when there are fewer than two records. Records are split on
/// `\n` only. The prefix is the exact byte range to remove from the live file,
/// and each segment is the original record bytes plus `\n`.
#[must_use]
pub fn plan_archive(raw: &[u8]) -> Option<ArchivePlan> {
    if raw.is_empty() {
        return None;
    }
    let parts: Vec<&[u8]> = raw.split(|byte| *byte == b'\n').collect();
    let indexes: Vec<usize> = parts
        .iter()
        .enumerate()
        .filter_map(|(index, part)| {
            let text = String::from_utf8_lossy(part);
            if text.trim().is_empty() {
                None
            } else {
                Some(index)
            }
        })
        .collect();
    let half = indexes.len() / 2;
    if half == 0 {
        return None;
    }
    let last = indexes[half - 1];
    let mut prefix = join_newline(&parts[..=last]);
    if last + 1 < parts.len() || raw.ends_with(b"\n") {
        prefix.push(b'\n');
    }
    if !raw.starts_with(&prefix) || prefix.len() >= raw.len() {
        return None;
    }
    let mut segments: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for &index in &indexes[..half] {
        let part = parts[index];
        let text = String::from_utf8_lossy(part);
        let month = month_of_record(text.trim());
        let bucket = segments.entry(month).or_default();
        bucket.extend_from_slice(part);
        bucket.push(b'\n');
    }
    Some(ArchivePlan { prefix, segments })
}

/// Ledger lines, archives in month order, then the live file.
///
/// Blank lines are omitted. A committed prefix is removed from the live bytes
/// only when those bytes still start with it.
pub fn read_record_lines(history_path: &Path, options: &RecordRead) -> Result<Vec<String>> {
    let drop_prefix = options
        .drop_committed_prefix
        .unwrap_or(options.include_archives);
    if !options.include_archives {
        return read_live_lines(history_path, drop_prefix, options.strict);
    }
    let prefix = if drop_prefix {
        committed_prefix_bytes(history_path)
    } else {
        Vec::new()
    };
    let mut live_raw = read_optional(history_path)?;
    if !prefix.is_empty() && live_raw.starts_with(&prefix) {
        live_raw = live_raw.split_off(prefix.len());
    }
    let mut lines = Vec::new();
    for path in iter_archive_files(history_path, options.since)? {
        lines.extend(read_path_lines(&path, options.strict)?);
    }
    lines.extend(lines_from_bytes(&live_raw, options.strict, true)?);
    Ok(lines)
}

/// Newest matching JSON objects, opening older month files only when needed.
///
/// The live file is read first. When it already holds `limit` matches, no
/// archive file is opened. Otherwise archive files are walked newest-first
/// and reading stops once `limit` matches are in hand. Month files that end
/// at or before `since` are not opened. `undated` is still a candidate.
pub fn query_recent(history_path: &Path, query: &RecentQuery) -> Result<RecentPage> {
    if query.limit == Some(0) {
        return Ok(RecentPage {
            lines: Vec::new(),
            opened_archives: Vec::new(),
        });
    }
    let live = matching_lines(read_live_lines(history_path, false, false)?, query.since);
    let live_enough = query
        .limit
        .is_some_and(|limit| limit > 0 && live.len() >= limit);
    if live_enough || !has_archives(history_path)? {
        return Ok(RecentPage {
            lines: newest_limited(live, query.limit),
            opened_archives: Vec::new(),
        });
    }

    let live = matching_lines(read_live_lines(history_path, true, false)?, query.since);
    let mut matched = live.len();
    let mut groups = Vec::new();
    let mut opened = Vec::new();
    for archive in iter_archive_files(history_path, query.since)?
        .into_iter()
        .rev()
    {
        if query.limit.is_some_and(|limit| matched >= limit) {
            break;
        }
        let lines = read_path_lines(&archive, false)?;
        let hits = matching_lines(lines, query.since);
        matched += hits.len();
        opened.push(archive);
        groups.push(hits);
    }
    groups.reverse();
    let mut oldest = Vec::new();
    for group in groups {
        oldest.extend(group);
    }
    oldest.extend(live);
    Ok(RecentPage {
        lines: newest_limited(oldest, query.limit),
        opened_archives: opened,
    })
}

/// Committed archive files in read order. `.pending` is not included.
pub fn iter_archive_files(
    history_path: &Path,
    since: Option<DateTime<Utc>>,
) -> Result<Vec<PathBuf>> {
    let directory = archive_directory(history_path);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(VyaneError::with_source(
                ErrorKind::Io,
                format!("read {}", directory.display()),
                err,
            ));
        }
    };
    let mut found = Vec::new();
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if name.starts_with('.') {
            continue;
        }
        let Some(stem) = archive_stem(name) else {
            continue;
        };
        if since.is_some_and(|since| month_ends_before(stem, since)) {
            continue;
        }
        let path = entry.path();
        match fs::metadata(&path) {
            Ok(meta) if meta.is_file() => found.push(path),
            Ok(_) => continue,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(VyaneError::with_source(
                    ErrorKind::Io,
                    format!("stat {}", path.display()),
                    err,
                ));
            }
        }
    }
    found.sort_by_key(|path| archive_sort_key(path));
    Ok(found)
}

/// Load a pending rotation whose manifest is complete and matches its bytes.
///
/// `Ok(None)` means there is nothing to resume. A complete manifest whose
/// bytes do not match is an error: the directory is left in place.
pub fn load_complete_pending(history_path: &Path) -> Result<Option<PendingRotation>> {
    let directory = pending_directory(history_path);
    let manifest_path = directory.join("manifest.json");
    if !manifest_path.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(&manifest_path).map_err(|err| {
        VyaneError::with_source(
            ErrorKind::Io,
            format!("unreadable archive rotation manifest: {err}"),
            err,
        )
    })?;
    let manifest_value: Value = serde_json::from_str(&text).map_err(|err| {
        VyaneError::with_source(
            ErrorKind::Io,
            format!("unreadable archive rotation manifest: {err}"),
            err,
        )
    })?;
    let Value::Object(manifest) = manifest_value else {
        return Ok(None);
    };
    if manifest.get("complete") != Some(&Value::Bool(true)) {
        return Ok(None);
    }
    validate_complete_manifest(&directory, &manifest).map(Some)
}

/// Remove a pending directory that never finished its manifest.
///
/// A manifest with `complete: true` is left alone, even when a later hash
/// check failed.
pub fn discard_incomplete_pending(history_path: &Path) -> Result<()> {
    let directory = pending_directory(history_path);
    if !directory.is_dir() {
        return Ok(());
    }
    let manifest_path = directory.join("manifest.json");
    if manifest_path.is_file()
        && let Ok(text) = fs::read_to_string(&manifest_path)
        && let Ok(Value::Object(manifest)) = serde_json::from_str::<Value>(&text)
        && manifest.get("complete") == Some(&Value::Bool(true))
    {
        return Ok(());
    }
    remove_directory(&directory)
}

/// Persist a rotation prefix. The live file is not modified.
pub fn create_pending(
    history_path: &Path,
    prefix: &[u8],
    segments: &BTreeMap<String, Vec<u8>>,
) -> Result<PendingRotation> {
    if prefix.is_empty() || segments.is_empty() {
        return Err(VyaneError::new(
            ErrorKind::Io,
            "archive rotation has nothing to move",
        ));
    }
    for (name, payload) in segments {
        if !segment_name_ok(name) || payload.is_empty() {
            return Err(VyaneError::new(
                ErrorKind::Io,
                format!("refusing archive segment {name:?}"),
            ));
        }
    }
    let directory = pending_directory(history_path);
    if directory.exists() {
        return Err(VyaneError::new(
            ErrorKind::Io,
            "archive rotation pending directory already exists",
        ));
    }
    let archive = archive_directory(history_path);
    fs::create_dir_all(&archive)?;
    fs::create_dir(&directory)?;
    sync_directory(&archive)?;

    let prefix_path = directory.join("prefix.bin");
    atomic_replace(&prefix_path, prefix, None, "tmp")?;
    let mut written = BTreeMap::new();
    let mut segment_meta = Map::new();
    for (name, payload) in segments {
        let path = directory.join(format!("{name}.jsonl"));
        atomic_replace(&path, payload, None, "tmp")?;
        written.insert(name.clone(), path);
        segment_meta.insert(
            name.clone(),
            json!({
                "nbytes": len_u64(payload)?,
                "sha256": sha256_hex(payload),
            }),
        );
    }
    let manifest = json!({
        "complete": true,
        "prefix_nbytes": len_u64(prefix)?,
        "prefix_sha256": sha256_hex(prefix),
        "segments": segment_meta,
        "version": MANIFEST_VERSION,
    });
    atomic_replace(
        &directory.join("manifest.json"),
        &python_bytes(&manifest)?,
        None,
        "tmp",
    )?;
    sync_directory(&directory)?;
    Ok(PendingRotation {
        directory,
        prefix_path,
        segments: written,
    })
}

/// Merge pending segments into monthly gzip files.
///
/// `mode` is the unix permission bits copied onto each gzip file. It is
/// ignored on non-unix targets. The live file is not modified. A retry whose
/// gzip already matches the recorded hash does not append the segment again.
pub fn commit_pending(pending: &PendingRotation, mode: u32) -> Result<()> {
    if pending.segments.is_empty() {
        return Err(VyaneError::new(
            ErrorKind::Io,
            "archive rotation manifest has no segments",
        ));
    }
    let archive_dir = pending.directory.parent().ok_or_else(|| {
        VyaneError::new(
            ErrorKind::Io,
            "archive rotation pending directory has no parent",
        )
    })?;
    for (name, path) in &pending.segments {
        if !segment_name_ok(name) {
            return Err(VyaneError::new(
                ErrorKind::Io,
                format!("refusing archive segment {name:?}"),
            ));
        }
        let segment = fs::read(path).map_err(|err| {
            VyaneError::with_source(
                ErrorKind::Io,
                format!("unreadable archive segment {name}: {err}"),
                err,
            )
        })?;
        merge_segment(
            &archive_dir.join(format!("{name}.jsonl.gz")),
            &segment,
            &pending.directory.join(format!("{name}.intent.json")),
            mode,
        )?;
    }
    write_committed_marker(pending)
}

/// Drop the archived prefix from the live file.
///
/// `Ok` means the live file no longer starts with that prefix, so the pending
/// directory may be removed. A prefix that sits in the middle of the live file
/// is an error and the file is left unchanged.
pub fn install_kept_suffix(history_path: &Path, pending: &PendingRotation) -> Result<()> {
    let prefix = pending.prefix_bytes()?;
    let raw = fs::read(history_path).map_err(|err| {
        VyaneError::with_source(
            ErrorKind::Io,
            "history rotation failed; history left as is",
            err,
        )
    })?;
    let real = fs::canonicalize(history_path).map_err(|err| {
        VyaneError::with_source(
            ErrorKind::Io,
            "history rotation failed; history left as is",
            err,
        )
    })?;
    if raw.starts_with(&prefix) {
        let mode = file_mode(&real);
        atomic_replace(&real, &raw[prefix.len()..], mode, "rotate")?;
        return Ok(());
    }
    if !contains_slice(&raw, &prefix) {
        if let Some(parent) = non_empty_parent(&real) {
            sync_directory(parent)?;
        }
        return Ok(());
    }
    Err(VyaneError::new(
        ErrorKind::Io,
        "history rotation stopped: archived prefix is not the live file head",
    ))
}

/// Remove a pending directory after the live file no longer has its prefix.
///
/// An unexpected subdirectory aborts before any entry is removed, so a retry
/// still sees the manifest.
pub fn clear_pending(pending: &PendingRotation) -> Result<()> {
    remove_directory(&pending.directory)
}

fn validate_complete_manifest(
    directory: &Path,
    manifest: &Map<String, Value>,
) -> Result<PendingRotation> {
    if manifest.get("version").and_then(Value::as_u64) != Some(MANIFEST_VERSION) {
        return Err(VyaneError::new(
            ErrorKind::Io,
            "unsupported archive rotation manifest",
        ));
    }
    let prefix_path = directory.join("prefix.bin");
    let prefix = fs::read(&prefix_path).map_err(|err| {
        VyaneError::with_source(
            ErrorKind::Io,
            format!("unreadable archive rotation prefix: {err}"),
            err,
        )
    })?;
    let prefix_len = len_u64(&prefix)?;
    let prefix_sha = manifest.get("prefix_sha256").and_then(Value::as_str);
    let prefix_nbytes = manifest.get("prefix_nbytes").and_then(Value::as_u64);
    if prefix_sha != Some(sha256_hex(&prefix).as_str()) || prefix_nbytes != Some(prefix_len) {
        return Err(VyaneError::new(
            ErrorKind::Io,
            "archive rotation prefix does not match its manifest",
        ));
    }
    let Some(meta) = manifest.get("segments").and_then(Value::as_object) else {
        return Err(VyaneError::new(
            ErrorKind::Io,
            "archive rotation manifest has no segments",
        ));
    };
    if meta.is_empty() {
        return Err(VyaneError::new(
            ErrorKind::Io,
            "archive rotation manifest has no segments",
        ));
    }
    let mut segments: BTreeMap<String, PathBuf> = BTreeMap::new();
    for (name, info) in meta {
        if !segment_name_ok(name) {
            return Err(VyaneError::new(
                ErrorKind::Io,
                "archive rotation segment name is not a month",
            ));
        }
        let Some(info) = info.as_object() else {
            return Err(VyaneError::new(
                ErrorKind::Io,
                "archive rotation segment manifest is invalid",
            ));
        };
        let path = directory.join(format!("{name}.jsonl"));
        let payload = fs::read(&path).map_err(|err| {
            VyaneError::with_source(
                ErrorKind::Io,
                format!("unreadable archive segment {name}: {err}"),
                err,
            )
        })?;
        let sha = info.get("sha256").and_then(Value::as_str);
        let nbytes = info.get("nbytes").and_then(Value::as_u64);
        if sha != Some(sha256_hex(&payload).as_str()) || nbytes != Some(len_u64(&payload)?) {
            return Err(VyaneError::new(
                ErrorKind::Io,
                format!("archive segment {name} does not match its manifest"),
            ));
        }
        segments.insert(name.clone(), path);
    }
    Ok(PendingRotation {
        directory: directory.to_path_buf(),
        prefix_path,
        segments,
    })
}

fn merge_segment(committed: &Path, segment: &[u8], intent_path: &Path, mode: u32) -> Result<()> {
    let segment_sha = sha256_hex(segment);
    if intent_path.is_file() {
        return merge_with_intent(committed, segment, intent_path, mode, &segment_sha);
    }
    let current_raw = read_optional(committed)?;
    let pre = sha256_hex(&current_raw);
    let member = gzip_member(segment)?;
    let mut new_raw = current_raw;
    new_raw.extend_from_slice(&member);
    let post = sha256_hex(&new_raw);
    let intent = json!({
        "post": &post,
        "pre": pre,
        "segment_sha256": segment_sha,
        "version": MANIFEST_VERSION,
    });
    atomic_replace(intent_path, &python_bytes(&intent)?, None, "tmp")?;
    let current = sha256_file(committed)?;
    if current == post {
        return Ok(());
    }
    if current != pre {
        return Err(VyaneError::new(
            ErrorKind::Io,
            format!("archive {} changed during rotation", file_label(committed)),
        ));
    }
    atomic_replace(committed, &new_raw, Some(mode), "tmp")
}

fn merge_with_intent(
    committed: &Path,
    segment: &[u8],
    intent_path: &Path,
    mode: u32,
    segment_sha: &str,
) -> Result<()> {
    let text = fs::read_to_string(intent_path).map_err(|err| {
        VyaneError::with_source(
            ErrorKind::Io,
            format!("unreadable archive intent for {}", file_label(committed)),
            err,
        )
    })?;
    let intent: Value = serde_json::from_str(&text).map_err(|err| {
        VyaneError::with_source(
            ErrorKind::Io,
            format!("unreadable archive intent for {}", file_label(committed)),
            err,
        )
    })?;
    let Some(intent) = intent.as_object() else {
        return Err(VyaneError::new(
            ErrorKind::Io,
            format!("archive intent does not match {}", file_label(committed)),
        ));
    };
    if intent.get("segment_sha256").and_then(Value::as_str) != Some(segment_sha) {
        return Err(VyaneError::new(
            ErrorKind::Io,
            format!("archive intent does not match {}", file_label(committed)),
        ));
    }
    let Some(pre) = intent.get("pre").and_then(Value::as_str) else {
        return Err(VyaneError::new(
            ErrorKind::Io,
            format!("archive intent for {} is incomplete", file_label(committed)),
        ));
    };
    let Some(post) = intent.get("post").and_then(Value::as_str) else {
        return Err(VyaneError::new(
            ErrorKind::Io,
            format!("archive intent for {} is incomplete", file_label(committed)),
        ));
    };
    let current = sha256_file(committed)?;
    if current == post {
        return Ok(());
    }
    if current != pre {
        return Err(VyaneError::new(
            ErrorKind::Io,
            format!("archive {} changed during rotation", file_label(committed)),
        ));
    }
    let new_raw = append_gzip_member(committed, segment)?;
    if sha256_hex(&new_raw) != post {
        return Err(VyaneError::new(
            ErrorKind::Io,
            format!(
                "archive {} rebuild does not match intent",
                file_label(committed)
            ),
        ));
    }
    atomic_replace(committed, &new_raw, Some(mode), "tmp")
}

fn write_committed_marker(pending: &PendingRotation) -> Result<()> {
    let prefix = pending.prefix_bytes()?;
    let raw = python_bytes(&json!({
        "prefix_nbytes": len_u64(&prefix)?,
        "prefix_sha256": sha256_hex(&prefix),
        "version": MANIFEST_VERSION,
    }))?;
    let path = pending.directory.join(COMMITTED_NAME);
    if path.is_file() {
        let current = fs::read(&path).map_err(|err| {
            VyaneError::with_source(ErrorKind::Io, "unreadable archive committed marker", err)
        })?;
        if current == raw {
            return Ok(());
        }
        return Err(VyaneError::new(
            ErrorKind::Io,
            "archive committed marker does not match this rotation",
        ));
    }
    atomic_replace(&path, &raw, None, "tmp")
}

fn committed_prefix_bytes(history_path: &Path) -> Vec<u8> {
    let directory = pending_directory(history_path);
    let marker_path = directory.join(COMMITTED_NAME);
    if !marker_path.is_file() {
        return Vec::new();
    }
    let Ok(text) = fs::read_to_string(&marker_path) else {
        return Vec::new();
    };
    let Ok(Value::Object(marker)) = serde_json::from_str::<Value>(&text) else {
        return Vec::new();
    };
    if marker.get("version").and_then(Value::as_u64) != Some(MANIFEST_VERSION) {
        return Vec::new();
    }
    let Some(nbytes) = marker.get("prefix_nbytes").and_then(Value::as_u64) else {
        return Vec::new();
    };
    let Some(digest) = marker.get("prefix_sha256").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Ok(prefix) = fs::read(directory.join("prefix.bin")) else {
        return Vec::new();
    };
    let Ok(len) = u64::try_from(prefix.len()) else {
        return Vec::new();
    };
    if len != nbytes || prefix.is_empty() || sha256_hex(&prefix) != digest {
        return Vec::new();
    }
    prefix
}

fn read_live_lines(history_path: &Path, drop_prefix: bool, strict: bool) -> Result<Vec<String>> {
    let prefix = if drop_prefix {
        committed_prefix_bytes(history_path)
    } else {
        Vec::new()
    };
    let mut live_raw = read_optional(history_path)?;
    if !prefix.is_empty() && live_raw.starts_with(&prefix) {
        live_raw = live_raw.split_off(prefix.len());
    }
    lines_from_bytes(&live_raw, strict, true)
}

fn read_path_lines(path: &Path, strict: bool) -> Result<Vec<String>> {
    lines_from_bytes(&read_archive_bytes(path)?, strict, false)
}

fn read_archive_bytes(path: &Path) -> Result<Vec<u8>> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    if name.ends_with(".gz") {
        let file = File::open(path)?;
        let mut decoder = flate2::read::MultiGzDecoder::new(file);
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).map_err(|err| {
            VyaneError::with_source(ErrorKind::Io, format!("read {}", path.display()), err)
        })?;
        Ok(out)
    } else {
        fs::read(path).map_err(|err| {
            VyaneError::with_source(ErrorKind::Io, format!("read {}", path.display()), err)
        })
    }
}

fn lines_from_bytes(raw: &[u8], strict: bool, live: bool) -> Result<Vec<String>> {
    if strict && !raw.is_empty() && !raw.ends_with(b"\n") {
        let message = if live {
            "incomplete live history record"
        } else {
            "incomplete archived history record"
        };
        return Err(VyaneError::new(ErrorKind::Io, message));
    }
    let mut lines = Vec::new();
    for part in raw.split(|byte| *byte == b'\n') {
        let text = decode_part(part, strict)?;
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            lines.push(trimmed.to_string());
        }
    }
    Ok(lines)
}

fn decode_part(part: &[u8], strict: bool) -> Result<String> {
    if strict {
        std::str::from_utf8(part)
            .map(str::to_string)
            .map_err(|_| VyaneError::new(ErrorKind::Io, "invalid utf-8 in history record"))
    } else {
        Ok(String::from_utf8_lossy(part).into_owned())
    }
}

fn matching_lines(lines: Vec<String>, since: Option<DateTime<Utc>>) -> Vec<String> {
    lines
        .into_iter()
        .filter(|line| line_matches(line, since))
        .collect()
}

fn line_matches(line: &str, since: Option<DateTime<Utc>>) -> bool {
    let Ok(Value::Object(object)) = serde_json::from_str::<Value>(line) else {
        return false;
    };
    let Some(since) = since else {
        return true;
    };
    let Some(text) = object.get("started_at").and_then(Value::as_str) else {
        return false;
    };
    let Ok(parsed) = DateTime::parse_from_rfc3339(text) else {
        return false;
    };
    parsed.with_timezone(&Utc) >= since
}

fn newest_limited(oldest_first: Vec<String>, limit: Option<usize>) -> Vec<String> {
    let mut newest: Vec<String> = oldest_first.into_iter().rev().collect();
    if let Some(limit) = limit {
        newest.truncate(limit);
    }
    newest
}

fn month_ends_before(stem: &str, since: DateTime<Utc>) -> bool {
    let Some((year, month)) = year_month(stem) else {
        return false;
    };
    if !(1..=12).contains(&month) {
        return false;
    }
    let (next_year, next_month) = if month == 12 {
        let Some(next_year) = year.checked_add(1) else {
            return false;
        };
        (next_year, 1)
    } else {
        (year, month + 1)
    };
    let Some(boundary) = Utc
        .with_ymd_and_hms(next_year, next_month, 1, 0, 0, 0)
        .single()
    else {
        return false;
    };
    boundary <= since
}

fn year_month(stem: &str) -> Option<(i32, u32)> {
    let (year, month) = stem.split_once('-')?;
    if year.len() != 4 || month.len() != 2 {
        return None;
    }
    Some((year.parse().ok()?, month.parse().ok()?))
}

fn archive_stem(name: &str) -> Option<&str> {
    let stem = name
        .strip_suffix(".jsonl.gz")
        .or_else(|| name.strip_suffix(".jsonl"))?;
    segment_name_ok(stem).then_some(stem)
}

fn archive_sort_key(path: &Path) -> (String, u8) {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    let gzipped = u8::from(name.ends_with(".gz"));
    let stem = archive_stem(name).unwrap_or(name).to_string();
    (stem, gzipped)
}

fn segment_name_ok(name: &str) -> bool {
    if name == UNDATED {
        return true;
    }
    let Some((year, month)) = name.split_once('-') else {
        return false;
    };
    year.len() == 4
        && month.len() == 2
        && !month.contains('-')
        && year.bytes().all(|byte| byte.is_ascii_digit())
        && month.bytes().all(|byte| byte.is_ascii_digit())
}

fn append_gzip_member(committed: &Path, segment: &[u8]) -> Result<Vec<u8>> {
    let mut raw = read_optional(committed)?;
    raw.extend_from_slice(&gzip_member(segment)?);
    Ok(raw)
}

fn gzip_member(segment: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = flate2::GzBuilder::new()
        .mtime(0)
        .operating_system(255)
        .write(Vec::new(), flate2::Compression::new(GZIP_LEVEL));
    encoder
        .write_all(segment)
        .map_err(|err| VyaneError::with_source(ErrorKind::Io, "compress archive segment", err))?;
    encoder
        .finish()
        .map_err(|err| VyaneError::with_source(ErrorKind::Io, "compress archive segment", err))
}

fn atomic_replace(path: &Path, data: &[u8], mode: Option<u32>, suffix: &str) -> Result<()> {
    if let Some(parent) = non_empty_parent(path) {
        fs::create_dir_all(parent)?;
    }
    let temporary = temp_sibling(path, suffix);
    let result: Result<()> = (|| {
        write_temp(&temporary, data, mode)?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if temporary.exists() {
        let _ = fs::remove_file(&temporary);
    }
    result?;
    if let Some(parent) = non_empty_parent(path) {
        sync_directory(parent)?;
    }
    Ok(())
}

fn write_temp(path: &Path, data: &[u8], mode: Option<u32>) -> Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(data)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    if let Some(mode) = mode {
        set_mode(path, mode);
    }
    Ok(())
}

fn temp_sibling(path: &Path, suffix: &str) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("ledger");
    let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let file_name = format!(".{name}.{}.{n}.{suffix}", std::process::id());
    match non_empty_parent(path) {
        Some(parent) => parent.join(file_name),
        None => PathBuf::from(file_name),
    }
}

fn remove_directory(directory: &Path) -> Result<()> {
    if !directory.is_dir() {
        return Ok(());
    }
    let mut children = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let meta = fs::symlink_metadata(&path)?;
        if meta.is_dir() {
            return Err(VyaneError::new(
                ErrorKind::Io,
                format!(
                    "unexpected directory in archive rotation: {}",
                    entry.file_name().to_string_lossy()
                ),
            ));
        }
        children.push(path);
    }
    for path in children {
        fs::remove_file(path)?;
    }
    fs::remove_dir(directory)?;
    if let Some(parent) = non_empty_parent(directory) {
        sync_directory(parent)?;
    }
    Ok(())
}

fn read_optional(path: &Path) -> Result<Vec<u8>> {
    match fs::read(path) {
        Ok(bytes) => Ok(bytes),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(VyaneError::with_source(
            ErrorKind::Io,
            format!("read {}", path.display()),
            err,
        )),
    }
}

fn sha256_file(path: &Path) -> Result<String> {
    match File::open(path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(sha256_hex(b"")),
        Err(err) => Err(VyaneError::with_source(
            ErrorKind::Io,
            format!("read {}", path.display()),
            err,
        )),
        Ok(mut file) => {
            let mut hasher = Sha256::new();
            let mut buf = [0u8; 1024 * 1024];
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(hex_encode(&hasher.finalize()))
        }
    }
}

fn sha256_hex(data: &[u8]) -> String {
    hex_encode(&Sha256::digest(data))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn len_u64(bytes: &[u8]) -> Result<u64> {
    u64::try_from(bytes.len())
        .map_err(|_| VyaneError::new(ErrorKind::Io, "archive byte length does not fit"))
}

fn python_bytes(value: &Value) -> Result<Vec<u8>> {
    let mut out = String::new();
    write_python(&mut out, value)?;
    out.push('\n');
    Ok(out.into_bytes())
}

fn write_python(out: &mut String, value: &Value) -> Result<()> {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                out.push_str(&value.to_string());
            } else if let Some(value) = number.as_u64() {
                out.push_str(&value.to_string());
            } else {
                return Err(VyaneError::new(
                    ErrorKind::Io,
                    "archive manifest number is not an integer",
                ));
            }
        }
        Value::String(text) => {
            out.push_str(&serde_json::to_string(text).map_err(|err| {
                VyaneError::with_source(ErrorKind::Io, "encode archive manifest string", err)
            })?);
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                write_python(out, item)?;
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                out.push_str(&serde_json::to_string(key).map_err(|err| {
                    VyaneError::with_source(ErrorKind::Io, "encode archive manifest key", err)
                })?);
                out.push_str(": ");
                write_python(out, &map[*key])?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn join_newline(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            out.push(b'\n');
        }
        out.extend_from_slice(part);
    }
    out
}

fn contains_slice(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }
    let mut longest = vec![0usize; needle.len()];
    let mut length = 0usize;
    let mut index = 1usize;
    while index < needle.len() {
        if needle[index] == needle[length] {
            length += 1;
            longest[index] = length;
            index += 1;
        } else if length == 0 {
            longest[index] = 0;
            index += 1;
        } else {
            length = longest[length - 1];
        }
    }
    let mut hay = 0usize;
    let mut ned = 0usize;
    while hay < haystack.len() {
        if haystack[hay] == needle[ned] {
            hay += 1;
            ned += 1;
            if ned == needle.len() {
                return true;
            }
        } else if ned == 0 {
            hay += 1;
        } else {
            ned = longest[ned - 1];
        }
    }
    false
}

fn file_label(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("archive")
        .to_string()
}

fn non_empty_parent(path: &Path) -> Option<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
}

#[cfg(unix)]
fn file_mode(path: &Path) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    fs::metadata(path)
        .ok()
        .map(|meta| meta.permissions().mode() & 0o7777)
}

#[cfg(not(unix))]
fn file_mode(_path: &Path) -> Option<u32> {
    None
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) {}

#[cfg(unix)]
fn sync_directory(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn rotate_cap_matches_python_history() {
        assert_eq!(ROTATE_MAX_BYTES, 10 * 1024 * 1024);
    }

    #[test]
    fn month_uses_started_at_rfc3339_in_utc() {
        assert_eq!(
            month_of_record(r#"{"started_at":"2026-09-15T12:00:00Z"}"#),
            "2026-09"
        );
        assert_eq!(
            month_of_record(r#"{"started_at":"2026-10-01T00:30:00+02:00"}"#),
            "2026-09"
        );
        assert_eq!(
            month_of_record(r#"{"started_at":"1969-12-31T23:59:59Z"}"#),
            "undated"
        );
        assert_eq!(month_of_record(r#"{"ts":1757937600}"#), "undated");
        assert_eq!(month_of_record(r#"{"started_at":1757937600}"#), "undated");
        assert_eq!(month_of_record("not-json"), "undated");
        assert_eq!(month_of_record("[]"), "undated");
        assert_eq!(month_of_record(r#"{"started_at":true}"#), "undated");
    }

    #[test]
    fn plan_archive_keeps_newline_bytes_and_line_separators() {
        let raw = b"a\nb\nc\nd\n";
        let plan = plan_archive(raw).unwrap();
        assert_eq!(plan.prefix, b"a\nb\n");
        assert_eq!(plan.segments["undated"], b"a\nb\n");
        assert!(raw.starts_with(&plan.prefix));
        assert!(plan_archive(b"only-one\n").is_none());
        assert!(plan_archive(b"").is_none());

        let raw = "a\u{2028}b\nc\n".as_bytes();
        let plan = plan_archive(raw).unwrap();
        assert_eq!(plan.prefix, "a\u{2028}b\n".as_bytes());
    }

    #[test]
    fn manifest_json_matches_python_dumps() {
        let value = json!({
            "version": 1,
            "complete": true,
            "prefix_sha256": "abc",
            "prefix_nbytes": 2,
            "segments": {
                "undated": {"sha256": "aaa", "nbytes": 1},
                "2026-10": {"sha256": "def", "nbytes": 3}
            }
        });
        let bytes = python_bytes(&value).unwrap();
        assert_eq!(
            bytes,
            b"{\"complete\": true, \"prefix_nbytes\": 2, \"prefix_sha256\": \"abc\", \"segments\": {\"2026-10\": {\"nbytes\": 3, \"sha256\": \"def\"}, \"undated\": {\"nbytes\": 1, \"sha256\": \"aaa\"}}, \"version\": 1}\n"
        );
    }

    #[test]
    fn gzip_members_are_deterministic_and_concatenate() {
        let first = gzip_member(b"a\n").unwrap();
        let second = gzip_member(b"a\n").unwrap();
        assert_eq!(first, second);
        assert!(first.starts_with(&[0x1f, 0x8b]));
        let mut both = gzip_member(b"a\n").unwrap();
        both.extend(gzip_member(b"b\n").unwrap());
        let mut decoder = flate2::read::MultiGzDecoder::new(both.as_slice());
        let mut text = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut text).unwrap();
        assert_eq!(text, "a\nb\n");
    }

    #[test]
    fn month_skip_uses_the_next_utc_month() {
        let cutoff = DateTime::parse_from_rfc3339("2026-09-30T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(month_ends_before("2020-01", cutoff));
        assert!(!month_ends_before("2026-09", cutoff));
        assert!(!month_ends_before("undated", cutoff));
        let october = DateTime::parse_from_rfc3339("2026-10-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(month_ends_before("2026-09", october));
        assert!(!month_ends_before("2026-10", october));
    }

    #[test]
    fn slice_search_finds_only_real_spans() {
        assert!(contains_slice(b"abcdef", b"cd"));
        assert!(contains_slice(b"abcdef", b"ab"));
        assert!(!contains_slice(b"abcdef", b"ce"));
        assert!(!contains_slice(b"ab", b"abcdef"));
        assert!(contains_slice(b"aaa", b"aa"));
    }
}
