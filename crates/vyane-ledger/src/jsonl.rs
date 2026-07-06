//! Append-only JSONL run ledger.
//!
//! [`JsonlLedger`] stores one [`RunRecord`] per line in a single file. Each
//! append:
//!
//! 1. serializes the record to one line terminated by `\n`,
//! 2. takes an **advisory exclusive file lock** (`fs4`) so concurrent writers —
//!    including other processes — serialize, and
//! 3. writes the whole line with a single `write_all` under that lock.
//!
//! The lock is the cross-process guarantee; opening the file in append mode
//! makes the offset-plus-write atomic at the OS level too, so even a stray
//! writer that bypassed the lock could not interleave a single line. The
//! critical section is deliberately tight: lock → write → unlock.
//!
//! Durability caveat: appends are **not** fsynced. The guarantee is
//! consistency against concurrent readers and writers, not durability across
//! an OS crash or power loss — a record acknowledged inside the crash window
//! may be lost with the page cache. Crash-durable accounting is a deliberate
//! non-goal for the plain-files v0.1 backend.
//!
//! `query` reads the file and walks it **most-recent-first**, skipping any line
//! that fails to parse (counted, never fatal). Reading takes no lock; if a
//! read ever races an in-flight append it will simply treat the still-incomplete
//! trailing line as corrupt and skip it, which is exactly the graceful path the
//! corrupt-line tolerance is designed for.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use fs4::fs_std::FileExt;
use vyane_core::error::{ErrorKind, Result, VyaneError};
use vyane_core::{Ledger, RunQuery, RunRecord};

/// Append-only JSONL [`Ledger`] backed by a single file.
pub struct JsonlLedger {
    path: PathBuf,
    /// Count of non-empty lines that failed to parse during the most recent
    /// [`JsonlLedger::query`]. Reset at the start of every query, so it always
    /// reflects that one call. Tests assert on it; production code may read it
    /// for cheap corruption monitoring.
    skipped_lines: AtomicU64,
}

impl JsonlLedger {
    /// Create a ledger backed at `path`. The file (and parent layout) is
    /// created lazily on the first append, so this never touches the disk.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            skipped_lines: AtomicU64::new(0),
        }
    }

    /// Path of the backing JSONL file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of non-empty lines skipped as corrupt during the most recent
    /// `query` (see the type-level docs).
    #[must_use]
    pub fn skipped_lines(&self) -> u64 {
        self.skipped_lines.load(Ordering::Relaxed)
    }

    /// Synchronous core of [`Ledger::query`]: reverse-scan the file, apply
    /// `query`'s filters, and return the matching records plus the count of
    /// corrupt lines that were skipped. A missing file is an empty ledger, not
    /// an error.
    fn query_blocking(path: &Path, query: RunQuery) -> Result<(Vec<RunRecord>, u64)> {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok((Vec::new(), 0));
            }
            Err(e) => return Err(e.into()),
        };

        // from_utf8_lossy: a record line is UTF-8 JSON; bytes that aren't UTF-8
        // (true corruption) become replacement chars and fail to parse, which
        // is the correct "skip and count" outcome.
        let text = String::from_utf8_lossy(&bytes);

        // Most-recent records are appended last, so walk the file backwards.
        // `limit` caps how many *matching* records we collect, not how many we
        // scan; `None` means return every match.
        let limit = query.limit.unwrap_or(usize::MAX);
        let mut out: Vec<RunRecord> = Vec::new();
        let mut skipped = 0u64;

        for line in text.rsplit('\n') {
            let trimmed = line.trim_end();
            if trimmed.is_empty() {
                // A blank line (e.g. the slot after a trailing newline) is not
                // a corrupt record — just an empty separator. Skip silently.
                continue;
            }
            match serde_json::from_str::<RunRecord>(trimmed) {
                Ok(record) => {
                    if matches_query(&record, &query) {
                        out.push(record);
                        if out.len() >= limit {
                            break;
                        }
                    }
                }
                Err(_) => {
                    // Unparseable non-empty line: skip it, but count it so a
                    // caller can detect corruption. Never panic.
                    skipped += 1;
                }
            }
        }

        Ok((out, skipped))
    }

    /// Synchronous core of [`Ledger::append`]: serialize, lock, write one line.
    fn append_blocking(path: &Path, record: &RunRecord) -> Result<()> {
        // Serialize once, outside the lock, so the critical section stays tight.
        let mut line = serde_json::to_vec(record).map_err(|e| {
            VyaneError::with_source(ErrorKind::Io, "serialize run record for ledger", e)
        })?;
        line.push(b'\n');

        // `append(true)` implies write access and opens with O_APPEND, so every
        // write lands at EOF atomically at the OS level.
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;

        // Advisory exclusive lock serializes concurrent writers across threads
        // and processes. The lock is released when `file` is dropped — at the
        // end of this scope on the Ok path, or by early `?` return on error —
        // so the critical section (lock → write → drop) stays tight.
        file.lock_exclusive()?;

        // Write the entire line in one call so no two writers can interleave a
        // single record's bytes.
        file.write_all(&line).and_then(|()| file.flush())?;
        Ok(())
    }
}

#[async_trait]
impl Ledger for JsonlLedger {
    async fn append(&self, record: &RunRecord) -> Result<()> {
        let path = self.path.clone();
        let record = record.clone();
        // File I/O is blocking; run it off the async executor.
        tokio::task::spawn_blocking(move || Self::append_blocking(&path, &record))
            .await
            .map_err(|join_err| VyaneError::new(ErrorKind::Io, join_err.to_string()))?
    }

    async fn query(&self, query: RunQuery) -> Result<Vec<RunRecord>> {
        let path = self.path.clone();
        let (records, skipped) =
            tokio::task::spawn_blocking(move || Self::query_blocking(&path, query))
                .await
                .map_err(|join_err| VyaneError::new(ErrorKind::Io, join_err.to_string()))??;
        self.skipped_lines.store(skipped, Ordering::Relaxed);
        Ok(records)
    }
}

/// Whether `record` passes every filter set on `query`. Unset fields match
/// anything. `owner` is matched exactly (owner is a scope, not a search term);
/// `since` is compared against `started_at` (inclusive).
fn matches_query(record: &RunRecord, query: &RunQuery) -> bool {
    if let Some(owner) = &query.owner {
        if record.owner != *owner {
            return false;
        }
    }
    if let Some(provider) = &query.provider {
        if record.target.provider != *provider {
            return false;
        }
    }
    if let Some(status) = query.status {
        if record.status != status {
            return false;
        }
    }
    if let Some(since) = query.since {
        if record.started_at < since {
            return false;
        }
    }
    true
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn matches_owner_exact() {
        // Owner is a scope, so matching is exact: case and spelling must agree.
        let rec = sample_record("local", "openai");
        assert!(matches_query(
            &rec,
            &RunQuery {
                owner: Some("local".to_string()),
                ..Default::default()
            }
        ));
        assert!(!matches_query(
            &rec,
            &RunQuery {
                owner: Some("LOCAL".to_string()),
                ..Default::default()
            }
        ));
        assert!(!matches_query(
            &rec,
            &RunQuery {
                owner: Some("other".to_string()),
                ..Default::default()
            }
        ));
    }

    #[test]
    fn matches_provider_and_status() {
        let mut rec = sample_record("local", "openai");
        rec.status = vyane_core::RunStatus::Error;
        assert!(matches_query(
            &rec,
            &RunQuery {
                provider: Some(vyane_core::ProviderId::new("openai")),
                ..Default::default()
            }
        ));
        assert!(!matches_query(
            &rec,
            &RunQuery {
                provider: Some(vyane_core::ProviderId::new("anthropic")),
                ..Default::default()
            }
        ));
        assert!(matches_query(
            &rec,
            &RunQuery {
                status: Some(vyane_core::RunStatus::Error),
                ..Default::default()
            }
        ));
        assert!(!matches_query(
            &rec,
            &RunQuery {
                status: Some(vyane_core::RunStatus::Success),
                ..Default::default()
            }
        ));
    }

    /// Build a minimal record for filter unit tests.
    fn sample_record(owner: &str, provider: &str) -> RunRecord {
        use vyane_core::*;
        RunRecord {
            run_id: "r1".into(),
            owner: owner.into(),
            started_at: chrono::Utc::now(),
            finished_at: chrono::Utc::now(),
            task_digest: "d".into(),
            task_preview: None,
            workdir: None,
            sandbox: Sandbox::ReadOnly,
            target: Target {
                provider: ProviderId::new(provider),
                protocol: Protocol::OpenaiChat,
                harness: None,
                model: ModelId::new("m"),
            },
            transport: AdapterTransport::DirectHttp,
            attempts: vec![],
            status: RunStatus::Success,
            usage: None,
            cost_usd: None,
            session_id: None,
            output_chars: None,
            error: None,
            labels: Default::default(),
        }
    }
}
