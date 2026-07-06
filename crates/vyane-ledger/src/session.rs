//! Filesystem-backed [`SessionStore`].
//!
//! [`FsSessionStore`] keeps **one JSON file per session** inside a directory.
//! Writes go to a uniquely-named temp file **beside** the target and are
//! published with an atomic `rename`, so a reader can only ever observe the
//! previous complete file or the new complete file — never a half-written one.
//! (`rename` is atomic when source and destination share a filesystem, which is
//! guaranteed by keeping the temp file in the same directory.)
//!
//! Durability caveat: neither the temp file nor the directory is fsynced
//! before the rename, so the guarantee is reader consistency, not durability
//! across an OS crash — the same trade-off as the JSONL ledger, and a
//! deliberate non-goal for the plain-files v0.1 backend.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use vyane_core::error::{ErrorKind, Result, VyaneError};
use vyane_core::{SessionRecord, SessionStore};

/// Monotonic counter that makes each temp file name unique, even when several
/// saves of the same session race within one process. Combined with the process
/// id it is unique across processes too.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// One JSON file per session, in a single directory.
pub struct FsSessionStore {
    dir: PathBuf,
}

impl FsSessionStore {
    /// Sessions are stored as `<dir>/<safe_id>.json`. The directory is created
    /// lazily on the first [`SessionStore::save`].
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// Directory holding the session files.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Reduce a session id to a safe single path component. Session ids are
    /// caller-supplied and could otherwise contain `/`, `\`, or `\0` and escape
    /// the store directory. Anything path-like becomes `_`; the result is
    /// deterministic so `load` and `save` agree on the same filename.
    fn safe_id(session_id: &str) -> String {
        session_id
            .chars()
            .map(|c| match c {
                '/' | '\\' | '\0' => '_',
                _ => c,
            })
            .collect()
    }

    fn target_path(&self, session_id: &str) -> PathBuf {
        self.dir.join(format!("{}.json", Self::safe_id(session_id)))
    }

    /// Synchronous core of [`SessionStore::load`].
    fn load_blocking(path: &Path) -> Result<Option<SessionRecord>> {
        match std::fs::read(path) {
            Ok(bytes) => {
                let record: SessionRecord = serde_json::from_slice(&bytes).map_err(|e| {
                    VyaneError::with_source(
                        ErrorKind::Io,
                        format!("parse session file {}", path.display()),
                        e,
                    )
                })?;
                Ok(Some(record))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Synchronous core of [`SessionStore::save`]: tmp + atomic rename.
    fn save_blocking(dir: &Path, session_id: &str, record: &SessionRecord) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        let safe = Self::safe_id(session_id);
        let target = dir.join(format!("{safe}.json"));
        // Temp file in the same directory so `rename` stays on one filesystem.
        let stamp = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = dir.join(format!(".{safe}.json.tmp.{}.{}", std::process::id(), stamp));

        let result = (|| -> Result<()> {
            let bytes = serde_json::to_vec(record).map_err(|e| {
                VyaneError::with_source(ErrorKind::Io, "serialize session record", e)
            })?;
            {
                let mut file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&tmp)?;
                file.write_all(&bytes)?;
                file.flush()?;
            }
            // Publish atomically: a concurrent reader sees the old file until
            // this instant, then the new one — never a partial write.
            std::fs::rename(&tmp, &target)?;
            Ok(())
        })();

        if result.is_err() {
            // Clean up an orphaned temp file on any failure path.
            let _ = std::fs::remove_file(&tmp);
        }
        result
    }

    /// Synchronous core of [`SessionStore::list`].
    fn list_blocking(dir: &Path, owner: Option<&str>) -> Result<Vec<SessionRecord>> {
        let mut out = Vec::new();
        let read_dir = match std::fs::read_dir(dir) {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };

        for entry in read_dir {
            let entry = entry?;
            let path = entry.path();
            // Only consider `<id>.json`; ignore temp files and anything else.
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            if let Ok(bytes) = std::fs::read(&path) {
                if let Ok(record) = serde_json::from_slice::<SessionRecord>(&bytes) {
                    if owner.is_none_or(|o| record.owner == o) {
                        out.push(record);
                    }
                }
                // A session file we cannot parse is skipped silently: session
                // files are written atomically, so a parse failure means either
                // an unrelated JSON file landed here or external tampering —
                // neither is recoverable by re-reading.
            }
        }

        // Most-recently-updated first, for a stable and useful default order.
        out.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        Ok(out)
    }
}

#[async_trait]
impl SessionStore for FsSessionStore {
    async fn load(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        let path = self.target_path(session_id);
        tokio::task::spawn_blocking(move || Self::load_blocking(&path))
            .await
            .map_err(|join_err| VyaneError::new(ErrorKind::Io, join_err.to_string()))?
    }

    async fn save(&self, record: &SessionRecord) -> Result<()> {
        let dir = self.dir.clone();
        let session_id = record.session_id.clone();
        let record = record.clone();
        tokio::task::spawn_blocking(move || Self::save_blocking(&dir, &session_id, &record))
            .await
            .map_err(|join_err| VyaneError::new(ErrorKind::Io, join_err.to_string()))?
    }

    async fn list(&self, owner: Option<&str>) -> Result<Vec<SessionRecord>> {
        let dir = self.dir.clone();
        let owner = owner.map(str::to_string);
        tokio::task::spawn_blocking(move || Self::list_blocking(&dir, owner.as_deref()))
            .await
            .map_err(|join_err| VyaneError::new(ErrorKind::Io, join_err.to_string()))?
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn safe_id_replaces_separators() {
        assert_eq!(FsSessionStore::safe_id("abc"), "abc");
        assert_eq!(FsSessionStore::safe_id("a/b"), "a_b");
        assert_eq!(FsSessionStore::safe_id("a\\b"), "a_b");
        assert_eq!(FsSessionStore::safe_id("a\0b"), "a_b");
        // Traversal attempts are neutralized.
        assert_eq!(FsSessionStore::safe_id(".."), "..");
        assert_eq!(FsSessionStore::safe_id("../etc/passwd"), ".._etc_passwd");
    }
}
