//! Filesystem-backed, owner-isolated [`SessionStore`].
//!
//! Each owner gets an opaque SHA-256 namespace below the configured root and
//! each session id gets a domain-separated SHA-256 filename:
//!
//! ```text
//! <root>/<sha256(owner)>/<sha256(session-id)>.json
//! ```
//!
//! The original pre-release layout stored `<safe_id>.json` directly below the
//! root. That layout cannot be migrated implicitly: its lossy filename mapping
//! allowed distinct ids to collide, and it had no physical owner namespace.
//! A load may migrate a legacy flat file only when its embedded owner and
//! session id exactly match the requested pair; lossy filename inference is
//! never trusted. Listing deliberately ignores all root-level JSON files.
//!
//! Writes serialize through a per-session advisory lock and publish a private
//! temp file with an atomic rename. Readers therefore observe either the old
//! complete record or the new complete record, never a partial JSON document.
//! Before publication, the temp file is synced; after rename, the containing
//! directory is synced on Unix. This prevents a successfully acknowledged
//! binding transition from reverting to an older directory entry after a
//! crash on filesystems that honour the usual `fsync` contract.

use std::fs::{File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;
use vyane_core::error::{ErrorKind, Result, VyaneError};
use vyane_core::{
    NativeSessionBinding, NativeSessionState, NativeSessionTransition, SessionExecutionLease,
    SessionRecord, SessionSnapshot, SessionStore, SessionUpdate,
};

const MAX_OWNER_BYTES: usize = 256;
const MAX_SESSION_ID_BYTES: usize = 1024;
const MAX_EXECUTION_ID_BYTES: usize = 256;
const MAX_NATIVE_SESSION_ID_BYTES: usize = 512;
const MAX_DOMAIN_TEXT_BYTES: usize = 512;
const MAX_CANONICAL_WORKDIR_BYTES: usize = 4 * 1024;
const SHA256_HEX_BYTES: usize = 64;
const SESSION_ENVELOPE_SCHEMA: u32 = 2;
const MAX_SESSION_RECORD_BYTES: u64 = 64 * 1024 * 1024;
const LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const EXECUTION_ADMISSION_TIMEOUT: Duration = Duration::from_millis(250);
const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(10);
const LEASE_COMMIT_FRESH: u8 = 0;
const LEASE_COMMIT_CONSUMED: u8 = 1;

/// Monotonic counter that makes temp file names unique inside one process.
/// Combined with the process id it also distinguishes concurrent processes.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Strict V2 on-disk shape. `session` is deliberately nested: a legacy
/// `SessionRecord` reader must fail on a V2 file rather than ignore revision
/// and binding authority, then overwrite it through an old writer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionEnvelope {
    schema: u32,
    #[serde(rename = "session")]
    record: SessionRecord,
    session_revision: u64,
    native_session: DiskNativeSession,
}

/// Required explicit state prevents a truncated/old writer from silently
/// turning a missing optional binding field into `None`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum DiskNativeSession {
    Absent,
    LegacyUnbound { native_session_id: String },
    Bound { binding: Box<NativeSessionBinding> },
}

impl SessionEnvelope {
    fn legacy(mut record: SessionRecord) -> Self {
        let native_session = record
            .native_session_id
            .take()
            .map_or(DiskNativeSession::Absent, |native_session_id| {
                DiskNativeSession::LegacyUnbound { native_session_id }
            });
        Self {
            schema: SESSION_ENVELOPE_SCHEMA,
            record,
            session_revision: 0,
            native_session,
        }
    }

    fn binding(&self) -> Option<&NativeSessionBinding> {
        match &self.native_session {
            DiskNativeSession::Absent | DiskNativeSession::LegacyUnbound { .. } => None,
            DiskNativeSession::Bound { binding } => Some(binding.as_ref()),
        }
    }

    fn set_binding(&mut self, binding: Option<NativeSessionBinding>) {
        self.native_session = match binding {
            Some(binding) => DiskNativeSession::Bound {
                binding: Box::new(binding),
            },
            None => DiskNativeSession::Absent,
        };
    }

    fn legacy_unbound_id(&self) -> Option<&str> {
        match &self.native_session {
            DiskNativeSession::LegacyUnbound { native_session_id } => Some(native_session_id),
            DiskNativeSession::Absent | DiskNativeSession::Bound { .. } => None,
        }
    }

    fn public_record(mut self) -> SessionRecord {
        if let DiskNativeSession::LegacyUnbound { native_session_id } = self.native_session {
            self.record.native_session_id = Some(native_session_id);
        }
        self.record
    }

    fn into_legacy_record(mut self) -> Result<SessionRecord> {
        match self.native_session {
            DiskNativeSession::Absent => Ok(self.record),
            DiskNativeSession::LegacyUnbound { native_session_id } => {
                self.record.native_session_id = Some(native_session_id);
                Ok(self.record)
            }
            DiskNativeSession::Bound { .. } => Err(VyaneError::unsupported(
                "domain-bound session requires load_snapshot until native resume authority is implemented",
            )),
        }
    }

    fn into_snapshot(mut self) -> SessionSnapshot {
        let native_session = match self.native_session {
            DiskNativeSession::Bound { binding } => NativeSessionState::Bound { binding },
            DiskNativeSession::LegacyUnbound { native_session_id } => {
                self.record.native_session_id = Some(native_session_id.clone());
                NativeSessionState::LegacyUnbound { native_session_id }
            }
            DiskNativeSession::Absent => NativeSessionState::Absent,
        };
        SessionSnapshot {
            record: self.record,
            session_revision: self.session_revision,
            native_session,
        }
    }
}

/// One private namespace per owner and one JSON file per logical session.
pub struct FsSessionStore {
    dir: PathBuf,
}

/// Live owner/session execution authority backed by an advisory file lock.
///
/// The file descriptor is intentionally process-local and non-serializable.
/// Kernel cancellation, task abortion, unwind, and process death all drop the
/// descriptor, so this local filesystem store needs no stale TTL or heartbeat
/// recovery path.
struct FsSessionExecutionLease {
    root: PathBuf,
    owner: String,
    session_id: String,
    execution_id: String,
    authority: Arc<FsSessionLeaseAuthority>,
    commit_state: AtomicU8,
}

struct FsSessionLeaseAuthority {
    lock: File,
}

impl Drop for FsSessionLeaseAuthority {
    fn drop(&mut self) {
        let _ = fs4::fs_std::FileExt::unlock(&self.lock);
    }
}

impl FsSessionExecutionLease {
    fn begin_commit(&self) -> Result<()> {
        self.commit_state
            .compare_exchange(
                LEASE_COMMIT_FRESH,
                LEASE_COMMIT_CONSUMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|_| {
                VyaneError::new(
                    ErrorKind::Conflict,
                    "session execution lease was already used for a commit attempt",
                )
            })
    }

    async fn run_blocking<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        // The closure, not only the async caller, owns one authority reference.
        // `spawn_blocking` cannot be cancelled once running: retaining this Arc
        // prevents an aborted async task from releasing the session lock while
        // its old mutation is still executing.
        let authority = Arc::clone(&self.authority);
        tokio::task::spawn_blocking(move || {
            let _authority = authority;
            operation()
        })
        .await
        .map_err(|join_err| VyaneError::new(ErrorKind::Io, join_err.to_string()))?
    }

    async fn save_record(&self, record: &SessionRecord) -> Result<()> {
        self.revalidate().await?;
        if record.owner != self.owner || record.session_id != self.session_id {
            return Err(VyaneError::config(
                "session execution lease does not authorize this saved record identity",
            ));
        }
        self.begin_commit()?;
        let root = self.root.clone();
        let owner = self.owner.clone();
        let record = record.clone();
        self.run_blocking(move || FsSessionStore::save_blocking(&root, &owner, &record))
            .await
    }

    async fn apply_unfenced_update(&self, update: &SessionUpdate) -> Result<SessionRecord> {
        self.revalidate().await?;
        if update.owner != self.owner || update.session_id != self.session_id {
            return Err(VyaneError::config(
                "session execution lease does not authorize this update identity",
            ));
        }
        self.begin_commit()?;
        let root = self.root.clone();
        let owner = self.owner.clone();
        let update = update.clone();
        self.run_blocking(move || FsSessionStore::apply_update_blocking(&root, &owner, &update))
            .await
    }
}

#[async_trait]
impl SessionExecutionLease for FsSessionExecutionLease {
    fn owner(&self) -> &str {
        &self.owner
    }

    fn session_id(&self) -> &str {
        &self.session_id
    }

    fn execution_id(&self) -> &str {
        &self.execution_id
    }

    async fn revalidate(&self) -> Result<()> {
        // A caller cannot invoke this method without a live guard, and every
        // in-flight blocking operation retains its own authority Arc.
        Ok(())
    }

    async fn load_snapshot(&self) -> Result<Option<SessionSnapshot>> {
        self.revalidate().await?;
        let root = self.root.clone();
        let owner = self.owner.clone();
        let session_id = self.session_id.clone();
        self.run_blocking(move || {
            FsSessionStore::load_snapshot_blocking(&root, &owner, &session_id)
        })
        .await
    }

    async fn apply_update(
        &self,
        expected_revision: u64,
        update: &SessionUpdate,
    ) -> Result<SessionSnapshot> {
        self.revalidate().await?;
        if update.owner != self.owner || update.session_id != self.session_id {
            return Err(VyaneError::config(
                "session execution lease does not authorize this update identity",
            ));
        }
        self.begin_commit()?;
        let root = self.root.clone();
        let owner = self.owner.clone();
        let update = update.clone();
        self.run_blocking(move || {
            FsSessionStore::apply_update_cas_blocking(&root, &owner, expected_revision, &update)
        })
        .await
    }

    async fn apply_native_transition(
        &self,
        transition: &NativeSessionTransition,
    ) -> Result<SessionSnapshot> {
        self.revalidate().await?;
        self.begin_commit()?;
        let root = self.root.clone();
        let owner = self.owner.clone();
        let session_id = self.session_id.clone();
        let transition = transition.clone();
        self.run_blocking(move || {
            FsSessionStore::apply_native_transition_blocking(
                &root,
                &owner,
                &session_id,
                &transition,
            )
        })
        .await
    }
}

impl FsSessionStore {
    /// Create a store rooted at `dir`. Directories are created lazily on the
    /// first [`SessionStore::save`].
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    async fn acquire_local_execution_lease(
        &self,
        owner: &str,
        session_id: &str,
        execution_id: &str,
        timeout: Duration,
    ) -> Result<FsSessionExecutionLease> {
        let root = self.dir.clone();
        let owner = owner.to_string();
        let session_id = session_id.to_string();
        let execution_id = execution_id.to_string();
        tokio::task::spawn_blocking(move || {
            Self::acquire_execution_lease_blocking(
                &root,
                &owner,
                &session_id,
                &execution_id,
                timeout,
            )
        })
        .await
        .map_err(|join_err| VyaneError::new(ErrorKind::Io, join_err.to_string()))?
    }

    /// Root directory holding owner namespaces.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Explicit administrative enumeration across every physical owner
    /// namespace. Runtime callers should use [`SessionStore::list`] instead.
    pub async fn list_all_admin(&self) -> Result<Vec<SessionRecord>> {
        let root = self.dir.clone();
        tokio::task::spawn_blocking(move || Self::list_blocking(&root, None))
            .await
            .map_err(|join_err| VyaneError::new(ErrorKind::Io, join_err.to_string()))?
    }

    fn load_envelope_blocking(
        root: &Path,
        owner: &str,
        session_id: &str,
    ) -> Result<Option<SessionEnvelope>> {
        validate_identity(owner, session_id)?;

        if !private_directory_exists(root)? {
            return Ok(None);
        }
        let owner_root = owner_directory(root, owner);
        if private_directory_exists(&owner_root)? {
            let path = owner_root.join(format!("{}.json", session_key(session_id)));
            if let Some(envelope) = read_envelope(&path)? {
                validate_record_identity(&envelope.record, owner, session_id, &path)?;
                return Ok(Some(envelope));
            }
        }

        migrate_matching_legacy_record(root, owner, session_id)
    }

    /// Synchronous core of [`SessionStore::load`].
    fn load_blocking(root: &Path, owner: &str, session_id: &str) -> Result<Option<SessionRecord>> {
        Self::load_envelope_blocking(root, owner, session_id)?
            .map(SessionEnvelope::into_legacy_record)
            .transpose()
    }

    fn load_snapshot_blocking(
        root: &Path,
        owner: &str,
        session_id: &str,
    ) -> Result<Option<SessionSnapshot>> {
        Ok(Self::load_envelope_blocking(root, owner, session_id)?
            .map(SessionEnvelope::into_snapshot))
    }

    fn acquire_execution_lease_blocking(
        root: &Path,
        owner: &str,
        session_id: &str,
        execution_id: &str,
        timeout: Duration,
    ) -> Result<FsSessionExecutionLease> {
        validate_identity(owner, session_id)?;
        validate_text("execution_id", execution_id, MAX_EXECUTION_ID_BYTES)?;
        secure_directory(root)?;
        let owner_root = owner_directory(root, owner);
        secure_directory(&owner_root)?;
        let key = session_key(session_id);
        let lock_path = owner_root.join(format!("{key}.execution.lock"));
        let lock = open_private_lock(&lock_path)?;
        acquire_execution_lock(&lock, timeout)?;
        Ok(FsSessionExecutionLease {
            root: root.to_path_buf(),
            owner: owner.to_string(),
            session_id: session_id.to_string(),
            execution_id: execution_id.to_string(),
            authority: Arc::new(FsSessionLeaseAuthority { lock }),
            commit_state: AtomicU8::new(LEASE_COMMIT_FRESH),
        })
    }

    /// Synchronous core of [`SessionStore::save`]: validate, lock, then publish
    /// a private temp file with an atomic rename.
    fn save_blocking(root: &Path, authority: &str, record: &SessionRecord) -> Result<()> {
        validate_owner(authority)?;
        validate_identity(&record.owner, &record.session_id)?;
        if record.owner != authority {
            return Err(VyaneError::config(
                "session save authority does not match the record owner",
            ));
        }
        secure_directory(root)?;
        let owner_root = owner_directory(root, &record.owner);
        secure_directory(&owner_root)?;

        let key = session_key(&record.session_id);
        let target = owner_root.join(format!("{key}.json"));
        let lock_path = owner_root.join(format!("{key}.lock"));
        let lock = open_private_lock(&lock_path)?;
        acquire_exclusive_lock(&lock)?;

        let result = (|| -> Result<()> {
            let existing = match read_envelope(&target)? {
                Some(existing) => {
                    validate_record_identity(
                        &existing.record,
                        &record.owner,
                        &record.session_id,
                        &target,
                    )?;
                    Some(existing)
                }
                None => {
                    reject_matching_legacy_record(root, &record.owner, &record.session_id, "save")?;
                    None
                }
            };
            if existing
                .as_ref()
                .is_some_and(|envelope| envelope.binding().is_some())
                && record.native_session_id.is_some()
            {
                return Err(VyaneError::config(
                    "session record cannot contain both a legacy native id and a domain binding",
                ));
            }
            let session_revision = next_revision(
                existing
                    .as_ref()
                    .map_or(0, |envelope| envelope.session_revision),
            )?;
            let mut persisted_record = record.clone();
            let supplied_legacy_id = persisted_record.native_session_id.take();
            let native_session = match existing.as_ref().map(|envelope| &envelope.native_session) {
                Some(DiskNativeSession::Bound { binding }) => DiskNativeSession::Bound {
                    binding: binding.clone(),
                },
                Some(DiskNativeSession::LegacyUnbound { native_session_id }) => {
                    DiskNativeSession::LegacyUnbound {
                        native_session_id: supplied_legacy_id
                            .unwrap_or_else(|| native_session_id.clone()),
                    }
                }
                Some(DiskNativeSession::Absent) | None => supplied_legacy_id
                    .map_or(DiskNativeSession::Absent, |native_session_id| {
                        DiskNativeSession::LegacyUnbound { native_session_id }
                    }),
            };
            let envelope = SessionEnvelope {
                schema: SESSION_ENVELOPE_SCHEMA,
                record: persisted_record,
                session_revision,
                native_session,
            };
            validate_envelope(&envelope)?;
            write_envelope_atomic(&owner_root, &key, &target, &envelope)
        })();

        finish_locked(lock, result)
    }

    fn apply_update_blocking(
        root: &Path,
        authority: &str,
        update: &SessionUpdate,
    ) -> Result<SessionRecord> {
        Self::apply_update_envelope_blocking(root, authority, update, None)
            .map(SessionEnvelope::public_record)
    }

    fn apply_update_cas_blocking(
        root: &Path,
        authority: &str,
        expected_revision: u64,
        update: &SessionUpdate,
    ) -> Result<SessionSnapshot> {
        Self::apply_update_envelope_blocking(root, authority, update, Some(expected_revision))
            .map(SessionEnvelope::into_snapshot)
    }

    fn apply_update_envelope_blocking(
        root: &Path,
        authority: &str,
        update: &SessionUpdate,
        expected_revision: Option<u64>,
    ) -> Result<SessionEnvelope> {
        validate_owner(authority)?;
        validate_identity(&update.owner, &update.session_id)?;
        if update.owner != authority {
            return Err(VyaneError::config(
                "session update authority does not match the update owner",
            ));
        }
        secure_directory(root)?;
        let owner_root = owner_directory(root, authority);
        secure_directory(&owner_root)?;
        let key = session_key(&update.session_id);
        let target = owner_root.join(format!("{key}.json"));
        let lock_path = owner_root.join(format!("{key}.lock"));
        let lock = open_private_lock(&lock_path)?;
        acquire_exclusive_lock(&lock)?;

        let result = (|| -> Result<SessionEnvelope> {
            let existing = match read_envelope(&target)? {
                Some(existing) => {
                    validate_record_identity(
                        &existing.record,
                        authority,
                        &update.session_id,
                        &target,
                    )?;
                    Some(existing)
                }
                None => {
                    reject_matching_legacy_record(root, authority, &update.session_id, "update")?;
                    None
                }
            };
            if existing
                .as_ref()
                .is_some_and(|envelope| envelope.binding().is_some())
                && update.native_session_id.is_some()
            {
                return Err(VyaneError::config(
                    "session update cannot add a legacy native id to a domain-bound session",
                ));
            }
            let observed_revision = existing
                .as_ref()
                .map_or(0, |envelope| envelope.session_revision);
            if let Some(expected) = expected_revision {
                if expected != observed_revision {
                    return Err(VyaneError::new(
                        ErrorKind::Conflict,
                        format!(
                            "session revision conflict: expected {expected}, observed {observed_revision}"
                        ),
                    ));
                }
            }
            let session_revision = next_revision(observed_revision)?;
            let prior_native_session = existing
                .as_ref()
                .map(|envelope| envelope.native_session.clone());
            let mut record = update.apply_to(existing.map(|envelope| envelope.record));
            let produced_legacy_id = record.native_session_id.take();
            let native_session = match prior_native_session {
                Some(DiskNativeSession::Bound { binding }) => DiskNativeSession::Bound { binding },
                Some(DiskNativeSession::LegacyUnbound { native_session_id }) => {
                    DiskNativeSession::LegacyUnbound {
                        native_session_id: produced_legacy_id.unwrap_or(native_session_id),
                    }
                }
                Some(DiskNativeSession::Absent) | None => produced_legacy_id
                    .map_or(DiskNativeSession::Absent, |native_session_id| {
                        DiskNativeSession::LegacyUnbound { native_session_id }
                    }),
            };
            validate_record_identity(&record, authority, &update.session_id, &target)?;
            let envelope = SessionEnvelope {
                schema: SESSION_ENVELOPE_SCHEMA,
                record,
                session_revision,
                native_session,
            };
            validate_envelope(&envelope)?;
            write_envelope_atomic(&owner_root, &key, &target, &envelope)?;
            Ok(envelope)
        })();

        finish_locked(lock, result)
    }

    fn apply_native_transition_blocking(
        root: &Path,
        authority: &str,
        session_id: &str,
        transition: &NativeSessionTransition,
    ) -> Result<SessionSnapshot> {
        validate_identity(authority, session_id)?;
        validate_transition_identity(authority, session_id, transition)?;
        secure_directory(root)?;
        let owner_root = owner_directory(root, authority);
        secure_directory(&owner_root)?;
        let key = session_key(session_id);
        let target = owner_root.join(format!("{key}.json"));
        let lock_path = owner_root.join(format!("{key}.lock"));
        let lock = open_private_lock(&lock_path)?;
        acquire_exclusive_lock(&lock)?;

        let result = (|| -> Result<SessionSnapshot> {
            let existing = match read_envelope(&target)? {
                Some(existing) => {
                    validate_record_identity(&existing.record, authority, session_id, &target)?;
                    Some(existing)
                }
                None => {
                    reject_matching_legacy_record(root, authority, session_id, "transition")?;
                    None
                }
            };
            let observed_revision = existing
                .as_ref()
                .map_or(0, |envelope| envelope.session_revision);
            if observed_revision != transition.expected_revision() {
                return Err(VyaneError::new(
                    ErrorKind::Conflict,
                    format!(
                        "session revision conflict: expected {}, observed {observed_revision}",
                        transition.expected_revision()
                    ),
                ));
            }
            let session_revision = next_revision(observed_revision)?;

            let envelope = match transition {
                NativeSessionTransition::Reset { .. } => {
                    let Some(mut existing) = existing else {
                        return Err(VyaneError::new(
                            ErrorKind::NotFound,
                            "cannot reset missing session",
                        ));
                    };
                    existing.record.native_session_id = None;
                    existing.set_binding(None);
                    existing.session_revision = session_revision;
                    existing
                }
                NativeSessionTransition::ForkFresh {
                    update, binding, ..
                } => {
                    let Some(mut existing) = existing else {
                        return Err(VyaneError::new(
                            ErrorKind::NotFound,
                            "cannot fork native state for a missing session",
                        ));
                    };
                    validate_binding_matches_update(binding, update)?;
                    existing.record.native_session_id = None;
                    let record = update.apply_to(Some(existing.record));
                    SessionEnvelope {
                        schema: SESSION_ENVELOPE_SCHEMA,
                        record,
                        session_revision,
                        native_session: DiskNativeSession::Bound {
                            binding: Box::new(binding.clone()),
                        },
                    }
                }
                NativeSessionTransition::Commit {
                    update, binding, ..
                } => {
                    validate_binding_matches_update(binding, update)?;
                    if let Some(existing) = existing.as_ref() {
                        if existing.legacy_unbound_id().is_some() {
                            return Err(VyaneError::config(
                                "legacy native session must be reset or forked fresh before commit",
                            ));
                        }
                        if existing.binding().is_some_and(|current| current != binding) {
                            return Err(VyaneError::config(
                                "native session binding drift requires an explicit fresh fork",
                            ));
                        }
                    }
                    let record = update.apply_to(existing.map(|envelope| envelope.record));
                    SessionEnvelope {
                        schema: SESSION_ENVELOPE_SCHEMA,
                        record,
                        session_revision,
                        native_session: DiskNativeSession::Bound {
                            binding: Box::new(binding.clone()),
                        },
                    }
                }
                _ => {
                    return Err(VyaneError::unsupported(
                        "session store does not support this native-session transition",
                    ));
                }
            };

            validate_record_identity(&envelope.record, authority, session_id, &target)?;
            validate_envelope(&envelope)?;
            write_envelope_atomic(&owner_root, &key, &target, &envelope)?;
            Ok(envelope.into_snapshot())
        })();

        finish_locked(lock, result)
    }

    /// Synchronous core of [`SessionStore::list`].
    fn list_blocking(root: &Path, owner: Option<&str>) -> Result<Vec<SessionRecord>> {
        if let Some(owner) = owner {
            validate_owner(owner)?;
        }
        if !private_directory_exists(root)? {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        match owner {
            Some(owner) => {
                // The scoped form intentionally touches only this owner's
                // digest namespace; it never scans other owners or legacy
                // root-level files.
                let owner_root = owner_directory(root, owner);
                if private_directory_exists(&owner_root)? {
                    collect_namespace(root, &owner_root, Some(owner), &mut out)?;
                }
            }
            None => {
                for entry in std::fs::read_dir(root)? {
                    let entry = entry?;
                    let path = entry.path();
                    let metadata = match std::fs::symlink_metadata(&path) {
                        Ok(metadata) => metadata,
                        Err(_) => continue,
                    };
                    if !metadata.file_type().is_dir() {
                        // Includes legacy root-level JSON files and symlinks.
                        continue;
                    }
                    collect_namespace(root, &path, None, &mut out)?;
                }
            }
        }

        out.sort_by_key(|record| std::cmp::Reverse(record.updated_at));
        Ok(out)
    }

    /// Strict revision-aware enumeration for one owner. Unlike the legacy
    /// record projection, this surfaces any malformed owner-namespace JSON so
    /// backup/control callers cannot mistake a decode omission for `Absent`.
    fn list_snapshots_blocking(root: &Path, owner: &str) -> Result<Vec<SessionSnapshot>> {
        validate_owner(owner)?;
        if !private_directory_exists(root)? {
            return Ok(Vec::new());
        }
        let owner_root = owner_directory(root, owner);
        if !private_directory_exists(&owner_root)? {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for entry in std::fs::read_dir(&owner_root)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let envelope = read_envelope(&path)?.ok_or_else(|| {
                VyaneError::new(ErrorKind::Io, "session snapshot disappeared during listing")
            })?;
            validate_record_identity(&envelope.record, owner, &envelope.record.session_id, &path)?;
            let expected =
                owner_root.join(format!("{}.json", session_key(&envelope.record.session_id)));
            if path != expected {
                return Err(VyaneError::config(format!(
                    "session snapshot path {} does not match its embedded identity",
                    path.display()
                )));
            }
            out.push(envelope.into_snapshot());
        }
        out.sort_by_key(|snapshot| std::cmp::Reverse(snapshot.record.updated_at));
        Ok(out)
    }
}

#[async_trait]
impl SessionStore for FsSessionStore {
    async fn acquire_execution_lease(
        &self,
        owner: &str,
        session_id: &str,
        execution_id: &str,
    ) -> Result<Box<dyn SessionExecutionLease>> {
        let lease = self
            .acquire_local_execution_lease(
                owner,
                session_id,
                execution_id,
                EXECUTION_ADMISSION_TIMEOUT,
            )
            .await?;
        Ok(Box::new(lease))
    }

    async fn load(&self, owner: &str, session_id: &str) -> Result<Option<SessionRecord>> {
        let root = self.dir.clone();
        let owner = owner.to_string();
        let session_id = session_id.to_string();
        tokio::task::spawn_blocking(move || Self::load_blocking(&root, &owner, &session_id))
            .await
            .map_err(|join_err| VyaneError::new(ErrorKind::Io, join_err.to_string()))?
    }

    async fn load_snapshot(
        &self,
        owner: &str,
        session_id: &str,
    ) -> Result<Option<SessionSnapshot>> {
        let root = self.dir.clone();
        let owner = owner.to_string();
        let session_id = session_id.to_string();
        tokio::task::spawn_blocking(move || {
            Self::load_snapshot_blocking(&root, &owner, &session_id)
        })
        .await
        .map_err(|join_err| VyaneError::new(ErrorKind::Io, join_err.to_string()))?
    }

    async fn save(&self, owner: &str, record: &SessionRecord) -> Result<()> {
        // Admission and mutation are separate await stages. If this future is
        // aborted while another execution owns the session, the detached
        // blocking lock attempt can only acquire-and-drop a guard; it has no
        // mutation closure to run later. Once `save_record` starts its blocking
        // publish, callers must treat cancellation as outcome-indeterminate and
        // reload before retrying.
        let execution_id = store_mutation_execution_id("save");
        let lease = self
            .acquire_local_execution_lease(owner, &record.session_id, &execution_id, LOCK_TIMEOUT)
            .await?;
        lease.save_record(record).await
    }

    async fn apply_update(&self, owner: &str, update: &SessionUpdate) -> Result<SessionRecord> {
        // Keep lock admission separate from the write for abort safety; see
        // `save` for the post-admission cancellation boundary.
        let execution_id = store_mutation_execution_id("update");
        let lease = self
            .acquire_local_execution_lease(owner, &update.session_id, &execution_id, LOCK_TIMEOUT)
            .await?;
        lease.apply_unfenced_update(update).await
    }

    async fn apply_native_transition(
        &self,
        owner: &str,
        session_id: &str,
        transition: &NativeSessionTransition,
    ) -> Result<SessionSnapshot> {
        // Keep lock admission separate from the write for abort safety; see
        // `save` for the post-admission cancellation boundary.
        let execution_id = store_mutation_execution_id("native-transition");
        let lease = self
            .acquire_local_execution_lease(
                owner,
                session_id,
                &execution_id,
                EXECUTION_ADMISSION_TIMEOUT,
            )
            .await?;
        lease.apply_native_transition(transition).await
    }

    async fn list_snapshots(&self, owner: &str) -> Result<Vec<SessionSnapshot>> {
        let root = self.dir.clone();
        let owner = owner.to_string();
        tokio::task::spawn_blocking(move || Self::list_snapshots_blocking(&root, &owner))
            .await
            .map_err(|join_err| VyaneError::new(ErrorKind::Io, join_err.to_string()))?
    }

    async fn list(&self, owner: &str) -> Result<Vec<SessionRecord>> {
        let root = self.dir.clone();
        let owner = owner.to_string();
        tokio::task::spawn_blocking(move || Self::list_blocking(&root, Some(&owner)))
            .await
            .map_err(|join_err| VyaneError::new(ErrorKind::Io, join_err.to_string()))?
    }
}

fn write_envelope_atomic(
    owner_root: &Path,
    key: &str,
    target: &Path,
    envelope: &SessionEnvelope,
) -> Result<()> {
    write_envelope_atomic_with_sync(owner_root, key, target, envelope, sync_directory)
}

fn write_envelope_atomic_with_sync(
    owner_root: &Path,
    key: &str,
    target: &Path,
    envelope: &SessionEnvelope,
    sync_parent: impl FnOnce(&Path) -> std::io::Result<()>,
) -> Result<()> {
    let bytes = serde_json::to_vec(envelope).map_err(|error| {
        VyaneError::with_source(ErrorKind::Io, "serialize session envelope", error)
    })?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_SESSION_RECORD_BYTES {
        return Err(VyaneError::new(
            ErrorKind::Io,
            format!("session record exceeds {MAX_SESSION_RECORD_BYTES} serialized bytes"),
        ));
    }
    let stamp = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = owner_root.join(format!(".{key}.json.tmp.{}.{}", std::process::id(), stamp));
    let result = (|| -> Result<()> {
        let mut file = create_private_file(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, target)?;
        sync_parent(owner_root).map_err(|source| {
            VyaneError::with_source(
                ErrorKind::Indeterminate,
                "session mutation was published but directory durability confirmation failed; reload the snapshot and compare session_revision before retrying",
                source,
            )
        })?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn validate_owner(owner: &str) -> Result<()> {
    validate_text("owner", owner, MAX_OWNER_BYTES)
}

fn validate_identity(owner: &str, session_id: &str) -> Result<()> {
    validate_owner(owner)?;
    validate_text("session_id", session_id, MAX_SESSION_ID_BYTES)
}

fn store_mutation_execution_id(operation: &str) -> String {
    format!("session-store-{operation}-{}", Uuid::now_v7())
}

fn validate_text(field: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.is_empty() || value.len() > max_bytes || value.contains('\0') {
        return Err(VyaneError::config(format!(
            "{field} must contain 1..={max_bytes} UTF-8 bytes without NUL"
        )));
    }
    Ok(())
}

fn validate_digest(field: &str, value: &str) -> Result<()> {
    if value.len() != SHA256_HEX_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(VyaneError::config(format!(
            "{field} must be a lowercase SHA-256 hex digest"
        )));
    }
    Ok(())
}

fn validate_binding(binding: &NativeSessionBinding) -> Result<()> {
    validate_domain_text(
        "native_session_id",
        &binding.native_session_id,
        MAX_NATIVE_SESSION_ID_BYTES,
    )?;
    let domain = &binding.domain;
    validate_domain_text("native runtime", &domain.runtime, MAX_DOMAIN_TEXT_BYTES)?;
    validate_domain_text(
        "native harness",
        domain.harness.as_str(),
        MAX_DOMAIN_TEXT_BYTES,
    )?;
    validate_domain_text(
        "native provider",
        domain.provider.as_str(),
        MAX_DOMAIN_TEXT_BYTES,
    )?;
    validate_domain_text("native model", domain.model.as_str(), MAX_DOMAIN_TEXT_BYTES)?;
    validate_digest("endpoint_routing_digest", &domain.endpoint_routing_digest)?;
    validate_digest("account_scope_digest", &domain.account_scope_digest)?;
    validate_digest("runtime_scope_digest", &domain.runtime_scope_digest)?;
    validate_domain_text(
        "checkpoint_namespace",
        &domain.checkpoint_namespace,
        MAX_DOMAIN_TEXT_BYTES,
    )?;
    if domain.checkpoint_schema == 0 {
        return Err(VyaneError::config(
            "checkpoint_schema must be a non-zero version",
        ));
    }
    if !domain.canonical_workdir.is_absolute() {
        return Err(VyaneError::config(
            "native session canonical_workdir must be absolute",
        ));
    }
    let workdir = domain.canonical_workdir.to_str().ok_or_else(|| {
        VyaneError::config("native session canonical_workdir must be valid UTF-8")
    })?;
    validate_domain_text(
        "native session canonical_workdir",
        workdir,
        MAX_CANONICAL_WORKDIR_BYTES,
    )?;
    Ok(())
}

fn validate_envelope(envelope: &SessionEnvelope) -> Result<()> {
    if envelope.schema != SESSION_ENVELOPE_SCHEMA {
        return Err(VyaneError::config(format!(
            "unsupported session envelope schema {}",
            envelope.schema
        )));
    }
    if envelope.record.native_session_id.is_some() {
        return Err(VyaneError::config(
            "V2 session record cannot contain a legacy native id; native authority must use the explicit native_session state and cannot be duplicated",
        ));
    }
    match &envelope.native_session {
        DiskNativeSession::Absent => {}
        DiskNativeSession::LegacyUnbound { native_session_id } => {
            validate_domain_text(
                "legacy native_session_id",
                native_session_id,
                MAX_NATIVE_SESSION_ID_BYTES,
            )?;
        }
        DiskNativeSession::Bound { binding } => validate_binding(binding)?,
    }
    Ok(())
}

fn validate_domain_text(field: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.trim().is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(VyaneError::config(format!(
            "{field} must contain 1..={max_bytes} non-control UTF-8 bytes"
        )));
    }
    Ok(())
}

fn validate_transition_identity(
    owner: &str,
    session_id: &str,
    transition: &NativeSessionTransition,
) -> Result<()> {
    let update = match transition {
        NativeSessionTransition::Reset { .. } => return Ok(()),
        NativeSessionTransition::ForkFresh { update, .. }
        | NativeSessionTransition::Commit { update, .. } => update,
        _ => {
            return Err(VyaneError::unsupported(
                "session store does not support this native-session transition",
            ));
        }
    };
    validate_identity(&update.owner, &update.session_id)?;
    if update.owner != owner || update.session_id != session_id {
        return Err(VyaneError::config(
            "native transition authority does not match the update owner/session",
        ));
    }
    if update.native_session_id.is_some() {
        return Err(VyaneError::config(
            "domain-aware native transition cannot carry a legacy native_session_id",
        ));
    }
    Ok(())
}

fn validate_binding_matches_update(
    binding: &NativeSessionBinding,
    update: &SessionUpdate,
) -> Result<()> {
    validate_binding(binding)?;
    let domain = &binding.domain;
    if update.target.provider != domain.provider
        || update.target.protocol != domain.protocol
        || update.target.model != domain.model
        || update.target.harness.as_ref() != Some(&domain.harness)
    {
        return Err(VyaneError::config(
            "native session domain does not match the committed target",
        ));
    }
    Ok(())
}

fn next_revision(revision: u64) -> Result<u64> {
    revision
        .checked_add(1)
        .ok_or_else(|| VyaneError::config("session revision exhausted"))
}

fn validate_record_identity(
    record: &SessionRecord,
    owner: &str,
    session_id: &str,
    path: &Path,
) -> Result<()> {
    validate_identity(&record.owner, &record.session_id)?;
    if record.owner != owner || record.session_id != session_id {
        return Err(VyaneError::config(format!(
            "session record identity mismatch in {}: requested owner/session do not match stored record",
            path.display()
        )));
    }
    Ok(())
}

fn sha256_hex(domain: &[u8], value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(value.as_bytes());
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn owner_key(owner: &str) -> String {
    // Keep this identical to the event ledger's owner namespace so all
    // owner-scoped stores use one predictable physical convention.
    Sha256::digest(owner.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn session_key(session_id: &str) -> String {
    sha256_hex(b"vyane.session.v1\0", session_id)
}

fn owner_directory(root: &Path, owner: &str) -> PathBuf {
    root.join(owner_key(owner))
}

fn legacy_safe_id(session_id: &str) -> String {
    session_id
        .chars()
        .map(|character| match character {
            '/' | '\\' | '\0' => '_',
            _ => character,
        })
        .collect()
}

fn legacy_path(root: &Path, session_id: &str) -> PathBuf {
    root.join(format!("{}.json", legacy_safe_id(session_id)))
}

fn migrate_matching_legacy_record(
    root: &Path,
    owner: &str,
    session_id: &str,
) -> Result<Option<SessionEnvelope>> {
    let legacy = legacy_path(root, session_id);
    let Some(envelope) = read_envelope(&legacy)? else {
        return Ok(None);
    };
    if envelope.record.owner != owner || envelope.record.session_id != session_id {
        return Ok(None);
    }
    validate_record_identity(&envelope.record, owner, session_id, &legacy)?;

    let owner_root = owner_directory(root, owner);
    secure_directory(&owner_root)?;
    let key = session_key(session_id);
    let target = owner_root.join(format!("{key}.json"));
    let lock_path = owner_root.join(format!("{key}.lock"));
    let lock = open_private_lock(&lock_path)?;
    acquire_exclusive_lock(&lock)?;
    let result = (|| -> Result<SessionEnvelope> {
        if let Some(existing) = read_envelope(&target)? {
            validate_record_identity(&existing.record, owner, session_id, &target)?;
            return Ok(existing);
        }
        let Some(envelope) = read_envelope(&legacy)? else {
            return Err(VyaneError::new(
                ErrorKind::Io,
                "legacy session disappeared during migration",
            ));
        };
        validate_record_identity(&envelope.record, owner, session_id, &legacy)?;
        write_envelope_atomic(&owner_root, &key, &target, &envelope)?;
        // Publication into the owner namespace is the authoritative commit.
        // Legacy-root cleanup is best effort after that point: reporting a
        // normal error would invite a blind retry even though the V2 record is
        // already durable and visible. Loads always prefer the V2 target.
        if std::fs::remove_file(&legacy).is_ok() {
            let _ = sync_directory(root);
        }
        Ok(envelope)
    })();
    finish_locked(lock, result).map(Some)
}

fn reject_matching_legacy_record(
    root: &Path,
    owner: &str,
    session_id: &str,
    operation: &str,
) -> Result<()> {
    let path = legacy_path(root, session_id);
    let Some(envelope) = read_envelope(&path)? else {
        return Ok(());
    };
    if envelope.record.owner == owner && envelope.record.session_id == session_id {
        return Err(VyaneError::config(format!(
            "cannot {operation} owner-scoped session `{owner}/{session_id}`: legacy flat record {} requires explicit migration into the owner namespace",
            path.display()
        )));
    }
    Ok(())
}

fn secure_directory(path: &Path) -> Result<()> {
    secure_directory_with_sync(path, &sync_directory)
}

fn secure_directory_with_sync(
    path: &Path,
    sync: &impl Fn(&Path) -> std::io::Result<()>,
) -> Result<()> {
    ensure_directory_exists(path, sync)?;
    set_private_directory_permissions(path)?;
    // Persist both the directory inode/permissions and its parent entry before
    // any session file is published below it. A later acknowledged file+dir
    // sync must not be undermined by an unflushed first-time mkdir.
    sync(path)?;
    if let Some(parent) = normalized_parent(path) {
        sync(parent)?;
    }
    Ok(())
}

fn ensure_directory_exists(
    path: &Path,
    sync: &impl Fn(&Path) -> std::io::Result<()>,
) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => return Ok(()),
        Ok(_) => {
            return Err(VyaneError::config(format!(
                "session storage directory {} is not a real directory",
                path.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = normalized_parent(path).ok_or_else(|| {
                VyaneError::config(format!(
                    "session storage directory {} has no creatable parent",
                    path.display()
                ))
            })?;
            ensure_directory_exists(parent, sync)?;
            match std::fs::create_dir(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let metadata = std::fs::symlink_metadata(path)?;
                    if !metadata.file_type().is_dir() {
                        return Err(VyaneError::config(format!(
                            "session storage directory {} raced with a non-directory entry",
                            path.display()
                        )));
                    }
                }
                Err(error) => return Err(error.into()),
            }
            set_private_directory_permissions(path)?;
            sync(path)?;
            sync(parent)?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn normalized_parent(path: &Path) -> Option<&Path> {
    path.parent().map(|parent| {
        if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        }
    })
}

fn private_directory_exists(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            set_private_directory_permissions(path)?;
            Ok(true)
        }
        Ok(_) => Err(VyaneError::config(format!(
            "session storage directory {} is not a real directory",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn read_envelope(path: &Path) -> Result<Option<SessionEnvelope>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => metadata,
        Ok(_) => {
            return Err(VyaneError::config(format!(
                "session storage entry {} is not a regular file",
                path.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.len() > MAX_SESSION_RECORD_BYTES {
        return Err(VyaneError::new(
            ErrorKind::Io,
            format!(
                "session record {} exceeds {MAX_SESSION_RECORD_BYTES} bytes",
                path.display()
            ),
        ));
    }

    let mut file = File::open(path)?;
    let opened_metadata = file.metadata()?;
    if !opened_metadata.file_type().is_file() {
        return Err(VyaneError::config(format!(
            "session storage entry {} changed identity while opening",
            path.display()
        )));
    }
    if opened_metadata.len() > MAX_SESSION_RECORD_BYTES {
        return Err(VyaneError::new(
            ErrorKind::Io,
            format!(
                "session record {} exceeds {MAX_SESSION_RECORD_BYTES} bytes",
                path.display()
            ),
        ));
    }
    set_private_file_permissions(path)?;
    let mut bytes = Vec::new();
    (&mut file)
        .take(MAX_SESSION_RECORD_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_SESSION_RECORD_BYTES {
        return Err(VyaneError::new(
            ErrorKind::Io,
            format!(
                "session record {} grew beyond {MAX_SESSION_RECORD_BYTES} bytes",
                path.display()
            ),
        ));
    }
    let envelope = decode_envelope(&bytes, path)?;
    validate_envelope(&envelope)?;
    Ok(Some(envelope))
}

fn decode_envelope(bytes: &[u8], path: &Path) -> Result<SessionEnvelope> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|error| {
        VyaneError::with_source(
            ErrorKind::Io,
            format!("parse session file {}", path.display()),
            error,
        )
    })?;
    let object = value.as_object().ok_or_else(|| {
        VyaneError::config(format!(
            "session file {} must contain a JSON object",
            path.display()
        ))
    })?;

    if object.contains_key("schema") {
        validate_v2_session_shape(object, path)?;
        return serde_json::from_slice(bytes).map_err(|error| {
            VyaneError::with_source(
                ErrorKind::Config,
                format!("invalid V2 session envelope {}", path.display()),
                error,
            )
        });
    }

    for reserved in [
        "session",
        "session_revision",
        "native_session",
        "native_session_binding",
    ] {
        if object.contains_key(reserved) {
            return Err(VyaneError::config(format!(
                "legacy session file {} contains reserved V2 authority field `{reserved}`",
                path.display()
            )));
        }
    }
    validate_legacy_session_shape(object, path)?;
    let record = serde_json::from_slice(bytes).map_err(|error| {
        VyaneError::with_source(
            ErrorKind::Io,
            format!("parse legacy session file {}", path.display()),
            error,
        )
    })?;
    Ok(SessionEnvelope::legacy(record))
}

fn validate_v2_session_shape(
    object: &serde_json::Map<String, serde_json::Value>,
    path: &Path,
) -> Result<()> {
    let session = object
        .get("session")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            VyaneError::config(format!(
                "V2 session envelope {} requires an object `session` field",
                path.display()
            ))
        })?;
    const SESSION_FIELDS: &[&str] = &[
        "session_id",
        "owner",
        "target",
        "transcript",
        "created_at",
        "updated_at",
        "run_count",
    ];
    if let Some(unknown) = session
        .keys()
        .find(|field| !SESSION_FIELDS.contains(&field.as_str()))
    {
        return Err(VyaneError::config(format!(
            "V2 session envelope {} contains unknown nested session field `{unknown}`",
            path.display()
        )));
    }
    for required in SESSION_FIELDS {
        if !session.contains_key(*required) {
            return Err(VyaneError::config(format!(
                "V2 session envelope {} is missing required nested session field `{required}`",
                path.display()
            )));
        }
    }
    validate_target_shape(session.get("target"), path)?;
    Ok(())
}

fn validate_legacy_session_shape(
    object: &serde_json::Map<String, serde_json::Value>,
    path: &Path,
) -> Result<()> {
    const LEGACY_FIELDS: &[&str] = &[
        "session_id",
        "owner",
        "target",
        "native_session_id",
        "transcript",
        "created_at",
        "updated_at",
        "run_count",
    ];
    if let Some(unknown) = object
        .keys()
        .find(|field| !LEGACY_FIELDS.contains(&field.as_str()))
    {
        return Err(VyaneError::config(format!(
            "legacy session file {} contains unknown field `{unknown}`",
            path.display()
        )));
    }
    for required in ["session_id", "target", "created_at", "updated_at"] {
        if !object.contains_key(required) {
            return Err(VyaneError::config(format!(
                "legacy session file {} is missing required field `{required}`",
                path.display()
            )));
        }
    }
    validate_target_shape(object.get("target"), path)?;
    Ok(())
}

fn validate_target_shape(target: Option<&serde_json::Value>, path: &Path) -> Result<()> {
    let target = target
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            VyaneError::config(format!(
                "session file {} requires an object `target` field",
                path.display()
            ))
        })?;
    const TARGET_FIELDS: &[&str] = &["provider", "protocol", "harness", "model"];
    if let Some(unknown) = target
        .keys()
        .find(|field| !TARGET_FIELDS.contains(&field.as_str()))
    {
        return Err(VyaneError::config(format!(
            "session file {} contains unknown target field `{unknown}`",
            path.display()
        )));
    }
    for required in ["provider", "protocol", "model"] {
        if !target.contains_key(required) {
            return Err(VyaneError::config(format!(
                "session file {} is missing required target field `{required}`",
                path.display()
            )));
        }
    }
    Ok(())
}

fn collect_namespace(
    root: &Path,
    namespace: &Path,
    expected_owner: Option<&str>,
    out: &mut Vec<SessionRecord>,
) -> Result<()> {
    for entry in std::fs::read_dir(namespace)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        let envelope = match read_envelope(&path) {
            Ok(Some(envelope)) => envelope,
            Ok(None) | Err(_) => continue,
        };
        let record = envelope.public_record();
        if validate_identity(&record.owner, &record.session_id).is_err()
            || expected_owner.is_some_and(|owner| record.owner != owner)
            || owner_directory(root, &record.owner) != namespace
            || owner_directory(root, &record.owner)
                .join(format!("{}.json", session_key(&record.session_id)))
                != path
        {
            continue;
        }
        out.push(record);
    }
    Ok(())
}

fn open_private_lock(path: &Path) -> Result<File> {
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if !metadata.file_type().is_file() {
            return Err(VyaneError::config(format!(
                "session lock entry {} is not a regular file",
                path.display()
            )));
        }
    }
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    set_private_file_permissions(path)?;
    Ok(file)
}

fn create_private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    set_private_file_permissions(path)?;
    Ok(file)
}

fn acquire_exclusive_lock(file: &File) -> Result<()> {
    let deadline = Instant::now() + LOCK_TIMEOUT;
    loop {
        if fs4::fs_std::FileExt::try_lock_exclusive(file)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(VyaneError::new(
                ErrorKind::Io,
                "timed out acquiring session store lock",
            ));
        }
        std::thread::sleep(LOCK_RETRY_INTERVAL);
    }
}

fn acquire_execution_lock(file: &File, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if fs4::fs_std::FileExt::try_lock_exclusive(file)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(VyaneError::new(
                ErrorKind::Conflict,
                "session already has an active execution or control mutation",
            ));
        }
        std::thread::sleep(LOCK_RETRY_INTERVAL);
    }
}

/// Release is best-effort after the protected operation has selected its
/// result. Dropping the file releases the advisory lock; an explicit unlock
/// failure must not turn an already-published mutation into a reported failure
/// that a caller might retry as though nothing committed.
fn finish_locked<T>(lock: File, result: Result<T>) -> Result<T> {
    let _ = fs4::fs_std::FileExt::unlock(&lock);
    drop(lock);
    result
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::cell::RefCell;

    use chrono::Utc;
    use vyane_core::{ModelId, Protocol, ProviderId, Target};

    use super::*;

    fn test_envelope() -> SessionEnvelope {
        let now = Utc::now();
        let mut envelope = SessionEnvelope::legacy(SessionRecord {
            session_id: "atomic".into(),
            owner: "alice".into(),
            target: Target {
                provider: ProviderId::new("provider"),
                protocol: Protocol::OpenaiChat,
                harness: None,
                model: ModelId::new("model"),
            },
            native_session_id: None,
            transcript: Vec::new(),
            created_at: now,
            updated_at: now,
            run_count: 0,
        });
        envelope.session_revision = 1;
        envelope
    }

    #[test]
    fn session_keys_do_not_alias_legacy_safe_ids() {
        assert_ne!(session_key("a/b"), session_key("a_b"));
        assert_ne!(session_key("a\\b"), session_key("a_b"));
        assert_eq!(session_key("abc"), session_key("abc"));
    }

    #[test]
    fn owner_namespaces_are_opaque_path_components() {
        let root = Path::new("sessions");
        let traversal = owner_directory(root, "../../outside");
        assert_eq!(traversal.parent(), Some(root));
        assert_eq!(traversal.file_name().unwrap().to_string_lossy().len(), 64);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborted_async_waiter_cannot_unlock_an_inflight_blocking_operation() {
        let root = tempfile::tempdir().unwrap();
        let lease = FsSessionStore::acquire_execution_lease_blocking(
            root.path(),
            "alice",
            "session",
            "run-aborted",
            EXECUTION_ADMISSION_TIMEOUT,
        )
        .unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        let operation = tokio::spawn(async move {
            lease
                .run_blocking(move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .await
        });
        tokio::task::spawn_blocking(move || {
            started_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("blocking operation did not start")
        })
        .await
        .unwrap();

        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());
        let store = FsSessionStore::new(root.path());
        let conflict = store
            .acquire_execution_lease("alice", "session", "run-conflict")
            .await
            .err()
            .expect("blocking operation must retain execution authority");
        assert_eq!(conflict.kind, ErrorKind::Conflict);

        release_tx.send(()).unwrap();
        let mut reacquired = None;
        for _ in 0..100 {
            match store
                .acquire_execution_lease("alice", "session", "run-after-abort")
                .await
            {
                Ok(lease) => {
                    reacquired = Some(lease);
                    break;
                }
                Err(error) if error.kind == ErrorKind::Conflict => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("unexpected reacquire failure: {error}"),
            }
        }
        assert!(reacquired.is_some(), "blocking authority was not released");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborted_control_waiters_never_commit_after_execution_authority_releases() {
        #[derive(Debug, Clone, Copy)]
        enum Mutation {
            Save,
            Update,
            Reset,
        }

        for mutation in [Mutation::Save, Mutation::Update, Mutation::Reset] {
            let root = tempfile::tempdir().unwrap();
            let store = FsSessionStore::new(root.path());
            let mut record = test_envelope().record;
            record.session_id = format!("abort-{mutation:?}").to_ascii_lowercase();
            store.save("alice", &record).await.unwrap();
            let lease = store
                .acquire_execution_lease("alice", &record.session_id, "active-run")
                .await
                .unwrap();
            let before = lease.load_snapshot().await.unwrap().unwrap();

            let waiting_store = FsSessionStore::new(root.path());
            let waiting_record = record.clone();
            let waiting_update = SessionUpdate {
                owner: "alice".into(),
                session_id: record.session_id.clone(),
                target: record.target.clone(),
                native_session_id: None,
                transcript_delta: vec![vyane_core::ChatMessage::user("must-not-commit")],
                occurred_at: Utc::now(),
            };
            let expected_revision = before.session_revision;
            let mut waiter = tokio::spawn(async move {
                match mutation {
                    Mutation::Save => waiting_store.save("alice", &waiting_record).await,
                    Mutation::Update => waiting_store
                        .apply_update("alice", &waiting_update)
                        .await
                        .map(|_| ()),
                    Mutation::Reset => waiting_store
                        .apply_native_transition(
                            "alice",
                            &waiting_record.session_id,
                            &NativeSessionTransition::Reset { expected_revision },
                        )
                        .await
                        .map(|_| ()),
                }
            });

            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut waiter)
                    .await
                    .is_err(),
                "{mutation:?} unexpectedly passed a live execution lease"
            );
            waiter.abort();
            assert!(waiter.await.unwrap_err().is_cancelled());
            drop(lease);

            let observer = FsSessionStore::new(root.path());
            let mut after = None;
            for _ in 0..100 {
                match observer
                    .acquire_execution_lease("alice", &record.session_id, "observer")
                    .await
                {
                    Ok(lease) => {
                        after = lease.load_snapshot().await.unwrap();
                        break;
                    }
                    Err(error) if error.kind == ErrorKind::Conflict => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("unexpected observer failure: {error}"),
                }
            }
            let after = after.expect("observer never reacquired execution authority");
            assert_eq!(after.session_revision, before.session_revision);
            assert_eq!(after.record.run_count, before.record.run_count);
            assert_eq!(after.record.transcript, before.record.transcript);
        }
    }

    #[test]
    fn post_rename_sync_failure_is_typed_indeterminate_and_requires_reload() {
        let root = tempfile::tempdir().unwrap();
        let owner_root = root.path().join("owner");
        std::fs::create_dir(&owner_root).unwrap();
        let target = owner_root.join("atomic.json");
        let envelope = test_envelope();

        let error =
            write_envelope_atomic_with_sync(&owner_root, "atomic", &target, &envelope, |_| {
                Err(std::io::Error::other("injected directory sync failure"))
            })
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Indeterminate);
        assert!(error.message.contains("reload"));
        let published = read_envelope(&target).unwrap().unwrap();
        assert_eq!(published.session_revision, 1);
        assert!(std::fs::read_dir(&owner_root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".tmp.")
        }));
    }

    #[test]
    fn first_time_directory_creation_syncs_each_new_inode_and_parent_entry() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("sessions");
        let owner = root.join("owner");
        let seen = RefCell::new(Vec::<PathBuf>::new());

        secure_directory_with_sync(&owner, &|path| {
            seen.borrow_mut().push(path.to_path_buf());
            Ok(())
        })
        .unwrap();

        let seen = seen.into_inner();
        assert!(root.is_dir());
        assert!(owner.is_dir());
        assert!(seen.contains(&root));
        assert!(seen.contains(&owner));
        assert!(seen.contains(&parent.path().to_path_buf()));
    }
}
