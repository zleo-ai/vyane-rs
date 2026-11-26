//! Same-machine session presence table.
//!
//! The table is one local JSON file. It records check-in metadata for sessions
//! that share a machine: session id, label, repo, an opaque card id, a one-line
//! status, harness name, and timestamps. It does not store prompts, tool
//! outputs, or A2A mailbox messages.
//!
//! A missing or empty file is an empty table. Unreadable JSON, a non-object
//! root, or a malformed `agents` map is corrupt: callers get an error and the
//! file is left unchanged. Lock acquisition is bounded by
//! [`DEFAULT_LOCK_TIMEOUT`] unless a caller selects another finite timeout.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use fs4::fs_std::FileExt as _;
use serde::Serialize;
use serde_json::{Map, Number, Value};
use thiserror::Error;

/// File name under the user config directory.
pub const PRESENCE_FILENAME: &str = "agent-presence.json";

/// Online window. A heartbeat every five minutes still counts after two misses.
pub const PRESENCE_TTL_SECONDS: f64 = 15.0 * 60.0;

/// Drop untouched records on the next write after this age.
pub const PRESENCE_GC_SECONDS: f64 = 24.0 * 3600.0;

/// How long a leave blocks a heartbeat from recreating that session.
pub const PRESENCE_TOMBSTONE_SECONDS: f64 = PRESENCE_TTL_SECONDS;

/// Predetermined lock wait used by the CLI and the native harness.
pub const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_millis(200);

/// Minimum gap between native post-tool heartbeats.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(300);

/// Native harness name stored on in-process check-ins.
pub const HARNESS_NATIVE: &str = "vyane-native";

/// Maximum characters accepted for one metadata field.
pub const FIELD_MAX_CHARS: usize = 256;

const MAX_TABLE_BYTES: u64 = 1024 * 1024;
const ENV_PRESENCE_PATH: &str = "VYANE_AGENT_PRESENCE_PATH";

/// Failure while reading or updating the presence table.
#[derive(Debug, Error)]
pub enum PresenceError {
    /// The session id was empty after trimming.
    #[error("session id is empty")]
    InvalidSession,
    /// A metadata field was empty-when-required, too long, or contained a control character.
    #[error("field {field} is not a single bounded metadata value")]
    InvalidField {
        /// Field name that failed validation.
        field: &'static str,
    },
    /// The caller-supplied clock was NaN or infinite.
    #[error("presence table clock is invalid")]
    InvalidTime,
    /// The default path could not be built from the environment.
    #[error("presence table path could not be resolved")]
    PathUnresolved,
    /// JSON or the agents map cannot be trusted. This is not an empty table.
    #[error("presence table is corrupt")]
    StoreCorrupt,
    /// The path is missing-as-a-file, a directory, a symlink, or otherwise unsafe to read.
    #[error("presence table is not a readable regular file")]
    StoreUnreadable,
    /// The lock was still held when the predetermined wait ended.
    #[error("presence table lock was not acquired within {timeout_ms}ms")]
    StoreBusy {
        /// Wait budget that elapsed, in milliseconds.
        timeout_ms: u128,
    },
    /// Filesystem failure that is not one of the classified table errors.
    #[error("presence table io failed")]
    Io(#[from] std::io::Error),
}

impl PresenceError {
    /// Stable snake_case token. Messages and paths stay out of the token.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidSession => "invalid_session",
            Self::InvalidField { .. } => "invalid_field",
            Self::InvalidTime => "invalid_time",
            Self::PathUnresolved => "path_unresolved",
            Self::StoreCorrupt => "store_corrupt",
            Self::StoreUnreadable => "store_unreadable",
            Self::StoreBusy { .. } => "store_busy",
            Self::Io(_) => "io",
        }
    }
}

/// Explicit check-in. Empty strings leave an existing value in place.
///
/// `card` is stored as given after trimming. It is not parsed as a board id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckIn {
    /// Session key in the table.
    pub session_id: String,
    /// Display label. Empty keeps the previous label.
    pub label: String,
    /// Repository label. Empty keeps the previous repo.
    pub repo: String,
    /// Opaque card id. Empty keeps the previous card.
    pub card: String,
    /// One-line status. Empty keeps the previous status.
    pub status: String,
    /// Harness name. Empty keeps the previous harness.
    pub harness: String,
}

/// Heartbeat. It refreshes `last_checkin` and optional identity fields.
///
/// Card and status are intentionally absent so a heartbeat cannot replace them
/// or carry a prompt or tool result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat {
    /// Session key in the table.
    pub session_id: String,
    /// Display label. Empty keeps the previous label.
    pub label: String,
    /// Repository label. Empty keeps the previous repo.
    pub repo: String,
    /// Harness name. Empty keeps the previous harness.
    pub harness: String,
}

/// Who to include when listing the table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListQuery {
    /// Session to mark as self. Empty disables the self filter.
    pub session_id: String,
    /// Include the session named by `session_id`.
    pub include_self: bool,
    /// Include sessions outside the online window.
    pub include_offline: bool,
}

/// One session as shown by list, check-in, and heartbeat.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PresenceView {
    /// Session key.
    pub session_id: String,
    /// Display label.
    pub label: String,
    /// Repository label.
    pub repo: String,
    /// Opaque card id.
    pub card: String,
    /// One-line status.
    pub status: String,
    /// Harness name.
    pub harness: String,
    /// Last successful check-in or heartbeat, when the field is a finite number.
    pub last_checkin: Option<f64>,
    /// Age in seconds, rounded to tenths, when `last_checkin` is known.
    pub age_seconds: Option<f64>,
    /// False when `last_checkin` is missing or not a finite number.
    pub age_known: bool,
    /// Online by TTL, or unknown age (unknown is not treated as absent).
    pub online: bool,
    /// True when this row is the session named by the query.
    pub is_self: bool,
}

/// Result of an explicit check-in.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CheckInOutcome {
    /// False when a leave tombstone or an older generation rejected the write.
    pub written: bool,
    /// True when a leave tombstone still blocked this check-in.
    pub blocked_by_leave: bool,
    /// True when this check-in was not newer than the current generation.
    pub stale_generation: bool,
    /// The stored row when this call wrote it.
    pub agent: Option<PresenceView>,
    /// Other online sessions after a successful write. Zero when nothing was written.
    pub online_peers: usize,
}

/// Result of a heartbeat.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HeartbeatOutcome {
    /// False when a leave tombstone or an older generation rejected the write.
    pub written: bool,
    /// The stored row when this call wrote it.
    pub agent: Option<PresenceView>,
}

/// Result of leaving the table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LeaveOutcome {
    /// True when a row was removed.
    pub removed: bool,
    /// True when a newer check-in made this leave stale.
    pub stale: bool,
}

/// Online sessions visible to one query.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PresenceList {
    /// Clock used for age and the online window.
    pub now: f64,
    /// Online window in seconds.
    pub ttl_seconds: f64,
    /// Rows after self and offline filters, newest heartbeat first.
    pub agents: Vec<PresenceView>,
}

/// JSON presence table. This is not the A2A message database.
#[derive(Debug, Clone)]
pub struct PresenceStore {
    path: PathBuf,
    lock_timeout: Duration,
}

impl PresenceStore {
    /// Open a table at `path`. The file is created on the first successful write.
    #[must_use]
    pub fn open(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            lock_timeout: DEFAULT_LOCK_TIMEOUT,
        }
    }

    /// Replace the lock wait. The wait is always finite; there is no blocking mode.
    #[must_use]
    pub fn with_lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = timeout;
        self
    }

    /// Table path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Lock-file path paired with this table. The lock file is not the table.
    pub fn lock_path(&self) -> Result<PathBuf, PresenceError> {
        presence_lock_path(&self.path)
    }

    /// Register or refresh a session, including its opaque card id and one-line status.
    pub fn check_in(&self, update: &CheckIn, now: f64) -> Result<CheckInOutcome, PresenceError> {
        let now = finite_time(now)?;
        let session_id = required_session(&update.session_id)?;
        let label = optional_field("label", &update.label)?;
        let repo = optional_field("repo", &update.repo)?;
        let card = optional_field("card", &update.card)?;
        let status = optional_field("status", &update.status)?;
        let harness = optional_field("harness", &update.harness)?;
        self.mutate(now, |table| {
            gc_tombstones(table, now);
            if let Some(outcome) = reject_stale_check_in(table, &session_id, now)? {
                return Ok(outcome);
            }
            let creating = !table.agents.contains_key(&session_id);
            let record = table
                .agents
                .entry(session_id.clone())
                .or_insert_with(|| Value::Object(Map::new()));
            let map = object_mut(record)?;
            map.insert("session_id".to_owned(), Value::String(session_id.clone()));
            set_if_some(map, "label", label.clone());
            set_if_some(map, "repo", repo.clone());
            set_if_some(map, "card", card.clone());
            set_if_some(map, "status", status.clone());
            set_if_some(map, "harness", harness.clone());
            if creating || !finite_of(map.get("first_seen")).is_some() {
                insert_f64(map, "first_seen", now)?;
            }
            insert_f64(map, "start_ts", now)?;
            insert_f64(map, "last_checkin", now)?;
            table.tombstones.remove(&session_id);
            gc_agents(table, now);
            let peers = online_others(table, &session_id, now)?;
            let agent = view_from_agents(table, &session_id, now, true)?;
            Ok(CheckInOutcome {
                written: true,
                blocked_by_leave: false,
                stale_generation: false,
                agent,
                online_peers: peers,
            })
        })
    }

    /// Refresh `last_checkin` without changing card or status.
    ///
    /// A session that recently left is not recreated. An older heartbeat does
    /// not rewind a newer check-in.
    pub fn heartbeat(
        &self,
        update: &Heartbeat,
        now: f64,
    ) -> Result<HeartbeatOutcome, PresenceError> {
        let now = finite_time(now)?;
        let session_id = required_session(&update.session_id)?;
        let label = optional_field("label", &update.label)?;
        let repo = optional_field("repo", &update.repo)?;
        let harness = optional_field("harness", &update.harness)?;
        self.mutate(now, |table| {
            gc_tombstones(table, now);
            if table.tombstones.contains_key(&session_id) && !table.agents.contains_key(&session_id)
            {
                return Ok(HeartbeatOutcome {
                    written: false,
                    agent: None,
                });
            }
            if let Some(existing) = table.agents.get(&session_id) {
                let map = match existing {
                    Value::Object(map) => map,
                    _ => return Err(PresenceError::StoreCorrupt),
                };
                if let Some(start) = finite_of(map.get("start_ts"))
                    && now < start
                {
                    return Ok(HeartbeatOutcome {
                        written: false,
                        agent: None,
                    });
                }
            }
            let creating = !table.agents.contains_key(&session_id);
            let record = table
                .agents
                .entry(session_id.clone())
                .or_insert_with(|| Value::Object(Map::new()));
            let map = object_mut(record)?;
            map.insert("session_id".to_owned(), Value::String(session_id.clone()));
            set_if_some(map, "label", label.clone());
            set_if_some(map, "repo", repo.clone());
            set_if_some(map, "harness", harness.clone());
            if creating {
                setdefault_empty(map, "card");
                setdefault_empty(map, "status");
            }
            if creating || !finite_of(map.get("first_seen")).is_some() {
                insert_f64(map, "first_seen", now)?;
            }
            insert_f64(map, "last_checkin", now)?;
            gc_agents(table, now);
            Ok(HeartbeatOutcome {
                written: true,
                agent: view_from_agents(table, &session_id, now, true)?,
            })
        })
    }

    /// Remove a session and remember the leave long enough to ignore a late heartbeat.
    pub fn leave(&self, session_id: &str, now: f64) -> Result<LeaveOutcome, PresenceError> {
        let now = finite_time(now)?;
        let session_id = required_session(session_id)?;
        self.mutate(now, |table| {
            gc_tombstones(table, now);
            if let Some(existing) = table.agents.get(&session_id) {
                let map = match existing {
                    Value::Object(map) => map,
                    _ => return Err(PresenceError::StoreCorrupt),
                };
                if let Some(start) = finite_of(map.get("start_ts"))
                    && now < start
                {
                    gc_agents(table, now);
                    return Ok(LeaveOutcome {
                        removed: false,
                        stale: true,
                    });
                }
            }
            let removed = table.agents.remove(&session_id).is_some();
            let prior = finite_of(table.tombstones.get(&session_id));
            let stamp = match prior {
                Some(prior) => prior.max(now),
                None => now,
            };
            insert_tombstone(table, &session_id, stamp)?;
            gc_agents(table, now);
            Ok(LeaveOutcome {
                removed,
                stale: false,
            })
        })
    }

    /// List sessions. A missing file is empty. A corrupt file is an error.
    pub fn list(&self, query: &ListQuery, now: f64) -> Result<PresenceList, PresenceError> {
        let now = finite_time(now)?;
        if !query.session_id.trim().is_empty() {
            let _ = required_session(&query.session_id)?;
        }
        if !table_exists(&self.path)? {
            return Ok(PresenceList {
                now,
                ttl_seconds: PRESENCE_TTL_SECONDS,
                agents: Vec::new(),
            });
        }
        let _guard = lock_table(&self.path, self.lock_timeout)?;
        let table = load_table(&self.path)?;
        let mut agents = Vec::new();
        for (sid, value) in &table.agents {
            let map = match value {
                Value::Object(map) => map,
                _ => return Err(PresenceError::StoreCorrupt),
            };
            let is_self = !query.session_id.is_empty() && sid == query.session_id.trim();
            if is_self && !query.include_self {
                continue;
            }
            let view = view_of(sid, map, now, is_self);
            if view.online || query.include_offline {
                agents.push(view);
            }
        }
        agents.sort_by(compare_views);
        Ok(PresenceList {
            now,
            ttl_seconds: PRESENCE_TTL_SECONDS,
            agents,
        })
    }

    fn mutate<T>(
        &self,
        now: f64,
        body: impl FnOnce(&mut Table) -> Result<T, PresenceError>,
    ) -> Result<T, PresenceError> {
        let parent = parent_dir(&self.path);
        fs::create_dir_all(parent)?;
        let _guard = lock_table(&self.path, self.lock_timeout)?;
        let mut table = load_table(&self.path)?;
        let before = encode(&table)?;
        let outcome = body(&mut table)?;
        gc_tombstones(&mut table, now);
        let after = encode(&table)?;
        if before != after {
            ensure_replaceable(&self.path)?;
            publish(&self.path, &after)?;
        }
        Ok(outcome)
    }
}

/// Resolve `--path`, then `VYANE_AGENT_PRESENCE_PATH`, then the user config directory.
pub fn resolve_presence_path(explicit: Option<&Path>) -> Result<PathBuf, PresenceError> {
    if let Some(path) = explicit {
        if path.as_os_str().is_empty() {
            return Err(PresenceError::InvalidField { field: "path" });
        }
        return Ok(path.to_path_buf());
    }
    if let Some(raw) = std::env::var_os(ENV_PRESENCE_PATH)
        && !raw.is_empty()
    {
        return Ok(PathBuf::from(raw));
    }
    let Some(config) = config_home() else {
        return Err(PresenceError::PathUnresolved);
    };
    Ok(config.join("vyane").join(PRESENCE_FILENAME))
}

/// Lock file that serializes read-modify-write of `table`.
pub fn presence_lock_path(table: &Path) -> Result<PathBuf, PresenceError> {
    let name = table
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or(PresenceError::InvalidField { field: "path" })?;
    let mut lock_name = name.to_os_string();
    lock_name.push(".lock");
    Ok(parent_dir(table).join(lock_name))
}

#[derive(Debug, Clone, PartialEq)]
struct Table {
    agents: Map<String, Value>,
    tombstones: Map<String, Value>,
    extra: Map<String, Value>,
}

impl Table {
    fn empty() -> Self {
        Self {
            agents: Map::new(),
            tombstones: Map::new(),
            extra: Map::new(),
        }
    }
}

fn reject_stale_check_in(
    table: &Table,
    session_id: &str,
    now: f64,
) -> Result<Option<CheckInOutcome>, PresenceError> {
    if !table.agents.contains_key(session_id) && table.tombstones.contains_key(session_id) {
        let tomb = finite_of(table.tombstones.get(session_id));
        if tomb.is_some_and(|tomb| now <= tomb) {
            return Ok(Some(CheckInOutcome {
                written: false,
                blocked_by_leave: true,
                stale_generation: false,
                agent: None,
                online_peers: 0,
            }));
        }
    }
    if let Some(existing) = table.agents.get(session_id) {
        let map = match existing {
            Value::Object(map) => map,
            _ => return Err(PresenceError::StoreCorrupt),
        };
        if let Some(start) = finite_of(map.get("start_ts"))
            && now <= start
        {
            return Ok(Some(CheckInOutcome {
                written: false,
                blocked_by_leave: false,
                stale_generation: true,
                agent: None,
                online_peers: 0,
            }));
        }
    }
    Ok(None)
}

fn online_others(table: &Table, session_id: &str, now: f64) -> Result<usize, PresenceError> {
    let mut count = 0;
    for (sid, value) in &table.agents {
        if sid == session_id {
            continue;
        }
        let map = match value {
            Value::Object(map) => map,
            _ => return Err(PresenceError::StoreCorrupt),
        };
        if view_of(sid, map, now, false).online {
            count += 1;
        }
    }
    Ok(count)
}

fn view_from_agents(
    table: &Table,
    session_id: &str,
    now: f64,
    is_self: bool,
) -> Result<Option<PresenceView>, PresenceError> {
    let Some(value) = table.agents.get(session_id) else {
        return Ok(None);
    };
    let map = match value {
        Value::Object(map) => map,
        _ => return Err(PresenceError::StoreCorrupt),
    };
    Ok(Some(view_of(session_id, map, now, is_self)))
}

fn view_of(session_id: &str, record: &Map<String, Value>, now: f64, is_self: bool) -> PresenceView {
    let last = finite_of(record.get("last_checkin"));
    let (online, age_seconds, age_known) = match last {
        Some(last) => {
            let age = (now - last).max(0.0);
            (age <= PRESENCE_TTL_SECONDS, Some(round_tenths(age)), true)
        }
        None => (true, None, false),
    };
    PresenceView {
        session_id: session_id.to_owned(),
        label: string_field(record, "label"),
        repo: string_field(record, "repo"),
        card: string_field(record, "card"),
        status: string_field(record, "status"),
        harness: string_field(record, "harness"),
        last_checkin: last,
        age_seconds,
        age_known,
        online,
        is_self,
    }
}

fn compare_views(left: &PresenceView, right: &PresenceView) -> std::cmp::Ordering {
    match (left.last_checkin, right.last_checkin) {
        (None, None) => left.session_id.cmp(&right.session_id),
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(left_last), Some(right_last)) => right_last
            .total_cmp(&left_last)
            .then_with(|| left.session_id.cmp(&right.session_id)),
    }
}

fn gc_tombstones(table: &mut Table, now: f64) {
    table.tombstones.retain(|_, value| {
        finite_of(Some(value)).is_some_and(|stamp| now - stamp <= PRESENCE_TOMBSTONE_SECONDS)
    });
}

fn gc_agents(table: &mut Table, now: f64) {
    table.agents.retain(|_, value| match value {
        Value::Object(map) => match finite_of(map.get("last_checkin")) {
            Some(last) => now - last <= PRESENCE_GC_SECONDS,
            None => true,
        },
        _ => true,
    });
}

fn insert_tombstone(table: &mut Table, session_id: &str, stamp: f64) -> Result<(), PresenceError> {
    let number = Number::from_f64(stamp).ok_or(PresenceError::InvalidTime)?;
    table
        .tombstones
        .insert(session_id.to_owned(), Value::Number(number));
    Ok(())
}

fn set_if_some(record: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        record.insert(key.to_owned(), Value::String(value));
    } else if !record.contains_key(key) {
        record.insert(key.to_owned(), Value::String(String::new()));
    }
}

fn setdefault_empty(record: &mut Map<String, Value>, key: &str) {
    if !record.contains_key(key) {
        record.insert(key.to_owned(), Value::String(String::new()));
    }
}

fn insert_f64(record: &mut Map<String, Value>, key: &str, value: f64) -> Result<(), PresenceError> {
    let number = Number::from_f64(value).ok_or(PresenceError::InvalidTime)?;
    record.insert(key.to_owned(), Value::Number(number));
    Ok(())
}

fn object_mut(value: &mut Value) -> Result<&mut Map<String, Value>, PresenceError> {
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(PresenceError::StoreCorrupt),
    }
}

fn string_field(record: &Map<String, Value>, key: &str) -> String {
    record
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned()
}

fn finite_of(value: Option<&Value>) -> Option<f64> {
    value
        .and_then(Value::as_f64)
        .filter(|number| number.is_finite())
}

fn finite_time(now: f64) -> Result<f64, PresenceError> {
    if now.is_finite() {
        Ok(now)
    } else {
        Err(PresenceError::InvalidTime)
    }
}

pub(crate) fn validate_check_in(update: &CheckIn) -> Result<(), PresenceError> {
    let _ = required_session(&update.session_id)?;
    let _ = optional_field("label", &update.label)?;
    let _ = optional_field("repo", &update.repo)?;
    let _ = optional_field("card", &update.card)?;
    let _ = optional_field("status", &update.status)?;
    let _ = optional_field("harness", &update.harness)?;
    Ok(())
}

fn required_session(value: &str) -> Result<String, PresenceError> {
    optional_field("session_id", value)?.ok_or(PresenceError::InvalidSession)
}

fn optional_field(field: &'static str, value: &str) -> Result<Option<String>, PresenceError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.chars().count() > FIELD_MAX_CHARS || trimmed.chars().any(char::is_control) {
        return Err(PresenceError::InvalidField { field });
    }
    Ok(Some(trimmed.to_owned()))
}

fn round_tenths(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

fn parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

fn config_home() -> Option<PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME")
        && !xdg.is_empty()
    {
        return Some(PathBuf::from(xdg));
    }
    dirs::config_dir()
}

fn table_exists(path: &Path) -> Result<bool, PresenceError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_file() => Ok(true),
        Ok(_) => Err(PresenceError::StoreUnreadable),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(PresenceError::Io(error)),
    }
}

fn ensure_replaceable(path: &Path) -> Result<(), PresenceError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_file() => Ok(()),
        Ok(_) => Err(PresenceError::StoreUnreadable),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(PresenceError::Io(error)),
    }
}

fn load_table(path: &Path) -> Result<Table, PresenceError> {
    let Some(file) = open_read(path)? else {
        return Ok(Table::empty());
    };
    let meta = file.metadata()?;
    if !meta.is_file() || meta.len() > MAX_TABLE_BYTES {
        return Err(if meta.is_file() {
            PresenceError::StoreCorrupt
        } else {
            PresenceError::StoreUnreadable
        });
    }
    let mut bytes = Vec::new();
    file.take(MAX_TABLE_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_TABLE_BYTES {
        return Err(PresenceError::StoreCorrupt);
    }
    parse_table(&bytes)
}

fn parse_table(bytes: &[u8]) -> Result<Table, PresenceError> {
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(Table::empty());
    }
    let value: Value = serde_json::from_slice(bytes).map_err(|_| PresenceError::StoreCorrupt)?;
    let Value::Object(mut root) = value else {
        return Err(PresenceError::StoreCorrupt);
    };
    let agents = match root.remove("agents") {
        None => Map::new(),
        Some(Value::Object(map)) => {
            if map.values().any(|value| !value.is_object()) {
                return Err(PresenceError::StoreCorrupt);
            }
            map
        }
        Some(_) => return Err(PresenceError::StoreCorrupt),
    };
    let tombstones = match root.remove("tombstones") {
        None => Map::new(),
        Some(Value::Object(map)) => map,
        Some(_) => return Err(PresenceError::StoreCorrupt),
    };
    Ok(Table {
        agents,
        tombstones,
        extra: root,
    })
}

fn encode(table: &Table) -> Result<Vec<u8>, PresenceError> {
    let mut root = Map::new();
    root.insert("agents".to_owned(), Value::Object(table.agents.clone()));
    if !table.tombstones.is_empty() {
        root.insert(
            "tombstones".to_owned(),
            Value::Object(table.tombstones.clone()),
        );
    }
    let mut extra = BTreeMap::new();
    for (key, value) in &table.extra {
        if key != "agents" && key != "tombstones" {
            extra.insert(key.clone(), value.clone());
        }
    }
    for (key, value) in extra {
        root.insert(key, value);
    }
    let mut bytes =
        serde_json::to_vec_pretty(&Value::Object(root)).map_err(|_| PresenceError::StoreCorrupt)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn publish(path: &Path, bytes: &[u8]) -> Result<(), PresenceError> {
    let mut tmp = tempfile::NamedTempFile::new_in(parent_dir(path))?;
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).map_err(|error| error.error)?;
    Ok(())
}

struct LockGuard {
    file: File,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = fs4::fs_std::FileExt::unlock(&self.file);
    }
}

fn lock_table(path: &Path, timeout: Duration) -> Result<LockGuard, PresenceError> {
    let lock_path = presence_lock_path(path)?;
    let file = open_lock(&lock_path)?;
    let deadline = Instant::now() + timeout;
    loop {
        match file.try_lock_exclusive() {
            Ok(true) => return Ok(LockGuard { file }),
            Ok(false) => {
                if Instant::now() >= deadline {
                    return Err(PresenceError::StoreBusy {
                        timeout_ms: timeout.as_millis(),
                    });
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(PresenceError::StoreBusy {
                        timeout_ms: timeout.as_millis(),
                    });
                }
                thread::sleep(Duration::from_millis(10).min(remaining));
            }
            Err(error) => return Err(PresenceError::Io(error)),
        }
    }
}

#[cfg(unix)]
fn open_read(path: &Path) -> Result<Option<File>, PresenceError> {
    use rustix::fs::{Mode, OFlags, open};

    match open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(fd) => Ok(Some(File::from(fd))),
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(rustix::io::Errno::LOOP)
        | Err(rustix::io::Errno::ISDIR)
        | Err(rustix::io::Errno::NXIO)
        | Err(rustix::io::Errno::NOTDIR) => Err(PresenceError::StoreUnreadable),
        Err(error) => Err(PresenceError::Io(std::io::Error::from(error))),
    }
}

#[cfg(not(unix))]
fn open_read(path: &Path) -> Result<Option<File>, PresenceError> {
    match File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(PresenceError::Io(error)),
    }
}

#[cfg(unix)]
fn open_lock(path: &Path) -> Result<File, PresenceError> {
    use rustix::fs::{Mode, OFlags, open};

    match open(
        path,
        OFlags::RDWR | OFlags::CREATE | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::from_bits_retain(0o600),
    ) {
        Ok(fd) => {
            let file = File::from(fd);
            let meta = file.metadata()?;
            if meta.is_file() {
                Ok(file)
            } else {
                Err(PresenceError::StoreUnreadable)
            }
        }
        Err(rustix::io::Errno::LOOP)
        | Err(rustix::io::Errno::ISDIR)
        | Err(rustix::io::Errno::NXIO)
        | Err(rustix::io::Errno::NOTDIR) => Err(PresenceError::StoreUnreadable),
        Err(error) => Err(PresenceError::Io(std::io::Error::from(error))),
    }
}

#[cfg(not(unix))]
fn open_lock(path: &Path) -> Result<File, PresenceError> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(PresenceError::Io)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::time::Instant;

    fn store(dir: &tempfile::TempDir) -> PresenceStore {
        PresenceStore::open(dir.path().join("agent-presence.json"))
    }

    fn check(session: &str, card: &str, status: &str) -> CheckIn {
        CheckIn {
            session_id: session.to_owned(),
            label: session.to_owned(),
            repo: "vyane-rs".to_owned(),
            card: card.to_owned(),
            status: status.to_owned(),
            harness: HARNESS_NATIVE.to_owned(),
        }
    }

    fn beat(session: &str) -> Heartbeat {
        Heartbeat {
            session_id: session.to_owned(),
            label: String::new(),
            repo: String::new(),
            harness: String::new(),
        }
    }

    #[test]
    fn two_sessions_see_each_other() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store
            .check_in(&check("alpha", "opaque-card", "editing"), 1_000.0)
            .unwrap();
        let beta = store
            .check_in(&check("beta", "other-card", "reading"), 1_001.0)
            .unwrap();
        assert_eq!(beta.online_peers, 1);
        assert_eq!(beta.agent.unwrap().card, "other-card");

        let alpha_sees = store
            .list(
                &ListQuery {
                    session_id: "alpha".to_owned(),
                    include_self: false,
                    include_offline: false,
                },
                1_002.0,
            )
            .unwrap();
        assert_eq!(alpha_sees.agents.len(), 1);
        assert_eq!(alpha_sees.agents[0].session_id, "beta");
        assert_eq!(alpha_sees.agents[0].card, "other-card");

        let beta_sees = store
            .list(
                &ListQuery {
                    session_id: "beta".to_owned(),
                    include_self: false,
                    include_offline: false,
                },
                1_002.0,
            )
            .unwrap();
        assert_eq!(beta_sees.agents[0].session_id, "alpha");
        assert_eq!(beta_sees.agents[0].card, "opaque-card");
    }

    #[test]
    fn heartbeat_keeps_card_and_status_and_leave_blocks_revival() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store
            .check_in(&check("alpha", "card-kept", "status-kept"), 2_000.0)
            .unwrap();
        let refreshed = store.heartbeat(&beat("alpha"), 2_100.0).unwrap();
        let agent = refreshed.agent.unwrap();
        assert_eq!(agent.card, "card-kept");
        assert_eq!(agent.status, "status-kept");
        assert_eq!(agent.last_checkin, Some(2_100.0));

        let left = store.leave("alpha", 2_200.0).unwrap();
        assert!(left.removed);
        let stale_beat = store.heartbeat(&beat("alpha"), 2_201.0).unwrap();
        assert!(!stale_beat.written);
        let listed = store.list(&ListQuery::default(), 2_202.0).unwrap();
        assert!(listed.agents.is_empty());

        let revived = store
            .check_in(&check("alpha", "card-kept", "status-kept"), 2_300.0)
            .unwrap();
        assert!(revived.written);
        assert_eq!(
            store
                .list(&ListQuery::default(), 2_301.0)
                .unwrap()
                .agents
                .len(),
            1
        );
    }

    #[test]
    fn card_is_stored_as_an_opaque_string() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let card = "任意/card-id";
        store
            .check_in(&check("alpha", card, "editing"), 10.0)
            .unwrap();
        let listed = store.list(&ListQuery::default(), 11.0).unwrap();
        assert_eq!(listed.agents[0].card, card);
    }

    #[test]
    fn metadata_does_not_accept_a_prompt_or_tool_output() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let prompt = format!("prompt\n{}", "x".repeat(FIELD_MAX_CHARS));
        let error = store
            .check_in(
                &CheckIn {
                    session_id: "alpha".to_owned(),
                    label: String::new(),
                    repo: String::new(),
                    card: String::new(),
                    status: prompt,
                    harness: String::new(),
                },
                10.0,
            )
            .unwrap_err();
        assert_eq!(error.as_str(), "invalid_field");
        assert!(!dir.path().join("agent-presence.json").exists());
    }

    #[test]
    fn corrupt_table_fails_and_is_not_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent-presence.json");
        let original = b"{not json";
        fs::write(&path, original).unwrap();
        let store = PresenceStore::open(&path);
        let listed = store.list(&ListQuery::default(), 10.0).unwrap_err();
        assert_eq!(listed.as_str(), "store_corrupt");
        let checked = store
            .check_in(&check("alpha", "card", "status"), 10.0)
            .unwrap_err();
        assert_eq!(checked.as_str(), "store_corrupt");
        assert_eq!(fs::read(&path).unwrap(), original);

        fs::write(&path, b"{\"agents\":[]}").unwrap();
        let wrong_type = store.list(&ListQuery::default(), 10.0).unwrap_err();
        assert_eq!(wrong_type.as_str(), "store_corrupt");
        assert_eq!(fs::read(&path).unwrap(), b"{\"agents\":[]}");
    }

    #[test]
    fn empty_and_missing_tables_list_nobody() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent-presence.json");
        let store = PresenceStore::open(&path);
        assert!(
            store
                .list(&ListQuery::default(), 10.0)
                .unwrap()
                .agents
                .is_empty()
        );
        fs::write(&path, b" \n").unwrap();
        assert!(
            store
                .list(&ListQuery::default(), 10.0)
                .unwrap()
                .agents
                .is_empty()
        );
    }

    #[test]
    fn directory_table_is_unreadable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent-presence.json");
        fs::create_dir(&path).unwrap();
        let error = PresenceStore::open(&path)
            .list(&ListQuery::default(), 10.0)
            .unwrap_err();
        assert_eq!(error.as_str(), "store_unreadable");
    }

    #[test]
    fn lock_timeout_returns_within_the_budget() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent-presence.json");
        let store = PresenceStore::open(&path).with_lock_timeout(Duration::from_millis(150));
        fs::write(&path, b"{\"agents\":{}}\n").unwrap();
        let lock_path = store.lock_path().unwrap();
        let held = open_lock(&lock_path).unwrap();
        assert!(fs4::fs_std::FileExt::try_lock_exclusive(&held).unwrap());
        let started = Instant::now();
        let error = store.list(&ListQuery::default(), 10.0).unwrap_err();
        let elapsed = started.elapsed();
        drop(held);
        assert_eq!(error.as_str(), "store_busy");
        assert!(
            elapsed < Duration::from_secs(1),
            "lock wait took {elapsed:?}, budget was 150ms"
        );
    }

    #[test]
    fn unknown_sibling_keys_stay_on_the_presence_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent-presence.json");
        fs::write(
            &path,
            r#"{"agents":{"alpha":{"session_id":"alpha","card":"kept","status":"kept","last_checkin":10,"pid":4}},"leases":{"card":1},"messages":[{"id":"not-an-inbox"}]}"#,
        )
        .unwrap();
        let store = PresenceStore::open(&path);
        store.heartbeat(&beat("alpha"), 20.0).unwrap();
        let value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(value["agents"]["alpha"]["card"], "kept");
        assert_eq!(value["agents"]["alpha"]["status"], "kept");
        assert_eq!(value["agents"]["alpha"]["pid"], 4);
        assert_eq!(value["leases"]["card"], 1);
        assert_eq!(value["messages"][0]["id"], "not-an-inbox");
        assert!(value.get("prompt").is_none());
    }

    #[test]
    fn stale_leave_does_not_remove_a_newer_check_in() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store
            .check_in(&check("alpha", "card", "live"), 5_000.0)
            .unwrap();
        let left = store.leave("alpha", 4_000.0).unwrap();
        assert!(left.stale);
        assert!(!left.removed);
        assert_eq!(
            store
                .list(&ListQuery::default(), 5_001.0)
                .unwrap()
                .agents
                .len(),
            1
        );
    }

    #[test]
    fn concurrent_check_ins_both_remain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent-presence.json");
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for name in ["alpha", "beta"] {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            let name = name.to_owned();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                PresenceStore::open(path)
                    .check_in(&check(&name, &name, "live"), 100.0)
                    .unwrap();
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let listed = PresenceStore::open(path)
            .list(&ListQuery::default(), 101.0)
            .unwrap();
        let mut ids: Vec<_> = listed
            .agents
            .iter()
            .map(|agent| agent.session_id.as_str())
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, ["alpha", "beta"]);
    }

    #[cfg(unix)]
    #[test]
    fn fifo_table_fails_without_hanging() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent-presence.json");
        let status = std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap();
        assert!(status.success());
        let started = Instant::now();
        let error = PresenceStore::open(&path)
            .list(&ListQuery::default(), 10.0)
            .unwrap_err();
        assert_eq!(error.as_str(), "store_unreadable");
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
