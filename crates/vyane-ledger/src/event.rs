//! Durable, owner-scoped event streams for tasks, workflows, harnesses, and brokers.
//!
//! Each `(owner, stream id)` pair owns one append-only JSONL file under an
//! opaque owner-digest namespace. Writers serialize through a private advisory
//! lock, allocate a monotonically increasing sequence while holding that lock,
//! and append one complete line. Readers are transport-independent: HTTP SSE,
//! MCP, a TUI, or a daemon can replay the same records after an acknowledged
//! sequence without becoming the source of truth.
//!
//! This store is for bounded, non-secret control-plane metadata. Raw prompts,
//! model token deltas, tool output bodies, credentials, and artifacts belong in
//! separate ephemeral or retention-governed stores.
//! Delivery is at least once: an I/O error returned after a write may be an
//! ambiguous commit. Reuse [`NewEvent::event_id`] when retrying and make
//! consumers deduplicate that stable id. Transactional task/message stores,
//! rather than this projection, remain the source of truth.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use uuid::Uuid;

const EVENT_SCHEMA: u32 = 1;
const MAX_STREAM_ID_BYTES: usize = 128;
const MAX_OWNER_BYTES: usize = 256;
const MAX_EVENT_TYPE_BYTES: usize = 128;
const MAX_REFERENCE_BYTES: usize = 256;
const MAX_SUMMARY_BYTES: usize = 512;
const MAX_PAYLOAD_ENTRIES: usize = 128;
const MAX_PAYLOAD_KEY_BYTES: usize = 128;
const MAX_PAYLOAD_BYTES: usize = 16 * 1024;
const MAX_READ_LIMIT: usize = 10_000;
const MAX_EVENT_LINE_BYTES: usize = 96 * 1024;
const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_ALLOCATOR_BYTES: u64 = 4 * 1024;
const ALLOCATOR_SCHEMA: u32 = 1;
const LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AllocatorState {
    schema: u32,
    event_schema: u32,
    sequence: u64,
    file_len: u64,
    checksum: String,
}

impl AllocatorState {
    fn new(sequence: u64, file_len: u64) -> Self {
        Self {
            schema: ALLOCATOR_SCHEMA,
            event_schema: EVENT_SCHEMA,
            sequence,
            file_len,
            checksum: allocator_checksum(sequence, file_len),
        }
    }

    fn checksum_is_valid(&self) -> bool {
        self.checksum == allocator_checksum(self.sequence, self.file_len)
    }
}

#[derive(Deserialize)]
struct SchemaHeader {
    schema: u64,
}

/// Broad event family. Concrete semantics live in [`EventRecord::event_type`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventCategory {
    Lifecycle,
    Model,
    Tool,
    Approval,
    Collaboration,
    Error,
    System,
}

/// Component that produced an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSource {
    Kernel,
    Workflow,
    Harness,
    Daemon,
    Broker,
    User,
    System,
}

/// Whether one append must reach the filesystem before it is acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventDurability {
    /// Flush for same-host readers but do not force a disk sync. Intended for
    /// reconstructable progress metadata that may be lost in an OS crash. This
    /// does not authorize persisting raw model or tool content.
    Buffered,
    /// Flush and `sync_data` before returning; newly created directory entries
    /// are also synced on Unix. Required for lifecycle, approval, terminal, and
    /// other control-plane facts.
    Durable,
}

/// Caller-supplied event fields. Sequence and timestamp are assigned by the log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewEvent {
    /// Stable idempotency identity. Clone and reuse this event on append retry.
    pub event_id: String,
    pub owner: String,
    pub category: EventCategory,
    pub event_type: String,
    pub source: EventSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub payload: BTreeMap<String, serde_json::Value>,
}

impl NewEvent {
    /// Construct a minimal event. Optional correlation and payload fields can
    /// be filled before append.
    #[must_use]
    pub fn new(
        owner: impl Into<String>,
        category: EventCategory,
        event_type: impl Into<String>,
        source: EventSource,
    ) -> Self {
        Self {
            event_id: Uuid::now_v7().to_string(),
            owner: owner.into(),
            category,
            event_type: event_type.into(),
            source,
            trace_id: None,
            correlation_id: None,
            summary: None,
            payload: BTreeMap::new(),
        }
    }

    fn validate(&self) -> EventResult<()> {
        validate_text("event_id", &self.event_id, MAX_REFERENCE_BYTES)?;
        validate_text("owner", &self.owner, MAX_OWNER_BYTES)?;
        validate_event_type(&self.event_type)?;
        validate_optional_text("trace_id", self.trace_id.as_deref(), MAX_REFERENCE_BYTES)?;
        validate_optional_text(
            "correlation_id",
            self.correlation_id.as_deref(),
            MAX_REFERENCE_BYTES,
        )?;
        validate_optional_text("summary", self.summary.as_deref(), MAX_SUMMARY_BYTES)?;
        if self.payload.len() > MAX_PAYLOAD_ENTRIES {
            return Err(EventLogError::InvalidInput(format!(
                "event payload has more than {MAX_PAYLOAD_ENTRIES} entries"
            )));
        }
        for key in self.payload.keys() {
            validate_text("payload key", key, MAX_PAYLOAD_KEY_BYTES)?;
        }
        let bytes = serde_json::to_vec(&self.payload).map_err(EventLogError::Serialize)?;
        if bytes.len() > MAX_PAYLOAD_BYTES {
            return Err(EventLogError::InvalidInput(format!(
                "event payload exceeds {MAX_PAYLOAD_BYTES} bytes"
            )));
        }
        Ok(())
    }
}

/// One persisted at-least-once projection event in a stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventRecord {
    pub schema: u32,
    pub event_id: String,
    pub stream_id: String,
    pub sequence: u64,
    pub occurred_at: DateTime<Utc>,
    pub owner: String,
    pub category: EventCategory,
    pub event_type: String,
    pub source: EventSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub payload: BTreeMap<String, serde_json::Value>,
}

impl EventRecord {
    fn validate_for_stream(&self, owner: &str, stream_id: &str) -> EventResult<()> {
        if self.schema != EVENT_SCHEMA {
            return Err(EventLogError::CorruptRecord);
        }
        validate_stream_id(&self.stream_id).map_err(|_| EventLogError::CorruptRecord)?;
        if self.owner != owner || self.stream_id != stream_id || self.sequence == 0 {
            return Err(EventLogError::CorruptRecord);
        }
        NewEvent {
            event_id: self.event_id.clone(),
            owner: self.owner.clone(),
            category: self.category,
            event_type: self.event_type.clone(),
            source: self.source,
            trace_id: self.trace_id.clone(),
            correlation_id: self.correlation_id.clone(),
            summary: self.summary.clone(),
            payload: self.payload.clone(),
        }
        .validate()
        .map_err(|_| EventLogError::CorruptRecord)
    }
}

/// Replay position returned by [`EventLog::read_from`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventCursor {
    pub sequence: u64,
    pub byte_offset: u64,
    /// Binds this position to one `(owner, stream id)` pair. This is an
    /// integrity check, not an authorization token.
    pub stream_digest: String,
}

/// Replay page plus corruption diagnostics.
#[derive(Debug, Clone, PartialEq)]
pub struct EventPage {
    pub events: Vec<EventRecord>,
    pub skipped_lines: u64,
    pub next_sequence: u64,
    pub next_cursor: EventCursor,
    /// More bytes remain after `next_cursor`; callers should continue even when
    /// this page contains fewer events than the requested count.
    pub has_more: bool,
}

/// Owner-private directory containing isolated append-only event streams.
#[derive(Debug, Clone)]
pub struct EventLog {
    root: PathBuf,
}

impl EventLog {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Append one event and return the exact persisted envelope.
    pub async fn append(
        &self,
        stream_id: &str,
        event: NewEvent,
        durability: EventDurability,
    ) -> EventResult<EventRecord> {
        validate_stream_id(stream_id)?;
        event.validate()?;
        let root = self.root.clone();
        let owner = event.owner.clone();
        let stream_id = stream_id.to_string();
        tokio::task::spawn_blocking(move || {
            append_blocking(&root, &owner, &stream_id, event, durability)
        })
        .await
        .map_err(EventLogError::Join)?
    }

    /// Replay one owner's valid, monotonically increasing events after
    /// `after_sequence`. Corrupt, mismatched, duplicate, and regressing rows are
    /// skipped.
    pub async fn read_after(
        &self,
        owner: &str,
        stream_id: &str,
        after_sequence: u64,
        limit: usize,
    ) -> EventResult<EventPage> {
        self.read_from(
            owner,
            stream_id,
            EventCursor {
                sequence: after_sequence,
                byte_offset: 0,
                stream_digest: stream_digest(owner, stream_id),
            },
            limit,
        )
        .await
    }

    /// Replay from a byte cursor returned by a prior page without rescanning
    /// the beginning of the stream. The corresponding sequence remains part of
    /// the cursor so duplicate or regressing records still fail closed.
    pub async fn read_from(
        &self,
        owner: &str,
        stream_id: &str,
        cursor: EventCursor,
        limit: usize,
    ) -> EventResult<EventPage> {
        validate_text("owner", owner, MAX_OWNER_BYTES)?;
        validate_stream_id(stream_id)?;
        let expected_digest = stream_digest(owner, stream_id);
        if (cursor.sequence != 0 || cursor.byte_offset != 0)
            && cursor.stream_digest != expected_digest
        {
            return Err(EventLogError::InvalidInput(
                "event cursor belongs to a different stream".into(),
            ));
        }
        let cursor = EventCursor {
            stream_digest: expected_digest,
            ..cursor
        };
        if !(1..=MAX_READ_LIMIT).contains(&limit) {
            return Err(EventLogError::InvalidInput(format!(
                "event read limit must be between 1 and {MAX_READ_LIMIT}"
            )));
        }
        let root = self.root.clone();
        let owner = owner.to_string();
        let stream_id = stream_id.to_string();
        tokio::task::spawn_blocking(move || {
            read_from_blocking(&root, &owner, &stream_id, cursor, limit)
        })
        .await
        .map_err(EventLogError::Join)?
    }
}

#[derive(Debug, Error)]
pub enum EventLogError {
    #[error("invalid event input: {0}")]
    InvalidInput(String),
    #[error("event storage error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialize event: {0}")]
    Serialize(serde_json::Error),
    #[error("join event storage operation: {0}")]
    Join(tokio::task::JoinError),
    #[error("event stream schema {found} is not supported (maximum {supported})")]
    UnsupportedSchema { found: u64, supported: u32 },
    #[error("timed out acquiring the event stream {mode} lock")]
    LockTimeout { mode: &'static str },
    #[error("corrupt event record")]
    CorruptRecord,
}

pub type EventResult<T> = Result<T, EventLogError>;

fn acquire_exclusive_lock(file: &File) -> EventResult<()> {
    acquire_lock(file, "exclusive", |file| {
        fs4::fs_std::FileExt::try_lock_exclusive(file)
    })
}

fn acquire_shared_lock(file: &File) -> EventResult<()> {
    acquire_lock(file, "shared", |file| {
        fs4::fs_std::FileExt::try_lock_shared(file)
    })
}

fn acquire_lock(
    file: &File,
    mode: &'static str,
    try_lock: impl Fn(&File) -> std::io::Result<bool>,
) -> EventResult<()> {
    let deadline = Instant::now() + LOCK_TIMEOUT;
    loop {
        if try_lock(file)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(EventLogError::LockTimeout { mode });
        }
        std::thread::sleep(LOCK_RETRY_INTERVAL);
    }
}

fn append_blocking(
    root: &Path,
    owner: &str,
    stream_id: &str,
    event: NewEvent,
    durability: EventDurability,
) -> EventResult<EventRecord> {
    let root_created = secure_directory(root)?;
    let owner_root = owner_directory(root, owner);
    let owner_created = secure_directory(&owner_root)?;
    let lock_path = lock_path(root, owner, stream_id);
    let lock_created = !lock_path.exists();
    let mut lock = open_private(&lock_path, false)?;
    acquire_exclusive_lock(&lock)?;
    let result = (|| {
        let path = event_path(root, owner, stream_id);
        let event_created = !path.exists();
        let mut file = open_private(&path, true)?;
        ensure_line_boundary(&mut file, durability)?;
        let file_len = file.metadata()?.len();
        let allocator = read_allocator_state(&mut lock)?;
        let maximum = match allocator {
            Some(state) if state.file_len == file_len => state.sequence,
            Some(state) => state
                .sequence
                .max(max_valid_sequence(&path, owner, stream_id)?),
            None => max_valid_sequence(&path, owner, stream_id)?,
        };
        let next = maximum
            .checked_add(1)
            .ok_or_else(|| EventLogError::InvalidInput("event sequence overflow".into()))?;
        let record = EventRecord {
            schema: EVENT_SCHEMA,
            event_id: event.event_id,
            stream_id: stream_id.to_string(),
            sequence: next,
            occurred_at: Utc::now(),
            owner: event.owner,
            category: event.category,
            event_type: event.event_type,
            source: event.source,
            trace_id: event.trace_id,
            correlation_id: event.correlation_id,
            summary: event.summary,
            payload: event.payload,
        };
        let mut line = serde_json::to_vec(&record).map_err(EventLogError::Serialize)?;
        line.push(b'\n');
        let line_len = u64::try_from(line.len())
            .map_err(|_| EventLogError::InvalidInput("event record is too large".into()))?;
        let expected_file_len = file_len
            .checked_add(line_len)
            .ok_or_else(|| EventLogError::InvalidInput("event file length overflow".into()))?;
        write_allocator_state(
            &mut lock,
            AllocatorState::new(next, expected_file_len),
            durability,
        )?;
        file.write_all(&line)?;
        file.flush()?;
        if durability == EventDurability::Durable {
            file.sync_data()?;
            if event_created || lock_created || owner_created {
                sync_directory(&owner_root)?;
            }
            if owner_created {
                sync_directory(root)?;
            }
            if root_created {
                if let Some(parent) = root.parent() {
                    if parent.as_os_str().is_empty() {
                        sync_directory(Path::new("."))?;
                    } else {
                        sync_directory(parent)?;
                    }
                }
            }
        }
        Ok(record)
    })();
    let unlock = fs4::fs_std::FileExt::unlock(&lock).map_err(EventLogError::Io);
    match (result, unlock) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(record), Ok(())) => Ok(record),
    }
}

fn read_allocator_state(file: &mut File) -> EventResult<Option<AllocatorState>> {
    file.seek(SeekFrom::Start(0))?;
    if file.metadata()?.len() > MAX_ALLOCATOR_BYTES {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    let header = match serde_json::from_slice::<SchemaHeader>(&bytes) {
        Ok(header) => header,
        Err(_) => return Ok(None),
    };
    if header.schema != u64::from(ALLOCATOR_SCHEMA) {
        return Err(EventLogError::UnsupportedSchema {
            found: header.schema,
            supported: ALLOCATOR_SCHEMA,
        });
    }
    let state = match serde_json::from_slice::<AllocatorState>(&bytes) {
        Ok(state) => state,
        Err(_) => return Ok(None),
    };
    if state.event_schema != EVENT_SCHEMA {
        return Err(EventLogError::UnsupportedSchema {
            found: u64::from(state.event_schema),
            supported: EVENT_SCHEMA,
        });
    }
    if !state.checksum_is_valid() {
        return Ok(None);
    }
    Ok(Some(state))
}

fn write_allocator_state(
    file: &mut File,
    state: AllocatorState,
    durability: EventDurability,
) -> EventResult<()> {
    let mut bytes = serde_json::to_vec(&state).map_err(EventLogError::Serialize)?;
    bytes.push(b'\n');
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&bytes)?;
    file.flush()?;
    if durability == EventDurability::Durable {
        file.sync_data()?;
    }
    Ok(())
}

fn ensure_line_boundary(file: &mut File, durability: EventDurability) -> EventResult<()> {
    let length = file.metadata()?.len();
    if length == 0 {
        return Ok(());
    }
    file.seek(SeekFrom::End(-1))?;
    let mut last = [0_u8; 1];
    file.read_exact(&mut last)?;
    if last[0] != b'\n' {
        file.write_all(b"\n")?;
        file.flush()?;
        if durability == EventDurability::Durable {
            file.sync_data()?;
        }
    }
    Ok(())
}

fn read_from_blocking(
    root: &Path,
    owner: &str,
    stream_id: &str,
    cursor: EventCursor,
    limit: usize,
) -> EventResult<EventPage> {
    let path = event_path(root, owner, stream_id);
    let mut file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(EventPage {
                events: Vec::new(),
                skipped_lines: 0,
                next_sequence: cursor.sequence,
                next_cursor: cursor,
                has_more: false,
            });
        }
        Err(error) => return Err(error.into()),
    };
    let lock_path = lock_path(root, owner, stream_id);
    let lock = open_private(&lock_path, false)?;
    acquire_shared_lock(&lock)?;
    let result = (|| {
        let stream_len = validate_cursor_boundary(&mut file, cursor.byte_offset)?;
        file.seek(SeekFrom::Start(cursor.byte_offset))?;
        let mut reader = BufReader::new(file);
        let mut line = Vec::with_capacity(MAX_EVENT_LINE_BYTES.min(8 * 1024));
        let mut events = Vec::new();
        let mut skipped_lines = 0_u64;
        let mut sequence = cursor.sequence;
        let mut byte_offset = cursor.byte_offset;
        let mut page_bytes = 0_usize;
        loop {
            let line_start = byte_offset;
            let (present, terminated, overflowed, consumed) =
                read_bounded_line(&mut reader, &mut line)?;
            if !present {
                break;
            }
            // A newline is the append commit boundary. A failed writer may
            // leave a partial JSON tail; do not return it or advance past it.
            if !terminated {
                break;
            }
            byte_offset = byte_offset.checked_add(consumed).ok_or_else(|| {
                EventLogError::InvalidInput("event cursor offset overflow".into())
            })?;
            if overflowed {
                skipped_lines = skipped_lines.saturating_add(1);
                continue;
            }
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let record = match decode_record(&line) {
                Ok(record) => record,
                Err(EventLogError::UnsupportedSchema { found, supported }) => {
                    return Err(EventLogError::UnsupportedSchema { found, supported });
                }
                Err(_) => {
                    skipped_lines = skipped_lines.saturating_add(1);
                    continue;
                }
            };
            if record.validate_for_stream(owner, stream_id).is_err() {
                skipped_lines = skipped_lines.saturating_add(1);
                continue;
            }
            if record.sequence <= cursor.sequence && cursor.byte_offset == 0 {
                continue;
            }
            if record.sequence <= sequence {
                skipped_lines = skipped_lines.saturating_add(1);
                continue;
            }
            if page_bytes.saturating_add(line.len()) > MAX_PAGE_BYTES {
                byte_offset = line_start;
                break;
            }
            page_bytes = page_bytes.saturating_add(line.len());
            sequence = record.sequence;
            events.push(record);
            if events.len() == limit {
                break;
            }
        }
        Ok(EventPage {
            events,
            skipped_lines,
            next_sequence: sequence,
            next_cursor: EventCursor {
                sequence,
                byte_offset,
                stream_digest: cursor.stream_digest,
            },
            has_more: byte_offset < stream_len,
        })
    })();
    let unlock = fs4::fs_std::FileExt::unlock(&lock).map_err(EventLogError::Io);
    match (result, unlock) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(page), Ok(())) => Ok(page),
    }
}

fn validate_cursor_boundary(file: &mut File, byte_offset: u64) -> EventResult<u64> {
    let file_len = file.metadata()?.len();
    if byte_offset > file_len {
        return Err(EventLogError::InvalidInput(
            "event cursor is past the end of the stream".into(),
        ));
    }
    if byte_offset == 0 {
        return Ok(file_len);
    }
    file.seek(SeekFrom::Start(byte_offset - 1))?;
    let mut boundary = [0_u8; 1];
    file.read_exact(&mut boundary)?;
    if boundary[0] != b'\n' {
        return Err(EventLogError::InvalidInput(
            "event cursor is not on a record boundary".into(),
        ));
    }
    Ok(file_len)
}

fn read_bounded_line(
    reader: &mut impl std::io::BufRead,
    line: &mut Vec<u8>,
) -> std::io::Result<(bool, bool, bool, u64)> {
    line.clear();
    let mut present = false;
    let mut overflowed = false;
    let mut consumed = 0_u64;
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            return Ok((present, false, overflowed, consumed));
        }
        present = true;
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let chunk_len = newline.map_or(buffer.len(), |index| index + 1);
        if !overflowed {
            let next_len = line.len().saturating_add(chunk_len);
            if next_len <= MAX_EVENT_LINE_BYTES {
                line.extend_from_slice(&buffer[..chunk_len]);
            } else {
                line.clear();
                overflowed = true;
            }
        }
        reader.consume(chunk_len);
        consumed = consumed.saturating_add(u64::try_from(chunk_len).unwrap_or(u64::MAX));
        if newline.is_some() {
            return Ok((present, true, overflowed, consumed));
        }
    }
}

fn decode_record(line: &[u8]) -> EventResult<EventRecord> {
    let value = serde_json::from_slice::<serde_json::Value>(line)
        .map_err(|_| EventLogError::CorruptRecord)?;
    let schema = value
        .get("schema")
        .and_then(serde_json::Value::as_u64)
        .ok_or(EventLogError::CorruptRecord)?;
    if schema != u64::from(EVENT_SCHEMA) {
        return Err(EventLogError::UnsupportedSchema {
            found: schema,
            supported: EVENT_SCHEMA,
        });
    }
    serde_json::from_value(value).map_err(|_| EventLogError::CorruptRecord)
}

fn max_valid_sequence(path: &Path, owner: &str, stream_id: &str) -> EventResult<u64> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let mut reader = BufReader::new(file);
    let mut line = Vec::with_capacity(MAX_EVENT_LINE_BYTES.min(8 * 1024));
    let mut maximum = 0_u64;
    loop {
        let (present, terminated, overflowed, _) = read_bounded_line(&mut reader, &mut line)?;
        if !present {
            break;
        }
        if !terminated || overflowed || line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let record = match decode_record(&line) {
            Ok(record) => record,
            Err(EventLogError::UnsupportedSchema { found, supported }) => {
                return Err(EventLogError::UnsupportedSchema { found, supported });
            }
            Err(_) => continue,
        };
        if record.validate_for_stream(owner, stream_id).is_ok() {
            maximum = maximum.max(record.sequence);
        }
    }
    Ok(maximum)
}

fn owner_directory(root: &Path, owner: &str) -> PathBuf {
    let digest = Sha256::digest(owner.as_bytes());
    let key = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    root.join(key)
}

fn stream_digest(owner: &str, stream_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"vyane.event-stream.v1\0");
    digest.update(owner.as_bytes());
    digest.update([0]);
    digest.update(stream_id.as_bytes());
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn allocator_checksum(sequence: u64, file_len: u64) -> String {
    let mut digest = Sha256::new();
    digest.update(b"vyane.event-allocator.v1\0");
    digest.update(ALLOCATOR_SCHEMA.to_le_bytes());
    digest.update(EVENT_SCHEMA.to_le_bytes());
    digest.update(sequence.to_le_bytes());
    digest.update(file_len.to_le_bytes());
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn event_path(root: &Path, owner: &str, stream_id: &str) -> PathBuf {
    owner_directory(root, owner).join(format!("{stream_id}.jsonl"))
}

fn lock_path(root: &Path, owner: &str, stream_id: &str) -> PathBuf {
    owner_directory(root, owner).join(format!("{stream_id}.lock"))
}

fn validate_stream_id(value: &str) -> EventResult<()> {
    if value.is_empty()
        || value.len() > MAX_STREAM_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(EventLogError::InvalidInput(
            "stream_id must match [A-Za-z0-9_-]{1,128}".into(),
        ));
    }
    Ok(())
}

fn validate_event_type(value: &str) -> EventResult<()> {
    if value.is_empty()
        || value.len() > MAX_EVENT_TYPE_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(EventLogError::InvalidInput(
            "event_type must contain only ASCII letters, digits, dot, dash, or underscore".into(),
        ));
    }
    Ok(())
}

fn validate_optional_text(field: &str, value: Option<&str>, limit: usize) -> EventResult<()> {
    if let Some(value) = value {
        validate_text(field, value, limit)?;
    }
    Ok(())
}

fn validate_text(field: &str, value: &str, limit: usize) -> EventResult<()> {
    if value.is_empty() || value.len() > limit || value.contains('\0') {
        return Err(EventLogError::InvalidInput(format!(
            "{field} must contain 1..={limit} UTF-8 bytes without NUL"
        )));
    }
    Ok(())
}

fn secure_directory(path: &Path) -> EventResult<bool> {
    let existed = match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => true,
        Ok(_) => {
            return Err(EventLogError::InvalidInput(format!(
                "event storage directory {} is not a real directory",
                path.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(!existed)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> EventResult<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> EventResult<()> {
    Ok(())
}

fn open_private(path: &Path, append: bool) -> EventResult<File> {
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if !metadata.file_type().is_file() {
            return Err(EventLogError::InvalidInput(format!(
                "event storage entry {} is not a regular file",
                path.display()
            )));
        }
    }
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true).append(append);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}
