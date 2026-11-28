//! Accepted server time and a same-boot monotonic floor.
//!
//! One row in `conversation_adapter_server_clocks` is the anchor for one
//! `(owner_user_id, delivery_id)`. [`server_time`] samples that anchor.
//! [`expiry_proof_time`] reports whether the anchor, plus elapsed monotonic
//! time on this boot, is enough to prove expiry. [`fence_expired_claim`]
//! records a token only after that proof is already past the deadline.
//!
//! The caller holds the lock and is already inside `BEGIN IMMEDIATE`, or an
//! equivalent immediate write transaction. Nothing here issues `BEGIN`,
//! `COMMIT`, or `ROLLBACK`. The caller commits a fence on rejection and
//! commits an anchor only after it has accepted the sample.
//!
//! Pass one `monotonic_at` into the proof, the fence, and the accepted
//! [`server_time`] write. A later monotonic sample must not move the saved
//! anchor past the deadline those calls just computed.
//!
//! On the same boot, a wall-clock spike by itself cannot prove expiry and,
//! unless the caller sets `advance`, cannot change the stored anchor. Across
//! boots monotonic stamps are not comparable. The wall high-water mark is
//! what remains. [`trusted_now`] is not a wall sample; it is an injected
//! trusted-domain clock supplied only to [`fence_expired_claim`].
//!
//! The boot id is fixed for the process. Windows asks [`crate::proc::boot_id`]
//! for `windows-boot-{BootId}`. `Ok(None)` and `Err` use the fallback. POSIX
//! reads `/proc/sys/kernel/random/boot_id` and does not call `proc::boot_id`.
//! A failed read, invalid UTF-8, or an empty file falls back to
//! `process:{pid}:{monotonic}`. That fallback is chosen here, not in `proc`.
//!
//! The kernel, the ledger, the CLI, the harness spawn path, and conversation
//! do not call this module.
//!
//! # What CI runs
//!
//! Linux CI runs the temporary-file SQLite tests, the procfs boot-id read,
//! the empty and failed boot-id reads, and the check that `monotonic_time`
//! tracks `/proc/uptime` (`CLOCK_MONOTONIC`). It also runs the pure boot-id
//! decision table, including Windows `Ok(None)` and `Err`.
//!
//! `GetTickCount64` and `proc::boot_id` are `cfg(windows)`. Linux CI does not
//! execute them. Windows CI compiles and runs them.
//!
//! [`trusted_now`]: fence_expired_claim

use std::collections::BTreeSet;
use std::sync::OnceLock;

use rusqlite::OptionalExtension as _;
use rusqlite::{Connection, params};
#[cfg(unix)]
use rustix::time::{ClockId, clock_gettime};
#[cfg(test)]
use std::cell::{Cell, RefCell};
#[cfg(test)]
use std::collections::VecDeque;
#[cfg(not(windows))]
use std::path::Path;

#[cfg(test)]
thread_local! {
    static FROZEN_MONOTONIC: Cell<Option<f64>> = const { Cell::new(None) };
    static MONOTONIC_SCRIPT: RefCell<VecDeque<f64>> = const { RefCell::new(VecDeque::new()) };
    static BOOT_ID_OVERRIDE: RefCell<Option<String>> = const { RefCell::new(None) };
}

#[cfg(not(windows))]
const POSIX_BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";

const CREATE_CLOCKS: &str = "\
CREATE TABLE IF NOT EXISTS conversation_adapter_server_clocks (
    owner_user_id TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    observed_at REAL NOT NULL,
    monotonic_at REAL,
    boot_id TEXT,
    fenced_claim_token TEXT,
    PRIMARY KEY (owner_user_id, delivery_id)
)";

const ADD_MONOTONIC_AT: &str =
    "ALTER TABLE conversation_adapter_server_clocks ADD COLUMN monotonic_at REAL";
const ADD_BOOT_ID: &str = "ALTER TABLE conversation_adapter_server_clocks ADD COLUMN boot_id TEXT";
const ADD_FENCED_CLAIM_TOKEN: &str =
    "ALTER TABLE conversation_adapter_server_clocks ADD COLUMN fenced_claim_token TEXT";

const SELECT_ANCHOR: &str = "\
SELECT observed_at, monotonic_at, boot_id, fenced_claim_token
  FROM conversation_adapter_server_clocks
 WHERE owner_user_id = ?1 AND delivery_id = ?2";

const UPSERT_ANCHOR: &str = "\
INSERT INTO conversation_adapter_server_clocks
    (owner_user_id, delivery_id, observed_at, monotonic_at, boot_id)
VALUES (?1, ?2, ?3, ?4, ?5)
ON CONFLICT(owner_user_id, delivery_id) DO UPDATE SET
    observed_at = excluded.observed_at,
    monotonic_at = excluded.monotonic_at,
    boot_id = excluded.boot_id";

const FENCE_CLAIM: &str = "\
UPDATE conversation_adapter_server_clocks
   SET fenced_claim_token = ?1
 WHERE owner_user_id = ?2 AND delivery_id = ?3";

#[derive(Debug, Clone, PartialEq)]
struct ClockAnchor {
    observed_at: f64,
    monotonic_at: Option<f64>,
    boot_id: Option<String>,
    fenced_claim_token: Option<String>,
}

/// Monotonic seconds.
///
/// POSIX reads `CLOCK_MONOTONIC`, which is the same counter in every process
/// on this boot. Windows reads `GetTickCount64` (milliseconds since boot).
/// The origin is arbitrary; only differences are meaningful. This is not wall
/// time.
#[must_use]
pub fn monotonic_time() -> f64 {
    #[cfg(test)]
    {
        let scripted = MONOTONIC_SCRIPT.with(|queue| queue.borrow_mut().pop_front());
        if let Some(tick) = scripted {
            return tick;
        }
        if let Some(tick) = FROZEN_MONOTONIC.with(Cell::get) {
            return tick;
        }
    }
    raw_monotonic_seconds()
}

/// Sample or accept one delivery's server time.
///
/// The caller is inside an immediate write transaction. This function does
/// not `BEGIN` or `COMMIT`.
///
/// `advance` false only returns `max(observed_at, floor)` and does not write.
/// A rejected wall-clock spike is that call: the returned sample may include
/// the spike, and the stored anchor stays put.
///
/// On the same boot, when the row has `monotonic_at`, `floor` is the accepted
/// `observed_at` plus `max(0, tick - saved monotonic_at)`. Otherwise `floor`
/// is the accepted `observed_at`. Monotonic stamps from another boot are not
/// added. `tick` is `monotonic_at` when it is `Some`; otherwise it is one
/// [`monotonic_time`] sample, including when the row is missing.
///
/// `advance` true writes `effective`, that same tick, and the current boot id.
/// It does not clear `fenced_claim_token`.
pub fn server_time(
    conn: &Connection,
    owner: &str,
    delivery: &str,
    observed_at: f64,
    advance: bool,
    monotonic_at: Option<f64>,
) -> Result<f64, rusqlite::Error> {
    ensure_schema(conn)?;
    let row = load_anchor(conn, owner, delivery)?;
    let tick = monotonic_at.unwrap_or_else(monotonic_time);
    let boot_id = process_boot_id();
    let effective = match row.as_ref() {
        Some(row) => effective_sample(row, observed_at, tick, &boot_id),
        None => observed_at,
    };
    if advance {
        conn.execute(
            UPSERT_ANCHOR,
            params![owner, delivery, effective, tick, boot_id],
        )?;
    }
    Ok(effective)
}

/// Accepted time plus same-boot elapsed time.
///
/// Returns `None` when this delivery has no row. A missing anchor cannot
/// prove expiry. On the same boot the result is the anchor plus elapsed
/// monotonic time, so a wall-clock spike cannot by itself prove sustained
/// expiry. Across boots the result is `max(anchor, observed_at)` because
/// monotonic stamps are not comparable.
///
/// Does not write. [`server_time`] creates the table; a missing table is a
/// SQLite error. Does not `BEGIN` or `COMMIT`.
pub fn expiry_proof_time(
    conn: &Connection,
    owner: &str,
    delivery: &str,
    observed_at: f64,
    monotonic_at: Option<f64>,
) -> Result<Option<f64>, rusqlite::Error> {
    let Some(row) = load_anchor(conn, owner, delivery)? else {
        return Ok(None);
    };
    Ok(Some(expiry_proof_from_row(&row, observed_at, monotonic_at)))
}

/// Fence a claim only when the anchor and elapsed time already prove expiry.
///
/// Writes `fenced_claim_token` and does not change `observed_at`,
/// `monotonic_at`, or `boot_id`. The same token returns true without a new
/// proof, including across boots. A missing row returns false and writes
/// nothing. A missing table is a SQLite error.
///
/// `trusted_now` is an injected trusted-domain clock. It is not a wall-clock
/// sample and not a native event timestamp. When it is `Some`, the proof is
/// `max(proof, trusted_now)`. It is not stored as `observed_at`.
///
/// A proof equal to `expires_at` is still live. Only a strictly later proof
/// writes the token. Does not `BEGIN` or `COMMIT`.
#[allow(clippy::too_many_arguments)]
pub fn fence_expired_claim(
    conn: &Connection,
    owner: &str,
    delivery: &str,
    token: &str,
    expires_at: f64,
    observed_at: f64,
    monotonic_at: Option<f64>,
    trusted_now: Option<f64>,
) -> Result<bool, rusqlite::Error> {
    let Some(row) = load_anchor(conn, owner, delivery)? else {
        return Ok(false);
    };
    if row.fenced_claim_token.as_deref() == Some(token) {
        return Ok(true);
    }
    let mut proof = expiry_proof_from_row(&row, observed_at, monotonic_at);
    if let Some(trusted_now) = trusted_now {
        proof = higher(proof, trusted_now);
    }
    if proof <= expires_at {
        return Ok(false);
    }
    conn.execute(FENCE_CLAIM, params![token, owner, delivery])?;
    Ok(true)
}

fn ensure_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute(CREATE_CLOCKS, ())?;
    let columns = existing_columns(conn)?;
    for (name, sql) in [
        ("monotonic_at", ADD_MONOTONIC_AT),
        ("boot_id", ADD_BOOT_ID),
        ("fenced_claim_token", ADD_FENCED_CLAIM_TOKEN),
    ] {
        if !columns.contains(name) {
            conn.execute(sql, ())?;
        }
    }
    Ok(())
}

fn existing_columns(conn: &Connection) -> Result<BTreeSet<String>, rusqlite::Error> {
    let mut statement = conn.prepare("PRAGMA table_info(conversation_adapter_server_clocks)")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
    rows.collect()
}

fn load_anchor(
    conn: &Connection,
    owner: &str,
    delivery: &str,
) -> Result<Option<ClockAnchor>, rusqlite::Error> {
    conn.query_row(SELECT_ANCHOR, params![owner, delivery], |row| {
        Ok(ClockAnchor {
            observed_at: row.get(0)?,
            monotonic_at: row.get(1)?,
            boot_id: row.get(2)?,
            fenced_claim_token: row.get(3)?,
        })
    })
    .optional()
}

fn effective_sample(row: &ClockAnchor, observed_at: f64, tick: f64, boot_id: &str) -> f64 {
    if let Some(saved) = saved_monotonic_on_this_boot(row, boot_id) {
        // The wall sample participates. A spike is returned and, only when
        // the caller accepts it, stored. Expiry proof does not do this.
        return higher(observed_at, row.observed_at + elapsed(saved, tick));
    }
    higher(observed_at, row.observed_at)
}

fn expiry_proof_from_row(row: &ClockAnchor, observed_at: f64, monotonic_at: Option<f64>) -> f64 {
    let boot_id = process_boot_id();
    if let Some(saved) = saved_monotonic_on_this_boot(row, &boot_id) {
        let tick = monotonic_at.unwrap_or_else(monotonic_time);
        return row.observed_at + elapsed(saved, tick);
    }
    // No comparable elapsed clock survives this boot boundary. Wall time may
    // prove expiry. It does not move the accepted anchor; this function does
    // not write.
    higher(row.observed_at, observed_at)
}

fn saved_monotonic_on_this_boot(row: &ClockAnchor, boot_id: &str) -> Option<f64> {
    if same_boot(row.boot_id.as_deref(), boot_id) {
        row.monotonic_at
    } else {
        None
    }
}

fn same_boot(saved: Option<&str>, current: &str) -> bool {
    saved == Some(current)
}

fn elapsed(saved: f64, tick: f64) -> f64 {
    let delta = tick - saved;
    if delta > 0.0 { delta } else { 0.0 }
}

fn higher(first: f64, second: f64) -> f64 {
    if second > first { second } else { first }
}

fn process_boot_id() -> String {
    #[cfg(test)]
    if let Some(boot_id) = BOOT_ID_OVERRIDE.with(|slot| slot.borrow().clone()) {
        return boot_id;
    }
    static CACHED: OnceLock<String> = OnceLock::new();
    CACHED.get_or_init(read_process_boot_id).clone()
}

fn read_process_boot_id() -> String {
    resolve_boot_id(
        read_platform_boot_text(),
        std::process::id(),
        raw_monotonic_seconds(),
    )
}

/// Keep a non-empty platform boot id. Otherwise stamp this process.
fn resolve_boot_id(platform: Option<String>, pid: u32, tick: f64) -> String {
    if let Some(text) = platform {
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    format!("process:{pid}:{tick}")
}

#[cfg(any(windows, test))]
fn accepted_platform_boot<E>(result: Result<Option<String>, E>) -> Option<String> {
    result.ok().flatten()
}

#[cfg(windows)]
fn read_platform_boot_text() -> Option<String> {
    accepted_platform_boot(crate::proc::boot_id())
}

#[cfg(not(windows))]
fn read_platform_boot_text() -> Option<String> {
    read_boot_text_at(Path::new(POSIX_BOOT_ID_PATH))
}

#[cfg(not(windows))]
fn read_boot_text_at(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

#[cfg(unix)]
fn raw_monotonic_seconds() -> f64 {
    let timespec = clock_gettime(ClockId::Monotonic);
    timespec.tv_sec as f64 + timespec.tv_nsec as f64 / 1_000_000_000.0
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "GetTickCount64 has no safe wrapper in this workspace. It is the system-wide millisecond counter used as monotonic time on Windows."
)]
fn raw_monotonic_seconds() -> f64 {
    let millis = unsafe { windows_sys::Win32::System::SystemInformation::GetTickCount64() };
    millis as f64 / 1_000.0
}

#[cfg(test)]
#[must_use]
struct MonotonicFreeze {
    tick: f64,
}

#[cfg(test)]
impl Drop for MonotonicFreeze {
    fn drop(&mut self) {
        FROZEN_MONOTONIC.with(|cell| cell.set(None));
    }
}

/// Stop [`monotonic_time`] at the current tick until this value is dropped.
///
/// Wall time is injected through `observed_at`. Real elapsed time must not
/// age an anchor while a test only moves that wall sample. Elapsed monotonic
/// time from before the freeze still counts.
#[cfg(test)]
fn freeze_monotonic() -> MonotonicFreeze {
    let tick = raw_monotonic_seconds();
    FROZEN_MONOTONIC.with(|cell| cell.set(Some(tick)));
    MonotonicFreeze { tick }
}

#[cfg(test)]
#[must_use]
struct MonotonicScript;

#[cfg(test)]
impl MonotonicScript {
    fn set(samples: impl IntoIterator<Item = f64>) -> Self {
        let samples = VecDeque::from_iter(samples);
        MONOTONIC_SCRIPT.with(|queue| *queue.borrow_mut() = samples);
        Self
    }

    fn remaining(&self) -> usize {
        MONOTONIC_SCRIPT.with(|queue| queue.borrow().len())
    }
}

#[cfg(test)]
impl Drop for MonotonicScript {
    fn drop(&mut self) {
        MONOTONIC_SCRIPT.with(|queue| queue.borrow_mut().clear());
    }
}

#[cfg(test)]
#[must_use]
struct BootIdOverride;

#[cfg(test)]
impl BootIdOverride {
    fn set(boot_id: &str) -> Self {
        BOOT_ID_OVERRIDE.with(|slot| *slot.borrow_mut() = Some(boot_id.to_string()));
        Self
    }

    fn replace(&self, boot_id: &str) {
        BOOT_ID_OVERRIDE.with(|slot| *slot.borrow_mut() = Some(boot_id.to_string()));
    }
}

#[cfg(test)]
impl Drop for BootIdOverride {
    fn drop(&mut self) {
        BOOT_ID_OVERRIDE.with(|slot| *slot.borrow_mut() = None);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    #[cfg(not(windows))]
    use std::fs;

    use rusqlite::{Connection, TransactionBehavior, params};
    use tempfile::NamedTempFile;

    #[cfg(target_os = "linux")]
    use super::monotonic_time;
    use super::{
        BootIdOverride, ClockAnchor, MonotonicFreeze, MonotonicScript, accepted_platform_boot,
        expiry_proof_time, fence_expired_claim, freeze_monotonic, load_anchor, process_boot_id,
        raw_monotonic_seconds, resolve_boot_id, server_time,
    };

    const OWNER: &str = "owner";
    const DELIVERY: &str = "delivery";

    struct TempDb {
        _file: NamedTempFile,
        conn: Connection,
    }

    impl TempDb {
        fn open() -> Self {
            let file = NamedTempFile::new().expect("temporary sqlite file");
            let conn = Connection::open(file.path()).expect("open sqlite file");
            Self { _file: file, conn }
        }

        fn immediate(&mut self) -> rusqlite::Transaction<'_> {
            self.conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .expect("BEGIN IMMEDIATE")
        }
    }

    fn assert_still_open(conn: &Connection) {
        assert!(
            !conn.is_autocommit(),
            "adapter_clock committed or rolled back the caller transaction"
        );
    }

    fn anchor_of(conn: &Connection, owner: &str, delivery: &str) -> ClockAnchor {
        load_anchor(conn, owner, delivery)
            .expect("read anchor")
            .expect("anchor row")
    }

    fn anchor(conn: &Connection) -> ClockAnchor {
        anchor_of(conn, OWNER, DELIVERY)
    }

    fn maybe_anchor(conn: &Connection, owner: &str, delivery: &str) -> Option<ClockAnchor> {
        load_anchor(conn, owner, delivery).expect("read anchor")
    }

    fn accept(conn: &Connection, observed_at: f64, monotonic_at: f64) {
        server_time(conn, OWNER, DELIVERY, observed_at, true, Some(monotonic_at))
            .expect("accept server time");
    }

    struct Column {
        name: String,
        kind: String,
        pk: i64,
    }

    fn table_columns(conn: &Connection) -> Vec<Column> {
        let mut statement = conn
            .prepare("PRAGMA table_info(conversation_adapter_server_clocks)")
            .expect("pragma table_info");
        let rows = statement
            .query_map([], |row| {
                Ok(Column {
                    name: row.get(1)?,
                    kind: row.get(2)?,
                    pk: row.get(5)?,
                })
            })
            .expect("pragma rows");
        rows.collect::<Result<Vec<_>, _>>().expect("column rows")
    }

    fn column_kinds(conn: &Connection) -> BTreeMap<String, String> {
        table_columns(conn)
            .into_iter()
            .map(|column| (column.name, column.kind))
            .collect()
    }

    fn assert_clock_schema(conn: &Connection) {
        let kinds = column_kinds(conn);
        let names: BTreeSet<_> = kinds.keys().cloned().collect();
        let expected: BTreeSet<_> = [
            "owner_user_id",
            "delivery_id",
            "observed_at",
            "monotonic_at",
            "boot_id",
            "fenced_claim_token",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        assert_eq!(names, expected);
        assert_eq!(kinds.get("monotonic_at").map(String::as_str), Some("REAL"));
        assert_eq!(kinds.get("boot_id").map(String::as_str), Some("TEXT"));
        assert_eq!(
            kinds.get("fenced_claim_token").map(String::as_str),
            Some("TEXT")
        );
        let mut key: Vec<_> = table_columns(conn)
            .into_iter()
            .filter(|column| column.pk > 0)
            .map(|column| (column.pk, column.name))
            .collect();
        key.sort_by_key(|(pk, _)| *pk);
        let key_names: Vec<_> = key.into_iter().map(|(_, name)| name).collect();
        assert_eq!(key_names, ["owner_user_id", "delivery_id"]);
    }

    #[test]
    fn rejected_wall_clock_spike_does_not_change_the_anchor() {
        let _boot = BootIdOverride::set("boot-a");
        let mut db = TempDb::open();
        let tx = db.immediate();
        accept(&tx, 100.0, 10.0);
        let before = anchor(&tx);

        let sample =
            server_time(&tx, OWNER, DELIVERY, 10_000.0, false, Some(10.0)).expect("sample spike");
        assert_eq!(sample, 10_000.0);
        assert_eq!(anchor(&tx), before);

        let proof = expiry_proof_time(&tx, OWNER, DELIVERY, 10_000.0, Some(10.0)).expect("proof");
        assert_eq!(proof, Some(100.0));
        let fenced = fence_expired_claim(
            &tx,
            OWNER,
            DELIVERY,
            "token",
            150.0,
            10_000.0,
            Some(10.0),
            None,
        )
        .expect("fence spike");
        assert!(!fenced);
        assert_eq!(anchor(&tx), before);

        let accepted =
            server_time(&tx, OWNER, DELIVERY, 110.0, true, Some(10.0)).expect("accept later");
        assert_eq!(accepted, 110.0);
        let row = anchor(&tx);
        assert_eq!(row.observed_at, 110.0);
        assert_eq!(row.monotonic_at, Some(10.0));
        assert_eq!(row.boot_id.as_deref(), Some("boot-a"));
        assert_eq!(row.fenced_claim_token, None);
        assert_still_open(&tx);
    }

    #[test]
    fn same_boot_monotonic_elapsed_ages_expiry_proof_time() {
        let _boot = BootIdOverride::set("boot-a");
        let mut db = TempDb::open();
        let tx = db.immediate();
        accept(&tx, 100.0, 10.0);
        let before = anchor(&tx);

        let sample =
            server_time(&tx, OWNER, DELIVERY, 90.0, false, Some(40.0)).expect("sample elapsed");
        assert_eq!(sample, 130.0);
        let proof = expiry_proof_time(&tx, OWNER, DELIVERY, 90.0, Some(40.0)).expect("proof");
        assert_eq!(proof, Some(130.0));
        assert_eq!(anchor(&tx), before);

        let sample =
            server_time(&tx, OWNER, DELIVERY, 100.0, false, Some(5.0)).expect("sample backward");
        assert_eq!(sample, 100.0);
        let proof = expiry_proof_time(&tx, OWNER, DELIVERY, 100.0, Some(5.0)).expect("proof");
        assert_eq!(proof, Some(100.0));
        assert_eq!(anchor(&tx), before);

        let accepted =
            server_time(&tx, OWNER, DELIVERY, 90.0, true, Some(40.0)).expect("accept elapsed");
        assert_eq!(accepted, 130.0);
        let row = anchor(&tx);
        assert_eq!(row.observed_at, 130.0);
        assert_eq!(row.monotonic_at, Some(40.0));
        assert_eq!(row.boot_id.as_deref(), Some("boot-a"));
        assert_still_open(&tx);
    }

    #[test]
    fn across_boots_only_the_wall_high_water_mark_is_kept() {
        let boot = BootIdOverride::set("boot-a");
        let mut db = TempDb::open();
        let tx = db.immediate();
        accept(&tx, 100.0, 10.0);

        boot.replace("boot-b");
        let sample = server_time(&tx, OWNER, DELIVERY, 80.0, false, Some(9_999.0))
            .expect("sample other boot");
        assert_eq!(sample, 100.0);
        let before = anchor(&tx);
        assert_eq!(before.observed_at, 100.0);
        assert_eq!(before.monotonic_at, Some(10.0));
        assert_eq!(before.boot_id.as_deref(), Some("boot-a"));

        let proof = expiry_proof_time(&tx, OWNER, DELIVERY, 250.0, Some(0.0)).expect("wall proof");
        assert_eq!(proof, Some(250.0));
        assert_eq!(anchor(&tx), before);

        let accepted =
            server_time(&tx, OWNER, DELIVERY, 90.0, true, Some(5.0)).expect("accept lower wall");
        assert_eq!(accepted, 100.0);
        let row = anchor(&tx);
        assert_eq!(row.observed_at, 100.0);
        assert_eq!(row.monotonic_at, Some(5.0));
        assert_eq!(row.boot_id.as_deref(), Some("boot-b"));

        let accepted =
            server_time(&tx, OWNER, DELIVERY, 140.0, true, Some(7.0)).expect("accept higher wall");
        assert_eq!(accepted, 140.0);

        boot.replace("boot-c");
        let sample = server_time(&tx, OWNER, DELIVERY, 130.0, false, Some(5_000.0))
            .expect("sample third boot");
        assert_eq!(sample, 140.0);
        let proof = expiry_proof_time(&tx, OWNER, DELIVERY, 130.0, Some(5_000.0)).expect("proof");
        assert_eq!(proof, Some(140.0));
        let row = anchor(&tx);
        assert_eq!(row.observed_at, 140.0);
        assert_eq!(row.monotonic_at, Some(7.0));
        assert_eq!(row.boot_id.as_deref(), Some("boot-b"));
        assert_still_open(&tx);
    }

    #[test]
    fn unobserved_boot_change_does_not_invent_expiry() {
        let boot = BootIdOverride::set("boot-a");
        let mut db = TempDb::open();
        let tx = db.immediate();
        accept(&tx, 1_000.0, 0.0);
        let before = anchor(&tx);

        boot.replace("unobserved-downtime");
        let sample = server_time(&tx, OWNER, DELIVERY, 999.0, false, Some(10_000.0))
            .expect("sample unobserved boot");
        assert_eq!(sample, 1_000.0);
        let proof = expiry_proof_time(&tx, OWNER, DELIVERY, 999.0, Some(10_000.0)).expect("proof");
        assert_eq!(proof, Some(1_000.0));
        let fenced = fence_expired_claim(
            &tx,
            OWNER,
            DELIVERY,
            "token",
            1_030.0,
            999.0,
            Some(10_000.0),
            None,
        )
        .expect("fence unobserved boot");
        assert!(!fenced);
        assert_eq!(anchor(&tx), before);
        assert_still_open(&tx);
    }

    #[test]
    fn each_delivery_uses_its_own_anchor() {
        let _boot = BootIdOverride::set("boot-a");
        let mut db = TempDb::open();
        let tx = db.immediate();
        server_time(&tx, "owner-a", "delivery-1", 200.0, true, Some(50.0)).expect("accept first");
        server_time(&tx, "owner-a", "delivery-2", 12.0, true, Some(1.0)).expect("accept second");
        server_time(&tx, "owner-b", "delivery-1", 3.0, true, Some(1.0))
            .expect("accept other owner");

        let sample = server_time(&tx, "owner-a", "delivery-2", 12.0, false, Some(10_000.0))
            .expect("sample second");
        assert_eq!(sample, 10_011.0);

        let first = anchor_of(&tx, "owner-a", "delivery-1");
        assert_eq!(first.observed_at, 200.0);
        assert_eq!(first.monotonic_at, Some(50.0));
        let second = anchor_of(&tx, "owner-a", "delivery-2");
        assert_eq!(second.observed_at, 12.0);
        assert_eq!(second.monotonic_at, Some(1.0));
        let other_owner = anchor_of(&tx, "owner-b", "delivery-1");
        assert_eq!(other_owner.observed_at, 3.0);
        assert_eq!(other_owner.monotonic_at, Some(1.0));
        assert_still_open(&tx);
    }

    #[test]
    fn validation_and_saved_anchor_share_one_monotonic_sample() {
        let _boot = BootIdOverride::set("boot-a");
        let mut db = TempDb::open();
        let tx = db.immediate();
        accept(&tx, 1_000.0, 0.0);
        let script = MonotonicScript::set([10.0, 100.0, 200.0]);

        let proof = expiry_proof_time(&tx, OWNER, DELIVERY, 1_000.0, Some(10.0)).expect("validate");
        assert_eq!(proof, Some(1_010.0));
        let fenced = fence_expired_claim(
            &tx,
            OWNER,
            DELIVERY,
            "token",
            1_010.0,
            1_000.0,
            Some(10.0),
            None,
        )
        .expect("fence at the deadline");
        assert!(!fenced);
        let accepted =
            server_time(&tx, OWNER, DELIVERY, 1_000.0, true, Some(10.0)).expect("persist");
        assert_eq!(accepted, 1_010.0);
        assert_eq!(script.remaining(), 3);
        let row = anchor(&tx);
        assert_eq!(row.observed_at, 1_010.0);
        assert_eq!(row.monotonic_at, Some(10.0));
        let proof_after =
            expiry_proof_time(&tx, OWNER, DELIVERY, 1_000.0, Some(10.0)).expect("proof again");
        assert_eq!(proof_after, Some(1_010.0));

        server_time(&tx, OWNER, "other", 1_000.0, true, Some(0.0)).expect("other anchor");
        let accepted = server_time(&tx, OWNER, "other", 1_000.0, true, None).expect("one sample");
        assert_eq!(accepted, 1_010.0);
        assert_eq!(script.remaining(), 2);
        let other = anchor_of(&tx, OWNER, "other");
        assert_eq!(other.observed_at, 1_010.0);
        assert_eq!(other.monotonic_at, Some(10.0));
        assert_eq!(anchor(&tx).monotonic_at, Some(10.0));
        assert_still_open(&tx);
    }

    #[test]
    fn fence_writes_only_after_expiry_without_advancing_observed_at() {
        let boot = BootIdOverride::set("boot-a");
        let mut db = TempDb::open();
        let tx = db.immediate();
        accept(&tx, 100.0, 0.0);

        let fenced = fence_expired_claim(
            &tx,
            OWNER,
            DELIVERY,
            "claim-a",
            130.0,
            100.0,
            Some(30.0),
            None,
        )
        .expect("fence at equality");
        assert!(!fenced);
        let before = anchor(&tx);
        assert_eq!(before.fenced_claim_token, None);

        let fenced = fence_expired_claim(
            &tx,
            OWNER,
            DELIVERY,
            "claim-a",
            130.0,
            10_000.0,
            Some(1.0),
            None,
        )
        .expect("fence wall spike");
        assert!(!fenced);
        assert_eq!(anchor(&tx), before);

        let fenced = fence_expired_claim(
            &tx,
            OWNER,
            DELIVERY,
            "claim-a",
            130.0,
            100.0,
            Some(30.5),
            None,
        )
        .expect("fence after elapsed expiry");
        assert!(fenced);
        let fenced_row = anchor(&tx);
        assert_eq!(fenced_row.observed_at, 100.0);
        assert_eq!(fenced_row.monotonic_at, Some(0.0));
        assert_eq!(fenced_row.boot_id.as_deref(), Some("boot-a"));
        assert_eq!(fenced_row.fenced_claim_token.as_deref(), Some("claim-a"));

        let script = MonotonicScript::set([999.0]);
        let fenced = fence_expired_claim(&tx, OWNER, DELIVERY, "claim-a", 0.0, 0.0, None, None)
            .expect("same token");
        assert!(fenced);
        assert_eq!(script.remaining(), 1);
        assert_eq!(anchor(&tx).observed_at, 100.0);
        drop(script);

        boot.replace("boot-b");
        let fenced =
            fence_expired_claim(&tx, OWNER, DELIVERY, "claim-a", 130.0, 0.0, Some(0.0), None)
                .expect("same token across boots");
        assert!(fenced);
        assert_eq!(anchor(&tx).observed_at, 100.0);
        assert_eq!(anchor(&tx).boot_id.as_deref(), Some("boot-a"));
        assert_eq!(anchor(&tx).fenced_claim_token.as_deref(), Some("claim-a"));

        let fenced =
            fence_expired_claim(&tx, OWNER, DELIVERY, "other", 130.0, 0.0, Some(0.0), None)
                .expect("other token without proof");
        assert!(!fenced);
        assert_eq!(anchor(&tx).fenced_claim_token.as_deref(), Some("claim-a"));

        let fenced =
            fence_expired_claim(&tx, OWNER, DELIVERY, "other", 130.0, 200.0, Some(0.0), None)
                .expect("other token with wall proof");
        assert!(fenced);
        let row = anchor(&tx);
        assert_eq!(row.observed_at, 100.0);
        assert_eq!(row.monotonic_at, Some(0.0));
        assert_eq!(row.boot_id.as_deref(), Some("boot-a"));
        assert_eq!(row.fenced_claim_token.as_deref(), Some("other"));

        let accepted =
            server_time(&tx, OWNER, DELIVERY, 110.0, true, Some(1.0)).expect("accept after fence");
        assert_eq!(accepted, 110.0);
        let row = anchor(&tx);
        assert_eq!(row.observed_at, 110.0);
        assert_eq!(row.monotonic_at, Some(1.0));
        assert_eq!(row.boot_id.as_deref(), Some("boot-b"));
        assert_eq!(row.fenced_claim_token.as_deref(), Some("other"));

        server_time(&tx, OWNER, "spike", 100.0, true, Some(0.0)).expect("spike anchor");
        let fenced = fence_expired_claim(
            &tx,
            OWNER,
            "spike",
            "spike-token",
            130.0,
            10_000.0,
            Some(1.0),
            Some(130.0),
        )
        .expect("trusted now equal to the deadline");
        assert!(!fenced);
        assert_eq!(anchor_of(&tx, OWNER, "spike").observed_at, 100.0);
        assert_eq!(anchor_of(&tx, OWNER, "spike").fenced_claim_token, None);

        let fenced = fence_expired_claim(
            &tx,
            OWNER,
            "spike",
            "spike-token",
            130.0,
            10_000.0,
            Some(1.0),
            Some(130.5),
        )
        .expect("trusted now proves expiry");
        assert!(fenced);
        let spike = anchor_of(&tx, OWNER, "spike");
        assert_eq!(spike.observed_at, 100.0);
        assert_eq!(spike.monotonic_at, Some(0.0));
        assert_eq!(spike.fenced_claim_token.as_deref(), Some("spike-token"));

        server_time(&tx, OWNER, "trusted-low", 100.0, true, Some(0.0)).expect("low trusted");
        let fenced = fence_expired_claim(
            &tx,
            OWNER,
            "trusted-low",
            "low",
            130.0,
            100.0,
            Some(50.0),
            Some(0.0),
        )
        .expect("trusted now does not hide elapsed expiry");
        assert!(fenced);
        assert_eq!(anchor_of(&tx, OWNER, "trusted-low").observed_at, 100.0);
        assert_still_open(&tx);
    }

    #[test]
    fn missing_anchor_cannot_fence() {
        let _boot = BootIdOverride::set("boot-a");
        let mut db = TempDb::open();
        let tx = db.immediate();
        let sample =
            server_time(&tx, OWNER, "other", 10.0, false, Some(0.0)).expect("create schema");
        assert_eq!(sample, 10.0);
        assert!(maybe_anchor(&tx, OWNER, "missing").is_none());
        assert!(maybe_anchor(&tx, OWNER, "other").is_none());
        assert_eq!(
            expiry_proof_time(&tx, OWNER, "missing", 50.0, Some(1.0)).expect("missing proof"),
            None
        );
        let fenced =
            fence_expired_claim(&tx, OWNER, "missing", "token", 1.0, 50.0, Some(1.0), None)
                .expect("missing fence");
        assert!(!fenced);
        assert!(maybe_anchor(&tx, OWNER, "missing").is_none());
        assert_still_open(&tx);
    }

    #[test]
    fn missing_table_is_not_an_anchor() {
        let _boot = BootIdOverride::set("boot-a");
        let mut db = TempDb::open();
        let tx = db.immediate();
        let expiry =
            expiry_proof_time(&tx, OWNER, DELIVERY, 1.0, Some(0.0)).expect_err("missing table");
        assert!(expiry.to_string().contains("no such table"), "{expiry}");
        let fence = fence_expired_claim(&tx, OWNER, DELIVERY, "token", 1.0, 1.0, Some(0.0), None)
            .expect_err("missing table");
        assert!(fence.to_string().contains("no such table"), "{fence}");
        assert_still_open(&tx);
    }

    #[test]
    fn freeze_monotonic_stops_elapsed_time_during_a_wall_jump() {
        let _boot = BootIdOverride::set("boot-a");
        let frozen: MonotonicFreeze = freeze_monotonic();
        let mut db = TempDb::open();
        let tx = db.immediate();
        let first = server_time(&tx, OWNER, DELIVERY, 100.0, true, None).expect("accept frozen");
        assert_eq!(first, 100.0);
        assert_eq!(anchor(&tx).monotonic_at, Some(frozen.tick));

        let second = server_time(&tx, OWNER, DELIVERY, 500.0, true, None).expect("wall jump");
        assert_eq!(second, 500.0);
        let row = anchor(&tx);
        assert_eq!(row.observed_at, 500.0);
        assert_eq!(row.monotonic_at, Some(frozen.tick));
        let proof = expiry_proof_time(&tx, OWNER, DELIVERY, 0.0, None).expect("frozen proof");
        assert_eq!(proof, Some(500.0));
        assert_still_open(&tx);
    }

    #[test]
    fn legacy_clock_table_gains_missing_columns() {
        let _boot = BootIdOverride::set("boot-a");
        let mut db = TempDb::open();
        let tx = db.immediate();
        tx.execute_batch(
            "CREATE TABLE conversation_adapter_server_clocks (
                owner_user_id TEXT NOT NULL,
                delivery_id TEXT NOT NULL,
                observed_at REAL NOT NULL,
                PRIMARY KEY (owner_user_id, delivery_id)
            )",
        )
        .expect("legacy table");
        tx.execute(
            "INSERT INTO conversation_adapter_server_clocks
                (owner_user_id, delivery_id, observed_at)
             VALUES (?1, ?2, ?3)",
            params![OWNER, DELIVERY, 50.0],
        )
        .expect("legacy row");

        let sample =
            server_time(&tx, OWNER, DELIVERY, 40.0, false, Some(10_000.0)).expect("sample legacy");
        assert_eq!(sample, 50.0);
        assert_clock_schema(&tx);
        let row = anchor(&tx);
        assert_eq!(row.observed_at, 50.0);
        assert_eq!(row.monotonic_at, None);
        assert_eq!(row.boot_id, None);
        assert_eq!(
            expiry_proof_time(&tx, OWNER, DELIVERY, 40.0, Some(10_000.0)).expect("legacy proof"),
            Some(50.0)
        );
        let sample_again =
            server_time(&tx, OWNER, DELIVERY, 40.0, false, Some(1.0)).expect("sample again");
        assert_eq!(sample_again, 50.0);
        assert_still_open(&tx);
    }

    #[test]
    fn partial_migration_does_not_readd_existing_columns() {
        let _boot = BootIdOverride::set("boot-a");
        let mut db = TempDb::open();
        let tx = db.immediate();
        tx.execute_batch(
            "CREATE TABLE conversation_adapter_server_clocks (
                owner_user_id TEXT NOT NULL,
                delivery_id TEXT NOT NULL,
                observed_at REAL NOT NULL,
                monotonic_at REAL,
                PRIMARY KEY (owner_user_id, delivery_id)
            )",
        )
        .expect("partial table");
        tx.execute(
            "INSERT INTO conversation_adapter_server_clocks
                (owner_user_id, delivery_id, observed_at, monotonic_at)
             VALUES (?1, ?2, ?3, NULL)",
            params![OWNER, DELIVERY, 50.0],
        )
        .expect("partial row");

        let sample =
            server_time(&tx, OWNER, DELIVERY, 40.0, false, Some(10_000.0)).expect("sample partial");
        assert_eq!(sample, 50.0);
        assert_clock_schema(&tx);
        let row = anchor(&tx);
        assert_eq!(row.monotonic_at, None);
        assert_eq!(row.boot_id, None);
        assert_still_open(&tx);
    }

    #[test]
    fn caller_immediate_transaction_is_left_open() {
        let _boot = BootIdOverride::set("boot-a");
        let file = NamedTempFile::new().expect("temporary sqlite file");
        let path = file.path().to_path_buf();
        {
            let conn = Connection::open(&path).expect("open sqlite file");
            conn.execute_batch("BEGIN IMMEDIATE")
                .expect("BEGIN IMMEDIATE");
            server_time(&conn, OWNER, DELIVERY, 100.0, true, Some(0.0)).expect("accept");
            let fenced = fence_expired_claim(
                &conn,
                OWNER,
                DELIVERY,
                "token",
                130.0,
                100.0,
                Some(40.0),
                None,
            )
            .expect("fence");
            assert!(fenced);
            assert_eq!(anchor(&conn).observed_at, 100.0);
            assert!(!conn.is_autocommit());
            conn.execute_batch("ROLLBACK").expect("ROLLBACK");
            assert!(conn.is_autocommit());
            let missing = conn.query_row(
                "SELECT COUNT(*) FROM conversation_adapter_server_clocks",
                [],
                |row| row.get::<_, i64>(0),
            );
            assert!(
                missing.is_err(),
                "rollback must remove the uncommitted anchor"
            );

            conn.execute_batch("BEGIN IMMEDIATE")
                .expect("BEGIN IMMEDIATE");
            server_time(&conn, OWNER, DELIVERY, 80.0, true, Some(4.0)).expect("accept again");
            assert!(!conn.is_autocommit());
            conn.execute_batch("COMMIT").expect("COMMIT");
        }
        let conn = Connection::open(&path).expect("reopen sqlite file");
        let observed: f64 = conn
            .query_row(
                "SELECT observed_at FROM conversation_adapter_server_clocks
                  WHERE owner_user_id = ?1 AND delivery_id = ?2",
                params![OWNER, DELIVERY],
                |row| row.get(0),
            )
            .expect("committed row");
        let monotonic: f64 = conn
            .query_row(
                "SELECT monotonic_at FROM conversation_adapter_server_clocks
                  WHERE owner_user_id = ?1 AND delivery_id = ?2",
                params![OWNER, DELIVERY],
                |row| row.get(0),
            )
            .expect("committed monotonic");
        let boot_id: String = conn
            .query_row(
                "SELECT boot_id FROM conversation_adapter_server_clocks
                  WHERE owner_user_id = ?1 AND delivery_id = ?2",
                params![OWNER, DELIVERY],
                |row| row.get(0),
            )
            .expect("committed boot id");
        assert_eq!(observed, 80.0);
        assert_eq!(monotonic, 4.0);
        assert_eq!(boot_id, "boot-a");
        let name: String = conn
            .query_row(
                "SELECT name FROM sqlite_master
                  WHERE type = 'table' AND name = 'conversation_adapter_server_clocks'",
                [],
                |row| row.get(0),
            )
            .expect("table name");
        assert_eq!(name, "conversation_adapter_server_clocks");
    }

    #[test]
    fn falsy_platform_boot_id_falls_back_inside_this_module() {
        assert_eq!(resolve_boot_id(Some("abc\n".to_string()), 7, 1.5), "abc");
        assert_eq!(
            resolve_boot_id(Some("  windows-boot-9  ".to_string()), 7, 1.5),
            "windows-boot-9"
        );
        assert_eq!(
            resolve_boot_id(Some("   \n".to_string()), 7, 1.5),
            "process:7:1.5"
        );
        assert_eq!(
            resolve_boot_id(Some(String::new()), 7, 1.5),
            "process:7:1.5"
        );
        assert_eq!(resolve_boot_id(None, 7, 1.5), "process:7:1.5");
        assert_eq!(
            resolve_boot_id(accepted_platform_boot(Ok::<_, ()>(None::<String>)), 7, 1.5),
            "process:7:1.5"
        );
        assert_eq!(
            resolve_boot_id(
                accepted_platform_boot(Err("use the platform boot id reader on POSIX")),
                7,
                1.5
            ),
            "process:7:1.5"
        );
        assert_eq!(
            resolve_boot_id(
                accepted_platform_boot::<&str>(Ok(Some("windows-boot-9".to_string()))),
                7,
                1.5
            ),
            "windows-boot-9"
        );
        let whitespace = accepted_platform_boot::<&str>(Ok(Some(" \n".to_string())));
        assert_eq!(resolve_boot_id(whitespace, 7, 1.5), "process:7:1.5");
    }

    #[cfg(not(windows))]
    #[test]
    fn posix_boot_id_reader_falls_back_when_the_file_is_missing_or_empty() {
        let file = NamedTempFile::new().expect("temporary boot id file");
        let missing = file.path().with_file_name("missing-adapter-clock-boot-id");
        assert!(super::read_boot_text_at(&missing).is_none());
        assert_eq!(
            resolve_boot_id(super::read_boot_text_at(&missing), 4, 1.25),
            "process:4:1.25"
        );

        fs::write(file.path(), " \n").expect("write blank boot id");
        assert_eq!(
            resolve_boot_id(super::read_boot_text_at(file.path()), 4, 1.25),
            "process:4:1.25"
        );
        fs::write(file.path(), b"\xff").expect("write invalid boot id");
        assert!(super::read_boot_text_at(file.path()).is_none());

        fs::write(file.path(), "unit-boot\n").expect("write boot id");
        assert_eq!(
            resolve_boot_id(super::read_boot_text_at(file.path()), 4, 1.25),
            "unit-boot"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn posix_boot_id_comes_from_procfs_or_the_process_fallback() {
        let boot = process_boot_id();
        assert_eq!(process_boot_id(), boot);
        assert!(!boot.contains("use the platform boot id reader"), "{boot}");
        match fs::read_to_string(super::POSIX_BOOT_ID_PATH) {
            Ok(text) if !text.trim().is_empty() => assert_eq!(boot, text.trim()),
            _ => {
                let prefix = format!("process:{}:", std::process::id());
                assert!(boot.starts_with(&prefix), "{boot}");
            }
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_boot_id_comes_from_proc_or_the_process_fallback() {
        let boot = process_boot_id();
        let prefix = format!("process:{}:", std::process::id());
        assert!(
            boot.starts_with("windows-boot-") || boot.starts_with(&prefix),
            "{boot}"
        );
    }

    #[test]
    fn monotonic_time_does_not_go_backwards() {
        let first = raw_monotonic_seconds();
        let second = raw_monotonic_seconds();
        assert!(first.is_finite() && first >= 0.0, "{first}");
        assert!(second >= first, "{first} then {second}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn monotonic_time_tracks_the_boot_clock() {
        // `/proc/uptime` includes time spent suspended. `CLOCK_MONOTONIC` does
        // not. Linux CI hosts are not suspended, so the two stay within a few
        // seconds. A process-local clock would be near zero.
        let text = fs::read_to_string("/proc/uptime").expect("read /proc/uptime");
        let seconds = text.split_whitespace().next().expect("uptime field");
        let uptime: f64 = seconds.parse().expect("parse uptime");
        let tick = raw_monotonic_seconds();
        let delta = (tick - uptime).abs();
        assert!(
            delta < 5.0,
            "monotonic {tick} should track /proc/uptime {uptime} (delta {delta})"
        );
        let via_api = monotonic_time();
        assert!(via_api >= tick);
        assert!(via_api - tick < 5.0, "api {via_api} raw {tick}");
    }
}
