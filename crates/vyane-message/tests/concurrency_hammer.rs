#![allow(clippy::unwrap_used)]

//! Same-process multi-connection hammer for `SqliteMessageStore`.
//!
//! Fingerprint-2 (vyane-task) deadlocked when two `SQLITE_OPEN_NO_MUTEX`
//! connections from one process overlapped: SQLite's POSIX-lock handling
//! parked both threads in `futex_wait` past the 5s busy timeout. Message-store
//! writers take an extra `.write-lock` flock, but readers (`unprojected_events`,
//! `get`, `events`) do not, so a read connection can still overlap a write
//! connection the same way.
//!
//! This test is a hammer, not a deterministic reproducer. The deadlock is
//! probabilistic. Linux reproduction without store serialization: 2 threads
//! (`expire_due` vs `unprojected_events`) stalled at 534 and 7082 ops with
//! both workers in `futex_do_wait` past the 5s busy timeout. Default runtime
//! is a few seconds for CI; raise `VYANE_STORE_HAMMER_MS` for longer probes.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{TimeDelta, Utc};
use tempfile::TempDir;
use vyane_message::{
    ClaimQuery, DeliveryMailbox, EndpointKind, EndpointRef, IdempotencyKey, LeaseRequest,
    MessageDirection, MessageStore, NewDelivery, NewMessage, SqliteMessageStore,
};

const OWNER: &str = "alice";
const PROJECTOR: &str = "vyane.event-log.message-lifecycle.v1";
const DEFAULT_DURATION_MS: u64 = 4_000;
const DEFAULT_STALL_MS: u64 = 30_000;
/// Two threads (expire_due writer vs unprojected_events reader) is the pairing
/// that reproduced the same-process SQLite stall on Linux. More threads still
/// overlap, but the two-connection case is the regression probe.
const DEFAULT_THREADS: usize = 2;
const LOG_CAP: usize = 64;

struct Probe {
    last_start_ms: AtomicU64,
    last_finish_ms: AtomicU64,
    in_op: AtomicBool,
    op: Mutex<String>,
}

struct Shared {
    stop: AtomicBool,
    stalled: AtomicBool,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    completed: AtomicU64,
    errors: AtomicU64,
    seq: AtomicU64,
    origin: Instant,
    stall_ms: u64,
    log: Mutex<VecDeque<String>>,
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn elapsed_ms(origin: Instant) -> u64 {
    u64::try_from(origin.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn mailbox(id: &str) -> DeliveryMailbox {
    DeliveryMailbox {
        route: "worker".into(),
        target: EndpointRef {
            kind: EndpointKind::Worker,
            id: id.into(),
        },
    }
}

fn message(key: &str, body: &str, target: &str) -> NewMessage {
    NewMessage {
        conversation_id: "conversation-1".into(),
        session_id: Some("session-1".into()),
        direction: MessageDirection::Internal,
        kind: "message".into(),
        sender: EndpointRef {
            kind: EndpointKind::Agent,
            id: "sender".into(),
        },
        body: body.into(),
        payload: serde_json::json!({"safe": "shape"}),
        reply_to: None,
        trace_id: Some("trace-1".into()),
        correlation_id: Some("correlation-1".into()),
        idempotency: IdempotencyKey {
            producer: "hammer".into(),
            key: key.into(),
        },
        deliveries: vec![NewDelivery {
            route: "worker".into(),
            target: EndpointRef {
                kind: EndpointKind::Worker,
                id: target.into(),
            },
            available_at: None,
            expires_at: None,
            max_attempts: 3,
        }],
    }
}

fn push_log(shared: &Shared, line: String) {
    let mut log = shared.log.lock().unwrap();
    if log.len() == LOG_CAP {
        log.pop_front();
    }
    log.push_back(line);
}

fn dump_linux_tasks() -> String {
    let mut out = String::new();
    let Ok(dir) = fs::read_dir("/proc/self/task") else {
        return "no /proc/self/task\n".into();
    };
    for entry in dir.flatten() {
        let path = entry.path();
        let tid = entry.file_name();
        let comm = fs::read_to_string(path.join("comm")).unwrap_or_default();
        let wchan = fs::read_to_string(path.join("wchan")).unwrap_or_default();
        let mut state = String::new();
        if let Ok(status) = fs::read_to_string(path.join("status")) {
            for line in status.lines() {
                if line.starts_with("State:") || line.starts_with("Name:") {
                    state.push_str(line.trim());
                    state.push(' ');
                }
            }
        }
        let _ = writeln!(
            out,
            "tid={} comm={} wchan={} {}",
            tid.to_string_lossy(),
            comm.trim(),
            wchan.trim(),
            state.trim()
        );
    }
    out
}

fn record_stall(shared: &Shared, probes: &[Arc<Probe>], reason: &str) {
    shared.stalled.store(true, Ordering::SeqCst);
    shared.stop.store(true, Ordering::SeqCst);
    let now = elapsed_ms(shared.origin);
    eprintln!("HAMMER STALL: {reason} at {now}ms");
    eprintln!(
        "completed={} errors={} in_flight={} max_in_flight={}",
        shared.completed.load(Ordering::SeqCst),
        shared.errors.load(Ordering::SeqCst),
        shared.in_flight.load(Ordering::SeqCst),
        shared.max_in_flight.load(Ordering::SeqCst)
    );
    for (id, probe) in probes.iter().enumerate() {
        eprintln!(
            "worker {id}: last_start={} last_finish={} op={}",
            probe.last_start_ms.load(Ordering::SeqCst),
            probe.last_finish_ms.load(Ordering::SeqCst),
            probe.op.lock().unwrap()
        );
    }
    for line in shared.log.lock().unwrap().iter() {
        eprintln!("log {line}");
    }
    eprint!("{}", dump_linux_tasks());
}

fn maybe_set_journal_mode(path: &Path) {
    let Ok(mode) = std::env::var("VYANE_STORE_HAMMER_JOURNAL") else {
        return;
    };
    if mode.is_empty() {
        return;
    }
    let connection = rusqlite::Connection::open(path).unwrap();
    connection
        .pragma_update(None, "journal_mode", mode.as_str())
        .unwrap();
    let applied: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .unwrap();
    eprintln!("hammer journal_mode requested={mode} applied={applied}");
}

fn run_op(shared: &Shared, probe: &Probe, worker: usize, name: &str, body: impl FnOnce()) {
    if shared.stalled.load(Ordering::SeqCst) {
        return;
    }
    let start = elapsed_ms(shared.origin);
    probe.last_start_ms.store(start, Ordering::SeqCst);
    probe.in_op.store(true, Ordering::SeqCst);
    *probe.op.lock().unwrap() = name.to_string();
    let in_flight = shared.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    shared.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
    push_log(
        shared,
        format!("{start} w{worker} start {name} in_flight={in_flight}"),
    );
    body();
    let finish = elapsed_ms(shared.origin);
    shared.in_flight.fetch_sub(1, Ordering::SeqCst);
    probe.last_finish_ms.store(finish, Ordering::SeqCst);
    probe.in_op.store(false, Ordering::SeqCst);
    shared.completed.fetch_add(1, Ordering::SeqCst);
    push_log(shared, format!("{finish} w{worker} end {name}"));
}

fn worker_loop(store: SqliteMessageStore, shared: Arc<Shared>, probe: Arc<Probe>, worker: usize) {
    let mut seeded_ids: Vec<String> = Vec::new();
    while !shared.stop.load(Ordering::SeqCst) && !shared.stalled.load(Ordering::SeqCst) {
        match worker {
            0 => run_op(&shared, &probe, worker, "expire_due", || {
                if store.expire_due(OWNER, 128).is_err() {
                    shared.errors.fetch_add(1, Ordering::SeqCst);
                }
            }),
            1 => run_op(&shared, &probe, worker, "unprojected_events", || {
                if store.unprojected_events(OWNER, PROJECTOR, 16).is_err() {
                    shared.errors.fetch_add(1, Ordering::SeqCst);
                }
            }),
            2 => run_op(&shared, &probe, worker, "claim_ack", || {
                match store.claim(
                    OWNER,
                    &ClaimQuery {
                        mailboxes: vec![mailbox("worker-a")],
                        limit: 4,
                    },
                    &LeaseRequest {
                        consumer: format!("consumer-{worker}"),
                        lease_seconds: 30,
                    },
                ) {
                    Ok(claimed) => {
                        for leased in claimed {
                            if store
                                .acknowledge(OWNER, &leased.receipt.mailbox, &leased.receipt)
                                .is_err()
                            {
                                shared.errors.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    }
                    Err(_) => {
                        shared.errors.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }),
            3 => run_op(&shared, &probe, worker, "mark_projected", || {
                match store.unprojected_events(OWNER, PROJECTOR, 8) {
                    Ok(page) => {
                        for event in page.items {
                            if store
                                .mark_projected(OWNER, PROJECTOR, &event.event_id)
                                .is_err()
                            {
                                shared.errors.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    }
                    Err(_) => {
                        shared.errors.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }),
            4 => run_op(
                &shared,
                &probe,
                worker,
                "snapshot_read",
                || match seeded_ids.last() {
                    Some(id) => {
                        if store.get(OWNER, id).is_err() {
                            shared.errors.fetch_add(1, Ordering::SeqCst);
                        }
                        if store.events(OWNER, id).is_err() {
                            shared.errors.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    None => {
                        if store.unprojected_events(OWNER, PROJECTOR, 1).is_err() {
                            shared.errors.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                },
            ),
            5 => run_op(&shared, &probe, worker, "reclaim_expired", || {
                if store.reclaim_expired(OWNER, 128).is_err() {
                    shared.errors.fetch_add(1, Ordering::SeqCst);
                }
            }),
            6 => run_op(&shared, &probe, worker, "enqueue", || {
                let key = format!("live-{}", shared.seq.fetch_add(1, Ordering::SeqCst));
                match store.enqueue(OWNER, &message(&key, "body", "worker-a")) {
                    Ok(outcome) => seeded_ids.push(outcome.bundle.message.id),
                    Err(_) => {
                        shared.errors.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }),
            _ => {
                let which = shared.seq.fetch_add(1, Ordering::SeqCst) % 6;
                match which {
                    0 => run_op(&shared, &probe, worker, "expire_due", || {
                        let _ = store.expire_due(OWNER, 32);
                    }),
                    1 => run_op(&shared, &probe, worker, "unprojected_events", || {
                        let _ = store.unprojected_events(OWNER, PROJECTOR, 8);
                    }),
                    2 => run_op(&shared, &probe, worker, "reclaim_expired", || {
                        let _ = store.reclaim_expired(OWNER, 32);
                    }),
                    3 => run_op(&shared, &probe, worker, "snapshot_read", || {
                        let _ = store.unprojected_events(OWNER, PROJECTOR, 1);
                    }),
                    4 => run_op(&shared, &probe, worker, "claim", || {
                        let _ = store.claim(
                            OWNER,
                            &ClaimQuery {
                                mailboxes: vec![mailbox("worker-a")],
                                limit: 1,
                            },
                            &LeaseRequest {
                                consumer: format!("mix-{worker}"),
                                lease_seconds: 30,
                            },
                        );
                    }),
                    _ => run_op(&shared, &probe, worker, "enqueue", || {
                        let key = format!("mix-{}", shared.seq.fetch_add(1, Ordering::SeqCst));
                        let _ = store.enqueue(OWNER, &message(&key, "body", "worker-a"));
                    }),
                }
            }
        }
    }
}

fn await_workers(
    joins: Vec<thread::JoinHandle<()>>,
    shared: &Shared,
    probes: &[Arc<Probe>],
    stall: Duration,
) {
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        for join in joins {
            let _ = join.join();
        }
        let _ = tx.send(());
    });
    if rx.recv_timeout(stall).is_err() && !shared.stalled.load(Ordering::SeqCst) {
        record_stall(shared, probes, "workers did not exit after stop");
    }
}

fn check_stall(shared: &Shared, probes: &[Arc<Probe>]) -> bool {
    let now = elapsed_ms(shared.origin);
    for (id, probe) in probes.iter().enumerate() {
        let start = probe.last_start_ms.load(Ordering::SeqCst);
        let in_op = probe.in_op.load(Ordering::SeqCst);
        if in_op && now.saturating_sub(start) >= shared.stall_ms {
            record_stall(
                shared,
                probes,
                &format!(
                    "worker {id} op {} ran {}ms (stall budget {}ms)",
                    probe.op.lock().unwrap(),
                    now.saturating_sub(start),
                    shared.stall_ms
                ),
            );
            return true;
        }
    }
    false
}

fn hammer(threads: usize, duration: Duration, stall: Duration) {
    let directory = TempDir::new().unwrap();
    let path: PathBuf = directory.path().join("messages.sqlite3");
    let store = SqliteMessageStore::open(&path).unwrap();
    maybe_set_journal_mode(&path);
    for index in 0..32 {
        store
            .enqueue(
                OWNER,
                &message(&format!("seed-{index}"), "seed", "worker-a"),
            )
            .unwrap();
    }
    let shared = Arc::new(Shared {
        stop: AtomicBool::new(false),
        stalled: AtomicBool::new(false),
        in_flight: AtomicUsize::new(0),
        max_in_flight: AtomicUsize::new(0),
        completed: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        seq: AtomicU64::new(0),
        origin: Instant::now(),
        stall_ms: u64::try_from(stall.as_millis()).unwrap_or(u64::MAX),
        log: Mutex::new(VecDeque::new()),
    });
    let probes: Vec<Arc<Probe>> = (0..threads)
        .map(|_| {
            Arc::new(Probe {
                last_start_ms: AtomicU64::new(0),
                last_finish_ms: AtomicU64::new(0),
                in_op: AtomicBool::new(false),
                op: Mutex::new(String::from("idle")),
            })
        })
        .collect();
    let mut joins = Vec::with_capacity(threads);
    for (worker, probe) in probes.iter().enumerate() {
        let store = store.clone();
        let shared = Arc::clone(&shared);
        let probe = Arc::clone(probe);
        joins.push(thread::spawn(move || {
            worker_loop(store, shared, probe, worker);
        }));
    }
    let verbose = std::env::var("VYANE_STORE_HAMMER_VERBOSE").is_ok();
    let stop_at = Instant::now() + duration;
    loop {
        if check_stall(&shared, &probes) {
            break;
        }
        if Instant::now() >= stop_at {
            shared.stop.store(true, Ordering::SeqCst);
            break;
        }
        if verbose {
            eprintln!(
                "progress completed={} errors={} in_flight={} max={}",
                shared.completed.load(Ordering::SeqCst),
                shared.errors.load(Ordering::SeqCst),
                shared.in_flight.load(Ordering::SeqCst),
                shared.max_in_flight.load(Ordering::SeqCst)
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
    await_workers(joins, &shared, &probes, stall);
    assert!(
        !shared.stalled.load(Ordering::SeqCst),
        "same-process message-store connections stalled past the 5s busy/write-lock timeout"
    );
    let completed = shared.completed.load(Ordering::SeqCst);
    let max_in_flight = shared.max_in_flight.load(Ordering::SeqCst);
    assert!(completed > 0, "hammer completed no operations");
    assert!(
        max_in_flight >= 2,
        "hammer never overlapped two store operations (max_in_flight={max_in_flight})"
    );
}

#[test]
fn same_process_connections_do_not_stall_under_supervisor_shaped_overlap() {
    let threads = env_usize("VYANE_STORE_HAMMER_THREADS", DEFAULT_THREADS).clamp(2, 16);
    let duration_ms = env_u64("VYANE_STORE_HAMMER_MS", DEFAULT_DURATION_MS);
    let stall_ms = env_u64("VYANE_STORE_HAMMER_STALL_MS", DEFAULT_STALL_MS);
    hammer(
        threads,
        Duration::from_millis(duration_ms),
        Duration::from_millis(stall_ms),
    );
}

#[test]
fn clock_advance_keeps_expire_due_writing_against_readers() {
    // A second, shorter shape: expire_due actually mutates rows while readers
    // hammer unprojected_events/get, matching maintenance_once vs project_once
    // and the wait_until(get) caller in the no-lane supervisor fixture.
    let directory = TempDir::new().unwrap();
    let clock_now = Arc::new(Mutex::new(Utc::now() - TimeDelta::seconds(60)));
    struct Clock(Arc<Mutex<chrono::DateTime<Utc>>>);
    impl vyane_message::MessageClock for Clock {
        fn now(&self) -> chrono::DateTime<Utc> {
            *self.0.lock().unwrap()
        }
    }
    let store = SqliteMessageStore::open_with_clock(
        directory.path().join("messages.sqlite3"),
        Arc::new(Clock(Arc::clone(&clock_now))),
    )
    .unwrap();
    let enqueue_now = *clock_now.lock().unwrap();
    for index in 0..16 {
        let mut request = message(&format!("exp-{index}"), "expiring", "worker-a");
        request.deliveries[0].expires_at = Some(enqueue_now + TimeDelta::seconds(5));
        store.enqueue(OWNER, &request).unwrap();
    }
    *clock_now.lock().unwrap() = enqueue_now + TimeDelta::seconds(30);
    let shared = Arc::new(Shared {
        stop: AtomicBool::new(false),
        stalled: AtomicBool::new(false),
        in_flight: AtomicUsize::new(0),
        max_in_flight: AtomicUsize::new(0),
        completed: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        seq: AtomicU64::new(0),
        origin: Instant::now(),
        stall_ms: DEFAULT_STALL_MS,
        log: Mutex::new(VecDeque::new()),
    });
    let probes: Vec<Arc<Probe>> = (0..4)
        .map(|_| {
            Arc::new(Probe {
                last_start_ms: AtomicU64::new(0),
                last_finish_ms: AtomicU64::new(0),
                in_op: AtomicBool::new(false),
                op: Mutex::new(String::from("idle")),
            })
        })
        .collect();
    let mut joins = Vec::new();
    for (worker, probe) in probes.iter().enumerate() {
        let store = store.clone();
        let shared = Arc::clone(&shared);
        let probe = Arc::clone(probe);
        joins.push(thread::spawn(move || {
            worker_loop(store, shared, probe, worker);
        }));
    }
    let stop_at = Instant::now() + Duration::from_millis(DEFAULT_DURATION_MS);
    while Instant::now() < stop_at {
        if check_stall(&shared, &probes) {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    shared.stop.store(true, Ordering::SeqCst);
    await_workers(
        joins,
        &shared,
        &probes,
        Duration::from_millis(DEFAULT_STALL_MS),
    );
    assert!(!shared.stalled.load(Ordering::SeqCst));
    assert!(shared.max_in_flight.load(Ordering::SeqCst) >= 2);
}
