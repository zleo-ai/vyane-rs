#![allow(clippy::unwrap_used)]

//! Same-process multi-connection hammer for `SqliteAgentStore`.
//!
//! The resident broker's agent projection loop is only one writer/reader
//! against this file, but the store uses the same `SQLITE_OPEN_NO_MUTEX`
//! connection-per-call shape plus a `.write-lock` flock on writes. This hammer
//! overlaps reads (`unprojected_events`, `get_run`) with writes (`mark_projected`,
//! `enqueue_run`) on one `agent.sqlite3`.
//!
//! This test is a hammer, not a deterministic reproducer. The deadlock is
//! probabilistic. Default runtime is a few seconds for CI; raise
//! `VYANE_STORE_HAMMER_MS` for longer probes.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;
use tempfile::TempDir;
use vyane_agent::{
    AgentStore, ExecutionBackend, NewAgentRun, NewWorker, RunMode, SqliteAgentStore,
};

const OWNER: &str = "alice";
const PROJECTOR: &str = "vyane.event-log.agent-lifecycle.v1";
const WORKER_ID: &str = "worker";
const DEFAULT_DURATION_MS: u64 = 2_500;
const DEFAULT_STALL_MS: u64 = 30_000;
const DEFAULT_THREADS: usize = 8;
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
    latest_run: Mutex<Option<String>>,
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

fn digest(byte: char) -> String {
    std::iter::repeat_n(byte, 64).collect()
}

fn run(id: &str) -> NewAgentRun {
    NewAgentRun {
        id: id.into(),
        worker_id: WORKER_ID.into(),
        task_id: Some(format!("task-{id}")),
        trace_id: Some(format!("trace-{id}")),
        parent_run_id: None,
        execution_backend: ExecutionBackend::NativeInProcess,
        mode: RunMode::Autonomous,
        target_key: "native/default".into(),
        prompt_digest: digest('a'),
        policy_digest: digest('b'),
        available_at: Utc::now(),
        timeout_seconds: 60,
        max_resume_attempts: 1,
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

fn worker_loop(store: SqliteAgentStore, shared: Arc<Shared>, probe: Arc<Probe>, worker: usize) {
    while !shared.stop.load(Ordering::SeqCst) && !shared.stalled.load(Ordering::SeqCst) {
        match worker {
            0 => run_op(&shared, &probe, worker, "unprojected_events", || {
                if store.unprojected_events(OWNER, PROJECTOR, 16).is_err() {
                    shared.errors.fetch_add(1, Ordering::SeqCst);
                }
            }),
            1 => run_op(&shared, &probe, worker, "mark_projected", || {
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
            2 => run_op(&shared, &probe, worker, "get_run", || {
                match shared.latest_run.lock().unwrap().clone() {
                    Some(id) => {
                        if store.get_run(OWNER, &id).is_err() {
                            shared.errors.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    None => {
                        if store.get_worker(OWNER, WORKER_ID).is_err() {
                            shared.errors.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }),
            3 => run_op(&shared, &probe, worker, "enqueue_run", || {
                let id = format!("run-{}", shared.seq.fetch_add(1, Ordering::SeqCst));
                match store.enqueue_run(OWNER, &run(&id)) {
                    Ok(record) => {
                        *shared.latest_run.lock().unwrap() = Some(record.id);
                    }
                    Err(_) => {
                        shared.errors.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }),
            _ => {
                let which = shared.seq.fetch_add(1, Ordering::SeqCst) % 4;
                match which {
                    0 => run_op(&shared, &probe, worker, "unprojected_events", || {
                        let _ = store.unprojected_events(OWNER, PROJECTOR, 8);
                    }),
                    1 => run_op(&shared, &probe, worker, "get_worker", || {
                        let _ = store.get_worker(OWNER, WORKER_ID);
                    }),
                    2 => run_op(&shared, &probe, worker, "mark_projected", || {
                        if let Ok(page) = store.unprojected_events(OWNER, PROJECTOR, 4) {
                            for event in page.items {
                                let _ = store.mark_projected(OWNER, PROJECTOR, &event.event_id);
                            }
                        }
                    }),
                    _ => run_op(&shared, &probe, worker, "enqueue_run", || {
                        let id = format!("mix-{}", shared.seq.fetch_add(1, Ordering::SeqCst));
                        if let Ok(record) = store.enqueue_run(OWNER, &run(&id)) {
                            *shared.latest_run.lock().unwrap() = Some(record.id);
                        }
                    }),
                }
            }
        }
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
    let store = SqliteAgentStore::open(directory.path().join("agent.sqlite3")).unwrap();
    store
        .create_root(
            OWNER,
            &NewWorker {
                id: WORKER_ID.into(),
                logical_session_id: None,
            },
            &run("root-run"),
        )
        .unwrap();
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
        latest_run: Mutex::new(Some("root-run".into())),
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
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        for join in joins {
            let _ = join.join();
        }
        let _ = tx.send(());
    });
    if rx.recv_timeout(stall).is_err() && !shared.stalled.load(Ordering::SeqCst) {
        record_stall(&shared, &probes, "workers did not exit after stop");
    }
    assert!(
        !shared.stalled.load(Ordering::SeqCst),
        "same-process agent-store connections stalled past the 5s busy/write-lock timeout"
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
fn same_process_connections_do_not_stall_under_projector_shaped_overlap() {
    let threads = env_usize("VYANE_STORE_HAMMER_THREADS", DEFAULT_THREADS).clamp(2, 16);
    let duration_ms = env_u64("VYANE_STORE_HAMMER_MS", DEFAULT_DURATION_MS);
    let stall_ms = env_u64("VYANE_STORE_HAMMER_STALL_MS", DEFAULT_STALL_MS);
    hammer(
        threads,
        Duration::from_millis(duration_ms),
        Duration::from_millis(stall_ms),
    );
}
