//! Process control for detached workers: put a worker in its own process
//! group, signal that whole group on `task cancel`, and probe worker liveness
//! for read-side orphan detection.
//!
//! This mirrors the harness spawn machinery (`vyane-harness/src/spawn.rs`): a
//! detached worker is placed in its **own process group** via `setsid(2)` in a
//! `pre_exec` hook, so cancellation can kill the whole group by negative PID
//! (`kill(-pgid, …)`) — a coding-CLI harness the worker itself spawns forks
//! grandchildren (language servers, MCP stdio servers), and a bare child kill
//! would leave them running.
//!
//! The workspace dependency set is frozen (no `libc` / `nix`), so the three
//! POSIX calls this needs — `setsid`, `kill`, and `getpgid` — are declared
//! directly, exactly as the harness crate does. Declaring them here keeps the
//! FFI surface auditable and tiny.

#[cfg(unix)]
use std::process::Command;

/// `SIGTERM`: ask the group to terminate cleanly (the worker catches it and
/// finalizes its `RunRecord` + status file).
#[cfg(unix)]
pub const SIGTERM: i32 = 15;
/// `SIGKILL`: force-kill anything in the group still alive after the grace.
#[cfg(unix)]
pub const SIGKILL: i32 = 9;

#[cfg(unix)]
unsafe extern "C" {
    fn setsid() -> i32;
    fn kill(pid: i32, sig: i32) -> i32;
    fn getpgid(pid: i32) -> i32;
}

/// Install a `pre_exec` hook that makes the spawned worker a process-group
/// leader (`pgid == pid`), so the parent can later signal the whole group.
///
/// Mirrors `vyane-harness`'s `install_process_group`.
#[cfg(unix)]
pub fn install_process_group(cmd: &mut Command) {
    use std::os::unix::process::CommandExt;

    // SAFETY: `pre_exec` runs the closure in the forked child after `fork(2)`
    // and before `execvp(2)`. `setsid(2)` is async-signal-safe and is the only
    // call the closure makes — it takes no arguments, allocates nothing,
    // touches no shared state, and cannot deadlock. It creates a new session
    // and process group with the worker as leader (pgid == worker pid), which
    // is exactly what lets us later signal the whole group by negative pid. On
    // failure `setsid` returns -1 and we surface the errno as a spawn failure.
    // This is one of the sanctioned `unsafe` sites in this crate (see the
    // crate's Cargo.toml lints).
    unsafe {
        cmd.pre_exec(|| {
            if setsid_raw() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
pub fn install_process_group(_cmd: &mut std::process::Command) {
    // Non-Unix: no setsid. Group semantics degrade to a direct child kill.
    // v0.1/v0.2 target Unix; this stub keeps the crate compiling elsewhere.
}

/// Wrapper so the `pre_exec` closure body reads clearly and the `unsafe` intent
/// is documented at one site.
#[cfg(unix)]
fn setsid_raw() -> i32 {
    // SAFETY: `setsid` takes no arguments and returns an int; calling it is
    // safe in the forked-child context (see `install_process_group`).
    unsafe { setsid() }
}

/// Send `sig` to the process group `pgid` by passing the negative pid, which is
/// how `kill(2)` addresses an entire group. Errors (e.g. ESRCH — the group is
/// already gone) are ignored, which is the desired idempotent behaviour.
#[cfg(unix)]
pub fn signal_group(pgid: i32, sig: i32) {
    // SAFETY: `kill` with a negative pid signals the process group; it has no
    // memory-safety implications. The return value is intentionally ignored so
    // signalling an already-dead group is a no-op.
    unsafe {
        let _ = kill(-pgid, sig);
    }
}

/// Is a process with `pid` still alive? Uses `kill(pid, 0)`, which delivers no
/// signal but performs the same existence + permission check the kernel would
/// for a real signal. This is the orphan-detection probe: a status file that
/// still says `running` while its recorded pid is dead means the worker died
/// without finalizing.
///
/// A live process the caller may not signal reports `EPERM` rather than
/// `ESRCH`; we treat that as "alive" — the process demonstrably exists. Only a
/// clear "no such process" counts as dead.
#[cfg(unix)]
pub fn pid_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    // SAFETY: `kill` with signal 0 sends nothing; it only reports whether the
    // target exists and is signalable. No memory-safety implications.
    let rc = unsafe { kill(pid, 0) };
    if rc == 0 {
        return true;
    }
    // rc == -1: distinguish "no such process" from "exists but not permitted".
    matches!(last_errno(), Some(EPERM))
}

#[cfg(not(unix))]
pub fn pid_alive(_pid: i32) -> bool {
    // Without POSIX signals we cannot cheaply probe; assume alive so we never
    // mislabel a run as orphaned on an unsupported platform.
    true
}

/// The process-group id of `pid`, or `None` if it could not be determined
/// (e.g. the process already exited). Used as a fallback when a status file
/// predates recorded `pgid`, and to confirm a worker's group before signalling.
#[cfg(unix)]
pub fn pgid_of(pid: i32) -> Option<i32> {
    if pid <= 0 {
        return None;
    }
    // SAFETY: `getpgid` reads the process-group id of an existing pid and has
    // no memory-safety implications. -1 signals failure (e.g. ESRCH).
    let pgid = unsafe { getpgid(pid) };
    (pgid > 0).then_some(pgid)
}

#[cfg(not(unix))]
pub fn pgid_of(_pid: i32) -> Option<i32> {
    None
}

#[cfg(unix)]
const EPERM: i32 = 1;

/// Best-effort read of the last OS errno, via `std::io::Error`.
#[cfg(unix)]
fn last_errno() -> Option<i32> {
    std::io::Error::last_os_error().raw_os_error()
}
