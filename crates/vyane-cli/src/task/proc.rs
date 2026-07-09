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

#[cfg(unix)]
use chrono::{DateTime, Utc};

/// `SIGTERM`: ask the group to terminate cleanly (the worker catches it and
/// finalizes its `RunRecord` + status file).
#[cfg(unix)]
pub const SIGTERM: i32 = 15;
/// `SIGKILL`: force-kill anything in the group still alive after the grace.
#[cfg(unix)]
pub const SIGKILL: i32 = 9;

/// Tolerance window when matching a process's start time against the task's
/// recorded `started_at`. The worker constructs `StatusFile::running` (which
/// stamps `started_at = Utc::now()`) as one of the first things it does after
/// exec, so its recorded `started_at` and the kernel-reported process start
/// time refer to the same instant to within a small skew. `ps` reports elapsed
/// time only to whole-second resolution, and there is a brief gap between the
/// kernel starting the process and the worker taking the `Utc::now()` stamp, so
/// a few seconds of slack is expected; ±30s is comfortably wider than CI runner
/// scheduling jitter and process-start skew, yet far tighter than any realistic
/// pid-reuse interval (a reused pid would have a start time differing by
/// minutes/hours, not seconds).
#[cfg(unix)]
pub const IDENTITY_START_TOLERANCE_SECS: i64 = 30;

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

/// The outcome of validating that the process currently occupying `pid` is
/// still the worker a task recorded — the guard that prevents signalling (or
/// mislabelling) a process whose pid has been reused since the task started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityCheck {
    /// The pid is live and both its process group and start time match the
    /// task's record: it is (to a very high confidence) the same worker.
    Match,
    /// No process currently holds this pid — the worker is gone.
    Dead,
    /// A process holds the pid but its group or start time does not match the
    /// task's record: the pid was almost certainly reused by an unrelated
    /// process. Signalling it would hit the wrong target.
    Mismatch(&'static str),
}

/// Validate that the process now holding `pid` is the same worker a task
/// recorded, by checking **both**:
///
/// 1. **process group** — `getpgid(pid)` must equal the recorded `pgid`. The
///    worker is a group leader (`pgid == pid` at spawn); an unrelated reused
///    pid will (almost always) live in a different group.
/// 2. **start time** — the process's start time, obtained from `ps`, must match
///    the task's recorded `started_at` within [`IDENTITY_START_TOLERANCE_SECS`]
///    (see that constant for why the window exists and why it is safe). A
///    reused pid necessarily started later than the original worker, so its
///    start time diverges by far more than the tolerance.
///
/// Both must hold to return [`IdentityCheck::Match`]. This is the check the
/// canceller runs before delivering any signal, and the check orphan detection
/// runs before deciding a still-`running` task is merely alive vs. genuinely
/// its own worker.
#[cfg(unix)]
pub fn verify_identity(pid: i32, expected_pgid: i32, started_at: DateTime<Utc>) -> IdentityCheck {
    if pid <= 0 {
        return IdentityCheck::Dead;
    }
    // Liveness first: a pid nobody holds is unambiguously dead. `pid_alive`
    // treats EPERM (exists but not signalable) as alive, which is correct here
    // — a live-but-foreign process must be caught by the identity checks below,
    // not waved through as dead.
    if !pid_alive(pid) {
        return IdentityCheck::Dead;
    }
    // (a) process-group identity.
    match pgid_of(pid) {
        // pid vanished between the liveness probe and here → treat as dead.
        None => return IdentityCheck::Dead,
        Some(pgid) if pgid != expected_pgid => {
            return IdentityCheck::Mismatch("process group mismatch");
        }
        Some(_) => {}
    }
    // (b) start-time identity.
    match process_start_time(pid) {
        // Could not read the process's start time (it may have just exited, or
        // `ps` is unavailable). Fail closed: without positive confirmation that
        // this is our worker we must not signal it.
        None => IdentityCheck::Mismatch("could not read process start time"),
        Some(actual_start) => {
            let skew = (actual_start - started_at).num_seconds().abs();
            if skew <= IDENTITY_START_TOLERANCE_SECS {
                IdentityCheck::Match
            } else {
                IdentityCheck::Mismatch("process start time mismatch")
            }
        }
    }
}

#[cfg(not(unix))]
pub fn verify_identity(
    _pid: i32,
    _expected_pgid: i32,
    _started_at: chrono::DateTime<chrono::Utc>,
) -> IdentityCheck {
    // Without POSIX we cannot cheaply establish identity; report Match so the
    // (Unix-only) detach feature's callers degrade to their pre-identity
    // behaviour rather than refusing every operation on an unsupported platform.
    IdentityCheck::Match
}

/// The wall-clock start time of the process currently holding `pid`, derived
/// from `ps -p <pid> -o etime=` (elapsed running time) subtracted from *now*.
///
/// Why `etime` rather than `lstart`: `lstart` prints an absolute, **locale- and
/// timezone-dependent** timestamp (`Mon Jul  7 08:24:13 2026`) that is fragile
/// to parse portably. `etime` is a fixed, locale-independent duration format
/// (`[[DD-]HH:]MM:SS`) that every POSIX `ps` emits identically, so we parse that
/// and compute `start ≈ now - etime`. The subtraction inherits `ps`'s
/// whole-second resolution, which the ±tolerance window in [`verify_identity`]
/// already accounts for. Returns `None` if `ps` fails, prints nothing, or emits
/// an unparseable field.
#[cfg(unix)]
pub fn process_start_time(pid: i32) -> Option<DateTime<Utc>> {
    if pid <= 0 {
        return None;
    }
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "etime="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let raw = String::from_utf8_lossy(&output.stdout);
    let etime = raw.trim();
    let elapsed_secs = parse_etime_secs(etime)?;
    let now = Utc::now();
    now.checked_sub_signed(chrono::Duration::seconds(elapsed_secs))
}

/// Parse a POSIX `ps` `etime` field (`[[DD-]HH:]MM:SS`) into whole seconds.
///
/// Accepted shapes (all produced by `ps -o etime`):
/// - `MM:SS`
/// - `HH:MM:SS`
/// - `DD-HH:MM:SS`
///
/// Returns `None` for anything malformed so the caller can fail closed.
#[cfg(unix)]
fn parse_etime_secs(etime: &str) -> Option<i64> {
    let etime = etime.trim();
    if etime.is_empty() {
        return None;
    }
    // Split off an optional leading `DD-` day component.
    let (days, hms) = match etime.split_once('-') {
        Some((d, rest)) => (d.parse::<i64>().ok()?, rest),
        None => (0, etime),
    };
    // Remaining is MM:SS or HH:MM:SS.
    let parts: Vec<&str> = hms.split(':').collect();
    let (hours, minutes, seconds) = match parts.as_slice() {
        [mm, ss] => (0i64, mm.parse::<i64>().ok()?, ss.parse::<i64>().ok()?),
        [hh, mm, ss] => (
            hh.parse::<i64>().ok()?,
            mm.parse::<i64>().ok()?,
            ss.parse::<i64>().ok()?,
        ),
        _ => return None,
    };
    // Field ranges as `ps` actually emits them: seconds and minutes are 0–59,
    // and hours are 0–23 (past 24h `ps` rolls into the `DD-` day prefix, so a
    // bare or day-prefixed HH field never exceeds 23). Anything outside these
    // ranges is not real `ps` output → reject so the caller fails closed.
    if !(0..60).contains(&seconds)
        || !(0..60).contains(&minutes)
        || !(0..24).contains(&hours)
        || days < 0
    {
        return None;
    }
    Some(((days * 24 + hours) * 60 + minutes) * 60 + seconds)
}

#[cfg(not(unix))]
pub fn process_start_time(_pid: i32) -> Option<chrono::DateTime<chrono::Utc>> {
    None
}

#[cfg(unix)]
const EPERM: i32 = 1;

/// Best-effort read of the last OS errno, via `std::io::Error`.
#[cfg(unix)]
fn last_errno() -> Option<i32> {
    std::io::Error::last_os_error().raw_os_error()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn own_pid_is_alive() {
        let me = std::process::id() as i32;
        assert!(pid_alive(me), "the test process must probe as alive");
    }

    #[test]
    fn nonexistent_pid_is_dead() {
        // i32::MAX is far above any real pid; kill(pid, 0) reports ESRCH.
        assert!(!pid_alive(i32::MAX));
    }

    #[test]
    fn nonpositive_pid_is_dead() {
        // 0 and negatives address groups / special targets, never a live pid
        // for a liveness probe — treat as dead so orphan detection is safe.
        assert!(!pid_alive(0));
        assert!(!pid_alive(-1));
    }

    #[test]
    fn own_pgid_is_available() {
        let me = std::process::id() as i32;
        let pgid = pgid_of(me).expect("own pgid resolvable");
        assert!(pgid > 0);
    }

    #[test]
    fn signal_group_on_dead_group_is_noop() {
        // Signalling a group that does not exist must not panic (ESRCH ignored).
        signal_group(i32::MAX, SIGTERM);
    }

    #[test]
    fn parse_etime_handles_all_shapes() {
        // MM:SS
        assert_eq!(parse_etime_secs("00:05"), Some(5));
        assert_eq!(parse_etime_secs("01:30"), Some(90));
        // HH:MM:SS
        assert_eq!(parse_etime_secs("01:00:00"), Some(3600));
        assert_eq!(parse_etime_secs("02:03:04"), Some(2 * 3600 + 3 * 60 + 4));
        // DD-HH:MM:SS
        assert_eq!(parse_etime_secs("1-00:00:00"), Some(86_400));
        assert_eq!(
            parse_etime_secs("2-01:02:03"),
            Some(2 * 86_400 + 3600 + 2 * 60 + 3)
        );
        // Surrounding whitespace (ps -o etime= can pad) is tolerated.
        assert_eq!(parse_etime_secs("   00:42  "), Some(42));
    }

    #[test]
    fn parse_etime_rejects_malformed() {
        assert_eq!(parse_etime_secs(""), None);
        assert_eq!(parse_etime_secs("nonsense"), None);
        assert_eq!(parse_etime_secs("12"), None); // no colon at all
        assert_eq!(parse_etime_secs("99:99"), None); // seconds out of range
        assert_eq!(parse_etime_secs("00:60"), None); // seconds == 60
        assert_eq!(parse_etime_secs("60:00:00"), None); // minutes == 60 in HH:MM:SS
        assert_eq!(parse_etime_secs("a-00:00"), None); // bad day field
    }

    #[test]
    fn process_start_time_of_real_child_is_recent() {
        // Spawn a real, briefly-living process and confirm we can read a start
        // time for it that is close to "now" (it just started).
        let mut child = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleep");
        let pid = child.id() as i32;

        let start = process_start_time(pid).expect("start time of live child");
        let skew = (Utc::now() - start).num_seconds().abs();
        assert!(
            skew <= IDENTITY_START_TOLERANCE_SECS,
            "child start time skew {skew}s exceeds tolerance"
        );

        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn verify_identity_matches_a_real_worker_like_child() {
        // A real spawned process whose recorded pgid + started_at we control,
        // exactly as the worker records them, must validate as Match.
        let mut child = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleep");
        let pid = child.id() as i32;
        // The child's real group (it is not a group leader here, but whatever
        // getpgid reports is what the worker would have stored).
        let pgid = pgid_of(pid).expect("child pgid");
        // Simulate the worker's started_at: it stamps Utc::now() right after
        // exec, so "now" is a faithful stand-in and lands inside tolerance.
        let started_at = Utc::now();

        assert_eq!(
            verify_identity(pid, pgid, started_at),
            IdentityCheck::Match,
            "a live child with matching pgid + start time must validate"
        );

        // Wrong pgid → mismatch (pid-reuse signature).
        assert!(matches!(
            verify_identity(pid, pgid + 1, started_at),
            IdentityCheck::Mismatch(_)
        ));

        // Start time far in the past → mismatch (the pid we hold started now,
        // not an hour ago, so this models a reused pid).
        let long_ago = Utc::now() - chrono::Duration::hours(1);
        assert!(matches!(
            verify_identity(pid, pgid, long_ago),
            IdentityCheck::Mismatch(_)
        ));

        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn verify_identity_reports_dead_for_absent_pid() {
        // No process holds i32::MAX → Dead, regardless of the recorded fields.
        assert_eq!(
            verify_identity(i32::MAX, i32::MAX, Utc::now()),
            IdentityCheck::Dead
        );
        // Non-positive pids are never live workers.
        assert_eq!(verify_identity(0, 0, Utc::now()), IdentityCheck::Dead);
        assert_eq!(verify_identity(-1, -1, Utc::now()), IdentityCheck::Dead);
    }

    /// Does the process group `pgid` still have any signalable member?
    /// `kill(-pgid, 0)` returns 0 while ≥1 member exists, ESRCH once the group
    /// is empty. (EPERM would mean "exists but not permitted" — not expected for
    /// our own descendants.)
    fn group_has_members(pgid: i32) -> bool {
        // SAFETY: signal 0 to a negative pid probes the group's existence
        // without delivering a signal; no memory-safety implications.
        let rc = unsafe { kill(-pgid, 0) };
        rc == 0
    }

    #[test]
    fn signal_group_reaps_a_child_in_the_group() {
        use std::io::Read as _;
        use std::os::unix::process::CommandExt;
        use std::time::{Duration, Instant};

        // Build a real, multi-process group exactly as the worker would: a
        // group-leader parent (setsid → pgid == its pid) that forks a `sleep`
        // grandchild sharing the group, then prints the grandchild's pid and
        // waits. This is the faithful shape `task cancel` group-kills — proving
        // that group-signalling the STORED pgid reaps the grandchild too, not
        // just the direct child.
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg("sleep 30 & echo $!; wait");
        cmd.stdout(std::process::Stdio::piped());
        // Put the shell in its own new process group (pgid == shell pid), the
        // same setsid trick `install_process_group` installs for a worker.
        // SAFETY: `setsid` is async-signal-safe, takes no args, and is the only
        // call in the pre_exec closure (mirrors `install_process_group`).
        unsafe {
            cmd.pre_exec(|| {
                if setsid_raw() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut shell = cmd.spawn().expect("spawn group-leader shell");
        let leader_pid = shell.id() as i32;
        // The shell's own new group has the shell as leader: pgid == leader_pid.
        let stored_pgid = leader_pid;

        // Read the grandchild (sleep) pid the shell printed on its first line.
        let mut out = shell.stdout.take().expect("piped stdout");
        let child_pid = {
            // Read until we have a full line, with a bounded wait.
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut buf = Vec::new();
            let mut byte = [0u8; 1];
            loop {
                match out.read(&mut byte) {
                    Ok(1) => {
                        if byte[0] == b'\n' {
                            break;
                        }
                        buf.push(byte[0]);
                    }
                    _ => {
                        if Instant::now() >= deadline {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(20));
                    }
                }
            }
            String::from_utf8_lossy(&buf)
                .trim()
                .parse::<i32>()
                .expect("grandchild pid on stdout")
        };

        // Both the leader and the grandchild are alive, and both are in the
        // group (getpgid(child) == stored_pgid).
        assert!(pid_alive(leader_pid), "group leader must be alive");
        assert!(pid_alive(child_pid), "grandchild must be alive");
        assert_eq!(
            pgid_of(child_pid),
            Some(stored_pgid),
            "grandchild must share the leader's group"
        );

        // Group-kill via the STORED pgid (what `task cancel` uses).
        signal_group(stored_pgid, SIGKILL);

        // The grandchild — not just the direct child — must die.
        let child_dead = {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if !pid_alive(child_pid) {
                    break true;
                }
                if Instant::now() >= deadline {
                    break false;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        };
        assert!(child_dead, "grandchild {child_pid} survived a group kill");

        // Reap the shell so we leave no zombie, then confirm the group is empty.
        let _ = shell.wait();
        assert!(
            !group_has_members(stored_pgid),
            "process group {stored_pgid} still has members after group kill"
        );
    }
}
