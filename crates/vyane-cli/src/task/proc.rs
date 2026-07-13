//! Process control for detached workers: put a worker in its own process
//! group, signal that whole group on `task cancel`, and probe worker liveness
//! for read-side orphan detection.
//!
//! A detached worker is placed in its **own POSIX session and process group**
//! through process-wrap's safe `ProcessSession` API (`setsid(2)` internally),
//! so cancellation can kill the whole group
//! by negative PID
//! (`kill(-pgid, …)`) — a coding-CLI harness the worker itself spawns forks
//! grandchildren (language servers, MCP stdio servers), and a bare child kill
//! would leave them running.
//!
//! Process inspection and signalling use rustix's safe POSIX wrappers; this
//! crate therefore needs no local FFI or pre-exec unsafe blocks.

#[cfg(unix)]
use std::process::Command;

#[cfg(unix)]
use process_wrap::std::{ProcessSession, StdCommandWrap};
#[cfg(unix)]
use rustix::process::{
    Pid, Signal, getpgid, kill_process, kill_process_group, test_kill_process,
    test_kill_process_group,
};

#[cfg(unix)]
use chrono::{DateTime, Utc};

#[cfg(unix)]
const TRUSTED_PS_PATHS: [&str; 3] = ["/usr/bin/ps", "/bin/ps", "/run/current-system/sw/bin/ps"];
#[cfg(unix)]
const PROCESS_START_QUERY_ATTEMPTS: usize = 3;
#[cfg(unix)]
const PROCESS_START_QUERY_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(1);

/// `SIGTERM`: ask the group to terminate cleanly (the worker catches it and
/// finalizes its `RunRecord` plus durable task metadata).
#[cfg(unix)]
pub const SIGTERM: i32 = 15;
#[cfg(not(unix))]
pub const SIGTERM: i32 = 15;
/// `SIGKILL`: force-kill anything in the group still alive after the grace.
#[cfg(unix)]
pub const SIGKILL: i32 = 9;
#[cfg(not(unix))]
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

/// Fixed descriptor carrying a parent's pinned workdir into its detached
/// worker. Harness children use 8 and lifecycle sentinels use 9.
#[cfg(target_os = "linux")]
pub const WORKER_PINNED_WORKDIR_FD: i32 = 7;

/// Put a test child in its own process group without changing its session.
/// Production detached workers and daemons use [`spawn_in_session`] instead.
#[cfg(all(test, unix))]
pub fn install_process_group(cmd: &mut Command) {
    use std::os::unix::process::CommandExt as _;

    cmd.process_group(0);
}

/// Spawn a command as the leader of a new POSIX session and process group.
///
/// `ProcessSession` contains the platform-specific pre-exec operation behind a
/// safe API, preserving the detached worker's original `setsid(2)` semantics
/// without granting this crate an unsafe-code exception.
#[cfg(unix)]
pub fn spawn_in_session(cmd: Command) -> std::io::Result<std::process::Child> {
    let mut wrapped = StdCommandWrap::from(cmd);
    wrapped.wrap(ProcessSession);
    Ok(wrapped.spawn()?.into_inner())
}

/// Tokio-command equivalent of [`spawn_in_session`], used by the resident
/// daemon so it retains the same new-session isolation as detached workers.
#[cfg(unix)]
pub fn spawn_tokio_in_session(
    cmd: tokio::process::Command,
) -> std::io::Result<tokio::process::Child> {
    use process_wrap::tokio::{ProcessSession, TokioCommandWrap};

    let mut wrapped = TokioCommandWrap::from(cmd);
    wrapped.wrap(ProcessSession);
    Ok(wrapped.spawn()?.into_inner())
}

#[cfg(not(unix))]
pub fn spawn_in_session(mut cmd: std::process::Command) -> std::io::Result<std::process::Child> {
    cmd.spawn()
}

#[cfg(not(unix))]
pub fn spawn_tokio_in_session(
    mut cmd: tokio::process::Command,
) -> std::io::Result<tokio::process::Child> {
    cmd.spawn()
}

#[cfg(all(test, not(unix)))]
pub fn install_process_group(_cmd: &mut std::process::Command) {
    // Non-Unix: no setsid. Group semantics degrade to a direct child kill.
    // v0.1/v0.2 target Unix; this stub keeps the crate compiling elsewhere.
}

/// Send `sig` to the process group `pgid` by passing the negative pid, which is
/// how `kill(2)` addresses an entire group. Errors (e.g. ESRCH — the group is
/// already gone) are ignored, which is the desired idempotent behaviour.
#[cfg(unix)]
pub fn signal_group(pgid: i32, sig: i32) {
    if let (Some(pid), Some(signal)) = (positive_pid(pgid), Signal::from_named_raw(sig)) {
        let _ = kill_process_group(pid, signal);
    }
}

#[cfg(not(unix))]
pub fn signal_group(_pgid: i32, _sig: i32) {
    // Durable controller verification fails closed on platforms where Vyane
    // cannot establish or signal a process group. The stub keeps the CLI
    // buildable without pretending cancellation was delivered.
}

/// Send `sig` to one exact process id. Callers must verify the recorded birth
/// identity immediately before invoking this function; unlike a process-group
/// handle, a bare numeric pid is reusable after exit.
#[cfg(unix)]
pub fn signal_process(pid: i32, sig: i32) {
    if let (Some(pid), Some(signal)) = (positive_pid(pid), Signal::from_named_raw(sig)) {
        let _ = kill_process(pid, signal);
    }
}

#[cfg(not(unix))]
pub fn signal_process(_pid: i32, _sig: i32) {
    // The resident daemon is Unix-first; unsupported platforms fail their
    // identity checks rather than pretending a signal was delivered.
}

/// Does a process group still contain at least one member? Signal 0 probes the
/// whole negative-pgid target without delivering a signal. EPERM still proves
/// that the group exists.
#[cfg(unix)]
pub fn process_group_alive(pgid: i32) -> bool {
    if pgid <= 0 {
        return false;
    }
    positive_pid(pgid).is_some_and(|pid| probe_exists(test_kill_process_group(pid)))
}

#[cfg(not(unix))]
pub fn process_group_alive(pgid: i32) -> bool {
    pid_alive(pgid)
}

/// Is a process with `pid` still alive? Uses `kill(pid, 0)`, which delivers no
/// signal but performs the same existence + permission check the kernel would
/// for a real signal. This is the orphan-detection probe: a durable controller
/// that still says `running` while its recorded pid is dead means the worker
/// died without settling.
///
/// A live process the caller may not signal reports `EPERM` rather than
/// `ESRCH`; we treat that as "alive" — the process demonstrably exists. Only a
/// clear "no such process" counts as dead.
#[cfg(unix)]
pub fn pid_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    positive_pid(pid).is_some_and(|pid| probe_exists(test_kill_process(pid)))
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
    getpgid(Some(positive_pid(pid)?)).ok().map(Pid::as_raw_pid)
}

#[cfg(unix)]
fn positive_pid(raw: i32) -> Option<Pid> {
    (raw > 0).then(|| Pid::from_raw(raw)).flatten()
}

#[cfg(unix)]
fn probe_exists(result: rustix::io::Result<()>) -> bool {
    result.is_ok() || matches!(result, Err(rustix::io::Errno::PERM))
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

/// Stable process-birth identity when the platform exposes one. Linux combines
/// the kernel boot id with `/proc/<pid>/stat` start ticks, which cannot collide
/// with a reused pid during the same boot and cannot collide across boots.
#[cfg(target_os = "linux")]
pub fn process_birth_fingerprint(pid: i32) -> Option<String> {
    if pid <= 0 {
        return None;
    }
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let start_ticks = linux_process_start_ticks(&stat)?;
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
    Some(format!("linux:{}:{start_ticks}", boot_id.trim()))
}

#[cfg(target_os = "linux")]
fn linux_process_start_ticks(stat: &str) -> Option<&str> {
    // `comm` is parenthesized but may itself contain spaces and `)` characters.
    // No later numeric field can contain `)`, so the final boundary is the only
    // unambiguous split. The suffix starts at field 3 (`state`); starttime is
    // field 22, i.e. suffix token 19.
    let after_name = stat.rsplit_once(')')?.1.trim_start();
    after_name.split_whitespace().nth(19)
}

#[cfg(all(unix, not(target_os = "linux")))]
pub fn process_birth_fingerprint(pid: i32) -> Option<String> {
    if pid <= 0 {
        return None;
    }
    // BSD/macOS `ps` exposes a stable absolute start string. Force a canonical
    // locale and timezone because attach and cancel may run under different
    // caller environments; the same process must produce the same fingerprint.
    let output = trusted_ps_command()
        .args(["-p", &pid.to_string(), "-o", "lstart="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let started = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!started.is_empty()).then(|| format!("ps-lstart:{started}"))
}

#[cfg(not(unix))]
pub fn process_birth_fingerprint(_pid: i32) -> Option<String> {
    None
}

/// Validate the live pid, exact process group, and exact recorded birth
/// fingerprint. The fingerprint is the authoritative birth identity and is
/// deliberately independent of [`verify_identity`]'s wall-clock estimate:
/// changing the system clock after a worker starts must not make a valid
/// controller look unrelated. A missing or unreadable fingerprint is
/// fail-closed, because process control must never degrade to PID/PGID alone.
#[cfg(unix)]
pub fn verify_controller_identity(
    pid: i32,
    expected_pgid: i32,
    _started_at: chrono::DateTime<chrono::Utc>,
    expected_fingerprint: Option<&str>,
) -> IdentityCheck {
    if pid <= 0 || !pid_alive(pid) {
        return IdentityCheck::Dead;
    }
    match pgid_of(pid) {
        None => return IdentityCheck::Dead,
        Some(actual_pgid) if actual_pgid != expected_pgid => {
            return IdentityCheck::Mismatch("process group mismatch");
        }
        Some(_) => {}
    }
    let Some(expected) = expected_fingerprint else {
        return IdentityCheck::Mismatch("process birth fingerprint was not recorded");
    };
    match process_birth_fingerprint(pid) {
        Some(actual) if actual == expected => IdentityCheck::Match,
        Some(_) => IdentityCheck::Mismatch("process birth fingerprint mismatch"),
        None => IdentityCheck::Mismatch("could not read process birth fingerprint"),
    }
}

#[cfg(not(unix))]
pub fn verify_controller_identity(
    _pid: i32,
    _expected_pgid: i32,
    _started_at: chrono::DateTime<chrono::Utc>,
    _expected_fingerprint: Option<&str>,
) -> IdentityCheck {
    // This check authorizes process control. Platforms without a stable birth
    // fingerprint and process-group verification must fail closed instead of
    // pretending PID-only control is safe.
    IdentityCheck::Mismatch("process birth fingerprint is unsupported on this platform")
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
    for attempt in 0..PROCESS_START_QUERY_ATTEMPTS {
        if let Some(started_at) = process_start_time_once(pid) {
            return Some(started_at);
        }
        if attempt + 1 == PROCESS_START_QUERY_ATTEMPTS || !pid_alive(pid) {
            break;
        }
        // WSL/procfs can expose a just-forked task to `ps` before all process
        // accounting fields are coherent. A tiny bounded retry avoids treating
        // that transient as durable identity failure; every failed final result
        // still remains fail-closed.
        std::thread::sleep(PROCESS_START_QUERY_RETRY_DELAY);
    }
    None
}

#[cfg(unix)]
fn process_start_time_once(pid: i32) -> Option<DateTime<Utc>> {
    let output = trusted_ps_command()
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

#[cfg(unix)]
fn trusted_ps_command() -> Command {
    let program = TRUSTED_PS_PATHS
        .iter()
        .copied()
        .find(|path| std::path::Path::new(path).is_file())
        .unwrap_or(TRUSTED_PS_PATHS[0]);
    let mut command = Command::new(program);
    // Process identity must not depend on a caller-controlled executable search
    // path, ps personality, output format, language, or timezone.
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("LC_ALL", "C")
        .env("LANG", "C")
        .env("TZ", "UTC");
    command
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
    days.checked_mul(24)?
        .checked_add(hours)?
        .checked_mul(60)?
        .checked_add(minutes)?
        .checked_mul(60)?
        .checked_add(seconds)
}

#[cfg(not(unix))]
pub fn process_start_time(_pid: i32) -> Option<chrono::DateTime<chrono::Utc>> {
    None
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn spawn_established_sleep() -> std::process::Child {
        const TRUSTED_SLEEP_PATHS: [&str; 3] = [
            "/bin/sleep",
            "/usr/bin/sleep",
            "/run/current-system/sw/bin/sleep",
        ];
        let program = TRUSTED_SLEEP_PATHS
            .iter()
            .copied()
            .find(|path| std::path::Path::new(path).is_file())
            .unwrap_or(TRUSTED_SLEEP_PATHS[0]);
        let mut child = Command::new(program)
            .arg("30")
            .spawn()
            .expect("spawn sleep");
        let pid = child.id() as i32;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            #[cfg(target_os = "linux")]
            let exec_complete = std::fs::read_to_string(format!("/proc/{pid}/comm"))
                .ok()
                .is_some_and(|comm| comm.trim() == "sleep");

            #[cfg(not(target_os = "linux"))]
            let exec_complete = trusted_ps_command()
                .args(["-p", &pid.to_string(), "-o", "comm="])
                .output()
                .ok()
                .filter(|output| output.status.success())
                .is_some_and(|output| {
                    std::path::Path::new(String::from_utf8_lossy(&output.stdout).trim())
                        .file_name()
                        .is_some_and(|name| name == "sleep")
                });

            if exec_complete {
                return child;
            }
            if let Some(status) = child.try_wait().expect("poll sleep startup") {
                panic!("sleep exited before exec readiness: {status}");
            }
            assert!(
                std::time::Instant::now() < deadline,
                "sleep did not complete exec before readiness deadline"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

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
        assert_eq!(parse_etime_secs("9223372036854775807-00:00:00"), None);
    }

    #[test]
    fn trusted_ps_uses_an_absolute_binary_and_canonical_locale() {
        use std::ffi::OsStr;

        fn env_value<'a>(command: &'a Command, name: &str) -> Option<&'a OsStr> {
            command
                .get_envs()
                .find(|(key, _)| *key == OsStr::new(name))
                .and_then(|(_, value)| value)
        }

        let command = trusted_ps_command();
        assert!(std::path::Path::new(command.get_program()).is_absolute());
        assert_eq!(
            env_value(&command, "PATH"),
            Some(OsStr::new("/usr/bin:/bin"))
        );
        assert_eq!(env_value(&command, "LC_ALL"), Some(OsStr::new("C")));
        assert_eq!(env_value(&command, "LANG"), Some(OsStr::new("C")));
        assert_eq!(env_value(&command, "TZ"), Some(OsStr::new("UTC")));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_stat_parser_uses_the_final_comm_boundary() {
        let numeric_fields = (1..=18)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        let stat = format!("123 (worker name ) with parens) S {numeric_fields} 424242 0 0");
        assert_eq!(linux_process_start_ticks(&stat), Some("424242"));
    }

    #[test]
    fn process_start_time_of_real_child_is_recent() {
        // Spawn a real, briefly-living process and confirm we can read a start
        // time for it that is close to "now" (it just started).
        let mut child = spawn_established_sleep();
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
        let mut child = spawn_established_sleep();
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

    #[cfg(target_os = "linux")]
    #[test]
    fn controller_identity_requires_exact_linux_birth_fingerprint() {
        let mut child = spawn_established_sleep();
        let pid = child.id() as i32;
        let pgid = pgid_of(pid).expect("child pgid");
        let fingerprint = process_birth_fingerprint(pid).expect("linux birth fingerprint");
        // The exact kernel birth fingerprint is authoritative. Even a wildly
        // stale wall-clock timestamp must not reject the same live process.
        let deliberately_wrong_wall_clock = Utc::now() - chrono::Duration::days(365);
        assert_eq!(
            verify_controller_identity(
                pid,
                pgid,
                deliberately_wrong_wall_clock,
                Some(&fingerprint),
            ),
            IdentityCheck::Match
        );
        assert!(matches!(
            verify_controller_identity(pid, pgid + 1, Utc::now(), Some(&fingerprint)),
            IdentityCheck::Mismatch("process group mismatch")
        ));
        assert!(matches!(
            verify_controller_identity(pid, pgid, Utc::now(), Some("linux:wrong:0")),
            IdentityCheck::Mismatch("process birth fingerprint mismatch")
        ));
        assert_eq!(
            verify_controller_identity(pid, pgid, Utc::now(), None),
            IdentityCheck::Mismatch("process birth fingerprint was not recorded")
        );

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
        process_group_alive(pgid)
    }

    #[test]
    fn signal_group_reaps_a_child_in_the_group() {
        use std::io::Read as _;
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
        // Put the shell in its own new session and process group, exactly as a
        // detached worker is spawned.
        let mut shell = spawn_in_session(cmd).expect("spawn group-leader shell");
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
