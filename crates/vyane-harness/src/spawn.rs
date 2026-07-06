//! Shared subprocess machinery: clean-env spawn into a fresh process group,
//! and group-wide kill on cancel or timeout.
//!
//! Coding CLIs fork helper subprocesses (language servers, MCP stdio servers,
//! shell tool invocations). A bare kill of the direct child leaves those
//! grandchildren running. So every harness child is placed in its **own
//! process group** via `setsid(2)` in a `pre_exec` hook, and cancellation kills
//! the whole group by negative PID (`kill(-pgid, …)`).
//!
//! The child environment is materialized **exclusively** through
//! [`vyane_core::EnvPolicy::build`] from a snapshot of the parent environment —
//! never the raw parent env. That is the entire point of the scrub: the calling
//! agent's `*_API_KEY` / `*_BASE_URL` overrides must not leak into the child.

use std::collections::BTreeMap;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use vyane_core::EnvPolicy;
use vyane_core::error::{ErrorKind, Result, VyaneError};

/// How the child terminated, before error classification.
#[derive(Debug)]
pub(crate) enum Termination {
    /// Process exited on its own; carries the exit code (128+signal if killed
    /// by a signal it didn't catch).
    Exited(i32),
    /// The caller's cancellation token fired; the group was killed.
    Cancelled,
    /// `timeout` elapsed; the group was killed.
    TimedOut,
}

/// The captured result of running a child to completion (or to cancellation).
#[derive(Debug)]
pub(crate) struct RunResult {
    pub termination: Termination,
    pub stdout: String,
    pub stderr: String,
    pub duration: Duration,
}

/// Grace period between `SIGTERM` and the escalating `SIGKILL` on the group.
const KILL_GRACE: Duration = Duration::from_secs(3);

/// Snapshot the current process environment once, so a run's child environment
/// is a pure function of `(policy, snapshot)` and therefore reproducible.
pub(crate) fn parent_env_snapshot() -> Vec<(String, String)> {
    std::env::vars().collect()
}

/// Build the concrete child environment from a policy and a parent snapshot.
///
/// This is the ONLY place a harness assembles a child environment. It delegates
/// to the frozen [`EnvPolicy::build`] (scrubbed baseline + injections) so the
/// scrub can never be bypassed by hand-passing the parent env.
pub(crate) fn materialize_env(
    policy: &EnvPolicy,
    parent: Vec<(String, String)>,
) -> BTreeMap<String, String> {
    policy.build(parent)
}

/// Log-safe view of which env keys were set for the child. Never logs values.
pub(crate) fn env_key_list(env: &BTreeMap<String, String>) -> String {
    env.keys().cloned().collect::<Vec<_>>().join(",")
}

/// Spawn `program` with `args` in its own process group and a scrubbed
/// environment, then drive it to completion, honoring `cancel` and `timeout`.
///
/// * The child gets `cwd` as its working directory when `Some`.
/// * `env` fully replaces the child environment (already materialized via
///   [`materialize_env`]); the parent env is never inherited implicitly.
/// * On `cancel` or `timeout` the **whole process group** is signalled
///   (`SIGTERM`, then `SIGKILL` after [`KILL_GRACE`]), not just the direct child.
///
/// stdout/stderr are captured to strings (harness output is machine-readable
/// and bounded; v0.1 harnesses are one-shot, so this is not a streaming path).
pub(crate) async fn run_capture(
    program: &str,
    args: &[String],
    cwd: Option<&std::path::Path>,
    env: &BTreeMap<String, String>,
    cancel: &CancellationToken,
    timeout: Option<Duration>,
) -> Result<RunResult> {
    let mut cmd = Command::new(program);
    cmd.args(args);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    // Clean slate: wipe any inherited env, then set exactly the materialized set.
    cmd.env_clear();
    cmd.envs(env);

    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }

    install_process_group(&mut cmd);

    let started = Instant::now();
    let mut child = cmd.spawn().map_err(|e| {
        // Missing / not-executable binary is the canonical SpawnFailed case.
        VyaneError::with_source(
            ErrorKind::SpawnFailed,
            format!("failed to spawn `{program}`: {e}"),
            e,
        )
    })?;

    // The child pid doubles as its process-group id, because `setsid` makes the
    // child a group leader (pid == pgid). `None` only if the child already
    // exited, which the wait below handles.
    let pgid = child.id().map(|id| id as i32);

    // Take the pipes up front so we can drain them concurrently with the wait —
    // otherwise a child that fills its stdout pipe buffer blocks forever.
    let mut stdout_pipe = child.stdout.take();
    let mut stderr_pipe = child.stderr.take();

    let drain_out = async {
        let mut buf = String::new();
        if let Some(mut p) = stdout_pipe.take() {
            let _ = p.read_to_string(&mut buf).await;
        }
        buf
    };
    let drain_err = async {
        let mut buf = String::new();
        if let Some(mut p) = stderr_pipe.take() {
            let _ = p.read_to_string(&mut buf).await;
        }
        buf
    };

    // Race: normal exit vs. cancellation vs. timeout. `tokio::select!` polls the
    // wait and the drains together so pipe backpressure can't deadlock the wait.
    let (termination, stdout, stderr) = {
        let wait_all = async {
            // Drain both pipes and wait for exit concurrently.
            let (status, out, err) = tokio::join!(child.wait(), drain_out, drain_err);
            (status, out, err)
        };

        tokio::pin!(wait_all);

        // A timeout of None means "run until completion".
        let timeout_fut = async {
            match timeout {
                Some(d) => tokio::time::sleep(d).await,
                // Never resolves.
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(timeout_fut);

        tokio::select! {
            res = &mut wait_all => {
                let (status, out, err) = res;
                let code = status
                    .map(exit_code_of)
                    .unwrap_or_else(|_| -1);
                (Termination::Exited(code), out, err)
            }
            _ = cancel.cancelled() => {
                kill_group(pgid).await;
                let (_status, out, err) = (&mut wait_all).await;
                (Termination::Cancelled, out, err)
            }
            _ = &mut timeout_fut => {
                kill_group(pgid).await;
                let (_status, out, err) = (&mut wait_all).await;
                (Termination::TimedOut, out, err)
            }
        }
    };

    Ok(RunResult {
        termination,
        stdout,
        stderr,
        duration: started.elapsed(),
    })
}

/// Extract a faithful exit code from an `ExitStatus`. On Unix a process killed
/// by an uncaught signal has no exit code; represent it as `128 + signal`
/// (shell convention) so callers still see a non-zero, informative code.
fn exit_code_of(status: std::process::ExitStatus) -> i32 {
    if let Some(code) = status.code() {
        return code;
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(sig) = status.signal() {
            return 128 + sig;
        }
    }
    -1
}

#[cfg(unix)]
fn install_process_group(cmd: &mut Command) {
    // `tokio::process::Command::pre_exec` is an inherent method (no trait import
    // needed). SAFETY: `pre_exec` runs the closure in the forked child after
    // `fork(2)`
    // and before `execvp(2)`. `setsid(2)` is async-signal-safe and is the only
    // call the closure makes — it takes no arguments, allocates nothing, touches
    // no shared state, and cannot deadlock. It creates a new session and process
    // group with the child as leader (pgid == child pid), which is exactly what
    // lets us later signal the whole group by negative pid. On failure `setsid`
    // returns -1 and we surface the errno as a spawn failure. This is the single
    // sanctioned `unsafe` in the workspace (see this crate's Cargo.toml lints).
    unsafe {
        cmd.pre_exec(|| {
            if libc_setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn install_process_group(_cmd: &mut Command) {
    // Non-Unix: no setsid. Group-kill semantics degrade to a direct child kill.
    // v0.1 targets Unix; this stub keeps the crate compiling elsewhere.
}

/// Kill the process group led by `pgid`: `SIGTERM`, a short grace, then
/// `SIGKILL`, all against the **negative** pid so every member dies — including
/// grandchildren the CLI forked. A child that ignores `SIGTERM` is escalated.
#[cfg(unix)]
async fn kill_group(pgid: Option<i32>) {
    let Some(pgid) = pgid else { return };
    // SIGTERM the whole group (negative pid targets the group).
    signal_group(pgid, SIGTERM);
    // Give it a moment to exit cleanly, then hard-kill anything still alive.
    tokio::time::sleep(KILL_GRACE).await;
    signal_group(pgid, SIGKILL);
}

#[cfg(not(unix))]
async fn kill_group(_pgid: Option<i32>) {}

// --- Minimal libc FFI, scoped to this module ---------------------------------
//
// The workspace dependency set is frozen (no `libc` / `nix`), so the two calls
// we need — `setsid` and `kill` — are declared directly. Both are stable POSIX
// symbols. Declaring them here keeps the FFI surface auditable and tiny.

#[cfg(unix)]
const SIGTERM: i32 = 15;
#[cfg(unix)]
const SIGKILL: i32 = 9;

#[cfg(unix)]
unsafe extern "C" {
    fn setsid() -> i32;
    fn kill(pid: i32, sig: i32) -> i32;
}

/// Wrapper so the `pre_exec` closure body reads clearly and the `unsafe` intent
/// is documented at one site.
#[cfg(unix)]
fn libc_setsid() -> i32 {
    // SAFETY: `setsid` takes no arguments and returns an int; calling it is safe
    // in the forked-child context (see `install_process_group`).
    unsafe { setsid() }
}

/// Send `sig` to the process group `pgid` by passing the negative pid, which is
/// how `kill(2)` addresses an entire group.
#[cfg(unix)]
fn signal_group(pgid: i32, sig: i32) {
    // SAFETY: `kill` with a negative pid signals the process group; it has no
    // memory-safety implications. ESRCH (group already gone) is ignored, which
    // is the desired idempotent behavior.
    unsafe {
        let _ = kill(-pgid, sig);
    }
}
