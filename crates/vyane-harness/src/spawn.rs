//! Shared subprocess machinery: clean-env spawn into a fresh process group,
//! and group-wide kill on cancel or timeout.
//!
//! Coding CLIs fork helper subprocesses (language servers, MCP stdio servers,
//! shell tool invocations). A bare kill of the direct child leaves those
//! grandchildren running. So every harness child is placed in its **own
//! process group** via `setsid(2)` in a `pre_exec` hook, and cancellation kills
//! the whole group by negative PID (`kill(-pgid, …)`).
//!
//! Grandchildren can also inherit stdout/stderr pipe write-ends. If the direct
//! CLI child exits while a helper keeps those descriptors open, EOF never
//! arrives on the pipes. Normal exits therefore wait only a bounded post-exit
//! grace for drains to finish; after that we SIGKILL the process group and
//! return the output captured so far. Bytes still buffered in a killed helper
//! process may be lost.
//!
//! The child environment is materialized **exclusively** through
//! [`vyane_core::EnvPolicy::build`] from a snapshot of the parent environment —
//! never the raw parent env. That is the entire point of the scrub: the calling
//! agent's `*_API_KEY` / `*_BASE_URL` overrides must not leak into the child.

use std::collections::BTreeMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
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
/// Maximum time to wait for stdout/stderr EOF after the direct child exits.
const POST_EXIT_DRAIN_GRACE: Duration = Duration::from_secs(2);

type SharedOutput = Arc<Mutex<Vec<u8>>>;

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
    let stdout_buf = Arc::new(Mutex::new(Vec::new()));
    let stderr_buf = Arc::new(Mutex::new(Vec::new()));
    let drain_out = spawn_drain(child.stdout.take(), Arc::clone(&stdout_buf));
    let drain_err = spawn_drain(child.stderr.take(), Arc::clone(&stderr_buf));

    // Race: normal exit vs. cancellation vs. timeout. `tokio::select!` polls the
    // wait while background tasks drain pipes so pipe backpressure can't
    // deadlock the child.
    let termination = {
        let wait_child = child.wait();
        tokio::pin!(wait_child);
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
            status = &mut wait_child => {
                let code = status.map(exit_code_of).unwrap_or(-1);
                Termination::Exited(code)
            }
            _ = cancel.cancelled() => {
                kill_group(pgid).await;
                let _ = (&mut wait_child).await;
                Termination::Cancelled
            }
            _ = &mut timeout_fut => {
                kill_group(pgid).await;
                let _ = (&mut wait_child).await;
                Termination::TimedOut
            }
        }
    };

    wait_for_post_exit_drains(drain_out, drain_err, pgid).await;
    let stdout = captured_string(&stdout_buf).await;
    let stderr = captured_string(&stderr_buf).await;

    Ok(RunResult {
        termination,
        stdout,
        stderr,
        duration: started.elapsed(),
    })
}

fn spawn_drain<R>(reader: Option<R>, output: SharedOutput) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let Some(mut reader) = reader else { return };
        let mut chunk = [0_u8; 8192];
        loop {
            match reader.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => output.lock().await.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
    })
}

async fn wait_for_post_exit_drains(
    mut drain_out: JoinHandle<()>,
    mut drain_err: JoinHandle<()>,
    pgid: Option<i32>,
) {
    let mut out_done = false;
    let mut err_done = false;
    let grace = tokio::time::sleep(POST_EXIT_DRAIN_GRACE);
    tokio::pin!(grace);

    loop {
        if out_done && err_done {
            return;
        }

        tokio::select! {
            _ = &mut drain_out, if !out_done => {
                out_done = true;
            }
            _ = &mut drain_err, if !err_done => {
                err_done = true;
            }
            _ = &mut grace => {
                sigkill_group(pgid);
                if !out_done {
                    drain_out.abort();
                }
                if !err_done {
                    drain_err.abort();
                }
                return;
            }
        }
    }
}

async fn captured_string(output: &SharedOutput) -> String {
    let bytes = output.lock().await.clone();
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Spawn `program` with `args` in its own process group and a scrubbed
/// environment, reading stdout **line-by-line** and calling `on_line` for each
/// line as it arrives — while still capturing the full stdout for post-run
/// parsing.
///
/// This is the streaming counterpart to [`run_capture`]. The process-group,
/// cancellation, timeout, and post-exit drain logic are identical. The only
/// difference is that stdout is read line-by-line in a background task, and
/// each line is passed to `on_line` before being appended to the capture
/// buffer.
///
/// `on_line` receives each **complete line** (without the trailing newline).
/// Partial lines (no newline before EOF) are also delivered. Lines are
/// delivered in arrival order; the callback is called from a tokio task so it
/// must be `Send + Sync`.
#[allow(dead_code)] // used by ClaudeCode/CodexCli run_stream (WP-36 steps 3-4)
pub(crate) async fn run_stream_capture(
    program: &str,
    args: &[String],
    cwd: Option<&std::path::Path>,
    env: &BTreeMap<String, String>,
    cancel: &CancellationToken,
    timeout: Option<Duration>,
    on_line: Box<dyn FnMut(&str) + Send + Sync>,
) -> Result<RunResult> {
    let mut cmd = Command::new(program);
    cmd.args(args);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    cmd.env_clear();
    cmd.envs(env);

    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }

    install_process_group(&mut cmd);

    let started = Instant::now();
    let mut child = cmd.spawn().map_err(|e| {
        VyaneError::with_source(
            ErrorKind::SpawnFailed,
            format!("failed to spawn `{program}`: {e}"),
            e,
        )
    })?;

    let pgid = child.id().map(|id| id as i32);

    // stdout is read line-by-line with callback; stderr is captured normally.
    let stdout_buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let stderr_buf = Arc::new(Mutex::new(Vec::new()));
    let drain_out = spawn_line_drain(child.stdout.take(), Arc::clone(&stdout_buf), on_line);
    let drain_err = spawn_drain(child.stderr.take(), Arc::clone(&stderr_buf));

    // Same race: normal exit vs. cancellation vs. timeout.
    let termination = {
        let wait_child = child.wait();
        tokio::pin!(wait_child);
        let timeout_fut = async {
            match timeout {
                Some(d) => tokio::time::sleep(d).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(timeout_fut);

        tokio::select! {
            status = &mut wait_child => {
                let code = status.map(exit_code_of).unwrap_or(-1);
                Termination::Exited(code)
            }
            _ = cancel.cancelled() => {
                kill_group(pgid).await;
                let _ = (&mut wait_child).await;
                Termination::Cancelled
            }
            _ = &mut timeout_fut => {
                kill_group(pgid).await;
                let _ = (&mut wait_child).await;
                Termination::TimedOut
            }
        }
    };

    wait_for_post_exit_drains(drain_out, drain_err, pgid).await;
    let stdout = captured_string(&stdout_buf).await;
    let stderr = captured_string(&stderr_buf).await;

    Ok(RunResult {
        termination,
        stdout,
        stderr,
        duration: started.elapsed(),
    })
}

/// Line-by-line stdout reader: reads complete lines from the child's stdout
/// pipe, calls `on_line` for each, and appends to the capture buffer.
#[allow(dead_code)] // used by run_stream_capture → ClaudeCode/CodexCli (WP-36 steps 3-4)
fn spawn_line_drain<R>(
    reader: Option<R>,
    output: SharedOutput,
    mut on_line: Box<dyn FnMut(&str) + Send + Sync>,
) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let Some(reader) = reader else { return };
        let mut reader = tokio::io::BufReader::new(reader);
        let mut partial = String::new();
        let mut buf = Vec::<u8>::new();

        loop {
            buf.clear();
            match reader.read_until(b'\n', &mut buf).await {
                Ok(0) => break,
                Ok(_) => {
                    // Append to capture buffer.
                    output.lock().await.extend_from_slice(&buf);

                    // Convert to string (lossy for safety).
                    let text = String::from_utf8_lossy(&buf);
                    partial.push_str(&text);

                    // Deliver complete lines (ending with \n).
                    while let Some(pos) = partial.find('\n') {
                        let line = partial[..pos].to_string();
                        on_line(&line);
                        partial = partial[pos + 1..].to_string();
                    }
                }
                Err(_) => break,
            }
        }

        // Deliver any remaining partial line (no trailing newline at EOF).
        if !partial.is_empty() {
            on_line(partial.trim_end_matches(['\r', '\n']));
        }
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

#[cfg(unix)]
fn sigkill_group(pgid: Option<i32>) {
    let Some(pgid) = pgid else { return };
    signal_group(pgid, SIGKILL);
}

#[cfg(not(unix))]
fn sigkill_group(_pgid: Option<i32>) {}

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
