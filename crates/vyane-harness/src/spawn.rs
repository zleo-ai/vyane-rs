//! Shared subprocess machinery: clean-env spawn into a fresh process group,
//! and group-wide kill on cancel or timeout.
//!
//! Coding CLIs fork helper subprocesses (language servers, MCP stdio servers,
//! shell tool invocations). A bare kill of the direct child leaves those
//! grandchildren running. So every harness child is placed in its **own
//! process group** via the safe `process_group(0)` command API, and cancellation kills
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
#[cfg(unix)]
use std::io::{Read as _, Seek as _, Write as _};
#[cfg(unix)]
use std::os::fd::{AsRawFd as _, OwnedFd, RawFd};
#[cfg(not(unix))]
type RawFd = i32;
#[cfg(unix)]
type InheritedFd = OwnedFd;
#[cfg(not(unix))]
type InheritedFd = RawFd;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(unix)]
use command_fds::{CommandFdExt as _, FdMapping};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use vyane_core::error::{ErrorKind, Result, VyaneError};
use vyane_core::{
    EnvPolicy, HarnessLifecycleEvent, HarnessLifecycleReporter, HarnessSpawnAuthority,
    PinnedWorkdir,
};

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
/// Brief bounded settle after SIGKILL before publishing `Stopped`.
const POST_KILL_SETTLE_GRACE: Duration = Duration::from_millis(500);
/// Linux/WSL can transiently return ETXTBSY while a directly spawned executable
/// replacement is becoming visible. Retry that one errno briefly; lifecycle
/// sentinels cannot distinguish a shell launch failure from a target that
/// intentionally exits 126, so they deliberately do not retry the target.
const EXECUTABLE_BUSY_RETRY_DELAY: Duration = Duration::from_millis(20);
const EXECUTABLE_BUSY_MAX_RETRIES: usize = 5;
/// Poll interval while waiting for a process group to disappear.
#[cfg(unix)]
const GROUP_EXIT_POLL_INTERVAL: Duration = Duration::from_millis(20);
/// Descriptor used only by the sentinel wrapper to return the real target's
/// exit code. Dash accepts redirections only for single-digit descriptors.
#[cfg(unix)]
const SENTINEL_STATUS_FD: RawFd = 9;
/// Stable descriptor inherited by the real CLI for its admitted directory.
/// The lifecycle shell closes only descriptor 9, so this remains available to
/// Codex/Claude and all descendants.
#[cfg(unix)]
const PINNED_WORKDIR_FD: RawFd = 8;
/// Private, inherited descriptor containing shell-quoted target environment
/// exports. The trusted start-gate sentinel itself never inherits target
/// loader/startup variables.
#[cfg(unix)]
const TARGET_ENV_FD: RawFd = 6;
#[cfg(target_os = "linux")]
pub(crate) const PINNED_WORKDIR_CHILD_PATH: &str = "/proc/self/fd/8";
// Kept defined so the adapters compile on every target. Capability admission
// refuses pinned mutating workdirs outside Linux, so this path is never used
// for execution there.
#[cfg(not(target_os = "linux"))]
pub(crate) const PINNED_WORKDIR_CHILD_PATH: &str = "/dev/fd/8";
/// Keep the source well away from the two fixed child descriptors so the
/// pre-exec dup operations cannot create a swap/collision problem.
#[cfg(unix)]
const PINNED_WORKDIR_SOURCE_MIN_FD: RawFd = 64;
#[cfg(unix)]
const SENTINEL_STATUS_PREFIX: &str = "vyane-exit:";
#[cfg(unix)]
const SENTINEL_STATUS_LIMIT: u64 = 64;
#[cfg(unix)]
const SENTINEL_STATUS_READ_TIMEOUT: Duration = Duration::from_millis(250);

/// Lifecycle-controlled Unix children use `/bin/sh` as a persistent, exact
/// process-group leader. The real CLI starts only after `Started` is published,
/// and it never inherits the private status descriptor. Caught signals interrupt
/// `wait(1)`, so the inner loop checks the exact child before accepting the
/// returned status. Once the target exits, the wrapper reports that status and
/// SIGKILLs its own group exactly once; this both reserves the numeric PGID for
/// the generation's lifetime and prevents residual helpers from escaping normal
/// completion.
#[cfg(unix)]
const START_GATE_SCRIPT: &str = r#"
trap 'kill -KILL 0 2>/dev/null || :' EXIT
trap ':' TERM HUP INT QUIT PIPE
IFS= read -r vyane_gate || exit 125
[ "$vyane_gate" = "vyane-start" ] || exit 125

unset PATH PWD
if [ -r /proc/self/fd/6 ]; then
    . /proc/self/fd/6 || exit 125
else
    . /dev/fd/6 || exit 125
fi

"$@" 6>&- 9>&- &
vyane_target=$!
while :; do
    wait "$vyane_target"
    vyane_status=$?
    if kill -0 "$vyane_target" 2>/dev/null; then
        continue
    fi
    break
done

printf 'vyane-exit:%s\n' "$vyane_status" >&9 || :
kill -KILL 0
exit 125
"#;

type SharedOutput = Arc<Mutex<Vec<u8>>>;

#[cfg(unix)]
type SentinelStatusReader = UnixStream;
#[cfg(not(unix))]
struct SentinelStatusReader;

/// Pre-publication status transport. Both endpoints are ordinary RAII handles:
/// every spawn error, cancelled retry sleep, or unwind closes them immediately.
/// After a successful spawn only the parent endpoint is retained; the child
/// endpoint has already been mapped to [`SENTINEL_STATUS_FD`] by the command.
#[cfg(unix)]
struct SentinelStatusPipe {
    parent: Option<UnixStream>,
    child: Option<UnixStream>,
}

/// High-numbered CLOEXEC duplicate retained by the parent until spawn.
#[cfg(unix)]
struct InheritedWorkdir {
    source: OwnedFd,
}

#[cfg(unix)]
struct InheritedTargetEnv {
    source: OwnedFd,
}

#[cfg(unix)]
impl InheritedTargetEnv {
    fn materialize(env: &BTreeMap<String, String>) -> Result<Self> {
        let mut file = tempfile::tempfile().map_err(|source| {
            VyaneError::with_source(
                ErrorKind::Io,
                "failed to create private target environment transport",
                source,
            )
        })?;
        for (key, value) in env {
            if !valid_shell_env_key(key) || value.contains('\0') {
                return Err(VyaneError::new(
                    ErrorKind::Config,
                    "lifecycle-gated target environment is invalid",
                ));
            }
            let quoted = value.replace('\'', "'\"'\"'");
            writeln!(file, "export {key}='{quoted}'").map_err(|source| {
                VyaneError::with_source(
                    ErrorKind::Io,
                    "failed to write private target environment transport",
                    source,
                )
            })?;
        }
        file.rewind().map_err(|source| {
            VyaneError::with_source(
                ErrorKind::Io,
                "failed to rewind private target environment transport",
                source,
            )
        })?;
        let source = duplicate_inherited_fd(
            &file,
            "failed to duplicate private target environment transport",
        )?;
        Ok(Self { source })
    }
}

#[cfg(unix)]
fn valid_shell_env_key(key: &str) -> bool {
    let mut bytes = key.bytes();
    matches!(bytes.next(), Some(b'A'..=b'Z' | b'a'..=b'z' | b'_'))
        && bytes.all(|byte| matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_'))
}

#[cfg(unix)]
fn duplicate_inherited_fd(fd: impl std::os::fd::AsFd, message: &'static str) -> Result<OwnedFd> {
    rustix::io::fcntl_dupfd_cloexec(fd.as_fd(), PINNED_WORKDIR_SOURCE_MIN_FD).map_err(|source| {
        VyaneError::with_source(
            ErrorKind::Io,
            message,
            std::io::Error::from_raw_os_error(source.raw_os_error()),
        )
    })
}

#[cfg(unix)]
impl InheritedWorkdir {
    fn duplicate(pinned: &PinnedWorkdir) -> Result<Self> {
        let source = duplicate_inherited_fd(
            pinned.handle(),
            "failed to duplicate pinned workdir handle for child",
        )?;
        Ok(Self { source })
    }
}

#[cfg(unix)]
impl SentinelStatusPipe {
    fn new() -> Result<Self> {
        let (parent, child) = UnixStream::pair().map_err(|source| {
            VyaneError::with_source(
                ErrorKind::Io,
                "failed to create harness sentinel status pipe",
                source,
            )
        })?;
        parent
            .set_read_timeout(Some(SENTINEL_STATUS_READ_TIMEOUT))
            .map_err(|source| {
                VyaneError::with_source(
                    ErrorKind::Io,
                    "failed to bound harness sentinel status reads",
                    source,
                )
            })?;
        Ok(Self {
            parent: Some(parent),
            child: Some(child),
        })
    }

    fn duplicate_child_fd(&self) -> Result<OwnedFd> {
        let child = self.child.as_ref().ok_or_else(|| {
            VyaneError::new(ErrorKind::Io, "sentinel status child endpoint was lost")
        })?;
        child.try_clone().map(Into::into).map_err(|source| {
            VyaneError::with_source(
                ErrorKind::Io,
                "failed to duplicate sentinel status transport",
                source,
            )
        })
    }

    fn into_parent(mut self) -> Result<UnixStream> {
        self.child.take();
        self.parent.take().ok_or_else(|| {
            VyaneError::new(ErrorKind::Io, "sentinel status parent endpoint was lost")
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainCompletion {
    /// Both pipes reached EOF without process-control intervention.
    Eof,
    /// A descendant held a pipe open past the grace, so the group was killed.
    ForcedGroupKill,
    /// The exact sentinel already SIGKILLed its group; pipe readers were aborted
    /// after the bounded grace without signalling a now-leaderless numeric PGID.
    PassiveAbort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaderResolution {
    Reaped,
    IdentityUnknown,
}

enum LeaderWait {
    Reaped,
    TimedOut,
    IdentityUnknown(std::io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResidualGroupControl {
    Active,
    Passive,
}

impl ResidualGroupControl {
    fn may_signal(self) -> bool {
        self == Self::Active
    }
}

fn residual_group_control(
    leader_identity_unknown: bool,
    group_already_killed: bool,
) -> ResidualGroupControl {
    if leader_identity_unknown || group_already_killed {
        ResidualGroupControl::Passive
    } else {
        ResidualGroupControl::Active
    }
}

/// Cancellation cleanup carries the final leader resolution separately from
/// its operational result. Both terminal resolutions have already revoked
/// numeric-PGID signal authority before this value is returned.
struct TerminateAndReapOutcome {
    leader: LeaderResolution,
    result: Result<Option<VyaneError>>,
}

/// Runtime controls shared by capture and streaming subprocess execution.
pub(crate) struct RunControl {
    cancel: CancellationToken,
    timeout: Option<Duration>,
    lifecycle_reporter: Option<HarnessLifecycleReporter>,
    spawn_authority: Option<HarnessSpawnAuthority>,
}

impl RunControl {
    pub(crate) fn new(
        cancel: CancellationToken,
        timeout: Option<Duration>,
        lifecycle_reporter: Option<HarnessLifecycleReporter>,
    ) -> Self {
        Self {
            cancel,
            timeout,
            lifecycle_reporter,
            spawn_authority: None,
        }
    }

    pub(crate) fn with_spawn_authority(
        mut self,
        spawn_authority: Option<HarnessSpawnAuthority>,
    ) -> Self {
        self.spawn_authority = spawn_authority;
        self
    }
}

/// Last-resort process-group cleanup for abrupt future drop or unwinding.
///
/// The async cancel/timeout paths perform a graceful TERM -> KILL sequence and
/// await the direct child. They cannot run when the owning future itself is
/// dropped, though (for example when a sibling workflow step panics). Keep this
/// guard armed from immediately after spawn until normal cleanup and pipe drains
/// are complete; its synchronous `Drop` sends SIGKILL to the whole process
/// group, never just the direct child.
struct ProcessGroupDropGuard {
    report_identity: Option<ProcessGroupIdentity>,
    signal_pgid: Option<i32>,
    reporter: Option<HarnessLifecycleReporter>,
}

impl ProcessGroupDropGuard {
    fn new(pid: Option<u32>, reporter: Option<HarnessLifecycleReporter>) -> Self {
        let identity = pid.map(|pid| ProcessGroupIdentity {
            pid,
            pgid: pid as i32,
        });
        Self {
            report_identity: identity.clone(),
            signal_pgid: identity.map(|identity| identity.pgid),
            reporter,
        }
    }

    fn report_started(&self) -> Result<()> {
        match (self.report_identity.as_ref(), self.reporter.as_ref()) {
            (Some(identity), Some(reporter)) => reporter.report(HarnessLifecycleEvent::Started {
                pid: identity.pid,
                pgid: identity.pgid,
            }),
            (None, Some(_)) => Err(VyaneError::new(
                ErrorKind::SpawnFailed,
                "spawned harness did not expose a process id",
            )),
            (_, None) => Ok(()),
        }
    }

    fn disarm(&mut self, group_empty: bool) {
        self.revoke_signal_authority();
        self.report_stopped(group_empty);
    }

    fn pgid(&self) -> Option<i32> {
        self.report_identity.as_ref().map(|identity| identity.pgid)
    }

    /// Once the exact leader has been reaped, its numeric PGID is no longer an
    /// authenticated signal target. Keep the reporting identity so Drop can
    /// still publish `Stopped(false)`, but permanently revoke kill authority.
    fn revoke_signal_authority(&mut self) {
        self.signal_pgid = None;
    }

    /// A returned wait result means the exact leader is either reaped or its
    /// identity is no longer knowable. Both cases permanently revoke numeric
    /// PGID authority; only a wait that has not returned (for example, an outer
    /// timeout) may leave the authority armed.
    fn leader_wait_returned<T>(&mut self, result: std::io::Result<T>) -> std::io::Result<T> {
        self.revoke_signal_authority();
        result
    }

    fn report_stopped(&mut self, group_empty: bool) {
        let Some(identity) = self.report_identity.take() else {
            return;
        };
        if let Some(reporter) = &self.reporter {
            if let Err(error) = reporter.report(HarnessLifecycleEvent::Stopped {
                pid: identity.pid,
                pgid: identity.pgid,
                group_empty,
            }) {
                tracing::warn!(
                    pid = identity.pid,
                    pgid = identity.pgid,
                    error = %error,
                    "failed to report stopped harness process group"
                );
            }
        }
    }
}

impl Drop for ProcessGroupDropGuard {
    fn drop(&mut self) {
        sigkill_group(self.signal_pgid.take());
        self.report_stopped(false);
    }
}

#[derive(Debug, Clone)]
struct ProcessGroupIdentity {
    pid: u32,
    pgid: i32,
}

/// Establish the reporter's durable view before allowing the harness to run.
///
/// Reporting `Started` is synchronous. If it fails, immediately SIGKILL the
/// fresh group and await the direct child so a detached caller is never left
/// with a live harness process that has no independently-readable controller.
async fn establish_lifecycle(
    child: &mut Child,
    guard: &mut ProcessGroupDropGuard,
    program: &str,
) -> Result<()> {
    let Err(source) = guard.report_started() else {
        return Ok(());
    };

    let reaped = force_kill_and_reap(child, guard.pgid(), guard).await;
    if reaped.is_ok() {
        let group_empty = group_is_empty_after_kill(guard.pgid()).await;
        guard.disarm(group_empty);
    }

    let reap_context = match reaped {
        Ok(()) => String::new(),
        Err(error) => format!("; failed to reap child after SIGKILL: {error}"),
    };
    Err(VyaneError::with_source(
        ErrorKind::SpawnFailed,
        format!("failed to establish lifecycle control for harness `{program}`{reap_context}"),
        source,
    ))
}

fn harness_command(program: &str, args: &[String], start_gated: bool) -> Command {
    #[cfg(unix)]
    if start_gated {
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg(START_GATE_SCRIPT)
            .arg("vyane-harness-start-gate")
            .arg(program)
            .args(args);
        return command;
    }

    let mut command = Command::new(program);
    command.args(args);
    command
}

/// Release a lifecycle-controlled child only after `Started` succeeded. A
/// failed or missing gate is treated like spawn failure and the still-gated
/// process is killed and reaped before control returns.
#[cfg(unix)]
fn prepare_start_gate(child: &mut Child) -> std::io::Result<std::fs::File> {
    let gate = child.stdin.take().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "lifecycle start gate stdin was not piped",
        )
    })?;
    #[cfg(target_os = "linux")]
    let path = format!("/proc/self/fd/{}", gate.as_raw_fd());
    #[cfg(all(unix, not(target_os = "linux")))]
    let path = format!("/dev/fd/{}", gate.as_raw_fd());
    std::fs::OpenOptions::new().write(true).open(path)
}

#[cfg(not(unix))]
fn prepare_start_gate(_child: &mut Child) -> std::io::Result<std::fs::File> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "lifecycle start gates require Unix file descriptors",
    ))
}

async fn authorize_and_release_start_gate(
    child: &mut Child,
    guard: &mut ProcessGroupDropGuard,
    program: &str,
    authority: Option<&HarnessSpawnAuthority>,
    cancel: &CancellationToken,
    deadline: Option<tokio::time::Instant>,
) -> Result<()> {
    let result = prepare_start_gate(child)
        .map_err(|source| {
            VyaneError::with_source(
                ErrorKind::SpawnFailed,
                format!("failed to release lifecycle start gate for `{program}`"),
                source,
            )
        })
        .and_then(|mut gate| {
            if cancel.is_cancelled() {
                return Err(VyaneError::cancelled());
            }
            if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
                return Err(VyaneError::new(
                    ErrorKind::Timeout,
                    "harness timed out before target release",
                ));
            }
            revalidate_spawn_authority(authority)?;
            if cancel.is_cancelled() {
                return Err(VyaneError::cancelled());
            }
            if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
                return Err(VyaneError::new(
                    ErrorKind::Timeout,
                    "harness timed out during target release authorization",
                ));
            }
            std::io::Write::write_all(&mut gate, b"vyane-start\n").map_err(|source| {
                VyaneError::with_source(
                    ErrorKind::SpawnFailed,
                    format!("failed to release lifecycle start gate for `{program}`"),
                    source,
                )
            })
        });
    let Err(source) = result else {
        return Ok(());
    };

    let reaped = force_kill_and_reap(child, guard.pgid(), guard).await;
    if reaped.is_ok() {
        let group_empty = group_is_empty_after_kill(guard.pgid()).await;
        guard.disarm(group_empty);
    }
    let reap_context = match reaped {
        Ok(()) => String::new(),
        Err(error) => format!("; failed to reap gated child: {error}"),
    };
    if reap_context.is_empty() {
        Err(source)
    } else {
        Err(VyaneError::new(
            ErrorKind::Io,
            "failed to reap gated harness after target release was denied",
        ))
    }
}

fn revalidate_spawn_authority(authority: Option<&HarnessSpawnAuthority>) -> Result<()> {
    if authority.is_none_or(HarnessSpawnAuthority::revalidate) {
        return Ok(());
    }
    Err(VyaneError::new(
        ErrorKind::Conflict,
        "harness spawn authority rejected",
    ))
}

fn control_deadline(timeout: Option<Duration>) -> Result<Option<tokio::time::Instant>> {
    timeout
        .map(|duration| {
            tokio::time::Instant::now()
                .checked_add(duration)
                .ok_or_else(|| {
                    VyaneError::new(ErrorKind::Config, "harness timeout exceeds runtime range")
                })
        })
        .transpose()
}

async fn spawn_harness_child(
    command: &mut Command,
    program: &str,
    spawn_authority: Option<&HarnessSpawnAuthority>,
    cancel: &CancellationToken,
    deadline: Option<tokio::time::Instant>,
) -> Result<Child> {
    let mut retries = 0;
    loop {
        if cancel.is_cancelled() {
            return Err(VyaneError::cancelled());
        }
        if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
            return Err(VyaneError::new(
                ErrorKind::Timeout,
                "harness timed out before spawn",
            ));
        }
        revalidate_spawn_authority(spawn_authority)?;
        if cancel.is_cancelled() {
            return Err(VyaneError::cancelled());
        }
        if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
            return Err(VyaneError::new(
                ErrorKind::Timeout,
                "harness timed out during spawn authorization",
            ));
        }
        match command.spawn() {
            Ok(child) => return Ok(child),
            Err(error)
                if error.raw_os_error() == Some(26) && retries < EXECUTABLE_BUSY_MAX_RETRIES =>
            {
                retries += 1;
                tokio::time::sleep(EXECUTABLE_BUSY_RETRY_DELAY).await;
            }
            Err(error) => {
                return Err(VyaneError::with_source(
                    ErrorKind::SpawnFailed,
                    format!("failed to spawn `{program}`: {error}"),
                    error,
                ));
            }
        }
    }
}

/// Install process isolation and, for lifecycle-controlled runs, a private
/// wrapper-to-parent status channel. The shell sentinel remains the exact group
/// leader until it has reported the real target status and killed its group.
struct ControlledSpawn<'a> {
    reporter: Option<&'a HarnessLifecycleReporter>,
    spawn_authority: Option<&'a HarnessSpawnAuthority>,
    cancel: &'a CancellationToken,
    deadline: Option<tokio::time::Instant>,
    pinned_workdir_fd: Option<InheritedFd>,
    target_env_fd: Option<InheritedFd>,
}

async fn spawn_controlled_harness_child(
    command: &mut Command,
    program: &str,
    control: ControlledSpawn<'_>,
) -> Result<(Child, Option<SentinelStatusReader>)> {
    #[cfg(unix)]
    {
        let status_pipe = control
            .reporter
            .map(|_| SentinelStatusPipe::new())
            .transpose()?;
        let sentinel_status_fd = status_pipe
            .as_ref()
            .map(SentinelStatusPipe::duplicate_child_fd)
            .transpose()?;
        install_process_group(
            command,
            sentinel_status_fd,
            control.pinned_workdir_fd,
            control.target_env_fd,
        )?;
        let child = spawn_harness_child(
            command,
            program,
            control.spawn_authority,
            control.cancel,
            control.deadline,
        )
        .await?;
        // `command-fds` retains its owned mapping sources on the Command so a
        // retry can spawn with the same descriptors. Once spawn succeeds the
        // parent must close those sources, especially the status socket's
        // child endpoint, or the reader can never observe EOF.
        drop(std::mem::replace(command, Command::new("")));
        let status_reader = match status_pipe {
            Some(pipe) => Some(pipe.into_parent()?),
            None => None,
        };
        Ok((child, status_reader))
    }

    #[cfg(not(unix))]
    {
        install_process_group(command)?;
        let _ = control.reporter;
        let _ = control.pinned_workdir_fd;
        let _ = control.target_env_fd;
        spawn_harness_child(
            command,
            program,
            control.spawn_authority,
            control.cancel,
            control.deadline,
        )
        .await
        .map(|child| (child, None))
    }
}

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
#[cfg(test)]
pub(crate) async fn run_capture(
    program: &str,
    args: &[String],
    cwd: Option<&std::path::Path>,
    env: &BTreeMap<String, String>,
    control: RunControl,
) -> Result<RunResult> {
    run_capture_with_pinned(program, args, cwd, None, env, control).await
}

/// [`run_capture`] with an optional stable directory object.  When present,
/// `cwd` is audit-only and the child cwd is established through the admitted
/// directory descriptor.
pub(crate) async fn run_capture_with_pinned(
    program: &str,
    args: &[String],
    cwd: Option<&std::path::Path>,
    pinned_workdir: Option<&PinnedWorkdir>,
    env: &BTreeMap<String, String>,
    control: RunControl,
) -> Result<RunResult> {
    let RunControl {
        cancel,
        timeout,
        lifecycle_reporter,
        spawn_authority,
    } = control;
    if spawn_authority.is_some() && lifecycle_reporter.is_none() {
        return Err(VyaneError::new(
            ErrorKind::Unsupported,
            "spawn-authorized harness execution requires lifecycle gating",
        ));
    }
    let control_deadline = control_deadline(timeout)?;
    #[cfg(not(unix))]
    if lifecycle_reporter.is_some() {
        return Err(VyaneError::new(
            ErrorKind::Unsupported,
            "durable harness lifecycle control requires a Unix start gate",
        ));
    }
    #[cfg(not(unix))]
    if pinned_workdir.is_some() {
        return Err(VyaneError::new(
            ErrorKind::Unsupported,
            "pinned mutating workdirs require Unix descriptor-backed cwd enforcement",
        ));
    }
    #[cfg(unix)]
    let inherited_workdir = pinned_workdir
        .map(InheritedWorkdir::duplicate)
        .transpose()?;
    #[cfg(unix)]
    let inherited_workdir_fd = inherited_workdir.map(|inherited| inherited.source);
    #[cfg(not(unix))]
    let inherited_workdir_fd = None;
    let start_gated = lifecycle_reporter.is_some();
    #[cfg(unix)]
    let inherited_target_env = start_gated
        .then(|| InheritedTargetEnv::materialize(env))
        .transpose()?;
    #[cfg(unix)]
    let inherited_target_env_fd = inherited_target_env.map(|inherited| inherited.source);
    #[cfg(not(unix))]
    let inherited_target_env_fd = None;
    let mut cmd = harness_command(program, args, start_gated);
    cmd.stdin(if start_gated {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    // On platforms without POSIX process groups, dropping the async run still
    // has a direct-child fallback instead of silently detaching it.
    cmd.kill_on_drop(true);

    // The trusted sentinel starts without target loader/startup variables. The
    // target receives its full environment from private descriptor 6 only
    // after the start gate is authorized.
    cmd.env_clear();
    if start_gated {
        cmd.env("PATH", "/usr/bin:/bin");
    } else {
        cmd.envs(env);
    }

    if pinned_workdir.is_none() {
        if let Some(dir) = cwd {
            cmd.current_dir(dir);
        }
    }

    let started = Instant::now();
    let (mut child, mut sentinel_status) = spawn_controlled_harness_child(
        &mut cmd,
        program,
        ControlledSpawn {
            reporter: lifecycle_reporter.as_ref(),
            spawn_authority: spawn_authority.as_ref(),
            cancel: &cancel,
            deadline: control_deadline,
            pinned_workdir_fd: inherited_workdir_fd,
            target_env_fd: inherited_target_env_fd,
        },
    )
    .await?;

    // The child pid doubles as its process-group id because process_group(0)
    // makes it a group leader (pid == pgid). `None` only if the child already
    // exited, which the wait below handles.
    let mut process_group_guard = ProcessGroupDropGuard::new(child.id(), lifecycle_reporter);
    establish_lifecycle(&mut child, &mut process_group_guard, program).await?;
    if start_gated {
        authorize_and_release_start_gate(
            &mut child,
            &mut process_group_guard,
            program,
            spawn_authority.as_ref(),
            &cancel,
            control_deadline,
        )
        .await?;
    }
    let pgid = process_group_guard.pgid();

    // Take the pipes up front so we can drain them concurrently with the wait —
    // otherwise a child that fills its stdout pipe buffer blocks forever.
    let stdout_buf = Arc::new(Mutex::new(Vec::new()));
    let stderr_buf = Arc::new(Mutex::new(Vec::new()));
    let drain_out = spawn_drain(child.stdout.take(), Arc::clone(&stdout_buf));
    let drain_err = spawn_drain(child.stderr.take(), Arc::clone(&stderr_buf));

    // Race: normal exit vs. cancellation vs. timeout. `tokio::select!` polls the
    // wait while background tasks drain pipes so pipe backpressure can't
    // deadlock the child.
    let sentinel_controlled = sentinel_status.is_some();
    let mut sentinel_error = None;
    let (termination, leader_identity_error) = {
        // A timeout of None means "run until completion".
        let timeout_fut = async {
            match control_deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                // Never resolves.
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(timeout_fut);

        tokio::select! {
            biased;
            _ = cancel.cancelled() => (Some(Termination::Cancelled), None),
            _ = &mut timeout_fut => (Some(Termination::TimedOut), None),
            status = child.wait() => {
                match process_group_guard.leader_wait_returned(status) {
                    Ok(status) => {
                        let wrapper_code = exit_code_of(status);
                        #[cfg(unix)]
                        let code = match sentinel_status.as_mut() {
                            Some(reader) => match read_sentinel_exit(reader, program) {
                                Ok(code) => code,
                                Err(error) => {
                                    sentinel_error = Some(error);
                                    wrapper_code
                                }
                            },
                            None => wrapper_code,
                        };
                        #[cfg(not(unix))]
                        let code = wrapper_code;
                        (Some(Termination::Exited(code)), None)
                    }
                    Err(source) => (None, Some(leader_identity_error(program, source))),
                }
            }
        }
    };

    let controlled_termination = termination.as_ref().is_some_and(|termination| {
        matches!(termination, Termination::Cancelled | Termination::TimedOut)
    });
    let control_error = if controlled_termination {
        let outcome = terminate_and_reap(
            &mut child,
            pgid,
            program,
            sentinel_controlled,
            &mut process_group_guard,
        )
        .await;
        debug_assert!(process_group_guard.signal_pgid.is_none());
        if outcome.leader == LeaderResolution::IdentityUnknown {
            debug_assert!(outcome.result.is_err());
        }
        outcome.result?
    } else {
        None
    };

    let group_already_killed = sentinel_controlled || controlled_termination;
    let group_control =
        residual_group_control(leader_identity_error.is_some(), group_already_killed);
    let drain_completion =
        wait_for_post_exit_drains(drain_out, drain_err, pgid, group_control.may_signal()).await;
    let exited_or_identity_unknown = leader_identity_error.is_some()
        || termination
            .as_ref()
            .is_some_and(|termination| matches!(termination, Termination::Exited(_)));
    let cleanup_mode = if exited_or_identity_unknown {
        drain_completion
    } else {
        // `terminate_and_reap` already performed TERM -> KILL as needed.
        DrainCompletion::ForcedGroupKill
    };
    let group_empty = cleanup_residual_group(pgid, cleanup_mode, group_control).await;
    let stdout = captured_string(&stdout_buf).await;
    let stderr = captured_string(&stderr_buf).await;
    process_group_guard.disarm(group_empty);

    if let Some(error) = leader_identity_error {
        return Err(error);
    }
    if let Some(error) = sentinel_error {
        return Err(error);
    }
    if let Some(error) = control_error {
        return Err(error);
    }
    let Some(termination) = termination else {
        return Err(VyaneError::new(
            ErrorKind::Io,
            format!("harness `{program}` completed without a terminal control result"),
        ));
    };

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
    signal_on_timeout: bool,
) -> DrainCompletion {
    let mut out_done = false;
    let mut err_done = false;
    let grace = tokio::time::sleep(POST_EXIT_DRAIN_GRACE);
    tokio::pin!(grace);

    loop {
        if out_done && err_done {
            return DrainCompletion::Eof;
        }

        tokio::select! {
            _ = &mut drain_out, if !out_done => {
                out_done = true;
            }
            _ = &mut drain_err, if !err_done => {
                err_done = true;
            }
            _ = &mut grace => {
                if signal_on_timeout {
                    sigkill_group(pgid);
                }
                if !out_done {
                    drain_out.abort();
                }
                if !err_done {
                    drain_err.abort();
                }
                return if signal_on_timeout {
                    DrainCompletion::ForcedGroupKill
                } else {
                    DrainCompletion::PassiveAbort
                };
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
#[cfg(test)]
pub(crate) async fn run_stream_capture(
    program: &str,
    args: &[String],
    cwd: Option<&std::path::Path>,
    env: &BTreeMap<String, String>,
    control: RunControl,
    on_line: Box<dyn FnMut(&str) + Send + Sync>,
) -> Result<RunResult> {
    run_stream_capture_with_pinned(program, args, cwd, None, env, control, on_line).await
}

/// Streaming counterpart to [`run_capture_with_pinned`].
pub(crate) async fn run_stream_capture_with_pinned(
    program: &str,
    args: &[String],
    cwd: Option<&std::path::Path>,
    pinned_workdir: Option<&PinnedWorkdir>,
    env: &BTreeMap<String, String>,
    control: RunControl,
    on_line: Box<dyn FnMut(&str) + Send + Sync>,
) -> Result<RunResult> {
    let RunControl {
        cancel,
        timeout,
        lifecycle_reporter,
        spawn_authority,
    } = control;
    if spawn_authority.is_some() && lifecycle_reporter.is_none() {
        return Err(VyaneError::new(
            ErrorKind::Unsupported,
            "spawn-authorized harness execution requires lifecycle gating",
        ));
    }
    let control_deadline = control_deadline(timeout)?;
    #[cfg(not(unix))]
    if lifecycle_reporter.is_some() {
        return Err(VyaneError::new(
            ErrorKind::Unsupported,
            "durable harness lifecycle control requires a Unix start gate",
        ));
    }
    #[cfg(not(unix))]
    if pinned_workdir.is_some() {
        return Err(VyaneError::new(
            ErrorKind::Unsupported,
            "pinned mutating workdirs require Unix descriptor-backed cwd enforcement",
        ));
    }
    #[cfg(unix)]
    let inherited_workdir = pinned_workdir
        .map(InheritedWorkdir::duplicate)
        .transpose()?;
    #[cfg(unix)]
    let inherited_workdir_fd = inherited_workdir.map(|inherited| inherited.source);
    #[cfg(not(unix))]
    let inherited_workdir_fd = None;
    let start_gated = lifecycle_reporter.is_some();
    #[cfg(unix)]
    let inherited_target_env = start_gated
        .then(|| InheritedTargetEnv::materialize(env))
        .transpose()?;
    #[cfg(unix)]
    let inherited_target_env_fd = inherited_target_env.map(|inherited| inherited.source);
    #[cfg(not(unix))]
    let inherited_target_env_fd = None;
    let mut cmd = harness_command(program, args, start_gated);
    cmd.stdin(if start_gated {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd.kill_on_drop(true);

    cmd.env_clear();
    if start_gated {
        cmd.env("PATH", "/usr/bin:/bin");
    } else {
        cmd.envs(env);
    }

    if pinned_workdir.is_none() {
        if let Some(dir) = cwd {
            cmd.current_dir(dir);
        }
    }

    let started = Instant::now();
    let (mut child, mut sentinel_status) = spawn_controlled_harness_child(
        &mut cmd,
        program,
        ControlledSpawn {
            reporter: lifecycle_reporter.as_ref(),
            spawn_authority: spawn_authority.as_ref(),
            cancel: &cancel,
            deadline: control_deadline,
            pinned_workdir_fd: inherited_workdir_fd,
            target_env_fd: inherited_target_env_fd,
        },
    )
    .await?;

    let mut process_group_guard = ProcessGroupDropGuard::new(child.id(), lifecycle_reporter);
    establish_lifecycle(&mut child, &mut process_group_guard, program).await?;
    if start_gated {
        authorize_and_release_start_gate(
            &mut child,
            &mut process_group_guard,
            program,
            spawn_authority.as_ref(),
            &cancel,
            control_deadline,
        )
        .await?;
    }
    let pgid = process_group_guard.pgid();

    // stdout is read line-by-line with callback; stderr is captured normally.
    let stdout_buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let stderr_buf = Arc::new(Mutex::new(Vec::new()));
    let drain_out = spawn_line_drain(child.stdout.take(), Arc::clone(&stdout_buf), on_line);
    let drain_err = spawn_drain(child.stderr.take(), Arc::clone(&stderr_buf));

    // Same race: normal exit vs. cancellation vs. timeout.
    let sentinel_controlled = sentinel_status.is_some();
    let mut sentinel_error = None;
    let (termination, leader_identity_error) = {
        let timeout_fut = async {
            match control_deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(timeout_fut);

        tokio::select! {
            biased;
            _ = cancel.cancelled() => (Some(Termination::Cancelled), None),
            _ = &mut timeout_fut => (Some(Termination::TimedOut), None),
            status = child.wait() => {
                match process_group_guard.leader_wait_returned(status) {
                    Ok(status) => {
                        let wrapper_code = exit_code_of(status);
                        #[cfg(unix)]
                        let code = match sentinel_status.as_mut() {
                            Some(reader) => match read_sentinel_exit(reader, program) {
                                Ok(code) => code,
                                Err(error) => {
                                    sentinel_error = Some(error);
                                    wrapper_code
                                }
                            },
                            None => wrapper_code,
                        };
                        #[cfg(not(unix))]
                        let code = wrapper_code;
                        (Some(Termination::Exited(code)), None)
                    }
                    Err(source) => (None, Some(leader_identity_error(program, source))),
                }
            }
        }
    };

    let controlled_termination = termination.as_ref().is_some_and(|termination| {
        matches!(termination, Termination::Cancelled | Termination::TimedOut)
    });
    let control_error = if controlled_termination {
        let outcome = terminate_and_reap(
            &mut child,
            pgid,
            program,
            sentinel_controlled,
            &mut process_group_guard,
        )
        .await;
        debug_assert!(process_group_guard.signal_pgid.is_none());
        if outcome.leader == LeaderResolution::IdentityUnknown {
            debug_assert!(outcome.result.is_err());
        }
        outcome.result?
    } else {
        None
    };

    let group_already_killed = sentinel_controlled || controlled_termination;
    let group_control =
        residual_group_control(leader_identity_error.is_some(), group_already_killed);
    let drain_completion =
        wait_for_post_exit_drains(drain_out, drain_err, pgid, group_control.may_signal()).await;
    let exited_or_identity_unknown = leader_identity_error.is_some()
        || termination
            .as_ref()
            .is_some_and(|termination| matches!(termination, Termination::Exited(_)));
    let cleanup_mode = if exited_or_identity_unknown {
        drain_completion
    } else {
        DrainCompletion::ForcedGroupKill
    };
    let group_empty = cleanup_residual_group(pgid, cleanup_mode, group_control).await;
    let stdout = captured_string(&stdout_buf).await;
    let stderr = captured_string(&stderr_buf).await;
    process_group_guard.disarm(group_empty);

    if let Some(error) = leader_identity_error {
        return Err(error);
    }
    if let Some(error) = sentinel_error {
        return Err(error);
    }
    if let Some(error) = control_error {
        return Err(error);
    }
    let Some(termination) = termination else {
        return Err(VyaneError::new(
            ErrorKind::Io,
            format!("streaming harness `{program}` completed without a terminal control result"),
        ));
    };

    Ok(RunResult {
        termination,
        stdout,
        stderr,
        duration: started.elapsed(),
    })
}

/// Line-by-line stdout reader: reads complete lines from the child's stdout
/// pipe, calls `on_line` for each, and appends to the capture buffer.
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

fn leader_identity_error(program: &str, source: std::io::Error) -> VyaneError {
    VyaneError::with_source(
        ErrorKind::Io,
        format!("lost exact leader identity while waiting for harness `{program}`"),
        source,
    )
}

#[cfg(unix)]
fn read_sentinel_exit(reader: &mut SentinelStatusReader, program: &str) -> Result<i32> {
    let mut payload = String::new();
    reader
        .take(SENTINEL_STATUS_LIMIT)
        .read_to_string(&mut payload)
        .map_err(|source| {
            VyaneError::with_source(
                ErrorKind::Io,
                format!("failed to read lifecycle sentinel status for `{program}`"),
                source,
            )
        })?;
    let line = payload.strip_suffix('\n').ok_or_else(|| {
        VyaneError::new(
            ErrorKind::HarnessFailed,
            format!("lifecycle sentinel for `{program}` omitted its exit status"),
        )
    })?;
    if line.contains(['\n', '\r']) {
        return Err(VyaneError::new(
            ErrorKind::HarnessFailed,
            format!("lifecycle sentinel for `{program}` returned multiple status lines"),
        ));
    }
    let code = line
        .strip_prefix(SENTINEL_STATUS_PREFIX)
        .ok_or_else(|| {
            VyaneError::new(
                ErrorKind::HarnessFailed,
                format!("lifecycle sentinel for `{program}` returned an invalid status"),
            )
        })?
        .parse::<i32>()
        .map_err(|source| {
            VyaneError::with_source(
                ErrorKind::HarnessFailed,
                format!("lifecycle sentinel for `{program}` returned a non-numeric status"),
                source,
            )
        })?;
    if !(0..=255).contains(&code) {
        return Err(VyaneError::new(
            ErrorKind::HarnessFailed,
            format!("lifecycle sentinel for `{program}` returned out-of-range status {code}"),
        ));
    }
    Ok(code)
}

#[cfg(unix)]
fn install_process_group(
    cmd: &mut Command,
    sentinel_status_fd: Option<OwnedFd>,
    pinned_workdir_fd: Option<OwnedFd>,
    target_env_fd: Option<OwnedFd>,
) -> Result<()> {
    cmd.process_group(0);
    let mut mappings = Vec::with_capacity(3);
    if let Some(fd) = sentinel_status_fd {
        mappings.push(FdMapping {
            parent_fd: fd,
            child_fd: SENTINEL_STATUS_FD,
        });
    }
    if let Some(fd) = pinned_workdir_fd {
        let source_fd = fd.as_raw_fd();
        #[cfg(target_os = "linux")]
        let workdir = format!("/proc/self/fd/{source_fd}");
        #[cfg(all(unix, not(target_os = "linux")))]
        let workdir = format!("/dev/fd/{source_fd}");
        cmd.current_dir(workdir);
        mappings.push(FdMapping {
            parent_fd: fd,
            child_fd: PINNED_WORKDIR_FD,
        });
    }
    if let Some(fd) = target_env_fd {
        mappings.push(FdMapping {
            parent_fd: fd,
            child_fd: TARGET_ENV_FD,
        });
    }
    cmd.fd_mappings(mappings).map_err(|source| {
        VyaneError::with_source(
            ErrorKind::Config,
            "harness child file-descriptor mappings collide",
            source,
        )
    })?;
    Ok(())
}

#[cfg(not(unix))]
fn install_process_group(_cmd: &mut Command) -> Result<()> {
    // Non-Unix: no process-group setup. Group-kill semantics degrade to a direct child kill.
    // v0.1 targets Unix; this stub keeps the crate compiling elsewhere.
    Ok(())
}

/// Terminate a running child after cancellation/timeout and reap it.
///
/// Unix uses the process group so descendants receive the same TERM -> KILL
/// policy. Non-Unix cannot provide that group guarantee, but it must still
/// force-kill and await the direct child; failure is surfaced as `Unsupported`
/// rather than waiting forever after a no-op group kill.
#[cfg(unix)]
async fn terminate_and_reap(
    child: &mut Child,
    pgid: Option<i32>,
    program: &str,
    sentinel_controlled: bool,
    guard: &mut ProcessGroupDropGuard,
) -> TerminateAndReapOutcome {
    let Some(pgid) = pgid.filter(|pgid| *pgid > 0) else {
        let killed = force_kill_direct_and_wait(child, guard).await;
        let leader = if killed.is_ok() {
            LeaderResolution::Reaped
        } else {
            LeaderResolution::IdentityUnknown
        };
        return TerminateAndReapOutcome {
            leader,
            result: killed.map(|()| None).map_err(|source| {
                VyaneError::with_source(
                    ErrorKind::Io,
                    format!(
                        "failed to terminate and reap harness `{program}` without a process id"
                    ),
                    source,
                )
            }),
        };
    };

    signal_group(pgid, SIGTERM);
    let deadline = tokio::time::Instant::now() + KILL_GRACE;

    // Reap the leader before probing for an empty group. Otherwise an exited
    // but unreaped leader remains a zombie and makes `kill(-pgid, 0)` look live
    // for the entire grace even when TERM worked immediately.
    let child_reaped = match wait_for_leader_until(child, deadline, guard).await {
        LeaderWait::Reaped => true,
        LeaderWait::TimedOut => false,
        LeaderWait::IdentityUnknown(source) => {
            return TerminateAndReapOutcome {
                leader: LeaderResolution::IdentityUnknown,
                result: Err(VyaneError::with_source(
                    ErrorKind::Io,
                    format!(
                        "lost exact leader identity while waiting to terminate harness `{program}`"
                    ),
                    source,
                )),
            };
        }
    };
    if child_reaped && wait_for_group_exit_until(pgid, deadline).await {
        return TerminateAndReapOutcome {
            leader: LeaderResolution::Reaped,
            result: Ok(None),
        };
    }
    if child_reaped && sentinel_controlled {
        // The exact group leader is already gone. Even if the numeric PGID
        // currently looks live, it can become empty and be reused between this
        // probe and a signal. Fail closed and let the caller publish
        // `Stopped { group_empty: false }`; never turn a dead sentinel into an
        // unauthenticated group kill.
        return TerminateAndReapOutcome {
            leader: LeaderResolution::Reaped,
            result: Ok(Some(VyaneError::new(
                ErrorKind::Io,
                format!(
                    "harness `{program}` sentinel exited before its process group became empty; refusing unsafe SIGKILL escalation"
                ),
            ))),
        };
    }

    signal_group(pgid, SIGKILL);
    if !child_reaped {
        // The exact sentinel is still alive, so it continues to reserve the
        // PGID across this escalation boundary. A direct-child hard kill is a
        // final fallback if group signalling did not reach it.
        if let Err(source) = force_kill_direct_and_wait(child, guard).await {
            return TerminateAndReapOutcome {
                leader: LeaderResolution::IdentityUnknown,
                result: Err(VyaneError::with_source(
                    ErrorKind::Io,
                    format!("failed to force-kill and reap harness `{program}`"),
                    source,
                )),
            };
        }
    }
    let _ = wait_for_group_exit(pgid, POST_KILL_SETTLE_GRACE).await;
    TerminateAndReapOutcome {
        leader: LeaderResolution::Reaped,
        result: Ok(None),
    }
}

#[cfg(not(unix))]
async fn terminate_and_reap(
    child: &mut Child,
    _pgid: Option<i32>,
    program: &str,
    _sentinel_controlled: bool,
    guard: &mut ProcessGroupDropGuard,
) -> TerminateAndReapOutcome {
    let killed = force_kill_direct_and_wait(child, guard).await;
    let leader = if killed.is_ok() {
        LeaderResolution::Reaped
    } else {
        LeaderResolution::IdentityUnknown
    };
    TerminateAndReapOutcome {
        leader,
        result: killed.map(|()| None).map_err(|source| {
            VyaneError::with_source(
                ErrorKind::Unsupported,
                format!(
                    "process-group control is unavailable and direct-child termination failed for harness `{program}`"
                ),
                source,
            )
        }),
    }
}

#[cfg(unix)]
async fn wait_for_leader_until(
    child: &mut Child,
    deadline: tokio::time::Instant,
    guard: &mut ProcessGroupDropGuard,
) -> LeaderWait {
    match tokio::time::timeout_at(deadline, child.wait()).await {
        Err(_) => LeaderWait::TimedOut,
        Ok(result) => match guard.leader_wait_returned(result) {
            Ok(_) => LeaderWait::Reaped,
            Err(error) => LeaderWait::IdentityUnknown(error),
        },
    }
}

/// Force the exact direct child down and wait for it. Once either operation
/// reports an error, process identity is no longer strong enough to authorize a
/// numeric process-group signal, so the guard fails closed before returning.
async fn force_kill_direct_and_wait(
    child: &mut Child,
    guard: &mut ProcessGroupDropGuard,
) -> std::io::Result<()> {
    if let Err(error) = child.start_kill() {
        guard.revoke_signal_authority();
        return Err(error);
    }
    guard.leader_wait_returned(child.wait().await).map(|_| ())
}

/// Hard-kill a just-spawned process whose `Started` report failed, then reap
/// the direct child before returning the lifecycle error.
#[cfg(unix)]
async fn force_kill_and_reap(
    child: &mut Child,
    pgid: Option<i32>,
    guard: &mut ProcessGroupDropGuard,
) -> std::io::Result<()> {
    if pgid.is_some_and(|pgid| pgid > 0) {
        sigkill_group(pgid);
        guard.leader_wait_returned(child.wait().await).map(|_| ())
    } else {
        force_kill_direct_and_wait(child, guard).await
    }
}

#[cfg(not(unix))]
async fn force_kill_and_reap(
    child: &mut Child,
    _pgid: Option<i32>,
    guard: &mut ProcessGroupDropGuard,
) -> std::io::Result<()> {
    force_kill_direct_and_wait(child, guard).await
}

#[cfg(unix)]
async fn group_is_empty_after_kill(pgid: Option<i32>) -> bool {
    match pgid.filter(|pgid| *pgid > 0) {
        Some(pgid) => wait_for_group_exit(pgid, POST_KILL_SETTLE_GRACE).await,
        None => true,
    }
}

#[cfg(not(unix))]
async fn group_is_empty_after_kill(_pgid: Option<i32>) -> bool {
    true
}

/// Before publishing `Stopped`, settle any descendants left in the group after
/// the direct leader exited. If pipe draining already had to SIGKILL the group,
/// or exact leader identity became unknown, only observe for a bounded settle;
/// otherwise use the normal graceful escalation.
#[cfg(unix)]
async fn cleanup_residual_group(
    pgid: Option<i32>,
    drain_completion: DrainCompletion,
    control: ResidualGroupControl,
) -> bool {
    let Some(pgid) = pgid.filter(|pgid| *pgid > 0) else {
        return true;
    };
    if !control.may_signal() {
        return wait_for_group_exit(pgid, POST_KILL_SETTLE_GRACE).await;
    }
    match drain_completion {
        DrainCompletion::Eof => terminate_residual_group(pgid).await,
        DrainCompletion::ForcedGroupKill => wait_for_group_exit(pgid, POST_KILL_SETTLE_GRACE).await,
        DrainCompletion::PassiveAbort => wait_for_group_exit(pgid, POST_KILL_SETTLE_GRACE).await,
    }
}

#[cfg(not(unix))]
async fn cleanup_residual_group(
    _pgid: Option<i32>,
    _drain_completion: DrainCompletion,
    _control: ResidualGroupControl,
) -> bool {
    // There is no portable descendant-group handle to clean after the direct
    // child exits. Active cancel/timeout still uses `terminate_and_reap` above.
    true
}

/// TERM a residual group, return as soon as it disappears, and only wait the
/// full grace when a descendant actually remains alive.
#[cfg(unix)]
async fn terminate_residual_group(pgid: i32) -> bool {
    if !process_group_alive(pgid) {
        return true;
    }
    signal_group(pgid, SIGTERM);
    if wait_for_group_exit(pgid, KILL_GRACE).await {
        return true;
    }
    signal_group(pgid, SIGKILL);
    wait_for_group_exit(pgid, POST_KILL_SETTLE_GRACE).await
}

#[cfg(unix)]
async fn wait_for_group_exit(pgid: i32, grace: Duration) -> bool {
    wait_for_group_exit_until(pgid, tokio::time::Instant::now() + grace).await
}

#[cfg(unix)]
async fn wait_for_group_exit_until(pgid: i32, deadline: tokio::time::Instant) -> bool {
    loop {
        if !process_group_alive(pgid) {
            return true;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return false;
        }
        tokio::time::sleep_until(std::cmp::min(deadline, now + GROUP_EXIT_POLL_INTERVAL)).await;
    }
}

#[cfg(unix)]
fn process_group_alive(pgid: i32) -> bool {
    let Some(pgid) = rustix::process::Pid::from_raw(pgid) else {
        return false;
    };
    matches!(
        rustix::process::test_kill_process_group(pgid),
        Ok(()) | Err(rustix::io::Errno::PERM)
    )
}

#[cfg(unix)]
fn sigkill_group(pgid: Option<i32>) {
    let Some(pgid) = pgid else { return };
    signal_group(pgid, SIGKILL);
}

#[cfg(not(unix))]
fn sigkill_group(_pgid: Option<i32>) {}

#[cfg(unix)]
const SIGTERM: i32 = 15;
#[cfg(unix)]
const SIGKILL: i32 = 9;

/// Send `sig` to the process group `pgid` by passing the negative pid, which is
/// how `kill(2)` addresses an entire group.
#[cfg(unix)]
fn signal_group(pgid: i32, sig: i32) {
    let Some(pgid) = rustix::process::Pid::from_raw(pgid) else {
        return;
    };
    let signal = match sig {
        SIGTERM => rustix::process::Signal::TERM,
        SIGKILL => rustix::process::Signal::KILL,
        _ => return,
    };
    let _ = rustix::process::kill_process_group(pgid, signal);
}

#[cfg(all(test, unix))]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::io::Write as _;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc as StdArc, Mutex as StdMutex};

    use super::*;

    fn recording_reporter() -> (
        HarnessLifecycleReporter,
        StdArc<StdMutex<Vec<HarnessLifecycleEvent>>>,
    ) {
        let events = StdArc::new(StdMutex::new(Vec::new()));
        let callback_events = StdArc::clone(&events);
        let reporter = HarnessLifecycleReporter::new(move |event| {
            callback_events.lock().unwrap().push(event);
            Ok(())
        });
        (reporter, events)
    }

    fn assert_started_then_stopped(events: &[HarnessLifecycleEvent]) {
        assert_eq!(events.len(), 2, "unexpected lifecycle events: {events:?}");
        let HarnessLifecycleEvent::Started {
            pid: started_pid,
            pgid: started_pgid,
            ..
        } = events[0]
        else {
            panic!("first event was not Started: {:?}", events[0]);
        };
        let HarnessLifecycleEvent::Stopped {
            pid: stopped_pid,
            pgid: stopped_pgid,
            ..
        } = events[1]
        else {
            panic!("second event was not Stopped: {:?}", events[1]);
        };
        assert_eq!(started_pid, stopped_pid);
        assert_eq!(started_pgid, stopped_pgid);
        assert_eq!(started_pid as i32, started_pgid);
    }

    fn assert_empty_target_environment(stdout: &str) {
        #[cfg(target_os = "macos")]
        {
            let keys = stdout
                .lines()
                .map(|line| line.split_once('=').map(|(key, _)| key).unwrap_or(line))
                .collect::<Vec<_>>();
            assert!(
                keys.iter().all(|key| matches!(*key, "SHLVL" | "_")),
                "empty target inherited unexpected environment keys: {keys:?}"
            );
        }
        #[cfg(not(target_os = "macos"))]
        assert_eq!(stdout, "");
    }

    fn cleanup_observing_reporter(
        heartbeat: std::path::PathBuf,
    ) -> (
        HarnessLifecycleReporter,
        StdArc<StdMutex<Vec<HarnessLifecycleEvent>>>,
        StdArc<AtomicBool>,
    ) {
        let events = StdArc::new(StdMutex::new(Vec::new()));
        let callback_events = StdArc::clone(&events);
        let stopped_after_cleanup = StdArc::new(AtomicBool::new(false));
        let callback_stopped_after_cleanup = StdArc::clone(&stopped_after_cleanup);
        let reporter = HarnessLifecycleReporter::new(move |event| {
            let stopped = matches!(event, HarnessLifecycleEvent::Stopped { .. });
            callback_events.lock().unwrap().push(event);
            if stopped {
                let before = std::fs::metadata(&heartbeat).map(|meta| meta.len()).ok();
                std::thread::sleep(Duration::from_millis(150));
                let after = std::fs::metadata(&heartbeat).map(|meta| meta.len()).ok();
                callback_stopped_after_cleanup
                    .store(before.is_some() && before == after, Ordering::SeqCst);
            }
            Ok(())
        });
        (reporter, events, stopped_after_cleanup)
    }

    fn closed_stdio_background_args(
        heartbeat: &std::path::Path,
        grandchild_pid: &std::path::Path,
    ) -> Vec<String> {
        let script = format!(
            "( exec >/dev/null 2>&1; trap '' TERM; while :; do printf x >> '{}'; /bin/sleep 0.02; done ) & printf '%s\\n' \"$!\" > '{}'; while [ ! -s '{}' ]; do /bin/sleep 0.01; done; printf 'captured\\n'; exit 0",
            heartbeat.display(),
            grandchild_pid.display(),
            heartbeat.display(),
        );
        vec!["-c".to_string(), script]
    }

    #[tokio::test]
    async fn run_capture_reports_started_then_stopped() {
        let (reporter, events) = recording_reporter();
        let args = vec!["-c".to_string(), "printf captured".to_string()];

        let result = run_capture(
            "/bin/sh",
            &args,
            None,
            &BTreeMap::new(),
            RunControl::new(
                CancellationToken::new(),
                Some(Duration::from_secs(5)),
                Some(reporter),
            ),
        )
        .await
        .unwrap();

        assert!(matches!(result.termination, Termination::Exited(0)));
        assert_eq!(result.stdout, "captured");
        assert_started_then_stopped(&events.lock().unwrap());
    }

    #[tokio::test]
    async fn spawn_authority_requires_a_lifecycle_start_gate() {
        let calls = StdArc::new(AtomicUsize::new(0));
        let callback_calls = StdArc::clone(&calls);
        let authority = HarnessSpawnAuthority::new(move || {
            callback_calls.fetch_add(1, Ordering::SeqCst);
            true
        });
        let error = run_capture(
            "/bin/sh",
            &["-c".into(), "exit 0".into()],
            None,
            &BTreeMap::new(),
            RunControl::new(CancellationToken::new(), Some(Duration::from_secs(5)), None)
                .with_spawn_authority(Some(authority)),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn pre_cancelled_run_creates_no_child_effect() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-exist");
        let cancel = CancellationToken::new();
        cancel.cancel();
        let error = run_capture(
            "/bin/sh",
            &["-c".into(), format!("printf ran > '{}'", marker.display())],
            None,
            &BTreeMap::new(),
            RunControl::new(cancel, Some(Duration::from_secs(5)), None),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Cancelled);
        assert!(!marker.exists());
    }

    #[tokio::test]
    async fn unrepresentable_timeout_is_rejected_before_spawn() {
        let error = run_capture(
            "/bin/sh",
            &["-c".into(), "exit 0".into()],
            None,
            &BTreeMap::new(),
            RunControl::new(CancellationToken::new(), Some(Duration::MAX), None),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Config);
    }

    #[tokio::test]
    async fn deadline_expiry_during_started_callback_never_releases_target() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-exist");
        let reporter = HarnessLifecycleReporter::new(|event| {
            if matches!(event, HarnessLifecycleEvent::Started { .. }) {
                std::thread::sleep(Duration::from_millis(30));
            }
            Ok(())
        });
        let error = run_capture(
            "/bin/sh",
            &["-c".into(), format!("printf ran > '{}'", marker.display())],
            None,
            &BTreeMap::new(),
            RunControl::new(
                CancellationToken::new(),
                Some(Duration::from_millis(5)),
                Some(reporter),
            ),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Timeout);
        assert!(!marker.exists());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn sentinel_never_inherits_target_loader_environment() {
        let sentinel_env = StdArc::new(StdMutex::new(Vec::new()));
        let callback_env = StdArc::clone(&sentinel_env);
        let reporter = HarnessLifecycleReporter::new(move |event| {
            if let HarnessLifecycleEvent::Started { pid, .. } = event {
                *callback_env.lock().unwrap() =
                    std::fs::read(format!("/proc/{pid}/environ")).unwrap();
            }
            Ok(())
        });
        let mut env = BTreeMap::new();
        env.insert("LD_PRELOAD".into(), "/nonexistent/target-only.so".into());
        env.insert("TARGET_ONLY".into(), "private-value".into());
        let result = run_capture(
            "/usr/bin/env",
            &[],
            None,
            &env,
            RunControl::new(
                CancellationToken::new(),
                Some(Duration::from_secs(5)),
                Some(reporter),
            ),
        )
        .await
        .unwrap();

        let sentinel_env = sentinel_env.lock().unwrap();
        assert!(
            sentinel_env
                .windows(b"PATH=/usr/bin:/bin\0".len())
                .any(|window| window == b"PATH=/usr/bin:/bin\0"),
            "did not capture the gated sentinel environment"
        );
        assert!(
            !sentinel_env
                .windows(b"LD_PRELOAD".len())
                .any(|window| window == b"LD_PRELOAD")
        );
        assert!(
            !sentinel_env
                .windows(b"TARGET_ONLY".len())
                .any(|window| window == b"TARGET_ONLY")
        );
        assert!(result.stdout.contains("TARGET_ONLY=private-value\n"));
    }

    #[tokio::test]
    async fn lifecycle_target_environment_replaces_sentinel_environment() {
        let (reporter, _) = recording_reporter();
        let result = run_capture(
            "/usr/bin/env",
            &[],
            None,
            &BTreeMap::new(),
            RunControl::new(
                CancellationToken::new(),
                Some(Duration::from_secs(5)),
                Some(reporter),
            ),
        )
        .await
        .unwrap();
        assert_empty_target_environment(&result.stdout);
    }

    #[tokio::test]
    async fn streaming_lifecycle_target_environment_replaces_sentinel_environment() {
        let (reporter, _) = recording_reporter();
        let lines = StdArc::new(StdMutex::new(Vec::new()));
        let callback_lines = StdArc::clone(&lines);
        let result = run_stream_capture(
            "/usr/bin/env",
            &[],
            None,
            &BTreeMap::new(),
            RunControl::new(
                CancellationToken::new(),
                Some(Duration::from_secs(5)),
                Some(reporter),
            ),
            Box::new(move |line| callback_lines.lock().unwrap().push(line.to_string())),
        )
        .await
        .unwrap();
        assert_empty_target_environment(&result.stdout);
        assert_eq!(
            *lines.lock().unwrap(),
            result.stdout.lines().map(str::to_owned).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn cancellation_after_started_never_releases_real_target() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-exist");
        let cancel = CancellationToken::new();
        let reporter_cancel = cancel.clone();
        let events = StdArc::new(StdMutex::new(Vec::new()));
        let callback_events = StdArc::clone(&events);
        let reporter = HarnessLifecycleReporter::new(move |event| {
            if matches!(event, HarnessLifecycleEvent::Started { .. }) {
                reporter_cancel.cancel();
            }
            callback_events.lock().unwrap().push(event);
            Ok(())
        });
        let error = run_capture(
            "/bin/sh",
            &["-c".into(), format!("printf ran > '{}'", marker.display())],
            None,
            &BTreeMap::new(),
            RunControl::new(cancel, Some(Duration::from_secs(5)), Some(reporter)),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Cancelled);
        assert!(!marker.exists());
        let events = events.lock().unwrap();
        assert!(matches!(
            events.first(),
            Some(HarnessLifecycleEvent::Started { .. })
        ));
        assert!(matches!(
            events.last(),
            Some(HarnessLifecycleEvent::Stopped { .. })
        ));
    }

    #[tokio::test]
    async fn rejected_pre_spawn_authority_creates_no_child_effect() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-exist");
        let (reporter, events) = recording_reporter();
        let calls = StdArc::new(AtomicUsize::new(0));
        let callback_calls = StdArc::clone(&calls);
        let authority = HarnessSpawnAuthority::new(move || {
            callback_calls.fetch_add(1, Ordering::SeqCst);
            false
        });
        let error = run_capture(
            "/bin/sh",
            &["-c".into(), format!("printf ran > '{}'", marker.display())],
            None,
            &BTreeMap::new(),
            RunControl::new(
                CancellationToken::new(),
                Some(Duration::from_secs(5)),
                Some(reporter),
            )
            .with_spawn_authority(Some(authority)),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Conflict);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!marker.exists());
        assert!(events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cancellation_during_pre_spawn_authorization_creates_no_child() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-exist");
        let (reporter, events) = recording_reporter();
        let cancel = CancellationToken::new();
        let callback_cancel = cancel.clone();
        let authority = HarnessSpawnAuthority::new(move || {
            callback_cancel.cancel();
            true
        });
        let error = run_capture(
            "/bin/sh",
            &["-c".into(), format!("printf ran > '{}'", marker.display())],
            None,
            &BTreeMap::new(),
            RunControl::new(cancel, Some(Duration::from_secs(5)), Some(reporter))
                .with_spawn_authority(Some(authority)),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Cancelled);
        assert!(!marker.exists());
        assert!(events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn revoked_authority_after_started_never_releases_real_target() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-exist");
        let allowed = StdArc::new(AtomicBool::new(true));
        let callback_allowed = StdArc::clone(&allowed);
        let calls = StdArc::new(AtomicUsize::new(0));
        let callback_calls = StdArc::clone(&calls);
        let authority = HarnessSpawnAuthority::new(move || {
            callback_calls.fetch_add(1, Ordering::SeqCst);
            callback_allowed.load(Ordering::SeqCst)
        });
        let events = StdArc::new(StdMutex::new(Vec::new()));
        let callback_events = StdArc::clone(&events);
        let reporter_allowed = StdArc::clone(&allowed);
        let reporter = HarnessLifecycleReporter::new(move |event| {
            if matches!(event, HarnessLifecycleEvent::Started { .. }) {
                reporter_allowed.store(false, Ordering::SeqCst);
            }
            callback_events.lock().unwrap().push(event);
            Ok(())
        });

        let error = run_capture(
            "/bin/sh",
            &["-c".into(), format!("printf ran > '{}'", marker.display())],
            None,
            &BTreeMap::new(),
            RunControl::new(
                CancellationToken::new(),
                Some(Duration::from_secs(5)),
                Some(reporter),
            )
            .with_spawn_authority(Some(authority)),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Conflict);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!marker.exists());
        let events = events.lock().unwrap();
        assert!(matches!(
            events.first(),
            Some(HarnessLifecycleEvent::Started { .. })
        ));
        assert!(matches!(
            events.last(),
            Some(HarnessLifecycleEvent::Stopped { .. })
        ));
    }

    #[tokio::test]
    async fn deadline_crossed_during_final_authorization_never_releases_target() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-exist");
        let (reporter, events) = recording_reporter();
        let calls = StdArc::new(AtomicUsize::new(0));
        let callback_calls = StdArc::clone(&calls);
        let authority = HarnessSpawnAuthority::new(move || {
            if callback_calls.fetch_add(1, Ordering::SeqCst) == 1 {
                std::thread::sleep(Duration::from_millis(600));
            }
            true
        });
        let error = run_capture(
            "/bin/sh",
            &["-c".into(), format!("printf ran > '{}'", marker.display())],
            None,
            &BTreeMap::new(),
            RunControl::new(
                CancellationToken::new(),
                Some(Duration::from_millis(500)),
                Some(reporter),
            )
            .with_spawn_authority(Some(authority)),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Timeout);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!marker.exists());
        let events = events.lock().unwrap();
        assert!(matches!(
            events.first(),
            Some(HarnessLifecycleEvent::Started { .. })
        ));
        assert!(matches!(
            events.last(),
            Some(HarnessLifecycleEvent::Stopped { .. })
        ));
    }

    #[tokio::test]
    async fn live_authority_is_checked_before_spawn_and_target_release() {
        let (reporter, events) = recording_reporter();
        let calls = StdArc::new(AtomicUsize::new(0));
        let callback_calls = StdArc::clone(&calls);
        let authority = HarnessSpawnAuthority::new(move || {
            callback_calls.fetch_add(1, Ordering::SeqCst);
            true
        });
        let result = run_capture(
            "/bin/sh",
            &["-c".into(), "printf authorized".into()],
            None,
            &BTreeMap::new(),
            RunControl::new(
                CancellationToken::new(),
                Some(Duration::from_secs(5)),
                Some(reporter),
            )
            .with_spawn_authority(Some(authority)),
        )
        .await
        .unwrap();

        assert!(matches!(result.termination, Termination::Exited(0)));
        assert_eq!(result.stdout, "authorized");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_started_then_stopped(&events.lock().unwrap());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn lifecycle_sentinel_remains_exact_group_leader_while_target_runs() {
        let dir = tempfile::TempDir::new().unwrap();
        let target_pid_path = dir.path().join("target.pid");
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let reporter = HarnessLifecycleReporter::new(move |event| {
            if let HarnessLifecycleEvent::Started { pid, pgid } = event {
                let _ = started_tx.send((pid, pgid));
            }
            Ok(())
        });
        let script = format!(
            "printf '%s\\n' \"$$\" > '{}'; exec /bin/sleep 0.5",
            target_pid_path.display()
        );

        let run = tokio::spawn(async move {
            run_capture(
                "/bin/sh",
                &["-c".to_string(), script],
                None,
                &BTreeMap::new(),
                RunControl::new(
                    CancellationToken::new(),
                    Some(Duration::from_secs(5)),
                    Some(reporter),
                ),
            )
            .await
        });

        let (sentinel_pid, sentinel_pgid) =
            tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(sentinel_pid as i32, sentinel_pgid);
        for _ in 0..100 {
            if target_pid_path.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let target_pid: i32 = std::fs::read_to_string(&target_pid_path)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let target_stat = std::fs::read_to_string(format!("/proc/{target_pid}/stat")).unwrap();
        let after_name = target_stat.rsplit_once(')').unwrap().1.trim_start();
        let mut fields = after_name.split_whitespace();
        let _state = fields.next().unwrap();
        let target_parent: i32 = fields.next().unwrap().parse().unwrap();
        let target_pgid: i32 = fields.next().unwrap().parse().unwrap();
        assert_eq!(target_parent, sentinel_pid as i32);
        assert_eq!(target_pgid, sentinel_pgid);
        let sentinel_pid = rustix::process::Pid::from_raw(sentinel_pid as i32).unwrap();
        assert_eq!(rustix::process::test_kill_process(sentinel_pid), Ok(()));

        let result = run.await.unwrap().unwrap();
        assert!(matches!(result.termination, Termination::Exited(0)));
    }

    #[tokio::test]
    async fn lifecycle_sentinel_preserves_real_target_exit_code() {
        let (reporter, events) = recording_reporter();
        let result = run_capture(
            "/bin/sh",
            &["-c".to_string(), "exit 37".to_string()],
            None,
            &BTreeMap::new(),
            RunControl::new(
                CancellationToken::new(),
                Some(Duration::from_secs(5)),
                Some(reporter),
            ),
        )
        .await
        .unwrap();

        assert!(matches!(result.termination, Termination::Exited(37)));
        assert_started_then_stopped(&events.lock().unwrap());
    }

    #[test]
    fn sentinel_status_pipe_rejects_missing_and_malformed_messages() {
        let (mut missing_reader, missing_writer) = UnixStream::pair().unwrap();
        drop(missing_writer);
        let missing = read_sentinel_exit(&mut missing_reader, "/bin/false").unwrap_err();
        assert_eq!(missing.kind, ErrorKind::HarnessFailed);
        assert!(missing.message.contains("omitted its exit status"));

        let (mut malformed_reader, mut malformed_writer) = UnixStream::pair().unwrap();
        malformed_writer.write_all(b"not-a-status\n").unwrap();
        drop(malformed_writer);
        let malformed = read_sentinel_exit(&mut malformed_reader, "/bin/false").unwrap_err();
        assert_eq!(malformed.kind, ErrorKind::HarnessFailed);
        assert!(malformed.message.contains("invalid status"));
    }

    #[tokio::test]
    async fn command_fd_mapping_preserves_unix_stream_endpoint() {
        let (mut parent, child) = UnixStream::pair().unwrap();
        parent
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("printf mapped >&9");
        command
            .fd_mappings(vec![FdMapping {
                parent_fd: child.into(),
                child_fd: SENTINEL_STATUS_FD,
            }])
            .unwrap();
        let status = command.status().await.unwrap();
        assert!(status.success());
        drop(command);
        let mut output = String::new();
        parent.read_to_string(&mut output).unwrap();
        assert_eq!(output, "mapped");
    }

    #[tokio::test]
    async fn command_fd_mapping_supports_lifecycle_sentinel() {
        let (mut parent, child) = UnixStream::pair().unwrap();
        parent
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let target_env = InheritedTargetEnv::materialize(&BTreeMap::new()).unwrap();
        let args = vec!["-c".to_string(), "exit 0".to_string()];
        let mut command = harness_command("/bin/sh", &args, true);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        install_process_group(
            &mut command,
            Some(child.into()),
            None,
            Some(target_env.source),
        )
        .unwrap();
        let mut child = command.spawn().unwrap();
        prepare_start_gate(&mut child)
            .unwrap()
            .write_all(b"vyane-start\n")
            .unwrap();
        let _ = child.wait().await.unwrap();
        drop(command);
        let mut output = String::new();
        parent.read_to_string(&mut output).unwrap();
        assert_eq!(output, "vyane-exit:0\n");
    }

    #[tokio::test]
    async fn sentinel_kills_its_group_when_status_reader_disappears() {
        let reporter = HarnessLifecycleReporter::new(|_| Ok(()));
        let args = vec!["-c".to_string(), "exit 0".to_string()];
        let mut command = harness_command("/bin/sh", &args, true);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let target_env = InheritedTargetEnv::materialize(&BTreeMap::new()).unwrap();
        let cancel = CancellationToken::new();
        let (mut child, status_reader) = spawn_controlled_harness_child(
            &mut command,
            "/bin/sh",
            ControlledSpawn {
                reporter: Some(&reporter),
                spawn_authority: None,
                cancel: &cancel,
                deadline: None,
                pinned_workdir_fd: None,
                target_env_fd: Some(target_env.source),
            },
        )
        .await
        .unwrap();
        let mut guard = ProcessGroupDropGuard::new(child.id(), Some(reporter));
        establish_lifecycle(&mut child, &mut guard, "/bin/sh")
            .await
            .unwrap();
        let pgid = guard.pgid();
        drop(status_reader);
        authorize_and_release_start_gate(&mut child, &mut guard, "/bin/sh", None, &cancel, None)
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .expect("sentinel survived a broken status pipe")
            .unwrap();
        guard.revoke_signal_authority();
        let group_empty = group_is_empty_after_kill(pgid).await;
        guard.disarm(group_empty);
        assert!(group_empty, "sentinel left a process group after EPIPE");
    }

    #[tokio::test]
    async fn cancellation_returns_before_kill_grace_when_group_exits_on_term() {
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            trigger.cancel();
        });
        let args = vec!["-c".to_string(), "exec /bin/sleep 30".to_string()];

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            run_capture(
                "/bin/sh",
                &args,
                None,
                &BTreeMap::new(),
                RunControl::new(cancel, None, None),
            ),
        )
        .await
        .expect("TERM-responsive group waited the full three-second kill grace")
        .unwrap();

        assert!(matches!(result.termination, Termination::Cancelled));
    }

    #[tokio::test]
    async fn lifecycle_capture_preserves_cancelled_when_target_exits_during_term() {
        let dir = tempfile::TempDir::new().unwrap();
        let ready = dir.path().join("capture-ready");
        let script = format!(
            "trap 'exit 0' TERM; printf ready > '{}'; while :; do /bin/sleep 0.05; done",
            ready.display()
        );
        let cancel = CancellationToken::new();
        let run_cancel = cancel.clone();
        let reporter = HarnessLifecycleReporter::new(|_| Ok(()));
        let run = tokio::spawn(async move {
            run_capture(
                "/bin/sh",
                &["-c".to_string(), script],
                None,
                &BTreeMap::new(),
                RunControl::new(run_cancel, None, Some(reporter)),
            )
            .await
        });
        for _ in 0..200 {
            if ready.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ready.exists(), "TERM-responsive target did not start");
        cancel.cancel();

        let result = tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("cancelled lifecycle capture hung")
            .unwrap()
            .unwrap();
        assert!(matches!(result.termination, Termination::Cancelled));
    }

    #[tokio::test]
    async fn lifecycle_stream_preserves_cancelled_when_target_exits_during_term() {
        let dir = tempfile::TempDir::new().unwrap();
        let ready = dir.path().join("stream-ready");
        let script = format!(
            "trap 'exit 0' TERM; printf ready > '{}'; printf 'live\\n'; while :; do /bin/sleep 0.05; done",
            ready.display()
        );
        let cancel = CancellationToken::new();
        let run_cancel = cancel.clone();
        let reporter = HarnessLifecycleReporter::new(|_| Ok(()));
        let run = tokio::spawn(async move {
            run_stream_capture(
                "/bin/sh",
                &["-c".to_string(), script],
                None,
                &BTreeMap::new(),
                RunControl::new(run_cancel, None, Some(reporter)),
                Box::new(|_| {}),
            )
            .await
        });
        for _ in 0..200 {
            if ready.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            ready.exists(),
            "TERM-responsive streaming target did not start"
        );
        cancel.cancel();

        let result = tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("cancelled lifecycle stream hung")
            .unwrap()
            .unwrap();
        assert!(matches!(result.termination, Termination::Cancelled));
    }

    #[tokio::test]
    async fn reaped_sentinel_never_authorizes_numeric_group_kill() {
        let dir = tempfile::TempDir::new().unwrap();
        let ready = dir.path().join("residual-ready");
        let script = format!(
            "( trap '' TERM; while :; do /bin/sleep 1; done ) & trap 'exit 0' TERM; printf ready > '{}'; while :; do wait; done",
            ready.display()
        );
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", &script])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        install_process_group(&mut command, None, None, None).unwrap();
        let mut child = command.spawn().unwrap();
        let pgid = child.id().unwrap() as i32;
        let mut guard = ProcessGroupDropGuard::new(child.id(), None);
        for _ in 0..200 {
            if ready.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ready.exists(), "residual-group fixture did not start");

        let outcome = terminate_and_reap(&mut child, Some(pgid), "/bin/sh", true, &mut guard).await;
        assert_eq!(outcome.leader, LeaderResolution::Reaped);
        let control_error = outcome.result.unwrap();
        assert!(
            control_error.is_some(),
            "a dead sentinel must surface unavailable group authority"
        );
        assert!(
            process_group_alive(pgid),
            "dead-sentinel escalation signalled an unauthenticated numeric PGID"
        );
        drop(guard);

        signal_group(pgid, SIGKILL);
        assert!(
            wait_for_group_exit(pgid, Duration::from_secs(2)).await,
            "test cleanup could not empty the residual group"
        );
    }

    #[tokio::test]
    async fn guard_drop_after_reap_reports_without_signalling_numeric_group() {
        let dir = tempfile::TempDir::new().unwrap();
        let heartbeat = dir.path().join("reaped-guard-heartbeat");
        let script = format!(
            "( trap '' HUP TERM; while :; do printf x >> '{}'; /bin/sleep 0.02; done ) & while [ ! -s '{}' ]; do /bin/sleep 0.01; done; exit 0",
            heartbeat.display(),
            heartbeat.display()
        );
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", &script])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        install_process_group(&mut command, None, None, None).unwrap();
        let mut child = command.spawn().unwrap();
        let pgid = child.id().unwrap() as i32;
        let (reporter, events) = recording_reporter();
        let mut guard = ProcessGroupDropGuard::new(child.id(), Some(reporter));
        guard.report_started().unwrap();

        let wait_result = child.wait().await;
        guard.leader_wait_returned(wait_result).unwrap();
        let before = std::fs::metadata(&heartbeat).unwrap().len();
        drop(guard);
        tokio::time::sleep(Duration::from_millis(120)).await;
        let after = std::fs::metadata(&heartbeat).unwrap().len();
        assert!(
            after > before,
            "reaped guard Drop signalled its stale numeric PGID"
        );
        let recorded = events.lock().unwrap().clone();
        assert_eq!(recorded.len(), 2);
        assert!(matches!(
            recorded[1],
            HarnessLifecycleEvent::Stopped {
                pid,
                pgid: stopped_pgid,
                group_empty: false,
            } if pid as i32 == pgid && stopped_pgid == pgid
        ));

        signal_group(pgid, SIGKILL);
        assert!(
            wait_for_group_exit(pgid, Duration::from_secs(2)).await,
            "test cleanup could not empty the residual group"
        );
    }

    #[test]
    fn returned_wait_error_revokes_signal_authority_but_keeps_report_identity() {
        let mut guard = ProcessGroupDropGuard::new(Some(2_000_000_000), None);
        let result: std::io::Result<()> =
            guard.leader_wait_returned(Err(std::io::Error::other("synthetic wait failure")));
        let signal_pgid = guard.signal_pgid;
        let report_identity = guard.report_identity.clone();
        // Avoid an accidental real signal even if a future regression makes the
        // assertion below fail before Drop can observe it.
        std::mem::forget(guard);

        assert!(result.is_err());
        assert_eq!(signal_pgid, None);
        assert!(report_identity.is_some());
    }

    #[test]
    fn wait_identity_unknown_forces_passive_drain_and_residual_cleanup() {
        let control = residual_group_control(true, false);
        assert_eq!(control, ResidualGroupControl::Passive);
        assert!(!control.may_signal());

        assert_eq!(
            residual_group_control(false, false),
            ResidualGroupControl::Active
        );
        assert_eq!(
            residual_group_control(false, true),
            ResidualGroupControl::Passive
        );
    }

    #[tokio::test]
    async fn direct_spawn_transient_executable_busy_is_retried() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::TempDir::new().unwrap();
        let program = dir.path().join("busy-script");
        std::fs::write(&program, "#!/bin/sh\nprintf ready").unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
        // Linux returns ETXTBSY while any process holds the executable open for
        // writing. Release it during the bounded retry window.
        let writer = std::fs::OpenOptions::new()
            .write(true)
            .open(&program)
            .unwrap();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(writer);
        });

        let result = run_capture(
            program.to_str().unwrap(),
            &[],
            None,
            &BTreeMap::new(),
            RunControl::new(CancellationToken::new(), Some(Duration::from_secs(2)), None),
        )
        .await
        .unwrap();
        assert!(matches!(result.termination, Termination::Exited(0)));
        assert_eq!(result.stdout, "ready");
    }

    #[tokio::test]
    async fn normal_capture_exit_cleans_closed_stdio_background_before_stopped() {
        let dir = tempfile::TempDir::new().unwrap();
        let heartbeat = dir.path().join("capture-heartbeat");
        let grandchild_pid = dir.path().join("capture-grandchild.pid");
        let args = closed_stdio_background_args(&heartbeat, &grandchild_pid);
        let (reporter, events, stopped_after_cleanup) =
            cleanup_observing_reporter(heartbeat.clone());

        let result = run_capture(
            "/bin/sh",
            &args,
            None,
            &BTreeMap::new(),
            RunControl::new(
                CancellationToken::new(),
                Some(Duration::from_secs(5)),
                Some(reporter),
            ),
        )
        .await
        .unwrap();

        assert!(matches!(result.termination, Termination::Exited(0)));
        assert_eq!(result.stdout, "captured\n");
        assert!(grandchild_pid.exists(), "background pid was never recorded");
        assert!(
            stopped_after_cleanup.load(Ordering::SeqCst),
            "background heartbeat was still advancing when Stopped was reported"
        );
        assert_started_then_stopped(&events.lock().unwrap());
    }

    #[tokio::test]
    async fn run_stream_capture_delivers_partial_line_and_preserves_stdout() {
        let seen = StdArc::new(StdMutex::new(Vec::<String>::new()));
        let callback_seen = StdArc::clone(&seen);
        let args = vec!["-c".to_string(), "printf 'first\\npartial'".to_string()];

        let result = run_stream_capture(
            "/bin/sh",
            &args,
            None,
            &BTreeMap::new(),
            RunControl::new(CancellationToken::new(), Some(Duration::from_secs(5)), None),
            Box::new(move |line| callback_seen.lock().unwrap().push(line.to_string())),
        )
        .await
        .unwrap();

        assert!(matches!(result.termination, Termination::Exited(0)));
        assert_eq!(result.stdout, "first\npartial");
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["first".to_string(), "partial".to_string()]
        );
    }

    #[tokio::test]
    async fn run_stream_capture_reports_started_then_stopped() {
        let (reporter, events) = recording_reporter();
        let args = vec!["-c".to_string(), "printf 'line\\n'".to_string()];

        let result = run_stream_capture(
            "/bin/sh",
            &args,
            None,
            &BTreeMap::new(),
            RunControl::new(
                CancellationToken::new(),
                Some(Duration::from_secs(5)),
                Some(reporter),
            ),
            Box::new(|_| {}),
        )
        .await
        .unwrap();

        assert!(matches!(result.termination, Termination::Exited(0)));
        assert_started_then_stopped(&events.lock().unwrap());
    }

    #[tokio::test]
    async fn normal_stream_exit_cleans_closed_stdio_background_before_stopped() {
        let dir = tempfile::TempDir::new().unwrap();
        let heartbeat = dir.path().join("stream-heartbeat");
        let grandchild_pid = dir.path().join("stream-grandchild.pid");
        let args = closed_stdio_background_args(&heartbeat, &grandchild_pid);
        let (reporter, events, stopped_after_cleanup) =
            cleanup_observing_reporter(heartbeat.clone());
        let seen = StdArc::new(StdMutex::new(Vec::<String>::new()));
        let callback_seen = StdArc::clone(&seen);

        let result = run_stream_capture(
            "/bin/sh",
            &args,
            None,
            &BTreeMap::new(),
            RunControl::new(
                CancellationToken::new(),
                Some(Duration::from_secs(5)),
                Some(reporter),
            ),
            Box::new(move |line| callback_seen.lock().unwrap().push(line.to_string())),
        )
        .await
        .unwrap();

        assert!(matches!(result.termination, Termination::Exited(0)));
        assert_eq!(result.stdout, "captured\n");
        assert_eq!(*seen.lock().unwrap(), vec!["captured".to_string()]);
        assert!(grandchild_pid.exists(), "background pid was never recorded");
        assert!(
            stopped_after_cleanup.load(Ordering::SeqCst),
            "background heartbeat was still advancing when Stopped was reported"
        );
        assert_started_then_stopped(&events.lock().unwrap());
    }

    #[tokio::test]
    async fn dropping_capture_future_reports_stopped() {
        let events = StdArc::new(StdMutex::new(Vec::new()));
        let callback_events = StdArc::clone(&events);
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let reporter = HarnessLifecycleReporter::new(move |event| {
            let started_pid = match &event {
                HarnessLifecycleEvent::Started { pid, .. } => Some(*pid),
                HarnessLifecycleEvent::Stopped { .. } => None,
            };
            callback_events.lock().unwrap().push(event);
            if let Some(pid) = started_pid {
                let _ = started_tx.send(pid);
            }
            Ok(())
        });

        let run = tokio::spawn(async move {
            let args = vec!["-c".to_string(), "exec /bin/sleep 30".to_string()];
            run_capture(
                "/bin/sh",
                &args,
                None,
                &BTreeMap::new(),
                RunControl::new(CancellationToken::new(), None, Some(reporter)),
            )
            .await
        });

        let sentinel_pid = tokio::time::timeout(Duration::from_secs(5), started_rx.recv())
            .await
            .expect("harness child was not reported as started")
            .expect("lifecycle reporter closed before Started");
        run.abort();
        let error = run
            .await
            .expect_err("aborted capture future must not complete");
        assert!(error.is_cancelled());

        assert_started_then_stopped(&events.lock().unwrap());
        assert!(
            wait_for_group_exit(sentinel_pid as i32, Duration::from_secs(2)).await,
            "aborted future left the exact sentinel group alive"
        );
    }

    #[tokio::test]
    async fn failed_started_report_never_releases_cli_start_gate_and_reaps_wrapper() {
        let dir = tempfile::TempDir::new().unwrap();
        let executed = dir.path().join("executed");
        let grandchild_pid = dir.path().join("grandchild.pid");
        let script = format!(
            "printf started > '{}'; ( exec >/dev/null 2>&1; /bin/sleep 30 ) & echo $! > '{}'; wait",
            executed.display(),
            grandchild_pid.display()
        );
        let args = vec!["-c".to_string(), script];

        let events = StdArc::new(StdMutex::new(Vec::new()));
        let callback_events = StdArc::clone(&events);
        let callback_executed = executed.clone();
        let cli_was_gated = StdArc::new(AtomicBool::new(false));
        let callback_cli_was_gated = StdArc::clone(&cli_was_gated);
        let reporter = HarnessLifecycleReporter::new(move |event| {
            let started = matches!(event, HarnessLifecycleEvent::Started { .. });
            callback_events.lock().unwrap().push(event);
            if started {
                std::thread::sleep(Duration::from_millis(150));
                callback_cli_was_gated.store(!callback_executed.exists(), Ordering::SeqCst);
                return Err(VyaneError::new(
                    ErrorKind::Io,
                    "simulated sidecar write failure",
                ));
            }
            Ok(())
        });

        let error = run_capture(
            "/bin/sh",
            &args,
            None,
            &BTreeMap::new(),
            RunControl::new(CancellationToken::new(), None, Some(reporter)),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::SpawnFailed);
        let recorded = events.lock().unwrap().clone();
        assert_started_then_stopped(&recorded);
        let HarnessLifecycleEvent::Started { pid, .. } = recorded[0] else {
            unreachable!();
        };
        let pid = rustix::process::Pid::from_raw(pid as i32).unwrap();
        assert_eq!(
            rustix::process::test_kill_process(pid),
            Err(rustix::io::Errno::SRCH)
        );
        assert!(
            cli_was_gated.load(Ordering::SeqCst),
            "real CLI executed before Started publication completed"
        );
        assert!(!executed.exists());
        assert!(!grandchild_pid.exists());
    }
}
