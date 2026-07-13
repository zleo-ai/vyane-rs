//! Parent side of `--detach`: re-exec this binary as the hidden `__worker <id>`
//! subcommand, send its one-shot request over piped stdin, and detach it into its
//! own process group with output redirected to the run's `task.log`.
//!
//! No daemon is involved: the "worker" is just another invocation of the
//! `vyane` executable. A safe process wrapper creates a new POSIX session and
//! process group, so the worker survives the parent exiting and can be
//! group-signalled later by `task cancel`.

use std::fs::{self, File};
use std::io::Write as _;
use std::path::Path;
use std::process::{Child, Command, Stdio};

use anyhow::{Context, Result};
#[cfg(target_os = "linux")]
use command_fds::{CommandFdExt as _, FdMapping};
use vyane_core::{PinnedWorkdir, WorkdirIdentity};

#[cfg(target_os = "linux")]
use super::proc::WORKER_PINNED_WORKDIR_FD;
#[cfg(unix)]
use super::proc::{SIGKILL, signal_group};
use super::proc::{process_birth_fingerprint, spawn_in_session};
use super::store::{TaskPaths, WorkerEnvelope};

/// The hidden subcommand name the worker runs under.
pub const WORKER_SUBCOMMAND: &str = "__worker";

#[cfg(target_os = "linux")]
fn worker_pin_mapping(pinned: &PinnedWorkdir) -> Result<FdMapping> {
    let duplicate = pinned
        .handle()
        .try_clone()
        .context("duplicate pinned workdir for detached worker")?;
    Ok(FdMapping {
        parent_fd: duplicate.into(),
        child_fd: WORKER_PINNED_WORKDIR_FD,
    })
}

/// Exact process evidence retained when a worker was spawned but its private
/// stdin handoff failed. The caller can use this to settle a controller that
/// the same child managed to attach before the pipe broke, without borrowing a
/// later executor's epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnedWorker {
    pub pid: i32,
    pub pgid: i32,
    pub birth_fingerprint: Option<String>,
}

/// A detached spawn failure, optionally carrying evidence for a child that was
/// already created, killed, and reaped by this module.
#[derive(Debug)]
pub struct SpawnDetachedError {
    error: anyhow::Error,
    spawned: Option<SpawnedWorker>,
}

impl SpawnDetachedError {
    pub fn spawned(&self) -> Option<&SpawnedWorker> {
        self.spawned.as_ref()
    }

    fn before_spawn(error: anyhow::Error) -> Self {
        Self {
            error,
            spawned: None,
        }
    }

    fn after_spawn(error: anyhow::Error, spawned: SpawnedWorker) -> Self {
        Self {
            error,
            spawned: Some(spawned),
        }
    }
}

impl std::fmt::Display for SpawnDetachedError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:#}", self.error)
    }
}

impl std::error::Error for SpawnDetachedError {}

/// Spawn the detached worker, hand it `envelope` once over stdin, and return its
/// pid. No request body is written into the task directory.
///
/// Steps, in order:
/// 1. serialize the envelope in memory and create the run directory,
/// 2. open `task.log` for the worker's combined stdout+stderr,
/// 3. re-exec this binary as `__worker <id>` with piped stdin in a fresh group,
/// 4. write the JSON envelope, close the pipe, and return without waiting.
///
/// Lifecycle metadata is owned by the shared SQLite task store. If the child
/// spawned but the private stdin handoff fails, this function kills and reaps
/// the worker group before returning the error to the caller for settlement.
pub fn spawn_detached(
    paths: &TaskPaths,
    envelope: &WorkerEnvelope,
    pinned_workdir: Option<&PinnedWorkdir>,
) -> std::result::Result<u32, SpawnDetachedError> {
    let exe = std::env::current_exe()
        .context("resolve current executable for worker re-exec")
        .map_err(SpawnDetachedError::before_spawn)?;
    spawn_detached_with_executable(paths, envelope, &exe, pinned_workdir)
}

fn spawn_detached_with_executable(
    paths: &TaskPaths,
    envelope: &WorkerEnvelope,
    executable: &Path,
    pinned_workdir: Option<&PinnedWorkdir>,
) -> std::result::Result<u32, SpawnDetachedError> {
    let payload = serde_json::to_vec(envelope)
        .context("serialize worker stdin envelope")
        .map_err(SpawnDetachedError::before_spawn)?;
    paths
        .ensure_dir()
        .map_err(SpawnDetachedError::before_spawn)?;

    let log = open_private_log(&paths.log()).map_err(SpawnDetachedError::before_spawn)?;
    let log_err = log
        .try_clone()
        .context("clone log handle for stderr redirect")
        .map_err(SpawnDetachedError::before_spawn)?;

    #[cfg(not(target_os = "linux"))]
    if pinned_workdir.is_some() {
        return Err(SpawnDetachedError::before_spawn(anyhow::anyhow!(
            "detached pinned workdirs are supported only on Linux"
        )));
    }

    let mut cmd = Command::new(executable);
    cmd.arg(WORKER_SUBCOMMAND).arg(&envelope.job.run_id);
    // The request is carried only by this one-shot pipe. stdout+stderr both land
    // in the log; no prompt-bearing value is placed in argv or the environment.
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::from(log));
    cmd.stderr(Stdio::from(log_err));

    // The worker resolves storage from VYANE_DATA_DIR exactly as the parent
    // did; the child inherits the environment, which for a re-exec of our own
    // binary is intended (unlike a foreign harness, there are no third-party
    // credentials to scrub here — the worker re-resolves config/secrets itself).
    #[cfg(target_os = "linux")]
    if let Some(pinned) = pinned_workdir {
        let mapping = worker_pin_mapping(pinned).map_err(SpawnDetachedError::before_spawn)?;
        cmd.fd_mappings(vec![mapping])
            .context("map pinned workdir into detached worker")
            .map_err(SpawnDetachedError::before_spawn)?;
    }

    let mut child = spawn_in_session(cmd)
        .with_context(|| format!("spawn detached worker for {}", envelope.job.run_id))
        .map_err(SpawnDetachedError::before_spawn)?;
    let pid = child.id() as i32;
    let spawned = SpawnedWorker {
        pid,
        // `setsid` makes the worker its own group leader before exec.
        pgid: pid,
        birth_fingerprint: process_birth_fingerprint(pid),
    };

    let handoff = (|| -> Result<()> {
        let mut stdin = child
            .stdin
            .take()
            .context("detached worker stdin pipe was not created")?;
        stdin
            .write_all(&payload)
            .context("write worker envelope to stdin")?;
        stdin.flush().context("flush worker envelope to stdin")?;
        // Dropping stdin delivers EOF, which is the worker's completeness marker.
        drop(stdin);
        Ok(())
    })();

    if let Err(error) = handoff {
        terminate_failed_worker(&mut child, pid);
        return Err(SpawnDetachedError::after_spawn(
            error.context("worker stdin handoff failed"),
            spawned,
        ));
    }

    Ok(pid as u32)
}

/// Take ownership of the fixed descriptor inherited by a detached worker and
/// rebuild the exact parent-side pin without reopening its pathname.
#[cfg(target_os = "linux")]
pub fn take_inherited_pinned_workdir(
    canonical_path: &Path,
    expected_identity: &WorkdirIdentity,
) -> Result<PinnedWorkdir> {
    // Re-open the inherited descriptor through procfs to obtain a normal
    // close-on-exec `File` owner without constructing one from a raw integer.
    // The pinned nix dependency's safe close wrapper consumes the
    // protocol-owned fixed descriptor so harness grandchildren cannot inherit
    // it.
    let file = File::open(format!("/proc/self/fd/{WORKER_PINNED_WORKDIR_FD}"))
        .context("required inherited workdir descriptor is unavailable")?;
    nix::unistd::close(WORKER_PINNED_WORKDIR_FD)
        .context("close inherited workdir transfer descriptor")?;
    PinnedWorkdir::from_open_file(canonical_path.to_path_buf(), file, expected_identity)
        .map_err(anyhow::Error::from)
}

#[cfg(not(target_os = "linux"))]
pub fn take_inherited_pinned_workdir(
    _canonical_path: &Path,
    _expected_identity: &WorkdirIdentity,
) -> Result<PinnedWorkdir> {
    anyhow::bail!("inherited pinned workdirs are supported only on Linux")
}

fn terminate_failed_worker(child: &mut Child, pid: i32) {
    #[cfg(unix)]
    signal_group(pid, SIGKILL);
    // Direct-child kill is the non-Unix fallback and harmless after a Unix group
    // kill. Waiting reaps the failed handoff worker instead of leaving a zombie.
    let _ = child.kill();
    let _ = child.wait();
}

fn open_private_log(path: &Path) -> Result<File> {
    let mut options = fs::OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
        options.mode(0o600);
        let file = options
            .open(path)
            .with_context(|| format!("create private log {}", path.display()))?;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .with_context(|| format!("chmod private log {}", path.display()))?;
        Ok(file)
    }
    #[cfg(not(unix))]
    {
        options
            .open(path)
            .with_context(|| format!("create private log {}", path.display()))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;
    use crate::task::store::{JobSpec, SandboxSpec};

    #[cfg(target_os = "linux")]
    fn count_parent_fds_for_identity(identity: &WorkdirIdentity) -> usize {
        use std::os::unix::fs::MetadataExt as _;

        fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(|entry| fs::metadata(entry.ok()?.path()).ok())
            .filter(|metadata| {
                metadata.dev() == identity.device && metadata.ino() == identity.inode
            })
            .count()
    }

    #[cfg(unix)]
    #[test]
    fn broken_stdin_handoff_kills_worker_without_writing_legacy_status() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = TempDir::new().unwrap();
        let paths = TaskPaths::new(dir.path(), "handoff-broken");
        let executable = dir.path().join("close-stdin-worker.sh");
        fs::write(
            &executable,
            "#!/bin/sh\necho $$ > \"$(dirname \"$0\")/worker.pid\"\nexec 0<&-\nexec sleep 30\n",
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        #[cfg(target_os = "linux")]
        let (pinned, matching_fds_before) = {
            let workdir = dir.path().join("pinned-workdir");
            fs::create_dir(&workdir).unwrap();
            let pinned = PinnedWorkdir::open(&workdir).unwrap();
            let matching_fds = count_parent_fds_for_identity(pinned.identity());
            (pinned, matching_fds)
        };

        // Larger than any normal pipe buffer: write_all must still be writing
        // when the child closes its read end, making the failure deterministic.
        let envelope = WorkerEnvelope::new(JobSpec {
            run_id: "handoff-broken".into(),
            task: "x".repeat(4 * 1024 * 1024),
            target: "review".into(),
            workdir: None,
            sandbox: SandboxSpec::ReadOnly,
            system: None,
            timeout_secs: None,
            labels: Vec::new(),
            session: None,
            config: None,
            target_snapshot: Vec::new(),
            capability_plan: None,
        });

        #[cfg(target_os = "linux")]
        let pinned_arg = Some(&pinned);
        #[cfg(not(target_os = "linux"))]
        let pinned_arg = None;
        let error = spawn_detached_with_executable(&paths, &envelope, &executable, pinned_arg)
            .expect_err("closed stdin must fail the private handoff");
        assert!(error.to_string().contains("worker stdin handoff failed"));

        let worker_pid: i32 = fs::read_to_string(dir.path().join("worker.pid"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let evidence = error
            .spawned()
            .expect("post-spawn EPIPE must retain exact controller evidence");
        assert_eq!(evidence.pid, worker_pid);
        assert_eq!(evidence.pgid, worker_pid);
        #[cfg(target_os = "linux")]
        assert!(
            evidence.birth_fingerprint.is_some(),
            "a live Linux child must have a boot-id/start-ticks fingerprint"
        );
        assert!(
            !crate::task::proc::pid_alive(worker_pid),
            "failed handoff worker must be killed and reaped"
        );
        assert!(
            !paths.status().exists(),
            "SQLite-owning caller, not spawn transport, records failure"
        );
        assert!(
            !paths.job().exists(),
            "a failed new handoff must not create job.json"
        );
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(paths.log()).unwrap().permissions().mode() & 0o777,
            0o600,
            "task log must stay private"
        );
        assert!(error.to_string().contains("worker stdin handoff failed"));
        #[cfg(target_os = "linux")]
        assert_eq!(
            count_parent_fds_for_identity(pinned.identity()),
            matching_fds_before,
            "failed worker handoff must not leak the high duplicate or fd 7 in the parent"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn inherited_pin_keeps_original_identity_after_rmdir_and_recreate() {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        use std::time::{Duration, Instant};

        let dir = TempDir::new().unwrap();
        let requested = dir.path().join("work");
        fs::create_dir(&requested).unwrap();
        let pinned = PinnedWorkdir::open(&requested).unwrap();
        let expected = pinned.identity().clone();
        fs::remove_dir(&requested).unwrap();
        fs::create_dir(&requested).unwrap();
        let replacement = fs::metadata(&requested).unwrap();
        assert_ne!(
            (replacement.dev(), replacement.ino()),
            (expected.device, expected.inode),
            "the open directory cannot release its inode for reuse"
        );

        let observed = dir.path().join("observed.txt");
        let executable = dir.path().join("inspect-worker-pin.sh");
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nset -eu\nstat -Lc '%d:%i' /proc/self/fd/7 > '{}.tmp'\nmv '{}.tmp' '{}'\ncat >/dev/null\n",
                observed.display(),
                observed.display(),
                observed.display(),
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();

        let paths = TaskPaths::new(dir.path(), "pin-inherit");
        let envelope = WorkerEnvelope::new(JobSpec {
            run_id: "pin-inherit".into(),
            task: "x".into(),
            target: "review".into(),
            workdir: Some(requested),
            sandbox: SandboxSpec::Write,
            system: None,
            timeout_secs: None,
            labels: Vec::new(),
            session: None,
            config: None,
            target_snapshot: Vec::new(),
            capability_plan: None,
        });
        spawn_detached_with_executable(&paths, &envelope, &executable, Some(&pinned)).unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        while !observed.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let value = fs::read_to_string(&observed).unwrap();
        assert_eq!(
            value.trim(),
            format!("{}:{}", expected.device, expected.inode)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn worker_pin_mapping_owns_and_closes_its_exact_duplicate() {
        let dir = TempDir::new().unwrap();
        let workdir = dir.path().join("work");
        fs::create_dir(&workdir).unwrap();
        let pinned = PinnedWorkdir::open(&workdir).unwrap();
        let before = count_parent_fds_for_identity(pinned.identity());
        let mapping = worker_pin_mapping(&pinned).unwrap();
        assert_eq!(count_parent_fds_for_identity(pinned.identity()), before + 1);
        drop(mapping);
        assert_eq!(count_parent_fds_for_identity(pinned.identity()), before);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn spawn_failure_returns_before_creating_a_worker() {
        let dir = TempDir::new().unwrap();
        let workdir = dir.path().join("work");
        fs::create_dir(&workdir).unwrap();
        let pinned = PinnedWorkdir::open(&workdir).unwrap();
        let paths = TaskPaths::new(dir.path(), "spawn-failure-pin");
        let envelope = WorkerEnvelope::new(JobSpec {
            run_id: "spawn-failure-pin".into(),
            task: "x".into(),
            target: "review".into(),
            workdir: Some(workdir),
            sandbox: SandboxSpec::Write,
            system: None,
            timeout_secs: None,
            labels: Vec::new(),
            session: None,
            config: None,
            target_snapshot: Vec::new(),
            capability_plan: None,
        });

        let error = spawn_detached_with_executable(
            &paths,
            &envelope,
            &dir.path().join("missing-worker"),
            Some(&pinned),
        )
        .unwrap_err();
        assert!(error.spawned().is_none());
    }
}
