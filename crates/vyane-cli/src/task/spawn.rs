//! Parent side of `--detach`: freeze the job, then re-exec this binary as the
//! hidden `__worker <id>` subcommand, detached into its own process group with
//! its output redirected to the run's `task.log`.
//!
//! No daemon is involved: the "worker" is just another invocation of the
//! `vyane` executable. Detachment relies on the same `setsid(2)` process-group
//! trick the harness uses, so the worker survives the parent exiting and can be
//! group-signalled later by `task cancel`.

use std::fs::File;
use std::process::{Command, Stdio};

use anyhow::{Context, Result};

use super::proc::install_process_group;
use super::store::{JobSpec, TaskPaths};

/// The hidden subcommand name the worker runs under.
pub const WORKER_SUBCOMMAND: &str = "__worker";

/// Freeze `job` into `paths`, spawn the detached worker, and return its pid.
///
/// Steps, in order:
/// 1. create the run directory and write `job.json` (the frozen request),
/// 2. open `task.log` for the worker's combined stdout+stderr,
/// 3. re-exec this binary as `__worker <id>` in a fresh process group,
/// 4. return immediately — the parent does **not** wait.
///
/// The worker itself writes the initial `status.json`; the parent only lays
/// down the job and starts the process, so there is a single writer of status.
pub fn spawn_detached(paths: &TaskPaths, job: &JobSpec) -> Result<u32> {
    paths.ensure_dir()?;
    paths.write_job(job)?;

    let log = File::create(paths.log())
        .with_context(|| format!("create log {}", paths.log().display()))?;
    let log_err = log
        .try_clone()
        .context("clone log handle for stderr redirect")?;

    let exe = std::env::current_exe().context("resolve current executable for worker re-exec")?;

    let mut cmd = Command::new(exe);
    cmd.arg(WORKER_SUBCOMMAND).arg(&job.run_id);
    // The worker is headless: no stdin, and stdout+stderr both land in the log.
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::from(log));
    cmd.stderr(Stdio::from(log_err));

    // The worker resolves storage from VYANE_DATA_DIR exactly as the parent
    // did; the child inherits the environment, which for a re-exec of our own
    // binary is intended (unlike a foreign harness, there are no third-party
    // credentials to scrub here — the worker re-resolves config/secrets itself).
    install_process_group(&mut cmd);

    let child = cmd
        .spawn()
        .with_context(|| format!("spawn detached worker for {}", job.run_id))?;
    Ok(child.id())
}
