//! Detached background runs: freeze a dispatch to disk, run it in a re-exec of
//! this binary (no daemon), and manage it afterwards with `vyane task`.
//!
//! Layout of the pieces:
//! - [`store`] — the on-disk contract (`job.json` / `status.json` / `task.log`
//!   / `output.txt`), atomic status writes, listing + orphan interpretation.
//! - [`proc`] — process-group control: `setsid` for the worker, group-kill for
//!   `task cancel`, pid-liveness probe for orphan detection.
//! - [`spawn`] — the parent side of `--detach`: write the job, re-exec the
//!   worker detached into its own group.
//!
//! The worker's run loop and the `task list/status/cancel` command handlers
//! live in [`crate::command`], where they reuse the existing config-resolution
//! and runtime-assembly helpers.

pub mod proc;
pub mod spawn;
pub mod store;

use std::path::PathBuf;

/// The tasks root directory: `<data_dir>/tasks`.
pub fn tasks_root(data_dir: &std::path::Path) -> PathBuf {
    data_dir.join(store::TASKS_DIR)
}
