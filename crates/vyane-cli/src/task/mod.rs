//! Detached background runs: hand a frozen dispatch directly to a re-exec of
//! this binary (no daemon), and manage it afterwards with `vyane task`.
//!
//! Layout of the pieces:
//! - [`store`] — the stdin envelope, mode-0600 artifacts, and read-only legacy
//!   `job.json` / `status.json` compatibility.
//! - [`proc`] — process-group control: `setsid` for the worker, group-kill for
//!   `task cancel`, exact process-birth identity, and orphan detection.
//! - [`spawn`] — the parent side of `--detach`: re-exec the worker in its own
//!   group and deliver the private request once over piped stdin.
//!
//! The worker's run loop and the `task list/status/cancel` command handlers
//! live in [`crate::command`], where they reuse the existing config-resolution
//! and runtime-assembly helpers.

pub mod proc;
pub mod spawn;
pub mod store;

use std::path::PathBuf;

use vyane_task::{TaskKind, TaskOrigin, TaskRecord};

/// Owner used by the local CLI and loopback REST frontends.
pub(crate) const LOCAL_TASK_OWNER: &str = "local";

/// Return whether a durable row belongs to one local dispatch frontend.
///
/// `origin` alone is not an ownership boundary: workflow rows deliberately
/// share this database and may reuse an existing submission origin. Keep the
/// full owner/kind/origin predicate in one place so a frontend never lists,
/// recovers, attaches, or cancels another task class by accident.
pub(crate) fn is_local_dispatch(record: &TaskRecord, origin: TaskOrigin) -> bool {
    record.matches_scope(LOCAL_TASK_OWNER, TaskKind::Dispatch, origin)
}

/// The tasks root directory: `<data_dir>/tasks`.
pub fn tasks_root(data_dir: &std::path::Path) -> PathBuf {
    data_dir.join(store::TASKS_DIR)
}
