//! On-disk layout and (de)serialization for detached runs.
//!
//! Each detached run owns a directory under `$VYANE_DATA_DIR/tasks/<id>/`:
//!
//! ```text
//! tasks/<id>/
//!   job.json      the frozen request the worker re-executes (written by parent)
//!   status.json   {schema, run_id, pid, pgid, state, …} (worker, atomic writes)
//!   task.log      combined worker stdout+stderr (worker's redirected fds)
//!   output.txt    the answer text on success (worker, on finalize)
//! ```
//!
//! `status.json` is the single source of truth for a run's lifecycle. It is
//! written atomically (write a sibling tmp file, then `rename(2)` over the
//! target) so a reader never observes a half-written file, and so a crash
//! mid-write cannot corrupt the last good status.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Directory name (under the data dir) holding all detached-run directories.
pub const TASKS_DIR: &str = "tasks";

const JOB_FILE: &str = "job.json";
const STATUS_FILE: &str = "status.json";
const LOG_FILE: &str = "task.log";
const OUTPUT_FILE: &str = "output.txt";

/// The current `status.json` schema version. Bumped only on a breaking change
/// to the status shape; readers can branch on it.
pub const STATUS_SCHEMA: u32 = 1;

/// The lifecycle state of a detached run, as persisted in `status.json`.
///
/// `Running` is written up front; the worker rewrites the file with a terminal
/// state when the dispatch completes. `Died` is **never persisted** — it is a
/// read-side interpretation of "state is `running` but the pid is gone" (see
/// [`crate::task::proc::pid_alive`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    Running,
    Success,
    Error,
    Timeout,
    Cancelled,
    /// Synthetic: an orphaned worker (status still `running`, pid dead). Only
    /// ever produced by read-side interpretation, never written to disk.
    Died,
}

impl TaskState {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskState::Running => "running",
            TaskState::Success => "success",
            TaskState::Error => "error",
            TaskState::Timeout => "timeout",
            TaskState::Cancelled => "cancelled",
            TaskState::Died => "died",
        }
    }

    /// A terminal state is one the worker persisted as its final word; a run in
    /// a terminal state has finished and its process is not expected alive.
    pub fn is_terminal(self) -> bool {
        !matches!(self, TaskState::Running)
    }
}

/// The frozen request a detached worker re-executes. Written once by the parent
/// before it spawns the worker; the worker reads it back and never mutates it.
///
/// The target is stored as the raw selector *string* (profile name or
/// `provider/model`), exactly as typed on the command line, so the worker
/// re-resolves it against config the same way the parent validated it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSpec {
    pub run_id: String,
    pub task: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<PathBuf>,
    pub sandbox: SandboxSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    /// Timeout in whole seconds; `None` means no timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    /// Optional `--config` override the parent was invoked with, so the worker
    /// resolves against the same config file(s).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<PathBuf>,
}

/// Serializable mirror of `vyane_core::Sandbox` (which is `#[non_exhaustive]`
/// and lives in a frozen crate). Kept local so the job spec stays self-owned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxSpec {
    ReadOnly,
    Write,
    Full,
}

impl From<vyane_core::Sandbox> for SandboxSpec {
    fn from(value: vyane_core::Sandbox) -> Self {
        match value {
            vyane_core::Sandbox::ReadOnly => SandboxSpec::ReadOnly,
            vyane_core::Sandbox::Write => SandboxSpec::Write,
            vyane_core::Sandbox::Full => SandboxSpec::Full,
        }
    }
}

impl From<SandboxSpec> for vyane_core::Sandbox {
    fn from(value: SandboxSpec) -> Self {
        match value {
            SandboxSpec::ReadOnly => vyane_core::Sandbox::ReadOnly,
            SandboxSpec::Write => vyane_core::Sandbox::Write,
            SandboxSpec::Full => vyane_core::Sandbox::Full,
        }
    }
}

/// The live status of a detached run, persisted to `status.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusFile {
    pub schema: u32,
    pub run_id: String,
    pub pid: i32,
    pub pgid: i32,
    pub state: TaskState,
    pub started_at: DateTime<Utc>,
    /// The resolved target string (best-effort human label of where it ran).
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// The ledger `run_id` of the completed dispatch (equal to `run_id` here,
    /// but recorded explicitly so the link to the ledger is unambiguous).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ledger_run_id: Option<String>,
    /// Terminal error message, when the run failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl StatusFile {
    /// The initial `running` status written before dispatch begins.
    pub fn running(
        run_id: impl Into<String>,
        pid: i32,
        pgid: i32,
        target: impl Into<String>,
        workdir: Option<String>,
    ) -> Self {
        Self {
            schema: STATUS_SCHEMA,
            run_id: run_id.into(),
            pid,
            pgid,
            state: TaskState::Running,
            started_at: Utc::now(),
            target: target.into(),
            workdir,
            finished_at: None,
            ledger_run_id: None,
            error: None,
        }
    }

    /// Duration between start and finish, if finished.
    pub fn duration_ms(&self) -> Option<i64> {
        self.finished_at
            .map(|end| (end - self.started_at).num_milliseconds())
    }
}

/// Filesystem paths for one detached run's directory.
#[derive(Debug, Clone)]
pub struct TaskPaths {
    pub dir: PathBuf,
}

impl TaskPaths {
    /// Paths for run `id` under the given tasks-root directory.
    pub fn new(tasks_root: &Path, id: &str) -> Self {
        Self {
            dir: tasks_root.join(id),
        }
    }

    pub fn job(&self) -> PathBuf {
        self.dir.join(JOB_FILE)
    }
    pub fn status(&self) -> PathBuf {
        self.dir.join(STATUS_FILE)
    }
    pub fn log(&self) -> PathBuf {
        self.dir.join(LOG_FILE)
    }
    pub fn output(&self) -> PathBuf {
        self.dir.join(OUTPUT_FILE)
    }

    /// Create the run directory (and parents).
    pub fn ensure_dir(&self) -> Result<()> {
        fs::create_dir_all(&self.dir)
            .with_context(|| format!("create task dir {}", self.dir.display()))
    }

    /// Serialize and write the job spec (plain write — parent-only, pre-spawn).
    pub fn write_job(&self, job: &JobSpec) -> Result<()> {
        let text = serde_json::to_string_pretty(job).context("serialize job spec")?;
        fs::write(self.job(), text).with_context(|| format!("write {}", self.job().display()))
    }

    /// Read the job spec back (worker side).
    pub fn read_job(&self) -> Result<JobSpec> {
        let text = fs::read_to_string(self.job())
            .with_context(|| format!("read {}", self.job().display()))?;
        serde_json::from_str(&text).with_context(|| format!("parse {}", self.job().display()))
    }

    /// Atomically write the status file: write a sibling tmp, then rename over
    /// the target. A reader therefore only ever sees a complete status.
    pub fn write_status(&self, status: &StatusFile) -> Result<()> {
        let text = serde_json::to_string_pretty(status).context("serialize status")?;
        atomic_write(&self.status(), text.as_bytes())
    }

    /// Read the status file, if present and parseable.
    pub fn read_status(&self) -> Result<StatusFile> {
        let path = self.status();
        let text = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))
    }

    /// The last `n` lines of the log file, verbatim and in order. A missing or
    /// unreadable log yields an empty vec.
    pub fn tail_log(&self, n: usize) -> Vec<String> {
        let Ok(text) = fs::read_to_string(self.log()) else {
            return Vec::new();
        };
        let lines: Vec<&str> = text.lines().collect();
        let start = lines.len().saturating_sub(n);
        lines[start..].iter().map(|s| s.to_string()).collect()
    }

    /// The captured answer text, if the run wrote one.
    pub fn read_output(&self) -> Option<String> {
        fs::read_to_string(self.output()).ok()
    }
}

/// Write `bytes` to `path` atomically via a temp file + rename in the same
/// directory (rename is atomic within a filesystem). The temp name embeds the
/// pid so concurrent writers never collide on it.
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp = dir.join(format!(
        ".{}.tmp.{}",
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("status"),
        std::process::id()
    ));
    {
        let mut f =
            fs::File::create(&tmp).with_context(|| format!("create temp {}", tmp.display()))?;
        f.write_all(bytes)
            .with_context(|| format!("write temp {}", tmp.display()))?;
        f.sync_all().ok();
    }
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

/// One row of `task list`, with orphan detection already applied.
#[derive(Debug, Clone, Serialize)]
pub struct TaskListRow {
    pub id: String,
    pub state: TaskState,
    pub target: String,
    pub started_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
}

/// Enumerate every task directory under `tasks_root`, read each status,
/// apply orphan detection, and return rows most-recent-first (by `started_at`).
///
/// Directories without a readable/parseable `status.json` are skipped — a task
/// dir the parent created but whose worker has not yet written status is
/// transient and simply not listed until it does.
pub fn list_tasks(tasks_root: &Path, is_alive: impl Fn(i32) -> bool) -> Vec<TaskListRow> {
    let mut rows = Vec::new();
    let Ok(entries) = fs::read_dir(tasks_root) else {
        return rows;
    };
    for entry in entries.flatten() {
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let Some(id) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let paths = TaskPaths::new(tasks_root, &id);
        let Ok(status) = paths.read_status() else {
            continue;
        };
        let state = interpret_state(&status, &is_alive);
        rows.push(TaskListRow {
            id,
            state,
            target: status.target.clone(),
            started_at: status.started_at,
            duration_ms: status.duration_ms(),
        });
    }
    rows.sort_by_key(|row| std::cmp::Reverse(row.started_at));
    rows
}

/// Interpret a persisted status into a *displayed* state: a `running` status
/// whose pid is dead becomes [`TaskState::Died`]. Terminal states pass through
/// unchanged, and the file on disk is never rewritten.
pub fn interpret_state(status: &StatusFile, is_alive: impl Fn(i32) -> bool) -> TaskState {
    if status.state == TaskState::Running && !is_alive(status.pid) {
        TaskState::Died
    } else {
        status.state
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_status(state: TaskState, pid: i32) -> StatusFile {
        let mut s = StatusFile::running("run-1", pid, pid, "test/model (openai_chat)", None);
        s.state = state;
        s
    }

    #[test]
    fn status_roundtrips_through_json() {
        let mut status = sample_status(TaskState::Success, 42);
        status.finished_at = Some(Utc::now());
        status.ledger_run_id = Some("ledger-9".into());
        let text = serde_json::to_string(&status).unwrap();
        let back: StatusFile = serde_json::from_str(&text).unwrap();
        assert_eq!(back.schema, STATUS_SCHEMA);
        assert_eq!(back.run_id, "run-1");
        assert_eq!(back.state, TaskState::Success);
        assert_eq!(back.ledger_run_id.as_deref(), Some("ledger-9"));
    }

    #[test]
    fn state_serializes_lowercase() {
        // Persisted names are the stable wire contract for --json consumers.
        assert_eq!(
            serde_json::to_string(&TaskState::Running).unwrap(),
            "\"running\""
        );
        assert_eq!(
            serde_json::to_string(&TaskState::Cancelled).unwrap(),
            "\"cancelled\""
        );
        assert_eq!(TaskState::Died.as_str(), "died");
    }

    #[test]
    fn terminal_states_classified() {
        assert!(!TaskState::Running.is_terminal());
        for s in [
            TaskState::Success,
            TaskState::Error,
            TaskState::Timeout,
            TaskState::Cancelled,
        ] {
            assert!(s.is_terminal(), "{s:?} should be terminal");
        }
    }

    #[test]
    fn interpret_state_marks_dead_running_as_died() {
        let running = sample_status(TaskState::Running, 7);
        // pid alive → stays running.
        assert_eq!(interpret_state(&running, |_| true), TaskState::Running);
        // pid dead → died (read-side only; the value in `running` is untouched).
        assert_eq!(interpret_state(&running, |_| false), TaskState::Died);
        assert_eq!(running.state, TaskState::Running);
    }

    #[test]
    fn interpret_state_leaves_terminal_untouched() {
        // A finished run is never reinterpreted, even if the pid is long gone.
        let done = sample_status(TaskState::Success, 7);
        assert_eq!(interpret_state(&done, |_| false), TaskState::Success);
    }

    #[test]
    fn atomic_write_leaves_no_temp_and_is_readable() {
        let dir = TempDir::new().unwrap();
        let paths = TaskPaths::new(dir.path(), "abc");
        paths.ensure_dir().unwrap();
        let status = sample_status(TaskState::Running, 123);
        paths.write_status(&status).unwrap();

        let back = paths.read_status().unwrap();
        assert_eq!(back.run_id, "run-1");
        assert_eq!(back.pid, 123);

        // No stray temp files remain in the run dir.
        let leftovers: Vec<_> = fs::read_dir(&paths.dir)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty(), "temp file left behind: {leftovers:?}");
    }

    #[test]
    fn job_spec_roundtrips() {
        let dir = TempDir::new().unwrap();
        let paths = TaskPaths::new(dir.path(), "j1");
        paths.ensure_dir().unwrap();
        let job = JobSpec {
            run_id: "j1".into(),
            task: "do it".into(),
            target: "review".into(),
            workdir: Some(PathBuf::from("/tmp/work")),
            sandbox: SandboxSpec::Write,
            system: Some("be terse".into()),
            timeout_secs: Some(30),
            labels: vec!["k=v".into()],
            session: Some("s1".into()),
            config: None,
        };
        paths.write_job(&job).unwrap();
        let back = paths.read_job().unwrap();
        assert_eq!(back.run_id, "j1");
        assert_eq!(back.target, "review");
        assert_eq!(back.sandbox, SandboxSpec::Write);
        assert_eq!(back.timeout_secs, Some(30));
        assert_eq!(back.labels, vec!["k=v".to_string()]);
    }

    #[test]
    fn tail_log_returns_last_n_lines() {
        let dir = TempDir::new().unwrap();
        let paths = TaskPaths::new(dir.path(), "log1");
        paths.ensure_dir().unwrap();
        fs::write(paths.log(), "l1\nl2\nl3\nl4\nl5\n").unwrap();
        assert_eq!(paths.tail_log(2), vec!["l4".to_string(), "l5".to_string()]);
        // Asking for more than exist yields all of them.
        assert_eq!(paths.tail_log(100).len(), 5);
        // Missing log → empty.
        let missing = TaskPaths::new(dir.path(), "nope");
        assert!(missing.tail_log(10).is_empty());
    }

    #[test]
    fn list_tasks_orders_recent_first_and_skips_unwritten() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();

        // Two runs with distinct start times.
        for (id, secs) in [("old", 0), ("new", 10)] {
            let paths = TaskPaths::new(root, id);
            paths.ensure_dir().unwrap();
            let mut s = sample_status(TaskState::Success, 1);
            s.run_id = id.to_string();
            s.started_at = DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap();
            s.finished_at = Some(s.started_at);
            paths.write_status(&s).unwrap();
        }
        // A dir with no status.json must be silently skipped.
        fs::create_dir_all(root.join("pending")).unwrap();

        let rows = list_tasks(root, |_| true);
        assert_eq!(rows.len(), 2, "pending (no status) dir must be skipped");
        assert_eq!(rows[0].id, "new", "most-recent-first ordering");
        assert_eq!(rows[1].id, "old");
    }

    #[test]
    fn list_tasks_applies_orphan_detection() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        let paths = TaskPaths::new(root, "orphan");
        paths.ensure_dir().unwrap();
        paths
            .write_status(&sample_status(TaskState::Running, 99))
            .unwrap();

        // Dead pid → the row reads `died` without the file being rewritten.
        let rows = list_tasks(root, |_| false);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, TaskState::Died);
        assert_eq!(paths.read_status().unwrap().state, TaskState::Running);
    }
}
