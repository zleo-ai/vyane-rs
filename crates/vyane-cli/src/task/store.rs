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

use super::proc::IdentityCheck;

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
/// state when the dispatch completes. `Died` and `Stale` are **never
/// persisted** — they are read-side interpretations:
/// - `Died`: state is `running` but the recorded process is gone or has been
///   reused (see [`crate::task::proc::verify_identity`]).
/// - `Stale`: the task dir has a `job.json` but no `status.json` at all — the
///   worker never published state (a spawn likely failed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    Running,
    Success,
    Error,
    Timeout,
    Cancelled,
    /// Synthetic: an orphaned worker (status still `running`, but its recorded
    /// process is dead or was reused). Only ever produced by read-side
    /// interpretation, never written to disk.
    Died,
    /// Synthetic: a task dir with `job.json` but no `status.json` — the worker
    /// never wrote status (spawn may have failed). Read-side only.
    Stale,
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
            TaskState::Stale => "stale",
        }
    }

    /// A terminal state is one the worker persisted as its final word; a run in
    /// a terminal state has finished and its process is not expected alive.
    /// `Stale` is *not* terminal in the persisted sense (nothing was persisted),
    /// but it is not `Running` either — it is treated as not-running so callers
    /// never wait on or signal it.
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

    /// The `job.json` modification time as a UTC timestamp, if the file exists.
    /// Used as the best-effort `started_at` for a *stale* row (a task dir whose
    /// worker never wrote status): the parent writes `job.json` immediately
    /// before spawning, so its mtime is roughly when the run was requested.
    pub fn job_mtime(&self) -> Option<DateTime<Utc>> {
        let modified = fs::metadata(self.job()).ok()?.modified().ok()?;
        Some(DateTime::<Utc>::from(modified))
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

/// A probe that answers "is the process this status recorded still the same
/// worker?" — given the recorded `(pid, pgid, started_at)`, it returns an
/// [`IdentityCheck`]. Read-side orphan detection uses it so a `running` status
/// is only trusted when the recorded pid still belongs to *its* worker (not a
/// reused pid), matching the guard the canceller applies before signalling.
///
/// The production probe is [`crate::task::proc::verify_identity`]; tests inject
/// a closure.
pub type IdentityProbe<'a> = dyn Fn(i32, i32, DateTime<Utc>) -> IdentityCheck + 'a;

/// Enumerate every task directory under `tasks_root`, read each status,
/// apply orphan detection, and return rows most-recent-first (by `started_at`).
///
/// A directory whose `status.json` is missing/unreadable but which *does* carry
/// a `job.json` surfaces as a [`TaskState::Stale`] row (the worker never wrote
/// status — probably a failed spawn), with its `started_at` taken from the
/// `job.json` mtime. A directory with neither file is transient scaffolding and
/// is skipped.
pub fn list_tasks(tasks_root: &Path, identity: &IdentityProbe<'_>) -> Vec<TaskListRow> {
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
        match paths.read_status() {
            Ok(status) => {
                let state = interpret_state(&status, identity);
                rows.push(TaskListRow {
                    id,
                    state,
                    target: status.target.clone(),
                    started_at: status.started_at,
                    duration_ms: status.duration_ms(),
                });
            }
            Err(_) => {
                // No readable status. If a job.json exists, the worker never
                // published state → show it as `stale` rather than hiding it.
                if let Some(started_at) = paths.job_mtime() {
                    rows.push(TaskListRow {
                        id,
                        state: TaskState::Stale,
                        target: "-".to_string(),
                        started_at,
                        duration_ms: None,
                    });
                }
                // Neither status nor job → transient scaffolding, skip.
            }
        }
    }
    rows.sort_by_key(|row| std::cmp::Reverse(row.started_at));
    rows
}

/// Interpret a persisted status into a *displayed* state. A `running` status is
/// only trusted when its recorded process still validates as *its own worker*
/// via `identity`: a dead pid, or a live pid whose group/start-time no longer
/// match (a reused pid), both surface as [`TaskState::Died`]. Terminal states
/// pass through unchanged, and the file on disk is never rewritten.
pub fn interpret_state(status: &StatusFile, identity: &IdentityProbe<'_>) -> TaskState {
    if status.state != TaskState::Running {
        return status.state;
    }
    match identity(status.pid, status.pgid, status.started_at) {
        IdentityCheck::Match => TaskState::Running,
        // Dead pid, or a reused pid that is no longer our worker: the worker is
        // gone without finalizing → died.
        IdentityCheck::Dead | IdentityCheck::Mismatch(_) => TaskState::Died,
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

    /// Identity probe stub that reports every recorded process as its own live
    /// worker — the "nothing has been reused, everything is alive" baseline.
    fn identity_all_match(_pid: i32, _pgid: i32, _started: DateTime<Utc>) -> IdentityCheck {
        IdentityCheck::Match
    }

    /// Identity probe stub that reports every recorded process as gone.
    fn identity_all_dead(_pid: i32, _pgid: i32, _started: DateTime<Utc>) -> IdentityCheck {
        IdentityCheck::Dead
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
        // identity match → stays running.
        assert_eq!(
            interpret_state(&running, &identity_all_match),
            TaskState::Running
        );
        // pid dead → died (read-side only; the value in `running` is untouched).
        assert_eq!(
            interpret_state(&running, &identity_all_dead),
            TaskState::Died
        );
        assert_eq!(running.state, TaskState::Running);
    }

    #[test]
    fn interpret_state_marks_reused_pid_running_as_died() {
        // A live pid whose identity no longer matches (pid reuse) must read as
        // died, exactly like a dead pid — a still-`running` status over a reused
        // pid is an orphan, not a live run.
        let running = sample_status(TaskState::Running, 7);
        let mismatch =
            |_: i32, _: i32, _: DateTime<Utc>| IdentityCheck::Mismatch("process group mismatch");
        assert_eq!(interpret_state(&running, &mismatch), TaskState::Died);
        assert_eq!(running.state, TaskState::Running);
    }

    #[test]
    fn interpret_state_leaves_terminal_untouched() {
        // A finished run is never reinterpreted, even if the pid is long gone.
        let done = sample_status(TaskState::Success, 7);
        assert_eq!(
            interpret_state(&done, &identity_all_dead),
            TaskState::Success
        );
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
        // A dir with neither status.json nor job.json is transient scaffolding
        // and must be silently skipped.
        fs::create_dir_all(root.join("pending")).unwrap();

        let rows = list_tasks(root, &identity_all_match);
        assert_eq!(
            rows.len(),
            2,
            "empty (no status, no job) dir must be skipped"
        );
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
        let rows = list_tasks(root, &identity_all_dead);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, TaskState::Died);
        assert_eq!(paths.read_status().unwrap().state, TaskState::Running);
    }

    #[test]
    fn list_tasks_surfaces_job_without_status_as_stale() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();

        // A task dir with a job.json but no status.json: the parent laid the job
        // down but the worker never wrote status (spawn likely failed).
        let stale = TaskPaths::new(root, "stalerun");
        stale.ensure_dir().unwrap();
        let job = JobSpec {
            run_id: "stalerun".into(),
            task: "never ran".into(),
            target: "review".into(),
            workdir: None,
            sandbox: SandboxSpec::ReadOnly,
            system: None,
            timeout_secs: None,
            labels: vec![],
            session: None,
            config: None,
        };
        stale.write_job(&job).unwrap();

        // Also a healthy finished run, to prove stale rows coexist and sort.
        let done = TaskPaths::new(root, "donerun");
        done.ensure_dir().unwrap();
        let mut s = sample_status(TaskState::Success, 1);
        s.run_id = "donerun".into();
        done.write_status(&s).unwrap();

        let rows = list_tasks(root, &identity_all_match);
        assert_eq!(rows.len(), 2, "both stale and done rows must appear");
        let stale_row = rows.iter().find(|r| r.id == "stalerun").expect("stale row");
        assert_eq!(stale_row.state, TaskState::Stale);
        // Its started_at comes from the job.json mtime (a real recent time).
        assert!(stale_row.started_at <= Utc::now());
    }

    #[test]
    fn stale_state_serializes_and_names() {
        assert_eq!(TaskState::Stale.as_str(), "stale");
        assert_eq!(
            serde_json::to_string(&TaskState::Stale).unwrap(),
            "\"stale\""
        );
        // Stale is treated as not-running (so callers never wait on / signal it).
        assert!(TaskState::Stale.is_terminal());
    }
}
