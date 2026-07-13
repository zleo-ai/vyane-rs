//! On-disk layout and (de)serialization for detached runs.
//!
//! Each detached run owns a directory under `$VYANE_DATA_DIR/tasks/<id>/`:
//!
//! ```text
//! tasks/<id>/
//!   task.log      combined worker stdout+stderr (worker's redirected fds)
//!   output.txt    the answer text on success (worker, on finalize)
//!   harness-controller.json  ephemeral exact identity of a nested CLI harness
//! ```
//!
//! New submissions carry the frozen request once over the worker's piped stdin
//! and store lifecycle metadata only in `tasks.sqlite3`; they create neither
//! `job.json` nor `status.json`. The file models and atomic writers below are a
//! read/execute compatibility layer for task directories created before the
//! SQLite migration and are never a second truth source for a new task.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use vyane_config::ResolvedConfig;
use vyane_core::{AdapterTransport, AuthStyle, Effort, GenParams, Target};
use vyane_kernel::CapabilityPlanSnapshot;

use super::proc::IdentityCheck;

/// Directory name (under the data dir) holding all detached-run directories.
pub const TASKS_DIR: &str = "tasks";

const JOB_FILE: &str = "job.json";
const STATUS_FILE: &str = "status.json";
const LOG_FILE: &str = "task.log";
const OUTPUT_FILE: &str = "output.txt";
const HARNESS_CONTROLLER_FILE: &str = "harness-controller.json";
const HARNESS_CONTROLLER_LOCK_FILE: &str = ".harness-controller.lock";
/// Sidecar operations are part of cancellation and lifecycle publication, so a
/// stopped or wedged peer must not make them wait forever.
const HARNESS_CONTROLLER_LOCK_WAIT: std::time::Duration = std::time::Duration::from_millis(500);
const HARNESS_CONTROLLER_LOCK_POLL: std::time::Duration = std::time::Duration::from_millis(10);

/// The current `status.json` schema version. Bumped only on a breaking change
/// to the status shape; readers can branch on it.
pub const STATUS_SCHEMA: u32 = 1;
/// Version of the one-shot parent-to-worker stdin envelope.
pub const WORKER_ENVELOPE_SCHEMA: u32 = 1;
/// Private runtime sidecar schema for the currently active nested CLI harness.
pub const HARNESS_CONTROLLER_SCHEMA: u32 = 1;

/// Exact process identity of a nested CLI harness group. This is operational
/// metadata only: it contains no request, output, credential, or raw error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessControllerFile {
    pub schema: u32,
    pub pid: i32,
    pub pgid: i32,
    pub started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub birth_fingerprint: Option<String>,
}

/// The lifecycle state of a detached run, as persisted in `status.json`.
///
/// `Running` is written up front; the worker rewrites the file with a terminal
/// state when the dispatch completes. `Died` and `Stale` are **never
/// persisted** — they are read-side interpretations:
/// - `Died`: state is `running` but the recorded process is gone or has been
///   reused (see [`crate::task::proc::verify_identity`]).
/// - `Stale`: the task dir exists but has no readable `status.json` — the worker
///   never published state (a spawn or stdin handoff likely failed).
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
    /// Synthetic: a task dir with no readable status — the worker never wrote
    /// state (spawn or stdin handoff may have failed). Read-side only.
    Stale,
}

impl TaskState {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskState::Running => "running",
            TaskState::Success => "succeeded",
            TaskState::Error => "failed",
            TaskState::Timeout => "timed_out",
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

/// The frozen request a detached worker re-executes. New parents wrap it in a
/// [`WorkerEnvelope`] and send it once over piped stdin; legacy task directories
/// may still contain this same shape in `job.json`.
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
    /// Secret-free snapshot of the exact chain approved by the parent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_snapshot: Vec<TargetSnapshot>,
    /// Parent-side capability admission frozen before any task-store write or
    /// worker spawn. The worker re-admits independently and compares this
    /// evidence; no process-local directory handle is serialized here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_plan: Option<CapabilityPlanSnapshot>,
}

/// One-shot request transport from a detached parent to its worker.
///
/// This value is serialized directly into the child's piped stdin and consumed
/// once. It is never persisted by the new submission path and is deliberately
/// versioned independently from the legacy `job.json` shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerEnvelope {
    pub schema: u32,
    pub job: JobSpec,
}

impl WorkerEnvelope {
    pub fn new(job: JobSpec) -> Self {
        Self {
            schema: WORKER_ENVELOPE_SCHEMA,
            job,
        }
    }
}

/// Persistable identity of one failover leg. Endpoints and credentials are
/// deliberately excluded, while every routing-relevant target field and
/// generation parameter is retained for drift detection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TargetSnapshot {
    pub target: Target,
    pub transport: AdapterTransport,
    pub params: GenParamsSnapshot,
    /// Hash of the resolved base URL. Hashing catches endpoint drift without
    /// persisting a URL that may itself contain sensitive query material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_digest: Option<String>,
    /// Credential presentation is safe metadata; the credential value is not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_style: Option<AuthStyle>,
    /// Digest of the harness environment-policy shape (inherit mode,
    /// allow-list, and injected variable names). Injected values are excluded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_policy_digest: Option<String>,
}

/// Generation parameters safe to persist in a detached target snapshot.
/// Provider-specific `extra` values are represented only by a digest because
/// arbitrary passthrough JSON can contain sensitive custom fields.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GenParamsSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<Effort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra_digest: Option<String>,
}

impl From<&GenParams> for GenParamsSnapshot {
    fn from(params: &GenParams) -> Self {
        let extra_digest = (!params.extra.is_empty()).then(|| {
            // serde_json::Map<String, Value> is always serializable.
            sha256_hex(
                serde_json::to_string(&params.extra)
                    .unwrap_or_default()
                    .as_bytes(),
            )
        });
        Self {
            temperature: params.temperature,
            top_p: params.top_p,
            max_output_tokens: params.max_output_tokens,
            effort: params.effort,
            extra_digest,
        }
    }
}

impl TargetSnapshot {
    pub fn from_bound(bound: &vyane_core::BoundTarget, config: &ResolvedConfig) -> Result<Self> {
        let env_policy_digest = config
            .env_policy_for(bound)?
            .map(|policy| {
                let mut allow = policy.allow;
                allow.sort();
                let source_mapping = config
                    .providers
                    .get(bound.target.provider.as_str())?
                    .env_inject
                    .clone();
                serde_json::to_vec(&(policy.mode, allow, source_mapping))
                    .context("serialize harness env-policy shape")
                    .map(|bytes| sha256_hex(&bytes))
            })
            .transpose()?;
        Ok(Self {
            target: bound.target.clone(),
            transport: bound.transport,
            params: GenParamsSnapshot::from(&bound.params),
            endpoint_digest: bound
                .endpoint
                .as_ref()
                .map(|endpoint| sha256_hex(endpoint.base_url.as_bytes())),
            auth_style: bound
                .endpoint
                .as_ref()
                .and_then(|endpoint| endpoint.auth.as_ref().map(|auth| auth.style)),
            env_policy_digest,
        })
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
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
    pub fn harness_controller(&self) -> PathBuf {
        self.dir.join(HARNESS_CONTROLLER_FILE)
    }
    pub fn harness_controller_lock(&self) -> PathBuf {
        self.dir.join(HARNESS_CONTROLLER_LOCK_FILE)
    }

    /// Create the run directory (and parents).
    pub fn ensure_dir(&self) -> Result<()> {
        fs::create_dir_all(&self.dir)
            .with_context(|| format!("create task dir {}", self.dir.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&self.dir, fs::Permissions::from_mode(0o700))
                .with_context(|| format!("chmod private task dir {}", self.dir.display()))?;
        }
        Ok(())
    }

    /// Serialize a legacy job file. New submissions must use `WorkerEnvelope`
    /// over stdin and never call this method.
    #[cfg(test)]
    pub fn write_job(&self, job: &JobSpec) -> Result<()> {
        let text = serde_json::to_string_pretty(job).context("serialize job spec")?;
        write_private_file(&self.job(), text.as_bytes())
    }

    /// Read a legacy on-disk job spec created by an older Vyane release.
    pub fn read_job(&self) -> Result<JobSpec> {
        let text = fs::read_to_string(self.job())
            .with_context(|| format!("read {}", self.job().display()))?;
        serde_json::from_str(&text).with_context(|| format!("parse {}", self.job().display()))
    }

    /// Best-effort creation time for a task whose worker never wrote status.
    ///
    /// New submissions have no `job.json`, so stale discovery falls back through
    /// the log and task-directory mtimes. Legacy jobs still use their job mtime.
    pub fn scaffold_mtime(&self) -> Option<DateTime<Utc>> {
        [self.job(), self.log(), self.dir.clone()]
            .into_iter()
            .filter_map(|path| fs::metadata(path).ok()?.modified().ok())
            .map(DateTime::<Utc>::from)
            .max()
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

    /// Write captured model output with owner-only permissions.
    pub fn write_output(&self, text: &str) -> Result<()> {
        atomic_write(&self.output(), text.as_bytes())
    }

    pub fn write_harness_controller(&self, controller: &HarnessControllerFile) -> Result<()> {
        let text = serde_json::to_string(controller).context("serialize harness controller")?;
        self.with_harness_controller_lock(|| {
            if let Some(current) = self.read_harness_controller_optional_unlocked()? {
                if current == *controller {
                    return Ok(());
                }
                anyhow::bail!(
                    "nested harness controller already names pending pid {} pgid {}; refusing to overwrite it with pid {} pgid {}",
                    current.pid,
                    current.pgid,
                    controller.pid,
                    controller.pgid
                );
            }
            atomic_write(&self.harness_controller(), text.as_bytes())
        })
    }

    #[cfg(test)]
    pub fn read_harness_controller(&self) -> Result<HarnessControllerFile> {
        self.read_harness_controller_optional()?
            .ok_or_else(|| anyhow::anyhow!("nested harness controller is absent"))
    }

    pub fn read_harness_controller_optional(&self) -> Result<Option<HarnessControllerFile>> {
        self.with_harness_controller_lock(|| self.read_harness_controller_optional_unlocked())
    }

    fn read_harness_controller_optional_unlocked(&self) -> Result<Option<HarnessControllerFile>> {
        let path = self.harness_controller();
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read nested harness controller {}", path.display()));
            }
        };
        let controller: HarnessControllerFile = serde_json::from_str(&text)
            .with_context(|| format!("parse nested harness controller {}", path.display()))?;
        if controller.schema != HARNESS_CONTROLLER_SCHEMA {
            anyhow::bail!(
                "unsupported nested harness controller schema {} (expected {})",
                controller.schema,
                HARNESS_CONTROLLER_SCHEMA
            );
        }
        Ok(Some(controller))
    }

    /// Remove the sidecar only if it still names the harness reporting its
    /// stop. A stale stop event must never erase a newer harness controller.
    pub fn remove_harness_controller(&self, expected: &HarnessControllerFile) -> Result<()> {
        self.with_harness_controller_lock(|| {
            let path = self.harness_controller();
            let current = self.read_harness_controller_optional_unlocked()?;
            if current.as_ref() == Some(expected) {
                match fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!("remove harness controller {}", path.display())
                        });
                    }
                }
            }
            Ok(())
        })
    }

    /// Serialize Started/Stopped updates across threads and controller
    /// processes. Atomic rename alone cannot make a read-compare-unlink
    /// conditional: without this advisory lock an old Stopped callback could
    /// read its own sidecar, race a new Started rename, and unlink the new one.
    fn with_harness_controller_lock<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        let path = self.harness_controller_lock();
        let mut options = fs::OpenOptions::new();
        options.create(true).truncate(false).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let file = options
            .open(&path)
            .with_context(|| format!("open harness controller lock {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .with_context(|| format!("chmod harness controller lock {}", path.display()))?;
        }

        let deadline = std::time::Instant::now() + HARNESS_CONTROLLER_LOCK_WAIT;
        loop {
            match fs4::fs_std::FileExt::try_lock_exclusive(&file) {
                Ok(true) => break,
                Ok(false) => {
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        anyhow::bail!(
                            "timed out after {} ms waiting for harness controller lock {}",
                            HARNESS_CONTROLLER_LOCK_WAIT.as_millis(),
                            path.display()
                        );
                    }
                    std::thread::sleep(std::cmp::min(
                        HARNESS_CONTROLLER_LOCK_POLL,
                        deadline.saturating_duration_since(now),
                    ));
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("lock harness controller {}", path.display()));
                }
            }
        }

        let result = operation();
        let unlock = fs4::fs_std::FileExt::unlock(&file)
            .with_context(|| format!("unlock harness controller {}", path.display()));
        match (result, unlock) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(value), Ok(())) => Ok(value),
        }
    }
}

#[cfg(test)]
fn write_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = fs::OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
        options.mode(0o600);
        let mut file = options
            .open(path)
            .with_context(|| format!("open private file {}", path.display()))?;
        // `mode` applies only at creation; force restrictive permissions when
        // overwriting a legacy file that may have been world-readable.
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .with_context(|| format!("chmod private file {}", path.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("write private file {}", path.display()))?;
        file.sync_all().ok();
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let mut file = options
            .open(path)
            .with_context(|| format!("open private file {}", path.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("write private file {}", path.display()))?;
        file.sync_all().ok();
        Ok(())
    }
}

/// Write `bytes` to `path` atomically via a temp file + rename in the same
/// directory (rename is atomic within a filesystem). Each write uses a unique,
/// create-new temp name so concurrent writers cannot truncate one another.
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp = dir.join(format!(
        ".{}.tmp.{}.{}",
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("status"),
        std::process::id(),
        uuid::Uuid::now_v7()
    ));
    {
        let mut options = fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut f = options
            .open(&tmp)
            .with_context(|| format!("create temp {}", tmp.display()))?;
        f.write_all(bytes)
            .with_context(|| format!("write temp {}", tmp.display()))?;
        f.sync_all().ok();
    }
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("chmod private file {}", path.display()))?;
    }
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
/// A directory whose `status.json` is missing or unreadable surfaces as a
/// [`TaskState::Stale`] row. Its time comes from the legacy job, log, or task
/// directory mtime, taking the latest available timestamp. This keeps new spawn
/// failures visible even though their private request was never written to disk.
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
                // No readable status. The directory itself is enough evidence
                // that submission started; show it as stale even without the
                // legacy job.json that new private-envelope tasks never create.
                if let Some(started_at) = paths.scaffold_mtime() {
                    rows.push(TaskListRow {
                        id,
                        state: TaskState::Stale,
                        target: "-".to_string(),
                        started_at,
                        duration_ms: None,
                    });
                }
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

    fn sample_job(run_id: &str) -> JobSpec {
        JobSpec {
            run_id: run_id.into(),
            task: "do it".into(),
            target: "review".into(),
            workdir: Some(PathBuf::from("/tmp/work")),
            sandbox: SandboxSpec::Write,
            system: Some("be terse".into()),
            timeout_secs: Some(30),
            labels: vec!["k=v".into()],
            session: Some("s1".into()),
            config: None,
            target_snapshot: Vec::new(),
            capability_plan: None,
        }
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
    fn harness_controller_roundtrips_privately_and_exact_stop_removes_only_itself() {
        let dir = TempDir::new().unwrap();
        let paths = TaskPaths::new(dir.path(), "nested");
        paths.ensure_dir().unwrap();
        let controller = HarnessControllerFile {
            schema: HARNESS_CONTROLLER_SCHEMA,
            pid: 101,
            pgid: 101,
            started_at: Utc::now(),
            birth_fingerprint: Some("birth-101".into()),
        };
        paths.write_harness_controller(&controller).unwrap();
        assert_eq!(paths.read_harness_controller().unwrap(), controller);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                fs::metadata(paths.harness_controller())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }

        let mut other = controller.clone();
        other.pid = 202;
        other.pgid = 202;
        paths.remove_harness_controller(&other).unwrap();
        assert!(paths.harness_controller().exists());
        let mut reused = controller.clone();
        reused.birth_fingerprint = Some("new-birth-101".into());
        paths.remove_harness_controller(&reused).unwrap();
        assert!(
            paths.harness_controller().exists(),
            "pid/pgid reuse must not let an old Stop erase a new birth identity"
        );
        paths.remove_harness_controller(&controller).unwrap();
        assert!(!paths.harness_controller().exists());
    }

    #[test]
    fn pending_controller_blocks_new_started_and_old_stop_cannot_remove_new() {
        let dir = TempDir::new().unwrap();
        let paths = TaskPaths::new(dir.path(), "nested-race");
        paths.ensure_dir().unwrap();
        let old = HarnessControllerFile {
            schema: HARNESS_CONTROLLER_SCHEMA,
            pid: 1_001,
            pgid: 1_001,
            started_at: Utc::now(),
            birth_fingerprint: Some("old-birth".into()),
        };
        let new = HarnessControllerFile {
            schema: HARNESS_CONTROLLER_SCHEMA,
            pid: 2_002,
            pgid: 2_002,
            started_at: Utc::now(),
            birth_fingerprint: Some("new-birth".into()),
        };

        paths.write_harness_controller(&old).unwrap();
        let error = paths.write_harness_controller(&new).unwrap_err();
        assert!(
            format!("{error:#}").contains("refusing to overwrite"),
            "unexpected pending-controller diagnostic: {error:#}"
        );
        assert_eq!(paths.read_harness_controller().unwrap(), old);

        paths.remove_harness_controller(&old).unwrap();
        paths.write_harness_controller(&new).unwrap();
        paths.remove_harness_controller(&old).unwrap();
        assert_eq!(
            paths.read_harness_controller().unwrap(),
            new,
            "a delayed old Stopped must not remove the accepted new sentinel"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                fs::metadata(paths.harness_controller_lock())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn harness_controller_lock_contention_is_bounded_and_diagnostic() {
        let dir = TempDir::new().unwrap();
        let paths = TaskPaths::new(dir.path(), "nested-lock-timeout");
        paths.ensure_dir().unwrap();

        let lock_path = paths.harness_controller_lock();
        let holder = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        fs4::fs_std::FileExt::lock_exclusive(&holder).unwrap();

        let started = std::time::Instant::now();
        let error = paths.read_harness_controller_optional().unwrap_err();
        let elapsed = started.elapsed();
        fs4::fs_std::FileExt::unlock(&holder).unwrap();

        let diagnostic = format!("{error:#}");
        let expected_timeout = format!(
            "timed out after {} ms waiting for harness controller lock",
            HARNESS_CONTROLLER_LOCK_WAIT.as_millis()
        );
        assert!(
            diagnostic.contains(&expected_timeout),
            "unexpected diagnostic: {diagnostic}"
        );
        assert!(
            diagnostic.contains(&lock_path.display().to_string()),
            "lock path missing from diagnostic: {diagnostic}"
        );
        assert!(elapsed >= HARNESS_CONTROLLER_LOCK_WAIT);
        assert!(
            elapsed < HARNESS_CONTROLLER_LOCK_WAIT + std::time::Duration::from_secs(2),
            "lock timeout exceeded its bounded scheduling slack: {elapsed:?}"
        );

        assert_eq!(paths.read_harness_controller_optional().unwrap(), None);
    }

    #[test]
    fn legacy_job_spec_roundtrips() {
        let dir = TempDir::new().unwrap();
        let paths = TaskPaths::new(dir.path(), "j1");
        paths.ensure_dir().unwrap();
        let job = sample_job("j1");
        paths.write_job(&job).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                fs::metadata(paths.job()).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let back = paths.read_job().unwrap();
        assert_eq!(back.run_id, "j1");
        assert_eq!(back.target, "review");
        assert_eq!(back.sandbox, SandboxSpec::Write);
        assert_eq!(back.timeout_secs, Some(30));
        assert_eq!(back.labels, vec!["k=v".to_string()]);
    }

    #[test]
    fn worker_envelope_roundtrips_with_independent_schema() {
        let envelope = WorkerEnvelope::new(sample_job("stdin-1"));
        let text = serde_json::to_string(&envelope).unwrap();
        let back: WorkerEnvelope = serde_json::from_str(&text).unwrap();

        assert_eq!(back.schema, WORKER_ENVELOPE_SCHEMA);
        assert_eq!(back.job.run_id, "stdin-1");
        assert_eq!(back.job.task, "do it");
        assert_eq!(back.job.system.as_deref(), Some("be terse"));
    }

    #[test]
    fn target_snapshot_detects_endpoint_identity_without_persisting_secret_or_url() {
        let mut extra = serde_json::Map::new();
        extra.insert(
            "custom_secret".into(),
            serde_json::Value::String("extra-secret-canary".into()),
        );
        let bound = vyane_core::BoundTarget {
            target: vyane_core::Target {
                provider: vyane_core::ProviderId::new("relay"),
                protocol: vyane_core::Protocol::OpenaiChat,
                harness: None,
                model: vyane_core::ModelId::new("model"),
            },
            transport: vyane_core::AdapterTransport::DirectHttp,
            endpoint: Some(vyane_core::Endpoint {
                base_url: "https://relay.invalid/v1?tenant=private".into(),
                auth: Some(vyane_core::AuthMaterial {
                    style: vyane_core::AuthStyle::Bearer,
                    secret: vyane_core::Secret::new("super-secret-token"),
                }),
            }),
            params: vyane_core::GenParams {
                extra,
                ..Default::default()
            },
        };
        let snapshot = TargetSnapshot::from_bound(&bound, &ResolvedConfig::default()).unwrap();
        let json = serde_json::to_string(&snapshot).unwrap();
        assert_eq!(snapshot.auth_style, Some(vyane_core::AuthStyle::Bearer));
        assert_eq!(snapshot.endpoint_digest.as_deref().map(str::len), Some(64));
        assert!(!json.contains("super-secret-token"));
        assert!(!json.contains("relay.invalid"));
        assert!(!json.contains("tenant=private"));
        assert!(!json.contains("extra-secret-canary"));
        assert_eq!(
            snapshot.params.extra_digest.as_deref().map(str::len),
            Some(64)
        );
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
    fn list_tasks_orders_recent_first_and_surfaces_unwritten_scaffold() {
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
        // A new-transport spawn can fail after creating only the task directory.
        // It must remain visible even though neither status.json nor job.json
        // exists.
        fs::create_dir_all(root.join("pending")).unwrap();

        let rows = list_tasks(root, &identity_all_match);
        assert_eq!(rows.len(), 3, "new task scaffolds must not disappear");
        let pending = rows.iter().find(|row| row.id == "pending").unwrap();
        assert_eq!(pending.state, TaskState::Stale);
        let persisted: Vec<_> = rows
            .iter()
            .filter(|row| row.id != "pending")
            .map(|row| row.id.as_str())
            .collect();
        assert_eq!(persisted, ["new", "old"], "persisted runs stay ordered");
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

        // A legacy task dir with job.json but no status.json: an older parent
        // laid down the job but its worker never wrote status.
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
            target_snapshot: Vec::new(),
            capability_plan: None,
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
    fn list_tasks_surfaces_new_log_scaffold_without_job_as_stale() {
        let dir = TempDir::new().unwrap();
        let paths = TaskPaths::new(dir.path(), "stdin-spawn-failed");
        paths.ensure_dir().unwrap();
        fs::write(paths.log(), "handoff never completed\n").unwrap();

        let rows = list_tasks(dir.path(), &identity_all_match);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "stdin-spawn-failed");
        assert_eq!(rows[0].state, TaskState::Stale);
        assert!(!paths.job().exists(), "new scaffolds have no job.json");
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
