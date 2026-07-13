//! Durable, payload-free controller collection for daemon workflow harnesses.
//!
//! A workflow may execute several CLI harnesses concurrently. Each harness
//! sentinel therefore owns an independent sidecar below the validated workflow
//! run id. Sidecars contain process identity only; prompts, arguments,
//! environment, output, and errors never enter this directory.

use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use chrono::{DateTime, Utc};
use fs4::fs_std::FileExt as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use vyane_core::{ErrorKind, HarnessLifecycleEvent, HarnessLifecycleReporter, VyaneError};
use vyane_workflow::WorkflowRunId;

use crate::task::proc::{
    IdentityCheck, SIGKILL, SIGTERM, process_birth_fingerprint, process_group_alive, signal_group,
    verify_controller_identity,
};

type ActiveControllers = BTreeMap<(i32, i32), VecDeque<HarnessController>>;

const CONTROLLERS_DIR: &str = "workflow-controllers";
const CONTROLLER_SCHEMA: u32 = 1;
const CONTROLLER_LOCK_FILE: &str = ".lock";
const CONTROLLER_PREFIX: &str = "controller-";
const CONTROLLER_SUFFIX: &str = ".json";
const MAX_CONTROLLER_BYTES: u64 = 16 * 1024;
const LOCK_WAIT: Duration = Duration::from_millis(500);
const LOCK_POLL: Duration = Duration::from_millis(10);
const CONTROL_POLL: Duration = Duration::from_millis(50);
const TERM_GRACE: Duration = Duration::from_secs(2);
const KILL_GRACE: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
struct ControllerPaths {
    collection_root: PathBuf,
    run_dir: PathBuf,
    lock: PathBuf,
}

impl ControllerPaths {
    fn new(run_id: &WorkflowRunId, data_dir: &Path) -> Self {
        let collection_root = data_dir.join(CONTROLLERS_DIR);
        let run_dir = collection_root.join(run_id.as_str());
        let lock = run_dir.join(CONTROLLER_LOCK_FILE);
        Self {
            collection_root,
            run_dir,
            lock,
        }
    }

    fn entry(&self, controller: &HarnessController) -> PathBuf {
        self.run_dir.join(controller_filename(controller))
    }
}

/// Exact identity of one workflow-owned harness sentinel.
///
/// This shape is intentionally closed and contains control metadata only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HarnessController {
    schema: u32,
    pid: i32,
    pgid: i32,
    started_at: DateTime<Utc>,
    birth_fingerprint: String,
}

impl HarnessController {
    fn validate(&self) -> Result<()> {
        if self.schema != CONTROLLER_SCHEMA {
            bail!(
                "unsupported workflow harness controller schema {} (expected {})",
                self.schema,
                CONTROLLER_SCHEMA
            );
        }
        if self.pid <= 0 || self.pgid <= 0 || self.pid != self.pgid {
            bail!(
                "workflow harness sentinel must be a positive process-group leader (pid {}, pgid {})",
                self.pid,
                self.pgid
            );
        }
        if self.birth_fingerprint.is_empty() || self.birth_fingerprint.len() > 512 {
            bail!("workflow harness birth fingerprint is empty or oversized");
        }
        Ok(())
    }
}

/// Result of attempting to clean one durable harness controller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum ControllerCleanupStatus {
    AlreadyGone,
    Terminated,
    Killed,
    RefusedDeadSentinelLiveGroup,
    RefusedIdentityMismatch { reason: String },
    StillAliveAfterKill,
    StorageError { message: String },
}

impl ControllerCleanupStatus {
    #[must_use]
    pub(crate) fn resolved(&self) -> bool {
        matches!(self, Self::AlreadyGone | Self::Terminated | Self::Killed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ControllerCleanupEntry {
    /// Filename-only identifier. Malformed sidecars never contribute paths or
    /// untrusted process identity to the report.
    pub(crate) sidecar: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pid: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pgid: Option<i32>,
    pub(crate) status: ControllerCleanupStatus,
}

/// Complete per-controller outcome. Refusals are data, not discarded logs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct ControllerCleanupReport {
    pub(crate) controllers: Vec<ControllerCleanupEntry>,
}

impl ControllerCleanupReport {
    #[must_use]
    pub(crate) fn all_resolved(&self) -> bool {
        self.controllers.iter().all(|entry| entry.status.resolved())
    }
}

#[derive(Debug, Default)]
struct ControllerInventory {
    valid: Vec<HarnessController>,
    storage_errors: Vec<ControllerCleanupEntry>,
}

/// Persistent controller set for one validated workflow run.
#[derive(Clone)]
pub(crate) struct WorkflowHarnessControl {
    paths: Arc<ControllerPaths>,
    active: Arc<Mutex<ActiveControllers>>,
    term_grace: Duration,
    kill_grace: Duration,
}

impl std::fmt::Debug for WorkflowHarnessControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowHarnessControl")
            .field("run_dir", &self.paths.run_dir)
            .finish_non_exhaustive()
    }
}

impl WorkflowHarnessControl {
    /// Construct the controller set below `data_dir` for one canonical run id.
    pub(crate) fn new(run_id: &WorkflowRunId, data_dir: &Path) -> Result<Self> {
        let paths = ControllerPaths::new(run_id, data_dir);
        secure_layout(&paths)?;
        Ok(Self {
            paths: Arc::new(paths),
            active: Arc::new(Mutex::new(BTreeMap::new())),
            term_grace: TERM_GRACE,
            kill_grace: KILL_GRACE,
        })
    }

    /// Lifecycle reporter installed on every task spawned for this workflow.
    pub(crate) fn reporter(&self) -> HarnessLifecycleReporter {
        let control = self.clone();
        HarnessLifecycleReporter::new(move |event| {
            control
                .handle_event(event)
                .map_err(workflow_controller_error)
        })
    }

    /// Startup-recovery cleanup. Every controller receives an independent,
    /// structured result so one fail-closed identity does not hide the rest.
    pub(crate) async fn cleanup_all(&self) -> Result<ControllerCleanupReport> {
        self.terminate_all().await
    }

    /// Explicit workflow cancellation uses the same exact-identity contract as
    /// startup recovery.
    pub(crate) async fn cancel_all(&self) -> Result<ControllerCleanupReport> {
        self.terminate_all().await
    }

    fn handle_event(&self, event: HarnessLifecycleEvent) -> Result<()> {
        match event {
            HarnessLifecycleEvent::Started { pid, pgid } => self.handle_started(pid, pgid),
            HarnessLifecycleEvent::Stopped {
                pid,
                pgid,
                group_empty,
            } => self.handle_stopped(pid, pgid, group_empty),
        }
    }

    fn handle_started(&self, pid: u32, pgid: i32) -> Result<()> {
        let pid = i32::try_from(pid).context("workflow harness pid exceeded i32")?;
        let birth_fingerprint = process_birth_fingerprint(pid)
            .context("workflow harness process birth fingerprint was unavailable")?;
        let controller = HarnessController {
            schema: CONTROLLER_SCHEMA,
            pid,
            pgid,
            started_at: Utc::now(),
            birth_fingerprint,
        };
        controller.validate()?;
        self.publish(&controller)?;

        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let queue = active.entry((pid, pgid)).or_default();
        if !queue.contains(&controller) {
            queue.push_back(controller);
        }
        Ok(())
    }

    fn handle_stopped(&self, pid: u32, pgid: i32, group_empty: bool) -> Result<()> {
        let Ok(pid) = i32::try_from(pid) else {
            return Ok(());
        };
        let expected = {
            let mut active = self
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(queue) = active.get_mut(&(pid, pgid)) else {
                return Ok(());
            };
            let expected = queue.pop_front();
            if queue.is_empty() {
                active.remove(&(pid, pgid));
            }
            expected
        };
        let Some(expected) = expected else {
            return Ok(());
        };
        if group_empty {
            let _ = self.remove_exact(&expected)?;
        }
        Ok(())
    }

    async fn terminate_all(&self) -> Result<ControllerCleanupReport> {
        let inventory = self.read_all()?;
        let statuses = futures::future::join_all(
            inventory
                .valid
                .iter()
                .map(|controller| self.cleanup_one(controller)),
        )
        .await;
        let mut controllers = inventory
            .valid
            .into_iter()
            .zip(statuses)
            .map(|(controller, status)| ControllerCleanupEntry {
                sidecar: controller_filename(&controller),
                pid: Some(controller.pid),
                pgid: Some(controller.pgid),
                status,
            })
            .collect::<Vec<_>>();
        controllers.extend(inventory.storage_errors);
        Ok(ControllerCleanupReport { controllers })
    }

    async fn cleanup_one(&self, controller: &HarnessController) -> ControllerCleanupStatus {
        match observe_controller(controller) {
            ControllerObservation::GroupGone => {
                return self.remove_status(controller, CleanupPhase::Gone);
            }
            ControllerObservation::DeadSentinelLiveGroup => {
                return ControllerCleanupStatus::RefusedDeadSentinelLiveGroup;
            }
            ControllerObservation::IdentityMismatch(reason) => {
                return ControllerCleanupStatus::RefusedIdentityMismatch {
                    reason: reason.to_string(),
                };
            }
            ControllerObservation::Exact => {}
        }

        signal_group(controller.pgid, SIGTERM);
        match wait_for_observation(controller, self.term_grace).await {
            ControllerObservation::GroupGone => {
                return self.remove_status(controller, CleanupPhase::Terminated);
            }
            ControllerObservation::DeadSentinelLiveGroup => {
                return ControllerCleanupStatus::RefusedDeadSentinelLiveGroup;
            }
            ControllerObservation::IdentityMismatch(reason) => {
                return ControllerCleanupStatus::RefusedIdentityMismatch {
                    reason: reason.to_string(),
                };
            }
            ControllerObservation::Exact => {}
        }

        // Revalidate at the forced-signal boundary. A timeout does not carry
        // authority forward across a potential PID/PGID reuse.
        match observe_controller(controller) {
            ControllerObservation::Exact => signal_group(controller.pgid, SIGKILL),
            ControllerObservation::GroupGone => {
                return self.remove_status(controller, CleanupPhase::Terminated);
            }
            ControllerObservation::DeadSentinelLiveGroup => {
                return ControllerCleanupStatus::RefusedDeadSentinelLiveGroup;
            }
            ControllerObservation::IdentityMismatch(reason) => {
                return ControllerCleanupStatus::RefusedIdentityMismatch {
                    reason: reason.to_string(),
                };
            }
        }

        match wait_for_observation(controller, self.kill_grace).await {
            ControllerObservation::GroupGone => {
                self.remove_status(controller, CleanupPhase::Killed)
            }
            ControllerObservation::DeadSentinelLiveGroup => {
                ControllerCleanupStatus::RefusedDeadSentinelLiveGroup
            }
            ControllerObservation::IdentityMismatch(reason) => {
                ControllerCleanupStatus::RefusedIdentityMismatch {
                    reason: reason.to_string(),
                }
            }
            ControllerObservation::Exact => ControllerCleanupStatus::StillAliveAfterKill,
        }
    }

    fn remove_status(
        &self,
        controller: &HarnessController,
        phase: CleanupPhase,
    ) -> ControllerCleanupStatus {
        match self.remove_exact(controller) {
            Ok(ExactRemoval::Removed | ExactRemoval::Absent) => phase.status(),
            Ok(ExactRemoval::Mismatch) => ControllerCleanupStatus::StorageError {
                message: "workflow controller sidecar changed before exact removal".into(),
            },
            Err(error) => ControllerCleanupStatus::StorageError {
                message: format!("{error:#}"),
            },
        }
    }

    fn publish(&self, controller: &HarnessController) -> Result<()> {
        controller.validate()?;
        self.with_lock(|| {
            let path = self.paths.entry(controller);
            if path.exists() {
                let current = read_controller(&path)?;
                if current == *controller {
                    secure_file(&path)?;
                    return Ok(());
                }
                bail!(
                    "workflow harness controller {} already contains a different identity",
                    path.display()
                );
            }
            let bytes = serde_json::to_vec(controller).context("serialize workflow controller")?;
            atomic_publish(&self.paths.run_dir, &path, &bytes)
        })
    }

    fn remove_exact(&self, expected: &HarnessController) -> Result<ExactRemoval> {
        self.with_lock(|| {
            let path = self.paths.entry(expected);
            let current = match read_controller_optional(&path)? {
                Some(current) => current,
                None => return Ok(ExactRemoval::Absent),
            };
            if current != *expected {
                return Ok(ExactRemoval::Mismatch);
            }
            match fs::remove_file(&path) {
                Ok(()) => {
                    sync_directory(&self.paths.run_dir)?;
                    Ok(ExactRemoval::Removed)
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    Ok(ExactRemoval::Absent)
                }
                Err(error) => Err(error)
                    .with_context(|| format!("remove workflow controller {}", path.display())),
            }
        })
    }

    fn read_all(&self) -> Result<ControllerInventory> {
        self.with_lock(|| read_all_unlocked(&self.paths))
    }

    fn with_lock<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        secure_layout(&self.paths)?;
        let file = open_lock(&self.paths.lock)?;
        let deadline = Instant::now() + LOCK_WAIT;
        loop {
            match file.try_lock_exclusive() {
                Ok(true) => break,
                Ok(false) => {
                    let now = Instant::now();
                    if now >= deadline {
                        bail!(
                            "timed out after {} ms waiting for workflow controller lock {}",
                            LOCK_WAIT.as_millis(),
                            self.paths.lock.display()
                        );
                    }
                    std::thread::sleep(std::cmp::min(
                        LOCK_POLL,
                        deadline.saturating_duration_since(now),
                    ));
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("lock workflow controller {}", self.paths.lock.display())
                    });
                }
            }
        }

        let result = operation();
        let unlock = fs4::fs_std::FileExt::unlock(&file)
            .with_context(|| format!("unlock workflow controller {}", self.paths.lock.display()));
        match (result, unlock) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(value), Ok(())) => Ok(value),
        }
    }

    #[cfg(all(test, target_os = "linux"))]
    fn with_graces(mut self, term_grace: Duration, kill_grace: Duration) -> Self {
        self.term_grace = term_grace;
        self.kill_grace = kill_grace;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExactRemoval {
    Removed,
    Absent,
    Mismatch,
}

#[derive(Debug, Clone, Copy)]
enum CleanupPhase {
    Gone,
    Terminated,
    Killed,
}

impl CleanupPhase {
    fn status(self) -> ControllerCleanupStatus {
        match self {
            Self::Gone => ControllerCleanupStatus::AlreadyGone,
            Self::Terminated => ControllerCleanupStatus::Terminated,
            Self::Killed => ControllerCleanupStatus::Killed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControllerObservation {
    Exact,
    GroupGone,
    DeadSentinelLiveGroup,
    IdentityMismatch(&'static str),
}

fn observe_controller(controller: &HarnessController) -> ControllerObservation {
    if !process_group_alive(controller.pgid) {
        return ControllerObservation::GroupGone;
    }
    match verify_controller_identity(
        controller.pid,
        controller.pgid,
        controller.started_at,
        Some(&controller.birth_fingerprint),
    ) {
        IdentityCheck::Match => ControllerObservation::Exact,
        IdentityCheck::Dead => ControllerObservation::DeadSentinelLiveGroup,
        IdentityCheck::Mismatch(reason) => ControllerObservation::IdentityMismatch(reason),
    }
}

async fn wait_for_observation(
    controller: &HarnessController,
    budget: Duration,
) -> ControllerObservation {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let observation = observe_controller(controller);
        // After an authorized signal, BSD/macOS may briefly keep the process
        // group observable while its leader is already a zombie or has just
        // been reaped. Keep polling those disappearance-shaped observations;
        // the deadline still returns the unresolved state fail-closed and no
        // later signal is authorized from it.
        match observation {
            ControllerObservation::GroupGone => return observation,
            ControllerObservation::IdentityMismatch(reason)
                if reason != "could not read process birth fingerprint" =>
            {
                return observation;
            }
            ControllerObservation::Exact
            | ControllerObservation::DeadSentinelLiveGroup
            | ControllerObservation::IdentityMismatch(_) => {}
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return observation;
        }
        tokio::time::sleep(deadline.saturating_duration_since(now).min(CONTROL_POLL)).await;
    }
}

fn workflow_controller_error(error: anyhow::Error) -> VyaneError {
    VyaneError::new(
        ErrorKind::Io,
        format!("workflow harness controller update failed: {error:#}"),
    )
}

fn controller_filename(controller: &HarnessController) -> String {
    let mut digest = Sha256::new();
    digest.update(controller.birth_fingerprint.as_bytes());
    let digest = digest.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    format!(
        "{CONTROLLER_PREFIX}{}-{hex}{CONTROLLER_SUFFIX}",
        controller.pid
    )
}

fn secure_layout(paths: &ControllerPaths) -> Result<()> {
    fs::create_dir_all(&paths.run_dir).with_context(|| {
        format!(
            "create workflow controller directory {}",
            paths.run_dir.display()
        )
    })?;
    secure_directory(&paths.collection_root)?;
    secure_directory(&paths.run_dir)?;
    Ok(())
}

#[cfg(unix)]
fn secure_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("chmod private directory {}", path.display()))
}

#[cfg(not(unix))]
fn secure_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn open_lock(path: &Path) -> Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options
        .open(path)
        .with_context(|| format!("open workflow controller lock {}", path.display()))?;
    secure_file_handle(&file, path)?;
    Ok(file)
}

#[cfg(unix)]
fn secure_file_handle(file: &fs::File, path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    file.set_permissions(fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod private file {}", path.display()))
}

#[cfg(not(unix))]
fn secure_file_handle(_file: &fs::File, _path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn secure_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod private file {}", path.display()))
}

#[cfg(not(unix))]
fn secure_file(_path: &Path) -> Result<()> {
    Ok(())
}

fn atomic_publish(directory: &Path, target: &Path, bytes: &[u8]) -> Result<()> {
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .context("workflow controller target has no UTF-8 filename")?;
    let temp = directory.join(format!(
        ".{file_name}.tmp.{}.{}",
        std::process::id(),
        uuid::Uuid::now_v7()
    ));
    let result = (|| -> Result<()> {
        let mut options = fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temp)
            .with_context(|| format!("create workflow controller temp {}", temp.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("write workflow controller temp {}", temp.display()))?;
        file.sync_all()
            .with_context(|| format!("sync workflow controller temp {}", temp.display()))?;
        drop(file);
        fs::rename(&temp, target).with_context(|| {
            format!(
                "publish workflow controller {} -> {}",
                temp.display(),
                target.display()
            )
        })?;
        secure_file(target)?;
        sync_directory(directory)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(path)
            .with_context(|| format!("open workflow controller directory {}", path.display()))?
            .sync_all()
            .with_context(|| format!("sync workflow controller directory {}", path.display()))?;
    }
    Ok(())
}

fn read_controller_optional(path: &Path) -> Result<Option<HarnessController>> {
    match fs::symlink_metadata(path) {
        Ok(_) => read_controller(path).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("stat workflow controller {}", path.display()))
        }
    }
}

fn read_controller(path: &Path) -> Result<HarnessController> {
    let metadata = fs::symlink_metadata(path)
        .context("workflow controller sidecar metadata is unavailable")?;
    if metadata.file_type().is_symlink() {
        bail!("workflow controller sidecar is a symlink");
    }
    if !metadata.file_type().is_file() {
        bail!("workflow controller sidecar is not a regular file");
    }
    if metadata.len() > MAX_CONTROLLER_BYTES {
        bail!("workflow controller sidecar exceeds the size limit");
    }
    secure_file(path).context("secure workflow controller sidecar permissions")?;
    let mut file = fs::File::open(path).context("open workflow controller sidecar")?;
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(MAX_CONTROLLER_BYTES + 1)
        .read_to_end(&mut bytes)
        .context("read workflow controller sidecar")?;
    if bytes.len() as u64 > MAX_CONTROLLER_BYTES {
        bail!("workflow controller sidecar exceeds the size limit");
    }
    let controller: HarnessController = serde_json::from_slice(&bytes)
        .context("workflow controller sidecar has invalid JSON or fields")?;
    controller
        .validate()
        .context("workflow controller sidecar identity failed validation")?;
    Ok(controller)
}

fn read_all_unlocked(paths: &ControllerPaths) -> Result<ControllerInventory> {
    let mut inventory = ControllerInventory::default();
    for entry in fs::read_dir(&paths.run_dir).with_context(|| {
        format!(
            "read workflow controller directory {}",
            paths.run_dir.display()
        )
    })? {
        let entry = entry.with_context(|| {
            format!(
                "enumerate workflow controller directory {}",
                paths.run_dir.display()
            )
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(CONTROLLER_PREFIX) || !name.ends_with(CONTROLLER_SUFFIX) {
            continue;
        }
        let path = entry.path();
        let controller = match read_controller(&path) {
            Ok(controller) => controller,
            Err(error) => {
                inventory.storage_errors.push(invalid_sidecar_entry(
                    name,
                    format!("invalid workflow controller sidecar: {error}"),
                ));
                continue;
            }
        };
        if path != paths.entry(&controller) {
            inventory.storage_errors.push(invalid_sidecar_entry(
                name,
                "workflow controller filename does not match its exact birth identity".into(),
            ));
            continue;
        }
        inventory.valid.push(controller);
    }
    inventory.valid.sort_by(|left, right| {
        left.pid
            .cmp(&right.pid)
            .then_with(|| left.birth_fingerprint.cmp(&right.birth_fingerprint))
    });
    inventory
        .storage_errors
        .sort_by(|left, right| left.sidecar.cmp(&right.sidecar));
    Ok(inventory)
}

fn invalid_sidecar_entry(sidecar: &str, message: String) -> ControllerCleanupEntry {
    ControllerCleanupEntry {
        sidecar: sidecar.to_string(),
        pid: None,
        pgid: None,
        status: ControllerCleanupStatus::StorageError { message },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::process::{Child, Command, Stdio};
    use std::sync::Barrier;

    use super::*;
    use crate::task::proc::{install_process_group, pid_alive};

    fn test_control() -> (tempfile::TempDir, WorkflowHarnessControl) {
        let directory = tempfile::tempdir().unwrap();
        let run_id = WorkflowRunId::generate();
        let control = WorkflowHarnessControl::new(&run_id, directory.path()).unwrap();
        (directory, control)
    }

    fn synthetic_controller(pid: i32, fingerprint: &str) -> HarnessController {
        HarnessController {
            schema: CONTROLLER_SCHEMA,
            pid,
            pgid: pid,
            started_at: Utc::now(),
            birth_fingerprint: fingerprint.into(),
        }
    }

    fn queue_active(control: &WorkflowHarnessControl, controller: HarnessController) {
        control
            .active
            .lock()
            .unwrap()
            .entry((controller.pid, controller.pgid))
            .or_default()
            .push_back(controller);
    }

    #[cfg(unix)]
    fn spawn_isolated(script: &str) -> Child {
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", script])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        install_process_group(&mut command);
        command.spawn().unwrap()
    }

    #[test]
    fn old_stop_removes_only_its_exact_birth_and_false_stop_retains_sidecar() {
        let (_directory, control) = test_control();
        let old = synthetic_controller(41_001, "birth-old");
        let new = synthetic_controller(41_001, "birth-new");
        control.publish(&old).unwrap();
        control.publish(&new).unwrap();
        queue_active(&control, old.clone());
        queue_active(&control, new.clone());

        control
            .handle_stopped(old.pid as u32, old.pgid, true)
            .unwrap();
        assert_eq!(control.read_all().unwrap().valid, vec![new.clone()]);

        control
            .handle_stopped(new.pid as u32, new.pgid, false)
            .unwrap();
        assert_eq!(control.read_all().unwrap().valid, vec![new.clone()]);
        // The false stop consumed its process-local callback identity. A later
        // duplicate stop cannot reinterpret that retained recovery sidecar.
        control
            .handle_stopped(new.pid as u32, new.pgid, true)
            .unwrap();
        assert_eq!(control.read_all().unwrap().valid, vec![new]);
    }

    #[cfg(unix)]
    #[test]
    fn controller_directory_lock_and_json_are_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;

        let (_directory, control) = test_control();
        let controller = synthetic_controller(41_002, "permission-birth");
        control.publish(&controller).unwrap();

        assert_eq!(
            fs::metadata(&control.paths.collection_root)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&control.paths.run_dir)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        for path in [control.paths.lock.clone(), control.paths.entry(&controller)] {
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600,
                "{} must be private",
                path.display()
            );
        }
        let json = fs::read_to_string(control.paths.entry(&controller)).unwrap();
        for forbidden in ["prompt", "argument", "environment", "output", "secret"] {
            assert!(!json.contains(forbidden));
        }
    }

    #[test]
    fn controller_lock_contention_is_bounded() {
        let (_directory, control) = test_control();
        let lock = open_lock(&control.paths.lock).unwrap();
        lock.lock_exclusive().unwrap();

        let contender = control.clone();
        let started = Instant::now();
        let join = std::thread::spawn(move || contender.read_all());
        let error = join.join().unwrap().unwrap_err();
        let elapsed = started.elapsed();
        assert!(error.to_string().contains("timed out after 500 ms"));
        assert!(elapsed >= Duration::from_millis(400));
        assert!(elapsed < Duration::from_secs(2));
        fs4::fs_std::FileExt::unlock(&lock).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn invalid_sidecar_kinds_are_structured_and_retained() {
        use std::os::unix::fs::symlink;

        let (_directory, control) = test_control();
        let malformed = control.paths.run_dir.join("controller-malformed.json");
        let oversized = control.paths.run_dir.join("controller-oversized.json");
        let nonregular = control.paths.run_dir.join("controller-directory.json");
        let symlinked = control.paths.run_dir.join("controller-symlink.json");

        fs::write(&malformed, b"{not-json").unwrap();
        fs::write(
            &oversized,
            vec![b'x'; usize::try_from(MAX_CONTROLLER_BYTES).unwrap() + 1],
        )
        .unwrap();
        fs::create_dir(&nonregular).unwrap();
        symlink(&malformed, &symlinked).unwrap();

        let report = control.cleanup_all().await.unwrap();
        assert!(!report.all_resolved());
        assert_eq!(report.controllers.len(), 4);
        for entry in &report.controllers {
            assert!(entry.pid.is_none());
            assert!(entry.pgid.is_none());
            assert!(matches!(
                &entry.status,
                ControllerCleanupStatus::StorageError { .. }
            ));
        }
        for path in [&malformed, &oversized, &nonregular, &symlinked] {
            assert!(fs::symlink_metadata(path).is_ok(), "{}", path.display());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn multiple_started_callbacks_publish_concurrently_and_cleanup_all_reports_each() {
        let (_directory, control) = test_control();
        let first = spawn_isolated("exec /bin/sleep 30");
        let second = spawn_isolated("exec /bin/sleep 30");
        let first_pid = first.id() as i32;
        let second_pid = second.id() as i32;
        let barrier = Arc::new(Barrier::new(3));

        let first_reporter = control.reporter();
        let first_barrier = Arc::clone(&barrier);
        let first_started = std::thread::spawn(move || {
            first_barrier.wait();
            first_reporter.report(HarnessLifecycleEvent::Started {
                pid: first_pid as u32,
                pgid: first_pid,
            })
        });
        let second_reporter = control.reporter();
        let second_barrier = Arc::clone(&barrier);
        let second_started = std::thread::spawn(move || {
            second_barrier.wait();
            second_reporter.report(HarnessLifecycleEvent::Started {
                pid: second_pid as u32,
                pgid: second_pid,
            })
        });
        barrier.wait();
        first_started.join().unwrap().unwrap();
        second_started.join().unwrap().unwrap();
        assert_eq!(control.read_all().unwrap().valid.len(), 2);

        let first_reaper = std::thread::spawn(move || {
            let mut first = first;
            first.wait().unwrap()
        });
        let second_reaper = std::thread::spawn(move || {
            let mut second = second;
            second.wait().unwrap()
        });
        let report = control.cleanup_all().await.unwrap();
        assert_eq!(report.controllers.len(), 2);
        assert!(report.all_resolved(), "{report:?}");
        first_reaper.join().unwrap();
        second_reaper.join().unwrap();
        assert!(!process_group_alive(first_pid));
        assert!(!process_group_alive(second_pid));
        assert!(control.read_all().unwrap().valid.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn misnamed_controller_does_not_block_exact_cleanup_or_authorize_a_signal() {
        let (_directory, control) = test_control();
        let valid_child = spawn_isolated("exec /bin/sleep 30");
        let valid_pid = valid_child.id() as i32;
        let valid_controller = HarnessController {
            schema: CONTROLLER_SCHEMA,
            pid: valid_pid,
            pgid: valid_pid,
            started_at: Utc::now(),
            birth_fingerprint: process_birth_fingerprint(valid_pid).unwrap(),
        };
        control.publish(&valid_controller).unwrap();

        let mut decoy_child = spawn_isolated("exec /bin/sleep 30");
        let decoy_pid = decoy_child.id() as i32;
        let misnamed_controller = HarnessController {
            schema: CONTROLLER_SCHEMA,
            pid: decoy_pid,
            pgid: decoy_pid,
            started_at: Utc::now(),
            birth_fingerprint: process_birth_fingerprint(decoy_pid).unwrap(),
        };
        let misnamed_sidecar = "controller-misnamed.json";
        let misnamed_path = control.paths.run_dir.join(misnamed_sidecar);
        let bytes = serde_json::to_vec(&misnamed_controller).unwrap();
        atomic_publish(&control.paths.run_dir, &misnamed_path, &bytes).unwrap();

        let valid_reaper = std::thread::spawn(move || {
            let mut valid_child = valid_child;
            valid_child.wait().unwrap()
        });
        let report = control.cleanup_all().await.unwrap();
        valid_reaper.join().unwrap();

        let valid_entry = report
            .controllers
            .iter()
            .find(|entry| entry.pid == Some(valid_pid))
            .unwrap();
        assert!(valid_entry.status.resolved(), "{valid_entry:?}");
        let invalid_entry = report
            .controllers
            .iter()
            .find(|entry| entry.sidecar == misnamed_sidecar)
            .unwrap();
        assert!(invalid_entry.pid.is_none());
        assert!(invalid_entry.pgid.is_none());
        assert!(matches!(
            &invalid_entry.status,
            ControllerCleanupStatus::StorageError { .. }
        ));
        assert!(!report.all_resolved());
        assert!(!process_group_alive(valid_pid));
        assert!(fs::symlink_metadata(&misnamed_path).is_ok());

        let inventory = control.read_all().unwrap();
        assert!(inventory.valid.is_empty());
        assert_eq!(inventory.storage_errors.len(), 1);

        let decoy_was_alive = pid_alive(decoy_pid) && process_group_alive(decoy_pid);
        signal_group(decoy_pid, SIGKILL);
        decoy_child.wait().unwrap();
        assert!(
            decoy_was_alive,
            "misnamed sidecar must not authorize signalling"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancel_all_terminates_a_normal_exact_sentinel_and_removes_its_sidecar() {
        let (_directory, control) = test_control();
        let child = spawn_isolated("exec /bin/sleep 30");
        let pid = child.id() as i32;
        control
            .reporter()
            .report(HarnessLifecycleEvent::Started {
                pid: pid as u32,
                pgid: pid,
            })
            .unwrap();
        let reaper = std::thread::spawn(move || {
            let mut child = child;
            child.wait().unwrap()
        });

        let report = control.cancel_all().await.unwrap();
        assert_eq!(report.controllers.len(), 1);
        assert!(report.all_resolved(), "{report:?}");
        assert!(matches!(
            report.controllers[0].status,
            ControllerCleanupStatus::Terminated | ControllerCleanupStatus::Killed
        ));
        reaper.join().unwrap();
        assert!(!process_group_alive(pid));
        assert!(control.read_all().unwrap().valid.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn birth_mismatch_is_reported_and_never_signalled() {
        let (_directory, control) = test_control();
        let mut child = spawn_isolated("exec /bin/sleep 30");
        let pid = child.id() as i32;
        let actual = process_birth_fingerprint(pid).unwrap();
        let wrong = HarnessController {
            schema: CONTROLLER_SCHEMA,
            pid,
            pgid: pid,
            started_at: Utc::now(),
            birth_fingerprint: format!("{actual}-wrong"),
        };
        control.publish(&wrong).unwrap();

        let report = control.cleanup_all().await.unwrap();
        assert!(matches!(
            &report.controllers[0].status,
            ControllerCleanupStatus::RefusedIdentityMismatch { reason }
                if reason == "process birth fingerprint mismatch"
        ));
        assert!(process_group_alive(pid));
        assert!(pid_alive(pid));

        signal_group(pid, SIGKILL);
        child.wait().unwrap();
        assert!(!process_group_alive(pid));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn dead_sentinel_with_live_numeric_group_fails_closed() {
        let (_directory, control) = test_control();
        let mut leader = spawn_isolated(
            "( trap '' TERM; while :; do /bin/sleep 1; done ) & /bin/sleep 0.4; exit 0",
        );
        let pid = leader.id() as i32;
        let fingerprint = process_birth_fingerprint(pid).unwrap();
        let controller = HarnessController {
            schema: CONTROLLER_SCHEMA,
            pid,
            pgid: pid,
            started_at: Utc::now(),
            birth_fingerprint: fingerprint,
        };
        control.publish(&controller).unwrap();
        assert!(leader.wait().unwrap().success());
        assert!(!pid_alive(pid));
        assert!(process_group_alive(pid));

        let report = control
            .clone()
            .with_graces(Duration::from_millis(50), Duration::from_millis(50))
            .cleanup_all()
            .await
            .unwrap();
        assert_eq!(
            report.controllers[0].status,
            ControllerCleanupStatus::RefusedDeadSentinelLiveGroup
        );
        assert!(process_group_alive(pid));
        assert_eq!(control.read_all().unwrap().valid, vec![controller]);

        signal_group(pid, SIGKILL);
        let deadline = Instant::now() + Duration::from_secs(2);
        while process_group_alive(pid) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(!process_group_alive(pid));
    }
}
