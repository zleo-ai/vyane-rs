//! Exact Linux process-controller evidence for a future AgentRun host.
//!
//! This module owns only the durable controller sidecar and recovery adapter.
//! It does not submit AgentRuns, start a resident loop, or expose an API. A
//! caller must synchronously persist [`HarnessLifecycleEvent::Started`] before
//! releasing the harness start gate. Missing, malformed, stale, or ambiguous
//! evidence always fails closed.

use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::time::Instant;
use vyane_agent::{ControllerKind, ControllerRef};
use vyane_service::{
    AgentControllerAdapter, ControllerRecoveryContext, ControllerRecoveryObservation,
};

use crate::task::proc::{
    IdentityCheck, SIGKILL, SIGTERM, process_birth_fingerprint, process_group_alive, signal_group,
    verify_controller_identity,
};

const SIDECAR_SCHEMA: u32 = 2;
const MAX_SIDECAR_BYTES: u64 = 16 * 1024;
const MAX_SIDECARS: usize = 4_096;
const MAX_IDENTITY_BYTES: usize = 512;
const TERM_GRACE: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessControllerError {
    InvalidOwner,
    InvalidRoot,
    InvalidController,
    InvalidProcess,
    UnsafePath,
    Io,
    CorruptSidecar,
    ConflictingSidecar,
    IdentityUnavailable,
}

impl std::fmt::Display for ProcessControllerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidOwner => "process controller owner is invalid",
            Self::InvalidRoot => "process controller directory is invalid",
            Self::InvalidController => "process controller identity is invalid",
            Self::InvalidProcess => "process controller process identity is invalid",
            Self::UnsafePath => "process controller path is unsafe",
            Self::Io => "process controller storage is unavailable",
            Self::CorruptSidecar => "process controller sidecar is invalid",
            Self::ConflictingSidecar => "process controller sidecar conflicts",
            Self::IdentityUnavailable => "process controller identity is unavailable",
        })
    }
}

impl std::error::Error for ProcessControllerError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SidecarState {
    Reserved,
    Started,
    Stopped,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessControllerSidecar {
    schema: u32,
    owner_digest: String,
    controller_id: String,
    controller_fingerprint: String,
    run_id: String,
    worker_id: String,
    worker_generation: u64,
    pid: i32,
    pgid: i32,
    started_at: DateTime<Utc>,
    birth_fingerprint: String,
    state: SidecarState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BoundProcessController {
    pub(crate) controller: ControllerRef,
    pub(crate) run_id: String,
    pub(crate) worker_id: String,
    pub(crate) worker_generation: u64,
}

/// Fixed-owner private sidecar namespace.
#[derive(Clone)]
pub(crate) struct ProcessControllerStore {
    root: PathBuf,
    owner_digest: String,
}

impl std::fmt::Debug for ProcessControllerStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessControllerStore")
            .finish_non_exhaustive()
    }
}

impl ProcessControllerStore {
    pub(crate) fn open(
        root: impl Into<PathBuf>,
        owner: &str,
    ) -> Result<Self, ProcessControllerError> {
        if !valid_identity(owner, 256) {
            return Err(ProcessControllerError::InvalidOwner);
        }
        let root = root.into();
        ensure_private_directory(&root)?;
        Ok(Self {
            root,
            owner_digest: hex_digest(owner.as_bytes()),
        })
    }

    /// Create durable never-started evidence before the AgentRun store may
    /// publish this Process controller as `Running`.
    pub(crate) fn reserve(
        &self,
        controller: &ControllerRef,
        run_id: &str,
        worker_id: &str,
        worker_generation: u64,
    ) -> Result<(), ProcessControllerError> {
        let fingerprint = validate_process_controller(controller)?;
        if !valid_identity(run_id, MAX_IDENTITY_BYTES)
            || !valid_identity(worker_id, MAX_IDENTITY_BYTES)
            || worker_generation == 0
        {
            return Err(ProcessControllerError::InvalidController);
        }
        let reserved = ProcessControllerSidecar {
            schema: SIDECAR_SCHEMA,
            owner_digest: self.owner_digest.clone(),
            controller_id: controller.id.clone(),
            controller_fingerprint: fingerprint.to_owned(),
            run_id: run_id.to_owned(),
            worker_id: worker_id.to_owned(),
            worker_generation,
            pid: 0,
            pgid: 0,
            started_at: DateTime::<Utc>::UNIX_EPOCH,
            birth_fingerprint: String::new(),
            state: SidecarState::Reserved,
        };
        match self.read(controller) {
            Ok(current) if current == reserved => Ok(()),
            Ok(_) => Err(ProcessControllerError::ConflictingSidecar),
            Err(ProcessControllerError::Io) if !self.path(controller)?.exists() => {
                self.write_atomic(controller, &reserved)
            }
            Err(error) => Err(error),
        }
    }

    /// Enumerate only sidecars that validate against their content-addressed
    /// path and fixed owner. Unknown or unsafe directory entries fail closed.
    pub(crate) fn bound_controllers(
        &self,
    ) -> Result<Vec<BoundProcessController>, ProcessControllerError> {
        ensure_private_directory(&self.root)?;
        let entries = fs::read_dir(&self.root).map_err(|_| ProcessControllerError::Io)?;
        let mut bound = Vec::new();
        // This is a bounded best-effort GC batch, not an admission gate. A
        // large residue set must never make the daemon permanently unable to
        // start; later restarts may process another filesystem iteration.
        for entry in entries.take(MAX_SIDECARS) {
            let entry = entry.map_err(|_| ProcessControllerError::Io)?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| ProcessControllerError::UnsafePath)?;
            if name.starts_with(".sidecar-") && name.ends_with(".tmp") {
                continue;
            }
            if name.len() != 69 || !name.ends_with(".json") || !name[..64].bytes().all(is_lower_hex)
            {
                return Err(ProcessControllerError::UnsafePath);
            }
            let file = open_private_regular(&entry.path())?;
            let metadata = file.metadata().map_err(|_| ProcessControllerError::Io)?;
            if metadata.len() > MAX_SIDECAR_BYTES {
                return Err(ProcessControllerError::CorruptSidecar);
            }
            let mut bytes = Vec::with_capacity(metadata.len() as usize);
            file.take(MAX_SIDECAR_BYTES + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| ProcessControllerError::Io)?;
            let sidecar: ProcessControllerSidecar = serde_json::from_slice(&bytes)
                .map_err(|_| ProcessControllerError::CorruptSidecar)?;
            let controller = ControllerRef {
                kind: ControllerKind::Process,
                id: sidecar.controller_id.clone(),
                fingerprint: Some(sidecar.controller_fingerprint.clone()),
            };
            if self.path(&controller)? != entry.path() || self.read(&controller)? != sidecar {
                return Err(ProcessControllerError::CorruptSidecar);
            }
            bound.push(BoundProcessController {
                controller,
                run_id: sidecar.run_id,
                worker_id: sidecar.worker_id,
                worker_generation: sidecar.worker_generation,
            });
        }
        Ok(bound)
    }

    fn remove_terminal_residue(
        &self,
        controller: &ControllerRef,
    ) -> Result<bool, ProcessControllerError> {
        let sidecar = self.read(controller)?;
        let safe_to_remove = match sidecar.state {
            SidecarState::Reserved => true,
            SidecarState::Stopped => !process_group_alive(sidecar.pgid),
            SidecarState::Started => false,
        };
        if !safe_to_remove {
            return Ok(false);
        }
        self.remove(controller)?;
        Ok(true)
    }

    pub(crate) fn remove(&self, controller: &ControllerRef) -> Result<(), ProcessControllerError> {
        let _ = self.read(controller)?;
        fs::remove_file(self.path(controller)?).map_err(|_| ProcessControllerError::Io)?;
        sync_directory(&self.root)
    }

    /// Persist the exact OS identity reported by a newly spawned, still-gated
    /// harness controller. This must complete before the real target runs.
    pub(crate) fn record_started(
        &self,
        controller: &ControllerRef,
        pid: u32,
        pgid: i32,
        started_at: DateTime<Utc>,
    ) -> Result<(), ProcessControllerError> {
        let controller_fingerprint = validate_process_controller(controller)?;
        let pid = i32::try_from(pid).map_err(|_| ProcessControllerError::InvalidProcess)?;
        if pid <= 0 || pgid != pid {
            return Err(ProcessControllerError::InvalidProcess);
        }
        let birth_fingerprint = process_birth_fingerprint(pid)
            .filter(|value| valid_identity(value, MAX_IDENTITY_BYTES))
            .ok_or(ProcessControllerError::IdentityUnavailable)?;
        if verify_controller_identity(pid, pgid, started_at, Some(&birth_fingerprint))
            != IdentityCheck::Match
        {
            return Err(ProcessControllerError::IdentityUnavailable);
        }
        let current = self.read(controller)?;
        let next = ProcessControllerSidecar {
            schema: SIDECAR_SCHEMA,
            owner_digest: self.owner_digest.clone(),
            controller_id: controller.id.clone(),
            controller_fingerprint: controller_fingerprint.to_owned(),
            run_id: current.run_id.clone(),
            worker_id: current.worker_id.clone(),
            worker_generation: current.worker_generation,
            pid,
            pgid,
            started_at,
            birth_fingerprint,
            state: SidecarState::Started,
        };
        match current {
            current
                if current.schema == next.schema
                    && current.owner_digest == next.owner_digest
                    && current.controller_id == next.controller_id
                    && current.controller_fingerprint == next.controller_fingerprint
                    && current.pid == next.pid
                    && current.pgid == next.pgid
                    && current.birth_fingerprint == next.birth_fingerprint
                    && current.state == SidecarState::Started =>
            {
                Ok(())
            }
            // A failover chain may run another CLI harness only after the
            // preceding isolated group has been synchronously proved empty.
            // Replace that stopped observation with the next exact identity;
            // a live or unverifiable prior group always fails closed.
            current if current.state == SidecarState::Reserved => {
                self.write_atomic(controller, &next)
            }
            current
                if current.state == SidecarState::Stopped && !process_group_alive(current.pgid) =>
            {
                self.write_atomic(controller, &next)
            }
            _ => Err(ProcessControllerError::ConflictingSidecar),
        }
    }

    /// Record a terminal proof only when the reporter observed the complete
    /// isolated group disappear. `group_empty == false` deliberately leaves
    /// the started evidence intact for recovery.
    pub(crate) fn record_stopped(
        &self,
        controller: &ControllerRef,
        pid: u32,
        pgid: i32,
        group_empty: bool,
    ) -> Result<(), ProcessControllerError> {
        if !group_empty {
            return Ok(());
        }
        let pid = i32::try_from(pid).map_err(|_| ProcessControllerError::InvalidProcess)?;
        let mut current = self.read(controller)?;
        if current.pid != pid || current.pgid != pgid {
            return Err(ProcessControllerError::ConflictingSidecar);
        }
        if current.state == SidecarState::Stopped {
            return Ok(());
        }
        if process_group_alive(pgid) {
            return Err(ProcessControllerError::ConflictingSidecar);
        }
        current.state = SidecarState::Stopped;
        self.write_atomic(controller, &current)
    }

    fn read(
        &self,
        controller: &ControllerRef,
    ) -> Result<ProcessControllerSidecar, ProcessControllerError> {
        let expected_fingerprint = validate_process_controller(controller)?;
        let path = self.path(controller)?;
        let file = open_private_regular(&path)?;
        let metadata = file.metadata().map_err(|_| ProcessControllerError::Io)?;
        if metadata.len() > MAX_SIDECAR_BYTES {
            return Err(ProcessControllerError::CorruptSidecar);
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(MAX_SIDECAR_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| ProcessControllerError::Io)?;
        if bytes.len() as u64 > MAX_SIDECAR_BYTES {
            return Err(ProcessControllerError::CorruptSidecar);
        }
        let sidecar: ProcessControllerSidecar =
            serde_json::from_slice(&bytes).map_err(|_| ProcessControllerError::CorruptSidecar)?;
        let valid_process = sidecar.pid > 0
            && sidecar.pgid == sidecar.pid
            && valid_identity(&sidecar.birth_fingerprint, MAX_IDENTITY_BYTES);
        let valid_reservation = sidecar.pid == 0
            && sidecar.pgid == 0
            && sidecar.started_at == DateTime::<Utc>::UNIX_EPOCH
            && sidecar.birth_fingerprint.is_empty();
        if sidecar.schema != SIDECAR_SCHEMA
            || sidecar.owner_digest != self.owner_digest
            || sidecar.controller_id != controller.id
            || sidecar.controller_fingerprint != expected_fingerprint
            || !valid_identity(&sidecar.run_id, MAX_IDENTITY_BYTES)
            || !valid_identity(&sidecar.worker_id, MAX_IDENTITY_BYTES)
            || sidecar.worker_generation == 0
            || match sidecar.state {
                SidecarState::Reserved => !valid_reservation,
                SidecarState::Started | SidecarState::Stopped => !valid_process,
            }
        {
            return Err(ProcessControllerError::CorruptSidecar);
        }
        Ok(sidecar)
    }

    fn path(&self, controller: &ControllerRef) -> Result<PathBuf, ProcessControllerError> {
        let fingerprint = validate_process_controller(controller)?;
        let mut material = Vec::with_capacity(controller.id.len() + fingerprint.len() + 1);
        material.extend_from_slice(controller.id.as_bytes());
        material.push(0);
        material.extend_from_slice(fingerprint.as_bytes());
        Ok(self.root.join(format!("{}.json", hex_digest(&material))))
    }

    fn write_atomic(
        &self,
        controller: &ControllerRef,
        sidecar: &ProcessControllerSidecar,
    ) -> Result<(), ProcessControllerError> {
        ensure_private_directory(&self.root)?;
        let destination = self.path(controller)?;
        if destination.exists() {
            // Never normalize or overwrite unsafe pre-existing filesystem
            // objects. A regular private file is replaced atomically below.
            let existing = open_private_regular(&destination)?;
            drop(existing);
        }
        let bytes =
            serde_json::to_vec(sidecar).map_err(|_| ProcessControllerError::CorruptSidecar)?;
        if bytes.len() as u64 > MAX_SIDECAR_BYTES {
            return Err(ProcessControllerError::CorruptSidecar);
        }
        let random = uuid::Uuid::now_v7();
        let temporary = self.root.join(format!(".sidecar-{random}.tmp"));
        let result = (|| {
            let mut options = OpenOptions::new();
            options
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
            let mut file = options
                .open(&temporary)
                .map_err(|_| ProcessControllerError::Io)?;
            file.write_all(&bytes)
                .and_then(|()| file.sync_all())
                .map_err(|_| ProcessControllerError::Io)?;
            validate_private_regular(&file.metadata().map_err(|_| ProcessControllerError::Io)?)?;
            fs::rename(&temporary, &destination).map_err(|_| ProcessControllerError::Io)?;
            sync_directory(&self.root)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }
}

/// Exact Process recovery adapter over one fixed-owner sidecar namespace.
#[derive(Clone)]
pub(crate) struct ProcessAgentControllerAdapter {
    sidecars: ProcessControllerStore,
}

impl std::fmt::Debug for ProcessAgentControllerAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessAgentControllerAdapter")
            .finish_non_exhaustive()
    }
}

impl ProcessAgentControllerAdapter {
    pub(crate) fn new(sidecars: ProcessControllerStore) -> Self {
        Self { sidecars }
    }

    pub(crate) async fn stop_exact(
        &self,
        deadline: Instant,
        controller: ControllerRef,
    ) -> ControllerRecoveryObservation {
        self.observe_until(deadline, controller).await
    }

    /// Retire only persisted never-started or already-stopped proof. This is
    /// intentionally observation-only and never signals a process.
    pub(crate) fn remove_terminal_residue(&self, controller: &ControllerRef) -> bool {
        self.sidecars
            .remove_terminal_residue(controller)
            .unwrap_or(false)
    }

    async fn observe_until(
        &self,
        deadline: Instant,
        controller: ControllerRef,
    ) -> ControllerRecoveryObservation {
        let mut sidecar = match self.sidecars.read(&controller) {
            Ok(sidecar) => sidecar,
            Err(_) => return ControllerRecoveryObservation::Unavailable,
        };
        if sidecar.state == SidecarState::Reserved {
            return ControllerRecoveryObservation::Gone;
        }
        if sidecar.state == SidecarState::Stopped {
            return if process_group_alive(sidecar.pgid) {
                ControllerRecoveryObservation::Unavailable
            } else {
                ControllerRecoveryObservation::Gone
            };
        }

        match exact_identity(&sidecar) {
            ExactIdentity::Gone => return self.persist_gone(&controller, &mut sidecar),
            ExactIdentity::Present => {}
            ExactIdentity::Uncertain => return ControllerRecoveryObservation::Unavailable,
        }
        if Instant::now() >= deadline {
            return ControllerRecoveryObservation::StillPresent;
        }

        // Revalidate immediately before every external control effect.
        if exact_identity(&sidecar) != ExactIdentity::Present {
            return ControllerRecoveryObservation::Unavailable;
        }
        signal_group(sidecar.pgid, SIGTERM);
        let term_deadline = deadline.min(Instant::now() + TERM_GRACE);
        loop {
            match exact_identity(&sidecar) {
                ExactIdentity::Gone => return self.persist_gone(&controller, &mut sidecar),
                ExactIdentity::Uncertain => return ControllerRecoveryObservation::Unavailable,
                ExactIdentity::Present if Instant::now() >= term_deadline => break,
                ExactIdentity::Present => tokio::time::sleep(POLL_INTERVAL).await,
            }
        }

        if exact_identity(&sidecar) != ExactIdentity::Present {
            return ControllerRecoveryObservation::Unavailable;
        }
        signal_group(sidecar.pgid, SIGKILL);
        loop {
            match exact_identity(&sidecar) {
                ExactIdentity::Gone => return self.persist_gone(&controller, &mut sidecar),
                ExactIdentity::Uncertain => return ControllerRecoveryObservation::Unavailable,
                ExactIdentity::Present if Instant::now() >= deadline => {
                    return ControllerRecoveryObservation::StillPresent;
                }
                ExactIdentity::Present => tokio::time::sleep(POLL_INTERVAL).await,
            }
        }
    }

    fn persist_gone(
        &self,
        controller: &ControllerRef,
        sidecar: &mut ProcessControllerSidecar,
    ) -> ControllerRecoveryObservation {
        if process_group_alive(sidecar.pgid) {
            return ControllerRecoveryObservation::Unavailable;
        }
        sidecar.state = SidecarState::Stopped;
        if self.sidecars.write_atomic(controller, sidecar).is_ok() {
            ControllerRecoveryObservation::Gone
        } else {
            ControllerRecoveryObservation::Unavailable
        }
    }
}

#[async_trait]
impl AgentControllerAdapter for ProcessAgentControllerAdapter {
    fn name(&self) -> &str {
        "linux-process-sidecar-v1"
    }

    fn kind(&self) -> ControllerKind {
        ControllerKind::Process
    }

    async fn observe_gone(
        &self,
        context: ControllerRecoveryContext,
        controller: ControllerRef,
    ) -> ControllerRecoveryObservation {
        self.observe_until(context.deadline(), controller).await
    }

    fn confirmed_gone(&self, controller: &ControllerRef) {
        let _ = self.sidecars.remove(controller);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExactIdentity {
    Present,
    Gone,
    Uncertain,
}

fn exact_identity(sidecar: &ProcessControllerSidecar) -> ExactIdentity {
    match verify_controller_identity(
        sidecar.pid,
        sidecar.pgid,
        sidecar.started_at,
        Some(&sidecar.birth_fingerprint),
    ) {
        IdentityCheck::Match => ExactIdentity::Present,
        IdentityCheck::Dead if !process_group_alive(sidecar.pgid) => ExactIdentity::Gone,
        IdentityCheck::Dead | IdentityCheck::Mismatch(_) => ExactIdentity::Uncertain,
    }
}

fn validate_process_controller(controller: &ControllerRef) -> Result<&str, ProcessControllerError> {
    if controller.kind != ControllerKind::Process
        || !valid_identity(&controller.id, MAX_IDENTITY_BYTES)
    {
        return Err(ProcessControllerError::InvalidController);
    }
    controller
        .fingerprint
        .as_deref()
        .filter(|value| valid_identity(value, MAX_IDENTITY_BYTES))
        .ok_or(ProcessControllerError::InvalidController)
}

fn valid_identity(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value.trim() == value
        && !value.contains('\0')
        && !value.chars().any(char::is_control)
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn hex_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn ensure_private_directory(path: &Path) -> Result<(), ProcessControllerError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink()
                || !metadata.is_dir()
                || metadata.permissions().mode() & 0o7777 != 0o700
            {
                return Err(ProcessControllerError::InvalidRoot);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(|_| ProcessControllerError::Io)?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                .map_err(|_| ProcessControllerError::Io)?;
        }
        Err(_) => return Err(ProcessControllerError::Io),
    }
    let metadata = fs::symlink_metadata(path).map_err(|_| ProcessControllerError::Io)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.permissions().mode() & 0o7777 != 0o700
    {
        return Err(ProcessControllerError::InvalidRoot);
    }
    Ok(())
}

fn open_private_regular(path: &Path) -> Result<File, ProcessControllerError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
    let file = options.open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ProcessControllerError::Io
        } else {
            ProcessControllerError::UnsafePath
        }
    })?;
    validate_private_regular(&file.metadata().map_err(|_| ProcessControllerError::Io)?)?;
    Ok(file)
}

fn validate_private_regular(metadata: &fs::Metadata) -> Result<(), ProcessControllerError> {
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o7777 != 0o600
    {
        return Err(ProcessControllerError::UnsafePath);
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), ProcessControllerError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW | nix::libc::O_DIRECTORY);
    options
        .open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| ProcessControllerError::Io)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::process::Command;

    use super::*;
    use crate::task::proc::{pid_alive, spawn_in_session};

    fn controller(id: &str) -> ControllerRef {
        ControllerRef {
            kind: ControllerKind::Process,
            id: id.into(),
            fingerprint: Some(format!("fingerprint-{id}")),
        }
    }

    fn fixture() -> (tempfile::TempDir, ProcessControllerStore) {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("controllers");
        let store = ProcessControllerStore::open(&root, "owner-a").unwrap();
        (directory, store)
    }

    fn spawn_sleep() -> std::process::Child {
        let program = ["/bin/sleep", "/usr/bin/sleep"]
            .into_iter()
            .find(|path| Path::new(path).is_file())
            .unwrap();
        let mut command = Command::new(program);
        command.arg("30");
        spawn_in_session(command).unwrap()
    }

    fn record_live(store: &ProcessControllerStore, controller: &ControllerRef, pid: u32) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while process_birth_fingerprint(pid as i32).is_none() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        if store.read(controller).is_err() {
            store.reserve(controller, "run-a", "worker-a", 1).unwrap();
        }
        store
            .record_started(controller, pid, pid as i32, Utc::now())
            .unwrap();
    }

    #[tokio::test]
    async fn reserved_controller_is_safe_never_started_proof_and_removed_after_confirmation() {
        let (_directory, store) = fixture();
        let control = controller("reserved");
        store.reserve(&control, "run-a", "worker-a", 1).unwrap();
        store.reserve(&control, "run-a", "worker-a", 1).unwrap();
        assert_eq!(
            store.bound_controllers().unwrap(),
            vec![BoundProcessController {
                controller: control.clone(),
                run_id: "run-a".into(),
                worker_id: "worker-a".into(),
                worker_generation: 1,
            }]
        );
        assert_eq!(
            store.reserve(&control, "run-b", "worker-a", 1),
            Err(ProcessControllerError::ConflictingSidecar)
        );
        assert_eq!(store.read(&control).unwrap().state, SidecarState::Reserved);

        let adapter = ProcessAgentControllerAdapter::new(store.clone());
        assert_eq!(
            adapter
                .observe_until(Instant::now() + Duration::from_millis(100), control.clone())
                .await,
            ControllerRecoveryObservation::Gone
        );
        assert!(store.path(&control).unwrap().exists());
        adapter.confirmed_gone(&control);
        assert!(!store.path(&control).unwrap().exists());
    }

    #[test]
    fn sidecar_namespace_and_file_are_private_and_idempotent() {
        let (_directory, store) = fixture();
        let mut child = spawn_sleep();
        let control = controller("private");
        record_live(&store, &control, child.id());
        record_live(&store, &control, child.id());

        let root_mode = fs::metadata(&store.root).unwrap().permissions().mode() & 0o7777;
        let path = store.path(&control).unwrap();
        let metadata = fs::metadata(path).unwrap();
        assert_eq!(root_mode, 0o700);
        assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
        assert_eq!(metadata.nlink(), 1);
        assert_eq!(format!("{store:?}"), "ProcessControllerStore { .. }");

        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn terminal_residue_cleanup_never_removes_started_evidence() {
        let (_directory, store) = fixture();
        let mut child = spawn_sleep();
        let control = controller("terminal-residue");
        record_live(&store, &control, child.id());
        assert!(!store.remove_terminal_residue(&control).unwrap());
        assert!(store.path(&control).unwrap().exists());

        let pid = child.id();
        child.kill().unwrap();
        child.wait().unwrap();
        store
            .record_stopped(&control, pid, pid as i32, true)
            .unwrap();
        assert!(store.remove_terminal_residue(&control).unwrap());
        assert!(!store.path(&control).unwrap().exists());
    }

    #[test]
    fn symlink_and_hardlink_sidecars_are_rejected() {
        use std::os::unix::fs::symlink;

        let (directory, store) = fixture();
        let control = controller("unsafe");
        let path = store.path(&control).unwrap();
        let target = directory.path().join("target");
        fs::write(&target, b"{}").unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        symlink(&target, &path).unwrap();
        assert!(matches!(
            store.read(&control),
            Err(ProcessControllerError::UnsafePath)
        ));
        fs::remove_file(&path).unwrap();
        fs::hard_link(&target, &path).unwrap();
        assert!(matches!(
            store.read(&control),
            Err(ProcessControllerError::UnsafePath)
        ));
    }

    #[tokio::test]
    async fn exact_recovery_stops_the_group_and_persists_gone() {
        let (_directory, store) = fixture();
        let child = spawn_sleep();
        let pid = child.id() as i32;
        let control = controller("recover");
        record_live(&store, &control, child.id());
        let waiter = std::thread::spawn(move || {
            let mut child = child;
            child.wait().unwrap()
        });
        let adapter = ProcessAgentControllerAdapter::new(store.clone());

        let result = adapter
            .observe_until(Instant::now() + Duration::from_secs(5), control.clone())
            .await;
        assert_eq!(result, ControllerRecoveryObservation::Gone);
        let _ = waiter.join().unwrap();
        assert!(!pid_alive(pid));
        assert_eq!(store.read(&control).unwrap().state, SidecarState::Stopped);
        assert_eq!(
            adapter
                .observe_until(Instant::now() + Duration::from_secs(1), control)
                .await,
            ControllerRecoveryObservation::Gone
        );
    }

    #[tokio::test]
    async fn missing_or_drifted_identity_is_unavailable_without_signalling() {
        let (_directory, store) = fixture();
        let adapter = ProcessAgentControllerAdapter::new(store.clone());
        assert_eq!(
            adapter
                .observe_until(
                    Instant::now() + Duration::from_millis(100),
                    controller("missing")
                )
                .await,
            ControllerRecoveryObservation::Unavailable
        );

        let mut child = spawn_sleep();
        let pid = child.id() as i32;
        let control = controller("drift");
        record_live(&store, &control, child.id());
        let mut sidecar = store.read(&control).unwrap();
        sidecar.birth_fingerprint = "linux:unrelated:0".into();
        store.write_atomic(&control, &sidecar).unwrap();
        assert_eq!(
            adapter
                .observe_until(Instant::now() + Duration::from_millis(100), control)
                .await,
            ControllerRecoveryObservation::Unavailable
        );
        assert!(pid_alive(pid));
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn stopped_requires_matching_identity_and_an_empty_group() {
        let (_directory, store) = fixture();
        let mut child = spawn_sleep();
        let control = controller("stopped");
        record_live(&store, &control, child.id());
        assert!(
            store
                .record_stopped(&control, child.id() + 1, child.id() as i32, true)
                .is_err()
        );
        assert!(
            store
                .record_stopped(&control, child.id(), child.id() as i32, true)
                .is_err()
        );
        store
            .record_stopped(&control, child.id(), child.id() as i32, false)
            .unwrap();
        assert_eq!(store.read(&control).unwrap().state, SidecarState::Started);
        let _ = child.kill();
        let _ = child.wait();
        store
            .record_stopped(&control, child.id(), child.id() as i32, true)
            .unwrap();
        assert_eq!(store.read(&control).unwrap().state, SidecarState::Stopped);
    }

    #[test]
    fn stopped_sidecar_can_advance_to_the_next_failover_process() {
        let (_directory, store) = fixture();
        let control = controller("failover");
        let mut first = spawn_sleep();
        let first_pid = first.id();
        record_live(&store, &control, first_pid);
        first.kill().unwrap();
        first.wait().unwrap();
        store
            .record_stopped(&control, first_pid, first_pid as i32, true)
            .unwrap();

        let mut second = spawn_sleep();
        let second_pid = second.id();
        record_live(&store, &control, second_pid);
        let current = store.read(&control).unwrap();
        assert_eq!(current.state, SidecarState::Started);
        assert_eq!(current.pid, second_pid as i32);
        assert_eq!(current.pgid, second_pid as i32);
        assert_ne!(current.pid, first_pid as i32);

        second.kill().unwrap();
        second.wait().unwrap();
    }

    #[test]
    fn unsafe_existing_directory_is_not_repaired() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("controllers");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o750)).unwrap();
        assert!(matches!(
            ProcessControllerStore::open(root, "owner"),
            Err(ProcessControllerError::InvalidRoot)
        ));
    }
}
