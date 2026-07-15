//! Private, one-shot input storage for fresh AgentRun harness execution.
//!
//! The spool is deliberately narrower than the detached-task `JobSpec`: it
//! cannot represent a logical session or resume authority. Caller-selected
//! identities are bound inside the envelope but never appear verbatim in its
//! filesystem path.

use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use vyane_kernel::CapabilityPlanSnapshot;

use crate::task::store::TargetSnapshot;

const INPUT_SCHEMA: u32 = 1;
const MAX_INPUT_BYTES: u64 = 1024 * 1024;
const MAX_PROMPT_BYTES: usize = 768 * 1024;
const MAX_SYSTEM_BYTES: usize = 128 * 1024;
const MAX_ID_BYTES: usize = 256;
const MAX_TARGET_BYTES: usize = 512;
const MAX_PATH_BYTES: usize = 4096;
const MAX_LABELS: usize = 64;
const MAX_LABEL_BYTES: usize = 256;
const MAX_LABEL_TOTAL_BYTES: usize = 8 * 1024;
const MAX_TARGET_SNAPSHOTS: usize = 16;
const MAX_TIMEOUT_SECONDS: u64 = 7 * 24 * 60 * 60;

const OWNER_PATH_DOMAIN: &[u8] = b"vyane.agent-input.owner-path.v1\0";
const RUN_PATH_DOMAIN: &[u8] = b"vyane.agent-input.run-path.v1\0";
const PROMPT_DOMAIN: &[u8] = b"vyane.agent-input.prompt.v1\0";
const POLICY_DOMAIN: &[u8] = b"vyane.agent-input.policy.v1\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AgentSpoolError {
    InvalidInput,
    InvalidRoot,
    UnsafePath,
    TooLarge,
    CorruptInput,
    DigestMismatch,
    BindingMismatch,
    ConflictingInput,
    NotFound,
    Io,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AgentSpoolCreate {
    Created,
    ExistingExact,
}

impl std::fmt::Display for AgentSpoolError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidInput => "agent input is invalid",
            Self::InvalidRoot => "agent input directory is invalid",
            Self::UnsafePath => "agent input storage is unsafe",
            Self::TooLarge => "agent input exceeds its size limit",
            Self::CorruptInput => "stored agent input is invalid",
            Self::DigestMismatch => "stored agent input digest does not match",
            Self::BindingMismatch => "stored agent input identity does not match",
            Self::ConflictingInput => "a different agent input already exists",
            Self::NotFound => "agent input is not available",
            Self::Io => "agent input storage is unavailable",
        })
    }
}

impl std::error::Error for AgentSpoolError {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AgentSpoolSandbox {
    #[default]
    ReadOnly,
    Write,
    Full,
}

/// Frozen execution policy covered by `policy_sha256`.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AgentSpoolPolicy {
    pub(crate) target: String,
    pub(crate) sandbox: AgentSpoolSandbox,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) workdir: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) system: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) timeout_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) config: Option<PathBuf>,
    /// Secret-free exact target chain approved at submission time.
    pub(crate) target_snapshot: Vec<TargetSnapshot>,
    /// Exact capability and workdir identity approved at submission time.
    pub(crate) capability_plan: CapabilityPlanSnapshot,
}

impl std::fmt::Debug for AgentSpoolPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentSpoolPolicy")
            .field("target", &"[REDACTED]")
            .field("sandbox", &self.sandbox)
            .field("workdir", &self.workdir.as_ref().map(|_| "[REDACTED]"))
            .field("system", &self.system.as_ref().map(|_| "[REDACTED]"))
            .field("timeout_seconds", &self.timeout_seconds)
            .field("labels", &format_args!("[{} labels]", self.labels.len()))
            .field("config", &self.config.as_ref().map(|_| "[REDACTED]"))
            .field(
                "target_snapshot",
                &format_args!("[{} targets]", self.target_snapshot.len()),
            )
            .field("capability_plan", &"[OPAQUE]")
            .finish()
    }
}

/// Strict, fresh/sessionless request consumed by the resident host.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AgentSpoolInput {
    schema: u32,
    pub(crate) owner: String,
    pub(crate) run_id: String,
    pub(crate) worker_id: String,
    pub(crate) prompt: String,
    pub(crate) prompt_sha256: String,
    pub(crate) policy: AgentSpoolPolicy,
    pub(crate) policy_sha256: String,
}

impl std::fmt::Debug for AgentSpoolInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentSpoolInput")
            .field("schema", &self.schema)
            .field("identity", &"[REDACTED]")
            .field("prompt", &"[REDACTED]")
            .field("prompt_sha256", &"[OPAQUE]")
            .field("policy", &self.policy)
            .field("policy_sha256", &"[OPAQUE]")
            .finish()
    }
}

impl AgentSpoolInput {
    pub(crate) fn fresh(
        owner: impl Into<String>,
        run_id: impl Into<String>,
        worker_id: impl Into<String>,
        prompt: impl Into<String>,
        policy: AgentSpoolPolicy,
    ) -> Result<Self, AgentSpoolError> {
        let prompt = prompt.into();
        let prompt_sha256 = prompt_digest(&prompt);
        let policy_sha256 = policy_digest(&policy)?;
        let input = Self {
            schema: INPUT_SCHEMA,
            owner: owner.into(),
            run_id: run_id.into(),
            worker_id: worker_id.into(),
            prompt,
            prompt_sha256,
            policy,
            policy_sha256,
        };
        input.validate()?;
        Ok(input)
    }

    fn validate(&self) -> Result<(), AgentSpoolError> {
        if self.schema != INPUT_SCHEMA
            || !valid_text(&self.owner, MAX_ID_BYTES)
            || !valid_text(&self.run_id, MAX_ID_BYTES)
            || !valid_text(&self.worker_id, MAX_ID_BYTES)
            || self.prompt.is_empty()
            || self.prompt.len() > MAX_PROMPT_BYTES
        {
            return Err(AgentSpoolError::InvalidInput);
        }
        validate_policy(&self.policy)?;
        if !valid_digest(&self.prompt_sha256) || !valid_digest(&self.policy_sha256) {
            return Err(AgentSpoolError::InvalidInput);
        }
        if self.prompt_sha256 != prompt_digest(&self.prompt)
            || self.policy_sha256 != policy_digest(&self.policy)?
        {
            return Err(AgentSpoolError::DigestMismatch);
        }
        let encoded = serde_json::to_vec(self).map_err(|_| AgentSpoolError::InvalidInput)?;
        if encoded.len() as u64 > MAX_INPUT_BYTES {
            return Err(AgentSpoolError::TooLarge);
        }
        Ok(())
    }
}

/// One fixed-owner namespace below the private spool root.
#[derive(Clone)]
pub(crate) struct AgentInputSpool {
    root: PathBuf,
    owner: String,
    owner_root: PathBuf,
}

impl std::fmt::Debug for AgentInputSpool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentInputSpool")
            .field("root", &"[REDACTED]")
            .field("owner", &"[REDACTED]")
            .finish()
    }
}

impl AgentInputSpool {
    pub(crate) fn open(
        root: impl Into<PathBuf>,
        owner: impl Into<String>,
    ) -> Result<Self, AgentSpoolError> {
        let root = root.into();
        let owner = owner.into();
        if !valid_text(&owner, MAX_ID_BYTES) {
            return Err(AgentSpoolError::InvalidInput);
        }
        reject_symlink_ancestors(&root)?;
        ensure_private_directory(&root, AgentSpoolError::InvalidRoot)?;
        let owner_root = root.join(hex_digest(OWNER_PATH_DOMAIN, &[owner.as_bytes()]));
        ensure_private_directory(&owner_root, AgentSpoolError::UnsafePath)?;
        Ok(Self {
            root,
            owner,
            owner_root,
        })
    }

    pub(crate) fn create(
        &self,
        input: &AgentSpoolInput,
    ) -> Result<AgentSpoolCreate, AgentSpoolError> {
        self.validate_binding(input)?;
        let bytes = serde_json::to_vec(input).map_err(|_| AgentSpoolError::InvalidInput)?;
        if bytes.len() as u64 > MAX_INPUT_BYTES {
            return Err(AgentSpoolError::TooLarge);
        }
        self.revalidate_directories()?;
        let destination = self.input_path(&input.run_id)?;
        let temporary = self
            .owner_root
            .join(format!(".input-{}.tmp", uuid::Uuid::now_v7()));
        let result = (|| {
            let mut options = OpenOptions::new();
            options
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
            let mut file = options.open(&temporary).map_err(|_| AgentSpoolError::Io)?;
            use std::os::unix::fs::PermissionsExt as _;
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(|_| AgentSpoolError::Io)?;
            file.write_all(&bytes)
                .and_then(|()| file.sync_all())
                .map_err(|_| AgentSpoolError::Io)?;
            validate_private_regular(&file.metadata().map_err(|_| AgentSpoolError::Io)?)?;
            match nix::fcntl::renameat2(
                None,
                &temporary,
                None,
                &destination,
                nix::fcntl::RenameFlags::RENAME_NOREPLACE,
            ) {
                Ok(()) => {
                    sync_directory(&self.owner_root)?;
                    let published = open_private_regular(&destination)?;
                    validate_private_regular(
                        &published.metadata().map_err(|_| AgentSpoolError::Io)?,
                    )?;
                    Ok(AgentSpoolCreate::Created)
                }
                Err(nix::errno::Errno::EEXIST) => {
                    match self.read(&input.run_id, &input.worker_id) {
                        Ok(existing) if existing == *input => Ok(AgentSpoolCreate::ExistingExact),
                        Ok(_) | Err(AgentSpoolError::BindingMismatch) => {
                            Err(AgentSpoolError::ConflictingInput)
                        }
                        Err(error) => Err(error),
                    }
                }
                Err(_) => Err(AgentSpoolError::Io),
            }
        })();
        if temporary.exists() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    pub(crate) fn read(
        &self,
        run_id: &str,
        worker_id: &str,
    ) -> Result<AgentSpoolInput, AgentSpoolError> {
        if !valid_text(run_id, MAX_ID_BYTES) || !valid_text(worker_id, MAX_ID_BYTES) {
            return Err(AgentSpoolError::InvalidInput);
        }
        self.revalidate_directories()?;
        let path = self.input_path(run_id)?;
        let file = match open_private_regular(&path) {
            Err(AgentSpoolError::NotFound) => return Err(AgentSpoolError::NotFound),
            result => result?,
        };
        let metadata = file.metadata().map_err(|_| AgentSpoolError::Io)?;
        if metadata.len() > MAX_INPUT_BYTES {
            return Err(AgentSpoolError::TooLarge);
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(MAX_INPUT_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| AgentSpoolError::Io)?;
        if bytes.len() as u64 > MAX_INPUT_BYTES {
            return Err(AgentSpoolError::TooLarge);
        }
        let input: AgentSpoolInput =
            serde_json::from_slice(&bytes).map_err(|_| AgentSpoolError::CorruptInput)?;
        input.validate().map_err(|error| match error {
            AgentSpoolError::DigestMismatch => AgentSpoolError::DigestMismatch,
            AgentSpoolError::TooLarge => AgentSpoolError::TooLarge,
            _ => AgentSpoolError::CorruptInput,
        })?;
        if input.owner != self.owner || input.run_id != run_id || input.worker_id != worker_id {
            return Err(AgentSpoolError::BindingMismatch);
        }
        Ok(input)
    }

    /// Remove one bound request. Missing input is an idempotent success.
    pub(crate) fn remove(&self, run_id: &str, worker_id: &str) -> Result<(), AgentSpoolError> {
        match self.read(run_id, worker_id) {
            Ok(_) => {}
            Err(AgentSpoolError::NotFound) => return Ok(()),
            Err(error) => return Err(error),
        }
        let path = self.input_path(run_id)?;
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(AgentSpoolError::Io),
        }
        sync_directory(&self.owner_root)
    }

    fn validate_binding(&self, input: &AgentSpoolInput) -> Result<(), AgentSpoolError> {
        input.validate()?;
        if input.owner != self.owner {
            return Err(AgentSpoolError::BindingMismatch);
        }
        Ok(())
    }

    fn input_path(&self, run_id: &str) -> Result<PathBuf, AgentSpoolError> {
        if !valid_text(run_id, MAX_ID_BYTES) {
            return Err(AgentSpoolError::InvalidInput);
        }
        let key = hex_digest(RUN_PATH_DOMAIN, &[self.owner.as_bytes(), run_id.as_bytes()]);
        Ok(self.owner_root.join(format!("{key}.json")))
    }

    fn revalidate_directories(&self) -> Result<(), AgentSpoolError> {
        validate_private_directory(&self.root, AgentSpoolError::InvalidRoot)?;
        validate_private_directory(&self.owner_root, AgentSpoolError::UnsafePath)
    }
}

fn validate_policy(policy: &AgentSpoolPolicy) -> Result<(), AgentSpoolError> {
    if !valid_text(&policy.target, MAX_TARGET_BYTES)
        || policy
            .system
            .as_ref()
            .is_some_and(|value| value.len() > MAX_SYSTEM_BYTES)
        || policy
            .timeout_seconds
            .is_some_and(|seconds| seconds == 0 || seconds > MAX_TIMEOUT_SECONDS)
        || policy.labels.len() > MAX_LABELS
        || policy.target_snapshot.is_empty()
        || policy.target_snapshot.len() > MAX_TARGET_SNAPSHOTS
        || policy
            .labels
            .iter()
            .any(|label| !valid_text(label, MAX_LABEL_BYTES))
        || policy.labels.iter().map(String::len).sum::<usize>() > MAX_LABEL_TOTAL_BYTES
    {
        return Err(AgentSpoolError::InvalidInput);
    }
    if let Some(path) = &policy.workdir {
        validate_path(path)?;
    }
    if let Some(path) = &policy.config {
        validate_path(path)?;
    }
    let sandbox = match policy.sandbox {
        AgentSpoolSandbox::ReadOnly => vyane_core::Sandbox::ReadOnly,
        AgentSpoolSandbox::Write => vyane_core::Sandbox::Write,
        AgentSpoolSandbox::Full => vyane_core::Sandbox::Full,
    };
    if policy.capability_plan.requested_sandbox != sandbox
        || policy.capability_plan.targets.len() != policy.target_snapshot.len()
        || policy
            .capability_plan
            .targets
            .iter()
            .zip(&policy.target_snapshot)
            .any(|(capability, target)| capability.target != target.target)
    {
        return Err(AgentSpoolError::InvalidInput);
    }
    match policy.sandbox {
        AgentSpoolSandbox::ReadOnly => {
            if policy.capability_plan.requires_inherited_workdir
                || policy.capability_plan.canonical_workdir.is_some()
                || policy.capability_plan.workdir_identity.is_some()
            {
                return Err(AgentSpoolError::InvalidInput);
            }
        }
        AgentSpoolSandbox::Write | AgentSpoolSandbox::Full => {
            if !policy.capability_plan.requires_inherited_workdir
                || policy.capability_plan.canonical_workdir.as_ref() != policy.workdir.as_ref()
                || policy.capability_plan.workdir_identity.is_none()
            {
                return Err(AgentSpoolError::InvalidInput);
            }
        }
    }
    Ok(())
}

fn validate_path(path: &Path) -> Result<(), AgentSpoolError> {
    let value = path.to_str().ok_or(AgentSpoolError::InvalidInput)?;
    if value.is_empty() || value.len() > MAX_PATH_BYTES {
        return Err(AgentSpoolError::InvalidInput);
    }
    Ok(())
}

fn valid_text(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

fn prompt_digest(prompt: &str) -> String {
    hex_digest(PROMPT_DOMAIN, &[prompt.as_bytes()])
}

fn policy_digest(policy: &AgentSpoolPolicy) -> Result<String, AgentSpoolError> {
    let bytes = serde_json::to_vec(policy).map_err(|_| AgentSpoolError::InvalidInput)?;
    Ok(hex_digest(POLICY_DOMAIN, &[&bytes]))
}

fn hex_digest(domain: &[u8], values: &[&[u8]]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    for value in values {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn ensure_private_directory(path: &Path, invalid: AgentSpoolError) -> Result<(), AgentSpoolError> {
    match fs::symlink_metadata(path) {
        Ok(_) => validate_private_directory(path, invalid),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(path) {
                Ok(()) => {
                    use std::os::unix::fs::PermissionsExt as _;

                    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                        .map_err(|_| AgentSpoolError::Io)?;
                    let parent = path.parent().ok_or(invalid)?;
                    sync_directory(parent)?;
                    sync_directory(path)?;
                    validate_private_directory(path, invalid)
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    validate_private_directory(path, invalid)
                }
                Err(_) => Err(AgentSpoolError::Io),
            }
        }
        Err(_) => Err(AgentSpoolError::Io),
    }
}

fn reject_symlink_ancestors(path: &Path) -> Result<(), AgentSpoolError> {
    for ancestor in path.ancestors() {
        match fs::symlink_metadata(ancestor) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(AgentSpoolError::InvalidRoot);
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(AgentSpoolError::Io),
        }
    }
    Ok(())
}

fn validate_private_directory(
    path: &Path,
    invalid: AgentSpoolError,
) -> Result<(), AgentSpoolError> {
    use std::os::unix::fs::PermissionsExt as _;

    let before = fs::symlink_metadata(path).map_err(|_| invalid)?;
    if !before.file_type().is_dir() || before.permissions().mode() & 0o7777 != 0o700 {
        return Err(invalid);
    }
    let opened = File::open(path).map_err(|_| invalid)?;
    let after = opened.metadata().map_err(|_| invalid)?;
    if !after.file_type().is_dir() || before.dev() != after.dev() || before.ino() != after.ino() {
        return Err(invalid);
    }
    Ok(())
}

fn open_private_regular(path: &Path) -> Result<File, AgentSpoolError> {
    let before = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(AgentSpoolError::NotFound);
        }
        Err(_) => return Err(AgentSpoolError::Io),
    };
    if !before.file_type().is_file() {
        return Err(AgentSpoolError::UnsafePath);
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
    let file = options
        .open(path)
        .map_err(|_| AgentSpoolError::UnsafePath)?;
    let after = file.metadata().map_err(|_| AgentSpoolError::Io)?;
    if before.dev() != after.dev() || before.ino() != after.ino() {
        return Err(AgentSpoolError::UnsafePath);
    }
    validate_private_regular(&after)?;
    Ok(file)
}

fn validate_private_regular(metadata: &fs::Metadata) -> Result<(), AgentSpoolError> {
    use std::os::unix::fs::PermissionsExt as _;

    if !metadata.file_type().is_file()
        || metadata.permissions().mode() & 0o7777 != 0o600
        || metadata.nlink() != 1
    {
        return Err(AgentSpoolError::UnsafePath);
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), AgentSpoolError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| AgentSpoolError::Io)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::task::store::GenParamsSnapshot;
    use vyane_core::{
        AdapterTransport, HarnessKind, ModelId, Protocol, ProviderId, Target, WorkdirIdentity,
    };
    use vyane_kernel::{
        CapabilityAdmissionDecision, CapabilityManifest, CapabilityTargetSnapshot,
        IsolationStrength,
    };

    fn write_private(path: &Path, bytes: impl AsRef<[u8]>) {
        use std::os::unix::fs::PermissionsExt as _;

        fs::write(path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn policy() -> AgentSpoolPolicy {
        let target_snapshot = TargetSnapshot {
            target: Target {
                provider: ProviderId::new("provider-a"),
                protocol: Protocol::OpenaiResponses,
                harness: Some(HarnessKind::CodexCli),
                model: ModelId::new("model-a"),
            },
            transport: AdapterTransport::CliWrap,
            params: GenParamsSnapshot::default(),
            endpoint_digest: None,
            auth_style: None,
            env_policy_digest: None,
        };
        let workdir = PathBuf::from("/workspace/private-marker");
        AgentSpoolPolicy {
            target: "profile-a".into(),
            sandbox: AgentSpoolSandbox::Write,
            workdir: Some(workdir.clone()),
            system: Some("system-body-marker".into()),
            timeout_seconds: Some(120),
            labels: vec!["kind=test".into()],
            config: Some(PathBuf::from("/config/private-marker.toml")),
            target_snapshot: vec![target_snapshot.clone()],
            capability_plan: CapabilityPlanSnapshot {
                requested_sandbox: vyane_core::Sandbox::Write,
                requires_inherited_workdir: true,
                canonical_workdir: Some(workdir),
                workdir_identity: Some(WorkdirIdentity {
                    device: 7,
                    inode: 11,
                }),
                targets: vec![CapabilityTargetSnapshot {
                    original_chain_ordinal: 0,
                    target: target_snapshot.target,
                    manifest: CapabilityManifest::local_workdir_editing(
                        IsolationStrength::AdapterDelegated,
                    ),
                    decision: CapabilityAdmissionDecision::Admitted,
                }],
            },
        }
    }

    fn input() -> AgentSpoolInput {
        AgentSpoolInput::fresh(
            "owner-marker",
            "run-marker",
            "worker-marker",
            "prompt-body-marker",
            policy(),
        )
        .unwrap()
    }

    #[test]
    fn read_only_policy_preserves_an_explicit_workdir_without_mutating_identity() {
        let mut policy = policy();
        policy.sandbox = AgentSpoolSandbox::ReadOnly;
        policy.workdir = Some(PathBuf::from("/workspace/read-only"));
        policy.capability_plan.requested_sandbox = vyane_core::Sandbox::ReadOnly;
        policy.capability_plan.requires_inherited_workdir = false;
        policy.capability_plan.canonical_workdir = None;
        policy.capability_plan.workdir_identity = None;

        let input = AgentSpoolInput::fresh(
            "owner-marker",
            "run-marker",
            "worker-marker",
            "prompt-body-marker",
            policy,
        )
        .expect("read-only policy with explicit workdir remains valid");
        assert_eq!(
            input.policy.workdir,
            Some(PathBuf::from("/workspace/read-only"))
        );
    }

    fn fixture() -> (tempfile::TempDir, AgentInputSpool) {
        let directory = tempfile::tempdir().unwrap();
        let spool = AgentInputSpool::open(directory.path().join("spool"), "owner-marker").unwrap();
        (directory, spool)
    }

    #[test]
    fn private_hashed_namespace_round_trips_and_removes_idempotently() {
        use std::os::unix::fs::PermissionsExt as _;

        let (_directory, spool) = fixture();
        let value = input();
        spool.create(&value).unwrap();
        spool.create(&value).unwrap();
        assert_eq!(spool.read("run-marker", "worker-marker").unwrap(), value);

        let path = spool.input_path("run-marker").unwrap();
        let rendered = path.to_string_lossy();
        assert!(!rendered.contains("owner-marker"));
        assert!(!rendered.contains("run-marker"));
        assert_eq!(
            fs::metadata(&spool.root).unwrap().permissions().mode() & 0o7777,
            0o700
        );
        assert_eq!(
            fs::metadata(&spool.owner_root)
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o700
        );
        let metadata = fs::metadata(&path).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
        assert_eq!(metadata.nlink(), 1);
        assert_eq!(
            fs::read_dir(&spool.owner_root).unwrap().count(),
            1,
            "atomic publication leaves no temporary artifact"
        );

        spool.remove("run-marker", "worker-marker").unwrap();
        spool.remove("run-marker", "worker-marker").unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn exact_binding_and_digests_reject_conflicts_and_tampering() {
        let (_directory, spool) = fixture();
        let value = input();
        spool.create(&value).unwrap();

        let mut conflicting = AgentSpoolInput::fresh(
            "owner-marker",
            "run-marker",
            "worker-marker",
            "different prompt",
            policy(),
        )
        .unwrap();
        assert_eq!(
            spool.create(&conflicting),
            Err(AgentSpoolError::ConflictingInput)
        );
        assert_eq!(
            spool.read("run-marker", "different-worker"),
            Err(AgentSpoolError::BindingMismatch)
        );

        conflicting = value;
        conflicting.prompt.push_str("tampered");
        let path = spool.input_path("run-marker").unwrap();
        write_private(&path, serde_json::to_vec(&conflicting).unwrap());
        assert_eq!(
            spool.read("run-marker", "worker-marker"),
            Err(AgentSpoolError::DigestMismatch)
        );
    }

    #[test]
    fn strict_schema_and_closed_sandbox_reject_unknown_data() {
        let (_directory, spool) = fixture();
        let value = input();
        let path = spool.input_path("run-marker").unwrap();
        let mut json = serde_json::to_value(value).unwrap();
        json.as_object_mut().unwrap().insert(
            "session".into(),
            serde_json::Value::String("forbidden".into()),
        );
        write_private(&path, serde_json::to_vec(&json).unwrap());
        assert_eq!(
            spool.read("run-marker", "worker-marker"),
            Err(AgentSpoolError::CorruptInput)
        );

        json.as_object_mut().unwrap().remove("session");
        json["policy"]["sandbox"] = serde_json::Value::String("future_mode".into());
        write_private(&path, serde_json::to_vec(&json).unwrap());
        assert_eq!(
            spool.read("run-marker", "worker-marker"),
            Err(AgentSpoolError::CorruptInput)
        );
    }

    #[test]
    fn symlink_hardlink_and_oversized_entries_fail_closed() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let (directory, spool) = fixture();
        let path = spool.input_path("run-marker").unwrap();
        let target = directory.path().join("target");
        fs::write(&target, b"{}").unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        symlink(&target, &path).unwrap();
        assert_eq!(
            spool.read("run-marker", "worker-marker"),
            Err(AgentSpoolError::UnsafePath)
        );
        fs::remove_file(&path).unwrap();
        fs::hard_link(&target, &path).unwrap();
        assert_eq!(
            spool.read("run-marker", "worker-marker"),
            Err(AgentSpoolError::UnsafePath)
        );
        fs::remove_file(&path).unwrap();
        write_private(&path, vec![b'x'; MAX_INPUT_BYTES as usize + 1]);
        assert_eq!(
            spool.read("run-marker", "worker-marker"),
            Err(AgentSpoolError::TooLarge)
        );
    }

    #[test]
    fn unsafe_directory_is_never_repaired_or_followed() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("spool");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o750)).unwrap();
        assert_eq!(
            AgentInputSpool::open(&root, "owner")
                .unwrap_err()
                .to_string(),
            AgentSpoolError::InvalidRoot.to_string()
        );

        fs::remove_dir(&root).unwrap();
        let real = directory.path().join("real");
        fs::create_dir(&real).unwrap();
        fs::set_permissions(&real, fs::Permissions::from_mode(0o700)).unwrap();
        symlink(&real, &root).unwrap();
        assert!(matches!(
            AgentInputSpool::open(&root, "owner"),
            Err(AgentSpoolError::InvalidRoot)
        ));
    }

    #[test]
    fn budgets_and_diagnostics_do_not_disclose_bodies_or_paths() {
        let mut too_many = policy();
        too_many.labels = (0..=MAX_LABELS).map(|index| format!("l={index}")).collect();
        assert!(matches!(
            AgentSpoolInput::fresh("owner", "run", "worker", "prompt", too_many),
            Err(AgentSpoolError::InvalidInput)
        ));
        assert!(matches!(
            AgentSpoolInput::fresh(
                "owner",
                "run",
                "worker",
                "x".repeat(MAX_PROMPT_BYTES + 1),
                policy()
            ),
            Err(AgentSpoolError::InvalidInput)
        ));

        let debug = format!("{:?}", input());
        for secret in [
            "owner-marker",
            "run-marker",
            "worker-marker",
            "prompt-body-marker",
            "system-body-marker",
            "/workspace/private-marker",
            "/config/private-marker.toml",
        ] {
            assert!(!debug.contains(secret));
        }
        let error = AgentSpoolError::CorruptInput.to_string();
        assert!(!error.contains("marker"));

        let (_directory, spool) = fixture();
        let spool_debug = format!("{spool:?}");
        assert!(!spool_debug.contains("owner-marker"));
        assert!(!spool_debug.contains(spool.root.to_string_lossy().as_ref()));
    }

    #[test]
    fn policy_digest_covers_every_execution_field() {
        let base = policy();
        let base_digest = policy_digest(&base).unwrap();
        let variants = [
            AgentSpoolPolicy {
                target: "other".into(),
                ..base.clone()
            },
            AgentSpoolPolicy {
                sandbox: AgentSpoolSandbox::Full,
                ..base.clone()
            },
            AgentSpoolPolicy {
                workdir: None,
                ..base.clone()
            },
            AgentSpoolPolicy {
                system: None,
                ..base.clone()
            },
            AgentSpoolPolicy {
                timeout_seconds: None,
                ..base.clone()
            },
            AgentSpoolPolicy {
                labels: Vec::new(),
                ..base.clone()
            },
            AgentSpoolPolicy {
                config: None,
                ..base.clone()
            },
            AgentSpoolPolicy {
                target_snapshot: vec![TargetSnapshot {
                    target: Target {
                        model: ModelId::new("model-b"),
                        ..base.target_snapshot[0].target.clone()
                    },
                    ..base.target_snapshot[0].clone()
                }],
                ..base.clone()
            },
            AgentSpoolPolicy {
                capability_plan: CapabilityPlanSnapshot {
                    workdir_identity: Some(WorkdirIdentity {
                        device: 7,
                        inode: 12,
                    }),
                    ..base.capability_plan.clone()
                },
                ..base.clone()
            },
        ];
        assert!(
            variants
                .iter()
                .all(|variant| policy_digest(variant).unwrap() != base_digest)
        );
    }
}
