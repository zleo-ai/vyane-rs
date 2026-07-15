//! Immutable private input storage for fresh native agent execution.
//!
//! The persisted shape is intentionally native-only and sessionless. Path
//! names are derived from bound identities, so caller-controlled identifiers
//! never become filesystem components.

use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use vyane_core::{Effort, WorkdirIdentity};

const SCHEMA: u32 = 2;
const MAX_INPUT_BYTES: u64 = 1024 * 1024;
const MAX_PROMPT_BYTES: usize = 768 * 1024;
const MAX_SYSTEM_BYTES: usize = 128 * 1024;
const MAX_ID_BYTES: usize = 256;
const MAX_SELECTOR_BYTES: usize = 512;
const MAX_TARGET_PART_BYTES: usize = 512;
const MAX_PATH_BYTES: usize = 4096;
const MAX_TIMEOUT_SECONDS: u64 = 7 * 24 * 60 * 60;
const MAX_MODEL_TURNS: u32 = 32;

const OWNER_PATH_DOMAIN: &[u8] = b"vyane.native-input.owner-path.v1\0";
const RUN_PATH_DOMAIN: &[u8] = b"vyane.native-input.run-path.v1\0";
const PROMPT_DOMAIN: &[u8] = b"vyane.native-input.prompt.v1\0";
const POLICY_DOMAIN: &[u8] = b"vyane.native-input.policy.v1\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeAgentSpoolError {
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

impl std::fmt::Display for NativeAgentSpoolError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidInput => "native input is invalid",
            Self::InvalidRoot => "native input directory is invalid",
            Self::UnsafePath => "native input storage is unsafe",
            Self::TooLarge => "native input exceeds its size limit",
            Self::CorruptInput => "stored native input is invalid",
            Self::DigestMismatch => "stored native input digest does not match",
            Self::BindingMismatch => "stored native input identity does not match",
            Self::ConflictingInput => "a native input already exists",
            Self::NotFound => "native input is not available",
            Self::Io => "native input storage is unavailable",
        })
    }
}

impl std::error::Error for NativeAgentSpoolError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeAgentSpoolCreate {
    Created,
    ExistingExact,
}

/// Closed protocol set supported by the native execution lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NativeProtocolSnapshot {
    OpenaiChat,
}

/// Secret-free wire authentication shape frozen with the approved route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NativeAuthStyleSnapshot {
    Bearer,
    XApiKey,
}

/// Persistable generation controls. Arbitrary extension values are digest-only.
#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeGenParamsSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) top_p: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) max_output_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) effort: Option<Effort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) extra_digest: Option<String>,
}

impl std::fmt::Debug for NativeGenParamsSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeGenParamsSnapshot")
            .field("temperature", &self.temperature)
            .field("top_p", &self.top_p)
            .field("max_output_tokens", &self.max_output_tokens)
            .field("effort", &self.effort)
            .field(
                "extra_digest",
                &self.extra_digest.as_ref().map(|_| "[OPAQUE]"),
            )
            .finish()
    }
}

/// Exact, secret-free native route approved before publication.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeTargetSnapshot {
    pub(crate) provider: String,
    pub(crate) protocol: NativeProtocolSnapshot,
    pub(crate) model: String,
    /// Credential presentation only; credential material is never persisted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) auth_style: Option<NativeAuthStyleSnapshot>,
    /// Digest of the canonical routing identity; never the routing value.
    pub(crate) routing_digest: String,
    pub(crate) params: NativeGenParamsSnapshot,
}

impl std::fmt::Debug for NativeTargetSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeTargetSnapshot")
            .field("provider", &"[REDACTED]")
            .field("protocol", &self.protocol)
            .field("model", &"[REDACTED]")
            .field("auth_style", &self.auth_style)
            .field("routing_digest", &"[OPAQUE]")
            .field("params", &self.params)
            .finish()
    }
}

/// Frozen policy covered in full by `policy_sha256`.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeAgentPolicy {
    pub(crate) target_selector: String,
    pub(crate) target: NativeTargetSnapshot,
    pub(crate) canonical_workdir: PathBuf,
    pub(crate) workdir_identity: WorkdirIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) system: Option<String>,
    pub(crate) timeout_seconds: u64,
    pub(crate) max_model_turns: u32,
}

impl std::fmt::Debug for NativeAgentPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeAgentPolicy")
            .field("target_selector", &"[REDACTED]")
            .field("target", &self.target)
            .field("canonical_workdir", &"[REDACTED]")
            .field("workdir_identity", &"[OPAQUE]")
            .field("system", &self.system.as_ref().map(|_| "[REDACTED]"))
            .field("timeout_seconds", &self.timeout_seconds)
            .field("max_model_turns", &self.max_model_turns)
            .finish()
    }
}

/// One fresh request. No session or replay authority is representable.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeAgentInput {
    schema: u32,
    pub(crate) owner: String,
    pub(crate) run_id: String,
    pub(crate) worker_id: String,
    pub(crate) prompt: String,
    pub(crate) prompt_sha256: String,
    pub(crate) policy: NativeAgentPolicy,
    pub(crate) policy_sha256: String,
}

impl std::fmt::Debug for NativeAgentInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeAgentInput")
            .field("schema", &self.schema)
            .field("identity", &"[REDACTED]")
            .field("prompt", &"[REDACTED]")
            .field("prompt_sha256", &"[OPAQUE]")
            .field("policy", &self.policy)
            .field("policy_sha256", &"[OPAQUE]")
            .finish()
    }
}

impl NativeAgentInput {
    pub(crate) fn fresh(
        owner: impl Into<String>,
        run_id: impl Into<String>,
        worker_id: impl Into<String>,
        prompt: impl Into<String>,
        policy: NativeAgentPolicy,
    ) -> Result<Self, NativeAgentSpoolError> {
        let prompt = prompt.into();
        let input = Self {
            schema: SCHEMA,
            owner: owner.into(),
            run_id: run_id.into(),
            worker_id: worker_id.into(),
            prompt_sha256: prompt_digest(&prompt),
            prompt,
            policy_sha256: policy_digest(&policy)?,
            policy,
        };
        input.validate()?;
        Ok(input)
    }

    fn validate(&self) -> Result<(), NativeAgentSpoolError> {
        if self.schema != SCHEMA
            || !valid_text(&self.owner, MAX_ID_BYTES)
            || !valid_text(&self.run_id, MAX_ID_BYTES)
            || !valid_text(&self.worker_id, MAX_ID_BYTES)
            || self.prompt.is_empty()
            || self.prompt.len() > MAX_PROMPT_BYTES
        {
            return Err(NativeAgentSpoolError::InvalidInput);
        }
        validate_policy(&self.policy)?;
        if !valid_digest(&self.prompt_sha256) || !valid_digest(&self.policy_sha256) {
            return Err(NativeAgentSpoolError::InvalidInput);
        }
        if self.prompt_sha256 != prompt_digest(&self.prompt)
            || self.policy_sha256 != policy_digest(&self.policy)?
        {
            return Err(NativeAgentSpoolError::DigestMismatch);
        }
        let encoded = serde_json::to_vec(self).map_err(|_| NativeAgentSpoolError::InvalidInput)?;
        if encoded.len() as u64 > MAX_INPUT_BYTES {
            return Err(NativeAgentSpoolError::TooLarge);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct NativeAgentInputSpool {
    root: PathBuf,
    owner: String,
    owner_root: PathBuf,
}

impl std::fmt::Debug for NativeAgentInputSpool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeAgentInputSpool")
            .field("root", &"[REDACTED]")
            .field("owner", &"[REDACTED]")
            .finish()
    }
}

impl NativeAgentInputSpool {
    pub(crate) fn open(
        root: impl Into<PathBuf>,
        owner: impl Into<String>,
    ) -> Result<Self, NativeAgentSpoolError> {
        let root = root.into();
        let owner = owner.into();
        if !valid_text(&owner, MAX_ID_BYTES) {
            return Err(NativeAgentSpoolError::InvalidInput);
        }
        reject_symlink_ancestors(&root)?;
        ensure_private_directory(&root, NativeAgentSpoolError::InvalidRoot)?;
        let owner_root = root.join(hex_digest(OWNER_PATH_DOMAIN, &[owner.as_bytes()]));
        ensure_private_directory(&owner_root, NativeAgentSpoolError::UnsafePath)?;
        Ok(Self {
            root,
            owner,
            owner_root,
        })
    }

    pub(crate) fn create(
        &self,
        input: &NativeAgentInput,
    ) -> Result<NativeAgentSpoolCreate, NativeAgentSpoolError> {
        input.validate()?;
        if input.owner != self.owner {
            return Err(NativeAgentSpoolError::BindingMismatch);
        }
        let bytes = serde_json::to_vec(input).map_err(|_| NativeAgentSpoolError::InvalidInput)?;
        if bytes.len() as u64 > MAX_INPUT_BYTES {
            return Err(NativeAgentSpoolError::TooLarge);
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
            let mut file = options
                .open(&temporary)
                .map_err(|_| NativeAgentSpoolError::Io)?;
            use std::os::unix::fs::PermissionsExt as _;
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(|_| NativeAgentSpoolError::Io)?;
            file.write_all(&bytes)
                .and_then(|()| file.sync_all())
                .map_err(|_| NativeAgentSpoolError::Io)?;
            validate_private_regular(&file.metadata().map_err(|_| NativeAgentSpoolError::Io)?)?;
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
                        &published
                            .metadata()
                            .map_err(|_| NativeAgentSpoolError::Io)?,
                    )?;
                    Ok(NativeAgentSpoolCreate::Created)
                }
                Err(nix::errno::Errno::EEXIST) => {
                    match self.read(&input.run_id, &input.worker_id) {
                        Ok(existing) if existing == *input => {
                            Ok(NativeAgentSpoolCreate::ExistingExact)
                        }
                        Ok(_) | Err(NativeAgentSpoolError::BindingMismatch) => {
                            Err(NativeAgentSpoolError::ConflictingInput)
                        }
                        Err(error) => Err(error),
                    }
                }
                Err(_) => Err(NativeAgentSpoolError::Io),
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
    ) -> Result<NativeAgentInput, NativeAgentSpoolError> {
        if !valid_text(run_id, MAX_ID_BYTES) || !valid_text(worker_id, MAX_ID_BYTES) {
            return Err(NativeAgentSpoolError::InvalidInput);
        }
        self.revalidate_directories()?;
        let path = self.input_path(run_id)?;
        let file = open_private_regular(&path)?;
        let metadata = file.metadata().map_err(|_| NativeAgentSpoolError::Io)?;
        if metadata.len() > MAX_INPUT_BYTES {
            return Err(NativeAgentSpoolError::TooLarge);
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(MAX_INPUT_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| NativeAgentSpoolError::Io)?;
        if bytes.len() as u64 > MAX_INPUT_BYTES {
            return Err(NativeAgentSpoolError::TooLarge);
        }
        let input: NativeAgentInput =
            serde_json::from_slice(&bytes).map_err(|_| NativeAgentSpoolError::CorruptInput)?;
        input.validate().map_err(|error| match error {
            NativeAgentSpoolError::DigestMismatch => NativeAgentSpoolError::DigestMismatch,
            NativeAgentSpoolError::TooLarge => NativeAgentSpoolError::TooLarge,
            _ => NativeAgentSpoolError::CorruptInput,
        })?;
        if input.owner != self.owner || input.run_id != run_id || input.worker_id != worker_id {
            return Err(NativeAgentSpoolError::BindingMismatch);
        }
        Ok(input)
    }

    /// Remove one input after its owning operation has quiesced and the
    /// complete value currently at its bound path matches `expected`.
    ///
    /// The path is derived solely from validated identities, and the private
    /// owner directory plus file shape are revalidated before unlinking. This
    /// fails closed for replacements observed before removal. Like ordinary
    /// pathname unlinking, it is not an atomic compare-and-delete primitive
    /// against a hostile process running as the same OS user; that actor is
    /// outside this private `0700` spool's trust boundary.
    pub(crate) fn remove_exact(
        &self,
        expected: &NativeAgentInput,
    ) -> Result<(), NativeAgentSpoolError> {
        expected.validate()?;
        if expected.owner != self.owner {
            return Err(NativeAgentSpoolError::BindingMismatch);
        }
        let observed = self.read(&expected.run_id, &expected.worker_id)?;
        if observed != *expected {
            return Err(NativeAgentSpoolError::ConflictingInput);
        }
        self.revalidate_directories()?;
        let path = self.input_path(&expected.run_id)?;
        fs::remove_file(path).map_err(|_| NativeAgentSpoolError::Io)?;
        sync_directory(&self.owner_root)
    }

    fn input_path(&self, run_id: &str) -> Result<PathBuf, NativeAgentSpoolError> {
        if !valid_text(run_id, MAX_ID_BYTES) {
            return Err(NativeAgentSpoolError::InvalidInput);
        }
        let key = hex_digest(RUN_PATH_DOMAIN, &[self.owner.as_bytes(), run_id.as_bytes()]);
        Ok(self.owner_root.join(format!("{key}.json")))
    }

    fn revalidate_directories(&self) -> Result<(), NativeAgentSpoolError> {
        validate_private_directory(&self.root, NativeAgentSpoolError::InvalidRoot)?;
        validate_private_directory(&self.owner_root, NativeAgentSpoolError::UnsafePath)
    }
}

fn validate_policy(policy: &NativeAgentPolicy) -> Result<(), NativeAgentSpoolError> {
    if !valid_text(&policy.target_selector, MAX_SELECTOR_BYTES)
        || !valid_text(&policy.target.provider, MAX_TARGET_PART_BYTES)
        || !valid_text(&policy.target.model, MAX_TARGET_PART_BYTES)
        || !valid_digest(&policy.target.routing_digest)
        || policy
            .system
            .as_ref()
            .is_some_and(|value| value.len() > MAX_SYSTEM_BYTES)
        || policy.timeout_seconds == 0
        || policy.timeout_seconds > MAX_TIMEOUT_SECONDS
        || policy.max_model_turns == 0
        || policy.max_model_turns > MAX_MODEL_TURNS
    {
        return Err(NativeAgentSpoolError::InvalidInput);
    }
    validate_path(&policy.canonical_workdir)?;
    if !policy.canonical_workdir.is_absolute() {
        return Err(NativeAgentSpoolError::InvalidInput);
    }
    validate_params(&policy.target.params)
}

fn validate_params(params: &NativeGenParamsSnapshot) -> Result<(), NativeAgentSpoolError> {
    if params.temperature.is_some_and(|value| !value.is_finite())
        || params
            .top_p
            .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
        || params.max_output_tokens == Some(0)
        || params
            .extra_digest
            .as_ref()
            .is_some_and(|value| !valid_digest(value))
    {
        return Err(NativeAgentSpoolError::InvalidInput);
    }
    Ok(())
}

fn validate_path(path: &Path) -> Result<(), NativeAgentSpoolError> {
    let value = path.to_str().ok_or(NativeAgentSpoolError::InvalidInput)?;
    if value.is_empty() || value.len() > MAX_PATH_BYTES || value.chars().any(char::is_control) {
        return Err(NativeAgentSpoolError::InvalidInput);
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

fn policy_digest(policy: &NativeAgentPolicy) -> Result<String, NativeAgentSpoolError> {
    let bytes = serde_json::to_vec(policy).map_err(|_| NativeAgentSpoolError::InvalidInput)?;
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

fn ensure_private_directory(
    path: &Path,
    invalid: NativeAgentSpoolError,
) -> Result<(), NativeAgentSpoolError> {
    match fs::symlink_metadata(path) {
        Ok(_) => validate_private_directory(path, invalid),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(path) {
                Ok(()) => {
                    use std::os::unix::fs::PermissionsExt as _;
                    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                        .map_err(|_| NativeAgentSpoolError::Io)?;
                    let parent = path.parent().ok_or(invalid)?;
                    sync_directory(parent)?;
                    sync_directory(path)?;
                    validate_private_directory(path, invalid)
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    validate_private_directory(path, invalid)
                }
                Err(_) => Err(NativeAgentSpoolError::Io),
            }
        }
        Err(_) => Err(NativeAgentSpoolError::Io),
    }
}

fn reject_symlink_ancestors(path: &Path) -> Result<(), NativeAgentSpoolError> {
    for ancestor in path.ancestors() {
        match fs::symlink_metadata(ancestor) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(NativeAgentSpoolError::InvalidRoot);
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(NativeAgentSpoolError::Io),
        }
    }
    Ok(())
}

fn validate_private_directory(
    path: &Path,
    invalid: NativeAgentSpoolError,
) -> Result<(), NativeAgentSpoolError> {
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

fn open_private_regular(path: &Path) -> Result<File, NativeAgentSpoolError> {
    let before = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(NativeAgentSpoolError::NotFound);
        }
        Err(_) => return Err(NativeAgentSpoolError::Io),
    };
    if !before.file_type().is_file() {
        return Err(NativeAgentSpoolError::UnsafePath);
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
    let file = options
        .open(path)
        .map_err(|_| NativeAgentSpoolError::UnsafePath)?;
    let after = file.metadata().map_err(|_| NativeAgentSpoolError::Io)?;
    if before.dev() != after.dev() || before.ino() != after.ino() {
        return Err(NativeAgentSpoolError::UnsafePath);
    }
    validate_private_regular(&after)?;
    Ok(file)
}

fn validate_private_regular(metadata: &fs::Metadata) -> Result<(), NativeAgentSpoolError> {
    use std::os::unix::fs::PermissionsExt as _;
    if !metadata.file_type().is_file()
        || metadata.permissions().mode() & 0o7777 != 0o600
        || metadata.nlink() != 1
    {
        return Err(NativeAgentSpoolError::UnsafePath);
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), NativeAgentSpoolError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| NativeAgentSpoolError::Io)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn write_private(path: &Path, bytes: impl AsRef<[u8]>) {
        use std::os::unix::fs::PermissionsExt as _;
        fs::write(path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn policy() -> NativeAgentPolicy {
        NativeAgentPolicy {
            target_selector: "profile-a".into(),
            target: NativeTargetSnapshot {
                provider: "provider-a".into(),
                protocol: NativeProtocolSnapshot::OpenaiChat,
                model: "model-a".into(),
                auth_style: Some(NativeAuthStyleSnapshot::Bearer),
                routing_digest: "a".repeat(64),
                params: NativeGenParamsSnapshot {
                    temperature: Some(0.2),
                    top_p: Some(0.9),
                    max_output_tokens: Some(1024),
                    effort: Some(Effort::High),
                    extra_digest: Some("b".repeat(64)),
                },
            },
            canonical_workdir: PathBuf::from("/workspace/private-marker"),
            workdir_identity: WorkdirIdentity {
                device: 7,
                inode: 11,
            },
            system: Some("system-body-marker".into()),
            timeout_seconds: 120,
            max_model_turns: 8,
        }
    }

    fn input() -> NativeAgentInput {
        NativeAgentInput::fresh(
            "owner-marker",
            "run-marker",
            "worker-marker",
            "prompt-body-marker",
            policy(),
        )
        .unwrap()
    }

    fn fixture() -> (tempfile::TempDir, NativeAgentInputSpool) {
        let directory = tempfile::tempdir().unwrap();
        let spool =
            NativeAgentInputSpool::open(directory.path().join("spool"), "owner-marker").unwrap();
        (directory, spool)
    }

    #[test]
    fn private_hashed_namespace_is_immutable_and_exactly_bound() {
        use std::os::unix::fs::PermissionsExt as _;
        let (_directory, spool) = fixture();
        let value = input();
        assert_eq!(
            spool.create(&value).unwrap(),
            NativeAgentSpoolCreate::Created
        );
        assert_eq!(
            spool.create(&value).unwrap(),
            NativeAgentSpoolCreate::ExistingExact
        );
        assert_eq!(spool.read("run-marker", "worker-marker").unwrap(), value);
        assert_eq!(
            spool.read("run-marker", "other-worker"),
            Err(NativeAgentSpoolError::BindingMismatch)
        );
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
        let metadata = fs::metadata(path).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
        assert_eq!(metadata.nlink(), 1);
        assert_eq!(fs::read_dir(&spool.owner_root).unwrap().count(), 1);
    }

    #[test]
    fn conflicting_publication_and_all_identity_mismatches_fail_closed() {
        let (_directory, spool) = fixture();
        let value = input();
        spool.create(&value).unwrap();
        let conflict = NativeAgentInput::fresh(
            "owner-marker",
            "run-marker",
            "worker-marker",
            "different",
            policy(),
        )
        .unwrap();
        assert_eq!(
            spool.create(&conflict),
            Err(NativeAgentSpoolError::ConflictingInput)
        );
        let other_owner = NativeAgentInput::fresh(
            "other-owner",
            "other-run",
            "worker-marker",
            "prompt",
            policy(),
        )
        .unwrap();
        assert_eq!(
            spool.create(&other_owner),
            Err(NativeAgentSpoolError::BindingMismatch)
        );

        let path = spool.input_path("run-marker").unwrap();
        for mismatched in [
            NativeAgentInput::fresh(
                "other-owner",
                "run-marker",
                "worker-marker",
                "prompt",
                policy(),
            )
            .unwrap(),
            NativeAgentInput::fresh(
                "owner-marker",
                "other-run",
                "worker-marker",
                "prompt",
                policy(),
            )
            .unwrap(),
            NativeAgentInput::fresh(
                "owner-marker",
                "run-marker",
                "other-worker",
                "prompt",
                policy(),
            )
            .unwrap(),
        ] {
            write_private(&path, serde_json::to_vec(&mismatched).unwrap());
            assert_eq!(
                spool.read("run-marker", "worker-marker"),
                Err(NativeAgentSpoolError::BindingMismatch)
            );
        }
    }

    #[test]
    fn exact_removal_consumes_only_the_matching_private_input() {
        let (_directory, spool) = fixture();
        let value = input();
        spool.create(&value).unwrap();
        let conflict = NativeAgentInput::fresh(
            "owner-marker",
            "run-marker",
            "worker-marker",
            "different",
            policy(),
        )
        .unwrap();
        assert_eq!(
            spool.remove_exact(&conflict),
            Err(NativeAgentSpoolError::ConflictingInput)
        );
        assert_eq!(spool.read("run-marker", "worker-marker").unwrap(), value);
        spool.remove_exact(&value).unwrap();
        assert_eq!(
            spool.read("run-marker", "worker-marker"),
            Err(NativeAgentSpoolError::NotFound)
        );
    }

    #[test]
    fn exact_removal_retains_a_replacement_observed_before_unlink() {
        let (_directory, spool) = fixture();
        let expected = input();
        spool.create(&expected).unwrap();
        let replacement = NativeAgentInput::fresh(
            "owner-marker",
            "run-marker",
            "worker-marker",
            "replacement-body-marker",
            policy(),
        )
        .unwrap();
        let path = spool.input_path("run-marker").unwrap();
        write_private(&path, serde_json::to_vec(&replacement).unwrap());

        assert_eq!(
            spool.remove_exact(&expected),
            Err(NativeAgentSpoolError::ConflictingInput)
        );
        assert_eq!(
            spool.read("run-marker", "worker-marker").unwrap(),
            replacement
        );
    }

    #[test]
    fn strict_sessionless_schema_and_digests_reject_tampering() {
        let (_directory, spool) = fixture();
        let path = spool.input_path("run-marker").unwrap();
        let mut json = serde_json::to_value(input()).unwrap();
        json.as_object_mut().unwrap().insert(
            "session".into(),
            serde_json::Value::String("forbidden".into()),
        );
        write_private(&path, serde_json::to_vec(&json).unwrap());
        assert_eq!(
            spool.read("run-marker", "worker-marker"),
            Err(NativeAgentSpoolError::CorruptInput)
        );
        json.as_object_mut().unwrap().remove("session");
        json["prompt"] = serde_json::Value::String("tampered".into());
        write_private(&path, serde_json::to_vec(&json).unwrap());
        assert_eq!(
            spool.read("run-marker", "worker-marker"),
            Err(NativeAgentSpoolError::DigestMismatch)
        );

        let encoded = serde_json::to_value(input()).unwrap();
        for forbidden in ["session", "resume", "endpoint_url", "credential"] {
            assert!(encoded.get(forbidden).is_none());
            assert!(encoded["policy"].get(forbidden).is_none());
            assert!(encoded["policy"]["target"].get(forbidden).is_none());
        }
        let mut json = serde_json::to_value(input()).unwrap();
        json["policy"]["target"]["routing_digest"] = serde_json::Value::String("c".repeat(64));
        write_private(&path, serde_json::to_vec(&json).unwrap());
        assert_eq!(
            spool.read("run-marker", "worker-marker"),
            Err(NativeAgentSpoolError::DigestMismatch)
        );
    }

    #[test]
    fn symlink_hardlink_wrong_mode_and_oversize_fail_closed() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};
        let (directory, spool) = fixture();
        let path = spool.input_path("run-marker").unwrap();
        let target = directory.path().join("target");
        write_private(&target, b"{}");
        symlink(&target, &path).unwrap();
        assert_eq!(
            spool.read("run-marker", "worker-marker"),
            Err(NativeAgentSpoolError::UnsafePath)
        );
        fs::remove_file(&path).unwrap();
        fs::hard_link(&target, &path).unwrap();
        assert_eq!(
            spool.read("run-marker", "worker-marker"),
            Err(NativeAgentSpoolError::UnsafePath)
        );
        fs::remove_file(&path).unwrap();
        write_private(&path, b"{}");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        assert_eq!(
            spool.read("run-marker", "worker-marker"),
            Err(NativeAgentSpoolError::UnsafePath)
        );
        fs::remove_file(&path).unwrap();
        write_private(&path, vec![b'x'; MAX_INPUT_BYTES as usize + 1]);
        assert_eq!(
            spool.read("run-marker", "worker-marker"),
            Err(NativeAgentSpoolError::TooLarge)
        );
    }

    #[test]
    fn unsafe_roots_and_ancestors_are_not_repaired_or_followed() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("spool");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o750)).unwrap();
        assert!(matches!(
            NativeAgentInputSpool::open(&root, "owner"),
            Err(NativeAgentSpoolError::InvalidRoot)
        ));

        fs::remove_dir(&root).unwrap();
        let real = directory.path().join("real");
        fs::create_dir(&real).unwrap();
        fs::set_permissions(&real, fs::Permissions::from_mode(0o700)).unwrap();
        symlink(&real, &root).unwrap();
        assert!(matches!(
            NativeAgentInputSpool::open(&root, "owner"),
            Err(NativeAgentSpoolError::InvalidRoot)
        ));

        let linked_parent = directory.path().join("linked-parent");
        symlink(&real, &linked_parent).unwrap();
        assert!(matches!(
            NativeAgentInputSpool::open(linked_parent.join("spool"), "owner"),
            Err(NativeAgentSpoolError::InvalidRoot)
        ));
    }

    #[test]
    fn policy_validation_is_bounded_and_requires_canonical_native_fields() {
        let mut variants = Vec::new();
        let mut value = policy();
        value.canonical_workdir = PathBuf::from("relative");
        variants.push(value);
        let mut value = policy();
        value.timeout_seconds = 0;
        variants.push(value);
        let mut value = policy();
        value.max_model_turns = MAX_MODEL_TURNS + 1;
        variants.push(value);
        let mut value = policy();
        value.target.params.top_p = Some(f32::NAN);
        variants.push(value);
        let mut value = policy();
        value.target.routing_digest = "not-a-digest".into();
        variants.push(value);
        assert!(variants.into_iter().all(|policy| {
            NativeAgentInput::fresh("owner", "run", "worker", "prompt", policy)
                == Err(NativeAgentSpoolError::InvalidInput)
        }));
        assert!(matches!(
            NativeAgentInput::fresh(
                "owner",
                "run",
                "worker",
                "x".repeat(MAX_PROMPT_BYTES + 1),
                policy()
            ),
            Err(NativeAgentSpoolError::InvalidInput)
        ));
    }

    #[test]
    fn policy_digest_covers_every_execution_field() {
        let base = policy();
        let digest = policy_digest(&base).unwrap();
        let mut variants = Vec::new();
        let mut value = base.clone();
        value.target_selector = "other".into();
        variants.push(value);
        let mut value = base.clone();
        value.target.model = "other".into();
        variants.push(value);
        let mut value = base.clone();
        value.target.auth_style = Some(NativeAuthStyleSnapshot::XApiKey);
        variants.push(value);
        let mut value = base.clone();
        value.target.routing_digest = "c".repeat(64);
        variants.push(value);
        let mut value = base.clone();
        value.target.params.max_output_tokens = Some(2048);
        variants.push(value);
        let mut value = base.clone();
        value.canonical_workdir = PathBuf::from("/workspace/other");
        variants.push(value);
        let mut value = base.clone();
        value.workdir_identity.inode += 1;
        variants.push(value);
        let mut value = base.clone();
        value.system = None;
        variants.push(value);
        let mut value = base.clone();
        value.timeout_seconds += 1;
        variants.push(value);
        let mut value = base;
        value.max_model_turns += 1;
        variants.push(value);
        assert!(
            variants
                .iter()
                .all(|value| policy_digest(value).unwrap() != digest)
        );
    }

    #[test]
    fn debug_and_errors_never_disclose_bodies_routes_or_paths() {
        let value = input();
        let debug = format!("{value:?}");
        for secret in [
            "owner-marker",
            "run-marker",
            "worker-marker",
            "prompt-body-marker",
            "system-body-marker",
            "provider-a",
            "model-a",
            "/workspace/private-marker",
        ] {
            assert!(!debug.contains(secret));
        }
        let (_directory, spool) = fixture();
        let debug = format!("{spool:?}");
        assert!(!debug.contains("owner-marker"));
        assert!(!debug.contains(spool.root.to_string_lossy().as_ref()));
        assert!(
            !NativeAgentSpoolError::CorruptInput
                .to_string()
                .contains("marker")
        );
    }
}
