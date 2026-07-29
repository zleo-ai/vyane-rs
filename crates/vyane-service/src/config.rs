//! Config loading and runtime assembly.
//!
//! Lifted verbatim from the old `vyane-cli/src/app.rs` so the CLI, REST API,
//! and MCP server all share the same config layers + secrets-file env lookup.
//! The env-lookup contract (secrets file wins over real process env) is the
//! one the kernel's `resolve_failover_with` relies on to keep endpoint secrets
//! out of the process environment.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use vyane_config::{ConfigLayers, ResolvedConfig, load_secrets_file};
use vyane_core::{Ledger, Sandbox, SessionStore};
use vyane_ledger::{FsSessionStore, JsonlLedger};

use crate::factory::AssemblerFactory;
use crate::native_permissions::NativePermissionSet;

const APP_DIR_NAME: &str = "vyane";
const SECRETS_FILE: &str = "secrets.env";
const TASK_METADATA_DB_FILE: &str = "tasks.sqlite3";
const AGENT_METADATA_DB_FILE: &str = "agent-runs.sqlite3";
const MESSAGE_DB_FILE: &str = "messages.sqlite3";
const GOAL_DB_FILE: &str = "goals.sqlite3";
const EVENT_LOG_DIR: &str = "events";
const MANAGED_PERMISSION_CONFIG_ENV: &str = "VYANE_MANAGED_PERMISSION_CONFIG";
const MANAGED_NATIVE_CONFIG_ENV: &str = "VYANE_MANAGED_NATIVE_CONFIG";

/// The loaded configuration plus the secrets needed to resolve endpoints.
///
/// Carries the env-lookup closure used by [`ResolvedConfig::resolve_failover_with`]:
/// secrets file wins over real process env, so a key placed in `secrets.env`
/// overrides one exported in the shell. This is what keeps endpoint secrets
/// out of `ps`/`/proc` visibility while still being injectable.
#[derive(Clone)]
pub struct LoadedConfig {
    pub config: ResolvedConfig,
    pub files: Vec<PathBuf>,
    pub secrets: BTreeMap<String, String>,
}

impl LoadedConfig {
    pub fn env_lookup(&self, name: &str) -> Option<String> {
        self.secrets
            .get(name)
            .cloned()
            .or_else(|| std::env::var(name).ok())
    }

    /// Check credential presence without cloning a secrets-file value or
    /// converting a process value into a `String`. Static diagnostics use this
    /// instead of constructing an authenticated endpoint, and never retain or
    /// serialize the observed process value.
    pub(crate) fn env_present(&self, name: &str) -> bool {
        self.secrets.contains_key(name) || std::env::var_os(name).is_some()
    }
}

/// Load the default user + project config layers, merging each file and its
/// sibling `secrets.env`. Pass `override_path` to load a single file instead
/// (mirrors `--config`). When `VYANE_MANAGED_PERMISSION_CONFIG` is set, that
/// exact file contributes final monotonic permission ceilings and cannot
/// configure providers, profiles, or secrets. The older
/// `VYANE_MANAGED_NATIVE_CONFIG` name remains a compatibility alias; setting
/// both is rejected as ambiguous.
pub fn load_config(override_path: Option<&Path>) -> Result<LoadedConfig> {
    let managed_permission_path =
        std::env::var_os(MANAGED_PERMISSION_CONFIG_ENV).map(PathBuf::from);
    let managed_native_path = std::env::var_os(MANAGED_NATIVE_CONFIG_ENV).map(PathBuf::from);
    let managed_path =
        select_managed_permission_path(managed_permission_path, managed_native_path)?;
    load_config_with_managed_path(override_path, managed_path.as_deref())
}

fn select_managed_permission_path(
    managed_permission_path: Option<PathBuf>,
    managed_native_path: Option<PathBuf>,
) -> Result<Option<PathBuf>> {
    match (managed_permission_path, managed_native_path) {
        (Some(_), Some(_)) => Err(anyhow!(
            "both managed permission config environment variables are set"
        )),
        (Some(path), None) | (None, Some(path)) => Ok(Some(path)),
        (None, None) => Ok(None),
    }
}

fn load_config_with_managed_path(
    override_path: Option<&Path>,
    managed_path: Option<&Path>,
) -> Result<LoadedConfig> {
    let mut files = config_file_list(override_path);
    let mut layers = ConfigLayers::new();
    let mut secrets = BTreeMap::new();

    for file in &files {
        layers
            .merge_file(file)
            .with_context(|| format!("load config {}", file.display()))?;
        if let Some(parent) = file.parent() {
            let path = parent.join(SECRETS_FILE);
            for (key, value) in load_secrets_file(&path)
                .with_context(|| format!("load secrets {}", path.display()))?
            {
                secrets.insert(key, value);
            }
        }
    }

    if let Some(path) = managed_path {
        layers
            .merge_managed_permission_file(path)
            .with_context(|| format!("load managed permission config {}", path.display()))?;
        files.push(path.to_path_buf());
    }
    for ceiling in &layers.native_permission_ceilings {
        NativePermissionSet::try_from(ceiling).context("validate native permission ceiling")?;
    }

    Ok(LoadedConfig {
        config: layers.into(),
        files,
        secrets,
    })
}

fn config_file_list(override_path: Option<&Path>) -> Vec<PathBuf> {
    if let Some(path) = override_path {
        return vec![path.to_path_buf()];
    }

    let mut files = Vec::new();
    if let Some(user_path) = vyane_config::default_user_config_path() {
        files.push(user_path);
    }
    files.push(vyane_config::default_project_config_path());
    files
}

/// The assembled dispatcher, ledger, and session store — everything a front-end
/// needs to dispatch and query runs.
#[derive(Clone)]
pub struct Runtime {
    pub dispatcher: vyane_kernel::Dispatcher,
    pub ledger: Arc<dyn Ledger>,
    pub sessions: Arc<dyn SessionStore>,
}

impl Runtime {
    pub fn new(config: ResolvedConfig, paths: StoragePaths) -> Result<Self> {
        std::fs::create_dir_all(&paths.data_dir)
            .with_context(|| format!("create data dir {}", paths.data_dir.display()))?;
        std::fs::create_dir_all(&paths.sessions_dir)
            .with_context(|| format!("create sessions dir {}", paths.sessions_dir.display()))?;
        if let Some(parent) = paths.ledger_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create ledger dir {}", parent.display()))?;
        }

        let harness_sandbox_ceiling = config
            .harness_permission_ceilings
            .iter()
            .fold(Sandbox::Full, |current, ceiling| {
                current.restrict_with(ceiling.max_sandbox)
            });
        let factory = Arc::new(AssemblerFactory::new(config));
        let ledger: Arc<dyn Ledger> = Arc::new(JsonlLedger::new(paths.ledger_path));
        let sessions: Arc<dyn SessionStore> = Arc::new(FsSessionStore::new(paths.sessions_dir));
        let dispatcher =
            vyane_kernel::Dispatcher::new(factory, Arc::clone(&ledger), Arc::clone(&sessions))
                .with_harness_sandbox_ceiling(harness_sandbox_ceiling);

        Ok(Self {
            dispatcher,
            ledger,
            sessions,
        })
    }
}

#[derive(Clone)]
pub struct StoragePaths {
    pub data_dir: PathBuf,
    pub ledger_path: PathBuf,
    pub sessions_dir: PathBuf,
    pub workflows_dir: PathBuf,
}

impl StoragePaths {
    /// Build every service storage path below an explicit data directory.
    ///
    /// This is the non-global construction path used by embedders and tests:
    /// callers do not need to mutate `VYANE_DATA_DIR`, so independently running
    /// services cannot race through process-wide environment state.
    #[must_use]
    pub fn from_data_dir(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        Self {
            ledger_path: data_dir.join("ledger.jsonl"),
            sessions_dir: data_dir.join("sessions"),
            workflows_dir: data_dir.join("workflows"),
            data_dir,
        }
    }

    pub fn resolve() -> Result<Self> {
        let data_dir = match std::env::var_os("VYANE_DATA_DIR") {
            Some(raw) => PathBuf::from(raw),
            None => dirs::data_dir()
                .ok_or_else(|| anyhow!("could not determine platform data directory"))?
                .join(APP_DIR_NAME),
        };
        Ok(Self::from_data_dir(data_dir))
    }

    /// SQLite file containing secret-free durable task control metadata.
    #[must_use]
    pub fn task_metadata_db_path(&self) -> PathBuf {
        self.data_dir.join(TASK_METADATA_DB_FILE)
    }

    /// SQLite source of truth for AgentRun and worker control metadata.
    #[must_use]
    pub fn agent_metadata_db_path(&self) -> PathBuf {
        self.data_dir.join(AGENT_METADATA_DB_FILE)
    }

    /// SQLite source of truth for immutable messages and mutable deliveries.
    #[must_use]
    pub fn message_db_path(&self) -> PathBuf {
        self.data_dir.join(MESSAGE_DB_FILE)
    }

    /// SQLite source of truth for owner-scoped goal snapshots and events.
    #[must_use]
    pub fn goal_db_path(&self) -> PathBuf {
        self.data_dir.join(GOAL_DB_FILE)
    }

    /// Owner-isolated EventLog projection root.
    #[must_use]
    pub fn event_log_dir(&self) -> PathBuf {
        self.data_dir.join(EVENT_LOG_DIR)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use vyane_config::HarnessPermissionCeiling;

    #[test]
    fn explicit_data_dir_derives_all_storage_paths_without_environment_state() {
        let paths = StoragePaths::from_data_dir("/tmp/vyane-explicit-path-test");

        assert_eq!(
            paths.ledger_path,
            PathBuf::from("/tmp/vyane-explicit-path-test/ledger.jsonl")
        );
        assert_eq!(
            paths.sessions_dir,
            PathBuf::from("/tmp/vyane-explicit-path-test/sessions")
        );
        assert_eq!(
            paths.workflows_dir,
            PathBuf::from("/tmp/vyane-explicit-path-test/workflows")
        );
        assert_eq!(
            paths.task_metadata_db_path(),
            PathBuf::from("/tmp/vyane-explicit-path-test/tasks.sqlite3")
        );
        assert_eq!(
            paths.agent_metadata_db_path(),
            PathBuf::from("/tmp/vyane-explicit-path-test/agent-runs.sqlite3")
        );
        assert_eq!(
            paths.message_db_path(),
            PathBuf::from("/tmp/vyane-explicit-path-test/messages.sqlite3")
        );
        assert_eq!(
            paths.goal_db_path(),
            PathBuf::from("/tmp/vyane-explicit-path-test/goals.sqlite3")
        );
        assert_eq!(
            paths.event_log_dir(),
            PathBuf::from("/tmp/vyane-explicit-path-test/events")
        );
    }

    #[test]
    fn managed_permission_ceilings_are_loaded_and_validated_without_environment_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.toml");
        let managed = dir.path().join("managed.toml");
        std::fs::write(&base, "").unwrap();
        std::fs::write(
            &managed,
            r#"
            [native_permissions.filesystem_read]
            exclude = [".env*"]

            [native_permissions.web_fetch]
            allow_domains = ["example.com"]
            max_fetches = 2

            [harness_permissions]
            max_sandbox = "write"
            "#,
        )
        .unwrap();

        let loaded = load_config_with_managed_path(Some(&base), Some(&managed)).unwrap();
        assert_eq!(loaded.config.native_permission_ceilings.len(), 1);
        assert_eq!(
            loaded.config.harness_permission_ceilings[0].max_sandbox,
            Sandbox::Write
        );
        assert_eq!(loaded.files, [base, managed]);
    }

    #[test]
    fn runtime_applies_the_strictest_harness_sandbox_ceiling_before_preparation() {
        let mut layers = ConfigLayers::new();
        layers.harness_permission_ceilings = vec![
            HarnessPermissionCeiling {
                max_sandbox: Sandbox::Write,
            },
            HarnessPermissionCeiling {
                max_sandbox: Sandbox::ReadOnly,
            },
        ];
        let directory = tempfile::tempdir().unwrap();
        let runtime =
            Runtime::new(layers.into(), StoragePaths::from_data_dir(directory.path())).unwrap();
        let task = vyane_core::TaskSpec::new("edit").with_sandbox(Sandbox::Write);
        let chain = vec![vyane_core::BoundTarget {
            target: vyane_core::Target {
                provider: vyane_core::ProviderId::new("local"),
                protocol: vyane_core::Protocol::AnthropicMessages,
                harness: Some(vyane_core::HarnessKind::ClaudeCode),
                model: vyane_core::ModelId::new("test"),
            },
            transport: vyane_core::AdapterTransport::CliWrap,
            endpoint: None,
            params: vyane_core::GenParams::default(),
        }];

        let error = match runtime.dispatcher.prepare(&task, chain) {
            Err(error) => error,
            Ok(_) => panic!("write request must exceed the effective read-only ceiling"),
        };
        assert_eq!(error.kind, vyane_core::ErrorKind::Config);
    }

    #[test]
    fn managed_permission_environment_aliases_are_unambiguous() {
        let current = PathBuf::from("/etc/vyane/permissions.toml");
        let legacy = PathBuf::from("/etc/vyane/native-permissions.toml");

        assert_eq!(
            select_managed_permission_path(Some(current.clone()), None).unwrap(),
            Some(current)
        );
        assert_eq!(
            select_managed_permission_path(None, Some(legacy.clone())).unwrap(),
            Some(legacy)
        );
        assert!(
            select_managed_permission_path(None, None)
                .unwrap()
                .is_none()
        );
        assert!(
            select_managed_permission_path(
                Some(PathBuf::from("current")),
                Some(PathBuf::from("legacy"))
            )
            .is_err()
        );
    }

    #[test]
    fn invalid_managed_native_ceiling_fails_config_loading() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.toml");
        let managed = dir.path().join("managed.toml");
        std::fs::write(&base, "").unwrap();
        std::fs::write(
            &managed,
            r#"
            [native_permissions.command_network]
            allow = [{ host = "example.com", ports = [443] }]
            "#,
        )
        .unwrap();

        assert!(load_config_with_managed_path(Some(&base), Some(&managed)).is_err());
    }

    #[test]
    fn command_ceiling_with_read_exclusions_fails_config_loading() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.toml");
        let managed = dir.path().join("managed.toml");
        std::fs::write(&base, "").unwrap();
        std::fs::write(
            &managed,
            r#"
            [native_permissions.filesystem_read]
            exclude = [".env*"]

            [native_permissions.command_execution]
            allow = [{ program = "git", args_prefix = ["status"] }]
            "#,
        )
        .unwrap();

        assert!(load_config_with_managed_path(Some(&base), Some(&managed)).is_err());
    }
}
