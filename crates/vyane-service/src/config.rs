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
use vyane_core::{Ledger, SessionStore};
use vyane_ledger::{FsSessionStore, JsonlLedger};

use crate::factory::AssemblerFactory;

const APP_DIR_NAME: &str = "vyane";
const SECRETS_FILE: &str = "secrets.env";
const TASK_METADATA_DB_FILE: &str = "tasks.sqlite3";
const AGENT_METADATA_DB_FILE: &str = "agent-runs.sqlite3";
const MESSAGE_DB_FILE: &str = "messages.sqlite3";
const GOAL_DB_FILE: &str = "goals.sqlite3";
const EVENT_LOG_DIR: &str = "events";

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
/// (mirrors `--config`).
pub fn load_config(override_path: Option<&Path>) -> Result<LoadedConfig> {
    let files = config_file_list(override_path);
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

        let factory = Arc::new(AssemblerFactory::new(config));
        let ledger: Arc<dyn Ledger> = Arc::new(JsonlLedger::new(paths.ledger_path));
        let sessions: Arc<dyn SessionStore> = Arc::new(FsSessionStore::new(paths.sessions_dir));
        let dispatcher =
            vyane_kernel::Dispatcher::new(factory, Arc::clone(&ledger), Arc::clone(&sessions));

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
mod tests {
    use super::*;

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
}
