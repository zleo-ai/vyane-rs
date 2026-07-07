use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use tracing_subscriber::EnvFilter;
use vyane_config::{ConfigLayers, ResolvedConfig, load_secrets_file};
use vyane_core::{Ledger, SessionStore};
use vyane_ledger::{FsSessionStore, JsonlLedger};

use crate::factory::AssemblerFactory;

const APP_DIR_NAME: &str = "vyane";
const SECRETS_FILE: &str = "secrets.env";

pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

pub struct LoadedConfig {
    pub config: ResolvedConfig,
    pub files: Vec<PathBuf>,
    pub secrets: BTreeMap<String, String>,
}

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

impl LoadedConfig {
    pub fn env_lookup(&self, name: &str) -> Option<String> {
        self.secrets
            .get(name)
            .cloned()
            .or_else(|| std::env::var(name).ok())
    }
}

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
    pub tasks_dir: PathBuf,
}

impl StoragePaths {
    pub fn resolve() -> Result<Self> {
        let data_dir = match std::env::var_os("VYANE_DATA_DIR") {
            Some(raw) => PathBuf::from(raw),
            None => dirs::data_dir()
                .ok_or_else(|| anyhow!("could not determine platform data directory"))?
                .join(APP_DIR_NAME),
        };
        Ok(Self {
            ledger_path: data_dir.join("ledger.jsonl"),
            sessions_dir: data_dir.join("sessions"),
            workflows_dir: data_dir.join("workflows"),
            tasks_dir: crate::task::tasks_root(&data_dir),
            data_dir,
        })
    }
}
