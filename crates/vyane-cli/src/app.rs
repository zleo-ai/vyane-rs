//! Tracing init + re-exports of the service-layer config/runtime types.
//!
//! The config loading, runtime assembly, and executor factory now live in
//! `vyane-service` so the REST API and MCP server share them. The CLI keeps
//! thin re-exports here so `command.rs` and `task/` callers are unchanged.

pub use vyane_service::{LoadedConfig, Runtime, StoragePaths, load_config};

pub fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

/// Resolve storage paths and derive the CLI-only `tasks_dir` in one step.
/// The detached-worker task layout (`<data_dir>/tasks`) is a CLI concern —
/// the REST API and MCP server don't manage detached runs.
pub fn resolve_paths_with_tasks() -> anyhow::Result<(StoragePaths, std::path::PathBuf)> {
    let paths = StoragePaths::resolve()?;
    let tasks_dir = crate::task::tasks_root(&paths.data_dir);
    Ok((paths, tasks_dir))
}
