//! # vyane-config
//!
//! Configuration and profile resolution: parses the layered TOML config
//! shape (see `profiles.example.toml`), merges layers with per-field
//! precedence, and resolves a profile name / `provider/model` pair / failover
//! chain into fully-resolved [`vyane_core::BoundTarget`]s.
//!
//! This crate only *resolves* configuration — it performs no HTTP calls and
//! spawns no processes. See `docs/plan/WP-01.md` for the work-package plan.

mod layer;
mod model;
mod resolve;
mod secrets;

pub use layer::ConfigLayers;
pub use model::{GenParamsPatch, ProfilePatch, RawFailoverElement, RawRoot};
pub use resolve::ResolvedConfig;
pub use secrets::{env_lookup_with_secrets, load_secrets_file};

use std::path::PathBuf;

/// Where the user-level and project-level config files live by convention.
///
/// Uses `dirs::config_dir()` for the user file (e.g. `~/.config` on Unix,
/// the platform config dir elsewhere), joined with `vyane/config.toml`. The
/// project file is always `.vyane/config.toml` relative to the current
/// directory, since project overrides are directory-scoped by definition.
pub fn default_user_config_path() -> Option<PathBuf> {
    dirs::config_dir().map(|dir| dir.join("vyane").join("config.toml"))
}

pub fn default_project_config_path() -> PathBuf {
    PathBuf::from(".vyane").join("config.toml")
}

/// Load and merge the standard on-disk layer stack in precedence order:
/// built-in defaults (empty for v0.1 — no compiled-in provider endpoints),
/// then the user file, then the project file. Missing files are skipped, not
/// errors.
///
/// A caller that also has CLI-supplied overrides merges those last, on top
/// of the returned [`ConfigLayers`], via [`ConfigLayers::merge`] — this
/// function stops at project scope because CLI-argument parsing lives
/// outside this crate.
pub fn load_default_layers() -> vyane_core::Result<ConfigLayers> {
    let mut layers = ConfigLayers::new();
    if let Some(user_path) = default_user_config_path() {
        layers.merge_file(&user_path)?;
    }
    layers.merge_file(&default_project_config_path())?;
    Ok(layers)
}
