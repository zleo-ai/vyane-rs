//! Binary presence/executability probe for [`vyane_core::Harness::available`].
//!
//! `available()` must NOT run a task — it only answers "is this CLI present and
//! runnable on this machine?". We resolve the binary name against `PATH` (and,
//! if the name is a path, check that path directly) and verify the file is
//! executable, without spawning it.

use std::path::{Path, PathBuf};

/// Whether `bin` is present and executable on this machine.
///
/// * If `bin` contains a path separator, that exact path is checked.
/// * Otherwise every `PATH` entry is searched for an executable `bin`.
///
/// Runs no subprocess. Returns quickly; async only so it fits the trait's
/// `async fn available`.
pub(crate) async fn binary_available(bin: &str) -> bool {
    let bin = bin.to_string();
    // Filesystem checks are cheap and blocking; run them on a blocking thread so
    // we never stall the async runtime, and so the API stays `async`.
    tokio::task::spawn_blocking(move || resolve_executable(&bin).is_some())
        .await
        .unwrap_or(false)
}

/// Resolve `bin` to an executable path, or `None` if not found.
fn resolve_executable(bin: &str) -> Option<PathBuf> {
    if bin.is_empty() {
        return None;
    }
    if bin.contains(std::path::MAIN_SEPARATOR) {
        let p = PathBuf::from(bin);
        return if is_executable(&p) { Some(p) } else { None };
    }
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(bin);
        if is_executable(&candidate) {
            return Some(candidate);
        }
    }
    None
}

#[cfg(unix)]
fn is_executable(p: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    match std::fs::metadata(p) {
        Ok(meta) => meta.is_file() && (meta.permissions().mode() & 0o111 != 0),
        Err(_) => false,
    }
}

#[cfg(not(unix))]
fn is_executable(p: &Path) -> bool {
    // Best-effort on non-Unix: existence as a file. v0.1 targets Unix.
    std::fs::metadata(p).map(|m| m.is_file()).unwrap_or(false)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn missing_binary_is_unavailable() {
        assert!(!binary_available("definitely-not-a-real-binary-xyz-42").await);
    }

    #[tokio::test]
    async fn empty_name_is_unavailable() {
        assert!(!binary_available("").await);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn present_executable_on_path_is_available() {
        // `sh` is guaranteed present and executable on any Unix.
        assert!(binary_available("sh").await);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explicit_path_to_executable_resolves() {
        assert!(binary_available("/bin/sh").await);
    }
}
