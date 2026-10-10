//! Test-only directory fixtures.
//!
//! Callers may use umask 002, which makes a new directory group-writable.
//! Database stores reject that parent. The fixture tightens it itself.

#![allow(clippy::unwrap_used, dead_code)]

use std::fs;
use std::path::Path;

use tempfile::TempDir;

pub fn private_tempdir() -> TempDir {
    let directory = tempfile::tempdir().expect("tempdir");
    restrict_private(directory.path());
    directory
}

/// Create `path` and every missing ancestor, then remove group and world
/// write from `path` and from ancestors that are still group- or world-writable.
///
/// Walking stops at a directory that is already private, or at a sticky
/// directory such as the system temp root. Callers that intentionally widen
/// a mode do so after this returns.
pub fn create_private_dir_all(path: &Path) {
    fs::create_dir_all(path).expect("create directory");
    restrict_private_upwards(path);
}

pub fn create_private_dir(path: &Path) {
    fs::create_dir(path).expect("create directory");
    restrict_private(path);
}

fn restrict_private(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .expect("restrict directory permissions");
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

fn restrict_private_upwards(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mut current = path.to_path_buf();
        loop {
            let metadata = fs::metadata(&current).expect("read directory mode");
            let mode = metadata.permissions().mode();
            if mode & 0o1000 != 0 {
                break;
            }
            if mode & 0o022 != 0 {
                fs::set_permissions(&current, fs::Permissions::from_mode(0o700))
                    .expect("restrict directory permissions");
            }
            let Some(parent) = current.parent() else {
                break;
            };
            if parent.as_os_str().is_empty() {
                break;
            }
            let parent_mode = fs::metadata(parent)
                .expect("read parent directory mode")
                .permissions()
                .mode();
            if parent_mode & 0o1000 != 0 || parent_mode & 0o022 == 0 {
                break;
            }
            current = parent.to_path_buf();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}
