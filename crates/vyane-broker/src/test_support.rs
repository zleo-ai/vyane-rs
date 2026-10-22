//! Test-only directory fixtures.
//!
//! Callers may use umask 002, which makes a new directory group-writable.
//! Database stores reject that parent. The fixture tightens it itself.

#![allow(clippy::unwrap_used, dead_code)]

use std::fs;
use std::path::{Component, Path, PathBuf};

use tempfile::TempDir;

pub fn private_tempdir() -> TempDir {
    let directory = tempfile::tempdir().expect("tempdir");
    restrict_private(directory.path()).expect("restrict tempdir");
    directory
}

/// Create `path` inside `boundary` and remove group and world write from every
/// directory from `path` up through `boundary`.
///
/// `boundary` is the temp root. Directories outside it are left untouched.
/// Every component of `boundary`, and every component from `boundary` down to
/// `path`, must be a real directory. A symbolic link on either walk is an
/// error, as is a path that leaves `boundary`.
pub fn create_private_dir_all(boundary: &Path, path: &Path) -> Result<(), String> {
    ensure_boundary(boundary)?;
    let relative = path
        .strip_prefix(boundary)
        .map_err(|_| "path is outside the private directory boundary".to_string())?;
    if relative.as_os_str().is_empty() {
        return restrict_private(boundary);
    }
    for component in relative.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err("path escapes the private directory boundary".into());
        }
    }
    restrict_private(boundary)?;
    let mut current = boundary.to_path_buf();
    for component in relative.components() {
        current.push(component);
        ensure_real_directory(&current)?;
    }
    Ok(())
}

pub fn create_private_dir(path: &Path) {
    fs::create_dir(path).expect("create directory");
    restrict_private(path).expect("restrict directory");
}

fn ensure_boundary(boundary: &Path) -> Result<(), String> {
    if boundary.as_os_str().is_empty() {
        return Err("private directory boundary is not a directory".into());
    }
    let mut current = PathBuf::new();
    for component in boundary.components() {
        current.push(component.as_os_str());
        let metadata = fs::symlink_metadata(&current).map_err(|error| error.to_string())?;
        if metadata.file_type().is_symlink() {
            return Err("private directory boundary passes through a symbolic link".into());
        }
        if !metadata.is_dir() {
            return Err("private directory boundary is not a directory".into());
        }
    }
    Ok(())
}

fn ensure_real_directory(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err("path passes through a symbolic link".into())
        }
        Ok(metadata) if metadata.is_dir() => restrict_private(path),
        Ok(_) => Err("path is not a directory".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(|error| error.to_string())?;
            let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err("path passes through a symbolic link".into());
            }
            restrict_private(path)
        }
        Err(error) => Err(error.to_string()),
    }
}

fn restrict_private(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| format!("restrict directory permissions: {error}"))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_dir_all_rejects_paths_outside_the_boundary_and_symbolic_links() {
        let root = private_tempdir();
        let boundary = root.path().join("boundary");
        fs::create_dir(&boundary).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(root.path(), fs::Permissions::from_mode(0o775)).unwrap();
        }

        let outside = root.path().join("outside");
        let error = create_private_dir_all(&boundary, &outside.join("child")).unwrap_err();
        assert!(error.contains("boundary"), "{error}");
        assert!(!outside.exists());

        let escaped = boundary.join("..").join("escaped");
        let error = create_private_dir_all(&boundary, &escaped).unwrap_err();
        assert!(error.contains("boundary"), "{error}");
        assert!(!root.path().join("escaped").exists());

        let created = boundary.join("wide").join("child");
        create_private_dir_all(&boundary, &created).unwrap();
        assert!(created.is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = |path: &Path| fs::metadata(path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(&created), 0o700);
            assert_eq!(mode(&boundary.join("wide")), 0o700);
            assert_eq!(mode(&boundary), 0o700);
            assert_eq!(mode(root.path()), 0o775);
        }

        #[cfg(unix)]
        {
            let link_target = root.path().join("link-target");
            fs::create_dir(&link_target).unwrap();
            std::os::unix::fs::symlink(&link_target, boundary.join("link")).unwrap();
            let error = create_private_dir_all(&boundary, &boundary.join("link").join("child"))
                .unwrap_err();
            assert!(error.contains("symbolic link"), "{error}");
            assert!(!link_target.join("child").exists());
        }
    }

    #[cfg(unix)]
    #[test]
    fn private_dir_all_rejects_a_symbolic_link_ancestor_of_the_boundary() {
        let root = private_tempdir();
        let real_parent = root.path().join("real-parent");
        fs::create_dir(&real_parent).unwrap();
        let linked_parent = root.path().join("linked-parent");
        std::os::unix::fs::symlink(&real_parent, &linked_parent).unwrap();
        let boundary = linked_parent.join("boundary");
        fs::create_dir(&boundary).unwrap();

        let error = create_private_dir_all(&boundary, &boundary.join("child")).unwrap_err();
        assert!(error.contains("symbolic link"), "{error}");
        assert!(!real_parent.join("boundary").join("child").exists());
    }
}
