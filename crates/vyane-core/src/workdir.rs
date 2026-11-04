//! Process-local pinning for a mutating execution workdir.
//!
//! A canonical path is useful audit evidence, but it is not an execution
//! capability: another process can rename or replace that path after
//! admission.  [`PinnedWorkdir`] keeps the admitted directory itself open so
//! Unix harnesses can inherit the stable object rather than reopening the
//! path.  The handle is intentionally not serializable; [`WorkdirIdentity`]
//! is the separate, serializable evidence used for drift checks.

use std::ffi::OsStr;
use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{ErrorKind, Result, VyaneError};

/// Workspace directory names whose contents a host tool executes or trusts
/// outside any sandbox: git hooks and `core.fsmonitor` / `core.hooksPath` in
/// `.git`, and harness settings and hooks in `.vyane`, `.codex` and
/// `.claude`.
///
/// Sandboxed execution (native file tools and command writable roots) must
/// never write beneath a path that has any component equal to one of these
/// names or to one of [`HOST_CONTROL_FILE_NAMES`], compared ASCII
/// case-insensitively. This is a hard floor that no policy exclusion list can
/// relax.
pub const HOST_CONTROL_COMPONENTS: [&str; 4] = [".git", ".vyane", ".codex", ".claude"];

/// Workspace file names that a supported harness reads at startup and acts on
/// outside any sandbox: `.mcp.json` is Claude Code's project MCP server
/// configuration, and Claude Code launches an approved server's `command` on
/// the host when it next starts in that workdir. Its permission flags
/// (`--permission-mode`, `--disallowedTools`) do not govern MCP server
/// startup.
///
/// These fall under the same floor as [`HOST_CONTROL_COMPONENTS`] and are
/// matched as whole components at any depth, so a directory with one of these
/// names is refused too.
pub const HOST_CONTROL_FILE_NAMES: [&str; 1] = [".mcp.json"];

/// Whether `component` names a host control directory or file (see
/// [`HOST_CONTROL_COMPONENTS`] and [`HOST_CONTROL_FILE_NAMES`]), ignoring
/// ASCII case.
#[must_use]
pub fn is_host_control_component(component: &OsStr) -> bool {
    let bytes = component.as_encoded_bytes();
    HOST_CONTROL_COMPONENTS
        .iter()
        .chain(HOST_CONTROL_FILE_NAMES.iter())
        .any(|control| bytes.eq_ignore_ascii_case(control.as_bytes()))
}

/// Serializable identity of an opened execution directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkdirIdentity {
    /// Filesystem device number returned by `fstat(2)`.
    pub device: u64,
    /// Inode number returned by `fstat(2)`.
    pub inode: u64,
}

/// An opened, stable execution directory.
///
/// Clones share the same open file description.  No serialization
/// implementation is provided, so an audit snapshot cannot turn back into an
/// executable directory handle after a process boundary.
#[derive(Clone)]
pub struct PinnedWorkdir {
    canonical_path: PathBuf,
    identity: WorkdirIdentity,
    handle: Arc<File>,
}

impl PinnedWorkdir {
    /// Open and identify an existing directory for mutating execution.
    ///
    /// Linux is the currently supported enforcement platform.  Other
    /// platforms fail closed instead of degrading to a path-only check.
    #[cfg(target_os = "linux")]
    pub fn open(requested_path: impl AsRef<Path>) -> Result<Self> {
        use std::os::fd::AsRawFd as _;
        use std::os::unix::fs::MetadataExt as _;

        let requested_path = requested_path.as_ref();
        // The open is the linearization point: first acquire the directory
        // object, then derive its audit path through that handle. Never resolve
        // a pathname and subsequently reopen the resolved string.
        for _ in 0..3 {
            let handle = File::open(requested_path).map_err(|source| {
                VyaneError::with_source(ErrorKind::Io, "failed to pin execution workdir", source)
            })?;
            let handle_metadata = handle.metadata().map_err(|source| {
                VyaneError::with_source(
                    ErrorKind::Io,
                    "failed to inspect pinned execution workdir",
                    source,
                )
            })?;
            if !handle_metadata.is_dir() {
                return Err(VyaneError::new(
                    ErrorKind::Unsupported,
                    "mutating execution workdir is not a directory",
                ));
            }

            let fd_path = PathBuf::from(format!("/proc/self/fd/{}", handle.as_raw_fd()));
            let canonical_path = match std::fs::canonicalize(&fd_path) {
                Ok(path) => path,
                Err(_) => continue,
            };
            let audit_metadata = match std::fs::metadata(&canonical_path) {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };
            let requested_metadata = match std::fs::metadata(requested_path) {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };
            if handle_metadata.dev() != audit_metadata.dev()
                || handle_metadata.ino() != audit_metadata.ino()
                || handle_metadata.dev() != requested_metadata.dev()
                || handle_metadata.ino() != requested_metadata.ino()
            {
                continue;
            }
            return Ok(Self {
                canonical_path,
                identity: WorkdirIdentity {
                    device: handle_metadata.dev(),
                    inode: handle_metadata.ino(),
                },
                handle: Arc::new(handle),
            });
        }
        Err(VyaneError::new(
            ErrorKind::Io,
            "execution workdir changed repeatedly during admission",
        ))
    }

    /// Rebuild a pin from an already inherited directory descriptor.
    ///
    /// The caller transfers ownership of `handle` and supplies the frozen
    /// parent-side audit identity. The current pathname is deliberately not
    /// reopened: it may have been renamed or replaced after submission.
    #[cfg(target_os = "linux")]
    pub fn from_open_file(
        canonical_path: impl Into<PathBuf>,
        handle: File,
        expected_identity: &WorkdirIdentity,
    ) -> Result<Self> {
        use std::os::unix::fs::MetadataExt as _;

        let canonical_path = canonical_path.into();
        if !canonical_path.is_absolute() {
            return Err(VyaneError::new(
                ErrorKind::Config,
                "inherited pinned workdir audit path is not absolute",
            ));
        }
        let metadata = handle.metadata().map_err(|source| {
            VyaneError::with_source(
                ErrorKind::Io,
                "failed to inspect inherited pinned workdir",
                source,
            )
        })?;
        if !metadata.is_dir() {
            return Err(VyaneError::new(
                ErrorKind::Unsupported,
                "inherited mutating workdir handle is not a directory",
            ));
        }
        let observed = WorkdirIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        if &observed != expected_identity {
            return Err(VyaneError::new(
                ErrorKind::Config,
                "inherited pinned workdir identity does not match frozen admission",
            ));
        }
        Ok(Self {
            canonical_path,
            identity: observed,
            handle: Arc::new(handle),
        })
    }

    #[cfg(not(target_os = "linux"))]
    pub fn from_open_file(
        _canonical_path: impl Into<PathBuf>,
        _handle: File,
        _expected_identity: &WorkdirIdentity,
    ) -> Result<Self> {
        Err(VyaneError::new(
            ErrorKind::Unsupported,
            "inherited pinned workdirs are currently supported only on Linux",
        ))
    }

    #[cfg(not(target_os = "linux"))]
    pub fn open(_canonical_path: impl AsRef<Path>) -> Result<Self> {
        Err(VyaneError::new(
            ErrorKind::Unsupported,
            "pinned mutating workdirs are currently supported only on Linux",
        ))
    }

    pub fn canonical_path(&self) -> &Path {
        &self.canonical_path
    }

    pub fn identity(&self) -> &WorkdirIdentity {
        &self.identity
    }

    /// Borrow the process-local directory handle.
    ///
    /// Harness spawn code duplicates this handle before `fork`; callers must
    /// never persist its numeric descriptor.
    pub fn handle(&self) -> &File {
        &self.handle
    }
}

impl fmt::Debug for PinnedWorkdir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedWorkdir")
            .field("canonical_path", &self.canonical_path)
            .field("identity", &self.identity)
            .field("handle", &"<process-local>")
            .finish()
    }
}

#[cfg(all(test, target_os = "linux"))]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::os::unix::fs::MetadataExt as _;

    use super::*;

    #[test]
    fn open_linearizes_on_handle_and_derives_canonical_symlink_target() {
        let root = tempfile::tempdir().unwrap();
        let actual = root.path().join("actual");
        let requested = root.path().join("requested-link");
        std::fs::create_dir(&actual).unwrap();
        std::os::unix::fs::symlink(&actual, &requested).unwrap();

        let pinned = PinnedWorkdir::open(&requested).unwrap();
        let metadata = pinned.handle().metadata().unwrap();
        assert_eq!(pinned.canonical_path(), actual.as_path());
        assert_eq!(pinned.identity().device, metadata.dev());
        assert_eq!(pinned.identity().inode, metadata.ino());
    }
}

#[cfg(test)]
mod control_component_tests {
    use std::ffi::OsStr;

    use super::is_host_control_component;

    #[test]
    fn host_control_components_match_whole_names_ignoring_ascii_case() {
        for name in [".git", ".GIT", ".Git", ".vyane", ".CODEX", ".Claude"] {
            assert!(is_host_control_component(OsStr::new(name)), "{name}");
        }
        for name in ["git", ".github", ".gitignore", "x.git", ".claude2", "", "."] {
            assert!(!is_host_control_component(OsStr::new(name)), "{name}");
        }
    }

    #[test]
    fn host_control_file_names_match_whole_names_ignoring_ascii_case() {
        for name in [".mcp.json", ".MCP.json", ".Mcp.Json"] {
            assert!(is_host_control_component(OsStr::new(name)), "{name}");
        }
        for name in [
            "mcp.json",
            ".mcp.json.bak",
            "x.mcp.json",
            ".mcp",
            ".mcp.jsonc",
        ] {
            assert!(!is_host_control_component(OsStr::new(name)), "{name}");
        }
    }
}
