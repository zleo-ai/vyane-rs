//! Bubblewrap argv prefix for a read-only filesystem view.
//!
//! The prefix bind-mounts the host read-only, replaces `/tmp`, `/var/tmp`, and
//! `/run` with private tmpfs mounts, binds the workdir read-only, and bind-mounts
//! only the writable directories the caller names. Hidden paths are mounted after
//! those binds. No adapter calls this module. It does not start a process, does
//! not open a D-Bus proxy, and does not read environment values to discover
//! secret aliases.
//!
//! `bwrap` is resolved on `PATH` when the argv is built. A missing binary is an
//! error. Callers must not fall through to an unisolated command.
//!
//! [`hardlink_runtime_guard`] can warn when a regular file with more than one
//! hardlink appears in a writable directory after a successful check. The scan
//! does not signal a process.

use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::rc::Rc;

/// Private `params.extra` key for a filtering bus socket.
///
/// This module does not read the key and does not start a bus proxy.
pub const VIEW_DBUS_SOCKET_KEY: &str = "_readonly_view_dbus_socket";

/// Private `params.extra` key for extra hidden paths.
///
/// This module does not read the key. Pass hide paths to
/// [`readonly_view_prefix`] directly.
pub const VIEW_HIDE_PATHS_KEY: &str = "_readonly_view_hide_paths";

/// Host paths replaced with private tmpfs mounts, in argv order.
pub const OVERLAY_PATHS: &[&str] = &["/tmp", "/var/tmp", "/run"];

/// Synthetic mounts. A workdir must not be stacked on either one.
pub const SYNTHETIC_PATHS: &[&str] = &["/dev", "/proc"];

/// Cap for hardlink paths named in a refusal or a warning message.
pub const MAX_HARDLINK_EXAMPLES: usize = 5;

/// Stable code for a hardlink that appeared after the view was checked.
pub const HARDLINK_RUNTIME_WARNING_CODE: &str = "readonly_view.hardlink_escape_runtime";

/// The runtime scan's only action. It does not signal a process.
pub const HARDLINK_RUNTIME_WARNING_ACTION: &str = "warn";

const SQLITE_SIDECARS: &[&str] = &["-wal", "-shm", "-journal"];

/// Names passed to `--unsetenv`, in argv order.
///
/// The list is closed. Values already in the environment are not inspected, and
/// aliases are not discovered by comparing secret text.
pub const UNSET_ENV: &[&str] = &[
    "DBUS_SESSION_BUS_ADDRESS",
    "DISPLAY",
    "WAYLAND_DISPLAY",
    "SSH_AUTH_SOCK",
    "VYANE_DAEMON_API_TOKEN",
    "VYANE_DAEMON_WORKER_TOKEN",
    "VYANE_DAEMON_OWNER_TOKENS",
    "VYANE_MEMORY_PRINCIPAL_TOKENS",
    "VYANE_DASHBOARD_API_TOKEN",
    "VYANE_DASHBOARD_OWNER_TOKENS",
    "VYANE_DASHBOARD_INTERNAL_API_TOKEN",
    "VYANE_ENV_FILE",
];

/// The read-only view cannot be built. Callers must not run unconfined.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ReadOnlyViewError {
    message: String,
}

fn view_err(message: impl Into<String>) -> ReadOnlyViewError {
    ReadOnlyViewError {
        message: message.into(),
    }
}

fn bwrap_missing() -> ReadOnlyViewError {
    view_err(
        "bwrap (bubblewrap) not found on PATH; this harness has no verified read-only mode of its own and will not run without the view",
    )
}

/// Existing resolved mount targets, and requested paths that were absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HiddenPathPlan {
    /// Resolved directories, regular files, and pathname sockets to cover.
    pub hidden: Vec<PathBuf>,
    /// Requested paths that did not exist, in first-seen order.
    pub skipped_missing: Vec<PathBuf>,
}

/// A hardlink that showed up in a writable directory after the check passed.
///
/// `action` is always [`HARDLINK_RUNTIME_WARNING_ACTION`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardlinkEscapeWarning {
    /// [`HARDLINK_RUNTIME_WARNING_CODE`].
    pub code: &'static str,
    /// Operator-facing text. It does not include file contents.
    pub message: String,
    /// New regular files whose link count is greater than 1.
    pub paths: Vec<PathBuf>,
    /// Writable directories that were scanned.
    pub writable_dirs: Vec<PathBuf>,
    /// Always `warn`.
    pub action: &'static str,
}

fn utf8_path(path: &Path) -> Result<String, ReadOnlyViewError> {
    path.to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| view_err(format!("path is not valid unicode: {}", path.display())))
}

fn push_unique(out: &mut Vec<PathBuf>, seen: &mut HashSet<PathBuf>, path: PathBuf) {
    if seen.insert(path.clone()) {
        out.push(path);
    }
}

fn is_filesystem_root(path: &Path) -> bool {
    let mut rooted = false;
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => rooted = true,
            Component::CurDir => {}
            Component::ParentDir | Component::Normal(_) => return false,
        }
    }
    rooted
}

fn anchor_of(path: &Path) -> PathBuf {
    let mut anchor = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => anchor.push(prefix.as_os_str()),
            Component::RootDir => {
                append_root(&mut anchor);
                break;
            }
            _ => break,
        }
    }
    anchor
}

fn append_root(path: &mut PathBuf) {
    if path.as_os_str().is_empty() {
        path.push(std::path::MAIN_SEPARATOR.to_string());
        return;
    }
    let mut text = path.as_os_str().to_os_string();
    text.push(std::path::MAIN_SEPARATOR.to_string());
    *path = PathBuf::from(text);
}

fn metadata_missing(path: &Path) -> io::Result<bool> {
    match fs::metadata(path) {
        Ok(_) => Ok(false),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(err) => Err(err),
    }
}

fn realpath(path: &Path) -> io::Result<PathBuf> {
    let absolute = std::path::absolute(path)?;
    let mut seen = HashSet::new();
    resolve_components(&absolute, &mut seen)
}

fn realpath_seen(path: &Path, seen: &mut HashSet<PathBuf>) -> io::Result<PathBuf> {
    let absolute = std::path::absolute(path)?;
    resolve_components(&absolute, seen)
}

fn resolve_components(path: &Path, seen: &mut HashSet<PathBuf>) -> io::Result<PathBuf> {
    let mut resolved = anchor_of(path);
    let mut missing = false;
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => {}
            Component::CurDir => {}
            Component::ParentDir => {
                if !is_filesystem_root(&resolved) {
                    resolved.pop();
                }
                missing = metadata_missing(&resolved)?;
            }
            Component::Normal(name) => {
                if missing {
                    resolved.push(name);
                    continue;
                }
                let next = resolved.join(name);
                match fs::symlink_metadata(&next) {
                    Ok(meta) if meta.file_type().is_symlink() => {
                        if seen.len() > 64 || !seen.insert(next.clone()) {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, "symlink loop"));
                        }
                        let target = fs::read_link(&next)?;
                        let combined = if target.is_absolute() {
                            target
                        } else {
                            resolved.join(target)
                        };
                        resolved = realpath_seen(&combined, seen)?;
                        missing = metadata_missing(&resolved)?;
                    }
                    Ok(_) => resolved = next,
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {
                        missing = true;
                        resolved = next;
                    }
                    Err(err) => return Err(err),
                }
            }
        }
    }
    Ok(resolved)
}

fn normalize_lexical(path: &Path) -> PathBuf {
    let mut out = anchor_of(path);
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => {}
            Component::CurDir => {}
            Component::ParentDir => {
                if !is_filesystem_root(&out) {
                    out.pop();
                }
            }
            Component::Normal(name) => out.push(name),
        }
    }
    out
}

fn lexical_absolute(path: &Path) -> io::Result<PathBuf> {
    Ok(normalize_lexical(&std::path::absolute(path)?))
}

fn expand_anchor(path: &Path) -> Result<Vec<PathBuf>, ReadOnlyViewError> {
    let mut paths = vec![path.to_path_buf()];
    match realpath(path) {
        Ok(resolved) if resolved != path => paths.push(resolved),
        Ok(_) => {}
        Err(err) => {
            return Err(view_err(format!(
                "cannot resolve {}: {err}",
                path.display()
            )));
        }
    }
    Ok(paths)
}

fn reject_conflicting_workdir(
    work: &Path,
    overlays: &[PathBuf],
    synthetics: &[PathBuf],
) -> Result<(), ReadOnlyViewError> {
    for overlay in overlays {
        for candidate in expand_anchor(overlay)? {
            if candidate.starts_with(work) {
                return Err(view_err(format!(
                    "workdir {} would re-expose the host {}; pick a directory below it or elsewhere",
                    work.display(),
                    overlay.display()
                )));
            }
        }
    }
    for synthetic in synthetics {
        for candidate in expand_anchor(synthetic)? {
            if work.starts_with(&candidate) || candidate.starts_with(work) {
                return Err(view_err(format!(
                    "workdir {} overlaps {}",
                    work.display(),
                    synthetic.display()
                )));
            }
        }
    }
    Ok(())
}

fn home_dir() -> Result<PathBuf, ReadOnlyViewError> {
    let home = dirs::home_dir().ok_or_else(|| view_err("home directory is unavailable"))?;
    realpath(&home).map_err(|err| view_err(format!("cannot resolve home directory: {err}")))
}

/// Refuse a layout in which the view would stop being read-only.
///
/// The workdir must be an explicit existing directory and not the filesystem
/// root. It must not be an overlay path, or an ancestor of one, because binding
/// that directory back would re-expose the host tmpfs underneath. A directory
/// below an overlay is allowed: the prefix bind-mounts it after the tmpfs. The
/// workdir also must not sit on `/dev` or `/proc`. Each writable directory must
/// be absolute and already exist, and must not contain the workdir or the home
/// directory.
pub fn validate_view_paths(
    workdir: &Path,
    writable_dirs: &[impl AsRef<Path>],
) -> Result<(), ReadOnlyViewError> {
    if workdir.as_os_str().is_empty() {
        return Err(view_err("read-only view needs an explicit workdir"));
    }
    let work = realpath(workdir).map_err(|err| {
        view_err(format!(
            "cannot resolve workdir {}: {err}",
            workdir.display()
        ))
    })?;
    if !work.is_dir() {
        return Err(view_err(format!(
            "workdir {} is not a directory",
            workdir.display()
        )));
    }
    if is_filesystem_root(&work) {
        return Err(view_err(format!(
            "workdir {} is the filesystem root",
            work.display()
        )));
    }
    let overlays: Vec<PathBuf> = OVERLAY_PATHS.iter().map(PathBuf::from).collect();
    let synthetics: Vec<PathBuf> = SYNTHETIC_PATHS.iter().map(PathBuf::from).collect();
    reject_conflicting_workdir(&work, &overlays, &synthetics)?;
    if writable_dirs.is_empty() {
        return Ok(());
    }
    let home = home_dir()?;
    for raw in writable_dirs {
        reject_writable(raw.as_ref(), &work, &home)?;
    }
    Ok(())
}

fn reject_writable(raw: &Path, work: &Path, home: &Path) -> Result<(), ReadOnlyViewError> {
    if raw.as_os_str().is_empty() || !raw.is_absolute() {
        return Err(view_err(format!(
            "writable dir must be absolute: {}",
            raw.display()
        )));
    }
    let resolved = realpath(raw).map_err(|err| {
        view_err(format!(
            "cannot resolve writable dir {}: {err}",
            raw.display()
        ))
    })?;
    if !resolved.is_dir() {
        return Err(view_err(format!(
            "writable dir does not exist: {}",
            raw.display()
        )));
    }
    if work.starts_with(&resolved) {
        return Err(view_err(format!(
            "writable dir {} contains the workdir {}; the workdir would become writable",
            resolved.display(),
            work.display()
        )));
    }
    if home.starts_with(&resolved) {
        return Err(view_err(format!(
            "writable dir {} contains the home directory",
            resolved.display()
        )));
    }
    Ok(())
}

fn link_count(meta: &fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        std::os::unix::fs::MetadataExt::nlink(meta)
    }
    #[cfg(windows)]
    {
        std::os::windows::fs::MetadataExt::number_of_links(meta)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = meta;
        1
    }
}

fn path_list(paths: &[impl AsRef<Path>]) -> Vec<PathBuf> {
    paths
        .iter()
        .map(|path| path.as_ref().to_path_buf())
        .collect()
}

/// Regular files under `writable_dirs` whose link count is greater than 1.
///
/// `limit` stops the walk after that many matches. `None` returns every match.
/// Symlinks and directories are ignored. An empty path is ignored so it cannot
/// mean the process working directory. A directory that cannot be listed is
/// skipped.
pub fn find_hardlink_escapes(
    writable_dirs: &[impl AsRef<Path>],
    limit: Option<usize>,
) -> Vec<PathBuf> {
    if limit == Some(0) {
        return Vec::new();
    }
    let mut offenders = Vec::new();
    for raw in writable_dirs {
        let raw = raw.as_ref();
        if raw.as_os_str().is_empty() {
            continue;
        }
        let Ok(root) = realpath(raw) else {
            continue;
        };
        walk_hardlinks(&root, limit, &mut offenders);
        if limit.is_some_and(|max| offenders.len() >= max) {
            break;
        }
    }
    offenders
}

fn walk_hardlinks(root: &Path, limit: Option<usize>, offenders: &mut Vec<PathBuf>) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        let mut children = Vec::new();
        for entry in entries {
            let Ok(entry) = entry else {
                continue;
            };
            let path = entry.path();
            let Ok(meta) = fs::symlink_metadata(&path) else {
                continue;
            };
            let file_type = meta.file_type();
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                children.push(path);
                continue;
            }
            if file_type.is_file() && link_count(&meta) > 1 {
                offenders.push(path);
                if limit.is_some_and(|max| offenders.len() >= max) {
                    return;
                }
            }
        }
        for child in children.into_iter().rev() {
            stack.push(child);
        }
    }
}

fn hardlink_refusal(offenders: &[PathBuf]) -> ReadOnlyViewError {
    let shown = offenders
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join("\n  ");
    // The walk stops at the cap, so a full cap cannot prove the list is complete.
    let more = if offenders.len() >= MAX_HARDLINK_EXAMPLES {
        " (and more)"
    } else {
        ""
    };
    view_err(format!(
        "read-only view refused: the writable set contains regular files with more than one hardlink, which would let a write reach the shared inode outside the view. Narrow the writable directories so they exclude hardlinked trees.{more}\n  {shown}"
    ))
}

/// Refuse to build the view when a writable directory holds a hardlinked file.
///
/// When [`hardlink_runtime_guard`] is active for the same directories, a passing
/// check replaces that guard's baseline with this empty result.
pub fn assert_no_hardlink_escape(
    writable_dirs: &[impl AsRef<Path>],
) -> Result<(), ReadOnlyViewError> {
    let owned = path_list(writable_dirs);
    let offenders = find_hardlink_escapes(&owned, Some(MAX_HARDLINK_EXAMPLES));
    if !offenders.is_empty() {
        return Err(hardlink_refusal(&offenders));
    }
    if let Some(guard) = active_guard()
        && !guard.is_closed()
        && same_writable_dirs(&guard.writable_dirs(), &owned)
    {
        guard.note_successful_build();
    }
    Ok(())
}

fn canonical_key(path: &Path) -> PathBuf {
    if let Ok(resolved) = realpath(path) {
        resolved
    } else if let Ok(absolute) = std::path::absolute(path) {
        absolute
    } else {
        path.to_path_buf()
    }
}

fn directory_key(dirs: &[PathBuf]) -> BTreeSet<PathBuf> {
    dirs.iter()
        .filter(|dir| !dir.as_os_str().is_empty())
        .map(|dir| canonical_key(dir))
        .collect()
}

fn same_writable_dirs(left: &[PathBuf], right: &[PathBuf]) -> bool {
    directory_key(left) == directory_key(right)
}

fn hardlink_runtime_message(paths: &[PathBuf]) -> String {
    let lines = paths
        .iter()
        .take(MAX_HARDLINK_EXAMPLES)
        .map(|path| format!("  {}", path.display()))
        .collect::<Vec<_>>()
        .join("\n");
    let extra = paths.len().saturating_sub(MAX_HARDLINK_EXAMPLES);
    let tail = if extra > 0 {
        format!("\n({extra} more not listed)")
    } else {
        String::new()
    };
    format!(
        "read-only view finished with regular files in a writable directory whose link count is greater than 1 and that were not present when the view was checked. A write through those links changes the same inode outside the view. This scan only warns and does not signal the process.\n{lines}{tail}"
    )
}

#[derive(Debug)]
struct GuardState {
    writable_dirs: Vec<PathBuf>,
    baseline: BTreeSet<PathBuf>,
    warnings: Vec<HardlinkEscapeWarning>,
    closed: bool,
    emitted: BTreeSet<PathBuf>,
}

fn warnings_from(state: &GuardState) -> Vec<HardlinkEscapeWarning> {
    let paths = find_hardlink_escapes(&state.writable_dirs, None)
        .into_iter()
        .filter(|path| !state.baseline.contains(path))
        .collect::<Vec<_>>();
    if paths.is_empty() {
        return Vec::new();
    }
    let message = hardlink_runtime_message(&paths);
    vec![HardlinkEscapeWarning {
        code: HARDLINK_RUNTIME_WARNING_CODE,
        message,
        paths,
        writable_dirs: state.writable_dirs.clone(),
        action: HARDLINK_RUNTIME_WARNING_ACTION,
    }]
}

fn fresh_message(state: &mut GuardState, warning: &HardlinkEscapeWarning) -> Option<String> {
    let fresh = warning
        .paths
        .iter()
        .filter(|path| state.emitted.insert((*path).clone()))
        .cloned()
        .collect::<Vec<_>>();
    if fresh.is_empty() {
        None
    } else {
        Some(hardlink_runtime_message(&fresh))
    }
}

fn log_messages(messages: &[String]) {
    for message in messages {
        tracing::warn!("{message}");
    }
}

/// Baseline compared again when the guarded block ends.
///
/// The constructor records hardlinks already present. [`assert_no_hardlink_escape`],
/// including the call inside [`readonly_view_prefix`], replaces that baseline
/// when the same directories pass. [`HardlinkRuntimeGuard::rescan`] and
/// [`HardlinkRuntimeGuard::close`] only report links that were not in the
/// baseline. Neither method removes files or signals a process.
#[derive(Clone, Debug)]
pub struct HardlinkRuntimeGuard {
    inner: Rc<RefCell<GuardState>>,
}

impl HardlinkRuntimeGuard {
    fn new(writable_dirs: &[impl AsRef<Path>]) -> Self {
        let writable_dirs = path_list(writable_dirs);
        let baseline = find_hardlink_escapes(&writable_dirs, None)
            .into_iter()
            .collect();
        Self {
            inner: Rc::new(RefCell::new(GuardState {
                writable_dirs,
                baseline,
                warnings: Vec::new(),
                closed: false,
                emitted: BTreeSet::new(),
            })),
        }
    }

    fn writable_dirs(&self) -> Vec<PathBuf> {
        self.inner.borrow().writable_dirs.clone()
    }

    #[cfg(test)]
    fn same_guard(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }

    /// Hardlinks recorded for this guard.
    pub fn baseline(&self) -> BTreeSet<PathBuf> {
        self.inner.borrow().baseline.clone()
    }

    /// Warnings stored by [`Self::close`]. Empty before the guard closes.
    pub fn warnings(&self) -> Vec<HardlinkEscapeWarning> {
        self.inner.borrow().warnings.clone()
    }

    /// Whether [`Self::close`] has already run.
    pub fn is_closed(&self) -> bool {
        self.inner.borrow().closed
    }

    /// Use the passing build scan as the baseline.
    ///
    /// A closed guard keeps the warnings it already published.
    pub fn note_successful_build(&self) {
        let mut state = self.inner.borrow_mut();
        if state.closed {
            return;
        }
        state.baseline.clear();
    }

    /// Return hardlinks that were not in the baseline. Does not signal a process.
    pub fn rescan(&self) -> Vec<HardlinkEscapeWarning> {
        let (found, messages) = {
            let mut state = self.inner.borrow_mut();
            let found = warnings_from(&state);
            let messages = found
                .iter()
                .filter_map(|warning| fresh_message(&mut state, warning))
                .collect::<Vec<_>>();
            (found, messages)
        };
        log_messages(&messages);
        found
    }

    /// Scan once. Later calls return the same warnings and do not signal a process.
    pub fn close(&self) -> Vec<HardlinkEscapeWarning> {
        let (warnings, messages) = {
            let mut state = self.inner.borrow_mut();
            if state.closed {
                return state.warnings.clone();
            }
            let warnings = warnings_from(&state);
            state.closed = true;
            let messages = warnings
                .iter()
                .filter_map(|warning| fresh_message(&mut state, warning))
                .collect::<Vec<_>>();
            state.warnings = warnings.clone();
            (warnings, messages)
        };
        log_messages(&messages);
        warnings
    }
}

tokio::task_local! {
    static ACTIVE_HARDLINK_GUARD: HardlinkRuntimeGuard;
}

fn active_guard() -> Option<HardlinkRuntimeGuard> {
    ACTIVE_HARDLINK_GUARD.try_get().ok()
}

struct CloseOnDrop(HardlinkRuntimeGuard);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// Run `body` and scan `writable_dirs` for new hardlinks when it returns.
///
/// A second guard for the same directories, opened while the first is still
/// active, reuses that first guard and does not scan on its own return. The
/// outer return is the one that warns. The block is not interrupted.
pub fn hardlink_runtime_guard<R>(
    writable_dirs: &[impl AsRef<Path>],
    body: impl FnOnce(&HardlinkRuntimeGuard) -> R,
) -> R {
    let owned = path_list(writable_dirs);
    if let Some(current) = active_guard()
        && !current.is_closed()
        && same_writable_dirs(&current.writable_dirs(), &owned)
    {
        return body(&current);
    }
    let guard = HardlinkRuntimeGuard::new(&owned);
    let closer = CloseOnDrop(guard.clone());
    ACTIVE_HARDLINK_GUARD.sync_scope(guard, || body(&closer.0))
}

fn is_sqlite_database(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("sqlite" | "db")
    )
}

fn is_supported_hide(meta: &fs::Metadata) -> bool {
    if meta.is_dir() || meta.is_file() {
        return true;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt;
        meta.file_type().is_socket()
    }
    #[cfg(not(unix))]
    {
        false
    }
}

fn sort_hidden(paths: &mut [PathBuf]) {
    paths.sort_by(|left, right| {
        left.components()
            .count()
            .cmp(&right.components().count())
            .then_with(|| left.as_os_str().cmp(right.as_os_str()))
    });
}

/// Resolve aliases and add SQLite sidecars for existing `.sqlite` and `.db` paths.
///
/// The stored path is the resolved target, so a symlink is covered by hiding
/// that target. Missing paths are reported and are not mount points. Reads in
/// general stay available: only this set is hidden.
pub fn plan_hidden_paths(
    hide_paths: &[impl AsRef<Path>],
) -> Result<HiddenPathPlan, ReadOnlyViewError> {
    let mut requested = Vec::new();
    let mut seen_requested = HashSet::new();
    for raw in hide_paths {
        let raw = raw.as_ref();
        if raw.as_os_str().is_empty() || !raw.is_absolute() {
            return Err(view_err(format!(
                "hidden path must be absolute: {}",
                raw.display()
            )));
        }
        push_unique(&mut requested, &mut seen_requested, raw.to_path_buf());
        if is_sqlite_database(raw) {
            push_sqlite_sidecars(raw, &mut requested, &mut seen_requested)?;
        }
    }
    let mut hidden = Vec::new();
    let mut seen_hidden = HashSet::new();
    let mut skipped = Vec::new();
    let mut seen_skipped = HashSet::new();
    for raw in requested {
        classify_hidden(
            raw,
            &mut hidden,
            &mut seen_hidden,
            &mut skipped,
            &mut seen_skipped,
        )?;
    }
    sort_hidden(&mut hidden);
    Ok(HiddenPathPlan {
        hidden,
        skipped_missing: skipped,
    })
}

fn push_sqlite_sidecars(
    raw: &Path,
    requested: &mut Vec<PathBuf>,
    seen: &mut HashSet<PathBuf>,
) -> Result<(), ReadOnlyViewError> {
    let resolved = realpath(raw).map_err(|err| {
        view_err(format!(
            "cannot inspect hidden path {}: {err}",
            raw.display()
        ))
    })?;
    let mut bases = vec![raw.to_path_buf()];
    if resolved != raw {
        bases.push(resolved);
    }
    for base in bases {
        let text = utf8_path(&base)?;
        for suffix in SQLITE_SIDECARS {
            push_unique(requested, seen, PathBuf::from(format!("{text}{suffix}")));
        }
    }
    Ok(())
}

fn classify_hidden(
    raw: PathBuf,
    hidden: &mut Vec<PathBuf>,
    seen_hidden: &mut HashSet<PathBuf>,
    skipped: &mut Vec<PathBuf>,
    seen_skipped: &mut HashSet<PathBuf>,
) -> Result<(), ReadOnlyViewError> {
    let resolved = realpath(&raw).map_err(|err| {
        view_err(format!(
            "cannot inspect hidden path {}: {err}",
            raw.display()
        ))
    })?;
    match fs::metadata(&resolved) {
        Ok(meta) => {
            if !is_supported_hide(&meta) {
                return Err(view_err(format!(
                    "unsupported hidden path type: {}",
                    raw.display()
                )));
            }
            push_unique(hidden, seen_hidden, resolved);
            Ok(())
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            let absolute = lexical_absolute(&raw).map_err(|abs_err| {
                view_err(format!(
                    "cannot inspect hidden path {}: {abs_err}",
                    raw.display()
                ))
            })?;
            push_unique(skipped, seen_skipped, absolute);
            Ok(())
        }
        Err(err) => Err(view_err(format!(
            "cannot inspect hidden path {}: {err}",
            raw.display()
        ))),
    }
}

/// Fail closed when sealing would cover the workdir or a writable directory.
///
/// A missing path is checked too. A path that does not exist yet is not
/// permission to accept a layout that becomes unsafe when it is created.
pub fn validate_hidden_paths(
    workdir: &Path,
    writable_dirs: &[impl AsRef<Path>],
    hide_paths: &[impl AsRef<Path>],
) -> Result<HiddenPathPlan, ReadOnlyViewError> {
    let mut protected = vec![realpath(workdir).map_err(|err| {
        view_err(format!(
            "cannot resolve workdir {}: {err}",
            workdir.display()
        ))
    })?];
    for raw in writable_dirs {
        let raw = raw.as_ref();
        protected.push(realpath(raw).map_err(|err| {
            view_err(format!(
                "cannot resolve writable dir {}: {err}",
                raw.display()
            ))
        })?);
    }
    for raw in hide_paths {
        let raw = raw.as_ref();
        if raw.as_os_str().is_empty() || !raw.is_absolute() {
            return Err(view_err(format!(
                "hidden path must be absolute: {}",
                raw.display()
            )));
        }
        let hidden = realpath(raw).map_err(|err| {
            view_err(format!(
                "cannot inspect hidden path {}: {err}",
                raw.display()
            ))
        })?;
        for required in &protected {
            if required.starts_with(&hidden) {
                return Err(view_err(format!(
                    "cannot hide {}: it contains the workdir or writable dir {}; move the config/state/workdir to disjoint directories",
                    hidden.display(),
                    required.display()
                )));
            }
        }
    }
    plan_hidden_paths(hide_paths)
}

/// Mounts for an existing hidden directory, file, or pathname socket.
///
/// Directories use `--tmpfs`. Files and sockets use `--ro-bind /dev/null`.
pub fn hidden_path_mounts(plan: &HiddenPathPlan) -> Result<Vec<String>, ReadOnlyViewError> {
    let mut argv = Vec::new();
    for hidden in &plan.hidden {
        let text = utf8_path(hidden)?;
        if hidden.is_dir() {
            argv.push("--tmpfs".to_string());
            argv.push(text);
        } else {
            argv.push("--ro-bind".to_string());
            argv.push("/dev/null".to_string());
            argv.push(text);
        }
    }
    Ok(argv)
}

/// The resolver file when a tmpfs would hide it, otherwise `None`.
///
/// Only a regular file whose resolved path is strictly below `/tmp`,
/// `/var/tmp`, or `/run` is returned. The parent directory is not a mount.
pub fn resolver_config_in_overlay() -> Result<Option<PathBuf>, ReadOnlyViewError> {
    let overlays: Vec<PathBuf> = OVERLAY_PATHS.iter().map(PathBuf::from).collect();
    resolver_file_under_overlays(Path::new("/etc/resolv.conf"), &overlays)
}

fn resolver_file_under_overlays(
    resolv: &Path,
    overlays: &[impl AsRef<Path>],
) -> Result<Option<PathBuf>, ReadOnlyViewError> {
    let resolved = match realpath(resolv) {
        Ok(resolved) => resolved,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(view_err(format!(
                "cannot inspect resolver config {}: {err}",
                resolv.display()
            )));
        }
    };
    if !resolved.is_file() {
        return Ok(None);
    }
    let covered = overlays.iter().any(|overlay| {
        let overlay = overlay.as_ref();
        resolved.starts_with(overlay) && resolved.as_path() != overlay
    });
    Ok(covered.then_some(resolved))
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    match fs::metadata(path) {
        Ok(meta) => meta.is_file() && meta.permissions().mode() & 0o111 != 0,
        Err(_) => false,
    }
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    fs::metadata(path).is_ok_and(|meta| meta.is_file())
}

fn resolve_bwrap(search_path: Option<&OsStr>) -> Result<PathBuf, ReadOnlyViewError> {
    let Some(search_path) = search_path.filter(|path| !path.is_empty()) else {
        return Err(bwrap_missing());
    };
    for dir in std::env::split_paths(search_path) {
        let candidate = dir.join("bwrap");
        if is_executable(&candidate) {
            return Ok(candidate);
        }
    }
    Err(bwrap_missing())
}

fn assemble_argv(
    bwrap: &Path,
    work: &Path,
    writable_dirs: &[PathBuf],
    resolver: Option<&Path>,
    plan: &HiddenPathPlan,
) -> Result<Vec<String>, ReadOnlyViewError> {
    let mut argv = Vec::new();
    argv.push(utf8_path(bwrap)?);
    for arg in ["--ro-bind", "/", "/", "--dev", "/dev", "--proc", "/proc"] {
        argv.push((*arg).to_string());
    }
    for overlay in OVERLAY_PATHS {
        argv.push("--tmpfs".to_string());
        argv.push((*overlay).to_string());
    }
    if let Some(resolver) = resolver {
        let text = utf8_path(resolver)?;
        argv.push("--ro-bind".to_string());
        argv.push(text.clone());
        argv.push(text);
    }
    let work_text = utf8_path(work)?;
    argv.push("--ro-bind".to_string());
    argv.push(work_text.clone());
    argv.push(work_text.clone());
    for dir in writable_dirs {
        let text = utf8_path(dir)?;
        argv.push("--bind".to_string());
        argv.push(text.clone());
        argv.push(text);
    }
    for name in UNSET_ENV {
        argv.push("--unsetenv".to_string());
        argv.push((*name).to_string());
    }
    argv.extend(hidden_path_mounts(plan)?);
    argv.push("--die-with-parent".to_string());
    argv.push("--new-session".to_string());
    argv.push("--chdir".to_string());
    argv.push(work_text);
    argv.push("--".to_string());
    Ok(argv)
}

/// Build the `bwrap … --` prefix using `bwrap` from the process `PATH`.
///
/// This does not spawn `bwrap`. A missing binary returns [`ReadOnlyViewError`].
/// `--unsetenv` uses only [`UNSET_ENV`]. Daemon memory roots are not added.
pub fn readonly_view_prefix(
    workdir: &Path,
    writable_dirs: &[impl AsRef<Path>],
    hide_paths: &[impl AsRef<Path>],
) -> Result<Vec<String>, ReadOnlyViewError> {
    readonly_view_prefix_with_search_path(
        workdir,
        writable_dirs,
        hide_paths,
        std::env::var_os("PATH").as_deref(),
    )
}

/// [`readonly_view_prefix`] with an explicit `PATH` value.
///
/// `None` and an empty value both fail closed. The function does not spawn
/// `bwrap` and does not read the process environment except through `search_path`.
pub fn readonly_view_prefix_with_search_path(
    workdir: &Path,
    writable_dirs: &[impl AsRef<Path>],
    hide_paths: &[impl AsRef<Path>],
    search_path: Option<&OsStr>,
) -> Result<Vec<String>, ReadOnlyViewError> {
    validate_view_paths(workdir, writable_dirs)?;
    assert_no_hardlink_escape(writable_dirs)?;
    let bwrap = resolve_bwrap(search_path)?;
    let work = realpath(workdir).map_err(|err| {
        view_err(format!(
            "cannot resolve workdir {}: {err}",
            workdir.display()
        ))
    })?;
    let mut writable = Vec::with_capacity(writable_dirs.len());
    for dir in writable_dirs {
        let dir = dir.as_ref();
        writable.push(realpath(dir).map_err(|err| {
            view_err(format!(
                "cannot resolve writable dir {}: {err}",
                dir.display()
            ))
        })?);
    }
    let plan = validate_hidden_paths(workdir, writable_dirs, hide_paths)?;
    let resolver = resolver_config_in_overlay()?;
    assemble_argv(&bwrap, &work, &writable, resolver.as_deref(), &plan)
}

/// True when `argv` starts with `bwrap --ro-bind / /`.
///
/// A bubblewrap sandbox that starts with `--die-with-parent` is a different
/// boundary and returns false.
pub fn is_readonly_view_argv(argv: &[impl AsRef<str>]) -> bool {
    let mut parts = argv.iter().map(AsRef::as_ref);
    if parts.next().is_none() {
        return false;
    }
    parts.next() == Some("--ro-bind") && parts.next() == Some("/") && parts.next() == Some("/")
}

/// Refuse a view child that would inherit a caller-supplied file descriptor.
///
/// `close_fds` must be true. Any non-empty `pass_fds` is refused. The original
/// open mode is not consulted: an inherited descriptor keeps the host mount.
pub fn assert_readonly_view_spawn(
    close_fds: bool,
    pass_fds: &[i32],
) -> Result<(), ReadOnlyViewError> {
    if !close_fds {
        return Err(view_err(
            "read-only view child must be spawned with close_fds=True (inherited fds bypass the ro-bind mount)",
        ));
    }
    if !pass_fds.is_empty() {
        return Err(view_err(
            "read-only view refused caller pass_fds: an inherited fd can be reopened via /proc/self/fd with a new access mode and write the host inode around the ro-bind mount",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::cloned_ref_to_slice_refs, clippy::unwrap_used)]
mod tests {
    use std::ffi::OsStr;
    use std::fs;
    use std::path::{Path, PathBuf};

    use super::*;

    fn temp() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn touch(path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, b"x").unwrap();
    }

    struct Layout {
        root: tempfile::TempDir,
        work: PathBuf,
        state: PathBuf,
        bin: PathBuf,
        bwrap: PathBuf,
    }

    impl Layout {
        fn new() -> Self {
            let root = temp();
            let work = root.path().join("work");
            let state = root.path().join("state");
            let bin = root.path().join("bin");
            fs::create_dir(&work).unwrap();
            fs::create_dir(&state).unwrap();
            fs::create_dir(&bin).unwrap();
            let bwrap = install_bwrap(&bin);
            Self {
                root,
                work,
                state,
                bin,
                bwrap,
            }
        }

        fn prefix(&self, hide: &[PathBuf]) -> Vec<String> {
            readonly_view_prefix_with_search_path(
                &self.work,
                &[self.state.clone()],
                hide,
                Some(self.bin.as_os_str()),
            )
            .unwrap()
        }
    }

    fn install_bwrap(dir: &Path) -> PathBuf {
        fs::create_dir_all(dir).unwrap();
        let program = dir.join("bwrap");
        let marker = dir.join("bwrap-executed");
        let script = format!("#!/bin/sh\n: > '{}'\n", marker.display());
        fs::write(&program, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&program).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&program, permissions).unwrap();
        }
        program
    }

    fn flag_at(argv: &[String], flag: &str, value: &str) -> usize {
        argv.windows(2)
            .position(|window| window[0] == flag && window[1] == value)
            .unwrap_or_else(|| panic!("missing {flag} {value} in {argv:?}"))
    }

    fn bind_at(argv: &[String], flag: &str, path: &str) -> usize {
        argv.windows(3)
            .position(|window| window[0] == flag && window[1] == path && window[2] == path)
            .unwrap_or_else(|| panic!("missing {flag} {path} {path} in {argv:?}"))
    }

    fn plant(writable: &Path, outside: &Path, name: &str) -> PathBuf {
        fs::create_dir_all(writable).unwrap();
        fs::create_dir_all(outside).unwrap();
        let target = outside.join(format!("{name}-target"));
        let link = writable.join(name);
        if let Some(parent) = link.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&target, b"x").unwrap();
        fs::hard_link(&target, &link).unwrap();
        realpath(&link).unwrap()
    }

    #[test]
    fn unsetenv_is_the_documented_name_list() {
        assert_eq!(
            UNSET_ENV,
            [
                "DBUS_SESSION_BUS_ADDRESS",
                "DISPLAY",
                "WAYLAND_DISPLAY",
                "SSH_AUTH_SOCK",
                "VYANE_DAEMON_API_TOKEN",
                "VYANE_DAEMON_WORKER_TOKEN",
                "VYANE_DAEMON_OWNER_TOKENS",
                "VYANE_MEMORY_PRINCIPAL_TOKENS",
                "VYANE_DASHBOARD_API_TOKEN",
                "VYANE_DASHBOARD_OWNER_TOKENS",
                "VYANE_DASHBOARD_INTERNAL_API_TOKEN",
                "VYANE_ENV_FILE",
            ]
        );
    }

    #[test]
    fn realpath_resolves_parents_and_stops_at_root() {
        let root = temp();
        let parent = realpath(root.path()).unwrap();
        assert_eq!(
            realpath(&root.path().join("missing")).unwrap(),
            parent.join("missing")
        );
        assert_eq!(
            realpath(&root.path().join("nosuch").join("..")).unwrap(),
            parent
        );
        let file = root.path().join("real.txt");
        touch(&file);
        assert_eq!(
            realpath(&root.path().join("nosuch").join("..").join("real.txt")).unwrap(),
            realpath(&file).unwrap()
        );
        #[cfg(unix)]
        assert!(is_filesystem_root(&realpath(Path::new("/")).unwrap()));
    }

    #[cfg(unix)]
    #[test]
    fn realpath_resolves_symlinks_and_rejects_loops() {
        let root = temp();
        let target = root.path().join("target");
        fs::create_dir(&target).unwrap();
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert_eq!(
            realpath(&link.join("missing")).unwrap(),
            realpath(&target).unwrap().join("missing")
        );
        assert_eq!(
            realpath(&link.join("..")).unwrap(),
            realpath(root.path()).unwrap()
        );
        let left = root.path().join("left");
        let right = root.path().join("right");
        std::os::unix::fs::symlink(&right, &left).unwrap();
        std::os::unix::fs::symlink(&left, &right).unwrap();
        assert!(realpath(&left).is_err());
    }

    #[test]
    fn prefix_shape_uses_fake_bwrap_and_does_not_execute_it() {
        let layout = Layout::new();
        let hidden = layout.work.join("sealed");
        fs::create_dir(&hidden).unwrap();
        let secret = layout.work.join("ledger.sqlite");
        touch(&secret);
        let argv = layout.prefix(&[hidden.clone(), secret.clone()]);
        assert!(!layout.bin.join("bwrap-executed").exists());
        assert_eq!(Path::new(&argv[0]), layout.bwrap.as_path());
        assert!(is_readonly_view_argv(&argv));
        assert_eq!(
            argv[1..8].iter().map(String::as_str).collect::<Vec<_>>(),
            ["--ro-bind", "/", "/", "--dev", "/dev", "--proc", "/proc"]
        );
        assert_eq!(
            argv[8..14].iter().map(String::as_str).collect::<Vec<_>>(),
            ["--tmpfs", "/tmp", "--tmpfs", "/var/tmp", "--tmpfs", "/run"]
        );
        let work = realpath(&layout.work).unwrap();
        let state = realpath(&layout.state).unwrap();
        let work_text = work.to_str().unwrap();
        let state_text = state.to_str().unwrap();
        let hidden_text = realpath(&hidden).unwrap();
        let hidden_text = hidden_text.to_str().unwrap().to_string();
        let secret_text = realpath(&secret).unwrap();
        let secret_text = secret_text.to_str().unwrap().to_string();
        let work_at = bind_at(&argv, "--ro-bind", work_text);
        let state_at = bind_at(&argv, "--bind", state_text);
        let hide_dir = flag_at(&argv, "--tmpfs", &hidden_text);
        let hide_file = argv
            .windows(3)
            .position(|window| {
                window[0] == "--ro-bind" && window[1] == "/dev/null" && window[2] == secret_text
            })
            .unwrap();
        let unset = argv.iter().position(|arg| arg == "--unsetenv").unwrap();
        assert!(work_at < state_at);
        assert!(state_at < unset);
        assert!(unset < hide_dir);
        assert!(unset < hide_file);
        for overlay in OVERLAY_PATHS {
            assert!(flag_at(&argv, "--tmpfs", overlay) < work_at);
        }
        let names = argv
            .windows(2)
            .filter(|window| window[0] == "--unsetenv")
            .map(|window| window[1].as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, UNSET_ENV);
        assert!(!argv.iter().any(|arg| {
            arg == "--unshare-net" || arg == "--setenv" || arg.contains("xdg-dbus-proxy")
        }));
        assert_eq!(argv[argv.len() - 5], "--die-with-parent");
        assert_eq!(argv[argv.len() - 4], "--new-session");
        assert_eq!(argv[argv.len() - 3], "--chdir");
        assert_eq!(argv[argv.len() - 2], work_text);
        assert_eq!(argv[argv.len() - 1], "--");
        let resolver = resolver_config_in_overlay().unwrap();
        match resolver {
            Some(path) => {
                let text = path.to_str().unwrap();
                let at = bind_at(&argv, "--ro-bind", text);
                assert!(flag_at(&argv, "--tmpfs", "/run") < at);
                assert!(at < work_at);
                assert_eq!(argv.iter().filter(|arg| arg.as_str() == text).count(), 2);
            }
            None => {
                assert!(!argv.iter().any(|arg| arg.contains("resolv.conf")));
            }
        }
    }

    #[test]
    fn resolver_bind_sits_between_overlays_and_the_workdir() {
        let root = temp();
        let work = root.path().join("work");
        let state = root.path().join("state");
        let hidden = root.path().join("hidden");
        fs::create_dir(&work).unwrap();
        fs::create_dir(&state).unwrap();
        fs::create_dir(&hidden).unwrap();
        let resolver = Path::new("/run/systemd/resolve/stub-resolv.conf");
        let plan = HiddenPathPlan {
            hidden: vec![hidden.clone()],
            skipped_missing: Vec::new(),
        };
        let argv = assemble_argv(
            Path::new("/usr/bin/bwrap"),
            &realpath(&work).unwrap(),
            &[realpath(&state).unwrap()],
            Some(resolver),
            &plan,
        )
        .unwrap();
        let work_text = realpath(&work).unwrap();
        let work_text = work_text.to_str().unwrap();
        let resolver_at = bind_at(&argv, "--ro-bind", "/run/systemd/resolve/stub-resolv.conf");
        let work_at = bind_at(&argv, "--ro-bind", work_text);
        assert!(flag_at(&argv, "--tmpfs", "/run") < resolver_at);
        assert!(resolver_at < work_at);
        assert!(work_at < bind_at(&argv, "--bind", realpath(&state).unwrap().to_str().unwrap()));
        assert!(
            bind_at(&argv, "--bind", realpath(&state).unwrap().to_str().unwrap())
                < flag_at(&argv, "--tmpfs", hidden.to_str().unwrap())
        );
        assert!(!argv.iter().any(|arg| arg == "/run/systemd/resolve"));
        assert_eq!(
            argv.iter()
                .filter(|arg| arg.as_str() == "/run/systemd/resolve/stub-resolv.conf")
                .count(),
            2
        );
        let without = assemble_argv(
            Path::new("/usr/bin/bwrap"),
            &realpath(&work).unwrap(),
            &[realpath(&state).unwrap()],
            None,
            &plan,
        )
        .unwrap();
        assert!(!without.iter().any(|arg| arg.contains("stub-resolv.conf")));
        let work_at = bind_at(&without, "--ro-bind", work_text);
        assert_eq!(without[work_at - 2], "--tmpfs");
        assert_eq!(without[work_at - 1], "/run");
    }

    #[test]
    fn resolver_file_must_be_a_regular_file_below_an_overlay() {
        let root = temp();
        let overlays = [
            root.path().join("tmp"),
            root.path().join("var").join("tmp"),
            root.path().join("run"),
        ];
        let cases = [
            ("run/systemd/resolve/stub-resolv.conf", "file", true),
            ("tmp/resolv.conf", "file", true),
            ("var/tmp/resolv.conf", "file", true),
            ("etc/resolv.conf", "file", false),
            ("etc/resolv-target.conf", "file", false),
            ("run-other/resolv.conf", "file", false),
            ("run/missing.conf", "missing", false),
            ("run/resolver-dir", "directory", false),
        ];
        for (location, kind, expected) in cases {
            let target = root.path().join(location);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            match kind {
                "file" => {
                    fs::write(&target, b"nameserver 127.0.0.53\n").unwrap();
                }
                "directory" => fs::create_dir(&target).unwrap(),
                "missing" => {}
                _ => unreachable!(),
            }
            let found = resolver_file_under_overlays(&target, &overlays)
                .unwrap()
                .is_some();
            assert_eq!(found, expected, "{location}");
        }
    }

    #[test]
    fn missing_bwrap_fails_closed_without_running_a_command() {
        let layout = Layout::new();
        for search in [
            None,
            Some(OsStr::new("")),
            Some(layout.root.path().as_os_str()),
        ] {
            let err = readonly_view_prefix_with_search_path(
                &layout.work,
                &[layout.state.clone()],
                &[] as &[PathBuf],
                search,
            )
            .unwrap_err();
            let text = err.to_string();
            assert!(text.contains("bwrap"), "{text}");
            assert!(text.contains("will not run"), "{text}");
        }
        assert!(!layout.bin.join("bwrap-executed").exists());
    }

    #[test]
    fn search_path_order_selects_the_first_executable_name() {
        let root = temp();
        let first = root.path().join("first");
        let second = root.path().join("second");
        fs::create_dir(&first).unwrap();
        let second_bwrap = install_bwrap(&second);
        let work = root.path().join("work");
        let state = root.path().join("state");
        fs::create_dir(&work).unwrap();
        fs::create_dir(&state).unwrap();
        let joined = std::env::join_paths([&first, &second]).unwrap();
        let argv = readonly_view_prefix_with_search_path(
            &work,
            &[state.clone()],
            &[] as &[PathBuf],
            Some(joined.as_os_str()),
        )
        .unwrap();
        assert_eq!(Path::new(&argv[0]), second_bwrap.as_path());
        let first_bwrap = install_bwrap(&first);
        let joined = std::env::join_paths([&first, &second]).unwrap();
        let argv = readonly_view_prefix_with_search_path(
            &work,
            &[state],
            &[] as &[PathBuf],
            Some(joined.as_os_str()),
        )
        .unwrap();
        assert_eq!(Path::new(&argv[0]), first_bwrap.as_path());
    }

    #[cfg(unix)]
    #[test]
    fn non_executable_bwrap_is_not_accepted() {
        let root = temp();
        let bin = root.path().join("bin");
        fs::create_dir(&bin).unwrap();
        let program = bin.join("bwrap");
        fs::write(&program, b"#!/bin/sh\nexit 0\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&program).unwrap().permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(&program, permissions).unwrap();
        let work = root.path().join("work");
        let state = root.path().join("state");
        fs::create_dir(&work).unwrap();
        fs::create_dir(&state).unwrap();
        let err = readonly_view_prefix_with_search_path(
            &work,
            &[state],
            &[] as &[PathBuf],
            Some(bin.as_os_str()),
        )
        .unwrap_err();
        assert!(err.to_string().contains("will not run"));
    }

    #[test]
    fn bad_workdirs_and_writable_dirs_are_refused() {
        let layout = Layout::new();
        assert!(
            validate_view_paths(Path::new(""), &[] as &[PathBuf])
                .unwrap_err()
                .to_string()
                .contains("explicit workdir")
        );
        let missing = layout.root.path().join("missing");
        assert!(
            validate_view_paths(&missing, &[] as &[PathBuf])
                .unwrap_err()
                .to_string()
                .contains("not a directory")
        );
        let file = layout.root.path().join("file");
        touch(&file);
        assert!(
            validate_view_paths(&file, &[] as &[PathBuf])
                .unwrap_err()
                .to_string()
                .contains("not a directory")
        );
        let parent = layout.work.parent().unwrap().to_path_buf();
        let err = validate_view_paths(&layout.work, &[parent]).unwrap_err();
        assert!(err.to_string().contains("contains the workdir"), "{err}");
        let err = validate_view_paths(&layout.work, &[layout.work.clone()]).unwrap_err();
        assert!(err.to_string().contains("contains the workdir"), "{err}");
        let err =
            validate_view_paths(&layout.work, &[PathBuf::from("relative/state")]).unwrap_err();
        assert!(err.to_string().contains("absolute"), "{err}");
        let err = validate_view_paths(&layout.work, &[PathBuf::from("")]).unwrap_err();
        assert!(err.to_string().contains("absolute"), "{err}");
        let absent = layout.root.path().join("no-state");
        let err = validate_view_paths(&layout.work, &[absent]).unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err}");
        let err = validate_view_paths(&layout.work, &[file]).unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err}");
        let inner = layout.work.join("nested-state");
        fs::create_dir(&inner).unwrap();
        validate_view_paths(&layout.work, &[inner]).unwrap();
        let home = realpath(&dirs::home_dir().unwrap()).unwrap();
        let work = realpath(&layout.work).unwrap();
        let err = validate_view_paths(&layout.work, &[home.clone()]).unwrap_err();
        if work.starts_with(&home) {
            assert!(err.to_string().contains("workdir"), "{err}");
        } else {
            assert!(err.to_string().contains("home"), "{err}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn overlay_and_synthetic_roots_are_refused_and_children_are_not() {
        let root_err = validate_view_paths(Path::new("/"), &[] as &[PathBuf]).unwrap_err();
        assert!(
            root_err.to_string().contains("filesystem root"),
            "{root_err}"
        );
        for bad in ["/tmp", "/run", "/var", "/dev", "/proc"] {
            assert!(
                validate_view_paths(Path::new(bad), &[] as &[PathBuf]).is_err(),
                "{bad}"
            );
        }
        let child = temp();
        validate_view_paths(child.path(), &[] as &[PathBuf]).unwrap();
        let root = temp();
        let link = root.path().join("tmp-link");
        std::os::unix::fs::symlink("/tmp", &link).unwrap();
        assert!(validate_view_paths(&link, &[] as &[PathBuf]).is_err());
        if let Some(parent) = realpath(&dirs::home_dir().unwrap())
            .unwrap()
            .parent()
            .map(Path::to_path_buf)
        {
            let work = child.path();
            if !realpath(work).unwrap().starts_with(&parent) {
                let err = validate_view_paths(work, &[parent]).unwrap_err();
                assert!(err.to_string().contains("home"), "{err}");
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn overlay_aliases_refuse_the_host_and_allow_a_child() {
        let root = temp();
        let host = root.path().join("host");
        let child = host.join("task");
        fs::create_dir_all(&child).unwrap();
        let alias = root.path().join("overlay-alias");
        std::os::unix::fs::symlink(&host, &alias).unwrap();
        for work in [
            host.clone(),
            alias.clone(),
            alias.parent().unwrap().to_path_buf(),
        ] {
            let resolved = realpath(&work).unwrap();
            assert!(
                reject_conflicting_workdir(&resolved, &[alias.clone()], &[]).is_err(),
                "{}",
                work.display()
            );
        }
        let child_real = realpath(&child).unwrap();
        assert!(reject_conflicting_workdir(&child_real, &[alias.clone()], &[]).is_ok());
        assert!(reject_conflicting_workdir(&child_real, &[], &[alias]).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn symlink_workdir_and_writable_dir_bind_the_target() {
        let layout = Layout::new();
        let work_link = layout.root.path().join("work-link");
        let state_link = layout.root.path().join("state-link");
        std::os::unix::fs::symlink(&layout.work, &work_link).unwrap();
        std::os::unix::fs::symlink(&layout.state, &state_link).unwrap();
        let argv = readonly_view_prefix_with_search_path(
            &work_link,
            &[state_link.clone()],
            &[] as &[PathBuf],
            Some(layout.bin.as_os_str()),
        )
        .unwrap();
        let work = realpath(&layout.work).unwrap();
        let state = realpath(&layout.state).unwrap();
        assert!(bind_at(&argv, "--ro-bind", work.to_str().unwrap()) > 0);
        assert!(bind_at(&argv, "--bind", state.to_str().unwrap()) > 0);
        assert!(!argv.iter().any(|arg| arg == work_link.to_str().unwrap()));
        assert!(!argv.iter().any(|arg| arg == state_link.to_str().unwrap()));
    }

    #[test]
    fn hardlinked_file_is_refused_and_a_narrow_directory_is_not() {
        let root = temp();
        let outside = root.path().join("outside");
        let state = root.path().join("state");
        let link = plant(&state.join("worktrees").join("venv"), &outside, "pkg");
        assert_eq!(find_hardlink_escapes(&[state.clone()], None), vec![link]);
        let err = assert_no_hardlink_escape(&[state.clone()]).unwrap_err();
        assert!(err.to_string().contains("hardlink"), "{err}");
        assert!(!err.to_string().contains("(and more)"), "{err}");
        let work = root.path().join("work");
        fs::create_dir(&work).unwrap();
        let bin = root.path().join("bin");
        install_bwrap(&bin);
        let err = readonly_view_prefix_with_search_path(
            &work,
            &[state.clone()],
            &[] as &[PathBuf],
            Some(bin.as_os_str()),
        )
        .unwrap_err();
        assert!(err.to_string().contains("hardlink"), "{err}");
        let sessions = state.join("sessions");
        fs::create_dir(&sessions).unwrap();
        touch(&sessions.join("s.json"));
        assert!(find_hardlink_escapes(&[sessions.clone()], None).is_empty());
        assert_no_hardlink_escape(&[sessions]).unwrap();
    }

    #[test]
    fn ordinary_files_and_directories_are_not_hardlink_escapes() {
        let root = temp();
        let state = root.path().join("state");
        fs::create_dir_all(state.join("nested").join("dir")).unwrap();
        touch(&state.join("plain.txt"));
        assert!(find_hardlink_escapes(&[state.clone()], None).is_empty());
        assert_no_hardlink_escape(&[state]).unwrap();
        assert!(find_hardlink_escapes(&[PathBuf::new()], None).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_are_not_hardlink_escapes() {
        let root = temp();
        let state = root.path().join("state");
        fs::create_dir(&state).unwrap();
        let plain = state.join("plain.txt");
        touch(&plain);
        std::os::unix::fs::symlink(&plain, state.join("link.txt")).unwrap();
        assert!(find_hardlink_escapes(&[state], None).is_empty());
    }

    #[test]
    fn hardlink_examples_stop_at_the_cap() {
        let root = temp();
        let outside = root.path().join("outside");
        let state = root.path().join("state");
        for index in 0..6 {
            plant(&state, &outside, &format!("f{index}"));
        }
        assert_eq!(
            find_hardlink_escapes(&[state.clone()], Some(MAX_HARDLINK_EXAMPLES)).len(),
            MAX_HARDLINK_EXAMPLES
        );
        assert_eq!(find_hardlink_escapes(&[state.clone()], None).len(), 6);
        let err = assert_no_hardlink_escape(&[state]).unwrap_err();
        assert!(err.to_string().contains("(and more)"), "{err}");
    }

    #[test]
    fn runtime_guard_warns_without_removing_the_new_link() {
        let root = temp();
        let state = root.path().join("state");
        let outside = root.path().join("outside");
        fs::create_dir(&state).unwrap();
        let planted = state.join("sessions").join("planted");
        let guard = hardlink_runtime_guard(&[state.clone()], |guard| {
            assert!(guard.baseline().is_empty());
            let outside_file = outside.join("secret");
            fs::create_dir(&outside).unwrap();
            touch(&outside_file);
            fs::create_dir_all(planted.parent().unwrap()).unwrap();
            fs::hard_link(&outside_file, &planted).unwrap();
            assert_eq!(guard.rescan()[0].paths, vec![realpath(&planted).unwrap()]);
            assert!(guard.warnings().is_empty());
            guard.clone()
        });
        let warning = &guard.warnings()[0];
        assert_eq!(warning.code, HARDLINK_RUNTIME_WARNING_CODE);
        assert_eq!(warning.action, HARDLINK_RUNTIME_WARNING_ACTION);
        assert!(warning.message.contains("does not signal"));
        assert_eq!(warning.paths, vec![realpath(&planted).unwrap()]);
        assert!(planted.is_file());
        assert!(guard.is_closed());
    }

    #[test]
    fn runtime_guard_is_quiet_when_nothing_new_is_linked() {
        let root = temp();
        let state = root.path().join("state");
        fs::create_dir(&state).unwrap();
        touch(&state.join("plain.txt"));
        let guard = hardlink_runtime_guard(&[state.clone()], |guard| {
            touch(&state.join("during.txt"));
            assert!(guard.rescan().is_empty());
            guard.clone()
        });
        assert!(guard.warnings().is_empty());
    }

    #[test]
    fn preexisting_hardlink_refuses_the_build_and_is_not_a_new_warning() {
        let root = temp();
        let state = root.path().join("state");
        let outside = root.path().join("outside");
        let planted = plant(&state, &outside, "already");
        assert!(assert_no_hardlink_escape(&[state.clone()]).is_err());
        let guard = hardlink_runtime_guard(&[state], |guard| {
            assert!(guard.baseline().contains(&planted));
            guard.clone()
        });
        assert!(guard.warnings().is_empty());
        let baseline = guard.baseline();
        guard.note_successful_build();
        assert_eq!(guard.baseline(), baseline);
        assert!(planted.is_file());
    }

    #[test]
    fn same_path_replanted_after_a_successful_check_is_warned() {
        let root = temp();
        let state = root.path().join("state");
        let outside = root.path().join("outside");
        let other = root.path().join("other");
        fs::create_dir(&other).unwrap();
        let planted = plant(&state, &outside, "p");
        let replacement = outside.join("b-target");
        touch(&replacement);
        let guard = hardlink_runtime_guard(&[state.clone()], |guard| {
            assert!(guard.baseline().contains(&planted));
            assert!(assert_no_hardlink_escape(&[state.clone()]).is_err());
            assert!(guard.baseline().contains(&planted));
            assert_no_hardlink_escape(&[other.clone()]).unwrap();
            assert!(guard.baseline().contains(&planted));
            fs::remove_file(&planted).unwrap();
            assert_no_hardlink_escape(&[state.clone()]).unwrap();
            assert!(guard.baseline().is_empty());
            fs::hard_link(&replacement, &planted).unwrap();
            assert_eq!(guard.rescan()[0].paths, vec![planted.clone()]);
            guard.clone()
        });
        assert_eq!(guard.warnings()[0].paths, vec![planted]);
        assert_eq!(guard.warnings()[0].action, "warn");
    }

    #[test]
    fn prefix_build_clears_a_replanted_baseline_even_if_bwrap_is_missing_later() {
        let layout = Layout::new();
        let outside = layout.root.path().join("outside");
        let planted = plant(&layout.state, &outside, "p");
        let guard = hardlink_runtime_guard(&[layout.state.clone()], |guard| {
            assert!(guard.baseline().contains(&planted));
            fs::remove_file(&planted).unwrap();
            let err = readonly_view_prefix_with_search_path(
                &layout.work,
                &[layout.state.clone()],
                &[] as &[PathBuf],
                None,
            )
            .unwrap_err();
            assert!(err.to_string().contains("will not run"), "{err}");
            assert!(
                guard.baseline().is_empty(),
                "a passing hardlink check clears the baseline before bwrap is resolved"
            );
            fs::hard_link(outside.join("p-target"), &planted).unwrap();
            guard.clone()
        });
        assert_eq!(guard.warnings()[0].paths, vec![planted]);
    }

    #[test]
    fn exit_scan_sees_a_new_link_past_the_startup_cap() {
        let root = temp();
        let state = root.path().join("state");
        let outside = root.path().join("outside");
        let mut old = BTreeSet::new();
        for index in 0..5 {
            old.insert(plant(&state, &outside, &format!("a-old-{index}")));
        }
        let fresh_target = outside.join("fresh-target");
        touch(&fresh_target);
        let planted = state.join("z-planted");
        let guard = hardlink_runtime_guard(&[state], |guard| {
            assert_eq!(guard.baseline(), old);
            fs::hard_link(&fresh_target, &planted).unwrap();
            guard.clone()
        });
        assert_eq!(guard.warnings()[0].paths, vec![realpath(&planted).unwrap()]);
    }

    #[test]
    fn extra_runtime_paths_stay_in_the_warning_and_the_message_is_capped() {
        let root = temp();
        let state = root.path().join("state");
        let outside = root.path().join("outside");
        fs::create_dir(&state).unwrap();
        let guard = hardlink_runtime_guard(&[state.clone()], |guard| {
            for index in 0..6 {
                plant(&state, &outside, &format!("new-{index}"));
            }
            guard.clone()
        });
        assert_eq!(guard.warnings()[0].paths.len(), 6);
        assert!(guard.warnings()[0].message.contains("(1 more not listed)"));
        assert!(guard.warnings()[0].message.contains("does not signal"));
    }

    #[test]
    fn nested_guard_for_the_same_directories_scans_once() {
        let root = temp();
        let state = root.path().join("state");
        let outside = root.path().join("outside");
        fs::create_dir(&state).unwrap();
        let planted = state.join("planted");
        let guard = hardlink_runtime_guard(&[state.clone()], |outer| {
            let inner = hardlink_runtime_guard(&[state.clone()], |inner| {
                assert!(outer.same_guard(inner));
                let target = outside.join("target");
                fs::create_dir(&outside).unwrap();
                touch(&target);
                fs::hard_link(&target, &planted).unwrap();
                inner.clone()
            });
            assert!(!inner.is_closed());
            assert!(inner.warnings().is_empty());
            outer.clone()
        });
        assert_eq!(guard.warnings().len(), 1);
        assert_eq!(guard.warnings()[0].paths, vec![realpath(&planted).unwrap()]);
    }

    #[test]
    fn nested_guard_for_other_directories_does_not_clear_the_outer_baseline() {
        let root = temp();
        let state = root.path().join("state");
        let other = root.path().join("other");
        let outside = root.path().join("outside");
        fs::create_dir(&other).unwrap();
        let planted = plant(&state, &outside, "keep");
        hardlink_runtime_guard(&[state], |outer| {
            let before = outer.baseline();
            assert!(before.contains(&planted));
            hardlink_runtime_guard(&[other], |_| ());
            assert_eq!(outer.baseline(), before);
            assert!(!outer.is_closed());
        });
    }

    #[test]
    fn hidden_sqlite_sidecars_links_and_missing_paths_follow_the_plan() {
        let layout = Layout::new();
        let db = layout.work.join("ledger.sqlite");
        touch(&db);
        for suffix in SQLITE_SIDECARS {
            touch(&PathBuf::from(format!("{}{suffix}", db.display())));
        }
        let missing = layout.work.join("absent");
        let plan = plan_hidden_paths(&[db.clone(), missing.clone()]).unwrap();
        let db_real = realpath(&db).unwrap();
        assert!(plan.hidden.contains(&db_real));
        for suffix in SQLITE_SIDECARS {
            let sidecar = PathBuf::from(format!("{}{suffix}", db_real.display()));
            assert!(plan.hidden.contains(&sidecar), "{}", sidecar.display());
        }
        assert!(plan.skipped_missing.contains(&missing));
        assert!(plan_hidden_paths(&[PathBuf::from("relative")]).is_err());
        let notes = layout.work.join("notes.txt");
        touch(&notes);
        let notes_plan = plan_hidden_paths(&[notes.clone()]).unwrap();
        assert_eq!(notes_plan.hidden, vec![realpath(&notes).unwrap()]);
        assert!(notes_plan.skipped_missing.is_empty());
        let upper = layout.work.join("File.SQLITE");
        touch(&upper);
        let upper_plan = plan_hidden_paths(&[upper.clone()]).unwrap();
        assert_eq!(upper_plan.hidden, vec![realpath(&upper).unwrap()]);
        let absent_db = layout.work.join("missing.sqlite");
        let absent_plan = plan_hidden_paths(&[absent_db.clone()]).unwrap();
        assert!(absent_plan.hidden.is_empty());
        assert!(absent_plan.skipped_missing.iter().any(|path| {
            path.file_name().and_then(|name| name.to_str()) == Some("missing.sqlite")
        }));
        assert!(absent_plan.skipped_missing.iter().any(|path| {
            path.file_name().and_then(|name| name.to_str()) == Some("missing.sqlite-wal")
        }));
        assert!(hidden_path_mounts(&absent_plan).unwrap().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn sqlite_alias_hides_the_target_and_both_sidecar_names() {
        let layout = Layout::new();
        let db = layout.work.join("ledger.sqlite");
        touch(&db);
        for suffix in SQLITE_SIDECARS {
            touch(&PathBuf::from(format!("{}{suffix}", db.display())));
        }
        let alias = layout.state.join("alias.db");
        std::os::unix::fs::symlink(&db, &alias).unwrap();
        let plan = plan_hidden_paths(&[alias.clone()]).unwrap();
        let db_real = realpath(&db).unwrap();
        assert!(plan.hidden.contains(&db_real));
        assert!(!plan.hidden.iter().any(|path| path == &alias));
        for suffix in SQLITE_SIDECARS {
            assert!(
                plan.hidden
                    .contains(&PathBuf::from(format!("{}{suffix}", db_real.display())))
            );
        }
    }

    #[test]
    fn hiding_a_protected_directory_is_refused_including_a_missing_ancestor() {
        let layout = Layout::new();
        for hidden in [
            layout.state.clone(),
            layout.state.parent().unwrap().to_path_buf(),
            layout.work.clone(),
        ] {
            let err =
                validate_hidden_paths(&layout.work, &[layout.state.clone()], &[hidden.clone()])
                    .unwrap_err();
            assert!(err.to_string().contains("cannot hide"), "{hidden:?} {err}");
        }
        let weird = layout.work.join("nosuch").join("..");
        let plan = plan_hidden_paths(&[weird.clone()]).unwrap();
        assert!(plan.hidden.contains(&realpath(&layout.work).unwrap()));
        let err =
            validate_hidden_paths(&layout.work, &[layout.state.clone()], &[weird]).unwrap_err();
        assert!(err.to_string().contains("cannot hide"), "{err}");
        let child = layout.work.join("secret.txt");
        touch(&child);
        let plan =
            validate_hidden_paths(&layout.work, &[layout.state.clone()], &[child.clone()]).unwrap();
        assert_eq!(plan.hidden, vec![realpath(&child).unwrap()]);
    }

    #[test]
    fn hidden_mounts_use_tmpfs_for_directories_and_dev_null_for_files() {
        let layout = Layout::new();
        let parent = layout.work.join("nest");
        let child = parent.join("inner");
        fs::create_dir_all(&child).unwrap();
        let file = layout.work.join("secret.txt");
        touch(&file);
        let plan = plan_hidden_paths(&[child.clone(), parent.clone(), file.clone()]).unwrap();
        assert_eq!(
            plan.hidden,
            vec![
                realpath(&parent).unwrap(),
                realpath(&file).unwrap(),
                realpath(&child).unwrap(),
            ]
        );
        let argv = layout.prefix(&[parent.clone(), child.clone(), file.clone()]);
        let parent_text = realpath(&parent).unwrap();
        let child_text = realpath(&child).unwrap();
        let file_text = realpath(&file).unwrap();
        assert!(
            flag_at(&argv, "--tmpfs", parent_text.to_str().unwrap())
                < flag_at(&argv, "--tmpfs", child_text.to_str().unwrap())
        );
        let file_at = argv
            .windows(3)
            .position(|window| {
                window[0] == "--ro-bind"
                    && window[1] == "/dev/null"
                    && window[2] == file_text.to_str().unwrap()
            })
            .unwrap();
        assert!(
            file_at
                > flag_at(
                    &argv,
                    "--bind",
                    realpath(&layout.state).unwrap().to_str().unwrap()
                )
        );
        let tmpfs = argv
            .windows(2)
            .filter(|window| window[0] == "--tmpfs")
            .map(|window| window[1].as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            tmpfs,
            vec![
                "/tmp",
                "/var/tmp",
                "/run",
                parent_text.to_str().unwrap(),
                child_text.to_str().unwrap(),
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn sockets_are_hidden_with_dev_null_and_devices_are_refused() {
        let layout = Layout::new();
        let socket = layout.state.join("service.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let plan = plan_hidden_paths(&[socket.clone()]).unwrap();
        assert_eq!(plan.hidden, vec![realpath(&socket).unwrap()]);
        let mounts = hidden_path_mounts(&plan).unwrap();
        assert_eq!(
            mounts,
            vec![
                "--ro-bind".to_string(),
                "/dev/null".to_string(),
                realpath(&socket).unwrap().to_str().unwrap().to_string(),
            ]
        );
        let err = plan_hidden_paths(&[PathBuf::from("/dev/null")]).unwrap_err();
        assert!(err.to_string().contains("unsupported"), "{err}");
    }

    #[test]
    fn hide_errors_wait_until_bwrap_is_found() {
        let layout = Layout::new();
        let err = readonly_view_prefix_with_search_path(
            &layout.work,
            &[layout.state.clone()],
            &[layout.work.clone()],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("will not run"), "{err}");
        let err = readonly_view_prefix_with_search_path(
            &layout.work,
            &[layout.state.clone()],
            &[layout.work.clone()],
            Some(layout.bin.as_os_str()),
        )
        .unwrap_err();
        assert!(err.to_string().contains("cannot hide"), "{err}");
    }

    #[test]
    fn invalid_workdir_is_reported_before_a_missing_bwrap() {
        let err = readonly_view_prefix_with_search_path(
            Path::new(""),
            &[] as &[PathBuf],
            &[] as &[PathBuf],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("explicit workdir"), "{err}");
    }

    #[test]
    fn spawn_guard_requires_close_fds_and_rejects_every_passed_fd() {
        assert_readonly_view_spawn(true, &[]).unwrap();
        let err = assert_readonly_view_spawn(false, &[]).unwrap_err();
        assert!(err.to_string().contains("close_fds=True"), "{err}");
        let err = assert_readonly_view_spawn(false, &[3]).unwrap_err();
        assert!(err.to_string().contains("close_fds=True"), "{err}");
        let err = assert_readonly_view_spawn(true, &[3]).unwrap_err();
        assert!(err.to_string().contains("caller pass_fds"), "{err}");
        let err = assert_readonly_view_spawn(true, &[3, 4]).unwrap_err();
        assert!(err.to_string().contains("caller pass_fds"), "{err}");
        let err = assert_readonly_view_spawn(true, &[10_000]).unwrap_err();
        assert!(err.to_string().contains("caller pass_fds"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn spawn_guard_rejects_a_live_readonly_fd_without_reading_it() {
        let root = temp();
        let path = root.path().join("plain.txt");
        touch(&path);
        let file = fs::File::open(&path).unwrap();
        use std::os::unix::io::AsRawFd;
        let err = assert_readonly_view_spawn(true, &[file.as_raw_fd()]).unwrap_err();
        assert!(err.to_string().contains("caller pass_fds"), "{err}");
        assert_eq!(fs::read(&path).unwrap(), b"x");
    }

    #[test]
    fn argv_detector_matches_the_prefix_and_not_a_sandbox_shape() {
        assert!(is_readonly_view_argv(&[
            "/usr/bin/bwrap",
            "--ro-bind",
            "/",
            "/",
            "--"
        ]));
        assert!(!is_readonly_view_argv(&[] as &[&str]));
        assert!(!is_readonly_view_argv(&["/usr/bin/bwrap"]));
        assert!(!is_readonly_view_argv(&[
            "/usr/bin/bwrap",
            "--die-with-parent",
            "--ro-bind",
            "/",
            "/"
        ]));
        assert!(!is_readonly_view_argv(&["/bin/true"]));
        let layout = Layout::new();
        let argv = layout.prefix(&[]);
        assert!(is_readonly_view_argv(&argv));
    }

    #[test]
    fn public_prefix_reads_process_path_and_does_not_require_a_spawn() {
        let root = temp();
        let work = root.path().join("work");
        let state = root.path().join("state");
        fs::create_dir(&work).unwrap();
        fs::create_dir(&state).unwrap();
        match readonly_view_prefix(&work, &[state], &[] as &[PathBuf]) {
            Ok(argv) => {
                assert!(is_readonly_view_argv(&argv));
                assert_eq!(
                    Path::new(&argv[0])
                        .file_name()
                        .and_then(|name| name.to_str()),
                    Some("bwrap")
                );
                assert_eq!(argv[argv.len() - 1], "--");
                assert!(argv.iter().any(|arg| arg == "--die-with-parent"));
            }
            Err(err) => {
                let text = err.to_string();
                assert!(text.contains("bwrap"), "{text}");
                assert!(text.contains("will not run"), "{text}");
            }
        }
    }

    #[test]
    fn view_keys_keep_their_private_names() {
        assert_eq!(VIEW_DBUS_SOCKET_KEY, "_readonly_view_dbus_socket");
        assert_eq!(VIEW_HIDE_PATHS_KEY, "_readonly_view_hide_paths");
    }
}
