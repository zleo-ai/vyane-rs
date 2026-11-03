//! `git`-backed [`WorktreeProvisioner`].
//!
//! Every command runs with a scrubbed environment (no `GIT_*` variable
//! survives), repository hooks disabled through `core.hooksPath=/dev/null`,
//! and `core.fsmonitor=false` so repository config cannot start a helper.
//! Paths are checked to stay below the configured worktree root before any
//! git call that creates or removes a worktree. Settlement keeps the tree
//! whenever a probe fails or reports anything but "identical to base".
//!
//! The worktree content is controlled by the target that ran in it, so
//! settlement never lets the tree choose the repository: every probe names
//! the per-worktree git directory recorded at creation explicitly, and
//! submodules are never inspected (a nested repository's config could
//! otherwise run commands through `git status`).

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;
use vyane_core::EnvPolicy;

use super::{
    CleanupRequest, IsolateRequest, IsolationChoice, ObservedTree, ProvisionedWorktree,
    WorktreeError, WorktreeKeptReason, WorktreePlanError, WorktreePlanner, WorktreeProvisioner,
    WorktreeRequest, WorktreeSettlement, sanitize_detail, validate_isolation_base,
};

/// Upper bound for one git command. A command that runs longer is killed and
/// treated as a failed probe.
const GIT_COMMAND_TIMEOUT: Duration = Duration::from_secs(120);

/// Largest `.git` file settlement reads from a worktree root.
const GIT_FILE_MAX_BYTES: u64 = 4096;

/// Index entry mode of a gitlink (submodule) in `git ls-files --stage`.
const GITLINK_MODE_PREFIX: &[u8] = b"160000 ";

/// Options prepended to every git invocation.
const GIT_SAFETY_OPTIONS: [&str; 4] = [
    "-c",
    "core.hooksPath=/dev/null",
    "-c",
    "core.fsmonitor=false",
];

/// Creates one git worktree per isolated target below `root`.
///
/// Mutating git calls are serialized through one lock so concurrent steps do
/// not contend for repository lock files.
#[derive(Debug)]
pub struct GitWorktreeProvisioner {
    root: PathBuf,
    git: PathBuf,
    lock: Mutex<()>,
}

struct GitOutput {
    success: bool,
    code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl GitWorktreeProvisioner {
    /// Use `git` from `PATH` and place worktrees below `root`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            git: PathBuf::from("git"),
            lock: Mutex::new(()),
        }
    }

    /// Use a specific git executable.
    #[must_use]
    pub fn with_git_program(mut self, git: impl Into<PathBuf>) -> Self {
        self.git = git.into();
        self
    }

    /// Directory that holds every worktree created by this provisioner.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    async fn git<I, S>(&self, cwd: &Path, args: I) -> Option<GitOutput>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = tokio::process::Command::new(&self.git);
        command
            .args(GIT_SAFETY_OPTIONS)
            .args(args)
            .current_dir(cwd)
            .env_clear()
            .envs(git_environment())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let output = tokio::time::timeout(GIT_COMMAND_TIMEOUT, command.output())
            .await
            .ok()?
            .ok()?;
        Some(GitOutput {
            success: output.status.success(),
            code: output.status.code(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }

    /// Run a probe and return its trimmed stdout only when it succeeded.
    async fn probe<I, S>(&self, cwd: &Path, args: I) -> Option<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.git(cwd, args).await?;
        if !output.success {
            return None;
        }
        let text = String::from_utf8(output.stdout).ok()?;
        Some(text.trim_end_matches(['\n', '\r']).to_string())
    }

    async fn commit_id(&self, cwd: &Path, spec: &str) -> Option<String> {
        let sha = self
            .probe(
                cwd,
                ["rev-parse", "--verify", "--quiet", "--end-of-options", spec],
            )
            .await?;
        is_object_id(&sha).then_some(sha)
    }

    async fn canonical_root(&self) -> Option<PathBuf> {
        tokio::fs::canonicalize(&self.root).await.ok()
    }

    /// Whether `path` is exactly `<root>/<run>/<leaf>` after symlinks are
    /// resolved. Anything else is never handed to a removing git command.
    async fn is_owned_worktree_path(&self, path: &Path) -> bool {
        let Some(root) = self.canonical_root().await else {
            return false;
        };
        let Ok(canonical) = tokio::fs::canonicalize(path).await else {
            return false;
        };
        canonical == path
            && canonical
                .parent()
                .and_then(Path::parent)
                .is_some_and(|grandparent| grandparent == root)
    }

    /// Resolve `git rev-parse --git-common-dir` for `cwd` to a canonical path.
    async fn common_dir(&self, cwd: &Path) -> Option<PathBuf> {
        let raw = self.probe(cwd, ["rev-parse", "--git-common-dir"]).await?;
        let path = PathBuf::from(raw);
        let absolute = if path.is_absolute() {
            path
        } else {
            cwd.join(path)
        };
        tokio::fs::canonicalize(absolute).await.ok()
    }

    /// Run git against one worktree with its git directory and work tree
    /// named explicitly, so a `.git` file rewritten inside the tree cannot
    /// redirect the command to another repository.
    async fn worktree_git(
        &self,
        worktree: &ProvisionedWorktree,
        args: &[&str],
    ) -> Option<GitOutput> {
        let mut full: Vec<OsString> = Vec::with_capacity(args.len() + 2);
        full.push(prefixed_os("--git-dir=", &worktree.git_dir));
        full.push(prefixed_os("--work-tree=", &worktree.root));
        full.extend(args.iter().map(OsString::from));
        self.git(&worktree.root, &full).await
    }

    async fn worktree_probe(
        &self,
        worktree: &ProvisionedWorktree,
        args: &[&str],
    ) -> Option<String> {
        let output = self.worktree_git(worktree, args).await?;
        if !output.success {
            return None;
        }
        let text = String::from_utf8(output.stdout).ok()?;
        Some(text.trim_end_matches(['\n', '\r']).to_string())
    }

    /// Canonical common git directory of one worktree, read through its
    /// recorded git directory.
    async fn worktree_common_dir(&self, worktree: &ProvisionedWorktree) -> Option<PathBuf> {
        let raw = self
            .worktree_probe(worktree, &["rev-parse", "--git-common-dir"])
            .await?;
        let path = PathBuf::from(raw);
        let absolute = if path.is_absolute() {
            path
        } else {
            worktree.git_dir.join(path)
        };
        tokio::fs::canonicalize(absolute).await.ok()
    }

    /// Per-worktree git directory of a worktree this provisioner just
    /// created. It must sit directly below `<common dir>/worktrees`.
    async fn new_worktree_git_dir(&self, source_root: &Path, root: &Path) -> Option<PathBuf> {
        let raw = self
            .probe(root, ["rev-parse", "--absolute-git-dir"])
            .await?;
        let git_dir = tokio::fs::canonicalize(raw).await.ok()?;
        let common = self.common_dir(source_root).await?;
        (git_dir.parent() == Some(common.join("worktrees").as_path())).then_some(git_dir)
    }

    /// Finish a worktree after `git worktree add`: locate its git directory
    /// and create the execution workdir without following any symlink.
    async fn finish_provision(
        &self,
        source_root: &Path,
        root: &Path,
        relative: &Path,
    ) -> Result<(PathBuf, PathBuf), WorktreeError> {
        let git_dir = self
            .new_worktree_git_dir(source_root, root)
            .await
            .ok_or_else(|| WorktreeError::AddFailed {
                detail: "could not locate the git directory of the new worktree".into(),
            })?;
        let execution = prepare_workdir(root, relative).await?;
        Ok((execution, git_dir))
    }

    /// Undo a worktree whose provisioning did not finish. Nothing has run in
    /// it, so the same conservative removal as settlement applies: no
    /// `--force`, and the branch is deleted only while it is still at base.
    async fn discard_new_worktree(
        &self,
        source_root: &Path,
        root: &Path,
        branch: &str,
        base_sha: &str,
    ) -> bool {
        let mut remove: Vec<OsString> = vec!["worktree".into(), "remove".into()];
        remove.push(root.as_os_str().to_os_string());
        if !self
            .git(source_root, &remove)
            .await
            .is_some_and(|output| output.success)
        {
            return false;
        }
        let branch_ref = format!("refs/heads/{branch}");
        self.git(
            source_root,
            [
                "update-ref",
                "--no-deref",
                "-d",
                branch_ref.as_str(),
                base_sha,
            ],
        )
        .await
        .is_some_and(|output| output.success)
    }

    /// Whether the worktree's `.git` file still points at `git_dir`.
    async fn git_file_matches(&self, worktree: &ProvisionedWorktree) -> bool {
        let dot_git = worktree.root.join(".git");
        let Ok(metadata) = tokio::fs::symlink_metadata(&dot_git).await else {
            return false;
        };
        if !metadata.file_type().is_file() || metadata.len() > GIT_FILE_MAX_BYTES {
            return false;
        }
        let Ok(content) = tokio::fs::read_to_string(&dot_git).await else {
            return false;
        };
        let Some(target) = content
            .strip_prefix("gitdir: ")
            .map(|rest| rest.trim_end_matches(['\n', '\r']))
            .filter(|rest| !rest.is_empty())
        else {
            return false;
        };
        let target = Path::new(target);
        let absolute = if target.is_absolute() {
            target.to_path_buf()
        } else {
            worktree.root.join(target)
        };
        tokio::fs::canonicalize(absolute)
            .await
            .is_ok_and(|resolved| resolved == worktree.git_dir)
    }

    /// Whether `reference` is a symbolic ref. `None` when the probe failed.
    async fn is_symbolic_ref(&self, cwd: &Path, reference: &str) -> Option<bool> {
        let output = self
            .git(cwd, ["symbolic-ref", "--quiet", reference])
            .await?;
        match output.code {
            Some(0) => Some(true),
            // `--quiet` exits 1 without output for a ref that is not symbolic.
            Some(1) => Some(false),
            _ => None,
        }
    }

    /// Whether the worktree index holds a gitlink. `None` when the probe
    /// failed.
    async fn has_gitlink(&self, worktree: &ProvisionedWorktree) -> Option<bool> {
        let output = self
            .worktree_git(worktree, &["ls-files", "--stage", "-z"])
            .await?;
        if !output.success {
            return None;
        }
        Some(
            output
                .stdout
                .split(|byte| *byte == 0)
                .any(|entry| entry.starts_with(GITLINK_MODE_PREFIX)),
        )
    }
}

#[async_trait]
impl WorktreeProvisioner for GitWorktreeProvisioner {
    async fn provision(
        &self,
        request: WorktreeRequest<'_>,
    ) -> Result<ProvisionedWorktree, WorktreeError> {
        validate_isolation_base(request.base).map_err(|_| WorktreeError::BaseUnresolved)?;
        let _guard = self.lock.lock().await;

        let source = tokio::fs::canonicalize(request.source_workdir)
            .await
            .map_err(|_| WorktreeError::NotARepository)?;
        let top_level = self
            .probe(&source, ["rev-parse", "--show-toplevel"])
            .await
            .filter(|top| !top.is_empty())
            .ok_or(WorktreeError::NotARepository)?;
        let source_root = tokio::fs::canonicalize(&top_level)
            .await
            .map_err(|_| WorktreeError::NotARepository)?;
        let relative = source
            .strip_prefix(&source_root)
            .map_err(|_| WorktreeError::NotARepository)?
            .to_path_buf();

        let base_sha = self
            .commit_id(&source_root, &format!("{}^{{commit}}", request.base))
            .await
            .ok_or(WorktreeError::BaseUnresolved)?;

        create_private_dir(&self.root).await?;
        let root = self
            .canonical_root()
            .await
            .ok_or(WorktreeError::PathEscape)?;
        // The run directory is checked before any permission change, so a
        // symlink planted there never redirects the `chmod`.
        let run_dir = root.join(request.wf_run_id.as_str());
        create_private_child_dir(&run_dir).await?;
        let run_dir = tokio::fs::canonicalize(&run_dir)
            .await
            .map_err(|_| WorktreeError::PathEscape)?;
        if run_dir.parent() != Some(root.as_path()) {
            return Err(WorktreeError::PathEscape);
        }
        let path = run_dir.join(request.leaf());
        let branch = request.branch();
        WorktreePlanner::plan(&IsolationChoice::Isolate(IsolateRequest {
            branch: branch.clone(),
            path: path.clone(),
            tree: ObservedTree::Clean,
            cleanup: CleanupRequest::Leave,
        }))
        .map_err(|error| match error {
            WorktreePlanError::EmptyBranch | WorktreePlanError::InvalidBranch => {
                WorktreeError::AddFailed {
                    detail: "generated branch name is invalid".into(),
                }
            }
            WorktreePlanError::NotOptedIn
            | WorktreePlanError::DirtyTreeCleanupRefused
            | WorktreePlanError::EmptyPath
            | WorktreePlanError::InvalidPath => WorktreeError::PathEscape,
        })?;
        match tokio::fs::symlink_metadata(&path).await {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) | Err(_) => return Err(WorktreeError::PathExists),
        }

        let mut args: Vec<OsString> = vec![
            "worktree".into(),
            "add".into(),
            "-b".into(),
            branch.clone().into(),
        ];
        args.push(path.clone().into_os_string());
        args.push(base_sha.clone().into());
        let output =
            self.git(&source_root, &args)
                .await
                .ok_or_else(|| WorktreeError::AddFailed {
                    detail: "git could not be run".into(),
                })?;
        if !output.success {
            return Err(WorktreeError::AddFailed {
                detail: sanitize_detail(&output.stderr),
            });
        }

        // From here on the worktree and its branch exist. Every failure
        // removes them again, or reports them so the caller can record them.
        match self.finish_provision(&source_root, &path, &relative).await {
            Ok((execution, git_dir)) => Ok(ProvisionedWorktree {
                path: execution,
                root: path,
                source_root,
                git_dir,
                branch,
                base_sha,
            }),
            Err(cause) => {
                if self
                    .discard_new_worktree(&source_root, &path, &branch, &base_sha)
                    .await
                {
                    Err(cause)
                } else {
                    Err(WorktreeError::LeftInPlace {
                        cause: Box::new(cause),
                        branch,
                        root: path,
                        base_sha,
                    })
                }
            }
        }
    }

    async fn settle(&self, worktree: &ProvisionedWorktree) -> WorktreeSettlement {
        let _guard = self.lock.lock().await;
        let unknown =
            |head: Option<String>| WorktreeSettlement::kept(WorktreeKeptReason::UnknownState, head);
        if !self.is_owned_worktree_path(&worktree.root).await || !is_object_id(&worktree.base_sha) {
            return unknown(None);
        }
        // The tree must still name the git directory it was created with;
        // a rewritten `.git` file means its state cannot be trusted.
        if !self.git_file_matches(worktree).await {
            return unknown(None);
        }
        // The worktree must still belong to the source repository; otherwise
        // the probes below would describe some other repository.
        let (Some(worktree_common), Some(source_common)) = (
            self.worktree_common_dir(worktree).await,
            self.common_dir(&worktree.source_root).await,
        ) else {
            return unknown(None);
        };
        if worktree_common != source_common {
            return unknown(None);
        }

        let head = self
            .worktree_probe(
                worktree,
                &[
                    "rev-parse",
                    "--verify",
                    "--quiet",
                    "--end-of-options",
                    "HEAD",
                ],
            )
            .await
            .filter(|sha| is_object_id(sha));
        // Submodules are never inspected: `git status` would run git inside
        // a nested repository whose config the target wrote. A gitlink in the
        // index means the state is not known, so the tree is kept.
        match self.has_gitlink(worktree).await {
            Some(false) => {}
            Some(true) | None => return unknown(head),
        }
        let Some(status) = self
            .worktree_git(
                worktree,
                &[
                    "status",
                    "--porcelain=v1",
                    "--untracked-files=all",
                    "--ignored=matching",
                    "--ignore-submodules=all",
                ],
            )
            .await
        else {
            return unknown(head);
        };
        if !status.success {
            return unknown(head);
        }
        if !status.stdout.is_empty() {
            return WorktreeSettlement::kept(WorktreeKeptReason::Dirty, head);
        }
        let Some(head) = head else {
            return unknown(None);
        };
        // A clean tree is not proof of "no work": commits made by the target
        // leave the tree clean. Compare HEAD and the branch tip to the base.
        if head != worktree.base_sha {
            return WorktreeSettlement::kept(WorktreeKeptReason::Committed, Some(head));
        }
        let branch_ref = format!("refs/heads/{}", worktree.branch);
        let Some(tip) = self.commit_id(&worktree.source_root, &branch_ref).await else {
            return unknown(Some(head));
        };
        if tip != worktree.base_sha {
            return WorktreeSettlement::kept(WorktreeKeptReason::Committed, Some(head));
        }
        // The target can turn its branch into a symbolic ref to another
        // branch at the base. Deleting through that link would remove the
        // other branch, so a symbolic branch keeps the tree.
        match self
            .is_symbolic_ref(&worktree.source_root, &branch_ref)
            .await
        {
            Some(false) => {}
            Some(true) | None => return unknown(Some(head)),
        }

        // No `--force`: git refuses to remove a tree it considers unclean.
        let mut remove: Vec<OsString> = vec!["worktree".into(), "remove".into()];
        remove.push(worktree.root.clone().into_os_string());
        match self.git(&worktree.source_root, &remove).await {
            Some(output) if output.success => {}
            Some(_) | None => return WorktreeSettlement::cleanup_failed(Some(head)),
        }
        // Compare-and-delete: a branch that moved since the checks is kept.
        // `--no-deref` deletes the branch ref itself, never a ref it names.
        match self
            .git(
                &worktree.source_root,
                [
                    "update-ref",
                    "--no-deref",
                    "-d",
                    branch_ref.as_str(),
                    worktree.base_sha.as_str(),
                ],
            )
            .await
        {
            Some(output) if output.success => WorktreeSettlement::removed(head),
            Some(_) | None => WorktreeSettlement::cleanup_failed(Some(head)),
        }
    }
}

/// Child environment for git: the scrubbed baseline with every `GIT_*`
/// variable removed, so a caller's `GIT_DIR` or `GIT_WORK_TREE` cannot
/// redirect a command to another repository.
fn git_environment() -> BTreeMap<String, String> {
    let parent = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)));
    let mut environment = EnvPolicy::scrubbed().build(parent);
    environment.retain(|key, _| !key.to_ascii_uppercase().starts_with("GIT_"));
    environment
}

fn prefixed_os(prefix: &str, path: &Path) -> OsString {
    let mut value = OsString::from(prefix);
    value.push(path.as_os_str());
    value
}

fn is_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Create or reuse `path` as an owner-only directory. An existing entry must
/// be a real directory: a symlink or any other file is a path escape and its
/// target's permissions are never changed.
async fn create_private_child_dir(path: &Path) -> Result<(), WorktreeError> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => return Err(WorktreeError::PathEscape),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir(path)
                .await
                .map_err(|_| WorktreeError::PathEscape)?;
        }
        Err(_) => return Err(WorktreeError::PathEscape),
    }
    set_private_permissions(path).await
}

/// Create the execution workdir `root/relative` one component at a time.
/// Missing components are created; an existing component must be a real
/// directory. A symlink (which could point outside the worktree) or a file
/// at the base commit fails before anything is created through it.
async fn prepare_workdir(root: &Path, relative: &Path) -> Result<PathBuf, WorktreeError> {
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(WorktreeError::PathEscape);
        };
        current.push(name);
        match tokio::fs::symlink_metadata(&current).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(WorktreeError::PathEscape);
            }
            Ok(metadata) if metadata.file_type().is_dir() => {}
            Ok(_) => {
                return Err(WorktreeError::AddFailed {
                    detail: "the workdir is not a directory at the isolation base".into(),
                });
            }
            // A subdirectory that is not tracked at the base commit. An empty
            // directory does not make the worktree dirty.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tokio::fs::create_dir(&current)
                    .await
                    .map_err(|_| WorktreeError::AddFailed {
                        detail: "could not create the workdir inside the worktree".into(),
                    })?;
            }
            Err(_) => {
                return Err(WorktreeError::AddFailed {
                    detail: "could not inspect the workdir inside the worktree".into(),
                });
            }
        }
    }
    let canonical = tokio::fs::canonicalize(&current)
        .await
        .map_err(|_| WorktreeError::PathEscape)?;
    if !canonical.starts_with(root) {
        return Err(WorktreeError::PathEscape);
    }
    Ok(canonical)
}

async fn create_private_dir(path: &Path) -> Result<(), WorktreeError> {
    tokio::fs::create_dir_all(path)
        .await
        .map_err(|_| WorktreeError::PathEscape)?;
    set_private_permissions(path).await
}

async fn set_private_permissions(path: &Path) -> Result<(), WorktreeError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(|_| WorktreeError::PathEscape)?;
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn object_ids_are_lowercase_hex_of_sha1_or_sha256_length() {
        assert!(is_object_id(&"a".repeat(40)));
        assert!(is_object_id(&"0".repeat(64)));
        assert!(!is_object_id(&"A".repeat(40)));
        assert!(!is_object_id(&"a".repeat(39)));
        assert!(!is_object_id("HEAD"));
    }

    #[test]
    fn git_environment_drops_every_git_variable() {
        let environment = git_environment();
        assert!(environment.keys().all(|key| !key.starts_with("GIT_")));
    }
}
