//! `git`-backed [`WorktreeProvisioner`].
//!
//! Every command runs with a scrubbed environment (no `GIT_*` variable
//! survives), repository hooks disabled through `core.hooksPath=/dev/null`,
//! and `core.fsmonitor=false` so repository config cannot start a helper.
//! Paths are checked to stay below the configured worktree root before any
//! git call that creates or removes a worktree. Settlement keeps the tree
//! whenever a probe fails or reports anything but "identical to base".

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
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

        let run_dir = self.root.join(request.wf_run_id.as_str());
        create_private_dir(&self.root).await?;
        create_private_dir(&run_dir).await?;
        let root = self
            .canonical_root()
            .await
            .ok_or(WorktreeError::PathEscape)?;
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

        let execution = path.join(&relative);
        if tokio::fs::symlink_metadata(&execution).await.is_err() {
            // A subdirectory that is not tracked at the base commit. An empty
            // directory does not make the worktree dirty.
            tokio::fs::create_dir_all(&execution)
                .await
                .map_err(|_| WorktreeError::AddFailed {
                    detail: "could not create the workdir inside the worktree".into(),
                })?;
        }
        let canonical_execution = tokio::fs::canonicalize(&execution)
            .await
            .map_err(|_| WorktreeError::PathEscape)?;
        if !canonical_execution.starts_with(&path) {
            return Err(WorktreeError::PathEscape);
        }
        Ok(ProvisionedWorktree {
            path: canonical_execution,
            root: path,
            source_root,
            branch,
            base_sha,
        })
    }

    async fn settle(&self, worktree: &ProvisionedWorktree) -> WorktreeSettlement {
        let _guard = self.lock.lock().await;
        let unknown =
            |head: Option<String>| WorktreeSettlement::kept(WorktreeKeptReason::UnknownState, head);
        if !self.is_owned_worktree_path(&worktree.root).await || !is_object_id(&worktree.base_sha) {
            return unknown(None);
        }
        // The worktree must still belong to the source repository; otherwise
        // the probes below would describe some other repository.
        let (Some(worktree_common), Some(source_common)) = (
            self.common_dir(&worktree.root).await,
            self.common_dir(&worktree.source_root).await,
        ) else {
            return unknown(None);
        };
        if worktree_common != source_common {
            return unknown(None);
        }

        let head = self.commit_id(&worktree.root, "HEAD").await;
        let Some(status) = self
            .git(
                &worktree.root,
                [
                    "status",
                    "--porcelain=v1",
                    "--untracked-files=all",
                    "--ignored=matching",
                    "--ignore-submodules=none",
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

        // No `--force`: git refuses to remove a tree it considers unclean.
        let mut remove: Vec<OsString> = vec!["worktree".into(), "remove".into()];
        remove.push(worktree.root.clone().into_os_string());
        match self.git(&worktree.source_root, &remove).await {
            Some(output) if output.success => {}
            Some(_) | None => return WorktreeSettlement::cleanup_failed(Some(head)),
        }
        // Compare-and-delete: a branch that moved since the checks is kept.
        match self
            .git(
                &worktree.source_root,
                [
                    "update-ref",
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

fn is_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

async fn create_private_dir(path: &Path) -> Result<(), WorktreeError> {
    tokio::fs::create_dir_all(path)
        .await
        .map_err(|_| WorktreeError::PathEscape)?;
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
