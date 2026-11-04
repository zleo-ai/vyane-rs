//! Opt-in git worktree isolation for workflow steps.
//!
//! [`WorktreePlanner::plan`] is a pure planner. It records a branch and a
//! path and never starts git or any other process, deletes files, or contacts
//! a network. Cleanup of a dirty tree is refused. A dirty tree that is left
//! untouched is recorded for salvage instead of being treated as clean.
//!
//! Execution goes through the [`WorktreeProvisioner`] port. The workflow
//! engine only talks to that trait; [`GitWorktreeProvisioner`] is the one
//! concrete implementation that spawns `git`. Settlement follows one rule:
//! when in doubt, keep. A failed probe never leads to a removal.

use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::journal::WorkflowRunId;

mod git;

pub use git::GitWorktreeProvisioner;

/// Maximum `isolation_base` length, in UTF-8 bytes.
pub const WORKTREE_BASE_MAX_BYTES: usize = 256;
/// Base commit-ish used when a step does not set `isolation_base`.
pub const WORKTREE_DEFAULT_BASE: &str = "HEAD";
/// Bounded diagnostic detail kept from a failed git command, in bytes.
pub const WORKTREE_ERROR_DETAIL_MAX_BYTES: usize = 512;

/// Per-step isolation mode. The default runs in the shared workdir.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowIsolation {
    /// Run in the step's workdir, shared with other targets (default).
    #[default]
    None,
    /// Run each target in its own git worktree.
    Worktree,
}

impl WorkflowIsolation {
    /// Stable snake_case token matching the serde rename.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Worktree => "worktree",
        }
    }

    /// Whether this is the default, shared-workdir mode.
    #[must_use]
    pub const fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Parse the TOML spelling. Unknown values are not echoed by callers.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "worktree" => Some(Self::Worktree),
            _ => None,
        }
    }
}

impl std::fmt::Display for WorkflowIsolation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Check one `isolation_base` spelling without echoing it.
///
/// The value is passed to git only after `--end-of-options`; this check also
/// keeps it from ever looking like an option or carrying control bytes.
pub fn validate_isolation_base(base: &str) -> Result<(), &'static str> {
    if base.trim().is_empty() {
        return Err("is empty");
    }
    if base.len() > WORKTREE_BASE_MAX_BYTES {
        return Err("exceeds 256 bytes");
    }
    if base.starts_with('-') {
        return Err("must not start with `-`");
    }
    if base.chars().any(char::is_control) {
        return Err("contains a control character");
    }
    Ok(())
}

/// Static isolation problems for one step, shared by every validator.
pub(crate) fn isolation_problems(
    step_id: &str,
    sandbox: vyane_core::Sandbox,
    isolation: WorkflowIsolation,
    isolation_base: Option<&str>,
) -> Vec<String> {
    let mut problems = Vec::new();
    if isolation == WorkflowIsolation::Worktree && sandbox == vyane_core::Sandbox::ReadOnly {
        problems.push(format!(
            "step `{step_id}` sets `isolation = \"worktree\"` with a read-only sandbox; worktree isolation requires `sandbox = \"write\"` or `\"full\"`"
        ));
    }
    if let Some(base) = isolation_base {
        if isolation != WorkflowIsolation::Worktree {
            problems.push(format!(
                "step `{step_id}` sets `isolation_base` without `isolation = \"worktree\"`"
            ));
        }
        if let Err(reason) = validate_isolation_base(base) {
            problems.push(format!("step `{step_id}` `isolation_base` {reason}"));
        }
    }
    problems
}

/// One target's worktree request. The provisioner derives branch and path.
#[derive(Debug, Clone, Copy)]
pub struct WorktreeRequest<'a> {
    pub wf_run_id: &'a WorkflowRunId,
    pub step_order: u32,
    pub target_index: u32,
    pub attempt: u32,
    /// Directory the target would have run in without isolation.
    pub source_workdir: &'a Path,
    /// Git commit-ish to branch from. Validated by [`validate_isolation_base`].
    pub base: &'a str,
}

impl WorktreeRequest<'_> {
    /// Branch name for this request: `vyane/wt/<run>/<order>-<index>-<attempt>`.
    #[must_use]
    pub fn branch(&self) -> String {
        format!("vyane/wt/{}/{}", self.wf_run_id, self.leaf())
    }

    /// Final path component for this request.
    #[must_use]
    pub fn leaf(&self) -> String {
        format!("{}-{}-{}", self.step_order, self.target_index, self.attempt)
    }
}

/// A created worktree. Holding this value does not keep anything alive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionedWorktree {
    /// Execution workdir: worktree root plus the source subdirectory.
    pub path: PathBuf,
    /// Worktree root.
    pub root: PathBuf,
    /// Top level of the source repository.
    pub source_root: PathBuf,
    /// Per-worktree git directory, recorded when the worktree was created.
    /// Settlement names it explicitly instead of trusting the `.git` file
    /// inside the tree, which the target may have rewritten.
    pub git_dir: PathBuf,
    pub branch: String,
    /// Full commit id the branch was created at.
    pub base_sha: String,
}

/// Journal record of one isolated target attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalWorktree {
    pub target: String,
    pub attempt: u32,
    pub branch: String,
    /// Worktree root path.
    pub path: String,
    pub base_sha: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_sha: Option<String>,
    pub disposition: WorktreeDisposition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kept_reason: Option<WorktreeKeptReason>,
}

impl JournalWorktree {
    /// A freshly created worktree that has not been settled yet.
    #[must_use]
    pub fn active(target: impl Into<String>, attempt: u32, worktree: &ProvisionedWorktree) -> Self {
        Self {
            target: target.into(),
            attempt,
            branch: worktree.branch.clone(),
            path: worktree.root.to_string_lossy().into_owned(),
            base_sha: worktree.base_sha.clone(),
            head_sha: None,
            disposition: WorktreeDisposition::Active,
            kept_reason: None,
        }
    }

    /// Record a worktree that provisioning created but could neither finish
    /// nor remove again. `None` for every other provisioning error.
    #[must_use]
    pub fn left_in_place(
        target: impl Into<String>,
        attempt: u32,
        error: &WorktreeError,
    ) -> Option<Self> {
        let WorktreeError::LeftInPlace {
            branch,
            root,
            base_sha,
            ..
        } = error
        else {
            return None;
        };
        Some(Self {
            target: target.into(),
            attempt,
            branch: branch.clone(),
            path: root.to_string_lossy().into_owned(),
            base_sha: base_sha.clone(),
            head_sha: None,
            disposition: WorktreeDisposition::CleanupFailed,
            kept_reason: None,
        })
    }

    /// Record a settlement outcome.
    pub fn apply(&mut self, settlement: &WorktreeSettlement) {
        self.disposition = settlement.disposition;
        self.kept_reason = settlement.kept_reason;
        if settlement.head_sha.is_some() {
            self.head_sha = settlement.head_sha.clone();
        }
    }

    /// Whether the worktree or its branch may still exist on disk.
    #[must_use]
    pub fn may_hold_work(&self) -> bool {
        self.disposition != WorktreeDisposition::Removed
    }

    /// Treat an unsettled entry as kept. Used when a run is resumed after a
    /// crash: an `active` entry is never removed automatically.
    pub fn keep_if_active(&mut self) {
        if self.disposition == WorktreeDisposition::Active {
            self.disposition = WorktreeDisposition::Kept;
            self.kept_reason = Some(WorktreeKeptReason::UnknownState);
        }
    }
}

/// Lifecycle of one worktree in the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorktreeDisposition {
    /// Created; not settled yet.
    Active,
    /// No changes: directory and branch removed.
    Removed,
    /// Left in place with its branch.
    Kept,
    /// Cleanup started but did not finish; something is left in place.
    CleanupFailed,
}

impl WorktreeDisposition {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Removed => "removed",
            Self::Kept => "kept",
            Self::CleanupFailed => "cleanup_failed",
        }
    }
}

/// Why a worktree was kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorktreeKeptReason {
    /// Uncommitted, untracked or ignored files exist.
    Dirty,
    /// HEAD or the branch moved past the base commit.
    Committed,
    /// A probe failed, so the state is not known.
    UnknownState,
}

impl WorktreeKeptReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dirty => "dirty",
            Self::Committed => "committed",
            Self::UnknownState => "unknown_state",
        }
    }
}

/// Result of [`WorktreeProvisioner::settle`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeSettlement {
    pub disposition: WorktreeDisposition,
    pub kept_reason: Option<WorktreeKeptReason>,
    pub head_sha: Option<String>,
}

impl WorktreeSettlement {
    #[must_use]
    pub fn removed(head_sha: impl Into<String>) -> Self {
        Self {
            disposition: WorktreeDisposition::Removed,
            kept_reason: None,
            head_sha: Some(head_sha.into()),
        }
    }

    #[must_use]
    pub fn kept(reason: WorktreeKeptReason, head_sha: Option<String>) -> Self {
        Self {
            disposition: WorktreeDisposition::Kept,
            kept_reason: Some(reason),
            head_sha,
        }
    }

    #[must_use]
    pub fn cleanup_failed(head_sha: Option<String>) -> Self {
        Self {
            disposition: WorktreeDisposition::CleanupFailed,
            kept_reason: None,
            head_sha,
        }
    }
}

/// Why a worktree could not be created. The target is not dispatched.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WorktreeError {
    #[error("workdir is not inside a git work tree")]
    NotARepository,
    #[error("isolation base does not name a commit")]
    BaseUnresolved,
    /// The worktree path, or the branch generated for it, already exists.
    /// The caller moves on to the next attempt.
    #[error("worktree path already exists")]
    PathExists,
    #[error("git worktree add failed: {detail}")]
    AddFailed { detail: String },
    #[error("worktree path is outside the worktree root")]
    PathEscape,
    /// Provisioning created a worktree or its branch, a step failed with
    /// `cause`, and removing what was created did not finish either. The
    /// caller must record the worktree; it is never left unreported.
    #[error("{cause}; the new worktree could not be removed and was left in place")]
    LeftInPlace {
        cause: Box<WorktreeError>,
        branch: String,
        root: PathBuf,
        base_sha: String,
    },
}

impl WorktreeError {
    /// Stable snake_case kind token. A worktree left in place reports the
    /// code of the failure that stopped provisioning.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotARepository => "not_a_repository",
            Self::BaseUnresolved => "base_unresolved",
            Self::PathExists => "path_exists",
            Self::AddFailed { .. } => "add_failed",
            Self::PathEscape => "path_escape",
            Self::LeftInPlace { cause, .. } => cause.as_str(),
        }
    }

    /// Core error class. Every provisioning failure is a local I/O failure.
    #[must_use]
    pub const fn kind(&self) -> vyane_core::ErrorKind {
        vyane_core::ErrorKind::Io
    }
}

/// Port for creating and settling per-target worktrees.
#[async_trait]
pub trait WorktreeProvisioner: Send + Sync {
    /// Create one worktree. A failure means nothing should be dispatched.
    async fn provision(
        &self,
        request: WorktreeRequest<'_>,
    ) -> Result<ProvisionedWorktree, WorktreeError>;

    /// Remove a worktree only when it provably holds no work.
    ///
    /// Must be "when in doubt, keep": any failed probe returns
    /// [`WorktreeKeptReason::UnknownState`].
    async fn settle(&self, worktree: &ProvisionedWorktree) -> WorktreeSettlement;
}

/// Bound a git diagnostic to [`WORKTREE_ERROR_DETAIL_MAX_BYTES`] and replace
/// control characters, so it is safe to print on one line.
#[must_use]
pub fn sanitize_detail(raw: &[u8]) -> String {
    let text = String::from_utf8_lossy(raw);
    let mut out = String::new();
    for character in text.trim().chars() {
        let character = if character.is_control() {
            ' '
        } else {
            character
        };
        if out.len() + character.len_utf8() > WORKTREE_ERROR_DETAIL_MAX_BYTES {
            break;
        }
        out.push(character);
    }
    out
}

/// Maximum recorded branch name length, in UTF-8 bytes.
pub const WORKTREE_BRANCH_MAX_BYTES: usize = 1_024;
/// Maximum recorded worktree path length, in UTF-8 bytes.
pub const WORKTREE_PATH_MAX_BYTES: usize = 4_096;

/// Namespace for worktree planning.
///
/// This type has no process handle, deleter, or client. Planning is a pure
/// function of the request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorktreePlanner;

/// Whether isolation was explicitly requested.
///
/// The default is to decline. A plan is produced only for [`Self::Isolate`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum IsolationChoice {
    /// Do not record a branch or path.
    #[default]
    Decline,
    /// Record the requested branch and path.
    Isolate(IsolateRequest),
}

/// One explicit isolation request.
///
/// `tree` and `cleanup` are caller observations and intent. The planner does
/// not inspect a repository and does not carry out `cleanup`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsolateRequest {
    pub branch: String,
    pub path: PathBuf,
    pub tree: ObservedTree,
    pub cleanup: CleanupRequest,
}

/// Caller-observed tree state. This is not a repository status probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservedTree {
    Clean,
    Dirty,
}

/// What the caller asked to do with the observed tree.
///
/// Neither variant removes files. [`Self::Clean`] on a dirty tree is refused.
/// [`Self::Clean`] on an already clean tree still only records the plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupRequest {
    /// Leave the tree untouched.
    Leave,
    /// Ask for a clean. Refused when the tree is dirty.
    Clean,
}

/// Recorded isolation plan.
///
/// Holding this value does not create a worktree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreePlan {
    pub branch: String,
    pub path: PathBuf,
    pub disposition: TreeDisposition,
}

/// How the observed tree was recorded. Neither variant removes files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeDisposition {
    /// The tree was clean. The plan only records the branch and path.
    Recorded,
    /// The tree was dirty and is left in place.
    Salvaged,
}

/// Why a worktree plan was not recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum WorktreePlanError {
    #[error("worktree isolation was not explicitly requested")]
    NotOptedIn,
    #[error("refusing to clean a dirty worktree")]
    DirtyTreeCleanupRefused,
    #[error("worktree branch is empty")]
    EmptyBranch,
    #[error("worktree branch cannot be recorded")]
    InvalidBranch,
    #[error("worktree path is empty")]
    EmptyPath,
    #[error("worktree path cannot be recorded")]
    InvalidPath,
}

impl WorktreePlanError {
    /// Stable snake_case kind token. Request text stays out of the token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotOptedIn => "not_opted_in",
            Self::DirtyTreeCleanupRefused => "dirty_tree_cleanup_refused",
            Self::EmptyBranch => "empty_branch",
            Self::InvalidBranch => "invalid_branch",
            Self::EmptyPath => "empty_path",
            Self::InvalidPath => "invalid_path",
        }
    }
}

impl WorktreePlanner {
    /// Record `choice` or refuse it.
    ///
    /// A dirty tree combined with [`CleanupRequest::Clean`] is refused before
    /// any branch or path is accepted, including when those fields are
    /// otherwise invalid. Declining isolation records nothing. No path through
    /// this function starts a process, deletes a file, or uses the network.
    pub fn plan(choice: &IsolationChoice) -> Result<WorktreePlan, WorktreePlanError> {
        let IsolationChoice::Isolate(request) = choice else {
            return Err(WorktreePlanError::NotOptedIn);
        };
        if request.tree == ObservedTree::Dirty && request.cleanup == CleanupRequest::Clean {
            return Err(WorktreePlanError::DirtyTreeCleanupRefused);
        }
        validate_branch(&request.branch)?;
        validate_path(&request.path)?;
        let disposition = match request.tree {
            ObservedTree::Clean => TreeDisposition::Recorded,
            ObservedTree::Dirty => TreeDisposition::Salvaged,
        };
        Ok(WorktreePlan {
            branch: request.branch.clone(),
            path: request.path.clone(),
            disposition,
        })
    }
}

fn validate_branch(branch: &str) -> Result<(), WorktreePlanError> {
    if branch.is_empty() {
        return Err(WorktreePlanError::EmptyBranch);
    }
    let component_rejected = branch.split('/').any(|component| {
        component.is_empty()
            || component == "."
            || component == ".."
            || component.starts_with('.')
            || component.ends_with(".lock")
    });
    if branch.len() > WORKTREE_BRANCH_MAX_BYTES
        || branch == "@"
        || branch.starts_with('-')
        || branch.starts_with('/')
        || branch.ends_with('/')
        || branch.ends_with('.')
        || branch.contains("//")
        || branch.contains("..")
        || branch.contains("@{")
        || branch.contains('\\')
        || branch.contains(':')
        || branch.contains('~')
        || branch.contains('^')
        || branch.contains('?')
        || branch.contains('*')
        || branch.contains('[')
        || branch.contains(']')
        || branch
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
        || component_rejected
    {
        return Err(WorktreePlanError::InvalidBranch);
    }
    Ok(())
}

fn validate_path(path: &Path) -> Result<(), WorktreePlanError> {
    let Some(text) = path.to_str() else {
        return Err(WorktreePlanError::InvalidPath);
    };
    if text.is_empty() {
        return Err(WorktreePlanError::EmptyPath);
    }
    // `Path::components` drops `.` and repeated separators. Check the recorded
    // spelling itself so those forms are not stored as if they were normal.
    if text.len() > WORKTREE_PATH_MAX_BYTES
        || text.contains('\0')
        || text.contains('\\')
        || text.chars().any(|character| character.is_control())
        || path
            .components()
            .any(|component| matches!(component, Component::Prefix(_)))
    {
        return Err(WorktreePlanError::InvalidPath);
    }
    let mut parts = text.split('/');
    if text.starts_with('/') {
        let Some("") = parts.next() else {
            return Err(WorktreePlanError::InvalidPath);
        };
    }
    let mut saw_normal = false;
    for part in parts {
        if part.is_empty() || part.trim().is_empty() || part == "." || part == ".." {
            return Err(WorktreePlanError::InvalidPath);
        }
        saw_normal = true;
    }
    if !saw_normal {
        return Err(WorktreePlanError::InvalidPath);
    }
    Ok(())
}
