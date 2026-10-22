//! Pure planner for an opt-in worktree isolation request.
//!
//! [`WorktreePlanner::plan`] records a branch and a path. It does not start
//! git or any other process, delete files, or contact a network. Cleanup of a
//! dirty tree is refused. A dirty tree that is left untouched is recorded for
//! salvage instead of being treated as clean.

use std::path::{Component, Path, PathBuf};

use thiserror::Error;

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
        if part.is_empty() || part == "." || part == ".." {
            return Err(WorktreePlanError::InvalidPath);
        }
        saw_normal = true;
    }
    if !saw_normal {
        return Err(WorktreePlanError::InvalidPath);
    }
    Ok(())
}
