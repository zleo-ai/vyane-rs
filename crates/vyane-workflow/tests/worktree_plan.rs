use std::ffi::OsString;
use std::path::PathBuf;

use vyane_workflow::worktree::{
    CleanupRequest, IsolateRequest, IsolationChoice, ObservedTree, TreeDisposition,
    WORKTREE_BRANCH_MAX_BYTES, WORKTREE_PATH_MAX_BYTES, WorktreePlanError, WorktreePlanner,
};

fn isolate(
    branch: &str,
    path: impl Into<PathBuf>,
    tree: ObservedTree,
    cleanup: CleanupRequest,
) -> IsolationChoice {
    IsolationChoice::Isolate(IsolateRequest {
        branch: branch.to_string(),
        path: path.into(),
        tree,
        cleanup,
    })
}

#[test]
fn opt_in_records_branch_and_path() {
    let choice = isolate(
        "dianmo/workflow-worktree-plan",
        "worktrees/job",
        ObservedTree::Clean,
        CleanupRequest::Leave,
    );
    let plan = WorktreePlanner::plan(&choice).expect("opt-in clean tree");
    assert_eq!(plan.branch, "dianmo/workflow-worktree-plan");
    assert_eq!(plan.path, PathBuf::from("worktrees/job"));
    assert_eq!(plan.disposition, TreeDisposition::Recorded);

    let asked_to_clean = isolate(
        "dianmo/workflow-worktree-plan",
        "worktrees/job",
        ObservedTree::Clean,
        CleanupRequest::Clean,
    );
    assert_eq!(
        WorktreePlanner::plan(&asked_to_clean).expect("clean tree is not deleted"),
        plan
    );
    assert_eq!(
        WorktreePlanner::plan(&choice).expect("repeat"),
        WorktreePlanner::plan(&choice).expect("repeat again")
    );
}

#[test]
fn dirty_tree_without_cleanup_is_salvaged() {
    let plan = WorktreePlanner::plan(&isolate(
        "topic/salvage",
        "/worktrees/job",
        ObservedTree::Dirty,
        CleanupRequest::Leave,
    ))
    .expect("dirty tree left in place");
    assert_eq!(plan.branch, "topic/salvage");
    assert_eq!(plan.path, PathBuf::from("/worktrees/job"));
    assert_eq!(plan.disposition, TreeDisposition::Salvaged);
}

#[test]
fn dirty_tree_cleanup_is_refused_before_other_validation() {
    let error = WorktreePlanner::plan(&isolate(
        "../not-a-branch",
        "foo/../../etc",
        ObservedTree::Dirty,
        CleanupRequest::Clean,
    ))
    .expect_err("dirty cleanup");
    assert_eq!(error, WorktreePlanError::DirtyTreeCleanupRefused);
    assert_eq!(error.as_str(), "dirty_tree_cleanup_refused");
    assert_eq!(error.to_string(), "refusing to clean a dirty worktree");
}

#[test]
fn declined_isolation_records_nothing() {
    let error = WorktreePlanner::plan(&IsolationChoice::Decline).expect_err("declined");
    assert_eq!(error, WorktreePlanError::NotOptedIn);
    assert_eq!(error.as_str(), "not_opted_in");
    assert_eq!(
        WorktreePlanner::plan(&IsolationChoice::default()).expect_err("default declines"),
        WorktreePlanError::NotOptedIn
    );
}

#[test]
fn branch_and_path_bounds_are_exact() {
    let branch = "a".repeat(WORKTREE_BRANCH_MAX_BYTES);
    let path = "b".repeat(WORKTREE_PATH_MAX_BYTES);
    let plan = WorktreePlanner::plan(&isolate(
        &branch,
        path.as_str(),
        ObservedTree::Clean,
        CleanupRequest::Leave,
    ))
    .expect("limits are inclusive");
    assert_eq!(plan.branch, branch);
    assert_eq!(plan.path, PathBuf::from(&path));

    let too_long_branch = "a".repeat(WORKTREE_BRANCH_MAX_BYTES + 1);
    assert_eq!(
        WorktreePlanner::plan(&isolate(
            &too_long_branch,
            "worktrees/job",
            ObservedTree::Clean,
            CleanupRequest::Leave,
        ))
        .expect_err("branch limit"),
        WorktreePlanError::InvalidBranch
    );
    let too_long_path = "b".repeat(WORKTREE_PATH_MAX_BYTES + 1);
    assert_eq!(
        WorktreePlanner::plan(&isolate(
            "topic/limit",
            too_long_path.as_str(),
            ObservedTree::Clean,
            CleanupRequest::Leave,
        ))
        .expect_err("path limit"),
        WorktreePlanError::InvalidPath
    );
}

#[test]
fn empty_branch_and_path_are_distinct_from_invalid_spelling() {
    assert_eq!(
        WorktreePlanner::plan(&isolate(
            "",
            "worktrees/job",
            ObservedTree::Clean,
            CleanupRequest::Leave,
        ))
        .expect_err("empty branch"),
        WorktreePlanError::EmptyBranch
    );
    assert_eq!(
        WorktreePlanner::plan(&isolate(
            "topic/job",
            "",
            ObservedTree::Clean,
            CleanupRequest::Leave,
        ))
        .expect_err("empty path"),
        WorktreePlanError::EmptyPath
    );
}

#[test]
fn unsafe_branch_spellings_are_rejected() {
    let rejected = [
        " ",
        "-rf",
        "feature/../secret",
        "feature//x",
        "feature.lock",
        "feature.",
        "a/.",
        ".hidden",
        "foo bar",
        "foo@{1}",
        "has\nnewline",
        "trailing/",
        "/leading",
        ".",
        "..",
        "@",
        "feat\\win",
        "refs:heads",
        "feat~1",
        "feat^2",
        "feat?",
        "feat*",
        "feat[0]",
    ];
    for branch in rejected {
        assert_eq!(
            WorktreePlanner::plan(&isolate(
                branch,
                "worktrees/job",
                ObservedTree::Clean,
                CleanupRequest::Leave,
            ))
            .expect_err(branch),
            WorktreePlanError::InvalidBranch,
            "{branch}"
        );
    }
}

#[test]
fn unsafe_paths_are_rejected_and_spaces_are_kept() {
    let rejected = [
        "..",
        "../out",
        "foo/../../etc",
        "/worktrees/../etc",
        "foo/./bar",
        "foo//bar",
        "foo/bar/",
        "/",
        ".",
        "foo\\bar",
        "foo\nbar",
        "foo\0bar",
        " ",
        "   ",
        "foo/ /bar",
        "/ ",
    ];
    for path in rejected {
        assert_eq!(
            WorktreePlanner::plan(&isolate(
                "topic/job",
                path,
                ObservedTree::Clean,
                CleanupRequest::Leave,
            ))
            .expect_err(path),
            WorktreePlanError::InvalidPath,
            "{path}"
        );
    }

    let spaced = WorktreePlanner::plan(&isolate(
        "topic/job",
        "work trees/job",
        ObservedTree::Clean,
        CleanupRequest::Leave,
    ))
    .expect("spaces are recorded, not trimmed");
    assert_eq!(spaced.path, PathBuf::from("work trees/job"));
}

#[test]
fn unicode_branch_is_recorded_unchanged() {
    let plan = WorktreePlanner::plan(&isolate(
        "主题/隔离",
        "worktrees/job",
        ObservedTree::Clean,
        CleanupRequest::Leave,
    ))
    .expect("unicode branch");
    assert_eq!(plan.branch, "主题/隔离");
}

#[cfg(unix)]
#[test]
fn non_utf8_path_is_rejected() {
    use std::os::unix::ffi::OsStringExt;

    let path = PathBuf::from(OsString::from_vec(vec![0xff, b'a']));
    assert_eq!(
        WorktreePlanner::plan(&isolate(
            "topic/job",
            path,
            ObservedTree::Clean,
            CleanupRequest::Leave,
        ))
        .expect_err("non-utf8 path"),
        WorktreePlanError::InvalidPath
    );
}

#[test]
fn planner_source_does_not_name_a_process_deleter_or_network_client() {
    let source = include_str!("../src/worktree.rs");
    for forbidden in [
        "std::process",
        "Command::",
        "remove_file",
        "remove_dir",
        "std::fs",
        "std::net",
        "TcpStream",
        "reqwest",
    ] {
        assert!(
            !source.contains(forbidden),
            "{forbidden} must not appear in the planner"
        );
    }
}
