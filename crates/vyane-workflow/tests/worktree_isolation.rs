//! Worktree isolation: validation, plan contract, the git provisioner and
//! engine wiring.
//!
//! Git tests create real repositories in temporary directories. Every helper
//! git call strips `GIT_*` variables so the host environment cannot leak in.

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tempfile::TempDir;
use vyane_core::{
    AdapterTransport, BoundTarget, CancellationToken, ErrorKind, GenParams, Harness,
    HarnessExecutionContext, HarnessJob, HarnessKind, HarnessOutcome, Ledger, ModelId, Protocol,
    ProviderId, Result as VyaneResult, RunQuery, RunRecord, Sandbox, SessionRecord, SessionStore,
    SessionUpdate, Target, VyaneError,
};
use vyane_kernel::{CapabilityManifest, Dispatcher, Executor, ExecutorFactory, IsolationStrength};
use vyane_workflow::{
    GitWorktreeProvisioner, JournalStepStatus, ProvisionedWorktree, TargetResolver, Workflow,
    WorkflowEngine, WorkflowError, WorkflowIsolation, WorkflowPlan, WorkflowPlanFeature,
    WorkflowPlanFeatureSupport, WorkflowRunId, WorkflowRunStatus, WorkflowSourceBundle,
    WorktreeDisposition, WorktreeError, WorktreeKeptReason, WorktreeProvisioner, WorktreeRequest,
    WorktreeSettlement, read_journal,
};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn bundle(toml: &str) -> WorkflowSourceBundle {
    WorkflowSourceBundle {
        workflow_toml: toml.to_string(),
        prompt_files: Vec::new(),
    }
}

fn isolated_workflow(step_extra: &str) -> String {
    format!(
        r#"[workflow]
name = "isolation"

[[step]]
id = "impl"
fan_out = ["alpha", "beta"]
prompt = "implement"
{step_extra}
"#
    )
}

/// Run git for test setup with a scrubbed `GIT_*` environment and a fixed
/// identity. Panics on failure.
fn git(dir: &Path, args: &[&str]) -> String {
    let mut command = Command::new("git");
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("GIT_") {
            command.env_remove(key);
        }
    }
    let output = command
        .args([
            "-c",
            "user.name=Vyane Test",
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "core.hooksPath=/dev/null",
        ])
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn git_ok(dir: &Path, args: &[&str]) -> bool {
    let mut command = Command::new("git");
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("GIT_") {
            command.env_remove(key);
        }
    }
    command
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap()
        .status
        .success()
}

struct Repo {
    _temp: TempDir,
    repo: PathBuf,
    worktrees: PathBuf,
}

impl Repo {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(repo.join("sub/dir")).unwrap();
        git(&repo, &["init", "-q"]);
        std::fs::write(repo.join("README.md"), "hello\n").unwrap();
        std::fs::write(repo.join("sub/dir/file.txt"), "nested\n").unwrap();
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "-q", "-m", "initial"]);
        let repo = std::fs::canonicalize(repo).unwrap();
        let worktrees = temp.path().join("data/worktrees");
        Self {
            _temp: temp,
            repo,
            worktrees,
        }
    }

    fn head(&self) -> String {
        git(&self.repo, &["rev-parse", "HEAD"])
    }

    fn provisioner(&self) -> GitWorktreeProvisioner {
        GitWorktreeProvisioner::new(&self.worktrees)
    }

    fn branch_exists(&self, branch: &str) -> bool {
        git_ok(
            &self.repo,
            &[
                "rev-parse",
                "--verify",
                "--quiet",
                &format!("refs/heads/{branch}"),
            ],
        )
    }

    fn worktree_count(&self) -> usize {
        git(&self.repo, &["worktree", "list", "--porcelain"])
            .lines()
            .filter(|line| line.starts_with("worktree "))
            .count()
    }
}

async fn provision(
    provisioner: &GitWorktreeProvisioner,
    run_id: &WorkflowRunId,
    source: &Path,
    base: &str,
) -> Result<ProvisionedWorktree, WorktreeError> {
    provisioner
        .provision(WorktreeRequest {
            wf_run_id: run_id,
            step_order: 0,
            target_index: 0,
            attempt: 0,
            source_workdir: source,
            base,
        })
        .await
}

// ---------------------------------------------------------------------------
// 1-2. Validation
// ---------------------------------------------------------------------------

struct AnyTarget;

impl TargetResolver for AnyTarget {
    fn resolve(&self, target: &str) -> VyaneResult<Vec<BoundTarget>> {
        Ok(vec![cli_target(target)])
    }
}

fn cli_target(model: &str) -> BoundTarget {
    BoundTarget {
        target: Target {
            provider: ProviderId::new("p"),
            protocol: Protocol::AnthropicMessages,
            harness: Some(HarnessKind::ClaudeCode),
            model: ModelId::new(model),
        },
        transport: AdapterTransport::CliWrap,
        endpoint: None,
        params: GenParams::default(),
    }
}

#[tokio::test]
async fn read_only_worktree_isolation_is_rejected_before_any_journal() {
    let directory = TempDir::new().unwrap();
    let journal_dir = directory.path().join("journals");
    let workflow = bundle(&isolated_workflow(r#"isolation = "worktree""#))
        .materialize()
        .unwrap();
    assert_eq!(workflow.steps[0].isolation, WorkflowIsolation::Worktree);
    assert_eq!(workflow.steps[0].sandbox, Sandbox::ReadOnly);

    let error = workflow.compile_plan().unwrap_err();
    assert!(error.is_validation_or_config());
    assert!(error.to_string().contains("read-only sandbox"), "{error}");
    let error = vyane_workflow::validate_workflow(&workflow, &BTreeMap::new(), &AnyTarget)
        .unwrap_err()
        .to_string();
    assert!(error.contains("read-only sandbox"), "{error}");

    let (engine, _) = engine_with(
        Arc::new(FakeProvisioner::new(directory.path())),
        &journal_dir,
    );
    let error = engine
        .run(&workflow, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("read-only sandbox"), "{error}");
    assert!(!journal_dir.exists(), "no journal directory may be created");
}

#[test]
fn unknown_isolation_bad_base_and_orphan_base_are_rejected_without_echo() {
    let error = bundle(&isolated_workflow(
        "sandbox = \"write\"\nisolation = \"container-CANARY\"",
    ))
    .materialize()
    .unwrap_err();
    let text = error.to_string();
    assert!(text.contains("unsupported `isolation` value"), "{text}");
    assert!(!text.contains("CANARY"), "{text}");

    for (base, reason) in [
        ("-x", "must not start with `-`"),
        ("", "is empty"),
        ("  ", "is empty"),
        ("HEAD\u{7}", "control character"),
    ] {
        let source = isolated_workflow(&format!(
            "sandbox = \"write\"\nisolation = \"worktree\"\nisolation_base = {}",
            toml_string(base)
        ));
        let workflow = bundle(&source).materialize().unwrap();
        let text = workflow.compile_plan().unwrap_err().to_string();
        assert!(text.contains(reason), "{base:?}: {text}");
        let validated =
            vyane_workflow::validate_workflow(&workflow, &BTreeMap::new(), &AnyTarget).unwrap_err();
        assert!(validated.to_string().contains(reason));
    }
    let long = "a".repeat(257);
    let source = isolated_workflow(&format!(
        "sandbox = \"write\"\nisolation = \"worktree\"\nisolation_base = \"{long}\""
    ));
    let text = bundle(&source)
        .materialize()
        .unwrap()
        .compile_plan()
        .unwrap_err()
        .to_string();
    assert!(text.contains("exceeds 256 bytes"), "{text}");
    assert!(!text.contains(&long));

    let orphan = isolated_workflow("sandbox = \"write\"\nisolation_base = \"main\"");
    let text = bundle(&orphan)
        .materialize()
        .unwrap()
        .compile_plan()
        .unwrap_err()
        .to_string();
    assert!(
        text.contains("`isolation_base` without `isolation = \"worktree\"`"),
        "{text}"
    );

    // `isolation = "none"` is the explicit default and stays valid.
    let none = isolated_workflow("isolation = \"none\"");
    let plan = bundle(&none).materialize().unwrap().compile_plan().unwrap();
    assert!(!plan.requests_isolation());
}

fn toml_string(value: &str) -> String {
    let mut out = String::from("\"");
    for character in value.chars() {
        if character.is_control() {
            out.push_str(&format!("\\u{:04X}", u32::from(character)));
        } else {
            out.push(character);
        }
    }
    out.push('"');
    out
}

// ---------------------------------------------------------------------------
// 3-4. Plan contract
// ---------------------------------------------------------------------------

/// Canonical bytes captured before isolation fields existed.
const BASELINE_SOURCE: &str = r#"[workflow]
name = "isolation-baseline"
max_concurrency = 2

[[step]]
id = "impl"
fan_out = ["alpha", "beta"]
sandbox = "write"
workdir = "sub/dir"
prompt = "implement {{vars.topic}}"

[[step]]
id = "review"
needs = ["impl"]
target = "gamma"
prompt = "{{steps.impl.outputs}}"
"#;
const BASELINE_PLAN: &str = r#"{"schema_version":1,"name":"isolation-baseline","max_concurrency":2,"source_sha256":"d00af52c13ab48d8a971e8c76448677a2d9a40f1584c5064307524b12ae7b46e","capability_manifest":{"maximum_sandbox":"write","uses_explicit_workdir":true,"uses_fan_out":true,"has_route_hints":false,"maximum_timeout":null},"steps":[{"order":0,"id":"impl","needs":[],"targets":{"kind":"fan_out","targets":["alpha","beta"]},"prompt_template":"implement {{vars.topic}}","workdir":"sub/dir","sandbox":"write","on_error":"abort","route":{"stage":null,"tier":null,"tags":[],"candidates":[],"allow_frontier":null,"effort":null}},{"order":1,"id":"review","needs":["impl"],"targets":{"kind":"single","target":"gamma"},"prompt_template":"{{steps.impl.outputs}}","sandbox":"read-only","on_error":"abort","route":{"stage":null,"tier":null,"tags":[],"candidates":[],"allow_frontier":null,"effort":null}}],"plan_sha256":"6dc7f1aa1fe6d6af308f3d9e733f1edc410f08128482afd776e8c5fc9347502a"}"#;

#[test]
fn plan_without_isolation_keeps_pre_isolation_bytes_and_digest() {
    let plan = bundle(BASELINE_SOURCE)
        .materialize()
        .unwrap()
        .compile_plan()
        .unwrap();
    let json = String::from_utf8(plan.to_canonical_json().unwrap()).unwrap();
    assert_eq!(json, BASELINE_PLAN);
    assert_eq!(
        plan.plan_sha256,
        "6dc7f1aa1fe6d6af308f3d9e733f1edc410f08128482afd776e8c5fc9347502a"
    );
    // Plans written before this change still load and verify.
    assert_eq!(
        WorkflowPlan::from_json(BASELINE_PLAN.as_bytes()).unwrap(),
        plan
    );
}

#[test]
fn isolated_plan_carries_fields_round_trips_and_old_shape_rejects_them() {
    let source = isolated_workflow(
        "sandbox = \"write\"\nisolation = \"worktree\"\nisolation_base = \"HEAD~1\"",
    );
    let plan = bundle(&source)
        .materialize()
        .unwrap()
        .compile_plan()
        .unwrap();
    let json = String::from_utf8(plan.to_canonical_json().unwrap()).unwrap();
    assert!(json.contains(r#""isolation":"worktree""#), "{json}");
    assert!(json.contains(r#""isolation_base":"HEAD~1""#), "{json}");
    assert!(plan.requests_isolation());
    let round_trip = WorkflowPlan::from_json(json.as_bytes()).unwrap();
    assert_eq!(round_trip, plan);
    assert_eq!(
        WorkflowPlan::feature_support(WorkflowPlanFeature::WorktreeIsolation),
        WorkflowPlanFeatureSupport::Supported
    );

    // A reader that predates these fields uses `deny_unknown_fields`; model
    // that shape with a strict struct and confirm it fails closed.
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct PreIsolationStep {
        order: u32,
        id: String,
        needs: Vec<String>,
        targets: serde_json::Value,
        prompt_template: String,
        #[serde(default)]
        system: Option<String>,
        #[serde(default)]
        workdir: Option<String>,
        sandbox: String,
        #[serde(default)]
        timeout: Option<serde_json::Value>,
        on_error: String,
        route: serde_json::Value,
    }
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    let step = value["steps"][0].clone();
    assert!(serde_json::from_value::<PreIsolationStep>(step).is_err());
    let baseline: serde_json::Value = serde_json::from_str(BASELINE_PLAN).unwrap();
    serde_json::from_value::<PreIsolationStep>(baseline["steps"][0].clone()).unwrap();

    // Tampering with the isolation field breaks the digest binding.
    let tampered = json.replace(
        r#""isolation_base":"HEAD~1""#,
        r#""isolation_base":"HEAD~2""#,
    );
    assert!(WorkflowPlan::from_json(tampered.as_bytes()).is_err());
}

// ---------------------------------------------------------------------------
// 5-10. Git provisioner
// ---------------------------------------------------------------------------

#[tokio::test]
async fn clean_worktree_is_removed_with_its_branch() {
    let repo = Repo::new();
    let provisioner = repo.provisioner();
    let run_id = WorkflowRunId::generate();
    let worktree = provision(&provisioner, &run_id, &repo.repo, "HEAD")
        .await
        .unwrap();
    assert_eq!(worktree.base_sha, repo.head());
    assert_eq!(worktree.branch, format!("vyane/wt/{run_id}/0-0-0"));
    assert!(
        worktree
            .root
            .starts_with(std::fs::canonicalize(&repo.worktrees).unwrap())
    );
    assert!(worktree.root.join("README.md").is_file());
    assert_eq!(repo.worktree_count(), 2);

    let settlement = provisioner.settle(&worktree).await;
    assert_eq!(settlement.disposition, WorktreeDisposition::Removed);
    assert_eq!(settlement.kept_reason, None);
    assert!(!worktree.root.exists());
    assert!(!repo.branch_exists(&worktree.branch));
    assert_eq!(repo.worktree_count(), 1, "only the main tree remains");
}

#[tokio::test]
async fn untracked_file_keeps_worktree_as_dirty() {
    let repo = Repo::new();
    let provisioner = repo.provisioner();
    let run_id = WorkflowRunId::generate();
    let worktree = provision(&provisioner, &run_id, &repo.repo, "HEAD")
        .await
        .unwrap();
    std::fs::write(worktree.path.join("new.txt"), "work").unwrap();

    let settlement = provisioner.settle(&worktree).await;
    assert_eq!(settlement.disposition, WorktreeDisposition::Kept);
    assert_eq!(settlement.kept_reason, Some(WorktreeKeptReason::Dirty));
    assert!(worktree.root.join("new.txt").is_file());
    assert!(repo.branch_exists(&worktree.branch));
}

#[tokio::test]
async fn ignored_file_keeps_worktree_as_dirty() {
    let repo = Repo::new();
    std::fs::write(repo.repo.join(".gitignore"), "build/\n").unwrap();
    git(&repo.repo, &["add", ".gitignore"]);
    git(&repo.repo, &["commit", "-q", "-m", "ignore build"]);
    let provisioner = repo.provisioner();
    let run_id = WorkflowRunId::generate();
    let worktree = provision(&provisioner, &run_id, &repo.repo, "HEAD")
        .await
        .unwrap();
    std::fs::create_dir(worktree.root.join("build")).unwrap();
    std::fs::write(worktree.root.join("build/out.bin"), "artifact").unwrap();

    let settlement = provisioner.settle(&worktree).await;
    assert_eq!(settlement.kept_reason, Some(WorktreeKeptReason::Dirty));
    assert!(worktree.root.join("build/out.bin").is_file());
}

/// Regression for the reference implementation, which only looked at
/// `status --porcelain` and deleted the branch of a committed, clean tree.
#[tokio::test]
async fn committed_work_in_a_clean_tree_is_kept() {
    let repo = Repo::new();
    let provisioner = repo.provisioner();
    let run_id = WorkflowRunId::generate();
    let worktree = provision(&provisioner, &run_id, &repo.repo, "HEAD")
        .await
        .unwrap();
    std::fs::write(worktree.root.join("feature.txt"), "done").unwrap();
    git(&worktree.root, &["add", "feature.txt"]);
    git(&worktree.root, &["commit", "-q", "-m", "agent work"]);
    assert!(
        git(&worktree.root, &["status", "--porcelain"]).is_empty(),
        "tree is clean after the commit"
    );

    let settlement = provisioner.settle(&worktree).await;
    assert_eq!(settlement.disposition, WorktreeDisposition::Kept);
    assert_eq!(settlement.kept_reason, Some(WorktreeKeptReason::Committed));
    let head = settlement.head_sha.clone().unwrap();
    assert_ne!(head, worktree.base_sha);
    assert_eq!(head, git(&worktree.root, &["rev-parse", "HEAD"]));
    assert!(worktree.root.join("feature.txt").is_file());
    assert!(repo.branch_exists(&worktree.branch));
}

/// A target that detaches HEAD before committing leaves the branch at the
/// base. Only the HEAD comparison sees that work; without it the tree would be
/// removed and the commits lost with the worktree's reflog.
#[tokio::test]
async fn detached_commit_with_branch_at_base_is_kept() {
    let repo = Repo::new();
    let provisioner = repo.provisioner();
    let run_id = WorkflowRunId::generate();
    let worktree = provision(&provisioner, &run_id, &repo.repo, "HEAD")
        .await
        .unwrap();
    git(&worktree.root, &["checkout", "-q", "--detach"]);
    std::fs::write(worktree.root.join("feature.txt"), "done").unwrap();
    git(&worktree.root, &["add", "feature.txt"]);
    git(&worktree.root, &["commit", "-q", "-m", "detached work"]);
    assert!(git(&worktree.root, &["status", "--porcelain"]).is_empty());
    assert_eq!(
        git(
            &repo.repo,
            &["rev-parse", &format!("refs/heads/{}", worktree.branch)]
        ),
        worktree.base_sha,
        "the branch itself never moved"
    );

    let settlement = provisioner.settle(&worktree).await;
    assert_eq!(settlement.disposition, WorktreeDisposition::Kept);
    assert_eq!(settlement.kept_reason, Some(WorktreeKeptReason::Committed));
    let head = settlement.head_sha.clone().unwrap();
    assert_ne!(head, worktree.base_sha);
    assert_eq!(head, git(&worktree.root, &["rev-parse", "HEAD"]));
    assert!(worktree.root.join("feature.txt").is_file());
    assert!(repo.branch_exists(&worktree.branch));
}

#[tokio::test]
async fn branch_moved_away_from_base_is_kept_even_with_head_at_base() {
    let repo = Repo::new();
    let base = repo.head();
    std::fs::write(repo.repo.join("second.txt"), "2").unwrap();
    git(&repo.repo, &["add", "second.txt"]);
    git(&repo.repo, &["commit", "-q", "-m", "second"]);
    let other = repo.head();
    let provisioner = repo.provisioner();
    let run_id = WorkflowRunId::generate();
    let worktree = provision(&provisioner, &run_id, &repo.repo, &base)
        .await
        .unwrap();
    assert_eq!(worktree.base_sha, base);
    // HEAD detaches at base; the branch itself points elsewhere.
    git(&worktree.root, &["checkout", "-q", "--detach"]);
    git(
        &repo.repo,
        &[
            "update-ref",
            &format!("refs/heads/{}", worktree.branch),
            &other,
        ],
    );

    let settlement = provisioner.settle(&worktree).await;
    assert_eq!(settlement.kept_reason, Some(WorktreeKeptReason::Committed));
    assert!(worktree.root.exists());
    assert!(repo.branch_exists(&worktree.branch));
}

/// The branch moves between the checks and the delete. The compare-and-delete
/// refuses, so the branch survives and the disposition says so.
#[tokio::test]
async fn branch_moved_during_settle_survives_compare_and_delete() {
    let repo = Repo::new();
    let base = repo.head();
    std::fs::write(repo.repo.join("second.txt"), "2").unwrap();
    git(&repo.repo, &["add", "second.txt"]);
    git(&repo.repo, &["commit", "-q", "-m", "second"]);
    let other = repo.head();
    let run_id = WorkflowRunId::generate();
    let branch = format!("vyane/wt/{run_id}/0-0-0");

    let tools = repo._temp.path().join("tools");
    std::fs::create_dir(&tools).unwrap();
    let wrapper = tools.join("racing-git");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nfor arg in \"$@\"; do\n  if [ \"$arg\" = \"update-ref\" ]; then\n    git update-ref refs/heads/{branch} {other} || exit 97\n  fi\ndone\nexec git \"$@\"\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let provisioner = repo.provisioner().with_git_program(&wrapper);

    let worktree = provision(&provisioner, &run_id, &repo.repo, &base)
        .await
        .unwrap();
    assert_eq!(worktree.branch, branch);
    let settlement = provisioner.settle(&worktree).await;
    assert_eq!(settlement.disposition, WorktreeDisposition::CleanupFailed);
    assert!(repo.branch_exists(&branch));
    assert_eq!(
        git(&repo.repo, &["rev-parse", &format!("refs/heads/{branch}")]),
        other
    );
}

/// A target turns its own branch into a symbolic ref to another branch that
/// sits at the base. Every check passes through the link, so deleting the
/// branch must not follow it: the other branch survives and the tree is kept.
#[tokio::test]
async fn symbolic_branch_never_deletes_the_branch_it_names() {
    let repo = Repo::new();
    git(&repo.repo, &["branch", "keepme"]);
    let provisioner = repo.provisioner();
    let run_id = WorkflowRunId::generate();
    let worktree = provision(&provisioner, &run_id, &repo.repo, "HEAD")
        .await
        .unwrap();
    git(
        &worktree.root,
        &[
            "symbolic-ref",
            &format!("refs/heads/{}", worktree.branch),
            "refs/heads/keepme",
        ],
    );
    assert_eq!(
        git(
            &repo.repo,
            &["rev-parse", &format!("refs/heads/{}", worktree.branch)]
        ),
        worktree.base_sha,
        "the link resolves to the base"
    );

    let settlement = provisioner.settle(&worktree).await;
    assert_eq!(settlement.disposition, WorktreeDisposition::Kept);
    assert_eq!(
        settlement.kept_reason,
        Some(WorktreeKeptReason::UnknownState)
    );
    assert!(
        repo.branch_exists("keepme"),
        "settle deleted another branch"
    );
    assert_eq!(
        git(&repo.repo, &["rev-parse", "refs/heads/keepme"]),
        worktree.base_sha
    );
    assert!(worktree.root.exists());
}

#[tokio::test]
async fn subdirectory_workdir_maps_into_the_worktree() {
    let repo = Repo::new();
    let provisioner = repo.provisioner();
    let run_id = WorkflowRunId::generate();
    let worktree = provision(&provisioner, &run_id, &repo.repo.join("sub/dir"), "HEAD")
        .await
        .unwrap();
    assert_eq!(worktree.path, worktree.root.join("sub/dir"));
    assert!(worktree.path.join("file.txt").is_file());
    assert_eq!(worktree.source_root, repo.repo);
    let settlement = provisioner.settle(&worktree).await;
    assert_eq!(settlement.disposition, WorktreeDisposition::Removed);
}

/// Commit a tree where `sub` is not a directory, then restore `sub/dir`.
/// Returns the commit in between.
fn commit_with_sub_replaced(repo: &Repo, replace: impl FnOnce(&Path)) -> String {
    git(&repo.repo, &["rm", "-r", "-q", "sub"]);
    replace(&repo.repo.join("sub"));
    git(&repo.repo, &["add", "sub"]);
    git(
        &repo.repo,
        &["commit", "-q", "-m", "sub is not a directory"],
    );
    let base = repo.head();
    git(&repo.repo, &["rm", "-q", "sub"]);
    std::fs::create_dir_all(repo.repo.join("sub/dir")).unwrap();
    std::fs::write(repo.repo.join("sub/dir/file.txt"), "nested\n").unwrap();
    git(&repo.repo, &["add", "sub"]);
    git(
        &repo.repo,
        &["commit", "-q", "-m", "sub is a directory again"],
    );
    base
}

fn assert_no_worktree_left(repo: &Repo, run_id: &WorkflowRunId) {
    let branch = format!("vyane/wt/{run_id}/0-0-0");
    assert!(!repo.branch_exists(&branch), "branch {branch} was left");
    assert_eq!(repo.worktree_count(), 1, "only the main tree remains");
    let leaf = repo.worktrees.join(run_id.as_str()).join("0-0-0");
    assert!(!leaf.exists(), "{leaf:?} was left");
}

/// The step workdir is a file at the isolation base. Provisioning fails after
/// `git worktree add`, and the new worktree and branch are removed again.
#[tokio::test]
async fn workdir_that_is_a_file_at_base_leaves_no_worktree() {
    let repo = Repo::new();
    let base = commit_with_sub_replaced(&repo, |sub| std::fs::write(sub, "a file\n").unwrap());
    let run_id = WorkflowRunId::generate();
    let error = provision(
        &repo.provisioner(),
        &run_id,
        &repo.repo.join("sub/dir"),
        &base,
    )
    .await
    .unwrap_err();
    assert_eq!(error.as_str(), "add_failed", "{error}");
    assert!(!matches!(error, WorktreeError::LeftInPlace { .. }));
    assert_no_worktree_left(&repo, &run_id);
}

/// The step workdir runs through a tracked symlink at the isolation base.
/// Nothing is created through it, and the new worktree is removed again.
#[tokio::test]
async fn workdir_through_a_symlink_at_base_creates_nothing_outside() {
    let repo = Repo::new();
    let outside = repo._temp.path().join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    let target = outside.clone();
    let base = commit_with_sub_replaced(&repo, move |sub| {
        std::os::unix::fs::symlink(&target, sub).unwrap();
    });
    let run_id = WorkflowRunId::generate();
    let error = provision(
        &repo.provisioner(),
        &run_id,
        &repo.repo.join("sub/dir"),
        &base,
    )
    .await
    .unwrap_err();
    assert_eq!(error.as_str(), "path_escape", "{error}");
    assert_eq!(
        std::fs::read_dir(&outside).unwrap().count(),
        0,
        "a directory was created outside the worktree"
    );
    assert_no_worktree_left(&repo, &run_id);
}

/// When the new worktree cannot be removed either, the error carries it so
/// the caller can record it.
#[tokio::test]
async fn unfinished_worktree_that_cannot_be_removed_is_reported() {
    let repo = Repo::new();
    let base = commit_with_sub_replaced(&repo, |sub| std::fs::write(sub, "a file\n").unwrap());
    let tools = repo._temp.path().join("tools");
    std::fs::create_dir(&tools).unwrap();
    let wrapper = tools.join("no-remove-git");
    std::fs::write(
        &wrapper,
        "#!/bin/sh\nfor arg in \"$@\"; do\n  if [ \"$arg\" = \"remove\" ]; then exit 1; fi\ndone\nexec git \"$@\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let run_id = WorkflowRunId::generate();
    let error = provision(
        &repo.provisioner().with_git_program(&wrapper),
        &run_id,
        &repo.repo.join("sub/dir"),
        &base,
    )
    .await
    .unwrap_err();
    assert_eq!(error.as_str(), "add_failed", "{error}");
    let WorktreeError::LeftInPlace {
        branch,
        root,
        base_sha,
        ..
    } = &error
    else {
        panic!("expected a left-in-place error, got {error:?}");
    };
    assert_eq!(base_sha, &base);
    assert!(root.exists());
    assert!(repo.branch_exists(branch));
    let entry = vyane_workflow::JournalWorktree::left_in_place("alpha", 0, &error).unwrap();
    assert_eq!(entry.disposition, WorktreeDisposition::CleanupFailed);
    assert_eq!(entry.branch, *branch);
    assert_eq!(PathBuf::from(&entry.path), *root);
}

#[tokio::test]
async fn provisioning_errors_are_closed_codes_and_never_dispatch_paths() {
    let repo = Repo::new();
    let provisioner = repo.provisioner();
    let run_id = WorkflowRunId::generate();

    let plain = repo._temp.path().join("plain");
    std::fs::create_dir(&plain).unwrap();
    let error = provision(&provisioner, &run_id, &plain, "HEAD")
        .await
        .unwrap_err();
    assert_eq!(error.as_str(), "not_a_repository");
    assert_eq!(error.kind(), ErrorKind::Io);

    let error = provision(&provisioner, &run_id, &repo.repo, "no-such-ref-CANARY")
        .await
        .unwrap_err();
    assert_eq!(error.as_str(), "base_unresolved");
    assert!(!error.to_string().contains("CANARY"));
    let error = provision(&provisioner, &run_id, &repo.repo, "--orphan")
        .await
        .unwrap_err();
    assert_eq!(error.as_str(), "base_unresolved");

    let first = provision(&provisioner, &run_id, &repo.repo, "HEAD")
        .await
        .unwrap();
    let error = provision(&provisioner, &run_id, &repo.repo, "HEAD")
        .await
        .unwrap_err();
    assert_eq!(error.as_str(), "path_exists");
    assert!(first.root.exists(), "an existing path is never touched");
}

/// A run-id directory that is a symlink out of the root is refused.
#[tokio::test]
async fn symlinked_run_directory_is_a_path_escape() {
    let repo = Repo::new();
    let outside = repo._temp.path().join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::create_dir_all(&repo.worktrees).unwrap();
    let run_id = WorkflowRunId::generate();
    std::os::unix::fs::symlink(&outside, repo.worktrees.join(run_id.as_str())).unwrap();
    let error = provision(&repo.provisioner(), &run_id, &repo.repo, "HEAD")
        .await
        .unwrap_err();
    assert_eq!(error.as_str(), "path_escape");
    assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 0);
    let mode = std::fs::metadata(&outside).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o755, "the symlink target's permissions changed");
}

fn set_mtime(path: &Path, stamp: &str) {
    let status = Command::new("touch")
        .args(["-m", "-t", stamp])
        .arg(path)
        .status()
        .unwrap();
    assert!(status.success());
}

/// A target plants a nested repository whose config defines a clean filter
/// and stages it as a gitlink. Settlement must not run git inside it: the
/// filter would run outside the target's sandbox with Vyane's privileges.
#[tokio::test]
async fn settle_never_runs_a_nested_repository_filter() {
    let repo = Repo::new();
    let provisioner = repo.provisioner();
    let run_id = WorkflowRunId::generate();
    let worktree = provision(&provisioner, &run_id, &repo.repo, "HEAD")
        .await
        .unwrap();
    let marker = repo._temp.path().join("filter-ran");
    let nested = worktree.root.join("nested");
    std::fs::create_dir(&nested).unwrap();
    git(&nested, &["init", "-q"]);
    std::fs::write(nested.join("f.txt"), "data\n").unwrap();
    git(&nested, &["add", "f.txt"]);
    git(&nested, &["commit", "-q", "-m", "nested"]);
    git(
        &nested,
        &[
            "config",
            "filter.x.clean",
            &format!("echo hit >> '{}'; cat", marker.display()),
        ],
    );
    std::fs::write(nested.join(".gitattributes"), "* filter=x\n").unwrap();
    git(&worktree.root, &["add", "nested"]);

    // The setup is live: a submodule-recursing status runs the filter.
    set_mtime(&nested.join("f.txt"), "200101010000");
    git(
        &worktree.root,
        &["status", "--porcelain", "--ignore-submodules=none"],
    );
    assert!(marker.exists(), "the planted filter did not run in setup");
    std::fs::remove_file(&marker).unwrap();
    set_mtime(&nested.join("f.txt"), "200201010000");

    let settlement = provisioner.settle(&worktree).await;
    assert!(
        !marker.exists(),
        "settlement ran a filter from the nested repository"
    );
    assert_eq!(settlement.disposition, WorktreeDisposition::Kept);
    assert_eq!(
        settlement.kept_reason,
        Some(WorktreeKeptReason::UnknownState)
    );
    assert!(worktree.root.exists());
    assert!(repo.branch_exists(&worktree.branch));
}

/// A `.git` file rewritten inside the tree no longer names the recorded git
/// directory, so the tree is kept without being probed through it.
#[tokio::test]
async fn rewritten_git_file_keeps_the_worktree() {
    let repo = Repo::new();
    let provisioner = repo.provisioner();
    let run_id = WorkflowRunId::generate();
    let worktree = provision(&provisioner, &run_id, &repo.repo, "HEAD")
        .await
        .unwrap();
    assert!(
        worktree
            .git_dir
            .starts_with(repo.repo.join(".git/worktrees")),
        "{:?}",
        worktree.git_dir
    );
    let original = std::fs::read(worktree.root.join(".git")).unwrap();
    std::fs::write(
        worktree.root.join(".git"),
        format!("gitdir: {}\n", worktree.root.join(".fake").display()),
    )
    .unwrap();

    let settlement = provisioner.settle(&worktree).await;
    assert_eq!(
        settlement.kept_reason,
        Some(WorktreeKeptReason::UnknownState)
    );
    assert!(worktree.root.exists());
    assert!(repo.branch_exists(&worktree.branch));

    // Restored, the same tree settles normally.
    std::fs::write(worktree.root.join(".git"), original).unwrap();
    let settlement = provisioner.settle(&worktree).await;
    assert_eq!(settlement.disposition, WorktreeDisposition::Removed);
}

/// Settle never runs a removing command on a path outside its root.
#[tokio::test]
async fn settle_keeps_a_worktree_outside_the_root() {
    let repo = Repo::new();
    let run_id = WorkflowRunId::generate();
    let worktree = provision(&repo.provisioner(), &run_id, &repo.repo, "HEAD")
        .await
        .unwrap();
    let elsewhere = GitWorktreeProvisioner::new(repo._temp.path().join("other-root"));
    std::fs::create_dir_all(repo._temp.path().join("other-root")).unwrap();
    let settlement = elsewhere.settle(&worktree).await;
    assert_eq!(
        settlement.kept_reason,
        Some(WorktreeKeptReason::UnknownState)
    );
    assert!(worktree.root.exists());
    assert!(repo.branch_exists(&worktree.branch));

    // A missing git binary is a failed probe: keep.
    let broken = repo.provisioner().with_git_program("/nonexistent/git");
    let settlement = broken.settle(&worktree).await;
    assert_eq!(
        settlement.kept_reason,
        Some(WorktreeKeptReason::UnknownState)
    );
    assert!(worktree.root.exists());
}

const HOSTILE_ENV_MARKER: &str = "VYANE_WORKTREE_HOSTILE_ENV_CHILD";

/// Re-run the inner test in a child process whose environment carries a
/// hostile `GIT_DIR` and `GIT_WORK_TREE`. The child also installs a failing
/// `post-checkout` hook and an fsmonitor helper in the source repository.
#[test]
fn hostile_git_environment_and_repository_hooks_are_ignored() {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "hostile_environment_child",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(HOSTILE_ENV_MARKER, "1")
        .env("GIT_DIR", "/nonexistent")
        .env("GIT_WORK_TREE", "/nonexistent")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed"),
        "child failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
#[ignore = "run by hostile_git_environment_and_repository_hooks_are_ignored"]
async fn hostile_environment_child() {
    assert_eq!(std::env::var(HOSTILE_ENV_MARKER).as_deref(), Ok("1"));
    assert_eq!(std::env::var("GIT_DIR").as_deref(), Ok("/nonexistent"));
    let repo = Repo::new();
    let marker = repo._temp.path().join("hook-ran");
    let hooks = repo.repo.join(".git/hooks");
    std::fs::create_dir_all(&hooks).unwrap();
    for hook in ["post-checkout", "reference-transaction"] {
        let path = hooks.join(hook);
        std::fs::write(
            &path,
            format!("#!/bin/sh\necho {hook} >> '{}'\nexit 1\n", marker.display()),
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let monitor = repo._temp.path().join("fsmonitor.sh");
    std::fs::write(
        &monitor,
        format!(
            "#!/bin/sh\necho fsmonitor >> '{}'\nexit 1\n",
            marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&monitor, std::fs::Permissions::from_mode(0o755)).unwrap();
    git(
        &repo.repo,
        &["config", "core.hooksPath", &hooks.display().to_string()],
    );
    git(
        &repo.repo,
        &["config", "core.fsmonitor", &monitor.display().to_string()],
    );

    let provisioner = repo.provisioner();
    let run_id = WorkflowRunId::generate();
    let worktree = provision(&provisioner, &run_id, &repo.repo, "HEAD")
        .await
        .unwrap();
    assert!(worktree.root.join("README.md").is_file());
    let settlement = provisioner.settle(&worktree).await;
    assert_eq!(settlement.disposition, WorktreeDisposition::Removed);
    assert!(
        !marker.exists(),
        "a repository hook or helper ran: {:?}",
        std::fs::read_to_string(&marker)
    );
}

// ---------------------------------------------------------------------------
// 11-13. Engine wiring with a fake provisioner
// ---------------------------------------------------------------------------

#[derive(Default)]
struct HarnessProbe {
    workdirs: Mutex<HashMap<String, PathBuf>>,
}

struct RecordingHarness {
    model: String,
    probe: Arc<HarnessProbe>,
    hang: bool,
}

#[async_trait]
impl Harness for RecordingHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::ClaudeCode
    }

    async fn available(&self) -> bool {
        true
    }

    async fn run_scoped(
        &self,
        job: HarnessJob,
        context: HarnessExecutionContext,
        cancel: CancellationToken,
    ) -> VyaneResult<HarnessOutcome> {
        // The kernel pins the admitted (isolated) workdir for write steps.
        assert!(context.pinned_workdir().is_some());
        self.run(job, cancel).await
    }

    async fn run(&self, job: HarnessJob, cancel: CancellationToken) -> VyaneResult<HarnessOutcome> {
        self.probe
            .workdirs
            .lock()
            .unwrap()
            .insert(self.model.clone(), job.workdir.clone().unwrap_or_default());
        if self.hang {
            cancel.cancelled().await;
            return Err(VyaneError::new(ErrorKind::Cancelled, "cancelled"));
        }
        Ok(HarnessOutcome {
            text: format!("{}-out", self.model),
            native_session_id: None,
            usage: None,
            exit_code: 0,
            duration: Duration::from_millis(1),
        })
    }
}

struct RecordingFactory {
    probe: Arc<HarnessProbe>,
    hang: bool,
}

impl ExecutorFactory for RecordingFactory {
    fn capability_manifest(&self, _target: &BoundTarget) -> CapabilityManifest {
        CapabilityManifest::local_workdir_editing(IsolationStrength::AdapterDelegated)
    }

    fn make(&self, target: &BoundTarget) -> VyaneResult<Executor> {
        Ok(Executor::Agent(Arc::new(RecordingHarness {
            model: target.target.model.as_str().to_string(),
            probe: Arc::clone(&self.probe),
            hang: self.hang,
        })))
    }
}

#[derive(Default)]
struct NullLedger;

#[async_trait]
impl Ledger for NullLedger {
    async fn append(&self, _record: &RunRecord) -> VyaneResult<()> {
        Ok(())
    }

    async fn query(&self, _query: RunQuery) -> VyaneResult<Vec<RunRecord>> {
        Ok(Vec::new())
    }
}

struct NullSessions;

#[async_trait]
impl SessionStore for NullSessions {
    async fn load(&self, _owner: &str, _id: &str) -> VyaneResult<Option<SessionRecord>> {
        Ok(None)
    }

    async fn save(&self, _owner: &str, _record: &SessionRecord) -> VyaneResult<()> {
        Ok(())
    }

    async fn apply_update(
        &self,
        _owner: &str,
        update: &SessionUpdate,
    ) -> VyaneResult<SessionRecord> {
        Ok(update.apply_to(None))
    }

    async fn list(&self, _owner: &str) -> VyaneResult<Vec<SessionRecord>> {
        Ok(Vec::new())
    }
}

/// Creates plain directories (no git) and records every call.
struct FakeProvisioner {
    root: PathBuf,
    requests: Mutex<Vec<(u32, u32, String)>>,
    settles: AtomicUsize,
    /// Journal file to inspect at settle time, when set.
    journal: Mutex<Option<PathBuf>>,
    seen_at_settle: Mutex<Vec<String>>,
    /// Journal file to replace with a directory while provisioning, so the
    /// engine's next journal write fails.
    break_journal: Mutex<Option<PathBuf>>,
    /// Target index whose worktree is created but reported as left in place.
    leave_in_place: Mutex<Option<u32>>,
}

impl FakeProvisioner {
    fn new(root: &Path) -> Self {
        Self {
            root: root.join("fake-worktrees"),
            requests: Mutex::new(Vec::new()),
            settles: AtomicUsize::new(0),
            journal: Mutex::new(None),
            seen_at_settle: Mutex::new(Vec::new()),
            break_journal: Mutex::new(None),
            leave_in_place: Mutex::new(None),
        }
    }
}

#[async_trait]
impl WorktreeProvisioner for FakeProvisioner {
    async fn provision(
        &self,
        request: WorktreeRequest<'_>,
    ) -> Result<ProvisionedWorktree, WorktreeError> {
        self.requests.lock().unwrap().push((
            request.target_index,
            request.attempt,
            request.base.to_string(),
        ));
        let root = self.root.join(request.leaf());
        if root.exists() {
            return Err(WorktreeError::PathExists);
        }
        std::fs::create_dir_all(&root).unwrap();
        if let Some(journal) = self.break_journal.lock().unwrap().take() {
            std::fs::remove_file(&journal).unwrap();
            std::fs::create_dir(&journal).unwrap();
        }
        if *self.leave_in_place.lock().unwrap() == Some(request.target_index) {
            return Err(WorktreeError::LeftInPlace {
                cause: Box::new(WorktreeError::PathEscape),
                branch: request.branch(),
                root,
                base_sha: "b".repeat(40),
            });
        }
        Ok(ProvisionedWorktree {
            path: root.clone(),
            git_dir: root.join(".git"),
            root,
            source_root: self.root.clone(),
            branch: request.branch(),
            base_sha: "b".repeat(40),
        })
    }

    async fn settle(&self, _worktree: &ProvisionedWorktree) -> WorktreeSettlement {
        self.settles.fetch_add(1, Ordering::SeqCst);
        if let Some(path) = self.journal.lock().unwrap().clone() {
            let journal: serde_json::Value =
                serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            for step in journal["steps"].as_object().unwrap().values() {
                for worktree in step["worktrees"].as_array().into_iter().flatten() {
                    self.seen_at_settle
                        .lock()
                        .unwrap()
                        .push(worktree["disposition"].as_str().unwrap().to_string());
                }
            }
        }
        WorktreeSettlement::kept(WorktreeKeptReason::Dirty, Some("c".repeat(40)))
    }
}

struct CliResolver;

impl TargetResolver for CliResolver {
    fn resolve(&self, target: &str) -> VyaneResult<Vec<BoundTarget>> {
        Ok(vec![cli_target(target)])
    }
}

fn engine_with(
    provisioner: Arc<FakeProvisioner>,
    journal_dir: &Path,
) -> (WorkflowEngine, Arc<HarnessProbe>) {
    engine_with_options(provisioner, journal_dir, false)
}

fn engine_with_options(
    provisioner: Arc<FakeProvisioner>,
    journal_dir: &Path,
    hang: bool,
) -> (WorkflowEngine, Arc<HarnessProbe>) {
    let probe = Arc::new(HarnessProbe::default());
    let dispatcher = Arc::new(Dispatcher::new(
        Arc::new(RecordingFactory {
            probe: Arc::clone(&probe),
            hang,
        }),
        Arc::new(NullLedger),
        Arc::new(NullSessions),
    ));
    let engine = WorkflowEngine::new(dispatcher, Arc::new(CliResolver), journal_dir.into())
        .with_worktree_provisioner(provisioner);
    (engine, probe)
}

fn write_fan_out() -> Workflow {
    bundle(&isolated_workflow(
        "sandbox = \"write\"\nisolation = \"worktree\"",
    ))
    .materialize()
    .unwrap()
}

#[tokio::test]
async fn fan_out_targets_get_distinct_workdirs_in_input_order() {
    let directory = TempDir::new().unwrap();
    let journal_dir = directory.path().join("journals");
    let provisioner = Arc::new(FakeProvisioner::new(directory.path()));
    let (engine, probe) = engine_with(Arc::clone(&provisioner), &journal_dir);

    let outcome = engine
        .run(&write_fan_out(), BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        outcome.status,
        WorkflowRunStatus::Completed,
        "{:?}",
        outcome.journal.steps
    );

    let workdirs = probe.workdirs.lock().unwrap().clone();
    let alpha = workdirs["alpha"].clone();
    let beta = workdirs["beta"].clone();
    assert_ne!(alpha, beta);
    assert!(alpha.ends_with("0-0-0"), "{alpha:?}");
    assert!(beta.ends_with("0-1-0"), "{beta:?}");

    let step = &outcome.journal.steps["impl"];
    let outputs = step.outputs.as_ref().unwrap();
    assert_eq!(
        outputs
            .iter()
            .map(|o| o.target.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "beta"]
    );
    assert_eq!(outputs[0].output.as_deref(), Some("alpha-out"));
    assert_eq!(outputs[1].output.as_deref(), Some("beta-out"));
    assert_eq!(step.worktrees.len(), 2);
    assert_eq!(step.worktrees[0].target, "alpha");
    assert_eq!(step.worktrees[1].target, "beta");
    for worktree in &step.worktrees {
        assert_eq!(worktree.disposition, WorktreeDisposition::Kept);
        assert_eq!(worktree.kept_reason, Some(WorktreeKeptReason::Dirty));
        assert_eq!(worktree.head_sha.as_deref(), Some("c".repeat(40).as_str()));
    }
    // The second target reuses the base the first one resolved.
    let requests = provisioner.requests.lock().unwrap().clone();
    assert_eq!(requests[0], (0, 0, "HEAD".to_string()));
    assert_eq!(requests[1], (1, 0, "b".repeat(40)));
    assert_eq!(provisioner.settles.load(Ordering::SeqCst), 2);

    // The persisted journal matches the returned one.
    let persisted = read_journal(&journal_dir, outcome.wf_run_id.as_str()).unwrap();
    assert_eq!(persisted.steps["impl"].worktrees, step.worktrees);
}

#[tokio::test]
async fn engine_without_provisioner_rejects_isolation_before_journal() {
    let directory = TempDir::new().unwrap();
    let journal_dir = directory.path().join("journals");
    let dispatcher = Arc::new(Dispatcher::new(
        Arc::new(RecordingFactory {
            probe: Arc::default(),
            hang: false,
        }),
        Arc::new(NullLedger),
        Arc::new(NullSessions),
    ));
    let engine = WorkflowEngine::new(dispatcher, Arc::new(CliResolver), journal_dir.clone());
    let error = engine
        .run(&write_fan_out(), BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(error, WorkflowError::IsolationUnsupported { .. }));
    assert_eq!(error.as_str(), "isolation_unsupported");
    assert!(error.is_validation_or_config());
    assert!(!journal_dir.exists());
}

#[tokio::test]
async fn cancelled_dispatch_still_settles_once_after_active_was_recorded() {
    let directory = TempDir::new().unwrap();
    let journal_dir = directory.path().join("journals");
    let provisioner = Arc::new(FakeProvisioner::new(directory.path()));
    let (engine, probe) = engine_with_options(Arc::clone(&provisioner), &journal_dir, true);
    let workflow = bundle(
        r#"[workflow]
name = "cancel"

[[step]]
id = "impl"
target = "alpha"
sandbox = "write"
isolation = "worktree"
prompt = "hang"
"#,
    )
    .materialize()
    .unwrap();
    let run_id = WorkflowRunId::generate();
    *provisioner.journal.lock().unwrap() =
        Some(journal_dir.join(format!("{}.json", run_id.as_str())));

    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    let watched = Arc::clone(&probe);
    tokio::spawn(async move {
        while watched.workdirs.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        trigger.cancel();
    });
    let outcome = engine
        .run_with_id(run_id, &workflow, BTreeMap::new(), cancel)
        .await
        .unwrap();
    assert_eq!(outcome.status, WorkflowRunStatus::Cancelled);
    assert_eq!(provisioner.settles.load(Ordering::SeqCst), 1);
    assert_eq!(
        provisioner.seen_at_settle.lock().unwrap().as_slice(),
        ["active"],
        "the journal recorded the worktree before settlement"
    );
    let step = &outcome.journal.steps["impl"];
    assert_eq!(step.status, JournalStepStatus::Cancelled);
    assert_eq!(step.worktrees.len(), 1);
    assert_eq!(step.worktrees[0].disposition, WorktreeDisposition::Kept);
}

#[tokio::test]
async fn resume_after_crash_keeps_active_entry_and_uses_a_new_attempt() {
    let directory = TempDir::new().unwrap();
    let journal_dir = directory.path().join("journals");
    let provisioner = Arc::new(FakeProvisioner::new(directory.path()));
    let (engine, probe) = engine_with(Arc::clone(&provisioner), &journal_dir);
    let workflow = write_fan_out();

    // Run once, then rewrite the journal into the state a crash right after
    // provisioning would leave behind: step running, entries `active`.
    let first = engine
        .run(&workflow, BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();
    let mut crashed = first.journal.clone();
    crashed.status = WorkflowRunStatus::Running;
    let step = crashed.steps.get_mut("impl").unwrap();
    step.status = JournalStepStatus::Running;
    step.outputs = None;
    step.run_ids.clear();
    for worktree in &mut step.worktrees {
        worktree.disposition = WorktreeDisposition::Active;
        worktree.kept_reason = None;
        worktree.head_sha = None;
    }
    std::fs::write(
        journal_dir.join(format!("{}.json", first.wf_run_id.as_str())),
        serde_json::to_vec_pretty(&crashed).unwrap(),
    )
    .unwrap();
    let old_paths = crashed.steps["impl"]
        .worktrees
        .iter()
        .map(|worktree| PathBuf::from(&worktree.path))
        .collect::<Vec<_>>();
    std::fs::write(old_paths[0].join("precious.txt"), "keep me").unwrap();
    probe.workdirs.lock().unwrap().clear();

    let resumed = engine
        .resume(
            first.wf_run_id.as_str(),
            &workflow,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(resumed.status, WorkflowRunStatus::Completed);
    let worktrees = &resumed.journal.steps["impl"].worktrees;
    assert_eq!(worktrees.len(), 4);
    for old in &worktrees[..2] {
        assert_eq!(old.attempt, 0);
        assert_eq!(old.disposition, WorktreeDisposition::Kept);
        assert_eq!(old.kept_reason, Some(WorktreeKeptReason::UnknownState));
    }
    for new in &worktrees[2..] {
        assert_eq!(new.attempt, 1);
        assert!(new.branch.ends_with("-1"), "{}", new.branch);
        assert!(!old_paths.contains(&PathBuf::from(&new.path)));
    }
    assert_eq!(
        std::fs::read_to_string(old_paths[0].join("precious.txt")).unwrap(),
        "keep me"
    );
    let workdirs = probe.workdirs.lock().unwrap().clone();
    assert!(workdirs["alpha"].ends_with("0-0-1"));
    assert!(workdirs["beta"].ends_with("0-1-1"));
    // Only the two new worktrees were settled in the resumed run.
    assert_eq!(provisioner.settles.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn provisioning_failure_fails_only_that_target_without_dispatch() {
    let directory = TempDir::new().unwrap();
    let journal_dir = directory.path().join("journals");
    let provisioner = Arc::new(FakeProvisioner::new(directory.path()));
    // Occupy every attempt slot of the second target.
    for attempt in 0..=8 {
        std::fs::create_dir_all(provisioner.root.join(format!("0-1-{attempt}"))).unwrap();
    }
    let (engine, probe) = engine_with(Arc::clone(&provisioner), &journal_dir);
    let outcome = engine
        .run(&write_fan_out(), BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();
    // Fan-out semantics: one success keeps the step successful.
    assert_eq!(
        outcome.status,
        WorkflowRunStatus::Completed,
        "{:?}",
        outcome.journal.steps
    );
    let step = &outcome.journal.steps["impl"];
    let outputs = step.outputs.as_ref().unwrap();
    assert!(outputs[0].ok);
    assert!(!outputs[1].ok);
    assert_eq!(step.worktrees.len(), 1);
    let workdirs = probe.workdirs.lock().unwrap().clone();
    assert!(workdirs.contains_key("alpha"));
    assert!(
        !workdirs.contains_key("beta"),
        "beta must not be dispatched"
    );
}

/// A worktree that provisioning created but could not remove again is
/// recorded as `cleanup_failed`, and its target is not dispatched.
#[tokio::test]
async fn worktree_left_in_place_by_provisioning_is_journaled() {
    let directory = TempDir::new().unwrap();
    let journal_dir = directory.path().join("journals");
    let provisioner = Arc::new(FakeProvisioner::new(directory.path()));
    *provisioner.leave_in_place.lock().unwrap() = Some(1);
    let (engine, probe) = engine_with(Arc::clone(&provisioner), &journal_dir);
    let outcome = engine
        .run(&write_fan_out(), BTreeMap::new(), CancellationToken::new())
        .await
        .unwrap();
    let step = &outcome.journal.steps["impl"];
    let outputs = step.outputs.as_ref().unwrap();
    assert!(outputs[0].ok);
    assert!(!outputs[1].ok);
    assert_eq!(step.worktrees.len(), 2);
    let left = &step.worktrees[1];
    assert_eq!(left.target, "beta");
    assert_eq!(left.attempt, 0);
    assert_eq!(left.disposition, WorktreeDisposition::CleanupFailed);
    assert!(left.branch.ends_with("/0-1-0"), "{}", left.branch);
    assert!(PathBuf::from(&left.path).ends_with("0-1-0"));
    assert!(!probe.workdirs.lock().unwrap().contains_key("beta"));
    // Only the dispatched worktree is settled; the left one is reported.
    assert_eq!(provisioner.settles.load(Ordering::SeqCst), 1);
    let persisted = read_journal(&journal_dir, outcome.wf_run_id.as_str()).unwrap();
    assert_eq!(persisted.steps["impl"].worktrees, step.worktrees);
}

/// The journal write that records new worktrees fails. Nothing was
/// dispatched, so the worktrees are settled before the error is returned.
#[tokio::test]
async fn failed_journal_write_after_provisioning_settles_the_new_worktrees() {
    let directory = TempDir::new().unwrap();
    let journal_dir = directory.path().join("journals");
    let provisioner = Arc::new(FakeProvisioner::new(directory.path()));
    let run_id = WorkflowRunId::generate();
    *provisioner.break_journal.lock().unwrap() =
        Some(journal_dir.join(format!("{}.json", run_id.as_str())));
    let (engine, probe) = engine_with(Arc::clone(&provisioner), &journal_dir);
    let error = engine
        .run_with_id(
            run_id,
            &write_fan_out(),
            BTreeMap::new(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, WorkflowError::WriteJournal { .. }),
        "{error}"
    );
    assert_eq!(provisioner.requests.lock().unwrap().len(), 2);
    assert_eq!(provisioner.settles.load(Ordering::SeqCst), 2);
    assert!(probe.workdirs.lock().unwrap().is_empty(), "nothing ran");
}
