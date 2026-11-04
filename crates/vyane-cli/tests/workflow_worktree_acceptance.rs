//! End-to-end acceptance for `isolation = "worktree"` workflow steps.
//!
//! A scripted fake `claude` harness writes one file into its working
//! directory. With isolation each fan-out target gets its own git worktree,
//! so both files survive in kept worktrees and the source tree stays clean.

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;

use assert_cmd::Command;
#[cfg(target_os = "linux")]
use serde_json::Value;
use tempfile::TempDir;

const VYANE_BIN: &str = env!("CARGO_BIN_EXE_vyane");

const CONFIG: &str = r#"
[providers.native]
base_url = "https://unused.invalid"
auth_style = "x_api_key"
protocol = "anthropic_messages"
default_model = "test-model"

[profiles.alpha]
provider = "native"
protocol = "anthropic_messages"
harness = "claude-code"
model = "test-model"

[profiles.beta]
provider = "native"
protocol = "anthropic_messages"
harness = "claude-code"
model = "test-model"
"#;

const ISOLATED_WORKFLOW: &str = r#"
[workflow]
name = "isolated-fan-out"

[[step]]
id = "impl"
fan_out = ["alpha", "beta"]
sandbox = "write"
isolation = "worktree"
prompt = "implement"
"#;

struct Fixture {
    /// Owns every other path; dropped with the fixture.
    _root: TempDir,
    repo: PathBuf,
    data: PathBuf,
    config: PathBuf,
    workflow: PathBuf,
    bin: PathBuf,
}

fn git(dir: &Path, args: &[&str]) -> String {
    let mut command = StdCommand::new("git");
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
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

impl Fixture {
    fn new(workflow: &str) -> Self {
        let root = common::private_tempdir();
        let repo = root.path().join("repo");
        fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "-q"]);
        fs::write(repo.join("README.md"), "hello\n").unwrap();
        git(&repo, &["add", "README.md"]);
        git(&repo, &["commit", "-q", "-m", "initial"]);
        let repo = fs::canonicalize(repo).unwrap();

        let config = root.path().join("config.toml");
        fs::write(&config, CONFIG).unwrap();
        let workflow_path = root.path().join("workflow.toml");
        fs::write(&workflow_path, workflow).unwrap();

        let bin = root.path().join("bin");
        fs::create_dir(&bin).unwrap();
        let claude = bin.join("claude");
        fs::write(
            &claude,
            r#"#!/bin/sh
printf 'agent output\n' > "$PWD/agent-$$.txt"
printf '%s\n' '{"result":"wrote a file","session_id":"worktree-acceptance"}'
"#,
        )
        .unwrap();
        fs::set_permissions(&claude, fs::Permissions::from_mode(0o755)).unwrap();

        Self {
            data: root.path().join("data"),
            _root: root,
            repo,
            config,
            workflow: workflow_path,
            bin,
        }
    }

    fn vyane(&self) -> Command {
        let mut paths = vec![self.bin.clone()];
        if let Some(existing) = std::env::var_os("PATH") {
            paths.extend(std::env::split_paths(&existing));
        }
        let mut command = Command::new(VYANE_BIN);
        command
            .env("PATH", std::env::join_paths(paths).unwrap())
            .env("VYANE_DATA_DIR", &self.data)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .current_dir(&self.repo)
            .arg("--config")
            .arg(&self.config);
        command
    }
}

// Writable pinned workdirs are currently supported only on Linux. Elsewhere a
// `sandbox = "write"` step is rejected at admission
// (`workdir_pinning_unavailable`) before the fake harness runs, so the runs
// that expect kept worktrees are Linux-only.
#[cfg(target_os = "linux")]
#[test]
fn isolated_fan_out_keeps_one_dirty_worktree_per_target() {
    let fixture = Fixture::new(ISOLATED_WORKFLOW);
    let output = fixture
        .vyane()
        .args(["workflow", "run"])
        .arg(&fixture.workflow)
        .arg("--json")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let outcome: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(outcome["status"], "completed");
    let step = &outcome["journal"]["steps"]["impl"];
    let worktrees = step["worktrees"].as_array().unwrap();
    assert_eq!(worktrees.len(), 2, "{step}");
    let worktrees_root = fs::canonicalize(fixture.data.join("worktrees")).unwrap();
    let head = git(&fixture.repo, &["rev-parse", "HEAD"]);
    for (worktree, target) in worktrees.iter().zip(["alpha", "beta"]) {
        assert_eq!(worktree["target"], target);
        assert_eq!(worktree["disposition"], "kept");
        assert_eq!(worktree["kept_reason"], "dirty");
        assert_eq!(worktree["base_sha"], head.as_str());
        let path = PathBuf::from(worktree["path"].as_str().unwrap());
        assert!(path.starts_with(&worktrees_root), "{path:?}");
        let written = fs::read_dir(&path)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("agent-"))
            .count();
        assert_eq!(written, 1, "one agent file in {path:?}");
        let branch = worktree["branch"].as_str().unwrap();
        assert!(branch.starts_with("vyane/wt/"), "{branch}");
        assert_eq!(
            git(
                &fixture.repo,
                &["rev-parse", &format!("refs/heads/{branch}")]
            ),
            head
        );
    }
    // The shared source tree was never written to.
    assert!(
        git(&fixture.repo, &["status", "--porcelain"]).is_empty(),
        "source tree must stay clean"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn human_run_output_names_kept_branches_and_plan_shows_isolation() {
    let fixture = Fixture::new(ISOLATED_WORKFLOW);
    let output = fixture
        .vyane()
        .args(["workflow", "run"])
        .arg(&fixture.workflow)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{stdout}");
    let kept = stdout
        .lines()
        .filter(|line| line.contains("worktree kept: vyane/wt/") && line.contains("dirty"))
        .count();
    assert_eq!(kept, 2, "{stdout}");
    assert!(stdout.contains("impl/alpha: "), "{stdout}");

    let plan = fixture
        .vyane()
        .args(["workflow", "plan"])
        .arg(&fixture.workflow)
        .output()
        .unwrap();
    let plan_text = String::from_utf8_lossy(&plan.stdout);
    assert!(plan.status.success(), "{plan_text}");
    assert!(plan_text.contains("| isolation=worktree"), "{plan_text}");
    let plan_json = fixture
        .vyane()
        .args(["workflow", "plan", "--json"])
        .arg(&fixture.workflow)
        .output()
        .unwrap();
    let plan_json: Value = serde_json::from_slice(&plan_json.stdout).unwrap();
    assert_eq!(plan_json["steps"][0]["isolation"], "worktree");
}

#[test]
fn read_only_isolation_fails_before_any_journal_or_worktree() {
    let fixture = Fixture::new(&ISOLATED_WORKFLOW.replace("sandbox = \"write\"\n", ""));
    let output = fixture
        .vyane()
        .args(["workflow", "run"])
        .arg(&fixture.workflow)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("read-only sandbox"), "{stderr}");
    assert!(!fixture.data.join("workflows").exists());
    assert!(!fixture.data.join("worktrees").exists());
}

#[test]
fn daemon_submit_rejects_isolation_before_contacting_the_daemon() {
    let fixture = Fixture::new(ISOLATED_WORKFLOW);
    let output = fixture
        .vyane()
        .args(["workflow", "submit"])
        .arg(&fixture.workflow)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("isolation_unsupported"), "{stderr}");
    // No daemon runs here; reaching the connect step would report this.
    assert!(!stderr.contains("definitely not submitted"), "{stderr}");
    assert!(!stderr.contains("submission id"), "{stderr}");
    assert!(!fixture.data.join("tasks.sqlite3").exists());
    assert!(!fixture.data.join("workflows").exists());
}
