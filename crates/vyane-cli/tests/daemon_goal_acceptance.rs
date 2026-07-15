//! End-to-end acceptance coverage for opt-in resident goal pursuit.

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::{Duration, Instant};

use assert_cmd::Command;
use rusqlite::Connection;
use serde_json::Value;
use tempfile::TempDir;

const VYANE_BIN: &str = env!("CARGO_BIN_EXE_vyane");

fn vyane() -> Command {
    Command::new(VYANE_BIN)
}

fn write_config(directory: &TempDir) -> PathBuf {
    let config = directory.path().join("config.toml");
    fs::write(
        &config,
        r#"
        [providers.native]
        base_url = "https://unused.invalid"
        auth_style = "x_api_key"
        protocol = "anthropic_messages"
        default_model = "test-model"

        [profiles.builder]
        provider = "native"
        protocol = "anthropic_messages"
        harness = "claude-code"
        model = "test-model"
        "#,
    )
    .unwrap();
    config
}

fn write_fake_harness(directory: &TempDir) -> PathBuf {
    let bin = directory.path().join("bin");
    fs::create_dir(&bin).unwrap();
    write_success_harness(&bin);
    bin
}

fn write_success_harness(bin: &Path) {
    let claude = bin.join("claude");
    fs::write(
        &claude,
        r#"#!/bin/sh
: > "$PWD/done.txt"
printf '%s\n' '{"result":"segment complete","session_id":"daemon-goal-segment"}'
"#,
    )
    .unwrap();
    fs::set_permissions(&claude, fs::Permissions::from_mode(0o755)).unwrap();
}

fn write_blocking_harness(bin: &Path) {
    let claude = bin.join("claude");
    fs::write(
        &claude,
        r#"#!/bin/sh
: > "$PWD/segment-started.txt"
while :; do sleep 1; done
"#,
    )
    .unwrap();
    fs::set_permissions(&claude, fs::Permissions::from_mode(0o755)).unwrap();
}

fn inherited_path(bin: &Path) -> std::ffi::OsString {
    let mut paths = vec![bin.to_path_buf()];
    if let Some(existing) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&existing));
    }
    std::env::join_paths(paths).unwrap()
}

fn create_goal(data_dir: &Path, workdir: &Path) {
    create_goal_named(data_dir, workdir, "resident-goal", "2");
}

fn create_goal_named(data_dir: &Path, workdir: &Path, id: &str, priority: &str) {
    vyane()
        .env("VYANE_DATA_DIR", data_dir)
        .args([
            "goal",
            "create",
            "--json",
            "--id",
            id,
            "--title",
            id,
            "--priority",
            priority,
            "--acceptance",
            "custom:cmd:/bin/test -f done.txt",
        ])
        .current_dir(workdir)
        .assert()
        .success();
}

struct DaemonGuard {
    data_dir: PathBuf,
    running: bool,
}

impl DaemonGuard {
    fn start_plain(data_dir: &Path, config: &Path) -> Self {
        vyane()
            .env("VYANE_DATA_DIR", data_dir)
            .arg("--config")
            .arg(config)
            .args(["daemon", "start", "--addr", "127.0.0.1:0"])
            .timeout(Duration::from_secs(30))
            .assert()
            .success();
        Self {
            data_dir: data_dir.to_path_buf(),
            running: true,
        }
    }

    fn start(data_dir: &Path, config: &Path, workdir: &Path, bin: &Path) -> Self {
        let sandbox = if cfg!(target_os = "linux") {
            "write"
        } else {
            "read-only"
        };
        vyane()
            .env("VYANE_DATA_DIR", data_dir)
            .env("PATH", inherited_path(bin))
            .arg("--config")
            .arg(config)
            .args([
                "daemon",
                "start",
                "--addr",
                "127.0.0.1:0",
                "--goal-auto-pursue",
                "--goal-target",
                "builder",
                "--goal-workdir",
            ])
            .arg(workdir)
            .args([
                "--goal-sandbox",
                sandbox,
                "--goal-overall-timeout-seconds",
                "20",
                "--goal-segment-timeout-seconds",
                "5",
                "--goal-verifier-timeout-seconds",
                "2",
                "--goal-max-segments",
                "2",
                "--goal-poll-millis",
                "50",
            ])
            .timeout(Duration::from_secs(30))
            .assert()
            .success();
        Self {
            data_dir: data_dir.to_path_buf(),
            running: true,
        }
    }

    fn stop(&mut self) -> Output {
        let output = stop_daemon(&self.data_dir);
        if output.status.success() {
            self.running = false;
        }
        output
    }
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        if self.running {
            let _ = stop_daemon(&self.data_dir);
        }
    }
}

fn stop_daemon(data_dir: &Path) -> Output {
    vyane()
        .env("VYANE_DATA_DIR", data_dir)
        .args(["daemon", "stop"])
        .timeout(Duration::from_secs(30))
        .output()
        .unwrap()
}

fn goal_detail(data_dir: &Path) -> Option<Value> {
    goal_detail_named(data_dir, "resident-goal")
}

fn goal_detail_named(data_dir: &Path, id: &str) -> Option<Value> {
    let output = vyane()
        .env("VYANE_DATA_DIR", data_dir)
        .args(["goal", "get", "--json", id])
        .output()
        .unwrap();
    if !output.status.success() {
        return None;
    }
    serde_json::from_slice(&output.stdout).ok()
}

fn wait_for_completion(data_dir: &Path, budget: Duration) -> Value {
    wait_for_completion_named(data_dir, "resident-goal", budget)
}

fn wait_for_completion_named(data_dir: &Path, id: &str, budget: Duration) -> Value {
    let deadline = Instant::now() + budget;
    let mut last = "unavailable".to_string();
    loop {
        if let Some(detail) = goal_detail_named(data_dir, id) {
            last = detail["goal"]["status"]
                .as_str()
                .unwrap_or("unknown")
                .to_string();
            if last == "completed" {
                return detail;
            }
        }
        assert!(
            Instant::now() < deadline,
            "resident goal did not complete; last status = {last}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_running_checkpoint(data_dir: &Path, workdir: &Path, budget: Duration) -> Value {
    let deadline = Instant::now() + budget;
    loop {
        if let Some(detail) = goal_detail(data_dir) {
            if detail["goal"]["status"] == "in_progress"
                && detail["pursuit_checkpoint"]["status"] == "running"
                && detail["pursuit_checkpoint"]["segments_started"] == 1
                && workdir.join("segment-started.txt").is_file()
            {
                return detail;
            }
        }
        assert!(
            Instant::now() < deadline,
            "resident goal did not start its first segment"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn detached_daemon_forwards_opt_in_and_pursues_a_queued_goal() {
    let fixture = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    let workdir = TempDir::new().unwrap();
    let config = write_config(&fixture);
    let bin = write_fake_harness(&fixture);
    create_goal(data.path(), workdir.path());

    let mut daemon = DaemonGuard::start(data.path(), &config, workdir.path(), &bin);
    let detail = wait_for_completion(data.path(), Duration::from_secs(15));

    assert_eq!(detail["goal"]["claimed_by"], Value::Null);
    assert_eq!(detail["goal"]["claim_generation"], 1);
    assert_eq!(detail["pursuit_checkpoint"]["status"], "achieved");
    assert_eq!(detail["pursuit_checkpoint"]["segments_started"], 1);
    assert!(workdir.path().join("done.txt").is_file());

    let stopped = daemon.stop();
    assert!(
        stopped.status.success(),
        "daemon stop failed: {}",
        String::from_utf8_lossy(&stopped.stderr)
    );
}

#[test]
fn daemon_restart_adopts_running_checkpoint_without_implicit_pause() {
    let fixture = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    let workdir = TempDir::new().unwrap();
    let config = write_config(&fixture);
    let bin = write_fake_harness(&fixture);
    write_blocking_harness(&bin);
    create_goal(data.path(), workdir.path());

    let mut first = DaemonGuard::start(data.path(), &config, workdir.path(), &bin);
    let before = wait_for_running_checkpoint(data.path(), workdir.path(), Duration::from_secs(10));
    assert_eq!(before["goal"]["claim_generation"], 1);
    assert!(first.stop().status.success());

    let interrupted = goal_detail(data.path()).unwrap();
    assert_eq!(interrupted["goal"]["status"], "in_progress");
    assert_eq!(interrupted["goal"]["claim_generation"], 1);
    assert_eq!(interrupted["pursuit_checkpoint"]["status"], "running");
    assert_eq!(interrupted["pursuit_checkpoint"]["segments_started"], 1);
    assert_eq!(interrupted["pursuit_checkpoint"]["segments_completed"], 0);

    write_success_harness(&bin);
    let mut second = DaemonGuard::start(data.path(), &config, workdir.path(), &bin);
    let completed = wait_for_completion(data.path(), Duration::from_secs(15));
    assert_eq!(completed["goal"]["claim_generation"], 1);
    assert_eq!(completed["pursuit_checkpoint"]["status"], "achieved");
    assert_eq!(completed["pursuit_checkpoint"]["segments_started"], 2);
    assert_eq!(completed["pursuit_checkpoint"]["segments_completed"], 1);
    assert!(second.stop().status.success());
}

#[test]
fn enabling_auto_pursuit_requires_stopping_a_plain_existing_daemon_first() {
    let fixture = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    let workdir = TempDir::new().unwrap();
    let config = write_config(&fixture);
    let bin = write_fake_harness(&fixture);
    let mut daemon = DaemonGuard::start_plain(data.path(), &config);

    let second = vyane()
        .env("VYANE_DATA_DIR", data.path())
        .env("PATH", inherited_path(&bin))
        .arg("--config")
        .arg(&config)
        .args([
            "daemon",
            "start",
            "--addr",
            "127.0.0.1:0",
            "--goal-auto-pursue",
            "--goal-target",
            "builder",
            "--goal-workdir",
        ])
        .arg(workdir.path())
        .output()
        .unwrap();
    assert!(!second.status.success());
    assert!(
        String::from_utf8_lossy(&second.stderr)
            .contains("stop it before changing or reasserting automatic goal pursuit")
    );
    assert!(daemon.stop().status.success());
}

#[test]
fn resident_daemon_settles_queued_goals_sequentially_by_priority() {
    let fixture = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    let workdir = TempDir::new().unwrap();
    let config = write_config(&fixture);
    let bin = write_fake_harness(&fixture);
    create_goal_named(data.path(), workdir.path(), "resident-low", "3");
    create_goal_named(data.path(), workdir.path(), "resident-high", "0");

    let mut daemon = DaemonGuard::start(data.path(), &config, workdir.path(), &bin);
    let high = wait_for_completion_named(data.path(), "resident-high", Duration::from_secs(15));
    let low = wait_for_completion_named(data.path(), "resident-low", Duration::from_secs(15));

    assert_eq!(high["pursuit_checkpoint"]["segments_started"], 1);
    assert_eq!(low["pursuit_checkpoint"]["segments_started"], 0);
    assert_eq!(high["goal"]["claim_generation"], 1);
    assert_eq!(low["goal"]["claim_generation"], 1);
    assert!(daemon.stop().status.success());
}

#[test]
fn pursuit_transaction_error_backs_off_and_allows_next_goal() {
    let fixture = TempDir::new().unwrap();
    let data = TempDir::new().unwrap();
    let workdir = TempDir::new().unwrap();
    let config = write_config(&fixture);
    let bin = write_fake_harness(&fixture);
    create_goal_named(data.path(), workdir.path(), "resident-failing", "0");
    create_goal_named(data.path(), workdir.path(), "resident-next", "1");
    Connection::open(data.path().join("goals.sqlite3"))
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_failing_pursuit_event \
             BEFORE INSERT ON goal_events \
             WHEN NEW.goal_id = 'resident-failing' \
              AND NEW.stage = 'pursuit.started' \
             BEGIN SELECT RAISE(ABORT, 'injected pursuit failure'); END;",
        )
        .unwrap();

    let mut daemon = DaemonGuard::start(data.path(), &config, workdir.path(), &bin);
    let next = wait_for_completion_named(data.path(), "resident-next", Duration::from_secs(15));
    let failing = goal_detail_named(data.path(), "resident-failing").unwrap();

    assert_eq!(next["goal"]["status"], "completed");
    assert_eq!(failing["goal"]["status"], "in_progress");
    assert_eq!(failing["goal"]["claimed_by"], "daemon-goal:auto-v1");
    assert_eq!(failing["pursuit_checkpoint"], Value::Null);
    assert!(daemon.stop().status.success());
}
