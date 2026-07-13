//! Acceptance tests for WP-08 — detached background runs.
//!
//! Every test drives the real `vyane` binary via `assert_cmd`, backed by a
//! `wiremock` OpenAI-chat endpoint. No real network, no real CLIs. The detached
//! worker is a re-exec of the same binary, so these exercise the full parent →
//! worker → SQLite task ledger → `task` lifecycle end to end, plus read-only
//! compatibility for pre-SQLite status files.

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::{Value, json};
use tempfile::TempDir;
use vyane_core::{HarnessKind, ModelId, Protocol, ProviderId, SessionRecord, SessionStore, Target};
use vyane_ledger::FsSessionStore;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn vyane() -> Command {
    Command::cargo_bin("vyane").expect("vyane binary")
}

fn config_for(server: &MockServer) -> String {
    format!(
        r#"
        [providers.test]
        base_url = "{}"
        api_key_env = "VYANE_CLI_TEST_KEY"
        auth_style = "bearer"
        protocol = "openai_chat"
        default_model = "test-model"

        [profiles.review]
        provider = "test"
        protocol = "openai_chat"
        harness = "none"
        model = "test-model"
        "#,
        server.uri()
    )
}

fn auto_config_for(server: &MockServer) -> String {
    format!(
        r#"
        [providers.test]
        base_url = "{}"
        api_key_env = "VYANE_CLI_TEST_KEY"
        auth_style = "bearer"
        protocol = "openai_chat"
        default_model = "cheap-model"

        [profiles.cheap]
        provider = "test"
        protocol = "openai_chat"
        harness = "none"
        model = "cheap-model"
        tier = "economy"
        "#,
        server.uri()
    )
}

fn guarded_auto_config_for(server: &MockServer) -> String {
    format!(
        r#"
        [providers.test]
        base_url = "{}"
        api_key_env = "VYANE_CLI_TEST_KEY"
        auth_style = "bearer"
        protocol = "openai_chat"
        default_model = "cheap-model"

        [profiles.cheap]
        provider = "test"
        protocol = "openai_chat"
        harness = "none"
        model = "cheap-model"
        tier = "economy"
        failover = ["frontier"]

        [profiles.cheap.params]
        effort = "low"

        [profiles.frontier]
        provider = "test"
        protocol = "openai_chat"
        harness = "none"
        model = "frontier-model"
        tier = "frontier"
        "#,
        server.uri()
    )
}

fn harness_config() -> String {
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
        "#
    .to_string()
}

#[cfg(target_os = "linux")]
fn write_stubborn_fake_claude(dir: &TempDir) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;

    let bin = dir.path().join("claude");
    fs::write(&bin, "#!/bin/sh\ntrap '' TERM\nwhile :; do sleep 1; done\n")
        .expect("write stubborn fake claude");
    fs::set_permissions(&bin, fs::Permissions::from_mode(0o755))
        .expect("make fake claude executable");
    bin
}

#[cfg(target_os = "linux")]
fn write_cancellable_fake_claude(dir: &TempDir) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;

    let bin = dir.path().join("claude");
    fs::write(&bin, "#!/bin/sh\nwhile :; do sleep 1; done\n")
        .expect("write cancellable fake claude");
    fs::set_permissions(&bin, fs::Permissions::from_mode(0o755))
        .expect("make fake claude executable");
    bin
}

#[cfg(target_os = "linux")]
fn write_success_fake_claude(dir: &TempDir) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;

    let bin = dir.path().join("claude");
    fs::write(
        &bin,
        "#!/bin/sh\nset -eu\nprintf '%s\\n' inherited > inherited-marker\nprintf '%s\\n' '{\"result\":\"detached pinned\",\"session_id\":\"native-new\"}'\n",
    )
    .expect("write successful fake claude");
    fs::set_permissions(&bin, fs::Permissions::from_mode(0o755))
        .expect("make fake claude executable");
    bin
}

#[cfg(target_os = "linux")]
struct DetachedHarnessProcesses {
    id: String,
    path: String,
    sidecar: std::path::PathBuf,
    worker_pid: i32,
    worker_pgid: i32,
    harness_pid: i32,
    harness_pgid: i32,
}

/// Start a real detached worker around a TERM-ignoring fake Claude CLI and wait
/// until both independently controlled process groups are durably observable.
#[cfg(target_os = "linux")]
async fn spawn_stubborn_detached_harness(
    config_dir: &TempDir,
    data_dir: &TempDir,
    bin_dir: &TempDir,
    task: &str,
) -> DetachedHarnessProcesses {
    write_stubborn_fake_claude(bin_dir);
    let config = write_config(config_dir, &harness_config());
    let path = format!(
        "{}:{}",
        bin_dir.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let out = vyane()
        .env("PATH", &path)
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["dispatch", task, "--target", "builder", "--detach"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = String::from_utf8(out).expect("utf8 id").trim().to_string();
    let sidecar = data_dir
        .path()
        .join("tasks")
        .join(&id)
        .join("harness-controller.json");

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let status = task_status_json(data_dir.path(), &id);
        let controller = fs::read(&sidecar)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
        if let (Some(status), Some(controller)) = (status, controller) {
            if status["state"] == "running" {
                let values = (
                    status["controller"]["pid"].as_i64(),
                    status["controller"]["pgid"].as_i64(),
                    controller["pid"].as_i64(),
                    controller["pgid"].as_i64(),
                );
                if let (
                    Some(worker_pid),
                    Some(worker_pgid),
                    Some(harness_pid),
                    Some(harness_pgid),
                ) = values
                {
                    let processes = DetachedHarnessProcesses {
                        id,
                        path,
                        sidecar,
                        worker_pid: worker_pid as i32,
                        worker_pgid: worker_pgid as i32,
                        harness_pid: harness_pid as i32,
                        harness_pgid: harness_pgid as i32,
                    };
                    assert_ne!(
                        processes.worker_pgid, processes.harness_pgid,
                        "harness must own a distinct group"
                    );
                    return processes;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "worker never published both outer and nested controllers"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn write_config(dir: &TempDir, text: &str) -> std::path::PathBuf {
    let path = dir.path().join("config.toml");
    fs::write(&path, text).expect("write config");
    path
}

/// Assert the `tasks/` directory was never created, or is empty — no run dir
/// leaked. Used by the exit-2 input-validation tests (config error, bad label):
/// invalid input must be rejected before anything is spawned.
fn assert_no_task_dir(data_dir: &Path) {
    let tasks = data_dir.join("tasks");
    let empty = match fs::read_dir(&tasks) {
        Ok(mut entries) => entries.next().is_none(),
        Err(_) => true, // not created at all
    };
    assert!(empty, "a task dir was created despite invalid input");
    assert!(
        !data_dir.join("tasks.sqlite3").exists(),
        "task metadata database was created despite invalid input"
    );
}

/// New detached submissions may persist lifecycle artifacts, but never the
/// private request or any of its canaries inside their task directory.
fn assert_private_request_absent(data_dir: &Path, id: &str, canaries: &[&str]) {
    let task_dir = data_dir.join("tasks").join(id);
    assert!(task_dir.is_dir(), "missing task directory for {id}");
    assert!(
        !task_dir.join("job.json").exists(),
        "new detached submissions must not persist job.json"
    );
    assert!(
        !task_dir.join("status.json").exists(),
        "new detached submissions must not maintain a second metadata ledger"
    );

    let mut pending = vec![task_dir];
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(&dir).expect("scan task directory") {
            let entry = entry.expect("task directory entry");
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            let contents = fs::read(&path).expect("read task artifact for privacy scan");
            let contents = String::from_utf8_lossy(&contents);
            for canary in canaries {
                assert!(
                    !contents.contains(canary),
                    "private canary leaked into {}",
                    path.display()
                );
            }
        }
    }
}

fn assert_task_database_excludes(data_dir: &Path, canaries: &[&str]) {
    for suffix in ["", "-wal", "-shm"] {
        let path = data_dir.join(format!("tasks.sqlite3{suffix}"));
        let Ok(contents) = fs::read(&path) else {
            continue;
        };
        let contents = String::from_utf8_lossy(&contents);
        for canary in canaries {
            assert!(
                !contents.contains(canary),
                "private canary leaked into {}",
                path.display()
            );
        }
    }
}

/// On Linux, directly prove the worker command line and inherited environment
/// contain only transport-neutral metadata, not fields from the private job.
#[cfg(target_os = "linux")]
fn assert_process_metadata_excludes(pid: i32, canaries: &[&str]) {
    for name in ["cmdline", "environ"] {
        let path = format!("/proc/{pid}/{name}");
        let contents = fs::read(&path).unwrap_or_else(|error| panic!("read {path}: {error}"));
        let contents = String::from_utf8_lossy(&contents);
        for canary in canaries {
            assert!(
                !contents.contains(canary),
                "private canary leaked into worker {name}"
            );
        }
    }
}

/// Mount an OpenAI chat mock returning `answer` after `delay`.
async fn mock_openai_delayed(server: &MockServer, answer: &str, delay: Duration) {
    let template = ResponseTemplate::new(200)
        .set_delay(delay)
        .set_body_json(json!({
            "id": "chatcmpl-test",
            "model": "test-model",
            "choices": [{
                "message": { "role": "assistant", "content": answer },
                "finish_reason": "stop"
            }],
            "usage": { "prompt_tokens": 3, "completion_tokens": 2 }
        }));
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(template)
        .mount(server)
        .await;
}

fn ledger_records(data_dir: &Path) -> Vec<Value> {
    let ledger = data_dir.join("ledger.jsonl");
    let text = fs::read_to_string(ledger).expect("ledger file");
    text.lines()
        .map(|line| serde_json::from_str(line).expect("run record json"))
        .collect()
}

/// Read `task status --json` for `id`, returning the parsed JSON. Returns
/// `None` when the task is not yet observable or the command produced no JSON.
fn task_status_json(data_dir: &Path, id: &str) -> Option<Value> {
    let out = vyane()
        .env("VYANE_DATA_DIR", data_dir)
        .args(["task", "status", id, "--json"])
        .assert()
        .get_output()
        .stdout
        .clone();
    if out.is_empty() {
        return None;
    }
    serde_json::from_slice(&out).ok()
}

/// Poll `task status --json` until its canonical state is terminal or
/// `budget` elapses. Returns the final parsed status. Panics on timeout so a
/// hung worker fails loudly rather than silently. Handles the race where the
/// status file does not exist yet by treating it as `running`.
fn poll_until_terminal(data_dir: &Path, id: &str, budget: Duration) -> Value {
    let deadline = Instant::now() + budget;
    let mut last_state = "(no status file yet)".to_string();
    loop {
        if let Some(status) = task_status_json(data_dir, id) {
            let state = status["state"].as_str().unwrap_or("").to_string();
            if !matches!(state.as_str(), "queued" | "running" | "cancelling") {
                return status;
            }
            last_state = state;
        }
        // No status file yet or still running — wait and retry.
        if Instant::now() >= deadline {
            panic!("run {id} did not finish within {budget:?}; last state = {last_state}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Poll a predicate over the public task-status JSON until true or timeout.
fn poll_status_file<F: Fn(&Value) -> bool>(
    data_dir: &Path,
    id: &str,
    budget: Duration,
    pred: F,
) -> bool {
    let deadline = Instant::now() + budget;
    loop {
        if task_status_json(data_dir, id).as_ref().is_some_and(&pred) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Acceptance #1: `--detach` returns fast with an id while a slow target is
/// still running; polling `task status` goes running → success; `output.txt`
/// matches the answer; the ledger records the run.
#[tokio::test]
async fn detach_returns_fast_then_completes_success() {
    let server = MockServer::start().await;
    // A long target delay so the run is *guaranteed* still in flight when the
    // parent returns, regardless of worker-spawn latency under a loaded CI.
    // This is the whole point of `--detach`: the parent hands the run off and
    // exits while the dispatch keeps going.
    const TARGET_DELAY: Duration = Duration::from_secs(10);
    mock_openai_delayed(&server, "detached answer", TARGET_DELAY).await;

    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    const TASK_CANARY: &str = "WP39-TASK-PRIVACY-CANARY";
    const SYSTEM_CANARY: &str = "WP39-SYSTEM-PRIVACY-CANARY";
    const LABEL_CANARY: &str = "WP39-LABEL-PRIVACY-CANARY";
    const SESSION_CANARY: &str = "WP39-SESSION-PRIVACY-CANARY";
    const CONFIG_CANARY: &str = "WP39-CONFIG-PATH-PRIVACY-CANARY";
    let canaries = [
        TASK_CANARY,
        SYSTEM_CANARY,
        LABEL_CANARY,
        SESSION_CANARY,
        CONFIG_CANARY,
    ];
    let config = config_dir.path().join(format!("{CONFIG_CANARY}.toml"));
    fs::write(&config, config_for(&server)).expect("write canary config");

    let started = Instant::now();
    let out = vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch",
            TASK_CANARY,
            "--target",
            "review",
            "--system",
            SYSTEM_CANARY,
            "--label",
            &format!("privacy={LABEL_CANARY}"),
            "--session",
            SESSION_CANARY,
            "--detach",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let elapsed = started.elapsed();

    let id = String::from_utf8(out).expect("utf8 id").trim().to_string();
    assert!(!id.is_empty(), "expected a run id on stdout");

    // The parent returned an id without blocking on the dispatch. Proven
    // structurally (timing-robust): the run is observably still `running` right
    // after the parent returns — a parent that wrongly waited for completion
    // could not return until the target answered (>= TARGET_DELAY), by which
    // point the status would already be terminal. The wall-clock ceiling below
    // is only a coarse hang-guard (measuring subprocess spawn under parallel
    // `cargo test` is inherently noisy), not the invariant itself.
    assert!(
        elapsed < TARGET_DELAY,
        "--detach did not return before the target could answer ({elapsed:?}); it likely blocked"
    );

    // Primary invariant: the handed-off run is genuinely in flight (the target
    // is still sleeping). A brief window absorbs the race with the worker's
    // first status write; TARGET_DELAY (10s) dwarfs any spawn latency, so this
    // is deterministic in practice.
    let saw_running = poll_status_file(data_dir.path(), &id, Duration::from_secs(5), |v| {
        v["state"] == "running"
    });
    assert!(
        saw_running,
        "worker never reported running while the target was still delayed"
    );
    #[cfg(target_os = "linux")]
    {
        let running_status = task_status_json(data_dir.path(), &id).expect("running status");
        assert_process_metadata_excludes(
            running_status["controller"]["pid"]
                .as_i64()
                .expect("worker pid") as i32,
            &canaries,
        );
    }

    // Poll to completion (well past TARGET_DELAY).
    let final_status = poll_until_terminal(data_dir.path(), &id, Duration::from_secs(30));
    assert_eq!(final_status["state"], "succeeded");
    // The task id names the durable metadata row; the kernel mints its own
    // ledger run_id, which the task links via `ledger_run_id`.
    assert_eq!(final_status["id"], id.as_str());
    let ledger_run_id = final_status["ledger_run_id"]
        .as_str()
        .expect("ledger_run_id set on success")
        .to_string();

    // output.txt matches the answer.
    let output = vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "status", &id, "--output"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output = String::from_utf8(output).expect("utf8 output");
    assert_eq!(output.trim(), "detached answer");

    // Ledger contains exactly this run, keyed by the kernel's run_id, which the
    // status file points at.
    let records = ledger_records(data_dir.path());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["status"], "success");
    assert_eq!(records[0]["run_id"], ledger_run_id.as_str());
    assert_private_request_absent(data_dir.path(), &id, &canaries);
    assert_task_database_excludes(data_dir.path(), &canaries);
}

#[tokio::test]
async fn detached_auto_route_freezes_profile_effort_and_labels() {
    let server = MockServer::start().await;
    mock_openai_delayed(&server, "auto detached", Duration::ZERO).await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &auto_config_for(&server));

    let output = vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["dispatch", "hello", "--target", "auto", "--detach"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = String::from_utf8(output)
        .expect("utf8 id")
        .trim()
        .to_string();
    let status = poll_until_terminal(data_dir.path(), &id, Duration::from_secs(15));
    assert_eq!(status["state"], "succeeded");

    assert!(
        !data_dir
            .path()
            .join("tasks")
            .join(&id)
            .join("job.json")
            .exists(),
        "auto detach must also use the private stdin envelope"
    );

    let requests = server.received_requests().await.expect("received requests");
    assert_eq!(requests.len(), 1);
    let body: Value = serde_json::from_slice(&requests[0].body).expect("request json");
    assert_eq!(body["model"], "cheap-model");
    assert_eq!(body["reasoning_effort"], "low");

    let records = ledger_records(data_dir.path());
    assert_eq!(records[0]["labels"]["routing.profile"], "cheap");
    assert_eq!(records[0]["labels"]["routing.effort"], "low");
}

#[tokio::test]
async fn detached_no_frontier_replays_parent_failover_filter() {
    let server = MockServer::start().await;
    mock_openai_delayed(&server, "guarded detached", Duration::ZERO).await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &guarded_auto_config_for(&server));

    let output = vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch",
            "hello",
            "--target",
            "auto",
            "--no-frontier",
            "--detach",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = String::from_utf8(output)
        .expect("utf8 id")
        .trim()
        .to_string();
    let status = poll_until_terminal(data_dir.path(), &id, Duration::from_secs(15));
    assert_eq!(status["state"], "succeeded");

    assert!(
        !data_dir
            .path()
            .join("tasks")
            .join(&id)
            .join("job.json")
            .exists(),
        "guarded auto detach must not persist its route snapshot"
    );

    let requests = server.received_requests().await.expect("received requests");
    assert_eq!(requests.len(), 1);
    let body: Value = serde_json::from_slice(&requests[0].body).expect("request json");
    assert_eq!(body["model"], "cheap-model");

    let records = ledger_records(data_dir.path());
    assert_eq!(records[0]["labels"]["routing.allow_frontier"], "false");
}

/// Acceptance #2: a config error with `--detach` exits 2 and creates no task
/// dir (validation happens before anything is spawned).
#[tokio::test]
async fn detach_config_error_exits_two_and_creates_no_task_dir() {
    let server = MockServer::start().await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &config_for(&server));

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch",
            "hello",
            "--target",
            "no-such-profile",
            "--detach",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("config error"));

    assert_no_task_dir(data_dir.path());
}

/// Acceptance #2b: an invalid `--label` (no `key=value`) with `--detach` is
/// rejected in the PARENT — exits 2, exactly like a config error, and creates
/// no task dir. The reviewer's repro: `--label bad`. This proves the full
/// TaskSpec (label parsing included) is validated before anything is spawned,
/// not deferred into a worker that would leave a stray task dir behind.
#[tokio::test]
async fn detach_bad_label_exits_two_and_creates_no_task_dir() {
    let server = MockServer::start().await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &config_for(&server));

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch", "hello", "--target",
            "review", // a VALID target, so only the label is wrong
            "--label", "bad", // no '=', must be rejected
            "--detach",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("config error"))
        .stderr(predicate::str::contains("key=value"));

    assert_no_task_dir(data_dir.path());
}

/// Capability admission is also a parent-side detached preflight: a chat-only
/// target cannot accept filesystem mutation, and rejection leaves both the
/// task database and worker population at zero.
#[tokio::test]
async fn detached_direct_http_write_is_rejected_before_task_or_process() {
    let server = MockServer::start().await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let workdir = TempDir::new().expect("workdir tempdir");
    let config = write_config(&config_dir, &config_for(&server));

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch",
            "edit",
            "--target",
            "review",
            "--sandbox",
            "write",
            "--workdir",
        ])
        .arg(workdir.path())
        .arg("--detach")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("config error"))
        .stderr(predicate::str::contains("local_editing_unavailable"));

    assert_no_task_dir(data_dir.path());
    assert!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .is_empty(),
        "preflight must not issue HTTP"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn detached_legacy_native_resume_is_rejected_before_task_or_process() {
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &harness_config());
    let now = chrono::Utc::now();
    let record = SessionRecord {
        session_id: "legacy-native".into(),
        owner: "local".into(),
        target: Target {
            provider: ProviderId::new("native"),
            protocol: Protocol::AnthropicMessages,
            harness: Some(HarnessKind::ClaudeCode),
            model: ModelId::new("test-model"),
        },
        native_session_id: Some("native-old".into()),
        transcript: Vec::new(),
        created_at: now,
        updated_at: now,
        run_count: 1,
    };
    FsSessionStore::new(data_dir.path().join("sessions"))
        .save("local", &record)
        .await
        .expect("seed legacy native session");

    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch",
            "edit",
            "--target",
            "builder",
            "--session",
            "legacy-native",
        ])
        .arg("--detach")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("NativeSessionDomain"));

    assert_no_task_dir(data_dir.path());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn detached_write_transfers_parent_pin_through_worker_to_harness() {
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let bin_dir = TempDir::new().expect("bin tempdir");
    let workdir = TempDir::new().expect("workdir tempdir");
    write_success_fake_claude(&bin_dir);
    let config = write_config(&config_dir, &harness_config());
    let path = format!(
        "{}:{}",
        bin_dir.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let output = vyane()
        .env("PATH", path)
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch",
            "edit",
            "--target",
            "builder",
            "--sandbox",
            "write",
            "--workdir",
        ])
        .arg(workdir.path())
        .arg("--detach")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = String::from_utf8(output)
        .expect("detached id is utf8")
        .trim()
        .to_string();
    let status = poll_until_terminal(data_dir.path(), &id, Duration::from_secs(15));
    assert_eq!(status["state"], "succeeded");
    assert_eq!(
        fs::read_to_string(workdir.path().join("inherited-marker"))
            .expect("fake harness wrote through inherited pin")
            .trim(),
        "inherited"
    );
}

/// A corrupt, non-empty stdin envelope must never fall back to legacy
/// `job.json`. The worker records a terminal error and exits nonzero instead of
/// leaving a `running`, `died`, or status-less task behind.
#[tokio::test]
async fn corrupt_stdin_envelope_finalizes_error_not_running() {
    let data_dir = TempDir::new().expect("data tempdir");
    let id = "0198c0de-0000-7000-8000-0000000badj0";
    let task_dir = data_dir.path().join("tasks").join(id);
    fs::create_dir_all(&task_dir).expect("create task dir");

    // Invoke the hidden worker exactly as the parent does: id in argv and the
    // one-shot request on a pipe that closes at EOF.
    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["__worker", id])
        .write_stdin("{ this is not valid envelope json ")
        .assert()
        .code(1)
        .stderr(predicate::str::contains("worker error"))
        .stderr(predicate::str::contains("parse worker stdin envelope"));

    // status.json exists and is terminal `error` — not left `running`, and (a
    // status file DOES exist so) not read as `stale` either.
    let status_path = task_dir.join("status.json");
    let status: Value = serde_json::from_str(
        &fs::read_to_string(&status_path).expect("worker must have written status.json"),
    )
    .expect("parse status.json");
    assert_eq!(
        status["state"], "error",
        "a worker setup failure must finalize as error, not running/died"
    );
    assert!(
        !status["error"].as_str().unwrap_or("").is_empty(),
        "the error status must carry a message"
    );

    // And `task status` reports it as error with exit 1 (unhappy terminal).
    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "status", id])
        .assert()
        .code(1)
        .stdout(predicate::str::contains("error"));
    assert!(
        !task_dir.join("job.json").exists(),
        "corrupt stdin handling must not materialize a request file"
    );
}

#[tokio::test]
async fn legacy_job_reader_preserves_target_snapshot_drift_check() {
    let server = MockServer::start().await;
    mock_openai_delayed(&server, "must not execute", Duration::ZERO).await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &config_for(&server));
    let id = "0198c0de-0000-7000-8000-0000000dr1f7";
    let task_dir = data_dir.path().join("tasks").join(id);
    fs::create_dir_all(&task_dir).expect("create task dir");

    // The config resolves `test-model`, while the parent-approved snapshot
    // records `model-before-edit`. This deterministically models a profile edit
    // between detached submission and worker startup.
    let job = json!({
        "run_id": id,
        "task": "do not run after drift",
        "target": "review",
        "sandbox": "read-only",
        "target_snapshot": [{
            "target": {
                "provider": "test",
                "protocol": "openai_chat",
                "harness": null,
                "model": "model-before-edit"
            },
            "transport": "direct_http",
            "params": {}
        }]
    });
    fs::write(
        task_dir.join("job.json"),
        serde_json::to_vec_pretty(&job).expect("serialize job"),
    )
    .expect("write job");

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["__worker", id])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "target configuration changed after submission",
        ));

    let status: Value =
        serde_json::from_slice(&fs::read(task_dir.join("status.json")).expect("terminal status"))
            .expect("status json");
    assert_eq!(status["state"], "error");
    assert!(
        status["error"]
            .as_str()
            .unwrap_or_default()
            .contains("target configuration changed after submission")
    );
    assert!(
        server
            .received_requests()
            .await
            .expect("request log")
            .is_empty(),
        "drifted target must be rejected before any provider request"
    );
}

/// Empty stdin is the explicit migration fallback for task directories written
/// by old parents. A valid legacy job must still run to completion and remain
/// readable after the stdin-envelope transport ships.
#[tokio::test]
async fn legacy_job_json_still_executes_with_empty_stdin() {
    let server = MockServer::start().await;
    mock_openai_delayed(&server, "legacy answer", Duration::ZERO).await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &config_for(&server));
    let id = "0198c0de-0000-7000-8000-00000001e9ac";
    let task_dir = data_dir.path().join("tasks").join(id);
    fs::create_dir_all(&task_dir).expect("create task dir");
    let job = json!({
        "run_id": id,
        "task": "execute old task",
        "target": "review",
        "sandbox": "read-only"
    });
    fs::write(
        task_dir.join("job.json"),
        serde_json::to_vec_pretty(&job).expect("serialize legacy job"),
    )
    .expect("write legacy job");

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["__worker", id])
        .write_stdin(Vec::<u8>::new())
        .assert()
        .success();

    let status: Value =
        serde_json::from_slice(&fs::read(task_dir.join("status.json")).expect("terminal status"))
            .expect("status json");
    assert_eq!(status["state"], "success");
    assert_eq!(
        fs::read_to_string(task_dir.join("output.txt"))
            .expect("legacy output")
            .trim(),
        "legacy answer"
    );
    assert!(
        task_dir.join("job.json").exists(),
        "compatibility reads must not delete the legacy job"
    );
}

/// Acceptance #4b: a task dir with `job.json` but NO `status.json` (the worker
/// never published status — a spawn likely failed) must be VISIBLE: `task list`
/// shows it as `stale`, and `task status <id>` exits nonzero explaining that
/// the worker never wrote status.
#[tokio::test]
async fn job_without_status_shows_stale_and_explains() {
    let data_dir = TempDir::new().expect("data tempdir");
    let id = "0198c0de-0000-7000-8000-00000000513e";
    let task_dir = data_dir.path().join("tasks").join(id);
    fs::create_dir_all(&task_dir).expect("create task dir");
    // A well-formed job.json but deliberately NO status.json — models a worker
    // that never came up.
    let job = json!({
        "run_id": id,
        "task": "never ran",
        "target": "review",
        "sandbox": "read-only"
    });
    fs::write(
        task_dir.join("job.json"),
        serde_json::to_string_pretty(&job).expect("serialize job"),
    )
    .expect("write job.json");

    // `task list` shows it as stale.
    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("stale"))
        .stdout(predicate::str::contains(id));

    // `task list --json` reports state stale for that id.
    let out = vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "list", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let rows: Value = serde_json::from_slice(&out).expect("task list json");
    let row = rows
        .as_array()
        .expect("array")
        .iter()
        .find(|r| r["id"] == id)
        .expect("stale row present");
    assert_eq!(row["state"], "stale");

    // `task status <id>` exits nonzero and explains the worker never wrote
    // status (points at the log for triage).
    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "status", id])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("stale"))
        .stderr(predicate::str::contains("worker never wrote status"));
}

/// New parents create the task directory and log before spawning/handing off.
/// If status publication never happens, that request remains discoverable even
/// though the private request was never written as job.json.
#[tokio::test]
async fn new_scaffold_without_status_or_job_shows_stale_and_explains() {
    let data_dir = TempDir::new().expect("data tempdir");
    let id = "0198c0de-0000-7000-8000-00000000571d";
    let task_dir = data_dir.path().join("tasks").join(id);
    fs::create_dir_all(&task_dir).expect("create task dir");
    fs::write(task_dir.join("task.log"), []).expect("create empty worker log");
    assert!(!task_dir.join("job.json").exists());
    assert!(!task_dir.join("status.json").exists());

    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains(id))
        .stdout(predicate::str::contains("stale"));

    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "status", id])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("stale"))
        .stderr(predicate::str::contains("stdin handoff"));
}

/// Acceptance #3: `task cancel` on a long-running detached run leaves
/// `status.json` == cancelled, kills the process group, and the ledger has the
/// cancelled RunRecord.
#[tokio::test]
async fn cancel_finalizes_cancelled_and_kills_group() {
    let server = MockServer::start().await;
    // Long delay so the run is comfortably still in-flight when we cancel.
    mock_openai_delayed(&server, "never delivered", Duration::from_secs(30)).await;

    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &config_for(&server));

    let out = vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["dispatch", "cancel me", "--target", "review", "--detach"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = String::from_utf8(out).expect("utf8 id").trim().to_string();
    assert!(
        !data_dir
            .path()
            .join("tasks")
            .join(&id)
            .join("job.json")
            .exists(),
        "cancel-capable detached tasks must not persist job.json"
    );

    // Wait until the worker is actually running (has written its pid). A
    // generous budget absorbs worker-spawn latency under parallel `cargo test`;
    // the 30s target delay means the run is nowhere near finishing meanwhile.
    let running = poll_status_file(data_dir.path(), &id, Duration::from_secs(15), |v| {
        v["state"] == "running" && v["controller"]["pid"].as_i64().unwrap_or(0) > 0
    });
    assert!(running, "worker never reached running with a pid");
    let running_status =
        task_status_json(data_dir.path(), &id).expect("status exists (already confirmed running)");
    let pid = running_status["controller"]["pid"].as_i64().expect("pid") as i32;
    // The worker's own process group (setsid → pgid == worker pid). `task
    // cancel` group-signals THIS pgid; we assert the whole group is gone after.
    let pgid = running_status["controller"]["pgid"].as_i64().expect("pgid") as i32;
    assert!(pgid > 0, "worker must record a real pgid");

    // Cancel — SIGTERM group, wait for finalize; the worker's SIGTERM handler
    // cancels the kernel so the run records `cancelled` and finalizes.
    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "cancel", &id])
        .assert()
        .success()
        .stdout(predicate::str::contains("cancelled"));

    // Durable task metadata is cancelled.
    let status = task_status_json(data_dir.path(), &id).expect("status exists after cancel");
    assert_eq!(status["state"], "cancelled");
    let ledger_run_id = status["ledger_run_id"]
        .as_str()
        .expect("ledger_run_id set after cancel finalize")
        .to_string();

    // Process group is dead: the worker pid is gone. `task cancel` already
    // waited out its SIGTERM grace + finalize window before returning, so the
    // worker should be exiting; allow extra headroom for the OS to reap it.
    let dead = wait_pid_dead(pid, Duration::from_secs(8));
    assert!(dead, "worker pid {pid} still alive after cancel");

    // Group-kill proof for the outer detached worker group. Nested CLI harness
    // groups have their own explicit controller and are covered separately.
    let group_empty = wait_group_empty(pgid, Duration::from_secs(8));
    assert!(
        group_empty,
        "process group {pgid} still has live members after cancel"
    );

    // Ledger has the cancelled RunRecord (keyed by the kernel's run_id).
    let records = ledger_records(data_dir.path());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["status"], "cancelled");
    assert_eq!(records[0]["run_id"], ledger_run_id.as_str());
}

/// Exact Linux boot-id/start-ticks identity is authoritative and must not
/// depend on `ps` being present in PATH. Cancellation remains safe and usable
/// even when no external process-inspection command can be executed.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn fingerprint_cancel_does_not_depend_on_ps_or_path() {
    let server = MockServer::start().await;
    mock_openai_delayed(&server, "never delivered", Duration::from_secs(30)).await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let empty_path = TempDir::new().expect("empty PATH tempdir");
    let config = write_config(&config_dir, &config_for(&server));

    let out = vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch",
            "retry cancellation",
            "--target",
            "review",
            "--detach",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = String::from_utf8(out).expect("utf8 id").trim().to_string();
    assert!(poll_status_file(
        data_dir.path(),
        &id,
        Duration::from_secs(15),
        |value| value["state"] == "running"
    ));

    vyane()
        .env("PATH", empty_path.path())
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "cancel", &id])
        .assert()
        .success()
        .stdout(predicate::str::contains("cancelled"));
    assert_eq!(
        task_status_json(data_dir.path(), &id)
            .expect("terminal task remains observable after fingerprint cancellation")["state"],
        "cancelled"
    );
}

/// Graceful cancellation signals only the outer worker. Its armed handler
/// cancels the harness token, so the harness reports Cancelled rather than
/// racing a direct nested TERM exit into HarnessFailed.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn graceful_nested_harness_cancel_is_classified_cancelled() {
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let bin_dir = TempDir::new().expect("fake bin tempdir");
    write_cancellable_fake_claude(&bin_dir);
    let config = write_config(&config_dir, &harness_config());
    let path = format!(
        "{}:{}",
        bin_dir.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let out = vyane()
        .env("PATH", &path)
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch",
            "cancel harness gracefully",
            "--target",
            "builder",
            "--detach",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let id = String::from_utf8(out).expect("utf8 id").trim().to_string();
    let sidecar = data_dir
        .path()
        .join("tasks")
        .join(&id)
        .join("harness-controller.json");
    let deadline = Instant::now() + Duration::from_secs(15);
    while !sidecar.exists() {
        assert!(
            Instant::now() < deadline,
            "nested harness controller was never published"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    vyane()
        .env("PATH", &path)
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "cancel", &id])
        .assert()
        .success()
        .stdout(predicate::str::contains("cancelled"));
    let final_status = task_status_json(data_dir.path(), &id).expect("terminal durable status");
    assert_eq!(final_status["state"], "cancelled");
    assert_eq!(final_status["failure_code"], "cancelled");
    assert!(!sidecar.exists());
}

/// A detached worker and its CLI harness deliberately own separate process
/// groups. Even if the worker event loop is stopped and cannot forward its
/// cancellation token, the private nested-controller sidecar lets a separate
/// `task cancel` invocation verify and kill both groups without PID guessing.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn forced_cancel_kills_stubborn_nested_harness_when_worker_is_stopped() {
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let bin_dir = TempDir::new().expect("fake bin tempdir");
    let processes =
        spawn_stubborn_detached_harness(&config_dir, &data_dir, &bin_dir, "run stubborn harness")
            .await;

    unix_signal_pid(processes.worker_pid, 19); // SIGSTOP: block token forwarding.
    assert!(unix_pid_alive(processes.worker_pid));
    assert!(unix_pid_alive(processes.harness_pid));

    vyane()
        .env("PATH", &processes.path)
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "cancel", &processes.id])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("interrupted"));

    assert!(
        wait_group_empty(processes.harness_pgid, Duration::from_secs(8)),
        "nested harness group {} survived forced cancellation",
        processes.harness_pgid
    );
    assert!(
        wait_group_empty(processes.worker_pgid, Duration::from_secs(8)),
        "outer worker group {} survived forced cancellation",
        processes.worker_pgid
    );
    assert!(
        wait_pid_dead(processes.harness_pid, Duration::from_secs(8)),
        "nested harness leader {} survived forced cancellation",
        processes.harness_pid
    );
    assert!(
        !processes.sidecar.exists(),
        "dead nested controller sidecar was not cleared"
    );
    let final_status =
        task_status_json(data_dir.path(), &processes.id).expect("terminal durable status");
    assert_eq!(final_status["state"], "interrupted");
    assert_eq!(final_status["failure_code"], "control_unavailable");
}

/// SIGKILL bypasses the worker's reporter Drop path. A direct cancel must use
/// the still-private nested sidecar before terminalizing the durable row.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn cancel_after_worker_sigkill_cleans_orphaned_nested_harness() {
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let bin_dir = TempDir::new().expect("fake bin tempdir");
    let processes =
        spawn_stubborn_detached_harness(&config_dir, &data_dir, &bin_dir, "crash outer worker")
            .await;

    unix_signal_pid(processes.worker_pid, 9);
    assert!(
        wait_pid_dead(processes.worker_pid, Duration::from_secs(8)),
        "outer worker {} was not reaped after SIGKILL",
        processes.worker_pid
    );
    assert!(
        unix_pid_alive(processes.harness_pid),
        "nested harness should survive the outer SIGKILL until explicit cleanup"
    );

    vyane()
        .env("PATH", &processes.path)
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "cancel", &processes.id])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("worker process is gone"));

    assert!(
        wait_group_empty(processes.harness_pgid, Duration::from_secs(8)),
        "nested harness group {} survived outer-dead cancellation",
        processes.harness_pgid
    );
    assert!(!processes.sidecar.exists());
    let final_status =
        task_status_json(data_dir.path(), &processes.id).expect("terminal durable status");
    assert_eq!(final_status["state"], "interrupted");
    assert_eq!(final_status["failure_code"], "worker_lost");
}

/// Status/list are non-signalling reads. When the outer worker is dead but its
/// exact nested sidecar remains live, reconciliation must preserve a
/// controllable row so a later explicit cancel can clean the orphan.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn status_after_worker_sigkill_preserves_nested_cleanup_entrypoint() {
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let bin_dir = TempDir::new().expect("fake bin tempdir");
    let processes = spawn_stubborn_detached_harness(
        &config_dir,
        &data_dir,
        &bin_dir,
        "observe crashed outer worker",
    )
    .await;

    unix_signal_pid(processes.worker_pid, 9);
    assert!(wait_pid_dead(processes.worker_pid, Duration::from_secs(8)));
    let observed = task_status_json(data_dir.path(), &processes.id).expect("active durable status");
    assert_eq!(
        observed["state"], "running",
        "read reconciliation must not make a live nested harness unreachable"
    );

    vyane()
        .env("PATH", &processes.path)
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "cancel", &processes.id])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("worker process is gone"));
    assert!(wait_group_empty(
        processes.harness_pgid,
        Duration::from_secs(8)
    ));
    assert!(!processes.sidecar.exists());
}

/// Rows written terminal by an older binary (or an external recovery action)
/// must not make an exact nested sidecar unreachable through `task cancel`.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn terminal_metadata_cancel_still_cleans_orphaned_nested_harness() {
    use vyane_task::{FailureCode, SqliteTaskStore, TaskStore as _};

    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let bin_dir = TempDir::new().expect("fake bin tempdir");
    let processes = spawn_stubborn_detached_harness(
        &config_dir,
        &data_dir,
        &bin_dir,
        "recover terminal orphan",
    )
    .await;

    unix_signal_pid(processes.worker_pid, 9);
    assert!(wait_pid_dead(processes.worker_pid, Duration::from_secs(8)));
    let store = SqliteTaskStore::open(data_dir.path().join("tasks.sqlite3"))
        .expect("open durable task store");
    let running = store
        .get("local", &processes.id)
        .expect("read durable row")
        .expect("durable row");
    let terminal = store
        .interrupt(
            "local",
            &running.id,
            running.revision,
            running.executor_epoch,
            FailureCode::WorkerLost,
            chrono::Utc::now(),
        )
        .expect("simulate older terminal recovery");
    assert_eq!(terminal.state.to_string(), "interrupted");

    vyane()
        .env("PATH", &processes.path)
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "cancel", &processes.id])
        .assert()
        .success()
        .stdout(predicate::str::contains("already interrupted"));
    assert!(wait_group_empty(
        processes.harness_pgid,
        Duration::from_secs(8)
    ));
    assert!(!processes.sidecar.exists());
}

/// A legacy/external recovery writer may terminalize metadata while the exact
/// worker is merely SIGSTOP'ed. Terminal cancel must kill that outer controller
/// too, otherwise the killed nested child remains its unreapable zombie.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn terminal_metadata_cancel_kills_stopped_outer_and_nested_groups() {
    use vyane_task::{FailureCode, SqliteTaskStore, TaskStore as _};

    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let bin_dir = TempDir::new().expect("fake bin tempdir");
    let processes = spawn_stubborn_detached_harness(
        &config_dir,
        &data_dir,
        &bin_dir,
        "terminalize stopped outer",
    )
    .await;
    unix_signal_pid(processes.worker_pid, 19);
    assert!(unix_pid_alive(processes.worker_pid));
    assert!(unix_pid_alive(processes.harness_pid));

    let store = SqliteTaskStore::open(data_dir.path().join("tasks.sqlite3"))
        .expect("open durable task store");
    let running = store
        .get("local", &processes.id)
        .expect("read durable row")
        .expect("durable row");
    store
        .interrupt(
            "local",
            &running.id,
            running.revision,
            running.executor_epoch,
            FailureCode::ControlUnavailable,
            chrono::Utc::now(),
        )
        .expect("simulate external terminal recovery");

    vyane()
        .env("PATH", &processes.path)
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "cancel", &processes.id])
        .assert()
        .success()
        .stdout(predicate::str::contains("already interrupted"));
    assert!(wait_group_empty(
        processes.worker_pgid,
        Duration::from_secs(8)
    ));
    assert!(wait_group_empty(
        processes.harness_pgid,
        Duration::from_secs(8)
    ));
    assert!(!processes.sidecar.exists());
}

/// Acceptance #4: a hand-crafted task dir with `state: running` + a dead pid is
/// shown as `died` by `task list`, and `task status` exits nonzero with a clear
/// message.
#[tokio::test]
async fn orphan_running_with_dead_pid_shows_died() {
    let data_dir = TempDir::new().expect("data tempdir");
    let id = "0198c0de-0000-7000-8000-00000000dead";
    let task_dir = data_dir.path().join("tasks").join(id);
    fs::create_dir_all(&task_dir).expect("create task dir");

    // A pid that cannot exist (kill(pid,0) → ESRCH → dead).
    let dead_pid = i32::MAX;
    let status = json!({
        "schema": 1,
        "run_id": id,
        "pid": dead_pid,
        "pgid": dead_pid,
        "state": "running",
        "started_at": "2026-01-01T00:00:00Z",
        "target": "test/test-model (openai_chat)"
    });
    fs::write(
        task_dir.join("status.json"),
        serde_json::to_string_pretty(&status).expect("serialize status"),
    )
    .expect("write status");

    // `task list` shows it as died.
    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("died"))
        .stdout(predicate::str::contains(id));

    // `task list --json` reports state died for that id.
    let out = vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "list", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let rows: Value = serde_json::from_slice(&out).expect("task list json");
    let row = rows
        .as_array()
        .expect("array")
        .iter()
        .find(|r| r["id"] == id)
        .expect("row present");
    assert_eq!(row["state"], "died");

    // `task status` exits nonzero with a clear "died" message and the on-disk
    // file is NOT rewritten (still says running).
    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "status", id])
        .assert()
        .code(1)
        .stdout(predicate::str::contains("died"));

    let on_disk: Value = serde_json::from_str(
        &fs::read_to_string(task_dir.join("status.json")).expect("read status file"),
    )
    .expect("parse status file");
    assert_eq!(
        on_disk["state"], "running",
        "orphan detection must not rewrite the status file"
    );
}

/// Acceptance #5: `task list` orders most-recent-first and `--json` parses.
#[tokio::test]
async fn task_list_orders_recent_first_and_json_parses() {
    let server = MockServer::start().await;
    mock_openai_delayed(&server, "quick", Duration::from_millis(50)).await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &config_for(&server));

    // Run three detached dispatches in sequence; uuidv7 ids are time-ordered,
    // so the last spawned must sort first.
    let mut ids = Vec::new();
    for i in 0..3 {
        let out = vyane()
            .env("VYANE_CLI_TEST_KEY", "sk-test")
            .env("VYANE_DATA_DIR", data_dir.path())
            .arg("--config")
            .arg(&config)
            .args([
                "dispatch",
                &format!("task {i}"),
                "--target",
                "review",
                "--detach",
            ])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let id = String::from_utf8(out).expect("utf8 id").trim().to_string();
        ids.push(id);
        // Small gap so started_at differs and ordering is unambiguous.
        std::thread::sleep(Duration::from_millis(30));
    }

    // Let them all finish.
    for id in &ids {
        poll_until_terminal(data_dir.path(), id, Duration::from_secs(15));
    }

    let out = vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "list", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let rows: Value = serde_json::from_slice(&out).expect("task list json");
    let listed: Vec<String> = rows
        .as_array()
        .expect("array")
        .iter()
        .map(|r| r["id"].as_str().expect("id string").to_string())
        .collect();

    assert_eq!(listed.len(), 3);
    // Most-recent-first: reverse of spawn order.
    let mut expected = ids.clone();
    expected.reverse();
    assert_eq!(listed, expected, "task list must be most-recent-first");

    // Every row parses with the documented fields.
    for row in rows.as_array().expect("array") {
        assert!(row["id"].is_string());
        assert!(row["state"].is_string());
        assert!(row["target"].is_string());
        assert!(row["started_at"].is_string());
    }
}

/// Whether `pid` is dead, polling `kill(pid, 0)` until it reports ESRCH or the
/// budget elapses.
fn wait_pid_dead(pid: i32, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    loop {
        if !unix_pid_alive(pid) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Whether the process GROUP `pgid` is empty, polling `kill(-pgid, 0)` until it
/// reports ESRCH (no members) or the budget elapses.
fn wait_group_empty(pgid: i32, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    loop {
        if !unix_group_has_members(pgid) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Test-local group-liveness probe: `kill(-pgid, 0)` returns 0 while ≥1 member
/// exists, ESRCH once the group is empty.
#[cfg(unix)]
fn unix_group_has_members(pgid: i32) -> bool {
    let Some(pid) = (pgid > 0)
        .then(|| rustix::process::Pid::from_raw(pgid))
        .flatten()
    else {
        return false;
    };
    let result = rustix::process::test_kill_process_group(pid);
    result.is_ok() || matches!(result, Err(rustix::io::Errno::PERM))
}

#[cfg(not(unix))]
fn unix_group_has_members(_pgid: i32) -> bool {
    false
}

/// Test-local liveness probe (the crate's own `pid_alive` is not part of the
/// public test surface). `kill(pid, 0)`: rc==0 → alive; ESRCH → dead; EPERM →
/// alive (exists but not signalable).
#[cfg(unix)]
fn unix_pid_alive(pid: i32) -> bool {
    let Some(pid) = (pid > 0)
        .then(|| rustix::process::Pid::from_raw(pid))
        .flatten()
    else {
        return false;
    };
    let result = rustix::process::test_kill_process(pid);
    result.is_ok() || matches!(result, Err(rustix::io::Errno::PERM))
}

#[cfg(unix)]
fn unix_signal_pid(pid: i32, signal: i32) {
    let pid = rustix::process::Pid::from_raw(pid).expect("positive worker pid");
    let signal = rustix::process::Signal::from_named_raw(signal).expect("named signal");
    rustix::process::kill_process(pid, signal)
        .unwrap_or_else(|error| panic!("signal {signal:?} to pid {pid} failed: {error}"));
}

#[cfg(not(unix))]
fn unix_pid_alive(_pid: i32) -> bool {
    true
}
