//! Acceptance tests for WP-08 — detached background runs.
//!
//! Every test drives the real `vyane` binary via `assert_cmd`, backed by a
//! `wiremock` OpenAI-chat endpoint. No real network, no real CLIs. The detached
//! worker is a re-exec of the same binary, so these exercise the full parent →
//! worker → status-file → `task` lifecycle end to end.

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::{Value, json};
use tempfile::TempDir;
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

fn write_config(dir: &TempDir, text: &str) -> std::path::PathBuf {
    let path = dir.path().join("config.toml");
    fs::write(&path, text).expect("write config");
    path
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

/// Read `task status --json` for `id`, returning the parsed JSON.
fn task_status_json(data_dir: &Path, id: &str) -> Value {
    let out = vyane()
        .env("VYANE_DATA_DIR", data_dir)
        .args(["task", "status", id, "--json"])
        .assert()
        .get_output()
        .stdout
        .clone();
    serde_json::from_slice(&out).expect("task status json")
}

/// Poll `task status --json` until its `state` is terminal (not `running`) or
/// `budget` elapses. Returns the final parsed status. Panics on timeout so a
/// hung worker fails loudly rather than silently.
fn poll_until_terminal(data_dir: &Path, id: &str, budget: Duration) -> Value {
    let deadline = Instant::now() + budget;
    loop {
        let status = task_status_json(data_dir, id);
        let state = status["state"].as_str().unwrap_or("");
        if state != "running" {
            return status;
        }
        if Instant::now() >= deadline {
            panic!("run {id} did not finish within {budget:?}; last state = {state}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Poll a raw predicate over the on-disk status file until true or timeout.
fn poll_status_file<F: Fn(&Value) -> bool>(
    data_dir: &Path,
    id: &str,
    budget: Duration,
    pred: F,
) -> bool {
    let status_path = data_dir.join("tasks").join(id).join("status.json");
    let deadline = Instant::now() + budget;
    loop {
        if let Ok(text) = fs::read_to_string(&status_path) {
            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                if pred(&value) {
                    return true;
                }
            }
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
    // A visible delay so the worker is demonstrably still running when
    // `--detach` returns, but short enough to keep the test quick.
    mock_openai_delayed(&server, "detached answer", Duration::from_millis(800)).await;

    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &config_for(&server));

    let started = Instant::now();
    let out = vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch",
            "background please",
            "--target",
            "review",
            "--detach",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let elapsed = started.elapsed();

    // Fast return: well under a couple of seconds, and long before the target
    // could have answered.
    assert!(
        elapsed < Duration::from_secs(2),
        "--detach took too long: {elapsed:?}"
    );

    let id = String::from_utf8(out).expect("utf8 id").trim().to_string();
    assert!(!id.is_empty(), "expected a run id on stdout");

    // Immediately after return the worker should be running (the target is
    // still sleeping). This can race the worker's first status write, so allow
    // a brief window for `running` to appear.
    let saw_running = poll_status_file(data_dir.path(), &id, Duration::from_secs(2), |v| {
        v["state"] == "running"
    });
    assert!(saw_running, "worker never reported running");

    // Poll to completion.
    let final_status = poll_until_terminal(data_dir.path(), &id, Duration::from_secs(15));
    assert_eq!(final_status["state"], "success");
    // The task id names the run directory / status file; the kernel mints its
    // own ledger run_id, which the status links via `ledger_run_id`.
    assert_eq!(final_status["run_id"], id.as_str());
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

    // No tasks directory (or an empty one) — certainly no run dir created.
    let tasks = data_dir.path().join("tasks");
    let empty = match fs::read_dir(&tasks) {
        Ok(mut entries) => entries.next().is_none(),
        Err(_) => true, // not created at all
    };
    assert!(empty, "a task dir was created despite a config error");
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

    // Wait until the worker is actually running (has written its pid).
    let running = poll_status_file(data_dir.path(), &id, Duration::from_secs(3), |v| {
        v["state"] == "running" && v["pid"].as_i64().unwrap_or(0) > 0
    });
    assert!(running, "worker never reached running with a pid");
    let pid = task_status_json(data_dir.path(), &id)["pid"]
        .as_i64()
        .expect("pid") as i32;

    // Cancel — SIGTERM group, wait for finalize; the worker's SIGTERM handler
    // cancels the kernel so the run records `cancelled` and finalizes.
    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "cancel", &id])
        .assert()
        .success()
        .stdout(predicate::str::contains("cancelled"));

    // status.json is cancelled.
    let status = task_status_json(data_dir.path(), &id);
    assert_eq!(status["state"], "cancelled");
    let ledger_run_id = status["ledger_run_id"]
        .as_str()
        .expect("ledger_run_id set after cancel finalize")
        .to_string();

    // Process group is dead: the worker pid is gone. Give the OS a beat.
    let dead = wait_pid_dead(pid, Duration::from_secs(3));
    assert!(dead, "worker pid {pid} still alive after cancel");

    // Ledger has the cancelled RunRecord (keyed by the kernel's run_id).
    let records = ledger_records(data_dir.path());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["status"], "cancelled");
    assert_eq!(records[0]["run_id"], ledger_run_id.as_str());
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

/// Test-local liveness probe (the crate's own `pid_alive` is not part of the
/// public test surface). `kill(pid, 0)`: rc==0 → alive; ESRCH → dead; EPERM →
/// alive (exists but not signalable).
#[cfg(unix)]
fn unix_pid_alive(pid: i32) -> bool {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    if pid <= 0 {
        return false;
    }
    // SAFETY: signal 0 sends nothing; it only probes existence. No memory
    // safety implications.
    let rc = unsafe { kill(pid, 0) };
    if rc == 0 {
        return true;
    }
    matches!(std::io::Error::last_os_error().raw_os_error(), Some(1))
}

#[cfg(not(unix))]
fn unix_pid_alive(_pid: i32) -> bool {
    true
}
