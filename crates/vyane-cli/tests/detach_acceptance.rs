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
/// `None` when the status file does not exist yet (the detached worker has not
/// written its initial `status.json`). This is expected during the brief window
/// between `--detach` returning and the worker process writing its status file;
/// callers that poll should treat `None` as "not ready yet".
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

/// Poll `task status --json` until its `state` is terminal (not `running`) or
/// `budget` elapses. Returns the final parsed status. Panics on timeout so a
/// hung worker fails loudly rather than silently. Handles the race where the
/// status file does not exist yet by treating it as `running`.
fn poll_until_terminal(data_dir: &Path, id: &str, budget: Duration) -> Value {
    let deadline = Instant::now() + budget;
    let mut last_state = "(no status file yet)".to_string();
    loop {
        if let Some(status) = task_status_json(data_dir, id) {
            let state = status["state"].as_str().unwrap_or("").to_string();
            if state != "running" {
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
    // A long target delay so the run is *guaranteed* still in flight when the
    // parent returns, regardless of worker-spawn latency under a loaded CI.
    // This is the whole point of `--detach`: the parent hands the run off and
    // exits while the dispatch keeps going.
    const TARGET_DELAY: Duration = Duration::from_secs(10);
    mock_openai_delayed(&server, "detached answer", TARGET_DELAY).await;

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

    // Poll to completion (well past TARGET_DELAY).
    let final_status = poll_until_terminal(data_dir.path(), &id, Duration::from_secs(30));
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

/// Acceptance #3b: a worker whose setup fails *after* it would publish state
/// (here: a corrupt `job.json` it cannot parse) must finalize as a terminal
/// `error`, never leave `running`/`died` behind. We craft the task dir the way
/// the parent would (valid id + `job.json`), then corrupt `job.json` and invoke
/// the hidden worker directly; it must write `state: error` and exit nonzero.
#[tokio::test]
async fn worker_setup_failure_finalizes_error_not_running() {
    let data_dir = TempDir::new().expect("data tempdir");
    let id = "0198c0de-0000-7000-8000-0000000badj0";
    let task_dir = data_dir.path().join("tasks").join(id);
    fs::create_dir_all(&task_dir).expect("create task dir");
    // A job.json that exists but does not parse as a JobSpec → read_job fails
    // inside the worker's setup phase.
    fs::write(task_dir.join("job.json"), "{ this is not valid json ")
        .expect("write corrupt job.json");

    // Invoke the hidden worker subcommand directly (as the parent's re-exec
    // would). It must not panic; it must record a terminal error status.
    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["__worker", id])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("worker error"));

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

    // Wait until the worker is actually running (has written its pid). A
    // generous budget absorbs worker-spawn latency under parallel `cargo test`;
    // the 30s target delay means the run is nowhere near finishing meanwhile.
    let running = poll_status_file(data_dir.path(), &id, Duration::from_secs(15), |v| {
        v["state"] == "running" && v["pid"].as_i64().unwrap_or(0) > 0
    });
    assert!(running, "worker never reached running with a pid");
    let running_status =
        task_status_json(data_dir.path(), &id).expect("status exists (already confirmed running)");
    let pid = running_status["pid"].as_i64().expect("pid") as i32;
    // The worker's own process group (setsid → pgid == worker pid). `task
    // cancel` group-signals THIS pgid; we assert the whole group is gone after.
    let pgid = running_status["pgid"].as_i64().expect("pgid") as i32;
    assert!(pgid > 0, "worker must record a real pgid");

    // Cancel — SIGTERM group, wait for finalize; the worker's SIGTERM handler
    // cancels the kernel so the run records `cancelled` and finalizes.
    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["task", "cancel", &id])
        .assert()
        .success()
        .stdout(predicate::str::contains("cancelled"));

    // status.json is cancelled.
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

    // Group-kill proof: the whole recorded process GROUP is empty, not just the
    // direct worker pid. `kill(-pgid, 0)` returns ESRCH once no member remains,
    // so this asserts group-level teardown via the stored pgid — the property
    // that guarantees any harness grandchildren in the group die with it.
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
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    if pgid <= 0 {
        return false;
    }
    // SAFETY: signal 0 to a negative pid probes the group's existence without
    // delivering a signal. No memory-safety implications.
    let rc = unsafe { kill(-pgid, 0) };
    rc == 0
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
