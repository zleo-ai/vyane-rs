#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used)]

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::{Duration, Instant};

use assert_cmd::Command;
use serde_json::{Value, json};
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const VYANE_BIN: &str = env!("CARGO_BIN_EXE_vyane");
const SUCCESS_RUN: &str = "0197f524-7a00-7000-8000-000000000101";
const MISSING_RUN: &str = "0197f524-7a00-7000-8000-000000000102";
const CANCEL_RUN: &str = "0197f524-7a00-7000-8000-000000000103";
const RESTART_RUN: &str = "0197f524-7a00-7000-8000-000000000104";
const SPAWN_RUN: &str = "0197f524-7a00-7000-8000-000000000105";
const READ_ONLY_RUN: &str = "0197f524-7a00-7000-8000-000000000107";
const STOP_RUN: &str = "0197f524-7a00-7000-8000-000000000108";
const CLEANUP_RUN: &str = "0197f524-7a00-7000-8000-000000000109";

fn vyane() -> Command {
    Command::new(VYANE_BIN)
}

fn write_config(directory: &TempDir) -> PathBuf {
    let path = directory.path().join("config.toml");
    fs::write(
        &path,
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
    path
}

fn write_native_config(directory: &TempDir, endpoint: &str) -> PathBuf {
    let path = directory.path().join("config.toml");
    fs::write(
        &path,
        format!(
            r#"
            [providers.native_http]
            base_url = "{endpoint}"
            auth_style = "bearer"
            protocol = "openai_chat"
            default_model = "native-test-model"

            [profiles.native]
            provider = "native_http"
            protocol = "openai_chat"
            model = "native-test-model"
            "#
        ),
    )
    .unwrap();
    path
}

fn write_claude(directory: &TempDir, body: &str) -> PathBuf {
    let path = directory.path().join("claude");
    fs::write(&path, format!("#!/bin/sh\nset -eu\n{body}\n")).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    path
}

struct DaemonGuard {
    data_dir: PathBuf,
    running: bool,
}

impl DaemonGuard {
    fn start(data_dir: &Path, config: &Path, path: &Path) -> Self {
        let output = vyane()
            .env("VYANE_DATA_DIR", data_dir)
            .env("PATH", path)
            .arg("--config")
            .arg(config)
            .args(["daemon", "start", "--addr", "127.0.0.1:0"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "daemon start failed: {}; log: {}",
            String::from_utf8_lossy(&output.stderr),
            fs::read_to_string(data_dir.join("daemon.log")).unwrap_or_default()
        );
        Self {
            data_dir: data_dir.to_path_buf(),
            running: true,
        }
    }

    fn stop(&mut self) -> Output {
        let output = stop_daemon(&self.data_dir).unwrap();
        if output.status.success() {
            self.running = false;
        }
        output
    }

    fn kill(&mut self) {
        let descriptor: Value =
            serde_json::from_slice(&fs::read(self.data_dir.join("daemon.json")).unwrap()).unwrap();
        let pid = descriptor["pid"].as_i64().unwrap().to_string();
        let status = std::process::Command::new("/bin/kill")
            .args(["-KILL", &pid])
            .status()
            .unwrap();
        assert!(status.success());
        self.running = false;
    }
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        if self.running {
            let _ = stop_daemon(&self.data_dir);
        }
    }
}

fn stop_daemon(data_dir: &Path) -> std::io::Result<Output> {
    let mut command = vyane();
    command
        .env("VYANE_DATA_DIR", data_dir)
        .args(["daemon", "stop"])
        .timeout(Duration::from_secs(90));
    command.output()
}

fn control(data_dir: &Path) -> (String, String) {
    let descriptor: Value =
        serde_json::from_slice(&fs::read(data_dir.join("daemon.json")).unwrap()).unwrap();
    let addr = descriptor["addr"].as_str().unwrap().to_string();
    let token = fs::read_to_string(data_dir.join("daemon.token"))
        .unwrap()
        .trim()
        .to_string();
    (format!("http://{addr}"), token)
}

async fn submit(data_dir: &Path, run_id: &str, timeout: u64) -> reqwest::Response {
    let (base, token) = control(data_dir);
    reqwest::Client::new()
        .post(format!("{base}/v1/agent-runs"))
        .bearer_auth(token)
        .json(&json!({
            "run_id": run_id,
            "task": "return a bounded answer",
            "target": "builder",
            "timeout_seconds": timeout
        }))
        .send()
        .await
        .unwrap()
}

async fn submit_native(data_dir: &Path, run_id: &str, workdir: &Path) -> reqwest::Response {
    let (base, token) = control(data_dir);
    reqwest::Client::new()
        .post(format!("{base}/v1/agent-runs"))
        .bearer_auth(token)
        .json(&json!({
            "run_id": run_id,
            "task": "return native answer",
            "target": "native",
            "sandbox": "read_only",
            "workdir": workdir,
            "execution_backend": "native_in_process"
        }))
        .send()
        .await
        .unwrap()
}

async fn get_json(data_dir: &Path, suffix: &str) -> (reqwest::StatusCode, Value) {
    let (base, token) = control(data_dir);
    let response = reqwest::Client::new()
        .get(format!("{base}{suffix}"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.json().await.unwrap();
    (status, body)
}

async fn post_json(data_dir: &Path, suffix: &str) -> (reqwest::StatusCode, Value) {
    let (base, token) = control(data_dir);
    let response = reqwest::Client::new()
        .post(format!("{base}{suffix}"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.json().await.unwrap();
    (status, body)
}

async fn terminal(data_dir: &Path, run_id: &str, budget: Duration) -> Value {
    let deadline = Instant::now() + budget;
    loop {
        let (_, body) = get_json(data_dir, &format!("/v1/agent-runs/{run_id}")).await;
        if !matches!(
            body["state"].as_str(),
            Some("queued" | "starting" | "running" | "cancelling")
        ) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "AgentRun did not become terminal: {body}; log: {}",
            fs::read_to_string(data_dir.join("daemon.log")).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn regular_files_below(root: &Path) -> Vec<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        let Ok(entries) = fs::read_dir(path) else {
            continue;
        };
        for entry in entries {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                files.push(entry.path());
            }
        }
    }
    files
}

#[tokio::test]
async fn process_agent_success_is_idempotent_and_publishes_exact_output() {
    let config_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let bin_dir = TempDir::new().unwrap();
    let invocation = bin_dir.path().join("invocations");
    write_claude(
        &bin_dir,
        &format!(
            "printf x >> '{}'\nprintf '%s\\n' '{{\"result\":\"agent answer\",\"session_id\":\"ignored\",\"usage\":{{\"input_tokens\":1,\"output_tokens\":2}}}}'",
            invocation.display()
        ),
    );
    let config = write_config(&config_dir);
    let mut daemon = DaemonGuard::start(data_dir.path(), &config, bin_dir.path());

    let response = submit(data_dir.path(), SUCCESS_RUN, 30).await;
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let first: Value = response.json().await.unwrap();
    assert_eq!(first["run_id"], SUCCESS_RUN);

    let done = terminal(data_dir.path(), SUCCESS_RUN, Duration::from_secs(15)).await;
    assert_eq!(done["state"], "succeeded");
    assert_eq!(done["completion_status"], "committed");
    let deadline = Instant::now() + Duration::from_secs(5);
    let output = loop {
        let (status, body) = get_json(
            data_dir.path(),
            &format!("/v1/agent-runs/{SUCCESS_RUN}/output"),
        )
        .await;
        if status == reqwest::StatusCode::OK {
            break body;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(output["output"], "agent answer");

    let retry = submit(data_dir.path(), SUCCESS_RUN, 30).await;
    assert_eq!(retry.status(), reqwest::StatusCode::ACCEPTED);
    assert_eq!(retry.json::<Value>().await.unwrap()["state"], "succeeded");
    assert_eq!(fs::read(&invocation).unwrap(), b"x");
    assert!(
        regular_files_below(&data_dir.path().join("agent-inputs")).is_empty(),
        "an exact terminal retry must not recreate a durable prompt spool"
    );
    assert!(daemon.stop().status.success());
}

#[tokio::test]
async fn native_agent_submit_uses_the_shared_resident_lane() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "native-test-model",
            "choices": [{
                "message": {"role": "assistant", "content": "native answer"},
                "finish_reason": "stop"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;
    let config_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let bin_dir = TempDir::new().unwrap();
    let config = write_native_config(&config_dir, &server.uri());
    let workdir = data_dir.path().join("native-workdir");
    fs::create_dir(&workdir).unwrap();
    let mut daemon = DaemonGuard::start(data_dir.path(), &config, bin_dir.path());

    let response = submit_native(
        data_dir.path(),
        "0197f524-7a00-7000-8000-000000000110",
        &workdir,
    )
    .await;
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let run_id = "0197f524-7a00-7000-8000-000000000110";
    let done = terminal(data_dir.path(), run_id, Duration::from_secs(15)).await;
    assert_eq!(done["state"], "succeeded");
    assert_eq!(done["completion_status"], "committed");
    let deadline = Instant::now() + Duration::from_secs(5);
    let output = loop {
        let (status, body) =
            get_json(data_dir.path(), &format!("/v1/agent-runs/{run_id}/output")).await;
        if status == reqwest::StatusCode::OK {
            break body;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(output["output"], "native answer");
    assert!(daemon.stop().status.success());
}

#[tokio::test]
async fn native_agent_cancel_uses_the_exact_in_process_controller() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(5))
                .set_body_json(json!({
                    "model": "native-test-model",
                    "choices": [{
                        "message": {"role": "assistant", "content": "late answer"},
                        "finish_reason": "stop"
                    }]
                })),
        )
        .mount(&server)
        .await;
    let config_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let bin_dir = TempDir::new().unwrap();
    let config = write_native_config(&config_dir, &server.uri());
    let workdir = data_dir.path().join("native-cancel-workdir");
    fs::create_dir(&workdir).unwrap();
    let mut daemon = DaemonGuard::start(data_dir.path(), &config, bin_dir.path());

    let run_id = "0197f524-7a00-7000-8000-000000000111";
    assert_eq!(
        submit_native(data_dir.path(), run_id, &workdir)
            .await
            .status(),
        reqwest::StatusCode::ACCEPTED
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (status, body) =
        post_json(data_dir.path(), &format!("/v1/agent-runs/{run_id}/cancel")).await;
    assert_eq!(status, reqwest::StatusCode::OK, "cancel body: {body}");
    let done = terminal(data_dir.path(), run_id, Duration::from_secs(10)).await;
    assert_eq!(done["state"], "cancelled");
    assert!(daemon.stop().status.success());
}

#[tokio::test]
async fn read_only_explicit_workdir_is_the_fake_harness_cwd() {
    let config_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let bin_dir = TempDir::new().unwrap();
    let workdir = TempDir::new().unwrap();
    let observed_cwd = bin_dir.path().join("observed-cwd");
    write_claude(
        &bin_dir,
        &format!(
            "/bin/pwd > '{}'; printf '%s\\n' '{{\"result\":\"cwd observed\"}}'",
            observed_cwd.display()
        ),
    );
    let config = write_config(&config_dir);
    let mut daemon = DaemonGuard::start(data_dir.path(), &config, bin_dir.path());
    let (base, token) = control(data_dir.path());
    let response = reqwest::Client::new()
        .post(format!("{base}/v1/agent-runs"))
        .bearer_auth(token)
        .json(&json!({
            "run_id": READ_ONLY_RUN,
            "task": "report the working directory",
            "target": "builder",
            "sandbox": "read_only",
            "workdir": workdir.path(),
            "timeout_seconds": 30
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let done = terminal(data_dir.path(), READ_ONLY_RUN, Duration::from_secs(15)).await;
    assert_eq!(done["state"], "succeeded");
    assert_eq!(
        fs::canonicalize(fs::read_to_string(observed_cwd).unwrap().trim()).unwrap(),
        fs::canonicalize(workdir.path()).unwrap()
    );
    assert!(daemon.stop().status.success());
}

#[tokio::test]
async fn exit_zero_without_terminal_result_fails_without_completion() {
    let config_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let bin_dir = TempDir::new().unwrap();
    write_claude(
        &bin_dir,
        "printf '%s\\n' '{\"type\":\"assistant\",\"message\":{\"content\":[]}}'",
    );
    let config = write_config(&config_dir);
    let mut daemon = DaemonGuard::start(data_dir.path(), &config, bin_dir.path());
    assert!(
        submit(data_dir.path(), MISSING_RUN, 30)
            .await
            .status()
            .is_success()
    );
    let done = terminal(data_dir.path(), MISSING_RUN, Duration::from_secs(15)).await;
    assert_eq!(done["state"], "failed");
    assert_eq!(done["failure_code"], "dispatch_failed");
    assert!(done.get("completion_status").is_none());
    let (status, _) = get_json(
        data_dir.path(),
        &format!("/v1/agent-runs/{MISSING_RUN}/output"),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    assert!(daemon.stop().status.success());
}

#[tokio::test]
async fn cancel_stops_the_exact_process_group_and_settles_cancelled() {
    let config_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let bin_dir = TempDir::new().unwrap();
    let pid_file = bin_dir.path().join("pids");
    write_claude(
        &bin_dir,
        &format!(
            "trap '' TERM\n/bin/sleep 300 &\nprintf '%s %s\\n' \"$$\" \"$!\" > '{}'\nwhile :; do /bin/sleep 1; done",
            pid_file.display()
        ),
    );
    let config = write_config(&config_dir);
    let mut daemon = DaemonGuard::start(data_dir.path(), &config, bin_dir.path());
    assert!(
        submit(data_dir.path(), CANCEL_RUN, 60)
            .await
            .status()
            .is_success()
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while !pid_file.exists() {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (_, cancelled) = post_json(
        data_dir.path(),
        &format!("/v1/agent-runs/{CANCEL_RUN}/cancel"),
    )
    .await;
    assert_eq!(cancelled["state"], "cancelled");
    let (retry_status, retry) = post_json(
        data_dir.path(),
        &format!("/v1/agent-runs/{CANCEL_RUN}/cancel"),
    )
    .await;
    assert_eq!(retry_status, reqwest::StatusCode::OK);
    assert_eq!(retry["state"], "cancelled");
    assert_eq!(
        fs::read_dir(data_dir.path().join("agent-controllers"))
            .unwrap()
            .count(),
        0,
        "successful cancellation retained controller evidence"
    );
    for pid in fs::read_to_string(&pid_file).unwrap().split_whitespace() {
        let proc_path = PathBuf::from(format!("/proc/{pid}"));
        let deadline = Instant::now() + Duration::from_secs(3);
        while proc_path.exists() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!proc_path.exists(), "cancel left process {pid} alive");
    }
    assert!(daemon.stop().status.success());
}

#[tokio::test]
async fn daemon_stop_leaves_no_orphan_from_a_term_ignoring_process_group() {
    let config_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let bin_dir = TempDir::new().unwrap();
    let pid_file = bin_dir.path().join("stop-pids");
    write_claude(
        &bin_dir,
        &format!(
            "trap '' TERM\n/bin/sleep 300 &\nprintf '%s %s\\n' \"$$\" \"$!\" > '{}'\nwhile :; do /bin/sleep 1; done",
            pid_file.display()
        ),
    );
    let config = write_config(&config_dir);
    let mut daemon = DaemonGuard::start(data_dir.path(), &config, bin_dir.path());
    assert!(
        submit(data_dir.path(), STOP_RUN, 60)
            .await
            .status()
            .is_success()
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while !pid_file.exists() {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let pids = fs::read_to_string(&pid_file).unwrap();
    assert!(daemon.stop().status.success());

    for pid in pids.split_whitespace() {
        let proc_path = PathBuf::from(format!("/proc/{pid}"));
        let deadline = Instant::now() + Duration::from_secs(3);
        while proc_path.exists() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!proc_path.exists(), "daemon stop left process {pid} alive");
    }
}

#[tokio::test]
async fn restart_recovers_exact_controller_without_replay() {
    let config_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let bin_dir = TempDir::new().unwrap();
    let invocation = bin_dir.path().join("invocations");
    write_claude(
        &bin_dir,
        &format!(
            "printf x >> '{}'\ntrap '' TERM\nwhile :; do /bin/sleep 1; done",
            invocation.display()
        ),
    );
    let config = write_config(&config_dir);
    let mut first = DaemonGuard::start(data_dir.path(), &config, bin_dir.path());
    assert!(
        submit(data_dir.path(), RESTART_RUN, 2)
            .await
            .status()
            .is_success()
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while !invocation.exists() {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    first.kill();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut second = DaemonGuard::start(data_dir.path(), &config, bin_dir.path());
    let done = terminal(data_dir.path(), RESTART_RUN, Duration::from_secs(15)).await;
    assert_ne!(done["state"], "succeeded");
    assert_eq!(fs::read(&invocation).unwrap(), b"x");
    assert!(second.stop().status.success());
}

#[tokio::test]
async fn restart_cleans_terminal_controller_left_after_database_commit() {
    let config_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let bin_dir = TempDir::new().unwrap();
    let started = bin_dir.path().join("cleanup-started");
    write_claude(
        &bin_dir,
        &format!(
            "printf x > '{}'
trap '' TERM
while :; do /bin/sleep 1; done",
            started.display()
        ),
    );
    let config = write_config(&config_dir);
    let mut first = DaemonGuard::start(data_dir.path(), &config, bin_dir.path());
    assert!(
        submit(data_dir.path(), CLEANUP_RUN, 60)
            .await
            .status()
            .is_success()
    );
    let controller_dir = data_dir.path().join("agent-controllers");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !started.exists()
        || fs::read_dir(&controller_dir)
            .map(|entries| entries.count() == 0)
            .unwrap_or(true)
    {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let controller_path = fs::read_dir(&controller_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut residue: Value = serde_json::from_slice(&fs::read(&controller_path).unwrap()).unwrap();
    let (_, cancelled) = post_json(
        data_dir.path(),
        &format!("/v1/agent-runs/{CLEANUP_RUN}/cancel"),
    )
    .await;
    assert_eq!(cancelled["state"], "cancelled");
    assert!(!controller_path.exists());

    // Recreate the exact durable state at the crash boundary: terminal DB
    // truth has cleared its controller, while already-quiesced sidecar proof
    // has not yet been unlinked.
    residue["state"] = Value::String("stopped".into());
    fs::write(&controller_path, serde_json::to_vec(&residue).unwrap()).unwrap();
    fs::set_permissions(&controller_path, fs::Permissions::from_mode(0o600)).unwrap();
    first.kill();

    let mut second = DaemonGuard::start(data_dir.path(), &config, bin_dir.path());
    assert_eq!(fs::read_dir(&controller_dir).unwrap().count(), 0);
    assert!(second.stop().status.success());
}

#[tokio::test]
async fn missing_target_binary_settles_dispatch_failed() {
    let config_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let bin_dir = TempDir::new().unwrap();
    let binary = write_claude(&bin_dir, "exit 0");
    let config = write_config(&config_dir);
    let mut daemon = DaemonGuard::start(data_dir.path(), &config, bin_dir.path());
    fs::remove_file(binary).unwrap();

    assert!(
        submit(data_dir.path(), SPAWN_RUN, 30)
            .await
            .status()
            .is_success()
    );
    let done = terminal(data_dir.path(), SPAWN_RUN, Duration::from_secs(15)).await;
    assert_eq!(done["state"], "failed");
    // The trusted lifecycle sentinel itself starts successfully; the missing
    // target exits inside that sentinel and is therefore a dispatch failure.
    assert_eq!(done["failure_code"], "dispatch_failed");
    assert!(daemon.stop().status.success());
}

#[tokio::test]
async fn submit_rejects_caller_owned_identity_fields() {
    let config_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let bin_dir = TempDir::new().unwrap();
    write_claude(&bin_dir, "printf '%s\\n' '{\"result\":\"unused\"}'");
    let config = write_config(&config_dir);
    let mut daemon = DaemonGuard::start(data_dir.path(), &config, bin_dir.path());
    let (base, token) = control(data_dir.path());
    let response = reqwest::Client::new()
        .post(format!("{base}/v1/agent-runs"))
        .bearer_auth(token)
        .json(&json!({
            "run_id": SPAWN_RUN,
            "task": "unused",
            "target": "builder",
            "owner": "caller-selected"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert!(daemon.stop().status.success());
}
