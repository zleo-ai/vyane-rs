//! End-to-end acceptance coverage for the resident workflow daemon.
//!
//! The test drives the real `vyane` binary, including detached daemon startup,
//! authenticated CLI submission/status calls, durable task/journal state, and
//! graceful shutdown. The target is a local Wiremock server, so no external
//! network or installed harness is required.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::process::Stdio;
use std::time::{Duration, Instant};

use assert_cmd::Command;
use rmcp::model::{CallToolRequestParam, CallToolResult};
use serde_json::{Value, json};
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const VYANE_BIN: &str = env!("CARGO_BIN_EXE_vyane");
const EXPLICIT_RUN_ID: &str = "0197f524-7a00-7000-8000-000000000001";
const MCP_RUN_ID: &str = "0197f524-7a00-7000-8000-000000000002";

fn vyane() -> Command {
    Command::new(VYANE_BIN)
}

fn write_config(dir: &TempDir, server: &MockServer) -> PathBuf {
    let path = dir.path().join("config.toml");
    fs::write(
        &path,
        format!(
            r#"
            [providers.test]
            base_url = "{}"
            api_key_env = "VYANE_DAEMON_ACCEPTANCE_KEY"
            auth_style = "bearer"
            protocol = "openai_chat"
            default_model = "test-model"

            [profiles.worker]
            provider = "test"
            protocol = "openai_chat"
            harness = "none"
            model = "test-model"
            tier = "economy"

            [profiles.worker.params]
            effort = "low"
            "#,
            server.uri()
        ),
    )
    .expect("write daemon acceptance config");
    path
}

fn write_workflow(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("workflow.toml");
    fs::write(
        &path,
        r#"
        [workflow]
        name = "resident-daemon-acceptance"

        [[step]]
        id = "answer"
        target = "auto"
        prompt = "answer {{vars.topic}}"
        [step.route]
        candidates = ["worker"]
        effort = "high"
        "#,
    )
    .expect("write daemon acceptance workflow");
    path
}

/// Owns the test daemon until an explicit successful stop. During panic
/// unwinding it makes one best-effort stop attempt before the temporary data
/// directory is removed.
struct DaemonGuard {
    data_dir: PathBuf,
    running: bool,
}

impl DaemonGuard {
    fn start(data_dir: &Path, config: &Path) -> Self {
        vyane()
            .env("VYANE_DATA_DIR", data_dir)
            .env("VYANE_DAEMON_ACCEPTANCE_KEY", "sk-test")
            .arg("--config")
            .arg(config)
            .args(["daemon", "start", "--addr", "127.0.0.1:0"])
            .assert()
            .success();
        Self {
            data_dir: data_dir.to_path_buf(),
            running: true,
        }
    }

    fn stop(&mut self) -> Output {
        let output = stop_daemon(&self.data_dir).expect("run daemon stop");
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

fn stop_daemon(data_dir: &Path) -> std::io::Result<Output> {
    let mut command = vyane();
    command
        .env("VYANE_DATA_DIR", data_dir)
        .args(["daemon", "stop"])
        .timeout(Duration::from_secs(90));
    command.output()
}

fn submit_workflow(data_dir: &Path, workflow: &Path) -> Value {
    let output = vyane()
        .env("VYANE_DATA_DIR", data_dir)
        .arg("workflow")
        .arg("submit")
        .arg(workflow)
        .args(["--id", EXPLICIT_RUN_ID, "--var", "topic=daemon", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    serde_json::from_slice(&output).expect("workflow submission JSON")
}

fn workflow_status(data_dir: &Path) -> Option<Value> {
    let output = vyane()
        .env("VYANE_DATA_DIR", data_dir)
        .args(["workflow", "status", EXPLICIT_RUN_ID, "--json"])
        .output()
        .expect("run workflow status");
    if !output.status.success() || output.stdout.is_empty() {
        return None;
    }
    serde_json::from_slice(&output.stdout).ok()
}

fn poll_workflow_terminal(data_dir: &Path, budget: Duration) -> Value {
    let deadline = Instant::now() + budget;
    let mut last_state = "not yet observable".to_string();
    loop {
        if let Some(view) = workflow_status(data_dir) {
            let state = view["task"]["state"].as_str().unwrap_or("unknown");
            if !matches!(state, "queued" | "running" | "cancelling") {
                return view;
            }
            last_state = state.to_string();
        }
        assert!(
            Instant::now() < deadline,
            "daemon workflow did not finish within {budget:?}; last state = {last_state}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[tokio::test]
async fn submitted_workflow_outlives_cli_and_explicit_id_retry_is_idempotent() {
    let server = MockServer::start().await;
    // Keep the target in flight long enough to prove that `workflow submit`
    // returned after durable admission rather than after executing the step.
    const TARGET_DELAY: Duration = Duration::from_secs(10);
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(TARGET_DELAY)
                .set_body_json(json!({
                    "id": "chatcmpl-daemon-acceptance",
                    "model": "test-model",
                    "choices": [{
                        "message": { "role": "assistant", "content": "daemon answer" },
                        "finish_reason": "stop"
                    }],
                    "usage": { "prompt_tokens": 3, "completion_tokens": 2 }
                })),
        )
        .expect(1)
        .mount(&server)
        .await;

    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &server);
    let workflow = write_workflow(&config_dir);
    let mut daemon = DaemonGuard::start(data_dir.path(), &config);

    let submitted = submit_workflow(data_dir.path(), &workflow);
    assert_eq!(submitted["task"]["id"], EXPLICIT_RUN_ID);
    assert!(
        matches!(
            submitted["task"]["state"].as_str(),
            Some("queued" | "running")
        ),
        "submission CLI must exit while the delayed workflow remains active: {submitted}"
    );
    assert_eq!(submitted["journal"]["id"], EXPLICIT_RUN_ID);
    assert!(
        matches!(
            submitted["journal"]["status"].as_str(),
            Some("pending" | "running")
        ),
        "initial journal must be non-terminal: {submitted}"
    );

    let terminal = poll_workflow_terminal(data_dir.path(), Duration::from_secs(30));
    assert_eq!(terminal["task"]["id"], EXPLICIT_RUN_ID);
    assert_eq!(terminal["task"]["state"], "succeeded");
    assert_eq!(terminal["journal"]["id"], EXPLICIT_RUN_ID);
    assert_eq!(terminal["journal"]["status"], "completed");
    assert_eq!(terminal["journal"]["steps"]["success"], 1);

    // Retrying the identical intent with the same caller-generated UUIDv7
    // returns the existing durable task/journal and must not replay the target.
    let retried = submit_workflow(data_dir.path(), &workflow);
    assert_eq!(retried["task"]["id"], terminal["task"]["id"]);
    assert_eq!(retried["task"]["state"], "succeeded");
    assert_eq!(retried["journal"]["id"], terminal["journal"]["id"]);
    assert_eq!(retried["journal"]["status"], "completed");

    let stopped = daemon.stop();
    assert!(
        stopped.status.success(),
        "daemon stop failed: {}",
        String::from_utf8_lossy(&stopped.stderr)
    );
    let requests = server.received_requests().await.expect("request recording");
    assert_eq!(
        requests.len(),
        1,
        "idempotent retry must not dispatch the workflow again"
    );
    let request: Value = serde_json::from_slice(&requests[0].body).expect("request JSON");
    assert_eq!(request["model"], "test-model");
    assert_eq!(request["reasoning_effort"], "high");

    let ledger = fs::read_to_string(data_dir.path().join("ledger.jsonl")).expect("read ledger");
    let records = ledger
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("ledger record"))
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["labels"]["routing.profile"], "worker");
    assert_eq!(records[0]["labels"]["routing.effort"], "high");
}

#[tokio::test]
async fn mcp_workflow_tools_use_the_authenticated_resident_daemon() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "chatcmpl-mcp-daemon-acceptance",
            "model": "test-model",
            "choices": [{
                "message": { "role": "assistant", "content": "mcp daemon answer" },
                "finish_reason": "stop"
            }],
            "usage": { "prompt_tokens": 3, "completion_tokens": 2 }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let config_dir = TempDir::new()?;
    let data_dir = TempDir::new()?;
    let config = write_config(&config_dir, &server);
    let workflow = write_workflow(&config_dir);
    let mut daemon = DaemonGuard::start(data_dir.path(), &config);

    let mut child = tokio::process::Command::new(VYANE_BIN)
        .env("VYANE_DATA_DIR", data_dir.path())
        .env("VYANE_DAEMON_ACCEPTANCE_KEY", "sk-test")
        .arg("--config")
        .arg(&config)
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = child.stdout.take().expect("piped MCP stdout");
    let stdin = child.stdin.take().expect("piped MCP stdin");
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), (stdout, stdin)).await?;

    let names = client
        .list_all_tools()
        .await?
        .into_iter()
        .map(|tool| tool.name.to_string())
        .collect::<Vec<_>>();
    assert_eq!(names.len(), 9);
    assert!(names.iter().any(|name| name == "vyane_workflow_submit"));

    let source = fs::read_to_string(workflow)?;
    let submitted = mcp_call(
        &client,
        "vyane_workflow_submit",
        json!({
            "caller_id": MCP_RUN_ID,
            "workflow_toml": source,
            "vars": { "topic": "daemon" }
        }),
    )
    .await?;
    assert_eq!(submitted["caller_id"], MCP_RUN_ID);
    assert!(matches!(
        submitted["state"].as_str(),
        Some("queued" | "running" | "succeeded")
    ));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let terminal = loop {
        let status = mcp_call(
            &client,
            "vyane_workflow_status",
            json!({ "caller_id": MCP_RUN_ID }),
        )
        .await?;
        if status["state"] == "succeeded" {
            break status;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "MCP workflow did not reach success: {status}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(terminal["caller_id"], MCP_RUN_ID);
    assert!(terminal.get("owner").is_none());
    assert!(terminal.get("controller").is_none());

    for _ in 0..2 {
        let cancelled = mcp_call(
            &client,
            "vyane_workflow_cancel",
            json!({ "caller_id": MCP_RUN_ID }),
        )
        .await?;
        assert_eq!(cancelled["state"], "succeeded");
    }

    client.cancel().await?;
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait()).await??;
    assert!(status.success(), "MCP child did not exit cleanly: {status}");
    assert!(daemon.stop().status.success());
    Ok(())
}

async fn mcp_call(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
    name: &str,
    arguments: Value,
) -> anyhow::Result<Value> {
    let result = client
        .call_tool(CallToolRequestParam {
            name: name.to_owned().into(),
            arguments: Some(arguments.as_object().expect("object arguments").clone()),
        })
        .await?;
    Ok(mcp_result_payload(result))
}

fn mcp_result_payload(result: CallToolResult) -> Value {
    let wire = serde_json::to_value(result).expect("serialize MCP result");
    serde_json::from_str(
        wire["content"][0]["text"]
            .as_str()
            .expect("MCP JSON text result"),
    )
    .expect("parse MCP JSON result")
}
