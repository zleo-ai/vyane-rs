use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::{Value, json};
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

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

fn failover_config(primary: &MockServer, backup: &MockServer) -> String {
    format!(
        r#"
        [providers.primary]
        base_url = "{}"
        api_key_env = "VYANE_CLI_TEST_KEY"
        auth_style = "bearer"
        protocol = "openai_chat"
        default_model = "primary-model"

        [providers.backup]
        base_url = "{}"
        api_key_env = "VYANE_CLI_TEST_KEY"
        auth_style = "bearer"
        protocol = "openai_chat"
        default_model = "backup-model"

        [profiles.resilient]
        provider = "primary"
        protocol = "openai_chat"
        harness = "none"
        model = "primary-model"
        failover = ["backup/backup-model"]
        "#,
        primary.uri(),
        backup.uri()
    )
}

fn responses_config_for(server: &MockServer) -> String {
    format!(
        r#"
        [providers.test]
        base_url = "{}"
        api_key_env = "VYANE_CLI_TEST_KEY"
        auth_style = "bearer"
        protocol = "openai_responses"
        default_model = "test-model"

        [profiles.review]
        provider = "test"
        protocol = "openai_responses"
        harness = "none"
        model = "test-model"
        "#,
        server.uri()
    )
}

fn auto_route_config_for(server: &MockServer) -> String {
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

        [profiles.cheap.params]
        effort = "low"

        [profiles.frontier]
        provider = "test"
        protocol = "openai_chat"
        harness = "none"
        model = "frontier-model"
        tier = "frontier"
        tags = ["architecture"]

        [profiles.frontier.params]
        effort = "xhigh"
        "#,
        server.uri()
    )
}

fn workflow_effort_failover_config(primary: &MockServer, backup: &MockServer) -> String {
    format!(
        r#"
        [providers.primary]
        base_url = "{}"
        api_key_env = "VYANE_CLI_TEST_KEY"
        auth_style = "bearer"
        protocol = "openai_chat"
        default_model = "primary-model"

        [providers.backup]
        base_url = "{}"
        api_key_env = "VYANE_CLI_TEST_KEY"
        auth_style = "bearer"
        protocol = "openai_chat"
        default_model = "backup-model"

        [profiles.mainline]
        provider = "primary"
        protocol = "openai_chat"
        harness = "none"
        model = "primary-model"
        tier = "mainline"
        failover = ["backup"]

        [profiles.mainline.params]
        effort = "low"

        [profiles.backup]
        provider = "backup"
        protocol = "openai_chat"
        harness = "none"
        model = "backup-model"
        tier = "economy"

        [profiles.backup.params]
        effort = "xhigh"
        "#,
        primary.uri(),
        backup.uri()
    )
}

/// A profile that resolves to a `CliWrap` (harness) target, never `DirectHttp`.
/// The fake Claude executable used by the streaming acceptance test resolves
/// through this target's scrubbed `PATH`.
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

#[cfg(unix)]
fn write_fake_streaming_claude(dir: &TempDir) -> std::path::PathBuf {
    let bin = dir.path().join("claude");
    fs::write(
        &bin,
        r#"#!/bin/sh
printf '%s\n' '{"type":"assistant","message":{"content":[{"type":"text","text":"harness delta"}]}}'
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"harness final","session_id":"harness-session","usage":{"input_tokens":1,"output_tokens":2}}'
"#,
    )
    .expect("write fake claude");
    let mut permissions = fs::metadata(&bin)
        .expect("fake claude metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&bin, permissions).expect("make fake claude executable");
    bin
}

fn write_config(dir: &TempDir, text: &str) -> std::path::PathBuf {
    let path = dir.path().join("config.toml");
    fs::write(&path, text).expect("write config");
    path
}

async fn mock_openai(server: &MockServer, status: u16, answer: &str) {
    let template = if status == 200 {
        ResponseTemplate::new(status).set_body_json(json!({
            "id": "chatcmpl-test",
            "model": "test-model",
            "choices": [{
                "message": { "role": "assistant", "content": answer },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 3,
                "completion_tokens": 2
            }
        }))
    } else {
        ResponseTemplate::new(status)
    };

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(template)
        .mount(server)
        .await;
}

async fn mock_openai_workflow(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(|req: &Request| {
            let body = String::from_utf8_lossy(&req.body);
            let answer = if body.contains("review draft answer") {
                "review answer"
            } else {
                "draft answer"
            };
            ResponseTemplate::new(200).set_body_json(json!({
                "id": "chatcmpl-test",
                "model": "test-model",
                "choices": [{
                    "message": { "role": "assistant", "content": answer },
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": 3,
                    "completion_tokens": 2
                }
            }))
        })
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

fn write_workflow(dir: &TempDir, text: &str) -> std::path::PathBuf {
    let path = dir.path().join("workflow.toml");
    fs::write(&path, text).expect("write workflow");
    path
}

#[tokio::test]
async fn dispatch_unknown_target_exits_with_config_code() {
    let server = MockServer::start().await;
    let config_dir = TempDir::new().expect("config tempdir");
    let config = write_config(&config_dir, &config_for(&server));

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .arg("--config")
        .arg(config)
        .arg("dispatch")
        .arg("hello")
        .arg("--target")
        .arg("no-such-profile")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("config error"));
}

#[tokio::test]
async fn check_with_temp_config_lists_profile() {
    let server = MockServer::start().await;
    let config_dir = TempDir::new().expect("config tempdir");
    let config = write_config(&config_dir, &config_for(&server));

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .arg("--config")
        .arg(config)
        .arg("check")
        .assert()
        .success()
        .stdout(predicate::str::contains("review"));
}

#[tokio::test]
async fn dispatch_openai_chat_writes_success_ledger_and_json_parses() {
    let server = MockServer::start().await;
    mock_openai(&server, 200, "wiremock answer").await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &config_for(&server));

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["dispatch", "say hi", "--target", "review"])
        .assert()
        .success()
        .stdout(predicate::str::contains("wiremock answer"));

    let records = ledger_records(data_dir.path());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["status"], "success");

    let output = vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(config)
        .args(["dispatch", "say json", "--target", "review", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let parsed: Value = serde_json::from_slice(&output).expect("json dispatch output");
    assert_eq!(parsed["record"]["status"], "success");
    assert_eq!(parsed["output"], "wiremock answer");
}

#[tokio::test]
async fn dispatch_auto_routes_executes_effort_and_records_decision() {
    let server = MockServer::start().await;
    mock_openai(&server, 200, "auto answer").await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &auto_route_config_for(&server));

    for args in [
        vec!["dispatch", "say hello", "--target", "auto"],
        vec![
            "dispatch",
            "design architecture",
            "--target",
            "auto",
            "--label",
            "routing.tier=frontier",
        ],
        vec![
            "dispatch",
            "design architecture",
            "--target",
            "auto",
            "--label",
            "routing.tier=frontier",
            "--label",
            "allow_frontier=true",
            "--label",
            "routing.allow_frontier=true",
            "--no-frontier",
        ],
    ] {
        vyane()
            .env("VYANE_CLI_TEST_KEY", "sk-test")
            .env("VYANE_DATA_DIR", data_dir.path())
            .arg("--config")
            .arg(&config)
            .args(args)
            .assert()
            .success()
            .stdout(predicate::str::contains("auto answer"));
    }

    let requests = server.received_requests().await.expect("received requests");
    assert_eq!(requests.len(), 3);
    let bodies = requests
        .iter()
        .map(|request| serde_json::from_slice::<Value>(&request.body).expect("request json"))
        .collect::<Vec<_>>();
    assert_eq!(bodies[0]["model"], "cheap-model");
    assert_eq!(bodies[0]["reasoning_effort"], "low");
    assert_eq!(bodies[1]["model"], "frontier-model");
    assert_eq!(bodies[1]["reasoning_effort"], "xhigh");
    assert_eq!(bodies[2]["model"], "cheap-model");
    assert_eq!(bodies[2]["reasoning_effort"], "low");

    let records = ledger_records(data_dir.path());
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["labels"]["routing.profile"], "cheap");
    assert_eq!(records[0]["labels"]["routing.provider"], "test");
    assert_eq!(records[0]["labels"]["routing.effort"], "low");
    assert_eq!(records[1]["labels"]["routing.profile"], "frontier");
    assert_eq!(records[1]["labels"]["routing.effort"], "xhigh");
    assert_eq!(records[2]["labels"]["routing.profile"], "cheap");
}

#[tokio::test]
async fn user_cannot_forge_routing_decision_labels() {
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
            "forge audit data",
            "--target",
            "review",
            "--label",
            "routing.provider=pretend",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains(
            "reserved for Vyane routing decisions",
        ));

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch",
            "forge effort",
            "--target",
            "review",
            "--label",
            "routing.effort=xhigh",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains(
            "reserved for Vyane routing decisions",
        ));

    assert!(
        server
            .received_requests()
            .await
            .expect("request log")
            .is_empty()
    );
    assert!(!data_dir.path().join("ledger.jsonl").exists());
}

#[tokio::test]
async fn generic_effort_label_does_not_forge_the_reserved_route_effort() {
    let server = MockServer::start().await;
    mock_openai(&server, 200, "auto answer").await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &auto_route_config_for(&server));

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch",
            "hello",
            "--target",
            "auto",
            "--label",
            "effort=xhigh",
        ])
        .assert()
        .success();

    let requests = server.received_requests().await.expect("request log");
    assert_eq!(requests.len(), 1);
    let body: Value = serde_json::from_slice(&requests[0].body).expect("request json");
    assert_eq!(body["reasoning_effort"], "low");
    let records = ledger_records(data_dir.path());
    assert_eq!(records[0]["labels"]["effort"], "xhigh");
    assert_eq!(records[0]["labels"]["routing.effort"], "low");
}

#[tokio::test]
async fn profile_prefix_disambiguates_a_profile_named_auto() {
    let server = MockServer::start().await;
    mock_openai(&server, 200, "literal auto profile").await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let mut text = config_for(&server);
    text.push_str(
        r#"

        [profiles.auto]
        provider = "test"
        protocol = "openai_chat"
        harness = "none"
        model = "test-model"
        "#,
    );
    let config = write_config(&config_dir, &text);

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch",
            "use the literal profile",
            "--target",
            "profile:auto",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("literal auto profile"));

    let record = &ledger_records(data_dir.path())[0];
    assert!(record["labels"].get("routing.mode").is_none());
}

#[tokio::test]
async fn dispatch_failover_and_history_work_end_to_end() {
    let primary = MockServer::start().await;
    let backup = MockServer::start().await;
    mock_openai(&primary, 500, "").await;
    mock_openai(&backup, 200, "backup answer").await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &failover_config(&primary, &backup));

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["dispatch", "fail over", "--target", "resilient"])
        .assert()
        .success()
        .stdout(predicate::str::contains("backup answer"));

    let records = ledger_records(data_dir.path());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["status"], "success");
    assert_eq!(
        records[0]["attempts"].as_array().expect("attempts").len(),
        2
    );

    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["history", "--limit", "5"])
        .assert()
        .success()
        .stdout(predicate::str::contains("backup/backup-model"))
        .stdout(predicate::str::contains("success"));

    let output = vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["history", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let parsed: Value = serde_json::from_slice(&output).expect("json history output");
    assert_eq!(parsed.as_array().expect("history array").len(), 1);
}

#[tokio::test]
async fn workflow_run_and_list_work_end_to_end() {
    let server = MockServer::start().await;
    mock_openai_workflow(&server).await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &config_for(&server));
    let workflow = write_workflow(
        &config_dir,
        r#"
        [workflow]
        name = "two-step"
        max_concurrency = 2

        [[step]]
        id = "draft"
        target = "review"
        prompt = "draft {{vars.topic}}"

        [[step]]
        id = "review"
        needs = ["draft"]
        target = "review"
        prompt = "review {{steps.draft.output}}"
        "#,
    );

    let output = vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .arg("workflow")
        .arg("run")
        .arg(&workflow)
        .args(["--var", "topic=wp07", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let parsed: Value = serde_json::from_slice(&output).expect("workflow json");
    assert_eq!(parsed["status"], "completed");
    assert_eq!(
        parsed["journal"]["steps"]["draft"]["output"],
        "draft answer"
    );
    assert_eq!(
        parsed["journal"]["steps"]["review"]["output"],
        "review answer"
    );
    let wf_run_id = parsed["wf_run_id"].as_str().expect("wf_run_id");
    let journal_path = data_dir
        .path()
        .join("workflows")
        .join(format!("{wf_run_id}.json"));
    assert!(journal_path.exists(), "workflow journal exists");

    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["workflow", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("two-step"))
        .stdout(predicate::str::contains("completed"));
}

#[tokio::test]
async fn workflow_replay_creates_a_new_run_without_reexecuting_recorded_successes() {
    let server = MockServer::start().await;
    mock_openai_workflow(&server).await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &config_for(&server));
    let workflow = write_workflow(
        &config_dir,
        r#"
        [workflow]
        name = "replay-cli"

        [[step]]
        id = "draft"
        target = "review"
        prompt = "draft"

        [[step]]
        id = "review"
        needs = ["draft"]
        target = "review"
        prompt = "review {{steps.draft.output}}"
        "#,
    );

    let first = vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["workflow", "run"])
        .arg(&workflow)
        .arg("--json")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let first: Value = serde_json::from_slice(&first).expect("first workflow json");
    let source_id = first["wf_run_id"].as_str().expect("source id");
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("recorded source workflow requests")
            .len(),
        2
    );

    let replay_output = vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["workflow", "replay", source_id, "--file"])
        .arg(&workflow)
        .arg("--json")
        .assert()
        .success()
        .get_output()
        .clone();
    let replay: Value =
        serde_json::from_slice(&replay_output.stdout).expect("replay workflow json");
    let replay_event: Value =
        serde_json::from_slice(&replay_output.stderr).expect("replay id event");
    assert_eq!(replay_event["event"], "workflow_replay_id");
    assert_eq!(replay_event["source_workflow_run_id"], first["wf_run_id"]);
    assert_eq!(replay_event["workflow_run_id"], replay["wf_run_id"]);
    assert_eq!(replay["status"], "completed");
    assert_ne!(replay["wf_run_id"], first["wf_run_id"]);
    assert_eq!(
        replay["journal"]["replay"]["source_wf_run_id"],
        first["wf_run_id"]
    );
    assert_eq!(
        replay["journal"]["replay"]["reused_step_ids"],
        json!(["draft", "review"])
    );
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("recorded replay workflow requests")
            .len(),
        2,
        "replay must not repeat journal-recorded all-success calls"
    );
}

#[test]
fn workflow_replay_rejects_var_before_config_or_journal_access() {
    let data_dir = TempDir::new().expect("data tempdir");
    let missing_config = data_dir.path().join("missing-config.toml");
    let missing_workflow = data_dir.path().join("missing-workflow.toml");

    vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&missing_config)
        .args([
            "workflow",
            "replay",
            "019f5bad-be63-7b72-9b85-c2b1e4b2e507",
            "--file",
        ])
        .arg(&missing_workflow)
        .args(["--var", "topic=changed", "--json"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("--var is not allowed"));

    assert!(!data_dir.path().join("workflows").exists());
}

#[tokio::test]
async fn workflow_auto_target_resolves_after_render_and_records_route() {
    let server = MockServer::start().await;
    mock_openai(&server, 200, "workflow auto").await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &auto_route_config_for(&server));
    let workflow = write_workflow(
        &config_dir,
        r#"
        [workflow]
        name = "auto-routing"
        max_concurrency = 1

        [[step]]
        id = "frontier"
        target = "auto"
        prompt = "design {{vars.subject}}"
        [step.route]
        tier = "frontier"
        tags = ["architecture"]

        [[step]]
        id = "guarded"
        needs = ["frontier"]
        target = "auto"
        prompt = "summarize {{steps.frontier.output}}"
        [step.route]
        tier = "frontier"
        allow_frontier = false
        "#,
    );

    let output = vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .arg("workflow")
        .arg("run")
        .arg(&workflow)
        .args(["--var", "subject=architecture", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let outcome: Value = serde_json::from_slice(&output).expect("workflow json");
    assert_eq!(outcome["status"], "completed");

    let requests = server.received_requests().await.expect("received requests");
    assert_eq!(requests.len(), 2);
    let first: Value = serde_json::from_slice(&requests[0].body).expect("first request");
    let second: Value = serde_json::from_slice(&requests[1].body).expect("second request");
    assert_eq!(first["model"], "frontier-model");
    assert_eq!(first["reasoning_effort"], "xhigh");
    assert_eq!(second["model"], "cheap-model");
    assert_eq!(second["reasoning_effort"], "low");

    let records = ledger_records(data_dir.path());
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["labels"]["workflow.step"], "frontier");
    assert_eq!(records[0]["labels"]["routing.profile"], "frontier");
    assert_eq!(records[1]["labels"]["workflow.step"], "guarded");
    assert_eq!(records[1]["labels"]["routing.profile"], "cheap");
}

#[tokio::test]
async fn workflow_explicit_effort_overrides_profile_and_tier_across_failover() {
    let primary = MockServer::start().await;
    let backup = MockServer::start().await;
    mock_openai(&primary, 500, "").await;
    mock_openai(&backup, 200, "backup answer").await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(
        &config_dir,
        &workflow_effort_failover_config(&primary, &backup),
    );
    let workflow = write_workflow(
        &config_dir,
        r#"
        [workflow]
        name = "explicit-effort-failover"

        [[step]]
        id = "routed"
        target = "auto"
        prompt = "run"
        [step.route]
        tier = "mainline"
        effort = "high"
        "#,
    );

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["workflow", "run"])
        .arg(&workflow)
        .assert()
        .success()
        .stdout(predicate::str::contains("completed"));

    let primary_requests = primary.received_requests().await.expect("primary requests");
    let backup_requests = backup.received_requests().await.expect("backup requests");
    assert!(!primary_requests.is_empty());
    assert_eq!(backup_requests.len(), 1);
    for request in primary_requests.iter().chain(&backup_requests) {
        let body: Value = serde_json::from_slice(&request.body).expect("request json");
        assert_eq!(body["reasoning_effort"], "high");
    }

    let records = ledger_records(data_dir.path());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["labels"]["routing.profile"], "mainline");
    assert_eq!(records[0]["labels"]["routing.tier"], "mainline");
    assert_eq!(records[0]["labels"]["routing.effort"], "high");
    assert_eq!(
        records[0]["attempts"].as_array().expect("attempts").len(),
        2
    );
}

#[tokio::test]
async fn invalid_workflow_effort_fails_before_journal_or_network_without_echo() {
    let server = MockServer::start().await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &auto_route_config_for(&server));
    let canary = "EFFORT_VALUE_MUST_NOT_BE_ECHOED";
    let workflow = write_workflow(
        &config_dir,
        &format!(
            r#"
        [workflow]
        name = "invalid-effort"

        [[step]]
        id = "invalid"
        target = "auto"
        prompt = "must never dispatch"
        [step.route]
        effort = "{canary}"
        "#
        ),
    );

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["workflow", "run"])
        .arg(&workflow)
        .assert()
        .code(2)
        .stderr(predicate::str::contains("config error"))
        .stderr(predicate::str::contains(canary).not());

    assert!(
        server
            .received_requests()
            .await
            .expect("request log")
            .is_empty()
    );
    assert!(!data_dir.path().join("workflows").exists());
    assert!(!data_dir.path().join("ledger.jsonl").exists());
}

#[tokio::test]
async fn workflow_effort_cannot_bypass_frontier_guard() {
    let server = MockServer::start().await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &auto_route_config_for(&server));
    let workflow = write_workflow(
        &config_dir,
        r#"
        [workflow]
        name = "guarded-effort"

        [[step]]
        id = "guarded"
        target = "auto"
        prompt = "must never dispatch"
        [step.route]
        candidates = ["frontier"]
        allow_frontier = false
        effort = "xhigh"
        "#,
    );

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["workflow", "run"])
        .arg(&workflow)
        .assert()
        .code(2)
        .stderr(predicate::str::contains("no eligible profiles"));

    assert!(
        server
            .received_requests()
            .await
            .expect("request log")
            .is_empty()
    );
    assert!(!data_dir.path().join("workflows").exists());
    assert!(!data_dir.path().join("ledger.jsonl").exists());
}

#[tokio::test]
async fn workflow_route_hints_on_explicit_target_fail_before_journal_or_network() {
    let server = MockServer::start().await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &config_for(&server));
    let workflow = write_workflow(
        &config_dir,
        r#"
        [workflow]
        name = "explicit-route-hint"

        [[step]]
        id = "explicit"
        target = "review"
        prompt = "must never dispatch"
        [step.route]
        effort = "high"
        "#,
    );

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["workflow", "run"])
        .arg(&workflow)
        .assert()
        .code(2)
        .stderr(predicate::str::contains(
            "route hints on a non-deferred target",
        ));

    assert!(
        server
            .received_requests()
            .await
            .expect("request log")
            .is_empty()
    );
    assert!(!data_dir.path().join("workflows").exists());
    assert!(!data_dir.path().join("ledger.jsonl").exists());
}

#[tokio::test]
async fn workflow_auto_route_hints_are_validated_before_journal_or_dispatch() {
    let server = MockServer::start().await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &auto_route_config_for(&server));
    let workflow = write_workflow(
        &config_dir,
        r#"
        [workflow]
        name = "invalid-auto-route"

        [[step]]
        id = "invalid"
        target = "auto"
        prompt = "must never dispatch"
        [step.route]
        candidates = ["missing-profile"]
        "#,
    );

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["workflow", "run"])
        .arg(&workflow)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "deferred target `auto` is invalid",
        ));

    assert!(
        server
            .received_requests()
            .await
            .expect("request log")
            .is_empty()
    );
    let workflow_dir = data_dir.path().join("workflows");
    assert!(
        !workflow_dir.exists()
            || fs::read_dir(workflow_dir)
                .expect("read workflow dir")
                .next()
                .is_none(),
        "validation must fail before a journal is created"
    );
}

#[tokio::test]
async fn workflow_auto_preflight_validates_every_eligible_profile() {
    let server = MockServer::start().await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let mut config_text = auto_route_config_for(&server);
    config_text.push_str(
        r#"

        [profiles.broken-unused-by-dummy-prompt]
        provider = "missing-provider"
        protocol = "openai_chat"
        harness = "none"
        model = "broken-model"
        tier = "mainline"
        "#,
    );
    let config = write_config(&config_dir, &config_text);
    let workflow = write_workflow(
        &config_dir,
        r#"
        [workflow]
        name = "all-candidates-preflight"

        [[step]]
        id = "simple"
        target = "auto"
        prompt = "hello"
        "#,
    );

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["workflow", "run"])
        .arg(&workflow)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "routing candidate `broken-unused-by-dummy-prompt`",
        ));

    assert!(
        server
            .received_requests()
            .await
            .expect("request log")
            .is_empty()
    );
    assert!(!data_dir.path().join("workflows").exists());
}

#[tokio::test]
async fn dispatch_stream_prints_deltas_and_writes_ledger() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            concat!(
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"strea\"}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"med answer\"}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":4,\"output_tokens\":3}}}\n\n",
            ),
            "text/event-stream",
        ))
        .mount(&server)
        .await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &responses_config_for(&server));

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch",
            "say hi",
            "--target",
            "review",
            "--stream",
            "--label",
            "purpose=acceptance-test",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("streamed answer"))
        .stderr(predicate::str::contains("notice:").not());

    let records = ledger_records(data_dir.path());
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record["status"], "success");
    assert_eq!(record["usage"]["input_tokens"], 4);
    assert_eq!(record["usage"]["output_tokens"], 3);
    assert_eq!(record["target"]["protocol"], "openai_responses");
    assert_eq!(record["attempts"].as_array().expect("attempts").len(), 1);

    let run_id = record["run_id"].as_str().expect("run_id string");
    uuid::Uuid::parse_str(run_id).expect("run_id parses as a UUID");
    assert_eq!(record["owner"], "local");
    let digest = record["task_digest"].as_str().expect("task_digest string");
    assert_eq!(digest.len(), 16);
    assert!(
        digest.chars().all(|c| c.is_ascii_hexdigit()),
        "task_digest must be hex: {digest}"
    );
    assert_eq!(record["output_chars"], "streamed answer".chars().count());
    assert_eq!(record["labels"]["purpose"], "acceptance-test");

    let output = vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(config)
        .args([
            "dispatch", "say json", "--target", "review", "--stream", "--json",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let parsed: Value = serde_json::from_slice(&output).expect("json stream dispatch output");
    assert_eq!(parsed["record"]["status"], "success");
    assert_eq!(parsed["output"], "streamed answer");
}

#[tokio::test]
async fn dispatch_stream_on_failover_chain_falls_back_and_records_both_attempts() {
    let primary = MockServer::start().await;
    let backup = MockServer::start().await;
    mock_openai(&primary, 500, "").await;
    mock_openai(&backup, 200, "backup answer").await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &failover_config(&primary, &backup));

    // `resilient` resolves to a two-element chain (primary + backup) —
    // `streamable_target` only accepts a single-target chain, so
    // `--stream` here must fall back to the ordinary non-streaming
    // `Dispatcher::dispatch` path, which is what actually exercises failover.
    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["dispatch", "fail over", "--target", "resilient", "--stream"])
        .assert()
        .success()
        .stdout(predicate::str::contains("backup answer"))
        .stderr(predicate::str::contains(
            "--stream only applies to a single target",
        ));

    // The fallback went through the full non-streaming dispatch path, so both
    // chain attempts (failed primary, successful backup) are visible on the
    // one recorded `RunRecord` — streaming's fallback must never truncate the
    // failover chain it hands off.
    let records = ledger_records(data_dir.path());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["status"], "success");
    let attempts = records[0]["attempts"].as_array().expect("attempts");
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0]["target"]["provider"], "primary");
    assert_eq!(attempts[1]["target"]["provider"], "backup");
}

#[tokio::test]
#[cfg(unix)]
async fn dispatch_stream_on_harness_target_streams() {
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let bin_dir = TempDir::new().expect("bin tempdir");
    let config = write_config(&config_dir, &harness_config());
    let _fake_claude = write_fake_streaming_claude(&bin_dir);

    vyane()
        .env_clear()
        .env("PATH", bin_dir.path())
        .env("HOME", std::env::temp_dir())
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["dispatch", "hello", "--target", "builder", "--stream"])
        .assert()
        .success()
        .stdout(predicate::str::contains("harness delta"))
        .stderr(predicate::str::contains("does not support streaming").not())
        .stderr(predicate::str::contains("--stream only applies").not());

    // Harness streaming is kernel-owned too: the successful streaming attempt
    // produces exactly one ledger record with the original CliWrap identity.
    let records = ledger_records(data_dir.path());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["status"], "success");
    assert_eq!(records[0]["transport"], "cli_wrap");
}

#[tokio::test]
async fn dispatch_stream_with_session_falls_back() {
    let server = MockServer::start().await;
    mock_openai(&server, 200, "session answer").await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &config_for(&server));

    // `--session` names a single direct-HTTP target — otherwise streamable —
    // but the streaming path carries no session continuity (see
    // docs/plan/WP-09.md's non-goals), so it must still fall back rather than
    // silently tag a `RunRecord.session_id` the session store never sees.
    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "dispatch",
            "hello",
            "--target",
            "review",
            "--session",
            "s1",
            "--stream",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("session answer"))
        .stderr(predicate::str::contains("no --session"));

    let records = ledger_records(data_dir.path());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["status"], "success");
    assert_eq!(records[0]["session_id"], "s1");
}

// ---------------------------------------------------------------------------
// New command smoke tests (serve / mcp / review / route)
// ---------------------------------------------------------------------------

/// `vyane route` with no config profiles produces a clean error (exit 1),
/// not a crash.
#[tokio::test]
async fn route_with_empty_config_errors_cleanly() {
    let config_dir = TempDir::new().expect("config tempdir");
    let config = config_dir.path().join("config.toml");
    fs::write(&config, "# empty config\n").expect("write config");

    vyane()
        .arg("--config")
        .arg(&config)
        .args(["route", "hello world"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no candidate profiles"));
}

/// `vyane route` with tier-configured profiles routes a simple task to economy.
#[tokio::test]
async fn route_simple_task_to_economy() {
    let config_dir = TempDir::new().expect("config tempdir");
    let config = config_dir.path().join("config.toml");
    fs::write(
        &config,
        r#"
        [providers.test]
        base_url = "http://localhost"
        api_key_env = "KEY"
        auth_style = "bearer"
        protocol = "openai_chat"
        default_model = "cheap"

        [profiles.cheap]
        provider = "test"
        protocol = "openai_chat"
        harness = "none"
        model = "cheap"
        tier = "economy"

        [profiles.expensive]
        provider = "test"
        protocol = "openai_chat"
        harness = "none"
        model = "powerful"
        tier = "frontier"
        "#,
    )
    .expect("write config");

    vyane()
        .arg("--config")
        .arg(&config)
        .args(["route", "say hello", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("economy"));
}

/// `vyane route --tier frontier` overrides to the frontier profile.
#[tokio::test]
async fn route_explicit_tier_override() {
    let config_dir = TempDir::new().expect("config tempdir");
    let config = config_dir.path().join("config.toml");
    fs::write(
        &config,
        r#"
        [providers.test]
        base_url = "http://localhost"
        api_key_env = "KEY"
        auth_style = "bearer"
        protocol = "openai_chat"
        default_model = "cheap"

        [profiles.cheap]
        provider = "test"
        protocol = "openai_chat"
        harness = "none"
        model = "cheap"
        tier = "economy"

        [profiles.expensive]
        provider = "test"
        protocol = "openai_chat"
        harness = "none"
        model = "powerful"
        tier = "frontier"
        "#,
    )
    .expect("write config");

    vyane()
        .arg("--config")
        .arg(&config)
        .args(["route", "simple", "--tier", "frontier", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("frontier"));
}

/// `vyane review` with fewer than 2 reviewers exits with a config error.
#[tokio::test]
async fn review_with_one_reviewer_exits_two() {
    let server = MockServer::start().await;
    let config_dir = TempDir::new().expect("config tempdir");
    let config = config_dir.path().join("config.toml");
    fs::write(&config, config_for(&server)).expect("write config");

    vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .arg("--config")
        .arg(&config)
        .args([
            "review",
            "do the thing",
            "--implementer",
            "review",
            "--reviewers",
            "review", // only 1 reviewer
            "--synthesizer",
            "review",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("at least 2"));
}

/// `vyane serve --help` exits successfully and shows the addr option.
#[test]
fn serve_help_shows_addr() {
    vyane()
        .args(["serve", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--addr"))
        .stdout(predicate::str::contains("127.0.0.1:9721"));
}

/// `vyane mcp --help` exits successfully.
#[test]
fn mcp_help_works() {
    vyane().args(["mcp", "--help"]).assert().success();
}

/// `vyane --help` lists all commands including review and route.
#[test]
fn help_lists_all_commands() {
    let output = vyane()
        .args(["--help"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let help = String::from_utf8(output).expect("utf8");
    for cmd in &[
        "dispatch",
        "broadcast",
        "workflow",
        "review",
        "route",
        "serve",
        "mcp",
        "task",
    ] {
        assert!(help.contains(cmd), "help text must list command '{cmd}'");
    }
}

// ---------------------------------------------------------------------------
// Review pipeline end-to-end
// ---------------------------------------------------------------------------

/// Mock that returns different answers based on which step the request is for.
/// The review pipeline sends 3 types of prompts:
/// 1. implement: the raw task text
/// 2. review: contains "Review this implementation"
/// 3. synthesize: contains "Synthesize these independent"
async fn mock_review_pipeline(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(|req: &Request| {
            let body = String::from_utf8_lossy(&req.body);
            let answer = if body.contains("Synthesize") {
                "APPROVE: implementation looks correct"
            } else if body.contains("Review this implementation") {
                "No issues found"
            } else {
                "def sort(arr): return sorted(arr)"
            };
            ResponseTemplate::new(200).set_body_json(json!({
                "id": "chatcmpl-review",
                "model": "test-model",
                "choices": [{
                    "message": { "role": "assistant", "content": answer },
                    "finish_reason": "stop"
                }],
                "usage": { "prompt_tokens": 5, "completion_tokens": 3 }
            }))
        })
        .mount(server)
        .await;
}

/// `vyane review` runs the full three-step pipeline (implement → review →
/// synthesize) and produces a completed workflow outcome.
#[tokio::test]
async fn review_pipeline_runs_three_steps_end_to_end() {
    let server = MockServer::start().await;
    mock_review_pipeline(&server).await;
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &config_for(&server));

    let output = vyane()
        .env("VYANE_CLI_TEST_KEY", "sk-test")
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "review",
            "implement a sorting function",
            "--implementer",
            "review",
            "--reviewers",
            "review,review", // 2 reviewers (same profile, different identity)
            "--synthesizer",
            "review",
            "--json",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let outcome: Value = serde_json::from_slice(&output).expect("review outcome json");
    assert_eq!(outcome["status"], "completed");

    // All three steps must be present in the journal.
    let steps = &outcome["journal"]["steps"];
    assert!(steps.get("implement").is_some(), "implement step exists");
    assert!(steps.get("review").is_some(), "review step exists");
    assert!(steps.get("synthesize").is_some(), "synthesize step exists");

    // The synthesize step should have output.
    let synth_output = steps["synthesize"]["output"]
        .as_str()
        .or_else(|| {
            steps["synthesize"]["outputs"]
                .as_array()
                .and_then(|arr| arr.first())
                .and_then(|item| item["output"].as_str())
        })
        .unwrap_or("");
    assert!(
        synth_output.contains("APPROVE") || !synth_output.is_empty(),
        "synthesize produced output"
    );

    // A workflow journal should have been written.
    let wf_run_id = outcome["wf_run_id"].as_str().expect("wf_run_id");
    let journal_path = data_dir
        .path()
        .join("workflows")
        .join(format!("{wf_run_id}.json"));
    assert!(journal_path.exists(), "review workflow journal exists");
}
