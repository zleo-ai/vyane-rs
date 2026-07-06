use std::fs;
use std::path::Path;

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

fn ledger_records(data_dir: &Path) -> Vec<Value> {
    let ledger = data_dir.join("ledger.jsonl");
    let text = fs::read_to_string(ledger).expect("ledger file");
    text.lines()
        .map(|line| serde_json::from_str(line).expect("run record json"))
        .collect()
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
