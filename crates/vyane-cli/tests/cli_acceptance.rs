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

/// A profile that resolves to a `CliWrap` (harness) target, never `DirectHttp`
/// — used to prove `--stream` falls back to non-streaming for harness
/// targets. `base_url`/`auth_style` are required by config resolution even
/// though the harness never uses them at spawn time; the value doesn't
/// matter because the harness is never actually run in this test (see the
/// scrubbed-`PATH` note on `dispatch_stream_on_harness_target_falls_back`): a
/// `PATH` with no `claude` binary on it turns any spawn attempt into an
/// immediate, side-effect-free `SpawnFailed`.
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
        .args(["dispatch", "say hi", "--target", "review", "--stream"])
        .assert()
        .success()
        .stdout(predicate::str::contains("streamed answer"))
        // The fallback notice must NOT fire — this is the genuine streaming
        // path, not a fallback.
        .stderr(predicate::str::contains("notice:").not());

    let records = ledger_records(data_dir.path());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["status"], "success");
    assert_eq!(records[0]["usage"]["input_tokens"], 4);
    assert_eq!(records[0]["usage"]["output_tokens"], 3);
    assert_eq!(records[0]["target"]["protocol"], "openai_responses");
    assert_eq!(
        records[0]["attempts"].as_array().expect("attempts").len(),
        1
    );

    // `--stream --json` mirrors the non-streaming `--json` shape exactly.
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
async fn dispatch_stream_on_harness_target_falls_back() {
    let config_dir = TempDir::new().expect("config tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let config = write_config(&config_dir, &harness_config());

    // `env_clear` + an empty `PATH` guarantee the real `claude` binary can
    // never be resolved by the child `vyane` process, however it snapshots
    // its environment — turning the harness attempt this test provokes into
    // an immediate, side-effect-free `SpawnFailed` rather than an actual
    // invocation of a real coding CLI. `HOME` is set because `vyane` itself
    // needs no OS services beyond what `VYANE_DATA_DIR` already overrides,
    // but keeping it present avoids surprises from other library init paths.
    vyane()
        .env_clear()
        .env("PATH", "")
        .env("HOME", std::env::temp_dir())
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args(["dispatch", "hello", "--target", "builder", "--stream"])
        .assert()
        .failure()
        .code(1)
        .stderr(predicate::str::contains(
            "--stream only applies to a single direct-HTTP target",
        ));

    // The fallback still produces exactly one `RunRecord` through the normal
    // (non-streaming) `Dispatcher::dispatch` path — streaming must never
    // silently skip the ledger write.
    let records = ledger_records(data_dir.path());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["status"], "error");
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
