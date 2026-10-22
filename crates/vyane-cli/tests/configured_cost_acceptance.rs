#![allow(clippy::unwrap_used)]
//! A configured model rate is stored on the run record.
//! No rate table, an unlisted model, or a run with no usage leaves `cost_usd` unset.
//! A reversed history window is still rejected.

mod common;

use std::fs;

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const MODEL: &str = "billing-fixture";

fn rates(model: &str) -> String {
    format!(
        r#"
        [cost.models."{model}"]
        input_per_mtok = 1.0
        output_per_mtok = 2.0
        cache_read_per_mtok = 0.1
        reasoning_per_mtok = 8.0
        "#
    )
}

fn config(base_url: &str, rates: &str) -> String {
    format!(
        r#"
        [providers.fixture]
        base_url = "{base_url}"
        auth_style = "bearer"
        protocol = "openai_chat"

        [profiles.test]
        provider = "fixture"
        protocol = "openai_chat"
        harness = "none"
        model = "{MODEL}"
        {rates}
        "#
    )
}

fn body(with_usage: bool) -> Value {
    let mut value = serde_json::json!({
        "model": MODEL,
        "choices": [{
            "message": {"role": "assistant", "content": "billing answer"},
            "finish_reason": "stop"
        }]
    });
    if with_usage {
        value["usage"] = serde_json::json!({
            "prompt_tokens": 1_000,
            "completion_tokens": 1_000,
            "prompt_tokens_details": {"cached_tokens": 900},
            "completion_tokens_details": {"reasoning_tokens": 400}
        });
    }
    value
}

async fn dispatch(rates: &str, with_usage: bool) -> (Value, Value) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body(with_usage)))
        .expect(1)
        .mount(&server)
        .await;

    let dir = common::private_tempdir();
    let config_path = dir.path().join("config.toml");
    fs::write(&config_path, config(&server.uri(), rates)).expect("write config");
    let stdout = Command::cargo_bin("vyane")
        .expect("vyane binary")
        .env("VYANE_DATA_DIR", dir.path())
        .arg("--config")
        .arg(&config_path)
        .args(["dispatch", "billing fixture", "--target", "test", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output: Value = serde_json::from_slice(&stdout).expect("dispatch json");
    let ledger_text = fs::read_to_string(dir.path().join("ledger.jsonl")).expect("ledger");
    let mut lines = ledger_text.lines();
    let ledger: Value =
        serde_json::from_str(lines.next().expect("ledger row")).expect("ledger json");
    assert!(lines.next().is_none(), "one run writes one ledger row");
    assert_eq!(output["output"], "billing answer");
    assert_eq!(output["record"]["status"], "success");
    assert_eq!(ledger["run_id"], output["record"]["run_id"]);
    (output["record"].clone(), ledger)
}

fn assert_unset(record: &Value, ledger: &Value) {
    assert!(record["cost_usd"].is_null());
    assert!(ledger["cost_usd"].is_null());
}

fn assert_cost(record: &Value, ledger: &Value, expected: f64) {
    for value in [record, ledger] {
        let actual = value["cost_usd"].as_f64().expect("stored cost");
        assert!(
            (actual - expected).abs() < 1e-12,
            "stored {actual}, expected {expected}"
        );
    }
}

fn assert_subset_usage(record: &Value) {
    assert_eq!(record["usage"]["input_tokens"], 1_000);
    assert_eq!(record["usage"]["output_tokens"], 1_000);
    assert_eq!(record["usage"]["cached_input_tokens"], 900);
    assert_eq!(record["usage"]["reasoning_tokens"], 400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_rate_is_stored_and_unknown_prices_stay_unset() {
    let (record, ledger) = dispatch(&rates(MODEL), true).await;
    assert_subset_usage(&record);
    // 100*1 + 900*0.1 + 600*2 + 400*8 = 4590 USD per million tokens.
    assert_cost(&record, &ledger, 0.004_59);

    let (record, ledger) = dispatch("", true).await;
    assert_subset_usage(&record);
    assert_unset(&record, &ledger);

    let (record, ledger) = dispatch(&rates("other-model"), true).await;
    assert_subset_usage(&record);
    assert_unset(&record, &ledger);

    let (record, ledger) = dispatch(&rates(MODEL), false).await;
    assert!(record["usage"].is_null());
    assert!(ledger["usage"].is_null());
    assert_unset(&record, &ledger);
}

#[test]
fn reversed_history_window_is_still_rejected() {
    let dir = common::private_tempdir();
    for mode in [Vec::new(), vec!["--stats"]] {
        Command::cargo_bin("vyane")
            .expect("vyane binary")
            .env("VYANE_DATA_DIR", dir.path())
            .args([
                "history",
                "--since",
                "2026-01-02T00:00:00Z",
                "--until",
                "2026-01-01T00:00:00Z",
            ])
            .args(&mode)
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "since must not be later than until",
            ));
    }
    assert!(!dir.path().join("ledger.jsonl").exists());
}
