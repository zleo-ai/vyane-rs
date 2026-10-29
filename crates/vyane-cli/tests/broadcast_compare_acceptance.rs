#![allow(clippy::unwrap_used)]
//! Broadcast comparison exercises real CLI dispatch against local HTTP fixtures.

mod common;

use std::fs;
use std::time::Duration;

use assert_cmd::Command;
use serde_json::{Value, json};
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct Fixture {
    server: MockServer,
    root: tempfile::TempDir,
}

impl Fixture {
    async fn new(replies: &[(&str, u16, &str, u64)]) -> Self {
        let server = MockServer::start().await;
        for &(model, status, answer, delay_ms) in replies {
            let body = if status == 200 {
                json!({
                    "model": model,
                    "choices": [{
                        "message": {"role": "assistant", "content": answer},
                        "finish_reason": "stop"
                    }]
                })
            } else {
                json!({"error": {"message": "fixture failure"}})
            };
            Mock::given(method("POST"))
                .and(path("/v1/chat/completions"))
                .and(body_partial_json(json!({"model": model})))
                .respond_with(
                    ResponseTemplate::new(status)
                        .set_body_json(body)
                        .set_delay(Duration::from_millis(delay_ms)),
                )
                .mount(&server)
                .await;
        }
        let root = common::private_tempdir();
        fs::write(
            root.path().join("config.toml"),
            format!(
                r#"
                [providers.fixture]
                base_url = "{}"
                auth_style = "bearer"
                protocol = "openai_chat"
                "#,
                server.uri()
            ),
        )
        .unwrap();
        Self { server, root }
    }

    fn run(&self, targets: &str, flags: &[&str], exit_code: i32) -> Vec<u8> {
        Command::cargo_bin("vyane")
            .unwrap()
            .env_clear()
            .env("HOME", self.root.path())
            .env("VYANE_DATA_DIR", self.root.path())
            .env("NO_COLOR", "1")
            .args(["--config"])
            .arg(self.root.path().join("config.toml"))
            .args(["broadcast", "compare fixture", "--targets", targets])
            .args(flags)
            .timeout(Duration::from_secs(15))
            .assert()
            .code(exit_code)
            .get_output()
            .stdout
            .clone()
    }

    fn json(&self, targets: &str, compare: bool, exit_code: i32) -> Value {
        let flags = if compare {
            vec!["--json", "--compare"]
        } else {
            vec!["--json"]
        };
        serde_json::from_slice(&self.run(targets, &flags, exit_code)).unwrap()
    }

    async fn assert_calls(&self, expected: usize) {
        assert_eq!(
            self.server.received_requests().await.unwrap().len(),
            expected,
            "comparison must not submit any additional requests"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lexical_scores_metrics_and_recorded_durations() {
    let fixture = Fixture::new(&[
        ("a", 200, "Alpha alpha beta", 250),
        ("b", 200, "BETA alpha", 0),
        ("c", 200, "delta epsilon", 0),
        ("d", 200, "beta gamma", 0),
    ])
    .await;
    let body = fixture.json("fixture/a,fixture/b,fixture/c,fixture/d", true, 0);
    let report = &body["comparison"];
    assert_eq!(report["comparable"], true);
    assert_eq!(report["target_count"], 4);
    let pairs = report["pairwise_similarity"].as_array().unwrap();
    assert_eq!(pairs.len(), 6);
    assert_eq!(
        pairs[0],
        json!({"left_index":0,"right_index":1,"similarity":1.0})
    );
    assert_eq!(pairs[1]["similarity"], 0.0);
    assert_eq!(pairs[2]["similarity"], 0.333);
    assert_eq!(report["average_similarity"], 0.278);
    assert_eq!(report["per_target"][0]["char_count"], 16);
    assert_eq!(report["per_target"][0]["word_count"], 3);
    assert_eq!(
        report["per_target"][0]["top_terms"],
        json!(["alpha", "beta"])
    );
    assert_eq!(
        report["per_target"][2]["unique_terms"],
        json!(["epsilon", "delta"])
    );
    let mut timings = Vec::new();
    for entry in report["per_target"].as_array().unwrap() {
        let index = entry["index"].as_u64().unwrap() as usize;
        let record = &body["results"][index]["record"];
        let start =
            chrono::DateTime::parse_from_rfc3339(record["started_at"].as_str().unwrap()).unwrap();
        let finish =
            chrono::DateTime::parse_from_rfc3339(record["finished_at"].as_str().unwrap()).unwrap();
        let elapsed = (finish - start).to_std().unwrap().as_secs_f64();
        assert!((entry["duration_seconds"].as_f64().unwrap() - elapsed).abs() < 1e-9);
        timings.push((index, elapsed));
    }
    timings.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
    assert_eq!(
        report["speed_ranking"],
        json!(timings.iter().map(|(index, _)| *index).collect::<Vec<_>>())
    );
    fixture.assert_calls(4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_failures_and_repeated_targets_keep_input_identity() {
    let fixture = Fixture::new(&[("ok", 200, "one 2", 0), ("bad", 400, "", 0)]).await;
    let body = fixture.json("fixture/ok,fixture/bad,fixture/ok", true, 1);
    assert_eq!(body["results"].as_array().unwrap().len(), 3);
    assert_eq!(body["results"][1]["record"]["status"], "error");
    let report = &body["comparison"];
    assert_eq!(report["target_count"], 2);
    assert_eq!(report["per_target"][0]["index"], 0);
    assert_eq!(report["per_target"][1]["index"], 2);
    assert_eq!(report["per_target"][0]["target"], "fixture/ok");
    assert_eq!(report["per_target"][1]["target"], "fixture/ok");
    assert_eq!(report["per_target"][0]["word_count"], 2);
    assert_eq!(
        report["pairwise_similarity"],
        json!([{"left_index":0,"right_index":2,"similarity":1.0}])
    );
    fixture.assert_calls(3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_ascii_and_empty_word_sets_are_unscorable() {
    let fixture = Fixture::new(&[
        ("zh", 200, "你好🦀", 0),
        ("punctuation", 200, "!!!", 0),
        ("ascii", 200, "A_1 éB", 0),
    ])
    .await;
    let body = fixture.json("fixture/zh,fixture/punctuation,fixture/ascii", true, 0);
    let report = &body["comparison"];
    assert_eq!(report["comparable"], true);
    assert_eq!(report["per_target"][0]["char_count"], 3);
    assert_eq!(report["per_target"][0]["word_count"], 0);
    assert_eq!(report["per_target"][0]["top_terms"], json!([]));
    assert_eq!(report["per_target"][2]["top_terms"], json!(["a", "1", "b"]));
    assert!(report["average_similarity"].is_null());
    assert!(
        report["pairwise_similarity"]
            .as_array()
            .unwrap()
            .iter()
            .all(|pair| pair["similarity"].is_null())
    );
    fixture.assert_calls(3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insufficient_successful_text_has_a_fixed_reason() {
    let fixture = Fixture::new(&[
        ("ok", 200, "hello", 0),
        ("bad", 400, "", 0),
        ("empty", 200, "", 0),
        ("blank", 200, " \n\t", 0),
    ])
    .await;
    for (targets, exit_code, count) in [
        ("fixture/bad", 1, 0),
        ("fixture/ok", 0, 1),
        ("fixture/ok,fixture/bad,fixture/empty,fixture/blank", 1, 1),
    ] {
        let body = fixture.json(targets, true, exit_code);
        assert_eq!(body["comparison"]["comparable"], false);
        assert_eq!(
            body["comparison"]["reason"],
            "Need at least 2 successful text results"
        );
        assert_eq!(body["comparison"]["target_count"], count);
        assert_eq!(body["comparison"]["pairwise_similarity"], json!([]));
        assert!(body["comparison"]["average_similarity"].is_null());
    }
    fixture.assert_calls(6).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_keeps_array_and_text_while_enabled_appends_readable_report() {
    let fixture = Fixture::new(&[("ok", 200, "hello", 0), ("bad", 400, "", 0)]).await;
    let body = fixture.json("fixture/ok,fixture/ok", false, 0);
    assert_eq!(body.as_array().unwrap().len(), 2);
    assert_eq!(body[0]["target"], "fixture/ok");
    assert_eq!(body[0]["record"]["status"], "success");
    assert_eq!(body[0]["output"], "hello");
    assert!(body[0].get("comparison").is_none());
    assert!(fixture.json("fixture/ok,fixture/bad", false, 1).is_array());
    let plain = String::from_utf8(fixture.run("fixture/ok,fixture/ok", &[], 0)).unwrap();
    assert!(plain.starts_with("target"));
    assert!(!plain.contains("Comparison"));
    let compared =
        String::from_utf8(fixture.run("fixture/ok,fixture/ok", &["--compare"], 0)).unwrap();
    assert!(compared.starts_with("target"));
    assert!(compared.contains("Comparison (lexical)"));
    assert!(compared.contains("[0] fixture/ok"));
    assert!(compared.contains("[1] fixture/ok"));
    assert!(compared.contains("1.000"));
    assert!(compared.contains("Speed ranking"));
    let insufficient = String::from_utf8(fixture.run("fixture/ok", &["--compare"], 0)).unwrap();
    assert!(insufficient.contains("Need at least 2 successful text results"));
    fixture.assert_calls(9).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn comparison_limit_is_rejected_before_execution_and_only_when_enabled() {
    let fixture = Fixture::new(&[("ok", 200, "x", 0)]).await;
    let targets = vec!["fixture/ok"; 65].join(",");
    Command::cargo_bin("vyane")
        .unwrap()
        .env_clear()
        .env("HOME", fixture.root.path())
        .env("VYANE_DATA_DIR", fixture.root.path())
        .arg("--config")
        .arg(fixture.root.path().join("config.toml"))
        .args([
            "broadcast",
            "fixture",
            "--targets",
            &targets,
            "--compare",
            "--json",
        ])
        .timeout(Duration::from_secs(15))
        .assert()
        .code(2)
        .stdout("")
        .stderr(predicates::str::contains("at most 64 targets"));
    fixture.assert_calls(0).await;
    assert!(!fixture.root.path().join("ledger.jsonl").exists());
    let body = fixture.json(&targets, false, 0);
    assert_eq!(body.as_array().unwrap().len(), 65);
    fixture.assert_calls(65).await;
    let body = fixture.json(&vec!["fixture/ok"; 64].join(","), true, 0);
    assert_eq!(body["comparison"]["target_count"], 64);
    assert_eq!(
        body["comparison"]["pairwise_similarity"]
            .as_array()
            .unwrap()
            .len(),
        2016
    );
    fixture.assert_calls(129).await;
}

#[test]
fn comparison_limit_validation_does_not_initialize_runtime_storage() {
    let root = common::private_tempdir();
    fs::write(
        root.path().join("config.toml"),
        r#"
        [providers.fixture]
        base_url = "http://127.0.0.1:9"
        auth_style = "bearer"
        protocol = "openai_chat"
    "#,
    )
    .unwrap();
    Command::cargo_bin("vyane")
        .unwrap()
        .env_clear()
        .env("HOME", root.path())
        .env("VYANE_DATA_DIR", root.path())
        .arg("--config")
        .arg(root.path().join("config.toml"))
        .args([
            "broadcast",
            "fixture",
            "--compare",
            "--targets",
            &vec!["fixture/ok"; 65].join(","),
        ])
        .assert()
        .code(2)
        .stdout("")
        .stderr(predicates::str::contains("at most 64 targets"));
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
}
