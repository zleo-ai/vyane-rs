#![allow(clippy::unwrap_used)]

mod common;

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::{Value, json};

const HISTORY: &str = include_str!("fixtures/history_stats.jsonl");

#[test]
fn history_stats_success_rates_use_filtered_counts_rounding_and_unknown_empty_samples() {
    let root = common::private_tempdir();
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY).unwrap();
    for (filters, expected) in [
        (vec![], json!(25.0)),
        (vec!["--until", "2026-01-01T11:00:00Z"], json!(25.0)),
        (vec!["--provider", "q", "--status", "success"], json!(100.0)),
        (vec!["--status", "error"], json!(0.0)),
        (vec!["--since", "2026-01-02T00:00:00Z"], Value::Null),
    ] {
        let output = Command::cargo_bin("vyane")
            .unwrap()
            .env("VYANE_DATA_DIR", root.path())
            .args(["history", "--stats", "--json"])
            .args(&filters)
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let body: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(
            body.get("success_rate"),
            Some(&expected),
            "filters: {filters:?}"
        );
        if filters.is_empty() {
            assert_eq!(body["by_provider"]["p"]["success_rate"], 0.0);
            assert_eq!(body["by_provider"]["q"]["success_rate"], 100.0);
        }
    }
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--stats"])
        .assert()
        .success()
        .stdout(predicate::str::contains("success rate: 25.0%"));
    assert_eq!(std::fs::read_to_string(&ledger).unwrap(), HISTORY);

    let template: Value = serde_json::from_str(HISTORY.lines().next().unwrap()).unwrap();
    for (success, total, expected) in [
        (1, 16, 6.2),
        (3, 16, 18.8),
        (111, 2000, 5.5),
        (57, 2000, 2.9),
        (1, 3, 33.3),
        (2, 3, 66.7),
    ] {
        let mut rows = String::new();
        for i in 0..total {
            let mut row = template.clone();
            row["run_id"] = json!(format!("rate-{i}"));
            row["target"]["provider"] = json!("p");
            row["status"] = json!(if i < success { "success" } else { "error" });
            rows.push_str(&row.to_string());
            rows.push('\n');
        }
        std::fs::write(&ledger, &rows).unwrap();
        let output = Command::cargo_bin("vyane")
            .unwrap()
            .env("VYANE_DATA_DIR", root.path())
            .args(["history", "--stats", "--json"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let body: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(body["total"], total);
        assert_eq!(
            body["success_rate"], expected,
            "success: {success}, total: {total}"
        );
        assert_eq!(body["by_provider"]["p"]["success_rate"], expected);
        assert_eq!(std::fs::read_to_string(&ledger).unwrap(), rows);
    }
}

#[test]
fn history_stats_counts_all_matching_runs_and_preserves_the_ledger() {
    let root = common::private_tempdir();
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY).unwrap();
    for (filters, expected) in [
        (
            vec![],
            json!({"total":120,"success":30,"error":30,"timeout":30,"cancelled":30}),
        ),
        (
            vec!["--since", "2026-01-01T14:00:00+02:00"],
            json!({"total":80,"success":20,"error":20,"timeout":20,"cancelled":20}),
        ),
        (
            vec![
                "--provider",
                "p",
                "--status",
                "error",
                "--since",
                "2026-01-01T12:00:00Z",
            ],
            json!({"total":20,"success":0,"error":20,"timeout":0,"cancelled":0}),
        ),
        (
            vec!["--since", "2026-01-02T00:00:00Z"],
            json!({"total":0,"success":0,"error":0,"timeout":0,"cancelled":0}),
        ),
    ] {
        let output = Command::cargo_bin("vyane")
            .unwrap()
            .env("VYANE_DATA_DIR", root.path())
            .args(["history", "--stats", "--json"])
            .args(&filters)
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let body: Value = serde_json::from_slice(&output).unwrap();
        for (key, value) in expected.as_object().unwrap() {
            assert_eq!(&body[key], value, "filters: {filters:?}, key: {key}");
        }
        assert!(!body.to_string().contains("STATS_PRIVATE_DATA_CANARY"));
        assert!(body["by_provider"].get("foreign-provider-canary").is_none());
        if filters.is_empty() {
            assert_eq!(
                body["by_provider"]["p"],
                json!({"total":90,"success":0,"error":30,"timeout":30,"cancelled":30,"success_rate":0.0})
            );
            assert_eq!(
                body["by_provider"]["q"],
                json!({"total":30,"success":30,"error":0,"timeout":0,"cancelled":0,"success_rate":100.0})
            );
        }
        if expected["total"] == 0 {
            assert_eq!(body["by_provider"], json!({}));
        }
    }
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--stats"])
        .assert()
        .success()
        .stdout(predicate::str::contains("120"));
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--json"])
        .assert()
        .success()
        .stdout(predicate::function(|output: &str| {
            serde_json::from_str::<Vec<Value>>(output).is_ok_and(|rows| rows.len() == 20)
        }));
    assert_eq!(std::fs::read_to_string(ledger).unwrap(), HISTORY);
}

#[test]
fn history_stats_rejects_an_explicit_limit_and_reports_empty_storage() {
    let root = common::private_tempdir();
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--stats", "--limit", "1"])
        .assert()
        .code(2);
    let output = Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--stats", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    assert_eq!(
        serde_json::from_slice::<Value>(&output).unwrap(),
        json!({"total":0,"success":0,"error":0,"timeout":0,"cancelled":0,"success_rate":null,"duration":{"samples":0,"invalid":0,"total_ms":null,"mean_ms":null},"by_provider":{},"by_provider_duration":{},"usage":{"input_tokens":{"known":0,"unknown":0,"total_tokens":null,"overflowed":false},"output_tokens":{"known":0,"unknown":0,"total_tokens":null,"overflowed":false},"reasoning_tokens":{"known":0,"unknown":0,"total_tokens":null,"overflowed":false},"cached_input_tokens":{"known":0,"unknown":0,"total_tokens":null,"overflowed":false}}})
    );
    assert!(!root.path().join("ledger.jsonl").exists());
}
