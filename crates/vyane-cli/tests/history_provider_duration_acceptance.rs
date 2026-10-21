#![allow(clippy::unwrap_used)]
mod common;
use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::{Value, json};
const HISTORY: &str = include_str!("fixtures/history_duration.jsonl");

fn fixture() -> String {
    let mut result = String::new();
    for line in HISTORY.lines() {
        let Ok(mut row) = serde_json::from_str::<Value>(line) else {
            result.push_str(line);
            result.push('\n');
            continue;
        };
        let mut first = row["attempts"][0].clone();
        first["target"]["provider"] = json!("first-attempt-only");
        first["outcome"] = json!({"result":"err","kind":"timeout","message":"DURATION_PRIVATE_CANARY","failed_over":true});
        let last = row["attempts"][0].clone();
        row["attempts"] = json!([first, last]);
        result.push_str(&row.to_string());
        result.push('\n');
    }
    result
}
fn stats(root: &std::path::Path, filters: &[&str]) -> Value {
    let output = Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root)
        .args(["history", "--stats", "--json"])
        .args(filters)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    serde_json::from_slice(&output).unwrap()
}
fn expected() -> Value {
    json!({"p":{"samples":33,"invalid":0,"total_ms":4250,"mean_ms":128.8},"q":{"samples":1,"invalid":1,"total_ms":2750,"mean_ms":2750.0}})
}

#[test]
fn provider_duration_cli_groups_complete_owned_runs_by_final_provider() {
    let root = common::private_tempdir();
    let ledger = root.path().join("ledger.jsonl");
    let original = fixture();
    std::fs::write(&ledger, &original).unwrap();
    let body = stats(root.path(), &[]);
    assert_eq!(body["by_provider_duration"], expected());
    assert_eq!(
        body["duration"],
        json!({"samples":34,"invalid":1,"total_ms":7000,"mean_ms":205.9})
    );
    assert_eq!(
        body["by_provider"]["p"],
        json!({"total":33,"success":2,"error":1,"timeout":30,"cancelled":0,"success_rate":6.1})
    );
    assert_eq!(
        body["by_provider"]["q"],
        json!({"total":2,"success":0,"error":0,"timeout":1,"cancelled":1,"success_rate":0.0})
    );
    assert!(!body.to_string().contains("first-attempt-only"));
    assert!(!body.to_string().contains("DURATION_PRIVATE_CANARY"));
    assert_eq!(std::fs::read_to_string(&ledger).unwrap(), original);
}

#[test]
fn provider_duration_cli_filters_preserve_zero_invalid_and_empty_groups() {
    let root = common::private_tempdir();
    let ledger = root.path().join("ledger.jsonl");
    let original = fixture();
    std::fs::write(&ledger, &original).unwrap();
    for (filters, groups) in [
        (
            vec!["--provider", "p"],
            json!({"p":{"samples":33,"invalid":0,"total_ms":4250,"mean_ms":128.8}}),
        ),
        (
            vec!["--status", "cancelled"],
            json!({"q":{"samples":0,"invalid":1,"total_ms":null,"mean_ms":null}}),
        ),
        (
            vec![
                "--provider",
                "p",
                "--since",
                "2026-01-01T13:00:00+02:00",
                "--until",
                "2026-01-01T14:00:00Z",
            ],
            json!({"p":{"samples":2,"invalid":0,"total_ms":0,"mean_ms":0.0}}),
        ),
        (
            vec!["--session", "duration-session", "--label", "team=alpha"],
            json!({"p":{"samples":3,"invalid":0,"total_ms":1250,"mean_ms":416.7},"q":{"samples":1,"invalid":1,"total_ms":2750,"mean_ms":2750.0}}),
        ),
        (
            vec![
                "--session",
                "duration-session",
                "--label",
                "team=alpha",
                "--label",
                "team=beta",
            ],
            json!({}),
        ),
        (vec!["--provider", "absent"], json!({})),
    ] {
        let body = stats(root.path(), &filters);
        assert_eq!(body["by_provider_duration"], groups, "{filters:?}");
        assert_eq!(
            body["by_provider"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            body["by_provider_duration"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>()
        );
    }
    assert_eq!(std::fs::read_to_string(&ledger).unwrap(), original);
    let empty = common::private_tempdir();
    assert_eq!(stats(empty.path(), &[])["by_provider_duration"], json!({}));
    assert!(!empty.path().join("ledger.jsonl").exists());
}

#[test]
fn provider_duration_cli_text_reports_each_groups_recorded_span() {
    let root = common::private_tempdir();
    std::fs::write(root.path().join("ledger.jsonl"), fixture()).unwrap();
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--stats"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "DURATION TOTAL\tDURATION MEAN\tDURATION SAMPLES\tDURATION INVALID",
        ))
        .stdout(predicate::str::contains(
            "p\t33\t2\t1\t30\t0\t6.1%\t4250 ms\t128.8 ms\t33\t0",
        ))
        .stdout(predicate::str::contains(
            "q\t2\t0\t0\t1\t1\t0.0%\t2750 ms\t2750.0 ms\t1\t1",
        ));
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--stats", "--status", "cancelled"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "q\t1\t0\t0\t0\t1\t0.0%\tn/a\tn/a\t0\t1",
        ));
}
