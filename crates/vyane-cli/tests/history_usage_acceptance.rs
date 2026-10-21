#![allow(clippy::unwrap_used)]
mod common;
use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::{Value, json};
const HISTORY: &str = include_str!("../../vyane-service/tests/fixtures/history_usage.jsonl");
const EXPECTED: &str =
    include_str!("../../vyane-service/tests/fixtures/history_usage_expected.json");

#[test]
fn history_usage_cli_counts_all_owned_filtered_records_without_rewriting_them() {
    let root = common::private_tempdir();
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY).unwrap();
    let expected: Value = serde_json::from_str(EXPECTED).unwrap();
    for (filters, case, total) in [
        (vec![], "all", 126),
        (vec!["--provider", "q"], "q", 2),
        (vec!["--status", "error"], "zero", 1),
        (
            vec![
                "--provider",
                "p",
                "--since",
                "2026-01-01T13:00:00+02:00",
                "--until",
                "2026-01-01T14:00:00Z",
            ],
            "window",
            2,
        ),
        (vec!["--provider", "absent"], "empty", 0),
        (
            vec![
                "--session",
                "usage-session",
                "--label",
                "fixture=usage",
                "--provider",
                "q",
            ],
            "q",
            2,
        ),
    ] {
        let result = Command::cargo_bin("vyane")
            .unwrap()
            .env("VYANE_DATA_DIR", root.path())
            .args(["history", "--stats", "--json"])
            .args(filters)
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let body: Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(body["total"], total);
        assert_eq!(body["usage"], expected[case], "{case}");
        let rendered = body.to_string();
        for forbidden in [
            "USAGE_PRIVATE_CANARY",
            "synthetic-private-directory",
            "usage-session",
            "first-attempt-only",
            "foreign-poison",
        ] {
            assert!(!rendered.contains(forbidden));
        }
    }
    assert_eq!(std::fs::read_to_string(ledger).unwrap(), HISTORY);
}

#[test]
fn history_usage_cli_separates_zero_missing_and_overflow_without_losing_integer_precision() {
    let root = common::private_tempdir();
    let mut first: Value = serde_json::from_str(HISTORY.lines().next().unwrap()).unwrap();
    first["usage"] = json!({"input_tokens":u64::MAX,"output_tokens":9_007_199_254_740_993_u64,"reasoning_tokens":u64::MAX,"cached_input_tokens":u64::MAX});
    let mut second = first.clone();
    second["run_id"] = json!("second");
    second["usage"] =
        json!({"input_tokens":1,"output_tokens":1,"reasoning_tokens":1,"cached_input_tokens":0});
    let text = format!("{first}\n{second}\n");
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, &text).unwrap();
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
    assert_eq!(
        body["usage"],
        json!({
            "input_tokens":{"known":2,"unknown":0,"total_tokens":null,"overflowed":true},
            "output_tokens":{"known":2,"unknown":0,"total_tokens":9_007_199_254_740_994_u64,"overflowed":false},
            "reasoning_tokens":{"known":2,"unknown":0,"total_tokens":null,"overflowed":true},
            "cached_input_tokens":{"known":2,"unknown":0,"total_tokens":u64::MAX,"overflowed":false}
        })
    );
    assert_eq!(std::fs::read_to_string(&ledger).unwrap(), text);
    let empty = common::private_tempdir();
    let output = Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", empty.path())
        .args(["history", "--stats", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let body: Value = serde_json::from_slice(&output).unwrap();
    let expected: Value = serde_json::from_str(EXPECTED).unwrap();
    assert_eq!(body["usage"], expected["empty"]);
    assert!(!empty.path().join("ledger.jsonl").exists());
}

#[test]
fn history_usage_cli_text_explains_partial_saved_totals() {
    let root = common::private_tempdir();
    std::fs::write(root.path().join("ledger.jsonl"), HISTORY).unwrap();
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--stats"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Input tokens: 130 (known: 4, unknown: 122, overflowed: false)",
        ))
        .stdout(predicate::str::contains(
            "Reasoning tokens: 5 (known: 3, unknown: 123, overflowed: false)",
        ));
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--stats", "--provider", "q"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Reasoning tokens: n/a (known: 0, unknown: 2, overflowed: false)",
        ));
}
