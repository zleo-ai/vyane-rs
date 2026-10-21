#![allow(clippy::unwrap_used)]

mod common;

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;

const HISTORY: &str = include_str!("fixtures/history_since.jsonl");

#[test]
fn history_since_filters_started_at_before_limit_and_preserves_other_filters() {
    let data = common::private_tempdir();
    let ledger = data.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY).unwrap();
    for (filters, expected) in [
        (
            vec![],
            vec![
                "late-old",
                "error",
                "other-provider",
                "after",
                "boundary",
                "before",
            ],
        ),
        (
            vec!["--since", "2026-01-01T12:00:00Z"],
            vec!["error", "other-provider", "after", "boundary"],
        ),
        (
            vec![
                "--since",
                "2026-01-01T14:00:00+02:00",
                "--provider",
                "p",
                "--status",
                "success",
            ],
            vec!["after", "boundary"],
        ),
        (
            vec![
                "--since",
                "2026-01-01T12:00:00Z",
                "--provider",
                "p",
                "--status",
                "success",
                "--limit",
                "1",
            ],
            vec!["after"],
        ),
        (vec!["--since", "2026-01-02T00:00:00Z"], vec![]),
    ] {
        let output = Command::cargo_bin("vyane")
            .unwrap()
            .env("VYANE_DATA_DIR", data.path())
            .args(["history", "--json"])
            .args(&filters)
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let records: Vec<Value> = serde_json::from_slice(&output).unwrap();
        let ids: Vec<_> = records
            .iter()
            .map(|record| record["run_id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, expected, "filters: {filters:?}");
    }
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", data.path())
        .args([
            "history",
            "--since",
            "2026-01-01T12:00:00Z",
            "--provider",
            "p",
            "--status",
            "success",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("boundary"))
        .stdout(predicate::str::contains("before").not())
        .stdout(predicate::str::contains("late-old").not());
    assert_eq!(std::fs::read_to_string(ledger).unwrap(), HISTORY);
}

#[test]
fn history_since_rejects_invalid_and_timezone_free_timestamps() {
    let data = common::private_tempdir();
    for since in [
        "yesterday",
        "2026-01-01",
        "2026-01-01T12:00:00",
        "2026-02-30T12:00:00Z",
    ] {
        Command::cargo_bin("vyane")
            .unwrap()
            .env("VYANE_DATA_DIR", data.path())
            .args(["history", "--since", since])
            .assert()
            .code(2);
    }
    assert!(!data.path().join("ledger.jsonl").exists());
}
