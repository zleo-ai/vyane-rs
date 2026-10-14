#![allow(clippy::unwrap_used)]

mod common;

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;

const HISTORY: &str = include_str!("fixtures/history_since.jsonl");

#[test]
fn history_until_bounds_lists_and_statistics_before_pagination() {
    let data = common::private_tempdir();
    let ledger = data.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY).unwrap();
    for (filters, expected) in [
        (
            vec!["--until", "2026-01-01T12:00:00Z"],
            vec!["late-old", "boundary", "before"],
        ),
        (
            vec![
                "--since",
                "2026-01-01T12:00:00Z",
                "--until",
                "2026-01-01T14:00:00+02:00",
            ],
            vec!["boundary"],
        ),
        (
            vec!["--until", "2026-01-01T14:00:00.001+02:00"],
            vec!["late-old", "after", "boundary", "before"],
        ),
        (
            vec![
                "--since",
                "2026-01-01T11:00:00Z",
                "--until",
                "2026-01-01T12:00:00Z",
                "--limit",
                "1",
            ],
            vec!["boundary"],
        ),
        (
            vec![
                "--until",
                "2026-01-01T14:00:00Z",
                "--provider",
                "p",
                "--status",
                "error",
            ],
            vec!["error"],
        ),
        (vec!["--until", "2026-01-01T09:00:00Z"], vec![]),
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
            .map(|r| r["run_id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, expected, "filters: {filters:?}");
    }
    for (filters, total, success, error) in [
        (vec!["--until", "2026-01-01T12:00:00Z"], 3, 3, 0),
        (
            vec![
                "--since",
                "2026-01-01T12:00:00Z",
                "--until",
                "2026-01-01T12:00:00Z",
            ],
            1,
            1,
            0,
        ),
        (
            vec![
                "--until",
                "2026-01-01T14:00:00Z",
                "--provider",
                "p",
                "--status",
                "error",
            ],
            1,
            0,
            1,
        ),
        (vec!["--until", "2026-01-01T09:00:00Z"], 0, 0, 0),
    ] {
        let output = Command::cargo_bin("vyane")
            .unwrap()
            .env("VYANE_DATA_DIR", data.path())
            .args(["history", "--stats", "--json"])
            .args(&filters)
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let body: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(body["total"], total);
        assert_eq!(body["success"], success);
        assert_eq!(body["error"], error);
        assert_eq!(body["timeout"], 0);
        assert_eq!(body["cancelled"], 0);
        assert!(body["by_provider"].get("q").is_none());
    }
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", data.path())
        .args([
            "history",
            "--since",
            "2026-01-01T12:00:00Z",
            "--until",
            "2026-01-01T12:00:00Z",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("boundary"))
        .stdout(predicate::str::contains("after").not());
    assert_eq!(std::fs::read_to_string(ledger).unwrap(), HISTORY);
}

#[test]
fn history_until_rejects_invalid_dates_and_reversed_windows_without_writing() {
    let data = common::private_tempdir();
    for mode in [vec![], vec!["--stats"]] {
        for until in [
            "yesterday",
            "2026-01-01",
            "2026-01-01T12:00:00",
            "2026-02-30T12:00:00Z",
        ] {
            Command::cargo_bin("vyane")
                .unwrap()
                .env("VYANE_DATA_DIR", data.path())
                .args(["history", "--until", until])
                .args(&mode)
                .assert()
                .code(2);
        }
        Command::cargo_bin("vyane")
            .unwrap()
            .env("VYANE_DATA_DIR", data.path())
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
    assert!(!data.path().join("ledger.jsonl").exists());
}
