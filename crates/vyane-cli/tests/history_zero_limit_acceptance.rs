#![allow(clippy::unwrap_used)]
mod common;
use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;
const HISTORY: &str = include_str!("fixtures/history_duration.jsonl");

#[test]
fn history_zero_limit_returns_empty_for_all_local_list_forms_without_changing_ledger() {
    let root = common::private_tempdir();
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY).unwrap();
    for extra in [
        vec![],
        vec!["--json"],
        vec!["--provider", "p"],
        vec!["--status", "success"],
        vec!["--session", "duration-session"],
        vec!["--label", "team=alpha"],
        vec!["--session", "duration-session", "--label", "team=alpha"],
        vec![
            "--since",
            "2026-01-01T10:00:00Z",
            "--until",
            "2026-01-01T14:00:00Z",
        ],
    ] {
        Command::cargo_bin("vyane")
            .unwrap()
            .env("VYANE_DATA_DIR", root.path())
            .args(["history", "--limit", "0"])
            .args(&extra)
            .assert()
            .success()
            .stdout(predicate::eq(if extra.contains(&"--json") {
                "[]\n"
            } else {
                ""
            }))
            .stderr(predicate::str::is_empty());
        assert_eq!(
            std::fs::read_to_string(&ledger).unwrap(),
            HISTORY,
            "{extra:?}"
        );
    }
}

#[test]
fn history_zero_limit_returns_empty_before_creating_any_storage() {
    let parent = common::private_tempdir();
    for (index, extra) in [
        vec![],
        vec!["--json"],
        vec!["--session", "duration-session"],
        vec!["--label", "team=alpha"],
    ]
    .iter()
    .enumerate()
    {
        let root = parent.path().join(format!("absent-{index}"));
        assert!(!root.exists());
        Command::cargo_bin("vyane")
            .unwrap()
            .env("VYANE_DATA_DIR", &root)
            .args(["history", "--limit", "0"])
            .args(extra)
            .assert()
            .success()
            .stdout(predicate::eq(if extra.contains(&"--json") {
                "[]\n"
            } else {
                ""
            }))
            .stderr(predicate::str::is_empty());
        assert!(!root.exists(), "invalid command created storage: {extra:?}");
    }
}

#[test]
fn history_positive_limits_defaults_and_statistics_keep_their_behavior() {
    let root = common::private_tempdir();
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY).unwrap();
    for (extra, count) in [
        (vec![], 20),
        (vec!["--limit", "1"], 1),
        (vec!["--limit", "100"], 35),
        (
            vec![
                "--session",
                "duration-session",
                "--label",
                "team=alpha",
                "--limit",
                "1",
            ],
            1,
        ),
    ] {
        let output = Command::cargo_bin("vyane")
            .unwrap()
            .env("VYANE_DATA_DIR", root.path())
            .args(["history", "--json"])
            .args(&extra)
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let rows: Vec<Value> = serde_json::from_slice(&output).unwrap();
        assert_eq!(rows.len(), count, "{extra:?}");
        assert!(
            rows.iter()
                .all(|r| r["owner"].is_null() || r["owner"] == "local")
        );
    }
    let output = Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--stats", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let stats: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(stats["total"], 35);
    assert_eq!(stats["duration"]["total_ms"], 7000);
    assert_eq!(std::fs::read_to_string(&ledger).unwrap(), HISTORY);
    let parent = common::private_tempdir();
    let absent = parent.path().join("absent-stats");
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", &absent)
        .args(["history", "--stats", "--limit", "0"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
    assert!(!absent.exists());
    for (index, filters) in [
        vec!["--label", "=bad"],
        vec![
            "--since",
            "2026-01-01T14:00:00Z",
            "--until",
            "2026-01-01T10:00:00Z",
        ],
    ]
    .iter()
    .enumerate()
    {
        let invalid = parent.path().join(format!("invalid-filter-{index}"));
        Command::cargo_bin("vyane")
            .unwrap()
            .env("VYANE_DATA_DIR", &invalid)
            .args(["history", "--limit", "0"])
            .args(filters)
            .assert()
            .failure()
            .stdout(predicate::str::is_empty());
        assert!(!invalid.exists());
    }
}
