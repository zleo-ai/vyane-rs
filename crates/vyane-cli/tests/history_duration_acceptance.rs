#![allow(clippy::unwrap_used)]
mod common;
use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::{Value, json};
const HISTORY: &str = include_str!("fixtures/history_duration.jsonl");

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

#[test]
fn history_duration_cli_counts_complete_filtered_samples_without_storage_writes() {
    let root = common::private_tempdir();
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY).unwrap();
    for (filters, total, expected) in [
        (
            vec![],
            35,
            json!({"samples":34,"invalid":1,"total_ms":7000,"mean_ms":205.9}),
        ),
        (
            vec!["--provider", "q"],
            2,
            json!({"samples":1,"invalid":1,"total_ms":2750,"mean_ms":2750.0}),
        ),
        (
            vec!["--status", "success"],
            2,
            json!({"samples":2,"invalid":0,"total_ms":1250,"mean_ms":625.0}),
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
            2,
            json!({"samples":2,"invalid":0,"total_ms":0,"mean_ms":0.0}),
        ),
    ] {
        let body = stats(root.path(), &filters);
        assert_eq!(body["total"], total, "{filters:?}");
        assert_eq!(body["duration"], expected, "{filters:?}");
        assert!(!body.to_string().contains("DURATION_PRIVATE_CANARY"));
        assert!(
            body["by_provider"]
                .get("foreign-duration-provider")
                .is_none()
        );
        assert_eq!(std::fs::read_to_string(&ledger).unwrap(), HISTORY);
    }
    let output = Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let list: Vec<Value> = serde_json::from_slice(&output).unwrap();
    assert_eq!(list.len(), 20);
    assert!(list.iter().all(|record| record.get("duration").is_none()));
}

#[test]
fn history_duration_cli_distinguishes_zero_invalid_and_empty_samples() {
    let root = common::private_tempdir();
    let empty = stats(root.path(), &[]);
    assert_eq!(
        empty["duration"],
        json!({"samples":0,"invalid":0,"total_ms":null,"mean_ms":null})
    );
    assert!(!root.path().join("ledger.jsonl").exists());
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY).unwrap();
    let invalid = stats(root.path(), &["--status", "cancelled"]);
    assert_eq!(invalid["total"], 1);
    assert_eq!(
        invalid["duration"],
        json!({"samples":0,"invalid":1,"total_ms":null,"mean_ms":null})
    );
    let empty = stats(root.path(), &["--provider", "absent"]);
    assert_eq!(
        empty["duration"],
        json!({"samples":0,"invalid":0,"total_ms":null,"mean_ms":null})
    );
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--stats"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Duration: total 7000 ms, mean 205.9 ms (samples: 34, invalid: 1)",
        ));
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--stats", "--status", "cancelled"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Duration: total n/a, mean n/a (samples: 0, invalid: 1)",
        ));
    assert_eq!(std::fs::read_to_string(&ledger).unwrap(), HISTORY);
}

#[test]
fn history_duration_cli_applies_existing_local_session_and_label_selection() {
    let root = common::private_tempdir();
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY).unwrap();
    let body = stats(
        root.path(),
        &["--session", "duration-session", "--label", "team=alpha"],
    );
    assert_eq!(body["total"], 5);
    assert_eq!(
        body["duration"],
        json!({"samples":4,"invalid":1,"total_ms":4000,"mean_ms":1000.0})
    );
    let body = stats(
        root.path(),
        &[
            "--session",
            "duration-session",
            "--label",
            "team=alpha",
            "--label",
            "team=beta",
        ],
    );
    assert_eq!(
        body["duration"],
        json!({"samples":0,"invalid":0,"total_ms":null,"mean_ms":null})
    );
    assert_eq!(std::fs::read_to_string(&ledger).unwrap(), HISTORY);
}
