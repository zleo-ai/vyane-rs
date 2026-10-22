#![allow(clippy::unwrap_used)]
mod common;
use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::{Value, json};
const HISTORY: &str = include_str!("../../vyane-service/tests/fixtures/history_cost.jsonl");
fn stats(root: &std::path::Path, args: &[&str]) -> Value {
    let output = Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root)
        .args(["history", "--stats", "--json"])
        .args(args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    serde_json::from_slice(&output).unwrap()
}
#[test]
fn history_cost_cli_filters_all_owned_runs_without_guessing_missing_prices() {
    let root = common::private_tempdir();
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, HISTORY).unwrap();
    for (args, expected) in [
        (
            vec![],
            json!({"known":123,"unknown":1,"invalid":1,"known_total_usd":62.0,"overflowed":false}),
        ),
        (
            vec!["--provider", "p"],
            json!({"known":122,"unknown":0,"invalid":0,"known_total_usd":61.25,"overflowed":false}),
        ),
        (
            vec!["--provider", "q"],
            json!({"known":1,"unknown":1,"invalid":1,"known_total_usd":0.75,"overflowed":false}),
        ),
        (
            vec!["--status", "timeout"],
            json!({"known":0,"unknown":1,"invalid":0,"known_total_usd":null,"overflowed":false}),
        ),
        (
            vec!["--status", "cancelled"],
            json!({"known":0,"unknown":0,"invalid":1,"known_total_usd":null,"overflowed":false}),
        ),
        (
            vec![
                "--since",
                "2026-01-01T13:00:00+02:00",
                "--until",
                "2026-01-01T13:00:00Z",
            ],
            json!({"known":1,"unknown":1,"invalid":1,"known_total_usd":1.25,"overflowed":false}),
        ),
        (
            vec!["--session", "kept", "--label", "team=alpha"],
            json!({"known":2,"unknown":1,"invalid":1,"known_total_usd":1.25,"overflowed":false}),
        ),
        (
            vec!["--provider", "absent"],
            json!({"known":0,"unknown":0,"invalid":0,"known_total_usd":null,"overflowed":false}),
        ),
    ] {
        let body = stats(root.path(), &args);
        assert_eq!(body["cost"], expected, "{args:?}");
        assert_eq!(
            body["total"].as_u64().unwrap(),
            expected["known"].as_u64().unwrap()
                + expected["unknown"].as_u64().unwrap()
                + expected["invalid"].as_u64().unwrap()
        );
        assert!(!body.to_string().contains("COST_PRIVATE_CANARY"));
        assert!(!body.to_string().contains("earlier-attempt"));
    }
    assert_eq!(std::fs::read_to_string(&ledger).unwrap(), HISTORY);
}
#[test]
fn history_cost_cli_empty_and_overflow_are_explicit_and_read_only() {
    let root = common::private_tempdir();
    assert_eq!(
        stats(root.path(), &[])["cost"],
        json!({"known":0,"unknown":0,"invalid":0,"known_total_usd":null,"overflowed":false})
    );
    assert!(!root.path().join("ledger.jsonl").exists());
    let mut row: Value = serde_json::from_str(HISTORY.lines().next().unwrap()).unwrap();
    row["cost_usd"] = json!(1e308);
    let input = format!("{row}\n{row}\n");
    let ledger = root.path().join("ledger.jsonl");
    std::fs::write(&ledger, &input).unwrap();
    assert_eq!(
        stats(root.path(), &[])["cost"],
        json!({"known":2,"unknown":0,"invalid":0,"known_total_usd":null,"overflowed":true})
    );
    assert_eq!(std::fs::read_to_string(ledger).unwrap(), input);
}
#[test]
fn history_cost_cli_text_labels_known_saved_cost_and_unknown_coverage() {
    let root = common::private_tempdir();
    std::fs::write(root.path().join("ledger.jsonl"), HISTORY).unwrap();
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--stats"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Known saved cost: 62 USD (known: 123, unknown: 1, invalid: 1, overflowed: false)",
        ));
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--stats", "--status", "timeout"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Known saved cost: n/a USD (known: 0, unknown: 1, invalid: 0, overflowed: false)",
        ));
}
