#![allow(clippy::unwrap_used)]

mod common;

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::{Value, json};

fn fixture() -> String {
    let template: Value = serde_json::from_str(
        include_str!("fixtures/history_stats.jsonl")
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    let mut rows = Vec::new();
    for (id, owner, session, team, status, provider, hour) in [
        (
            "match-success",
            Some("local"),
            Some("session-a"),
            "alpha",
            "success",
            "q",
            11,
        ),
        (
            "match-error",
            Some("local"),
            Some("session-a"),
            "alpha",
            "error",
            "p",
            12,
        ),
        (
            "legacy-timeout",
            None,
            Some("session-a"),
            "alpha",
            "timeout",
            "p",
            13,
        ),
        (
            "different-label",
            Some("local"),
            Some("session-a"),
            "alphabet",
            "success",
            "q",
            14,
        ),
        (
            "session-prefix",
            Some("local"),
            Some("session-a-extra"),
            "alpha",
            "success",
            "q",
            14,
        ),
        (
            "missing-session",
            Some("local"),
            None,
            "alpha",
            "success",
            "q",
            14,
        ),
        (
            "foreign",
            Some("foreign"),
            Some("session-a"),
            "alpha",
            "success",
            "q",
            14,
        ),
        (
            "owner-prefix",
            Some("local-extra"),
            Some("session-a"),
            "alpha",
            "success",
            "q",
            14,
        ),
    ] {
        let mut row = template.clone();
        row["run_id"] = json!(id);
        if let Some(owner) = owner {
            row["owner"] = json!(owner);
        } else {
            row.as_object_mut().unwrap().remove("owner");
        }
        row["session_id"] = json!(session);
        row["labels"] = json!({"team":team,"note":"a=b","empty":""});
        row["status"] = json!(status);
        row["target"]["provider"] = json!(provider);
        row["started_at"] = json!(format!("2026-01-01T{hour:02}:00:00Z"));
        row["finished_at"] = json!(format!("2026-01-01T{hour:02}:00:01Z"));
        rows.push(row.to_string());
    }
    // Matching rows are older than a full default page of unrelated records.
    for i in 0..60 {
        let mut row = template.clone();
        row["run_id"] = json!(format!("unrelated-{i}"));
        row["session_id"] = json!("session-b");
        row["labels"] = json!({"team":"beta"});
        rows.push(row.to_string());
    }
    rows.join("\n") + "\n\nnot-json\n"
}

fn query(root: &std::path::Path, args: &[&str]) -> Value {
    let output = Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root)
        .args(["history", "--json"])
        .args(args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    serde_json::from_slice(&output).unwrap()
}

#[test]
fn local_history_filters_are_exact_owned_and_applied_before_limit() {
    let root = common::private_tempdir();
    let ledger = root.path().join("ledger.jsonl");
    let rows = fixture();
    std::fs::write(&ledger, &rows).unwrap();
    let selectors = ["--session", "session-a", "--label", "team=alpha"];
    let filtered = query(root.path(), &selectors);
    let ids: Vec<_> = filtered
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["run_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["legacy-timeout", "match-error", "match-success"]);
    assert!(
        filtered
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["owner"] == "local")
    );
    let first = query(
        root.path(),
        &[
            "--session",
            "session-a",
            "--label",
            "team=alpha",
            "--limit",
            "1",
        ],
    );
    assert_eq!(first.as_array().unwrap().len(), 1);
    assert_eq!(first[0]["run_id"], "legacy-timeout");
    let empty = query(
        root.path(),
        &[
            "--session",
            "session-a",
            "--label",
            "team=alpha",
            "--limit",
            "0",
        ],
    );
    assert_eq!(empty, json!([]));
    let error = query(
        root.path(),
        &[
            "--session",
            "session-a",
            "--label",
            "team=alpha",
            "--status",
            "error",
        ],
    );
    assert_eq!(error.as_array().unwrap().len(), 1);
    assert_eq!(error[0]["run_id"], "match-error");
    let labels_only = query(root.path(), &["--label", "team=alpha"]);
    assert_eq!(labels_only.as_array().unwrap().len(), 5);
    assert!(
        labels_only
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["run_id"] == "missing-session")
    );
    let unchanged = query(root.path(), &[]);
    assert_eq!(unchanged.as_array().unwrap().len(), 20);
    assert_eq!(unchanged[0]["run_id"], "unrelated-59");
    assert_eq!(std::fs::read_to_string(ledger).unwrap(), rows);
}

#[test]
fn local_history_filter_statistics_use_every_match_and_keep_private_projection() {
    let root = common::private_tempdir();
    let ledger = root.path().join("ledger.jsonl");
    let rows = fixture();
    std::fs::write(&ledger, &rows).unwrap();
    let stats = query(
        root.path(),
        &[
            "--stats",
            "--session",
            "session-a",
            "--label",
            "team=alpha",
            "--label",
            "note=a=b",
            "--label",
            "empty=",
        ],
    );
    assert_eq!(stats["total"], 3);
    assert_eq!(stats["success"], 1);
    assert_eq!(stats["error"], 1);
    assert_eq!(stats["timeout"], 1);
    assert_eq!(stats["success_rate"], 33.3);
    assert_eq!(stats["by_provider"]["p"]["total"], 2);
    assert_eq!(stats["by_provider"]["q"]["total"], 1);
    let mut keys: Vec<_> = stats
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "by_provider",
            "cancelled",
            "error",
            "success",
            "success_rate",
            "timeout",
            "total"
        ]
    );
    assert!(!stats.to_string().contains("session-a"));
    assert!(!stats.to_string().contains("STATS_PRIVATE_DATA_CANARY"));
    let bounded = query(
        root.path(),
        &[
            "--stats",
            "--session",
            "session-a",
            "--label",
            "team=alpha",
            "--since",
            "2026-01-01T12:00:00Z",
            "--until",
            "2026-01-01T15:00:00+02:00",
        ],
    );
    assert_eq!(bounded["total"], 2);
    assert_eq!(bounded["success_rate"], 0.0);
    let provider = query(
        root.path(),
        &[
            "--stats",
            "--session",
            "session-a",
            "--label",
            "team=alpha",
            "--provider",
            "q",
        ],
    );
    assert_eq!(provider["total"], 1);
    assert_eq!(provider["success_rate"], 100.0);
    let contradiction = query(
        root.path(),
        &[
            "--stats",
            "--label",
            "team=alpha",
            "--label",
            "team=alphabet",
        ],
    );
    assert_eq!(contradiction["total"], 0);
    assert!(contradiction["success_rate"].is_null());
    assert_eq!(std::fs::read_to_string(ledger).unwrap(), rows);
}

#[test]
fn local_history_filter_validation_never_creates_storage() {
    let root = common::private_tempdir();
    for label in ["missing-equals", "=value"] {
        Command::cargo_bin("vyane")
            .unwrap()
            .env("VYANE_DATA_DIR", root.path())
            .args(["history", "--label", label])
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "history label filter must be KEY=VALUE",
            ));
    }
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args(["history", "--session", ""])
        .assert()
        .code(2);
    Command::cargo_bin("vyane")
        .unwrap()
        .env("VYANE_DATA_DIR", root.path())
        .args([
            "history",
            "--stats",
            "--label",
            "team=alpha",
            "--limit",
            "1",
        ])
        .assert()
        .code(2);
    let empty = query(
        root.path(),
        &["--stats", "--session", "absent", "--label", "team=alpha"],
    );
    assert_eq!(empty["total"], 0);
    assert!(empty["success_rate"].is_null());
    assert!(!root.path().join("ledger.jsonl").exists());
}
