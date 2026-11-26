//! `vyane waiting` reads three existing sources and does not write them.

#![allow(clippy::unwrap_used)]

mod common;

use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::Command;
use chrono::{TimeZone as _, Utc};
use serde_json::Value;
use vyane_goal::{
    GoalContinuityMode, GoalContinuityPolicy, GoalExecutionTarget, GoalQuotaEvent, GoalStore,
    NewGoal, SqliteGoalStore, apply_quota_handoff_events,
};
use vyane_service::{ApprovalGrantBinding, DeliveryEvent, KernelStore};

fn vyane() -> Command {
    Command::cargo_bin("vyane").expect("vyane binary")
}

fn at(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).single().unwrap()
}

fn digest(seed: u8) -> String {
    format!("{seed:02x}").repeat(32)
}

fn target(role: &str) -> GoalExecutionTarget {
    GoalExecutionTarget {
        provider: "provider".into(),
        protocol: "openai_chat".into(),
        harness: "harness".into(),
        model: "model".into(),
        profile: None,
        role: role.into(),
    }
}

fn freeze_sqlite(path: &Path) {
    let connection = rusqlite::Connection::open(path).unwrap();
    connection
        .pragma_update(None, "wal_checkpoint", "TRUNCATE")
        .unwrap();
    connection
        .pragma_update(None, "journal_mode", "DELETE")
        .unwrap();
}

fn entries(directory: &Path) -> Vec<String> {
    let mut names = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}

struct Seed {
    directory: tempfile::TempDir,
    goal_db: PathBuf,
    kernel_db: PathBuf,
}

fn seed() -> Seed {
    let directory = common::private_tempdir();
    let goal_db = directory.path().join("goals.sqlite");
    let kernel_db = directory.path().join("kernel.sqlite");
    let goals = SqliteGoalStore::open(&goal_db).unwrap();
    let mut decide = NewGoal::new("Need a decision", at(1_700_000_000));
    decide.id = Some("decide-goal".into());
    decide.continuity_policy = Some(GoalContinuityPolicy {
        mode: GoalContinuityMode::QuotaHandoff,
        primary: target("primary"),
        takeover: Vec::new(),
        reviewer: None,
        resume_primary_after_reset: false,
        require_review_before_resume: false,
        wait_for_review_checks_before_resume: false,
    });
    goals.create("local", decide).unwrap();
    goals
        .start("local", "decide-goal", at(1_700_000_001))
        .unwrap();
    apply_quota_handoff_events(
        &goals,
        "local",
        &[GoalQuotaEvent {
            event_id: "quota-decide".into(),
            goal_id: Some("decide-goal".into()),
            provider: "provider".into(),
            harness: "harness".into(),
            model: "model".into(),
            session_id: None,
            observed_at: at(1_700_000_002),
            estimated_reset_at: None,
        }],
        at(1_700_000_003),
    )
    .unwrap();
    let mut queued = NewGoal::new("Still needs a queue", at(1_700_000_000));
    queued.id = Some("queue-goal".into());
    queued.continuity_policy = Some(GoalContinuityPolicy {
        mode: GoalContinuityMode::QuotaHandoff,
        primary: target("primary"),
        takeover: vec![target("takeover")],
        reviewer: None,
        resume_primary_after_reset: false,
        require_review_before_resume: false,
        wait_for_review_checks_before_resume: false,
    });
    goals.create("local", queued).unwrap();
    goals
        .start("local", "queue-goal", at(1_700_000_001))
        .unwrap();
    apply_quota_handoff_events(
        &goals,
        "local",
        &[GoalQuotaEvent {
            event_id: "quota-queue".into(),
            goal_id: Some("queue-goal".into()),
            provider: "provider".into(),
            harness: "harness".into(),
            model: "model".into(),
            session_id: None,
            observed_at: at(1_700_000_002),
            estimated_reset_at: None,
        }],
        at(1_700_000_003),
    )
    .unwrap();
    drop(goals);

    let kernel = KernelStore::open(&kernel_db).unwrap();
    let now = at(1_700_000_010);
    kernel
        .record_approval_required(
            "local",
            "apr-wait",
            "rcpt-wait",
            "run-wait",
            &digest(1),
            1,
            now,
        )
        .unwrap();
    kernel
        .record_approval_required(
            "local",
            "apr-done",
            "rcpt-done",
            "run-done",
            &digest(2),
            1,
            now,
        )
        .unwrap();
    kernel
        .grant_approval(
            &ApprovalGrantBinding {
                owner: "local".into(),
                receipt_id: "rcpt-done".into(),
                run_id: "run-done".into(),
                request_digest: digest(2),
                expected_revision: 1,
                lease_owner: "lease".into(),
                generation: 1,
                decided_by: "operator".into(),
            },
            now,
        )
        .unwrap();
    let (_phase, revision) = kernel
        .ensure_delivery_running("local", "rcpt-stage", "run-stage", now)
        .unwrap();
    kernel
        .set_delivery_phase(
            "local",
            "rcpt-stage",
            "run-stage",
            revision,
            DeliveryEvent::AskRequired,
            Some("apr-stage"),
            now,
        )
        .unwrap();
    kernel
        .ensure_delivery_running("local", "rcpt-run", "run-run", now)
        .unwrap();
    drop(kernel);
    freeze_sqlite(&goal_db);
    freeze_sqlite(&kernel_db);
    Seed {
        directory,
        goal_db,
        kernel_db,
    }
}

fn run(seed: &Seed, extra: &[&str]) -> std::process::Output {
    let mut command = vyane();
    command
        .env("VYANE_DATA_DIR", seed.directory.path().join("unused-data"))
        .args([
            "waiting",
            "--goal-db",
            seed.goal_db.to_str().unwrap(),
            "--kernel-db",
            seed.kernel_db.to_str().unwrap(),
        ])
        .args(extra);
    command.output().unwrap()
}

fn json(output: &std::process::Output) -> Value {
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn waiting_lists_three_sources_and_names_a_failure_without_writing() {
    let seed = seed();
    let before = (
        fs::read(&seed.goal_db).unwrap(),
        fs::read(&seed.kernel_db).unwrap(),
    );
    let files = entries(seed.directory.path());

    let listed = json(&run(&seed, &["--json"]));
    assert_eq!(listed["schema"], "waiting-list.v1");
    let rows: Vec<_> = listed["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| {
            (
                item["source"].as_str().unwrap(),
                item["id"].as_str().unwrap(),
                item["kind"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            ("approvals", "apr-wait", "pending"),
            ("tasks", "rcpt-stage", "approval_required"),
            ("projections", "decide-goal", "manual_decision"),
        ]
    );
    assert_eq!(listed["items"][2]["label"], "Need a decision");
    let encoded = listed.to_string();
    assert!(!encoded.contains("apr-done"));
    assert!(!encoded.contains("queue-goal"));
    assert!(!encoded.contains("rcpt-run"));
    assert!(!listed.to_string().contains(seed.goal_db.to_str().unwrap()));
    for source in listed["sources"].as_array().unwrap() {
        assert_eq!(source["status"], "ok");
        assert!(source.get("reason").is_none());
    }

    let human = run(&seed, &[]);
    assert_eq!(human.status.code(), Some(0));
    assert!(human.stderr.is_empty());
    let text = String::from_utf8(human.stdout).unwrap();
    assert!(text.contains("approvals ok"));
    assert!(text.contains("tasks ok"));
    assert!(text.contains("projections ok"));
    assert!(text.contains("approvals apr-wait rcpt-wait pending"));
    assert!(text.contains("tasks rcpt-stage run-stage approval_required"));
    assert!(text.contains("projections decide-goal Need a decision manual_decision"));

    let without = json(&run(&seed, &["--json", "--without", "tasks"]));
    assert_eq!(without["sources"][1]["name"], "tasks");
    assert_eq!(without["sources"][1]["status"], "off");
    assert_eq!(without["sources"][1]["reason"], "turned_off");
    let without_ids: Vec<_> = without["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap())
        .collect();
    assert_eq!(without_ids, ["apr-wait", "decide-goal"]);

    let missing = seed.goal_db.with_file_name("missing-goals.sqlite");
    let output = vyane()
        .args([
            "waiting",
            "--json",
            "--goal-db",
            missing.to_str().unwrap(),
            "--kernel-db",
            seed.kernel_db.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    let partial = json(&output);
    assert_eq!(partial["sources"][0]["status"], "ok");
    assert_eq!(partial["sources"][1]["status"], "ok");
    assert_eq!(partial["sources"][2]["name"], "projections");
    assert_eq!(partial["sources"][2]["status"], "unavailable");
    assert_eq!(partial["sources"][2]["reason"], "not_found");
    let partial_ids: Vec<_> = partial["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap())
        .collect();
    assert_eq!(partial_ids, ["apr-wait", "rcpt-stage"]);

    assert_eq!(fs::read(&seed.goal_db).unwrap(), before.0);
    assert_eq!(fs::read(&seed.kernel_db).unwrap(), before.1);
    assert_eq!(entries(seed.directory.path()), files);

    {
        let connection = rusqlite::Connection::open(&seed.kernel_db).unwrap();
        connection
            .execute("DROP TABLE kernel_approvals", [])
            .unwrap();
    }
    freeze_sqlite(&seed.kernel_db);
    let after_drop = fs::read(&seed.kernel_db).unwrap();
    let failed = json(&run(&seed, &["--json"]));
    assert_eq!(fs::read(&seed.kernel_db).unwrap(), after_drop);
    assert_eq!(fs::read(&seed.goal_db).unwrap(), before.0);
    assert_eq!(failed["sources"][0]["name"], "approvals");
    assert_eq!(failed["sources"][0]["status"], "unavailable");
    assert_eq!(failed["sources"][0]["reason"], "read_failed");
    assert_eq!(failed["sources"][1]["status"], "ok");
    assert_eq!(failed["sources"][2]["status"], "ok");
    let failed_ids: Vec<_> = failed["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap())
        .collect();
    assert_eq!(failed_ids, ["rcpt-stage", "decide-goal"]);
    assert!(!failed.to_string().contains("kernel"));
}

#[test]
fn turned_off_sources_do_not_create_a_database() {
    let directory = common::private_tempdir();
    let data = directory.path().join("data");
    let output = vyane()
        .env("VYANE_DATA_DIR", &data)
        .args([
            "waiting",
            "--json",
            "--without",
            "approvals",
            "--without",
            "tasks",
            "--without",
            "projections",
        ])
        .output()
        .unwrap();
    let listed = json(&output);
    assert!(listed["items"].as_array().unwrap().is_empty());
    assert!(
        listed["sources"]
            .as_array()
            .unwrap()
            .iter()
            .all(|source| source["status"] == "off" && source["reason"] == "turned_off")
    );
    assert!(!data.exists());
}

#[test]
fn an_invalid_owner_exits_without_echoing_the_value() {
    let directory = common::private_tempdir();
    let output = vyane()
        .env("VYANE_DATA_DIR", directory.path().join("data"))
        .args(["waiting", "--owner", " bad", "--without", "projections"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "owner is invalid\n"
    );
    assert!(!directory.path().join("data").exists());

    let output = vyane()
        .args([
            "waiting",
            "--json",
            "--owner",
            " bad",
            "--without",
            "projections",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stderr.is_empty());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "{\"status\":\"error\",\"error\":\"owner is invalid\"}\n"
    );
}
