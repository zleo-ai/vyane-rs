#![allow(clippy::unwrap_used)]

use std::path::Path;
#[cfg(unix)]
use std::process::Stdio;
#[cfg(unix)]
use std::thread;
#[cfg(unix)]
use std::time::Duration;
#[cfg(unix)]
use std::{fs, os::unix::fs::PermissionsExt as _};

use assert_cmd::Command;
use chrono::{TimeZone as _, Utc};
use rusqlite::Connection;
use serde_json::Value;
use tempfile::TempDir;
use vyane_goal::{
    GoalContinuityMode, GoalContinuityPolicy, GoalContinuityStepStatus, GoalExecutionTarget,
    GoalQuotaEvent, GoalStore, NewGoal, SqliteGoalStore, TakeoverApprovalStatus, TakeoverFinish,
    TakeoverRunStatus, apply_quota_handoff_events,
};

fn vyane() -> Command {
    Command::cargo_bin("vyane").expect("vyane binary")
}

fn db_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn json_output(args: &[&str], expected_code: i32) -> Value {
    let output = vyane().args(args).output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(expected_code),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn create(db: &str, owner: &str, id: &str, title: &str, priority: &str) -> Value {
    json_output(
        &[
            "goal",
            "create",
            "--db",
            db,
            "--owner",
            owner,
            "--json",
            "--id",
            id,
            "--title",
            title,
            "--priority",
            priority,
        ],
        0,
    )
}

#[cfg(unix)]
fn pursuit_fixture(directory: &TempDir) -> (std::path::PathBuf, TempDir) {
    let config = directory.path().join("config.toml");
    fs::write(
        &config,
        r#"
        [providers.native]
        base_url = "https://unused.invalid"
        auth_style = "x_api_key"
        protocol = "anthropic_messages"
        default_model = "test-model"

        [profiles.builder]
        provider = "native"
        protocol = "anthropic_messages"
        harness = "claude-code"
        model = "test-model"
        "#,
    )
    .expect("write pursuit config");
    let bin = TempDir::new().expect("bin tempdir");
    let claude = bin.path().join("claude");
    #[cfg(target_os = "linux")]
    let script = r#"#!/bin/sh
: > "$PWD/done.txt"
printf '%s\n' "$*" > "$PWD/last-prompt.txt"
printf '%s\n' '{"result":"segment complete","session_id":"fresh-segment"}'
"#;
    #[cfg(not(target_os = "linux"))]
    let script = r#"#!/bin/sh
printf '%s\n' '{"result":"segment complete","session_id":"fresh-segment"}'
"#;
    fs::write(&claude, script).expect("write fake claude");
    fs::set_permissions(&claude, fs::Permissions::from_mode(0o755)).expect("chmod fake claude");
    (config, bin)
}

#[cfg(unix)]
fn pursuit_path(bin: &TempDir) -> std::ffi::OsString {
    std::env::join_paths([bin.path(), Path::new("/usr/bin"), Path::new("/bin")])
        .expect("join pursuit fixture PATH")
}

#[cfg(all(unix, target_os = "linux"))]
const PURSUIT_SANDBOX: &str = "write";

// Writable pinned workdirs are currently supported only on Linux.
#[cfg(all(unix, not(target_os = "linux")))]
const PURSUIT_SANDBOX: &str = "read-only";

#[cfg(all(unix, target_os = "linux"))]
fn pursuit_acceptance(_: &TempDir) -> String {
    "custom:cmd:/usr/bin/test -f done.txt".into()
}

#[cfg(all(unix, not(target_os = "linux")))]
fn pursuit_acceptance(directory: &TempDir) -> String {
    let check = directory.path().join("pursuit-ready-check");
    fs::write(
        &check,
        r#"#!/bin/sh
if [ -f pursuit-ready ]; then
    exit 0
fi
: > pursuit-ready
exit 1
"#,
    )
    .expect("write portable pursuit acceptance check");
    fs::set_permissions(&check, fs::Permissions::from_mode(0o755))
        .expect("chmod portable pursuit acceptance check");
    "custom:cmd:./pursuit-ready-check".into()
}

#[test]
fn lifecycle_round_trip_has_stable_json_and_persisted_acceptance() {
    let directory = TempDir::new().unwrap();
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let created = json_output(
        &[
            "goal",
            "create",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--id",
            "goal-public",
            "--title",
            "Ship goal lifecycle",
            "--description",
            "Exercise every Phase 1 transition",
            "--priority",
            "1",
            "--acceptance",
            "test-passes:workspace",
            "--acceptance",
            "manual-confirm:release approved",
        ],
        0,
    );
    assert_eq!(created["status"], "success");
    assert_eq!(created["goal"]["status"], "queued");
    assert_eq!(
        created["goal"]["acceptance_criteria"][0]["kind"],
        "test-passes"
    );

    for args in [
        vec![
            "goal",
            "start",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "goal-public",
        ],
        vec![
            "goal",
            "progress",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "goal-public",
            "--stage",
            "implementation",
            "--detail",
            "store is wired",
        ],
        vec![
            "goal",
            "pause",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "goal-public",
            "--reason",
            "review",
        ],
        vec![
            "goal",
            "resume",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "goal-public",
        ],
        vec![
            "goal",
            "satisfy",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "goal-public",
            "--index",
            "0",
        ],
        vec![
            "goal",
            "satisfy",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "goal-public",
            "--index",
            "1",
        ],
        vec![
            "goal",
            "done",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "goal-public",
            "--summary",
            "verified",
        ],
    ] {
        assert_eq!(json_output(&args, 0)["status"], "success");
    }

    let detail = json_output(
        &[
            "goal",
            "get",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "goal-public",
        ],
        0,
    );
    assert_eq!(detail["goal"]["status"], "completed");
    assert_eq!(detail["goal"]["revision"], 7);
    assert_eq!(detail["verifications"], serde_json::json!([]));
    assert_eq!(detail["goal"]["completion_summary"], "verified");
    assert!(detail["goal"]["acceptance_criteria"][0]["satisfied_at"].is_string());
    assert!(detail["goal"]["acceptance_criteria"][1]["satisfied_at"].is_string());
    assert_eq!(detail["events"].as_array().unwrap().len(), 8);
    assert_eq!(detail["events"][2]["stage"], "implementation");
}

#[test]
fn create_persists_typed_continuity_policy_without_starting_handoff() {
    let directory = TempDir::new().unwrap();
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let policy = r#"{"mode":"quota_handoff","primary":{"provider":"primary","protocol":"openai_responses","harness":"codex-cli","model":"main","role":"primary"},"takeover":[{"provider":"backup","protocol":"anthropic_messages","harness":"claude-code","model":"fallback","role":"takeover"}],"reviewer":{"provider":"primary","protocol":"openai_responses","harness":"codex-cli","model":"main","role":"reviewer"},"resume_primary_after_reset":true,"require_review_before_resume":true}"#;

    let created = json_output(
        &[
            "goal",
            "create",
            "--db",
            &db,
            "--json",
            "--id",
            "continuity-policy",
            "--title",
            "Keep work visible",
            "--continuity-policy-json",
            policy,
        ],
        0,
    );

    assert_eq!(
        created["goal"]["continuity_policy"]["primary"]["harness"],
        "codex-cli"
    );
    assert_eq!(
        created["goal"]["continuity_policy"]["takeover"][0]["provider"],
        "backup"
    );
    assert!(created["goal"]["continuity_state"].is_null());
    let fetched = json_output(
        &["goal", "get", "--db", &db, "--json", "continuity-policy"],
        0,
    );
    assert_eq!(
        fetched["goal"]["continuity_policy"]["mode"],
        "quota_handoff"
    );
    assert!(fetched["goal"]["continuity_state"].is_null());
}

#[test]
fn malformed_continuity_policy_fails_before_goal_creation() {
    let directory = TempDir::new().unwrap();
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let output = vyane()
        .args([
            "goal",
            "create",
            "--db",
            &db,
            "--id",
            "bad-continuity",
            "--title",
            "Reject malformed policy",
            "--continuity-policy-json",
            "{not-json}",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("parse continuity policy JSON"));

    let missing = json_output(&["goal", "get", "--db", &db, "--json", "bad-continuity"], 2);
    assert_eq!(missing["status"], "error");
}

#[test]
fn continuity_signal_is_typed_json_and_never_dispatches() {
    let directory = TempDir::new().unwrap();
    let db_path = directory.path().join("goals.sqlite3");
    let db = db_text(&db_path);
    let policy = r#"{"mode":"quota_handoff","primary":{"provider":"primary","protocol":"openai_responses","harness":"codex-cli","model":"main","role":"primary"},"takeover":[{"provider":"backup","protocol":"anthropic_messages","harness":"claude-code","model":"fallback","role":"takeover"}],"reviewer":{"provider":"primary","protocol":"openai_responses","harness":"codex-cli","model":"main","role":"reviewer"},"resume_primary_after_reset":true,"require_review_before_resume":true}"#;
    json_output(
        &[
            "goal",
            "create",
            "--db",
            &db,
            "--json",
            "--id",
            "signal-goal",
            "--title",
            "Typed signal",
            "--continuity-policy-json",
            policy,
        ],
        0,
    );
    json_output(&["goal", "start", "--db", &db, "--json", "signal-goal"], 0);
    let store = SqliteGoalStore::open(&db_path).unwrap();
    apply_quota_handoff_events(
        &store,
        "local",
        &[GoalQuotaEvent {
            event_id: "quota-signal".into(),
            goal_id: Some("signal-goal".into()),
            provider: "primary".into(),
            harness: "codex-cli".into(),
            model: "main".into(),
            session_id: None,
            observed_at: Utc.timestamp_opt(1_000, 0).unwrap(),
            estimated_reset_at: None,
        }],
        Utc.timestamp_opt(1_001, 0).unwrap(),
    )
    .unwrap();

    let wrong = json_output(
        &[
            "goal",
            "continuity-signal",
            "--db",
            &db,
            "--json",
            "signal-goal",
            "quota-reset",
            "--quota-event-id",
            "quota-signal",
            "--provider",
            "wrong-primary",
            "--harness",
            "codex-cli",
            "--model",
            "main",
            "--source",
            "test-reader",
        ],
        2,
    );
    assert_eq!(wrong["status"], "error");
    assert!(
        wrong["error"]
            .as_str()
            .unwrap()
            .contains("does not match the current primary quota boundary")
    );

    let recorded = json_output(
        &[
            "goal",
            "continuity-signal",
            "--db",
            &db,
            "--json",
            "signal-goal",
            "quota-reset",
            "--quota-event-id",
            "quota-signal",
            "--provider",
            "primary",
            "--harness",
            "codex-cli",
            "--model",
            "main",
            "--source",
            "test-reader",
        ],
        0,
    );
    assert_eq!(recorded["status"], "success");
    assert_eq!(recorded["changed"], true);
    assert_eq!(recorded["result"]["signal"]["kind"], "quota_reset");
    assert_eq!(
        recorded["result"]["state"]["ready_signals"][0]["quota_event_id"],
        "quota-signal"
    );
    assert_eq!(
        recorded["result"]["state"]["handoff_plan"]["steps"][2]["status"],
        "waiting_for_review"
    );

    let goal = store.get("local", "signal-goal").unwrap().unwrap();
    assert_eq!(goal.revision, 3);
    assert_eq!(store.events("local", "signal-goal").unwrap().len(), 4);

    let repeated = json_output(
        &[
            "goal",
            "continuity-signal",
            "--db",
            &db,
            "--json",
            "signal-goal",
            "quota-reset",
            "--quota-event-id",
            "quota-signal",
            "--provider",
            "primary",
            "--harness",
            "codex-cli",
            "--model",
            "main",
            "--source",
            "test-reader",
        ],
        0,
    );
    assert_eq!(repeated["changed"], false);
    assert_eq!(
        repeated["result"]["signal"]["observed_at"],
        recorded["result"]["signal"]["observed_at"]
    );
    assert_eq!(
        store.get("local", "signal-goal").unwrap().unwrap().revision,
        3
    );
    assert_eq!(store.events("local", "signal-goal").unwrap().len(), 4);

    let help = vyane()
        .args(["goal", "continuity-signal", "--help"])
        .output()
        .unwrap();
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("never dispatches"));
}

#[test]
fn continuity_queue_and_decide_are_explicit_json_roundtrip() {
    let directory = TempDir::new().unwrap();
    let db_path = directory.path().join("goals.sqlite3");
    let db = db_text(&db_path);
    let policy = r#"{"mode":"quota_handoff","primary":{"provider":"primary","protocol":"openai_responses","harness":"codex-cli","model":"main","role":"primary"},"takeover":[{"provider":"backup","protocol":"anthropic_messages","harness":"claude-code","model":"fallback","role":"takeover"}],"reviewer":{"provider":"primary","protocol":"openai_responses","harness":"codex-cli","model":"main","role":"reviewer"},"resume_primary_after_reset":true,"require_review_before_resume":true}"#;
    json_output(
        &[
            "goal",
            "create",
            "--db",
            &db,
            "--json",
            "--id",
            "controlled",
            "--title",
            "Controlled takeover",
            "--continuity-policy-json",
            policy,
        ],
        0,
    );
    json_output(&["goal", "start", "--db", &db, "--json", "controlled"], 0);
    let store = SqliteGoalStore::open(&db_path).unwrap();
    apply_quota_handoff_events(
        &store,
        "local",
        &[GoalQuotaEvent {
            event_id: "quota-cli".into(),
            goal_id: Some("controlled".into()),
            provider: "primary".into(),
            harness: "codex-cli".into(),
            model: "main".into(),
            session_id: None,
            observed_at: Utc.timestamp_opt(1_000, 0).unwrap(),
            estimated_reset_at: None,
        }],
        Utc.timestamp_opt(1_001, 0).unwrap(),
    )
    .unwrap();

    let workdir = db_text(directory.path());
    let queued = json_output(
        &[
            "goal",
            "continuity-queue",
            "--db",
            &db,
            "--json",
            "controlled",
            "--workdir",
            &workdir,
            "--sandbox",
            "write",
            "--timeout-seconds",
            "30",
        ],
        0,
    );
    assert_eq!(queued["status"], "pending");
    assert_eq!(queued["approval"]["target"]["provider"], "backup");
    assert_eq!(queued["approval"]["timeout_secs"], 30);
    let approval_id = queued["approval"]["approval_id"].as_str().unwrap();

    let not_approved = json_output(
        &[
            "goal",
            "continuity-execute",
            "--db",
            &db,
            "--json",
            approval_id,
        ],
        2,
    );
    assert!(not_approved["error"].as_str().unwrap().contains("pending"));
    assert_eq!(
        store
            .get_takeover_approval("local", approval_id)
            .unwrap()
            .unwrap()
            .status,
        TakeoverApprovalStatus::Pending
    );

    let approved = json_output(
        &[
            "goal",
            "continuity-decide",
            "--db",
            &db,
            "--json",
            approval_id,
            "--decision",
            "approve",
            "--decided-by",
            "operator",
            "--reason",
            "explicit",
        ],
        0,
    );
    assert_eq!(approved["status"], "approved");
    assert_eq!(approved["approval"]["decided_by"], "operator");
    assert_eq!(approved["approval"]["status"], "approved");
    let repeated = json_output(
        &[
            "goal",
            "continuity-queue",
            "--db",
            &db,
            "--json",
            "controlled",
            "--workdir",
            &workdir,
            "--sandbox",
            "write",
            "--timeout-seconds",
            "30",
        ],
        0,
    );
    assert_eq!(repeated["status"], "approved");
    assert_eq!(repeated["approval"]["approval_id"], approval_id);
}

#[test]
fn continuity_review_queue_requires_persisted_successful_takeover_evidence() {
    let directory = TempDir::new().unwrap();
    let db_path = directory.path().join("goals.sqlite3");
    let db = db_text(&db_path);
    let target = |role: &str| GoalExecutionTarget {
        provider: "native".into(),
        protocol: "anthropic_messages".into(),
        harness: "claude-code".into(),
        model: "test-model".into(),
        profile: None,
        role: role.into(),
    };
    let store = SqliteGoalStore::open(&db_path).unwrap();
    let mut goal = NewGoal::new(
        "Missing review evidence",
        Utc.timestamp_opt(1_000, 0).unwrap(),
    );
    goal.id = Some("missing-review-evidence".into());
    goal.continuity_policy = Some(GoalContinuityPolicy {
        mode: GoalContinuityMode::QuotaHandoff,
        primary: target("primary"),
        takeover: vec![target("takeover")],
        reviewer: Some(target("reviewer")),
        resume_primary_after_reset: true,
        require_review_before_resume: true,
        wait_for_review_checks_before_resume: false,
    });
    store.create("local", goal).unwrap();
    store
        .start(
            "local",
            "missing-review-evidence",
            Utc.timestamp_opt(1_001, 0).unwrap(),
        )
        .unwrap();
    apply_quota_handoff_events(
        &store,
        "local",
        &[GoalQuotaEvent {
            event_id: "quota-missing-review".into(),
            goal_id: Some("missing-review-evidence".into()),
            provider: "native".into(),
            harness: "claude-code".into(),
            model: "test-model".into(),
            session_id: None,
            observed_at: Utc.timestamp_opt(1_002, 0).unwrap(),
            estimated_reset_at: None,
        }],
        Utc.timestamp_opt(1_003, 0).unwrap(),
    )
    .unwrap();

    let workdir = db_text(directory.path());
    let queued = json_output(
        &[
            "goal",
            "continuity-queue",
            "--db",
            &db,
            "--json",
            "missing-review-evidence",
            "--workdir",
            &workdir,
        ],
        0,
    );
    let approval_id = queued["approval"]["approval_id"].as_str().unwrap();
    json_output(
        &[
            "goal",
            "continuity-decide",
            "--db",
            &db,
            "--json",
            approval_id,
            "--decision",
            "approve",
            "--decided-by",
            "operator",
        ],
        0,
    );
    store
        .consume_takeover_approval("local", approval_id, Utc::now())
        .unwrap();
    store
        .finish_takeover_approval(
            "local",
            approval_id,
            &TakeoverFinish {
                run_id: Some("completed-run".into()),
                run_status: TakeoverRunStatus::Success,
                detail: "completed".into(),
            },
            Utc::now(),
        )
        .unwrap();
    Connection::open(&db_path)
        .unwrap()
        .execute(
            "DELETE FROM goal_takeover_approvals WHERE approval_id = ?1",
            [approval_id],
        )
        .unwrap();

    let refused = json_output(
        &[
            "goal",
            "continuity-queue",
            "--db",
            &db,
            "--json",
            "missing-review-evidence",
            "--workdir",
            &workdir,
        ],
        2,
    );
    assert!(
        refused["error"]
            .as_str()
            .unwrap()
            .contains("review step has no exact successful takeover run evidence")
    );
}

#[cfg(unix)]
#[test]
fn continuity_execute_dispatches_once_and_settles_done() {
    let directory = TempDir::new().expect("temporary directory");
    let data_dir = TempDir::new().expect("data directory");
    let db_path = directory.path().join("goals.sqlite3");
    let db = db_text(&db_path);
    let (config, bin) = pursuit_fixture(&directory);
    let target = |role: &str| GoalExecutionTarget {
        provider: "native".into(),
        protocol: "anthropic_messages".into(),
        harness: "claude-code".into(),
        model: "test-model".into(),
        profile: Some("builder".into()),
        role: role.into(),
    };
    let store = SqliteGoalStore::open(&db_path).expect("goal store");
    let mut goal = NewGoal::new("Execute takeover", Utc.timestamp_opt(1_000, 0).unwrap());
    goal.id = Some("execute-controlled".into());
    goal.continuity_policy = Some(GoalContinuityPolicy {
        mode: GoalContinuityMode::QuotaHandoff,
        primary: target("primary"),
        takeover: vec![target("takeover")],
        reviewer: Some(target("reviewer")),
        resume_primary_after_reset: true,
        require_review_before_resume: true,
        wait_for_review_checks_before_resume: true,
    });
    store.create("local", goal).expect("create goal");
    store
        .start(
            "local",
            "execute-controlled",
            Utc.timestamp_opt(1_001, 0).unwrap(),
        )
        .expect("start goal");
    apply_quota_handoff_events(
        &store,
        "local",
        &[GoalQuotaEvent {
            event_id: "quota-execute".into(),
            goal_id: Some("execute-controlled".into()),
            provider: "native".into(),
            harness: "claude-code".into(),
            model: "test-model".into(),
            session_id: None,
            observed_at: Utc.timestamp_opt(1_002, 0).unwrap(),
            estimated_reset_at: None,
        }],
        Utc.timestamp_opt(1_003, 0).unwrap(),
    )
    .expect("apply quota event");

    let workdir = db_text(directory.path());
    let queued = json_output(
        &[
            "goal",
            "continuity-queue",
            "--db",
            &db,
            "--json",
            "execute-controlled",
            "--workdir",
            &workdir,
            "--sandbox",
            PURSUIT_SANDBOX,
            "--timeout-seconds",
            "30",
        ],
        0,
    );
    let approval_id = queued["approval"]["approval_id"].as_str().unwrap();
    json_output(
        &[
            "goal",
            "continuity-decide",
            "--db",
            &db,
            "--json",
            approval_id,
            "--decision",
            "approve",
            "--decided-by",
            "operator",
        ],
        0,
    );

    let output = vyane()
        .env_clear()
        .env("PATH", bin.path())
        .env("HOME", directory.path())
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "goal",
            "continuity-execute",
            "--db",
            &db,
            "--json",
            approval_id,
        ])
        .output()
        .expect("execute takeover");
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let executed: Value = serde_json::from_slice(&output.stdout).expect("execution JSON");
    assert_eq!(executed["status"], "success");
    assert_eq!(executed["approval"]["status"], "done");
    assert_eq!(executed["approval"]["run_status"], "success");
    assert!(executed["approval"]["run_id"].is_string());
    assert_eq!(
        store
            .get("local", "execute-controlled")
            .expect("read goal")
            .expect("goal exists")
            .continuity_state
            .expect("continuity state")
            .handoff_plan
            .steps[0]
            .status,
        GoalContinuityStepStatus::Done
    );

    let review_queued = json_output(
        &[
            "goal",
            "continuity-queue",
            "--db",
            &db,
            "--json",
            "execute-controlled",
            "--workdir",
            &workdir,
            "--sandbox",
            PURSUIT_SANDBOX,
            "--timeout-seconds",
            "30",
        ],
        0,
    );
    assert_eq!(review_queued["approval"]["step_id"], "review_takeover");
    assert_eq!(
        review_queued["approval"]["upstream_approval_id"],
        approval_id
    );
    assert!(review_queued["approval"]["upstream_run_id"].is_string());
    let review_approval_id = review_queued["approval"]["approval_id"]
        .as_str()
        .expect("review approval id");
    json_output(
        &[
            "goal",
            "continuity-decide",
            "--db",
            &db,
            "--json",
            review_approval_id,
            "--decision",
            "approve",
            "--decided-by",
            "operator",
        ],
        0,
    );
    let review_output = vyane()
        .env_clear()
        .env("PATH", bin.path())
        .env("HOME", directory.path())
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "goal",
            "continuity-execute",
            "--db",
            &db,
            "--json",
            review_approval_id,
        ])
        .output()
        .expect("execute review");
    assert_eq!(
        review_output.status.code(),
        Some(0),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&review_output.stdout),
        String::from_utf8_lossy(&review_output.stderr)
    );
    let reviewed: Value =
        serde_json::from_slice(&review_output.stdout).expect("review execution JSON");
    assert_eq!(reviewed["approval"]["status"], "done");
    let goal = store
        .get("local", "execute-controlled")
        .expect("read reviewed goal")
        .expect("reviewed goal exists");
    let state = goal.continuity_state.expect("reviewed continuity state");
    let review = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.id == "review_takeover")
        .expect("review step");
    let resume = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| step.id == "resume_primary")
        .expect("resume step");
    assert_eq!(review.status, GoalContinuityStepStatus::Done);
    assert_eq!(
        resume.status,
        GoalContinuityStepStatus::WaitingForQuotaResetAndReview
    );
    assert!(state.handoff_plan.next_ready_step.is_empty());

    let signal = json_output(
        &[
            "goal",
            "continuity-signal",
            "--db",
            &db,
            "--json",
            "execute-controlled",
            "quota-reset",
            "--quota-event-id",
            "quota-execute",
            "--provider",
            "native",
            "--harness",
            "claude-code",
            "--model",
            "test-model",
            "--source",
            "acceptance-reader",
        ],
        0,
    );
    assert_eq!(
        signal["result"]["state"]["handoff_plan"]["next_ready_step"],
        ""
    );
    let failed = json_output(
        &[
            "goal",
            "continuity-signal",
            "--db",
            &db,
            "--json",
            "execute-controlled",
            "review-checks-failed",
            "--quota-event-id",
            "quota-execute",
            "--provider",
            "native",
            "--harness",
            "claude-code",
            "--model",
            "test-model",
            "--source",
            "github-check-reader",
            "--repository",
            "example/vyane-rs",
            "--pull-request",
            "27",
            "--observation-id",
            "checks-failed-v1",
            "--observation-sequence",
            "1",
        ],
        0,
    );
    assert_eq!(
        failed["result"]["state"]["handoff_plan"]["next_ready_step"],
        "repair_failed_review"
    );
    assert_eq!(
        failed["result"]["signal"]["review_check"]["observation_id"],
        "checks-failed-v1"
    );
    let passed = json_output(
        &[
            "goal",
            "continuity-signal",
            "--db",
            &db,
            "--json",
            "execute-controlled",
            "review-checks-passed",
            "--quota-event-id",
            "quota-execute",
            "--provider",
            "native",
            "--harness",
            "claude-code",
            "--model",
            "test-model",
            "--source",
            "github-check-reader",
            "--repository",
            "example/vyane-rs",
            "--pull-request",
            "27",
            "--observation-id",
            "checks-passed-v1",
            "--observation-sequence",
            "2",
        ],
        0,
    );
    assert_eq!(
        passed["result"]["state"]["handoff_plan"]["next_ready_step"],
        "repair_failed_review"
    );
    assert_eq!(
        passed["result"]["signal"]["review_check"]["observation_id"],
        "checks-passed-v1"
    );
    let repair_queued = json_output(
        &[
            "goal",
            "continuity-queue",
            "--db",
            &db,
            "--json",
            "execute-controlled",
            "--workdir",
            &workdir,
            "--sandbox",
            pursuit_test_sandbox(),
            "--timeout-seconds",
            "30",
        ],
        0,
    );
    assert_eq!(repair_queued["approval"]["step_id"], "repair_failed_review");
    assert_eq!(
        repair_queued["approval"]["upstream_approval_id"],
        review_approval_id
    );
    let repair_approval_id = repair_queued["approval"]["approval_id"]
        .as_str()
        .expect("repair approval id");
    json_output(
        &[
            "goal",
            "continuity-decide",
            "--db",
            &db,
            "--json",
            repair_approval_id,
            "--decision",
            "approve",
            "--decided-by",
            "operator",
        ],
        0,
    );
    let repair_output = vyane()
        .env_clear()
        .env("PATH", bin.path())
        .env("HOME", directory.path())
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "goal",
            "continuity-execute",
            "--db",
            &db,
            "--json",
            repair_approval_id,
        ])
        .output()
        .expect("execute review repair");
    assert_eq!(
        repair_output.status.code(),
        Some(0),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&repair_output.stdout),
        String::from_utf8_lossy(&repair_output.stderr)
    );
    let resume_queued = json_output(
        &[
            "goal",
            "continuity-queue",
            "--db",
            &db,
            "--json",
            "execute-controlled",
            "--workdir",
            &workdir,
            "--sandbox",
            PURSUIT_SANDBOX,
            "--timeout-seconds",
            "30",
        ],
        0,
    );
    assert_eq!(resume_queued["approval"]["step_id"], "resume_primary");
    assert_eq!(
        resume_queued["approval"]["upstream_approval_id"],
        repair_approval_id
    );
    let resume_approval_id = resume_queued["approval"]["approval_id"]
        .as_str()
        .expect("resume approval id");
    json_output(
        &[
            "goal",
            "continuity-decide",
            "--db",
            &db,
            "--json",
            resume_approval_id,
            "--decision",
            "approve",
            "--decided-by",
            "operator",
        ],
        0,
    );
    let resume_output = vyane()
        .env_clear()
        .env("PATH", bin.path())
        .env("HOME", directory.path())
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "goal",
            "continuity-execute",
            "--db",
            &db,
            "--json",
            resume_approval_id,
        ])
        .output()
        .expect("execute primary resume");
    assert_eq!(
        resume_output.status.code(),
        Some(0),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&resume_output.stdout),
        String::from_utf8_lossy(&resume_output.stderr)
    );
    let resumed: Value =
        serde_json::from_slice(&resume_output.stdout).expect("resume execution JSON");
    assert_eq!(resumed["approval"]["status"], "done");
    assert_eq!(resumed["approval"]["run_status"], "success");
    #[cfg(target_os = "linux")]
    {
        let resume_prompt = fs::read_to_string(directory.path().join("last-prompt.txt"))
            .expect("read primary resume prompt");
        assert!(resume_prompt.contains("Primary resume evidence:"));
        assert!(resume_prompt.contains("review.approval_id:"));
        assert!(resume_prompt.contains("review.run_id:"));
        assert!(resume_prompt.contains("repair.approval_id:"));
        assert!(resume_prompt.contains("repair.run_id:"));
        assert!(resume_prompt.contains("takeover.approval_id:"));
        assert!(resume_prompt.contains("takeover.run_id:"));
        assert!(resume_prompt.contains("signal.quota_reset:"));
        assert!(resume_prompt.contains("signal.review_checks_failed:"));
        assert!(resume_prompt.contains("signal.review_checks_passed:"));
        assert!(resume_prompt.contains("review: example/vyane-rs#27"));
        assert!(resume_prompt.contains("Approved target provider: native"));
        assert!(resume_prompt.contains("Approved target harness: claude-code"));
    }
    let goal = store
        .get("local", "execute-controlled")
        .expect("read resumed goal")
        .expect("resumed goal exists");
    assert_eq!(goal.status, vyane_goal::GoalStatus::InProgress);
    assert_eq!(
        goal.continuity_state
            .unwrap()
            .handoff_plan
            .steps
            .iter()
            .find(|step| step.id == "resume_primary")
            .unwrap()
            .status,
        GoalContinuityStepStatus::Done
    );
}

#[test]
fn done_requires_satisfied_criteria_or_an_explicit_waiver() {
    let directory = TempDir::new().unwrap();
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let created = json_output(
        &[
            "goal",
            "create",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--id",
            "gated",
            "--title",
            "Gated goal",
            "--acceptance",
            "test-passes:workspace",
        ],
        0,
    );
    assert_eq!(created["status"], "success");
    json_output(
        &[
            "goal", "start", "--db", &db, "--owner", "owner-a", "--json", "gated",
        ],
        0,
    );

    let refused = json_output(
        &[
            "goal", "done", "--db", &db, "--owner", "owner-a", "--json", "gated",
        ],
        2,
    );
    assert_eq!(refused["status"], "error");
    assert!(
        refused["error"]
            .as_str()
            .unwrap()
            .contains("unsatisfied acceptance criteria")
    );

    let waived = json_output(
        &[
            "goal",
            "done",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "gated",
            "--waive",
            "manual override for test",
        ],
        0,
    );
    assert_eq!(waived["goal"]["status"], "completed");
    assert!(waived["goal"]["acceptance_criteria"][0]["satisfied_at"].is_null());
}

#[test]
fn claim_holds_a_lease_that_blocks_other_workers() {
    let directory = TempDir::new().unwrap();
    let db = db_text(&directory.path().join("goals.sqlite3"));
    create(&db, "owner-a", "leased", "Leased", "1");

    let claimed = json_output(
        &[
            "goal",
            "claim-next",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--worker",
            "worker-1",
            "--lease-seconds",
            "600",
        ],
        0,
    );
    assert_eq!(claimed["goal"]["id"], "leased");
    assert_eq!(claimed["goal"]["status"], "in_progress");
    assert_eq!(claimed["goal"]["claimed_by"], "worker-1");
    assert_eq!(claimed["goal"]["claim_generation"], 1);

    let refused = json_output(
        &[
            "goal", "claim", "--db", &db, "--owner", "owner-a", "--json", "leased", "--worker",
            "worker-2",
        ],
        2,
    );
    assert_eq!(refused["status"], "error");
    assert!(refused["error"].as_str().unwrap().contains("worker-1"));

    let renewed = json_output(
        &[
            "goal", "renew", "--db", &db, "--owner", "owner-a", "--json", "leased", "--worker",
            "worker-1",
        ],
        0,
    );
    assert_eq!(renewed["goal"]["claimed_by"], "worker-1");

    // The fence holds at the CLI boundary: a non-holder cannot complete.
    let fenced = json_output(
        &[
            "goal", "done", "--db", &db, "--owner", "owner-a", "--json", "leased", "--worker",
            "worker-2",
        ],
        2,
    );
    assert_eq!(fenced["status"], "error");
    assert!(fenced["error"].as_str().unwrap().contains("worker-1"));

    // The holder completes; terminal state releases the lease.
    let completed = json_output(
        &[
            "goal", "done", "--db", &db, "--owner", "owner-a", "--json", "leased", "--worker",
            "worker-1",
        ],
        0,
    );
    assert_eq!(completed["goal"]["status"], "completed");
    assert!(completed["goal"]["claimed_by"].is_null());
    assert!(completed["goal"]["claim_expires_at"].is_null());
    assert_eq!(completed["goal"]["claim_generation"], 1);
}

#[test]
fn next_and_list_use_priority_order_and_optional_auto_start() {
    let directory = TempDir::new().unwrap();
    let db = db_text(&directory.path().join("goals.sqlite3"));
    create(&db, "owner-a", "normal", "Normal", "2");
    create(&db, "owner-a", "urgent", "Urgent", "0");

    let next = json_output(
        &[
            "goal",
            "next",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--auto-start",
        ],
        0,
    );
    assert_eq!(next["goal"]["id"], "urgent");
    assert_eq!(next["goal"]["status"], "in_progress");

    let running = json_output(
        &[
            "goal",
            "list",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--state",
            "in_progress",
        ],
        0,
    );
    assert_eq!(running["count"], 1);
    assert_eq!(running["goals"][0]["id"], "urgent");

    let queued = json_output(
        &[
            "goal", "list", "--db", &db, "--owner", "owner-a", "--json", "--state", "queued",
        ],
        0,
    );
    assert_eq!(queued["count"], 1);
    assert_eq!(queued["goals"][0]["id"], "normal");
}

#[test]
fn owner_scope_hides_foreign_goals_and_allows_same_id() {
    let directory = TempDir::new().unwrap();
    let db = db_text(&directory.path().join("goals.sqlite3"));
    create(&db, "owner-a", "shared", "Owner A", "2");
    create(&db, "owner-b", "shared", "Owner B", "2");

    let owner_b = json_output(
        &[
            "goal", "get", "--db", &db, "--owner", "owner-b", "--json", "shared",
        ],
        0,
    );
    assert_eq!(owner_b["goal"]["title"], "Owner B");

    let foreign = json_output(
        &[
            "goal", "get", "--db", &db, "--owner", "foreign", "--json", "shared",
        ],
        2,
    );
    assert_eq!(foreign["status"], "error");
    assert!(foreign["error"].as_str().unwrap().contains("was not found"));
}

#[test]
fn malformed_input_and_illegal_transition_have_json_error_and_exit_two() {
    let directory = TempDir::new().unwrap();
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let malformed = json_output(
        &[
            "goal",
            "create",
            "--db",
            &db,
            "--json",
            "--title",
            "Bad acceptance",
            "--acceptance",
            "missing-separator",
        ],
        2,
    );
    assert_eq!(malformed["status"], "error");

    create(&db, "owner-a", "queued", "Queued", "2");
    let illegal = json_output(
        &[
            "goal", "done", "--db", &db, "--owner", "owner-a", "--json", "queued",
        ],
        2,
    );
    assert_eq!(illegal["status"], "error");
    assert!(
        illegal["error"]
            .as_str()
            .unwrap()
            .contains("while it is queued")
    );
}

#[test]
fn verify_runs_bounded_commands_and_persists_only_satisfied_criteria() {
    let directory = TempDir::new().unwrap();
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let created = json_output(
        &[
            "goal",
            "create",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--id",
            "verified-goal",
            "--title",
            "Verify acceptance",
            "--acceptance",
            "custom:cmd:sh -c exit 0",
            "--acceptance",
            "manual-confirm:operator approval",
        ],
        0,
    );
    assert_eq!(
        created["goal"]["acceptance_criteria"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    json_output(
        &[
            "goal",
            "start",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "verified-goal",
        ],
        0,
    );

    let verified = json_output(
        &[
            "goal",
            "verify",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--workdir",
            directory.path().to_str().unwrap(),
            "--timeout-seconds",
            "2",
            "verified-goal",
        ],
        3,
    );
    assert_eq!(verified["status"], "inconclusive");
    assert_eq!(verified["verification"]["all_satisfied"], false);
    assert_eq!(verified["artifact"]["goal_id"], "verified-goal");
    assert_eq!(
        verified["artifact"]["payload_sha256"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
    assert_eq!(
        verified["verification"]["results"][0]["status"],
        "satisfied"
    );
    assert_eq!(
        verified["verification"]["results"][1]["status"],
        "manual_required"
    );
    assert!(verified["goal"]["acceptance_criteria"][0]["satisfied_at"].is_string());
    assert!(verified["goal"]["acceptance_criteria"][1]["satisfied_at"].is_null());

    let verified_again = json_output(
        &[
            "goal",
            "verify",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--workdir",
            directory.path().to_str().unwrap(),
            "verified-goal",
        ],
        3,
    );
    assert_ne!(
        verified_again["artifact"]["verification_id"],
        verified["artifact"]["verification_id"]
    );

    let detail = json_output(
        &[
            "goal",
            "get",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "verified-goal",
        ],
        0,
    );
    assert_eq!(detail["verifications"].as_array().unwrap().len(), 2);
    assert_eq!(
        detail["verifications"][0]["verification_id"],
        verified["artifact"]["verification_id"]
    );
    assert_eq!(
        detail["verifications"][1]["verification_id"],
        verified_again["artifact"]["verification_id"]
    );
}

#[test]
fn verify_all_satisfied_returns_success_exit_code() {
    let directory = TempDir::new().unwrap();
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let output = vyane()
        .args([
            "goal",
            "create",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--id",
            "verified-success-2",
            "--title",
            "Verify success",
            "--acceptance",
            "custom:cmd:sh -c exit 0",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    json_output(
        &[
            "goal",
            "start",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "verified-success-2",
        ],
        0,
    );
    let verified = json_output(
        &[
            "goal",
            "verify",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--workdir",
            directory.path().to_str().unwrap(),
            "verified-success-2",
        ],
        0,
    );
    assert_eq!(verified["status"], "success");
    assert_eq!(verified["verification"]["all_satisfied"], true);
}

#[test]
fn verify_requires_an_in_progress_goal_before_recording_evidence() {
    let directory = TempDir::new().unwrap();
    let db = db_text(&directory.path().join("goals.sqlite3"));
    json_output(
        &[
            "goal",
            "create",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--id",
            "queued-verification",
            "--title",
            "Queued verification",
            "--acceptance",
            "manual-confirm:operator",
        ],
        0,
    );
    let output = json_output(
        &[
            "goal",
            "verify",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--workdir",
            directory.path().to_str().unwrap(),
            "queued-verification",
        ],
        2,
    );
    assert_eq!(output["status"], "error");
    assert_eq!(
        output["error"],
        "goal `queued-verification` must be in_progress before verification; current status is queued"
    );
}

#[test]
fn verify_requires_the_matching_cli_worker_for_an_active_lease() {
    let directory = TempDir::new().unwrap();
    let db = db_text(&directory.path().join("goals.sqlite3"));
    json_output(
        &[
            "goal",
            "create",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--id",
            "leased-verification",
            "--title",
            "Leased verification",
            "--acceptance",
            "manual-confirm:operator",
        ],
        0,
    );
    json_output(
        &[
            "goal",
            "claim",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "leased-verification",
            "--worker",
            "worker-1",
        ],
        0,
    );
    let refused = json_output(
        &[
            "goal",
            "verify",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--workdir",
            directory.path().to_str().unwrap(),
            "--worker",
            "worker-2",
            "leased-verification",
        ],
        2,
    );
    assert_eq!(
        refused["error"],
        "goal `leased-verification` has an active lease held by `worker-1`; pass the matching --worker"
    );
    let verified = json_output(
        &[
            "goal",
            "verify",
            "--db",
            &db,
            "--owner",
            "owner-a",
            "--json",
            "--workdir",
            directory.path().to_str().unwrap(),
            "--worker",
            "worker-1",
            "leased-verification",
        ],
        3,
    );
    assert_eq!(verified["artifact"]["worker_id"], "worker-1");
}

#[cfg(unix)]
#[test]
fn pursue_auto_dispatches_fresh_segment_reverifies_and_completes() {
    let directory = TempDir::new().expect("tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let (config, bin) = pursuit_fixture(&directory);
    let acceptance = pursuit_acceptance(&directory);
    json_output(
        &[
            "goal",
            "create",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "--id",
            "pursued-goal",
            "--title",
            "Pursued goal",
            "--acceptance",
            &acceptance,
        ],
        0,
    );
    json_output(
        &[
            "goal",
            "claim",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "pursued-goal",
            "--worker",
            "pursuer",
        ],
        0,
    );

    let output = vyane()
        .env_clear()
        .env("PATH", pursuit_path(&bin))
        .env("HOME", directory.path())
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "goal",
            "pursue",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "--target",
            "AUTO",
            "--sandbox",
            PURSUIT_SANDBOX,
            "--workdir",
            directory.path().to_str().expect("utf8 workdir"),
            "--max-segments",
            "2",
            "--max-failures",
            "1",
            "--segment-timeout-seconds",
            "5",
            "--verifier-timeout-seconds",
            "2",
            "--worker",
            "pursuer",
            "pursued-goal",
        ])
        .output()
        .expect("run pursuit");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("pursuit JSON");
    assert_eq!(value["status"], "success");
    assert_eq!(value["pursuit"]["status"], "achieved");
    assert_eq!(value["pursuit"]["segments_started"], 1);
    assert_eq!(value["goal"]["status"], "completed");
    assert!(value["goal"]["acceptance_criteria"][0]["satisfied_at"].is_string());

    let detail = json_output(
        &[
            "goal",
            "get",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "pursued-goal",
        ],
        0,
    );
    assert_eq!(
        detail["verifications"].as_array().expect("artifacts").len(),
        2
    );
    let checkpoint = detail["pursuit_checkpoint"]
        .as_object()
        .expect("checkpoint view");
    assert_eq!(checkpoint["status"], "achieved");
    assert!(!checkpoint.contains_key("workdir"));
    assert!(!checkpoint.contains_key("worker_id"));
    assert!(!checkpoint.contains_key("runtime"));
}

#[cfg(unix)]
#[test]
fn pursue_reports_runtime_error_as_paused_exit_three() {
    let directory = TempDir::new().expect("tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let (config, bin) = pursuit_fixture(&directory);
    let claude = bin.path().join("claude");
    fs::write(&claude, "#!/bin/sh\nexit 1\n").expect("write failing fake claude");
    fs::set_permissions(&claude, fs::Permissions::from_mode(0o755))
        .expect("chmod failing fake claude");
    json_output(
        &[
            "goal",
            "create",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "--id",
            "failed-pursuit",
            "--title",
            "Failed pursuit",
            "--acceptance",
            "custom:cmd:/usr/bin/false",
        ],
        0,
    );
    json_output(
        &[
            "goal",
            "claim",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "failed-pursuit",
            "--worker",
            "pursuer",
        ],
        0,
    );

    let output = vyane()
        .env_clear()
        .env("PATH", pursuit_path(&bin))
        .env("HOME", directory.path())
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "goal",
            "pursue",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "--target",
            "builder",
            "--sandbox",
            PURSUIT_SANDBOX,
            "--workdir",
            directory.path().to_str().expect("utf8 workdir"),
            "--max-failures",
            "1",
            "--worker",
            "pursuer",
            "failed-pursuit",
        ])
        .output()
        .expect("run failing pursuit");
    assert_eq!(
        output.status.code(),
        Some(3),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("pursuit JSON");
    assert_eq!(value["status"], "paused");
    assert_eq!(value["pursuit"]["status"], "paused");
    assert_eq!(value["pursuit"]["reason"], "pursuit max failures reached");
    assert_eq!(value["pursuit"]["consecutive_failures"], 1);
    assert_eq!(value["pursuit"]["segments_started"], 1);
    assert_eq!(value["pursuit"]["segments_completed"], 1);

    let detail = json_output(
        &[
            "goal",
            "get",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "failed-pursuit",
        ],
        0,
    );
    assert!(
        detail["events"]
            .as_array()
            .expect("events")
            .iter()
            .any(|event| {
                event["stage"] == "pursuit.segment.failed"
                    && event["detail"]
                        .as_str()
                        .is_some_and(|detail| detail.contains("Error"))
            }),
        "events: {}",
        detail["events"]
    );
}

#[cfg(unix)]
#[test]
fn pursue_missing_acceptance_returns_paused_exit_three() {
    let directory = TempDir::new().expect("tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let (config, bin) = pursuit_fixture(&directory);
    create(
        &db,
        "local",
        "missing-acceptance",
        "Missing acceptance",
        "2",
    );
    json_output(
        &[
            "goal",
            "claim",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "missing-acceptance",
            "--worker",
            "pursuer",
        ],
        0,
    );

    let output = vyane()
        .env_clear()
        .env("PATH", pursuit_path(&bin))
        .env("HOME", directory.path())
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "goal",
            "pursue",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "--target",
            "builder",
            "--sandbox",
            PURSUIT_SANDBOX,
            "--workdir",
            directory.path().to_str().expect("utf8 workdir"),
            "--worker",
            "pursuer",
            "missing-acceptance",
        ])
        .output()
        .expect("run pursuit without acceptance");
    assert_eq!(output.status.code(), Some(3));
    let value: Value = serde_json::from_slice(&output.stdout).expect("pursuit JSON");
    assert_eq!(value["status"], "paused");
    assert_eq!(value["pursuit"]["reason"], "acceptance criteria required");
}

#[cfg(unix)]
#[test]
fn pursue_external_pause_returns_stopped_exit_four() {
    let directory = TempDir::new().expect("tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let (config, bin) = pursuit_fixture(&directory);
    let claude = bin.path().join("claude");
    fs::write(
        &claude,
        r#"#!/bin/sh
: > "$PWD/segment-started"
/bin/sleep 1
printf '%s\n' '{"result":"segment complete","session_id":"fresh-segment"}'
"#,
    )
    .expect("write blocking fake claude");
    fs::set_permissions(&claude, fs::Permissions::from_mode(0o755))
        .expect("chmod blocking fake claude");
    json_output(
        &[
            "goal",
            "create",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "--id",
            "stopped-pursuit",
            "--title",
            "Stopped pursuit",
            "--acceptance",
            "custom:cmd:/usr/bin/false",
        ],
        0,
    );
    json_output(
        &[
            "goal",
            "claim",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "stopped-pursuit",
            "--worker",
            "pursuer",
        ],
        0,
    );

    let program = vyane().get_program().to_owned();
    let child = std::process::Command::new(program)
        .env_clear()
        .env("PATH", pursuit_path(&bin))
        .env("HOME", directory.path())
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "goal",
            "pursue",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "--target",
            "builder",
            "--sandbox",
            PURSUIT_SANDBOX,
            "--workdir",
            directory.path().to_str().expect("utf8 workdir"),
            "--worker",
            "pursuer",
            "stopped-pursuit",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pursuit");
    let marker = directory.path().join("segment-started");
    for _ in 0..200 {
        if marker.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(marker.exists(), "runtime segment did not start");
    json_output(
        &[
            "goal",
            "pause",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "stopped-pursuit",
            "--worker",
            "pursuer",
            "--reason",
            "external pause",
        ],
        0,
    );
    let output = child.wait_with_output().expect("wait for pursuit");
    assert_eq!(
        output.status.code(),
        Some(4),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("pursuit JSON");
    assert_eq!(value["status"], "stopped");
    assert_eq!(value["pursuit"]["status"], "stopped");
    assert_eq!(value["pursuit"]["reason"], "goal status is paused");
}

#[test]
fn pursue_rejects_non_local_owner_before_goal_or_runtime_access() {
    let directory = TempDir::new().expect("tempdir");
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let value = json_output(
        &[
            "goal",
            "pursue",
            "--db",
            &db,
            "--owner",
            "other-owner",
            "--json",
            "--target",
            "builder",
            "--worker",
            "pursuer",
            "unread-goal",
        ],
        2,
    );
    assert_eq!(value["status"], "error");
    assert_eq!(
        value["error"],
        "goal pursue currently requires the local single-user owner scope"
    );
    assert!(!Path::new(&db).exists());
}

#[cfg(unix)]
#[test]
fn pursue_sigint_cancels_the_active_segment_and_pauses_immediately() {
    let directory = TempDir::new().expect("tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let (config, bin) = pursuit_fixture(&directory);
    let claude = bin.path().join("claude");
    fs::write(
        &claude,
        r#"#!/bin/sh
: > "$PWD/sigint-segment-started"
/bin/sleep 5
printf '%s\n' '{"result":"segment complete","session_id":"fresh-segment"}'
"#,
    )
    .expect("write interruptible fake claude");
    fs::set_permissions(&claude, fs::Permissions::from_mode(0o755))
        .expect("chmod interruptible fake claude");
    json_output(
        &[
            "goal",
            "create",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "--id",
            "cancelled-pursuit",
            "--title",
            "Cancelled pursuit",
            "--acceptance",
            "custom:cmd:/usr/bin/false",
        ],
        0,
    );
    json_output(
        &[
            "goal",
            "claim",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "cancelled-pursuit",
            "--worker",
            "pursuer",
        ],
        0,
    );

    let program = vyane().get_program().to_owned();
    let child = std::process::Command::new(program)
        .env_clear()
        .env("PATH", pursuit_path(&bin))
        .env("HOME", directory.path())
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "goal",
            "pursue",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "--target",
            "builder",
            "--sandbox",
            PURSUIT_SANDBOX,
            "--workdir",
            directory.path().to_str().expect("utf8 workdir"),
            "--overall-timeout-seconds",
            "2",
            "--segment-timeout-seconds",
            "5",
            "--max-failures",
            "3",
            "--worker",
            "pursuer",
            "cancelled-pursuit",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pursuit");
    let marker = directory.path().join("sigint-segment-started");
    for _ in 0..500 {
        if marker.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(marker.exists(), "runtime segment did not start");
    let signal = std::process::Command::new("/bin/kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("send SIGINT");
    assert!(signal.success());
    let output = child.wait_with_output().expect("wait for pursuit");
    assert_eq!(
        output.status.code(),
        Some(3),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("pursuit JSON");
    assert_eq!(value["status"], "paused");
    assert_eq!(value["pursuit"]["reason"], "pursuit cancelled");
    assert_eq!(value["pursuit"]["segments_started"], 1);
    let history = vyane()
        .env("VYANE_DATA_DIR", data_dir.path())
        .args(["history", "--json"])
        .output()
        .expect("read cancellation ledger");
    assert!(history.status.success());
    let records: Value = serde_json::from_slice(&history.stdout).expect("history JSON");
    assert_eq!(records.as_array().unwrap().len(), 1);
    assert_eq!(records[0]["status"], "cancelled");
}

#[cfg(unix)]
#[test]
fn pursue_sigint_during_verification_kills_process_group_and_pauses() {
    let directory = TempDir::new().expect("tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let (config, bin) = pursuit_fixture(&directory);
    let verifier = directory.path().join("interruptible-verifier");
    let descendant = directory.path().join("verifier-descendant");
    let escaped = directory.path().join("escaped-verifier-child");
    fs::write(
        &verifier,
        r#"#!/bin/sh
"$2" "$1" &
: > "$PWD/verifier-started"
/bin/sleep 5
"#,
    )
    .expect("write interruptible verifier");
    fs::set_permissions(&verifier, fs::Permissions::from_mode(0o755))
        .expect("chmod interruptible verifier");
    fs::write(&descendant, "#!/bin/sh\n/bin/sleep 2\n: > \"$1\"\n")
        .expect("write verifier descendant");
    fs::set_permissions(&descendant, fs::Permissions::from_mode(0o755))
        .expect("chmod verifier descendant");
    json_output(
        &[
            "goal",
            "create",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "--id",
            "cancelled-verification",
            "--title",
            "Cancelled verification",
            "--acceptance",
            &format!(
                "custom:cmd:{} {} {}",
                verifier.display(),
                escaped.display(),
                descendant.display()
            ),
        ],
        0,
    );
    json_output(
        &[
            "goal",
            "claim",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "cancelled-verification",
            "--worker",
            "pursuer",
        ],
        0,
    );

    let program = vyane().get_program().to_owned();
    let child = std::process::Command::new(program)
        .env_clear()
        .env("PATH", pursuit_path(&bin))
        .env("HOME", directory.path())
        .env("VYANE_DATA_DIR", data_dir.path())
        .arg("--config")
        .arg(&config)
        .args([
            "goal",
            "pursue",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "--target",
            "builder",
            "--sandbox",
            PURSUIT_SANDBOX,
            "--workdir",
            directory.path().to_str().expect("utf8 workdir"),
            "--overall-timeout-seconds",
            "10",
            "--verifier-timeout-seconds",
            "10",
            "--worker",
            "pursuer",
            "cancelled-verification",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pursuit");
    let marker = directory.path().join("verifier-started");
    for _ in 0..500 {
        if marker.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(marker.exists(), "acceptance verifier did not start");
    let interrupted_at = std::time::Instant::now();
    let signal = std::process::Command::new("/bin/kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("send SIGINT");
    assert!(signal.success());
    let output = child.wait_with_output().expect("wait for pursuit");
    assert!(
        interrupted_at.elapsed() < Duration::from_secs(2),
        "verification cancellation was not prompt"
    );
    assert_eq!(
        output.status.code(),
        Some(3),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("pursuit JSON");
    assert_eq!(value["status"], "paused");
    assert_eq!(value["pursuit"]["reason"], "pursuit cancelled");
    assert_eq!(value["pursuit"]["segments_started"], 0);
    assert_eq!(value["goal"]["status"], "paused");
    let detail = json_output(
        &[
            "goal",
            "get",
            "--db",
            &db,
            "--owner",
            "local",
            "--json",
            "cancelled-verification",
        ],
        0,
    );
    assert!(detail["verifications"].as_array().unwrap().is_empty());
    thread::sleep(Duration::from_millis(2_200));
    assert!(
        !escaped.exists(),
        "cancelled verifier descendants must not escape their process group"
    );
}

#[test]
fn default_database_uses_vyane_data_dir_and_help_marks_scope_as_unauthenticated() {
    let directory = TempDir::new().unwrap();
    let created = vyane()
        .env("VYANE_DATA_DIR", directory.path())
        .args(["goal", "create", "--json", "--title", "Default storage"])
        .output()
        .unwrap();
    assert!(created.status.success());
    let value: Value = serde_json::from_slice(&created.stdout).unwrap();
    assert_eq!(
        value["db"],
        directory
            .path()
            .join("goals.sqlite3")
            .to_string_lossy()
            .as_ref()
    );

    let help = vyane().args(["goal", "create", "--help"]).output().unwrap();
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    assert!(help.contains("not authenticated authority"));
    assert!(help.contains("KIND:TARGET"));
}
