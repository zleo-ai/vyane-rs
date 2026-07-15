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
use serde_json::Value;
use tempfile::TempDir;

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
    fs::write(
        &claude,
        r#"#!/bin/sh
: > "$PWD/done.txt"
printf '%s\n' '{"result":"segment complete","session_id":"fresh-segment"}'
"#,
    )
    .expect("write fake claude");
    fs::set_permissions(&claude, fs::Permissions::from_mode(0o755)).expect("chmod fake claude");
    (config, bin)
}

#[cfg(unix)]
const fn pursuit_test_sandbox() -> &'static str {
    if cfg!(target_os = "linux") {
        "write"
    } else {
        // Mutating harness admission intentionally fails closed outside Linux;
        // these fixtures test the pursuit loop with a non-enforcing fake CLI.
        "read-only"
    }
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
fn pursue_dispatches_fresh_segment_reverifies_and_completes() {
    let directory = TempDir::new().expect("tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let db = db_text(&directory.path().join("goals.sqlite3"));
    let (config, bin) = pursuit_fixture(&directory);
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
            "custom:cmd:/bin/test -f done.txt",
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
        .env("PATH", bin.path())
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
            pursuit_test_sandbox(),
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
        .env("PATH", bin.path())
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
            pursuit_test_sandbox(),
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
        .env("PATH", bin.path())
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
            pursuit_test_sandbox(),
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
        .env("PATH", bin.path())
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
            pursuit_test_sandbox(),
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
        .env("PATH", bin.path())
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
            pursuit_test_sandbox(),
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
