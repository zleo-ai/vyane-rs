#![allow(clippy::unwrap_used)]

use std::path::Path;

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
