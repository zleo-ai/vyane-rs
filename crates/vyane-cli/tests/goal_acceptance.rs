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
    assert_eq!(detail["goal"]["revision"], 5);
    assert_eq!(detail["goal"]["completion_summary"], "verified");
    assert_eq!(detail["events"].as_array().unwrap().len(), 6);
    assert_eq!(detail["events"][2]["stage"], "implementation");
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
