//! `vyane quota-snapshot` reports freshness and does not write the file.

#![allow(clippy::unwrap_used)]

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use assert_cmd::Command;
use serde_json::{Value, json};

fn vyane() -> Command {
    Command::cargo_bin("vyane").expect("vyane binary")
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn names(directory: &Path) -> Vec<String> {
    let mut found = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    found.sort();
    found
}

fn write_json(path: &Path, value: &Value) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
}

fn fresh_body(now: i64, marker: &str) -> Value {
    json!({
        "schema_version": 1,
        "generated_at_ms": now,
        "probe_status": "ok",
        "marker": marker,
        "providers": []
    })
}

fn isolate(command: &mut Command, home: &Path) {
    command
        .env("HOME", home)
        .env("USERPROFILE", home.join("userprofile"))
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_DATA_HOME", home.join("data"))
        .env("VYANE_DATA_DIR", home.join("vyane-data"))
        .env_remove("XDG_STATE_HOME")
        .env_remove("RUST_LOG")
        .env_remove("VYANE_MANAGED_PERMISSION_CONFIG")
        .env_remove("VYANE_MANAGED_NATIVE_CONFIG")
        .current_dir(home);
}

fn run_snapshot(home: &Path, args: &[&str], state_home: Option<&Path>) -> std::process::Output {
    let mut command = vyane();
    isolate(&mut command, home);
    if let Some(state_home) = state_home {
        command.env("XDG_STATE_HOME", state_home);
    }
    command.args(args);
    command.output().unwrap()
}

fn document(output: &std::process::Output) -> Value {
    assert!(
        output.status.success(),
        "status {:?}\nstderr {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        output.stdout.iter().filter(|byte| **byte == b'\n').count(),
        1
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn fresh_snapshot_is_printed_and_the_command_does_not_write() {
    let directory = common::private_tempdir();
    let home = directory.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let snap_dir = directory.path().join("snap");
    fs::create_dir_all(&snap_dir).unwrap();
    let path = snap_dir.join("quota-snapshot.json");
    let body = fresh_body(now_ms(), "kept-note");
    write_json(&path, &body);
    let bytes = fs::read(&path).unwrap();
    let modified = fs::metadata(&path).unwrap().modified().unwrap();
    let listing = names(&snap_dir);

    let marker = directory.path().join("omp-was-called");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let bin = directory.path().join("bin");
        fs::create_dir_all(&bin).unwrap();
        let script = bin.join("omp");
        fs::write(&script, format!("#!/bin/sh\ntouch {}\n", marker.display())).unwrap();
        let mut permissions = fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script, permissions).unwrap();
    }
    let config = directory.path().join("broken.toml");
    fs::write(&config, "this is not toml [\n").unwrap();

    let mut command = vyane();
    isolate(&mut command, &home);
    let path_arg = path.to_str().unwrap();
    #[cfg(unix)]
    {
        let bin = directory.path().join("bin");
        let mut paths = vec![bin];
        if let Some(current) = std::env::var_os("PATH") {
            paths.extend(std::env::split_paths(&current));
        }
        command.env("PATH", std::env::join_paths(paths).unwrap());
    }
    command
        .arg("--config")
        .arg(&config)
        .arg("quota-snapshot")
        .arg(path_arg);
    let output = command.output().unwrap();
    let view = document(&output);

    assert_eq!(view["schema"], "quota-snapshot-read.v1");
    assert_eq!(view["state"], "fresh");
    assert_eq!(view["reasons"], json!([]));
    assert_eq!(view["snapshot"], body);
    assert_eq!(view["stale_after_ms"], 2_700_000);
    assert!(view["generated_at_ms"].as_i64().is_some());
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), modified);
    assert_eq!(names(&snap_dir), listing);
    assert!(!marker.exists());
    assert!(!snap_dir.join("quota-snapshot.json.lock").exists());
}

#[test]
fn missing_bad_schema_and_stale_reasons_are_reported() {
    let directory = common::private_tempdir();
    let home = directory.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let now = now_ms();
    let missing = directory
        .path()
        .join("no-such-dir")
        .join("quota-snapshot.json");
    let mismatch = directory.path().join("mismatch.json");
    let old = directory.path().join("old.json");
    let failed = directory.path().join("failed.json");
    let invalidated = directory.path().join("invalidated.json");
    write_json(&mismatch, &json!({"schema_version": 2, "providers": []}));
    write_json(
        &old,
        &json!({
            "schema_version": 1,
            "generated_at_ms": now - 3 * 60 * 60 * 1000,
            "probe_status": "ok",
            "providers": []
        }),
    );
    write_json(
        &failed,
        &json!({
            "schema_version": 1,
            "generated_at_ms": now,
            "probe_status": "error",
            "providers": []
        }),
    );
    let generated = now - 60_000;
    write_json(
        &invalidated,
        &json!({
            "schema_version": 1,
            "generated_at_ms": generated,
            "invalidated_at_ms": generated,
            "probe_status": "ok",
            "providers": []
        }),
    );

    let missing_view = document(&run_snapshot(
        &home,
        &["quota-snapshot", missing.to_str().unwrap()],
        None,
    ));
    assert_eq!(missing_view["state"], "missing");
    assert_eq!(missing_view["reasons"], json!(["file_missing"]));
    assert!(missing_view["snapshot"].is_null());
    assert!(!directory.path().join("no-such-dir").exists());

    let mismatch_bytes = fs::read(&mismatch).unwrap();
    let mismatch_view = document(&run_snapshot(
        &home,
        &["quota-snapshot", mismatch.to_str().unwrap()],
        None,
    ));
    assert_eq!(mismatch_view["state"], "missing");
    assert_eq!(mismatch_view["reasons"], json!(["schema_mismatch"]));
    assert!(mismatch_view["snapshot"].is_null());
    assert_eq!(fs::read(&mismatch).unwrap(), mismatch_bytes);

    let old_view = document(&run_snapshot(
        &home,
        &["quota-snapshot", old.to_str().unwrap()],
        None,
    ));
    assert_eq!(old_view["state"], "stale");
    assert_eq!(old_view["reasons"], json!(["too_old"]));
    assert!(old_view["snapshot"].is_object());

    let failed_view = document(&run_snapshot(
        &home,
        &["quota-snapshot", failed.to_str().unwrap()],
        None,
    ));
    assert_eq!(failed_view["state"], "stale");
    assert_eq!(failed_view["reasons"], json!(["probe_failed"]));

    let invalidated_view = document(&run_snapshot(
        &home,
        &["quota-snapshot", invalidated.to_str().unwrap()],
        None,
    ));
    assert_eq!(invalidated_view["state"], "stale");
    assert_eq!(invalidated_view["reasons"], json!(["invalidated"]));
}

#[test]
fn default_path_follows_state_home_then_home_and_explicit_path_wins() {
    let directory = common::private_tempdir();
    let home = directory.path().join("home");
    let userprofile = home.join("userprofile");
    let state = directory.path().join("state");
    let now = now_ms();
    let state_file = state.join("aios/quota-snapshot.json");
    let home_file = home.join(".local/state/aios/quota-snapshot.json");
    let profile_file = userprofile.join(".local/state/aios/quota-snapshot.json");
    let explicit = directory.path().join("explicit.json");
    write_json(&state_file, &fresh_body(now, "state"));
    write_json(&home_file, &fresh_body(now, "home"));
    write_json(&profile_file, &fresh_body(now, "userprofile"));
    write_json(&explicit, &fresh_body(now, "explicit"));

    let from_state = document(&run_snapshot(&home, &["quota-snapshot"], Some(&state)));
    assert_eq!(from_state["state"], "fresh");
    assert_eq!(from_state["snapshot"]["marker"], "state");

    fs::remove_file(&state_file).unwrap();
    let missing_state = document(&run_snapshot(&home, &["quota-snapshot"], Some(&state)));
    assert_eq!(missing_state["state"], "missing");
    assert_eq!(missing_state["reasons"], json!(["file_missing"]));
    assert!(!state_file.exists());

    let from_home = document(&run_snapshot(&home, &["quota-snapshot"], None));
    let mut empty_state = vyane();
    isolate(&mut empty_state, &home);
    empty_state.env("XDG_STATE_HOME", "");
    empty_state.arg("quota-snapshot");
    let from_empty = document(&empty_state.output().unwrap());
    // Unix reads `HOME`. Windows reads `USERPROFILE` through the same helper
    // automatic routing uses, then joins `.local/state`.
    let home_marker = if cfg!(windows) { "userprofile" } else { "home" };
    assert_eq!(from_home["snapshot"]["marker"], home_marker);
    assert_eq!(from_empty["snapshot"]["marker"], home_marker);

    let overridden = document(&run_snapshot(
        &home,
        &["quota-snapshot", explicit.to_str().unwrap()],
        Some(&state),
    ));
    assert_eq!(overridden["snapshot"]["marker"], "explicit");
}

fn write_profiles(directory: &Path) -> PathBuf {
    let config = directory.join("config.toml");
    fs::write(
        &config,
        r#"
[providers.busy-provider]
base_url = "https://busy.example.invalid"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "busy-model"

[providers.free-provider]
base_url = "https://free.example.invalid"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "free-model"

[profiles.busy]
provider = "busy-provider"
protocol = "openai_chat"
harness = "none"
model = "busy-model"
tier = "economy"

[profiles.free]
provider = "free-provider"
protocol = "openai_chat"
harness = "none"
model = "free-model"
tier = "economy"
"#,
    )
    .unwrap();
    config
}

fn route_selection(home: &Path, config: &Path, snapshot: &Path) -> String {
    let mut command = vyane();
    isolate(&mut command, home);
    command
        .arg("--config")
        .arg(config)
        .args(["route", "ping", "--json", "--quota-snapshot"])
        .arg(snapshot);
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "route failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let decision: Value = serde_json::from_slice(&output.stdout).unwrap();
    decision["selection_key"].as_str().unwrap().to_string()
}

#[test]
fn stale_view_leaves_route_fail_open_and_a_fresh_file_still_filters() {
    let directory = common::private_tempdir();
    let home = directory.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let config = write_profiles(directory.path());
    let snap_dir = directory.path().join("snap");
    fs::create_dir_all(&snap_dir).unwrap();
    let path = snap_dir.join("quota-snapshot.json");
    let now = now_ms();
    let week_ms = 7 * 24 * 3_600 * 1_000;
    let mut body = json!({
        "schema_version": 1,
        "generated_at_ms": now - 3 * 60 * 60 * 1000,
        "probe_status": "ok",
        "providers": [
            {
                "provider": "busy-provider",
                "kind": "subscription",
                "windows": [{
                    "id": "busy-provider:7d",
                    "duration_ms": week_ms,
                    "used_pct": 96.0,
                    "pace_ratio": 2.1
                }]
            },
            {
                "provider": "free-provider",
                "kind": "subscription",
                "windows": [{
                    "id": "free-provider:7d",
                    "duration_ms": week_ms,
                    "used_pct": 40.0,
                    "pace_ratio": 1.0
                }]
            }
        ]
    });
    write_json(&path, &body);
    let stale_bytes = fs::read(&path).unwrap();
    let stale_modified = fs::metadata(&path).unwrap().modified().unwrap();

    let stale_view = document(&run_snapshot(
        &home,
        &["quota-snapshot", path.to_str().unwrap()],
        None,
    ));
    assert_eq!(stale_view["state"], "stale");
    assert_eq!(stale_view["reasons"], json!(["too_old"]));
    assert_eq!(route_selection(&home, &config, &path), "busy");
    assert_eq!(fs::read(&path).unwrap(), stale_bytes);
    assert_eq!(
        fs::metadata(&path).unwrap().modified().unwrap(),
        stale_modified
    );
    assert_eq!(names(&snap_dir), vec!["quota-snapshot.json".to_string()]);

    body["generated_at_ms"] = json!(now);
    write_json(&path, &body);
    let fresh_bytes = fs::read(&path).unwrap();
    let fresh_view = document(&run_snapshot(
        &home,
        &["quota-snapshot", path.to_str().unwrap()],
        None,
    ));
    assert_eq!(fresh_view["state"], "fresh");
    assert_eq!(route_selection(&home, &config, &path), "free");
    assert_eq!(fs::read(&path).unwrap(), fresh_bytes);
}
