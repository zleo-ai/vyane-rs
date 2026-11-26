//! Acceptance for the local presence table.
//!
//! Two temporary sessions must see each other. A held lock must return inside
//! a predetermined bound. A corrupt table must fail and must not report that
//! nobody is online.

#![allow(clippy::unwrap_used)]

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::process::Output;
use std::time::{Duration, Instant};

use assert_cmd::Command;
use fs4::fs_std::FileExt as _;
use serde_json::Value;
use vyane_harness::presence::presence_lock_path;

fn vyane() -> Command {
    Command::cargo_bin("vyane").expect("vyane binary")
}

fn run(args: &[&str]) -> Output {
    vyane()
        .args(args)
        .timeout(Duration::from_secs(5))
        .output()
        .expect("run vyane")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        text(&output.stdout),
        text(&output.stderr)
    );
}

fn json_ok(args: &[&str]) -> Value {
    let output = run(args);
    assert_success(&output);
    serde_json::from_slice(&output.stdout).expect("json stdout")
}

fn path_arg(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[test]
fn two_temporary_sessions_see_each_other() {
    let dir = tempfile::tempdir().unwrap();
    let table = dir.path().join("presence.json");
    let table = path_arg(&table);
    let first = json_ok(&[
        "presence",
        "checkin",
        "--path",
        &table,
        "--json",
        "--now",
        "1000",
        "--session",
        "alpha",
        "--label",
        "Alpha",
        "--card",
        "opaque-alpha",
        "--status",
        "editing",
    ]);
    assert_eq!(first["status"], "success");
    assert_eq!(first["written"], true);
    assert_eq!(first["agent"]["card"], "opaque-alpha");

    let second = json_ok(&[
        "presence",
        "checkin",
        "--path",
        &table,
        "--json",
        "--now",
        "1001",
        "--session",
        "beta",
        "--label",
        "Beta",
        "--card",
        "opaque-beta",
        "--status",
        "reading",
    ]);
    assert_eq!(second["online_peers"], 1);

    let alpha_sees = json_ok(&[
        "presence",
        "list",
        "--path",
        &table,
        "--json",
        "--now",
        "1002",
        "--session",
        "alpha",
    ]);
    assert_eq!(alpha_sees["count"], 1);
    assert_eq!(alpha_sees["agents"][0]["session_id"], "beta");
    assert_eq!(alpha_sees["agents"][0]["card"], "opaque-beta");

    let beta_sees = json_ok(&[
        "presence",
        "list",
        "--path",
        &table,
        "--json",
        "--now",
        "1002",
        "--session",
        "beta",
    ]);
    assert_eq!(beta_sees["agents"][0]["session_id"], "alpha");
    assert_eq!(beta_sees["agents"][0]["status"], "editing");

    let heartbeat = json_ok(&[
        "presence",
        "heartbeat",
        "--path",
        &table,
        "--json",
        "--now",
        "1003",
        "--session",
        "alpha",
    ]);
    assert_eq!(heartbeat["written"], true);
    assert_eq!(heartbeat["agent"]["card"], "opaque-alpha");
    assert_eq!(heartbeat["agent"]["status"], "editing");

    let left = json_ok(&[
        "presence",
        "leave",
        "--path",
        &table,
        "--json",
        "--now",
        "1004",
        "--session",
        "alpha",
    ]);
    assert_eq!(left["removed"], true);
    let after_leave = json_ok(&[
        "presence",
        "list",
        "--path",
        &table,
        "--json",
        "--now",
        "1005",
        "--session",
        "beta",
    ]);
    assert_eq!(after_leave["count"], 0);
    let human_empty = run(&[
        "presence",
        "list",
        "--path",
        &table,
        "--now",
        "1005",
        "--session",
        "beta",
    ]);
    assert_success(&human_empty);
    assert_eq!(text(&human_empty.stdout), "no online peers\n");
}

#[test]
fn corrupt_table_fails_instead_of_reporting_nobody_online() {
    let dir = tempfile::tempdir().unwrap();
    let table = dir.path().join("presence.json");
    let original = b"{not json";
    fs::write(&table, original).unwrap();
    let table_arg = path_arg(&table);
    let output = run(&["presence", "list", "--path", &table_arg, "--json"]);
    assert!(!output.status.success());
    assert_eq!(output.status.code(), Some(1));
    let stdout = text(&output.stdout);
    let stderr = text(&output.stderr);
    for body in [&stdout, &stderr] {
        assert!(!body.contains("no online peers"), "{body}");
        assert!(!body.contains("没有人在线"), "{body}");
    }
    assert!(stderr.contains("store_corrupt"), "{stderr}");
    let parsed: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(parsed["status"], "error");
    assert_eq!(parsed["error"]["code"], "store_corrupt");
    assert_eq!(fs::read(&table).unwrap(), original);

    let human = run(&["presence", "list", "--path", &table_arg]);
    assert_eq!(human.status.code(), Some(1));
    let human_out = text(&human.stdout);
    let human_err = text(&human.stderr);
    assert!(human_out.is_empty(), "{human_out}");
    assert!(human_err.contains("store_corrupt"), "{human_err}");
    assert!(!human_err.contains("no online peers"));
    assert!(!human_err.contains("没有人在线"));
    assert_eq!(fs::read(&table).unwrap(), original);

    let checkin = run(&[
        "presence",
        "checkin",
        "--path",
        &table_arg,
        "--session",
        "alpha",
        "--card",
        "should-not-land",
    ]);
    assert_eq!(checkin.status.code(), Some(1));
    let checkin_out = text(&checkin.stdout);
    let checkin_err = text(&checkin.stderr);
    assert!(!checkin_out.contains("checked in"), "{checkin_out}");
    assert!(!checkin_out.contains("no online peers"), "{checkin_out}");
    assert!(checkin_err.contains("store_corrupt"), "{checkin_err}");
    assert_eq!(fs::read(&table).unwrap(), original);
}

#[test]
fn lock_timeout_returns_within_one_second() {
    let dir = tempfile::tempdir().unwrap();
    let table = dir.path().join("presence.json");
    fs::write(&table, b"{\"agents\":{}}\n").unwrap();
    let lock_path = presence_lock_path(&table).unwrap();
    let mut held = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .unwrap();
    held.write_all(b"held\n").unwrap();
    assert!(held.try_lock_exclusive().unwrap());

    let table_arg = path_arg(&table);
    let started = Instant::now();
    let output = run(&["presence", "list", "--path", &table_arg]);
    let elapsed = started.elapsed();
    drop(held);

    assert!(
        elapsed < Duration::from_secs(1),
        "presence list waited {elapsed:?} for a held lock"
    );
    assert_eq!(output.status.code(), Some(1));
    let stdout = text(&output.stdout);
    let stderr = text(&output.stderr);
    assert!(stderr.contains("store_busy"), "{stderr}");
    assert!(!stdout.contains("no online peers"));
    assert!(!stderr.contains("no online peers"));
    assert!(!stdout.contains("没有人在线"));
    assert!(!stderr.contains("没有人在线"));
}
