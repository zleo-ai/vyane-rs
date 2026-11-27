//! `vyane liveness sandbox-orphans` reads a fixture proc tree and does not write it.

#![allow(clippy::unwrap_used)]

mod common;

use std::fs;
use std::path::Path;
use std::process::Output;

use assert_cmd::Command;
use serde_json::Value;

fn vyane() -> Command {
    Command::cargo_bin("vyane").expect("vyane binary")
}

fn run(home: &Path, args: &[&str]) -> Output {
    let mut command = vyane();
    command
        .env_clear()
        .env("HOME", home)
        .env("USERPROFILE", home.join("userprofile"))
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_DATA_HOME", home.join("data"))
        .env("VYANE_DATA_DIR", home.join("vyane-data"))
        .env("NO_COLOR", "1")
        .env("TERM", "dumb")
        .env("COLUMNS", "80")
        .env("LC_ALL", "C")
        .current_dir(home)
        .args(args);
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile);
    }
    command.output().unwrap()
}

#[cfg(unix)]
fn document(output: &Output) -> Value {
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    serde_json::from_slice(&output.stdout).unwrap()
}

#[cfg(unix)]
fn stat_text(pid: u32, comm: &str, ppid: u32) -> String {
    let mut fields = vec!["S".to_string(), ppid.to_string()];
    fields.extend(std::iter::repeat_n("0".to_string(), 18));
    fields[19] = "0".to_string();
    format!("{pid} ({comm}) {}\n", fields.join(" "))
}

#[cfg(unix)]
fn cmdline(argv: &[&str]) -> Vec<u8> {
    let mut raw = Vec::new();
    for (index, arg) in argv.iter().enumerate() {
        if index > 0 {
            raw.push(0);
        }
        raw.extend(arg.as_bytes());
    }
    raw.push(0);
    raw
}

#[cfg(unix)]
fn write_proc(
    proc: &Path,
    pid: u32,
    comm: &str,
    argv: &[&str],
    ppid: u32,
    uid: u32,
    subreaper: bool,
) {
    let base = proc.join(pid.to_string());
    fs::create_dir_all(&base).unwrap();
    fs::write(base.join("comm"), format!("{comm}\n")).unwrap();
    fs::write(base.join("cmdline"), cmdline(argv)).unwrap();
    fs::write(base.join("stat"), stat_text(pid, comm, ppid)).unwrap();
    let mut status = format!("Name:\t{comm}\nUid:\t{uid}\t{uid}\t{uid}\t{uid}\n");
    if subreaper {
        status.push_str("ChildSubreaper:\t1\n");
    }
    fs::write(base.join("status"), status).unwrap();
}

#[cfg(unix)]
fn snapshot(proc: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files = Vec::new();
    let mut walk = vec![proc.to_path_buf()];
    while let Some(dir) = walk.pop() {
        for entry in fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                walk.push(path);
            } else {
                let relative = path
                    .strip_prefix(proc)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                files.push((relative, fs::read(&path).unwrap()));
            }
        }
    }
    files.sort();
    files
}

#[cfg(unix)]
#[test]
fn fixture_scan_reports_json_and_text_without_writing() {
    let uid = rustix::process::getuid().as_raw();
    let root = common::private_tempdir();
    let home = root.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let proc = root.path().join("proc");
    fs::create_dir_all(&proc).unwrap();
    fs::write(proc.join("stat"), "cpu 0 0 0\nbtime 1700000000\n").unwrap();
    write_proc(
        &proc,
        10,
        "systemd",
        &["/usr/lib/systemd/systemd", "--user"],
        1,
        uid,
        true,
    );
    write_proc(
        &proc,
        30,
        "codex-linux-san",
        &["/usr/lib/codex/codex-linux-sandbox", "--", "sleep", "30"],
        10,
        uid,
        false,
    );
    let before = snapshot(&proc);
    let proc_arg = proc.to_str().unwrap();

    let warn = document(&run(
        &home,
        &[
            "liveness",
            "sandbox-orphans",
            "--proc-root",
            proc_arg,
            "--warn-after-seconds",
            "1",
            "--json",
        ],
    ));
    assert_eq!(warn["schema"], "codex-sandbox-orphans.v1");
    assert_eq!(warn["available"], true);
    assert_eq!(warn["readonly"], true);
    assert_eq!(warn["cleanup"], false);
    assert_eq!(warn["status"], "warn");
    assert_eq!(warn["orphan_count"], 1);
    assert_eq!(warn["warn_after_seconds"], 1);
    assert_eq!(warn["orphans"][0]["pid"], 30);
    assert_eq!(warn["orphans"][0]["ppid"], 10);
    assert_eq!(warn["orphans"][0]["kind"], "codex-linux-sandbox");
    assert_eq!(warn["orphans"][0]["parent_kind"], "systemd_user");
    assert!(warn["orphans"][0]["age_seconds"].as_i64().unwrap() > 21_600);

    let before_flag = document(&run(
        &home,
        &[
            "liveness",
            "--json",
            "sandbox-orphans",
            "--proc-root",
            proc_arg,
        ],
    ));
    assert_eq!(before_flag["orphan_count"], 1);
    assert_eq!(before_flag["status"], "warn");
    assert_eq!(before_flag["warn_after_seconds"], 21_600);

    let text = run(
        &home,
        &["liveness", "sandbox-orphans", "--proc-root", proc_arg],
    );
    assert_eq!(text.status.code(), Some(0));
    assert!(text.stderr.is_empty());
    let rendered = String::from_utf8(text.stdout).unwrap();
    assert!(rendered.starts_with("Codex sandbox orphans (read-only, no cleanup)\n"));
    assert!(rendered.contains("status warn"));
    assert!(rendered.contains("pid=30"));
    assert_eq!(snapshot(&proc), before);
}

#[cfg(unix)]
#[test]
fn missing_proc_root_exits_unavailable_for_json_and_text() {
    let root = common::private_tempdir();
    let home = root.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let missing = root.path().join("missing");
    let missing_arg = missing.to_str().unwrap();

    let json = run(
        &home,
        &[
            "liveness",
            "sandbox-orphans",
            "--proc-root",
            missing_arg,
            "--json",
        ],
    );
    assert_eq!(json.status.code(), Some(1));
    assert!(json.stderr.is_empty());
    let body: Value = serde_json::from_slice(&json.stdout).unwrap();
    assert_eq!(body["available"], false);
    assert_eq!(body["status"], "unavailable");
    assert_eq!(body["reason"], "proc_unavailable");
    assert_eq!(body["cleanup"], false);
    assert_eq!(body["orphan_count"], 0);
    assert!(!missing.exists());

    let text = run(
        &home,
        &["liveness", "sandbox-orphans", "--proc-root", missing_arg],
    );
    assert_eq!(text.status.code(), Some(1));
    assert!(text.stderr.is_empty());
    assert_eq!(
        String::from_utf8(text.stdout).unwrap(),
        "Codex sandbox orphan check unavailable: proc_unavailable\n"
    );
}

#[test]
fn help_and_invalid_threshold_do_not_scan() {
    let root = common::private_tempdir();
    let home = root.path().join("home");
    fs::create_dir_all(&home).unwrap();

    let help = run(&home, &["liveness", "sandbox-orphans", "--help"]);
    assert_eq!(help.status.code(), Some(0));
    let help_text = String::from_utf8(help.stdout).unwrap();
    assert!(help_text.contains("Read-only"));
    assert!(help_text.contains("Does not stop any process"));
    assert!(help_text.contains("--proc-root"));
    assert!(help_text.contains("--warn-after-seconds"));
    assert!(help_text.contains("--json"));

    let missing = run(&home, &["liveness"]);
    assert_eq!(missing.status.code(), Some(2));
    assert!(!String::from_utf8_lossy(&missing.stdout).contains("codex-sandbox-orphans.v1"));

    let invalid = run(
        &home,
        &[
            "liveness",
            "sandbox-orphans",
            "--warn-after-seconds",
            "nope",
        ],
    );
    assert_eq!(invalid.status.code(), Some(2));
    assert!(invalid.stdout.is_empty());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("non-negative"));

    let negative = run(
        &home,
        &["liveness", "sandbox-orphans", "--warn-after-seconds=-1"],
    );
    assert_eq!(negative.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&negative.stderr).contains("non-negative"));
}

#[cfg(not(target_os = "linux"))]
#[test]
fn host_scan_is_unavailable_off_linux() {
    let root = common::private_tempdir();
    let home = root.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let output = run(&home, &["liveness", "sandbox-orphans", "--json"]);
    assert_eq!(output.status.code(), Some(1));
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["available"], false);
    assert_eq!(body["cleanup"], false);
    assert_eq!(body["orphan_count"], 0);
    let reason = body["reason"].as_str().unwrap();
    assert!(reason == "proc_unavailable" || reason == "uid_unavailable");
}

#[test]
fn command_sources_do_not_signal() {
    let scanner = include_str!("../../vyane-service/src/codex_sandbox_orphans.rs");
    let command = include_str!("../src/liveness.rs");
    for source in [scanner, command] {
        let production = source.split("mod tests {").next().unwrap();
        for needle in [
            concat!("SIG", "KILL"),
            concat!("SIG", "TERM"),
            concat!("libc::", "kill"),
            concat!("rustix::process::", "kill"),
            concat!("std::", "process::", "Command"),
        ] {
            assert!(!production.contains(needle), "{needle}");
        }
    }
}
