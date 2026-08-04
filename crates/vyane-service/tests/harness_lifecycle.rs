//! Integration tests for the hermetic harness lifecycle protocol binary.
//!
//! This is a public-safe CLI lifecycle stand-in — not formal vendor integration.

use std::fs;
use std::process::Command;
use std::thread;
use std::time::Duration;

#[test]
fn hermetic_harness_lifecycle_spawn_ask_resume_artifact_dual_run() {
    let bin = env!("CARGO_BIN_EXE_vyane_harness_lifecycle");
    let root = tempfile::tempdir().unwrap();
    let workdir = root.path().join("wd");
    fs::create_dir_all(&workdir).unwrap();

    for (run_id, grant_name) in [("lifecycle-1", "g1"), ("lifecycle-2", "g2")] {
        let grant = workdir.join(grant_name);
        let child = Command::new(bin)
            .args([
                "--mode",
                "ask",
                "--workdir",
                workdir.to_str().unwrap(),
                "--run-id",
                run_id,
                "--payload",
                "hermetic-v1",
                "--grant-file",
                grant.to_str().unwrap(),
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        thread::sleep(Duration::from_millis(80));
        fs::write(&grant, "grant").unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "harness {run_id} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        for event in [
            "started",
            "approval_required",
            "resumed",
            "effect_recorded",
            "artifact_finalized",
            "completed",
        ] {
            assert!(
                stdout.contains(&format!("\"event\":\"{event}\"")),
                "missing event {event} in {stdout}"
            );
        }
        assert!(
            workdir
                .join("artifacts")
                .join(format!("{run_id}.txt"))
                .exists()
        );
    }
    assert_eq!(
        fs::read_to_string(workdir.join("MARKER")).unwrap().trim(),
        "PASS"
    );
}

#[test]
fn hermetic_harness_cancel_via_kill() {
    let bin = env!("CARGO_BIN_EXE_vyane_harness_lifecycle");
    let root = tempfile::tempdir().unwrap();
    let workdir = root.path().join("wd");
    fs::create_dir_all(&workdir).unwrap();
    let mut child = Command::new(bin)
        .args([
            "--mode",
            "ask",
            "--workdir",
            workdir.to_str().unwrap(),
            "--run-id",
            "kill-1",
            "--grant-file",
            workdir.join("never.grant").to_str().unwrap(),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_millis(50));
    let _ = child.kill();
    let status = child.wait().unwrap();
    assert!(!status.success());
}

#[test]
fn hermetic_harness_normal_mode_and_crash_window() {
    let bin = env!("CARGO_BIN_EXE_vyane_harness_lifecycle");
    let root = tempfile::tempdir().unwrap();
    let workdir = root.path().join("wd");
    fs::create_dir_all(&workdir).unwrap();
    let output = Command::new(bin)
        .args([
            "--mode",
            "normal",
            "--workdir",
            workdir.to_str().unwrap(),
            "--run-id",
            "normal-1",
            "--payload",
            "n1",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let crash = Command::new(bin)
        .args([
            "--mode",
            "crash-after-start",
            "--workdir",
            workdir.to_str().unwrap(),
            "--run-id",
            "crash-1",
        ])
        .output()
        .unwrap();
    assert!(!crash.status.success());
}
