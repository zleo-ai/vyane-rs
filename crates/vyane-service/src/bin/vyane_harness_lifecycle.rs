//! Hermetic harness lifecycle test protocol executable.
//!
//! This is a **public, safe stand-in** for Claude/Codex/Grok CLI lifecycle
//! shapes used by integration tests. It is **not** a formal vendor harness
//! product integration.
//!
//! Lifecycle:
//!   spawn → running → structured stdout/stderr/event → optional approval wait
//!   → resume → artifact → exit with verifiable code
//!
//! Injectors (env / flags):
//!   --mode normal|ask|crash-after-start|duplicate-effect|stale-gen
//!   --grant-file PATH   (when present with content "grant", resume after ask)
//!   --workdir PATH
//!   --run-id ID
//!   --payload TEXT

use std::env;
use std::fs;
use std::io::{self, Write as _};
use std::path::PathBuf;
use std::process;
use std::thread;
use std::time::Duration;

fn emit_event(kind: &str, fields: &[(&str, &str)]) {
    let mut obj = serde_json::json!({
        "schema": "vyane.harness_lifecycle.v1",
        "event": kind,
    });
    if let Some(map) = obj.as_object_mut() {
        for (k, v) in fields {
            map.insert(
                (*k).to_string(),
                serde_json::Value::String((*v).to_string()),
            );
        }
    }
    println!("{obj}");
    let _ = io::stdout().flush();
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut mode = "normal".to_string();
    let mut workdir = env::temp_dir().join("vyane-harness-lifecycle");
    let mut run_id = "run-default".to_string();
    let mut payload = "harness-payload-v1".to_string();
    let mut grant_file: Option<PathBuf> = None;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" if i + 1 < args.len() => {
                mode = args[i + 1].clone();
                i += 2;
            }
            "--workdir" if i + 1 < args.len() => {
                workdir = PathBuf::from(&args[i + 1]);
                i += 2;
            }
            "--run-id" if i + 1 < args.len() => {
                run_id = args[i + 1].clone();
                i += 2;
            }
            "--payload" if i + 1 < args.len() => {
                payload = args[i + 1].clone();
                i += 2;
            }
            "--grant-file" if i + 1 < args.len() => {
                grant_file = Some(PathBuf::from(&args[i + 1]));
                i += 2;
            }
            "--help" | "-h" => {
                eprintln!(
                    "vyane_harness_lifecycle — hermetic CLI lifecycle stand-in (not vendor integration)"
                );
                process::exit(0);
            }
            other => {
                eprintln!("unknown arg: {other}");
                process::exit(2);
            }
        }
    }

    let _ = fs::create_dir_all(&workdir);
    emit_event(
        "started",
        &[
            ("run_id", &run_id),
            ("mode", &mode),
            ("pid", &process::id().to_string()),
        ],
    );
    eprintln!("harness-lifecycle: running mode={mode} run_id={run_id}");

    if mode == "crash-after-start" {
        emit_event("crashing", &[("window", "after_start")]);
        process::exit(137);
    }

    if mode == "stale-gen" {
        emit_event(
            "error",
            &[("code", "stale_generation"), ("run_id", &run_id)],
        );
        process::exit(3);
    }

    // Long-running tick so cancel/SIGTERM tests can observe a live child.
    thread::sleep(Duration::from_millis(50));
    emit_event("running", &[("run_id", &run_id)]);

    if mode == "ask" {
        let ask_path = workdir.join("approval.required");
        let _ = fs::write(&ask_path, format!("ask:{run_id}"));
        emit_event(
            "approval_required",
            &[
                ("run_id", &run_id),
                ("digest", "pending"),
                ("path", &ask_path.to_string_lossy()),
            ],
        );
        // Wait for grant file (bounded).
        let grant = grant_file.unwrap_or_else(|| workdir.join("approval.grant"));
        let mut granted = false;
        for _ in 0..200 {
            if grant.exists()
                && let Ok(body) = fs::read_to_string(&grant)
            {
                if body.trim() == "grant" {
                    granted = true;
                    break;
                }
                if body.trim() == "deny" {
                    emit_event("denied", &[("run_id", &run_id)]);
                    process::exit(4);
                }
            }
            thread::sleep(Duration::from_millis(25));
        }
        if !granted {
            emit_event("error", &[("code", "approval_timeout")]);
            process::exit(5);
        }
        emit_event("resumed", &[("run_id", &run_id)]);
    }

    // Effect + artifact
    let effect_marker = workdir.join("effect.marker");
    if mode == "duplicate-effect" && effect_marker.exists() {
        emit_event(
            "error",
            &[("code", "duplicate_effect"), ("run_id", &run_id)],
        );
        process::exit(6);
    }
    if let Err(e) = fs::write(&effect_marker, &payload) {
        eprintln!("write effect: {e}");
        process::exit(7);
    }
    emit_event(
        "effect_recorded",
        &[
            ("run_id", &run_id),
            ("path", &effect_marker.to_string_lossy()),
        ],
    );

    let artifact_dir = workdir.join("artifacts");
    let _ = fs::create_dir_all(&artifact_dir);
    let artifact_path = artifact_dir.join(format!("{run_id}.txt"));
    let body = format!("artifact:{payload}");
    if let Err(e) = fs::write(&artifact_path, &body) {
        eprintln!("write artifact: {e}");
        process::exit(8);
    }
    emit_event(
        "artifact_finalized",
        &[
            ("run_id", &run_id),
            ("path", &artifact_path.to_string_lossy()),
            ("bytes", &body.len().to_string()),
        ],
    );

    // Truth probe companion: write MARKER=PASS for dogfood-compatible probes.
    let _ = fs::write(workdir.join("MARKER"), "PASS");
    emit_event("completed", &[("run_id", &run_id), ("exit", "0")]);
    process::exit(0);
}
