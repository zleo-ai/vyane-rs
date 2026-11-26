mod common;

use std::fs;

use assert_cmd::Command;
use serde_json::Value;

fn write_profiles(directory: &std::path::Path) -> std::path::PathBuf {
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
    .expect("write config");
    config
}

fn write_snapshot(path: &std::path::Path) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let week_ms = 7 * 24 * 3_600 * 1_000;
    let body = serde_json::json!({
        "schema_version": 1,
        "generated_at_ms": now,
        "probe_status": "ok",
        "note": "snapshot-note-not-routed",
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
    fs::write(path, serde_json::to_vec(&body).expect("snapshot json")).expect("write snapshot");
}

fn route(directory: &std::path::Path, config: &std::path::Path, extra: &[&str]) -> Vec<u8> {
    let mut command = Command::cargo_bin("vyane").expect("vyane binary");
    command
        .env("HOME", directory)
        .env("XDG_CONFIG_HOME", directory.join("config-home"))
        .env("XDG_STATE_HOME", directory.join("state"))
        .env_remove("VYANE_MANAGED_PERMISSION_CONFIG")
        .env_remove("VYANE_MANAGED_NATIVE_CONFIG")
        .arg("--config")
        .arg(config)
        .args(["route", "ping", "--json"])
        .args(extra);
    let output = command.output().expect("run route");
    assert!(
        output.status.success(),
        "route failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

#[test]
fn route_json_uses_a_local_snapshot_only_when_the_flag_is_set() {
    let directory = common::private_tempdir();
    let config = write_profiles(directory.path());
    let decoy = directory.path().join("state/aios");
    fs::create_dir_all(&decoy).expect("decoy dir");
    write_snapshot(&decoy.join("quota-snapshot.json"));
    let explicit = directory.path().join("quota-snapshot.json");
    write_snapshot(&explicit);

    let off = route(directory.path(), &config, &[]);
    let missing_path = directory.path().join("missing.json");
    let missing = route(
        directory.path(),
        &config,
        &[
            "--quota-snapshot",
            missing_path.to_str().expect("utf-8 temp path"),
        ],
    );
    assert_eq!(off, missing);

    let off_decision: Value = serde_json::from_slice(&off).expect("route json");
    assert_eq!(off_decision["selection_key"], "busy");
    assert_eq!(off_decision["provider"], "busy-provider");
    assert!(!String::from_utf8_lossy(&off).contains("snapshot-note-not-routed"));

    let filtered = route(
        directory.path(),
        &config,
        &[
            "--quota-snapshot",
            explicit.to_str().expect("utf-8 temp path"),
        ],
    );
    assert_ne!(filtered, off);
    let filtered_decision: Value = serde_json::from_slice(&filtered).expect("filtered route json");
    assert_eq!(filtered_decision["selection_key"], "free");
    assert_eq!(filtered_decision["provider"], "free-provider");
    assert!(!String::from_utf8_lossy(&filtered).contains("snapshot-note-not-routed"));
    assert!(!String::from_utf8_lossy(&filtered).contains("busy-provider"));
}
