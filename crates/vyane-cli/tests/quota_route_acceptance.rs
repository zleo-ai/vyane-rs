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

fn write_snapshot(path: &std::path::Path, busy_hot: bool) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let week_ms = 7 * 24 * 3_600 * 1_000;
    let (busy_used, busy_pace, free_used, free_pace) = if busy_hot {
        (96.0, 2.1, 40.0, 1.0)
    } else {
        (40.0, 1.0, 96.0, 2.1)
    };
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
                    "used_pct": busy_used,
                    "pace_ratio": busy_pace
                }]
            },
            {
                "provider": "free-provider",
                "kind": "subscription",
                "windows": [{
                    "id": "free-provider:7d",
                    "duration_ms": week_ms,
                    "used_pct": free_used,
                    "pace_ratio": free_pace
                }]
            }
        ]
    });
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("snapshot dir");
    }
    fs::write(path, serde_json::to_vec(&body).expect("snapshot json")).expect("write snapshot");
}

enum StateHome<'a> {
    Unset,
    Empty,
    Path(&'a std::path::Path),
}

fn route(
    home: &std::path::Path,
    state_home: StateHome<'_>,
    config: &std::path::Path,
    extra: &[&str],
) -> Vec<u8> {
    let mut command = Command::cargo_bin("vyane").expect("vyane binary");
    command
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("XDG_CONFIG_HOME", home.join("config-home"))
        .env_remove("VYANE_MANAGED_PERMISSION_CONFIG")
        .env_remove("VYANE_MANAGED_NATIVE_CONFIG")
        .arg("--config")
        .arg(config)
        .args(["route", "ping", "--json"])
        .args(extra);
    match state_home {
        StateHome::Unset => {
            command.env_remove("XDG_STATE_HOME");
        }
        StateHome::Empty => {
            command.env("XDG_STATE_HOME", "");
        }
        StateHome::Path(path) => {
            command.env("XDG_STATE_HOME", path);
        }
    }
    let output = command.output().expect("run route");
    assert!(
        output.status.success(),
        "route failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn selection(bytes: &[u8]) -> String {
    let decision: Value = serde_json::from_slice(bytes).expect("route json");
    decision["selection_key"]
        .as_str()
        .expect("selection_key")
        .to_string()
}

#[test]
fn omitted_route_reads_the_xdg_state_snapshot_and_a_missing_file_stays_unfiltered() {
    let directory = common::private_tempdir();
    let config = write_profiles(directory.path());
    let state = directory.path().join("state");
    let home = directory.path().join("home");
    let default_file = state.join("aios/quota-snapshot.json");
    let home_file = home.join(".local/state/aios/quota-snapshot.json");
    write_snapshot(&default_file, true);
    write_snapshot(&home_file, false);

    let from_state = route(&home, StateHome::Path(&state), &config, &[]);
    assert_eq!(selection(&from_state), "free");
    assert!(!String::from_utf8_lossy(&from_state).contains("snapshot-note-not-routed"));
    assert!(!String::from_utf8_lossy(&from_state).contains("busy-provider"));

    fs::remove_file(&default_file).expect("remove default snapshot");
    write_snapshot(&home_file, true);
    let missing_default = route(&home, StateHome::Path(&state), &config, &[]);
    let missing_path = directory.path().join("missing.json");
    let explicit_missing = route(
        &home,
        StateHome::Path(&state),
        &config,
        &[
            "--quota-snapshot",
            missing_path.to_str().expect("utf-8 temp path"),
        ],
    );
    assert_eq!(missing_default, explicit_missing);
    assert_eq!(selection(&missing_default), "busy");
    assert_eq!(
        serde_json::from_slice::<Value>(&missing_default).expect("route json")["provider"],
        "busy-provider"
    );
}

#[test]
fn omitted_route_uses_home_local_state_when_xdg_state_home_is_unset_or_empty() {
    let directory = common::private_tempdir();
    let config = write_profiles(directory.path());
    let home = directory.path().join("home");
    let home_file = home.join(".local/state/aios/quota-snapshot.json");
    write_snapshot(&home_file, true);

    let unset = route(&home, StateHome::Unset, &config, &[]);
    let empty = route(&home, StateHome::Empty, &config, &[]);
    assert_eq!(selection(&unset), "free");
    assert_eq!(unset, empty);
    assert!(!String::from_utf8_lossy(&unset).contains("busy-provider"));
}

#[test]
fn explicit_quota_snapshot_overrides_the_default_file() {
    let directory = common::private_tempdir();
    let config = write_profiles(directory.path());
    let state = directory.path().join("state");
    let home = directory.path().join("home");
    write_snapshot(&state.join("aios/quota-snapshot.json"), true);
    let explicit = directory.path().join("quota-snapshot.json");
    write_snapshot(&explicit, false);

    let omitted = route(&home, StateHome::Path(&state), &config, &[]);
    let overridden = route(
        &home,
        StateHome::Path(&state),
        &config,
        &[
            "--quota-snapshot",
            explicit.to_str().expect("utf-8 temp path"),
        ],
    );
    let missing_path = directory.path().join("missing.json");
    let explicit_missing = route(
        &home,
        StateHome::Path(&state),
        &config,
        &[
            "--quota-snapshot",
            missing_path.to_str().expect("utf-8 temp path"),
        ],
    );
    assert_eq!(selection(&omitted), "free");
    assert_eq!(selection(&overridden), "busy");
    assert_eq!(selection(&explicit_missing), "busy");
    assert_ne!(omitted, overridden);
    assert_ne!(explicit_missing, omitted);
    assert!(!String::from_utf8_lossy(&overridden).contains("snapshot-note-not-routed"));
}
