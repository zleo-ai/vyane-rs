mod common;

use std::fs;
use std::path::Path;
use std::time::Duration;

use assert_cmd::Command;
use serde_json::{Value, json};

const CONFIG: &str = r#"
[providers.example]
base_url = "http://127.0.0.1:1/PRIVATE_ENDPOINT"
api_key_env = "PRIVATE_CREDENTIAL_NAME"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "PRIVATE_MODEL"

[profiles.sample]
provider = "example"
model = "PRIVATE_MODEL"

[harness_permissions]
max_sandbox = "read-only"
"#;

fn command(root: &Path) -> Command {
    let mut command = Command::cargo_bin("vyane").expect("vyane binary");
    command
        .env_clear()
        .env("HOME", root)
        .env("PATH", root.join("bin"))
        .env("XDG_CONFIG_HOME", root.join("config"))
        .env("XDG_DATA_HOME", root.join("data"))
        .env("VYANE_DATA_DIR", root.join("vyane-data"))
        .current_dir(root)
        .timeout(Duration::from_secs(10))
        .arg("--config")
        .arg(root.join("PRIVATE_CONFIG.toml"))
        .args(["check", "--json"]);
    if let Some(profile_file) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile_file);
    }
    command
}

fn check(root: &Path, exit: i32) -> (Vec<u8>, Value) {
    let output = command(root)
        .assert()
        .code(exit)
        .stderr("")
        .get_output()
        .stdout
        .clone();
    let text = String::from_utf8_lossy(&output);
    for private in ["PRIVATE_", root.to_str().expect("fixture path")] {
        assert!(
            !text.contains(private),
            "private fixture data escaped: {text}"
        );
    }
    for storage in ["config", "data", "vyane-data"] {
        assert!(!root.join(storage).exists(), "created runtime storage");
    }
    let value = serde_json::from_slice(&output).expect("one JSON document");
    (output, value)
}

#[test]
fn check_json_reports_static_configuration_without_probing_or_disclosing_secrets() {
    let root = common::private_tempdir();
    fs::write(root.path().join("PRIVATE_CONFIG.toml"), CONFIG).expect("write config");
    fs::write(
        root.path().join("secrets.env"),
        "PRIVATE_CREDENTIAL_NAME=PRIVATE_SECRET\n",
    )
    .expect("write secrets");
    let (first, value) = check(root.path(), 0);
    assert_eq!(value["status"], "valid");
    assert_eq!(value["scope"], "static_config_only");
    assert_eq!(value["provider_count"], 1);
    assert_eq!(value["profile_count"], 1);
    assert_eq!(
        value["providers"],
        json!([{
            "id": "example", "protocol": "openai_chat", "has_default_model": true,
            "credential": "present"
        }])
    );
    assert_eq!(
        value["profiles"],
        json!([{
            "name": "sample", "status": "resolvable", "chain_length": 1
        }])
    );
    assert_eq!(value["issues"], json!([]));
    assert_eq!(
        value["permissions"]["harness"],
        json!({
            "ceiling_layers": 1, "max_sandbox": "read-only"
        })
    );
    let keys: Vec<_> = value
        .as_object()
        .expect("report")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        [
            "issues",
            "permissions",
            "profile_count",
            "profiles",
            "provider_count",
            "providers",
            "scope",
            "status"
        ]
    );
    assert_eq!(
        check(root.path(), 0).0,
        first,
        "deterministic static report"
    );
    assert_eq!(
        fs::read_to_string(root.path().join("PRIVATE_CONFIG.toml")).expect("config"),
        CONFIG
    );
    assert_eq!(fs::read_dir(root.path()).expect("fixture").count(), 2);
}

#[test]
fn check_json_partial_is_a_successful_report_with_closed_issues() {
    let root = common::private_tempdir();
    fs::write(root.path().join("PRIVATE_CONFIG.toml"), CONFIG).expect("write config");
    let (_, value) = check(root.path(), 0);
    assert_eq!(value["status"], "partial");
    assert_eq!(value["providers"][0]["credential"], "missing");
    assert!(
        value["issues"]
            .as_array()
            .expect("issues")
            .iter()
            .any(|issue| issue["code"] == "credential_missing")
    );
    fs::write(root.path().join("PRIVATE_CONFIG.toml"), "").expect("empty config");
    let (_, empty) = check(root.path(), 0);
    assert_eq!(empty["status"], "partial");
    assert_eq!(empty["provider_count"], 0);
    assert_eq!(empty["profile_count"], 0);
    assert_eq!(empty["providers"], json!([]));
    assert_eq!(empty["profiles"], json!([]));
}

#[test]
fn check_json_load_errors_are_fixed_json_without_raw_config_or_path() {
    let root = common::private_tempdir();
    fs::write(
        root.path().join("PRIVATE_CONFIG.toml"),
        "PRIVATE_INVALID = [",
    )
    .expect("invalid config");
    let (_, invalid) = check(root.path(), 2);
    assert_eq!(
        invalid,
        json!({"error": {"code": "config_invalid", "message": "vyane configuration is invalid"}})
    );
    fs::write(root.path().join("PRIVATE_CONFIG.toml"), CONFIG).expect("valid config");
    fs::write(
        root.path().join("secrets.env"),
        "PRIVATE_INVALID_SECRET_LINE",
    )
    .expect("invalid secrets");
    assert_eq!(check(root.path(), 2).1, invalid);
}

#[test]
fn check_json_rejects_diagnostic_overflow_without_partial_report() {
    let root = common::private_tempdir();
    let mut config = String::new();
    for index in 0..=vyane_service::DIAGNOSTIC_MAX_CONFIG_ITEMS {
        config.push_str(&format!("[providers.p{index}]\nbase_url='https://example.invalid/v1'\nprotocol='openai_chat'\nauth_style='bearer'\ndefault_model='model'\n"));
    }
    fs::write(root.path().join("PRIVATE_CONFIG.toml"), config).expect("oversized config");
    assert_eq!(
        check(root.path(), 2).1,
        json!({"error": {"code": "budget_exceeded", "message": "diagnostic safety limit exceeded"}})
    );
}

#[cfg(unix)]
#[test]
fn check_json_does_not_launch_available_harness_binaries() {
    use std::os::unix::fs::PermissionsExt;
    let root = common::private_tempdir();
    fs::write(root.path().join("PRIVATE_CONFIG.toml"), CONFIG).expect("config");
    fs::create_dir(root.path().join("bin")).expect("bin");
    for name in ["claude", "codex"] {
        let path = root.path().join("bin").join(name);
        fs::write(
            &path,
            "#!/bin/sh\nprintf invoked >> harness-invoked\nexit 1\n",
        )
        .expect("fake harness");
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("executable");
    }
    check(root.path(), 0);
    assert!(!root.path().join("harness-invoked").exists());
}
