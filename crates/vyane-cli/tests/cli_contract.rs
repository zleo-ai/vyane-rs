//! Exact CLI output contracts for parser-only and local validation paths.
//! These fixtures describe Rust behavior, not cross-implementation parity.

mod common;

use std::collections::BTreeSet;
use std::fs;
use std::time::Duration;

use assert_cmd::Command;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    name: String,
    args: Vec<String>,
    exit_code: i32,
    stdout: String,
    stderr: String,
}

const NESTED_PARENTS: &[&str] = &["session", "workflow", "task", "daemon", "a2a", "goal"];

fn nested_command_names(help: &str) -> Vec<&str> {
    let mut names = Vec::new();
    let mut commands = false;
    for line in help.lines() {
        if !commands {
            if line == "Commands:" {
                commands = true;
            }
            continue;
        }
        if line.is_empty() || line.starts_with("Options:") {
            break;
        }
        let indent = line.len() - line.trim_start().len();
        if indent != 2 {
            continue;
        }
        let name = line.split_whitespace().next().expect("nested command row");
        if name != "help" {
            names.push(name);
        }
    }
    assert!(!names.is_empty(), "nested parent help listed no commands");
    names
}

#[test]
fn cli_help_and_exit_contract() {
    let snapshots: Vec<Snapshot> =
        serde_json::from_str(include_str!("fixtures/cli-contract.json")).expect("valid snapshots");
    assert!(!snapshots.is_empty(), "the contract must contain cases");
    let mut names = BTreeSet::new();
    let mut arg_sets = BTreeSet::new();
    for snapshot in &snapshots {
        assert!(names.insert(snapshot.name.clone()), "duplicate case name");
        assert!(
            arg_sets.insert(snapshot.args.clone()),
            "duplicate case args: {}",
            snapshot.name
        );
    }
    for parent in NESTED_PARENTS {
        let parent_help = snapshots
            .iter()
            .find(|snapshot| {
                snapshot.args.len() == 2
                    && snapshot.args[0] == *parent
                    && snapshot.args[1] == "--help"
            })
            .unwrap_or_else(|| panic!("missing {parent} help snapshot"));
        for command in nested_command_names(&parent_help.stdout) {
            let args = [
                (*parent).to_string(),
                command.to_string(),
                "--help".to_string(),
            ];
            assert!(
                arg_sets.contains(args.as_slice()),
                "uncovered nested command: {parent} {command}"
            );
        }
    }
    for snapshot in snapshots {
        let root = common::private_tempdir();
        let config = root.path().join("invalid-config.toml");
        let config_contents = "this is not valid TOML\n";
        fs::write(&config, config_contents).expect("write sentinel config");
        let mut command = Command::cargo_bin("vyane").expect("vyane binary");
        command
            .env_clear()
            .env("HOME", root.path())
            .env("XDG_CONFIG_HOME", root.path().join("config"))
            .env("XDG_DATA_HOME", root.path().join("data"))
            .env("VYANE_DATA_DIR", root.path().join("vyane-data"))
            .env("NO_COLOR", "1")
            .env("TERM", "dumb")
            .env("COLUMNS", "80")
            .env("LC_ALL", "C")
            .current_dir(root.path())
            .timeout(Duration::from_secs(10))
            .args(["--config", "invalid-config.toml"])
            .args(&snapshot.args);
        // Coverage belongs in the collector's directory, outside the sentinel
        // fixture used to detect application-created storage.
        if let Some(profile_file) = std::env::var_os("LLVM_PROFILE_FILE") {
            command.env("LLVM_PROFILE_FILE", profile_file);
        }
        let output = command.output().expect("run CLI contract case");
        assert_eq!(
            output.status.code(),
            Some(snapshot.exit_code),
            "{}: exit code; stderr={}",
            snapshot.name,
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            output.stdout,
            snapshot.stdout.as_bytes(),
            "{}: stdout",
            snapshot.name
        );
        assert_eq!(
            output.stderr,
            snapshot.stderr.as_bytes(),
            "{}: stderr",
            snapshot.name
        );
        assert_eq!(
            fs::read_to_string(&config).expect("sentinel config"),
            config_contents
        );
        let entries: Vec<_> = fs::read_dir(root.path())
            .expect("fixture directory")
            .map(|entry| entry.expect("fixture entry").file_name())
            .collect();
        assert_eq!(
            entries,
            ["invalid-config.toml"],
            "{}: unexpected application storage",
            snapshot.name
        );
    }
}
