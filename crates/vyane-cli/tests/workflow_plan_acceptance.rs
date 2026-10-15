mod common;

use std::fs;
use std::path::Path;

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;
use vyane_workflow::{Workflow, WorkflowPlan};

const SOURCE: &str = r#"
[workflow]
name = "inspect-before-run"
max_concurrency = 2

[[step]]
id = "gather"
fan_out = ["not-configured-a", "not-configured-b"]
prompt_file = "prompt.txt"
system = "private system instructions"
workdir = "workspace"
sandbox = "read-only"
timeout_secs = 7

[[step]]
id = "summarize"
needs = ["gather"]
target = "auto"
prompt = "Summarize {{steps.gather.outputs}}"
[step.route]
effort = "high"
"#;

fn plan_command(root: &Path) -> Command {
    let mut command = Command::cargo_bin("vyane").expect("vyane binary");
    command
        .env_clear()
        .env("HOME", root)
        .env("XDG_CONFIG_HOME", root.join("missing-config"))
        .env("VYANE_DATA_DIR", root.join("missing-data"))
        .current_dir(root)
        .arg("--config")
        .arg(root.join("must-not-be-loaded.toml"))
        .args(["workflow", "plan"])
        .arg(root.join("workflow.toml"));
    // Keep instrumented subprocess profiles in the collector's output directory,
    // so the fixture directory remains a check for application side effects.
    if let Some(profile_file) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile_file);
    }
    command
}

#[test]
fn workflow_plan_exports_the_existing_wire_payload_without_runtime_setup() {
    let root = common::private_tempdir();
    let workflow = root.path().join("workflow.toml");
    fs::write(&workflow, SOURCE).expect("test fixture operation succeeds");
    fs::write(root.path().join("prompt.txt"), "Find {{vars.topic}}\n")
        .expect("test fixture operation succeeds");
    let output = plan_command(root.path())
        .arg("--json")
        .assert()
        .success()
        .stderr("")
        .get_output()
        .stdout
        .clone();
    let plan: WorkflowPlan = serde_json::from_slice(&output).expect("existing plan schema");
    plan.verify().expect("valid digest-bound plan");
    let expected = Workflow::from_path(&workflow)
        .expect("test fixture operation succeeds")
        .compile_plan()
        .expect("test fixture operation succeeds");
    assert_eq!(plan, expected);
    let json: Value = serde_json::from_slice(&output).expect("test fixture operation succeeds");
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["steps"][0]["prompt_template"], "Find {{vars.topic}}\n");
    assert_eq!(json["steps"][0]["targets"]["kind"], "fan_out");
    assert_eq!(json["steps"][1]["needs"][0], "gather");
    assert_eq!(json["steps"][1]["route"]["effort"], "high");
    assert_eq!(json["capability_manifest"]["uses_fan_out"], true);
    assert!(json.get("source_path").is_none());
    assert!(!root.path().join("missing-data").exists());
    assert_eq!(
        fs::read_to_string(&workflow).expect("test fixture operation succeeds"),
        SOURCE
    );
    assert_eq!(
        fs::read_to_string(root.path().join("prompt.txt"))
            .expect("test fixture operation succeeds"),
        "Find {{vars.topic}}\n"
    );
    assert_eq!(
        fs::read_dir(root.path())
            .expect("test fixture operation succeeds")
            .count(),
        2
    );

    let repeated = plan_command(root.path())
        .arg("--json")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    assert_eq!(output, repeated);
    fs::write(root.path().join("prompt.txt"), "Changed {{vars.topic}}\n")
        .expect("test fixture operation succeeds");
    let changed = plan_command(root.path())
        .arg("--json")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let changed: WorkflowPlan =
        serde_json::from_slice(&changed).expect("test fixture operation succeeds");
    assert_ne!(plan.source_sha256, changed.source_sha256);
    assert_ne!(plan.plan_sha256, changed.plan_sha256);
}

#[test]
fn workflow_plan_human_output_describes_static_steps_without_prompt_payload() {
    let root = common::private_tempdir();
    fs::write(root.path().join("workflow.toml"), SOURCE).expect("test fixture operation succeeds");
    fs::write(root.path().join("prompt.txt"), "private prompt body")
        .expect("test fixture operation succeeds");
    plan_command(root.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("inspect-before-run"))
        .stdout(predicate::str::contains("gather"))
        .stdout(predicate::str::contains("summarize"))
        .stdout(predicate::str::contains("not-configured-a"))
        .stdout(predicate::str::contains("targets are unresolved"))
        .stdout(predicate::str::contains("private prompt body").not())
        .stdout(predicate::str::contains("private system instructions").not());
    assert!(!root.path().join("missing-data").exists());
}

#[test]
fn workflow_plan_rejects_invalid_sources_without_emitting_a_partial_plan() {
    let root = common::private_tempdir();
    for source in [
        "this is not TOML",
        "[workflow]\nname = 'broken'\n[[step]]\nid = 'one'\ntarget = 'unused'\nprompt = 'hello'\nneeds = ['missing']\n",
    ] {
        fs::write(root.path().join("workflow.toml"), source)
            .expect("test fixture operation succeeds");
        plan_command(root.path())
            .arg("--json")
            .assert()
            .code(2)
            .stdout("")
            .stderr(predicate::str::contains("workflow"));
        assert!(!root.path().join("missing-data").exists());
    }
}

#[test]
fn workflow_plan_help_explains_the_static_and_sensitive_payload_boundaries() {
    Command::cargo_bin("vyane")
        .expect("test fixture operation succeeds")
        .args(["workflow", "plan", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("does not resolve targets"))
        .stdout(predicate::str::contains(
            "prompts, system text, and workdirs",
        ));
}
