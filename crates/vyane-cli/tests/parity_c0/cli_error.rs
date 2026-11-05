use std::fs;
use std::path::Path;
use std::time::Duration;

use assert_cmd::Command;
use serde_json::{json, Value};
use vyane_core::ErrorKind;
use vyane_service::DiagnosticErrorKind;

pub(super) fn replay(input: &Value, temp: &Path) -> Value {
    match input["op"].as_str().expect("cli op") {
        "error_kind" => error_kind(input["kind"].as_str().expect("kind")),
        "unmapped" => json!({
            "concept": input["concept"].as_str().expect("concept"),
            "failover_eligible": false,
            "mapped": false,
            "reason": Value::Null,
            "rust_kind": Value::Null,
        }),
        "diagnostic_kind" => json!({
            "kind": diagnostic(input["kind"].as_str().expect("diagnostic kind")),
        }),
        "feedback_missing" => feedback_missing(temp, input["json"].as_bool().expect("json flag")),
        "feedback_rating" => feedback_rating(temp),
        "unknown_command" => unknown_command(temp),
        "version" => version_shape(temp),
        "session_commands" => command_names(temp, &["session", "--help"]),
        "workflow_commands" => command_names(temp, &["workflow", "--help"]),
        other => panic!("unknown cli op {other}"),
    }
}

fn error_kind(name: &str) -> Value {
    let kind = match name {
        "timeout" => ErrorKind::Timeout,
        "rate_limited" => ErrorKind::RateLimited,
        "spawn_failed" => ErrorKind::SpawnFailed,
        "harness_failed" => ErrorKind::HarnessFailed,
        "other" => ErrorKind::Other,
        "transport" => ErrorKind::Transport,
        "protocol" => ErrorKind::Protocol,
        "auth" => ErrorKind::Auth,
        "unsupported" => ErrorKind::Unsupported,
        "cancelled" => ErrorKind::Cancelled,
        "config" => ErrorKind::Config,
        "not_found" => ErrorKind::NotFound,
        "conflict" => ErrorKind::Conflict,
        "io" => ErrorKind::Io,
        "indeterminate" => ErrorKind::Indeterminate,
        other => panic!("unknown error kind {other}"),
    };
    let reason = match kind {
        ErrorKind::Timeout => "timeout",
        ErrorKind::RateLimited => "rate_limit",
        ErrorKind::SpawnFailed => "runtime_unavailable",
        ErrorKind::HarnessFailed => "runtime_crash",
        ErrorKind::Other => "execution_error",
        ErrorKind::Transport => "transport",
        ErrorKind::Protocol => "protocol",
        ErrorKind::Auth => "auth",
        ErrorKind::Unsupported => "unsupported",
        ErrorKind::Cancelled => "cancelled",
        ErrorKind::Config => "config",
        ErrorKind::NotFound => "not_found",
        ErrorKind::Conflict => "conflict",
        ErrorKind::Io => "io",
        ErrorKind::Indeterminate => "indeterminate",
        other => panic!("unpinned error kind {}", other.as_str()),
    };
    json!({
        "failover_eligible": kind.failover_eligible(),
        "mapped": true,
        "reason": reason,
        "rust_kind": kind.as_str(),
    })
}

fn diagnostic(name: &str) -> &'static str {
    match name {
        "invalid_input" => DiagnosticErrorKind::InvalidInput.as_str(),
        "config_invalid" => DiagnosticErrorKind::ConfigInvalid.as_str(),
        "budget_exceeded" => DiagnosticErrorKind::BudgetExceeded.as_str(),
        other => panic!("unknown diagnostic kind {other}"),
    }
}

fn feedback_missing(temp: &Path, json_mode: bool) -> Value {
    let args: &[&str] = if json_mode {
        &["feedback", "rate", "--json"]
    } else {
        &["feedback", "rate"]
    };
    let output = run(temp, args);
    let stderr = normalize(&output.stderr);
    let message = if json_mode {
        stderr
            .lines()
            .rev()
            .find(|line| line.starts_with('{'))
            .unwrap_or("")
            .to_string()
    } else {
        stderr
            .lines()
            .find(|line| line.starts_with("error:"))
            .unwrap_or("")
            .to_string()
    };
    json!({
        "class": "config",
        "command": "feedback rate",
        "exit": exit_code(&output),
        "message": message,
    })
}

fn feedback_rating(temp: &Path) -> Value {
    let output = run(temp, &["feedback", "rate", "public-run", "9"]);
    let stderr = normalize(&output.stderr);
    let message = stderr
        .lines()
        .find(|line| line.starts_with("error:"))
        .unwrap_or("")
        .to_string();
    json!({
        "class": "config",
        "command": "feedback rate",
        "exit": exit_code(&output),
        "message": message,
    })
}

fn unknown_command(temp: &Path) -> Value {
    let output = run(temp, &["definitely-not-a-command"]);
    json!({
        "class": "usage",
        "exit": exit_code(&output),
        "stderr_nonempty": !output.stderr.is_empty(),
        "stdout_empty": output.stdout.is_empty(),
    })
}

fn version_shape(temp: &Path) -> Value {
    let output = run(temp, &["--version"]);
    let stdout = normalize(&output.stdout);
    let lines: Vec<&str> = stdout.lines().filter(|line| !line.is_empty()).collect();
    let first = lines.first().copied().unwrap_or("");
    let program = first.split_whitespace().next().unwrap_or("");
    let program = if program == "vyane" || program.ends_with("vyane.exe") {
        "vyane"
    } else {
        "other"
    };
    json!({
        "exit": exit_code(&output),
        "lines": lines.len(),
        "program": program,
    })
}

fn command_names(temp: &Path, args: &[&str]) -> Value {
    let output = run(temp, args);
    let stdout = normalize(&output.stdout);
    json!({
        "commands": nested_command_names(&stdout),
        "exit": exit_code(&output),
    })
}

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
        let name = line.split_whitespace().next().expect("command row");
        if name != "help" {
            names.push(name);
        }
    }
    assert!(!names.is_empty(), "help listed no commands");
    names
}

fn run(temp: &Path, args: &[&str]) -> std::process::Output {
    fs::create_dir_all(temp.join("tmp")).expect("tmp");
    fs::create_dir_all(temp.join("data-dir")).expect("data dir");
    let mut command = Command::cargo_bin("vyane").expect("vyane binary");
    command.env_clear();
    for key in ["SystemRoot", "WINDIR", "COMSPEC"] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    if let Some(root) = std::env::var_os("SystemRoot") {
        let mut path = std::path::PathBuf::from(&root);
        path.push("System32");
        command.env("PATH", path);
    }
    command
        .env("HOME", temp)
        .env("USERPROFILE", temp)
        .env("XDG_CONFIG_HOME", temp.join("config"))
        .env("XDG_DATA_HOME", temp.join("data"))
        .env("XDG_CACHE_HOME", temp.join("cache"))
        .env("VYANE_DATA_DIR", temp.join("data-dir"))
        .env("TMP", temp.join("tmp"))
        .env("TEMP", temp.join("tmp"))
        .env("TMPDIR", temp.join("tmp"))
        .env("NO_COLOR", "1")
        .env("TERM", "dumb")
        .env("COLUMNS", "80")
        .env("LC_ALL", "C")
        .current_dir(temp)
        .timeout(Duration::from_secs(30))
        .args(args);
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile);
    }
    command.output().expect("run vyane")
}

fn normalize(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).replace("\r\n", "\n")
}

fn exit_code(output: &std::process::Output) -> i32 {
    output.status.code().expect("exit code")
}
