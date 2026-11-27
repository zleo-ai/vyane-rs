//! `vyane mcp-smoke` against this binary's stdio MCP server.
//!
//! The sentinel directory is HOME, the XDG roots, and `VYANE_DATA_DIR`. The
//! command must not create or modify anything there.

mod common;

use std::collections::BTreeSet;
use std::fs;
use std::time::Duration;

use assert_cmd::Command;
use serde::Deserialize;

const TOOLS: &[&str] = &[
    "vyane_broadcast",
    "vyane_check",
    "vyane_dispatch",
    "vyane_feedback",
    "vyane_history",
    "vyane_outcomes",
    "vyane_route",
    "vyane_sessions",
    "vyane_workflow_cancel",
    "vyane_workflow_plan",
    "vyane_workflow_status",
    "vyane_workflow_submit",
];

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Success {
    status: String,
    transport: String,
    tool_count: usize,
    tools: Vec<String>,
    elapsed_ms: u64,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Failure {
    status: String,
    transport: String,
    error_type: String,
    error: String,
}

struct Isolated {
    root: tempfile::TempDir,
}

impl Isolated {
    fn new() -> Self {
        let root = common::private_tempdir();
        fs::write(root.path().join("sentinel"), "untouched").expect("sentinel");
        fs::write(root.path().join("bad.toml"), "this is not toml {\n").expect("bad config");
        Self { root }
    }

    fn command(&self) -> Command {
        let mut command = Command::cargo_bin("vyane").expect("vyane binary");
        command
            .env_clear()
            .env("HOME", self.root.path())
            .env("XDG_CONFIG_HOME", self.root.path().join("xdg-config"))
            .env("XDG_DATA_HOME", self.root.path().join("xdg-data"))
            .env("XDG_STATE_HOME", self.root.path().join("xdg-state"))
            .env("VYANE_DATA_DIR", self.root.path().join("vyane-data"))
            .env("NO_COLOR", "1")
            .env("TERM", "dumb")
            .env("LC_ALL", "C")
            .current_dir(self.root.path())
            .timeout(Duration::from_secs(30));
        if let Some(profile_file) = std::env::var_os("LLVM_PROFILE_FILE") {
            command.env("LLVM_PROFILE_FILE", profile_file);
        }
        command
    }

    fn assert_untouched(&self) {
        let entries: BTreeSet<String> = fs::read_dir(self.root.path())
            .expect("sentinel directory")
            .map(|entry| {
                entry
                    .expect("sentinel entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(
            entries,
            BTreeSet::from(["bad.toml".to_string(), "sentinel".to_string()])
        );
        assert_eq!(
            fs::read_to_string(self.root.path().join("sentinel")).expect("sentinel bytes"),
            "untouched"
        );
        assert_eq!(
            fs::read_to_string(self.root.path().join("bad.toml")).expect("bad config bytes"),
            "this is not toml {\n"
        );
        for absent in [
            "vyane-data",
            "xdg-config",
            "xdg-data",
            "xdg-state",
            ".local",
        ] {
            assert!(
                !self.root.path().join(absent).exists(),
                "{absent} was created under {}",
                self.root.path().display()
            );
        }
    }
}

fn stdout_text(output: &std::process::Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout utf-8")
}

#[test]
fn mcp_smoke_json_lists_live_tools_without_touching_home() {
    let isolated = Isolated::new();
    let output = isolated
        .command()
        .args([
            "--config",
            "bad.toml",
            "mcp-smoke",
            "--json",
            "--timeout",
            "10",
            "--tool-limit",
            "2",
        ])
        .output()
        .expect("run mcp-smoke");
    let text = stdout_text(&output);
    assert!(
        output.status.success(),
        "status {:?}\nstdout {text}\nstderr {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Success = serde_json::from_str(&text).expect("success json");
    assert_eq!(payload.status, "success");
    assert_eq!(payload.transport, "stdio");
    assert_eq!(payload.tool_count, TOOLS.len());
    assert_eq!(payload.tools, TOOLS[..2]);
    assert!(
        payload.elapsed_ms < 10_000,
        "elapsed {}",
        payload.elapsed_ms
    );
    assert_eq!(
        text,
        format!(
            "{{\n  \"status\": \"success\",\n  \"transport\": \"stdio\",\n  \"tool_count\": {},\n  \"tools\": [\n    \"vyane_broadcast\",\n    \"vyane_check\"\n  ],\n  \"elapsed_ms\": {}\n}}\n",
            payload.tool_count, payload.elapsed_ms
        )
    );
    isolated.assert_untouched();
}

#[test]
fn mcp_smoke_human_output_lists_sorted_names() {
    let isolated = Isolated::new();
    let output = isolated
        .command()
        .args(["mcp-smoke", "--timeout", "10"])
        .output()
        .expect("run mcp-smoke");
    let text = stdout_text(&output);
    assert!(
        output.status.success(),
        "status {:?}\nstdout {text}\nstderr {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let mut lines = text.lines();
    let first = lines.next().expect("status line");
    let millis = first
        .strip_prefix("MCP stdio smoke OK: 12 tool(s), ")
        .and_then(|rest| rest.strip_suffix("ms"))
        .expect(first);
    assert!(millis.chars().all(|ch| ch.is_ascii_digit()), "{first}");
    assert_eq!(
        lines.next().expect("tool line"),
        format!("  {}", TOOLS.join(", "))
    );
    assert!(lines.next().is_none(), "{text}");
    isolated.assert_untouched();
}

#[test]
fn mcp_smoke_zero_tool_limit_omits_the_name_line() {
    let isolated = Isolated::new();
    let output = isolated
        .command()
        .args(["mcp-smoke", "--tool-limit", "0", "--timeout", "10"])
        .output()
        .expect("run mcp-smoke");
    let text = stdout_text(&output);
    assert!(
        output.status.success(),
        "status {:?}\nstdout {text}\nstderr {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let mut lines = text.lines();
    let first = lines.next().expect("status line");
    assert!(
        first.starts_with("MCP stdio smoke OK: 12 tool(s), ") && first.ends_with("ms"),
        "{first}"
    );
    assert!(lines.next().is_none(), "{text}");
    isolated.assert_untouched();
}

#[test]
fn mcp_smoke_timeout_failure_leaves_home_alone() {
    let isolated = Isolated::new();
    let output = isolated
        .command()
        .args([
            "--config",
            "bad.toml",
            "mcp-smoke",
            "--timeout",
            "0",
            "--json",
        ])
        .output()
        .expect("run mcp-smoke");
    let text = stdout_text(&output);
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout {text}\nstderr {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Failure = serde_json::from_str(&text).expect("failure json");
    assert_eq!(payload.status, "error");
    assert_eq!(payload.transport, "stdio");
    assert_eq!(payload.error_type, "Elapsed");
    assert_eq!(payload.error, "deadline has elapsed");
    assert_eq!(
        text,
        "{\n  \"status\": \"error\",\n  \"transport\": \"stdio\",\n  \"error_type\": \"Elapsed\",\n  \"error\": \"deadline has elapsed\"\n}\n"
    );
    isolated.assert_untouched();

    let plain = isolated
        .command()
        .args(["mcp-smoke", "--timeout", "0"])
        .output()
        .expect("run plain mcp-smoke");
    assert_eq!(plain.status.code(), Some(1));
    assert_eq!(
        stdout_text(&plain),
        "MCP stdio smoke failed: Elapsed: deadline has elapsed\n"
    );
    isolated.assert_untouched();
}
