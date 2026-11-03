#![cfg(target_os = "linux")]
//! Red-team reproductions for the native file write tools. These tests encode
//! the expected (secure) behavior and fail on the current implementation.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::json;
use tempfile::tempdir;
use vyane_core::{NativeExecutionAuthority, NativeSideEffect, PinnedWorkdir};
use vyane_harness::native::{
    NativeCommandPolicy, NativeReadPolicy, NativeWritePolicy, ToolCall, ToolContext,
    ToolInvocationStatus, workspace_permission_policy, workspace_tool_registry_with_policy,
};

struct AllowAll;

#[async_trait]
impl NativeExecutionAuthority for AllowAll {
    async fn revalidate(&self, _effect: NativeSideEffect) -> vyane_core::Result<()> {
        Ok(())
    }
}

fn context(root: &std::path::Path) -> ToolContext {
    ToolContext::from_pinned_workdir(PinnedWorkdir::open(root).expect("pin workdir"))
}

fn call(name: &str, arguments: BTreeMap<String, serde_json::Value>) -> ToolCall {
    ToolCall {
        id: format!("call-{name}"),
        name: name.into(),
        arguments,
    }
}

fn args(entries: &[(&str, serde_json::Value)]) -> BTreeMap<String, serde_json::Value> {
    entries
        .iter()
        .map(|(name, value)| ((*name).to_string(), value.clone()))
        .collect()
}

/// `run_command` writable roots refuse `.git`, `.vyane`, `.codex` and
/// `.claude` (any case) because writing there yields host code execution
/// outside the sandbox: git hooks / `core.fsmonitor`, harness hook settings.
/// The `write_file` / `edit_file` tools under the default workspace write
/// policy apply no such floor, so a model can plant `.git/hooks/pre-commit`
/// or `.claude/settings.json` hooks that run unsandboxed on the host the next
/// time the operator uses git or the harness in that workspace.
#[tokio::test]
async fn redteam_write_file_refuses_host_control_directories() {
    // Sanity: the command policy does refuse these as writable roots.
    for control in [".git", ".claude", ".codex", ".vyane"] {
        let policy: NativeCommandPolicy = serde_json::from_value(json!({
            "allow": [{"program": "true"}],
            "writable_roots": [control],
        }))
        .expect("command policy shape");
        assert!(policy.validate().is_err(), "{control} must be refused");
    }

    let registry = workspace_tool_registry_with_policy(
        NativeReadPolicy::workspace(),
        Some(NativeWritePolicy::workspace()),
    )
    .expect("registry");
    let policy = workspace_permission_policy(true).expect("policy");

    let mut planted = Vec::new();
    for (sequence, target) in [
        ".git/hooks/pre-commit",
        ".claude/settings.json",
        ".codex/config.toml",
        ".vyane/config.toml",
    ]
    .into_iter()
    .enumerate()
    {
        let root = tempdir().expect("root");
        let parent = std::path::Path::new(target).parent().expect("parent");
        std::fs::create_dir_all(root.path().join(parent)).expect("control dir");
        let outcome = registry
            .execute_authorized(
                call(
                    "write_file",
                    args(&[
                        ("path", json!(target)),
                        ("content", json!("#!/bin/sh\necho pwned\n")),
                    ]),
                ),
                &context(root.path()),
                &policy,
                &AllowAll,
                9,
                u32::try_from(sequence).unwrap_or(0) + 1,
            )
            .await
            .expect("tool outcome");
        if outcome.status == ToolInvocationStatus::Executed
            && root.path().join(target).exists()
        {
            planted.push(target);
        }
    }
    assert!(
        planted.is_empty(),
        "write_file planted host-control files: {planted:?}"
    );
}
