#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;
use tempfile::tempdir;
use vyane_core::{
    ErrorKind, NativeExecutionAuthority, NativeSideEffect, PinnedWorkdir, VyaneError,
};
use vyane_harness::native::{
    NativeReadPolicy, PermissionEffect, PermissionPolicy, ToolCall, ToolContext,
    ToolInvocationStatus, read_only_permission_policy, read_only_tool_definitions,
    read_only_tool_registry, read_only_tool_registry_with_policy,
};

#[derive(Default)]
struct RecordingAuthority {
    effects: Mutex<Vec<NativeSideEffect>>,
    fail_at: Option<usize>,
}

impl RecordingAuthority {
    fn failing_at(call: usize) -> Self {
        Self {
            effects: Mutex::new(Vec::new()),
            fail_at: Some(call),
        }
    }

    fn effects(&self) -> Vec<NativeSideEffect> {
        self.effects.lock().expect("effects lock").clone()
    }
}

#[async_trait]
impl NativeExecutionAuthority for RecordingAuthority {
    async fn revalidate(&self, effect: NativeSideEffect) -> vyane_core::Result<()> {
        let mut effects = self.effects.lock().expect("effects lock");
        effects.push(effect);
        if self.fail_at == Some(effects.len()) {
            return Err(VyaneError::new(ErrorKind::Auth, "test authority revoked"));
        }
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

#[tokio::test]
async fn read_file_uses_pinned_root_and_revalidates_at_open() {
    let parent = tempdir().expect("parent");
    let admitted = parent.path().join("workspace");
    std::fs::create_dir(&admitted).expect("workspace");
    std::fs::write(admitted.join("note.txt"), "original\n").expect("seed");
    let context = context(&admitted);

    let moved = parent.path().join("moved-workspace");
    std::fs::rename(&admitted, &moved).expect("move admitted directory");
    std::fs::create_dir(&admitted).expect("replacement workspace");
    std::fs::write(admitted.join("note.txt"), "replacement\n").expect("replacement file");

    let registry = read_only_tool_registry().expect("registry");
    let authority = RecordingAuthority::default();
    let invocation = registry
        .execute_authorized(
            call("read_file", args(&[("path", json!("note.txt"))])),
            &context,
            &PermissionPolicy::allow_by_default(),
            &authority,
            1,
            1,
        )
        .await
        .expect("authorized invocation");

    assert_eq!(invocation.status, ToolInvocationStatus::Executed);
    assert_eq!(invocation.output, "original\n");
    assert_eq!(
        authority.effects(),
        vec![
            NativeSideEffect::ToolOperation {
                turn: 1,
                ordinal: 1
            },
            NativeSideEffect::ToolOperation {
                turn: 1,
                ordinal: 1
            },
            NativeSideEffect::ToolOperation {
                turn: 1,
                ordinal: 1
            }
        ]
    );
}

#[tokio::test]
async fn every_nested_component_open_revalidates_live_authority() {
    let root = tempdir().expect("root");
    std::fs::create_dir_all(root.path().join("a/b")).expect("nested");
    std::fs::write(root.path().join("a/b/note.txt"), "nested\n").expect("note");
    let authority = RecordingAuthority::default();

    let invocation = read_only_tool_registry()
        .expect("registry")
        .execute_authorized(
            call("read_file", args(&[("path", json!("a/b/note.txt"))])),
            &context(root.path()),
            &PermissionPolicy::allow_by_default(),
            &authority,
            5,
            1,
        )
        .await
        .expect("authorized invocation");

    assert_eq!(invocation.status, ToolInvocationStatus::Executed);
    // Registry dispatch + a + b + O_PATH note.txt + exact read-open.
    assert_eq!(authority.effects().len(), 5);
}

#[tokio::test]
async fn read_file_rejects_escape_absolute_and_symlink_paths() {
    let root = tempdir().expect("root");
    let outside = tempdir().expect("outside");
    std::fs::write(outside.path().join("secret.txt"), "outside").expect("outside file");
    std::fs::create_dir(root.path().join(".git")).expect("git");
    std::fs::write(root.path().join(".git/config"), "credential").expect("git config");
    std::os::unix::fs::symlink(
        outside.path().join("secret.txt"),
        root.path().join("escape.txt"),
    )
    .expect("symlink");

    let registry = read_only_tool_registry().expect("registry");
    let policy = PermissionPolicy::allow_by_default();
    for path in ["../secret.txt", "escape.txt", "/etc/passwd"] {
        let invocation = registry
            .execute_authorized(
                call("read_file", args(&[("path", json!(path))])),
                &context(root.path()),
                &policy,
                &RecordingAuthority::default(),
                2,
                1,
            )
            .await
            .expect("closed tool error");
        assert_eq!(invocation.status, ToolInvocationStatus::ToolError);
        assert!(!invocation.output.contains("outside"));
        assert!(!invocation.output.contains("credential"));
    }

    let admitted_dotfile = registry
        .execute_authorized(
            call("read_file", args(&[("path", json!(".git/config"))])),
            &context(root.path()),
            &policy,
            &RecordingAuthority::default(),
            2,
            1,
        )
        .await
        .expect("workspace dotfile");
    assert_eq!(admitted_dotfile.status, ToolInvocationStatus::Executed);
    assert_eq!(admitted_dotfile.output, "credential");
}

#[tokio::test]
async fn trusted_tools_reject_unknown_arguments() {
    let root = tempdir().expect("root");
    std::fs::write(root.path().join("note.txt"), "not disclosed").expect("note");

    let invocation = read_only_tool_registry()
        .expect("registry")
        .execute_authorized(
            call(
                "read_file",
                args(&[
                    ("path", json!("note.txt")),
                    ("unexpected", json!("ignored?")),
                ]),
            ),
            &context(root.path()),
            &PermissionPolicy::allow_by_default(),
            &RecordingAuthority::default(),
            2,
            1,
        )
        .await
        .expect("closed tool result");

    assert_eq!(invocation.status, ToolInvocationStatus::ToolError);
    assert!(!invocation.output.contains("not disclosed"));
}

#[tokio::test]
async fn special_files_are_classified_without_blocking_or_reading() {
    use rustix::fs::{CWD, Mode, mkfifoat};

    let root = tempdir().expect("root");
    mkfifoat(
        CWD,
        root.path().join("model-controlled-pipe"),
        Mode::RUSR | Mode::WUSR,
    )
    .expect("fifo");

    let invocation = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        read_only_tool_registry()
            .expect("registry")
            .execute_authorized(
                call(
                    "read_file",
                    args(&[("path", json!("model-controlled-pipe"))]),
                ),
                &context(root.path()),
                &PermissionPolicy::allow_by_default(),
                &RecordingAuthority::default(),
                6,
                1,
            ),
    )
    .await
    .expect("special file classification must not block")
    .expect("closed tool result");

    assert_eq!(invocation.status, ToolInvocationStatus::ToolError);
}

#[tokio::test]
async fn search_is_deterministic_bounded_and_honors_exclusions() {
    let root = tempdir().expect("root");
    std::fs::create_dir_all(root.path().join("b")).expect("b");
    std::fs::create_dir_all(root.path().join("a")).expect("a");
    std::fs::create_dir_all(root.path().join(".git")).expect("git");
    std::fs::write(root.path().join("b/two.txt"), "needle two\n").expect("two");
    std::fs::write(root.path().join("a/one.txt"), "needle one\nneedle again\n").expect("one");
    std::fs::write(root.path().join("z.txt"), "needle root\n").expect("root file");
    std::fs::write(root.path().join(".env"), "needle password\n").expect("env");
    std::fs::write(root.path().join(".git/config"), "needle token\n").expect("git config");

    let authority = Arc::new(RecordingAuthority::default());
    let invocation = read_only_tool_registry_with_policy(NativeReadPolicy::excluding(vec![
        ".env*".into(),
        ".git".into(),
    ]))
    .expect("configured registry")
    .execute_authorized(
        call(
            "search_files",
            args(&[("query", json!("needle")), ("max_results", json!(2))]),
        ),
        &context(root.path()),
        &PermissionPolicy::allow_by_default(),
        authority.as_ref(),
        3,
        1,
    )
    .await
    .expect("search");

    assert_eq!(invocation.status, ToolInvocationStatus::Executed);
    assert_eq!(
        invocation.output,
        "a/one.txt:1:needle one\na/one.txt:2:needle again"
    );
    assert!(!invocation.output.contains("password"));
    assert!(!invocation.output.contains("token"));
    assert!(authority.effects().len() >= 4);
}

#[tokio::test]
async fn authority_revocation_at_file_open_escapes_as_outer_error() {
    let root = tempdir().expect("root");
    std::fs::write(root.path().join("note.txt"), "not disclosed").expect("note");
    let authority = RecordingAuthority::failing_at(2);

    let error = read_only_tool_registry()
        .expect("registry")
        .execute_authorized(
            call("read_file", args(&[("path", json!("note.txt"))])),
            &context(root.path()),
            &PermissionPolicy::allow_by_default(),
            &authority,
            4,
            1,
        )
        .await
        .expect_err("revocation must escape");

    assert_eq!(error.kind, ErrorKind::Auth);
    assert_eq!(authority.effects().len(), 2);
}

#[tokio::test]
async fn trusted_tools_fail_closed_without_authorized_entry() {
    let root = tempdir().expect("root");
    std::fs::write(root.path().join("note.txt"), "not disclosed").expect("note");
    let invocation = read_only_tool_registry()
        .expect("registry")
        .execute(
            call("read_file", args(&[("path", json!("note.txt"))])),
            &context(root.path()),
            &PermissionPolicy::allow_by_default(),
        )
        .await;

    assert_eq!(invocation.status, ToolInvocationStatus::ToolError);
    assert!(!invocation.output.contains("not disclosed"));
}

#[tokio::test]
async fn admitted_workspace_dotfiles_are_readable_by_default() {
    let root = tempdir().expect("root");
    std::fs::write(
        root.path().join(".npmrc"),
        "//registry/:_authToken=validation-secret",
    )
    .expect("credential");

    let invocation = read_only_tool_registry()
        .expect("registry")
        .execute_authorized(
            call("read_file", args(&[("path", json!(".npmrc"))])),
            &context(root.path()),
            &read_only_permission_policy().expect("policy"),
            &RecordingAuthority::default(),
            7,
            1,
        )
        .await
        .expect("authorized invocation");

    assert_eq!(invocation.status, ToolInvocationStatus::Executed);
    assert!(invocation.output.contains("validation-secret"));
}

#[tokio::test]
async fn configured_exclusions_apply_to_reads_and_recursive_search() {
    let root = tempdir().expect("root");
    std::fs::create_dir(root.path().join("private")).expect("private");
    std::fs::write(root.path().join("private/token.txt"), "needle private").expect("private file");
    std::fs::write(root.path().join("public.txt"), "needle public").expect("public file");
    let registry =
        read_only_tool_registry_with_policy(NativeReadPolicy::excluding(vec!["private".into()]))
            .expect("configured registry");

    let read = registry
        .execute_authorized(
            call("read_file", args(&[("path", json!("private/token.txt"))])),
            &context(root.path()),
            &read_only_permission_policy().expect("policy"),
            &RecordingAuthority::default(),
            8,
            1,
        )
        .await
        .expect("closed denial");
    assert_eq!(read.status, ToolInvocationStatus::ToolError);
    assert!(!read.output.contains("private"));

    let search = registry
        .execute_authorized(
            call("search_files", args(&[("query", json!("needle"))])),
            &context(root.path()),
            &read_only_permission_policy().expect("policy"),
            &RecordingAuthority::default(),
            8,
            2,
        )
        .await
        .expect("search");
    assert_eq!(search.status, ToolInvocationStatus::Executed);
    assert_eq!(search.output, "public.txt:1:needle public");
}

#[test]
fn malformed_or_unbounded_exclusions_are_rejected() {
    for pattern in [
        "../outside",
        r"..\outside",
        "/absolute",
        "private/",
        "private//nested",
        r"private\",
        "[",
    ] {
        assert!(
            NativeReadPolicy::excluding(vec![pattern.into()])
                .validate()
                .is_err()
        );
    }
    assert!(
        NativeReadPolicy::excluding(vec!["private".into(); 129])
            .validate()
            .is_err()
    );
}

#[test]
fn advertised_definitions_exactly_match_registry() {
    let definitions = read_only_tool_definitions();
    let registry = read_only_tool_registry().expect("registry");
    assert_eq!(
        definitions
            .iter()
            .map(|definition| definition.name.as_str())
            .collect::<Vec<_>>(),
        registry.names().collect::<Vec<_>>()
    );
}

#[test]
fn production_policy_allows_only_the_advertised_read_tools() {
    let policy = read_only_permission_policy().expect("policy");
    let root = tempdir().expect("root");
    let context = context(root.path());

    assert_eq!(
        policy
            .decide(
                &call("read_file", args(&[("path", json!("note.txt"))])),
                &context
            )
            .effect,
        PermissionEffect::Allow
    );
    assert_eq!(
        policy
            .decide(
                &call("search_files", args(&[("query", json!("needle"))])),
                &context
            )
            .effect,
        PermissionEffect::Allow
    );
    assert_eq!(
        policy
            .decide(&call("future_tool", BTreeMap::new()), &context)
            .effect,
        PermissionEffect::Deny
    );
}
