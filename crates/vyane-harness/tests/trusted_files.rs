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
    NativeReadPolicy, NativeWritePolicy, PermissionEffect, PermissionPolicy, ToolCall, ToolContext,
    ToolInvocationStatus, read_only_permission_policy, read_only_tool_definitions,
    read_only_tool_registry, read_only_tool_registry_with_policy, validate_read_only_host,
    workspace_permission_policy, workspace_tool_definitions, workspace_tool_registry_with_policy,
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

struct MutatingAuthority {
    effects: Mutex<Vec<NativeSideEffect>>,
    mutate_at: usize,
    path: std::path::PathBuf,
    content: &'static str,
}

struct ReplacingParentAuthority {
    original: std::path::PathBuf,
    moved: std::path::PathBuf,
    replaced: Mutex<bool>,
}

struct ReplacingStageAuthority {
    parent: std::path::PathBuf,
    replacement: std::path::PathBuf,
    replaced: Mutex<bool>,
}

#[async_trait]
impl NativeExecutionAuthority for ReplacingStageAuthority {
    async fn revalidate(&self, _effect: NativeSideEffect) -> vyane_core::Result<()> {
        let mut replaced = self.replaced.lock().expect("replacement lock");
        if !*replaced {
            let temporary = std::fs::read_dir(&self.parent)
                .expect("parent entries")
                .filter_map(Result::ok)
                .find(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".vyane-write-")
                })
                .map(|entry| entry.path());
            if let Some(temporary) = temporary {
                std::fs::remove_file(&temporary).expect("remove staged link");
                std::os::unix::fs::symlink(&self.replacement, &temporary)
                    .expect("replace staged link");
                *replaced = true;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl NativeExecutionAuthority for ReplacingParentAuthority {
    async fn revalidate(&self, _effect: NativeSideEffect) -> vyane_core::Result<()> {
        let mut replaced = self.replaced.lock().expect("replacement lock");
        if !*replaced
            && std::fs::read_dir(&self.original)
                .expect("parent entries")
                .any(|entry| {
                    entry
                        .expect("entry")
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".vyane-write-")
                })
        {
            std::fs::rename(&self.original, &self.moved).expect("move staged parent");
            std::fs::create_dir(&self.original).expect("replacement parent");
            *replaced = true;
        }
        Ok(())
    }
}

#[async_trait]
impl NativeExecutionAuthority for MutatingAuthority {
    async fn revalidate(&self, effect: NativeSideEffect) -> vyane_core::Result<()> {
        let should_mutate = {
            let mut effects = self.effects.lock().expect("effects lock");
            effects.push(effect);
            effects.len() == self.mutate_at
        };
        if should_mutate {
            std::fs::write(&self.path, self.content).expect("mutate source");
        }
        Ok(())
    }
}

fn context(root: &std::path::Path) -> ToolContext {
    ToolContext::from_pinned_workdir(PinnedWorkdir::open(root).expect("pin workdir"))
}

#[test]
fn admitted_host_supports_the_required_openat2_confinement() {
    let root = tempdir().expect("root");
    let pinned = PinnedWorkdir::open(root.path()).expect("pin workdir");
    validate_read_only_host(&pinned).expect("openat2 confinement");
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
async fn search_classifies_before_opening_a_candidate_for_reading_once() {
    let root = tempdir().expect("root");
    std::fs::write(root.path().join("note.txt"), "needle\n").expect("note");
    let authority = RecordingAuthority::default();

    let invocation = read_only_tool_registry()
        .expect("registry")
        .execute_authorized(
            call("search_files", args(&[("query", json!("needle"))])),
            &context(root.path()),
            &PermissionPolicy::allow_by_default(),
            &authority,
            3,
            1,
        )
        .await
        .expect("search");

    assert_eq!(invocation.status, ToolInvocationStatus::Executed);
    assert_eq!(invocation.output, "note.txt:1:needle");
    // Registry dispatch + directory enumeration + discovery O_PATH +
    // content O_PATH + exact procfd read-open.
    assert_eq!(authority.effects().len(), 5);
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
        "private/./nested",
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
    assert!(
        NativeWritePolicy::excluding(vec!["../outside".into()])
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

#[test]
fn write_tools_require_an_explicit_write_policy() {
    let read_only = workspace_tool_registry_with_policy(NativeReadPolicy::workspace(), None)
        .expect("read-only registry");
    assert_eq!(
        read_only.names().collect::<Vec<_>>(),
        vec!["read_file", "search_files"]
    );

    let writable = workspace_tool_registry_with_policy(
        NativeReadPolicy::workspace(),
        Some(NativeWritePolicy::workspace()),
    )
    .expect("writable registry");
    assert_eq!(
        writable.names().collect::<Vec<_>>(),
        vec!["edit_file", "read_file", "search_files", "write_file"]
    );
    assert_eq!(
        workspace_tool_definitions(true)
            .iter()
            .map(|definition| definition.name.as_str())
            .collect::<std::collections::BTreeSet<_>>(),
        writable.names().collect::<std::collections::BTreeSet<_>>()
    );
}

#[tokio::test]
async fn write_file_atomically_creates_but_never_overwrites() {
    let root = tempdir().expect("root");
    let registry = workspace_tool_registry_with_policy(
        NativeReadPolicy::workspace(),
        Some(NativeWritePolicy::workspace()),
    )
    .expect("registry");
    let policy = workspace_permission_policy(true).expect("policy");

    let created = registry
        .execute_authorized(
            call(
                "write_file",
                args(&[("path", json!("new.txt")), ("content", json!("first\n"))]),
            ),
            &context(root.path()),
            &policy,
            &RecordingAuthority::default(),
            9,
            1,
        )
        .await
        .expect("create");
    assert_eq!(created.status, ToolInvocationStatus::Executed);
    assert_eq!(
        std::fs::read_to_string(root.path().join("new.txt")).expect("created content"),
        "first\n"
    );

    let conflict = registry
        .execute_authorized(
            call(
                "write_file",
                args(&[("path", json!("new.txt")), ("content", json!("second\n"))]),
            ),
            &context(root.path()),
            &policy,
            &RecordingAuthority::default(),
            9,
            2,
        )
        .await
        .expect("conflict");
    assert_eq!(conflict.status, ToolInvocationStatus::ToolError);
    assert_eq!(
        std::fs::read_to_string(root.path().join("new.txt")).expect("preserved content"),
        "first\n"
    );

    let raced_path = root.path().join("raced.txt");
    let raced = registry
        .execute_authorized(
            call(
                "write_file",
                args(&[("path", json!("raced.txt")), ("content", json!("model\n"))]),
            ),
            &context(root.path()),
            &policy,
            &MutatingAuthority {
                effects: Mutex::new(Vec::new()),
                mutate_at: 3,
                path: raced_path.clone(),
                content: "external\n",
            },
            9,
            3,
        )
        .await
        .expect("concurrent create");
    assert_eq!(raced.status, ToolInvocationStatus::ToolError);
    assert_eq!(
        std::fs::read_to_string(raced_path).expect("raced content"),
        "external\n"
    );
    assert!(
        std::fs::read_dir(root.path())
            .expect("entries")
            .all(|entry| !entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .starts_with(".vyane-write-"))
    );
}

#[tokio::test]
async fn edit_file_composes_guarded_text_edit_and_preserves_mode() {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let root = tempdir().expect("root");
    let path = root.path().join("script.sh");
    std::fs::write(&path, "echo old\n").expect("seed");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o7750))
        .expect("executable with special bits");
    let original_metadata = std::fs::metadata(&path).expect("original metadata");
    let registry = workspace_tool_registry_with_policy(
        NativeReadPolicy::workspace(),
        Some(NativeWritePolicy::workspace()),
    )
    .expect("registry");

    let invocation = registry
        .execute_authorized(
            call(
                "edit_file",
                args(&[
                    ("path", json!("script.sh")),
                    ("old_string", json!("old")),
                    ("new_string", json!("new")),
                ]),
            ),
            &context(root.path()),
            &workspace_permission_policy(true).expect("policy"),
            &RecordingAuthority::default(),
            10,
            1,
        )
        .await
        .expect("edit");

    assert_eq!(invocation.status, ToolInvocationStatus::Executed);
    assert_eq!(
        std::fs::read_to_string(&path).expect("edited"),
        "echo new\n"
    );
    let edited_metadata = std::fs::metadata(&path).expect("edited metadata");
    assert_eq!(edited_metadata.permissions().mode() & 0o7777, 0o7750);
    assert_eq!(edited_metadata.uid(), original_metadata.uid());
    assert_eq!(edited_metadata.gid(), original_metadata.gid());
}

#[tokio::test]
async fn write_file_does_not_require_directory_read_permission() {
    use std::os::unix::fs::PermissionsExt as _;

    let root = tempdir().expect("root");
    let directory = root.path().join("write-only");
    std::fs::create_dir(&directory).expect("directory");
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o300))
        .expect("write and search only");
    let registry = workspace_tool_registry_with_policy(
        NativeReadPolicy::workspace(),
        Some(NativeWritePolicy::workspace()),
    )
    .expect("registry");

    let invocation = registry
        .execute_authorized(
            call(
                "write_file",
                args(&[
                    ("path", json!("write-only/created.txt")),
                    ("content", json!("created\n")),
                ]),
            ),
            &context(root.path()),
            &workspace_permission_policy(true).expect("policy"),
            &RecordingAuthority::default(),
            10,
            2,
        )
        .await
        .expect("write");

    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
        .expect("restore directory permissions");
    assert_eq!(invocation.status, ToolInvocationStatus::Executed);
    assert_eq!(
        std::fs::read_to_string(directory.join("created.txt")).expect("created"),
        "created\n"
    );
}

#[tokio::test]
async fn write_and_edit_reject_parent_replacement_before_publication() {
    let root = tempdir().expect("root");
    let registry = workspace_tool_registry_with_policy(
        NativeReadPolicy::workspace(),
        Some(NativeWritePolicy::workspace()),
    )
    .expect("registry");
    let policy = workspace_permission_policy(true).expect("policy");

    let write_parent = root.path().join("write-parent");
    let moved_write_parent = root.path().join("moved-write-parent");
    std::fs::create_dir(&write_parent).expect("write parent");
    let write = registry
        .execute_authorized(
            call(
                "write_file",
                args(&[
                    ("path", json!("write-parent/created.txt")),
                    ("content", json!("created\n")),
                ]),
            ),
            &context(root.path()),
            &policy,
            &ReplacingParentAuthority {
                original: write_parent.clone(),
                moved: moved_write_parent.clone(),
                replaced: Mutex::new(false),
            },
            10,
            3,
        )
        .await
        .expect("write result");
    assert_eq!(write.status, ToolInvocationStatus::ToolError);
    assert!(!write_parent.join("created.txt").exists());
    assert!(!moved_write_parent.join("created.txt").exists());

    let edit_parent = root.path().join("edit-parent");
    let moved_edit_parent = root.path().join("moved-edit-parent");
    std::fs::create_dir(&edit_parent).expect("edit parent");
    std::fs::write(edit_parent.join("note.txt"), "old\n").expect("source");
    let edit = registry
        .execute_authorized(
            call(
                "edit_file",
                args(&[
                    ("path", json!("edit-parent/note.txt")),
                    ("old_string", json!("old")),
                    ("new_string", json!("new")),
                ]),
            ),
            &context(root.path()),
            &policy,
            &ReplacingParentAuthority {
                original: edit_parent.clone(),
                moved: moved_edit_parent.clone(),
                replaced: Mutex::new(false),
            },
            10,
            4,
        )
        .await
        .expect("edit result");
    assert_eq!(edit.status, ToolInvocationStatus::ToolError);
    assert!(!edit_parent.join("note.txt").exists());
    assert_eq!(
        std::fs::read_to_string(moved_edit_parent.join("note.txt")).expect("unchanged source"),
        "old\n"
    );
    for directory in [&moved_write_parent, &moved_edit_parent] {
        assert!(std::fs::read_dir(directory).expect("entries").all(|entry| {
            !entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .starts_with(".vyane-write-")
        }));
    }
}

#[tokio::test]
async fn write_and_edit_bind_publication_to_the_staged_inode() {
    let root = tempdir().expect("root");
    let registry = workspace_tool_registry_with_policy(
        NativeReadPolicy::workspace(),
        Some(NativeWritePolicy::workspace()),
    )
    .expect("registry");
    let policy = workspace_permission_policy(true).expect("policy");
    let replacement = root.path().join("replacement.txt");
    std::fs::write(&replacement, "attacker\n").expect("replacement");

    let write_parent = root.path().join("write-parent");
    std::fs::create_dir(&write_parent).expect("write parent");
    let write = registry
        .execute_authorized(
            call(
                "write_file",
                args(&[
                    ("path", json!("write-parent/created.txt")),
                    ("content", json!("created\n")),
                ]),
            ),
            &context(root.path()),
            &policy,
            &ReplacingStageAuthority {
                parent: write_parent.clone(),
                replacement: replacement.clone(),
                replaced: Mutex::new(false),
            },
            10,
            5,
        )
        .await
        .expect("write result");
    assert_eq!(write.status, ToolInvocationStatus::ToolError);
    assert!(!write_parent.join("created.txt").exists());

    let edit_parent = root.path().join("edit-parent");
    std::fs::create_dir(&edit_parent).expect("edit parent");
    std::fs::write(edit_parent.join("note.txt"), "old\n").expect("source");
    let edit = registry
        .execute_authorized(
            call(
                "edit_file",
                args(&[
                    ("path", json!("edit-parent/note.txt")),
                    ("old_string", json!("old")),
                    ("new_string", json!("new")),
                ]),
            ),
            &context(root.path()),
            &policy,
            &ReplacingStageAuthority {
                parent: edit_parent.clone(),
                replacement: replacement.clone(),
                replaced: Mutex::new(false),
            },
            10,
            6,
        )
        .await
        .expect("edit result");
    assert_eq!(edit.status, ToolInvocationStatus::ToolError);
    assert_eq!(
        std::fs::read_to_string(edit_parent.join("note.txt")).expect("source"),
        "old\n"
    );
    assert_eq!(
        std::fs::read_to_string(&replacement).expect("replacement"),
        "attacker\n"
    );
    for directory in [&write_parent, &edit_parent] {
        assert!(std::fs::read_dir(directory).expect("entries").all(|entry| {
            !entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .starts_with(".vyane-write-")
        }));
    }
}

#[tokio::test]
async fn edit_rejects_extended_security_metadata_it_cannot_preserve() {
    let root = tempdir().expect("root");
    let path = root.path().join("note.txt");
    std::fs::write(&path, "old\n").expect("source");
    let source = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("source descriptor");
    rustix::fs::fsetxattr(
        &source,
        "user.vyane-test",
        b"security-marker",
        rustix::fs::XattrFlags::CREATE,
    )
    .expect("set xattr");
    let registry = workspace_tool_registry_with_policy(
        NativeReadPolicy::workspace(),
        Some(NativeWritePolicy::workspace()),
    )
    .expect("registry");

    let edit = registry
        .execute_authorized(
            call(
                "edit_file",
                args(&[
                    ("path", json!("note.txt")),
                    ("old_string", json!("old")),
                    ("new_string", json!("new")),
                ]),
            ),
            &context(root.path()),
            &workspace_permission_policy(true).expect("policy"),
            &RecordingAuthority::default(),
            10,
            7,
        )
        .await
        .expect("edit result");

    assert_eq!(edit.status, ToolInvocationStatus::ToolError);
    assert_eq!(std::fs::read_to_string(&path).expect("source"), "old\n");
}

#[tokio::test]
async fn ambiguous_edit_and_source_drift_fail_before_publication() {
    let root = tempdir().expect("root");
    let path = root.path().join("note.txt");
    std::fs::write(&path, "same\nsame\n").expect("seed");
    let registry = workspace_tool_registry_with_policy(
        NativeReadPolicy::workspace(),
        Some(NativeWritePolicy::workspace()),
    )
    .expect("registry");
    let policy = workspace_permission_policy(true).expect("policy");
    let ambiguous = registry
        .execute_authorized(
            call(
                "edit_file",
                args(&[
                    ("path", json!("note.txt")),
                    ("old_string", json!("same")),
                    ("new_string", json!("changed")),
                ]),
            ),
            &context(root.path()),
            &policy,
            &RecordingAuthority::default(),
            11,
            1,
        )
        .await
        .expect("ambiguous");
    assert_eq!(ambiguous.status, ToolInvocationStatus::ToolError);
    assert_eq!(
        std::fs::read_to_string(&path).expect("unchanged"),
        "same\nsame\n"
    );

    std::fs::write(&path, "old\n").expect("reset");
    let drift = registry
        .execute_authorized(
            call(
                "edit_file",
                args(&[
                    ("path", json!("note.txt")),
                    ("old_string", json!("old")),
                    ("new_string", json!("new")),
                ]),
            ),
            &context(root.path()),
            &policy,
            &MutatingAuthority {
                effects: Mutex::new(Vec::new()),
                mutate_at: 5,
                path: path.clone(),
                content: "external\n",
            },
            11,
            2,
        )
        .await
        .expect("drift result");
    assert_eq!(drift.status, ToolInvocationStatus::ToolError);
    assert_eq!(
        std::fs::read_to_string(&path).expect("external content preserved"),
        "external\n"
    );
    assert!(
        std::fs::read_dir(root.path())
            .expect("entries")
            .all(|entry| !entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .starts_with(".vyane-write-"))
    );
}

#[tokio::test]
async fn authority_revocation_at_edit_publication_preserves_source() {
    let root = tempdir().expect("root");
    let path = root.path().join("note.txt");
    std::fs::write(&path, "old\n").expect("seed");
    let registry = workspace_tool_registry_with_policy(
        NativeReadPolicy::workspace(),
        Some(NativeWritePolicy::workspace()),
    )
    .expect("registry");
    let authority = RecordingAuthority::failing_at(7);

    let error = registry
        .execute_authorized(
            call(
                "edit_file",
                args(&[
                    ("path", json!("note.txt")),
                    ("old_string", json!("old")),
                    ("new_string", json!("new")),
                ]),
            ),
            &context(root.path()),
            &workspace_permission_policy(true).expect("policy"),
            &authority,
            12,
            1,
        )
        .await
        .expect_err("revoked publication");

    assert_eq!(error.kind, ErrorKind::Auth);
    assert_eq!(std::fs::read_to_string(&path).expect("source"), "old\n");
    assert!(
        std::fs::read_dir(root.path())
            .expect("entries")
            .all(|entry| !entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .starts_with(".vyane-write-"))
    );
}

#[tokio::test]
async fn write_exclusions_do_not_inherit_from_read_or_widen_it() {
    let root = tempdir().expect("root");
    std::fs::write(root.path().join("read-denied.txt"), "old\n").expect("seed");
    let registry = workspace_tool_registry_with_policy(
        NativeReadPolicy::excluding(vec!["read-denied.txt".into()]),
        Some(NativeWritePolicy::excluding(vec![
            "write-denied.txt".into(),
        ])),
    )
    .expect("registry");
    let policy = workspace_permission_policy(true).expect("policy");

    let read_denied_edit = registry
        .execute_authorized(
            call(
                "edit_file",
                args(&[
                    ("path", json!("read-denied.txt")),
                    ("old_string", json!("old")),
                    ("new_string", json!("new")),
                ]),
            ),
            &context(root.path()),
            &policy,
            &RecordingAuthority::default(),
            13,
            1,
        )
        .await
        .expect("read denial");
    assert_eq!(read_denied_edit.status, ToolInvocationStatus::ToolError);

    let write_denied = registry
        .execute_authorized(
            call(
                "write_file",
                args(&[
                    ("path", json!("write-denied.txt")),
                    ("content", json!("blocked")),
                ]),
            ),
            &context(root.path()),
            &policy,
            &RecordingAuthority::default(),
            13,
            2,
        )
        .await
        .expect("write denial");
    assert_eq!(write_denied.status, ToolInvocationStatus::ToolError);
    assert!(!root.path().join("write-denied.txt").exists());

    let allowed = registry
        .execute_authorized(
            call(
                "write_file",
                args(&[("path", json!("allowed.txt")), ("content", json!("ok"))]),
            ),
            &context(root.path()),
            &policy,
            &RecordingAuthority::default(),
            13,
            3,
        )
        .await
        .expect("allowed write");
    assert_eq!(allowed.status, ToolInvocationStatus::Executed);
}

#[tokio::test]
async fn write_tools_reject_traversal_absolute_and_symlink_parent_paths() {
    let root = tempdir().expect("root");
    let outside = tempdir().expect("outside");
    std::os::unix::fs::symlink(outside.path(), root.path().join("escape"))
        .expect("directory symlink");
    let registry = workspace_tool_registry_with_policy(
        NativeReadPolicy::workspace(),
        Some(NativeWritePolicy::workspace()),
    )
    .expect("registry");
    let policy = workspace_permission_policy(true).expect("policy");

    for path in ["../outside.txt", "/tmp/absolute.txt", "escape/linked.txt"] {
        let invocation = registry
            .execute_authorized(
                call(
                    "write_file",
                    args(&[("path", json!(path)), ("content", json!("blocked"))]),
                ),
                &context(root.path()),
                &policy,
                &RecordingAuthority::default(),
                14,
                1,
            )
            .await
            .expect("closed write");
        assert_eq!(invocation.status, ToolInvocationStatus::ToolError);
    }
    assert!(!outside.path().join("linked.txt").exists());
}
