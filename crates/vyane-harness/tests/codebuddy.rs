//! Fake-process contract for [`vyane_harness::CodebuddyHarness`].
//!
//! The child is always a local script. These tests do not invoke a CodeBuddy
//! install, a model API, or stored credentials.

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use vyane_core::env::{EnvPolicy, InheritMode};
use vyane_core::error::ErrorKind;
use vyane_core::target::{AuthMaterial, AuthStyle, Endpoint, ModelId, Protocol, Sandbox, Secret};
use vyane_core::task::GenParams;
use vyane_core::traits::{Harness, HarnessJob};
use vyane_harness::CodebuddyHarness;

const RESULT: &str = r#"[
  {"type":"message","role":"assistant","providerData":{"model":"kimi-k2.5","rawUsage":{"credit":2.81}}},
  {"type":"result","subtype":"success","is_error":false,"result":"OK","session_id":"e92d4a23-3c7a-4cd5-856f-a08d5ddd30b6","usage":{"input_tokens":24455,"output_tokens":23,"credit":2.81}}
]
"#;

fn job(prompt: &str) -> HarnessJob {
    HarnessJob {
        prompt: prompt.to_string(),
        model: ModelId::new(""),
        protocol: Protocol::OpenaiResponses,
        endpoint: None,
        params: GenParams::default(),
        workdir: None,
        sandbox: Sandbox::ReadOnly,
        resume: None,
        env: EnvPolicy::scrubbed(),
        timeout: None,
        harness_lifecycle_reporter: None,
    }
}

fn shell_quote(value: &Path) -> String {
    format!("'{}'", value.display().to_string().replace('\'', "'\\''"))
}

fn write_script(dir: &TempDir, name: &str, body: &str) -> PathBuf {
    let path = dir.path().join(name);
    fs::write(&path, body).unwrap();
    let mut perms = fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).unwrap();
    path
}

fn read_lines(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

fn parent_env() -> Vec<(String, String)> {
    vec![
        ("PATH".into(), "/usr/bin:/bin".into()),
        ("HOME".into(), "/tmp/codebuddy-home".into()),
        ("CODEBUDDY_API_KEY".into(), "parent-codebuddy-canary".into()),
        ("OTHER_PARENT".into(), "kept-parent".into()),
    ]
}

fn harness(bin: &Path) -> CodebuddyHarness {
    CodebuddyHarness::with_binary_and_parent_env_for_tests(bin.to_string_lossy(), parent_env())
}

fn env_has_key(text: &str, name: &str) -> bool {
    text.lines().any(|line| {
        line.split_once('=')
            .is_some_and(|(key, _)| key.eq_ignore_ascii_case(name))
    })
}

fn capture_script(
    dir: &TempDir,
    stdout_body: &str,
    exit_code: i32,
    before: &str,
    stderr_line: &str,
) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    let stdout_path = dir.path().join("stdout.txt");
    fs::write(&stdout_path, stdout_body).unwrap();
    let argv_file = dir.path().join("argv.txt");
    let env_file = dir.path().join("env.txt");
    let stdin_file = dir.path().join("stdin.txt");
    let stderr = if stderr_line.is_empty() {
        String::new()
    } else {
        format!(
            "printf '%s\\n' {} >&2\n",
            shell_quote(Path::new(stderr_line))
        )
    };
    let before = if before.is_empty() {
        String::new()
    } else {
        format!("{before}\n")
    };
    let body = format!(
        r#"#!/bin/sh
set -eu
: > {argv}
for arg in "$@"; do
  printf '%s\n' "$arg" >> {argv}
done
env | sort > {env}
cat > {stdin}
{before}{stderr}cat {stdout}
exit {exit_code}
"#,
        argv = shell_quote(&argv_file),
        env = shell_quote(&env_file),
        stdin = shell_quote(&stdin_file),
        before = before,
        stderr = stderr,
        stdout = shell_quote(&stdout_path),
        exit_code = exit_code,
    );
    let bin = write_script(dir, "fake-codebuddy", &body);
    (bin, argv_file, env_file, stdin_file)
}

fn endpoint() -> Endpoint {
    Endpoint {
        base_url: "https://codebuddy.example/v1".into(),
        auth: Some(AuthMaterial {
            style: AuthStyle::Bearer,
            secret: Secret::new("endpoint-canary-secret"),
        }),
    }
}

#[tokio::test]
async fn fake_process_maps_json_argv_env_cwd_and_closed_stdin() {
    let dir = TempDir::new().unwrap();
    let workdir = dir.path().join("work");
    fs::create_dir(&workdir).unwrap();
    let workdir = workdir.canonicalize().unwrap();
    let pwd_file = dir.path().join("pwd.txt");
    let before = format!("pwd -P > {}", shell_quote(&pwd_file));
    let (bin, argv_file, env_file, stdin_file) =
        capture_script(&dir, RESULT, 0, &before, "(node:1) Warning: ignored");

    let mut sample = job("hello from codebuddy");
    sample.workdir = Some(workdir.clone());
    sample.endpoint = Some(endpoint());

    let outcome = harness(&bin)
        .run(sample, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(outcome.text, "OK");
    assert_eq!(
        outcome.native_session_id.as_deref(),
        Some("e92d4a23-3c7a-4cd5-856f-a08d5ddd30b6")
    );
    let usage = outcome.usage.unwrap();
    assert_eq!(usage.input_tokens, 24455);
    assert_eq!(usage.output_tokens, 23);
    assert_eq!(usage.reasoning_tokens, None);
    assert_eq!(outcome.exit_code, 0);
    assert!(!outcome.text.contains("2.81"));
    assert!(!outcome.text.contains("kimi-k2.5"));
    assert_eq!(
        PathBuf::from(fs::read_to_string(&pwd_file).unwrap().trim()),
        workdir
    );
    assert!(fs::read(&stdin_file).unwrap().is_empty());
    assert_eq!(
        read_lines(&argv_file),
        vec![
            "-p",
            "hello from codebuddy",
            "--model",
            "deepseek-v4.1-flash",
            "--output-format",
            "json",
            "--setting-sources",
            "user",
        ]
    );
    let child_env = fs::read_to_string(&env_file).unwrap();
    assert!(child_env.contains("HOME=/tmp/codebuddy-home\n"));
    assert!(
        child_env.contains("PATH=/usr/bin:/bin\n") || child_env.contains("PATH=/bin:/usr/bin\n")
    );
    assert!(!env_has_key(&child_env, "CODEBUDDY_API_KEY"));
    assert!(!child_env.contains("parent-codebuddy-canary"));
    assert!(!child_env.contains("endpoint-canary-secret"));
    assert!(!child_env.contains("https://codebuddy.example/v1"));
    assert!(!child_env.contains("kept-parent"));

    let mut write = job("hi");
    write.sandbox = Sandbox::Write;
    write.resume = Some("resume-id".into());
    write.model = ModelId::new("kimi-k2.5");
    write
        .params
        .extra
        .insert("effort".into(), serde_json::json!("high"));
    write
        .params
        .extra
        .insert("add_dir".into(), serde_json::json!(["/one", "/two"]));
    write
        .params
        .extra
        .insert("allowed_tools".into(), serde_json::json!("Read"));
    write
        .params
        .extra
        .insert("disallowed_tools".into(), serde_json::json!("Bash"));
    write
        .params
        .extra
        .insert("fallback_model".into(), serde_json::json!("n"));
    write
        .params
        .extra
        .insert("owner_user_id".into(), serde_json::json!("secret"));
    write
        .params
        .extra
        .insert("bound_owner".into(), serde_json::json!("owner"));
    write
        .params
        .extra
        .insert("run_id".into(), serde_json::json!("id"));
    write
        .params
        .extra
        .insert("task_type".into(), serde_json::json!("code_edit_test"));
    write
        .params
        .extra
        .insert("chain_index".into(), serde_json::json!(3));
    harness(&bin)
        .run(write, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        read_lines(&argv_file),
        vec![
            "-p",
            "hi",
            "--model",
            "kimi-k2.5",
            "--output-format",
            "json",
            "--setting-sources",
            "user",
            "--permission-mode",
            "acceptEdits",
            "--resume",
            "resume-id",
            "--effort",
            "high",
            "--allowedTools",
            "Read",
            "--disallowedTools",
            "Bash",
            "--add-dir",
            "/one",
            "--add-dir",
            "/two",
            "--fallback-model",
            "n",
        ]
    );

    let mut full = job("hi");
    full.sandbox = Sandbox::Full;
    harness(&bin)
        .run(full, CancellationToken::new())
        .await
        .unwrap();
    let full_args = read_lines(&argv_file);
    assert!(full_args.iter().any(|arg| arg == "-y"));
    assert!(!full_args.iter().any(|arg| arg == "--permission-mode"));
    assert!(
        full_args
            .windows(2)
            .any(|pair| pair == ["--model", "deepseek-v4.1-flash"])
    );
}

#[tokio::test]
async fn full_inherit_keeps_parent_codebuddy_env() {
    let dir = TempDir::new().unwrap();
    let (bin, _, env_file, _) = capture_script(&dir, RESULT, 0, "", "(node:1) Warning: ignored");
    let mut sample = job("hi");
    sample.env = EnvPolicy {
        mode: InheritMode::Full,
        ..EnvPolicy::scrubbed()
    };
    harness(&bin)
        .run(sample, CancellationToken::new())
        .await
        .unwrap();
    let child_env = fs::read_to_string(&env_file).unwrap();
    assert!(env_has_key(&child_env, "CODEBUDDY_API_KEY"));
    assert!(child_env.contains("parent-codebuddy-canary"));
    assert!(child_env.contains("OTHER_PARENT=kept-parent\n"));
}

#[tokio::test]
async fn result_error_and_nonzero_exit_fail() {
    let dir = TempDir::new().unwrap();
    let denied =
        r#"{"type":"result","subtype":"error","is_error":true,"result":"denied","session_id":"s"}"#;
    let (bin, _, _, _) = capture_script(&dir, denied, 0, "", "(node:1) Warning: ignored");
    let err = harness(&bin)
        .run(job("hi"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert_eq!(err.message, "denied");

    let (bin, _, _, _) = capture_script(&dir, RESULT, 9, "", "(node:1) Warning: ignored");
    let err = harness(&bin)
        .run(job("hi"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("code 9"), "{}", err.message);
    assert!(err.message.contains("OK"), "{}", err.message);
}

#[tokio::test]
async fn truncated_and_second_document_fail_closed() {
    let dir = TempDir::new().unwrap();
    let (bin, _, _, _) = capture_script(&dir, "{\n", 0, "", "");
    let err = harness(&bin)
        .run(job("hi"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("output truncated"), "{}", err.message);
    assert!(!err.message.contains("OK"));

    let docs = "\
{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"OK\",\"session_id\":\"s\"}
{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"LATER\"}
";
    let (bin, _, _, _) = capture_script(&dir, docs, 0, "", "(node:1) Warning: ignored");
    let err = harness(&bin)
        .run(job("hi"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("LATER"), "{}", err.message);
    assert_ne!(err.message, "OK");
}

#[tokio::test]
async fn oversized_prompt_does_not_spawn() {
    let dir = TempDir::new().unwrap();
    let (bin, argv_file, _, _) = capture_script(&dir, RESULT, 0, "", "");
    let prompt = "Q".repeat(32 * 4096);
    let err = harness(&bin)
        .run(job(&prompt), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Config);
    assert!(err.message.contains("MAX_ARG_STRLEN"), "{}", err.message);
    assert!(!argv_file.exists());
    assert!(!err.message.contains("QQQ"));
}
