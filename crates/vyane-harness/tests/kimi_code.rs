//! Fake-process contract for [`vyane_harness::KimiCodeHarness`].
//!
//! The child is always a local script. These tests do not invoke a Kimi
//! install, a model API, or stored credentials.

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use vyane_core::HarnessSpawnAuthority;
use vyane_core::env::{EnvPolicy, InheritMode};
use vyane_core::error::ErrorKind;
use vyane_core::target::{AuthMaterial, AuthStyle, Endpoint, ModelId, Protocol, Sandbox, Secret};
use vyane_core::task::GenParams;
use vyane_core::traits::{Harness, HarnessExecutionContext, HarnessJob};
use vyane_harness::KimiCodeHarness;

#[cfg(target_os = "linux")]
use vyane_core::PinnedWorkdir;

const STREAM: &str = r#"warning: something on stderr
{"role":"assistant","tool_calls":[{"type":"function","id":"tool_x","function":{"name":"Read","arguments":"{\"path\":\"sample.txt\"}"}}]}
{"role":"tool","tool_call_id":"tool_x","content":"1\thello world"}
{"role":"assistant","content":""}
{"role":"assistant","content":"part one"}
{"role":"assistant","content":"part two","usage":{"input_tokens":9,"output_tokens":4,"total_tokens":13}}
{"role":"meta","type":"other","session_id":"not-this"}
{"role":"meta","type":"session.resume_hint","session_id":"session_first","content":"To resume: kimi -r session_first"}
{"role":"meta","type":"session.resume_hint","session_id":""}
{"role":"meta","type":"session.resume_hint","session_id":"session_abc","command":"kimi -r session_abc","content":"To resume: kimi -r session_abc"}
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
        ("HOME".into(), "/tmp/kimi-home".into()),
        ("KIMI_API_KEY".into(), "parent-metered-key".into()),
        ("KIMI_MODEL_API_KEY".into(), "parent-model-key".into()),
        ("kimi_api_key".into(), "parent-metered-lower".into()),
        ("Kimi_Model_Api_Key".into(), "parent-model-mixed".into()),
        ("OTHER_PARENT".into(), "kept-parent".into()),
    ]
}

fn harness(bin: &Path) -> KimiCodeHarness {
    KimiCodeHarness::with_binary_and_parent_env_for_tests(bin.to_string_lossy(), parent_env())
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
    before_stdout: &str,
) -> (PathBuf, PathBuf, PathBuf, PathBuf, PathBuf) {
    let stdout_path = dir.path().join("stdout.txt");
    fs::write(&stdout_path, stdout_body).unwrap();
    let argv_file = dir.path().join("argv.txt");
    let argv0_file = dir.path().join("argv0.txt");
    let env_file = dir.path().join("env.txt");
    let stdin_file = dir.path().join("stdin.txt");
    let before = if before_stdout.is_empty() {
        String::new()
    } else {
        format!("{before_stdout}\n")
    };
    let body = format!(
        r#"#!/bin/sh
set -eu
printf '%s\n' "$0" > {argv0}
: > {argv}
for arg in "$@"; do
  printf '%s\n' "$arg" >> {argv}
done
env | sort > {env}
cat > {stdin}
{before}cat {stdout}
exit {exit_code}
"#,
        argv0 = shell_quote(&argv0_file),
        argv = shell_quote(&argv_file),
        env = shell_quote(&env_file),
        stdin = shell_quote(&stdin_file),
        before = before,
        stdout = shell_quote(&stdout_path),
        exit_code = exit_code,
    );
    let bin = write_script(dir, "fake-kimi", &body);
    (bin, argv_file, argv0_file, env_file, stdin_file)
}

#[tokio::test]
async fn fake_process_maps_stream_json_argv_env_cwd_and_closed_stdin() {
    let dir = TempDir::new().unwrap();
    let workdir = dir.path().join("work");
    fs::create_dir(&workdir).unwrap();
    let workdir = workdir.canonicalize().unwrap();
    let pwd_file = dir.path().join("pwd.txt");
    let marker = workdir.join("cwd-marker");
    let before_stdout = format!(
        "pwd -P > {}\nprintf ran > {}",
        shell_quote(&pwd_file),
        shell_quote(&marker)
    );
    let (bin, argv_file, argv0_file, env_file, stdin_file) =
        capture_script(&dir, STREAM, 0, &before_stdout);

    for sandbox in [Sandbox::ReadOnly, Sandbox::Write, Sandbox::Full] {
        let _ = fs::remove_file(&argv_file);
        let _ = fs::remove_file(&stdin_file);
        let _ = fs::remove_file(&marker);
        let mut sample = job("hello from kimi");
        sample.sandbox = sandbox;
        sample.model = ModelId::new("kimi-k2.6");
        sample.resume = Some("session_resume".into());
        sample.workdir = Some(workdir.clone());
        sample
            .params
            .extra
            .insert("add_dir".into(), serde_json::json!("/extra"));
        sample
            .params
            .extra
            .insert("provider".into(), serde_json::json!("moonshot"));
        sample
            .params
            .extra
            .insert("owner_user_id".into(), serde_json::json!("alice"));
        sample.env = EnvPolicy {
            mode: InheritMode::Full,
            ..EnvPolicy::scrubbed()
        }
        .inject("KIMI_API_KEY", "injected-metered-key")
        .inject("KIMI_MODEL_API_KEY", "injected-model-key")
        .inject("KIMI_MODEL_NAME", "kimi-k2")
        .inject("EXPLICIT_CHILD", "yes");
        sample.endpoint = Some(Endpoint {
            base_url: "https://kimi.example/v1".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("endpoint-canary-secret"),
            }),
        });

        let outcome = harness(&bin)
            .run(sample, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(outcome.text, "part one\npart two");
        assert_eq!(outcome.native_session_id.as_deref(), Some("session_abc"));
        assert!(outcome.usage.is_none());
        assert_eq!(outcome.exit_code, 0);
        assert!(!outcome.text.contains("sample.txt"));
        assert!(!outcome.text.contains("warning"));
        assert!(!outcome.text.contains("kimi -r"));
        assert!(!outcome.text.contains("not-this"));
        assert!(marker.is_file());
        assert_eq!(
            PathBuf::from(fs::read_to_string(&pwd_file).unwrap().trim()),
            workdir
        );
        assert_eq!(
            fs::read_to_string(&argv0_file).unwrap().trim(),
            bin.to_str().unwrap()
        );
        assert!(fs::read(&stdin_file).unwrap().is_empty());

        let argv = read_lines(&argv_file);
        assert_eq!(
            argv,
            vec![
                "-p",
                "hello from kimi",
                "--output-format",
                "stream-json",
                "-m",
                "kimi-k2.6",
                "-S",
                "session_resume",
                "--add-dir",
                "/extra",
            ]
        );
        for forbidden in [
            "--sandbox",
            "-y",
            "--auto",
            "--yolo",
            "-r",
            "--resume",
            "--prompt-file",
            "/dev/stdin",
            "--provider",
            "moonshot",
            "--owner-user-id",
            "alice",
            "endpoint-canary-secret",
            "https://kimi.example/v1",
            "KIMI_API_KEY",
            "parent-metered-key",
            "injected-metered-key",
        ] {
            assert!(
                !argv.iter().any(|arg| arg.contains(forbidden)),
                "{forbidden} in {argv:?}"
            );
        }
        assert!(
            !argv
                .iter()
                .any(|arg| arg.contains(workdir.to_str().unwrap()))
        );

        let child_env = fs::read_to_string(&env_file).unwrap();
        assert!(child_env.contains("HOME=/tmp/kimi-home\n"));
        assert!(child_env.contains("EXPLICIT_CHILD=yes\n"));
        assert!(child_env.contains("OTHER_PARENT=kept-parent\n"));
        assert!(child_env.contains("KIMI_MODEL_NAME=kimi-k2\n"));
        assert!(!env_has_key(&child_env, "KIMI_API_KEY"));
        assert!(!env_has_key(&child_env, "KIMI_MODEL_API_KEY"));
        for forbidden in [
            "parent-metered-key",
            "parent-model-key",
            "parent-metered-lower",
            "parent-model-mixed",
            "injected-metered-key",
            "injected-model-key",
            "endpoint-canary-secret",
            "https://kimi.example/v1",
        ] {
            assert!(
                !child_env.contains(forbidden),
                "{forbidden} reached the child env"
            );
        }
    }
}

#[tokio::test]
async fn plain_text_fallback_uses_the_smoke_command() {
    let dir = TempDir::new().unwrap();
    let (bin, argv_file, _, _, stdin_file) =
        capture_script(&dir, "just plain output\nsecond line\n", 0, "");
    let outcome = harness(&bin)
        .run(job("hello world"), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(outcome.text, "just plain output\nsecond line");
    assert!(outcome.native_session_id.is_none());
    assert!(outcome.usage.is_none());
    assert_eq!(
        read_lines(&argv_file),
        vec!["-p", "hello world", "--output-format", "stream-json"]
    );
    assert!(fs::read(&stdin_file).unwrap().is_empty());
}

#[tokio::test]
async fn error_role_fails_without_mixing_in_the_other_field() {
    let dir = TempDir::new().unwrap();
    let (bin, _, _, _, _) = capture_script(
        &dir,
        "{\"role\":\"error\",\"content\":\"not logged in\",\"error\":\"other\"}\n",
        0,
        "",
    );
    let err = harness(&bin)
        .run(job("hi"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("not logged in"), "{}", err.message);
    assert!(!err.message.contains("other"), "{}", err.message);
}

#[tokio::test]
async fn top_level_error_field_fails_the_run() {
    let dir = TempDir::new().unwrap();
    let (bin, _, _, _, _) = capture_script(
        &dir,
        "{\"role\":\"meta\",\"error\":\"quota exceeded\"}\n",
        0,
        "",
    );
    let err = harness(&bin)
        .run(job("hi"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("quota exceeded"), "{}", err.message);
}

#[tokio::test]
async fn nonzero_exit_is_harness_failed() {
    let dir = TempDir::new().unwrap();
    let (bin, _, _, _, _) = capture_script(&dir, "cli said no\n", 23, "");
    let err = harness(&bin)
        .run(job("hi"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("23"), "{}", err.message);
    assert!(err.message.contains("cli said no"), "{}", err.message);
}

#[tokio::test]
async fn empty_stdout_is_harness_failed() {
    let dir = TempDir::new().unwrap();
    let (bin, _, _, _, _) = capture_script(&dir, "", 0, "");
    let err = harness(&bin)
        .run(job("hi"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(
        err.message.contains("did not include answer text"),
        "{}",
        err.message
    );
}

#[tokio::test]
async fn missing_binary_is_spawn_failed() {
    let dir = TempDir::new().unwrap();
    let err = harness(&dir.path().join("missing-kimi"))
        .run(job("hi"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::SpawnFailed);
}

#[tokio::test]
async fn oversized_prompt_does_not_spawn() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let bin = write_script(
        &dir,
        "fake-kimi-big",
        &format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker)),
    );
    let err = harness(&bin)
        .run(job(&"Q".repeat(32 * 4096)), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Config);
    assert!(err.message.contains("MAX_ARG_STRLEN"), "{}", err.message);
    assert!(!err.message.contains("QQQ"), "{}", err.message);
    assert!(!marker.exists());
}

#[tokio::test]
async fn object_extra_does_not_spawn() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let bin = write_script(
        &dir,
        "fake-kimi-object",
        &format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker)),
    );
    let mut sample = job("hi");
    sample
        .params
        .extra
        .insert("nested".into(), serde_json::json!({"a": 1}));
    let err = harness(&bin)
        .run(sample, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    assert!(err.message.contains("nested"), "{}", err.message);
    assert!(!marker.exists());
}

#[tokio::test]
async fn available_checks_the_binary_without_running_it() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let bin = write_script(
        &dir,
        "fake-kimi-available",
        &format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker)),
    );
    assert!(
        KimiCodeHarness::with_binary(bin.to_string_lossy())
            .available()
            .await
    );
    assert!(
        !KimiCodeHarness::with_binary(dir.path().join("missing").to_string_lossy())
            .available()
            .await
    );
    assert!(!marker.exists());
}

#[tokio::test]
async fn stream_and_spawn_authority_fail_closed_without_starting_the_process() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let bin = write_script(
        &dir,
        "fake-kimi-closed",
        &format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker)),
    );
    let stream = KimiCodeHarness::with_binary(bin.to_string_lossy())
        .run_stream(job("stream"), CancellationToken::new(), Box::new(|_| {}))
        .await
        .unwrap_err();
    assert_eq!(stream.kind, ErrorKind::Unsupported);
    assert!(stream.message.contains("kimi-code"), "{}", stream.message);

    let context =
        HarnessExecutionContext::with_spawn_authority(HarnessSpawnAuthority::new(|| true));
    let scoped = KimiCodeHarness::with_binary(bin.to_string_lossy())
        .run_scoped(job("scoped"), context, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(scoped.kind, ErrorKind::Unsupported);
    assert!(scoped.message.contains("kimi-code"), "{}", scoped.message);
    assert!(!marker.exists());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn pinned_workdir_fails_closed_without_starting_the_process() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let bin = write_script(
        &dir,
        "fake-kimi-pinned",
        &format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker)),
    );
    let pinned = PinnedWorkdir::open(dir.path()).unwrap();
    let context = HarnessExecutionContext::with_pinned_workdir(pinned);
    let err = KimiCodeHarness::with_binary(bin.to_string_lossy())
        .run_scoped(job("pinned"), context, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    assert!(err.message.contains("pinned"), "{}", err.message);
    assert!(!marker.exists());
}
