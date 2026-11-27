//! Fake-process contract for [`vyane_harness::CursorCliHarness`].
//!
//! The child is always a local script. These tests do not invoke a Cursor
//! install, a model API, or stored credentials.

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use vyane_core::HarnessSpawnAuthority;
use vyane_core::env::{EnvPolicy, InheritMode};
use vyane_core::error::ErrorKind;
use vyane_core::target::{AuthMaterial, AuthStyle, Endpoint, ModelId, Protocol, Sandbox, Secret};
use vyane_core::task::GenParams;
use vyane_core::traits::{Harness, HarnessExecutionContext, HarnessJob};
use vyane_harness::CursorCliHarness;

#[cfg(target_os = "linux")]
use vyane_core::PinnedWorkdir;

static KILL_TREE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn base_job(prompt: &str) -> HarnessJob {
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

fn spawning_job(prompt: &str) -> HarnessJob {
    let mut job = base_job(prompt);
    job.sandbox = Sandbox::Full;
    job
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

fn assert_subscription_env_absent(child_env: &str) {
    for line in child_env.lines() {
        let name = line.split_once('=').map(|(key, _)| key).unwrap_or(line);
        assert!(
            !name.eq_ignore_ascii_case("CURSOR_API_KEY"),
            "child environment kept CURSOR_API_KEY"
        );
        assert!(
            !name.eq_ignore_ascii_case("CURSOR_API_ENDPOINT"),
            "child environment kept CURSOR_API_ENDPOINT"
        );
    }
}

fn contains_window(lines: &[String], expected: &[&str]) -> bool {
    lines.windows(expected.len()).any(|window| {
        window
            .iter()
            .map(String::as_str)
            .eq(expected.iter().copied())
    })
}

fn flags_before_prompt(lines: &[String]) -> &[String] {
    let end = lines.iter().rposition(|line| line == "--").unwrap();
    &lines[..end]
}

const SUCCESS_JSON: &str = r#"{"type":"result","subtype":"success","is_error":false,"duration_ms":5,"duration_api_ms":4,"result":"cursor final","session_id":"cursor-session","request_id":"req-1","usage":{"input_tokens":9,"output_tokens":8}}"#;

#[tokio::test]
async fn fake_process_maps_one_json_result_and_records_argv_env_and_cwd() {
    let dir = TempDir::new().unwrap();
    let workdir = dir.path().join("work");
    fs::create_dir(&workdir).unwrap();
    let argv_file = dir.path().join("argv.txt");
    let env_file = dir.path().join("env.txt");
    let cwd_marker = workdir.join("cwd-marker");
    let body = format!(
        r#"#!/bin/sh
set -eu
: > {argv}
for arg in "$@"; do
  printf '%s\n' "$arg" >> {argv}
done
env | sort > {env}
printf ran > {marker}
printf '%s\n' '{json}'
"#,
        argv = shell_quote(&argv_file),
        env = shell_quote(&env_file),
        marker = shell_quote(&cwd_marker),
        json = SUCCESS_JSON,
    );
    let bin = write_script(&dir, "fake-cursor", &body);

    let mut job = spawning_job("answer this");
    job.model = ModelId::new("grok-4.7");
    job.resume = Some("cursor-resume".into());
    job.workdir = Some(workdir.clone());
    job.endpoint = Some(Endpoint {
        base_url: String::new(),
        auth: Some(AuthMaterial {
            style: AuthStyle::Bearer,
            secret: Secret::new("test-cursor-child-key"),
        }),
    });
    job.env = EnvPolicy::scrubbed().inject("EXPLICIT_CHILD", "yes");

    let outcome = CursorCliHarness::with_binary(bin.to_string_lossy())
        .run(job, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.text, "cursor final");
    assert_eq!(outcome.native_session_id.as_deref(), Some("cursor-session"));
    assert!(outcome.usage.is_none());
    assert_eq!(outcome.exit_code, 0);
    assert!(cwd_marker.is_file());

    let argv = read_lines(&argv_file);
    assert_eq!(argv.first().map(String::as_str), Some("-p"));
    assert_eq!(argv[argv.len() - 2], "--");
    assert_eq!(argv[argv.len() - 1], "answer this");
    assert!(contains_window(&argv, &["--output-format", "json"]));
    assert!(contains_window(&argv, &["--model", "grok-4.7"]));
    assert!(contains_window(&argv, &["--sandbox", "disabled"]));
    assert!(contains_window(&argv, &["--force"]));
    assert!(contains_window(
        &argv,
        &["--workspace", workdir.to_str().unwrap()]
    ));
    assert!(contains_window(&argv, &["--resume", "cursor-resume"]));
    let flags = flags_before_prompt(&argv);
    assert_eq!(flags.iter().filter(|arg| *arg == "--force").count(), 1);
    for forbidden in [
        "--trust",
        "--yolo",
        "--mode",
        "acp",
        "stream-json",
        "--api-key",
        "test-cursor-child-key",
    ] {
        assert!(!flags.iter().any(|arg| arg == forbidden), "{argv:?}");
    }

    let child_env = fs::read_to_string(&env_file).unwrap();
    assert_subscription_env_absent(&child_env);
    assert!(!child_env.contains("test-cursor-child-key"));
    assert!(child_env.contains("EXPLICIT_CHILD=yes\n"));
}

/// Write is refused before spawn. Read-only still records `--mode ask` and
/// omits `--force`, `--sandbox`, and `--trust` when hostile config files are
/// present. The child is a local script, so this does not show that the
/// vendor binary ignores those files.
#[tokio::test]
async fn write_does_not_spawn_and_readonly_argv_stays_ask_beside_hostile_config() {
    let dir = TempDir::new().unwrap();
    let home = dir.path().join("home");
    let xdg = dir.path().join("xdg");
    let config_dir = dir.path().join("cursor-config");
    let work = dir.path().join("work");
    fs::create_dir_all(home.join(".cursor")).unwrap();
    fs::write(
        home.join(".cursor/cli-config.json"),
        r#"{"version":1,"approvalMode":"unrestricted","permissions":{"allow":["Shell(sh)"],"deny":[]}}"#,
    )
    .unwrap();
    fs::create_dir_all(xdg.join("cursor")).unwrap();
    fs::write(
        xdg.join("cursor/cli-config.json"),
        r#"{"version":1,"approvalMode":"unrestricted"}"#,
    )
    .unwrap();
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(
        config_dir.join("cli-config.json"),
        r#"{"version":1,"approvalMode":"unrestricted"}"#,
    )
    .unwrap();
    fs::create_dir_all(work.join(".cursor")).unwrap();
    fs::write(
        work.join(".cursor/cli.json"),
        r#"{"permissions":{"allow":["Shell(sh)","Write(**)"]}}"#,
    )
    .unwrap();
    fs::write(
        work.join(".cursor/hooks.json"),
        r#"{"version":1,"hooks":{"beforeSubmitPrompt":[{"command":"sh -c true"}]}}"#,
    )
    .unwrap();

    let marker = dir.path().join("spawned");
    let argv_file = dir.path().join("argv.txt");
    let body = format!(
        r#"#!/bin/sh
set -eu
: > {argv}
for arg in "$@"; do
  printf '%s\n' "$arg" >> {argv}
done
printf spawned > {marker}
printf '%s\n' '{{"type":"result","subtype":"success","is_error":false,"result":"ran"}}'
"#,
        argv = shell_quote(&argv_file),
        marker = shell_quote(&marker),
    );
    let bin = write_script(&dir, "hostile-agent", &body);
    let parent = vec![
        ("PATH".to_string(), "/usr/bin:/bin".to_string()),
        ("HOME".to_string(), home.display().to_string()),
        ("XDG_CONFIG_HOME".to_string(), xdg.display().to_string()),
        (
            "CURSOR_CONFIG_DIR".to_string(),
            config_dir.display().to_string(),
        ),
    ];

    let mut write_job = base_job("do not run");
    write_job.sandbox = Sandbox::Write;
    write_job.workdir = Some(work.clone());
    write_job.env.allow.extend([
        "HOME".to_string(),
        "XDG_CONFIG_HOME".to_string(),
        "CURSOR_CONFIG_DIR".to_string(),
    ]);
    let err = CursorCliHarness::with_binary_and_parent_env_for_tests(
        bin.to_string_lossy(),
        parent.clone(),
    )
    .run(write_job, CancellationToken::new())
    .await
    .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    assert!(err.message.contains("refuses to spawn"), "{}", err.message);
    assert!(err.message.contains("write"), "{}", err.message);
    assert!(!marker.exists(), "write sandbox spawned the child");
    assert!(!argv_file.exists(), "write sandbox wrote argv");

    let mut readonly = base_job("ask only");
    readonly.sandbox = Sandbox::ReadOnly;
    readonly.workdir = Some(work.clone());
    readonly.env.allow.extend([
        "HOME".to_string(),
        "XDG_CONFIG_HOME".to_string(),
        "CURSOR_CONFIG_DIR".to_string(),
    ]);
    let outcome = CursorCliHarness::with_binary_and_parent_env_for_tests(
        bin.to_string_lossy(),
        parent.clone(),
    )
    .run(readonly, CancellationToken::new())
    .await
    .unwrap();
    assert_eq!(outcome.text, "ran");
    assert!(marker.is_file());
    let readonly_argv = read_lines(&argv_file);
    let readonly_flags = flags_before_prompt(&readonly_argv);
    assert!(contains_window(readonly_flags, &["--mode", "ask"]));
    assert_eq!(
        readonly_flags.iter().filter(|arg| *arg == "--mode").count(),
        1
    );
    for forbidden in [
        "--force",
        "--sandbox",
        "--trust",
        "--yolo",
        "--api-key",
        "agent",
    ] {
        assert!(
            !readonly_flags.iter().any(|arg| arg == forbidden),
            "{readonly_argv:?}"
        );
    }

    let mut explicit = spawning_job("explicit full");
    explicit.workdir = Some(work);
    explicit.env.allow.extend([
        "HOME".to_string(),
        "XDG_CONFIG_HOME".to_string(),
        "CURSOR_CONFIG_DIR".to_string(),
    ]);
    let outcome =
        CursorCliHarness::with_binary_and_parent_env_for_tests(bin.to_string_lossy(), parent)
            .run(explicit, CancellationToken::new())
            .await
            .unwrap();
    assert_eq!(outcome.text, "ran");
    assert!(marker.is_file());
    let argv = read_lines(&argv_file);
    let flags = flags_before_prompt(&argv);
    assert!(contains_window(flags, &["--sandbox", "disabled"]));
    assert_eq!(flags.iter().filter(|arg| *arg == "--force").count(), 1);
    assert!(!flags.iter().any(|arg| arg == "--trust"));
    assert!(!flags.iter().any(|arg| arg == "--mode"));
}

#[tokio::test]
async fn custom_endpoint_does_not_spawn() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("spawned");
    let body = format!("#!/bin/sh\nprintf spawned > {}\n", shell_quote(&marker));
    let bin = write_script(&dir, "endpoint-agent", &body);

    for sandbox in [Sandbox::ReadOnly, Sandbox::Full] {
        let mut job = base_job("do not retarget");
        job.sandbox = sandbox;
        job.endpoint = Some(Endpoint {
            base_url: "https://endpoint.example".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("must-not-matter"),
            }),
        });
        let err = CursorCliHarness::with_binary(bin.to_string_lossy())
            .run(job, CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unsupported);
        assert!(err.message.contains("custom endpoint"), "{}", err.message);
        assert!(!err.message.contains("must-not-matter"));
        assert!(!err.message.contains("https://endpoint.example"));
        assert!(!marker.exists());
    }
}

#[tokio::test]
async fn full_prompt_that_looks_like_force_stays_positional() {
    let dir = TempDir::new().unwrap();
    let argv_file = dir.path().join("argv.txt");
    let body = format!(
        r#"#!/bin/sh
set -eu
: > {argv}
for arg in "$@"; do
  printf '%s\n' "$arg" >> {argv}
done
printf '%s\n' '{{"type":"result","subtype":"success","is_error":false,"result":"kept"}}'
"#,
        argv = shell_quote(&argv_file),
    );
    let bin = write_script(&dir, "fake-cursor-readonly", &body);

    let outcome = CursorCliHarness::with_binary(bin.to_string_lossy())
        .run(spawning_job("--force"), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(outcome.text, "kept");

    let argv = read_lines(&argv_file);
    assert_eq!(argv[argv.len() - 2], "--");
    assert_eq!(argv[argv.len() - 1], "--force");
    let flags = flags_before_prompt(&argv);
    assert!(contains_window(flags, &["--sandbox", "disabled"]));
    assert_eq!(flags.iter().filter(|arg| *arg == "--force").count(), 1);
    assert!(!flags.iter().any(|arg| arg == "--trust"));
    assert!(!flags.iter().any(|arg| arg == "acp"));
}

#[tokio::test]
async fn readonly_fake_process_passes_ask_mode_and_strips_key_and_endpoint() {
    let dir = TempDir::new().unwrap();
    let env_file = dir.path().join("env.txt");
    let argv_file = dir.path().join("argv.txt");
    let body = format!(
        r#"#!/bin/sh
set -eu
: > {argv}
for arg in "$@"; do
  printf '%s\n' "$arg" >> {argv}
done
env | sort > {env}
printf '%s\n' '{{"type":"result","subtype":"success","is_error":false,"result":"native"}}'
"#,
        argv = shell_quote(&argv_file),
        env = shell_quote(&env_file),
    );
    let bin = write_script(&dir, "fake-cursor-env", &body);
    let parent_env = vec![
        ("PATH".to_string(), "/usr/bin:/bin".to_string()),
        ("HOME".to_string(), "/tmp/cursor-home".to_string()),
        (
            "CURSOR_API_KEY".to_string(),
            "parent-cursor-key".to_string(),
        ),
        (
            "cursor_api_key".to_string(),
            "parent-cursor-key-lower".to_string(),
        ),
        (
            "CURSOR_API_ENDPOINT".to_string(),
            "https://parent.example".to_string(),
        ),
        (
            "Cursor_Api_Endpoint".to_string(),
            "https://parent-mixed.example".to_string(),
        ),
    ];

    let mut job = base_job("--mode");
    job.sandbox = Sandbox::ReadOnly;
    job.model = ModelId::new("grok-4.7");
    job.env = EnvPolicy {
        mode: InheritMode::Full,
        ..EnvPolicy::scrubbed()
    };
    job.env.allow.extend([
        "CURSOR_API_KEY".to_string(),
        "CURSOR_API_ENDPOINT".to_string(),
    ]);
    job.env = job
        .env
        .clone()
        .inject("CURSOR_API_KEY", "injected-cursor-key")
        .inject("CURSOR_API_ENDPOINT", "https://injected.example")
        .inject("cursor_api_key", "injected-cursor-key-lower")
        .inject("EXPLICIT_CHILD", "yes");
    job.endpoint = Some(Endpoint {
        base_url: String::new(),
        auth: Some(AuthMaterial {
            style: AuthStyle::Bearer,
            secret: Secret::new("endpoint-cursor-secret"),
        }),
    });

    let outcome =
        CursorCliHarness::with_binary_and_parent_env_for_tests(bin.to_string_lossy(), parent_env)
            .run(job, CancellationToken::new())
            .await
            .unwrap();
    assert_eq!(outcome.text, "native");

    let child_env = fs::read_to_string(&env_file).unwrap();
    assert_subscription_env_absent(&child_env);
    assert!(child_env.contains("EXPLICIT_CHILD=yes\n"));
    assert!(child_env.contains("HOME=/tmp/cursor-home\n"));
    for forbidden in [
        "parent-cursor-key",
        "parent-cursor-key-lower",
        "https://parent.example",
        "https://parent-mixed.example",
        "injected-cursor-key",
        "injected-cursor-key-lower",
        "https://injected.example",
        "endpoint-cursor-secret",
    ] {
        assert!(
            !child_env.contains(forbidden),
            "{forbidden} reached the child environment"
        );
    }

    let argv = read_lines(&argv_file);
    assert_eq!(argv.first().map(String::as_str), Some("-p"));
    assert_eq!(argv[argv.len() - 2], "--");
    assert_eq!(argv[argv.len() - 1], "--mode");
    let flags = flags_before_prompt(&argv);
    assert!(contains_window(flags, &["--output-format", "json"]));
    assert!(contains_window(flags, &["--model", "grok-4.7"]));
    assert!(contains_window(flags, &["--mode", "ask"]));
    assert_eq!(flags.iter().filter(|arg| *arg == "--mode").count(), 1);
    for forbidden in [
        "--force",
        "--sandbox",
        "--trust",
        "--yolo",
        "--api-key",
        "agent",
        "parent-cursor-key",
        "injected-cursor-key",
        "endpoint-cursor-secret",
    ] {
        assert!(!flags.iter().any(|arg| arg == forbidden), "{argv:?}");
    }
}

#[tokio::test]
async fn exit_zero_error_envelope_is_harness_failed() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(
        &dir,
        "fake-cursor-error",
        r#"#!/bin/sh
set -eu
printf '%s\n' '{"type":"result","subtype":"error","is_error":true,"result":"turn limit reached"}'
exit 0
"#,
    );

    let err = CursorCliHarness::with_binary(bin.to_string_lossy())
        .run(spawning_job("fail"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("error"));
    assert!(err.message.contains("turn limit reached"));
}

#[tokio::test]
async fn exit_zero_without_terminal_result_does_not_return_partial_text() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(
        &dir,
        "fake-cursor-partial",
        r#"#!/bin/sh
set -eu
printf '%s\n' '{"type":"assistant","result":"partial answer should not leak"}'
exit 0
"#,
    );

    let err = CursorCliHarness::with_binary(bin.to_string_lossy())
        .run(spawning_job("fail closed"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("missing_result"));
    assert!(err.message.contains("one-round"));
    assert!(!err.message.contains("partial answer should not leak"));
}

#[tokio::test]
async fn nonzero_exit_is_harness_failed() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(
        &dir,
        "fake-cursor-status",
        "#!/bin/sh\nprintf '%s\\n' 'cursor failed' >&2\nexit 23\n",
    );

    let err = CursorCliHarness::with_binary(bin.to_string_lossy())
        .run(spawning_job("fail"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("23"));
    assert!(err.message.contains("cursor failed"));
}

#[tokio::test]
async fn write_missing_binary_is_unsupported_before_spawn() {
    let mut job = base_job("fail");
    job.sandbox = Sandbox::Write;
    let err = CursorCliHarness::with_binary("/no/such/cursor-agent")
        .run(job, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    assert!(err.message.contains("refuses to spawn"), "{}", err.message);
    assert!(err.message.contains("write"), "{}", err.message);
}

#[tokio::test]
async fn missing_binary_is_spawn_failed() {
    let dir = TempDir::new().unwrap();
    for sandbox in [Sandbox::ReadOnly, Sandbox::Full] {
        let mut job = base_job("fail");
        job.sandbox = sandbox;
        let err = CursorCliHarness::with_binary(dir.path().join("missing").to_string_lossy())
            .run(job, CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::SpawnFailed);
    }
}

#[tokio::test]
async fn available_probes_the_configured_binary_without_running_it() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let body = format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker));
    let bin = write_script(&dir, "present-cursor", &body);

    assert!(
        CursorCliHarness::with_binary(bin.to_string_lossy())
            .available()
            .await
    );
    assert!(
        !CursorCliHarness::with_binary(dir.path().join("missing").to_string_lossy())
            .available()
            .await
    );
    assert!(!marker.exists());
}

#[tokio::test]
async fn stream_and_spawn_authority_fail_closed_without_starting_the_process() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let body = format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker));
    let bin = write_script(&dir, "fake-cursor-closed", &body);
    let harness = CursorCliHarness::with_binary(bin.to_string_lossy());

    let stream = harness
        .run_stream(
            base_job("stream"),
            CancellationToken::new(),
            Box::new(|_| {}),
        )
        .await
        .unwrap_err();
    assert_eq!(stream.kind, ErrorKind::Unsupported);
    assert!(stream.message.contains("cursor-cli"));

    let context =
        HarnessExecutionContext::with_spawn_authority(HarnessSpawnAuthority::new(|| true));
    let scoped = CursorCliHarness::with_binary(bin.to_string_lossy())
        .run_scoped(base_job("scoped"), context, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(scoped.kind, ErrorKind::Unsupported);
    assert!(scoped.message.contains("cursor-cli"));
    assert!(!marker.exists());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn pinned_workdir_fails_closed_without_starting_the_process() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let body = format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker));
    let bin = write_script(&dir, "fake-cursor-pinned", &body);
    let pinned = PinnedWorkdir::open(dir.path()).unwrap();
    let context = HarnessExecutionContext::with_pinned_workdir(pinned);

    let err = CursorCliHarness::with_binary(bin.to_string_lossy())
        .run_scoped(base_job("pinned"), context, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    assert!(err.message.contains("pinned"));
    assert!(!marker.exists());
}

#[tokio::test]
async fn timeout_kills_the_process_group() {
    let _guard = KILL_TREE_TEST_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let heartbeat = dir.path().join("heartbeat");
    let child_pid = dir.path().join("child.pid");
    let bin = kill_tree_script(&dir, &heartbeat, &child_pid);

    let mut job = spawning_job("hang");
    job.timeout = Some(Duration::from_secs(10));

    let err = CursorCliHarness::with_binary(bin.to_string_lossy())
        .run(job, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Timeout);
    assert_heartbeat_stops(&heartbeat);
    assert_pid_dead(&child_pid);
}

#[tokio::test]
async fn cancellation_kills_the_process_group() {
    let _guard = KILL_TREE_TEST_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let heartbeat = dir.path().join("heartbeat");
    let child_pid = dir.path().join("child.pid");
    let bin = kill_tree_script(&dir, &heartbeat, &child_pid);

    let token = CancellationToken::new();
    let harness_token = token.clone();
    let mut run = tokio::spawn(async move {
        CursorCliHarness::with_binary(bin.to_string_lossy())
            .run(spawning_job("hang"), harness_token)
            .await
    });

    tokio::select! {
        pid = wait_for_live_pid(&child_pid) => {
            assert!(!pid.is_empty(), "pid file was empty");
        }
        result = &mut run => {
            panic!("harness exited before grandchild was ready: {result:?}");
        }
    }

    token.cancel();
    let err = run.await.unwrap().unwrap_err();
    assert_eq!(err.kind, ErrorKind::Cancelled);
    assert_heartbeat_stops(&heartbeat);
    assert_pid_dead(&child_pid);
}

fn kill_tree_script(dir: &TempDir, heartbeat: &Path, child_pid: &Path) -> PathBuf {
    let body = format!(
        r#"#!/bin/sh
set -eu
printf start > {heartbeat}
(
  trap '' TERM
  while :; do
    printf x >> {heartbeat}
    sleep 0.05
  done
) &
printf '%s\n' "$!" > {child_pid}
wait
"#,
        heartbeat = shell_quote(heartbeat),
        child_pid = shell_quote(child_pid),
    );
    write_script(dir, "cursor-kill-tree", &body)
}

fn assert_heartbeat_stops(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(path.exists(), "heartbeat file was never written");

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut len = fs::metadata(path).unwrap().len();
    let mut stable_since = Instant::now();
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
        let current = fs::metadata(path).unwrap().len();
        if current == len {
            if stable_since.elapsed() >= Duration::from_millis(500) {
                return;
            }
        } else {
            len = current;
            stable_since = Instant::now();
        }
    }
    panic!("grandchild kept writing after group kill");
}

fn assert_pid_dead(path: &Path) {
    let pid = read_pid(path).unwrap_or_default();
    assert!(!pid.is_empty(), "pid file was empty");
    let deadline = Instant::now() + Duration::from_secs(5);
    while pid_is_running(&pid) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(!pid_is_running(&pid), "grandchild pid {pid} is still alive");
}

async fn wait_for_live_pid(path: &Path) -> String {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if let Some(pid) = read_pid(path)
            && pid_is_running(&pid)
        {
            return pid;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("grandchild pid was not recorded alive before cancellation");
}

fn read_pid(path: &Path) -> Option<String> {
    let raw = fs::read_to_string(path).ok()?;
    let pid = raw.trim();
    (!pid.is_empty()).then(|| pid.to_string())
}

fn pid_is_running(pid: &str) -> bool {
    let status = std::process::Command::new("kill")
        .args(["-0", pid])
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    status.success() && !pid_is_zombie(pid)
}

fn pid_is_zombie(pid: &str) -> bool {
    let Ok(output) = std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", pid])
        .output()
    else {
        return false;
    };
    if !output.status.success() {
        return false;
    }
    String::from_utf8_lossy(&output.stdout).contains('Z')
}
