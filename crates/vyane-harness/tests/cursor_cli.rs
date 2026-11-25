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
use vyane_core::env::EnvPolicy;
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

    let mut job = base_job("answer this");
    job.model = ModelId::new("grok-4.7");
    job.sandbox = Sandbox::Write;
    job.resume = Some("cursor-resume".into());
    job.workdir = Some(workdir.clone());
    job.endpoint = Some(Endpoint {
        base_url: "https://endpoint.example".into(),
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
    assert!(contains_window(&argv, &["--sandbox", "enabled"]));
    assert!(contains_window(
        &argv,
        &["--workspace", workdir.to_str().unwrap()]
    ));
    assert!(contains_window(&argv, &["--resume", "cursor-resume"]));
    let flags = flags_before_prompt(&argv);
    for forbidden in [
        "--force",
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
    assert!(child_env.contains("CURSOR_API_KEY=test-cursor-child-key\n"));
    assert!(child_env.contains("CURSOR_API_ENDPOINT=https://endpoint.example\n"));
    assert!(child_env.contains("EXPLICIT_CHILD=yes\n"));
}

#[tokio::test]
async fn read_only_prompt_that_looks_like_force_stays_positional() {
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
        .run(base_job("--force"), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(outcome.text, "kept");

    let argv = read_lines(&argv_file);
    assert_eq!(argv[argv.len() - 2], "--");
    assert_eq!(argv[argv.len() - 1], "--force");
    let flags = flags_before_prompt(&argv);
    assert!(contains_window(flags, &["--mode", "ask"]));
    assert!(contains_window(flags, &["--sandbox", "enabled"]));
    assert!(!flags.iter().any(|arg| arg == "--force"));
    assert!(!flags.iter().any(|arg| arg == "acp"));
}

#[tokio::test]
async fn parent_cursor_credentials_stay_out_of_the_child() {
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
            "CURSOR_API_ENDPOINT".to_string(),
            "https://parent.example".to_string(),
        ),
    ];

    let outcome =
        CursorCliHarness::with_binary_and_parent_env_for_tests(bin.to_string_lossy(), parent_env)
            .run(base_job("native auth"), CancellationToken::new())
            .await
            .unwrap();
    assert_eq!(outcome.text, "native");

    let child_env = fs::read_to_string(env_file).unwrap();
    assert!(!child_env.contains("parent-cursor-key"));
    assert!(!child_env.contains("https://parent.example"));
    assert!(!child_env.contains("CURSOR_API_KEY="));
    assert!(!child_env.contains("CURSOR_API_ENDPOINT="));
    let argv = fs::read_to_string(argv_file).unwrap();
    assert!(!argv.contains("parent-cursor-key"));
    assert!(!argv.contains("--api-key"));
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
        .run(base_job("fail"), CancellationToken::new())
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
        .run(base_job("fail closed"), CancellationToken::new())
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
        .run(base_job("fail"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("23"));
    assert!(err.message.contains("cursor failed"));
}

#[tokio::test]
async fn missing_binary_is_spawn_failed() {
    let dir = TempDir::new().unwrap();
    let err = CursorCliHarness::with_binary(dir.path().join("missing").to_string_lossy())
        .run(base_job("fail"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::SpawnFailed);
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

    let mut job = base_job("hang");
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
            .run(base_job("hang"), harness_token)
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
