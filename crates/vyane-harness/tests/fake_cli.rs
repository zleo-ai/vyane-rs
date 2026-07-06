#![cfg(unix)]
#![allow(clippy::unwrap_used)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use vyane_core::env::EnvPolicy;
use vyane_core::error::ErrorKind;
use vyane_core::target::{AuthMaterial, AuthStyle, Endpoint, ModelId, Protocol, Sandbox, Secret};
use vyane_core::task::GenParams;
use vyane_core::traits::{Harness, HarnessJob};
use vyane_harness::{ClaudeCodeHarness, CodexCliHarness};

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

fn argv_capture_script(dir: &TempDir, name: &str, argv_file: &Path, env_file: &Path) -> PathBuf {
    let body = format!(
        r#"#!/bin/sh
set -eu
ARGV_FILE={argv_file}
ENV_FILE={env_file}
: > "$ARGV_FILE"
for arg in "$@"; do
  printf '%s\n' "$arg" >> "$ARGV_FILE"
done
env | sort > "$ENV_FILE"
case "$0" in
  *claude*)
    printf '%s\n' '{{"result":"claude final","session_id":"claude-session","usage":{{"input_tokens":2,"cache_creation_input_tokens":3,"cache_read_input_tokens":5,"output_tokens":7}}}}'
    ;;
  *)
    out=""
    prev=""
    for arg in "$@"; do
      if [ "$prev" = "-o" ] || [ "$prev" = "--output-last-message" ]; then
        out="$arg"
      fi
      prev="$arg"
    done
    if [ -n "$out" ]; then
      printf '%s\n' 'codex final' > "$out"
    fi
    printf '%s\n' '{{"type":"thread.started","thread_id":"codex-thread"}}'
    printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":11,"output_tokens":13,"cached_input_tokens":17}}}}'
    ;;
esac
"#,
        argv_file = shell_quote(argv_file),
        env_file = shell_quote(env_file),
    );
    write_script(dir, name, &body)
}

fn read_lines(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

fn contains_window(lines: &[String], expected: &[&str]) -> bool {
    lines
        .windows(expected.len())
        .any(|w| w.iter().map(String::as_str).eq(expected.iter().copied()))
}

#[tokio::test]
async fn claude_fake_cli_argv_env_and_parsing() {
    let dir = TempDir::new().unwrap();
    let argv_file = dir.path().join("argv.txt");
    let env_file = dir.path().join("env.txt");
    let bin = argv_capture_script(&dir, "fake-claude", &argv_file, &env_file);

    let mut job = base_job("answer this");
    job.model = ModelId::new("claude-test-model");
    job.sandbox = Sandbox::Write;
    job.resume = Some("claude-resume-id".into());
    job.endpoint = Some(Endpoint {
        base_url: "https://endpoint.example/v1".into(),
        auth: Some(AuthMaterial {
            style: AuthStyle::Bearer,
            secret: Secret::new("test-child-token"),
        }),
    });
    job.env = EnvPolicy::scrubbed().inject("EXPLICIT_CHILD", "yes");

    let outcome = ClaudeCodeHarness::with_binary(bin.to_string_lossy())
        .run(job, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.text, "claude final");
    assert_eq!(outcome.native_session_id.as_deref(), Some("claude-session"));
    let usage = outcome.usage.unwrap();
    assert_eq!(usage.input_tokens, 10);
    assert_eq!(usage.output_tokens, 7);
    assert_eq!(usage.cached_input_tokens, Some(5));
    assert_eq!(outcome.exit_code, 0);

    let argv = read_lines(&argv_file);
    assert_eq!(argv[0], "-p");
    assert_eq!(argv[1], "answer this");
    assert!(contains_window(&argv, &["--output-format", "json"]));
    assert!(contains_window(&argv, &["--model", "claude-test-model"]));
    assert!(contains_window(
        &argv,
        &["--permission-mode", "acceptEdits"]
    ));
    assert!(contains_window(&argv, &["--resume", "claude-resume-id"]));

    let child_env = fs::read_to_string(env_file).unwrap();
    assert!(child_env.contains("ANTHROPIC_BASE_URL=https://endpoint.example/v1\n"));
    assert!(child_env.contains("ANTHROPIC_AUTH_TOKEN=test-child-token\n"));
    assert!(child_env.contains("ANTHROPIC_MODEL=claude-test-model\n"));
    assert!(child_env.contains("EXPLICIT_CHILD=yes\n"));
}

#[tokio::test]
async fn codex_fake_cli_argv_env_and_parsing() {
    let dir = TempDir::new().unwrap();
    let argv_file = dir.path().join("argv.txt");
    let env_file = dir.path().join("env.txt");
    let bin = argv_capture_script(&dir, "fake-codex", &argv_file, &env_file);

    let workdir = dir.path().join("work");
    fs::create_dir(&workdir).unwrap();

    let mut job = base_job("-prompt starts with dash");
    job.model = ModelId::new("gpt-test-model");
    job.sandbox = Sandbox::Full;
    job.workdir = Some(workdir.clone());
    job.endpoint = Some(Endpoint {
        base_url: "https://openai-compatible.example/v1".into(),
        auth: Some(AuthMaterial {
            style: AuthStyle::XApiKey,
            secret: Secret::new("test-codex-child-key"),
        }),
    });

    let outcome = CodexCliHarness::with_binary(bin.to_string_lossy())
        .run(job, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome.text, "codex final");
    assert_eq!(outcome.native_session_id.as_deref(), Some("codex-thread"));
    let usage = outcome.usage.unwrap();
    assert_eq!(usage.input_tokens, 11);
    assert_eq!(usage.output_tokens, 13);
    assert_eq!(usage.cached_input_tokens, Some(17));
    assert_eq!(outcome.exit_code, 0);

    let argv = read_lines(&argv_file);
    assert!(contains_window(&argv, &["--ask-for-approval", "never"]));
    assert!(contains_window(&argv, &["--model", "gpt-test-model"]));
    assert!(contains_window(&argv, &["--sandbox", "danger-full-access"]));
    assert!(argv.iter().any(|a| a == "exec"));
    assert!(argv.iter().any(|a| a == "--json"));
    assert!(argv.iter().any(|a| a == "--skip-git-repo-check"));
    assert!(argv.iter().any(|a| a == "--ignore-user-config"));
    assert!(contains_window(
        &argv,
        &["-C", &workdir.display().to_string()]
    ));
    assert!(argv
        .iter()
        .any(|a| a == "model_providers.vyane.base_url=\"https://openai-compatible.example/v1\""));
    assert!(
        argv.iter()
            .any(|a| a == "model_providers.vyane.wire_api=\"responses\"")
    );
    assert!(
        argv.iter()
            .any(|a| a == "model_providers.vyane.env_key=\"OPENAI_API_KEY\"")
    );
    assert!(argv.iter().any(|a| a == "model_provider=\"vyane\""));
    assert_eq!(argv[argv.len() - 2], "--");
    assert_eq!(argv[argv.len() - 1], "-prompt starts with dash");

    let child_env = fs::read_to_string(env_file).unwrap();
    assert!(child_env.contains("OPENAI_API_KEY=test-codex-child-key\n"));
}

#[tokio::test]
async fn codex_anthropic_messages_protocol_is_unsupported_before_spawn() {
    let dir = TempDir::new().unwrap();
    let argv_file = dir.path().join("argv.txt");
    let env_file = dir.path().join("env.txt");
    let bin = argv_capture_script(&dir, "fake-codex", &argv_file, &env_file);

    let mut job = base_job("unsupported");
    job.protocol = Protocol::AnthropicMessages;

    let err = CodexCliHarness::with_binary(bin.to_string_lossy())
        .run(job, CancellationToken::new())
        .await
        .unwrap_err();

    assert_eq!(err.kind, ErrorKind::Unsupported);
    assert!(err.message.contains("anthropic_messages / codex-cli"));
    assert!(!argv_file.exists());
}

#[tokio::test]
async fn codex_resume_places_sandbox_before_exec_and_session_after_resume_flags() {
    let dir = TempDir::new().unwrap();
    let argv_file = dir.path().join("argv.txt");
    let env_file = dir.path().join("env.txt");
    let bin = argv_capture_script(&dir, "fake-codex", &argv_file, &env_file);

    let mut job = base_job("continue");
    job.sandbox = Sandbox::Write;
    job.resume = Some("resume-thread".into());

    CodexCliHarness::with_binary(bin.to_string_lossy())
        .run(job, CancellationToken::new())
        .await
        .unwrap();

    let argv = read_lines(&argv_file);
    assert!(contains_window(&argv, &["--sandbox", "workspace-write"]));
    let exec_pos = argv.iter().position(|a| a == "exec").unwrap();
    let sandbox_pos = argv.iter().position(|a| a == "--sandbox").unwrap();
    let resume_pos = argv.iter().position(|a| a == "resume").unwrap();
    let session_pos = argv.iter().position(|a| a == "resume-thread").unwrap();
    assert!(sandbox_pos < exec_pos);
    assert_eq!(argv[exec_pos + 1], "resume");
    assert!(resume_pos < session_pos);
    assert!(
        argv.iter()
            .position(|a| a == "--ignore-user-config")
            .unwrap()
            < session_pos
    );
    assert!(!argv.iter().any(|a| a == "-C"));
}

#[tokio::test]
async fn codex_resume_does_not_set_process_cwd_from_job_workdir() {
    let dir = TempDir::new().unwrap();
    let cwd_file = dir.path().join("cwd.txt");
    let bin = write_script(
        &dir,
        "fake-codex",
        &format!(
            r#"#!/bin/sh
set -eu
pwd > {cwd_file}
out=""
prev=""
for arg in "$@"; do
  if [ "$prev" = "-o" ]; then
    out="$arg"
  fi
  prev="$arg"
done
printf '%s\n' 'codex final' > "$out"
printf '%s\n' '{{"type":"thread.started","thread_id":"codex-thread"}}'
"#,
            cwd_file = shell_quote(&cwd_file),
        ),
    );

    let mut job = base_job("continue");
    job.resume = Some("resume-thread".into());
    job.workdir = Some(dir.path().join("job-workdir"));
    fs::create_dir(job.workdir.as_ref().unwrap()).unwrap();

    let parent_cwd = std::env::current_dir().unwrap();
    CodexCliHarness::with_binary(bin.to_string_lossy())
        .run(job, CancellationToken::new())
        .await
        .unwrap();

    let observed = fs::read_to_string(cwd_file).unwrap();
    assert_eq!(observed.trim(), parent_cwd.display().to_string());
}

#[tokio::test]
async fn env_scrub_drops_parent_api_keys_and_keeps_injections() {
    let dir = TempDir::new().unwrap();
    let argv_file = dir.path().join("argv.txt");
    let env_file = dir.path().join("env.txt");
    let bin = argv_capture_script(&dir, "fake-claude", &argv_file, &env_file);

    let mut job = base_job("env");
    job.env = EnvPolicy::scrubbed().inject("CHILD_ONLY_API_KEY", "child-secret");

    let parent_env = vec![
        ("PATH".to_string(), "/bin:/usr/bin".to_string()),
        (
            "PARENT_ONLY_API_KEY".to_string(),
            "parent-secret".to_string(),
        ),
    ];

    ClaudeCodeHarness::with_binary_and_parent_env_for_tests(bin.to_string_lossy(), parent_env)
        .run(job, CancellationToken::new())
        .await
        .unwrap();

    let child_env = fs::read_to_string(env_file).unwrap();
    assert!(!child_env.contains("PARENT_ONLY_API_KEY=parent-secret"));
    assert!(child_env.contains("CHILD_ONLY_API_KEY=child-secret\n"));
}

#[tokio::test]
async fn available_uses_executable_probe() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(&dir, "present-cli", "#!/bin/sh\nexit 0\n");

    assert!(
        ClaudeCodeHarness::with_binary(bin.to_string_lossy())
            .available()
            .await
    );
    assert!(
        !ClaudeCodeHarness::with_binary(dir.path().join("missing").to_string_lossy())
            .available()
            .await
    );
}

#[tokio::test]
async fn nonzero_exit_is_harness_failed() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(
        &dir,
        "fail-cli",
        "#!/bin/sh\nprintf '%s\n' 'fake failure' >&2\nexit 23\n",
    );

    let err = ClaudeCodeHarness::with_binary(bin.to_string_lossy())
        .run(base_job("fail"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
}

#[tokio::test]
async fn claude_exit_zero_error_envelope_is_harness_failed() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(
        &dir,
        "fake-claude",
        r#"#!/bin/sh
set -eu
printf '%s\n' '{"type":"result","subtype":"error_max_turns","is_error":true,"result":"turn limit reached"}'
exit 0
"#,
    );

    let err = ClaudeCodeHarness::with_binary(bin.to_string_lossy())
        .run(base_job("fail"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("error_max_turns"));
    assert!(err.message.contains("turn limit reached"));
}

#[tokio::test]
async fn codex_missing_last_message_file_is_harness_failed() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(
        &dir,
        "fake-codex",
        "#!/bin/sh\nprintf '%s\n' '{\"type\":\"thread.started\",\"thread_id\":\"codex-thread\"}'\nexit 0\n",
    );

    let err = CodexCliHarness::with_binary(bin.to_string_lossy())
        .run(base_job("missing last"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("last-message"));
}

#[tokio::test]
async fn missing_binary_is_spawn_failed() {
    let dir = TempDir::new().unwrap();
    let err = ClaudeCodeHarness::with_binary(dir.path().join("missing").to_string_lossy())
        .run(base_job("fail"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::SpawnFailed);
}

#[tokio::test]
async fn timeout_kills_process_group_grandchild() {
    let _guard = KILL_TREE_TEST_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let heartbeat = dir.path().join("heartbeat");
    let child_pid = dir.path().join("child.pid");
    let bin = kill_tree_script(&dir, &heartbeat, &child_pid);

    let mut job = base_job("hang");
    // Keep this comfortably above process startup latency under a busy test
    // runner; the assertion is about timeout-triggered group kill, not whether
    // the fake shell can create its liveness markers within a tiny window.
    job.timeout = Some(Duration::from_secs(10));

    let err = ClaudeCodeHarness::with_binary(bin.to_string_lossy())
        .run(job, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Timeout);

    assert_heartbeat_stops(&heartbeat);
    assert_pid_dead(&child_pid);
}

#[tokio::test]
async fn cancellation_kills_process_group_grandchild() {
    let _guard = KILL_TREE_TEST_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let heartbeat = dir.path().join("heartbeat");
    let child_pid = dir.path().join("child.pid");
    let bin = kill_tree_script(&dir, &heartbeat, &child_pid);

    let token = CancellationToken::new();
    let harness_token = token.clone();
    let mut run = tokio::spawn(async move {
        ClaudeCodeHarness::with_binary(bin.to_string_lossy())
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

#[tokio::test]
async fn normal_exit_returns_when_grandchild_keeps_stdout_open() {
    let _guard = KILL_TREE_TEST_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let child_pid = dir.path().join("child.pid");
    let bin = inherited_stdout_script(&dir, &child_pid);

    let started = Instant::now();
    let outcome = ClaudeCodeHarness::with_binary(bin.to_string_lossy())
        .run(base_job("prompt"), CancellationToken::new())
        .await
        .unwrap();

    assert!(
        started.elapsed() < Duration::from_secs(5),
        "run waited too long for inherited stdout EOF"
    );
    assert_eq!(outcome.text, "captured before exit");
    assert_pid_dead(&child_pid);
}

#[tokio::test]
#[ignore = "requires a real Claude Code install and configured auth"]
async fn real_claude_smoke_available_only() {
    assert!(ClaudeCodeHarness::new().available().await);
}

#[tokio::test]
#[ignore = "requires a real Claude Code install and configured auth; verifies read-only headless behavior"]
async fn real_claude_smoke_read_only_headless() {
    let mut job = base_job("Reply with exactly: vyane-read-only-ok");
    job.sandbox = Sandbox::ReadOnly;
    let outcome = ClaudeCodeHarness::new()
        .run(job, CancellationToken::new())
        .await
        .unwrap();
    assert!(outcome.text.contains("vyane-read-only-ok"));
}

#[tokio::test]
#[ignore = "requires a real Claude Code install and configured auth; verifies full sandbox opt-in behavior"]
async fn real_claude_smoke_full_headless() {
    let mut job = base_job("Reply with exactly: vyane-full-ok");
    job.sandbox = Sandbox::Full;
    let outcome = ClaudeCodeHarness::new()
        .run(job, CancellationToken::new())
        .await
        .unwrap();
    assert!(outcome.text.contains("vyane-full-ok"));
}

#[tokio::test]
#[ignore = "requires a real Codex CLI install and configured auth"]
async fn real_codex_smoke_available_only() {
    assert!(CodexCliHarness::new().available().await);
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
    write_script(dir, "kill-tree-cli", &body)
}

fn inherited_stdout_script(dir: &TempDir, child_pid: &Path) -> PathBuf {
    let body = format!(
        r#"#!/bin/sh
set -eu
printf '%s\n' '{{"result":"captured before exit"}}'
(
  trap '' TERM
  sleep 60
) &
printf '%s\n' "$!" > {child_pid}
exit 0
"#,
        child_pid = shell_quote(child_pid),
    );
    write_script(dir, "inherited-stdout-cli", &body)
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
        if let Some(pid) = read_pid(path) {
            if pid_is_running(&pid) {
                return pid;
            }
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
