//! Fake-process contract for [`vyane_harness::GrokBuildHarness`].
//!
//! The child is always a local script. These tests do not invoke a Grok
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
use vyane_core::task::{Effort, GenParams, HarnessLifecycleReporter};
use vyane_core::traits::{Harness, HarnessExecutionContext, HarnessJob};
use vyane_harness::GrokBuildHarness;

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
    job.sandbox = Sandbox::Write;
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

fn contains_window(lines: &[String], expected: &[&str]) -> bool {
    lines.windows(expected.len()).any(|window| {
        window
            .iter()
            .map(String::as_str)
            .eq(expected.iter().copied())
    })
}

fn parent_env() -> Vec<(String, String)> {
    vec![
        ("PATH".to_string(), "/usr/bin:/bin".to_string()),
        ("HOME".to_string(), "/tmp/grok-home".to_string()),
        ("XAI_API_KEY".to_string(), "parent-metered-key".to_string()),
        ("OTHER_PARENT".to_string(), "not-baseline".to_string()),
    ]
}

fn harness(bin: &Path) -> GrokBuildHarness {
    GrokBuildHarness::with_binary_and_parent_env_for_tests(bin.to_string_lossy(), parent_env())
}

const SUCCESS_JSON: &str = r#"{
  "text": "grok final",
  "stopReason": "end_turn",
  "sessionId": "grok-session",
  "thought": "hidden",
  "usage": {
    "input_tokens": 10,
    "output_tokens": 4,
    "reasoning_tokens": 3,
    "total_tokens": 20,
    "cache_read_input_tokens": 7,
    "cache_creation_input_tokens": 2
  },
  "total_cost_usd": 0.01,
  "modelUsage": {"grok-4.7-build": {"input_tokens": 10}}
}"#;

#[tokio::test]
async fn fake_process_maps_one_json_result_from_stdin_for_write_and_full() {
    let dir = TempDir::new().unwrap();
    let workdir = dir.path().join("work");
    fs::create_dir(&workdir).unwrap();
    let json_path = dir.path().join("result.json");
    fs::write(&json_path, SUCCESS_JSON).unwrap();
    let argv_file = dir.path().join("argv.txt");
    let env_file = dir.path().join("env.txt");
    let stdin_file = dir.path().join("stdin.txt");
    let cwd_marker = workdir.join("cwd-marker");
    let body = format!(
        r#"#!/bin/sh
set -eu
: > {argv}
for arg in "$@"; do
  printf '%s\n' "$arg" >> {argv}
done
env | sort > {env}
cat > {stdin}
printf '%s\n' 'banner'
printf '%s\n' 'ParseFile {{ path: x }}' >&2
cat {json}
printf ran > {marker}
"#,
        argv = shell_quote(&argv_file),
        env = shell_quote(&env_file),
        stdin = shell_quote(&stdin_file),
        json = shell_quote(&json_path),
        marker = shell_quote(&cwd_marker),
    );
    let bin = write_script(&dir, "fake-grok", &body);

    for sandbox in [Sandbox::Write, Sandbox::Full] {
        let _ = fs::remove_file(&argv_file);
        let _ = fs::remove_file(&stdin_file);
        let _ = fs::remove_file(&cwd_marker);
        let mut job = spawning_job("--always-approve\nkept");
        job.sandbox = sandbox;
        job.model = ModelId::new("grok-4.7");
        job.params.effort = Some(Effort::High);
        job.resume = Some("grok-resume".into());
        job.workdir = Some(workdir.clone());
        job.env = EnvPolicy::scrubbed()
            .inject("EXPLICIT_CHILD", "yes")
            .inject("XAI_API_KEY", "injected-metered-key");

        let outcome = harness(&bin)
            .run(job, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(outcome.text, "grok final");
        assert_eq!(outcome.native_session_id.as_deref(), Some("grok-session"));
        let usage = outcome.usage.unwrap();
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.output_tokens, 4);
        assert_eq!(usage.reasoning_tokens, Some(3));
        assert_eq!(usage.cached_input_tokens, Some(7));
        assert_eq!(outcome.exit_code, 0);
        assert!(cwd_marker.is_file());

        let argv = read_lines(&argv_file);
        assert!(contains_window(&argv, &["--prompt-file", "/dev/stdin"]));
        assert!(contains_window(&argv, &["--output-format", "json"]));
        assert!(contains_window(&argv, &["-m", "grok-4.7"]));
        assert!(contains_window(&argv, &["--reasoning-effort", "high"]));
        assert!(contains_window(&argv, &["--resume", "grok-resume"]));
        for forbidden in [
            "--sandbox",
            "--permission-mode",
            "--always-approve",
            "--model",
            "XAI_API_KEY",
            "parent-metered-key",
            "injected-metered-key",
            "--always-approve\nkept",
        ] {
            assert!(
                !argv.iter().any(|arg| arg == forbidden),
                "{forbidden} in {argv:?}"
            );
        }
        assert_eq!(
            fs::read_to_string(&stdin_file).unwrap(),
            "--always-approve\nkept"
        );

        let child_env = fs::read_to_string(&env_file).unwrap();
        assert!(child_env.contains("HOME=/tmp/grok-home\n"));
        assert!(child_env.contains("EXPLICIT_CHILD=yes\n"));
        assert!(!child_env.contains("XAI_API_KEY="));
        assert!(!child_env.contains("parent-metered-key"));
        assert!(!child_env.contains("injected-metered-key"));
        assert!(!child_env.contains("OTHER_PARENT="));
    }
}

#[tokio::test]
async fn read_only_does_not_spawn_under_a_metered_parent_key() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("spawned");
    let body = format!("#!/bin/sh\nprintf spawned > {}\n", shell_quote(&marker));
    let bin = write_script(&dir, "hostile-grok", &body);
    let mut job = base_job("do not run");
    job.endpoint = Some(Endpoint {
        base_url: "https://endpoint.example".into(),
        auth: Some(AuthMaterial {
            style: AuthStyle::Bearer,
            secret: Secret::new("must-not-matter"),
        }),
    });
    job.params
        .extra
        .insert("sandbox_profile".into(), serde_json::json!("read-only"));

    let err = harness(&bin)
        .run(job, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    assert!(err.message.contains("read-only"), "{}", err.message);
    assert!(err.message.contains("not started"), "{}", err.message);
    assert!(!err.message.contains("must-not-matter"));
    assert!(!err.message.contains("https://endpoint.example"));
    assert!(!marker.exists());
}

#[tokio::test]
async fn endpoint_credential_does_not_spawn() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("spawned");
    let body = format!("#!/bin/sh\nprintf spawned > {}\n", shell_quote(&marker));
    let bin = write_script(&dir, "endpoint-grok", &body);
    let mut job = spawning_job("do not retarget");
    job.endpoint = Some(Endpoint {
        base_url: String::new(),
        auth: Some(AuthMaterial {
            style: AuthStyle::Bearer,
            secret: Secret::new("must-not-matter"),
        }),
    });

    let err = harness(&bin)
        .run(job, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    assert!(err.message.contains("XAI_API_KEY"), "{}", err.message);
    assert!(!err.message.contains("must-not-matter"));
    assert!(!marker.exists());
}

#[tokio::test]
async fn exit_zero_cancelled_document_is_harness_failed() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(
        &dir,
        "fake-grok-cancelled",
        r#"#!/bin/sh
set -eu
cat >/dev/null
printf '%s\n' '{"text":"partial answer","stopReason":"cancelled","sessionId":"s"}'
exit 0
"#,
    );

    let err = harness(&bin)
        .run(spawning_job("fail"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(
        err.message.contains("stopReason=cancelled"),
        "{}",
        err.message
    );
}

#[tokio::test]
async fn exit_zero_without_a_document_is_not_success() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(
        &dir,
        "fake-grok-logs",
        r#"#!/bin/sh
set -eu
cat >/dev/null
printf '%s\n' 'error: auth failed'
printf '%s\n' 'ParseFile { path: x }' >&2
exit 0
"#,
    );

    let err = harness(&bin)
        .run(spawning_job("fail closed"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(
        err.message.contains("no JSON result document"),
        "{}",
        err.message
    );
    assert!(
        err.message.contains("error: auth failed"),
        "{}",
        err.message
    );
}

#[tokio::test]
async fn nonzero_exit_is_harness_failed() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(
        &dir,
        "fake-grok-status",
        "#!/bin/sh\ncat >/dev/null\nprintf '%s\\n' 'grok failed' >&2\nexit 23\n",
    );

    let err = harness(&bin)
        .run(spawning_job("fail"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("23"), "{}", err.message);
    assert!(err.message.contains("grok failed"), "{}", err.message);
}

#[tokio::test]
async fn read_only_missing_binary_is_unsupported_before_spawn() {
    let mut job = base_job("fail");
    job.sandbox = Sandbox::ReadOnly;
    let err = GrokBuildHarness::with_binary_and_parent_env_for_tests("/no/such/grok", parent_env())
        .run(job, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    assert!(err.message.contains("not started"), "{}", err.message);
}

#[tokio::test]
async fn missing_binary_is_spawn_failed() {
    let dir = TempDir::new().unwrap();
    let err = harness(&dir.path().join("missing"))
        .run(spawning_job("fail"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::SpawnFailed);
}

#[tokio::test]
async fn available_probes_the_configured_binary_without_running_it() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let body = format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker));
    let bin = write_script(&dir, "present-grok", &body);

    assert!(harness(&bin).available().await);
    assert!(!harness(&dir.path().join("missing")).available().await);
    assert!(!marker.exists());
}

#[tokio::test]
async fn stream_authority_and_lifecycle_fail_closed_without_starting_the_process() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let body = format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker));
    let bin = write_script(&dir, "fake-grok-closed", &body);

    let stream = harness(&bin)
        .run_stream(
            spawning_job("stream"),
            CancellationToken::new(),
            Box::new(|_| {}),
        )
        .await
        .unwrap_err();
    assert_eq!(stream.kind, ErrorKind::Unsupported);
    assert!(stream.message.contains("grok-build"), "{}", stream.message);

    let context =
        HarnessExecutionContext::with_spawn_authority(HarnessSpawnAuthority::new(|| true));
    let scoped = harness(&bin)
        .run_scoped(spawning_job("scoped"), context, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(scoped.kind, ErrorKind::Unsupported);
    assert!(scoped.message.contains("grok-build"), "{}", scoped.message);

    let mut gated = spawning_job("gated");
    gated.harness_lifecycle_reporter = Some(HarnessLifecycleReporter::new(|_| Ok(())));
    let gate = harness(&bin)
        .run(gated, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(gate.kind, ErrorKind::Unsupported);
    assert!(gate.message.contains("stdin"), "{}", gate.message);
    assert!(!marker.exists());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn pinned_workdir_fails_closed_without_starting_the_process() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let body = format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker));
    let bin = write_script(&dir, "fake-grok-pinned", &body);
    let pinned = PinnedWorkdir::open(dir.path()).unwrap();
    let context = HarnessExecutionContext::with_pinned_workdir(pinned);

    let err = harness(&bin)
        .run_scoped(spawning_job("pinned"), context, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    assert!(err.message.contains("pinned"), "{}", err.message);
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

    let err = harness(&bin)
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
    let bin_text = bin.to_string_lossy().to_string();
    let mut run = tokio::spawn(async move {
        harness(Path::new(&bin_text))
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
    write_script(dir, "grok-kill-tree", &body)
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
