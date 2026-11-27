//! Fake-process contract for [`vyane_harness::OpencodeHarness`].
//!
//! The child is always a local script. These tests do not invoke an OpenCode
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
use vyane_harness::OpencodeHarness;

#[cfg(target_os = "linux")]
use vyane_core::PinnedWorkdir;

const ENDPOINT_CANARY: &str = "endpoint-canary-secret";
const PARENT_SECRET: &str = "parent-secret";

fn base_job(prompt: &str) -> HarnessJob {
    HarnessJob {
        prompt: prompt.to_string(),
        model: ModelId::new(""),
        protocol: Protocol::OpenaiChat,
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
        ("PATH".to_string(), "/usr/bin:/bin".to_string()),
        ("HOME".to_string(), "/tmp/opencode-home".to_string()),
        ("SOME_API_KEY".to_string(), PARENT_SECRET.to_string()),
    ]
}

fn harness(bin: &Path) -> OpencodeHarness {
    OpencodeHarness::with_binary_and_parent_env_for_tests(bin.to_string_lossy(), parent_env())
}

#[tokio::test]
async fn fake_process_maps_one_json_shot_for_every_sandbox() {
    let dir = TempDir::new().unwrap();
    let workdir = dir.path().join("workdir-marker");
    fs::create_dir(&workdir).unwrap();
    let json_path = dir.path().join("events.ndjson");
    fs::write(
        &json_path,
        "\
{\"type\":\"step_start\",\"sessionID\":\"ses_old\",\"part\":{\"type\":\"step-start\"}}\n\
{\"type\":\"text\",\"sessionID\":\"ses_123\",\"part\":{\"type\":\"text\",\"text\":\"OK<|assistant|>\"}}\n\
{\"type\":\"step_finish\",\"sessionID\":\"ses_old\",\"part\":{\"tokens\":{\"input\":1,\"output\":1,\"total\":2}}}\n\
{\"type\":\"step_finish\",\"sessionID\":\"ses_123\",\"part\":{\"tokens\":{\"input\":10,\"output\":2,\"total\":12}}}\n",
    )
    .unwrap();
    let argv_file = dir.path().join("argv.txt");
    let env_file = dir.path().join("env.txt");
    let stdin_file = dir.path().join("stdin.txt");
    let cwd_file = dir.path().join("cwd.txt");
    let marker = workdir.join("cwd-marker");
    let body = format!(
        r#"#!/bin/sh
: > {argv}
for arg in "$@"; do
  printf '%s\n' "$arg" >> {argv}
done
env | sort > {env}
cat > {stdin} || true
pwd > {cwd}
printf ran > {marker}
printf '%s\n' 'stdout-plain-line'
cat {json}
printf '%s\n' 'stderr-plain-line' >&2
"#,
        argv = shell_quote(&argv_file),
        env = shell_quote(&env_file),
        stdin = shell_quote(&stdin_file),
        cwd = shell_quote(&cwd_file),
        marker = shell_quote(&marker),
        json = shell_quote(&json_path),
    );
    let bin = write_script(&dir, "fake-opencode", &body);
    let expected_argv = [
        "run",
        "hello world",
        "--format",
        "json",
        "--model",
        "anthropic/gpt-4o",
        "--session",
        "s1",
        "--continue",
        "--title",
        "review",
    ];

    for sandbox in [Sandbox::ReadOnly, Sandbox::Write, Sandbox::Full] {
        let _ = fs::remove_file(&marker);
        let mut job = base_job("hello world");
        job.sandbox = sandbox;
        job.model = ModelId::new("gpt-4o");
        job.resume = Some("s1".into());
        job.workdir = Some(workdir.clone());
        job.endpoint = Some(Endpoint {
            base_url: "https://endpoint.example".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::XApiKey,
                secret: Secret::new(ENDPOINT_CANARY),
            }),
        });
        job.env = EnvPolicy::scrubbed().inject("OPENCODE_API_KEY", "kept-on-purpose");
        let extra = &mut job.params.extra;
        extra.insert(
            "model".into(),
            serde_json::Value::String("from-extra".into()),
        );
        extra.insert(
            "provider".into(),
            serde_json::Value::String("anthropic".into()),
        );
        extra.insert(
            "format".into(),
            serde_json::Value::String("stream-json".into()),
        );
        extra.insert(
            "owner_user_id".into(),
            serde_json::Value::String("alice".into()),
        );
        extra.insert("run_id".into(), serde_json::Value::String("disp-1".into()));
        extra.insert("continue".into(), serde_json::Value::Bool(true));
        extra.insert("title".into(), serde_json::Value::String("review".into()));

        let outcome = harness(&bin)
            .run(job, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(outcome.text, "stdout-plain-line\nOK\nstderr-plain-line");
        assert!(!outcome.text.contains("<|assistant|>"));
        assert!(!outcome.text.contains("step-start"));
        assert_eq!(outcome.native_session_id.as_deref(), Some("ses_123"));
        let usage = outcome.usage.unwrap();
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.output_tokens, 2);
        assert_eq!(usage.reasoning_tokens, None);
        assert_eq!(usage.cached_input_tokens, None);
        assert_eq!(outcome.exit_code, 0);
        assert!(marker.is_file());
        assert_eq!(
            fs::canonicalize(fs::read_to_string(&cwd_file).unwrap().trim()).unwrap(),
            fs::canonicalize(&workdir).unwrap()
        );
        assert_eq!(read_lines(&argv_file), expected_argv);
        assert_eq!(fs::read_to_string(&stdin_file).unwrap(), "");

        let child_env = fs::read_to_string(&env_file).unwrap();
        assert!(
            child_env.contains("HOME=/tmp/opencode-home\n"),
            "{child_env}"
        );
        assert!(
            child_env.contains("OPENCODE_API_KEY=kept-on-purpose\n"),
            "{child_env}"
        );
        assert!(!child_env.contains(PARENT_SECRET), "{child_env}");
        assert!(!child_env.contains(ENDPOINT_CANARY), "{child_env}");
        assert!(
            !child_env.contains("https://endpoint.example"),
            "{child_env}"
        );
        let argv_text = read_lines(&argv_file).join("\n");
        for forbidden in [
            "--sandbox",
            "--provider",
            "--owner-user-id",
            "--run-id",
            "alice",
            "disp-1",
            "from-extra",
            "stream-json",
            ENDPOINT_CANARY,
            "https://endpoint.example",
            PARENT_SECRET,
            "workdir-marker",
        ] {
            assert!(!argv_text.contains(forbidden), "{forbidden} in {argv_text}");
        }
    }
}

#[tokio::test]
async fn error_event_on_zero_exit_is_harness_failed() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(
        &dir,
        "fake-opencode-error",
        "#!/bin/sh\nprintf '%s\\n' '{\"type\":\"text\",\"sessionID\":\"s\",\"part\":{\"text\":\"partial\"}}'\nprintf '%s\\n' '{\"type\":\"event_error\",\"message\":\"event failed\"}'\nexit 0\n",
    );
    let err = harness(&bin)
        .run(base_job("hi"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("event failed"), "{}", err.message);
    assert!(!err.message.contains("partial"), "{}", err.message);
}

#[tokio::test]
async fn nonzero_exit_is_harness_failed() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(
        &dir,
        "fake-opencode-status",
        "#!/bin/sh\nprintf '%s\\n' '{\"type\":\"error\",\"error\":\"from-error\"}'\nprintf '%s\\n' 'tail-line'\nexit 23\n",
    );
    let err = harness(&bin)
        .run(base_job("hi"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("23"), "{}", err.message);
    assert!(err.message.contains("from-error"), "{}", err.message);
}

#[tokio::test]
async fn missing_binary_is_spawn_failed() {
    let dir = TempDir::new().unwrap();
    let err = harness(&dir.path().join("missing"))
        .run(base_job("hi"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::SpawnFailed);
}

#[tokio::test]
async fn available_probes_the_configured_binary_without_running_it() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let body = format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker));
    let bin = write_script(&dir, "present-opencode", &body);
    assert!(harness(&bin).available().await);
    assert!(!harness(&dir.path().join("missing")).available().await);
    assert!(!marker.exists());
}

#[tokio::test]
async fn stream_and_spawn_authority_fail_closed_without_starting_the_process() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let body = format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker));
    let bin = write_script(&dir, "fake-opencode-closed", &body);

    let stream = harness(&bin)
        .run_stream(
            base_job("stream"),
            CancellationToken::new(),
            Box::new(|_| {}),
        )
        .await
        .unwrap_err();
    assert_eq!(stream.kind, ErrorKind::Unsupported);
    assert!(stream.message.contains("opencode"), "{}", stream.message);
    assert!(stream.message.contains("streaming"), "{}", stream.message);

    let context =
        HarnessExecutionContext::with_spawn_authority(HarnessSpawnAuthority::new(|| true));
    let scoped = harness(&bin)
        .run_scoped(base_job("scoped"), context, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(scoped.kind, ErrorKind::Unsupported);
    assert!(scoped.message.contains("opencode"), "{}", scoped.message);
    assert!(!marker.exists());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn pinned_workdir_fails_closed_without_starting_the_process() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let body = format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker));
    let bin = write_script(&dir, "fake-opencode-pinned", &body);
    let pinned = PinnedWorkdir::open(dir.path()).unwrap();
    let context = HarnessExecutionContext::with_pinned_workdir(pinned);

    let err = harness(&bin)
        .run_scoped(base_job("pinned"), context, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    assert!(err.message.contains("pinned"), "{}", err.message);
    assert!(!marker.exists());
}

#[tokio::test]
async fn timeout_is_classified() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(&dir, "sleep-opencode", "#!/bin/sh\nsleep 30\n");
    let mut job = base_job("hang");
    job.timeout = Some(Duration::from_secs(1));
    let err = harness(&bin)
        .run(job, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Timeout);
    assert!(err.message.contains("timed out"), "{}", err.message);
}

#[tokio::test]
async fn cancellation_is_classified() {
    let dir = TempDir::new().unwrap();
    let ready = dir.path().join("ready");
    let body = format!(
        "#!/bin/sh\nprintf ready > {}\nsleep 30\n",
        shell_quote(&ready)
    );
    let bin = write_script(&dir, "cancel-opencode", &body);
    let token = CancellationToken::new();
    let harness_token = token.clone();
    let bin_text = bin.to_string_lossy().to_string();
    let run = tokio::spawn(async move {
        harness(Path::new(&bin_text))
            .run(base_job("hang"), harness_token)
            .await
    });

    let start = Instant::now();
    loop {
        if ready.exists() {
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("ready file was not written");
        }
        if run.is_finished() {
            panic!("harness exited before the child was ready: {:?}", run.await);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    token.cancel();
    let err = run.await.unwrap().unwrap_err();
    assert_eq!(err.kind, ErrorKind::Cancelled);
}
