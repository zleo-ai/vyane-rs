//! Fake-process contract for [`vyane_harness::GenericCliHarness`].
//!
//! The child is always a local script. These tests do not invoke a model API
//! or read stored credentials.

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use vyane_core::HarnessSpawnAuthority;
use vyane_core::env::EnvPolicy;
use vyane_core::error::ErrorKind;
use vyane_core::target::{AuthMaterial, AuthStyle, Endpoint, ModelId, Protocol, Sandbox, Secret};
use vyane_core::task::GenParams;
use vyane_core::traits::{Harness, HarnessExecutionContext, HarnessJob};
use vyane_harness::GenericCliHarness;

#[cfg(target_os = "linux")]
use vyane_core::PinnedWorkdir;

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

fn read_nul_args(path: &Path) -> Vec<String> {
    let bytes = fs::read(path).unwrap();
    if bytes.is_empty() {
        return Vec::new();
    }
    let mut parts: Vec<String> = bytes
        .split(|byte| *byte == 0)
        .map(|chunk| String::from_utf8(chunk.to_vec()).unwrap())
        .collect();
    if bytes.last() == Some(&0) {
        parts.pop();
    }
    parts
}

fn argv_and_answer_script(dir: &TempDir, argv_file: &Path, env_file: &Path) -> PathBuf {
    let body = format!(
        "#!/bin/sh\nset -eu\nARGV_FILE={argv}\nENV_FILE={env}\n: > \"$ARGV_FILE\"\nfor arg in \"$@\"; do\n  printf '%s\\0' \"$arg\" >> \"$ARGV_FILE\"\ndone\nenv | sort > \"$ENV_FILE\"\nprintf '%s\\n' '{json}'\nprintf '%s\\n' 'plain line'\n",
        argv = shell_quote(argv_file),
        env = shell_quote(env_file),
        json = r#"{"session_id":"native-1","usage":{"input_tokens":9,"output_tokens":4}}"#,
    );
    write_script(dir, "fake-generic", &body)
}

#[tokio::test]
async fn one_shot_substitutes_the_template_and_keeps_stdout_raw() {
    let dir = TempDir::new().unwrap();
    let argv_file = dir.path().join("argv");
    let env_file = dir.path().join("env");
    let bin = argv_and_answer_script(&dir, &argv_file, &env_file);
    let workdir = dir.path().join("work");
    fs::create_dir(&workdir).unwrap();

    let mut job = base_job("--real prompt\nline2");
    job.workdir = Some(workdir.clone());
    job.sandbox = Sandbox::Write;
    job.resume = Some("real-id".into());
    job.endpoint = Some(Endpoint {
        base_url: "https://endpoint.example/ignored".into(),
        auth: Some(AuthMaterial {
            style: AuthStyle::Bearer,
            secret: Secret::new("must-not-matter"),
        }),
    });
    job.env = job.env.inject("GENERIC_VISIBLE", "present");
    job.params
        .extra
        .insert("model".into(), serde_json::json!("llama3"));
    job.params
        .extra
        .insert("task".into(), serde_json::json!("INJECTED"));
    job.params
        .extra
        .insert("workdir".into(), serde_json::json!("/evil/dir"));
    job.params
        .extra
        .insert("sandbox".into(), serde_json::json!("danger-full-access"));
    job.params
        .extra
        .insert("session_id".into(), serde_json::json!("hijacked-id"));
    job.params
        .extra
        .insert("note".into(), serde_json::json!("kept"));

    let template = [
        "--prompt",
        "{task}",
        "--workdir",
        "{workdir}",
        "--sandbox",
        "{sandbox}",
        "--session",
        "{session_id}",
        "--model",
        "{model}",
        "--note",
        "{note}",
    ];
    let harness =
        GenericCliHarness::with_args_template("deepseek", bin.to_string_lossy().as_ref(), template)
            .with_parent_env_for_tests(vec![
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("HOME".into(), "/tmp".into()),
                ("NOT_BASELINE".into(), "must-not-matter".into()),
            ]);
    assert_eq!(
        harness.kind(),
        vyane_core::HarnessKind::Other("deepseek".into())
    );

    let outcome = harness.run(job, CancellationToken::new()).await.unwrap();

    let expected_stdout = "{\"session_id\":\"native-1\",\"usage\":{\"input_tokens\":9,\"output_tokens\":4}}\nplain line\n";
    assert_eq!(outcome.text, expected_stdout);
    assert!(outcome.text.contains("session_id"));
    assert!(outcome.text.contains("input_tokens"));
    assert_eq!(outcome.native_session_id, None);
    assert_eq!(outcome.usage, None);
    assert_eq!(outcome.exit_code, 0);

    let argv = read_nul_args(&argv_file);
    let workdir_text = workdir.to_string_lossy().into_owned();
    assert_eq!(
        argv,
        vec![
            "--prompt".to_string(),
            "--real prompt\nline2".to_string(),
            "--workdir".to_string(),
            workdir_text,
            "--sandbox".to_string(),
            "write".to_string(),
            "--session".to_string(),
            "real-id".to_string(),
            "--model".to_string(),
            "llama3".to_string(),
            "--note".to_string(),
            "kept".to_string(),
        ]
    );
    let rendered = argv.join("\n");
    for forbidden in [
        "INJECTED",
        "/evil/dir",
        "danger-full-access",
        "hijacked-id",
        "must-not-matter",
        "https://endpoint.example/ignored",
    ] {
        assert!(!rendered.contains(forbidden), "{forbidden} reached argv");
    }

    let child_env = fs::read_to_string(&env_file).unwrap();
    assert!(
        child_env
            .lines()
            .any(|line| line == "GENERIC_VISIBLE=present"),
        "job env injection did not reach the child"
    );
    for forbidden in [
        "must-not-matter",
        "NOT_BASELINE",
        "https://endpoint.example/ignored",
        "endpoint.example",
    ] {
        assert!(
            !child_env.contains(forbidden),
            "{forbidden} reached the child environment"
        );
    }
}

#[tokio::test]
async fn empty_stdout_is_a_successful_empty_answer() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(&dir, "fake-generic-empty", "#!/bin/sh\nexit 0\n");
    let harness = GenericCliHarness::new("empty-cli", bin.to_string_lossy().as_ref());
    let outcome = harness
        .run(base_job("hello"), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(outcome.text, "");
    assert_eq!(outcome.native_session_id, None);
    assert_eq!(outcome.usage, None);
    assert_eq!(outcome.exit_code, 0);
}

#[tokio::test]
async fn non_zero_exit_is_harness_failed() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(
        &dir,
        "fake-generic-fail",
        "#!/bin/sh\nprintf '%s\\n' 'generic failed' >&2\nexit 3\n",
    );
    let err = GenericCliHarness::with_args_template(
        "deepseek",
        bin.to_string_lossy().as_ref(),
        ["{task}"],
    )
    .run(base_job("hello"), CancellationToken::new())
    .await
    .unwrap_err();
    assert_eq!(err.kind, ErrorKind::HarnessFailed);
    assert!(err.message.contains("3"), "{}", err.message);
    assert!(err.message.contains("generic failed"), "{}", err.message);
    assert!(err.message.contains("deepseek"), "{}", err.message);
}

#[tokio::test]
async fn missing_binary_is_spawn_failed() {
    let dir = TempDir::new().unwrap();
    let err = GenericCliHarness::new("deepseek", dir.path().join("missing").to_string_lossy())
        .run(base_job("hello"), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::SpawnFailed);
}

#[tokio::test]
async fn available_checks_the_caller_command_without_running_it() {
    let dir = TempDir::new().unwrap();
    let bin = write_script(&dir, "fake-generic-present", "#!/bin/sh\nexit 0\n");
    assert!(
        GenericCliHarness::new("deepseek", bin.to_string_lossy().as_ref())
            .available()
            .await
    );
    assert!(
        !GenericCliHarness::new("deepseek", dir.path().join("missing").to_string_lossy())
            .available()
            .await
    );
}

#[tokio::test]
async fn stream_and_spawn_authority_fail_closed_without_starting_the_process() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let body = format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker));
    let bin = write_script(&dir, "fake-generic-closed", &body);
    let harness = GenericCliHarness::new("deepseek", bin.to_string_lossy().as_ref());

    let stream = harness
        .run_stream(
            base_job("stream"),
            CancellationToken::new(),
            Box::new(|_| {}),
        )
        .await
        .unwrap_err();
    assert_eq!(stream.kind, ErrorKind::Unsupported);
    assert!(stream.message.contains("deepseek"), "{}", stream.message);

    let context =
        HarnessExecutionContext::with_spawn_authority(HarnessSpawnAuthority::new(|| true));
    let scoped = GenericCliHarness::new("deepseek", bin.to_string_lossy().as_ref())
        .run_scoped(base_job("scoped"), context, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(scoped.kind, ErrorKind::Unsupported);
    assert!(scoped.message.contains("deepseek"), "{}", scoped.message);
    assert!(!marker.exists());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn pinned_workdir_fails_closed_without_starting_the_process() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("ran");
    let body = format!("#!/bin/sh\nprintf ran > {}\n", shell_quote(&marker));
    let bin = write_script(&dir, "fake-generic-pinned", &body);
    let pinned = PinnedWorkdir::open(dir.path()).unwrap();
    let context = HarnessExecutionContext::with_pinned_workdir(pinned);

    let err = GenericCliHarness::new("deepseek", bin.to_string_lossy().as_ref())
        .run_scoped(base_job("pinned"), context, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    assert!(err.message.contains("pinned"), "{}", err.message);
    assert!(!marker.exists());
}
