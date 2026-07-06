use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use vyane_core::{
    CancellationToken, EnvPolicy, ErrorKind, Harness, HarnessJob, HarnessKind, HarnessOutcome,
    Result, Sandbox, VyaneError,
};

#[derive(Debug, Clone)]
pub struct CliHarness {
    kind: HarnessKind,
    binary: &'static str,
}

impl CliHarness {
    pub fn for_kind(kind: HarnessKind) -> Result<Self> {
        let binary = match kind {
            HarnessKind::ClaudeCode => "claude",
            HarnessKind::CodexCli => "codex",
            HarnessKind::OpenCode | HarnessKind::Other(_) => {
                return Err(VyaneError::new(
                    ErrorKind::Unsupported,
                    format!("unsupported CLI harness `{kind}`"),
                ));
            }
        };
        Ok(Self { kind, binary })
    }

    fn argv(&self, job: &HarnessJob) -> Vec<String> {
        match self.kind {
            HarnessKind::ClaudeCode => claude_argv(job),
            HarnessKind::CodexCli => codex_argv(job),
            HarnessKind::OpenCode | HarnessKind::Other(_) => Vec::new(),
        }
    }

    async fn run_inner(
        &self,
        job: HarnessJob,
        cancel: CancellationToken,
    ) -> Result<HarnessOutcome> {
        let started = Instant::now();
        let env = job.env.build(std::env::vars());
        let mut command = Command::new(self.binary);
        command
            .args(self.argv(&job))
            .env_clear()
            .envs(env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        put_child_in_process_group(&mut command);
        if let Some(workdir) = &job.workdir {
            command.current_dir(workdir);
        }

        let mut child = command.spawn().map_err(|e| {
            VyaneError::with_source(
                ErrorKind::SpawnFailed,
                format!("failed to spawn `{}` harness", self.kind),
                e,
            )
        })?;

        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(job.prompt.as_bytes()).await.map_err(|e| {
                VyaneError::with_source(
                    ErrorKind::HarnessFailed,
                    format!("failed to write prompt to `{}` harness", self.kind),
                    e,
                )
            })?;
        }

        let output = wait_with_controls(child, job.timeout, cancel).await?;
        let duration = started.elapsed();
        if !output.status.success() {
            return Err(VyaneError::new(
                ErrorKind::HarnessFailed,
                format!(
                    "`{}` harness exited with status {}: {}",
                    self.kind,
                    output.status,
                    first_nonempty_line(&String::from_utf8_lossy(&output.stderr))
                ),
            ));
        }

        Ok(HarnessOutcome {
            text: parse_harness_text(&output.stdout),
            native_session_id: None,
            usage: None,
            exit_code: output.status.code().unwrap_or(0),
            duration,
        })
    }
}

#[async_trait]
impl Harness for CliHarness {
    fn kind(&self) -> HarnessKind {
        self.kind.clone()
    }

    async fn available(&self) -> bool {
        command_exists(self.binary)
    }

    async fn run(&self, job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome> {
        self.run_inner(job, cancel).await
    }
}

#[derive(Debug, Clone)]
pub struct EnvInjectedHarness {
    inner: CliHarness,
    env: EnvPolicy,
}

impl EnvInjectedHarness {
    pub fn new(inner: CliHarness, env: EnvPolicy) -> Self {
        Self { inner, env }
    }
}

#[async_trait]
impl Harness for EnvInjectedHarness {
    fn kind(&self) -> HarnessKind {
        self.inner.kind()
    }

    async fn available(&self) -> bool {
        self.inner.available().await
    }

    async fn run(&self, mut job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome> {
        job.env = self.env.clone();
        self.inner.run(job, cancel).await
    }
}

async fn wait_with_controls(
    child: tokio::process::Child,
    timeout: Option<Duration>,
    cancel: CancellationToken,
) -> Result<std::process::Output> {
    let child_id = child.id();
    let wait = child.wait_with_output();
    tokio::pin!(wait);

    match timeout {
        Some(duration) => {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    terminate_process_group(child_id).await;
                    Err(VyaneError::cancelled())
                },
                elapsed = tokio::time::timeout(duration, &mut wait) => {
                    match elapsed {
                        Ok(output) => output.map_err(VyaneError::from),
                        Err(_) => {
                            terminate_process_group(child_id).await;
                            Err(VyaneError::new(
                                ErrorKind::Timeout,
                                format!("harness exceeded timeout of {}ms", duration.as_millis()),
                            ))
                        },
                    }
                }
            }
        }
        None => {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    terminate_process_group(child_id).await;
                    Err(VyaneError::cancelled())
                },
                output = &mut wait => output.map_err(VyaneError::from),
            }
        }
    }
}

#[cfg(unix)]
fn put_child_in_process_group(command: &mut Command) {
    command.process_group(0);
}

#[cfg(not(unix))]
fn put_child_in_process_group(_command: &mut Command) {}

#[cfg(unix)]
async fn terminate_process_group(child_id: Option<u32>) {
    let Some(pid) = child_id else {
        return;
    };
    let group = format!("-{pid}");
    let _ = Command::new("kill")
        .arg("-TERM")
        .arg(&group)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let _ = Command::new("kill")
        .arg("-KILL")
        .arg(group)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
}

#[cfg(not(unix))]
async fn terminate_process_group(_child_id: Option<u32>) {}

fn claude_argv(job: &HarnessJob) -> Vec<String> {
    let mut args = vec![
        "-p".to_string(),
        "--model".to_string(),
        job.model.as_str().to_string(),
        "--output-format".to_string(),
        "text".to_string(),
    ];
    match job.sandbox {
        Sandbox::ReadOnly => args.extend(["--permission-mode".to_string(), "plan".to_string()]),
        Sandbox::Write => args.extend(["--permission-mode".to_string(), "acceptEdits".to_string()]),
        Sandbox::Full => {
            args.extend([
                "--permission-mode".to_string(),
                "bypassPermissions".to_string(),
            ]);
        }
    }
    if let Some(resume) = &job.resume {
        args.extend(["--resume".to_string(), resume.clone()]);
    }
    args
}

fn codex_argv(job: &HarnessJob) -> Vec<String> {
    let mut args = vec![
        "exec".to_string(),
        "--model".to_string(),
        job.model.as_str().to_string(),
        "--json".to_string(),
        "--sandbox".to_string(),
        match job.sandbox {
            Sandbox::ReadOnly => "read-only",
            Sandbox::Write => "workspace-write",
            Sandbox::Full => "danger-full-access",
        }
        .to_string(),
    ];
    if let Some(resume) = &job.resume {
        args.extend(["--resume".to_string(), resume.clone()]);
    }
    args
}

fn command_exists(binary: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|dir| executable_candidate(dir.join(binary)))
}

fn executable_candidate(path: PathBuf) -> bool {
    is_executable_file(&path)
}

#[cfg(unix)]
fn is_executable_file(path: &std::path::Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    path.is_file()
        && path
            .metadata()
            .map(|metadata| metadata.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable_file(path: &std::path::Path) -> bool {
    path.is_file()
}

fn parse_harness_text(stdout: &[u8]) -> String {
    let text = String::from_utf8_lossy(stdout).trim().to_string();
    if text.is_empty() {
        return text;
    }
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
        if let Some(answer) = value.get("text").and_then(|v| v.as_str()) {
            return answer.to_string();
        }
        if let Some(answer) = value.get("answer").and_then(|v| v.as_str()) {
            return answer.to_string();
        }
        if let Some(answer) = value.get("output").and_then(|v| v.as_str()) {
            return answer.to_string();
        }
    }
    text
}

fn first_nonempty_line(text: &str) -> String {
    text.lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("")
        .trim()
        .to_string()
}
