//! [`Harness`] for the **Codex CLI**, invoked non-interactively via `exec`.
//!
//! Command shape (verified against `codex --help`, `codex exec --help`, and
//! `codex exec resume --help`, CLI 0.142.x):
//!
//! ```text
//! codex --ask-for-approval never [--model M] --sandbox <mode> [-c ...]
//!       exec --json -o <last-message-file> --skip-git-repo-check
//!       --ignore-user-config -C <workdir> -- <prompt>
//! ```
//!
//! Resume shape:
//!
//! ```text
//! codex --ask-for-approval never [--model M] --sandbox <mode> [-c ...]
//!       exec resume --json -o <last-message-file> --skip-git-repo-check
//!       --ignore-user-config <session-id> -- <prompt>
//! ```
//!
//! * `--json` emits JSONL events (parsed for session id + usage); `-o/--output-
//!   last-message` writes the final answer to a file we read (authoritative
//!   answer text, not the event stream).
//! * `--sandbox` mapping: see [`sandbox_value`]
//!   (`read-only`/`workspace-write`/`danger-full-access`).
//! * `--sandbox` is placed before `exec`: `codex exec resume` does not accept
//!   the flag, while the top-level `codex --sandbox ... exec resume ...` form
//!   is accepted and preserves the sandbox contract on resumed runs.
//! * Resumed runs omit both `-C <workdir>` and process cwd override: Codex
//!   reuses the cwd recorded in the native session, and applying a second cwd
//!   at process spawn would make argv and runtime behavior disagree.
//! * `--ask-for-approval never` keeps non-interactive runs headless without
//!   granting more filesystem access than the selected sandbox.
//! * Custom endpoint: defined **inline** via `-c model_providers.<name>.*`
//!   config overrides (base URL / wire API / key-env name) plus
//!   `-c model_provider=<name>` to select it — never by writing the user's
//!   global `~/.codex/config.toml`. The API key value is injected into the
//!   child env under the name Codex is told to read (`env_key`). We add
//!   `--ignore-user-config` so the run is fully self-contained.

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use vyane_core::error::{ErrorKind, Result, VyaneError};
use vyane_core::target::{Endpoint, HarnessKind, Protocol, Sandbox};
use vyane_core::traits::{Harness, HarnessJob, HarnessOutcome};

use crate::parse::{combine_codex, parse_codex_events};
use crate::spawn::{Termination, env_key_list, materialize_env, parent_env_snapshot, run_capture};

/// The Codex binary name (resolved via `PATH` at spawn time).
const BIN: &str = "codex";

/// Config key/provider name we define inline for a custom endpoint. A safe
/// identifier (no dots) so the `-c model_providers.<name>.*` dotted path parses.
const PROVIDER_NAME: &str = "vyane";

/// Env var Codex reads for the OpenAI-compatible API key. We tell Codex to read
/// the key from this name (`env_key`) and inject the value under it.
const ENV_API_KEY: &str = "OPENAI_API_KEY";

/// `Harness` adapter for the Codex CLI.
#[derive(Debug, Clone)]
pub struct CodexCliHarness {
    bin: String,
}

impl CodexCliHarness {
    pub fn new() -> Self {
        Self { bin: BIN.into() }
    }

    /// Construct a harness with an explicit binary path/name.
    ///
    /// This is primarily useful for tests with fake CLI scripts; production
    /// callers normally use [`Self::new`] and resolve `codex` through `PATH`.
    pub fn with_binary(bin: impl Into<String>) -> Self {
        Self { bin: bin.into() }
    }
}

impl Default for CodexCliHarness {
    fn default() -> Self {
        Self::new()
    }
}

/// Map a [`Sandbox`] level onto the Codex `--sandbox` value.
///
/// Verified against `codex exec --help`: possible values are `read-only`,
/// `workspace-write`, `danger-full-access`. Vyane's `ReadOnly`/`Write`/`Full`
/// map straight onto them; the mapping never grants more than asked.
fn sandbox_value(sandbox: Sandbox) -> &'static str {
    match sandbox {
        Sandbox::ReadOnly => "read-only",
        Sandbox::Write => "workspace-write",
        Sandbox::Full => "danger-full-access",
    }
}

/// Map a [`Protocol`] to the Codex `wire_api` value for a custom provider.
/// Codex speaks either the OpenAI Chat Completions API (`chat`) or the OpenAI
/// Responses API (`responses`).
fn wire_api_for(protocol: Protocol) -> Result<&'static str> {
    match protocol {
        Protocol::OpenaiChat => Ok("chat"),
        Protocol::OpenaiResponses => Ok("responses"),
        Protocol::AnthropicMessages => Err(VyaneError::new(
            ErrorKind::Unsupported,
            "transport/protocol/harness combo unsupported: cli_wrap / anthropic_messages / codex-cli",
        )),
        _ => Err(VyaneError::new(
            ErrorKind::Unsupported,
            format!("unsupported codex-cli wire protocol {protocol}"),
        )),
    }
}

/// Emit the `-c` config-override tokens that define the custom provider inline
/// and select it. Only emitted when `endpoint` carries a base URL; otherwise
/// the harness authenticates natively and we add nothing.
///
/// `protocol` decides the `wire_api`. TOML values are JSON-quoted so a value
/// containing a quote can't break the `-c` token.
pub(crate) fn provider_config_args(
    endpoint: Option<&Endpoint>,
    protocol: Protocol,
) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let wire = wire_api_for(protocol)?;

    let Some(ep) = endpoint else {
        return Ok(out);
    };
    if ep.base_url.is_empty() {
        return Ok(out);
    }

    let mut kv = |key: &str, value: &str| {
        out.push("-c".to_string());
        // serde_json::to_string on a &str yields a valid TOML basic string.
        let quoted = serde_json::to_string(value).unwrap_or_else(|_| format!("\"{value}\""));
        out.push(format!("{key}={quoted}"));
    };

    kv(
        &format!("model_providers.{PROVIDER_NAME}.name"),
        PROVIDER_NAME,
    );
    kv(
        &format!("model_providers.{PROVIDER_NAME}.base_url"),
        &ep.base_url,
    );
    kv(&format!("model_providers.{PROVIDER_NAME}.wire_api"), wire);
    // Tell Codex which env var holds the API key (we inject its value below).
    if ep.auth.is_some() {
        kv(
            &format!("model_providers.{PROVIDER_NAME}.env_key"),
            ENV_API_KEY,
        );
    }
    // Select the provider we just defined.
    kv("model_provider", PROVIDER_NAME);

    Ok(out)
}

/// Construct the full Codex argv (excluding the program name) for a job.
///
/// `last_message_path` is where the CLI writes the final answer (`-o`); the
/// harness reads it back. `protocol` drives the custom-provider `wire_api`.
///
/// Pure function so an argv-echo fake CLI can assert it exactly.
pub(crate) fn build_argv(
    job: &HarnessJob,
    last_message_path: &str,
    protocol: Protocol,
) -> Result<Vec<String>> {
    let mut args: Vec<String> = Vec::new();

    let resuming = job.resume.as_ref().map(|s| !s.is_empty()).unwrap_or(false);

    // Global options accepted before `exec`. Keeping model/sandbox/config here
    // makes fresh and resumed runs use one shape; in particular
    // `codex exec resume` rejects `--sandbox`, but top-level `codex --sandbox
    // ... exec resume ...` accepts it.
    args.push("--ask-for-approval".into());
    args.push("never".into());

    if !job.model.as_str().is_empty() {
        args.push("--model".into());
        args.push(job.model.as_str().to_string());
    }

    args.push("--sandbox".into());
    args.push(sandbox_value(job.sandbox).into());

    args.extend(provider_config_args(job.endpoint.as_ref(), protocol)?);

    args.push("exec".into());
    if resuming {
        args.push("resume".into());
    }

    // Machine-readable events + final-answer file.
    args.push("--json".into());
    args.push("-o".into());
    args.push(last_message_path.to_string());

    // Run outside a git repo without complaint; self-contained config.
    args.push("--skip-git-repo-check".into());
    args.push("--ignore-user-config".into());

    // Working root. On a fresh run pass `-C <workdir>`; on resume Codex reuses
    // the recorded session cwd, so `-C` is omitted to avoid conflicting with it.
    if !resuming {
        if let Some(dir) = &job.workdir {
            args.push("-C".into());
            args.push(dir.display().to_string());
        }
    }

    if resuming {
        // Safe: resuming implies Some(non-empty).
        if let Some(id) = &job.resume {
            args.push(id.clone());
        }
    }

    // `--` guards a prompt that begins with `-` from being parsed as a flag.
    args.push("--".into());
    args.push(job.prompt.clone());

    Ok(args)
}

/// Env-var injections for Codex from `job.endpoint`: only the API key value,
/// under the name Codex is told to read (`env_key`). Base URL / wire API go via
/// `-c` config args, not env. `None` endpoint ⇒ nothing (native auth).
pub(crate) fn endpoint_injections(endpoint: Option<&Endpoint>) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let Some(ep) = endpoint {
        if let Some(auth) = &ep.auth {
            out.push((ENV_API_KEY.into(), auth.secret.expose().to_string()));
        }
    }
    out
}

#[async_trait]
impl Harness for CodexCliHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::CodexCli
    }

    async fn available(&self) -> bool {
        crate::probe::binary_available(&self.bin).await
    }

    async fn run(&self, job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome> {
        // Per-run temp dir for the `--output-last-message` file. It is
        // self-contained and never touches the user's global Codex config.
        let tmp = RunTempDir::create("vyane-codex-")?;
        let last_message_path = tmp.path().join("last-message.txt");
        let last_message_str = last_message_path.to_string_lossy().to_string();

        let argv = build_argv(&job, &last_message_str, job.protocol)?;
        let resuming = job.resume.as_ref().map(|s| !s.is_empty()).unwrap_or(false);
        // On resume, Codex restores the native session cwd. Do not also set the
        // process cwd from the new job, or runtime cwd may diverge from argv.
        let process_cwd = if resuming {
            None
        } else {
            job.workdir.as_deref()
        };

        // Materialize the child env: scrubbed baseline + policy inject + the API
        // key injection. Injection wins; the parent env is never inherited.
        let mut policy = job.env.clone();
        for (k, v) in endpoint_injections(job.endpoint.as_ref()) {
            policy.inject.insert(k, v);
        }
        let env = materialize_env(&policy, parent_env_snapshot());

        tracing::debug!(
            harness = "codex-cli",
            argc = argv.len(),
            env_keys = %env_key_list(&env),
            "spawning codex-cli harness"
        );

        let result = run_capture(&self.bin, &argv, process_cwd, &env, &cancel, job.timeout).await?;

        match result.termination {
            Termination::Cancelled => Err(VyaneError::cancelled()),
            Termination::TimedOut => Err(VyaneError::new(
                ErrorKind::Timeout,
                "codex-cli harness timed out",
            )),
            Termination::Exited(code) => {
                if code == 0 {
                    let events = parse_codex_events(&result.stdout);
                    // Read the authoritative final answer from the -o file.
                    // A zero exit without it is a harness failure, not an empty
                    // successful answer.
                    let last = tokio::fs::read_to_string(last_message_path.as_path())
                        .await
                        .map_err(|e| {
                            VyaneError::with_source(
                                ErrorKind::HarnessFailed,
                                format!(
                                    "codex-cli exited successfully but expected last-message file `{}` was not readable: {e}",
                                    last_message_path.display()
                                ),
                                e,
                            )
                        })?;
                    let parsed = combine_codex(&last, events);
                    Ok(HarnessOutcome {
                        text: parsed.text,
                        native_session_id: parsed.native_session_id,
                        usage: parsed.usage,
                        exit_code: code,
                        duration: result.duration,
                    })
                } else {
                    Err(VyaneError::new(
                        ErrorKind::HarnessFailed,
                        format!(
                            "codex-cli exited with code {code}: {}",
                            stderr_tail(&result.stderr)
                        ),
                    ))
                }
            }
        }
    }
}

/// Last ~200 chars of stderr, single-lined, for an error message.
fn stderr_tail(stderr: &str) -> String {
    let s = stderr.trim().replace('\n', " ");
    if s.len() <= 200 {
        s
    } else {
        let tail = s
            .chars()
            .rev()
            .take(200)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<String>();
        format!("...{tail}")
    }
}

/// Small standard-library temp dir guard.
///
/// `tempfile` is intentionally only a dev-dependency in this work package, so
/// production code uses a tiny per-run directory helper instead.
#[derive(Debug)]
struct RunTempDir {
    path: std::path::PathBuf,
}

impl RunTempDir {
    fn create(prefix: &str) -> Result<Self> {
        use std::sync::atomic::{AtomicU64, Ordering};

        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let base = std::env::temp_dir();
        let pid = std::process::id();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);

        for _ in 0..16 {
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = base.join(format!("{prefix}{pid}-{now}-{n}"));
            match std::fs::create_dir(&path) {
                Ok(()) => {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let _ =
                            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700));
                    }
                    return Ok(Self { path });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => {
                    return Err(VyaneError::with_source(
                        ErrorKind::Io,
                        "failed to create codex temp dir",
                        e,
                    ));
                }
            }
        }

        Err(VyaneError::new(
            ErrorKind::Io,
            "failed to create unique codex temp dir",
        ))
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for RunTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use vyane_core::env::EnvPolicy;
    use vyane_core::target::{AuthMaterial, AuthStyle, ModelId, Secret};
    use vyane_core::task::GenParams;

    fn job(prompt: &str) -> HarnessJob {
        HarnessJob {
            prompt: prompt.into(),
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

    #[test]
    fn argv_fresh_read_only() {
        let j = job("hi");
        let a = build_argv(&j, "/tmp/lm.txt", Protocol::OpenaiResponses).unwrap();
        assert_eq!(a[0], "--ask-for-approval");
        assert_eq!(a[1], "never");
        assert!(a.windows(2).any(|w| w == ["--sandbox", "read-only"]));
        assert!(a.iter().any(|x| x == "exec"));
        assert!(a.windows(2).any(|w| w == ["-o", "/tmp/lm.txt"]));
        assert!(a.iter().any(|x| x == "--json"));
        assert!(a.iter().any(|x| x == "--skip-git-repo-check"));
        assert!(a.iter().any(|x| x == "--ignore-user-config"));
        // Prompt is last, guarded by `--`.
        assert_eq!(a[a.len() - 2], "--");
        assert_eq!(a[a.len() - 1], "hi");
        assert!(!a.iter().any(|x| x == "resume"));
    }

    #[test]
    fn argv_sandbox_write_and_full_map_correctly() {
        let mut j = job("x");
        j.sandbox = Sandbox::Write;
        let a = build_argv(&j, "/tmp/lm", Protocol::OpenaiResponses).unwrap();
        assert!(a.windows(2).any(|w| w == ["--sandbox", "workspace-write"]));

        j.sandbox = Sandbox::Full;
        let a = build_argv(&j, "/tmp/lm", Protocol::OpenaiResponses).unwrap();
        assert!(
            a.windows(2)
                .any(|w| w == ["--sandbox", "danger-full-access"])
        );
    }

    #[test]
    fn argv_resume_puts_session_id_after_resume() {
        let mut j = job("continue");
        j.resume = Some("thread-77".into());
        let a = build_argv(&j, "/tmp/lm", Protocol::OpenaiResponses).unwrap();
        let resume_pos = a.iter().position(|x| x == "resume").unwrap();
        assert_eq!(a[resume_pos - 1], "exec");
        assert!(a.windows(2).any(|w| w == ["--sandbox", "read-only"]));
        assert!(
            a.windows(2)
                .any(|w| w == ["--ignore-user-config", "thread-77"])
        );
        // No -C on resume (session cwd is reused).
        assert!(!a.iter().any(|x| x == "-C"));
    }

    #[test]
    fn argv_fresh_with_workdir_and_model() {
        let mut j = job("build");
        j.model = ModelId::new("gpt-5.5");
        j.workdir = Some("/tmp/proj".into());
        let a = build_argv(&j, "/tmp/lm", Protocol::OpenaiResponses).unwrap();
        assert!(a.windows(2).any(|w| w == ["-C", "/tmp/proj"]));
        assert!(a.windows(2).any(|w| w == ["--model", "gpt-5.5"]));
    }

    #[test]
    fn provider_config_defines_and_selects_inline() {
        let ep = Endpoint {
            base_url: "https://relay.example/v1".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("k"),
            }),
        };
        let args = provider_config_args(Some(&ep), Protocol::OpenaiResponses).unwrap();
        let joined = args.join(" ");
        assert!(joined.contains(r#"model_providers.vyane.base_url="https://relay.example/v1""#));
        assert!(joined.contains(r#"model_providers.vyane.wire_api="responses""#));
        assert!(joined.contains(r#"model_providers.vyane.env_key="OPENAI_API_KEY""#));
        assert!(joined.contains(r#"model_provider="vyane""#));
    }

    #[test]
    fn provider_config_chat_protocol_emits_chat_wire_api() {
        let ep = Endpoint {
            base_url: "https://relay.example/v1".into(),
            auth: None,
        };
        let args = provider_config_args(Some(&ep), Protocol::OpenaiChat).unwrap();
        assert!(
            args.join(" ")
                .contains(r#"model_providers.vyane.wire_api="chat""#)
        );
        // No auth ⇒ no env_key emitted.
        assert!(!args.join(" ").contains("env_key"));
    }

    #[test]
    fn provider_config_none_endpoint_is_empty() {
        assert!(
            provider_config_args(None, Protocol::OpenaiResponses)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn provider_config_anthropic_protocol_is_unsupported() {
        let err = provider_config_args(None, Protocol::AnthropicMessages).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unsupported);
        assert!(err.message.contains("anthropic_messages / codex-cli"));
    }

    #[test]
    fn endpoint_injection_puts_key_in_openai_api_key() {
        let ep = Endpoint {
            base_url: "https://relay.example/v1".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("test-api-key"),
            }),
        };
        let inj = endpoint_injections(Some(&ep));
        assert_eq!(
            inj,
            vec![(ENV_API_KEY.to_string(), "test-api-key".to_string())]
        );
    }

    #[test]
    fn endpoint_injection_none_is_empty() {
        assert!(endpoint_injections(None).is_empty());
    }
}
