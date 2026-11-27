//! [`Harness`] for the **Cursor CLI**, one print-mode JSON round.
//!
//! The program is the Windows standalone binary `cursor-agent`. Print flags
//! are arguments of that program. There is no `agent` subcommand. On Windows
//! the `cursor` launcher starts the desktop editor, so the headless one-shot
//! binary is `cursor-agent`. This adapter uses that invocation on every host.
//!
//! Command shape for [`Sandbox::ReadOnly`]:
//!
//! ```text
//! cursor-agent -p --output-format json [--model M] --mode ask
//!               [--workspace <workdir>] [--resume <session-id>] -- <prompt>
//! ```
//!
//! Command shape for [`Sandbox::Full`]:
//!
//! ```text
//! cursor-agent -p --output-format json [--model M]
//!               --sandbox disabled --force
//!               [--workspace <workdir>] [--resume <session-id>] -- <prompt>
//! ```
//!
//! * `-p` / `--print` with `--output-format json` emits one JSON object and
//!   exits. This adapter reads that object only. It does not pass
//!   `stream-json`, and it does not invoke the `acp` subcommand.
//! * The prompt is one positional argument after `--`, so task text that
//!   starts with `-` cannot be parsed as a CLI flag.
//! * [`Sandbox::ReadOnly`] passes `--mode ask` and does not pass `--force`,
//!   `--sandbox`, or `--trust`. Ask mode is the read-only switch for this
//!   one-shot. It does not mean the vendor binary ignores a global
//!   `cli-config.json` `approvalMode`, project `.cursor/cli.json` allow
//!   rules, or a trust decision already saved for the workspace.
//!   `--trust` stays omitted: it records workspace trust, and trusted
//!   project hooks can run shell commands. Omitting it does not clear a
//!   trust decision already saved for that workspace.
//! * [`Sandbox::Write`] is refused with [`ErrorKind::Unsupported`] before any
//!   process is spawned. Documented print mode can still apply global
//!   `approvalMode`, project allow rules, and trusted-workspace hooks, and
//!   this adapter has no write flag that isolates them.
//! * [`Sandbox::Full`] is the explicit unrestricted print: `--sandbox disabled
//!   --force`, and never `--trust` or `--mode`. `--force` is the documented
//!   switch for running actions that would otherwise need approval. Deny
//!   rules and team policy can still reject it; they cannot grant a wider
//!   sandbox than `Full`.
//! * `--resume <id>` continues a native session from `job.resume`.
//! * `CURSOR_API_KEY` and `CURSOR_API_ENDPOINT` are removed after the child
//!   environment is materialized. Removal wins over a full parent inherit, an
//!   allow entry, an inject entry, and endpoint auth, including a different
//!   case of the same name. The CLI then uses the local subscription login.
//!   A non-empty endpoint base URL is refused before spawn. Endpoint auth
//!   is not copied into the child.
//! * `--model` is passed only when the job's model id is non-empty. Reasoning
//!   effort and other [`vyane_core::GenParams`] fields are not rewritten into
//!   the model id or into extra flags; this CLI's documented one-round JSON
//!   surface has no separate effort flag.
//! * The published JSON result has no token-usage object. [`HarnessOutcome::usage`]
//!   stays empty even if a future or non-standard payload adds one. The
//!   `type` field must be the string `result`.
//!
//! This type is not selected by the service assembler. Profiles and routing
//! keep the Claude Code and Codex CLI support matrix. Auto, failover, and
//! broadcast do not gain a Cursor default pool from this adapter.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use vyane_core::error::{ErrorKind, Result, VyaneError};
use vyane_core::target::{Endpoint, HarnessKind, Sandbox};
use vyane_core::traits::{Harness, HarnessJob, HarnessOutcome};

use crate::spawn::{
    RunControl, RunResult, Termination, env_key_list, materialize_env, parent_env_snapshot,
    run_capture_with_pinned,
};

/// Metered key. Removed from the child so the local subscription login is used.
const ENV_API_KEY: &str = "CURSOR_API_KEY";

/// Metered endpoint. Removed from the child with [`ENV_API_KEY`].
const ENV_API_ENDPOINT: &str = "CURSOR_API_ENDPOINT";

/// Names removed after environment materialization. Comparison is
/// case-insensitive because Windows environment names are.
const SUBSCRIPTION_ENV_REMOVALS: &[&str] = &[ENV_API_KEY, ENV_API_ENDPOINT];

/// Windows standalone Cursor Agent binary, resolved through `PATH` at spawn.
///
/// Print flags are arguments of this program. The `cursor` launcher on
/// Windows starts the desktop editor, so this adapter does not call it and
/// does not insert an `agent` subcommand.
const BIN: &str = "cursor-agent";

/// Stable [`HarnessKind::Other`] token for this adapter.
const KIND: &str = "cursor-cli";

/// `Harness` adapter for one Cursor CLI JSON round.
#[derive(Debug, Clone)]
pub struct CursorCliHarness {
    bin: String,
    parent_env: Option<Vec<(String, String)>>,
}

impl CursorCliHarness {
    pub fn new() -> Self {
        Self {
            bin: BIN.into(),
            parent_env: None,
        }
    }

    /// Construct a harness with an explicit binary path or name.
    ///
    /// Tests use this with a fake executable. Production callers normally use
    /// [`Self::new`] and resolve `cursor-agent` through `PATH`.
    pub fn with_binary(bin: impl Into<String>) -> Self {
        Self {
            bin: bin.into(),
            parent_env: None,
        }
    }

    #[doc(hidden)]
    pub fn with_binary_and_parent_env_for_tests(
        bin: impl Into<String>,
        parent_env: Vec<(String, String)>,
    ) -> Self {
        Self {
            bin: bin.into(),
            parent_env: Some(parent_env),
        }
    }

    fn build_env(&self, job: &HarnessJob) -> BTreeMap<String, String> {
        let parent_env = self.parent_env.clone().unwrap_or_else(parent_env_snapshot);
        let mut env = materialize_env(&job.env, parent_env);
        strip_subscription_overrides(&mut env);
        env
    }

    fn classify_result(&self, result: RunResult) -> Result<HarnessOutcome> {
        match result.termination {
            Termination::Cancelled => Err(VyaneError::cancelled()),
            Termination::TimedOut => Err(VyaneError::new(
                ErrorKind::Timeout,
                "cursor-cli harness timed out",
            )),
            Termination::Exited(code) => {
                if code == 0 {
                    let parsed = parse_cursor_json(&result.stdout);
                    if parsed.is_error {
                        let subtype = match parsed.subtype.as_deref() {
                            Some(subtype) => format!(" ({subtype})"),
                            None => String::new(),
                        };
                        return Err(VyaneError::new(
                            ErrorKind::HarnessFailed,
                            format!(
                                "cursor-cli returned error envelope{subtype}: {}",
                                bound_text(&parsed.text)
                            ),
                        ));
                    }
                    Ok(HarnessOutcome {
                        text: parsed.text,
                        native_session_id: parsed.native_session_id,
                        usage: None,
                        exit_code: code,
                        duration: result.duration,
                    })
                } else {
                    Err(VyaneError::new(
                        ErrorKind::HarnessFailed,
                        format!(
                            "cursor-cli exited with code {code}: {}",
                            stderr_tail(&result.stderr)
                        ),
                    ))
                }
            }
        }
    }
}

impl Default for CursorCliHarness {
    fn default() -> Self {
        Self::new()
    }
}

/// Refuse [`Sandbox::Write`] before spawn.
///
/// [`Sandbox::ReadOnly`] is `--mode ask`. [`Sandbox::Full`] is
/// `--sandbox disabled --force`. Write has no flag in this adapter that
/// isolates global `approvalMode`, project allow rules, and trusted-workspace
/// hooks.
fn sandbox_args(sandbox: Sandbox) -> Result<Vec<String>> {
    match sandbox {
        Sandbox::ReadOnly => Ok(vec!["--mode".into(), "ask".into()]),
        Sandbox::Full => Ok(vec![
            "--sandbox".into(),
            "disabled".into(),
            "--force".into(),
        ]),
        Sandbox::Write => Err(VyaneError::new(
            ErrorKind::Unsupported,
            "cursor-cli refuses to spawn for sandbox write: documented print mode can still apply global cli-config.json approvalMode, project .cursor/cli.json allow rules, and trusted-workspace hooks, and this adapter has no write flag that isolates them",
        )),
    }
}

/// A non-empty base URL would retarget the CLI. That override is not part of
/// the verified public auth surface, so the run stops before spawn.
fn reject_custom_endpoint(endpoint: Option<&Endpoint>) -> Result<()> {
    if endpoint.is_some_and(|endpoint| !endpoint.base_url.is_empty()) {
        return Err(VyaneError::new(
            ErrorKind::Unsupported,
            "cursor-cli refuses to spawn because a custom endpoint base URL is not a verified override",
        ));
    }
    Ok(())
}

/// Drop metered Cursor login overrides after [`materialize_env`].
///
/// Matching ignores ASCII case so a Windows environment cannot keep the same
/// variable under a different spelling. Endpoint auth is not inserted before
/// this pass, and a name that arrived through inherit, allow, or inject is
/// removed here.
fn strip_subscription_overrides(env: &mut BTreeMap<String, String>) {
    env.retain(|key, _| {
        !SUBSCRIPTION_ENV_REMOVALS
            .iter()
            .any(|blocked| key.eq_ignore_ascii_case(blocked))
    });
}

/// Construct the Cursor CLI argv (excluding the program name) for one JSON round.
///
/// Returns [`ErrorKind::Unsupported`] for [`Sandbox::Write`] or a custom
/// endpoint. Callers must not spawn after that error. The program name is
/// [`BIN`], not an `agent` subcommand in this vector.
fn build_argv(job: &HarnessJob) -> Result<Vec<String>> {
    let mode_args = sandbox_args(job.sandbox)?;
    reject_custom_endpoint(job.endpoint.as_ref())?;

    let mut args = vec![
        "-p".to_string(),
        "--output-format".to_string(),
        "json".to_string(),
    ];

    if !job.model.as_str().is_empty() {
        args.push("--model".into());
        args.push(job.model.as_str().to_string());
    }

    args.extend(mode_args);

    if let Some(dir) = &job.workdir {
        let text = dir.display().to_string();
        if !text.is_empty() {
            args.push("--workspace".into());
            args.push(text);
        }
    }

    if let Some(id) = &job.resume
        && !id.is_empty()
    {
        args.push("--resume".into());
        args.push(id.clone());
    }

    args.push("--".into());
    args.push(job.prompt.clone());
    Ok(args)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Parsed {
    text: String,
    native_session_id: Option<String>,
    is_error: bool,
    subtype: Option<String>,
}

/// Parse one Cursor CLI `--output-format json` document.
///
/// The document is a single JSON object. `result` is the answer text and must
/// be a string (an empty string is a present answer). `session_id` is kept
/// when it is a non-empty string. `duration_ms`, `duration_api_ms`,
/// `request_id`, and any `usage` object are ignored.
///
/// A zero exit is not terminal evidence by itself. Malformed output, a
/// non-object document, a non-`result` type, or an object without a string
/// `result` is a typed `missing_result` error. The stdout body is not copied
/// into that error.
fn parse_cursor_json(stdout: &str) -> Parsed {
    let root: Value = match serde_json::from_str(stdout.trim()) {
        Ok(value) => value,
        Err(_) => return missing("one-round output was not valid JSON"),
    };
    let Some(obj) = root.as_object() else {
        return missing("one-round output was not a JSON object");
    };

    match obj.get("type") {
        Some(kind) => match kind.as_str() {
            Some("result") => {}
            Some(_) => return missing("one-round output type was not result"),
            None => return missing("one-round output type was not a string"),
        },
        None => return missing("one-round output type was absent"),
    }

    let Some(text) = obj.get("result").and_then(Value::as_str) else {
        return missing("one-round output had no string result field");
    };

    let is_error_flag = match obj.get("is_error") {
        None => false,
        Some(Value::Bool(value)) => *value,
        Some(_) => return missing("one-round is_error field was not a boolean"),
    };
    let subtype = match obj.get("subtype") {
        None => None,
        Some(Value::String(value)) if !value.is_empty() => Some(value.clone()),
        Some(Value::String(_)) => None,
        Some(_) => return missing("one-round subtype field was not a string"),
    };
    let subtype_failed = subtype.as_deref().is_some_and(|value| value != "success");

    Parsed {
        text: text.to_string(),
        native_session_id: nonempty_string(obj.get("session_id")),
        is_error: is_error_flag || subtype_failed,
        subtype,
    }
}

fn nonempty_string(value: Option<&Value>) -> Option<String> {
    match value.and_then(Value::as_str) {
        Some(text) if !text.is_empty() => Some(text.to_string()),
        _ => None,
    }
}

fn missing(detail: &'static str) -> Parsed {
    Parsed {
        text: format!("cursor-cli output ended without a one-round JSON result: {detail}"),
        native_session_id: None,
        is_error: true,
        subtype: Some("missing_result".to_string()),
    }
}

fn bound_text(text: &str) -> String {
    const LIMIT: usize = 200;
    if text.chars().count() <= LIMIT {
        return text.to_string();
    }
    let tail: String = text
        .chars()
        .rev()
        .take(LIMIT)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("...{tail}")
}

/// Last ~200 chars of stderr, single-lined, for an error message.
fn stderr_tail(stderr: &str) -> String {
    let flattened = stderr.trim().replace('\n', " ");
    bound_text(&flattened)
}

#[async_trait]
impl Harness for CursorCliHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::Other(KIND.into())
    }

    async fn available(&self) -> bool {
        crate::probe::binary_available(&self.bin).await
    }

    async fn run(&self, job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome> {
        let argv = build_argv(&job)?;
        let env = self.build_env(&job);

        tracing::debug!(
            harness = KIND,
            argc = argv.len(),
            env_keys = %env_key_list(&env),
            "spawning cursor-cli harness"
        );

        let result = run_capture_with_pinned(
            &self.bin,
            &argv,
            job.workdir.as_deref(),
            None,
            &env,
            RunControl::new(cancel, job.timeout, job.harness_lifecycle_reporter.clone()),
        )
        .await?;

        self.classify_result(result)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use vyane_core::env::{EnvPolicy, InheritMode};
    use vyane_core::target::{AuthMaterial, AuthStyle, ModelId, Protocol, Secret};
    use vyane_core::task::{Effort, GenParams};

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
            harness_lifecycle_reporter: None,
        }
    }

    fn flags_before_prompt(args: &[String]) -> &[String] {
        let end = args.iter().rposition(|arg| arg == "--").unwrap();
        &args[..end]
    }

    fn full_job(prompt: &str) -> HarnessJob {
        let mut sample = job(prompt);
        sample.sandbox = Sandbox::Full;
        sample
    }

    #[test]
    fn write_sandbox_is_rejected_before_argv() {
        let mut sample = job("hello");
        sample.sandbox = Sandbox::Write;
        sample.endpoint = Some(Endpoint {
            base_url: "https://endpoint.example".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("must-not-matter"),
            }),
        });
        let err = build_argv(&sample).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unsupported);
        assert!(err.message.contains("refuses to spawn"), "{}", err.message);
        assert!(err.message.contains("write"), "{}", err.message);
        assert!(!err.message.contains("must-not-matter"));
        assert!(!err.message.contains("--trust"));
        assert!(!err.message.contains("--mode"));
    }

    #[test]
    fn readonly_argv_uses_ask_mode_without_force_sandbox_or_trust() {
        let mut sample = job("--mode");
        sample.sandbox = Sandbox::ReadOnly;
        sample.model = ModelId::new("grok-4.7");
        sample.workdir = Some("/work/tree".into());
        sample.resume = Some("cursor-session".into());
        sample.endpoint = Some(Endpoint {
            base_url: String::new(),
            auth: Some(AuthMaterial {
                style: AuthStyle::XApiKey,
                secret: Secret::new("must-not-matter"),
            }),
        });
        let args = build_argv(&sample).unwrap();
        assert_eq!(
            args,
            vec![
                "-p",
                "--output-format",
                "json",
                "--model",
                "grok-4.7",
                "--mode",
                "ask",
                "--workspace",
                "/work/tree",
                "--resume",
                "cursor-session",
                "--",
                "--mode",
            ]
        );
        let flags = flags_before_prompt(&args);
        assert_eq!(flags.iter().filter(|arg| *arg == "--mode").count(), 1);
        assert_eq!(flags.iter().filter(|arg| *arg == "ask").count(), 1);
        for forbidden in [
            "--force",
            "--sandbox",
            "--trust",
            "--yolo",
            "--api-key",
            "agent",
            "cursor",
            "must-not-matter",
        ] {
            assert!(!flags.iter().any(|arg| arg == forbidden), "{args:?}");
        }
    }

    #[test]
    fn default_program_is_the_windows_standalone_binary() {
        assert_eq!(CursorCliHarness::new().bin, "cursor-agent");
        assert_eq!(CursorCliHarness::default().bin, "cursor-agent");
    }

    #[test]
    fn custom_endpoint_is_rejected_before_argv_on_readonly_and_full() {
        for sandbox in [Sandbox::ReadOnly, Sandbox::Full] {
            for base_url in [
                "https://endpoint.example",
                " https://endpoint.example ",
                "  ",
            ] {
                let mut sample = job("hello");
                sample.sandbox = sandbox;
                sample.endpoint = Some(Endpoint {
                    base_url: base_url.into(),
                    auth: Some(AuthMaterial {
                        style: AuthStyle::Bearer,
                        secret: Secret::new("must-not-matter"),
                    }),
                });
                let err = build_argv(&sample).unwrap_err();
                assert_eq!(err.kind, ErrorKind::Unsupported);
                assert!(err.message.contains("custom endpoint"), "{}", err.message);
                assert!(!err.message.contains("must-not-matter"));
                assert!(!err.message.contains("https://endpoint.example"));
            }
        }
    }

    #[test]
    fn argv_full_disables_sandbox_and_sets_force_before_prompt() {
        let mut sample = full_job("--force");
        let args = build_argv(&sample).unwrap();
        let flags = flags_before_prompt(&args);
        assert!(
            flags
                .windows(2)
                .any(|pair| pair == ["--sandbox", "disabled"])
        );
        assert!(flags.iter().any(|arg| arg == "--force"));
        assert!(!flags.iter().any(|arg| arg == "--trust"));
        assert!(!flags.iter().any(|arg| arg == "--yolo"));
        assert!(!flags.iter().any(|arg| arg == "--mode"));
        assert_eq!(args[args.len() - 2], "--");
        assert_eq!(args[args.len() - 1], "--force");
        assert_eq!(flags.iter().filter(|arg| *arg == "--force").count(), 1);
        sample.prompt = "hello".into();
        assert!(
            !build_argv(&sample)
                .unwrap()
                .iter()
                .any(|arg| arg == "--trust")
        );
    }

    #[test]
    fn argv_prompt_that_looks_like_a_flag_stays_behind_end_of_options() {
        for prompt in ["--force", "--yolo", "-p", "--", "acp", "- fix x"] {
            let args = build_argv(&full_job(prompt)).unwrap();
            assert_eq!(args[args.len() - 2], "--");
            assert_eq!(args[args.len() - 1], prompt);
            let flags = flags_before_prompt(&args);
            assert_eq!(flags.iter().filter(|arg| *arg == "--force").count(), 1);
            assert!(!flags.iter().any(|arg| arg == "--trust"), "{args:?}");
            assert!(!flags.iter().any(|arg| arg == "acp"), "{args:?}");
            assert!(!flags.iter().any(|arg| arg == "stream-json"), "{args:?}");
            assert!(!flags.iter().any(|arg| arg == "--api-key"), "{args:?}");
        }
    }

    #[test]
    fn argv_passes_model_workspace_and_resume_without_rewriting_effort() {
        let mut sample = full_job("answer\nthis");
        sample.model = ModelId::new("grok-4.7");
        sample.workdir = Some("/work/tree".into());
        sample.resume = Some("cursor-session".into());
        sample.params.effort = Some(Effort::Xhigh);
        sample.params.temperature = Some(0.2);
        sample
            .params
            .extra
            .insert("fast".into(), Value::Bool(false));
        sample.protocol = Protocol::AnthropicMessages;

        let with_params = build_argv(&sample).unwrap();
        sample.params = GenParams::default();
        sample.protocol = Protocol::OpenaiChat;
        let without_params = build_argv(&sample).unwrap();
        assert_eq!(with_params, without_params);
        assert_eq!(
            with_params,
            vec![
                "-p",
                "--output-format",
                "json",
                "--model",
                "grok-4.7",
                "--sandbox",
                "disabled",
                "--force",
                "--workspace",
                "/work/tree",
                "--resume",
                "cursor-session",
                "--",
                "answer\nthis",
            ]
        );
    }

    #[test]
    fn argv_omits_empty_model_resume_and_workdir() {
        let mut sample = full_job("hello");
        sample.model = ModelId::new("");
        sample.resume = Some(String::new());
        sample.workdir = Some("".into());
        let args = build_argv(&sample).unwrap();
        assert!(!args.iter().any(|arg| arg == "--model"));
        assert!(!args.iter().any(|arg| arg == "--resume"));
        assert!(!args.iter().any(|arg| arg == "--workspace"));
        assert!(!args.iter().any(|arg| arg == "--trust"));
    }

    fn assert_subscription_names_absent(env: &BTreeMap<String, String>) {
        assert!(!env.keys().any(|key| key.eq_ignore_ascii_case(ENV_API_KEY)
            || key.eq_ignore_ascii_case(ENV_API_ENDPOINT)));
    }

    #[test]
    fn build_env_strips_cursor_key_and_endpoint_after_every_source() {
        let parent = vec![
            ("PATH".to_string(), "/usr/bin".to_string()),
            ("HOME".to_string(), "/tmp/home".to_string()),
            ("CURSOR_API_KEY".to_string(), "parent-secret".to_string()),
            (
                "cursor_api_key".to_string(),
                "parent-secret-lower".to_string(),
            ),
            (
                "CURSOR_API_ENDPOINT".to_string(),
                "https://parent.example".to_string(),
            ),
            (
                "Cursor_Api_Endpoint".to_string(),
                "https://parent-mixed.example".to_string(),
            ),
            ("EDITOR".to_string(), "vim".to_string()),
        ];
        let harness =
            CursorCliHarness::with_binary_and_parent_env_for_tests("cursor-agent", parent);

        let scrubbed = harness.build_env(&full_job("hello"));
        assert_eq!(scrubbed.get("PATH").map(String::as_str), Some("/usr/bin"));
        assert_eq!(scrubbed.get("HOME").map(String::as_str), Some("/tmp/home"));
        assert_subscription_names_absent(&scrubbed);
        assert!(!scrubbed.contains_key("EDITOR"));

        let mut allowed = full_job("hello");
        allowed.env.allow.extend([
            ENV_API_KEY.to_string(),
            "cursor_api_key".to_string(),
            ENV_API_ENDPOINT.to_string(),
            "Cursor_Api_Endpoint".to_string(),
            "EDITOR".to_string(),
        ]);
        let allowed_env = harness.build_env(&allowed);
        assert_eq!(allowed_env.get("EDITOR").map(String::as_str), Some("vim"));
        assert_subscription_names_absent(&allowed_env);

        let mut sample = full_job("hello");
        let mut env = EnvPolicy {
            mode: InheritMode::Full,
            ..EnvPolicy::scrubbed()
        };
        env.allow
            .extend([ENV_API_KEY.to_string(), ENV_API_ENDPOINT.to_string()]);
        sample.env = env
            .inject(ENV_API_KEY, "injected-secret")
            .inject(ENV_API_ENDPOINT, "https://injected.example")
            .inject("cursor_api_key", "injected-secret-lower")
            .inject("EXPLICIT_CHILD", "yes");
        sample.endpoint = Some(Endpoint {
            base_url: String::new(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("endpoint-secret"),
            }),
        });
        let child = harness.build_env(&sample);
        assert_eq!(child.get("EXPLICIT_CHILD").map(String::as_str), Some("yes"));
        assert_eq!(child.get("EDITOR").map(String::as_str), Some("vim"));
        assert_eq!(child.get("PATH").map(String::as_str), Some("/usr/bin"));
        assert_subscription_names_absent(&child);
        for forbidden in [
            "parent-secret",
            "parent-secret-lower",
            "https://parent.example",
            "https://parent-mixed.example",
            "injected-secret",
            "injected-secret-lower",
            "https://injected.example",
            "endpoint-secret",
        ] {
            assert!(
                !child.values().any(|value| value.contains(forbidden)),
                "{forbidden} reached the child"
            );
        }

        sample.endpoint = Some(Endpoint {
            base_url: "https://endpoint.example".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::XApiKey,
                secret: Secret::new("must-not-leak"),
            }),
        });
        let blocked = harness.build_env(&sample);
        assert_subscription_names_absent(&blocked);
        assert!(
            !blocked
                .values()
                .any(|value| value.contains("must-not-leak"))
        );
        assert!(
            !blocked
                .values()
                .any(|value| value.contains("https://endpoint.example"))
        );
    }

    #[test]
    fn parse_success_keeps_result_and_session_and_ignores_extra_fields() {
        let doc = serde_json::json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "duration_ms": 5,
            "duration_api_ms": 4,
            "result": "line1\nline2",
            "session_id": "cursor-session",
            "request_id": "req-1",
            "usage": {"input_tokens": 9, "output_tokens": 8}
        });
        let parsed = parse_cursor_json(&format!("{doc}\n"));
        assert!(!parsed.is_error);
        assert_eq!(parsed.text, "line1\nline2");
        assert_eq!(parsed.native_session_id.as_deref(), Some("cursor-session"));
        assert_eq!(parsed.subtype.as_deref(), Some("success"));
    }

    #[test]
    fn parse_allows_empty_result_and_missing_session() {
        let parsed = parse_cursor_json(
            r#"{"type":"result","subtype":"success","is_error":false,"result":"","session_id":""}"#,
        );
        assert!(!parsed.is_error);
        assert_eq!(parsed.text, "");
        assert_eq!(parsed.native_session_id, None);
    }

    #[test]
    fn parse_error_envelope_and_failed_subtype() {
        let flagged = parse_cursor_json(
            r#"{"type":"result","subtype":"error","is_error":true,"result":"boom"}"#,
        );
        assert!(flagged.is_error);
        assert_eq!(flagged.text, "boom");
        assert_eq!(flagged.subtype.as_deref(), Some("error"));

        let subtype = parse_cursor_json(
            r#"{"type":"result","subtype":"error_max_turns","is_error":false,"result":"stopped"}"#,
        );
        assert!(subtype.is_error);
        assert_eq!(subtype.subtype.as_deref(), Some("error_max_turns"));
    }

    #[test]
    fn parse_rejects_non_terminal_documents_without_copying_them() {
        let cases = [
            "not json at all partial answer should not leak",
            r#"["result"]"#,
            r#"{"type":"result","subtype":"success","is_error":false}"#,
            r#"{"subtype":"success","is_error":false,"result":"partial answer should not leak"}"#,
            r#"{"type":"assistant","result":"partial answer should not leak"}"#,
            r#"{"type":"result","subtype":"success","is_error":"false","result":"x"}"#,
            "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"ok\"}\n{\"result\":\"second\"}",
        ];
        for document in cases {
            let parsed = parse_cursor_json(document);
            assert!(parsed.is_error, "{document}");
            assert_eq!(parsed.subtype.as_deref(), Some("missing_result"));
            assert!(!parsed.text.contains("partial answer should not leak"));
            assert!(!parsed.text.contains("second"));
            assert!(parsed.text.contains("one-round"));
        }
    }

    #[test]
    fn kind_token_is_cursor_cli() {
        assert_eq!(
            CursorCliHarness::new().kind(),
            HarnessKind::Other("cursor-cli".into())
        );
        assert_eq!(CursorCliHarness::new().kind().as_str(), "cursor-cli");
    }
}
