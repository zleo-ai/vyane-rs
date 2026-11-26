//! [`Harness`] for the **Cursor CLI**, one print-mode JSON round.
//!
//! Command shape for [`Sandbox::Full`] (documented `agent` parameters):
//!
//! ```text
//! agent -p --output-format json [--model M]
//!       --sandbox disabled --force
//!       [--workspace <workdir>] [--resume <session-id>] -- <prompt>
//! ```
//!
//! * `-p` / `--print` with `--output-format json` emits one JSON object and
//!   exits. This adapter reads that object only. It does not pass
//!   `stream-json`, and it does not invoke the `acp` subcommand.
//! * The prompt is one positional argument after `--`, so task text that
//!   starts with `-` cannot be parsed as a CLI flag.
//! * [`Sandbox::ReadOnly`] and [`Sandbox::Write`] are refused with
//!   [`ErrorKind::Unsupported`] before any process is spawned. Documented
//!   print mode still runs workspace edits, allowlisted commands, and every
//!   `permissions.allow` rule without `--force`. Global `cli-config.json` can
//!   set `approvalMode` to `unrestricted`, which the headless docs treat as
//!   `--force`. Project `.cursor/cli.json` can add those allow rules.
//!   `CURSOR_CONFIG_DIR` and `XDG_CONFIG_HOME` only relocate the global file.
//!   `--trust` records workspace trust, and trusted project hooks can run
//!   shell commands; omitting `--trust` does not clear a trust decision
//!   already saved for that workspace. `--mode ask` and `--sandbox enabled`
//!   are not documented to override those inputs, so this adapter does not
//!   claim that they enforce a ceiling.
//! * [`Sandbox::Full`] is the explicit unrestricted print: `--sandbox disabled
//!   --force`, and never `--trust`. `--force` is the documented switch for
//!   running actions that would otherwise need approval. Deny rules and team
//!   policy can still reject it; they cannot grant a wider sandbox than
//!   `Full`.
//! * `--resume <id>` continues a native session from `job.resume`.
//! * A non-empty endpoint base URL is refused before spawn. The public auth
//!   docs name `CURSOR_API_KEY` and `--api-key`; this adapter uses the
//!   environment variable only, and only when the base URL is empty. Both
//!   [`AuthStyle`] values use that one variable.
//! * `--model` is passed only when the job's model id is non-empty. Reasoning
//!   effort and other [`vyane_core::GenParams`] fields are not rewritten into
//!   the model id or into extra flags; this CLI's documented one-round JSON
//!   surface has no separate effort flag.
//! * The published JSON result has no token-usage object. [`HarnessOutcome::usage`]
//!   stays empty even if a future or non-standard payload adds one. The
//!   `type` field must be the string `result`.
//!
//! This type is not selected by the service assembler. Profiles and routing
//! keep the Claude Code and Codex CLI support matrix.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use vyane_core::error::{ErrorKind, Result, VyaneError};
use vyane_core::target::{Endpoint, HarnessKind, Sandbox, Secret};
use vyane_core::traits::{Harness, HarnessJob, HarnessOutcome};

use crate::spawn::{
    RunControl, RunResult, Termination, env_key_list, materialize_env, parent_env_snapshot,
    run_capture_with_pinned,
};

/// Cursor CLI API key variable. Documented alternative to `--api-key`.
const ENV_API_KEY: &str = "CURSOR_API_KEY";

/// Documented Cursor Agent binary name, resolved through `PATH` at spawn time.
const BIN: &str = "agent";

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
    /// [`Self::new`] and resolve `agent` through `PATH`.
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
        let mut policy = job.env.clone();
        for (key, value) in endpoint_injections(job.endpoint.as_ref()) {
            policy.inject.insert(key, value);
        }
        let parent_env = self.parent_env.clone().unwrap_or_else(parent_env_snapshot);
        materialize_env(&policy, parent_env)
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

/// Refuse [`Sandbox::ReadOnly`] and [`Sandbox::Write`] before spawn.
///
/// See the module docs for the documented print-mode inputs that can widen
/// those requests. [`Sandbox::Full`] is the only level this adapter can state
/// explicitly, via `--sandbox disabled --force`.
fn reject_restricted_sandbox(sandbox: Sandbox) -> Result<()> {
    match sandbox {
        Sandbox::ReadOnly | Sandbox::Write => Err(VyaneError::new(
            ErrorKind::Unsupported,
            format!(
                "cursor-cli refuses to spawn for sandbox {}: documented print mode still applies global cli-config.json approvalMode, project .cursor/cli.json allow rules, and trusted-workspace hooks, and no documented switch isolates them",
                sandbox.as_str()
            ),
        )),
        Sandbox::Full => Ok(()),
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

/// Flags for the one sandbox level this adapter executes.
fn full_sandbox_args() -> Vec<String> {
    vec!["--sandbox".into(), "disabled".into(), "--force".into()]
}

/// Construct the Cursor CLI argv (excluding the program name) for one JSON round.
///
/// Returns [`ErrorKind::Unsupported`] for a restricted sandbox or a custom
/// endpoint. Callers must not spawn after that error.
fn build_argv(job: &HarnessJob) -> Result<Vec<String>> {
    reject_restricted_sandbox(job.sandbox)?;
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

    args.extend(full_sandbox_args());

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

/// Endpoint environment injections for a run that has already passed
/// [`reject_custom_endpoint`].
///
/// A non-empty base URL injects nothing, including the API key, so a rejected
/// endpoint cannot leave a credential in a child environment. An empty base
/// URL with auth injects `CURSOR_API_KEY` only.
fn endpoint_injections(endpoint: Option<&Endpoint>) -> Vec<(String, Secret)> {
    let Some(endpoint) = endpoint else {
        return Vec::new();
    };
    if !endpoint.base_url.is_empty() {
        return Vec::new();
    }
    match &endpoint.auth {
        Some(auth) => vec![(ENV_API_KEY.into(), auth.secret.clone())],
        None => Vec::new(),
    }
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
    use super::*;
    use vyane_core::env::EnvPolicy;
    use vyane_core::target::{AuthMaterial, AuthStyle, ModelId, Protocol};
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
    fn restricted_sandboxes_are_rejected_before_argv() {
        for sandbox in [Sandbox::ReadOnly, Sandbox::Write] {
            let mut sample = job("hello");
            sample.sandbox = sandbox;
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
            assert!(err.message.contains(sandbox.as_str()), "{}", err.message);
            assert!(!err.message.contains("must-not-matter"));
            assert!(!err.message.contains("--trust"));
        }
    }

    #[test]
    fn custom_endpoint_is_rejected_before_argv_on_full() {
        for base_url in [
            "https://endpoint.example",
            " https://endpoint.example ",
            "  ",
        ] {
            let mut sample = full_job("hello");
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

    #[test]
    fn endpoint_injections_keep_only_an_api_key_on_an_empty_base_url() {
        assert!(endpoint_injections(None).is_empty());

        for style in [AuthStyle::XApiKey, AuthStyle::Bearer] {
            let key_only = Endpoint {
                base_url: String::new(),
                auth: Some(AuthMaterial {
                    style,
                    secret: Secret::new("child-key"),
                }),
            };
            let injected = endpoint_injections(Some(&key_only));
            assert_eq!(injected.len(), 1);
            assert_eq!(injected[0].0, ENV_API_KEY);
            assert_eq!(injected[0].1.expose(), "child-key");
        }

        let custom = Endpoint {
            base_url: "https://endpoint.example".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("must-not-leak"),
            }),
        };
        assert!(endpoint_injections(Some(&custom)).is_empty());
        let whitespace = Endpoint {
            base_url: "  ".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("must-not-leak"),
            }),
        };
        assert!(endpoint_injections(Some(&whitespace)).is_empty());
    }

    #[test]
    fn build_env_scrubs_parent_cursor_key_unless_the_job_injects_one() {
        let parent = vec![
            ("PATH".to_string(), "/usr/bin".to_string()),
            ("HOME".to_string(), "/tmp/home".to_string()),
            ("CURSOR_API_KEY".to_string(), "parent-secret".to_string()),
            (
                "CURSOR_API_ENDPOINT".to_string(),
                "https://parent.example".to_string(),
            ),
        ];
        let harness = CursorCliHarness::with_binary_and_parent_env_for_tests("agent", parent);

        let scrubbed = harness.build_env(&full_job("hello"));
        assert_eq!(scrubbed.get("PATH").map(String::as_str), Some("/usr/bin"));
        assert_eq!(scrubbed.get("HOME").map(String::as_str), Some("/tmp/home"));
        assert!(!scrubbed.contains_key("CURSOR_API_KEY"));
        assert!(!scrubbed.contains_key("CURSOR_API_ENDPOINT"));

        let mut sample = full_job("hello");
        sample.env = EnvPolicy::scrubbed().inject("EXPLICIT_CHILD", "yes");
        sample.endpoint = Some(Endpoint {
            base_url: String::new(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("child-secret"),
            }),
        });
        let injected = harness.build_env(&sample);
        assert_eq!(
            injected.get("CURSOR_API_KEY").map(String::as_str),
            Some("child-secret")
        );
        assert!(!injected.contains_key("CURSOR_API_ENDPOINT"));
        assert_eq!(
            injected.get("EXPLICIT_CHILD").map(String::as_str),
            Some("yes")
        );

        sample.endpoint = Some(Endpoint {
            base_url: "https://endpoint.example".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("must-not-leak"),
            }),
        });
        let blocked = harness.build_env(&sample);
        assert!(!blocked.contains_key("CURSOR_API_KEY"));
        assert!(!blocked.contains_key("CURSOR_API_ENDPOINT"));
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
