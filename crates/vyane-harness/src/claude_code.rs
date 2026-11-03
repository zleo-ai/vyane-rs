//! [`Harness`] for **Claude Code**, invoked headlessly as a one-shot.
//!
//! Command shape (verified against `claude --help`, CLI 2.1.x):
//!
//! ```text
//! claude -p --output-format json [--model M] [--effort E]
//!        [--permission-mode dontAsk --disallowedTools Bash,Edit,Write,NotebookEdit
//!         | --permission-mode acceptEdits | --dangerously-skip-permissions]
//!        [--add-dir <workdir>] [--resume <session-id>] -- <prompt>
//! ```
//!
//! * `-p/--print` runs non-interactively and exits — the workspace-trust dialog
//!   is skipped, so it never blocks on a prompt.
//! * The prompt is the positional argument and always follows a `--`
//!   end-of-options marker, so task text that starts with `-` can never be
//!   parsed as a CLI option (for example a task reading
//!   `--dangerously-skip-permissions`).
//! * `--output-format json` yields a single JSON result object we parse for the
//!   answer text, native session id, and usage ([`crate::parse`]).
//! * Sandbox mapping (`Sandbox` → permission flags): see [`sandbox_args`].
//! * `--resume <id>` continues a native session from `job.resume`.
//! * Endpoint injection is via env (Claude Code reads these): base URL, an
//!   auth-token or api-key var chosen by [`AuthStyle`], and a model var.

use std::collections::BTreeMap;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use vyane_core::error::{ErrorKind, Result, VyaneError};
use vyane_core::target::{AuthStyle, Endpoint, HarnessKind, Sandbox, Secret};
use vyane_core::traits::{
    Harness, HarnessExecutionContext, HarnessJob, HarnessOutcome, HarnessStreamEvent,
};
use vyane_core::{HarnessSpawnAuthority, PinnedWorkdir};

use crate::parse::{parse_claude_json, parse_claude_stream_json};
use crate::spawn::{
    PINNED_WORKDIR_CHILD_PATH, RunControl, RunResult, Termination, env_key_list, materialize_env,
    parent_env_snapshot, run_capture_with_pinned, run_stream_capture_with_pinned,
};

/// Environment variables Claude Code reads for a custom endpoint. Public Claude
/// Code documentation names these; see the crate-level notes.
const ENV_BASE_URL: &str = "ANTHROPIC_BASE_URL";
/// Bearer-style auth (`Authorization: Bearer <token>`).
const ENV_AUTH_TOKEN: &str = "ANTHROPIC_AUTH_TOKEN";
/// `x-api-key`-style auth.
const ENV_API_KEY: &str = "ANTHROPIC_API_KEY";
/// Model override the CLI honors.
const ENV_MODEL: &str = "ANTHROPIC_MODEL";

/// The Claude Code binary name (resolved via `PATH` at spawn time).
const BIN: &str = "claude";

/// `Harness` adapter for Claude Code.
#[derive(Debug, Clone)]
pub struct ClaudeCodeHarness {
    bin: String,
    parent_env: Option<Vec<(String, String)>>,
}

impl ClaudeCodeHarness {
    pub fn new() -> Self {
        Self {
            bin: BIN.into(),
            parent_env: None,
        }
    }

    /// Construct a harness with an explicit binary path/name.
    ///
    /// This is primarily useful for tests with fake CLI scripts; production
    /// callers normally use [`Self::new`] and resolve `claude` through `PATH`.
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

    /// Build the materialized child environment for a job (shared by run + run_stream).
    fn build_env(&self, job: &HarnessJob) -> BTreeMap<String, String> {
        let mut policy = job.env.clone();
        for (k, v) in endpoint_injections(job.endpoint.as_ref()) {
            policy.inject.insert(k, v);
        }
        if !job.model.as_str().is_empty() && job.endpoint.is_some() {
            policy
                .inject
                .entry(ENV_MODEL.into())
                .or_insert_with(|| Secret::new(job.model.as_str()));
        }
        let parent_env = self.parent_env.clone().unwrap_or_else(parent_env_snapshot);
        materialize_env(&policy, parent_env)
    }

    /// Classify a captured run. Stream mode is NDJSON, while the one-shot path
    /// is a single JSON object; both share the same termination/error mapping.
    fn classify_result(&self, result: RunResult, stream: bool) -> Result<HarnessOutcome> {
        match result.termination {
            Termination::Cancelled => Err(VyaneError::cancelled()),
            Termination::TimedOut => Err(VyaneError::new(
                ErrorKind::Timeout,
                "claude-code harness timed out",
            )),
            Termination::Exited(code) => {
                if code == 0 {
                    let parsed = if stream {
                        parse_claude_stream_json(&result.stdout)
                    } else {
                        parse_claude_json(&result.stdout)
                    };
                    if parsed.is_error {
                        return Err(VyaneError::new(
                            ErrorKind::HarnessFailed,
                            format!(
                                "claude-code returned error envelope{}: {}",
                                parsed
                                    .subtype
                                    .as_deref()
                                    .map(|s| format!(" ({s})"))
                                    .unwrap_or_default(),
                                parsed.text
                            ),
                        ));
                    }
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
                            "claude-code exited with code {code}: {}",
                            stderr_tail(&result.stderr)
                        ),
                    ))
                }
            }
        }
    }
}

impl Default for ClaudeCodeHarness {
    fn default() -> Self {
        Self::new()
    }
}

/// Built-in Claude Code tools that change files or run commands; denied for
/// [`Sandbox::ReadOnly`]. A single comma-separated value, so the variadic
/// `--disallowedTools` option cannot absorb a following argument.
const READ_ONLY_DENIED_TOOLS: &str = "Bash,Edit,Write,NotebookEdit";

/// Map a [`Sandbox`] level onto Claude Code permission flags.
///
/// Rationale (mirrors what the upstream Python adapter proved works headlessly,
/// verified against the real `--permission-mode` choices
/// acceptEdits/auto/bypassPermissions/manual/dontAsk/plan):
///
/// * **`ReadOnly`** → `--permission-mode dontAsk --disallowedTools
///   Bash,Edit,Write,NotebookEdit`. Both are explicit on purpose: command-line
///   flags take precedence over settings files, so a workspace
///   `.claude/settings.json` with `defaultMode: bypassPermissions` or with
///   `allow` rules for `Bash`/`Write` cannot widen a read-only run. `dontAsk`
///   denies anything not pre-approved instead of prompting, and the deny list
///   wins over any `allow` rule. Reading and searching stay available. We do
///   NOT use `--permission-mode plan`: plan mode is built for interactive
///   planning and can yield a plan instead of an answer in print mode.
///   Project settings are still loaded (`--setting-sources` is not narrowed),
///   because doing so would also drop the project's `CLAUDE.md`; settings
///   hooks therefore still run as they do for an interactive session.
/// * **`Write`** → `--permission-mode acceptEdits`. Auto-approves file edits
///   without prompting — the minimum real write capability. (Note: Claude Code
///   has no workspace-scoped *command-exec* sandbox like Codex's
///   `workspace-write`; `acceptEdits` grants edits but still gates Bash. An
///   agent that must also run commands needs `Full`. We intentionally keep
///   `Write` more contained than `Full` rather than collapsing the two.)
/// * **`Full`** → `--dangerously-skip-permissions`. Bypasses all checks (edits
///   and command execution). Reserve for isolated worktrees.
///
/// Every arm runs without an interactive prompt — the point of headless use.
fn sandbox_args(sandbox: Sandbox) -> Vec<String> {
    match sandbox {
        Sandbox::ReadOnly => vec![
            "--permission-mode".into(),
            "dontAsk".into(),
            "--disallowedTools".into(),
            READ_ONLY_DENIED_TOOLS.into(),
        ],
        Sandbox::Write => vec!["--permission-mode".into(), "acceptEdits".into()],
        Sandbox::Full => vec!["--dangerously-skip-permissions".into()],
    }
}

/// Construct the full Claude Code argv (excluding the program name) for a job.
///
/// Kept as a pure function so an argv-echo fake CLI can assert it exactly.
#[cfg(test)]
pub(crate) fn build_argv(job: &HarnessJob) -> Vec<String> {
    build_argv_scoped(job, None)
}

fn build_argv_scoped(job: &HarnessJob, pinned_workdir: Option<&PinnedWorkdir>) -> Vec<String> {
    // Non-interactive print mode + machine-readable output. The prompt is
    // appended last, after `--` (see `push_prompt`).
    let mut args: Vec<String> = vec!["-p".into(), "--output-format".into(), "json".into()];

    // Model selection (empty model id ⇒ let the CLI/endpoint decide).
    if !job.model.as_str().is_empty() {
        args.push("--model".into());
        args.push(job.model.as_str().to_string());
    }

    // Normalized reasoning effort maps directly to Claude Code's CLI flag.
    if let Some(effort) = job.params.effort {
        args.push("--effort".into());
        args.push(effort.as_str().to_string());
    }

    // Sandbox → permission flags.
    args.extend(sandbox_args(job.sandbox));

    // Grant tool access to the working directory when one is set. The cwd is set
    // on the process separately (see run); `--add-dir` widens tool reach to it.
    let workdir = if pinned_workdir.is_some() {
        Some(std::path::PathBuf::from(PINNED_WORKDIR_CHILD_PATH))
    } else {
        job.workdir.clone()
    };
    if let Some(dir) = workdir {
        args.push("--add-dir".into());
        args.push(dir.display().to_string());
    }

    // Resume a native session.
    if let Some(id) = &job.resume
        && !id.is_empty()
    {
        args.push("--resume".into());
        args.push(id.clone());
    }

    push_prompt(&mut args, &job.prompt);
    args
}

/// Append the prompt as the final positional argument behind a `--`
/// end-of-options marker. `-p/--print` is a boolean flag, so without the
/// marker a prompt starting with `-` would reach the CLI option parser.
fn push_prompt(args: &mut Vec<String>, prompt: &str) {
    args.push("--".into());
    args.push(prompt.to_string());
}

/// Build argv for streaming mode: identical to [`build_argv`] but uses
/// `--output-format stream-json` instead of `json`, so the CLI emits NDJSON
/// events (one per line) instead of a single JSON object.
#[cfg(test)]
pub(crate) fn build_stream_argv(job: &HarnessJob) -> Vec<String> {
    build_stream_argv_scoped(job, None)
}

fn build_stream_argv_scoped(
    job: &HarnessJob,
    pinned_workdir: Option<&PinnedWorkdir>,
) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "-p".into(),
        "--output-format".into(),
        "stream-json".into(),
        // Claude Code requires verbose mode for stream-json in print mode.
        "--verbose".into(),
    ];

    if !job.model.as_str().is_empty() {
        args.push("--model".into());
        args.push(job.model.as_str().to_string());
    }

    if let Some(effort) = job.params.effort {
        args.push("--effort".into());
        args.push(effort.as_str().to_string());
    }

    args.extend(sandbox_args(job.sandbox));

    let workdir = if pinned_workdir.is_some() {
        Some(std::path::PathBuf::from(PINNED_WORKDIR_CHILD_PATH))
    } else {
        job.workdir.clone()
    };
    if let Some(dir) = workdir {
        args.push("--add-dir".into());
        args.push(dir.display().to_string());
    }

    if let Some(id) = &job.resume
        && !id.is_empty()
    {
        args.push("--resume".into());
        args.push(id.clone());
    }

    push_prompt(&mut args, &job.prompt);
    args
}

fn json_summary(value: &serde_json::Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

/// Convert one Claude Code stream-json line into live harness events.
///
/// Tool-use blocks are nested inside an `assistant.message.content` array in
/// the real CLI schema. A top-level `tool_use` arm is retained for older or
/// relay-specific emitters.
fn parse_stream_line(line: &str) -> Vec<HarnessStreamEvent> {
    let Ok(event) = serde_json::from_str::<serde_json::Value>(line) else {
        return Vec::new();
    };

    match event.get("type").and_then(serde_json::Value::as_str) {
        Some("assistant") => event
            .pointer("/message/content")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(
                |block| match block.get("type").and_then(serde_json::Value::as_str) {
                    Some("text") => block
                        .get("text")
                        .and_then(serde_json::Value::as_str)
                        .filter(|text| !text.is_empty())
                        .map(|text| HarnessStreamEvent::Delta(text.to_string())),
                    Some("tool_use") => Some(HarnessStreamEvent::ToolUse {
                        name: block
                            .get("name")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("unknown")
                            .to_string(),
                        summary: json_summary(
                            block.get("input").unwrap_or(&serde_json::Value::Null),
                        ),
                    }),
                    _ => None,
                },
            )
            .collect(),
        Some("tool_use") => vec![HarnessStreamEvent::ToolUse {
            name: event
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            summary: json_summary(event.get("input").unwrap_or(&serde_json::Value::Null)),
        }],
        _ => Vec::new(),
    }
}

/// Compute endpoint env-var injections for Claude Code from `job.endpoint`.
///
/// Returns `(key, value)` pairs to layer onto the [`EnvPolicy`] inject set.
/// When `endpoint` is `None` the harness authenticates natively — we inject
/// nothing for auth (an empty list). Auth style picks the variable name:
/// `Bearer` → auth-token var, `XApiKey` → api-key var.
pub(crate) fn endpoint_injections(endpoint: Option<&Endpoint>) -> Vec<(String, Secret)> {
    let mut out = Vec::new();
    let Some(ep) = endpoint else {
        return out;
    };
    if !ep.base_url.is_empty() {
        out.push((ENV_BASE_URL.into(), Secret::new(ep.base_url.clone())));
    }
    if let Some(auth) = &ep.auth {
        let var = match auth.style {
            AuthStyle::Bearer => ENV_AUTH_TOKEN,
            AuthStyle::XApiKey => ENV_API_KEY,
        };
        out.push((var.into(), auth.secret.clone()));
    }
    out
}

#[async_trait]
impl Harness for ClaudeCodeHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::ClaudeCode
    }

    async fn available(&self) -> bool {
        crate::probe::binary_available(&self.bin).await
    }

    async fn run(&self, job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome> {
        self.run_with_context(job, None, None, cancel).await
    }

    async fn run_scoped(
        &self,
        job: HarnessJob,
        context: HarnessExecutionContext,
        cancel: CancellationToken,
    ) -> Result<HarnessOutcome> {
        self.run_with_context(
            job,
            context.pinned_workdir(),
            context.spawn_authority(),
            cancel,
        )
        .await
    }

    async fn run_stream(
        &self,
        job: HarnessJob,
        cancel: CancellationToken,
        on_event: Box<dyn FnMut(HarnessStreamEvent) + Send + Sync>,
    ) -> Result<HarnessOutcome> {
        self.run_stream_with_context(job, None, None, cancel, on_event)
            .await
    }

    async fn run_stream_scoped(
        &self,
        job: HarnessJob,
        context: HarnessExecutionContext,
        cancel: CancellationToken,
        on_event: Box<dyn FnMut(HarnessStreamEvent) + Send + Sync>,
    ) -> Result<HarnessOutcome> {
        self.run_stream_with_context(
            job,
            context.pinned_workdir(),
            context.spawn_authority(),
            cancel,
            on_event,
        )
        .await
    }
}

impl ClaudeCodeHarness {
    async fn run_with_context(
        &self,
        job: HarnessJob,
        pinned_workdir: Option<&PinnedWorkdir>,
        spawn_authority: Option<&HarnessSpawnAuthority>,
        cancel: CancellationToken,
    ) -> Result<HarnessOutcome> {
        reject_pinned_resume(&job, pinned_workdir)?;
        let argv = build_argv_scoped(&job, pinned_workdir);
        let env = self.build_env(&job);

        tracing::debug!(
            harness = "claude-code",
            argc = argv.len(),
            env_keys = %env_key_list(&env),
            "spawning claude-code harness"
        );

        let result = run_capture_with_pinned(
            &self.bin,
            &argv,
            job.workdir.as_deref(),
            pinned_workdir,
            &env,
            RunControl::new(cancel, job.timeout, job.harness_lifecycle_reporter.clone())
                .with_spawn_authority(spawn_authority.cloned()),
        )
        .await?;

        self.classify_result(result, false)
    }

    async fn run_stream_with_context(
        &self,
        job: HarnessJob,
        pinned_workdir: Option<&PinnedWorkdir>,
        spawn_authority: Option<&HarnessSpawnAuthority>,
        cancel: CancellationToken,
        on_event: Box<dyn FnMut(HarnessStreamEvent) + Send + Sync>,
    ) -> Result<HarnessOutcome> {
        reject_pinned_resume(&job, pinned_workdir)?;
        let argv = build_stream_argv_scoped(&job, pinned_workdir);
        let env = self.build_env(&job);

        tracing::debug!(
            harness = "claude-code",
            mode = "stream",
            argc = argv.len(),
            env_keys = %env_key_list(&env),
            "spawning claude-code harness (streaming)"
        );

        // The stdout drain owns this callback and invokes it sequentially, so
        // no mutex or lossy try_lock bridge is needed.
        let mut on_event = on_event;

        let result = run_stream_capture_with_pinned(
            &self.bin,
            &argv,
            job.workdir.as_deref(),
            pinned_workdir,
            &env,
            RunControl::new(cancel, job.timeout, job.harness_lifecycle_reporter.clone())
                .with_spawn_authority(spawn_authority.cloned()),
            Box::new(move |line: &str| {
                for event in parse_stream_line(line) {
                    on_event(event);
                }
            }),
        )
        .await?;

        self.classify_result(result, true)
    }
}

fn reject_pinned_resume(job: &HarnessJob, pinned_workdir: Option<&PinnedWorkdir>) -> Result<()> {
    if pinned_workdir.is_some()
        && job
            .resume
            .as_ref()
            .is_some_and(|session| !session.is_empty())
    {
        return Err(VyaneError::new(
            ErrorKind::Unsupported,
            "claude-code mutating resume requires an exact NativeSessionDomain",
        ));
    }
    Ok(())
}

/// Last ~200 chars of stderr, single-lined, for an error message. Prompt text
/// is never on stderr, so this is safe to surface.
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use vyane_core::env::EnvPolicy;
    use vyane_core::target::{AuthMaterial, ModelId, Secret};
    use vyane_core::task::{Effort, GenParams};

    fn job(prompt: &str) -> HarnessJob {
        HarnessJob {
            prompt: prompt.into(),
            model: ModelId::new(""),
            protocol: vyane_core::target::Protocol::AnthropicMessages,
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

    #[test]
    fn argv_read_only_minimal() {
        let j = job("hello");
        let a = build_argv(&j);
        assert_eq!(
            a,
            vec![
                "-p",
                "--output-format",
                "json",
                "--permission-mode",
                "dontAsk",
                "--disallowedTools",
                "Bash,Edit,Write,NotebookEdit",
                "--",
                "hello",
            ]
        );
    }

    /// A read-only run must not inherit a looser default from workspace
    /// settings, so the mode and the mutating-tool deny list are explicit on
    /// both the one-shot and the streaming argv.
    #[test]
    fn argv_read_only_pins_mode_and_denies_mutating_tools() {
        for build in [build_argv, build_stream_argv] {
            let a = build(&job("hello"));
            assert!(a.windows(2).any(|w| w == ["--permission-mode", "dontAsk"]));
            assert!(
                a.windows(2)
                    .any(|w| w == ["--disallowedTools", "Bash,Edit,Write,NotebookEdit"])
            );
            assert!(!a.iter().any(|x| x == "--dangerously-skip-permissions"));
            assert!(!a.iter().any(|x| x == "acceptEdits"));
        }
    }

    /// `-p/--print` is a boolean flag and the prompt is positional, so the
    /// prompt must follow a `--` end-of-options marker as the last argument.
    /// Otherwise a task such as `--dangerously-skip-permissions` would turn
    /// that option on, and a markdown bullet task `- fix x` would fail as an
    /// unknown option.
    #[test]
    fn argv_prompt_follows_end_of_options_marker() {
        for build in [build_argv_scoped, build_stream_argv_scoped] {
            for prompt in ["--dangerously-skip-permissions", "- fix x", "-p", "--"] {
                let mut j = job(prompt);
                j.resume = Some("session-1".into());
                j.workdir = Some("/tmp/work".into());
                let args = build(&j, None);
                let (last, rest) = args.split_last().unwrap();
                assert_eq!(last, prompt);
                assert_eq!(rest.last().map(String::as_str), Some("--"), "{args:?}");
                assert_eq!(
                    rest.iter().filter(|arg| *arg == "--").count(),
                    1,
                    "{args:?}"
                );
                assert!(
                    !rest
                        .iter()
                        .any(|arg| arg == "--dangerously-skip-permissions"),
                    "{args:?}"
                );
            }
        }
    }

    #[test]
    fn argv_write_uses_accept_edits() {
        let mut j = job("go");
        j.sandbox = Sandbox::Write;
        let a = build_argv(&j);
        assert!(
            a.windows(2)
                .any(|w| w == ["--permission-mode", "acceptEdits"])
        );
        assert!(!a.iter().any(|x| x == "--dangerously-skip-permissions"));
    }

    #[test]
    fn argv_full_uses_bypass() {
        let mut j = job("go");
        j.sandbox = Sandbox::Full;
        let a = build_argv(&j);
        assert!(a.iter().any(|x| x == "--dangerously-skip-permissions"));
        assert!(!a.iter().any(|x| x == "--permission-mode"));
    }

    #[test]
    fn argv_includes_model_and_resume_and_add_dir() {
        let mut j = job("go");
        j.model = ModelId::new("claude-opus-4-8");
        j.resume = Some("sess-42".into());
        j.workdir = Some("/tmp/work".into());
        let a = build_argv(&j);
        assert!(a.windows(2).any(|w| w == ["--model", "claude-opus-4-8"]));
        assert!(a.windows(2).any(|w| w == ["--resume", "sess-42"]));
        assert!(a.windows(2).any(|w| w == ["--add-dir", "/tmp/work"]));
    }

    #[test]
    fn argv_empty_resume_is_omitted() {
        let mut j = job("go");
        j.resume = Some(String::new());
        let a = build_argv(&j);
        assert!(!a.iter().any(|x| x == "--resume"));
    }

    #[test]
    fn argv_effort_maps_for_fresh_resume_and_stream() {
        for (effort, expected) in [
            (Effort::Low, "low"),
            (Effort::Medium, "medium"),
            (Effort::High, "high"),
            (Effort::Xhigh, "xhigh"),
        ] {
            let mut fresh = job("go");
            fresh.params.effort = Some(effort);
            assert!(
                build_argv(&fresh)
                    .windows(2)
                    .any(|window| window == ["--effort", expected])
            );

            let mut resumed = fresh.clone();
            resumed.resume = Some("session-1".into());
            let resumed_argv = build_argv(&resumed);
            assert!(
                resumed_argv
                    .windows(2)
                    .any(|window| window == ["--effort", expected])
            );
            assert!(
                resumed_argv
                    .windows(2)
                    .any(|window| window == ["--resume", "session-1"])
            );

            assert!(
                build_stream_argv(&fresh)
                    .windows(2)
                    .any(|window| window == ["--effort", expected])
            );
        }
    }

    #[test]
    fn stream_argv_uses_stream_json_and_verbose() {
        let a = build_stream_argv(&job("hello"));
        assert!(
            a.windows(2)
                .any(|w| w == ["--output-format", "stream-json"])
        );
        assert!(a.iter().any(|arg| arg == "--verbose"));
    }

    #[test]
    fn stream_line_emits_nested_text_and_tool_use_in_order() {
        let line = r#"{
            "type":"assistant",
            "message":{"content":[
                {"type":"text","text":"working"},
                {"type":"tool_use","name":"Read","input":{"path":"src/lib.rs"}},
                {"type":"text","text":"done"}
            ]}
        }"#;

        let events = parse_stream_line(line);
        assert_eq!(events.len(), 3);
        assert!(matches!(&events[0], HarnessStreamEvent::Delta(text) if text == "working"));
        assert!(matches!(
            &events[1],
            HarnessStreamEvent::ToolUse { name, summary }
                if name == "Read" && summary == r#"{"path":"src/lib.rs"}"#
        ));
        assert!(matches!(&events[2], HarnessStreamEvent::Delta(text) if text == "done"));
    }

    #[test]
    fn endpoint_injection_bearer_uses_auth_token() {
        let ep = Endpoint {
            base_url: "https://relay.example/v1".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("test-bearer-token"),
            }),
        };
        let inj = endpoint_injections(Some(&ep));
        assert!(inj.contains(&(ENV_BASE_URL.into(), Secret::new("https://relay.example/v1"))));
        assert!(inj.contains(&(ENV_AUTH_TOKEN.into(), Secret::new("test-bearer-token"))));
        assert!(!inj.iter().any(|(k, _)| k == ENV_API_KEY));
    }

    #[test]
    fn endpoint_injection_xapikey_uses_api_key() {
        let ep = Endpoint {
            base_url: "https://api.anthropic.com".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::XApiKey,
                secret: Secret::new("test-x-api-key"),
            }),
        };
        let inj = endpoint_injections(Some(&ep));
        assert!(inj.contains(&(ENV_API_KEY.into(), Secret::new("test-x-api-key"))));
        assert!(!inj.iter().any(|(k, _)| k == ENV_AUTH_TOKEN));
    }

    #[test]
    fn endpoint_none_injects_nothing() {
        assert!(endpoint_injections(None).is_empty());
    }
}
