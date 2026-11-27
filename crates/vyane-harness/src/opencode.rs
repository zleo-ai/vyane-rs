//! [`Harness`] for one OpenCode CLI shot.
//!
//! Command shape, for every sandbox tier:
//!
//! ```text
//! opencode run <prompt> --format json
//!          [--model MODEL] [--session ID]
//!          [caller passthrough flags]
//! ```
//!
//! The prompt is an argv token. Stdin stays closed. There is no `--` separator.
//!
//! * `--model` is the job model when that id is non-empty, otherwise
//!   `params.extra["model"]`. A non-empty `params.extra["provider"]` is joined
//!   in front when the chosen model does not already contain `/`. Provider
//!   alone does not emit a flag. `format` in extra is ignored so the format
//!   flag stays `json`.
//! * `--session` is `job.resume` when that id is non-empty.
//! * Remaining `params.extra` entries are passthrough flags, in the map's
//!   iteration order: snake case becomes a kebab flag, true emits the flag
//!   alone, false and null are skipped, a string that starts with `-` is
//!   dropped, and a non-scalar value is refused before spawn. Identity keys
//!   (`owner_user_id`, `bound_owner`, `run_id`, and their kebab aliases) are
//!   never flags.
//! * No sandbox tier adds a flag. The child working directory is the job
//!   workdir. No environment name is removed after [`vyane_core::EnvPolicy`]
//!   is applied. An endpoint on the job is not injected and does not refuse
//!   the spawn.
//! * Answer text is non-JSON lines, plus truthy `part.text` scalars on
//!   `type=text` events, joined by newlines. Chat markers and tool markers
//!   are then removed. Other JSON objects are not answer text. A non-empty
//!   string, `true`, or a non-zero number `sessionID` is the session id. A
//!   non-JSON `Session:` or
//!   `session:` line is a fallback; the last id wins. `type=error` and
//!   `event_error` fail the run. A zero exit with empty answer text fails.
//!   A non-zero exit fails.
//! * `type=step_finish` `part.tokens.input` and `part.tokens.output` map onto
//!   [`vyane_core::Usage`]. `total` is read with the same count rules and is
//!   not stored: [`vyane_core::Usage`] has no total field. The last finish
//!   event with a token object wins, including an all-zero object.
//! * Stderr lines are parsed after stdout lines. The child captures them on
//!   separate pipes, so their interleaving is not reconstructed.
//!
//! The harness kind is [`HarnessKind::Other`] with token `opencode`.
//! [`HarnessKind::OpenCode`] already displays that token and is left
//! unchanged. `Other("opencode")` serializes as `opencode`, which decodes
//! back as [`HarnessKind::OpenCode`]; the two values are not equal.
//!
//! Streaming, a pinned workdir, and live spawn authority fail closed before a
//! process starts. This type is not selected by the service assembler.
//! Profiles, auto, failover, and broadcast keep the Claude Code and Codex CLI
//! support matrix.

use std::collections::BTreeMap;
use std::sync::LazyLock;
use std::time::Duration;

use async_trait::async_trait;
use regex::Regex;
use serde_json::{Map, Number, Value};
use tokio_util::sync::CancellationToken;
use vyane_core::error::{ErrorKind, Result, VyaneError};
use vyane_core::run::Usage;
use vyane_core::target::HarnessKind;
use vyane_core::traits::{Harness, HarnessJob, HarnessOutcome};

use crate::spawn::{
    RunControl, RunResult, Termination, env_key_list, materialize_env, parent_env_snapshot,
    run_capture_with_pinned,
};

/// Documented OpenCode binary name, resolved through `PATH` at spawn time.
const BIN: &str = "opencode";

/// Stable [`HarnessKind::Other`] token for this adapter.
const KIND: &str = "opencode";

const ERROR_TAIL_MAX_CHARS: usize = 2000;
const ERROR_TAIL_MAX_LINES: usize = 50;

const IDENTITY_SKIP: &[&str] = &[
    "owner_user_id",
    "bound_owner",
    "run_id",
    "owner-user-id",
    "bound-owner",
    "run-id",
];

/// `Harness` adapter for one OpenCode JSON shot.
#[derive(Debug, Clone)]
pub struct OpencodeHarness {
    bin: String,
    parent_env: Option<Vec<(String, String)>>,
}

impl OpencodeHarness {
    pub fn new() -> Self {
        Self {
            bin: BIN.into(),
            parent_env: None,
        }
    }

    /// Construct a harness with an explicit binary path or name.
    ///
    /// Tests use this with a fake executable. Production callers normally use
    /// [`Self::new`] and resolve `opencode` through `PATH`.
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
        materialize_env(&job.env, parent_env)
    }

    fn classify_result(&self, result: RunResult) -> Result<HarnessOutcome> {
        match result.termination {
            Termination::Cancelled => Err(VyaneError::cancelled()),
            Termination::TimedOut => Err(VyaneError::new(
                ErrorKind::Timeout,
                "opencode harness timed out",
            )),
            Termination::Exited(code) => {
                classify_exit(code, &result.stdout, &result.stderr, result.duration)
            }
        }
    }
}

impl Default for OpencodeHarness {
    fn default() -> Self {
        Self::new()
    }
}

fn unsupported(message: impl Into<String>) -> VyaneError {
    VyaneError::unsupported(message)
}

fn build_args(job: &HarnessJob) -> Result<Vec<String>> {
    let mut extra = sanitize_extra(&job.params.extra);
    extra.remove("format");
    let provider = take_optional_string(&mut extra, "provider")?;
    let extra_model = take_optional_string(&mut extra, "model")?;
    let model = resolve_model(job.model.as_str(), extra_model, provider);
    let passthrough = extra_flags(&extra)?;

    let mut args = vec![
        "run".to_string(),
        job.prompt.clone(),
        "--format".to_string(),
        "json".to_string(),
    ];
    if !model.is_empty() {
        args.push("--model".into());
        args.push(model);
    }
    if let Some(id) = job.resume.as_ref().filter(|id| !id.is_empty()) {
        args.push("--session".into());
        args.push(id.clone());
    }
    args.extend(passthrough);
    Ok(args)
}

fn resolve_model(job_model: &str, extra_model: Option<String>, provider: Option<String>) -> String {
    let mut model = if job_model.is_empty() {
        extra_model.unwrap_or_default()
    } else {
        job_model.to_string()
    };
    if let Some(provider) = provider.filter(|provider| !provider.is_empty())
        && !model.is_empty()
        && !model.contains('/')
    {
        model = format!("{provider}/{model}");
    }
    model
}

fn take_optional_string(extra: &mut Map<String, Value>, key: &str) -> Result<Option<String>> {
    match extra.remove(key) {
        None | Some(Value::Null) | Some(Value::Bool(false)) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(unsupported(format!(
            "opencode refuses to spawn because extra argument `{key}` must be a string"
        ))),
    }
}

fn sanitize_extra(extra: &Map<String, Value>) -> Map<String, Value> {
    let mut safe = Map::new();
    for (key, value) in extra {
        match value {
            Value::String(text) if text.starts_with('-') => {}
            Value::Array(items) => {
                let filtered = items
                    .iter()
                    .filter(|item| match item {
                        Value::String(text) => !text.starts_with('-'),
                        _ => true,
                    })
                    .cloned()
                    .collect();
                safe.insert(key.clone(), Value::Array(filtered));
            }
            other => {
                safe.insert(key.clone(), other.clone());
            }
        }
    }
    safe
}

fn extra_flags(extra: &Map<String, Value>) -> Result<Vec<String>> {
    let mut tokens = Vec::new();
    for (key, value) in extra {
        if IDENTITY_SKIP.contains(&key.as_str()) {
            continue;
        }
        let flag = flag_name(key);
        match value {
            Value::Null | Value::Bool(false) => {}
            Value::Bool(true) => tokens.push(flag),
            Value::Array(items) => {
                for item in items {
                    if item.is_null() {
                        continue;
                    }
                    tokens.push(flag.clone());
                    tokens.push(scalar_flag(key, item)?);
                }
            }
            other => {
                tokens.push(flag);
                tokens.push(scalar_flag(key, other)?);
            }
        }
    }
    Ok(tokens)
}

fn flag_name(key: &str) -> String {
    format!("--{}", key.replace('_', "-"))
}

fn scalar_flag(key: &str, value: &Value) -> Result<String> {
    match value {
        Value::Bool(true) => Ok("True".to_string()),
        Value::Bool(false) => Ok("False".to_string()),
        Value::Number(number) => Ok(number_flag(number)),
        Value::String(text) => Ok(text.clone()),
        Value::Null | Value::Array(_) | Value::Object(_) => Err(unsupported(format!(
            "opencode refuses to spawn because extra argument `{key}` is not a scalar flag value"
        ))),
    }
}

fn number_flag(number: &Number) -> String {
    if let Some(value) = number.as_i64() {
        return value.to_string();
    }
    if let Some(value) = number.as_u64() {
        return value.to_string();
    }
    if let Some(value) = number.as_f64()
        && value.is_finite()
    {
        let rendered = format!("{value}");
        if rendered.contains('.') || rendered.contains('e') || rendered.contains('E') {
            return rendered;
        }
        return format!("{rendered}.0");
    }
    number.to_string()
}

fn classify_exit(
    code: i32,
    stdout: &str,
    stderr: &str,
    duration: Duration,
) -> Result<HarnessOutcome> {
    let lines = collect_lines(stdout, stderr);
    let parsed = parse_lines(&lines);
    if code != 0 {
        return Err(VyaneError::new(
            ErrorKind::HarnessFailed,
            nonzero_message(code, &parsed.error_text, &lines),
        ));
    }
    if !parsed.error_text.is_empty() {
        return Err(VyaneError::new(
            ErrorKind::HarnessFailed,
            format!("opencode returned an error: {}", parsed.error_text),
        ));
    }
    if parsed.text.is_empty() {
        return Err(VyaneError::new(
            ErrorKind::HarnessFailed,
            empty_answer_message(&lines),
        ));
    }
    Ok(HarnessOutcome {
        text: parsed.text,
        native_session_id: parsed.session_id,
        usage: parsed.usage,
        exit_code: code,
        duration,
    })
}

fn nonzero_message(code: i32, error_text: &str, lines: &[String]) -> String {
    let raw_tail = output_tail(lines);
    let details =
        if !error_text.is_empty() && !raw_tail.is_empty() && !raw_tail.contains(error_text) {
            format!("{error_text}; output tail: {raw_tail}")
        } else if !error_text.is_empty() {
            error_text.to_string()
        } else if !raw_tail.is_empty() {
            raw_tail
        } else {
            format!("Command exited with code {code}")
        };
    format!("opencode exited with code {code}: {details}")
}

fn empty_answer_message(lines: &[String]) -> String {
    let raw_tail = output_tail(lines);
    if raw_tail.is_empty() {
        "opencode returned no answer text".to_string()
    } else {
        format!("opencode returned no answer text; output tail: {raw_tail}")
    }
}

fn output_tail(lines: &[String]) -> String {
    let start = lines.len().saturating_sub(ERROR_TAIL_MAX_LINES);
    let raw = lines[start..].join("\n");
    let raw = raw.trim();
    if raw.chars().count() <= ERROR_TAIL_MAX_CHARS {
        return raw.to_string();
    }
    let tail: String = raw
        .chars()
        .rev()
        .take(ERROR_TAIL_MAX_CHARS)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("...{tail}")
}

fn collect_lines(stdout: &str, stderr: &str) -> Vec<String> {
    let mut lines = Vec::new();
    lines.extend(stdout.lines().map(str::to_string));
    lines.extend(stderr.lines().map(str::to_string));
    lines
}

struct Parsed {
    text: String,
    session_id: Option<String>,
    error_text: String,
    usage: Option<Usage>,
}

fn parse_lines(lines: &[String]) -> Parsed {
    let mut output_lines = Vec::new();
    let mut session_id = String::new();
    let mut error_lines = Vec::new();
    let mut usage = None;

    for line in lines {
        if let Ok(Value::Object(data)) = serde_json::from_str::<Value>(line) {
            if let Some(id) = data.get("sessionID").and_then(session_token) {
                session_id = id;
            }
            match data.get("type").and_then(Value::as_str) {
                Some("text") => {
                    if let Some(fragment) = data
                        .get("part")
                        .and_then(Value::as_object)
                        .and_then(|part| part.get("text"))
                        .and_then(text_fragment)
                    {
                        output_lines.push(fragment);
                    }
                }
                Some("error" | "event_error") => error_lines.push(error_message(&data)),
                Some("step_finish") => {
                    if let Some(next) = usage_from_step(&data) {
                        usage = Some(next);
                    }
                }
                _ => {}
            }
            continue;
        }

        match classify_plain(line) {
            PlainLine::Session(Some(id)) => session_id = id,
            PlainLine::Session(None) => {}
            PlainLine::Text => output_lines.push(line.clone()),
        }
    }

    let session_id = if session_id.is_empty() {
        None
    } else {
        Some(session_id)
    };
    Parsed {
        text: clean_text(&output_lines.join("\n")),
        session_id,
        error_text: error_lines.join("\n"),
        usage,
    }
}

enum PlainLine {
    Session(Option<String>),
    Text,
}

fn classify_plain(line: &str) -> PlainLine {
    let rest = if let Some(rest) = line.strip_prefix("Session:") {
        rest
    } else if let Some(rest) = line.strip_prefix("session:") {
        rest
    } else {
        return PlainLine::Text;
    };
    let id = rest.trim();
    if id.is_empty() {
        PlainLine::Session(None)
    } else {
        PlainLine::Session(Some(id.to_string()))
    }
}

/// Truthy scalars only. A non-empty string is the event shape this CLI emits.
fn text_fragment(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Bool(true) => Some("True".to_string()),
        Value::Number(number) if !number_is_zero(number) => Some(number_flag(number)),
        _ => None,
    }
}

fn session_token(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Bool(true) => Some("True".to_string()),
        Value::Number(number) if !number_is_zero(number) => Some(number_flag(number)),
        _ => None,
    }
}

fn error_message(data: &Map<String, Value>) -> String {
    if let Some(text) = truthy_display(data.get("error")) {
        return text;
    }
    if let Some(text) = truthy_display(data.get("message")) {
        return text;
    }
    serde_json::to_string(data).unwrap_or_else(|_| "unprintable error".to_string())
}

fn truthy_display(value: Option<&Value>) -> Option<String> {
    let value = value?;
    match value {
        Value::Null | Value::Bool(false) => None,
        Value::Bool(true) => Some("True".to_string()),
        Value::Number(number) if number_is_zero(number) => None,
        Value::Number(number) => Some(number_flag(number)),
        Value::String(text) if text.is_empty() => None,
        Value::String(text) => Some(text.clone()),
        Value::Array(items) if items.is_empty() => None,
        Value::Object(map) if map.is_empty() => None,
        other => serde_json::to_string(other).ok(),
    }
}

fn usage_from_step(data: &Map<String, Value>) -> Option<Usage> {
    let part = data.get("part")?.as_object()?;
    let tokens = part.get("tokens")?.as_object()?;
    Some(usage_from_tokens(tokens))
}

fn usage_from_tokens(tokens: &Map<String, Value>) -> Usage {
    let input_tokens = token_count(tokens.get("input"));
    let output_tokens = token_count(tokens.get("output"));
    // `Usage` has no total field. The count is still parsed so a non-numeric
    // total follows the same rules as the split counts.
    let _total = token_count(tokens.get("total"));
    Usage {
        input_tokens,
        output_tokens,
        reasoning_tokens: None,
        cached_input_tokens: None,
    }
}

fn token_count(value: Option<&Value>) -> u64 {
    let Some(value) = value else {
        return 0;
    };
    match value {
        Value::Null | Value::Bool(false) | Value::Array(_) | Value::Object(_) => 0,
        Value::Bool(true) => 1,
        Value::Number(number) => number_to_count(number),
        Value::String(text) => parse_count_string(text),
    }
}

fn number_to_count(number: &Number) -> u64 {
    if let Some(value) = number.as_u64() {
        return value;
    }
    if let Some(value) = number.as_i64() {
        return if value < 0 { 0 } else { value as u64 };
    }
    if let Some(value) = number.as_f64() {
        if !value.is_finite() || value <= 0.0 || value >= u64::MAX as f64 {
            return 0;
        }
        return value.trunc() as u64;
    }
    0
}

fn parse_count_string(text: &str) -> u64 {
    let text = text.trim();
    let digits = text.strip_prefix('+').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return 0;
    }
    digits.parse::<u64>().unwrap_or(u64::MAX)
}

fn number_is_zero(number: &Number) -> bool {
    number.as_f64().is_some_and(|value| value == 0.0)
        || number.as_i64() == Some(0)
        || number.as_u64() == Some(0)
}

fn clean_text(text: &str) -> String {
    let without_chat = chat_marker_re().replace_all(text, "");
    let without_tool = tool_marker_re().replace_all(&without_chat, "");
    without_tool.trim().to_string()
}

fn chat_marker_re() -> &'static Regex {
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"<\|(?:assistant|user|system|tool|endoftext|end)\|>")
            .expect("opencode chat marker pattern")
    });
    &RE
}

fn tool_marker_re() -> &'static Regex {
    static RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"</?tool_calls?>").expect("opencode tool marker pattern"));
    &RE
}

#[async_trait]
impl Harness for OpencodeHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::Other(KIND.into())
    }

    async fn available(&self) -> bool {
        crate::probe::binary_available(&self.bin).await
    }

    async fn run(&self, job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome> {
        let args = build_args(&job)?;
        let env = self.build_env(&job);
        let cwd = job
            .workdir
            .as_deref()
            .filter(|dir| !dir.as_os_str().is_empty());

        tracing::debug!(
            harness = KIND,
            argc = args.len(),
            env_keys = %env_key_list(&env),
            "spawning opencode harness"
        );

        let result = run_capture_with_pinned(
            &self.bin,
            &args,
            cwd,
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
    use vyane_core::env::{EnvPolicy, InheritMode};
    use vyane_core::target::{
        AuthMaterial, AuthStyle, Endpoint, ModelId, Protocol, Sandbox, Secret,
    };
    use vyane_core::task::{Effort, GenParams};

    fn job(prompt: &str) -> HarnessJob {
        HarnessJob {
            prompt: prompt.into(),
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

    fn args_of(sample: &HarnessJob) -> Vec<String> {
        build_args(sample).unwrap()
    }

    fn basic() -> Vec<String> {
        vec!["run".into(), "hi".into(), "--format".into(), "json".into()]
    }

    #[test]
    fn kind_token_is_other_opencode() {
        assert_eq!(
            OpencodeHarness::new().kind(),
            HarnessKind::Other("opencode".into())
        );
        assert_eq!(OpencodeHarness::new().kind().as_str(), "opencode");
        assert_ne!(OpencodeHarness::new().kind(), HarnessKind::OpenCode);
        assert_eq!(OpencodeHarness::default().bin, "opencode");
    }

    #[test]
    fn basic_argv_puts_the_prompt_on_argv() {
        assert_eq!(
            args_of(&job("hello world")),
            vec![
                "run".to_string(),
                "hello world".to_string(),
                "--format".to_string(),
                "json".to_string(),
            ]
        );
        let mut with_newline = job("hello\nworld");
        assert_eq!(args_of(&with_newline)[1], "hello\nworld");
        assert_eq!(
            args_of(&job("")),
            vec!["run", "", "--format", "json"]
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>()
        );
        with_newline.protocol = Protocol::AnthropicMessages;
        with_newline.model = ModelId::new("");
        assert_eq!(args_of(&job("hello\nworld")), args_of(&with_newline));
    }

    #[test]
    fn model_provider_and_session_flags() {
        let mut provider_only = job("hi");
        provider_only
            .params
            .extra
            .insert("provider".into(), Value::String("anthropic".into()));
        assert_eq!(args_of(&provider_only), basic());

        let mut from_extra = job("hi");
        from_extra
            .params
            .extra
            .insert("model".into(), Value::String("gpt-4o".into()));
        assert_eq!(
            args_of(&from_extra),
            vec!["run", "hi", "--format", "json", "--model", "gpt-4o"]
        );

        let mut joined = job("hi");
        joined
            .params
            .extra
            .insert("model".into(), Value::String("claude-4-sonnet".into()));
        joined
            .params
            .extra
            .insert("provider".into(), Value::String("anthropic".into()));
        assert_eq!(
            args_of(&joined),
            vec![
                "run",
                "hi",
                "--format",
                "json",
                "--model",
                "anthropic/claude-4-sonnet",
            ]
        );
        assert!(!args_of(&joined).iter().any(|arg| arg == "--provider"));

        let mut slashed = job("hi");
        slashed
            .params
            .extra
            .insert("model".into(), Value::String("openai/gpt-4o".into()));
        slashed
            .params
            .extra
            .insert("provider".into(), Value::String("anthropic".into()));
        assert_eq!(
            args_of(&slashed)
                .into_iter()
                .filter(|arg| arg != "run" && arg != "hi" && arg != "--format" && arg != "json")
                .collect::<Vec<_>>(),
            vec!["--model".to_string(), "openai/gpt-4o".to_string()]
        );

        let mut from_job = job("hi");
        from_job.model = ModelId::new("gpt-4o");
        from_job
            .params
            .extra
            .insert("model".into(), Value::String("from-extra".into()));
        from_job
            .params
            .extra
            .insert("provider".into(), Value::String("anthropic".into()));
        let args = args_of(&from_job);
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--model", "anthropic/gpt-4o"])
        );
        assert!(!args.iter().any(|arg| arg == "from-extra"));

        let mut session = job("task");
        session.model = ModelId::new("gpt-4o");
        session.resume = Some("s1".into());
        assert_eq!(
            args_of(&session),
            vec![
                "run",
                "task",
                "--format",
                "json",
                "--model",
                "gpt-4o",
                "--session",
                "s1",
            ]
        );
        let mut empty_session = job("hi");
        empty_session.resume = Some(String::new());
        assert!(!args_of(&empty_session).iter().any(|arg| arg == "--session"));
    }

    #[test]
    fn sandbox_does_not_change_argv() {
        let read_only = args_of(&job("hi"));
        for sandbox in [Sandbox::ReadOnly, Sandbox::Write, Sandbox::Full] {
            let mut sample = job("hi");
            sample.sandbox = sandbox;
            let args = args_of(&sample);
            assert_eq!(args, read_only);
            assert!(!args.iter().any(|arg| arg == "--sandbox"));
            assert!(!args.iter().any(|arg| arg == "--permission-mode"));
        }
    }

    #[test]
    fn identity_keys_are_not_flags_and_passthrough_remains() {
        let mut sample = job("hi");
        let extra = &mut sample.params.extra;
        extra.insert("owner_user_id".into(), Value::String("alice".into()));
        extra.insert("bound_owner".into(), Value::String("alice".into()));
        extra.insert("run_id".into(), Value::String("disp-1".into()));
        extra.insert("owner-user-id".into(), Value::String("alice".into()));
        extra.insert("bound-owner".into(), Value::String("alice".into()));
        extra.insert("run-id".into(), Value::String("disp-1".into()));
        extra.insert("continue".into(), Value::Bool(true));
        extra.insert("title".into(), Value::String("review".into()));
        assert_eq!(
            args_of(&sample),
            vec![
                "run",
                "hi",
                "--format",
                "json",
                "--continue",
                "--title",
                "review",
            ]
        );

        let mut kept = job("hi");
        kept.params
            .extra
            .insert("model".into(), Value::String("gpt-4o".into()));
        kept.params
            .extra
            .insert("owner_user_id".into(), Value::String("alice".into()));
        kept.params
            .extra
            .insert("variant".into(), Value::String("fast".into()));
        assert_eq!(
            args_of(&kept),
            vec![
                "run",
                "hi",
                "--format",
                "json",
                "--model",
                "gpt-4o",
                "--variant",
                "fast",
            ]
        );
    }

    #[test]
    fn format_stays_json_and_flag_like_values_are_dropped() {
        let mut sample = job("hi");
        let extra = &mut sample.params.extra;
        extra.insert("format".into(), Value::String("stream-json".into()));
        extra.insert("model".into(), Value::String("--injected".into()));
        extra.insert(
            "file".into(),
            Value::Array(vec![
                Value::String("ok".into()),
                Value::from(2),
                Value::String("-rf".into()),
                Value::Null,
                Value::Bool(false),
            ]),
        );
        extra.insert("max_steps".into(), Value::from(3));
        extra.insert("ratio".into(), Value::from(1.5_f64));
        extra.insert("verbose".into(), Value::Bool(false));
        extra.insert("note".into(), Value::Null);
        extra.insert("empty".into(), Value::String(String::new()));
        extra.insert("zero".into(), Value::from(0));
        assert_eq!(
            args_of(&sample),
            vec![
                "run",
                "hi",
                "--format",
                "json",
                "--empty",
                "",
                "--file",
                "ok",
                "--file",
                "2",
                "--file",
                "False",
                "--max-steps",
                "3",
                "--ratio",
                "1.5",
                "--zero",
                "0",
            ]
        );
        assert!(!args_of(&sample).iter().any(|arg| arg == "stream-json"));
        assert!(!args_of(&sample).iter().any(|arg| arg == "--injected"));
        assert!(!args_of(&sample).iter().any(|arg| arg == "-rf"));
    }

    #[test]
    fn non_scalar_extra_is_refused_without_copying_the_value() {
        let mut sample = job("hi");
        sample.params.extra.insert(
            "file".into(),
            Value::Object(Map::from_iter([(
                "path".to_string(),
                Value::String("secret-path".into()),
            )])),
        );
        let err = build_args(&sample).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unsupported);
        assert!(err.message.contains("file"), "{}", err.message);
        assert!(!err.message.contains("secret-path"), "{}", err.message);

        let mut model = job("hi");
        model.params.extra.insert("model".into(), Value::from(1));
        let err = build_args(&model).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unsupported);
        assert!(err.message.contains("model"), "{}", err.message);
        assert!(!err.message.contains('1'), "{}", err.message);
    }

    #[test]
    fn generation_params_and_endpoint_do_not_change_argv() {
        let mut sample = job("hi");
        sample.params.temperature = Some(0.2);
        sample.params.top_p = Some(0.5);
        sample.params.max_output_tokens = Some(16);
        sample.params.effort = Some(Effort::High);
        sample.endpoint = Some(Endpoint {
            base_url: "https://endpoint.example".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("endpoint-canary-secret"),
            }),
        });
        let args = args_of(&sample);
        assert_eq!(args, basic());
        let rendered = args.join("\n");
        assert!(!rendered.contains("endpoint-canary-secret"));
        assert!(!rendered.contains("https://endpoint.example"));
        assert!(!rendered.contains("high"));
        assert!(!rendered.contains("0.2"));
    }

    #[test]
    fn env_policy_is_applied_without_extra_removals() {
        let parent = vec![
            ("PATH".to_string(), "/usr/bin:/bin".to_string()),
            ("HOME".to_string(), "/tmp/opencode-home".to_string()),
            ("SOME_API_KEY".to_string(), "parent-secret".to_string()),
        ];
        let harness =
            OpencodeHarness::with_binary_and_parent_env_for_tests("opencode", parent.clone());
        let mut scrubbed = job("hi");
        scrubbed.env = EnvPolicy::scrubbed().inject("OPENCODE_API_KEY", "kept-on-purpose");
        let env = harness.build_env(&scrubbed);
        assert_eq!(
            env.get("OPENCODE_API_KEY").map(String::as_str),
            Some("kept-on-purpose")
        );
        assert_eq!(
            env.get("HOME").map(String::as_str),
            Some("/tmp/opencode-home")
        );
        assert!(!env.contains_key("SOME_API_KEY"));

        let mut full = job("hi");
        full.env = EnvPolicy {
            mode: InheritMode::Full,
            ..EnvPolicy::default()
        };
        let env = harness.build_env(&full);
        assert_eq!(
            env.get("SOME_API_KEY").map(String::as_str),
            Some("parent-secret")
        );
    }

    #[test]
    fn json_text_session_usage_and_markers() {
        let stdout = "\
{\"type\":\"step_start\",\"sessionID\":\"ses_123\",\"part\":{\"type\":\"step-start\"}}\n\
{\"type\":\"text\",\"sessionID\":\"ses_123\",\"part\":{\"type\":\"text\",\"text\":\"OK<|assistant|>\"}}\n\
{\"type\":\"step_finish\",\"sessionID\":\"ses_123\",\"part\":{\"type\":\"step-finish\",\"tokens\":{\"input\":10,\"output\":2,\"total\":12}}}\n";
        let parsed = parse_lines(&collect_lines(stdout, ""));
        assert_eq!(parsed.text, "OK");
        assert_eq!(parsed.session_id.as_deref(), Some("ses_123"));
        assert!(parsed.error_text.is_empty());
        let usage = parsed.usage.unwrap();
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.output_tokens, 2);
        assert_eq!(usage.reasoning_tokens, None);
        assert_eq!(usage.cached_input_tokens, None);
        assert!(!parsed.text.contains("step_start"));

        let marked = parse_lines(&collect_lines(
            "{\"type\":\"text\",\"part\":{\"text\":\"OK<|assistant|></tool_call>\"}}\n",
            "",
        ));
        assert_eq!(marked.text, "OK");
        let plain = parse_lines(&collect_lines("OK<|assistant|></tool_calls>\n", ""));
        assert_eq!(plain.text, "OK");
        let stripped = parse_lines(&collect_lines(
            "<|end|><|endoftext|>X<|tool|><|user|><|system|>\n<tool_call>A</tool_call><tool_calls>B</tool_calls>\n",
            "",
        ));
        assert_eq!(stripped.text, "X\nAB");
    }

    #[test]
    fn plain_session_lines_and_json_ids_share_one_last_wins_slot() {
        let parsed = parse_lines(&collect_lines("Hello!\nHow can I help?\n", ""));
        assert_eq!(parsed.text, "Hello!\nHow can I help?");
        assert_eq!(parsed.session_id, None);
        assert!(parsed.error_text.is_empty());

        let upper = parse_lines(&collect_lines("Session: abc-123\nOutput here\n", ""));
        assert_eq!(upper.session_id.as_deref(), Some("abc-123"));
        assert_eq!(upper.text, "Output here");

        let lower = parse_lines(&collect_lines("session: xyz-789\nResult\n", ""));
        assert_eq!(lower.session_id.as_deref(), Some("xyz-789"));
        assert_eq!(lower.text, "Result");

        let empty = parse_lines(&collect_lines("", ""));
        assert_eq!(empty.text, "");
        assert_eq!(empty.session_id, None);

        let blank = parse_lines(&collect_lines("Session:\ntext\n", ""));
        assert_eq!(blank.session_id, None);
        assert_eq!(blank.text, "text");

        let last = parse_lines(&collect_lines(
            "Session: first\nSession: second\noutput\n",
            "",
        ));
        assert_eq!(last.session_id.as_deref(), Some("second"));
        assert_eq!(last.text, "output");

        let colon = parse_lines(&collect_lines("Session: abc:def\n", "kept\n"));
        assert_eq!(colon.session_id.as_deref(), Some("abc:def"));
        assert_eq!(colon.text, "kept");

        let case = parse_lines(&collect_lines("SESSION: no\n", ""));
        assert_eq!(case.session_id, None);
        assert_eq!(case.text, "SESSION: no");

        let json_wins = parse_lines(&collect_lines(
            "Session: plain\n{\"type\":\"text\",\"sessionID\":\"from-json\",\"part\":{\"text\":\"OK\"}}\n",
            "",
        ));
        assert_eq!(json_wins.session_id.as_deref(), Some("from-json"));
        assert_eq!(json_wins.text, "OK");

        let plain_wins = parse_lines(&collect_lines(
            "{\"type\":\"text\",\"sessionID\":\"from-json\",\"part\":{\"text\":\"OK\"}}\nSession: plain\n",
            "",
        ));
        assert_eq!(plain_wins.session_id.as_deref(), Some("plain"));
        assert_eq!(plain_wins.text, "OK");

        let numeric = parse_lines(&collect_lines(
            "{\"sessionID\":7,\"type\":\"text\",\"part\":{\"text\":\"OK\"}}\n",
            "",
        ));
        assert_eq!(numeric.session_id.as_deref(), Some("7"));

        let truthy = parse_lines(&collect_lines(
            "{\"sessionID\":true,\"type\":\"text\",\"part\":{\"text\":\"OK\"}}\n",
            "",
        ));
        assert_eq!(truthy.session_id.as_deref(), Some("True"));

        let keep = parse_lines(&collect_lines(
            "Session: keep\n{\"sessionID\":\"\",\"type\":\"text\",\"part\":{\"text\":\"OK\"}}\n{\"sessionID\":0,\"type\":\"text\",\"part\":{\"text\":\"still\"}}\n",
            "",
        ));
        assert_eq!(keep.session_id.as_deref(), Some("keep"));
        assert_eq!(keep.text, "OK\nstill");

        let spaced = parse_lines(&collect_lines(
            "{\"sessionID\":\"  \",\"type\":\"text\",\"part\":{\"text\":\"OK\"}}\n",
            "",
        ));
        assert_eq!(spaced.session_id.as_deref(), Some("  "));

        let stderr = parse_lines(&collect_lines("A\n", "Session: from-err\nB\n"));
        assert_eq!(stderr.text, "A\nB");
        assert_eq!(stderr.session_id.as_deref(), Some("from-err"));
    }

    #[test]
    fn error_events_fail_and_plain_error_lines_stay_text() {
        let plain = parse_lines(&collect_lines("ERROR: something went wrong\n", ""));
        assert!(plain.error_text.is_empty());
        assert_eq!(plain.text, "ERROR: something went wrong");

        let error = parse_lines(&collect_lines(
            "{\"type\":\"error\",\"error\":\"from-error\",\"message\":\"from-message\"}\n",
            "",
        ));
        assert_eq!(error.error_text, "from-error");

        let message = parse_lines(&collect_lines(
            "{\"type\":\"error\",\"error\":\"\",\"message\":\"from-message\"}\n",
            "",
        ));
        assert_eq!(message.error_text, "from-message");

        let event = parse_lines(&collect_lines(
            "{\"type\":\"event_error\",\"message\":\"event failed\"}\n",
            "",
        ));
        assert_eq!(event.error_text, "event failed");

        let both = parse_lines(&collect_lines(
            "{\"type\":\"error\",\"error\":\"one\"}\n{\"type\":\"event_error\",\"error\":\"two\"}\n",
            "",
        ));
        assert_eq!(both.error_text, "one\ntwo");

        let bare = parse_lines(&collect_lines("{\"type\":\"error\"}\n", ""));
        assert!(
            bare.error_text.contains("\"type\":\"error\""),
            "{}",
            bare.error_text
        );

        let outcome = classify_exit(
            0,
            "{\"type\":\"text\",\"part\":{\"text\":\"partial\"},\"sessionID\":\"s\"}\n{\"type\":\"error\",\"error\":\"provider down\"}\n",
            "",
            Duration::ZERO,
        )
        .unwrap_err();
        assert_eq!(outcome.kind, ErrorKind::HarnessFailed);
        assert!(
            outcome.message.contains("provider down"),
            "{}",
            outcome.message
        );

        let empty = classify_exit(0, "<|assistant|>\n", "", Duration::ZERO).unwrap_err();
        assert_eq!(empty.kind, ErrorKind::HarnessFailed);
        assert!(empty.message.contains("no answer"), "{}", empty.message);

        let nonzero = classify_exit(23, "boom\n", "", Duration::ZERO).unwrap_err();
        assert_eq!(nonzero.kind, ErrorKind::HarnessFailed);
        assert!(nonzero.message.contains("23"), "{}", nonzero.message);
        assert!(nonzero.message.contains("boom"), "{}", nonzero.message);
        assert_eq!(
            nonzero_message(4, "unique-error-token", &["plain".into()]),
            "opencode exited with code 4: unique-error-token; output tail: plain"
        );
        assert_eq!(
            nonzero_message(4, "plain", &["plain".into()]),
            "opencode exited with code 4: plain"
        );
        assert_eq!(
            nonzero_message(4, "", &[]),
            "opencode exited with code 4: Command exited with code 4"
        );
    }

    #[test]
    fn usage_object_follows_the_last_finish_event() {
        let lines = collect_lines(
            "\
{\"type\":\"step_finish\",\"part\":{\"tokens\":{\"input\":1,\"output\":1,\"total\":2}}}\n\
{\"type\":\"step_finish\",\"part\":{\"type\":\"step-finish\"}}\n\
{\"type\":\"step_finish\",\"part\":{\"tokens\":{\"input\":9,\"output\":8,\"total\":17}}}\n\
{\"type\":\"text\",\"part\":{\"text\":\"OK\"}}\n",
            "",
        );
        let parsed = parse_lines(&lines);
        let usage = parsed.usage.unwrap();
        assert_eq!(usage.input_tokens, 9);
        assert_eq!(usage.output_tokens, 8);

        let preserved = parse_lines(&collect_lines(
            "\
{\"type\":\"step_finish\",\"part\":{\"tokens\":{\"input\":9,\"output\":8,\"total\":17}}}\n\
{\"type\":\"step_finish\",\"part\":{\"type\":\"step-finish\"}}\n\
{\"type\":\"text\",\"part\":{\"text\":\"OK\"}}\n",
            "",
        ));
        let usage = preserved.usage.unwrap();
        assert_eq!(usage.input_tokens, 9);
        assert_eq!(usage.output_tokens, 8);

        let zero = parse_lines(&collect_lines(
            "{\"type\":\"step_finish\",\"part\":{\"tokens\":{}}}\n{\"type\":\"text\",\"part\":{\"text\":\"OK\"}}\n",
            "",
        ));
        let usage = zero.usage.unwrap();
        assert_eq!(usage.input_tokens, 0);
        assert_eq!(usage.output_tokens, 0);

        let total_only = parse_lines(&collect_lines(
            "{\"type\":\"step_finish\",\"part\":{\"tokens\":{\"total\":12}}}\n{\"type\":\"text\",\"part\":{\"text\":\"OK\"}}\n",
            "",
        ));
        let usage = total_only.usage.unwrap();
        assert_eq!(usage.input_tokens, 0);
        assert_eq!(usage.output_tokens, 0);

        let coerced = parse_lines(&collect_lines(
            "{\"type\":\"step_finish\",\"part\":{\"tokens\":{\"input\":\" 8 \",\"output\":true,\"total\":-5}}}\n{\"type\":\"text\",\"part\":{\"text\":\"OK\"}}\n",
            "",
        ));
        let usage = coerced.usage.unwrap();
        assert_eq!(usage.input_tokens, 8);
        assert_eq!(usage.output_tokens, 1);

        let negative = parse_lines(&collect_lines(
            "{\"type\":\"step_finish\",\"part\":{\"tokens\":{\"input\":-5,\"output\":4.9}}}\n{\"type\":\"text\",\"part\":{\"text\":\"OK\"}}\n",
            "",
        ));
        let usage = negative.usage.unwrap();
        assert_eq!(usage.input_tokens, 0);
        assert_eq!(usage.output_tokens, 4);

        let none = parse_lines(&collect_lines(
            "{\"type\":\"text\",\"part\":{\"text\":\"OK\"}}\n",
            "",
        ));
        assert!(none.usage.is_none());

        let scalar = parse_lines(&collect_lines(
            "{\"type\":\"text\",\"part\":{\"text\":1}}\n{\"type\":\"text\",\"part\":{\"text\":0}}\n{\"type\":\"text\",\"part\":{\"text\":true}}\n{\"type\":\"text\",\"part\":{\"text\":\"\"}}\n",
            "",
        ));
        assert_eq!(scalar.text, "1\nTrue");

        let raw_json = parse_lines(&collect_lines("\"hello\"\n[1,2]\nnot-json {\n", ""));
        assert_eq!(raw_json.text, "\"hello\"\n[1,2]\nnot-json {");
        assert!(raw_json.error_text.is_empty());
    }

    #[test]
    fn output_tail_keeps_the_last_lines_and_bounds_characters() {
        let mut lines: Vec<String> = (0..60).map(|index| format!("noise-{index}")).collect();
        lines.push("TAIL_MARK".into());
        let tail = output_tail(&lines);
        assert!(tail.starts_with("noise-11\n"), "{tail}");
        assert!(tail.ends_with("TAIL_MARK"), "{tail}");
        assert!(!tail.contains("noise-0"), "{tail}");
        assert!(!tail.contains("noise-10"), "{tail}");
        let long = "x".repeat(2001);
        let bounded = output_tail(&[long]);
        assert!(bounded.starts_with("..."));
        assert_eq!(bounded.chars().count(), ERROR_TAIL_MAX_CHARS + 3);
    }
}
