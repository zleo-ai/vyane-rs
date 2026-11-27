//! [`Harness`] for the **Grok Build CLI**, one POSIX JSON round.
//!
//! Command shape for [`Sandbox::Write`] and [`Sandbox::Full`]:
//!
//! ```text
//! grok --prompt-file /dev/stdin --output-format json
//!      [-m MODEL] [--reasoning-effort EFFORT]
//!      [--sandbox PROFILE] [--resume ID]
//!      [caller passthrough flags]
//! ```
//!
//! The prompt is the child stdin payload. It is not an argv token.
//! `--prompt-file /dev/stdin` is the POSIX prompt path. Native Windows is
//! refused before spawn: that launch replaces `/dev/stdin` with a prompt file
//! and, for `write` / `full`, passes `--always-approve`. This adapter does not
//! implement that relocation, so it does not start a partial command.
//!
//! * [`Sandbox::ReadOnly`] is refused with [`ErrorKind::Unsupported`] before
//!   any process starts. Vendor `--sandbox read-only` and `--permission-mode
//!   plan` do not stop writes. The read-only tier is a bubblewrap view; this
//!   adapter does not build that view, and the bare CLI is not started in its
//!   place.
//! * [`Sandbox::Write`] and [`Sandbox::Full`] do not add a vendor sandbox flag,
//!   `--permission-mode`, or `--always-approve`. An explicit `sandbox_profile`
//!   string in [`vyane_core::GenParams::extra`] is still passed through as
//!   `--sandbox`, which is not a mapping of Vyane's sandbox tiers and is not
//!   an enforcement claim. Workspace scoping for these tiers is the child
//!   working directory.
//! * `--resume <id>` continues a native session from `job.resume`. The child
//!   working directory is the job workdir. This adapter does not add a
//!   separate resume-directory flag.
//! * `-m` receives the job model id when it is non-empty. A `model` string in
//!   `params.extra` is used only when the job model id is empty.
//! * `--reasoning-effort` comes from `params.extra["reasoning_effort"]` when
//!   that key is present, and otherwise from [`vyane_core::GenParams::effort`].
//!   A present null, false, or empty string omits the flag. Temperature and
//!   other generation fields are not rewritten into flags.
//! * Remaining `params.extra` entries follow the passthrough rules: snake case
//!   becomes a kebab flag, true emits the flag alone, false and null are
//!   skipped, strings that start with `-` are dropped before they become
//!   values, and an object value is refused before spawn. Identity keys
//!   (`owner_user_id`, `bound_owner`, `run_id`, and their kebab aliases) are
//!   never flags. Private view keys are removed and are not a read-only view.
//! * A non-empty endpoint base URL, or any endpoint credential, is refused
//!   before spawn. This adapter injects no credential. `XAI_API_KEY` is
//!   removed from the child environment after the policy is applied, including
//!   when the caller inherited or injected that name. Subscription login and
//!   metered API billing stay separate.
//! * `--output-format` defaults to `json`. One pretty-printed JSON object is
//!   recovered from stdout and then stderr, including when log lines surround
//!   it. A line is a candidate only when, after leading whitespace, it starts
//!   with `{`. `text` is the answer. A non-empty `sessionId` is kept.
//!   `stopReason` of `cancelled` is a failure even when the process exits 0.
//!   A truthy `error` field is a failure. Output with no JSON object is a
//!   failure, not an answer. An empty `text` is a failure. `usage.input_tokens`
//!   and `usage.output_tokens` stay the raw fields. `reasoning_tokens` and
//!   `cache_read_input_tokens` are stored beside them and are not added into
//!   the input count. A negative count is stored as zero. `cache_creation_input_tokens`, `total_tokens`,
//!   `total_cost_usd`, and `modelUsage` are not fields of
//!   [`vyane_core::HarnessOutcome`]. `total_tokens` is used only as the Python
//!   gate for whether a usage object exists; when the vendor reports only that
//!   total, the split counts stay zero.
//!
//! Streaming, a pinned workdir, and live spawn authority fail closed before a
//! process starts. A lifecycle reporter is also refused: its start gate needs
//! the same stdin pipe as the prompt.
//!
//! This type is not selected by the service assembler. Profiles and routing
//! keep the Claude Code and Codex CLI support matrix.

use std::collections::BTreeMap;
use std::path::PathBuf;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;
use vyane_core::error::{ErrorKind, Result, VyaneError};
use vyane_core::run::Usage;
use vyane_core::target::{Endpoint, HarnessKind, Sandbox};
use vyane_core::task::Effort;
use vyane_core::traits::{Harness, HarnessJob, HarnessOutcome};

use crate::spawn::{
    RunControl, RunResult, Termination, env_key_list, materialize_env, parent_env_snapshot,
    run_capture_with_stdin,
};

/// Metered API key. This adapter removes it and never injects it.
const ENV_API_KEY: &str = "XAI_API_KEY";

/// Documented Grok Build binary name, resolved through `PATH` at spawn time.
const BIN: &str = "grok";

/// Stable [`HarnessKind::Other`] token for this adapter.
const KIND: &str = "grok-build";

/// POSIX prompt path. Windows launch would replace this; this adapter refuses
/// that host instead of leaving the path in place.
const POSIX_PROMPT_FILE: &str = "/dev/stdin";

const NO_RESULT_DOCUMENT: &str = "grok-build output has no JSON result document";
const GROK_RUN_CANCELLED: &str = "grok-build run was cancelled (stopReason=cancelled)";
const GROK_READONLY_AUTH_REFRESH_FAILURE: &str = "\
grok auth refresh failed: the access token has to be refreshed outside \
the read-only view (grok could not take its auth lock on a read-only \
filesystem and ran without credentials, so its model catalog was empty; \
the 'unknown model id' that follows is a consequence, not the cause). \
Run `grok models` outside the view, or `grok login` if that fails";

const VIEW_DBUS_SOCKET_KEY: &str = "_readonly_view_dbus_socket";
const VIEW_HIDE_PATHS_KEY: &str = "_readonly_view_hide_paths";

const IDENTITY_SKIP: &[&str] = &[
    "owner_user_id",
    "bound_owner",
    "run_id",
    "owner-user-id",
    "bound-owner",
    "run-id",
];

/// Substring checks start at this length so a short prompt is not treated as
/// part of an unrelated flag.
const EMBEDDED_PROMPT_MIN_CHARS: usize = 40;

/// Host prompt transport this adapter can state honestly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromptHost {
    Posix,
    Windows,
}

fn current_prompt_host() -> PromptHost {
    if cfg!(windows) {
        PromptHost::Windows
    } else {
        PromptHost::Posix
    }
}

/// `Harness` adapter for one Grok Build JSON round.
#[derive(Debug, Clone)]
pub struct GrokBuildHarness {
    bin: String,
    parent_env: Option<Vec<(String, String)>>,
}

impl GrokBuildHarness {
    pub fn new() -> Self {
        Self {
            bin: BIN.into(),
            parent_env: None,
        }
    }

    /// Construct a harness with an explicit binary path or name.
    ///
    /// Tests use this with a fake executable. Production callers normally use
    /// [`Self::new`] and resolve `grok` through `PATH`.
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
        env.remove(ENV_API_KEY);
        env
    }

    fn classify_result(&self, result: RunResult) -> Result<HarnessOutcome> {
        match result.termination {
            Termination::Cancelled => Err(VyaneError::cancelled()),
            Termination::TimedOut => Err(VyaneError::new(
                ErrorKind::Timeout,
                "grok-build harness timed out",
            )),
            Termination::Exited(code) => {
                classify_exit(code, &result.stdout, &result.stderr, result.duration)
            }
        }
    }
}

impl Default for GrokBuildHarness {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct Launch {
    args: Vec<String>,
    cwd: Option<PathBuf>,
}

fn unsupported(message: impl Into<String>) -> VyaneError {
    VyaneError::new(ErrorKind::Unsupported, message)
}

fn build_launch(job: &HarnessJob, host: PromptHost) -> Result<Launch> {
    if job.sandbox == Sandbox::ReadOnly {
        return Err(unsupported(
            "grok-build refuses to spawn for sandbox read-only: vendor --sandbox and --permission-mode flags are not enforcement, and this adapter does not build the bubblewrap read-only view, so the bare CLI is not started",
        ));
    }
    if host == PromptHost::Windows {
        return Err(unsupported(
            "grok-build refuses to spawn on Windows: this adapter implements the POSIX stdin prompt (--prompt-file /dev/stdin) and does not relocate that prompt file or add --always-approve",
        ));
    }
    reject_endpoint(job.endpoint.as_ref())?;
    if job.harness_lifecycle_reporter.is_some() {
        return Err(unsupported(
            "grok-build refuses to spawn because the prompt is delivered on stdin and cannot share that pipe with a lifecycle start gate",
        ));
    }

    let args = build_argv(job)?;
    assert_prompt_not_in_argv(&args, &job.prompt)?;
    Ok(Launch {
        args,
        cwd: job
            .workdir
            .as_ref()
            .filter(|dir| !dir.as_os_str().is_empty())
            .cloned(),
    })
}

fn reject_endpoint(endpoint: Option<&Endpoint>) -> Result<()> {
    let Some(endpoint) = endpoint else {
        return Ok(());
    };
    if !endpoint.base_url.is_empty() || endpoint.auth.is_some() {
        return Err(unsupported(
            "grok-build refuses to spawn because this adapter injects no endpoint and never sets XAI_API_KEY",
        ));
    }
    Ok(())
}

fn build_argv(job: &HarnessJob) -> Result<Vec<String>> {
    let mut extra = sanitize_extra(&job.params.extra);
    extra.remove(VIEW_DBUS_SOCKET_KEY);
    extra.remove(VIEW_HIDE_PATHS_KEY);
    extra.remove("provider");

    let output_format = consume_output_format(&mut extra)?;
    let model = consume_model(&mut extra, job.model.as_str())?;
    let reasoning = consume_reasoning(&mut extra, job.params.effort)?;
    let sandbox_profile = consume_sandbox_profile(&mut extra)?;
    let passthrough = extra_flags(&extra)?;

    let mut args = vec!["--prompt-file".to_string(), POSIX_PROMPT_FILE.to_string()];
    if !output_format.is_empty() {
        args.push("--output-format".into());
        args.push(output_format);
    }
    if !model.is_empty() {
        args.push("-m".into());
        args.push(model);
    }
    if let Some(effort) = reasoning {
        args.push("--reasoning-effort".into());
        args.push(effort);
    }
    if let Some(profile) = sandbox_profile {
        args.push("--sandbox".into());
        args.push(profile);
    }
    if let Some(id) = job.resume.as_ref().filter(|id| !id.is_empty()) {
        args.push("--resume".into());
        args.push(id.clone());
    }
    args.extend(passthrough);
    Ok(args)
}

fn consume_output_format(extra: &mut Map<String, Value>) -> Result<String> {
    match extra.remove("output_format") {
        None => Ok("json".to_string()),
        Some(Value::String(value)) => Ok(value),
        Some(_) => Err(unsupported(
            "grok-build refuses to spawn because output_format must be a string",
        )),
    }
}

fn consume_model(extra: &mut Map<String, Value>, job_model: &str) -> Result<String> {
    let from_extra = match extra.remove("model") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value),
        Some(_) => {
            return Err(unsupported(
                "grok-build refuses to spawn because model extra must be a string",
            ));
        }
    };
    if !job_model.is_empty() {
        return Ok(job_model.to_string());
    }
    Ok(from_extra.unwrap_or_default())
}

fn consume_reasoning(
    extra: &mut Map<String, Value>,
    effort: Option<Effort>,
) -> Result<Option<String>> {
    match extra.remove("reasoning_effort") {
        None => Ok(effort.map(|effort| effort.as_str().to_string())),
        Some(Value::Null) | Some(Value::Bool(false)) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(unsupported(
            "grok-build refuses to spawn because reasoning_effort must be a string",
        )),
    }
}

fn consume_sandbox_profile(extra: &mut Map<String, Value>) -> Result<Option<String>> {
    match extra.remove("sandbox_profile") {
        None | Some(Value::Null) | Some(Value::Bool(false)) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(unsupported(
            "grok-build refuses to spawn because sandbox_profile must be a string",
        )),
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
            "grok-build refuses to spawn because extra argument `{key}` is not a scalar flag value"
        ))),
    }
}

fn number_flag(number: &serde_json::Number) -> String {
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

fn assert_prompt_not_in_argv(argv: &[String], prompt: &str) -> Result<()> {
    if prompt.is_empty() {
        return Ok(());
    }
    let check_substring = prompt.chars().count() >= EMBEDDED_PROMPT_MIN_CHARS;
    for (index, token) in argv.iter().enumerate() {
        let leaked = token == prompt || (check_substring && token.contains(prompt));
        if leaked {
            return Err(unsupported(format!(
                "grok-build refuses to spawn because the prompt is delivered on stdin and also appears in argv token {index}"
            )));
        }
    }
    Ok(())
}

fn classify_exit(
    code: i32,
    stdout: &str,
    stderr: &str,
    duration: std::time::Duration,
) -> Result<HarnessOutcome> {
    let parsed = parse_grok_output(stdout, stderr);
    if code != 0 {
        return Err(VyaneError::new(
            ErrorKind::HarnessFailed,
            nonzero_message(code, &parsed.error_text, stdout, stderr),
        ));
    }
    if parsed.error_text.is_empty() && !parsed.text.is_empty() {
        return Ok(HarnessOutcome {
            text: parsed.text,
            native_session_id: parsed.session_id,
            usage: parsed.usage,
            exit_code: code,
            duration,
        });
    }
    Err(VyaneError::new(
        ErrorKind::HarnessFailed,
        failure_message(&parsed.error_text, &parsed.raw),
    ))
}

fn failure_message(error_text: &str, raw: &str) -> String {
    let tail = bound_text(&raw.trim().replace('\n', " "));
    if error_text.is_empty() {
        if tail.is_empty() {
            return "grok-build JSON result did not include answer text".to_string();
        }
        return format!("grok-build JSON result did not include answer text; output tail: {tail}");
    }
    let detail = error_detail(error_text);
    if tail.is_empty() || tail.contains(error_text) || detail.contains(&tail) {
        format!("grok-build returned an error: {detail}")
    } else {
        format!("grok-build returned an error: {detail}; output tail: {tail}")
    }
}

fn nonzero_message(code: i32, error_text: &str, stdout: &str, stderr: &str) -> String {
    let tail = output_tail(stdout, stderr);
    let details = if error_text.is_empty() {
        if tail.is_empty() {
            format!("Command exited with code {code}")
        } else {
            tail
        }
    } else if tail.is_empty() || tail.contains(error_text) {
        error_detail(error_text)
    } else {
        format!("{}; output tail: {}", error_detail(error_text), tail)
    };
    format!("grok-build exited with code {code}: {details}")
}

fn error_detail(error_text: &str) -> String {
    if error_text == NO_RESULT_DOCUMENT
        || error_text == GROK_RUN_CANCELLED
        || error_text == GROK_READONLY_AUTH_REFRESH_FAILURE
        || error_text.chars().count() <= 200
    {
        error_text.to_string()
    } else {
        bound_text(error_text)
    }
}

fn output_tail(stdout: &str, stderr: &str) -> String {
    let mut combined = String::new();
    push_flat(&mut combined, stdout);
    push_flat(&mut combined, stderr);
    bound_text(combined.trim())
}

fn push_flat(combined: &mut String, text: &str) {
    let flat = text.trim().replace('\n', " ");
    if flat.is_empty() {
        return;
    }
    if !combined.is_empty() {
        combined.push(' ');
    }
    combined.push_str(&flat);
}

struct Parsed {
    text: String,
    session_id: Option<String>,
    error_text: String,
    usage: Option<Usage>,
    raw: String,
}

fn parse_grok_output(stdout: &str, stderr: &str) -> Parsed {
    let mut lines = Vec::new();
    lines.extend(stdout.lines().map(str::to_string));
    lines.extend(stderr.lines().map(str::to_string));
    let raw = lines.join("\n");
    let document = parse_result_document(&lines);
    let mut parsed = match document {
        None => Parsed {
            text: raw.clone(),
            session_id: None,
            error_text: NO_RESULT_DOCUMENT.to_string(),
            usage: None,
            raw,
        },
        Some(data) => {
            let text = match data.get("text") {
                Some(Value::String(text)) => text.clone(),
                _ => String::new(),
            };
            let session_id = match data.get("sessionId") {
                Some(Value::String(id)) if !id.is_empty() => Some(id.clone()),
                _ => None,
            };
            Parsed {
                text,
                session_id,
                error_text: error_from_document(&data),
                usage: parse_usage(&data),
                raw,
            }
        }
    };
    if (!parsed.error_text.is_empty() || parsed.text.is_empty()) && auth_refresh_failure(&lines) {
        parsed.error_text = GROK_READONLY_AUTH_REFRESH_FAILURE.to_string();
    }
    parsed
}

fn parse_result_document(lines: &[String]) -> Option<Map<String, Value>> {
    let joined = lines.join("\n");
    let mut offset = 0usize;
    for line in lines {
        let body = line.trim_start();
        if body.starts_with('{') {
            let start = offset + (line.len() - body.len());
            if let Some(object) = raw_decode_object(&joined[start..]) {
                return Some(object);
            }
        }
        offset = offset.saturating_add(line.len()).saturating_add(1);
    }
    None
}

fn raw_decode_object(input: &str) -> Option<Map<String, Value>> {
    let mut deserializer = serde_json::Deserializer::from_str(input);
    let value = Value::deserialize(&mut deserializer).ok()?;
    value.as_object().cloned()
}

fn error_from_document(data: &Map<String, Value>) -> String {
    let mut error_text = match data.get("error").filter(|value| json_truthy(value)) {
        Some(Value::String(text)) => text.clone(),
        Some(other) => {
            serde_json::to_string(other).unwrap_or_else(|_| "unprintable error".to_string())
        }
        None => String::new(),
    };
    if data.get("stopReason").and_then(Value::as_str) == Some("cancelled") {
        if error_text.is_empty() {
            error_text = GROK_RUN_CANCELLED.to_string();
        } else {
            error_text = format!("{error_text}; {GROK_RUN_CANCELLED}");
        }
    }
    error_text
}

fn json_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => !number_is_zero(number),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

fn number_is_zero(number: &serde_json::Number) -> bool {
    number.as_f64().is_some_and(|value| value == 0.0)
        || number.as_i64() == Some(0)
        || number.as_u64() == Some(0)
}

fn parse_usage(data: &Map<String, Value>) -> Option<Usage> {
    let usage = data.get("usage")?.as_object()?;
    let input_tokens = int_value(usage.get("input_tokens"));
    let output_tokens = int_value(usage.get("output_tokens"));
    let mut total_tokens = int_value(usage.get("total_tokens"));
    if total_tokens == 0 {
        total_tokens = input_tokens.saturating_add(output_tokens);
    }
    if input_tokens == 0 && output_tokens == 0 && total_tokens == 0 {
        return None;
    }
    Some(Usage {
        input_tokens,
        output_tokens,
        reasoning_tokens: reported_count(usage, "reasoning_tokens"),
        cached_input_tokens: reported_count(usage, "cache_read_input_tokens"),
    })
}

fn reported_count(usage: &Map<String, Value>, key: &str) -> Option<u64> {
    usage.get(key).map(|value| int_value(Some(value)))
}

fn int_value(value: Option<&Value>) -> u64 {
    let Some(value) = value else {
        return 0;
    };
    match value {
        Value::Bool(true) => 1,
        Value::Bool(false) | Value::Null | Value::Array(_) | Value::Object(_) => 0,
        Value::Number(number) => number_to_count(number),
        Value::String(text) => parse_int_string(text),
    }
}

fn number_to_count(number: &serde_json::Number) -> u64 {
    if let Some(value) = number.as_u64() {
        return value;
    }
    if let Some(value) = number.as_i64() {
        return if value < 0 {
            0
        } else {
            u64::try_from(value).unwrap_or(0)
        };
    }
    if let Some(value) = number.as_f64()
        && value.is_finite()
        && value > 0.0
        && value < u64::MAX as f64
    {
        return value.trunc() as u64;
    }
    0
}

fn parse_int_string(text: &str) -> u64 {
    let text = text.trim();
    let digits = text.strip_prefix('+').unwrap_or(text);
    if digits.is_empty()
        || digits.starts_with('-')
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return 0;
    }
    digits.parse::<u64>().unwrap_or(0)
}

fn auth_refresh_failure(lines: &[String]) -> bool {
    let mut lock_on_readonly_fs = false;
    let mut no_credentials = false;
    for raw in lines {
        let line = strip_ansi(raw);
        if line.contains("auth lock")
            && (line.contains("Read-only file system") || line.contains("os error 30"))
        {
            lock_on_readonly_fs = true;
        }
        if line.contains("No auth credentials") {
            no_credentials = true;
        }
    }
    lock_on_readonly_fs && no_credentials
}

fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find('\u{1b}') {
        out.push_str(&rest[..start]);
        let after = &rest[start..];
        if let Some(stripped) = consume_ansi(after) {
            rest = stripped;
        } else {
            out.push('\u{1b}');
            rest = &after['\u{1b}'.len_utf8()..];
        }
    }
    out.push_str(rest);
    out
}

fn consume_ansi(text: &str) -> Option<&str> {
    let bytes = text.as_bytes();
    if bytes.len() < 3 || bytes[0] != 0x1b || bytes[1] != b'[' {
        return None;
    }
    let mut index = 2;
    while index < bytes.len() && (bytes[index].is_ascii_digit() || bytes[index] == b';') {
        index += 1;
    }
    if index < bytes.len() && bytes[index].is_ascii_alphabetic() {
        Some(&text[index + 1..])
    } else {
        None
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

#[async_trait]
impl Harness for GrokBuildHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::Other(KIND.into())
    }

    async fn available(&self) -> bool {
        crate::probe::binary_available(&self.bin).await
    }

    async fn run(&self, job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome> {
        let launch = build_launch(&job, current_prompt_host())?;
        let env = self.build_env(&job);
        let stdin = job.prompt.clone();

        tracing::debug!(
            harness = KIND,
            argc = launch.args.len(),
            env_keys = %env_key_list(&env),
            "spawning grok-build harness"
        );

        let result = run_capture_with_stdin(
            &self.bin,
            &launch.args,
            launch.cwd.as_deref(),
            &env,
            RunControl::new(cancel, job.timeout, None),
            stdin.as_bytes(),
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
    use vyane_core::target::{AuthMaterial, AuthStyle, ModelId, Protocol, Secret};
    use vyane_core::task::{GenParams, HarnessLifecycleReporter};

    fn job(prompt: &str) -> HarnessJob {
        HarnessJob {
            prompt: prompt.into(),
            model: ModelId::new(""),
            protocol: Protocol::OpenaiResponses,
            endpoint: None,
            params: GenParams::default(),
            workdir: None,
            sandbox: Sandbox::Write,
            resume: None,
            env: EnvPolicy::scrubbed(),
            timeout: None,
            harness_lifecycle_reporter: None,
        }
    }

    fn posix_args(sample: &HarnessJob) -> Vec<String> {
        build_launch(sample, PromptHost::Posix).unwrap().args
    }

    #[test]
    fn kind_token_is_grok_build() {
        assert_eq!(
            GrokBuildHarness::new().kind(),
            HarnessKind::Other("grok-build".into())
        );
        assert_eq!(GrokBuildHarness::new().kind().as_str(), "grok-build");
    }

    #[test]
    fn read_only_is_rejected_before_argv_even_with_a_sandbox_profile() {
        let mut sample = job("hello");
        sample.sandbox = Sandbox::ReadOnly;
        sample.endpoint = Some(Endpoint {
            base_url: "https://endpoint.example".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("must-not-matter"),
            }),
        });
        sample
            .params
            .extra
            .insert("sandbox_profile".into(), Value::String("read-only".into()));
        let err = build_launch(&sample, PromptHost::Posix).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unsupported);
        assert!(err.message.contains("read-only"), "{}", err.message);
        assert!(err.message.contains("not started"), "{}", err.message);
        assert!(!err.message.contains("must-not-matter"));
        assert!(!err.message.contains("https://endpoint.example"));
    }

    #[test]
    fn windows_write_and_full_are_rejected_before_spawn() {
        for sandbox in [Sandbox::Write, Sandbox::Full] {
            let mut sample = job("hello");
            sample.sandbox = sandbox;
            let err = build_launch(&sample, PromptHost::Windows).unwrap_err();
            assert_eq!(err.kind, ErrorKind::Unsupported);
            assert!(err.message.contains("Windows"), "{}", err.message);
            assert!(err.message.contains("--always-approve"), "{}", err.message);
            assert!(err.message.contains("/dev/stdin"), "{}", err.message);
        }
    }

    #[test]
    fn endpoint_auth_and_base_url_are_rejected_on_write_and_full() {
        for sandbox in [Sandbox::Write, Sandbox::Full] {
            let mut native = job("hello");
            native.sandbox = sandbox;
            native.endpoint = Some(Endpoint {
                base_url: String::new(),
                auth: None,
            });
            assert!(
                posix_args(&native)
                    .windows(2)
                    .any(|pair| pair == ["--prompt-file", "/dev/stdin"])
            );

            for base_url in ["https://endpoint.example", "  ", ""] {
                let mut sample = job("hello");
                sample.sandbox = sandbox;
                sample.endpoint = Some(Endpoint {
                    base_url: base_url.into(),
                    auth: Some(AuthMaterial {
                        style: AuthStyle::XApiKey,
                        secret: Secret::new("must-not-matter"),
                    }),
                });
                let err = build_launch(&sample, PromptHost::Posix).unwrap_err();
                assert_eq!(err.kind, ErrorKind::Unsupported, "{base_url:?} {sandbox}");
                assert!(err.message.contains("XAI_API_KEY"), "{}", err.message);
                assert!(!err.message.contains("must-not-matter"));
                assert!(!err.message.contains("https://endpoint.example"));
            }
        }
    }

    #[test]
    fn write_and_full_share_the_posix_json_argv_and_keep_the_prompt_off_it() {
        for sandbox in [Sandbox::Write, Sandbox::Full] {
            for prompt in ["--always-approve", "--sandbox", "-m", "--", "answer"] {
                let mut sample = job(prompt);
                sample.sandbox = sandbox;
                let args = posix_args(&sample);
                assert_eq!(
                    args,
                    vec!["--prompt-file", "/dev/stdin", "--output-format", "json"]
                );
                assert!(
                    !args
                        .iter()
                        .any(|arg| arg == prompt && prompt.starts_with('-'))
                );
                assert!(!args.iter().any(|arg| arg == "--permission-mode"));
                assert!(!args.iter().any(|arg| arg == "--always-approve"));
                assert!(!args.iter().any(|arg| arg == "--sandbox"));
                assert!(!args.iter().any(|arg| arg == "XAI_API_KEY"));
            }
        }
    }

    #[test]
    fn argv_passes_model_effort_resume_and_explicit_sandbox_profile() {
        let mut sample = job("answer\nthis");
        sample.model = ModelId::new("grok-4.7");
        sample.workdir = Some("/work/tree".into());
        sample.resume = Some("session-1".into());
        sample.params.effort = Some(Effort::High);
        sample.params.temperature = Some(0.2);
        sample.sandbox = Sandbox::Full;
        sample
            .params
            .extra
            .insert("sandbox_profile".into(), Value::String("workspace".into()));
        sample
            .params
            .extra
            .insert("model".into(), Value::String("other-model".into()));

        let launch = build_launch(&sample, PromptHost::Posix).unwrap();
        assert_eq!(launch.cwd.unwrap().as_os_str(), "/work/tree");
        assert_eq!(
            launch.args,
            vec![
                "--prompt-file",
                "/dev/stdin",
                "--output-format",
                "json",
                "-m",
                "grok-4.7",
                "--reasoning-effort",
                "high",
                "--sandbox",
                "workspace",
                "--resume",
                "session-1",
            ]
        );
        assert!(!launch.args.iter().any(|arg| arg == "--model"));
        assert!(!launch.args.iter().any(|arg| arg == "other-model"));
        assert!(!launch.args.iter().any(|arg| arg == "answer\nthis"));
    }

    #[test]
    fn extra_reasoning_overrides_effort_and_false_omits_it() {
        let mut sample = job("hello");
        sample.params.effort = Some(Effort::Low);
        sample
            .params
            .extra
            .insert("reasoning_effort".into(), Value::String("max".into()));
        assert!(
            posix_args(&sample)
                .windows(2)
                .any(|pair| pair == ["--reasoning-effort", "max"])
        );

        sample
            .params
            .extra
            .insert("reasoning_effort".into(), Value::Bool(false));
        assert!(
            !posix_args(&sample)
                .iter()
                .any(|arg| arg == "--reasoning-effort")
        );
    }

    #[test]
    fn passthrough_skips_identity_view_keys_and_flag_like_values() {
        let mut sample = job("hello");
        let extra = &mut sample.params.extra;
        extra.insert("max_turns".into(), Value::from(3));
        extra.insert("fast".into(), Value::Bool(true));
        extra.insert("disabled".into(), Value::Bool(false));
        extra.insert("note".into(), Value::String("-injected".into()));
        extra.insert("run_id".into(), Value::String("run-1".into()));
        extra.insert("owner_user_id".into(), Value::String("owner".into()));
        extra.insert("bound-owner".into(), Value::String("bound".into()));
        extra.insert(
            VIEW_DBUS_SOCKET_KEY.into(),
            Value::String("/tmp/bus".into()),
        );
        extra.insert(
            VIEW_HIDE_PATHS_KEY.into(),
            Value::Array(vec![Value::String("/tmp/hidden".into())]),
        );
        extra.insert(
            "tag".into(),
            Value::Array(vec![
                Value::String("kept".into()),
                Value::String("-dropped".into()),
                Value::Null,
                Value::Bool(false),
            ]),
        );
        extra.insert("provider".into(), Value::String("grok-build".into()));

        let args = posix_args(&sample);
        assert!(args.windows(2).any(|pair| pair == ["--max-turns", "3"]));
        assert!(args.iter().any(|arg| arg == "--fast"));
        assert!(args.windows(2).any(|pair| pair == ["--tag", "kept"]));
        assert!(args.windows(2).any(|pair| pair == ["--tag", "False"]));
        for forbidden in [
            "--disabled",
            "-injected",
            "--run-id",
            "--owner-user-id",
            "--bound-owner",
            "--readonly-view-dbus-socket",
            "--readonly-view-hide-paths",
            "/tmp/bus",
            "/tmp/hidden",
            "--provider",
            "grok-build",
            "--permission-mode",
            "--always-approve",
        ] {
            assert!(
                !args.iter().any(|arg| arg == forbidden),
                "{forbidden} in {args:?}"
            );
        }
    }

    #[test]
    fn prompt_leaks_in_argv_are_refused_for_short_and_long_prompts() {
        let mut short = job("answer this");
        short
            .params
            .extra
            .insert("note".into(), Value::String("answer this".into()));
        let err = build_launch(&short, PromptHost::Posix).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unsupported);
        assert!(err.message.contains("argv token"), "{}", err.message);
        assert!(!err.message.contains("answer this"));

        let long = "x".repeat(EMBEDDED_PROMPT_MIN_CHARS);
        let mut embedded = job(&long);
        embedded.params.extra.insert(
            "body".into(),
            Value::String(format!("prefix {long} suffix")),
        );
        let err = build_launch(&embedded, PromptHost::Posix).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unsupported);
        assert!(!err.message.contains(&long));
    }

    #[test]
    fn object_extra_is_refused_without_copying_the_value() {
        let mut sample = job("hello");
        sample.params.extra.insert(
            "nested".into(),
            serde_json::json!({"token": "must-not-matter"}),
        );
        let err = build_launch(&sample, PromptHost::Posix).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unsupported);
        assert!(err.message.contains("nested"), "{}", err.message);
        assert!(!err.message.contains("must-not-matter"));
    }

    #[test]
    fn lifecycle_reporter_is_rejected_before_spawn() {
        let mut sample = job("hello");
        sample.harness_lifecycle_reporter = Some(HarnessLifecycleReporter::new(|_| Ok(())));
        let err = build_launch(&sample, PromptHost::Posix).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unsupported);
        assert!(err.message.contains("stdin"), "{}", err.message);
    }

    #[test]
    fn build_env_removes_xai_api_key_from_inherit_and_injection() {
        let parent = vec![
            ("PATH".to_string(), "/usr/bin".to_string()),
            ("HOME".to_string(), "/tmp/home".to_string()),
            ("XAI_API_KEY".to_string(), "parent-metered".to_string()),
            ("OTHER".to_string(), "kept".to_string()),
        ];
        let harness =
            GrokBuildHarness::with_binary_and_parent_env_for_tests("grok", parent.clone());

        let scrubbed = harness.build_env(&job("hello"));
        assert_eq!(scrubbed.get("PATH").map(String::as_str), Some("/usr/bin"));
        assert_eq!(scrubbed.get("HOME").map(String::as_str), Some("/tmp/home"));
        assert!(!scrubbed.contains_key("XAI_API_KEY"));
        assert!(!scrubbed.contains_key("OTHER"));
        assert!(
            !scrubbed
                .values()
                .any(|value| value.contains("parent-metered"))
        );

        let mut full = job("hello");
        full.env.mode = InheritMode::Full;
        full.env
            .inject
            .insert("XAI_API_KEY".into(), Secret::new("injected-metered"));
        full.env
            .inject
            .insert("EXPLICIT_CHILD".into(), Secret::new("yes"));
        let inherited = harness.build_env(&full);
        assert_eq!(inherited.get("OTHER").map(String::as_str), Some("kept"));
        assert_eq!(
            inherited.get("EXPLICIT_CHILD").map(String::as_str),
            Some("yes")
        );
        assert!(!inherited.contains_key("XAI_API_KEY"));
        assert!(
            !inherited
                .values()
                .any(|value| value.contains("parent-metered"))
        );
        assert!(
            !inherited
                .values()
                .any(|value| value.contains("injected-metered"))
        );
    }

    #[test]
    fn parse_pretty_document_among_logs_keeps_text_session_and_raw_usage() {
        let stdout = "\
banner\n\
{\n\
  \"text\": \"line1\\nline2\",\n\
  \"stopReason\": \"end_turn\",\n\
  \"sessionId\": \"session-1\",\n\
  \"thought\": \"hidden\",\n\
  \"usage\": {\n\
    \"input_tokens\": 10,\n\
    \"output_tokens\": 4,\n\
    \"reasoning_tokens\": 3,\n\
    \"total_tokens\": 20,\n\
    \"cache_read_input_tokens\": 7,\n\
    \"cache_creation_input_tokens\": 2\n\
  },\n\
  \"total_cost_usd\": 0.01,\n\
  \"modelUsage\": {\"grok-4.7-build\": {\"input_tokens\": 10}}\n\
}\n\
after\n";
        let stderr = "info ParseFile { path: \"x\" }\n";
        let parsed = parse_grok_output(stdout, stderr);
        assert_eq!(parsed.text, "line1\nline2");
        assert_eq!(parsed.session_id.as_deref(), Some("session-1"));
        assert!(parsed.error_text.is_empty(), "{}", parsed.error_text);
        let usage = parsed.usage.unwrap();
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.output_tokens, 4);
        assert_eq!(usage.reasoning_tokens, Some(3));
        assert_eq!(usage.cached_input_tokens, Some(7));
    }

    #[test]
    fn parse_uses_a_stderr_document_when_stdout_has_none() {
        let stdout = "banner only\n";
        let stderr = "{\"text\":\"from stderr\",\"sessionId\":\"s\"}\n";
        let parsed = parse_grok_output(stdout, stderr);
        assert_eq!(parsed.text, "from stderr");
        assert_eq!(parsed.session_id.as_deref(), Some("s"));
        assert!(parsed.error_text.is_empty());
    }

    #[test]
    fn parse_skips_a_broken_brace_line_and_keeps_the_later_object() {
        let stdout = "{not json\n{\"text\":\"kept\",\"stopReason\":\"end_turn\"}\n";
        let parsed = parse_grok_output(stdout, "");
        assert_eq!(parsed.text, "kept");
        assert!(parsed.error_text.is_empty());
    }

    #[test]
    fn parse_failure_paths_match_the_document_contract() {
        let cancelled = parse_grok_output(
            r#"{"text":"partial","stopReason":"cancelled","sessionId":"s"}"#,
            "",
        );
        assert_eq!(cancelled.error_text, GROK_RUN_CANCELLED);
        assert_eq!(cancelled.text, "partial");

        let errored = parse_grok_output(r#"{"text":"nope","error":"boom"}"#, "");
        assert_eq!(errored.error_text, "boom");

        let object_error = parse_grok_output(r#"{"text":"nope","error":{"code":7}}"#, "");
        assert!(
            object_error.error_text.contains("code"),
            "{}",
            object_error.error_text
        );

        let empty = parse_grok_output(r#"{"text":"","stopReason":"end_turn"}"#, "");
        assert!(empty.text.is_empty());
        assert!(empty.error_text.is_empty());

        let missing = parse_grok_output("error: auth failed\n", "log { brace }\n");
        assert_eq!(missing.error_text, NO_RESULT_DOCUMENT);
        assert!(missing.text.contains("error: auth failed"));
        assert!(missing.usage.is_none());
    }

    #[test]
    fn parse_auth_refresh_failure_replaces_the_error_only_when_both_markers_match() {
        let lines = "\
{\"text\":\"\"}\n\
\u{1b}[31mfailed auth lock: Read-only file system (os error 30)\u{1b}[0m\n\
\u{1b}[0mNo auth credentials\u{1b}[0m\n"
            .to_string();
        let parsed = parse_grok_output(&lines, "");
        assert_eq!(parsed.error_text, GROK_READONLY_AUTH_REFRESH_FAILURE);

        let one_marker = parse_grok_output("{\"text\":\"\"}\nNo auth credentials\n", "");
        assert!(
            one_marker.error_text.is_empty(),
            "{}",
            one_marker.error_text
        );

        let with_answer = parse_grok_output(
            "{\"text\":\"kept\"}\nauth lock Read-only file system\nNo auth credentials\n",
            "",
        );
        assert_eq!(with_answer.text, "kept");
        assert!(
            with_answer.error_text.is_empty(),
            "{}",
            with_answer.error_text
        );
    }

    #[test]
    fn usage_ignores_an_all_zero_object_and_does_not_fold_cache_into_input() {
        let zero = parse_grok_output(
            r#"{"text":"ok","usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0,"cache_read_input_tokens":4}}"#,
            "",
        );
        assert!(zero.usage.is_none());

        let only_total = parse_grok_output(
            r#"{"text":"ok","usage":{"total_tokens":9,"cache_creation_input_tokens":5}}"#,
            "",
        );
        let usage = only_total.usage.unwrap();
        assert_eq!(usage.input_tokens, 0);
        assert_eq!(usage.output_tokens, 0);
        assert_eq!(usage.cached_input_tokens, None);

        let numeric_strings = parse_grok_output(
            r#"{"text":"ok","usage":{"input_tokens":" 12 ","output_tokens":true,"reasoning_tokens":"nope"}}"#,
            "",
        );
        let usage = numeric_strings.usage.unwrap();
        assert_eq!(usage.input_tokens, 12);
        assert_eq!(usage.output_tokens, 1);
        assert_eq!(usage.reasoning_tokens, Some(0));
    }

    #[test]
    fn empty_model_resume_and_output_format_are_omitted() {
        let mut sample = job("");
        sample.model = ModelId::new("");
        sample.resume = Some(String::new());
        sample.workdir = Some(PathBuf::new());
        sample
            .params
            .extra
            .insert("output_format".into(), Value::String(String::new()));
        let launch = build_launch(&sample, PromptHost::Posix).unwrap();
        assert_eq!(launch.args, vec!["--prompt-file", "/dev/stdin"]);
        assert!(launch.cwd.is_none());
        assert!(!launch.args.iter().any(|arg| arg == "--output-format"));
        assert!(!launch.args.iter().any(|arg| arg == "--resume"));
        assert!(!launch.args.iter().any(|arg| arg == "-m"));
    }
}
