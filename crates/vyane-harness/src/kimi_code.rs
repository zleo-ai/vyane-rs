//! [`Harness`] for one **Kimi Code CLI** `stream-json` round.
//!
//! Command shape:
//!
//! ```text
//! kimi -p <prompt> --output-format stream-json
//!      [-m MODEL] [-S SESSION]
//!      [caller passthrough flags]
//! ```
//!
//! The installed binary is `kimi`. The harness kind token is `kimi-code`.
//!
//! * The prompt is the value of `-p`. This CLI has no stdin prompt channel and
//!   no `--prompt-file` flag. A `-` prompt is literal text, so this adapter
//!   does not open a prompt pipe. Stdin stays closed unless the shared spawn
//!   path is using it as a lifecycle start gate.
//! * `-m` is the job model id when that id is non-empty. A string `model` in
//!   [`vyane_core::GenParams::extra`] is used only when the job model id is
//!   empty. `-S` is `job.resume` when that id is non-empty. Resume does not
//!   use `-r`.
//! * [`vyane_core::Sandbox`] adds no flag. Non-interactive `-p` is already
//!   auto-approved and rejects `-y` / `--auto`. The workspace is the child
//!   working directory, not an argument.
//! * `output_format` in `params.extra` replaces the default `stream-json`.
//!   A null, false, or empty string omits `--output-format`. `provider` is
//!   dropped. Remaining entries follow the passthrough rules: snake case
//!   becomes a kebab flag, true emits the flag alone, false and null are
//!   skipped, a string or list item that starts with `-` is dropped, and an
//!   object value is refused before spawn. Identity keys (`owner_user_id`,
//!   `bound_owner`, `run_id`, and their kebab aliases) are never flags.
//! * An argument of 128 KiB or more is rejected before spawn. That is Linux
//!   `MAX_ARG_STRLEN`. The prompt has nowhere else to go.
//! * `KIMI_API_KEY` and `KIMI_MODEL_API_KEY` are removed after the child
//!   environment is built, including a different ASCII case of the same name.
//!   Removal wins over inherit, allow, and inject. No endpoint base URL and
//!   no endpoint credential is copied into the environment or the argument
//!   list. An endpoint present on the job does not change the command: the
//!   CLI keeps its own login.
//! * Stdout is NDJSON. Non-empty string `content` on `role=assistant` is the
//!   answer, joined with newlines. `tool_calls` and `role=tool` are skipped.
//!   `session_id` is taken from the last `role=meta` event whose `type` is
//!   `session.resume_hint`, when that id is a non-empty string. `role=error`,
//!   and a truthy top-level `error` on any other object, become the error
//!   text. A stream with no JSON object falls back to the raw lines. Token
//!   usage is not read.
//! * Exit 0 with answer text and no error text is success. Exit 0 with error
//!   text, or with no answer text, is [`ErrorKind::HarnessFailed`]. A
//!   non-zero exit is `HarnessFailed` as well.
//!
//! Streaming, a pinned workdir, and live spawn authority fail closed before a
//! process starts. This type is not selected by the service assembler.
//! Profiles, auto, failover, and broadcast keep the Claude Code and Codex CLI
//! support matrix.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;
use vyane_core::error::{ErrorKind, Result, VyaneError};
use vyane_core::target::{HarnessKind, Sandbox};
use vyane_core::traits::{Harness, HarnessJob, HarnessOutcome};

use crate::spawn::{
    RunControl, RunResult, Termination, env_key_list, materialize_env, parent_env_snapshot,
    run_capture_with_pinned,
};

/// Metered key. Removed from the child and never injected.
const ENV_API_KEY: &str = "KIMI_API_KEY";

/// Metered model key. Removed from the child and never injected.
const ENV_MODEL_API_KEY: &str = "KIMI_MODEL_API_KEY";

/// Names removed after environment materialization. Comparison is
/// case-insensitive because Windows environment names are.
const METERED_ENV_REMOVALS: &[&str] = &[ENV_API_KEY, ENV_MODEL_API_KEY];

/// Documented Kimi Code binary name, resolved through `PATH` at spawn time.
const BIN: &str = "kimi";

/// Stable [`HarnessKind::Other`] token for this adapter.
const KIND: &str = "kimi-code";

/// Linux per-argument ceiling (`MAX_ARG_STRLEN`, 32 pages). A token of this
/// size is already too big: `execve` rejects it with `E2BIG`.
const MAX_ARG_STRLEN: usize = 32 * 4096;

const IDENTITY_SKIP: &[&str] = &[
    "owner_user_id",
    "bound_owner",
    "run_id",
    "owner-user-id",
    "bound-owner",
    "run-id",
];

const ERROR_TAIL_MAX_CHARS: usize = 2000;
const ERROR_TAIL_MAX_LINES: usize = 50;

/// `Harness` adapter for one Kimi Code `stream-json` round.
#[derive(Debug, Clone)]
pub struct KimiCodeHarness {
    bin: String,
    parent_env: Option<Vec<(String, String)>>,
}

impl KimiCodeHarness {
    pub fn new() -> Self {
        Self {
            bin: BIN.into(),
            parent_env: None,
        }
    }

    /// Construct a harness with an explicit binary path or name.
    ///
    /// Tests use this with a fake executable. Production callers normally use
    /// [`Self::new`] and resolve `kimi` through `PATH`.
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
        strip_metered_keys(&mut env);
        env
    }

    fn classify_result(&self, result: RunResult) -> Result<HarnessOutcome> {
        match result.termination {
            Termination::Cancelled => Err(VyaneError::cancelled()),
            Termination::TimedOut => Err(VyaneError::new(
                ErrorKind::Timeout,
                "kimi-code harness timed out",
            )),
            Termination::Exited(code) => {
                classify_exit(code, &result.stdout, &result.stderr, result.duration)
            }
        }
    }
}

impl Default for KimiCodeHarness {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct Launch {
    args: Vec<String>,
    cwd: Option<PathBuf>,
}

fn build_launch(job: &HarnessJob, program: &str) -> Result<Launch> {
    let args = build_argv(job)?;
    assert_argv_within_arg_max(program, &args)?;
    Ok(Launch {
        args,
        cwd: job
            .workdir
            .as_ref()
            .filter(|dir| !dir.as_os_str().is_empty())
            .cloned(),
    })
}

fn build_argv(job: &HarnessJob) -> Result<Vec<String>> {
    // Every sandbox tier is accepted and none of them becomes a flag.
    match job.sandbox {
        Sandbox::ReadOnly | Sandbox::Write | Sandbox::Full => {}
    }

    let mut extra = sanitize_extra(&job.params.extra);
    extra.remove("provider");
    let output_format = take_output_format(&mut extra)?;
    let model = take_model(&mut extra, job.model.as_str())?;
    let passthrough = extra_flags(&extra)?;

    let mut args = vec!["-p".to_string(), job.prompt.clone()];
    if let Some(format) = output_format {
        args.push("--output-format".into());
        args.push(format);
    }
    if let Some(model) = model {
        args.push("-m".into());
        args.push(model);
    }
    if let Some(session) = job.resume.as_ref().filter(|id| !id.is_empty()) {
        args.push("-S".into());
        args.push(session.clone());
    }
    args.extend(passthrough);
    Ok(args)
}

fn take_output_format(extra: &mut Map<String, Value>) -> Result<Option<String>> {
    match extra.remove("output_format") {
        None => Ok(Some("stream-json".to_string())),
        Some(Value::Null | Value::Bool(false)) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(VyaneError::unsupported(
            "kimi-code refuses to spawn because output_format must be a string",
        )),
    }
}

fn take_model(extra: &mut Map<String, Value>, job_model: &str) -> Result<Option<String>> {
    let from_extra = extra.remove("model");
    if !job_model.is_empty() {
        return Ok(Some(job_model.to_string()));
    }
    match from_extra {
        None | Some(Value::Null | Value::Bool(false)) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(VyaneError::unsupported(
            "kimi-code refuses to spawn because model must be a string",
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
                    .filter(|item| !matches!(item, Value::String(text) if text.starts_with('-')))
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
        Value::Number(number) => Ok(number.to_string()),
        Value::String(text) => Ok(text.clone()),
        Value::Null | Value::Array(_) | Value::Object(_) => Err(VyaneError::unsupported(format!(
            "kimi-code refuses to spawn because extra argument `{key}` is not a scalar flag value"
        ))),
    }
}

fn assert_argv_within_arg_max(program: &str, args: &[String]) -> Result<()> {
    check_arg_len(0, program, None)?;
    for (offset, token) in args.iter().enumerate() {
        let previous = if offset == 0 {
            program
        } else {
            args[offset - 1].as_str()
        };
        check_arg_len(offset + 1, token, Some(previous))?;
    }
    Ok(())
}

fn check_arg_len(index: usize, token: &str, previous: Option<&str>) -> Result<()> {
    let size = token.len();
    if size < MAX_ARG_STRLEN {
        return Ok(());
    }
    let flag = match previous {
        Some(text) => format!("{text:?}"),
        None => "\"(the binary itself)\"".to_string(),
    };
    Err(VyaneError::new(
        ErrorKind::Config,
        format!(
            "argv token #{index} is {size} bytes — over Linux's per-argument \
             limit of {MAX_ARG_STRLEN} bytes (MAX_ARG_STRLEN). It follows {flag}. \
             execve would reject this with E2BIG before the CLI started"
        ),
    ))
}

fn strip_metered_keys(env: &mut BTreeMap<String, String>) {
    env.retain(|key, _| {
        !METERED_ENV_REMOVALS
            .iter()
            .any(|blocked| key.eq_ignore_ascii_case(blocked))
    });
}

fn classify_exit(
    code: i32,
    stdout: &str,
    stderr: &str,
    duration: Duration,
) -> Result<HarnessOutcome> {
    let parsed = parse_kimi_stdout(stdout);
    if code != 0 {
        return Err(VyaneError::new(
            ErrorKind::HarnessFailed,
            nonzero_message(code, &parsed.error_text, stdout, stderr),
        ));
    }
    if !parsed.error_text.is_empty() {
        return Err(VyaneError::new(
            ErrorKind::HarnessFailed,
            format!("kimi-code returned an error: {}", parsed.error_text),
        ));
    }
    if parsed.text.is_empty() {
        return Err(VyaneError::new(
            ErrorKind::HarnessFailed,
            empty_answer_message(stdout),
        ));
    }
    Ok(HarnessOutcome {
        text: parsed.text,
        native_session_id: parsed.session_id,
        usage: None,
        exit_code: code,
        duration,
    })
}

fn nonzero_message(code: i32, error_text: &str, stdout: &str, stderr: &str) -> String {
    let raw_tail = output_tail(stdout, stderr);
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
    format!("kimi-code exited with code {code}: {details}")
}

fn empty_answer_message(stdout: &str) -> String {
    let tail = tail_text(stdout);
    if tail.is_empty() {
        "kimi-code output did not include answer text".to_string()
    } else {
        format!("kimi-code output did not include answer text; output tail: {tail}")
    }
}

fn output_tail(stdout: &str, stderr: &str) -> String {
    let stdout_tail = tail_text(stdout);
    if !stdout_tail.is_empty() {
        return stdout_tail;
    }
    tail_text(stderr)
}

fn tail_text(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(ERROR_TAIL_MAX_LINES);
    let joined = lines[start..].join("\n");
    let trimmed = joined.trim();
    if trimmed.chars().count() <= ERROR_TAIL_MAX_CHARS {
        return trimmed.to_string();
    }
    let tail: String = trimmed
        .chars()
        .rev()
        .take(ERROR_TAIL_MAX_CHARS)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("...{tail}")
}

struct Parsed {
    text: String,
    session_id: Option<String>,
    error_text: String,
}

fn parse_kimi_stdout(stdout: &str) -> Parsed {
    let lines: Vec<&str> = stdout.lines().collect();
    let mut session_id = None;
    let mut text_parts = Vec::new();
    let mut error_parts = Vec::new();
    let mut saw_json = false;

    for line in &lines {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(data) = value.as_object() else {
            continue;
        };
        saw_json = true;
        let role = data.get("role").and_then(Value::as_str);
        match role {
            Some("assistant") => {
                if let Some(content) = data.get("content").and_then(Value::as_str)
                    && !content.is_empty()
                {
                    text_parts.push(content.to_string());
                }
            }
            Some("meta") => {
                if data.get("type").and_then(Value::as_str) == Some("session.resume_hint")
                    && let Some(sid) = data.get("session_id").and_then(Value::as_str)
                    && !sid.is_empty()
                {
                    session_id = Some(sid.to_string());
                }
            }
            Some("error") => {
                if let Some(message) = first_error_message(data) {
                    error_parts.push(message);
                }
            }
            _ => {}
        }
        if role != Some("error")
            && let Some(err) = data.get("error").filter(|value| json_truthy(value))
        {
            error_parts.push(display_json(err));
        }
    }

    if !saw_json {
        return Parsed {
            text: lines.join("\n"),
            session_id: None,
            error_text: String::new(),
        };
    }
    Parsed {
        text: text_parts.join("\n"),
        session_id,
        error_text: error_parts.join("\n"),
    }
}

fn first_error_message(data: &Map<String, Value>) -> Option<String> {
    for key in ["content", "error", "message"] {
        if let Some(value) = data.get(key).filter(|value| json_truthy(value)) {
            return Some(display_json(value));
        }
    }
    None
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
    if let Some(value) = number.as_i64() {
        return value == 0;
    }
    if let Some(value) = number.as_u64() {
        return value == 0;
    }
    number.as_f64().is_some_and(|value| value == 0.0)
}

fn display_json(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Bool(true) => "True".to_string(),
        Value::Bool(false) => "False".to_string(),
        Value::Number(number) => number.to_string(),
        Value::Null => "None".to_string(),
        other => other.to_string(),
    }
}

#[async_trait]
impl Harness for KimiCodeHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::Other(KIND.into())
    }

    async fn available(&self) -> bool {
        crate::probe::binary_available(&self.bin).await
    }

    async fn run(&self, job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome> {
        let launch = build_launch(&job, &self.bin)?;
        let env = self.build_env(&job);

        tracing::debug!(
            harness = KIND,
            argc = launch.args.len(),
            env_keys = %env_key_list(&env),
            "spawning kimi-code harness"
        );

        let result = run_capture_with_pinned(
            &self.bin,
            &launch.args,
            launch.cwd.as_deref(),
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
    use std::time::Duration;

    use super::*;
    use serde_json::json;
    use vyane_core::env::{EnvPolicy, InheritMode};
    use vyane_core::target::{AuthMaterial, AuthStyle, Endpoint, ModelId, Protocol, Secret};
    use vyane_core::task::{Effort, GenParams};

    const SAMPLE_PLAIN: &str = "\
{\"role\":\"assistant\",\"content\":\"采样OK\"}\n\
{\"role\":\"meta\",\"type\":\"session.resume_hint\",\"session_id\":\"session_4945d535-4b05-473b-976c-0719cad3b099\",\"command\":\"kimi -r session_4945d535-4b05-473b-976c-0719cad3b099\",\"content\":\"To resume this session: kimi -r session_4945d535-4b05-473b-976c-0719cad3b099\"}\n";

    const SAMPLE_TOOL: &str = "\
{\"role\":\"assistant\",\"tool_calls\":[{\"type\":\"function\",\"id\":\"tool_D11CkNPn1nJm2WTok3Qocokv\",\"function\":{\"name\":\"Read\",\"arguments\":\"{\\\"path\\\":\\\"sample.txt\\\"}\"}}]}\n\
{\"role\":\"tool\",\"tool_call_id\":\"tool_D11CkNPn1nJm2WTok3Qocokv\",\"content\":\"1\\thello world\"}\n\
{\"role\":\"assistant\",\"content\":\"hello world\"}\n\
{\"role\":\"meta\",\"type\":\"session.resume_hint\",\"session_id\":\"session_abc\",\"command\":\"kimi -r session_abc\",\"content\":\"To resume: kimi -r session_abc\"}\n";

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

    fn args_of(sample: &HarnessJob) -> Vec<String> {
        build_launch(sample, "kimi").unwrap().args
    }

    fn parent_env() -> Vec<(String, String)> {
        vec![
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("HOME".into(), "/tmp/kimi-home".into()),
            ("KIMI_API_KEY".into(), "parent-metered-key".into()),
            ("KIMI_MODEL_API_KEY".into(), "parent-model-key".into()),
            ("kimi_api_key".into(), "parent-metered-lower".into()),
            ("Kimi_Model_Api_Key".into(), "parent-model-mixed".into()),
            ("OTHER_PARENT".into(), "kept-parent".into()),
        ]
    }

    #[test]
    fn kind_token_is_kimi_code_and_binary_is_kimi() {
        let harness = KimiCodeHarness::new();
        assert_eq!(harness.kind(), HarnessKind::Other("kimi-code".into()));
        assert_eq!(harness.kind().as_str(), "kimi-code");
        assert!(format!("{harness:?}").contains("bin: \"kimi\""));
        assert_eq!(
            format!("{:?}", KimiCodeHarness::default()),
            format!("{harness:?}")
        );
    }

    #[test]
    fn basic_argv_is_prompt_then_stream_json() {
        let args = args_of(&job("只回复:壳通"));
        assert_eq!(
            args,
            vec!["-p", "只回复:壳通", "--output-format", "stream-json",]
        );
    }

    #[test]
    fn prompt_is_the_argv_value_of_p_even_when_it_looks_like_a_flag() {
        let args = args_of(&job("--help"));
        assert_eq!(args[0], "-p");
        assert_eq!(args[1], "--help");
        assert!(!args.iter().any(|arg| arg == "--prompt-file"));
        assert!(!args.iter().any(|arg| arg == "-"));

        let multiline = args_of(&job("line1\nline2"));
        assert_eq!(multiline[1], "line1\nline2");
    }

    #[test]
    fn model_and_session_use_m_and_capital_s() {
        let mut sample = job("hi");
        sample.model = ModelId::new("kimi-k2.6");
        sample.resume = Some("session_abc".into());
        let args = args_of(&sample);
        assert_eq!(
            args,
            vec![
                "-p",
                "hi",
                "--output-format",
                "stream-json",
                "-m",
                "kimi-k2.6",
                "-S",
                "session_abc",
            ]
        );
        assert!(!args.iter().any(|arg| arg == "-r"));
        assert!(!args.iter().any(|arg| arg == "--resume"));
    }

    #[test]
    fn empty_model_and_empty_session_omit_those_flags() {
        let mut sample = job("hi");
        sample.model = ModelId::new("");
        sample.resume = Some(String::new());
        let args = args_of(&sample);
        assert!(!args.iter().any(|arg| arg == "-m" || arg == "-S"));
    }

    #[test]
    fn extra_model_is_used_only_when_the_job_model_is_empty() {
        let mut from_extra = job("hi");
        from_extra
            .params
            .extra
            .insert("model".into(), json!("kimi-k2.6"));
        let extra_args = args_of(&from_extra);
        assert!(
            extra_args
                .windows(2)
                .any(|window| window == ["-m", "kimi-k2.6"])
        );

        let mut from_job = job("hi");
        from_job.model = ModelId::new("from-job");
        from_job
            .params
            .extra
            .insert("model".into(), json!("from-extra"));
        let job_args = args_of(&from_job);
        assert!(
            job_args
                .windows(2)
                .any(|window| window == ["-m", "from-job"])
        );
        assert!(!job_args.iter().any(|arg| arg == "from-extra"));
        assert!(!job_args.iter().any(|arg| arg == "--model"));
    }

    #[test]
    fn sandbox_effort_and_workdir_add_no_flags() {
        for sandbox in [Sandbox::ReadOnly, Sandbox::Write, Sandbox::Full] {
            let mut sample = job("hi");
            sample.sandbox = sandbox;
            sample.params.effort = Some(Effort::High);
            sample.params.temperature = Some(0.2);
            sample.workdir = Some(PathBuf::from("/some/work/dir"));
            let args = args_of(&sample);
            assert_eq!(args, vec!["-p", "hi", "--output-format", "stream-json"]);
            for forbidden in [
                "--sandbox",
                "-y",
                "--auto",
                "--yolo",
                "--permission-mode",
                "--add-dir",
                "/some/work/dir",
                "--reasoning-effort",
                "high",
            ] {
                assert!(!args.iter().any(|arg| arg == forbidden), "{forbidden}");
            }
            assert_eq!(
                build_launch(&sample, "kimi").unwrap().cwd,
                Some(PathBuf::from("/some/work/dir"))
            );
        }
    }

    #[test]
    fn extra_args_pass_through_and_skip_identity_provider_and_flag_like_values() {
        let mut sample = job("hi");
        sample.params.extra = json!({
            "add_dir": "/extra",
            "provider": "moonshot",
            "owner_user_id": "alice",
            "bound_owner": "alice",
            "run_id": "disp-1",
            "owner-user-id": "alice",
            "bound-owner": "alice",
            "run-id": "disp-1",
            "verbose": true,
            "yolo": false,
            "skipped": null,
            "max_turns": 0,
            "inject": "--evil",
        })
        .as_object()
        .unwrap()
        .clone();
        let args = args_of(&sample);
        assert!(
            args.windows(2)
                .any(|window| window == ["--add-dir", "/extra"])
        );
        assert!(args.windows(2).any(|window| window == ["--max-turns", "0"]));
        assert!(args.iter().any(|arg| arg == "--verbose"));
        for forbidden in [
            "--provider",
            "moonshot",
            "--owner-user-id",
            "--bound-owner",
            "--run-id",
            "alice",
            "disp-1",
            "--yolo",
            "--skipped",
            "--inject",
            "--evil",
        ] {
            assert!(
                !args.iter().any(|arg| arg == forbidden),
                "{forbidden} in {args:?}"
            );
        }
    }

    #[test]
    fn list_extra_repeats_the_flag_and_drops_flag_like_items() {
        let mut sample = job("hi");
        sample.params.extra.insert(
            "add_dir".into(),
            json!(["/ok", "--evil", null, false, true]),
        );
        let args = args_of(&sample);
        assert_eq!(
            args.iter()
                .filter(|arg| arg.as_str() == "--add-dir")
                .count(),
            3
        );
        assert!(args.windows(2).any(|window| window == ["--add-dir", "/ok"]));
        assert!(
            args.windows(2)
                .any(|window| window == ["--add-dir", "False"])
        );
        assert!(
            args.windows(2)
                .any(|window| window == ["--add-dir", "True"])
        );
        assert!(!args.iter().any(|arg| arg == "--evil"));
    }

    #[test]
    fn output_format_override_replaces_the_default() {
        let mut sample = job("hi");
        sample
            .params
            .extra
            .insert("output_format".into(), json!("text"));
        let args = args_of(&sample);
        assert_eq!(
            args.iter()
                .filter(|arg| arg.as_str() == "--output-format")
                .count(),
            1
        );
        assert!(
            args.windows(2)
                .any(|window| window == ["--output-format", "text"])
        );

        sample
            .params
            .extra
            .insert("output_format".into(), json!(""));
        let omitted = args_of(&sample);
        assert!(!omitted.iter().any(|arg| arg == "--output-format"));
    }

    #[test]
    fn object_extra_and_bad_output_format_are_refused() {
        let mut sample = job("hi");
        sample.params.extra.insert("nested".into(), json!({"a": 1}));
        let err = build_launch(&sample, "kimi").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unsupported);
        assert!(err.message.contains("nested"), "{}", err.message);

        let mut format = job("hi");
        format.params.extra.insert("output_format".into(), json!(3));
        let err = build_launch(&format, "kimi").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unsupported);
        assert!(err.message.contains("output_format"), "{}", err.message);
    }

    #[test]
    fn oversized_prompt_is_rejected_before_spawn() {
        let prompt = "Q".repeat(MAX_ARG_STRLEN);
        let err = build_launch(&job(&prompt), "kimi").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Config);
        assert!(err.message.contains("MAX_ARG_STRLEN"), "{}", err.message);
        assert!(err.message.contains("\"-p\""), "{}", err.message);
        assert!(!err.message.contains("QQQ"), "{}", err.message);

        let fits = "Q".repeat(MAX_ARG_STRLEN - 1);
        assert!(build_launch(&job(&fits), "kimi").is_ok());
    }

    #[test]
    fn metered_keys_are_removed_and_endpoint_is_not_copied() {
        let harness = KimiCodeHarness::with_binary_and_parent_env_for_tests("kimi", parent_env());
        let mut sample = job("hi");
        sample.env = EnvPolicy {
            mode: InheritMode::Full,
            ..EnvPolicy::scrubbed()
        }
        .inject("KIMI_API_KEY", "injected-metered-key")
        .inject("KIMI_MODEL_API_KEY", "injected-model-key")
        .inject("kimi_model_api_key", "injected-model-lower")
        .inject("KIMI_MODEL_NAME", "kimi-k2")
        .inject("EXPLICIT_CHILD", "yes");
        sample.endpoint = Some(Endpoint {
            base_url: "https://kimi.example/v1".into(),
            auth: Some(AuthMaterial {
                style: AuthStyle::Bearer,
                secret: Secret::new("endpoint-canary-secret"),
            }),
        });
        let env = harness.build_env(&sample);
        assert_eq!(env.get("HOME").map(String::as_str), Some("/tmp/kimi-home"));
        assert_eq!(env.get("EXPLICIT_CHILD").map(String::as_str), Some("yes"));
        assert_eq!(
            env.get("OTHER_PARENT").map(String::as_str),
            Some("kept-parent")
        );
        assert_eq!(
            env.get("KIMI_MODEL_NAME").map(String::as_str),
            Some("kimi-k2")
        );
        for key in env.keys() {
            assert!(!key.eq_ignore_ascii_case("KIMI_API_KEY"), "{key}");
            assert!(!key.eq_ignore_ascii_case("KIMI_MODEL_API_KEY"), "{key}");
        }
        for forbidden in [
            "parent-metered-key",
            "parent-model-key",
            "parent-metered-lower",
            "parent-model-mixed",
            "injected-metered-key",
            "injected-model-key",
            "injected-model-lower",
            "endpoint-canary-secret",
            "https://kimi.example/v1",
        ] {
            assert!(
                !env.values().any(|value| value.contains(forbidden)),
                "{forbidden} reached the child"
            );
        }

        let mut allowed = job("hi");
        allowed.env.allow.extend([
            "KIMI_API_KEY".to_string(),
            "kimi_api_key".to_string(),
            "KIMI_MODEL_API_KEY".to_string(),
        ]);
        let scrubbed = harness.build_env(&allowed);
        assert!(scrubbed.keys().all(|key| {
            !key.eq_ignore_ascii_case("KIMI_API_KEY")
                && !key.eq_ignore_ascii_case("KIMI_MODEL_API_KEY")
        }));
        assert_eq!(
            scrubbed.get("HOME").map(String::as_str),
            Some("/tmp/kimi-home")
        );
        assert!(!scrubbed.contains_key("OTHER_PARENT"));
    }

    #[test]
    fn plain_sample_keeps_answer_and_session_and_ignores_usage() {
        let mut with_usage = SAMPLE_PLAIN.to_string();
        with_usage = with_usage.replacen(
            "\"content\":\"采样OK\"",
            "\"content\":\"采样OK\",\"usage\":{\"input_tokens\":9,\"output_tokens\":4}",
            1,
        );
        let outcome = classify_exit(0, &with_usage, "", Duration::ZERO).unwrap();
        assert_eq!(outcome.text, "采样OK");
        assert_eq!(
            outcome.native_session_id.as_deref(),
            Some("session_4945d535-4b05-473b-976c-0719cad3b099")
        );
        assert!(outcome.usage.is_none());
        assert_eq!(outcome.exit_code, 0);
    }

    #[test]
    fn tool_call_sample_keeps_only_the_final_answer() {
        let outcome = classify_exit(0, SAMPLE_TOOL, "", Duration::ZERO).unwrap();
        assert_eq!(outcome.text, "hello world");
        assert_eq!(outcome.native_session_id.as_deref(), Some("session_abc"));
        assert!(outcome.usage.is_none());
        assert!(!outcome.text.contains("sample.txt"));
        assert!(!outcome.text.contains("Read"));
        assert!(!outcome.text.contains("tool_call"));
        assert!(!outcome.text.contains("kimi -r"));
    }

    #[test]
    fn session_id_comes_only_from_the_last_resume_hint() {
        let parsed = parse_kimi_stdout("{\"role\":\"assistant\",\"content\":\"hi\"}\n");
        assert_eq!(parsed.text, "hi");
        assert!(parsed.session_id.is_none());

        let parsed = parse_kimi_stdout(
            "\
{\"role\":\"assistant\",\"content\":\"hi\",\"session_id\":\"nope\"}\n\
{\"role\":\"meta\",\"type\":\"session.resume_hint\",\"session_id\":\"first\"}\n\
{\"role\":\"meta\",\"type\":\"session.resume_hint\",\"session_id\":\"\"}\n\
{\"role\":\"meta\",\"type\":\"other\",\"session_id\":\"later\"}\n\
{\"role\":\"meta\",\"type\":\"session.resume_hint\",\"session_id\":\"second\"}\n",
        );
        assert_eq!(parsed.session_id.as_deref(), Some("second"));
        assert_eq!(parsed.text, "hi");
    }

    #[test]
    fn assistant_content_accumulates_and_skips_empty_or_non_string_content() {
        let parsed = parse_kimi_stdout(
            "\
{\"role\":\"assistant\",\"content\":\"part one\"}\n\
{\"role\":\"assistant\",\"content\":\"\"}\n\
{\"role\":\"assistant\",\"content\":[\"skipped\"]}\n\
{\"role\":\"assistant\",\"content\":\"part two\"}\n",
        );
        assert_eq!(parsed.text, "part one\npart two");
    }

    #[test]
    fn error_role_and_top_level_error_are_surfaced_once() {
        let parsed = parse_kimi_stdout(
            "{\"role\":\"error\",\"content\":\"not logged in\",\"error\":\"other\",\"message\":\"third\"}\n",
        );
        assert_eq!(parsed.error_text, "not logged in");
        assert!(parsed.text.is_empty());

        let parsed = parse_kimi_stdout("{\"role\":\"error\",\"error\":\"quota\"}\n");
        assert_eq!(parsed.error_text, "quota");

        let parsed = parse_kimi_stdout("{\"role\":\"error\",\"message\":\"third\"}\n");
        assert_eq!(parsed.error_text, "third");

        let parsed = parse_kimi_stdout(
            "{\"role\":\"meta\",\"type\":\"session.resume_hint\",\"session_id\":\"sid\",\"error\":\"quota exceeded\",\"content\":\"To resume\"}\n",
        );
        assert_eq!(parsed.error_text, "quota exceeded");
        assert_eq!(parsed.session_id.as_deref(), Some("sid"));
        assert!(parsed.text.is_empty());

        let parsed = parse_kimi_stdout(
            "{\"role\":\"tool\",\"content\":\"secret-tool\",\"error\":\"tool failed\"}\n",
        );
        assert!(parsed.text.is_empty());
        assert_eq!(parsed.error_text, "tool failed");

        let parsed = parse_kimi_stdout("{\"error\":\"boom\"}\n");
        assert_eq!(parsed.error_text, "boom");

        for line in [
            "{\"role\":\"meta\",\"error\":0}\n",
            "{\"role\":\"meta\",\"error\":false}\n",
            "{\"role\":\"meta\",\"error\":\"\"}\n",
        ] {
            let parsed = parse_kimi_stdout(line);
            assert!(parsed.error_text.is_empty(), "{line}");
        }
    }

    #[test]
    fn non_json_lines_are_ignored_only_when_some_object_parsed() {
        let parsed = parse_kimi_stdout(
            "warning: something on stderr\n{\"role\":\"assistant\",\"content\":\"OK\"}\n",
        );
        assert_eq!(parsed.text, "OK");
        assert!(!parsed.text.contains("warning"));
    }

    #[test]
    fn plain_text_and_non_object_json_fall_back_to_raw_lines() {
        let parsed = parse_kimi_stdout("just plain output\nsecond line\n");
        assert_eq!(parsed.text, "just plain output\nsecond line");
        assert!(parsed.session_id.is_none());
        assert!(parsed.error_text.is_empty());

        let parsed = parse_kimi_stdout("null\n");
        assert_eq!(parsed.text, "null");

        let parsed = parse_kimi_stdout("\"hello\"\n");
        assert_eq!(parsed.text, "\"hello\"");

        let parsed = parse_kimi_stdout("");
        assert_eq!(parsed.text, "");
        assert!(parsed.session_id.is_none());
        assert!(parsed.error_text.is_empty());
    }

    #[test]
    fn exit_status_follows_answer_text_and_error_text() {
        let ok = classify_exit(0, "plain answer\n", "", Duration::from_millis(3)).unwrap();
        assert_eq!(ok.text, "plain answer");
        assert!(ok.usage.is_none());
        assert_eq!(ok.duration, Duration::from_millis(3));

        let err = classify_exit(
            0,
            "{\"role\":\"error\",\"content\":\"not logged in\"}\n",
            "",
            Duration::ZERO,
        )
        .unwrap_err();
        assert_eq!(err.kind, ErrorKind::HarnessFailed);
        assert!(err.message.contains("not logged in"), "{}", err.message);
        assert!(!err.message.contains("other"));

        let empty = classify_exit(0, "", "", Duration::ZERO).unwrap_err();
        assert_eq!(empty.kind, ErrorKind::HarnessFailed);
        assert!(
            empty.message.contains("did not include answer text"),
            "{}",
            empty.message
        );

        let nonzero = classify_exit(
            9,
            "{\"role\":\"assistant\",\"content\":\"partial\"}\n",
            "",
            Duration::ZERO,
        )
        .unwrap_err();
        assert_eq!(nonzero.kind, ErrorKind::HarnessFailed);
        assert!(
            nonzero.message.contains("exited with code 9"),
            "{}",
            nonzero.message
        );
    }

    #[tokio::test]
    async fn missing_name_is_unavailable_without_spawning() {
        assert!(
            !KimiCodeHarness::with_binary("definitely-not-a-real-binary-kimi-42")
                .available()
                .await
        );
    }
}
