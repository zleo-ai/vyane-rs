//! [`Harness`] for one CodeBuddy CLI JSON shot.
//!
//! Command shape:
//!
//! ```text
//! codebuddy -p <prompt> --model <id> --output-format json --setting-sources user
//!           [--permission-mode acceptEdits | -y]
//!           [--resume ID]
//!           [--effort VALUE] [--allowedTools VALUE] [--disallowedTools VALUE]
//!           [--add-dir DIR ...] [--fallback-model ID]
//! ```
//!
//! The prompt is an argument. Stdin stays closed. There is no prompt file and
//! no capture wrapper.
//!
//! * `--model` is the job model when that id is non-empty, otherwise a
//!   non-empty string `params.extra["model"]`, otherwise `deepseek-v4.1-flash`.
//!   `task_type`, `chain_index`, and model-chain environment variables are
//!   ignored.
//! * `Write` adds `--permission-mode acceptEdits`. `Full` adds `-y`.
//!   `ReadOnly` adds no sandbox flag.
//! * `job.resume` adds `--resume` when the id is non-empty.
//! * Optional extras are only `effort`, `allowed_tools`, `disallowed_tools`,
//!   `add_dir`, and `fallback_model`. `fallback_model` is omitted unless the
//!   caller supplied a truthy value. Every other extra key is ignored,
//!   including `owner_user_id`, `bound_owner`, and `run_id`.
//! * An argument of 128 KiB or more is rejected before spawn. That is Linux
//!   `MAX_ARG_STRLEN`, measured in UTF-8 bytes on the argv that is actually
//!   spawned.
//! * No environment name is removed after [`vyane_core::EnvPolicy`] is
//!   applied. An endpoint on the job is not injected and does not refuse the
//!   spawn.
//! * On Windows, a bare `codebuddy` is resolved on `PATH` and an npm
//!   `codebuddy.cmd` forwarding shim is unwrapped to its `node.exe` plus
//!   script. Other batch files are left unchanged. Node lookup environment
//!   variables are not read.
//! * Stdout lines, then stderr lines, are one document. SGR color is stripped.
//!   Only the first JSON value is decoded (an event array or one object). A
//!   second JSON value, or other trailing junk on the same line, fails. The
//!   last `type=result` event is the result. `session_id` or `sessionId` is
//!   the session id. `is_error: true`, or a `subtype` other than `success`,
//!   is an error. Truncated JSON and a missing result fail closed. Token
//!   usage is only `result.usage` `input_tokens` and `output_tokens`.
//!
//! The harness kind is [`HarnessKind::Other`] with token `codebuddy`.
//! Streaming, a pinned workdir, and live spawn authority fail closed before a
//! process starts. This type is not selected by the service assembler.
//! Profiles, auto, failover, and broadcast keep the Claude Code and Codex CLI
//! support matrix.

use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::Duration;

use async_trait::async_trait;
use regex::Regex;
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;
use vyane_core::error::{ErrorKind, Result, VyaneError};
use vyane_core::run::Usage;
use vyane_core::target::{HarnessKind, Sandbox};
use vyane_core::traits::{Harness, HarnessJob, HarnessOutcome};

use crate::spawn::{
    RunControl, RunResult, Termination, env_key_list, materialize_env, parent_env_snapshot,
    run_capture_with_pinned,
};

/// Documented CodeBuddy binary name, resolved through `PATH` at spawn time.
const BIN: &str = "codebuddy";

/// Stable [`HarnessKind::Other`] token for this adapter.
const KIND: &str = "codebuddy";

/// Sole id in the pinned default model preference.
const DEFAULT_MODEL: &str = "deepseek-v4.1-flash";

/// Linux per-argument ceiling (`MAX_ARG_STRLEN`, 32 pages).
const MAX_ARG_STRLEN: usize = 32 * 4096;

/// npm cmd shims are a few hundred bytes. Anything larger is not unwrapped.
const SHIM_MAX_BYTES: u64 = 64 * 1024;

const TRUNCATED_OUTPUT: &str = "\
CodeBuddy 输出被截断（output truncated）：JSON 不完整；\
积分用量未知，本次未记账（credit_observed=false），\
不代表未消耗积分。";

const NO_RESULT: &str = "codebuddy output has no JSON result";

const HARMLESS_SET_VARS: &[&str] = &["dp0", "_prog", "pathext"];

const INTERPRETER_PROGRAMS: &[&str] = &[
    "cmd",
    "command",
    "powershell",
    "powershell_ise",
    "pwsh",
    "wscript",
    "cscript",
    "mshta",
    "rundll32",
    "start",
    "forfiles",
    "msiexec",
    "regsvr32",
    "conhost",
    "explorer",
    "wt",
    "bash",
    "sh",
    "wsl",
];

const CMD_BUILTINS: &[&str] = &[
    "assoc", "break", "call", "cd", "chdir", "cls", "color", "copy", "date", "del", "dir", "dpath",
    "echo", "endlocal", "erase", "exit", "for", "ftype", "goto", "if", "keys", "md", "mkdir",
    "mklink", "move", "path", "pause", "popd", "prompt", "pushd", "rd", "rem", "ren", "rename",
    "rmdir", "set", "setlocal", "shift", "start", "time", "title", "type", "ver", "verify", "vol",
];

const LAUNCHABLE_EXTS: &[&str] = &[".com", ".exe", ".bat", ".cmd"];

/// `Harness` adapter for one CodeBuddy JSON shot.
#[derive(Debug, Clone)]
pub struct CodebuddyHarness {
    bin: String,
    parent_env: Option<Vec<(String, String)>>,
}

impl CodebuddyHarness {
    pub fn new() -> Self {
        Self {
            bin: BIN.into(),
            parent_env: None,
        }
    }

    /// Construct a harness with an explicit binary path or name.
    ///
    /// Tests use this with a fake executable. Production callers normally use
    /// [`Self::new`] and resolve `codebuddy` through `PATH`.
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

    fn classify_result(&self, result: RunResult) -> Result<HarnessOutcome> {
        match result.termination {
            Termination::Cancelled => Err(VyaneError::cancelled()),
            Termination::TimedOut => Err(VyaneError::new(
                ErrorKind::Timeout,
                "codebuddy harness timed out",
            )),
            Termination::Exited(code) => {
                classify_exit(code, &result.stdout, &result.stderr, result.duration)
            }
        }
    }
}

impl Default for CodebuddyHarness {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct Launch {
    program: String,
    args: Vec<String>,
    cwd: Option<PathBuf>,
}

fn prepare_launch(job: &HarnessJob, bin: &str, windows: bool, path_env: &str) -> Result<Launch> {
    let args = build_args(job)?;
    let (program, args) = launch_argv(bin, &args, windows, path_env);
    let mut argv = Vec::with_capacity(args.len() + 1);
    argv.push(program.clone());
    argv.extend(args.iter().cloned());
    assert_argv_within_arg_max(&argv)?;
    Ok(Launch {
        program,
        args,
        cwd: job
            .workdir
            .as_ref()
            .filter(|dir| !dir.as_os_str().is_empty())
            .cloned(),
    })
}

fn build_args(job: &HarnessJob) -> Result<Vec<String>> {
    let model = requested_model(job);
    let mut args = vec![
        "-p".to_string(),
        job.prompt.clone(),
        "--model".to_string(),
        model,
        "--output-format".to_string(),
        "json".to_string(),
        "--setting-sources".to_string(),
        "user".to_string(),
    ];
    match job.sandbox {
        Sandbox::Write => {
            args.push("--permission-mode".to_string());
            args.push("acceptEdits".to_string());
        }
        Sandbox::Full => args.push("-y".to_string()),
        Sandbox::ReadOnly => {}
    }
    if let Some(session) = job.resume.as_ref().filter(|id| !id.is_empty()) {
        args.push("--resume".to_string());
        args.push(session.clone());
    }
    push_truthy_flag(&mut args, "--effort", job.params.extra.get("effort"));
    push_truthy_flag(
        &mut args,
        "--allowedTools",
        job.params.extra.get("allowed_tools"),
    );
    push_truthy_flag(
        &mut args,
        "--disallowedTools",
        job.params.extra.get("disallowed_tools"),
    );
    if let Some(Value::Array(dirs)) = job.params.extra.get("add_dir") {
        for directory in dirs {
            args.push("--add-dir".to_string());
            args.push(python_str(directory));
        }
    }
    push_truthy_flag(
        &mut args,
        "--fallback-model",
        job.params.extra.get("fallback_model"),
    );
    Ok(args)
}

fn requested_model(job: &HarnessJob) -> String {
    if !job.model.as_str().is_empty() {
        return job.model.as_str().to_string();
    }
    if let Some(Value::String(model)) = job.params.extra.get("model")
        && !model.is_empty()
    {
        return model.clone();
    }
    DEFAULT_MODEL.to_string()
}

fn push_truthy_flag(args: &mut Vec<String>, flag: &str, value: Option<&Value>) {
    let Some(value) = value else {
        return;
    };
    if json_truthy(value) {
        args.push(flag.to_string());
        args.push(python_str(value));
    }
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

fn python_str(value: &Value) -> String {
    match value {
        Value::Null => "None".to_string(),
        Value::Bool(true) => "True".to_string(),
        Value::Bool(false) => "False".to_string(),
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        other => python_dumps(other),
    }
}

fn python_dumps(value: &Value) -> String {
    let mut out = String::new();
    write_python_json(&mut out, value);
    out
}

fn write_python_json(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(number) => out.push_str(&number.to_string()),
        Value::String(text) => out.push_str(&quoted_json(text)),
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                write_python_json(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (index, (key, item)) in map.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                out.push_str(&quoted_json(key));
                out.push_str(": ");
                write_python_json(out, item);
            }
            out.push('}');
        }
    }
}

fn quoted_json(text: &str) -> String {
    serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string())
}

fn assert_argv_within_arg_max(argv: &[String]) -> Result<()> {
    for (index, token) in argv.iter().enumerate() {
        let size = token.len();
        if size < MAX_ARG_STRLEN {
            continue;
        }
        let flag = if index == 0 {
            "(the binary itself)".to_string()
        } else {
            argv[index - 1].clone()
        };
        return Err(VyaneError::new(
            ErrorKind::Config,
            format!(
                "argv token #{index} is {size} bytes — over Linux's per-argument \
                 limit of {MAX_ARG_STRLEN} bytes (MAX_ARG_STRLEN). It follows {}. \
                 execve would reject this with E2BIG / [Errno 7] Argument list too long \
                 before the CLI even started. Variable-length content (a prompt, a diff, \
                 a report, a system prompt) must travel over stdin or a file, never argv.",
                python_repr(&flag)
            ),
        ));
    }
    Ok(())
}

fn python_repr(text: &str) -> String {
    let mut out = String::from("'");
    for ch in text.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

fn launch_argv(
    program: &str,
    args: &[String],
    windows: bool,
    path_env: &str,
) -> (String, Vec<String>) {
    if !windows {
        return (program.to_string(), args.to_vec());
    }
    let resolved = resolve_windows_codebuddy(program, path_env);
    if let Some(mut unwrapped) = unwrap_npm_codebuddy_cmd(Path::new(&resolved), path_env)
        && !unwrapped.is_empty()
    {
        let program = unwrapped.remove(0);
        unwrapped.extend(args.iter().cloned());
        return (program, unwrapped);
    }
    (resolved, args.to_vec())
}

fn resolve_windows_codebuddy(program: &str, path_env: &str) -> String {
    if has_path_separator(program) || !program.eq_ignore_ascii_case("codebuddy") {
        return program.to_string();
    }
    find_on_path("codebuddy", path_env).unwrap_or_else(|| program.to_string())
}

fn has_path_separator(name: &str) -> bool {
    name.contains('/') || name.contains('\\')
}

fn find_on_path(name: &str, path_env: &str) -> Option<String> {
    for dir in std::env::split_paths(path_env) {
        if dir.as_os_str().is_empty() || !dir.is_absolute() {
            continue;
        }
        for ext in LAUNCHABLE_EXTS {
            let candidate = dir.join(format!("{name}{ext}"));
            if candidate.is_file() {
                return Some(candidate.to_string_lossy().into_owned());
            }
        }
    }
    None
}

fn is_codebuddy_cmd(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.eq_ignore_ascii_case("codebuddy.cmd"))
}

/// Unwrap one npm `codebuddy.cmd` forwarding shim.
///
/// Returns `[node, ...fixed args]` when the file is the npm cmd-shim shape
/// that forwards to a direct `.exe` or `.com`. Anything else returns `None`.
fn unwrap_npm_codebuddy_cmd(path: &Path, path_env: &str) -> Option<Vec<String>> {
    if !is_codebuddy_cmd(path) {
        return None;
    }
    let lines = read_shim(path)?;
    let parent = path.parent().unwrap_or(path);
    let shim_prefix = shim_dir_prefix(parent);
    let (candidates, prog) = forwarding_candidates(&lines, &shim_prefix)?;
    for tokens in candidates {
        let program_token = tokens.first()?.as_str();
        let mut options = Vec::new();
        if program_token == "%_prog%" {
            options.push(parent.join("node.exe").to_string_lossy().into_owned());
            if let Some(value) = &prog {
                options.push(value.clone());
            }
        } else {
            options.push(program_token.to_string());
        }
        for option in options {
            if option.is_empty() || option.contains('%') {
                continue;
            }
            let Some(program) = resolve_shim_program(&option, path_env) else {
                continue;
            };
            let mut argv = vec![program];
            argv.extend(tokens.iter().skip(1).cloned());
            return Some(argv);
        }
    }
    None
}

fn shim_dir_prefix(parent: &Path) -> String {
    let mut prefix = parent.to_string_lossy().into_owned();
    if !prefix.ends_with('\\') && !prefix.ends_with('/') {
        prefix.push('\\');
    }
    prefix
}

fn read_shim(path: &Path) -> Option<Vec<String>> {
    let meta = std::fs::metadata(path).ok()?;
    if !meta.is_file() || meta.len() > SHIM_MAX_BYTES {
        return None;
    }
    let data = std::fs::read(path).ok()?;
    if data.len() > SHIM_MAX_BYTES as usize || data.contains(&0) {
        return None;
    }
    let text = std::str::from_utf8(&data).ok()?;
    Some(text.lines().map(|line| line.trim().to_string()).collect())
}

fn forwarding_candidates(
    lines: &[String],
    shim_prefix: &str,
) -> Option<(Vec<Vec<String>>, Option<String>)> {
    let mut prog = None;
    let mut candidates = Vec::new();
    let mut has_else = false;
    for raw in lines {
        let line = normalize_shim_line(raw);
        if line.is_empty() || shim_noise().is_match(&line) {
            if shim_else().is_match(&line) {
                has_else = true;
            }
            continue;
        }
        if let Some(caps) = shim_set().captures(&line) {
            let name = caps.get(1)?.as_str().to_ascii_lowercase();
            if !HARMLESS_SET_VARS.contains(&name.as_str()) {
                return None;
            }
            if name == "_prog" {
                let value = caps.get(2)?.as_str().trim().trim_matches('"');
                prog = Some(value.to_string());
            }
            continue;
        }
        if !line.ends_with("%*") {
            return None;
        }
        let command = if line.contains("\"%_prog%\"") {
            line.rsplit_once(" & ")
                .map_or(line.as_str(), |(_, tail)| tail)
        } else {
            line.as_str()
        };
        let body = command.strip_suffix("%*")?;
        let body = body.trim_end();
        let mut tokens = Vec::new();
        for caps in shim_token().captures_iter(body) {
            let (token, bare) = if let Some(quoted) = caps.get(1) {
                (quoted.as_str(), false)
            } else {
                let bare_token = caps.get(2)?;
                (bare_token.as_str(), true)
            };
            if bare && !batch_safe().is_match(token) {
                return None;
            }
            match expand_shim_token(token, shim_prefix) {
                Ok(expanded) => tokens.push(expanded),
                Err(()) => {
                    if token.trim_matches('"') != "%_prog%" {
                        return None;
                    }
                    tokens.push("%_prog%".to_string());
                }
            }
        }
        if tokens.is_empty()
            || tokens
                .iter()
                .filter(|token| token.as_str() != "%_prog%")
                .any(|token| token.contains('%'))
        {
            return None;
        }
        candidates.push(tokens);
    }
    if candidates.is_empty() || (candidates.len() > 1 && !has_else) {
        return None;
    }
    Some((candidates, prog))
}

fn normalize_shim_line(raw: &str) -> String {
    raw.trim_start_matches('@').trim().to_string()
}

fn expand_shim_token(token: &str, shim_prefix: &str) -> std::result::Result<String, ()> {
    let mut out = String::new();
    let mut last = 0usize;
    for caps in shim_expand().captures_iter(token) {
        let Some(whole) = caps.get(0) else {
            return Err(());
        };
        let Some(name) = caps.get(1) else {
            return Err(());
        };
        out.push_str(&token[last..whole.start()]);
        if name.as_str() == "~dp0" || name.as_str() == "dp0" {
            out.push_str(shim_prefix);
        } else {
            return Err(());
        }
        last = whole.end();
    }
    out.push_str(&token[last..]);
    if out.contains(shim_prefix) {
        Ok(windows_style_norm(&out))
    } else {
        Ok(out)
    }
}

fn resolve_shim_program(option: &str, path_env: &str) -> Option<String> {
    if has_path_separator(option) {
        return direct_executable(option);
    }
    if CMD_BUILTINS
        .iter()
        .any(|builtin| option.eq_ignore_ascii_case(builtin))
    {
        return None;
    }
    let found = find_on_path(option, path_env)?;
    direct_executable(&found)
}

fn direct_executable(path: &str) -> Option<String> {
    let norm = windows_style_norm(path);
    let candidate = Path::new(&norm);
    let ext = candidate
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase())?;
    if ext != "exe" && ext != "com" {
        return None;
    }
    let stem = candidate
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map(|stem| stem.to_ascii_lowercase())?;
    if INTERPRETER_PROGRAMS.iter().any(|name| *name == stem) {
        return None;
    }
    if !candidate.is_file() {
        return None;
    }
    Some(norm)
}

fn windows_style_norm(raw: &str) -> String {
    let drive =
        raw.len() >= 2 && raw.as_bytes()[1] == b':' && raw.as_bytes()[0].is_ascii_alphabetic();
    let absolute = drive || raw.starts_with('/') || raw.starts_with('\\');
    let rest = if drive { &raw[2..] } else { raw };
    let mut parts: Vec<&str> = Vec::new();
    for part in rest.split(['/', '\\']) {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            if parts.pop().is_none() && !absolute {
                parts.push("..");
            }
            continue;
        }
        parts.push(part);
    }
    let sep = std::path::MAIN_SEPARATOR;
    let mut out = String::new();
    if drive {
        out.push_str(&raw[..2]);
        out.push(sep);
    } else if raw.starts_with('/') || raw.starts_with('\\') {
        out.push(sep);
    }
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            out.push(sep);
        }
        out.push_str(part);
    }
    if out.is_empty() { ".".to_string() } else { out }
}

fn shim_noise() -> &'static Regex {
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(
            r#"(?i)\A(?:echo\s+off|setlocal|endlocal|goto\s+\S+|exit\s+/b(?:\s+\d+)?|:\S+|call\s+:\S+|rem(?:\s.*)?|::.*|\)|\)\s*else\s*\(|if\s+(?:not\s+)?exist\s+"[^"]*"\s*\(|if\s+not\s+defined\s+\S+\s*\()\z"#,
        )
        .expect("codebuddy shim noise pattern")
    });
    &RE
}

fn shim_else() -> &'static Regex {
    static RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)\)\s*else\b").expect("codebuddy shim else pattern"));
    &RE
}

fn shim_set() -> &'static Regex {
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r#"(?i)\Aset\s+(?:/[a-z]\s+)?"?([^=" ]+)\s*=(.*?)"?\s*\z"#)
            .expect("codebuddy shim set pattern")
    });
    &RE
}

fn shim_token() -> &'static Regex {
    static RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r#""([^"]*)"|(\S+)"#).expect("codebuddy shim token pattern"));
    &RE
}

fn shim_expand() -> &'static Regex {
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"%(~dp0|[A-Za-z_][A-Za-z0-9_]*)%?").expect("codebuddy shim expand pattern")
    });
    &RE
}

fn batch_safe() -> &'static Regex {
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"\A[\w\-+./\\:@#~\[\]{}' \t]*\z").expect("codebuddy batch safe pattern")
    });
    &RE
}

fn parent_path(parent: &[(String, String)]) -> String {
    parent
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("PATH"))
        .map(|(_, value)| value.clone())
        .unwrap_or_default()
}

struct Parsed {
    text: String,
    session_id: String,
    error: String,
    usage: Option<Usage>,
}

fn classify_exit(
    code: i32,
    stdout: &str,
    stderr: &str,
    duration: Duration,
) -> Result<HarnessOutcome> {
    let parsed = parse_streams(stdout, stderr);
    if code != 0 {
        let details = if !parsed.error.is_empty() {
            parsed.error
        } else if !parsed.text.is_empty() {
            parsed.text
        } else {
            let raw = joined_output(stdout, stderr);
            if raw.is_empty() {
                format!("Command exited with code {code}")
            } else {
                raw
            }
        };
        return Err(VyaneError::new(
            ErrorKind::HarnessFailed,
            format!("codebuddy exited with code {code}: {details}"),
        ));
    }
    if !parsed.error.is_empty() {
        return Err(VyaneError::new(ErrorKind::HarnessFailed, parsed.error));
    }
    if parsed.text.is_empty() {
        let raw = joined_output(stdout, stderr);
        let message = if raw.is_empty() {
            NO_RESULT.to_string()
        } else {
            raw
        };
        return Err(VyaneError::new(ErrorKind::HarnessFailed, message));
    }
    Ok(HarnessOutcome {
        text: parsed.text,
        native_session_id: (!parsed.session_id.is_empty()).then_some(parsed.session_id),
        usage: parsed.usage,
        exit_code: code,
        duration,
    })
}

fn parse_streams(stdout: &str, stderr: &str) -> Parsed {
    parse_joined(&joined_output(stdout, stderr))
}

fn joined_output(stdout: &str, stderr: &str) -> String {
    let mut lines = Vec::new();
    lines.extend(stdout.lines().map(str::to_string));
    lines.extend(stderr.lines().map(str::to_string));
    lines.join("\n")
}

fn parse_joined(raw: &str) -> Parsed {
    let text = strip_sgr(raw);
    match decode_document(&text) {
        Decoded::Events(events) => finish_events(&events),
        Decoded::Truncated => Parsed {
            text: String::new(),
            session_id: String::new(),
            error: TRUNCATED_OUTPUT.to_string(),
            usage: None,
        },
        Decoded::Rejected => Parsed {
            text: String::new(),
            session_id: String::new(),
            error: if raw.is_empty() {
                NO_RESULT.to_string()
            } else {
                raw.to_string()
            },
            usage: None,
        },
    }
}

fn finish_events(events: &[Map<String, Value>]) -> Parsed {
    let Some(data) = result_event(events) else {
        return Parsed {
            text: String::new(),
            session_id: String::new(),
            error: NO_RESULT.to_string(),
            usage: None,
        };
    };
    let session_id = session_id(data);
    let usage = usage_of(data);
    let is_error = matches!(data.get("is_error"), Some(Value::Bool(true)));
    let success = data.get("subtype").and_then(Value::as_str) == Some("success");
    if is_error || !success {
        let error = match data.get("result") {
            Some(Value::String(answer)) if !answer.is_empty() => answer.clone(),
            _ => python_dumps(&Value::Object(data.clone())),
        };
        return Parsed {
            text: String::new(),
            session_id,
            error,
            usage,
        };
    }
    match data.get("result") {
        Some(Value::String(answer)) => Parsed {
            text: answer.clone(),
            session_id,
            error: String::new(),
            usage,
        },
        _ => Parsed {
            text: String::new(),
            session_id,
            error: NO_RESULT.to_string(),
            usage,
        },
    }
}

fn result_event(events: &[Map<String, Value>]) -> Option<&Map<String, Value>> {
    events
        .iter()
        .rev()
        .find(|item| item.get("type").and_then(Value::as_str) == Some("result"))
}

fn session_id(data: &Map<String, Value>) -> String {
    for key in ["session_id", "sessionId"] {
        if let Some(Value::String(value)) = data.get(key)
            && !value.is_empty()
        {
            return value.clone();
        }
    }
    String::new()
}

fn usage_of(data: &Map<String, Value>) -> Option<Usage> {
    let usage = data.get("usage")?.as_object()?;
    let input_tokens = json_count(usage.get("input_tokens"))?;
    let output_tokens = json_count(usage.get("output_tokens"))?;
    Some(Usage {
        input_tokens,
        output_tokens,
        reasoning_tokens: None,
        cached_input_tokens: None,
    })
}

fn json_count(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Number(number) => {
            if let Some(value) = number.as_u64() {
                return Some(value);
            }
            if let Some(value) = number.as_i64() {
                return u64::try_from(value).ok();
            }
            None
        }
        _ => None,
    }
}

enum Decoded {
    Events(Vec<Map<String, Value>>),
    Truncated,
    Rejected,
}

fn decode_document(text: &str) -> Decoded {
    let Some(start) = json_start(text) else {
        return Decoded::Rejected;
    };
    if !text.is_char_boundary(start) {
        return Decoded::Rejected;
    }
    let slice = &text[start..];
    let (value, consumed) = match raw_decode(slice) {
        Ok(decoded) => decoded,
        Err(err) => {
            return if is_truncated(slice, &err) {
                Decoded::Truncated
            } else {
                Decoded::Rejected
            };
        }
    };
    let end = start.saturating_add(consumed);
    if end > text.len() || !text.is_char_boundary(end) {
        return Decoded::Truncated;
    }
    if tail_rejects(&text[end..]) {
        return Decoded::Rejected;
    }
    match value {
        Value::Object(map) => Decoded::Events(vec![map]),
        Value::Array(items) => Decoded::Events(
            items
                .into_iter()
                .filter_map(|item| match item {
                    Value::Object(map) => Some(map),
                    _ => None,
                })
                .collect(),
        ),
        _ => Decoded::Rejected,
    }
}

fn raw_decode(input: &str) -> std::result::Result<(Value, usize), serde_json::Error> {
    let mut stream = serde_json::Deserializer::from_str(input).into_iter::<Value>();
    match stream.next() {
        Some(Ok(value)) => Ok((value, stream.byte_offset())),
        Some(Err(err)) => Err(err),
        None => match serde_json::from_str::<Value>(input) {
            Ok(value) => Ok((value, input.len())),
            Err(err) => Err(err),
        },
    }
}

fn json_start(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut line_start = 0usize;
    while line_start < bytes.len() {
        let rest = &bytes[line_start..];
        let line_len = rest
            .iter()
            .position(|byte| *byte == b'\n')
            .unwrap_or(rest.len());
        let line = &rest[..line_len];
        let mut index = 0usize;
        while index < line.len() && (line[index] == b' ' || line[index] == b'\t') {
            index += 1;
        }
        if index < line.len() && (line[index] == b'[' || line[index] == b'{') {
            return Some(line_start + index);
        }
        if line_start + line_len >= bytes.len() {
            break;
        }
        line_start += line_len + 1;
    }
    None
}

fn tail_rejects(tail: &str) -> bool {
    if tail.trim().is_empty() {
        return false;
    }
    let lead = tail.trim_start_matches([' ', '\t', '\r']);
    let same_line_junk = !lead.starts_with('\n');
    same_line_junk || json_start(tail).is_some()
}

fn is_truncated(full: &str, err: &serde_json::Error) -> bool {
    let lower = err.to_string().to_ascii_lowercase();
    if err.is_eof() || lower.contains("unterminated") {
        return true;
    }
    let trimmed = full.trim_end();
    let pos = error_byte_index(full, err.line(), err.column()).min(full.len());
    if full.is_char_boundary(pos) {
        let pos_chars = full[..pos].chars().count();
        if pos_chars >= trimmed.chars().count() {
            return true;
        }
    }
    incomplete_unicode_escape(trimmed) && lower.contains("escape")
}

fn error_byte_index(text: &str, line: usize, column: usize) -> usize {
    if line == 0 {
        return text.len();
    }
    let mut current_line = 1usize;
    let mut line_start = 0usize;
    for (idx, ch) in text.char_indices() {
        if current_line == line {
            line_start = idx;
            break;
        }
        if ch == '\n' {
            current_line += 1;
            line_start = idx + ch.len_utf8();
        }
    }
    if current_line != line {
        return text.len();
    }
    for (col, (offset, ch)) in (1usize..).zip(text[line_start..].char_indices()) {
        if ch == '\n' || col == column {
            return line_start + offset;
        }
    }
    line_start
        + text[line_start..]
            .find('\n')
            .unwrap_or(text.len() - line_start)
}

fn incomplete_unicode_escape(text: &str) -> bool {
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"\\u[0-9a-fA-F]{0,3}$").expect("codebuddy unicode escape pattern")
    });
    RE.is_match(text)
}

fn strip_sgr(text: &str) -> String {
    static RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new("\u{1b}\\[[0-9;]*m").expect("codebuddy sgr pattern"));
    RE.replace_all(text, "").into_owned()
}

#[cfg(test)]
fn parse_output_lines(lines: &[&str]) -> (String, String, String) {
    let parsed = parse_joined(&lines.join("\n"));
    (parsed.text, parsed.session_id, parsed.error)
}

#[cfg(test)]
fn parse_usage_lines(lines: &[&str]) -> Option<Usage> {
    parse_joined(&lines.join("\n")).usage
}

#[async_trait]
impl Harness for CodebuddyHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::Other(KIND.into())
    }

    async fn available(&self) -> bool {
        crate::probe::binary_available(&self.bin).await
    }

    async fn run(&self, job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome> {
        let parent = self.parent_env.clone().unwrap_or_else(parent_env_snapshot);
        let path_env = parent_path(&parent);
        let launch = prepare_launch(&job, &self.bin, cfg!(windows), &path_env)?;
        let env = materialize_env(&job.env, parent);

        tracing::debug!(
            harness = KIND,
            argc = launch.args.len(),
            env_keys = %env_key_list(&env),
            "spawning codebuddy harness"
        );

        let result = run_capture_with_pinned(
            &launch.program,
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
    use super::*;
    use serde_json::json;
    use vyane_core::env::EnvPolicy;
    use vyane_core::target::{ModelId, Protocol};
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
            harness_lifecycle_reporter: None,
        }
    }

    fn args_of(sample: &HarnessJob) -> Vec<String> {
        prepare_launch(sample, "codebuddy", false, "").unwrap().args
    }

    fn base_tail(prompt: &str, model: &str) -> Vec<String> {
        vec![
            "-p".to_string(),
            prompt.to_string(),
            "--model".to_string(),
            model.to_string(),
            "--output-format".to_string(),
            "json".to_string(),
            "--setting-sources".to_string(),
            "user".to_string(),
        ]
    }

    fn success_object(result: &str, session_key: &str, session: &str) -> Value {
        json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "result": result,
            session_key: session,
            "usage": {"input_tokens": 3, "output_tokens": 1, "total_tokens": 99}
        })
    }

    fn npm_codebuddy_cmd(script_rel: &str) -> String {
        let template = r#"@ECHO off
GOTO start
:find_dp0
SET dp0=%~dp0
EXIT /b
:start
SETLOCAL
CALL :find_dp0

IF EXIST "%dp0%\node.exe" (
  SET "_prog=%dp0%\node.exe"
) ELSE (
  SET "_prog=node"
  SET PATHEXT=%PATHEXT:;.JS;=;%
)

endLocal & goto #_undefined_# 2>NUL || title %COMSPEC% & "%_prog%"  "%dp0%\SCRIPT_REL" %*
"#;
        template.replace("SCRIPT_REL", script_rel)
    }

    #[test]
    fn kind_token_is_other_codebuddy() {
        let harness = CodebuddyHarness::new();
        assert_eq!(harness.kind(), HarnessKind::Other("codebuddy".into()));
        assert_eq!(harness.kind().as_str(), "codebuddy");
        assert_ne!(harness.kind(), HarnessKind::ClaudeCode);
        assert_ne!(harness.kind(), HarnessKind::CodexCli);
        assert_ne!(harness.kind(), HarnessKind::OpenCode);
        assert_eq!(
            HarnessKind::from("codebuddy"),
            HarnessKind::Other("codebuddy".into())
        );
        assert_eq!(
            format!("{:?}", CodebuddyHarness::default()),
            format!("{harness:?}")
        );
    }

    #[test]
    fn default_command_uses_pinned_model_and_no_sandbox_flag() {
        let args = args_of(&job("hi"));
        assert_eq!(args, base_tail("hi", DEFAULT_MODEL));
        assert_eq!(
            args[args.iter().position(|arg| arg == "--model").unwrap() + 1],
            DEFAULT_MODEL
        );
        assert!(!args.iter().any(|arg| arg == "--fallback-model"));
        assert!(!args.iter().any(|arg| arg == "--permission-mode"));
        assert!(!args.iter().any(|arg| arg == "-y"));
        assert!(!args.iter().any(|arg| arg == "--resume"));
        assert!(!args.iter().any(|arg| arg == "--prompt-file"));

        let mut unknown = job("hi");
        unknown.sandbox = Sandbox::ReadOnly;
        unknown.params.extra.insert("model".into(), json!("m"));
        assert_eq!(args_of(&unknown), base_tail("hi", "m"));
    }

    #[test]
    fn prompt_stays_on_argv_including_flags_and_newlines() {
        let args = args_of(&job("--help"));
        assert_eq!(args[0], "-p");
        assert_eq!(args[1], "--help");
        assert_eq!(args_of(&job("line1\nline2"))[1], "line1\nline2");
        assert_eq!(args_of(&job(""))[1], "");
    }

    #[test]
    fn model_comes_from_the_job_then_extra_then_the_default() {
        let mut from_job = job("hi");
        from_job.model = ModelId::new("from-job");
        from_job
            .params
            .extra
            .insert("model".into(), json!("from-extra"));
        assert!(
            args_of(&from_job)
                .windows(2)
                .any(|pair| pair == ["--model", "from-job"])
        );
        assert!(!args_of(&from_job).iter().any(|arg| arg == "from-extra"));

        let mut from_extra = job("hi");
        from_extra
            .params
            .extra
            .insert("model".into(), json!("kimi-k2.5"));
        assert!(
            args_of(&from_extra)
                .windows(2)
                .any(|pair| pair == ["--model", "kimi-k2.5"])
        );

        let mut numeric = job("hi");
        numeric.params.extra.insert("model".into(), json!(1));
        assert!(
            args_of(&numeric)
                .windows(2)
                .any(|pair| pair == ["--model", DEFAULT_MODEL])
        );
    }

    #[test]
    fn task_type_and_chain_index_do_not_select_a_model() {
        let mut sample = job("hi");
        sample
            .params
            .extra
            .insert("task_type".into(), json!("code_edit_test"));
        sample.params.extra.insert("chain_index".into(), json!(3));
        let args = args_of(&sample);
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--model", DEFAULT_MODEL])
        );
        assert!(
            !args
                .iter()
                .any(|arg| arg == "--task-type" || arg == "--chain-index")
        );
        assert!(!args.iter().any(|arg| arg.contains("glm")));

        sample.params.extra.insert("chain_index".into(), json!(-1));
        assert!(prepare_launch(&sample, "codebuddy", false, "").is_ok());
        sample
            .params
            .extra
            .insert("chain_index".into(), json!(true));
        assert!(
            args_of(&sample)
                .windows(2)
                .any(|pair| pair == ["--model", DEFAULT_MODEL])
        );
    }

    #[test]
    fn write_full_resume_and_whitelist_flags_match_the_command_contract() {
        let mut sample = job("hi");
        sample.sandbox = Sandbox::Write;
        sample.resume = Some("resume-id".into());
        sample.params.extra.insert("model".into(), json!("m"));
        sample.params.extra.insert("effort".into(), json!("high"));
        sample
            .params
            .extra
            .insert("add_dir".into(), json!(["/one", "/two"]));
        sample
            .params
            .extra
            .insert("allowed_tools".into(), json!("Read"));
        sample
            .params
            .extra
            .insert("disallowed_tools".into(), json!("Bash"));
        sample
            .params
            .extra
            .insert("fallback_model".into(), json!("n"));
        sample
            .params
            .extra
            .insert("owner_user_id".into(), json!("secret"));
        sample.params.extra.insert("run_id".into(), json!("id"));
        sample
            .params
            .extra
            .insert("bound_owner".into(), json!("owner"));
        sample.params.extra.insert("invented".into(), json!("x"));
        let args = args_of(&sample);
        assert_eq!(
            args,
            vec![
                "-p",
                "hi",
                "--model",
                "m",
                "--output-format",
                "json",
                "--setting-sources",
                "user",
                "--permission-mode",
                "acceptEdits",
                "--resume",
                "resume-id",
                "--effort",
                "high",
                "--allowedTools",
                "Read",
                "--disallowedTools",
                "Bash",
                "--add-dir",
                "/one",
                "--add-dir",
                "/two",
                "--fallback-model",
                "n",
            ]
        );
        for secret in [
            "secret",
            "owner",
            "--run-id",
            "--invented",
            "code_edit_test",
        ] {
            assert!(
                !args.iter().any(|arg| arg.contains(secret)),
                "{secret} in {args:?}"
            );
        }

        let mut full = job("hi");
        full.sandbox = Sandbox::Full;
        full.params.extra.insert("model".into(), json!("m"));
        let full_args = args_of(&full);
        assert!(full_args.iter().any(|arg| arg == "-y"));
        assert!(!full_args.iter().any(|arg| arg == "--permission-mode"));
    }

    #[test]
    fn falsy_extras_and_non_list_add_dir_are_omitted() {
        let mut sample = job("hi");
        sample.params.extra.insert("effort".into(), json!(""));
        sample
            .params
            .extra
            .insert("allowed_tools".into(), json!(false));
        sample
            .params
            .extra
            .insert("disallowed_tools".into(), json!(0));
        sample
            .params
            .extra
            .insert("fallback_model".into(), json!(null));
        sample
            .params
            .extra
            .insert("add_dir".into(), json!("/not-a-list"));
        let args = args_of(&sample);
        for flag in [
            "--effort",
            "--allowedTools",
            "--disallowedTools",
            "--fallback-model",
            "--add-dir",
        ] {
            assert!(!args.iter().any(|arg| arg == flag), "{flag} in {args:?}");
        }

        sample.params.extra.insert("effort".into(), json!(true));
        sample.params.extra.insert("allowed_tools".into(), json!(1));
        sample
            .params
            .extra
            .insert("add_dir".into(), json!(["", "/one"]));
        let args = args_of(&sample);
        assert!(args.windows(2).any(|pair| pair == ["--effort", "True"]));
        assert!(args.windows(2).any(|pair| pair == ["--allowedTools", "1"]));
        assert_eq!(
            args.iter()
                .filter(|arg| arg.as_str() == "--add-dir")
                .count(),
            2
        );
        assert!(args.windows(2).any(|pair| pair == ["--add-dir", ""]));
        assert!(args.windows(2).any(|pair| pair == ["--add-dir", "/one"]));
    }

    #[test]
    fn structured_effort_is_not_a_flag() {
        let mut sample = job("hi");
        sample.params.effort = Some(vyane_core::task::Effort::High);
        assert!(!args_of(&sample).iter().any(|arg| arg == "--effort"));
    }

    #[test]
    fn oversized_prompt_is_rejected_before_spawn() {
        let prompt = "Q".repeat(MAX_ARG_STRLEN);
        let err = prepare_launch(&job(&prompt), "codebuddy", false, "").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Config);
        assert!(err.message.contains("MAX_ARG_STRLEN"), "{}", err.message);
        assert!(err.message.contains("token #2"), "{}", err.message);
        assert!(err.message.contains("'-p'"), "{}", err.message);
        assert!(!err.message.contains("QQQ"), "{}", err.message);

        let fits = "Q".repeat(MAX_ARG_STRLEN - 1);
        assert!(prepare_launch(&job(&fits), "codebuddy", false, "").is_ok());

        let over_chars = "字".repeat((MAX_ARG_STRLEN / 3) + 1);
        assert!(over_chars.chars().count() < MAX_ARG_STRLEN);
        assert!(over_chars.len() >= MAX_ARG_STRLEN);
        assert_eq!(
            prepare_launch(&job(&over_chars), "codebuddy", false, "")
                .unwrap_err()
                .kind,
            ErrorKind::Config
        );
        let under_chars = "字".repeat(MAX_ARG_STRLEN / 3);
        assert!(under_chars.len() < MAX_ARG_STRLEN);
        assert!(prepare_launch(&job(&under_chars), "codebuddy", false, "").is_ok());
    }

    #[test]
    fn posix_launch_argv_does_not_unwrap_a_cmd_shim() {
        let dir = tempfile::tempdir().unwrap();
        let shim = dir.path().join("codebuddy.cmd");
        std::fs::write(&shim, npm_codebuddy_cmd("cli.js")).unwrap();
        let (program, args) = launch_argv(&shim.to_string_lossy(), &["a&b".into()], false, "");
        assert_eq!(program, shim.to_string_lossy());
        assert_eq!(args, vec!["a&b".to_string()]);
    }

    #[test]
    fn npm_codebuddy_cmd_unwraps_like_launch_argv() {
        let dir = tempfile::tempdir().unwrap();
        let shim_dir = dir.path().join("npm");
        let script = shim_dir
            .join("node_modules")
            .join("@scope")
            .join("tool")
            .join("bin")
            .join("codebuddy.js");
        std::fs::create_dir_all(script.parent().unwrap()).unwrap();
        std::fs::write(&script, "").unwrap();
        let node = shim_dir.join("node.exe");
        std::fs::write(&node, b"MZ").unwrap();
        let shim = shim_dir.join("codebuddy.cmd");
        let body = npm_codebuddy_cmd(r"node_modules\@scope\tool\bin\codebuddy.js");
        std::fs::write(&shim, body.replace('\n', "\r\n")).unwrap();

        let (program, args) = launch_argv(&shim.to_string_lossy(), &["a&b".into()], true, "");
        assert_eq!(PathBuf::from(&program), node);
        assert_eq!(PathBuf::from(&args[0]), script);
        assert_eq!(args[1], "a&b");
        assert!(!args.iter().any(|arg| arg.ends_with(".cmd")));
    }

    #[test]
    fn npm_shim_uses_path_node_when_the_sibling_exe_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let shim_dir = dir.path().join("npm");
        std::fs::create_dir_all(&shim_dir).unwrap();
        let node_dir = dir.path().join("nodejs");
        std::fs::create_dir_all(&node_dir).unwrap();
        let node = node_dir.join("node.exe");
        std::fs::write(&node, b"MZ").unwrap();
        let shim = shim_dir.join("codebuddy.cmd");
        std::fs::write(&shim, npm_codebuddy_cmd("cli.js")).unwrap();
        let (program, args) = launch_argv(
            &shim.to_string_lossy(),
            &["hi".into()],
            true,
            node_dir.to_string_lossy().as_ref(),
        );
        assert_eq!(PathBuf::from(&program), node);
        assert!(args[0].ends_with("cli.js"), "{}", args[0]);
    }

    #[test]
    fn codebuddy_exe_on_path_wins_over_the_cmd_shim() {
        let dir = tempfile::tempdir().unwrap();
        let bin_dir = dir.path().join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let exe = bin_dir.join("codebuddy.exe");
        std::fs::write(&exe, b"MZ").unwrap();
        let shim = bin_dir.join("codebuddy.cmd");
        std::fs::write(&shim, npm_codebuddy_cmd("cli.js")).unwrap();
        let (program, _) = launch_argv(
            "codebuddy",
            &["hi".into()],
            true,
            bin_dir.to_string_lossy().as_ref(),
        );
        assert_eq!(PathBuf::from(&program), exe);
    }

    #[test]
    fn relative_path_entries_and_other_cmd_names_are_not_unwrapped() {
        let dir = tempfile::tempdir().unwrap();
        let shim_dir = dir.path().join("npm");
        std::fs::create_dir_all(&shim_dir).unwrap();
        let node = shim_dir.join("node.exe");
        std::fs::write(&node, b"MZ").unwrap();
        let tool = shim_dir.join("tool.cmd");
        std::fs::write(&tool, npm_codebuddy_cmd("cli.js")).unwrap();
        let (program, _) = launch_argv(&tool.to_string_lossy(), &["a&b".into()], true, "");
        assert_eq!(program, tool.to_string_lossy());
        assert_ne!(PathBuf::from(&program), node);

        let planted = dir.path().join("codebuddy.cmd");
        std::fs::write(&planted, npm_codebuddy_cmd("cli.js")).unwrap();
        let (program, _) = launch_argv("codebuddy", &["hi".into()], true, ".");
        assert_eq!(program, "codebuddy");
    }

    #[test]
    fn shim_that_forwards_to_cmd_exe_is_not_unwrapped() {
        let dir = tempfile::tempdir().unwrap();
        let shim_dir = dir.path().join("npm");
        std::fs::create_dir_all(&shim_dir).unwrap();
        let cmd = shim_dir.join("cmd.exe");
        std::fs::write(&cmd, b"MZ").unwrap();
        let shim = shim_dir.join("codebuddy.cmd");
        let body = npm_codebuddy_cmd("cli.js").replace(
            "SET \"_prog=node\"",
            &format!("SET \"_prog={}\"", cmd.to_string_lossy().replace('/', "\\")),
        );
        std::fs::write(&shim, body).unwrap();
        let (program, args) = launch_argv(&shim.to_string_lossy(), &["a&b".into()], true, "");
        assert_eq!(program, shim.to_string_lossy());
        assert_eq!(args, vec!["a&b".to_string()]);
    }

    #[test]
    fn array_and_lone_object_parse_the_last_result_and_token_usage() {
        let array = json!([
            {
                "type": "message",
                "role": "assistant",
                "providerData": {"model": "kimi-k2.5", "rawUsage": {"credit": 2.81}}
            },
            {
                "type": "result",
                "subtype": "error",
                "is_error": true,
                "result": "earlier",
                "session_id": "not-this"
            },
            {
                "type": "result",
                "subtype": "success",
                "is_error": false,
                "result": "OK",
                "session_id": "e92d4a23-3c7a-4cd5-856f-a08d5ddd30b6",
                "usage": {
                    "input_tokens": 24455,
                    "output_tokens": 23,
                    "reasoning_tokens": 9,
                    "cache_read_input_tokens": 4,
                    "total_tokens": 999,
                    "credit": 2.81
                }
            }
        ]);
        let line = array.to_string();
        assert_eq!(
            parse_output_lines(&[&line]),
            (
                "OK".to_string(),
                "e92d4a23-3c7a-4cd5-856f-a08d5ddd30b6".to_string(),
                String::new()
            )
        );
        let usage = parse_usage_lines(&[&line]).unwrap();
        assert_eq!(usage.input_tokens, 24455);
        assert_eq!(usage.output_tokens, 23);
        assert_eq!(usage.reasoning_tokens, None);
        assert_eq!(usage.cached_input_tokens, None);

        let lone = success_object("OK", "sessionId", "legacy-session");
        let text = format!("(node:1) Warning: ignored\n{lone}");
        let refs: Vec<&str> = text.lines().collect();
        assert_eq!(
            parse_output_lines(&refs),
            (
                "OK".to_string(),
                "legacy-session".to_string(),
                String::new()
            )
        );
        let usage = parse_usage_lines(&refs).unwrap();
        assert_eq!(usage.input_tokens, 3);
        assert_eq!(usage.output_tokens, 1);

        let live = r#"{"type":"result","subtype":"success","is_error":false,"result":"OK","session_id":"01a16730-c444-7ebc-a925-4ebcc5fe4598","usage":{"input_tokens":23682,"output_tokens":2,"credit":0.3}}"#;
        let live_text = format!("(node:1) Warning: ignored\n{live}");
        let live_refs: Vec<&str> = live_text.lines().collect();
        assert_eq!(
            parse_output_lines(&live_refs),
            (
                "OK".to_string(),
                "01a16730-c444-7ebc-a925-4ebcc5fe4598".to_string(),
                String::new()
            )
        );
        let live_usage = parse_usage_lines(&live_refs).unwrap();
        assert_eq!(live_usage.input_tokens, 23682);
        assert_eq!(live_usage.output_tokens, 2);
        assert!(!parse_output_lines(&live_refs).0.contains("0.3"));
    }

    #[test]
    fn session_id_prefers_snake_case_and_skips_empty_values() {
        let both = r#"{"type":"result","subtype":"success","is_error":false,"result":"OK","session_id":"snake","sessionId":"camel"}"#;
        assert_eq!(parse_output_lines(&[both]).1, "snake");
        let empty = r#"{"type":"result","subtype":"success","is_error":false,"result":"OK","session_id":"","sessionId":"camel"}"#;
        assert_eq!(parse_output_lines(&[empty]).1, "camel");
        let numeric = r#"{"type":"result","subtype":"success","is_error":false,"result":"OK","session_id":1,"sessionId":"camel"}"#;
        assert_eq!(parse_output_lines(&[numeric]).1, "camel");
    }

    #[test]
    fn ansi_sgr_and_diagnostic_lines_around_one_document_are_kept() {
        let payload = success_object("OK", "session_id", "s");
        let colored = format!("\u{1b}[1;31m{payload}\u{1b}[0m");
        assert_eq!(parse_output_lines(&[&colored]).0, "OK");
        let wrapped = format!("(node:1) Warning: ignored\n{payload}\n(node:1) done");
        let refs: Vec<&str> = wrapped.lines().collect();
        assert_eq!(parse_output_lines(&refs).0, "OK");
        assert_eq!(
            parse_output_lines(&["\u{1b}[2J still not json"]).2,
            "\u{1b}[2J still not json"
        );
    }

    #[test]
    fn error_subtype_and_is_error_fail_closed() {
        let denied = r#"[{"type":"result","subtype":"error","is_error":true,"result":"denied"}]"#;
        assert_eq!(
            parse_output_lines(&[denied]),
            (String::new(), String::new(), "denied".to_string())
        );
        let subtype_only = r#"{"type":"result","subtype":"error_during_execution","is_error":false,"result":"boom","session_id":"s"}"#;
        assert_eq!(
            parse_output_lines(&[subtype_only]),
            (String::new(), "s".to_string(), "boom".to_string())
        );
        let flag_only = r#"{"type":"result","subtype":"success","is_error":true,"result":"nope","sessionId":"legacy"}"#;
        assert_eq!(
            parse_output_lines(&[flag_only]),
            (String::new(), "legacy".to_string(), "nope".to_string())
        );
        let missing_subtype =
            r#"{"type":"result","is_error":false,"result":"OK","session_id":"s"}"#;
        assert_eq!(parse_output_lines(&[missing_subtype]).0, "");
        assert_eq!(parse_output_lines(&[missing_subtype]).2, "OK");
        let stringly = r#"{"type":"result","subtype":"success","is_error":"true","result":"OK","session_id":"s"}"#;
        assert_eq!(parse_output_lines(&[stringly]).0, "OK");
    }

    #[test]
    fn missing_result_and_non_string_result_fail_closed() {
        assert_eq!(
            parse_output_lines(&["[]"]),
            (String::new(), String::new(), NO_RESULT.to_string())
        );
        let messages = r#"[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"OK"}]}]"#;
        assert_eq!(parse_output_lines(&[messages]).2, NO_RESULT);
        let blank = r#"[{"type":"result","subtype":"success","is_error":false,"session_id":"s","result":null}]"#;
        assert_eq!(
            parse_output_lines(&[blank]),
            (String::new(), "s".to_string(), NO_RESULT.to_string())
        );
        assert_eq!(parse_output_lines(&["broken"]).2, "broken");
        assert_eq!(parse_output_lines(&[""]).2, NO_RESULT);
        let nested = r#"[{"type":"message","inner":{"type":"result","subtype":"success","is_error":false,"result":"OK"}}]"#;
        assert_eq!(parse_output_lines(&[nested]).2, NO_RESULT);
        let non_object = "42";
        assert_eq!(parse_output_lines(&[non_object]).2, "42");
    }

    #[test]
    fn fallback_error_is_python_spaced_json_when_result_is_not_a_string() {
        let line = r#"{"type":"result","subtype":"error","is_error":true,"session_id":"s"}"#;
        let (_, session, error) = parse_output_lines(&[line]);
        assert_eq!(session, "s");
        let parsed: Value = serde_json::from_str(&error).unwrap();
        assert_eq!(error, python_dumps(&parsed));
        assert_eq!(parsed["subtype"], "error");
        assert_eq!(parsed["is_error"], true);
        assert!(error.contains(": "));
        assert!(error.contains(", "));
    }

    #[test]
    fn second_document_and_same_line_junk_are_not_rescued() {
        let first = r#"{"type":"result","subtype":"success","is_error":false,"result":"OK","session_id":"s"}"#;
        let second = r#"{"type":"result","subtype":"success","is_error":false,"result":"LATER"}"#;
        let raw = format!("{first}\n{second}");
        let refs: Vec<&str> = raw.lines().collect();
        let (text, _, error) = parse_output_lines(&refs);
        assert_eq!(text, "");
        assert_eq!(error, raw);
        assert_ne!(error, "OK");

        let junk = format!("{first} TRAILING");
        assert_eq!(parse_output_lines(&[&junk]).2, junk);
    }

    #[test]
    fn truncated_json_fails_closed_without_scanning_later_documents() {
        for sample in [
            "{",
            r#"[{"type":"result""#,
            "{\"type\":\"result\",\"result\":\"unterminated",
        ] {
            assert_eq!(
                parse_output_lines(&[sample]).2,
                TRUNCATED_OUTPUT,
                "{sample}"
            );
        }
        let escape = r#"{"a":"\u12"#;
        assert_eq!(parse_output_lines(&[escape]).2, TRUNCATED_OUTPUT);
        let later = "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"OK\"\n{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"LATER\",\"session_id\":\"s\"}";
        let (text, session, error) = parse_output_lines(&later.lines().collect::<Vec<_>>());
        assert_eq!(text, "");
        assert_eq!(session, "");
        assert_ne!(error, "LATER");
        assert_ne!(error, "OK");
        assert!(error == TRUNCATED_OUTPUT || error == later, "{error}");

        assert_eq!(parse_output_lines(&["{bad}"]).2, "{bad}");
        assert_ne!(parse_output_lines(&["{bad}"]).2, TRUNCATED_OUTPUT);
    }

    #[test]
    fn usage_requires_both_unsigned_integer_fields_on_the_result() {
        let ok = r#"{"type":"result","subtype":"success","is_error":false,"result":"OK","usage":{"input_tokens":3,"output_tokens":1}}"#;
        assert!(parse_usage_lines(&[ok]).is_some());
        let float = r#"{"type":"result","subtype":"success","is_error":false,"result":"OK","usage":{"input_tokens":1.0,"output_tokens":2}}"#;
        assert!(parse_usage_lines(&[float]).is_none());
        let boolean = r#"{"type":"result","subtype":"success","is_error":false,"result":"OK","usage":{"input_tokens":true,"output_tokens":1}}"#;
        assert!(parse_usage_lines(&[boolean]).is_none());
        let negative = r#"{"type":"result","subtype":"success","is_error":false,"result":"OK","usage":{"input_tokens":-1,"output_tokens":1}}"#;
        assert!(parse_usage_lines(&[negative]).is_none());
        let half = r#"{"type":"result","subtype":"success","is_error":false,"result":"OK","usage":{"input_tokens":3}}"#;
        assert!(parse_usage_lines(&[half]).is_none());
        let elsewhere = r#"[{"type":"message","usage":{"input_tokens":9,"output_tokens":9}},{"type":"result","subtype":"success","is_error":false,"result":"OK"}]"#;
        assert!(parse_usage_lines(&[elsewhere]).is_none());
    }

    #[test]
    fn classify_exit_maps_parse_failures_and_keeps_usage_on_success() {
        let ok = r#"{"type":"result","subtype":"success","is_error":false,"result":"OK","session_id":"s","usage":{"input_tokens":3,"output_tokens":1}}"#;
        let outcome = classify_exit(0, ok, "", Duration::from_secs(0)).unwrap();
        assert_eq!(outcome.text, "OK");
        assert_eq!(outcome.native_session_id.as_deref(), Some("s"));
        assert_eq!(outcome.usage.unwrap().input_tokens, 3);
        assert_eq!(outcome.exit_code, 0);

        let denied = classify_exit(
            0,
            r#"{"type":"result","subtype":"error","is_error":true,"result":"denied"}"#,
            "",
            Duration::from_secs(0),
        )
        .unwrap_err();
        assert_eq!(denied.kind, ErrorKind::HarnessFailed);
        assert_eq!(denied.message, "denied");

        let nonzero = classify_exit(7, ok, "", Duration::from_secs(0)).unwrap_err();
        assert_eq!(nonzero.kind, ErrorKind::HarnessFailed);
        assert!(nonzero.message.contains("code 7"), "{}", nonzero.message);
        assert!(nonzero.message.contains("OK"), "{}", nonzero.message);
    }
}
