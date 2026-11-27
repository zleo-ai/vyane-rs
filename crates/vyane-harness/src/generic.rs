//! One-shot adapter for a caller-supplied CLI.
//!
//! Callers construct [`GenericCliHarness`] with a harness name, the command to
//! execute, and an argv template. The template is rendered by literal
//! substring replacement, in this order:
//!
//! 1. `{task}` from the job prompt
//! 2. `{workdir}` from the job working directory (`""` when absent)
//! 3. `{sandbox}` from [`vyane_core::Sandbox::as_str`] (`read-only`, `write`, or `full`)
//! 4. `{session_id}` from `job.resume` (`""` when absent)
//! 5. each remaining entry of [`vyane_core::GenParams::extra`], in key order
//!
//! Keys `task`, `workdir`, `sandbox`, and `session_id` in `params.extra` are
//! ignored, so extras cannot replace those four values. Replacement is the
//! literal substring `{key}`: `{task}` does not match `{task_extra}`. A value
//! inserted by an earlier key can still contain a later placeholder. Text
//! inserted for a key is not scanned for that same key again. An empty
//! template means `["{task}"]`. The command string itself is not a template.
//!
//! String extras are substituted as their text. Any other JSON value is
//! substituted as its JSON text (`true`, `null`, `1`, objects and arrays).
//! Extras are template variables, not flags appended after the template.
//!
//! The child is one scrubbed process. Stdin is empty. The whole stdout string
//! is the answer: it is not parsed for a session id or token usage. A non-zero
//! exit is a harness failure. The endpoint on the job is not read and is not
//! copied into the child environment.
//!
//! This type is not selected by provider command configuration, the service
//! assembler, or the auto, failover, and broadcast pools. A profile whose
//! harness name happens to match the name passed here still does not construct
//! this adapter.

use std::collections::BTreeMap;
use std::path::Path;

use async_trait::async_trait;
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;
use vyane_core::error::{ErrorKind, Result, VyaneError};
use vyane_core::target::HarnessKind;
use vyane_core::traits::{Harness, HarnessJob, HarnessOutcome};

use crate::spawn::{
    RunControl, RunResult, Termination, env_key_list, materialize_env, parent_env_snapshot,
    run_capture_with_pinned,
};

/// Keys filled from the job. Entries of the same name in `params.extra` are
/// dropped before substitution.
const RESERVED_KEYS: &[&str] = &["task", "workdir", "sandbox", "session_id"];

/// Plain-text parse of one generic CLI stdout.
///
/// `session_id` and `error_text` are always empty. Token usage is not part of
/// this parse; the harness outcome leaves it unset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenericCliOutput {
    pub text: String,
    pub session_id: String,
    pub error_text: String,
}

/// `Harness` adapter that runs one caller-supplied command from an argv template.
#[derive(Debug, Clone)]
pub struct GenericCliHarness {
    name: String,
    command: String,
    args_template: Vec<String>,
    parent_env: Option<Vec<(String, String)>>,
}

impl GenericCliHarness {
    /// Command plus the default template `["{task}"]`.
    ///
    /// `name` is the [`HarnessKind::Other`] token. It is not passed through
    /// [`HarnessKind`]'s built-in name mapping, so `"claude-code"` stays
    /// `Other`, not [`HarnessKind::ClaudeCode`].
    pub fn new(name: impl Into<String>, command: impl Into<String>) -> Self {
        Self::with_args_template(name, command, std::iter::empty::<String>())
    }

    /// Command plus an argv template.
    ///
    /// An empty template uses `["{task}"]`. A template that contains a blank
    /// argument is kept as written.
    pub fn with_args_template<I, S>(
        name: impl Into<String>,
        command: impl Into<String>,
        args_template: I,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let args_template: Vec<String> = args_template.into_iter().map(Into::into).collect();
        let args_template = if args_template.is_empty() {
            vec!["{task}".to_string()]
        } else {
            args_template
        };
        Self {
            name: name.into(),
            command: command.into(),
            args_template,
            parent_env: None,
        }
    }

    /// Fix the parent environment snapshot used to build the child environment.
    ///
    /// Tests use this so the child environment is not the live process
    /// environment. Production callers use [`Self::new`] or
    /// [`Self::with_args_template`], which snapshot the parent at spawn.
    #[doc(hidden)]
    pub fn with_parent_env_for_tests(mut self, parent_env: Vec<(String, String)>) -> Self {
        self.parent_env = Some(parent_env);
        self
    }

    /// Caller-supplied [`HarnessKind::Other`] token.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Program executed for one shot. This string is not template-rendered.
    #[must_use]
    pub fn command(&self) -> &str {
        &self.command
    }

    /// Argv template after the empty-template default has been applied.
    #[must_use]
    pub fn args_template(&self) -> &[String] {
        &self.args_template
    }

    /// Render `[command, …template]` for one call.
    ///
    /// `sandbox` and `session_id` are the literal strings written into the
    /// template. [`Self::build_command_for_job`] is the mapping used by
    /// [`Harness::run`].
    #[must_use]
    pub fn build_command(
        &self,
        prompt: &str,
        workdir: &str,
        sandbox: &str,
        session_id: &str,
        extra_args: &Map<String, Value>,
    ) -> Vec<String> {
        let substitutions = substitution_pairs(prompt, workdir, sandbox, session_id, extra_args);
        let mut argv = Vec::with_capacity(self.args_template.len() + 1);
        argv.push(self.command.clone());
        for arg in &self.args_template {
            argv.push(render_arg(arg, &substitutions));
        }
        argv
    }

    /// [`Self::build_command`] using the job fields [`Harness::run`] uses.
    #[must_use]
    pub fn build_command_for_job(&self, job: &HarnessJob) -> Vec<String> {
        self.build_command(
            &job.prompt,
            &workdir_text(job.workdir.as_deref()),
            job.sandbox.as_str(),
            job.resume.as_deref().unwrap_or(""),
            &job.params.extra,
        )
    }

    /// Return all of `stdout` as the answer. The session and error strings are
    /// empty, and this function does not look for token usage.
    #[must_use]
    pub fn parse_output(stdout: &str) -> GenericCliOutput {
        GenericCliOutput {
            text: stdout.to_string(),
            session_id: String::new(),
            error_text: String::new(),
        }
    }

    fn build_env(&self, job: &HarnessJob) -> BTreeMap<String, String> {
        let parent = self.parent_env.clone().unwrap_or_else(parent_env_snapshot);
        materialize_env(&job.env, parent)
    }

    fn rendered_args(&self, job: &HarnessJob) -> Vec<String> {
        self.build_command_for_job(job)
            .into_iter()
            .skip(1)
            .collect()
    }

    fn classify_result(&self, result: RunResult) -> Result<HarnessOutcome> {
        match result.termination {
            Termination::Cancelled => Err(VyaneError::cancelled()),
            Termination::TimedOut => Err(VyaneError::new(
                ErrorKind::Timeout,
                format!("{} harness timed out", self.name),
            )),
            Termination::Exited(code) => {
                if code == 0 {
                    let parsed = Self::parse_output(&result.stdout);
                    debug_assert!(parsed.session_id.is_empty());
                    debug_assert!(parsed.error_text.is_empty());
                    Ok(HarnessOutcome {
                        text: parsed.text,
                        native_session_id: None,
                        usage: None,
                        exit_code: code,
                        duration: result.duration,
                    })
                } else {
                    Err(VyaneError::new(
                        ErrorKind::HarnessFailed,
                        format!(
                            "{} exited with code {code}: {}",
                            self.name,
                            stderr_tail(&result.stderr)
                        ),
                    ))
                }
            }
        }
    }
}

fn is_reserved(key: &str) -> bool {
    RESERVED_KEYS.contains(&key)
}

fn substitution_pairs(
    prompt: &str,
    workdir: &str,
    sandbox: &str,
    session_id: &str,
    extra_args: &Map<String, Value>,
) -> Vec<(String, String)> {
    let mut pairs = vec![
        ("task".to_string(), prompt.to_string()),
        ("workdir".to_string(), workdir.to_string()),
        ("sandbox".to_string(), sandbox.to_string()),
        ("session_id".to_string(), session_id.to_string()),
    ];
    for (key, value) in extra_args {
        if is_reserved(key) {
            continue;
        }
        pairs.push((key.clone(), extra_value_text(value)));
    }
    pairs
}

fn extra_value_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn render_arg(template: &str, substitutions: &[(String, String)]) -> String {
    let mut rendered = template.to_string();
    for (key, value) in substitutions {
        let placeholder = format!("{{{key}}}");
        rendered = rendered.replace(&placeholder, value);
    }
    rendered
}

fn workdir_text(path: Option<&Path>) -> String {
    path.map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Last ~200 chars of stderr, single-lined, for an error message.
fn stderr_tail(stderr: &str) -> String {
    let flattened = stderr.trim().replace('\n', " ");
    if flattened.chars().count() <= 200 {
        return flattened;
    }
    let tail: String = flattened
        .chars()
        .rev()
        .take(200)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("...{tail}")
}

#[async_trait]
impl Harness for GenericCliHarness {
    fn kind(&self) -> HarnessKind {
        HarnessKind::Other(self.name.clone())
    }

    async fn available(&self) -> bool {
        crate::probe::binary_available(&self.command).await
    }

    async fn run(&self, job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome> {
        let args = self.rendered_args(&job);
        let env = self.build_env(&job);

        tracing::debug!(
            harness = %self.name,
            argc = args.len(),
            env_keys = %env_key_list(&env),
            "spawning generic cli harness"
        );

        let result = run_capture_with_pinned(
            &self.command,
            &args,
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
    use vyane_core::target::{ModelId, Protocol, Sandbox};
    use vyane_core::task::GenParams;

    fn sample_job(prompt: &str) -> HarnessJob {
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

    fn command(
        template: &[&str],
        prompt: &str,
        workdir: &str,
        sandbox: &str,
        session_id: &str,
        extra: &Map<String, Value>,
    ) -> Vec<String> {
        GenericCliHarness::with_args_template("test", "echo", template.iter().copied())
            .build_command(prompt, workdir, sandbox, session_id, extra)
    }

    #[test]
    fn empty_template_defaults_to_task_but_a_blank_arg_is_kept() {
        let defaulted = GenericCliHarness::new("n", "cmd");
        assert_eq!(defaulted.args_template(), ["{task}"]);
        assert_eq!(
            defaulted.build_command("hello", "", "read-only", "", &Map::new()),
            vec!["cmd".to_string(), "hello".to_string()]
        );

        let blank = GenericCliHarness::with_args_template("n", "cmd", [""]);
        assert_eq!(blank.args_template(), [""]);
        assert_eq!(
            blank.build_command("p", "/w", "read-only", "s", &Map::new()),
            vec!["cmd".to_string(), String::new()]
        );
    }

    #[test]
    fn command_string_is_not_a_template() {
        let harness = GenericCliHarness::with_args_template("{task}", "{task}", ["{task}"]);
        assert_eq!(
            harness.build_command("hello", "", "read-only", "", &Map::new()),
            vec!["{task}".to_string(), "hello".to_string()]
        );
    }

    #[test]
    fn reserved_keys_cannot_overwrite_job_values() {
        let mut extra = Map::new();
        extra.insert("task".into(), Value::String("INJECTED".into()));
        assert_eq!(
            command(&["{task}"], "real prompt", "/tmp", "read-only", "", &extra),
            vec!["echo".to_string(), "real prompt".to_string()]
        );

        extra.clear();
        extra.insert("workdir".into(), Value::String("/evil/dir".into()));
        assert_eq!(
            command(
                &["{workdir}"],
                "prompt",
                "/safe/dir",
                "read-only",
                "",
                &extra
            ),
            vec!["echo".to_string(), "/safe/dir".to_string()]
        );

        extra.clear();
        extra.insert("sandbox".into(), Value::String("danger-full-access".into()));
        assert_eq!(
            command(&["{sandbox}"], "prompt", "/tmp", "read-only", "", &extra),
            vec!["echo".to_string(), "read-only".to_string()]
        );

        extra.clear();
        extra.insert("session_id".into(), Value::String("hijacked-id".into()));
        assert_eq!(
            command(
                &["{session_id}"],
                "prompt",
                "/tmp",
                "read-only",
                "real-id",
                &extra
            ),
            vec!["echo".to_string(), "real-id".to_string()]
        );
    }

    #[test]
    fn custom_extra_is_substituted_and_strings_are_not_json_quoted() {
        let mut extra = Map::new();
        extra.insert("model".into(), Value::String("llama3".into()));
        extra.insert("task".into(), Value::String("INJECTED".into()));
        assert_eq!(
            command(
                &["{task}", "{model}"],
                "prompt",
                "/tmp",
                "read-only",
                "",
                &extra
            ),
            vec![
                "echo".to_string(),
                "prompt".to_string(),
                "llama3".to_string()
            ]
        );
    }

    #[test]
    fn non_string_extras_use_json_text() {
        let mut extra = Map::new();
        extra.insert("flag".into(), Value::Bool(true));
        extra.insert("count".into(), serde_json::json!(3));
        extra.insert("empty".into(), Value::Null);
        assert_eq!(
            command(
                &["{flag}", "{count}", "{empty}"],
                "prompt",
                "",
                "read-only",
                "",
                &extra
            ),
            vec![
                "echo".to_string(),
                "true".to_string(),
                "3".to_string(),
                "null".to_string()
            ]
        );
    }

    #[test]
    fn extras_follow_key_order_and_the_same_key_is_not_rescanned() {
        // `params.extra` iterates in key order, so `a` is applied before `b`.
        // `{b}` becomes `{a}` after `a` has already been substituted.
        let mut extra = Map::new();
        extra.insert("b".into(), Value::String("{a}".into()));
        extra.insert("a".into(), Value::String("Z".into()));
        assert_eq!(
            command(&["{b}"], "prompt", "", "read-only", "", &extra),
            vec!["echo".to_string(), "{a}".to_string()]
        );

        let mut later = Map::new();
        later.insert("note".into(), Value::String("{task}".into()));
        assert_eq!(
            command(&["{note}"], "real", "", "read-only", "", &later),
            vec!["echo".to_string(), "{task}".to_string()]
        );

        assert_eq!(
            command(&["{task}"], "{task}", "", "read-only", "", &Map::new()),
            vec!["echo".to_string(), "{task}".to_string()]
        );

        assert_eq!(
            command(&["{task}-{task}"], "a", "", "read-only", "", &Map::new()),
            vec!["echo".to_string(), "a-a".to_string()]
        );
    }

    #[test]
    fn task_replacement_matches_the_whole_placeholder() {
        assert_eq!(
            command(&["{task_extra}"], "X", "", "read-only", "", &Map::new()),
            vec!["echo".to_string(), "{task_extra}".to_string()]
        );

        let mut extra = Map::new();
        extra.insert("model".into(), Value::String("llama3".into()));
        assert_eq!(
            command(&["{task}"], "see {model}", "", "read-only", "", &extra),
            vec!["echo".to_string(), "see llama3".to_string()]
        );
    }

    #[test]
    fn differently_cased_reserved_names_are_ordinary_extras() {
        let mut extra = Map::new();
        extra.insert("Task".into(), Value::String("other".into()));
        assert_eq!(
            command(&["{task}", "{Task}"], "real", "", "read-only", "", &extra),
            vec!["echo".to_string(), "real".to_string(), "other".to_string()]
        );
    }

    #[test]
    fn job_fields_map_onto_the_four_placeholders() {
        let harness = GenericCliHarness::with_args_template(
            "deepseek",
            "cmd",
            ["{task}", "{workdir}", "{sandbox}", "{session_id}"],
        );
        let mut job = sample_job("prompt");
        job.sandbox = Sandbox::Full;
        job.workdir = None;
        job.resume = None;
        assert_eq!(
            harness.build_command_for_job(&job),
            vec![
                "cmd".to_string(),
                "prompt".to_string(),
                String::new(),
                "full".to_string(),
                String::new()
            ]
        );

        job.sandbox = Sandbox::Write;
        job.workdir = Some("/work/tree".into());
        job.resume = Some("real-id".into());
        assert_eq!(
            harness.build_command_for_job(&job),
            vec![
                "cmd".to_string(),
                "prompt".to_string(),
                "/work/tree".to_string(),
                "write".to_string(),
                "real-id".to_string()
            ]
        );
    }

    #[test]
    fn kind_uses_other_without_remapping_built_in_names() {
        let harness = GenericCliHarness::new("claude-code", "echo");
        assert_eq!(harness.kind(), HarnessKind::Other("claude-code".into()));
        assert_ne!(harness.kind(), HarnessKind::ClaudeCode);
        assert_eq!(harness.name(), "claude-code");
        assert_eq!(harness.command(), "echo");
    }

    #[test]
    fn assembler_config_and_cli_do_not_name_this_adapter() {
        let files = [
            include_str!("../../vyane-service/src/factory.rs"),
            include_str!("../../vyane-config/src/model.rs"),
            include_str!("../../vyane-config/src/resolve.rs"),
            include_str!("../../vyane-cli/src/command.rs"),
        ];
        for source in files {
            assert!(!source.contains("GenericCliHarness"));
            assert!(!source.contains("vyane_harness::generic"));
        }
    }

    #[test]
    fn parse_output_returns_stdout_unchanged_with_no_session_or_error() {
        let stdout = "{\"session_id\":\"native-1\",\"usage\":{\"input_tokens\":9}}\nplain line\n";
        let parsed = GenericCliHarness::parse_output(stdout);
        assert_eq!(parsed.text, stdout);
        assert_eq!(parsed.session_id, "");
        assert_eq!(parsed.error_text, "");
        assert_eq!(GenericCliHarness::parse_output("").text, "");
    }
}
