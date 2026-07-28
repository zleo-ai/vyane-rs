//! Linux-only, explicitly allowlisted native command execution.
//!
//! Commands are argv vectors rather than shell strings. Bubblewrap receives
//! the exact pinned workspace descriptor and mounts it read-only, while the
//! existing subprocess driver owns timeout, cancellation and descendant
//! cleanup.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use vyane_core::{
    NativeExecutionAuthority, NativeSideEffect, PinnedWorkdir, Result as VyaneResult,
    ToolDefinition,
};

use crate::spawn::{RunControl, Termination, run_capture_with_pinned_limit};

use super::{
    MAX_TOOL_OUTPUT_CHARS, NativeTool, PermissionEffect, PermissionPolicy, PermissionRule,
    PermissionRuleError, ToolContext, ToolError, ToolRegistry,
};

const BWRAP: &str = "/usr/bin/bwrap";
const DEFAULT_COMMAND_SECONDS: u64 = 60;
const MAX_COMMAND_SECONDS: u64 = 60 * 60;
const MAX_RULES: usize = 128;
const MAX_ARGS: usize = 128;
const MAX_PROGRAM_BYTES: usize = 128;
const MAX_ARG_BYTES: usize = 4096;
const MAX_TOTAL_ARG_BYTES: usize = 64 * 1024;
const CAPTURE_BYTES_PER_STREAM: usize = 32 * 1024;
const DISPLAY_CHARS_PER_STREAM: usize = 14_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCommandRule {
    pub program: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args_prefix: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCommandPolicy {
    pub allow: Vec<NativeCommandRule>,
    #[serde(default = "default_command_seconds")]
    pub max_seconds: u64,
}

impl NativeCommandPolicy {
    pub fn validate(&self) -> Result<(), NativeCommandPolicyError> {
        if self.allow.is_empty() {
            return Err(NativeCommandPolicyError::EmptyAllowlist);
        }
        if self.allow.len() > MAX_RULES {
            return Err(NativeCommandPolicyError::TooManyRules);
        }
        if self.max_seconds == 0 || self.max_seconds > MAX_COMMAND_SECONDS {
            return Err(NativeCommandPolicyError::InvalidTimeout);
        }
        for rule in &self.allow {
            validate_program(&rule.program)?;
            validate_args(&rule.args_prefix)?;
        }
        Ok(())
    }

    fn permits(&self, program: &str, args: &[String]) -> bool {
        self.allow.iter().any(|rule| {
            rule.program == program
                && args
                    .get(..rule.args_prefix.len())
                    .is_some_and(|prefix| prefix == rule.args_prefix)
        })
    }
}

const fn default_command_seconds() -> u64 {
    DEFAULT_COMMAND_SECONDS
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NativeCommandPolicyError {
    #[error("native command allowlist must not be empty")]
    EmptyAllowlist,
    #[error("native command policy contains too many rules")]
    TooManyRules,
    #[error("native command policy contains an invalid program")]
    InvalidProgram,
    #[error("native command policy contains invalid arguments")]
    InvalidArguments,
    #[error("native command timeout is invalid")]
    InvalidTimeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NativeCommandHostError {
    #[error("native command tools require Linux bubblewrap confinement")]
    Unsupported,
}

pub fn command_tool_definition() -> ToolDefinition {
    ToolDefinition {
        name: "run_command".into(),
        description:
            "Run one explicitly allowlisted argv command in a read-only, networkless workspace."
                .into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "program": {
                    "type": "string",
                    "description": "Allowlisted executable basename"
                },
                "args": {
                    "type": "array",
                    "items": { "type": "string" },
                    "maxItems": MAX_ARGS,
                    "default": []
                },
                "timeout_seconds": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_COMMAND_SECONDS
                }
            },
            "required": ["program"],
            "additionalProperties": false
        }),
    }
}

pub fn command_tool_registry(
    policy: NativeCommandPolicy,
) -> Result<ToolRegistry, NativeCommandPolicyError> {
    policy.validate()?;
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(RunCommandTool {
            policy: Arc::new(policy),
        }))
        .map_err(|_| NativeCommandPolicyError::InvalidProgram)?;
    Ok(registry)
}

pub fn register_command_tool(
    registry: &mut ToolRegistry,
    policy: NativeCommandPolicy,
) -> Result<(), NativeCommandPolicyError> {
    policy.validate()?;
    registry
        .register(Arc::new(RunCommandTool {
            policy: Arc::new(policy),
        }))
        .map_err(|_| NativeCommandPolicyError::InvalidProgram)
}

pub fn command_permission_policy(
    mut policy: PermissionPolicy,
) -> Result<PermissionPolicy, PermissionRuleError> {
    policy.push_rule(PermissionRule::new("run_command", PermissionEffect::Allow)?);
    Ok(policy)
}

/// Probe the same descriptor-bound, networkless Bubblewrap profile used by the
/// tool before a native submission can issue a paid model request.
#[cfg(target_os = "linux")]
pub async fn validate_command_host(
    pinned: &PinnedWorkdir,
    policy: &NativeCommandPolicy,
) -> Result<(), NativeCommandHostError> {
    policy
        .validate()
        .map_err(|_| NativeCommandHostError::Unsupported)?;
    if !std::path::Path::new(BWRAP).is_file() {
        return Err(NativeCommandHostError::Unsupported);
    }
    let args = sandbox_args("/usr/bin/true", &[]);
    let result = run_capture_with_pinned_limit(
        BWRAP,
        &args,
        Some(pinned.canonical_path()),
        Some(pinned),
        &command_environment(),
        RunControl::new(
            vyane_core::CancellationToken::new(),
            Some(Duration::from_secs(5)),
            None,
        ),
        1024,
    )
    .await
    .map_err(|_| NativeCommandHostError::Unsupported)?;
    match result.termination {
        Termination::Exited(0) => Ok(()),
        _ => Err(NativeCommandHostError::Unsupported),
    }
}

#[cfg(not(target_os = "linux"))]
pub async fn validate_command_host(
    _pinned: &PinnedWorkdir,
    _policy: &NativeCommandPolicy,
) -> Result<(), NativeCommandHostError> {
    Err(NativeCommandHostError::Unsupported)
}

struct RunCommandTool {
    policy: Arc<NativeCommandPolicy>,
}

#[async_trait]
impl NativeTool for RunCommandTool {
    fn name(&self) -> &str {
        "run_command"
    }

    async fn execute(
        &self,
        _arguments: &BTreeMap<String, Value>,
        _context: &ToolContext,
    ) -> Result<String, ToolError> {
        Err(ToolError::new(
            "run_command requires live native execution authority",
        ))
    }

    async fn execute_authorized(
        &self,
        arguments: &BTreeMap<String, Value>,
        context: &ToolContext,
        authority: &dyn NativeExecutionAuthority,
        effect: NativeSideEffect,
    ) -> VyaneResult<Result<String, ToolError>> {
        let request = match CommandRequest::parse(arguments, &self.policy) {
            Ok(request) => request,
            Err(error) => return Ok(Err(error)),
        };
        let Some(pinned) = context.pinned_workdir() else {
            return Ok(Err(ToolError::new(
                "run_command requires the admitted workspace handle",
            )));
        };

        // Registry dispatch already checked the call. This second check is
        // owned by the command tool and sits directly before the spawn path.
        authority.revalidate(effect).await?;
        let args = sandbox_args(&request.program, &request.args);
        let result = run_capture_with_pinned_limit(
            BWRAP,
            &args,
            Some(pinned.canonical_path()),
            Some(pinned),
            &command_environment(),
            RunControl::new(
                context.cancellation_token().clone(),
                Some(Duration::from_secs(request.timeout_seconds)),
                None,
            ),
            CAPTURE_BYTES_PER_STREAM,
        )
        .await?;
        Ok(Ok(format_command_result(result)))
    }
}

struct CommandRequest {
    program: String,
    args: Vec<String>,
    timeout_seconds: u64,
}

impl CommandRequest {
    fn parse(
        arguments: &BTreeMap<String, Value>,
        policy: &NativeCommandPolicy,
    ) -> Result<Self, ToolError> {
        if arguments
            .keys()
            .any(|key| !matches!(key.as_str(), "program" | "args" | "timeout_seconds"))
        {
            return Err(ToolError::new("run_command arguments are invalid"));
        }
        let program = arguments
            .get("program")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::new("run_command program is required"))?
            .to_string();
        validate_program(&program).map_err(|_| ToolError::new("run_command program is invalid"))?;
        let args = match arguments.get("args") {
            None => Vec::new(),
            Some(Value::Array(values)) => values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_string)
                        .ok_or_else(|| ToolError::new("run_command arguments are invalid"))
                })
                .collect::<Result<Vec<_>, _>>()?,
            Some(_) => return Err(ToolError::new("run_command arguments are invalid")),
        };
        validate_args(&args).map_err(|_| ToolError::new("run_command arguments are invalid"))?;
        let timeout_seconds = match arguments.get("timeout_seconds") {
            None => policy.max_seconds.min(DEFAULT_COMMAND_SECONDS),
            Some(value) => value
                .as_u64()
                .filter(|seconds| *seconds > 0 && *seconds <= policy.max_seconds)
                .ok_or_else(|| ToolError::new("run_command timeout is invalid"))?,
        };
        if !policy.permits(&program, &args) {
            return Err(ToolError::new(
                "run_command is outside the configured allowlist",
            ));
        }
        Ok(Self {
            program,
            args,
            timeout_seconds,
        })
    }
}

fn validate_program(program: &str) -> Result<(), NativeCommandPolicyError> {
    if program.is_empty()
        || program.len() > MAX_PROGRAM_BYTES
        || program == "."
        || program == ".."
        || !program
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'+'))
    {
        return Err(NativeCommandPolicyError::InvalidProgram);
    }
    Ok(())
}

fn validate_args(args: &[String]) -> Result<(), NativeCommandPolicyError> {
    if args.len() > MAX_ARGS {
        return Err(NativeCommandPolicyError::InvalidArguments);
    }
    let mut total = 0usize;
    for argument in args {
        if argument.len() > MAX_ARG_BYTES || argument.contains('\0') {
            return Err(NativeCommandPolicyError::InvalidArguments);
        }
        total = total.saturating_add(argument.len());
        if total > MAX_TOTAL_ARG_BYTES {
            return Err(NativeCommandPolicyError::InvalidArguments);
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn sandbox_args(program: &str, args: &[String]) -> Vec<String> {
    let mut sandbox = vec![
        "--die-with-parent".into(),
        "--unshare-user".into(),
        "--unshare-pid".into(),
        "--unshare-ipc".into(),
        "--unshare-uts".into(),
        "--unshare-cgroup-try".into(),
        "--unshare-net".into(),
        "--new-session".into(),
        "--proc".into(),
        "/proc".into(),
        "--dev".into(),
        "/dev".into(),
        "--tmpfs".into(),
        "/tmp".into(),
        "--dir".into(),
        "/usr".into(),
        "--dir".into(),
        "/workspace".into(),
        "--ro-bind".into(),
        "/proc/self/fd/8".into(),
        "/workspace".into(),
    ];
    for path in [
        "/usr/bin",
        "/usr/lib",
        "/usr/lib64",
        "/usr/libexec",
        "/usr/share",
        "/bin",
        "/lib",
        "/lib64",
    ] {
        if std::path::Path::new(path).exists() {
            sandbox.extend(["--ro-bind".into(), path.into(), path.into()]);
        }
    }
    sandbox.extend([
        "--chdir".into(),
        "/workspace".into(),
        "--clearenv".into(),
        "--setenv".into(),
        "PATH".into(),
        "/usr/bin:/bin".into(),
        "--setenv".into(),
        "HOME".into(),
        "/tmp".into(),
        "--setenv".into(),
        "TMPDIR".into(),
        "/tmp".into(),
        "--setenv".into(),
        "LANG".into(),
        "C.UTF-8".into(),
        "--".into(),
        program.into(),
    ]);
    sandbox.extend(args.iter().cloned());
    sandbox
}

#[cfg(not(target_os = "linux"))]
fn sandbox_args(_program: &str, _args: &[String]) -> Vec<String> {
    Vec::new()
}

fn command_environment() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("HOME".into(), "/tmp".into()),
        ("LANG".into(), "C.UTF-8".into()),
        ("PATH".into(), "/usr/bin:/bin".into()),
        ("TMPDIR".into(), "/tmp".into()),
    ])
}

fn format_command_result(result: crate::spawn::RunResult) -> String {
    let status = match result.termination {
        Termination::Exited(code) => format!("exit_code: {code}"),
        Termination::Cancelled => "status: cancelled".into(),
        Termination::TimedOut => "status: timed_out".into(),
    };
    let mut output = format!("{status}\nstdout:\n");
    append_stream(&mut output, &result.stdout, "stdout");
    output.push_str("\nstderr:\n");
    append_stream(&mut output, &result.stderr, "stderr");
    debug_assert!(output.chars().count() <= MAX_TOOL_OUTPUT_CHARS);
    output
}

fn append_stream(output: &mut String, stream: &str, label: &str) {
    let mut chars = stream.chars();
    output.extend(chars.by_ref().take(DISPLAY_CHARS_PER_STREAM));
    if chars.next().is_some() || stream.len() == CAPTURE_BYTES_PER_STREAM {
        output.push_str(&format!("\n... [{label} truncated]"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> NativeCommandPolicy {
        NativeCommandPolicy {
            allow: vec![
                NativeCommandRule {
                    program: "git".into(),
                    args_prefix: vec!["status".into()],
                },
                NativeCommandRule {
                    program: "printf".into(),
                    args_prefix: Vec::new(),
                },
            ],
            max_seconds: 30,
        }
    }

    #[test]
    fn policy_matches_exact_program_and_argument_prefix() {
        let policy = policy();
        assert!(policy.permits("git", &["status".into()]));
        assert!(policy.permits("git", &["status".into(), "--short".into()]));
        assert!(!policy.permits("git", &["diff".into()]));
        assert!(!policy.permits("git-status", &[]));
    }

    #[test]
    fn policy_rejects_paths_empty_lists_and_unbounded_timeouts() {
        for program in ["", "../git", "/usr/bin/git", "git status"] {
            let invalid = NativeCommandPolicy {
                allow: vec![NativeCommandRule {
                    program: program.into(),
                    args_prefix: Vec::new(),
                }],
                max_seconds: 30,
            };
            assert!(invalid.validate().is_err(), "{program}");
        }
        let mut invalid = policy();
        invalid.allow.clear();
        assert_eq!(
            invalid.validate(),
            Err(NativeCommandPolicyError::EmptyAllowlist)
        );
        invalid = policy();
        invalid.max_seconds = MAX_COMMAND_SECONDS + 1;
        assert_eq!(
            invalid.validate(),
            Err(NativeCommandPolicyError::InvalidTimeout)
        );
    }

    #[test]
    fn request_cannot_exceed_frozen_rule_or_timeout() {
        let policy = policy();
        let denied = BTreeMap::from([
            ("program".into(), Value::String("git".into())),
            (
                "args".into(),
                Value::Array(vec![Value::String("diff".into())]),
            ),
        ]);
        assert!(CommandRequest::parse(&denied, &policy).is_err());
        let timeout = BTreeMap::from([
            ("program".into(), Value::String("printf".into())),
            ("timeout_seconds".into(), Value::from(31)),
        ]);
        assert!(CommandRequest::parse(&timeout, &policy).is_err());
    }

    #[test]
    fn command_definition_is_stable_and_closed() {
        let definition = command_tool_definition();
        assert_eq!(definition.name, "run_command");
        assert_eq!(definition.input_schema["additionalProperties"], false);
        assert_eq!(definition.input_schema["required"], json!(["program"]));
    }

    #[test]
    fn standalone_registry_contains_only_command_tool() {
        let registry = command_tool_registry(policy()).expect("registry");
        assert_eq!(registry.names().collect::<Vec<_>>(), vec!["run_command"]);
    }
}
