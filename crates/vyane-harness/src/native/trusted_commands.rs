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

#[cfg(target_os = "linux")]
use crate::spawn::run_capture_with_pinned_limit_authorized_channel;
use crate::spawn::{RunControl, Termination, run_capture_with_pinned_limit_authorized};

#[cfg(target_os = "linux")]
use super::trusted_network::run_network_broker;
#[cfg(target_os = "linux")]
use super::trusted_network::validate_network_route_host;
use super::{
    MAX_TOOL_OUTPUT_CHARS, NativeTool, PermissionEffect, PermissionPolicy, PermissionRule,
    PermissionRuleError, ToolContext, ToolError, ToolRegistry,
};
use super::{NativeCommandNetworkPolicy, NativeCommandNetworkPolicyError};

const BWRAP: &str = "/usr/bin/bwrap";
const PRLIMIT: &str = "/usr/bin/prlimit";
const KEYCTL: &str = "/usr/bin/keyctl";
const DEFAULT_COMMAND_SECONDS: u64 = 60;
const MAX_COMMAND_SECONDS: u64 = 60 * 60;
const MAX_RULES: usize = 128;
const MAX_ARGS: usize = 128;
const MAX_PROGRAM_BYTES: usize = 128;
const MAX_ARG_BYTES: usize = 4096;
const MAX_TOTAL_ARG_BYTES: usize = 64 * 1024;
const CAPTURE_BYTES_PER_STREAM: usize = 32 * 1024;
const DISPLAY_CHARS_PER_STREAM: usize = 14_000;
const COMMAND_ADDRESS_SPACE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const COMMAND_PROCESS_HEADROOM: u64 = 64;
const COMMAND_PROCESS_LIMIT_MAX: u64 = 4096;
#[cfg(target_os = "linux")]
const CAP_SYS_ADMIN_BIT: u64 = 21;
#[cfg(target_os = "linux")]
const CAP_SYS_RESOURCE_BIT: u64 = 24;
const COMMAND_OPEN_FILES: u64 = 256;
const COMMAND_FILE_SIZE_BYTES: u64 = 64 * 1024 * 1024;
#[cfg(target_os = "linux")]
const COMMAND_ROOT_TMP_BYTES: u64 = 16 * 1024 * 1024;
#[cfg(target_os = "linux")]
const COMMAND_TMP_BYTES: u64 = 64 * 1024 * 1024;
#[cfg(target_os = "linux")]
const SECCOMP_FILTER_FD: &str = "7";
#[cfg(target_os = "linux")]
const NETWORK_PROXY_SCRIPT: &str = r#"
import ctypes, os, select, socket, struct, subprocess, sys

def exact(sock, count):
    data = b""
    while len(data) < count:
        part = sock.recv(count - len(data))
        if not part:
            raise EOFError()
        data += part
    return data

broker = socket.socket(fileno=5)
if ctypes.CDLL(None, use_errno=True).prctl(4, 0, 0, 0, 0) != 0:
    raise OSError(ctypes.get_errno(), "prctl(PR_SET_DUMPABLE) failed")
listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", 3128))
listener.listen(8)

env = os.environ.copy()
env.update({
    "HTTP_PROXY": "http://127.0.0.1:3128",
    "HTTPS_PROXY": "http://127.0.0.1:3128",
    "ALL_PROXY": "http://127.0.0.1:3128",
    "http_proxy": "http://127.0.0.1:3128",
    "https_proxy": "http://127.0.0.1:3128",
    "all_proxy": "http://127.0.0.1:3128",
    "NO_PROXY": "",
    "no_proxy": "",
})
child = subprocess.Popen(sys.argv[1:], env=env, close_fds=True)

while child.poll() is None:
    ready, _, _ = select.select([listener], [], [], 0.1)
    if not ready:
        continue
    client, _ = listener.accept()
    client.settimeout(5)
    established = False
    broker_synchronized = True
    client_eof = remote_eof = False
    header = b""
    while b"\r\n\r\n" not in header and len(header) <= 16384:
        part = client.recv(4096)
        if not part:
            break
        header += part
    try:
        first = header.split(b"\r\n", 1)[0].decode("ascii")
        method, authority, _ = first.split(" ", 2)
        host, port_text = authority.rsplit(":", 1)
        port = int(port_text)
        if method != "CONNECT" or not (0 < port < 65536):
            raise ValueError()
        host_bytes = host.lower().encode("ascii")
        broker.sendall(b"\x01" + struct.pack("!HH", len(host_bytes), port) + host_bytes)
        if exact(broker, 1) != b"\x01":
            raise PermissionError()
        client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        established = True
        client.settimeout(None)
        while not (client_eof and remote_eof):
            if child.poll() is not None:
                break
            watched = []
            if not client_eof:
                watched.append(client)
            if not remote_eof:
                watched.append(broker)
            readable, _, _ = select.select(watched, [], [], 0.1)
            if child.poll() is not None:
                break
            if broker in readable:
                length = struct.unpack("!I", exact(broker, 4))[0]
                if length == 0:
                    remote_eof = True
                    client.shutdown(socket.SHUT_WR)
                elif length <= 65536:
                    client.sendall(exact(broker, length))
                else:
                    raise ValueError()
            if client in readable:
                data = client.recv(65536)
                broker.sendall(struct.pack("!I", len(data)) + data)
                if not data:
                    client_eof = True
    except Exception:
        if not established:
            try:
                client.sendall(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
            except Exception:
                pass
        else:
            try:
                if not client_eof:
                    broker.sendall(struct.pack("!I", 0))
                while not remote_eof:
                    length = struct.unpack("!I", exact(broker, 4))[0]
                    if length == 0:
                        remote_eof = True
                    elif length <= 65536:
                        exact(broker, length)
                    else:
                        raise ValueError()
            except Exception:
                broker_synchronized = False
    finally:
        client.close()
    if not broker_synchronized:
        child.terminate()
        break

listener.close()
sys.exit(child.wait())
"#;

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
    #[error("native command tools require x86_64 or aarch64 Linux bubblewrap confinement")]
    Unsupported,
}

#[cfg(target_os = "linux")]
pub async fn validate_command_network_host(
    pinned: &PinnedWorkdir,
    command_policy: &NativeCommandPolicy,
    policy: &NativeCommandNetworkPolicy,
) -> Result<(), NativeCommandHostError> {
    validate_command_host_requirements(command_policy)?;
    policy
        .validate()
        .map_err(|_| NativeCommandHostError::Unsupported)?;
    validate_network_route_host(policy).map_err(|_| NativeCommandHostError::Unsupported)?;
    let probe_arguments = command_probe_arguments(command_policy);
    let args = launcher_args("/bin/sh", &probe_arguments, 5, true)
        .map_err(|_| NativeCommandHostError::Unsupported)?;
    let probe_authority = ProbeAuthority;
    let result = run_networked_command(
        &args,
        pinned,
        RunControl::new(
            vyane_core::CancellationToken::new(),
            Some(Duration::from_secs(5)),
            None,
        ),
        &probe_authority,
        NativeSideEffect::ToolOperation {
            turn: 0,
            ordinal: 0,
        },
        policy,
    )
    .await
    .map_err(|_| NativeCommandHostError::Unsupported)?;
    match result.termination {
        Termination::Exited(0) => Ok(()),
        _ => Err(NativeCommandHostError::Unsupported),
    }
}

#[cfg(not(target_os = "linux"))]
pub async fn validate_command_network_host(
    _pinned: &PinnedWorkdir,
    _command_policy: &NativeCommandPolicy,
    _policy: &NativeCommandNetworkPolicy,
) -> Result<(), NativeCommandHostError> {
    Err(NativeCommandHostError::Unsupported)
}

#[cfg(target_os = "linux")]
struct ProbeAuthority;

#[cfg(target_os = "linux")]
#[async_trait]
impl NativeExecutionAuthority for ProbeAuthority {
    async fn revalidate(&self, _effect: NativeSideEffect) -> VyaneResult<()> {
        Ok(())
    }
}

pub fn command_tool_definition() -> ToolDefinition {
    ToolDefinition {
        name: "run_command".into(),
        description: "Run one explicitly allowlisted argv command in a read-only workspace. \
            Networking is absent unless the submission grants a separate HTTPS destination policy."
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
            network: None,
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
            network: None,
        }))
        .map_err(|_| NativeCommandPolicyError::InvalidProgram)
}

pub fn register_command_tool_with_network(
    registry: &mut ToolRegistry,
    policy: NativeCommandPolicy,
    network: NativeCommandNetworkPolicy,
) -> Result<(), RegisterCommandToolError> {
    policy.validate()?;
    network.validate()?;
    registry
        .register(Arc::new(RunCommandTool {
            policy: Arc::new(policy),
            network: Some(Arc::new(network)),
        }))
        .map_err(|_| RegisterCommandToolError::Registry)
}

#[derive(Debug, thiserror::Error)]
pub enum RegisterCommandToolError {
    #[error(transparent)]
    Command(#[from] NativeCommandPolicyError),
    #[error(transparent)]
    Network(#[from] NativeCommandNetworkPolicyError),
    #[error("native command tool registration failed")]
    Registry,
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
    validate_command_host_requirements(policy)?;
    let probe_arguments = command_probe_arguments(policy);
    let args = launcher_args("/bin/sh", &probe_arguments, 5, false)
        .map_err(|_| NativeCommandHostError::Unsupported)?;
    let probe_authority = ProbeAuthority;
    let result = run_capture_with_pinned_limit_authorized(
        PRLIMIT,
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
        &probe_authority,
        NativeSideEffect::ToolOperation {
            turn: 0,
            ordinal: 0,
        },
        &command_seccomp_filter(false),
    )
    .await
    .map_err(|_| NativeCommandHostError::Unsupported)?;
    match result.termination {
        Termination::Exited(0) => Ok(()),
        _ => Err(NativeCommandHostError::Unsupported),
    }
}

#[cfg(target_os = "linux")]
fn validate_command_host_requirements(
    policy: &NativeCommandPolicy,
) -> Result<(), NativeCommandHostError> {
    policy
        .validate()
        .map_err(|_| NativeCommandHostError::Unsupported)?;
    if !command_arch_supported() {
        return Err(NativeCommandHostError::Unsupported);
    }
    if [BWRAP, PRLIMIT, KEYCTL]
        .iter()
        .any(|path| !std::path::Path::new(path).is_file())
    {
        return Err(NativeCommandHostError::Unsupported);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn command_probe_arguments(policy: &NativeCommandPolicy) -> Vec<String> {
    let mut arguments = vec![
        "-c".into(),
        concat!(
            "for vyane_program do ",
            "{ [ -f \"/usr/bin/$vyane_program\" ] && ",
            "[ -x \"/usr/bin/$vyane_program\" ]; } || ",
            "{ [ -f \"/bin/$vyane_program\" ] && ",
            "[ -x \"/bin/$vyane_program\" ]; } || exit 127; ",
            "done"
        )
        .into(),
        "vyane-command-program-probe".into(),
    ];
    let mut programs = policy
        .allow
        .iter()
        .map(|rule| rule.program.clone())
        .collect::<Vec<_>>();
    programs.sort();
    programs.dedup();
    arguments.extend(programs);
    arguments
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
    network: Option<Arc<NativeCommandNetworkPolicy>>,
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
        let args = match launcher_args(
            &request.program,
            &request.args,
            request.timeout_seconds,
            self.network.is_some(),
        ) {
            Ok(args) => args,
            Err(()) => {
                return Ok(Err(ToolError::new(
                    "run_command could not establish the process ceiling",
                )));
            }
        };
        let control = || {
            RunControl::new(
                context.cancellation_token().clone(),
                Some(Duration::from_secs(request.timeout_seconds)),
                None,
            )
        };
        let result = match self.network.as_deref() {
            None => {
                run_capture_with_pinned_limit_authorized(
                    PRLIMIT,
                    &args,
                    Some(pinned.canonical_path()),
                    Some(pinned),
                    &command_environment(),
                    control(),
                    CAPTURE_BYTES_PER_STREAM,
                    authority,
                    effect,
                    &command_seccomp_filter(false),
                )
                .await?
            }
            Some(network) => {
                run_networked_command(&args, pinned, control(), authority, effect, network).await?
            }
        };
        Ok(Ok(format_command_result(result)))
    }
}

struct CommandRequest {
    program: String,
    args: Vec<String>,
    timeout_seconds: u64,
}

#[cfg(target_os = "linux")]
async fn run_networked_command(
    args: &[String],
    pinned: &PinnedWorkdir,
    control: RunControl,
    authority: &dyn NativeExecutionAuthority,
    effect: NativeSideEffect,
    network: &NativeCommandNetworkPolicy,
) -> VyaneResult<crate::spawn::RunResult> {
    use std::os::fd::OwnedFd;

    let (broker_channel, child_channel) = std::os::unix::net::UnixStream::pair()?;
    let broker_fd: OwnedFd = broker_channel.into();
    let child_fd: OwnedFd = child_channel.into();
    let environment = command_environment();
    let seccomp_filter = command_seccomp_filter(true);
    let (control, broker_failure_cancel) = control.with_child_cancellation();
    let run = run_capture_with_pinned_limit_authorized_channel(
        PRLIMIT,
        args,
        Some(pinned.canonical_path()),
        Some(pinned),
        &environment,
        control,
        CAPTURE_BYTES_PER_STREAM,
        authority,
        effect,
        &seccomp_filter,
        child_fd,
    );
    let broker = run_network_broker(broker_fd, network, authority, effect);
    tokio::pin!(run);
    tokio::pin!(broker);
    tokio::select! {
        run_result = &mut run => run_result,
        broker_result = &mut broker => match broker_result {
            Ok(()) => run.await,
            Err(error) => {
                broker_failure_cancel.cancel();
                let _ = run.await;
                Err(error)
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
async fn run_networked_command(
    _args: &[String],
    _pinned: &PinnedWorkdir,
    _control: RunControl,
    _authority: &dyn NativeExecutionAuthority,
    _effect: NativeSideEffect,
    _network: &NativeCommandNetworkPolicy,
) -> VyaneResult<crate::spawn::RunResult> {
    Err(vyane_core::VyaneError::new(
        vyane_core::ErrorKind::Unsupported,
        "native command networking requires Linux",
    ))
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
fn sandbox_args(program: &str, args: &[String], network: bool) -> Vec<String> {
    let mut sandbox = vec![
        "--die-with-parent".into(),
        "--unshare-user".into(),
        "--unshare-pid".into(),
        "--unshare-ipc".into(),
        "--unshare-uts".into(),
        "--unshare-cgroup-try".into(),
        "--unshare-net".into(),
        "--new-session".into(),
        "--seccomp".into(),
        SECCOMP_FILTER_FD.into(),
        "--size".into(),
        COMMAND_ROOT_TMP_BYTES.to_string(),
        "--tmpfs".into(),
        "/".into(),
        "--proc".into(),
        "/proc".into(),
        "--dir".into(),
        "/dev".into(),
        "--dev-bind".into(),
        "/dev/null".into(),
        "/dev/null".into(),
        "--dev-bind".into(),
        "/dev/zero".into(),
        "/dev/zero".into(),
        "--dev-bind".into(),
        "/dev/random".into(),
        "/dev/random".into(),
        "--dev-bind".into(),
        "/dev/urandom".into(),
        "/dev/urandom".into(),
        "--symlink".into(),
        "/proc/self/fd".into(),
        "/dev/fd".into(),
        "--symlink".into(),
        "/proc/self/fd/0".into(),
        "/dev/stdin".into(),
        "--symlink".into(),
        "/proc/self/fd/1".into(),
        "/dev/stdout".into(),
        "--symlink".into(),
        "/proc/self/fd/2".into(),
        "/dev/stderr".into(),
        "--size".into(),
        COMMAND_TMP_BYTES.to_string(),
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
    if network {
        sandbox.extend(["--dir".into(), "/etc".into()]);
        if std::path::Path::new("/etc/ssl").is_dir() {
            sandbox.extend(["--dir".into(), "/etc/ssl".into()]);
        }
        for path in [
            "/etc/ssl/certs",
            "/etc/ssl/cert.pem",
            "/etc/ssl/openssl.cnf",
        ] {
            if std::path::Path::new(path).exists() {
                sandbox.extend(["--ro-bind".into(), path.into(), path.into()]);
            }
        }
        if std::path::Path::new("/etc/pki").is_dir() {
            sandbox.extend(["--dir".into(), "/etc/pki".into()]);
        }
        if std::path::Path::new("/etc/pki/tls").is_dir() {
            sandbox.extend(["--dir".into(), "/etc/pki/tls".into()]);
        }
        if std::path::Path::new("/etc/pki/ca-trust").is_dir() {
            sandbox.extend([
                "--ro-bind".into(),
                "/etc/pki/ca-trust".into(),
                "/etc/pki/ca-trust".into(),
            ]);
        }
        for path in ["/etc/pki/tls/certs", "/etc/pki/tls/cert.pem"] {
            if std::path::Path::new(path).exists() {
                sandbox.extend(["--ro-bind".into(), path.into(), path.into()]);
            }
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
    ]);
    sandbox.push("--".into());
    if network {
        sandbox.extend([
            "/usr/bin/python3".into(),
            "-c".into(),
            NETWORK_PROXY_SCRIPT.into(),
            program.into(),
        ]);
    } else {
        sandbox.push(program.into());
    }
    sandbox.extend(args.iter().cloned());
    sandbox
}

#[cfg(not(target_os = "linux"))]
fn sandbox_args(_program: &str, _args: &[String], _network: bool) -> Vec<String> {
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

fn launcher_args(
    program: &str,
    args: &[String],
    cpu_seconds: u64,
    network: bool,
) -> Result<Vec<String>, ()> {
    if !command_arch_supported() {
        return Err(());
    }
    let process_count = current_uid_thread_count()?
        .checked_add(COMMAND_PROCESS_HEADROOM)
        .filter(|count| *count <= COMMAND_PROCESS_LIMIT_MAX)
        .ok_or(())?;
    let mut launcher = vec![
        format!("--as={COMMAND_ADDRESS_SPACE_BYTES}"),
        format!("--nproc={process_count}"),
        format!("--cpu={}", cpu_seconds.max(1)),
        format!("--nofile={COMMAND_OPEN_FILES}"),
        format!("--fsize={COMMAND_FILE_SIZE_BYTES}"),
        "--core=0".into(),
        "--".into(),
        KEYCTL.into(),
        "session".into(),
        "-".into(),
        BWRAP.into(),
    ];
    launcher.extend(sandbox_args(program, args, network));
    Ok(launcher)
}

const fn command_arch_supported() -> bool {
    cfg!(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))
}

#[cfg(target_os = "linux")]
fn current_uid_thread_count() -> Result<u64, ()> {
    let uid = rustix::process::getuid().as_raw();
    let self_status = std::fs::read_to_string("/proc/self/status").map_err(|_| ())?;
    let effective_capabilities = self_status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:"))
        .and_then(|value| u64::from_str_radix(value.trim(), 16).ok())
        .ok_or(())?;
    if !nproc_limit_is_enforced(uid, effective_capabilities) {
        return Err(());
    }
    let mut total = 0u64;
    for entry in std::fs::read_dir("/proc").map_err(|_| ())? {
        let Ok(entry) = entry else {
            continue;
        };
        if !entry
            .file_name()
            .to_string_lossy()
            .bytes()
            .all(|byte| byte.is_ascii_digit())
        {
            continue;
        }
        let Ok(status) = std::fs::read_to_string(entry.path().join("status")) else {
            continue;
        };
        let owner = status
            .lines()
            .find_map(|line| line.strip_prefix("Uid:"))
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse::<u32>().ok());
        if owner != Some(uid) {
            continue;
        }
        let threads = status
            .lines()
            .find_map(|line| line.strip_prefix("Threads:"))
            .and_then(|value| value.trim().parse::<u64>().ok())
            .ok_or(())?;
        total = total.checked_add(threads).ok_or(())?;
    }
    (total > 0).then_some(total).ok_or(())
}

#[cfg(target_os = "linux")]
fn nproc_limit_is_enforced(uid: u32, effective_capabilities: u64) -> bool {
    let exempt_capabilities = (1u64 << CAP_SYS_ADMIN_BIT) | (1u64 << CAP_SYS_RESOURCE_BIT);
    uid != 0 && effective_capabilities & exempt_capabilities == 0
}

#[cfg(not(target_os = "linux"))]
fn current_uid_thread_count() -> Result<u64, ()> {
    Err(())
}

#[cfg(target_os = "linux")]
fn command_seccomp_filter(network: bool) -> Vec<u8> {
    const BPF_LD_W_ABS: u16 = 0x20;
    const BPF_JMP_JEQ_K: u16 = 0x15;
    const BPF_JMP_JSET_K: u16 = 0x45;
    const BPF_RET_K: u16 = 0x06;
    const SECCOMP_RET_KILL_PROCESS: u32 = 0x8000_0000;
    const SECCOMP_RET_ERRNO: u32 = 0x0005_0000;
    const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
    const SECCOMP_DATA_NR: u32 = 0;
    const SECCOMP_DATA_ARCH: u32 = 4;
    const SECCOMP_DATA_ARG0: u32 = 16;
    #[cfg(target_arch = "x86_64")]
    const AUDIT_ARCH_NATIVE: u32 = 0xc000_003e;
    #[cfg(target_arch = "aarch64")]
    const AUDIT_ARCH_NATIVE: u32 = 0xc000_00b7;
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    const AUDIT_ARCH_NATIVE: u32 = 0;

    let mut program = Vec::new();
    let mut instruction = |code: u16, jt: u8, jf: u8, k: u32| {
        program.extend_from_slice(&code.to_ne_bytes());
        program.push(jt);
        program.push(jf);
        program.extend_from_slice(&k.to_ne_bytes());
    };

    instruction(BPF_LD_W_ABS, 0, 0, SECCOMP_DATA_ARCH);
    instruction(BPF_JMP_JEQ_K, 1, 0, AUDIT_ARCH_NATIVE);
    instruction(BPF_RET_K, 0, 0, SECCOMP_RET_KILL_PROCESS);
    instruction(BPF_LD_W_ABS, 0, 0, SECCOMP_DATA_NR);
    #[cfg(target_arch = "x86_64")]
    {
        instruction(BPF_JMP_JSET_K, 0, 1, 0x4000_0000);
        instruction(BPF_RET_K, 0, 0, SECCOMP_RET_KILL_PROCESS);
    }
    if network {
        instruction(BPF_JMP_JEQ_K, 0, 5, libc::SYS_socket as u32);
        instruction(BPF_LD_W_ABS, 0, 0, SECCOMP_DATA_ARG0);
        instruction(BPF_JMP_JEQ_K, 2, 0, libc::AF_INET as u32);
        instruction(BPF_JMP_JEQ_K, 1, 0, libc::AF_INET6 as u32);
        instruction(BPF_RET_K, 0, 0, SECCOMP_RET_ERRNO | libc::EPERM as u32);
        instruction(BPF_RET_K, 0, 0, SECCOMP_RET_ALLOW);
    }
    let mut denied = vec![
        libc::SYS_keyctl,
        libc::SYS_add_key,
        libc::SYS_request_key,
        libc::SYS_io_uring_setup,
        libc::SYS_io_uring_enter,
        libc::SYS_io_uring_register,
        libc::SYS_memfd_create,
        libc::SYS_shmget,
        libc::SYS_msgget,
        libc::SYS_semget,
        libc::SYS_mq_open,
        libc::SYS_ptrace,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        libc::SYS_pidfd_getfd,
        libc::SYS_socketpair,
    ];
    if !network {
        denied.push(libc::SYS_socket);
    }
    for syscall in denied {
        instruction(BPF_JMP_JEQ_K, 0, 1, syscall as u32);
        instruction(BPF_RET_K, 0, 0, SECCOMP_RET_ERRNO | libc::EPERM as u32);
    }
    instruction(BPF_RET_K, 0, 0, SECCOMP_RET_ALLOW);
    program
}

#[cfg(not(target_os = "linux"))]
fn command_seccomp_filter(_network: bool) -> Vec<u8> {
    Vec::new()
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

    #[cfg(target_os = "linux")]
    #[test]
    fn process_ceiling_rejects_rlimit_nproc_exempt_identities() {
        assert!(!nproc_limit_is_enforced(0, 0));
        assert!(!nproc_limit_is_enforced(1000, 1 << CAP_SYS_ADMIN_BIT));
        assert!(!nproc_limit_is_enforced(1000, 1 << CAP_SYS_RESOURCE_BIT));
        assert!(nproc_limit_is_enforced(1000, 0));
    }

    #[test]
    fn command_architecture_support_is_explicit() {
        assert_eq!(
            command_arch_supported(),
            cfg!(all(
                target_os = "linux",
                any(target_arch = "x86_64", target_arch = "aarch64")
            ))
        );
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
