#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tempfile::tempdir;
use vyane_core::{NativeExecutionAuthority, NativeSideEffect, PinnedWorkdir};
use vyane_harness::native::{
    NativeCommandPolicy, NativeCommandRule, PermissionPolicy, ToolCall, ToolContext,
    ToolInvocationStatus, command_permission_policy, command_tool_registry, validate_command_host,
};

#[derive(Default)]
struct RecordingAuthority {
    effects: Mutex<Vec<NativeSideEffect>>,
    fail_at: Option<usize>,
}

#[async_trait]
impl NativeExecutionAuthority for RecordingAuthority {
    async fn revalidate(&self, effect: NativeSideEffect) -> vyane_core::Result<()> {
        self.effects.lock().expect("effects").push(effect);
        if self.fail_at == Some(self.effects.lock().expect("effects after append").len()) {
            return Err(vyane_core::VyaneError::new(
                vyane_core::ErrorKind::Auth,
                "test authority revoked",
            ));
        }
        Ok(())
    }
}

fn policy(rules: &[(&str, &[&str])]) -> NativeCommandPolicy {
    NativeCommandPolicy {
        allow: rules
            .iter()
            .map(|(program, prefix)| NativeCommandRule {
                program: (*program).into(),
                args_prefix: prefix.iter().map(|argument| (*argument).into()).collect(),
            })
            .collect(),
        max_seconds: 10,
    }
}

fn call(program: &str, args: &[&str]) -> ToolCall {
    ToolCall {
        id: "call-command".into(),
        name: "run_command".into(),
        arguments: BTreeMap::from([
            ("program".into(), Value::String(program.into())),
            (
                "args".into(),
                Value::Array(
                    args.iter()
                        .map(|argument| Value::String((*argument).into()))
                        .collect(),
                ),
            ),
        ]),
    }
}

async fn execute(root: &std::path::Path, policy: NativeCommandPolicy, call: ToolCall) -> String {
    let registry = command_tool_registry(policy).expect("command registry");
    let permissions =
        command_permission_policy(PermissionPolicy::deny_by_default()).expect("permissions");
    let context =
        ToolContext::from_pinned_workdir(PinnedWorkdir::open(root).expect("pin command workspace"))
            .with_timeout(Duration::from_secs(15));
    let invocation = registry
        .execute_authorized(
            call,
            &context,
            &permissions,
            &RecordingAuthority::default(),
            1,
            1,
        )
        .await
        .expect("authorized command");
    assert_eq!(invocation.status, ToolInvocationStatus::Executed);
    invocation.output
}

#[tokio::test]
async fn revocation_at_the_tool_owned_spawn_check_prevents_execution() {
    let root = tempdir().expect("workspace");
    let registry = command_tool_registry(policy(&[("printf", &[])])).expect("command registry");
    let permissions =
        command_permission_policy(PermissionPolicy::deny_by_default()).expect("permissions");
    let context = ToolContext::from_pinned_workdir(
        PinnedWorkdir::open(root.path()).expect("pin command workspace"),
    );
    let authority = RecordingAuthority {
        effects: Mutex::new(Vec::new()),
        fail_at: Some(2),
    };
    let result = registry
        .execute_authorized(
            call("printf", &["must-not-run"]),
            &context,
            &permissions,
            &authority,
            3,
            2,
        )
        .await;
    assert!(result.is_err());
    assert_eq!(authority.effects.lock().expect("effects").len(), 2);
}

#[tokio::test]
async fn revocation_at_the_physical_spawn_check_prevents_execution() {
    let root = tempdir().expect("workspace");
    let registry = command_tool_registry(policy(&[("printf", &[])])).expect("command registry");
    let permissions =
        command_permission_policy(PermissionPolicy::deny_by_default()).expect("permissions");
    let context = ToolContext::from_pinned_workdir(
        PinnedWorkdir::open(root.path()).expect("pin command workspace"),
    );
    let authority = RecordingAuthority {
        effects: Mutex::new(Vec::new()),
        fail_at: Some(3),
    };
    let result = registry
        .execute_authorized(
            call("printf", &["must-not-run"]),
            &context,
            &permissions,
            &authority,
            3,
            2,
        )
        .await;
    assert!(result.is_err());
    assert_eq!(authority.effects.lock().expect("effects").len(), 3);
}

#[tokio::test]
async fn host_probe_and_descriptor_bound_read_succeed() {
    let root = tempdir().expect("workspace");
    std::fs::write(root.path().join("visible.txt"), "workspace-data").expect("fixture");
    let policy = policy(&[("cat", &["visible.txt"])]);
    validate_command_host(&PinnedWorkdir::open(root.path()).expect("pin"), &policy)
        .await
        .expect("supported command host");

    let output = execute(root.path(), policy, call("cat", &["visible.txt"])).await;
    assert!(output.contains("exit_code: 0"), "{output}");
    assert!(output.contains("workspace-data"), "{output}");
}

#[tokio::test]
async fn workspace_is_read_only_and_host_etc_is_not_mounted() {
    let root = tempdir().expect("workspace");
    let write_output = execute(
        root.path(),
        policy(&[("touch", &[])]),
        call("touch", &["blocked.txt"]),
    )
    .await;
    assert!(!write_output.contains("exit_code: 0"), "{write_output}");
    assert!(!root.path().join("blocked.txt").exists());

    let host_output = execute(
        root.path(),
        policy(&[("cat", &[])]),
        call("cat", &["/etc/passwd"]),
    )
    .await;
    assert!(!host_output.contains("exit_code: 0"), "{host_output}");
    assert!(!host_output.contains("root:x:"), "{host_output}");
}

#[tokio::test]
async fn network_namespace_has_no_loopback_connectivity() {
    let root = tempdir().expect("workspace");
    let source = "import socket; s=socket.socket(); s.settimeout(0.2); s.connect(('127.0.0.1', 9))";
    let output = execute(
        root.path(),
        policy(&[("python3", &["-c"])]),
        call("python3", &["-c", source]),
    )
    .await;
    assert!(!output.contains("exit_code: 0"), "{output}");
    assert!(
        !output.contains("bwrap:"),
        "the sandbox itself must start before the network probe: {output}"
    );
    assert!(
        output.contains("Traceback") || output.contains("Network is unreachable"),
        "{output}"
    );
}

#[tokio::test]
async fn unix_sockets_and_kernel_keyring_calls_are_blocked() {
    use std::os::unix::net::UnixListener;

    let root = tempdir().expect("workspace");
    let socket_path = root.path().join("service.sock");
    let listener = UnixListener::bind(&socket_path).expect("host unix listener");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let source = "import socket; s=socket.socket(socket.AF_UNIX); s.connect('service.sock')";
    let output = execute(
        root.path(),
        policy(&[("python3", &["-c"])]),
        call("python3", &["-c", source]),
    )
    .await;
    assert!(!output.contains("exit_code: 0"), "{output}");
    assert!(
        output.contains("PermissionError") || output.contains("Operation not permitted"),
        "{output}"
    );
    assert!(
        listener.accept().is_err(),
        "sandbox reached the host unix socket"
    );

    let output = execute(
        root.path(),
        policy(&[("keyctl", &["show"])]),
        call("keyctl", &["show"]),
    )
    .await;
    assert!(!output.contains("exit_code: 0"), "{output}");
    assert!(output.contains("Operation not permitted"), "{output}");
}

#[tokio::test]
async fn command_process_has_hard_resource_ceilings() {
    let root = tempdir().expect("workspace");
    let source = concat!(
        "import resource; ",
        "print(resource.getrlimit(resource.RLIMIT_AS)); ",
        "nproc=resource.getrlimit(resource.RLIMIT_NPROC); ",
        "assert nproc[0] == nproc[1] and 0 < nproc[0] <= 4096; print(nproc); ",
        "print(resource.getrlimit(resource.RLIMIT_NOFILE)); ",
        "print(resource.getrlimit(resource.RLIMIT_FSIZE)); ",
        "print(resource.getrlimit(resource.RLIMIT_CORE))"
    );
    let output = execute(
        root.path(),
        policy(&[("python3", &["-c"])]),
        call("python3", &["-c", source]),
    )
    .await;
    assert!(output.contains("exit_code: 0"), "{output}");
    assert!(output.contains("(2147483648, 2147483648)"), "{output}");
    assert!(output.contains("(256, 256)"), "{output}");
    assert!(output.contains("(67108864, 67108864)"), "{output}");
    assert!(output.contains("(0, 0)"), "{output}");
}

#[tokio::test]
async fn timeout_kills_the_sandbox_and_output_is_bounded() {
    let root = tempdir().expect("workspace");
    let output = execute(
        root.path(),
        NativeCommandPolicy {
            allow: vec![NativeCommandRule {
                program: "python3".into(),
                args_prefix: vec!["-c".into()],
            }],
            max_seconds: 1,
        },
        call(
            "python3",
            &[
                "-c",
                "import subprocess,time; subprocess.Popen(['sleep','30']); time.sleep(30)",
            ],
        ),
    )
    .await;
    assert!(output.contains("status: timed_out"), "{output}");

    let output = execute(
        root.path(),
        policy(&[("python3", &["-c"])]),
        call("python3", &["-c", "print('x' * 100000)"]),
    )
    .await;
    assert!(output.contains("[stdout truncated]"), "{output}");
    assert!(output.chars().count() <= 30_000);
}

#[test]
fn policy_json_is_closed_and_round_trips() {
    let value = json!({
        "allow": [
            { "program": "git", "args_prefix": ["status"] }
        ],
        "max_seconds": 120
    });
    let policy: NativeCommandPolicy = serde_json::from_value(value.clone()).expect("policy");
    assert_eq!(serde_json::to_value(policy).expect("json"), value);
    assert!(
        serde_json::from_value::<NativeCommandPolicy>(json!({
            "allow": [{ "program": "git" }],
            "unknown": true
        }))
        .is_err()
    );
}
