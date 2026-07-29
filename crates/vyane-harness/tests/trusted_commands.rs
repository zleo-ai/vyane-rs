#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tempfile::tempdir;
use vyane_core::{NativeExecutionAuthority, NativeSideEffect, PinnedWorkdir};
use vyane_harness::native::{
    NativeCommandNetworkPolicy, NativeCommandNetworkRoute, NativeCommandNetworkRule,
    NativeCommandPolicy, NativeCommandRule, PermissionPolicy, ToolCall, ToolContext,
    ToolInvocationStatus, command_permission_policy, command_tool_registry, prepare_command_mounts,
    register_command_tool_with_network, validate_command_host, validate_command_network_host,
    workspace_tool_registry_with_policy,
};

static COMMAND_TEST_GATE: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

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
        writable_roots: Vec::new(),
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
    let _permit = COMMAND_TEST_GATE
        .acquire()
        .await
        .expect("command test gate");
    let pinned = PinnedWorkdir::open(root).expect("pin command workspace");
    let mounts = (!policy.writable_roots.is_empty())
        .then(|| prepare_command_mounts(&pinned, &policy).expect("admit command mounts"));
    let registry = command_tool_registry(policy).expect("command registry");
    let permissions =
        command_permission_policy(PermissionPolicy::deny_by_default()).expect("permissions");
    let mut context =
        ToolContext::from_pinned_workdir(pinned).with_timeout(Duration::from_secs(15));
    if let Some(mounts) = mounts {
        context = context.with_command_mounts(mounts);
    }
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

fn network_policy() -> NativeCommandNetworkPolicy {
    NativeCommandNetworkPolicy {
        allow: vec![NativeCommandNetworkRule {
            host: "example.com".into(),
            ports: vec![443],
        }],
        route: NativeCommandNetworkRoute::Direct,
        max_connections: 2,
        max_bytes: 1024 * 1024,
        connect_timeout_seconds: 1,
    }
}

async fn execute_network(
    root: &std::path::Path,
    authority: &RecordingAuthority,
    call: ToolCall,
) -> vyane_core::Result<String> {
    execute_network_with_route(root, authority, call, NativeCommandNetworkRoute::Direct).await
}

async fn execute_network_with_route(
    root: &std::path::Path,
    authority: &RecordingAuthority,
    call: ToolCall,
    route: NativeCommandNetworkRoute,
) -> vyane_core::Result<String> {
    let _permit = COMMAND_TEST_GATE
        .acquire()
        .await
        .expect("command test gate");
    let mut network = network_policy();
    network.route = route;
    let mut registry =
        workspace_tool_registry_with_policy(Default::default(), None).expect("workspace registry");
    register_command_tool_with_network(&mut registry, policy(&[("python3", &["-c"])]), network)
        .expect("network command registry");
    let permissions =
        command_permission_policy(PermissionPolicy::deny_by_default()).expect("permissions");
    let context =
        ToolContext::from_pinned_workdir(PinnedWorkdir::open(root).expect("pin command workspace"))
            .with_timeout(Duration::from_secs(15));
    let invocation = registry
        .execute_authorized(call, &context, &permissions, authority, 1, 1)
        .await?;
    assert_eq!(invocation.status, ToolInvocationStatus::Executed);
    Ok(invocation.output)
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
async fn host_probe_rejects_allowlisted_program_missing_from_sandbox() {
    let root = tempdir().expect("workspace");
    let missing = policy(&[("vyane-program-that-does-not-exist", &[])]);
    assert!(
        validate_command_host(&PinnedWorkdir::open(root.path()).expect("pin"), &missing)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn network_host_probe_runs_the_proxy_launcher_before_admission() {
    let root = tempdir().expect("workspace");
    let pinned = PinnedWorkdir::open(root.path()).expect("pin");
    validate_command_network_host(&pinned, &policy(&[("python3", &["-c"])]), &network_policy())
        .await
        .expect("supported network command host");

    let missing = policy(&[("vyane-program-that-does-not-exist", &[])]);
    assert!(
        validate_command_network_host(&pinned, &missing, &network_policy())
            .await
            .is_err()
    );
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
async fn only_explicit_descriptor_bound_roots_are_writable() {
    let root = tempdir().expect("workspace");
    std::fs::create_dir(root.path().join("src")).expect("writable root");
    std::fs::create_dir(root.path().join(".git")).expect("protected sibling");
    let mut writable = policy(&[("touch", &[]), ("python3", &["-c"])]);
    writable.writable_roots = vec!["src".into()];

    validate_command_host(
        &PinnedWorkdir::open(root.path()).expect("pin command workspace"),
        &writable,
    )
    .await
    .expect("supported writable-root command host");

    let allowed = execute(
        root.path(),
        writable.clone(),
        call("touch", &["src/allowed.txt"]),
    )
    .await;
    assert!(allowed.contains("exit_code: 0"), "{allowed}");
    assert!(root.path().join("src/allowed.txt").is_file());

    let root_denied = execute(
        root.path(),
        writable.clone(),
        call("touch", &["blocked.txt"]),
    )
    .await;
    assert!(!root_denied.contains("exit_code: 0"), "{root_denied}");
    assert!(!root.path().join("blocked.txt").exists());

    let metadata_denied = execute(root.path(), writable, call("touch", &[".git/blocked"])).await;
    assert!(
        !metadata_denied.contains("exit_code: 0"),
        "{metadata_denied}"
    );
    assert!(!root.path().join(".git/blocked").exists());
}

#[tokio::test]
async fn regular_file_writable_roots_are_rejected() {
    let root = tempdir().expect("workspace");
    std::fs::write(root.path().join("manifest.txt"), "original").expect("file root");
    let mut writable = policy(&[("python3", &["-c"])]);
    writable.writable_roots = vec!["manifest.txt".into()];
    let pinned = PinnedWorkdir::open(root.path()).expect("pin command workspace");

    assert!(prepare_command_mounts(&pinned, &writable).is_err());
}

#[tokio::test]
async fn hard_links_cannot_escape_a_writable_root() {
    let root = tempdir().expect("workspace");
    std::fs::create_dir(root.path().join("src")).expect("writable root");
    std::fs::create_dir(root.path().join(".git")).expect("protected sibling");
    std::fs::write(root.path().join(".git/config"), "protected").expect("protected file");
    std::fs::hard_link(
        root.path().join(".git/config"),
        root.path().join("src/config-alias"),
    )
    .expect("hard-link fixture");
    let mut writable = policy(&[("touch", &[])]);
    writable.writable_roots = vec!["src".into()];
    let pinned = PinnedWorkdir::open(root.path()).expect("pin command workspace");
    assert!(prepare_command_mounts(&pinned, &writable).is_err());

    std::fs::remove_file(root.path().join("src/config-alias")).expect("remove fixture alias");
    let mut link_policy = policy(&[("ln", &[])]);
    link_policy.writable_roots = vec!["src".into()];
    let output = execute(
        root.path(),
        link_policy,
        call("ln", &[".git/config", "src/config-alias"]),
    )
    .await;
    assert!(!output.contains("exit_code: 0"), "{output}");
    assert!(!root.path().join("src/config-alias").exists());
    assert_eq!(
        std::fs::read_to_string(root.path().join(".git/config")).expect("protected file"),
        "protected"
    );
}

#[tokio::test]
async fn admitted_writable_root_handle_survives_path_replacement() {
    let _permit = COMMAND_TEST_GATE
        .acquire()
        .await
        .expect("command test gate");
    let root = tempdir().expect("workspace");
    let admitted = root.path().join("src");
    let moved = root.path().join("moved-src");
    std::fs::create_dir(&admitted).expect("writable root");
    let pinned = PinnedWorkdir::open(root.path()).expect("pin command workspace");
    let mut writable = policy(&[("touch", &[])]);
    writable.writable_roots = vec!["src".into()];
    let mounts = prepare_command_mounts(&pinned, &writable).expect("admit command mounts");
    std::fs::rename(&admitted, &moved).expect("move admitted root");
    std::fs::create_dir(&admitted).expect("replacement root");

    let registry = command_tool_registry(writable).expect("command registry");
    let permissions =
        command_permission_policy(PermissionPolicy::deny_by_default()).expect("permissions");
    let context = ToolContext::from_pinned_workdir(pinned)
        .with_command_mounts(mounts)
        .with_timeout(Duration::from_secs(15));
    let invocation = registry
        .execute_authorized(
            call("touch", &["src/retained.txt"]),
            &context,
            &permissions,
            &RecordingAuthority::default(),
            1,
            1,
        )
        .await
        .expect("authorized command");
    assert_eq!(invocation.status, ToolInvocationStatus::Executed);
    assert!(
        invocation.output.contains("exit_code: 0"),
        "{}",
        invocation.output
    );
    assert!(moved.join("retained.txt").is_file());
    assert!(!admitted.join("retained.txt").exists());
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
async fn network_mode_exposes_only_the_policy_proxy_and_denies_unlisted_hosts() {
    let root = tempdir().expect("workspace");
    let source = concat!(
        "import socket; ",
        "s=socket.create_connection(('127.0.0.1',3128),1); ",
        "s.sendall(b'CONNECT 127.0.0.1:443 HTTP/1.1\\r\\n\\r\\n'); ",
        "data=s.recv(128); assert b'403 Forbidden' in data, data"
    );
    let output = execute_network(
        root.path(),
        &RecordingAuthority::default(),
        call("python3", &["-c", source]),
    )
    .await
    .expect("network command");
    assert!(output.contains("exit_code: 0"), "{output}");
}

#[tokio::test]
async fn network_mode_still_cannot_connect_directly_outside_the_namespace() {
    let root = tempdir().expect("workspace");
    let source = concat!(
        "import socket; s=socket.socket(); s.settimeout(.2); ",
        "ok=False\n",
        "try: s.connect(('1.1.1.1',443))\n",
        "except OSError: ok=True\n",
        "assert ok"
    );
    let output = execute_network(
        root.path(),
        &RecordingAuthority::default(),
        call("python3", &["-c", source]),
    )
    .await
    .expect("network command");
    assert!(output.contains("exit_code: 0"), "{output}");
}

#[tokio::test]
async fn untrusted_command_cannot_reopen_the_broker_descriptor_from_the_proxy() {
    let root = tempdir().expect("workspace");
    let source = concat!(
        "import os; path=f'/proc/{os.getppid()}/fd/5'; denied=False\n",
        "try: os.open(path,os.O_RDWR)\n",
        "except OSError: denied=True\n",
        "assert denied"
    );
    let output = execute_network(
        root.path(),
        &RecordingAuthority::default(),
        call("python3", &["-c", source]),
    )
    .await
    .expect("network command");
    assert!(output.contains("exit_code: 0"), "{output}");
}

#[tokio::test]
#[ignore = "requires public DNS and Internet"]
async fn allowed_https_host_is_reachable_through_the_policy_broker() {
    let root = tempdir().expect("workspace");
    let source = concat!(
        "import urllib.request; ",
        "response=urllib.request.urlopen('https://example.com',timeout=5); ",
        "print(response.status)"
    );
    let output = execute_network_with_route(
        root.path(),
        &RecordingAuthority::default(),
        call("python3", &["-c", source]),
        NativeCommandNetworkRoute::EnvironmentProxy,
    )
    .await
    .expect("network command");
    assert!(output.contains("exit_code: 0"), "{output}");
    assert!(output.contains("200"), "{output}");
}

#[tokio::test]
#[ignore = "requires the configured host HTTPS proxy"]
async fn rejected_tls_tunnel_does_not_poison_a_later_allowed_request() {
    let root = tempdir().expect("workspace");
    let source = concat!(
        "import socket,ssl,urllib.request\n",
        "s=socket.create_connection(('127.0.0.1',3128),2)\n",
        "s.sendall(b'CONNECT example.com:443 HTTP/1.1\\r\\n\\r\\n')\n",
        "assert b' 200 ' in s.recv(4096)\n",
        "try: ssl.create_default_context().wrap_socket(s,server_hostname='wrong.example.com')\n",
        "except Exception: s.close()\n",
        "response=urllib.request.urlopen('https://example.com',timeout=5)\n",
        "print(response.status)\n"
    );
    let output = execute_network_with_route(
        root.path(),
        &RecordingAuthority::default(),
        call("python3", &["-c", source]),
        NativeCommandNetworkRoute::EnvironmentProxy,
    )
    .await
    .expect("network command");
    assert!(output.contains("exit_code: 0"), "{output}");
    assert!(output.contains("200"), "{output}");
}

#[tokio::test]
#[ignore = "requires the configured host HTTPS proxy"]
async fn reset_established_tunnel_does_not_poison_a_later_allowed_request() {
    let root = tempdir().expect("workspace");
    let source = concat!(
        "import socket,ssl,struct,urllib.request\n",
        "s=socket.create_connection(('127.0.0.1',3128),2)\n",
        "s.sendall(b'CONNECT example.com:443 HTTP/1.1\\r\\n\\r\\n')\n",
        "assert b' 200 ' in s.recv(4096)\n",
        "tls=ssl.create_default_context().wrap_socket(s,server_hostname='example.com')\n",
        "fd=tls.detach()\n",
        "raw=socket.socket(fileno=fd)\n",
        "raw.setsockopt(socket.SOL_SOCKET,socket.SO_LINGER,struct.pack('ii',1,0))\n",
        "raw.close()\n",
        "response=urllib.request.urlopen('https://example.com',timeout=5)\n",
        "print(response.status)\n"
    );
    let output = execute_network_with_route(
        root.path(),
        &RecordingAuthority::default(),
        call("python3", &["-c", source]),
        NativeCommandNetworkRoute::EnvironmentProxy,
    )
    .await
    .expect("network command");
    assert!(output.contains("exit_code: 0"), "{output}");
    assert!(output.contains("200"), "{output}");
}

#[tokio::test]
#[ignore = "requires the configured host HTTPS proxy"]
async fn environment_proxy_connect_revalidates_authority_at_the_physical_attempt() {
    let root = tempdir().expect("workspace");
    let authority = RecordingAuthority {
        effects: Mutex::new(Vec::new()),
        fail_at: Some(6),
    };
    let source = concat!(
        "import urllib.request; ",
        "urllib.request.urlopen('https://example.com',timeout=5)"
    );
    let result = execute_network_with_route(
        root.path(),
        &authority,
        call("python3", &["-c", source]),
        NativeCommandNetworkRoute::EnvironmentProxy,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(authority.effects.lock().expect("effects").len(), 6);
}

#[tokio::test]
async fn command_network_revalidates_live_authority_before_connecting() {
    let root = tempdir().expect("workspace");
    let authority = RecordingAuthority {
        effects: Mutex::new(Vec::new()),
        fail_at: Some(4),
    };
    let source = concat!(
        "import socket; ",
        "s=socket.create_connection(('127.0.0.1',3128),1); ",
        "s.sendall(b'CONNECT example.com:443 HTTP/1.1\\r\\n\\r\\n'); ",
        "s.recv(128)"
    );
    let result = execute_network(root.path(), &authority, call("python3", &["-c", source])).await;
    assert!(result.is_err());
    assert_eq!(authority.effects.lock().expect("effects").len(), 4);
}

#[tokio::test]
async fn command_timeout_is_not_extended_by_an_idle_network_broker() {
    let root = tempdir().expect("workspace");
    let mut invocation = call("python3", &["-c", "import time; time.sleep(30)"]);
    invocation
        .arguments
        .insert("timeout_seconds".into(), Value::from(1));
    let started = std::time::Instant::now();
    let output = execute_network(root.path(), &RecordingAuthority::default(), invocation)
        .await
        .expect("network command timeout");
    assert!(output.contains("status: timed_out"), "{output}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "network broker extended the command timeout: {:?}",
        started.elapsed()
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
        "import os,resource; ",
        "print(resource.getrlimit(resource.RLIMIT_AS)); ",
        "nproc=resource.getrlimit(resource.RLIMIT_NPROC); ",
        "assert nproc[0] == nproc[1] and 0 < nproc[0] <= 4096; print(nproc); ",
        "print(resource.getrlimit(resource.RLIMIT_NOFILE)); ",
        "print(resource.getrlimit(resource.RLIMIT_FSIZE)); ",
        "print(resource.getrlimit(resource.RLIMIT_CORE)); ",
        "tmp=os.statvfs('/tmp'); ",
        "assert tmp.f_blocks * tmp.f_frsize <= 67108864; ",
        "root=os.statvfs('/'); ",
        "assert root.f_blocks * root.f_frsize <= 16777216; ",
        "assert not os.path.exists('/dev/shm')"
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
            writable_roots: Vec::new(),
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

#[test]
fn command_network_policy_json_is_closed_and_round_trips() {
    let value = json!({
        "allow": [
            { "host": "crates.io", "ports": [443] },
            { "host": "*.github.com", "ports": [443] }
        ],
        "route": "direct",
        "max_connections": 4,
        "max_bytes": 1048576,
        "connect_timeout_seconds": 5
    });
    let policy: NativeCommandNetworkPolicy =
        serde_json::from_value(value.clone()).expect("network policy");
    policy.validate().expect("valid network policy");
    assert_eq!(serde_json::to_value(policy).expect("json"), value);
    assert!(
        serde_json::from_value::<NativeCommandNetworkPolicy>(json!({
            "allow": [{ "host": "crates.io", "ports": [443] }],
            "unknown": true
        }))
        .is_err()
    );
}
