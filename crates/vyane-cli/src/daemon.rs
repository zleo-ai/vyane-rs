//! Authenticated loopback lifecycle for the resident workflow supervisor.

use std::future::IntoFuture as _;
use std::io::{Read as _, Write as _};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{ExitCode, Stdio};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use vyane_service::{StoragePaths, VyaneService};
use vyane_workflow::WORKFLOW_SOURCE_MAX_TOTAL_BYTES;

use crate::cli::{DaemonRunArgs, DaemonStartArgs, DaemonStatusArgs};
use crate::daemon_workflow::{DaemonWorkflowSupervisor, WORKFLOW_VARS_MAX_TOTAL_BYTES};
use crate::supervisor::{PreparedShutdownSignal, acquire_task_supervisor_lock};
use crate::task::proc::{
    IdentityCheck, SIGKILL, SIGTERM, pgid_of, process_birth_fingerprint, process_start_time,
    signal_process, spawn_tokio_in_session, verify_controller_identity,
};

const DAEMON_DESCRIPTOR_SCHEMA: u32 = 1;
const DAEMON_DESCRIPTOR_FILE: &str = "daemon.json";
const DAEMON_TOKEN_FILE: &str = "daemon.token";
const DAEMON_LOG_FILE: &str = "daemon.log";
const DAEMON_CONTROL_LOCK_FILE: &str = "daemon-control.lock";
const SUPERVISOR_LOCK_FILE: &str = "task-supervisor.lock";
const MAX_DESCRIPTOR_BYTES: usize = 16 * 1024;
const MAX_TOKEN_BYTES: usize = 128;
const HEALTH_BODY_LIMIT: usize = 16 * 1024;
const HEALTH_TIMEOUT: Duration = Duration::from_secs(2);
const START_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_DRAIN_BUDGET: Duration = Duration::from_secs(10);
// The HTTP and workflow supervisor cooperative phase budgets total 68 seconds:
// 10 seconds to close in-flight requests, then 58 seconds for initializer,
// execution, controller, watcher and SQLite quiescence. Leave scheduling
// margin before an external stop treats a safety fallback as wedged.
const TERM_GRACE: Duration = Duration::from_secs(75);
const KILL_GRACE: Duration = Duration::from_secs(2);
const CONTROL_POLL: Duration = Duration::from_millis(50);
const CONTROL_LOCK_BUDGET: Duration = Duration::from_millis(500);
const CONTROL_LOCK_POLL: Duration = Duration::from_millis(10);
// JSON may escape each source byte as `\u00xx`. Leave additional structural
// overhead so every semantically valid source bundle fits under one fixed
// transport limit.
const DAEMON_BODY_LIMIT: usize =
    (WORKFLOW_SOURCE_MAX_TOTAL_BYTES + WORKFLOW_VARS_MAX_TOTAL_BYTES) * 6 + 1024 * 1024;

#[derive(Debug, Clone)]
struct DaemonPaths {
    data_dir: PathBuf,
}

impl DaemonPaths {
    fn from_storage(storage: &StoragePaths) -> Self {
        Self {
            data_dir: storage.data_dir.clone(),
        }
    }

    fn descriptor(&self) -> PathBuf {
        self.data_dir.join(DAEMON_DESCRIPTOR_FILE)
    }

    fn token(&self) -> PathBuf {
        self.data_dir.join(DAEMON_TOKEN_FILE)
    }

    fn log(&self) -> PathBuf {
        self.data_dir.join(DAEMON_LOG_FILE)
    }

    fn control_lock(&self) -> PathBuf {
        self.data_dir.join(DAEMON_CONTROL_LOCK_FILE)
    }

    fn supervisor_lock(&self) -> PathBuf {
        self.data_dir.join(SUPERVISOR_LOCK_FILE)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DaemonDescriptor {
    schema: u32,
    instance_id: String,
    pid: i32,
    pgid: i32,
    started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    birth_fingerprint: Option<String>,
    addr: SocketAddr,
}

/// Minimal verified control material consumed by local daemon clients.
/// Process-birth fields stay private to this module so every client shares one
/// descriptor parser, permission check, lock, and identity decision.
pub(crate) struct DaemonClientControl {
    pub(crate) addr: SocketAddr,
    pub(crate) instance_id: String,
    pub(crate) token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HealthResponse {
    status: String,
    instance_id: String,
}

#[derive(Debug, Serialize)]
struct DaemonStatusView<'a> {
    status: &'static str,
    instance_id: &'a str,
    pid: i32,
    addr: SocketAddr,
}

#[derive(Clone)]
pub(crate) struct DaemonHttpState {
    pub(crate) instance_id: Arc<str>,
    pub(crate) workflows: DaemonWorkflowSupervisor,
}

#[derive(Clone)]
struct BearerToken(Arc<str>);

struct StartingDaemon {
    child: Option<tokio::process::Child>,
}

impl StartingDaemon {
    fn new(child: tokio::process::Child) -> Self {
        Self { child: Some(child) }
    }

    fn child_mut(&mut self) -> Result<&mut tokio::process::Child> {
        self.child
            .as_mut()
            .context("starting daemon child was already disarmed")
    }

    async fn kill_and_reap(&mut self) -> Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        let _ = child.start_kill();
        child
            .wait()
            .await
            .context("reap daemon after failed startup")?;
        Ok(())
    }

    fn disarm(mut self) {
        drop(self.child.take());
    }
}

impl Drop for StartingDaemon {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            // Cancellation cannot await a reap, but it must not leave the
            // not-yet-ready daemon running. Tokio queues the killed child for
            // best-effort orphan reaping when its handle is dropped.
            let _ = child.start_kill();
        }
    }
}

pub(crate) async fn run_daemon(
    config_path: Option<PathBuf>,
    args: DaemonRunArgs,
) -> Result<ExitCode> {
    let requested = parse_loopback_addr(&args.addr)?;
    let service = Arc::new(VyaneService::load(config_path.as_deref())?);
    let paths = DaemonPaths::from_storage(service.storage_paths());
    let listener = tokio::net::TcpListener::bind(requested)
        .await
        .with_context(|| format!("bind daemon listener {requested}"))?;
    let addr = listener
        .local_addr()
        .context("read daemon listen address")?;

    // Binding happens before ownership/recovery. A port conflict must never
    // mutate another supervisor's task state or control files.
    let _supervisor_lock = acquire_task_supervisor_lock(&paths.supervisor_lock())?;
    let token = generate_bearer_token()?;
    let descriptor = current_descriptor(addr)?;
    let workflows =
        DaemonWorkflowSupervisor::open(Arc::clone(&service), descriptor.instance_id.clone())
            .await
            .context("open daemon workflow supervisor")?;
    let recovered = workflows
        .recover_interrupted()
        .await
        .context("recover interrupted daemon workflows")?;
    if recovered > 0 {
        tracing::warn!(recovered, "marked abandoned daemon workflows interrupted");
    }
    let app = daemon_router(&descriptor.instance_id, &token, workflows.clone());
    let shutdown = PreparedShutdownSignal::install()
        .context("install daemon shutdown handlers before control publication")?;

    publish_control(&paths, &token, &descriptor)?;

    eprintln!("vyane daemon listening on {addr}");
    let (signal_seen_tx, signal_seen_rx) = tokio::sync::oneshot::channel();
    let shutdown_workflows = workflows.clone();
    let graceful_shutdown = async move {
        shutdown.wait().await;
        shutdown_workflows.begin_shutdown();
        let _ = signal_seen_tx.send(());
    };
    let serve_result = {
        let serve = axum::serve(listener, app)
            .with_graceful_shutdown(graceful_shutdown)
            .into_future();
        tokio::pin!(serve);
        tokio::select! {
            result = &mut serve => result,
            signal_seen = signal_seen_rx => {
                match signal_seen {
                    Ok(()) => match tokio::time::timeout(HTTP_DRAIN_BUDGET, &mut serve).await {
                        Ok(result) => result,
                        Err(_) => {
                            tracing::warn!(
                                budget_seconds = HTTP_DRAIN_BUDGET.as_secs(),
                                "forcing close of in-flight daemon HTTP requests"
                            );
                            Ok(())
                        }
                    },
                    Err(_) => (&mut serve).await,
                }
            }
        }
    };
    // Stop advertising the endpoint as soon as its listener closes. The
    // supervisor lock remains held until all admitted work has drained.
    remove_control_if_matches(&paths, &descriptor.instance_id);
    let drain_result = workflows.shutdown_and_drain().await;
    serve_result.with_context(|| format!("serve daemon on {addr}"))?;
    drain_result.context("drain daemon workflows during shutdown")?;
    Ok(ExitCode::SUCCESS)
}

pub(crate) async fn start_daemon(
    config_path: Option<PathBuf>,
    args: DaemonStartArgs,
) -> Result<ExitCode> {
    let requested = parse_loopback_addr(&args.addr)?;
    let storage = StoragePaths::resolve()?;
    let paths = DaemonPaths::from_storage(&storage);
    secure_directory(&paths.data_dir)?;

    if let Some((existing, token)) = read_live_control(&paths)? {
        authenticated_health(&existing, &token)
            .await
            .with_context(|| {
                format!(
                    "recorded daemon {} is alive but its authenticated health check failed",
                    existing.pid
                )
            })?;
        println!("vyane daemon already running at {}", existing.addr);
        return Ok(ExitCode::SUCCESS);
    }

    let executable = std::env::current_exe().context("resolve current vyane executable")?;
    let log = open_private_log(&paths.log())?;
    let stderr = log.try_clone().context("clone daemon log handle")?;
    let mut command = Command::new(executable);
    if let Some(config_path) = config_path {
        command.arg("--config").arg(config_path);
    }
    command
        .arg("daemon")
        .arg("run")
        .arg("--addr")
        .arg(requested.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr));
    // The resident supervisor must outlive its launcher and own an isolated
    // process group. A new POSIX session preserves that daemon architecture;
    // a group-only spawn would remain attached to the launcher's session.
    let child = spawn_tokio_in_session(command).context("spawn resident vyane daemon")?;
    let mut starting = StartingDaemon::new(child);
    let readiness: Result<(SocketAddr, i32)> = async {
        let child_pid = starting
            .child_mut()?
            .id()
            .and_then(|pid| i32::try_from(pid).ok())
            .context("spawned daemon did not expose a valid pid")?;
        let deadline = tokio::time::Instant::now() + START_TIMEOUT;
        loop {
            if let Some(status) = starting
                .child_mut()?
                .try_wait()
                .context("poll starting daemon")?
            {
                bail!(
                    "vyane daemon exited during startup with {status}; see {}",
                    paths.log().display()
                );
            }
            if let Some((descriptor, Some(token))) = read_control_optional(&paths)? {
                if descriptor.pid == child_pid
                    && matches!(
                        classify_descriptor_identity(&descriptor),
                        DescriptorIdentity::Exact
                    )
                    && authenticated_health(&descriptor, &token).await.is_ok()
                {
                    break Ok((descriptor.addr, child_pid));
                }
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "timed out waiting for daemon readiness; see {}",
                    paths.log().display()
                );
            }
            tokio::time::sleep(CONTROL_POLL).await;
        }
    }
    .await;

    match readiness {
        Ok((addr, child_pid)) => {
            starting.disarm();
            println!("vyane daemon started at {addr} (pid {child_pid})");
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            if let Err(cleanup_error) = starting.kill_and_reap().await {
                return Err(error.context(format!(
                    "failed to stop daemon after readiness error: {cleanup_error:#}"
                )));
            }
            Err(error)
        }
    }
}

pub(crate) async fn status_daemon(args: DaemonStatusArgs) -> Result<ExitCode> {
    let storage = StoragePaths::resolve()?;
    let paths = DaemonPaths::from_storage(&storage);
    let Some((descriptor, token)) = read_live_control(&paths)? else {
        eprintln!("vyane daemon is not running");
        return Ok(ExitCode::from(1));
    };
    authenticated_health(&descriptor, &token).await?;
    let view = DaemonStatusView {
        status: "running",
        instance_id: &descriptor.instance_id,
        pid: descriptor.pid,
        addr: descriptor.addr,
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&view)?);
    } else {
        println!(
            "vyane daemon running at {} (pid {})",
            descriptor.addr, descriptor.pid
        );
    }
    Ok(ExitCode::SUCCESS)
}

pub(crate) fn read_verified_client_control() -> Result<DaemonClientControl> {
    let storage = StoragePaths::resolve()?;
    let paths = DaemonPaths::from_storage(&storage);
    let Some((descriptor, token)) = read_live_control(&paths)? else {
        bail!("workflow daemon is not running (control descriptor is absent)");
    };
    Ok(DaemonClientControl {
        addr: descriptor.addr,
        instance_id: descriptor.instance_id,
        token,
    })
}

pub(crate) async fn stop_daemon() -> Result<ExitCode> {
    let storage = StoragePaths::resolve()?;
    let paths = DaemonPaths::from_storage(&storage);
    let Some(descriptor) = read_descriptor_optional(&paths)? else {
        eprintln!("vyane daemon is not running");
        return Ok(ExitCode::from(1));
    };
    match classify_descriptor_identity(&descriptor) {
        DescriptorIdentity::Exact => {}
        DescriptorIdentity::Gone => {
            remove_control_if_matches(&paths, &descriptor.instance_id);
            eprintln!("vyane daemon is not running (stale descriptor removed)");
            return Ok(ExitCode::from(1));
        }
        DescriptorIdentity::Unverifiable(reason) => {
            bail!(
                "refusing to signal unverifiable daemon {}: {reason}",
                descriptor.pid
            )
        }
    }

    signal_process(descriptor.pid, SIGTERM);
    if wait_for_descriptor_exit(&descriptor, TERM_GRACE).await? {
        remove_control_if_matches(&paths, &descriptor.instance_id);
        println!("vyane daemon stopped");
        return Ok(ExitCode::SUCCESS);
    }

    // Revalidate immediately before forced termination. A stale numeric pid is
    // never sufficient authority to send SIGKILL.
    match classify_descriptor_identity(&descriptor) {
        DescriptorIdentity::Exact => signal_process(descriptor.pid, SIGKILL),
        DescriptorIdentity::Gone => {
            remove_control_if_matches(&paths, &descriptor.instance_id);
            println!("vyane daemon stopped");
            return Ok(ExitCode::SUCCESS);
        }
        DescriptorIdentity::Unverifiable(reason) => {
            bail!("daemon did not stop after TERM and its identity became unverifiable: {reason}")
        }
    }
    if !wait_for_descriptor_exit(&descriptor, KILL_GRACE).await? {
        bail!("daemon {} remained alive after SIGKILL", descriptor.pid);
    }
    remove_control_if_matches(&paths, &descriptor.instance_id);
    println!("vyane daemon stopped");
    Ok(ExitCode::SUCCESS)
}

fn daemon_router(instance_id: &str, token: &str, workflows: DaemonWorkflowSupervisor) -> Router {
    daemon_router_with_body_limit(instance_id, token, workflows, DAEMON_BODY_LIMIT)
}

fn daemon_router_with_body_limit(
    instance_id: &str,
    token: &str,
    workflows: DaemonWorkflowSupervisor,
    body_limit: usize,
) -> Router {
    let state = DaemonHttpState {
        instance_id: Arc::from(instance_id),
        workflows,
    };
    Router::new()
        .route("/health", get(daemon_health))
        .merge(crate::daemon_workflow::routes())
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(state)
        .layer(middleware::from_fn_with_state(
            BearerToken(Arc::from(token)),
            require_bearer,
        ))
}

async fn daemon_health(State(state): State<DaemonHttpState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
        instance_id: state.instance_id.to_string(),
    })
}

async fn require_bearer(
    State(token): State<BearerToken>,
    request: Request,
    next: Next,
) -> Response {
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|provided| bearer_tokens_equal(provided, token.0.as_ref()));
    if !authorized {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(request).await
}

pub(crate) fn bearer_tokens_equal(provided: &str, expected: &str) -> bool {
    if provided.len() != expected.len() {
        return false;
    }
    provided
        .bytes()
        .zip(expected.bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

async fn authenticated_health(descriptor: &DaemonDescriptor, token: &str) -> Result<()> {
    if !descriptor.addr.ip().is_loopback() {
        bail!(
            "daemon descriptor contains non-loopback address {}",
            descriptor.addr
        );
    }
    let client = reqwest::Client::builder()
        .timeout(HEALTH_TIMEOUT)
        .no_proxy()
        .gzip(false)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build daemon health client")?;
    let response = client
        .get(format!("http://{}/health", descriptor.addr))
        .bearer_auth(token)
        .send()
        .await
        .with_context(|| format!("connect to daemon at {}", descriptor.addr))?;
    if response.status() != reqwest::StatusCode::OK {
        bail!("daemon health returned HTTP {}", response.status());
    }
    if response
        .content_length()
        .is_some_and(|length| length > HEALTH_BODY_LIMIT as u64)
    {
        bail!("daemon health response exceeds {HEALTH_BODY_LIMIT} bytes");
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("read daemon health response")?;
        if bytes.len().saturating_add(chunk.len()) > HEALTH_BODY_LIMIT {
            bail!("daemon health response exceeds {HEALTH_BODY_LIMIT} bytes");
        }
        bytes.extend_from_slice(&chunk);
    }
    let health: HealthResponse =
        serde_json::from_slice(&bytes).context("parse daemon health response")?;
    if health.status != "ok" || health.instance_id != descriptor.instance_id {
        bail!("daemon health identity does not match its control descriptor");
    }
    Ok(())
}

fn current_descriptor(addr: SocketAddr) -> Result<DaemonDescriptor> {
    let pid = i32::try_from(std::process::id()).context("daemon pid does not fit i32")?;
    let pgid = pgid_of(pid).context("read daemon process group")?;
    let started_at = process_start_time(pid).context("read daemon process start time")?;
    let birth_fingerprint = process_birth_fingerprint(pid);
    #[cfg(target_os = "linux")]
    if birth_fingerprint.is_none() {
        bail!("read daemon Linux birth fingerprint");
    }
    Ok(DaemonDescriptor {
        schema: DAEMON_DESCRIPTOR_SCHEMA,
        instance_id: format!("daemon:{}", uuid::Uuid::now_v7()),
        pid,
        pgid,
        started_at,
        birth_fingerprint,
        addr,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DescriptorIdentity {
    Exact,
    Gone,
    Unverifiable(&'static str),
}

fn classify_descriptor_identity(descriptor: &DaemonDescriptor) -> DescriptorIdentity {
    match verify_controller_identity(
        descriptor.pid,
        descriptor.pgid,
        descriptor.started_at,
        descriptor.birth_fingerprint.as_deref(),
    ) {
        IdentityCheck::Match => DescriptorIdentity::Exact,
        IdentityCheck::Dead => DescriptorIdentity::Gone,
        IdentityCheck::Mismatch(
            "process group mismatch"
            | "process start time mismatch"
            | "process birth fingerprint mismatch",
        ) => DescriptorIdentity::Gone,
        IdentityCheck::Mismatch(reason) => DescriptorIdentity::Unverifiable(reason),
    }
}

async fn wait_for_descriptor_exit(descriptor: &DaemonDescriptor, grace: Duration) -> Result<bool> {
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        match classify_descriptor_identity(descriptor) {
            DescriptorIdentity::Gone => return Ok(true),
            DescriptorIdentity::Exact => {}
            DescriptorIdentity::Unverifiable(reason) => {
                bail!("lost exact daemon identity while waiting for exit: {reason}")
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(CONTROL_POLL).await;
    }
}

fn parse_loopback_addr(value: &str) -> Result<SocketAddr> {
    let addr: SocketAddr = value
        .parse()
        .with_context(|| format!("invalid daemon listen address `{value}`"))?;
    if !addr.ip().is_loopback() {
        bail!("daemon listen address must be loopback, got {addr}");
    }
    Ok(addr)
}

pub(crate) fn generate_bearer_token() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).context("generate daemon bearer token")?;
    let mut token = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut token, "{byte:02x}");
    }
    Ok(token)
}

pub(crate) fn validate_bearer_token(token: &str) -> Result<()> {
    if token.len() != 64
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("daemon bearer token file is malformed");
    }
    Ok(())
}

fn read_token_unlocked(path: &Path) -> Result<String> {
    let bytes = read_bounded(path, MAX_TOKEN_BYTES)?;
    let token = std::str::from_utf8(&bytes)
        .context("daemon token is not UTF-8")?
        .trim_end_matches(['\r', '\n'])
        .to_string();
    validate_bearer_token(&token)?;
    require_owner_only(path)?;
    Ok(token)
}

fn read_descriptor_optional_unlocked(path: &Path) -> Result<Option<DaemonDescriptor>> {
    let bytes = match read_bounded(path, MAX_DESCRIPTOR_BYTES) {
        Ok(bytes) => bytes,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    require_owner_only(path)?;
    let descriptor: DaemonDescriptor =
        serde_json::from_slice(&bytes).context("parse daemon control descriptor")?;
    if descriptor.schema != DAEMON_DESCRIPTOR_SCHEMA {
        bail!("unsupported daemon descriptor schema {}", descriptor.schema);
    }
    if descriptor.pid <= 0 || descriptor.pgid <= 0 || !descriptor.addr.ip().is_loopback() {
        bail!("daemon control descriptor contains invalid process or address fields");
    }
    validate_instance_id(&descriptor.instance_id)?;
    Ok(Some(descriptor))
}

fn validate_instance_id(value: &str) -> Result<()> {
    let Some(raw) = value.strip_prefix("daemon:") else {
        bail!("daemon control descriptor contains invalid instance identity");
    };
    let parsed = uuid::Uuid::parse_str(raw)
        .context("daemon control descriptor contains invalid instance identity")?;
    if parsed.get_version_num() != 7
        || parsed.get_variant() != uuid::Variant::RFC4122
        || parsed.hyphenated().to_string() != raw
    {
        bail!("daemon control descriptor contains non-canonical instance identity");
    }
    Ok(())
}

#[derive(Debug)]
struct DaemonControlLock {
    file: std::fs::File,
}

impl Drop for DaemonControlLock {
    fn drop(&mut self) {
        let _ = fs4::fs_std::FileExt::unlock(&self.file);
    }
}

fn acquire_control_lock(paths: &DaemonPaths) -> Result<DaemonControlLock> {
    secure_directory(&paths.data_dir)?;
    let path = paths.control_lock();
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            bail!(
                "daemon control lock {} is not a regular file",
                path.display()
            )
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("stat daemon control lock {}", path.display()));
        }
    }
    let mut options = std::fs::OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options
        .open(&path)
        .with_context(|| format!("open daemon control lock {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("chmod daemon control lock {}", path.display()))?;
    }

    let deadline = std::time::Instant::now() + CONTROL_LOCK_BUDGET;
    loop {
        match fs4::fs_std::FileExt::try_lock_exclusive(&file) {
            Ok(true) => return Ok(DaemonControlLock { file }),
            Ok(false) if std::time::Instant::now() < deadline => {
                std::thread::sleep(CONTROL_LOCK_POLL);
            }
            Ok(false) => {
                bail!(
                    "timed out after {} ms waiting for daemon control lock {}",
                    CONTROL_LOCK_BUDGET.as_millis(),
                    path.display()
                )
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("lock daemon control file {}", path.display()));
            }
        }
    }
}

fn read_control_optional(
    paths: &DaemonPaths,
) -> Result<Option<(DaemonDescriptor, Option<String>)>> {
    let _lock = acquire_control_lock(paths)?;
    let Some(descriptor) = read_descriptor_optional_unlocked(&paths.descriptor())? else {
        return Ok(None);
    };
    let token = if paths.token().try_exists().with_context(|| {
        format!(
            "check daemon token existence at {}",
            paths.token().display()
        )
    })? {
        Some(read_token_unlocked(&paths.token())?)
    } else {
        None
    };
    Ok(Some((descriptor, token)))
}

/// Read one internally consistent, live control generation.
///
/// Identity is classified while the control lock is held. A dead generation
/// is removed before returning, without consulting its token; a live or
/// unverifiable generation is never replaced merely because its token is
/// missing or malformed.
fn read_live_control(paths: &DaemonPaths) -> Result<Option<(DaemonDescriptor, String)>> {
    let _lock = acquire_control_lock(paths)?;
    let Some(descriptor) = read_descriptor_optional_unlocked(&paths.descriptor())? else {
        return Ok(None);
    };
    match classify_descriptor_identity(&descriptor) {
        DescriptorIdentity::Gone => {
            let _ = std::fs::remove_file(paths.descriptor());
            let _ = std::fs::remove_file(paths.token());
            let _ = sync_directory(&paths.data_dir);
            Ok(None)
        }
        DescriptorIdentity::Unverifiable(reason) => {
            bail!("workflow daemon control identity is unverifiable: {reason}")
        }
        DescriptorIdentity::Exact => {
            if !paths.token().try_exists().with_context(|| {
                format!(
                    "check daemon token existence at {}",
                    paths.token().display()
                )
            })? {
                bail!("workflow daemon control is incomplete (bearer token is absent)");
            }
            let token = read_token_unlocked(&paths.token())?;
            Ok(Some((descriptor, token)))
        }
    }
}

fn read_descriptor_optional(paths: &DaemonPaths) -> Result<Option<DaemonDescriptor>> {
    let _lock = acquire_control_lock(paths)?;
    read_descriptor_optional_unlocked(&paths.descriptor())
}

fn publish_control(paths: &DaemonPaths, token: &str, descriptor: &DaemonDescriptor) -> Result<()> {
    let _lock = acquire_control_lock(paths)?;
    write_private_atomic(&paths.token(), token.as_bytes())?;
    if let Err(error) = write_private_json(&paths.descriptor(), descriptor) {
        let _ = std::fs::remove_file(paths.token());
        return Err(error);
    }
    Ok(())
}

fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(anyhow::Error::new)
        .with_context(|| format!("stat {}", path.display()))?;
    if !metadata.file_type().is_file() {
        bail!("{} is not a regular file", path.display());
    }
    require_owner_only(path)?;
    if metadata.len() > limit as u64 {
        bail!("{} exceeds {limit} bytes", path.display());
    }
    let mut file = std::fs::File::open(path)
        .map_err(anyhow::Error::new)
        .with_context(|| format!("open {}", path.display()))?;
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(u64::try_from(limit).unwrap_or(u64::MAX) + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read {}", path.display()))?;
    if bytes.len() > limit {
        bail!("{} exceeds {limit} bytes", path.display());
    }
    Ok(bytes)
}

fn write_private_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value).context("serialize daemon descriptor")?;
    write_private_atomic(path, &bytes)
}

pub(crate) fn write_private_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("{} has no parent", path.display()))?;
    secure_directory(parent)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("private control filename is not UTF-8")?;
    let temp = parent.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        uuid::Uuid::now_v7()
    ));
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temp)
        .with_context(|| format!("create {}", temp.display()))?;
    let result = (|| -> Result<()> {
        file.write_all(bytes)
            .with_context(|| format!("write {}", temp.display()))?;
        file.sync_all()
            .with_context(|| format!("sync {}", temp.display()))?;
        drop(file);
        std::fs::rename(&temp, path).with_context(|| format!("publish {}", path.display()))?;
        sync_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn secure_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path).with_context(|| format!("create {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("chmod {}", path.display()))?;
    }
    Ok(())
}

fn open_private_log(path: &Path) -> Result<std::fs::File> {
    let parent = path
        .parent()
        .with_context(|| format!("{} has no parent", path.display()))?;
    secure_directory(parent)?;
    let mut options = std::fs::OpenOptions::new();
    options.create(true).append(true).read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options
        .open(path)
        .with_context(|| format!("open daemon log {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("chmod daemon log {}", path.display()))?;
    }
    Ok(file)
}

fn require_owner_only(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(path)
            .with_context(|| format!("stat {}", path.display()))?
            .permissions()
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            bail!("{} is not owner-only (mode {mode:o})", path.display());
        }
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    std::fs::File::open(path)
        .with_context(|| format!("open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn remove_control_if_matches(paths: &DaemonPaths, instance_id: &str) {
    let Ok(_lock) = acquire_control_lock(paths) else {
        return;
    };
    let matches = read_descriptor_optional_unlocked(&paths.descriptor())
        .ok()
        .flatten()
        .is_some_and(|descriptor| descriptor.instance_id == instance_id);
    if !matches {
        return;
    }
    let _ = std::fs::remove_file(paths.descriptor());
    let _ = std::fs::remove_file(paths.token());
    let _ = sync_directory(&paths.data_dir);
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt as _;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const INSTANCE_A: &str = "daemon:01890f3e-7b7c-7cc2-98d2-3f9a2b6c7d8e";
    const INSTANCE_B: &str = "daemon:01890f3e-7b7d-7cc2-98d2-3f9a2b6c7d8e";

    async fn test_workflow_supervisor(
        data_dir: &Path,
        instance_id: &str,
    ) -> DaemonWorkflowSupervisor {
        let service = VyaneService::from_loaded_with_paths(
            vyane_service::LoadedConfig {
                config: vyane_config::ResolvedConfig::default(),
                files: Vec::new(),
                secrets: std::collections::BTreeMap::new(),
            },
            StoragePaths::from_data_dir(data_dir),
        )
        .unwrap();
        DaemonWorkflowSupervisor::open(Arc::new(service), instance_id.to_string())
            .await
            .unwrap()
    }

    #[test]
    fn tokens_are_256_bit_lowercase_hex() {
        let first = generate_bearer_token().unwrap();
        let second = generate_bearer_token().unwrap();
        validate_bearer_token(&first).unwrap();
        assert_eq!(first.len(), 64);
        assert_ne!(first, second);
    }

    #[test]
    fn daemon_rejects_non_loopback_addresses() {
        assert!(parse_loopback_addr("127.0.0.1:0").is_ok());
        assert!(parse_loopback_addr("[::1]:0").is_ok());
        assert!(parse_loopback_addr("0.0.0.0:9722").is_err());
        assert!(parse_loopback_addr("192.0.2.1:9722").is_err());
    }

    #[tokio::test]
    async fn health_requires_the_exact_bearer_token() {
        let directory = tempfile::tempdir().unwrap();
        let workflows = test_workflow_supervisor(directory.path(), INSTANCE_A).await;
        let app = daemon_router(INSTANCE_A, &"a".repeat(64), workflows);
        for authorization in [None, Some("Bearer wrong")] {
            let mut request = HttpRequest::get("/health");
            if let Some(value) = authorization {
                request = request.header(header::AUTHORIZATION, value);
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }

        let response = app
            .oneshot(
                HttpRequest::get("/health")
                    .header(header::AUTHORIZATION, format!("Bearer {}", "a".repeat(64)))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn workflow_routes_are_authenticated_cors_closed_and_body_bounded() {
        let directory = tempfile::tempdir().unwrap();
        let workflows = test_workflow_supervisor(directory.path(), INSTANCE_A).await;
        let token = "a".repeat(64);
        let app = daemon_router_with_body_limit(INSTANCE_A, &token, workflows, 32);

        let preflight = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("OPTIONS")
                    .uri("/v1/workflows")
                    .header(header::ORIGIN, "https://attacker.invalid")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(preflight.status(), StatusCode::UNAUTHORIZED);
        assert!(
            preflight
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none()
        );

        let oversized = app
            .oneshot(
                HttpRequest::post("/v1/workflows")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(vec![b'x'; 33]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn health_response_is_rejected_above_the_streaming_limit() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(
                ResponseTemplate::new(200).set_body_bytes(vec![b'x'; HEALTH_BODY_LIMIT + 1]),
            )
            .mount(&server)
            .await;
        let descriptor = DaemonDescriptor {
            schema: DAEMON_DESCRIPTOR_SCHEMA,
            instance_id: INSTANCE_A.into(),
            pid: 1,
            pgid: 1,
            started_at: Utc::now(),
            birth_fingerprint: Some("unused".into()),
            addr: *server.address(),
        };

        let error = authenticated_health(&descriptor, &"a".repeat(64))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exceeds 16384 bytes"));
    }

    #[cfg(unix)]
    #[test]
    fn control_files_and_directory_are_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let path = data_dir.join("control");
        write_private_atomic(&path, b"private").unwrap();
        assert_eq!(
            std::fs::metadata(&data_dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn old_generation_cleanup_cannot_remove_new_descriptor_or_token() {
        let directory = tempfile::tempdir().unwrap();
        let storage = StoragePaths::from_data_dir(directory.path());
        let paths = DaemonPaths::from_storage(&storage);
        let descriptor = |instance_id: &str, pid: i32| DaemonDescriptor {
            schema: DAEMON_DESCRIPTOR_SCHEMA,
            instance_id: instance_id.into(),
            pid,
            pgid: pid,
            started_at: Utc::now(),
            birth_fingerprint: Some(format!("birth-{pid}")),
            addr: "127.0.0.1:9722".parse().unwrap(),
        };
        let old = descriptor(INSTANCE_A, 101);
        let new = descriptor(INSTANCE_B, 202);
        let old_token = "a".repeat(64);
        let new_token = "b".repeat(64);

        publish_control(&paths, &old_token, &old).unwrap();
        publish_control(&paths, &new_token, &new).unwrap();
        remove_control_if_matches(&paths, &old.instance_id);

        let (current, token) = read_control_optional(&paths).unwrap().unwrap();
        assert_eq!(current, new);
        assert_eq!(token.as_deref(), Some(new_token.as_str()));
    }

    #[test]
    fn descriptor_only_read_is_not_blocked_by_a_malformed_token() {
        let directory = tempfile::tempdir().unwrap();
        let storage = StoragePaths::from_data_dir(directory.path());
        let paths = DaemonPaths::from_storage(&storage);
        let descriptor = DaemonDescriptor {
            schema: DAEMON_DESCRIPTOR_SCHEMA,
            instance_id: INSTANCE_A.into(),
            pid: 101,
            pgid: 101,
            started_at: Utc::now(),
            birth_fingerprint: Some("birth-101".into()),
            addr: "127.0.0.1:9722".parse().unwrap(),
        };
        publish_control(&paths, &"a".repeat(64), &descriptor).unwrap();
        write_private_atomic(&paths.token(), b"malformed").unwrap();

        assert!(read_control_optional(&paths).is_err());
        assert_eq!(read_descriptor_optional(&paths).unwrap(), Some(descriptor));
    }

    #[test]
    fn stale_generation_cleanup_does_not_parse_its_malformed_token() {
        let directory = tempfile::tempdir().unwrap();
        let storage = StoragePaths::from_data_dir(directory.path());
        let paths = DaemonPaths::from_storage(&storage);
        let descriptor = DaemonDescriptor {
            schema: DAEMON_DESCRIPTOR_SCHEMA,
            instance_id: INSTANCE_A.into(),
            pid: 2_000_000_000,
            pgid: 2_000_000_000,
            started_at: Utc::now(),
            birth_fingerprint: Some("definitely-not-live".into()),
            addr: "127.0.0.1:9722".parse().unwrap(),
        };
        publish_control(&paths, &"a".repeat(64), &descriptor).unwrap();
        write_private_atomic(&paths.token(), b"malformed").unwrap();

        assert!(read_live_control(&paths).unwrap().is_none());
        assert!(!paths.descriptor().exists());
        assert!(!paths.token().exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn readiness_error_cleanup_kills_and_reaps_the_child() {
        let mut command = Command::new("sh");
        command.args(["-c", "exec sleep 30"]);
        let child = spawn_tokio_in_session(command).unwrap();
        let pid = i32::try_from(child.id().unwrap()).unwrap();
        let process_pid = rustix::process::Pid::from_raw(pid).unwrap();
        assert_eq!(
            rustix::process::getsid(Some(process_pid)).unwrap(),
            process_pid,
            "resident daemon startup must create a new POSIX session"
        );
        let mut starting = StartingDaemon::new(child);

        starting.kill_and_reap().await.unwrap();

        assert!(!crate::task::proc::pid_alive(pid));
    }
}
