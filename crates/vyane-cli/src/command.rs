use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::{Read as _, Write as _};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use vyane_config::ResolvedConfig;
use vyane_core::{
    AdapterTransport, BoundTarget, CancellationToken, ErrorKind, Harness, HarnessKind,
    HarnessLifecycleEvent, HarnessLifecycleReporter, ProviderId, RunQuery, RunStatus, SessionRef,
    TaskSpec, VyaneError,
};
use vyane_harness::{ClaudeCodeHarness, CodexCliHarness};
use vyane_service::{SessionView, VyaneService, resolve_target_chain, split_targets};
use vyane_task::{
    ControllerRef, FailureCode, NewTask, SqliteTaskStore, TaskKind, TaskOrigin, TaskQuery,
    TaskRecord, TaskSettlement, TaskState as DurableTaskState, TaskStore, TaskStoreError,
};
use vyane_workflow::{
    StepEvent, TargetResolver, Workflow, WorkflowEngine, WorkflowError, WorkflowRunId,
    WorkflowSourceBundle,
};

use crate::app::{LoadedConfig, Runtime, StoragePaths, load_config};
use crate::cli::{
    BroadcastArgs, Cli, Command, DaemonCommand, DispatchArgs, HistoryArgs, ReviewArgs, RouteArgs,
    ServeArgs, SessionCommand, SessionInspectArgs, SessionResetNativeArgs, SessionsArgs,
    TaskCancelArgs, TaskCommand, TaskListArgs, TaskStatusArgs, WorkerArgs, WorkflowCancelArgs,
    WorkflowCommand, WorkflowReplayArgs, WorkflowResumeArgs, WorkflowRunArgs, WorkflowStatusArgs,
    WorkflowSubmitArgs,
};
use crate::daemon_client::{DaemonWorkflowClient, WorkflowTaskView};
use crate::output::{BroadcastJson, BroadcastRow, DurableTaskStatusJson, RunJson, TaskRow};
use crate::task::proc::{
    IdentityCheck, SIGKILL, SIGTERM, pgid_of, process_birth_fingerprint, process_group_alive,
    signal_group, verify_controller_identity, verify_identity,
};
use crate::task::store::{
    HARNESS_CONTROLLER_SCHEMA, HarnessControllerFile, JobSpec, StatusFile, TargetSnapshot,
    TaskPaths, TaskState, WORKER_ENVELOPE_SCHEMA, WorkerEnvelope, interpret_state, list_tasks,
};
use crate::task::{LOCAL_TASK_OWNER, is_local_dispatch};

/// The production identity probe used by read-side orphan detection: a
/// still-`running` status is only trusted if the recorded process still
/// validates as its own worker (see [`verify_identity`]).
fn identity_probe(pid: i32, pgid: i32, started_at: chrono::DateTime<chrono::Utc>) -> IdentityCheck {
    verify_identity(pid, pgid, started_at)
}

/// The nested harness owns a different process group from its detached worker.
/// Give the worker enough time to drive the harness's own TERM -> KILL -> pipe
/// drain sequence before a separate controller intervenes.
const NESTED_HARNESS_CANCEL_GRACE: Duration = Duration::from_secs(7);
/// Once the nested controller is gone, leave a distinct window for ledger and
/// SQLite settlement before force-killing the outer worker.
const WORKER_SETTLEMENT_GRACE: Duration = Duration::from_secs(6);
/// Bound waits after an exact, verified SIGKILL.
const FORCED_PROCESS_CLEANUP_GRACE: Duration = Duration::from_secs(2);

fn is_local_detached_dispatch(record: &TaskRecord) -> bool {
    is_local_dispatch(record, TaskOrigin::CliDetached)
}

pub async fn run(cli: Cli) -> Result<ExitCode> {
    match cli.command {
        Command::Check => run_check(cli.config).await,
        Command::Dispatch(args) => run_dispatch(cli.config, args).await,
        Command::Broadcast(args) => run_broadcast(cli.config, args).await,
        Command::History(args) => run_history(args).await,
        Command::Sessions(args) => run_sessions(args).await,
        Command::Session(command) => run_session(command).await,
        Command::Workflow(command) => match command {
            WorkflowCommand::Run(args) => run_workflow(cli.config, args).await,
            WorkflowCommand::Submit(args) => submit_daemon_workflow(args).await,
            WorkflowCommand::Status(args) => status_daemon_workflow(args).await,
            WorkflowCommand::Cancel(args) => cancel_daemon_workflow(args).await,
            WorkflowCommand::Resume(args) => resume_workflow(cli.config, args).await,
            WorkflowCommand::Replay(args) => replay_workflow(cli.config, args).await,
            WorkflowCommand::List(args) => list_workflows(args).await,
        },
        Command::Review(args) => run_review_command(cli.config, args).await,
        Command::Route(args) => run_route_command(cli.config, args).await,
        Command::Task(task) => run_task(task).await,
        Command::Serve(args) => run_serve(cli.config, args).await,
        Command::Daemon(command) => match command {
            DaemonCommand::Run(args) => crate::daemon::run_daemon(cli.config, args).await,
            DaemonCommand::Start(args) => crate::daemon::start_daemon(cli.config, args).await,
            DaemonCommand::Status(args) => crate::daemon::status_daemon(args).await,
            DaemonCommand::Stop => crate::daemon::stop_daemon().await,
        },
        Command::Mcp => run_mcp(cli.config).await,
        Command::Worker(args) => run_worker(cli.config, args).await,
    }
}

async fn run_task(command: TaskCommand) -> Result<ExitCode> {
    match command {
        TaskCommand::List(args) => run_task_list(args).await,
        TaskCommand::Status(args) => run_task_status(args).await,
        TaskCommand::Cancel(args) => run_task_cancel(args).await,
    }
}

async fn run_serve(config_path: Option<PathBuf>, args: ServeArgs) -> Result<ExitCode> {
    let addr: SocketAddr = args.addr.parse().context("invalid --addr")?;
    if !addr.ip().is_loopback() {
        eprintln!("config error: vyane serve only accepts loopback listen addresses");
        return Ok(ExitCode::from(2));
    }
    let service = VyaneService::load(config_path.as_deref())?;
    eprintln!("vyane serve starting on {addr}");
    crate::api::run_server(service, addr).await?;
    Ok(ExitCode::SUCCESS)
}

/// Run the MCP server over stdio. The server speaks JSON-RPC on stdin/stdout,
/// so any client output (status lines, errors) belongs on stderr to keep the
/// transport stream clean.
async fn run_mcp(config_path: Option<PathBuf>) -> Result<ExitCode> {
    let service = VyaneService::load(config_path.as_deref())?;
    vyane_mcp::run_stdio(service).await?;
    Ok(ExitCode::SUCCESS)
}

async fn run_check(config_path: Option<PathBuf>) -> Result<ExitCode> {
    let loaded = match load_config(config_path.as_deref()) {
        Ok(loaded) => loaded,
        Err(error) => {
            eprintln!("config error: {error:#}");
            return Ok(ExitCode::from(2));
        }
    };

    println!("config files:");
    for path in &loaded.files {
        let state = if path.exists() { "loaded" } else { "missing" };
        println!("  {} ({state})", path.display());
    }

    println!("providers:");
    for (id, provider) in &loaded.config.providers.providers {
        println!(
            "  {id}: {} default_model={}",
            provider.protocol,
            provider
                .default_model
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_string())
        );
    }

    println!("profiles:");
    for name in loaded.config.profiles.keys() {
        match loaded
            .config
            .resolve_failover_with(name, &|key| loaded.env_lookup(key))
        {
            Ok(chain) => {
                let display = chain
                    .iter()
                    .map(|bound| bound.target.to_string())
                    .collect::<Vec<_>>()
                    .join(" -> ");
                println!("  {name}: {display}");
            }
            Err(error) => {
                println!("  {name}: warning: {}", error.message);
            }
        }
    }

    println!("harnesses:");
    for kind in [HarnessKind::ClaudeCode, HarnessKind::CodexCli] {
        let available = harness_available(kind.clone()).await;
        println!(
            "  {kind}: {}",
            if available { "available" } else { "missing" }
        );
    }

    println!("profile environment:");
    for name in loaded.config.profiles.keys() {
        let vars = env_vars_for_profile(&loaded.config, name);
        if vars.is_empty() {
            println!("  {name}: none");
            continue;
        }
        for var in vars {
            let state = if loaded.env_lookup(&var).is_some() {
                "present"
            } else {
                "missing"
            };
            println!("  {name}: {var} {state}");
        }
    }

    Ok(ExitCode::SUCCESS)
}

async fn run_dispatch(config_path: Option<PathBuf>, args: DispatchArgs) -> Result<ExitCode> {
    // Input-phase failures exit 2 (mirroring `check`), so wrappers can tell
    // "fix your invocation/config" apart from "the run failed" (exit 1). This
    // validation runs FIRST — before `--detach` spawns anything — so bad input
    // never leaves a stray task directory behind. It validates BOTH config +
    // target resolution AND the full TaskSpec (crucially, `--label` parsing:
    // `--label bad` with no `=` is rejected here, in the parent, not deferred
    // into a worker that would otherwise briefly exist as a task dir).
    let phase = load_config(config_path.as_deref()).and_then(|loaded| {
        // Build once so auto-routing sees the exact labels/task that the kernel
        // will record. Explicit selectors pass through the same plan boundary.
        let mut task = task_base(
            args.task.clone(),
            args.workdir.clone(),
            args.sandbox.into(),
            args.system.clone(),
            args.timeout,
            args.label.clone(),
        )?;
        vyane_service::validate_user_routing_labels(&task.labels)?;
        if args.no_frontier {
            // Command-line policy wins over both accepted label spellings. In
            // particular, `--label allow_frontier=true --no-frontier` must not
            // let the generic alias shadow the canonical false value.
            task.labels.remove("allow_frontier");
            task.labels
                .insert("routing.allow_frontier".into(), "false".into());
        }
        if let Some(session) = args.session.as_ref() {
            task.session = Some(SessionRef::new(session.clone()));
        }
        let plan = vyane_service::plan_dispatch(&loaded, &args.target, &mut task)?;
        Ok((loaded, plan, task))
    });
    let (loaded, plan, task) = match phase {
        Ok(value) => value,
        Err(error) => {
            eprintln!("config error: {error:#}");
            return Ok(ExitCode::from(2));
        }
    };

    // Detached path: freeze the request and hand it to a re-exec'd worker, then
    // return immediately. Config + full TaskSpec are already validated above, so
    // reaching here means the target resolves and every field (labels included)
    // is well-formed.
    if args.detach {
        // Finish capability and session admission before the first durable
        // task row or worker process exists. The worker independently repeats
        // admission and compares this serializable snapshot.
        let prepared: Result<vyane_kernel::PreparedDispatch> = async {
            let runtime = Runtime::new(loaded.config.clone(), StoragePaths::resolve()?)?;
            let prepared = runtime.dispatcher.prepare(&task, plan.chain.clone())?;
            runtime
                .dispatcher
                .validate_session_admission(&task, &prepared)
                .await?;
            Ok(prepared)
        }
        .await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                eprintln!("config error: {error:#}");
                return Ok(ExitCode::from(2));
            }
        };
        let target_snapshot = plan
            .chain
            .iter()
            .map(|bound| TargetSnapshot::from_bound(bound, &loaded.config))
            .collect::<Result<Vec<_>>>()?;
        let mut frozen = args;
        frozen.target = plan.selector;
        frozen.label = task
            .labels
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        return spawn_detached_dispatch(config_path, frozen, target_snapshot, prepared);
    }

    let json = args.json;
    let want_stream = args.stream;
    let chain = plan.chain;
    let runtime = Runtime::new(loaded.config, StoragePaths::resolve()?)?;
    // Built once and shared by both the streaming attempt and the
    // non-streaming fallback below, so a ctrl-c during the streaming path is
    // still honored if control falls through to `Dispatcher::dispatch`.
    let cancel = cancellation_token();
    let mut stream_fallback = None;

    if want_stream {
        match streamable_target(&chain, &task) {
            Some(bound) => {
                let bound = bound.clone();
                let prepared = runtime.dispatcher.prepare(&task, chain.clone())?;
                match run_dispatch_streaming(&runtime, &task, &prepared, json, cancel.clone())
                    .await?
                {
                    Some(code) => return Ok(code),
                    // `Unsupported` from the client itself: fall through to
                    // the non-streaming path with the same prepared identity.
                    None => {
                        eprintln!(
                            "notice: {} does not support streaming; falling back to non-streaming",
                            bound.target
                        );
                        stream_fallback = Some(prepared);
                    }
                }
            }
            None => eprintln!(
                "notice: --stream only applies to a single target with no --session; falling back to non-streaming"
            ),
        }
    }

    let outcome = match stream_fallback {
        Some(prepared) => {
            runtime
                .dispatcher
                .dispatch_prepared(&task, prepared, cancel)
                .await?
        }
        None => runtime.dispatcher.dispatch(&task, chain, cancel).await?,
    };
    let record = outcome.record;
    let output = outcome.output;
    let success = record.status == RunStatus::Success;

    if json {
        print_run_json(record, output)?;
    } else if let Some(text) = output.as_deref() {
        println!("{text}");
    } else if let Some(error) = &record.error {
        eprintln!("{error}");
    }

    Ok(if success {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

/// The chain qualifies for `--stream` when it is one direct-HTTP or CLI-harness
/// target (no failover) and the task names no session. The streaming path has no
/// session continuity, so `--session --stream` falls back rather than
/// half-applying session semantics (tagging a `RunRecord.session_id` while never
/// touching the session store's transcript/run_count).
fn streamable_target<'a>(chain: &'a [BoundTarget], task: &TaskSpec) -> Option<&'a BoundTarget> {
    if task.session.is_some() {
        return None;
    }
    match chain {
        [bound]
            if matches!(
                bound.transport,
                AdapterTransport::DirectHttp | AdapterTransport::CliWrap
            ) =>
        {
            Some(bound)
        }
        _ => None,
    }
}

/// Drive the single-target streaming path via the kernel's `dispatch_stream`
/// method. Deltas are printed to stdout as they arrive; the assembled
/// `RunRecord` is ledger-appended by the kernel (no CLI-side duplication).
///
/// Returns `Ok(Some(exit_code))` when the run was handled (success or failure,
/// a `RunRecord` was recorded). Returns `Ok(None)` when the client itself
/// declined streaming (`ErrorKind::Unsupported`) so the caller can fall back
/// to non-streaming `Dispatcher::dispatch`.
async fn run_dispatch_streaming(
    runtime: &Runtime,
    task: &TaskSpec,
    prepared: &vyane_kernel::PreparedDispatch,
    json: bool,
    cancel: CancellationToken,
) -> Result<Option<ExitCode>> {
    let outcome = runtime
        .dispatcher
        .dispatch_stream_prepared(task, prepared, cancel, move |event| match event {
            vyane_kernel::StreamDispatchEvent::Delta(delta) => {
                if !json {
                    print!("{delta}");
                    let _ = std::io::stdout().flush();
                }
            }
            vyane_kernel::StreamDispatchEvent::ReasoningDelta(_) => {
                // Reasoning deltas are not printed to stdout.
            }
            vyane_kernel::StreamDispatchEvent::ToolUse { name, summary } => {
                if !json {
                    eprintln!("\n[tool] {name}: {summary}");
                }
            }
        })
        .await?;

    match outcome {
        None => {
            // Client declined streaming — caller falls back to non-streaming.
            Ok(None)
        }
        Some(outcome) => {
            if !json {
                // Trailing newline if any deltas were printed (i.e. the run
                // produced output text).
                if outcome.output.is_some() {
                    println!();
                }
            }
            let record = outcome.record;
            let output = outcome.output;
            let success = record.status == RunStatus::Success;

            if json {
                print_run_json(record, output)?;
            } else if !success {
                if let Some(error) = &record.error {
                    eprintln!("{error}");
                }
            }

            Ok(Some(if success {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }))
        }
    }
}

/// Freeze the dispatch request into a one-shot in-memory envelope and spawn a
/// detached worker to run it. Prints the run id and returns without waiting.
///
/// The target chain was already resolved by the caller (so config errors have
/// exited 2 before we get here). The request crosses only the worker's piped
/// stdin; new submissions never persist `job.json` or put request fields in
/// argv/environment.
fn spawn_detached_dispatch(
    config_path: Option<PathBuf>,
    args: DispatchArgs,
    target_snapshot: Vec<TargetSnapshot>,
    prepared: vyane_kernel::PreparedDispatch,
) -> Result<ExitCode> {
    let storage = StoragePaths::resolve()?;
    let tasks_dir = crate::task::tasks_root(&storage.data_dir);
    let store = SqliteTaskStore::open(storage.task_metadata_db_path())
        .context("open durable task metadata store")?;
    let run_id = uuid::Uuid::now_v7().to_string();
    let task_digest = vyane_kernel::task_digest(&args.task);
    let target_key = args.target.clone();
    let created = store.create(
        LOCAL_TASK_OWNER,
        NewTask {
            id: run_id.clone(),
            kind: TaskKind::Dispatch,
            origin: TaskOrigin::CliDetached,
            task_digest,
            target_key,
            created_at: chrono::Utc::now(),
        },
    )?;
    let job = JobSpec {
        run_id: run_id.clone(),
        task: args.task,
        target: args.target,
        workdir: args.workdir,
        sandbox: vyane_core::Sandbox::from(args.sandbox).into(),
        system: args.system,
        timeout_secs: args.timeout,
        labels: args.label,
        session: args.session,
        config: config_path,
        target_snapshot,
        capability_plan: Some(prepared.capability_snapshot().clone()),
    };
    let envelope = WorkerEnvelope::new(job);
    let paths = TaskPaths::new(&tasks_dir, &run_id);
    if let Err(spawn_error) =
        crate::task::spawn::spawn_detached(&paths, &envelope, prepared.pinned_workdir())
    {
        let settlement = settle_spawn_failure(&store, &created, spawn_error.spawned());
        if let Err(settlement_error) = settlement {
            return Err(anyhow!(
                "spawn detached worker: {spawn_error:#}; additionally failed to settle task metadata: {settlement_error:#}"
            ));
        }
        return Err(anyhow!(spawn_error).context("spawn detached worker"));
    }

    if args.json {
        println!("{}", serde_json::json!({ "id": run_id }));
    } else {
        println!("{run_id}");
    }
    Ok(ExitCode::SUCCESS)
}

/// Settle a failed spawn without ever borrowing a newer controller's epoch.
/// Before exec, only the exact create snapshot is owned. If the child attached
/// before an EPIPE, the spawn transport supplies the PID plus birth fingerprint
/// of the child it killed and reaped; only that exact controller may be settled.
fn settle_spawn_failure(
    store: &SqliteTaskStore,
    created: &TaskRecord,
    spawned: Option<&crate::task::spawn::SpawnedWorker>,
) -> Result<TaskRecord> {
    loop {
        let record = store.get(LOCAL_TASK_OWNER, &created.id)?.ok_or_else(|| {
            anyhow!(
                "durable task `{}` disappeared after spawn failure",
                created.id
            )
        })?;
        if record.state.is_terminal() {
            return Ok(record);
        }
        let owns_created = record.state == DurableTaskState::Queued
            && record.revision == created.revision
            && record.executor_epoch == created.executor_epoch
            && record.controller.is_none();
        let owns_attached = spawned.is_some_and(|spawned| {
            matches!(
                &record.controller,
                Some(ControllerRef::ProcessGroup {
                    pid,
                    pgid,
                    birth_fingerprint: Some(fingerprint),
                    ..
                }) if *pid == spawned.pid
                    && *pgid == spawned.pgid
                    && spawned.birth_fingerprint.as_deref() == Some(fingerprint.as_str())
            )
        });
        if !owns_created && !owns_attached {
            bail!(
                "task `{}` is now {} under an unrecognized executor; refusing spawn-failure settlement",
                record.id,
                record.state
            );
        }
        match store.settle(
            LOCAL_TASK_OWNER,
            &record.id,
            record.revision,
            record.executor_epoch,
            TaskSettlement::Failed {
                code: FailureCode::SpawnFailed,
                ledger_run_id: None,
            },
            chrono::Utc::now(),
        ) {
            Ok(settled) => return Ok(settled),
            Err(TaskStoreError::Conflict { .. }) => continue,
            Err(TaskStoreError::InvalidState { .. }) => {
                let latest = store.get(LOCAL_TASK_OWNER, &record.id)?.ok_or_else(|| {
                    anyhow!(
                        "durable task `{}` disappeared after spawn failure",
                        record.id
                    )
                })?;
                if latest.state.is_terminal() {
                    return Ok(latest);
                }
                return Err(anyhow!(
                    "task `{}` remained {} while spawn-failure settlement was rejected",
                    latest.id,
                    latest.state
                ));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

/// The detached worker: attach its exact process controller in SQLite,
/// re-resolve the frozen job, dispatch it through the normal kernel path, then
/// settle durable metadata and optionally write `output.txt`. A `SIGTERM` (from
/// `task cancel`) cancels the kernel so the run finalizes as `cancelled` with
/// its `RunRecord` still on the ledger. Old `job.json` tasks retain their
/// historical `status.json` behavior through an isolated compatibility path.
///
/// The worker owns its own process group (installed by the parent via
/// `setsid`); it never re-exec's further.
///
/// ## Two ordering invariants this shell enforces
///
/// 1. **Cancellation handler is armed before any `running` state is
///    observable.** The `SIGTERM` → [`CancellationToken`] handler is installed
///    before controller attachment changes `queued` to `running`. Any
///    canceller allowed to signal the recorded controller therefore reaches an
///    armed token handler, never a raw process teardown mid-run.
///
/// 2. **Returned failures settle durably.** [`worker_body`] errors are converted
///    to a bounded failure code in SQLite; the full diagnostic stays in the
///    private log. Abrupt process death is detected by controller identity and
///    reconciled to `interrupted` by task readers.
async fn run_worker(config_path: Option<PathBuf>, args: WorkerArgs) -> Result<ExitCode> {
    let (storage, tasks_dir) = crate::app::resolve_paths_with_tasks()?;
    let paths = TaskPaths::new(&tasks_dir, &args.id);
    let store = SqliteTaskStore::open(storage.task_metadata_db_path())
        .context("open durable task metadata store")?;
    let durable = store.get(LOCAL_TASK_OWNER, &args.id)?.is_some();

    // Invariant (1): arm the cancellation handler BEFORE running any body that
    // could publish `running`. See the doc comment above.
    let cancel = worker_cancellation_token()?;
    let worker_epoch = if durable {
        let pid = std::process::id() as i32;
        let pgid = pgid_of(pid).unwrap_or(pid);
        match attach_detached_controller(&store, &args.id, pid, pgid) {
            Ok(record) => Some(record.executor_epoch),
            Err(error) => {
                // Never borrow the current epoch after a failed attach. This
                // invocation does not own an already-running controller.
                eprintln!("worker error: {error:#}");
                return Ok(ExitCode::from(1));
            }
        }
    } else {
        None
    };

    match worker_body(
        config_path,
        &args,
        &paths,
        &store,
        worker_epoch,
        storage,
        cancel,
    )
    .await
    {
        Ok(code) => Ok(code),
        Err(error) => {
            if durable {
                // Persist only a bounded classification. The full diagnostic
                // belongs in task.log, never in task metadata.
                let settlement = worker_epoch
                    .ok_or_else(|| anyhow!("durable worker lost its executor epoch"))
                    .and_then(|epoch| {
                        settle_current_task(
                            &store,
                            &args.id,
                            epoch,
                            TaskSettlement::Failed {
                                code: FailureCode::Configuration,
                                ledger_run_id: None,
                            },
                        )
                    });
                if let Err(settlement_error) = settlement {
                    eprintln!(
                        "worker metadata settlement failed for {}: {settlement_error:#}",
                        args.id
                    );
                }
            } else {
                // Read-only compatibility for pre-SQLite jobs. New tasks never
                // write status.json.
                let pid = std::process::id() as i32;
                let pgid = pgid_of(pid).unwrap_or(pid);
                let mut status = StatusFile::running(&args.id, pid, pgid, "-", None);
                status.state = TaskState::Error;
                status.finished_at = Some(chrono::Utc::now());
                status.error = Some(format!("{error:#}"));
                let _ = paths.write_status(&status);
            }
            eprintln!("worker error: {error:#}");
            Ok(ExitCode::from(1))
        }
    }
}

/// The worker's real work, factored out so [`run_worker`] can guarantee a
/// terminal status on any failure. Returns the process exit code on the happy
/// path (0 on success, 1 on a non-success terminal run); returns `Err` for any
/// setup or dispatch failure, which the caller records as a terminal `error`.
///
/// The cancellation token is passed in already-armed so the ordering invariant
/// (handler before `running`) is owned by the caller.
async fn worker_body(
    config_path: Option<PathBuf>,
    args: &WorkerArgs,
    paths: &TaskPaths,
    store: &SqliteTaskStore,
    worker_epoch: Option<u64>,
    storage: StoragePaths,
    cancel: CancellationToken,
) -> Result<ExitCode> {
    let durable = worker_epoch.is_some();
    let job = read_worker_job(args, paths)
        .with_context(|| format!("read worker request for {}", args.id))?;
    if job.run_id != args.id {
        bail!(
            "worker request run id mismatch: argv names `{}`, request names `{}`",
            args.id,
            job.run_id
        );
    }

    // The job's own recorded config override wins over any inherited flag; the
    // parent always sends it (possibly `None`).
    let config_path = job.config.clone().or(config_path);

    let loaded = load_config(config_path.as_deref())
        .with_context(|| format!("load config for {}", args.id))?;
    let mut task = task_from_job(&job)?;
    task.harness_lifecycle_reporter = Some(detached_harness_reporter(paths.clone()));
    // Reconstruct the exact parent-approved chain. Auto routes replay the same
    // frontier filtering and effort override; explicit routes resolve normally.
    let chain = if task.labels.get("routing.mode").map(String::as_str) == Some("auto") {
        vyane_service::replay_recorded_auto_chain(&loaded, &job.target, &task.labels)
    } else {
        resolve_target_chain(&loaded, &job.target)
    }
    .with_context(|| format!("resolve config/target for {}", args.id))?;
    verify_target_snapshot(&job.target_snapshot, &chain, &loaded.config)?;
    let runtime = Runtime::new(loaded.config, storage)?;
    let inherited_pin = match job.capability_plan.as_ref() {
        Some(plan) if plan.requires_inherited_workdir => {
            let canonical_path = plan.canonical_workdir.as_deref().ok_or_else(|| {
                anyhow!("frozen capability plan requires a workdir without an audit path")
            })?;
            let identity = plan.workdir_identity.as_ref().ok_or_else(|| {
                anyhow!("frozen capability plan requires a workdir without an identity")
            })?;
            Some(
                crate::task::spawn::take_inherited_pinned_workdir(canonical_path, identity)
                    .with_context(|| format!("take inherited workdir for {}", args.id))?,
            )
        }
        _ => None,
    };
    let prepared = match inherited_pin {
        Some(pinned) => {
            runtime
                .dispatcher
                .prepare_with_pinned_workdir(&task, chain.clone(), pinned)
        }
        None => runtime.dispatcher.prepare(&task, chain.clone()),
    }
    .with_context(|| format!("re-admit capability plan for {}", args.id))?;
    if let Some(expected) = job.capability_plan.as_ref() {
        prepared
            .verify_capability_snapshot(expected)
            .with_context(|| format!("verify frozen capability plan for {}", args.id))?;
    }
    runtime
        .dispatcher
        .validate_session_admission(&task, &prepared)
        .await
        .with_context(|| format!("validate session admission for {}", args.id))?;

    let workdir = job
        .workdir
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned());

    // The status target is a best-effort label: the first resolved target's
    // identity reads better than the raw selector, but falls back to it.
    let target_label = chain
        .first()
        .map(|bound| bound.target.to_string())
        .unwrap_or_else(|| job.target.clone());
    let pid = std::process::id() as i32;
    let pgid = pgid_of(pid).unwrap_or(pid);

    // Announce `running` up front (atomic write) so `task list/status` observe
    // the run the instant the worker is live. The cancellation handler is
    // already armed by `run_worker`, so any observable `running` implies a
    // canceller's SIGTERM will be caught (ordering invariant 1).
    let running = (!durable)
        .then(|| StatusFile::running(&job.run_id, pid, pgid, &target_label, workdir.clone()));
    if let Some(status) = &running {
        paths.write_status(status)?;
    }

    // From here, a failure still lands as a terminal `error` (invariant 2): the
    // `?` bubbles up to `run_worker`, which overwrites this `running` file.
    let outcome = runtime
        .dispatcher
        .dispatch_prepared(&task, prepared, cancel)
        .await?;
    let record = outcome.record;
    let output = outcome.output;

    // Persist the answer (if any) beside the status, then finalize status.
    if let Some(text) = output.as_deref() {
        if let Err(error) = paths.write_output(text) {
            // The ledger/run outcome remains authoritative even if its optional
            // convenience artifact cannot be written.
            eprintln!("write {}: {error:#}", paths.output().display());
        }
    }

    if durable {
        let worker_epoch =
            worker_epoch.ok_or_else(|| anyhow!("durable worker has no attached executor epoch"))?;
        settle_current_task(
            store,
            &args.id,
            worker_epoch,
            settlement_from_run(record.status, record.run_id.clone()),
        )?;
    } else if let Some(mut final_status) = running {
        final_status.state = run_status_to_task_state(record.status);
        final_status.finished_at = Some(record.finished_at);
        final_status.ledger_run_id = Some(record.run_id.clone());
        final_status.error = record.error.clone();
        paths.write_status(&final_status)?;
    }

    Ok(if record.status == RunStatus::Success {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

fn detached_harness_reporter(paths: TaskPaths) -> HarnessLifecycleReporter {
    let active = Arc::new(std::sync::Mutex::new(BTreeMap::<
        (i32, i32),
        HarnessControllerFile,
    >::new()));
    HarnessLifecycleReporter::new(move |event| match event {
        HarnessLifecycleEvent::Started { pid, pgid } => {
            let pid = i32::try_from(pid).map_err(|_| {
                VyaneError::new(
                    ErrorKind::Io,
                    "nested harness pid exceeded the supported range",
                )
            })?;
            let Some(birth_fingerprint) = process_birth_fingerprint(pid) else {
                return Err(VyaneError::new(
                    ErrorKind::Io,
                    "nested harness process identity was unavailable",
                ));
            };
            let controller = HarnessControllerFile {
                schema: HARNESS_CONTROLLER_SCHEMA,
                pid,
                pgid,
                started_at: chrono::Utc::now(),
                birth_fingerprint: Some(birth_fingerprint),
            };
            paths
                .write_harness_controller(&controller)
                .map_err(|error| {
                    eprintln!(
                        "nested harness controller write failed at {}: {error:#}",
                        paths.harness_controller().display()
                    );
                    VyaneError::new(ErrorKind::Io, "nested harness controller write failed")
                })?;
            active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert((pid, pgid), controller);
            Ok(())
        }
        HarnessLifecycleEvent::Stopped {
            pid,
            pgid,
            group_empty,
        } => {
            let Ok(pid) = i32::try_from(pid) else {
                return Ok(());
            };
            let expected = active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&(pid, pgid));
            let Some(expected) = expected else {
                return Ok(());
            };
            if !group_empty {
                // Drop/unwind has issued SIGKILL but cannot synchronously reap
                // the full group. Retain the exact sentinel sidecar so a later
                // task controller can diagnose or finish cleanup.
                return Ok(());
            }
            if let Err(error) = paths.remove_harness_controller(&expected) {
                // The group is already dead. Leave a stale exact-identity
                // sidecar for the next canceller/read to clear safely.
                eprintln!(
                    "nested harness controller cleanup failed at {}: {error:#}",
                    paths.harness_controller().display()
                );
            }
            Ok(())
        }
    })
}

/// Upper bound for the one-shot stdin request. The public CLI is constrained by
/// OS argv limits already, but the hidden worker must not accept unbounded input
/// when invoked directly.
const MAX_WORKER_ENVELOPE_BYTES: u64 = 32 * 1024 * 1024;

fn read_worker_job(args: &WorkerArgs, paths: &TaskPaths) -> Result<JobSpec> {
    let mut bytes = Vec::new();
    std::io::stdin()
        .lock()
        .take(MAX_WORKER_ENVELOPE_BYTES + 1)
        .read_to_end(&mut bytes)
        .context("read worker envelope from stdin")?;

    if bytes.is_empty() {
        // Compatibility with pre-envelope tasks and older parents, whose worker
        // stdin was `/dev/null` and whose request lives in job.json.
        return paths
            .read_job()
            .context("stdin was empty and no readable legacy job.json was available");
    }
    if bytes.len() as u64 > MAX_WORKER_ENVELOPE_BYTES {
        bail!(
            "worker stdin envelope exceeds {} bytes",
            MAX_WORKER_ENVELOPE_BYTES
        );
    }

    let envelope: WorkerEnvelope =
        serde_json::from_slice(&bytes).context("parse worker stdin envelope")?;
    if envelope.schema != WORKER_ENVELOPE_SCHEMA {
        bail!(
            "unsupported worker envelope schema {} (expected {})",
            envelope.schema,
            WORKER_ENVELOPE_SCHEMA
        );
    }
    if envelope.job.run_id != args.id {
        bail!(
            "worker envelope run id mismatch: argv names `{}`, envelope names `{}`",
            args.id,
            envelope.job.run_id
        );
    }
    Ok(envelope.job)
}

fn attach_detached_controller(
    store: &SqliteTaskStore,
    id: &str,
    pid: i32,
    pgid: i32,
) -> Result<TaskRecord> {
    loop {
        let record = store
            .get(LOCAL_TASK_OWNER, id)?
            .ok_or_else(|| anyhow!("durable task `{id}` was not found"))?;
        if !is_local_detached_dispatch(&record) {
            bail!("task `{id}` is not a local detached dispatch");
        }
        if record.state != DurableTaskState::Queued {
            bail!(
                "cannot attach detached worker to task `{id}` while it is {}",
                record.state
            );
        }
        let controller = ControllerRef::ProcessGroup {
            pid,
            pgid,
            started_at: chrono::Utc::now(),
            birth_fingerprint: process_birth_fingerprint(pid),
        };
        match store.attach_controller(
            LOCAL_TASK_OWNER,
            id,
            record.revision,
            record.executor_epoch,
            controller,
            None,
            chrono::Utc::now(),
        ) {
            Ok(attached) => return Ok(attached),
            Err(TaskStoreError::Conflict { .. }) => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

fn settlement_from_run(status: RunStatus, ledger_run_id: String) -> TaskSettlement {
    let ledger_run_id = Some(ledger_run_id);
    match status {
        RunStatus::Success => TaskSettlement::Succeeded { ledger_run_id },
        RunStatus::Error => TaskSettlement::Failed {
            code: FailureCode::DispatchFailed,
            ledger_run_id,
        },
        RunStatus::Timeout => TaskSettlement::TimedOut { ledger_run_id },
        RunStatus::Cancelled => TaskSettlement::Cancelled { ledger_run_id },
    }
}

/// Settle against the latest revision while preserving executor-epoch
/// ownership. A concurrent cancellation may advance the revision, but a newer
/// executor epoch must never be overwritten by this worker.
fn settle_current_task(
    store: &SqliteTaskStore,
    id: &str,
    expected_executor_epoch: u64,
    settlement: TaskSettlement,
) -> Result<TaskRecord> {
    loop {
        let record = store
            .get(LOCAL_TASK_OWNER, id)?
            .ok_or_else(|| anyhow!("durable task `{id}` was not found"))?;
        if expected_executor_epoch != record.executor_epoch {
            bail!(
                "task `{id}` executor epoch advanced from {expected_executor_epoch} to {}; refusing stale settlement",
                record.executor_epoch
            );
        }
        if record.state.is_terminal() {
            return Ok(record);
        }
        match store.settle(
            LOCAL_TASK_OWNER,
            id,
            record.revision,
            record.executor_epoch,
            settlement.clone(),
            chrono::Utc::now(),
        ) {
            Ok(settled) => return Ok(settled),
            Err(TaskStoreError::Conflict { .. }) => continue,
            Err(TaskStoreError::InvalidState { .. }) => {
                let latest = store
                    .get(LOCAL_TASK_OWNER, id)?
                    .ok_or_else(|| anyhow!("durable task `{id}` was not found"))?;
                if latest.state.is_terminal() {
                    return Ok(latest);
                }
                return Err(anyhow!(
                    "task `{id}` remained {} while settlement was rejected",
                    latest.state
                ));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn interrupt_current_task(
    store: &SqliteTaskStore,
    id: &str,
    expected_executor_epoch: Option<u64>,
    code: FailureCode,
) -> Result<TaskRecord> {
    loop {
        let record = store
            .get(LOCAL_TASK_OWNER, id)?
            .ok_or_else(|| anyhow!("durable task `{id}` was not found"))?;
        if record.state.is_terminal() {
            return Ok(record);
        }
        if expected_executor_epoch.is_some_and(|epoch| epoch != record.executor_epoch) {
            return Ok(record);
        }
        match store.interrupt(
            LOCAL_TASK_OWNER,
            id,
            record.revision,
            record.executor_epoch,
            code,
            chrono::Utc::now(),
        ) {
            Ok(interrupted) => return Ok(interrupted),
            Err(TaskStoreError::Conflict { .. }) => continue,
            Err(TaskStoreError::InvalidState { .. }) => {
                let latest = store
                    .get(LOCAL_TASK_OWNER, id)?
                    .ok_or_else(|| anyhow!("durable task `{id}` was not found"))?;
                if latest.state.is_terminal()
                    || expected_executor_epoch.is_some_and(|epoch| epoch != latest.executor_epoch)
                {
                    return Ok(latest);
                }
                return Err(anyhow!(
                    "task `{id}` remained {} while interruption was rejected",
                    latest.state
                ));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn run_task_list(args: TaskListArgs) -> Result<ExitCode> {
    let (storage, tasks_dir) = crate::app::resolve_paths_with_tasks()?;
    let store = SqliteTaskStore::open(storage.task_metadata_db_path())?;
    let all_records = list_all_local_tasks(&store)?;
    let durable_ids: HashSet<String> = all_records.iter().map(|record| record.id.clone()).collect();
    let mut records: Vec<TaskRecord> = all_records
        .into_iter()
        .filter(is_local_detached_dispatch)
        .collect();
    for record in &mut records {
        let paths = TaskPaths::new(&tasks_dir, &record.id);
        *record = reconcile_detached_process(&store, &paths, record.clone())?;
    }
    let mut rows: Vec<TaskRow> = records.iter().map(TaskRow::from_record).collect();
    rows.extend(
        list_tasks(&tasks_dir, &identity_probe)
            .into_iter()
            .filter(|legacy| !durable_ids.contains(legacy.id.as_str()))
            .map(|legacy| TaskRow::from_legacy(&legacy)),
    );
    rows.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| right.id.cmp(&left.id))
    });

    if args.json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
    } else if rows.is_empty() {
        println!("no detached runs");
    } else {
        crate::output::print_task_table(&rows);
    }
    Ok(ExitCode::SUCCESS)
}

fn list_all_local_tasks(store: &SqliteTaskStore) -> Result<Vec<TaskRecord>> {
    let mut query = TaskQuery {
        kinds: vec![TaskKind::Dispatch],
        origins: vec![TaskOrigin::CliDetached],
        limit: 1_000,
        ..TaskQuery::default()
    };
    let mut records = Vec::new();
    loop {
        let page = store.list(LOCAL_TASK_OWNER, &query)?;
        records.extend(page.items);
        let Some(cursor) = page.next_cursor else {
            break;
        };
        query.cursor = Some(cursor);
    }
    Ok(records)
}

fn reconcile_detached_process(
    store: &SqliteTaskStore,
    paths: &TaskPaths,
    record: TaskRecord,
) -> Result<TaskRecord> {
    if !is_local_detached_dispatch(&record) {
        return Ok(record);
    }
    if record.state == DurableTaskState::Queued {
        // A queued row has no controller evidence. Its age alone cannot prove
        // the submitter died: a slow pipe handoff or a contended SQLite attach
        // may still be legitimate. Keep reads non-destructive; queued tasks are
        // observable and can be cancelled explicitly by exact CAS.
        return Ok(record);
    }
    if !matches!(
        record.state,
        DurableTaskState::Running | DurableTaskState::Cancelling
    ) {
        return Ok(record);
    }
    let Some(ControllerRef::ProcessGroup {
        pid,
        pgid,
        started_at,
        birth_fingerprint,
    }) = record.controller.clone()
    else {
        // Missing controller evidence is degraded control, not proof of death.
        // A status/list read must never swallow a real worker's later success.
        return Ok(record);
    };
    let failure_code =
        match verify_controller_identity(pid, pgid, started_at, birth_fingerprint.as_deref()) {
            IdentityCheck::Match => None,
            IdentityCheck::Dead
            | IdentityCheck::Mismatch(
                "process group mismatch"
                | "process start time mismatch"
                | "process birth fingerprint mismatch",
            ) => Some(FailureCode::WorkerLost),
            // An unavailable `ps`/`procfs` observation or an old record without a
            // fingerprint is not affirmative evidence that the worker changed.
            // Cancellation still fails closed, but reads remain non-destructive.
            IdentityCheck::Mismatch(_) => None,
        };
    let Some(failure_code) = failure_code else {
        return Ok(record);
    };
    // The outer worker and a CLI harness deliberately live in different
    // process groups. A dead worker is not proof that its nested harness is
    // gone: SIGKILL and crashes bypass the reporter's Drop cleanup. Keep the
    // row controllable while a sidecar may still name a live group; status/list
    // remain non-signalling reads and explicit `task cancel` performs cleanup.
    if nested_harness_group_pending(paths) {
        return Ok(record);
    }
    interrupt_current_task(store, &record.id, Some(record.executor_epoch), failure_code)
}

async fn run_task_status(args: TaskStatusArgs) -> Result<ExitCode> {
    let (storage, tasks_dir) = crate::app::resolve_paths_with_tasks()?;
    let paths = TaskPaths::new(&tasks_dir, &args.id);
    let store = SqliteTaskStore::open(storage.task_metadata_db_path())?;

    if let Some(record) = store.get(LOCAL_TASK_OWNER, &args.id)? {
        if !is_local_detached_dispatch(&record) {
            eprintln!("{} is not a local detached dispatch", args.id);
            return Ok(ExitCode::from(1));
        }
        let record = reconcile_detached_process(&store, &paths, record)?;
        if args.output {
            return match paths.read_output() {
                Some(text) => {
                    print!("{text}");
                    if !text.ends_with('\n') {
                        println!();
                    }
                    Ok(ExitCode::SUCCESS)
                }
                None => {
                    eprintln!("no output recorded for {}", args.id);
                    Ok(ExitCode::from(1))
                }
            };
        }
        let log_tail = paths.tail_log(10);
        if args.json {
            println!(
                "{}",
                serde_json::to_string_pretty(&DurableTaskStatusJson {
                    task: &record,
                    log_tail: &log_tail,
                })?
            );
        } else {
            crate::output::print_durable_task_status(&record, &log_tail);
        }
        let ok = matches!(
            record.state,
            DurableTaskState::Queued
                | DurableTaskState::Running
                | DurableTaskState::Cancelling
                | DurableTaskState::Succeeded
        );
        return Ok(if ok {
            ExitCode::SUCCESS
        } else {
            ExitCode::from(1)
        });
    }

    let status = match paths.read_status() {
        Ok(status) => status,
        Err(_) => {
            // No readable status. Distinguish a stale submission directory (the
            // worker never published state) from a genuinely unknown id. New
            // tasks intentionally have no job.json, so use scaffold metadata.
            if paths.scaffold_mtime().is_some() {
                eprintln!(
                    "{}: stale — worker never wrote status (spawn or stdin handoff may have failed); see {}",
                    args.id,
                    paths.log().display()
                );
                return Ok(ExitCode::from(1));
            }
            eprintln!("no such detached run: {}", args.id);
            return Ok(ExitCode::from(1));
        }
    };
    let displayed = interpret_state(&status, &identity_probe);

    // `--output` prints the captured answer and nothing else.
    if args.output {
        match paths.read_output() {
            Some(text) => {
                print!("{text}");
                if !text.ends_with('\n') {
                    println!();
                }
                return Ok(ExitCode::SUCCESS);
            }
            None => {
                eprintln!("no output recorded for {}", args.id);
                return Ok(ExitCode::from(1));
            }
        }
    }

    let log_tail = paths.tail_log(10);

    if args.json {
        let view = crate::output::TaskStatusJson {
            status: &status,
            displayed_state: displayed.as_str(),
            log_tail: &log_tail,
        };
        println!("{}", serde_json::to_string_pretty(&view)?);
    } else {
        crate::output::print_task_status(&status, displayed, &log_tail);
    }

    // A run that finished unhappily, or an orphan, exits nonzero so scripts can
    // branch on it; `running` and `success` exit 0.
    let ok = matches!(displayed, TaskState::Running | TaskState::Success);
    Ok(if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

async fn run_task_cancel(args: TaskCancelArgs) -> Result<ExitCode> {
    let (storage, tasks_dir) = crate::app::resolve_paths_with_tasks()?;
    let paths = TaskPaths::new(&tasks_dir, &args.id);
    let store = SqliteTaskStore::open(storage.task_metadata_db_path())?;
    if let Some(record) = store.get(LOCAL_TASK_OWNER, &args.id)? {
        return run_durable_task_cancel(&args.id, &paths, &store, record).await;
    }

    let status = match paths.read_status() {
        Ok(status) => status,
        Err(_) => {
            eprintln!("no such detached run: {}", args.id);
            return Ok(ExitCode::from(1));
        }
    };

    // Metadata may already be terminal even though a worker crash bypassed the
    // nested harness reporter's cleanup. Explicit cancel remains the recovery
    // entrypoint for any exact sidecar left behind.
    if status.state.is_terminal() {
        if let Err(error) = cleanup_terminal_legacy_processes(&status, &paths).await {
            eprintln!(
                "{}: task is already {}, but process cleanup failed: {error:#}",
                args.id,
                status.state.as_str()
            );
            return Ok(ExitCode::from(1));
        }
        println!("{} already {}", args.id, status.state.as_str());
        return Ok(ExitCode::SUCCESS);
    }

    // Process-identity gate — the guard against pid reuse. Between the worker
    // recording its pid and this cancel, the OS may have recycled that pid onto
    // an unrelated process; blindly signalling it (worse, its whole group by
    // negative pid) could kill something we never launched. Only signal if the
    // process now holding the pid still validates as *this* worker: its process
    // group matches the recorded `pgid` AND its start time matches the recorded
    // `started_at` (see `verify_identity`).
    match verify_identity(status.pid, status.pgid, status.started_at) {
        IdentityCheck::Match => {}
        IdentityCheck::Dead => {
            if let Err(error) = force_cleanup_orphaned_nested_harness(&paths).await {
                eprintln!(
                    "{}: worker is gone and nested harness cleanup failed: {error:#}",
                    args.id
                );
                return Ok(ExitCode::from(1));
            }
            eprintln!(
                "{}: worker process is gone (died); nested harness cleanup complete",
                args.id
            );
            return Ok(ExitCode::from(1));
        }
        IdentityCheck::Mismatch(reason) => {
            if let Err(error) = force_cleanup_orphaned_nested_harness(&paths).await {
                eprintln!(
                    "{}: outer identity mismatch ({reason}) and nested harness cleanup failed: {error:#}",
                    args.id
                );
                return Ok(ExitCode::from(1));
            }
            eprintln!(
                "{}: process identity mismatch ({reason}; pid likely reused); refusing to signal",
                args.id
            );
            return Ok(ExitCode::from(1));
        }
    }

    // Identity confirmed: `status.pgid` is the live worker's group. SIGTERM the
    // whole group — the worker catches it and finalizes; any harness
    // grandchildren it spawned die with the group.
    let pgid = status.pgid;
    signal_group(pgid, SIGTERM);

    // First let the worker drive the nested harness's own cancellation path.
    // Only once that distinct budget is exhausted do we use the sidecar for an
    // independently verified SIGKILL.
    if wait_until(
        NESTED_HARNESS_CANCEL_GRACE,
        Duration::from_millis(100),
        || !nested_harness_group_pending(&paths),
    )
    .await
    .is_none()
    {
        // Do not require the group to look empty yet: if the outer worker is
        // SIGSTOP'ed, a killed nested leader remains its unreaped zombie until
        // the outer worker resumes or is force-killed below.
        if let Err(error) = signal_nested_harness(&paths, SIGKILL) {
            eprintln!(
                "{}: nested harness identity unavailable before SIGKILL: {error:#}",
                args.id
            );
            return Ok(ExitCode::from(1));
        }
    }

    // The nested reporter finished or an exact SIGKILL was delivered. Give the
    // worker a separate window to reap it and append ledger/terminal metadata.
    if wait_until(WORKER_SETTLEMENT_GRACE, Duration::from_millis(100), || {
        !process_group_alive(pgid)
    })
    .await
    .is_none()
    {
        // A numeric process-group id may be reused after its leader exits.
        // Revalidate the original worker at the exact escalation boundary so
        // a delayed SIGKILL can never target an unrelated replacement group.
        match verify_identity(status.pid, status.pgid, status.started_at) {
            IdentityCheck::Match => signal_group(pgid, SIGKILL),
            IdentityCheck::Dead => eprintln!(
                "{}: worker leader exited but its group remains; refusing unsafe SIGKILL escalation",
                args.id
            ),
            IdentityCheck::Mismatch(reason) => eprintln!(
                "{}: process identity changed before SIGKILL ({reason}); refusing escalation",
                args.id
            ),
        }
    }

    if wait_until(
        FORCED_PROCESS_CLEANUP_GRACE,
        Duration::from_millis(50),
        || !process_group_alive(pgid) && !nested_harness_group_pending(&paths),
    )
    .await
    .is_none()
    {
        eprintln!(
            "{}: cancellation did not finish every owned process group",
            args.id
        );
        return Ok(ExitCode::from(1));
    }

    // Separately, wait up to 2s for the worker to write its *final* status.
    // (A clean SIGTERM finalize writes `cancelled`; a SIGKILL leaves it
    // `running`, which read-side interpretation later shows as `died`.)
    let finalized = wait_until(Duration::from_secs(2), Duration::from_millis(50), || {
        paths
            .read_status()
            .map(|s| s.state.is_terminal())
            .unwrap_or(false)
    })
    .await
    .is_some();

    if finalized {
        let final_state = paths
            .read_status()
            .map(|s| s.state.as_str().to_string())
            .unwrap_or_else(|_| "cancelled".to_string());
        println!("{} {}", args.id, final_state);
        Ok(ExitCode::SUCCESS)
    } else {
        eprintln!("{}: kill delivered; worker did not finalize", args.id);
        Ok(ExitCode::from(1))
    }
}

async fn run_durable_task_cancel(
    id: &str,
    paths: &TaskPaths,
    store: &SqliteTaskStore,
    mut record: TaskRecord,
) -> Result<ExitCode> {
    if !is_local_detached_dispatch(&record) {
        eprintln!("{id} is not a local detached dispatch; cancel it through its owning frontend");
        return Ok(ExitCode::from(1));
    }
    if record.state.is_terminal() {
        return Ok(finish_observed_terminal_cancel(id, paths, &record).await);
    }

    if record.state == DurableTaskState::Queued {
        record = request_cancel_current(store, id, record.executor_epoch)?;
        if record.state.is_terminal() {
            return Ok(finish_observed_terminal_cancel(id, paths, &record).await);
        }
        // An attach may win the queued cancellation CAS. In that case continue
        // through controller verification instead of reporting false success.
    }

    let Some(ControllerRef::ProcessGroup {
        pid,
        pgid,
        started_at,
        birth_fingerprint,
    }) = record.controller.clone()
    else {
        return refuse_unverifiable_cancel(
            store,
            id,
            paths,
            "detached process controller was not recorded",
            "before signal delivery",
        )
        .await;
    };
    let controller_epoch = record.executor_epoch;

    match verify_controller_identity(pid, pgid, started_at, birth_fingerprint.as_deref()) {
        IdentityCheck::Match => {}
        IdentityCheck::Dead => {
            if let Err(error) = force_cleanup_orphaned_nested_harness(paths).await {
                return refuse_unverifiable_cancel(
                    store,
                    id,
                    paths,
                    &format!("worker is gone and nested harness cleanup failed: {error:#}"),
                    "after worker loss",
                )
                .await;
            }
            let interrupted = interrupt_current_task(
                store,
                id,
                Some(record.executor_epoch),
                FailureCode::WorkerLost,
            )?;
            return Ok(report_cancel_interruption(
                id,
                paths,
                "worker process is gone",
                &interrupted,
            )
            .await);
        }
        IdentityCheck::Mismatch(reason) => {
            if !is_proven_identity_mismatch(reason) {
                return refuse_unverifiable_cancel(
                    store,
                    id,
                    paths,
                    reason,
                    "before signal delivery",
                )
                .await;
            }
            if let Err(error) = force_cleanup_orphaned_nested_harness(paths).await {
                return refuse_unverifiable_cancel(
                    store,
                    id,
                    paths,
                    &format!(
                        "outer process identity mismatch ({reason}) and nested harness cleanup failed: {error:#}"
                    ),
                    "after proven worker replacement",
                )
                .await;
            }
            let interrupted = interrupt_current_task(
                store,
                id,
                Some(record.executor_epoch),
                FailureCode::ControlUnavailable,
            )?;
            return Ok(report_cancel_interruption(
                id,
                paths,
                &format!("process identity mismatch ({reason}); refusing to signal"),
                &interrupted,
            )
            .await);
        }
    }

    if record.state == DurableTaskState::Running {
        record = request_cancel_current(store, id, record.executor_epoch)?;
        if record.state.is_terminal() {
            return Ok(finish_observed_terminal_cancel(id, paths, &record).await);
        }
        if record.executor_epoch != controller_epoch {
            eprintln!("{id}: executor ownership changed while cancellation was requested");
            return Ok(ExitCode::from(1));
        }
    }

    match verify_controller_identity(pid, pgid, started_at, birth_fingerprint.as_deref()) {
        IdentityCheck::Match => {}
        IdentityCheck::Dead => {
            if let Err(error) = force_cleanup_orphaned_nested_harness(paths).await {
                return refuse_unverifiable_cancel(
                    store,
                    id,
                    paths,
                    &format!(
                        "worker exited before signal delivery and nested harness cleanup failed: {error:#}"
                    ),
                    "after worker loss",
                )
                .await;
            }
            let interrupted =
                interrupt_current_task(store, id, Some(controller_epoch), FailureCode::WorkerLost)?;
            return Ok(report_cancel_interruption(
                id,
                paths,
                "worker exited before signal delivery",
                &interrupted,
            )
            .await);
        }
        IdentityCheck::Mismatch(reason) => {
            if !is_proven_identity_mismatch(reason) {
                return refuse_unverifiable_cancel(
                    store,
                    id,
                    paths,
                    reason,
                    "before signal delivery",
                )
                .await;
            }
            if let Err(error) = force_cleanup_orphaned_nested_harness(paths).await {
                return refuse_unverifiable_cancel(
                    store,
                    id,
                    paths,
                    &format!(
                        "outer process identity changed ({reason}) and nested harness cleanup failed: {error:#}"
                    ),
                    "after proven worker replacement",
                )
                .await;
            }
            let interrupted = interrupt_current_task(
                store,
                id,
                Some(controller_epoch),
                FailureCode::ControlUnavailable,
            )?;
            return Ok(report_cancel_interruption(
                id,
                paths,
                &format!("process identity changed before signal delivery ({reason})"),
                &interrupted,
            )
            .await);
        }
    }

    signal_group(pgid, SIGTERM);
    // Do not signal the nested harness independently during the graceful
    // phase. The worker's SIGTERM handler sets the cancellation token, letting
    // the harness classify the outcome as Cancelled. A direct nested TERM could
    // win the child-wait race and be misclassified as HarnessFailed. The
    // sidecar is reserved for verified SIGKILL escalation if the worker cannot
    // forward cancellation within the grace period.
    if wait_until(
        NESTED_HARNESS_CANCEL_GRACE,
        Duration::from_millis(100),
        || !nested_harness_group_pending(paths),
    )
    .await
    .is_none()
    {
        // A stopped outer worker cannot reap a SIGKILLed nested leader, so the
        // group may remain visible as a zombie until outer escalation. Signal
        // exact identity here and enforce final two-group emptiness below.
        if let Err(error) = signal_nested_harness(paths, SIGKILL) {
            return refuse_unverifiable_cancel(
                store,
                id,
                paths,
                &format!("nested harness controller unavailable: {error:#}"),
                "before SIGKILL escalation",
            )
            .await;
        }
    }

    // The harness reporter finished or an exact SIGKILL was delivered. Keep a
    // separate budget for reaping, ledger, and SQLite settlement so the outer
    // worker is not killed at the exact edge of the inner cleanup window.
    if wait_until(WORKER_SETTLEMENT_GRACE, Duration::from_millis(100), || {
        !process_group_alive(pgid)
    })
    .await
    .is_none()
    {
        match verify_controller_identity(pid, pgid, started_at, birth_fingerprint.as_deref()) {
            IdentityCheck::Match => signal_group(pgid, SIGKILL),
            IdentityCheck::Dead => {
                let interrupted = interrupt_current_task(
                    store,
                    id,
                    Some(controller_epoch),
                    FailureCode::ControlUnavailable,
                )?;
                return Ok(
                    report_cancel_interruption(
                        id,
                        paths,
                        "worker leader exited but its group remains; refusing unsafe SIGKILL escalation",
                        &interrupted,
                    )
                    .await,
                );
            }
            IdentityCheck::Mismatch(reason) => {
                if !is_proven_identity_mismatch(reason) {
                    return refuse_unverifiable_cancel(
                        store,
                        id,
                        paths,
                        reason,
                        "before SIGKILL escalation",
                    )
                    .await;
                }
                let interrupted = interrupt_current_task(
                    store,
                    id,
                    Some(controller_epoch),
                    FailureCode::ControlUnavailable,
                )?;
                return Ok(report_cancel_interruption(
                    id,
                    paths,
                    &format!(
                        "process identity changed before SIGKILL ({reason}); refusing escalation"
                    ),
                    &interrupted,
                )
                .await);
            }
        }
    }

    let process_cleanup = wait_until(
        FORCED_PROCESS_CLEANUP_GRACE,
        Duration::from_millis(50),
        || !process_group_alive(pgid) && !nested_harness_group_pending(paths),
    )
    .await
    .is_some();
    if !process_cleanup {
        let interrupted = interrupt_current_task(
            store,
            id,
            Some(controller_epoch),
            FailureCode::ControlUnavailable,
        )?;
        return Ok(report_cancel_interruption(
            id,
            paths,
            "verified signal delivery did not finish every owned process group",
            &interrupted,
        )
        .await);
    }

    let finalized = wait_until(Duration::from_secs(2), Duration::from_millis(50), || {
        store
            .get(LOCAL_TASK_OWNER, id)
            .ok()
            .flatten()
            .is_some_and(|current| current.state.is_terminal())
    })
    .await
    .is_some();

    if finalized {
        let current = store
            .get(LOCAL_TASK_OWNER, id)?
            .ok_or_else(|| anyhow!("durable task `{id}` disappeared after cancellation"))?;
        Ok(finish_observed_terminal_cancel(id, paths, &current).await)
    } else {
        let interrupted = interrupt_current_task(
            store,
            id,
            Some(record.executor_epoch),
            FailureCode::ControlUnavailable,
        )?;
        Ok(report_cancel_interruption(
            id,
            paths,
            "kill delivered; worker did not settle metadata",
            &interrupted,
        )
        .await)
    }
}

/// A control-side interruption can race the worker's own final settlement. A
/// natural terminal result is an idempotent successful no-op; only an
/// `interrupted` record produced by the failed control path exits nonzero.
async fn report_cancel_interruption(
    id: &str,
    paths: &TaskPaths,
    diagnostic: &str,
    record: &TaskRecord,
) -> ExitCode {
    if record.state.is_terminal() && !cleanup_observed_terminal(id, paths, record).await {
        return ExitCode::from(1);
    }
    if record.state != DurableTaskState::Interrupted && record.state.is_terminal() {
        println!("{id} already {}", record.state);
        ExitCode::SUCCESS
    } else {
        eprintln!("{id}: {diagnostic}; task is {}", record.state);
        ExitCode::from(1)
    }
}

fn is_proven_identity_mismatch(reason: &str) -> bool {
    matches!(
        reason,
        "process group mismatch"
            | "process start time mismatch"
            | "process birth fingerprint mismatch"
    )
}

/// Process control fails closed when identity cannot be read, but absence of
/// evidence is not evidence of worker loss. Preserve the active state so a
/// later retry or the real worker's settlement remains authoritative.
async fn refuse_unverifiable_cancel(
    store: &SqliteTaskStore,
    id: &str,
    paths: &TaskPaths,
    reason: &str,
    phase: &str,
) -> Result<ExitCode> {
    let current = store
        .get(LOCAL_TASK_OWNER, id)?
        .ok_or_else(|| anyhow!("durable task `{id}` disappeared during cancellation"))?;
    if current.state.is_terminal() {
        return Ok(finish_observed_terminal_cancel(id, paths, &current).await);
    }
    eprintln!(
        "{id}: process identity unavailable {phase} ({reason}); refusing control; task remains {}",
        current.state
    );
    Ok(ExitCode::from(1))
}

/// Every terminal snapshot observed by detached cancellation is also a cleanup
/// obligation. Terminal metadata can race ahead of process exit (or be written
/// by an external recovery path), so success is reported only after both exact
/// controllers have been reconciled.
async fn finish_observed_terminal_cancel(
    id: &str,
    paths: &TaskPaths,
    record: &TaskRecord,
) -> ExitCode {
    if !cleanup_observed_terminal(id, paths, record).await {
        return ExitCode::from(1);
    }
    println!("{id} already {}", record.state);
    ExitCode::SUCCESS
}

async fn cleanup_observed_terminal(id: &str, paths: &TaskPaths, record: &TaskRecord) -> bool {
    debug_assert!(record.state.is_terminal());
    match cleanup_terminal_detached_processes(record, paths).await {
        Ok(()) => true,
        Err(error) => {
            eprintln!(
                "{id}: task is already {}, but process cleanup failed: {error:#}",
                record.state
            );
            false
        }
    }
}

/// Terminal metadata is not proof that its recorded OS controllers are gone:
/// an older recovery path may have terminalized a SIGSTOP'ed worker. Explicit
/// cancel cleans both exact groups, killing the outer worker as well so a
/// nested zombie can be reparented and reaped before the final emptiness check.
async fn cleanup_terminal_detached_processes(record: &TaskRecord, paths: &TaskPaths) -> Result<()> {
    let outer_pgid = match &record.controller {
        Some(ControllerRef::ProcessGroup {
            pid,
            pgid,
            started_at,
            birth_fingerprint,
        }) => {
            match verify_controller_identity(*pid, *pgid, *started_at, birth_fingerprint.as_deref())
            {
                IdentityCheck::Match => Some(*pgid),
                IdentityCheck::Dead => None,
                IdentityCheck::Mismatch(reason) if is_proven_identity_mismatch(reason) => None,
                IdentityCheck::Mismatch(reason) => {
                    bail!("terminal outer worker identity is unavailable ({reason})")
                }
            }
        }
        Some(ControllerRef::InProcess { .. }) => {
            bail!("detached task has an in-process terminal controller")
        }
        None => None,
    };

    if let Some(pgid) = outer_pgid {
        signal_group(pgid, SIGKILL);
        // A SIGSTOP'ed outer cannot reap a dead nested leader. Give its hard
        // kill a bounded chance to release/reparent that zombie before judging
        // a dead nested sentinel against a still-visible numeric PGID.
        let _ = wait_until(
            FORCED_PROCESS_CLEANUP_GRACE,
            Duration::from_millis(50),
            || !process_group_alive(pgid),
        )
        .await;
    }
    signal_nested_harness(paths, SIGKILL)?;
    if wait_until(
        FORCED_PROCESS_CLEANUP_GRACE,
        Duration::from_millis(50),
        || {
            outer_pgid.is_none_or(|pgid| !process_group_alive(pgid))
                && !nested_harness_group_pending(paths)
        },
    )
    .await
    .is_none()
    {
        bail!("terminal task controllers did not become empty after verified SIGKILL");
    }
    Ok(())
}

async fn cleanup_terminal_legacy_processes(status: &StatusFile, paths: &TaskPaths) -> Result<()> {
    let outer_pgid = match verify_identity(status.pid, status.pgid, status.started_at) {
        IdentityCheck::Match => Some(status.pgid),
        IdentityCheck::Dead => None,
        IdentityCheck::Mismatch(reason) if is_proven_identity_mismatch(reason) => None,
        IdentityCheck::Mismatch(reason) => {
            bail!("terminal legacy worker identity is unavailable ({reason})")
        }
    };
    if let Some(pgid) = outer_pgid {
        signal_group(pgid, SIGKILL);
        let _ = wait_until(
            FORCED_PROCESS_CLEANUP_GRACE,
            Duration::from_millis(50),
            || !process_group_alive(pgid),
        )
        .await;
    }
    signal_nested_harness(paths, SIGKILL)?;
    if wait_until(
        FORCED_PROCESS_CLEANUP_GRACE,
        Duration::from_millis(50),
        || {
            outer_pgid.is_none_or(|pgid| !process_group_alive(pgid))
                && !nested_harness_group_pending(paths)
        },
    )
    .await
    .is_none()
    {
        bail!("terminal legacy controllers did not become empty after verified SIGKILL");
    }
    Ok(())
}

/// Force-clean a nested harness after its outer worker can no longer forward a
/// cancellation token. The sidecar identity is verified before SIGKILL and the
/// call does not return success until the process group is empty and the exact
/// sidecar has been removed.
async fn force_cleanup_orphaned_nested_harness(paths: &TaskPaths) -> Result<()> {
    signal_nested_harness(paths, SIGKILL)?;
    if wait_until(
        FORCED_PROCESS_CLEANUP_GRACE,
        Duration::from_millis(50),
        || !nested_harness_group_pending(paths),
    )
    .await
    .is_none()
    {
        bail!("verified nested harness SIGKILL did not empty the reported process group");
    }
    Ok(())
}

/// Signal the currently reported nested CLI harness only after exact process
/// identity verification. A corrupt, stale, or temporarily unverifiable
/// sidecar is never converted into a blind PID/PGID signal.
fn signal_nested_harness(paths: &TaskPaths, signal: i32) -> Result<()> {
    let Some(controller) = paths.read_harness_controller_optional()? else {
        return Ok(());
    };
    match verify_controller_identity(
        controller.pid,
        controller.pgid,
        controller.started_at,
        controller.birth_fingerprint.as_deref(),
    ) {
        IdentityCheck::Match => {
            signal_group(controller.pgid, signal);
            Ok(())
        }
        IdentityCheck::Dead if !process_group_alive(controller.pgid) => {
            paths.remove_harness_controller(&controller)
        }
        IdentityCheck::Dead => bail!(
            "nested harness sentinel {} exited while numeric process group {} remains; refusing an unauthenticated group signal",
            controller.pid,
            controller.pgid
        ),
        IdentityCheck::Mismatch(reason) => bail!(
            "nested harness identity mismatch for pid {} pgid {} ({reason})",
            controller.pid,
            controller.pgid
        ),
    }
}

/// Conservative liveness used only to decide whether cancellation cleanup has
/// finished. An unreadable sidecar counts as pending so escalation cannot
/// silently ignore a potentially live nested process group.
fn nested_harness_group_pending(paths: &TaskPaths) -> bool {
    let controller = match paths.read_harness_controller_optional() {
        Ok(Some(controller)) => controller,
        Ok(None) => return false,
        Err(_) => return true,
    };
    let identity = verify_controller_identity(
        controller.pid,
        controller.pgid,
        controller.started_at,
        controller.birth_fingerprint.as_deref(),
    );
    match identity {
        IdentityCheck::Match | IdentityCheck::Dead => observed_nested_harness_group_pending(
            paths,
            &controller,
            identity,
            process_group_alive(controller.pgid),
        ),
        IdentityCheck::Mismatch(_) => true,
    }
}

/// Finish one already-verified nested-controller liveness observation.
///
/// Identity and process-group probes are necessarily separate syscalls. A
/// matching sentinel may exit between them; a now-empty group must therefore
/// remove the exact sidecar instead of being reported as already clean. The
/// follow-up read keeps a concurrently replaced controller pending: the
/// conditional remove never erases a different generation.
fn observed_nested_harness_group_pending(
    paths: &TaskPaths,
    controller: &HarnessControllerFile,
    identity: IdentityCheck,
    group_alive: bool,
) -> bool {
    debug_assert!(matches!(
        identity,
        IdentityCheck::Match | IdentityCheck::Dead
    ));
    if group_alive {
        return true;
    }
    if paths.remove_harness_controller(controller).is_err() {
        return true;
    }
    !matches!(paths.read_harness_controller_optional(), Ok(None))
}

fn request_cancel_current(
    store: &SqliteTaskStore,
    id: &str,
    expected_executor_epoch: u64,
) -> Result<TaskRecord> {
    loop {
        let record = store
            .get(LOCAL_TASK_OWNER, id)?
            .ok_or_else(|| anyhow!("durable task `{id}` was not found"))?;
        if record.state.is_terminal() || record.state == DurableTaskState::Cancelling {
            return Ok(record);
        }
        if record.executor_epoch != expected_executor_epoch {
            return Ok(record);
        }
        match store.request_cancel(
            LOCAL_TASK_OWNER,
            id,
            record.revision,
            record.executor_epoch,
            chrono::Utc::now(),
        ) {
            Ok(cancelling) => return Ok(cancelling),
            Err(TaskStoreError::Conflict { .. }) => continue,
            Err(TaskStoreError::InvalidState { .. }) => {
                let latest = store
                    .get(LOCAL_TASK_OWNER, id)?
                    .ok_or_else(|| anyhow!("durable task `{id}` was not found"))?;
                if latest.state.is_terminal()
                    || latest.state == DurableTaskState::Cancelling
                    || latest.executor_epoch != expected_executor_epoch
                {
                    return Ok(latest);
                }
                return Err(anyhow!(
                    "task `{id}` remained {} while cancellation was rejected",
                    latest.state
                ));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

/// Poll `predicate` every `interval` until it is true or `budget` elapses.
/// Returns `Some(())` if it became true in time, `None` on timeout.
async fn wait_until(
    budget: Duration,
    interval: Duration,
    mut predicate: impl FnMut() -> bool,
) -> Option<()> {
    let deadline = std::time::Instant::now() + budget;
    loop {
        if predicate() {
            return Some(());
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(interval).await;
    }
}

/// Build a `TaskSpec` from a frozen [`JobSpec`], reusing the shared task
/// assembly so a detached run and an online run are constructed identically.
fn task_from_job(job: &JobSpec) -> Result<TaskSpec> {
    let mut task = task_base(
        job.task.clone(),
        job.workdir.clone(),
        job.sandbox.into(),
        job.system.clone(),
        job.timeout_secs,
        job.labels.clone(),
    )?;
    if let Some(session) = &job.session {
        task.session = Some(SessionRef::new(session.clone()));
    }
    Ok(task)
}

/// Map a completed run's status onto the persisted task state.
fn run_status_to_task_state(status: RunStatus) -> TaskState {
    match status {
        RunStatus::Success => TaskState::Success,
        RunStatus::Error => TaskState::Error,
        RunStatus::Timeout => TaskState::Timeout,
        RunStatus::Cancelled => TaskState::Cancelled,
    }
}

fn verify_target_snapshot(
    expected: &[TargetSnapshot],
    actual: &[BoundTarget],
    config: &ResolvedConfig,
) -> Result<()> {
    // Empty snapshots retain read compatibility with jobs created before this
    // field existed. Every new detached submission writes a non-empty chain.
    if expected.is_empty() {
        return Ok(());
    }
    let actual = actual
        .iter()
        .map(|bound| TargetSnapshot::from_bound(bound, config))
        .collect::<Result<Vec<_>>>()?;
    if expected != actual {
        bail!(
            "detached target configuration changed after submission; refusing to execute stale routing labels"
        );
    }
    Ok(())
}

/// A cancellation token wired to `SIGTERM` (in addition to ctrl-c). `task
/// cancel` SIGTERMs the worker's group; catching it here cancels the kernel so
/// the dispatch unwinds and records a `cancelled` run.
fn worker_cancellation_token() -> Result<CancellationToken> {
    let token = CancellationToken::new();

    // ctrl-c, same as an online dispatch.
    let ctrl_c_token = token.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            ctrl_c_token.cancel();
        }
    });

    // SIGTERM: the signal `task cancel` delivers.
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        // Constructing the stream installs Tokio's OS handler synchronously.
        // Do this before returning the token; putting `signal(...)` inside the
        // spawned future would leave a scheduling window where SIGTERM still
        // has its default process-terminating action.
        let mut term =
            signal(SignalKind::terminate()).context("install detached worker SIGTERM handler")?;
        let term_token = token.clone();
        tokio::spawn(async move {
            term.recv().await;
            term_token.cancel();
        });
    }

    Ok(token)
}

async fn run_broadcast(config_path: Option<PathBuf>, args: BroadcastArgs) -> Result<ExitCode> {
    // Same config-phase exit-code contract as `run_dispatch`.
    let phase = load_config(config_path.as_deref()).and_then(|loaded| {
        let targets = split_targets(&args.targets)?;
        let mut chains = Vec::with_capacity(targets.len());
        for target in &targets {
            chains.push(resolve_target_chain(&loaded, target)?);
        }
        Ok((loaded, targets, chains))
    });
    let (loaded, targets, chains) = match phase {
        Ok(value) => value,
        Err(error) => {
            eprintln!("config error: {error:#}");
            return Ok(ExitCode::from(2));
        }
    };
    let json = args.json;
    let task = task_from_broadcast(args)?;
    vyane_service::validate_user_routing_labels(&task.labels)?;
    let runtime = Runtime::new(loaded.config, StoragePaths::resolve()?)?;
    let cancel = cancellation_token();
    let results = runtime.dispatcher.broadcast(&task, chains, cancel).await;

    let mut rows = Vec::with_capacity(results.len());
    let mut json_rows = Vec::with_capacity(results.len());
    let mut all_success = true;

    for (target, result) in targets.into_iter().zip(results) {
        match result {
            Ok(outcome) => {
                let record = outcome.record;
                let output = outcome.output;
                all_success &= record.status == RunStatus::Success;
                json_rows.push(BroadcastJson {
                    target: target.clone(),
                    record: Some(record.clone()),
                    output: output.clone(),
                    error: None,
                });
                rows.push(BroadcastRow {
                    target,
                    record: Some(record),
                    output,
                    error: None,
                });
            }
            Err(error) => {
                all_success = false;
                let message = error.to_string();
                json_rows.push(BroadcastJson {
                    target: target.clone(),
                    record: None,
                    output: None,
                    error: Some(message.clone()),
                });
                rows.push(BroadcastRow {
                    target,
                    record: None,
                    output: None,
                    error: Some(message),
                });
            }
        }
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&json_rows)?);
    } else {
        crate::output::print_broadcast_table(&rows);
    }

    Ok(if all_success {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

async fn harness_available(kind: HarnessKind) -> bool {
    match kind {
        HarnessKind::ClaudeCode => ClaudeCodeHarness::new().available().await,
        HarnessKind::CodexCli => CodexCliHarness::new().available().await,
        HarnessKind::OpenCode | HarnessKind::Other(_) => false,
    }
}

async fn run_history(args: HistoryArgs) -> Result<ExitCode> {
    let runtime = Runtime::new(ResolvedConfig::default(), StoragePaths::resolve()?)?;
    let records = runtime
        .ledger
        .query(RunQuery {
            owner: Some("local".to_string()),
            provider: args.provider.map(ProviderId::new),
            status: args.status.map(Into::into),
            since: None,
            limit: Some(args.limit),
        })
        .await?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&records)?);
    } else {
        for record in &records {
            crate::output::print_record_line(record);
        }
    }

    Ok(ExitCode::SUCCESS)
}

async fn run_sessions(args: SessionsArgs) -> Result<ExitCode> {
    let runtime = Runtime::new(ResolvedConfig::default(), StoragePaths::resolve()?)?;
    let sessions = runtime.sessions.list("local").await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&sessions)?);
    } else {
        for session in &sessions {
            crate::output::print_legacy_session_line(session);
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn run_session(command: SessionCommand) -> Result<ExitCode> {
    run_session_with_paths(command, StoragePaths::resolve()).await
}

async fn run_session_with_paths(
    command: SessionCommand,
    paths: Result<StoragePaths>,
) -> Result<ExitCode> {
    let json = match &command {
        SessionCommand::List(args) => args.json,
        SessionCommand::Inspect(args) => args.json,
        SessionCommand::ResetNative(args) => args.json,
    };
    let paths = match paths {
        Ok(paths) => paths,
        Err(_) => return print_session_control_error(ErrorKind::Io, json),
    };
    let service = match VyaneService::from_local_storage(paths) {
        Ok(service) => service,
        Err(_) => return print_session_control_error(ErrorKind::Io, json),
    };
    match command {
        SessionCommand::List(args) => {
            let sessions = match service.session_views().await {
                Ok(sessions) => sessions,
                Err(error) => {
                    return print_session_control_error(vyane_error_kind(&error), args.json);
                }
            };
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "status": "ok",
                        "operation": "list",
                        "items": sessions,
                    }))?
                );
            } else {
                for session in &sessions {
                    crate::output::print_session_view_line(session);
                }
            }
            Ok(ExitCode::SUCCESS)
        }
        SessionCommand::Inspect(args) => run_session_inspect(&service, args).await,
        SessionCommand::ResetNative(args) => run_session_reset_native(&service, args).await,
    }
}

fn vyane_error_kind(error: &anyhow::Error) -> ErrorKind {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<VyaneError>())
        .map_or(ErrorKind::Other, |error| error.kind)
}

async fn run_session_inspect(service: &VyaneService, args: SessionInspectArgs) -> Result<ExitCode> {
    match service.session(&args.id).await {
        Ok(Some(session)) => {
            print_session_view("inspect", &session, args.json)?;
            Ok(ExitCode::SUCCESS)
        }
        Ok(None) => Ok(print_session_control_error(ErrorKind::NotFound, args.json)?),
        Err(error) => Ok(print_session_control_error(error.kind, args.json)?),
    }
}

async fn run_session_reset_native(
    service: &VyaneService,
    args: SessionResetNativeArgs,
) -> Result<ExitCode> {
    match service
        .reset_native_session(&args.id, args.expected_revision)
        .await
    {
        Ok(session) => {
            print_session_view("reset_native", &session, args.json)?;
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => Ok(print_session_control_error(error.kind, args.json)?),
    }
}

fn print_session_view(operation: &'static str, session: &SessionView, json: bool) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "ok",
                "operation": operation,
                "session": session,
            }))?
        );
    } else {
        crate::output::print_session_view_line(session);
    }
    Ok(())
}

fn print_session_control_error(kind: ErrorKind, json: bool) -> Result<ExitCode> {
    let (code, message, exit) = match kind {
        ErrorKind::NotFound => ("not_found", "session not found", ExitCode::from(2)),
        ErrorKind::Conflict => (
            "conflict",
            "session revision changed; inspect the session and retry with its current revision",
            ExitCode::from(3),
        ),
        ErrorKind::Indeterminate => (
            "indeterminate",
            "session reset may have been published; inspect the session before deciding whether to retry",
            ExitCode::from(4),
        ),
        ErrorKind::Config => (
            "invalid_argument",
            "invalid session control request",
            ExitCode::from(2),
        ),
        ErrorKind::Unsupported => (
            "unsupported",
            "session control is unavailable for this store",
            ExitCode::from(1),
        ),
        ErrorKind::Io => (
            "storage_error",
            "session storage operation failed",
            ExitCode::from(1),
        ),
        _ => (
            "operation_failed",
            "session control operation failed",
            ExitCode::from(1),
        ),
    };
    if json {
        eprintln!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "error",
                "error": {
                    "kind": code,
                    "message": message,
                    "retryable": false,
                    "inspect_before_retry": matches!(kind, ErrorKind::Conflict | ErrorKind::Indeterminate),
                },
            }))?
        );
    } else {
        eprintln!("error: {message}");
    }
    Ok(exit)
}

async fn submit_daemon_workflow(args: WorkflowSubmitArgs) -> Result<ExitCode> {
    let WorkflowSubmitArgs {
        file,
        id,
        vars,
        json,
    } = args;
    let vars = match parse_vars(vars) {
        Ok(vars) => vars,
        Err(error) => {
            eprintln!("config error: {error:#}");
            return Ok(ExitCode::from(2));
        }
    };

    // Submission is daemon-only. Authenticate the exact resident daemon before
    // reading and packaging a potentially large local source tree; there is no
    // foreground fallback on any control or transport failure.
    let client = DaemonWorkflowClient::connect()
        .await
        .context("workflow was definitely not submitted")?;
    let bundle = match WorkflowSourceBundle::from_path(&file) {
        Ok(bundle) => bundle,
        Err(error) => {
            print_workflow_error(&error);
            return Ok(workflow_error_exit(&error));
        }
    };
    let execution_cwd = canonical_workflow_submission_cwd()?;
    // Generate only after all bounded workflow sources were collected. The
    // caller can instead supply this exact UUIDv7 for an idempotent retry.
    let run_id = id.unwrap_or_else(WorkflowRunId::generate);

    // stderr remains separate from successful `--json` stdout. Flush before
    // POST so a timeout/reset can always be reconciled with this exact id.
    {
        let stderr = std::io::stderr();
        let mut stderr = stderr.lock();
        if json {
            writeln!(stderr, "{}", workflow_submission_id_event(&run_id))
        } else {
            writeln!(stderr, "workflow submission id: {run_id}")
        }
        .context("workflow was definitely not submitted: write submission id")?;
        stderr
            .flush()
            .context("workflow was definitely not submitted: flush submission id")?;
    }

    let view = match client.submit(&run_id, execution_cwd, bundle, vars).await {
        Ok(view) => view,
        Err(error) => {
            if json {
                eprintln!("{}", serde_json::to_string(&error.json_value())?);
            } else {
                eprintln!("error: {error}");
            }
            return Ok(ExitCode::from(error.exit_code()));
        }
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&view)?);
    } else {
        print_daemon_workflow_view(&view);
    }
    Ok(ExitCode::SUCCESS)
}

fn workflow_submission_id_event(run_id: &WorkflowRunId) -> serde_json::Value {
    serde_json::json!({
        "event": "workflow_submission_id",
        "workflow_run_id": run_id,
    })
}

fn canonical_workflow_submission_cwd() -> Result<PathBuf> {
    let current = std::env::current_dir()
        .context("workflow was definitely not submitted: read current directory")?;
    let canonical = std::fs::canonicalize(current)
        .context("workflow was definitely not submitted: canonicalize current directory")?;
    if !canonical.is_absolute() {
        bail!("workflow was definitely not submitted: canonical current directory is not absolute");
    }
    if canonical.to_str().is_none() {
        bail!("workflow was definitely not submitted: canonical current directory is not UTF-8");
    }
    Ok(canonical)
}

async fn status_daemon_workflow(args: WorkflowStatusArgs) -> Result<ExitCode> {
    let client = DaemonWorkflowClient::connect().await?;
    let view = client.status(&args.wf_run_id).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&view)?);
    } else {
        print_daemon_workflow_view(&view);
    }
    Ok(ExitCode::SUCCESS)
}

async fn cancel_daemon_workflow(args: WorkflowCancelArgs) -> Result<ExitCode> {
    let client = DaemonWorkflowClient::connect().await?;
    let task = client.cancel(&args.wf_run_id).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&task)?);
    } else {
        println!("workflow {} {}", task.id, task.state);
    }
    Ok(ExitCode::SUCCESS)
}

fn print_daemon_workflow_view(view: &WorkflowTaskView) {
    println!("workflow {} {}", view.task.id, view.task.state);
    if let Some(journal) = view.journal.as_ref() {
        let counts = &journal.steps;
        println!(
            "journal {} {}: {}/{} ok, {} failed, {} skipped, {} cancelled",
            journal.name,
            crate::output::workflow_status_name(journal.status),
            counts.success,
            counts.success
                + counts.failed
                + counts.skipped
                + counts.cancelled
                + counts.pending
                + counts.running,
            counts.failed,
            counts.skipped,
            counts.cancelled
        );
    }
}

async fn run_workflow(config_path: Option<PathBuf>, args: WorkflowRunArgs) -> Result<ExitCode> {
    let vars = match parse_vars(args.vars) {
        Ok(vars) => vars,
        Err(error) => {
            eprintln!("config error: {error:#}");
            return Ok(ExitCode::from(2));
        }
    };
    let phase = load_config(config_path.as_deref()).and_then(|loaded| {
        let wf = Workflow::from_path(&args.file).map_err(anyhow::Error::from)?;
        Ok((loaded, wf))
    });
    let (loaded, wf) = match phase {
        Ok(value) => value,
        Err(error) => {
            eprintln!("config error: {error:#}");
            return Ok(ExitCode::from(2));
        }
    };

    let paths = StoragePaths::resolve()?;
    let runtime = Runtime::new(loaded.config.clone(), paths.clone())?;
    let resolver = Arc::new(CliWorkflowResolver { loaded });
    let mut engine = WorkflowEngine::new(
        Arc::new(runtime.dispatcher.clone()),
        resolver,
        paths.workflows_dir.clone(),
    );
    if !args.json {
        engine = engine.with_observer(Arc::new(CliWorkflowObserver));
    }

    let outcome = match engine.run(&wf, vars, cancellation_token()).await {
        Ok(outcome) => outcome,
        Err(error) => {
            print_workflow_error(&error);
            return Ok(workflow_error_exit(&error));
        }
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&outcome)?);
    } else {
        crate::output::print_workflow_summary(&outcome);
    }
    Ok(if outcome.status.is_success() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

async fn resume_workflow(
    config_path: Option<PathBuf>,
    args: WorkflowResumeArgs,
) -> Result<ExitCode> {
    if !args.vars.is_empty() {
        eprintln!(
            "config error: workflow resume uses variables from the journal; --var is not allowed"
        );
        return Ok(ExitCode::from(2));
    }
    let phase = load_config(config_path.as_deref()).and_then(|loaded| {
        let wf = Workflow::from_path(&args.file).map_err(anyhow::Error::from)?;
        Ok((loaded, wf))
    });
    let (loaded, wf) = match phase {
        Ok(value) => value,
        Err(error) => {
            eprintln!("config error: {error:#}");
            return Ok(ExitCode::from(2));
        }
    };

    let paths = StoragePaths::resolve()?;
    let runtime = Runtime::new(loaded.config.clone(), paths.clone())?;
    let resolver = Arc::new(CliWorkflowResolver { loaded });
    let mut engine = WorkflowEngine::new(
        Arc::new(runtime.dispatcher.clone()),
        resolver,
        paths.workflows_dir.clone(),
    );
    if !args.json {
        engine = engine.with_observer(Arc::new(CliWorkflowObserver));
    }

    let outcome = match engine
        .resume(&args.wf_run_id, &wf, cancellation_token())
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            print_workflow_error(&error);
            return Ok(workflow_error_exit(&error));
        }
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&outcome)?);
    } else {
        crate::output::print_workflow_summary(&outcome);
    }
    Ok(if outcome.status.is_success() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

async fn replay_workflow(
    config_path: Option<PathBuf>,
    args: WorkflowReplayArgs,
) -> Result<ExitCode> {
    if !args.vars.is_empty() {
        eprintln!(
            "config error: workflow replay uses variables from the source journal; --var is not allowed"
        );
        return Ok(ExitCode::from(2));
    }
    let phase = load_config(config_path.as_deref()).and_then(|loaded| {
        let wf = Workflow::from_path(&args.file).map_err(anyhow::Error::from)?;
        Ok((loaded, wf))
    });
    let (loaded, wf) = match phase {
        Ok(value) => value,
        Err(error) => {
            eprintln!("config error: {error:#}");
            return Ok(ExitCode::from(2));
        }
    };

    let paths = StoragePaths::resolve()?;
    let runtime = Runtime::new(loaded.config.clone(), paths.clone())?;
    let resolver = Arc::new(CliWorkflowResolver { loaded });
    let mut engine = WorkflowEngine::new(
        Arc::new(runtime.dispatcher.clone()),
        resolver,
        paths.workflows_dir.clone(),
    );
    if !args.json {
        engine = engine.with_observer(Arc::new(CliWorkflowObserver));
    }

    let new_id = args.id.unwrap_or_else(WorkflowRunId::generate);
    // Publish and flush the exact identity before create-only journal
    // publication or any live suffix call. A killed foreground process can
    // therefore be reconciled without generating a second replay identity.
    {
        let stderr = std::io::stderr();
        let mut stderr = stderr.lock();
        if args.json {
            writeln!(
                stderr,
                "{}",
                workflow_replay_id_event(&args.source_wf_run_id, &new_id)
            )
        } else {
            writeln!(stderr, "workflow replay id: {new_id}")
        }
        .context("workflow replay was definitely not started: write replay id")?;
        stderr
            .flush()
            .context("workflow replay was definitely not started: flush replay id")?;
    }
    let outcome = match engine
        .replay_with_id(
            new_id,
            args.source_wf_run_id.as_str(),
            &wf,
            cancellation_token(),
        )
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            print_workflow_error(&error);
            return Ok(workflow_error_exit(&error));
        }
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&outcome)?);
    } else {
        crate::output::print_workflow_summary(&outcome);
    }
    Ok(if outcome.status.is_success() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

fn workflow_replay_id_event(
    source_wf_run_id: &WorkflowRunId,
    new_wf_run_id: &WorkflowRunId,
) -> serde_json::Value {
    serde_json::json!({
        "event": "workflow_replay_id",
        "source_workflow_run_id": source_wf_run_id,
        "workflow_run_id": new_wf_run_id,
    })
}

async fn run_review_command(config_path: Option<PathBuf>, args: ReviewArgs) -> Result<ExitCode> {
    let reviewers = crate::review::ReviewArgs::parse_reviewers(&args.reviewers);
    if reviewers.len() < 2 {
        eprintln!("config error: --reviewers needs at least 2 targets for independent review");
        return Ok(ExitCode::from(2));
    }

    let loaded = match load_config(config_path.as_deref()) {
        Ok(loaded) => loaded,
        Err(error) => {
            eprintln!("config error: {error:#}");
            return Ok(ExitCode::from(2));
        }
    };

    let paths = StoragePaths::resolve()?;
    let runtime = Runtime::new(loaded.config.clone(), paths.clone())?;
    let resolver = Arc::new(CliWorkflowResolver { loaded });
    let json = args.json;

    let review_args = crate::review::ReviewArgs {
        task: args.task,
        implementer: args.implementer,
        reviewers,
        synthesizer: args.synthesizer,
        workdir: args.workdir,
        timeout_secs: args.timeout_secs,
    };

    let outcome = match crate::review::run_review(
        review_args,
        Arc::new(runtime.dispatcher.clone()),
        resolver,
        paths.workflows_dir.clone(),
        cancellation_token(),
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            eprintln!("error: {error:#}");
            return Ok(ExitCode::from(1));
        }
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&outcome)?);
    } else {
        crate::output::print_workflow_summary(&outcome);
    }
    Ok(if outcome.status.is_success() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

async fn run_route_command(config_path: Option<PathBuf>, args: RouteArgs) -> Result<ExitCode> {
    let loaded = match load_config(config_path.as_deref()) {
        Ok(loaded) => loaded,
        Err(error) => {
            eprintln!("config error: {error:#}");
            return Ok(ExitCode::from(2));
        }
    };

    let extra_tags = args
        .tags
        .as_deref()
        .map(|t| {
            t.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let candidate_profiles = args
        .candidates
        .as_deref()
        .map(|c| {
            c.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let route_params = vyane_service::RouteParams {
        task: args.task,
        stage: args.stage,
        explicit_tier: args.tier,
        explicit_effort: None,
        extra_tags,
        changed_files: args.changed_files,
        dependency_edges: args.dependency_edges,
        retry_count: args.retry_count,
        candidate_profiles,
        allow_frontier: !args.no_frontier,
    };

    let result = match vyane_service::route_task(&loaded.config, route_params) {
        Ok(result) => result,
        Err(error) => {
            eprintln!("error: {error:#}");
            return Ok(ExitCode::from(1));
        }
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&result.decision)?);
    } else {
        println!("profile:     {}", result.profile);
        println!("provider:    {}", result.decision.provider);
        println!("model:       {}", result.decision.model);
        println!("tier:        {}", result.decision.tier.as_str());
        println!("effort:      {}", result.decision.effort.as_str());
        println!("score:       {:.3}", result.decision.complexity_score);
        println!("tag:         {}", result.decision.tag);
        println!("intent:      {}", result.decision.intent);
        println!("reason:      {}", result.decision.reason);
    }

    Ok(ExitCode::SUCCESS)
}

async fn list_workflows(args: crate::cli::WorkflowListArgs) -> Result<ExitCode> {
    let paths = StoragePaths::resolve()?;
    let summaries = vyane_workflow::list_journals(&paths.workflows_dir)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&summaries)?);
    } else {
        crate::output::print_workflow_list(&summaries);
    }
    Ok(ExitCode::SUCCESS)
}

fn task_from_broadcast(args: BroadcastArgs) -> Result<TaskSpec> {
    task_base(
        args.task,
        args.workdir,
        args.sandbox.into(),
        args.system,
        args.timeout,
        args.label,
    )
}

fn task_base(
    prompt: String,
    workdir: Option<PathBuf>,
    sandbox: vyane_core::Sandbox,
    system: Option<String>,
    timeout_secs: Option<u64>,
    labels: Vec<String>,
) -> Result<TaskSpec> {
    let mut task = TaskSpec::new(prompt).with_sandbox(sandbox);
    task.workdir = workdir;
    task.system = system;
    task.timeout = timeout_secs.map(Duration::from_secs);
    task.labels = parse_labels(labels)?;
    Ok(task)
}

fn parse_labels(raw: Vec<String>) -> Result<BTreeMap<String, String>> {
    let mut labels = BTreeMap::new();
    for label in raw {
        let (key, value) = label
            .split_once('=')
            .ok_or_else(|| anyhow!("label `{label}` must be in key=value form"))?;
        if key.is_empty() {
            bail!("label `{label}` has an empty key");
        }
        labels.insert(key.to_string(), value.to_string());
    }
    Ok(labels)
}

fn parse_vars(raw: Vec<String>) -> Result<BTreeMap<String, String>> {
    let mut vars = BTreeMap::new();
    for var in raw {
        let (key, value) = var
            .split_once('=')
            .ok_or_else(|| anyhow!("workflow variable `{var}` must be in key=value form"))?;
        if key.is_empty() {
            bail!("workflow variable `{var}` has an empty key");
        }
        vars.insert(key.to_string(), value.to_string());
    }
    Ok(vars)
}

fn cancellation_token() -> CancellationToken {
    let token = CancellationToken::new();
    let child = token.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            child.cancel();
        }
    });
    token
}

fn print_run_json(record: vyane_core::RunRecord, output: Option<String>) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&RunJson { record, output })?
    );
    Ok(())
}

fn print_workflow_error(error: &WorkflowError) {
    if error.is_validation_or_config() {
        eprintln!("config error: {error}");
    } else {
        eprintln!("error: {error}");
    }
}

fn workflow_error_exit(error: &WorkflowError) -> ExitCode {
    if error.is_validation_or_config() {
        ExitCode::from(2)
    } else {
        ExitCode::from(1)
    }
}

pub(crate) struct CliWorkflowResolver {
    loaded: LoadedConfig,
}

impl CliWorkflowResolver {
    pub(crate) fn new(loaded: LoadedConfig) -> Self {
        Self { loaded }
    }
}

impl TargetResolver for CliWorkflowResolver {
    fn resolve(&self, target: &str) -> vyane_core::Result<Vec<BoundTarget>> {
        resolve_target_chain(&self.loaded, target).map_err(|error| {
            vyane_core::VyaneError::new(vyane_core::ErrorKind::Config, error.to_string())
        })
    }

    fn resolve_for_validation(&self, target: &str) -> vyane_core::Result<Option<Vec<BoundTarget>>> {
        if target.eq_ignore_ascii_case("auto") {
            Ok(None)
        } else {
            self.resolve(target).map(Some)
        }
    }

    fn resolve_for_task(
        &self,
        target: &str,
        task: &mut TaskSpec,
    ) -> vyane_core::Result<Vec<BoundTarget>> {
        if !target.eq_ignore_ascii_case("auto") {
            return self.resolve(target);
        }
        vyane_service::plan_dispatch(&self.loaded, target, task)
            .map(|plan| plan.chain)
            .map_err(|error| {
                vyane_core::VyaneError::new(vyane_core::ErrorKind::Config, error.to_string())
            })
    }

    fn validate_deferred(
        &self,
        target: &str,
        route: &vyane_workflow::WorkflowRouteHints,
    ) -> vyane_core::Result<()> {
        if !target.eq_ignore_ascii_case("auto") {
            return Ok(());
        }
        let mut task = TaskSpec::new("workflow route validation");
        route.apply_to_labels(&mut task.labels);
        vyane_service::validate_auto_route_candidates(&self.loaded, &task.labels).map_err(|error| {
            vyane_core::VyaneError::new(vyane_core::ErrorKind::Config, error.to_string())
        })
    }
}

struct CliWorkflowObserver;

impl vyane_workflow::WorkflowObserver for CliWorkflowObserver {
    fn on_event(&self, event: StepEvent) {
        match event {
            StepEvent::Started { step_id } => {
                eprintln!("workflow step {step_id}: started");
            }
            StepEvent::Succeeded { step_id, duration } => {
                eprintln!(
                    "workflow step {step_id}: succeeded in {}ms",
                    duration.as_millis()
                );
            }
            StepEvent::Failed {
                step_id,
                duration,
                error,
            } => {
                eprintln!(
                    "workflow step {step_id}: failed in {}ms: {error}",
                    duration.as_millis()
                );
            }
            StepEvent::Skipped { step_id, reason } => {
                eprintln!("workflow step {step_id}: skipped: {reason}");
            }
            StepEvent::Cancelled { step_id, duration } => {
                eprintln!(
                    "workflow step {step_id}: cancelled in {}ms",
                    duration.as_millis()
                );
            }
        }
    }
}

fn env_vars_for_profile(config: &ResolvedConfig, profile_name: &str) -> Vec<String> {
    let mut out = BTreeSet::new();
    add_profile_env_vars(config, profile_name, &mut out, &mut BTreeSet::new());
    out.into_iter().collect()
}

fn add_profile_env_vars(
    config: &ResolvedConfig,
    profile_name: &str,
    out: &mut BTreeSet<String>,
    seen: &mut BTreeSet<String>,
) {
    if !seen.insert(profile_name.to_string()) {
        return;
    }
    let Some(profile) = config.profiles.get(profile_name) else {
        return;
    };
    if let Some(provider_id) = profile.provider.as_deref() {
        if let Ok(provider) = config.providers.get(provider_id) {
            if let Some(env) = &provider.api_key_env {
                out.insert(env.clone());
            }
        }
    }
    if let Some(failover) = &profile.failover {
        for element in failover {
            match element {
                vyane_config::RawFailoverElement::ProfileName(name) => {
                    add_profile_env_vars(config, name, out, seen);
                }
                vyane_config::RawFailoverElement::ProviderModel { provider, .. } => {
                    if let Ok(provider_config) = config.providers.get(provider) {
                        if let Some(env) = &provider_config.api_key_env {
                            out.insert(env.clone());
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    #[cfg(target_os = "linux")]
    use crate::task::spawn::SpawnedWorker;

    fn session_test_service(directory: &TempDir) -> VyaneService {
        VyaneService::from_loaded_with_paths(
            vyane_service::LoadedConfig {
                config: ResolvedConfig::default(),
                files: Vec::new(),
                secrets: BTreeMap::new(),
            },
            StoragePaths::from_data_dir(directory.path()),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn session_reset_cli_is_exact_revision_only_and_never_retries_conflict() {
        let directory = TempDir::new().unwrap();
        let service = session_test_service(&directory);
        let now = chrono::Utc::now();
        service
            .runtime()
            .sessions
            .save(
                "local",
                &vyane_core::SessionRecord {
                    session_id: "control".into(),
                    owner: "local".into(),
                    target: vyane_core::Target {
                        provider: ProviderId::new("provider"),
                        protocol: vyane_core::Protocol::OpenaiChat,
                        harness: None,
                        model: vyane_core::ModelId::new("model"),
                    },
                    native_session_id: Some("native".into()),
                    transcript: vec![vyane_core::ChatMessage::user("preserve")],
                    created_at: now,
                    updated_at: now,
                    run_count: 2,
                },
            )
            .await
            .unwrap();
        let revision = service
            .session("control")
            .await
            .unwrap()
            .unwrap()
            .session_revision;

        let success = run_session_reset_native(
            &service,
            SessionResetNativeArgs {
                id: "control".into(),
                expected_revision: revision,
                json: true,
            },
        )
        .await
        .unwrap();
        assert_eq!(success, ExitCode::SUCCESS);
        let after = service.session("control").await.unwrap().unwrap();
        assert_eq!(
            after.native_state,
            vyane_service::SessionNativeState::Absent
        );
        assert_eq!(after.run_count, 2);
        assert_eq!(after.transcript_messages, 1);

        let stale = run_session_reset_native(
            &service,
            SessionResetNativeArgs {
                id: "control".into(),
                expected_revision: revision,
                json: true,
            },
        )
        .await
        .unwrap();
        assert_eq!(stale, ExitCode::from(3));
        assert_eq!(
            service
                .session("control")
                .await
                .unwrap()
                .unwrap()
                .session_revision,
            after.session_revision,
            "a conflict must not auto-reload and retry"
        );
    }

    #[tokio::test]
    async fn session_control_initialization_failure_uses_bounded_exit_contract() {
        let directory = TempDir::new().unwrap();
        let blocked_root = directory.path().join("not-a-directory");
        std::fs::write(&blocked_root, "block directory creation").unwrap();
        let code = run_session_with_paths(
            SessionCommand::Inspect(SessionInspectArgs {
                id: "session".into(),
                json: true,
            }),
            Ok(StoragePaths::from_data_dir(blocked_root)),
        )
        .await
        .unwrap();
        assert_eq!(code, ExitCode::from(1));
    }

    #[test]
    fn session_control_error_exit_codes_distinguish_uncertain_outcomes() {
        assert_eq!(
            print_session_control_error(ErrorKind::NotFound, true).unwrap(),
            ExitCode::from(2)
        );
        assert_eq!(
            print_session_control_error(ErrorKind::Conflict, true).unwrap(),
            ExitCode::from(3)
        );
        assert_eq!(
            print_session_control_error(ErrorKind::Indeterminate, true).unwrap(),
            ExitCode::from(4)
        );
    }

    #[tokio::test]
    async fn serve_rejects_non_loopback_before_loading_config() {
        let directory = TempDir::new().unwrap();
        let missing_config = directory.path().join("missing-config.toml");
        let code = run_serve(
            Some(missing_config),
            ServeArgs {
                addr: "0.0.0.0:0".into(),
            },
        )
        .await
        .unwrap();
        assert_eq!(code, ExitCode::from(2));
    }

    #[test]
    fn workflow_submission_id_json_event_is_one_bounded_line() {
        let run_id: WorkflowRunId = "01890f3e-7b7c-7cc2-98d2-3f9a2b6c7d8e".parse().unwrap();
        let rendered = serde_json::to_string(&workflow_submission_id_event(&run_id)).unwrap();

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&rendered).unwrap(),
            serde_json::json!({
                "event": "workflow_submission_id",
                "workflow_run_id": run_id,
            })
        );
        assert!(!rendered.contains('\n'));
        assert!(rendered.len() < 256);
    }

    #[test]
    fn workflow_replay_id_json_event_is_one_bounded_line() {
        let source: WorkflowRunId = "01890f3e-7b7c-7cc2-98d2-3f9a2b6c7d8e".parse().unwrap();
        let new: WorkflowRunId = "01890f3e-7b7d-7cc2-98d2-3f9a2b6c7d8e".parse().unwrap();
        let rendered = serde_json::to_string(&workflow_replay_id_event(&source, &new)).unwrap();

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&rendered).unwrap(),
            serde_json::json!({
                "event": "workflow_replay_id",
                "source_workflow_run_id": source,
                "workflow_run_id": new,
            })
        );
        assert!(!rendered.contains('\n'));
        assert!(rendered.len() < 256);
    }

    fn create_detached(
        store: &SqliteTaskStore,
        id: &str,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> TaskRecord {
        create_detached_for_owner(store, LOCAL_TASK_OWNER, id, created_at)
    }

    fn create_detached_for_owner(
        store: &SqliteTaskStore,
        owner: &str,
        id: &str,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> TaskRecord {
        store
            .create(
                owner,
                NewTask {
                    id: id.into(),
                    kind: TaskKind::Dispatch,
                    origin: TaskOrigin::CliDetached,
                    task_digest: "a".repeat(64),
                    target_key: "review".into(),
                    created_at,
                },
            )
            .unwrap()
    }

    #[tokio::test]
    async fn detached_scope_rejects_same_origin_workflows_and_foreign_owners() {
        let directory = TempDir::new().unwrap();
        let store = SqliteTaskStore::open(directory.path().join("tasks.sqlite3")).unwrap();
        let created_at = chrono::Utc::now();

        let workflow = NewTask {
            id: "detached-scope-workflow".into(),
            kind: TaskKind::Workflow,
            origin: TaskOrigin::CliDetached,
            task_digest: "a".repeat(64),
            target_key: "workflow".into(),
            created_at,
        };
        let foreign = NewTask {
            id: "detached-scope-foreign".into(),
            kind: TaskKind::Dispatch,
            origin: TaskOrigin::CliDetached,
            task_digest: "b".repeat(64),
            target_key: "review".into(),
            created_at,
        };

        let workflow = store.create(LOCAL_TASK_OWNER, workflow).unwrap();
        let foreign = store.create("other-owner", foreign).unwrap();

        let visible = list_all_local_tasks(&store)
            .unwrap()
            .into_iter()
            .filter(is_local_detached_dispatch)
            .collect::<Vec<_>>();
        assert!(visible.is_empty());

        for record in [workflow, foreign] {
            assert!(!is_local_detached_dispatch(&record));
            let paths = TaskPaths::new(&directory.path().join("tasks"), &record.id);
            assert_eq!(
                reconcile_detached_process(&store, &paths, record.clone()).unwrap(),
                record
            );
            let error = attach_detached_controller(
                &store,
                &record.id,
                std::process::id() as i32,
                std::process::id() as i32,
            )
            .unwrap_err();
            if record.owner == LOCAL_TASK_OWNER {
                assert!(error.to_string().contains("not a local detached dispatch"));
            } else {
                let missing = attach_detached_controller(
                    &store,
                    "actually-missing",
                    std::process::id() as i32,
                    std::process::id() as i32,
                )
                .unwrap_err();
                assert_eq!(
                    error.to_string().replace(&record.id, "<id>"),
                    missing.to_string().replace("actually-missing", "<id>")
                );
            }

            let cancel = run_durable_task_cancel(&record.id, &paths, &store, record.clone()).await;
            assert_eq!(cancel.unwrap(), ExitCode::from(1));
            let stored = store.get(&record.owner, &record.id).unwrap().unwrap();
            assert_eq!(stored.state, DurableTaskState::Queued);
        }
    }

    #[test]
    fn old_queued_task_is_not_interrupted_by_a_read() {
        let directory = TempDir::new().unwrap();
        let store = SqliteTaskStore::open(directory.path().join("tasks.sqlite3")).unwrap();
        let created = create_detached(
            &store,
            "queued-slow-start",
            chrono::Utc::now() - chrono::Duration::hours(24),
        );
        let paths = TaskPaths::new(&directory.path().join("tasks"), &created.id);

        let observed = reconcile_detached_process(&store, &paths, created.clone()).unwrap();
        assert_eq!(observed, created);
        assert_eq!(
            store
                .get(LOCAL_TASK_OWNER, &created.id)
                .unwrap()
                .unwrap()
                .state,
            DurableTaskState::Queued
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn temporarily_unverifiable_controller_is_not_interrupted_by_a_read() {
        let directory = TempDir::new().unwrap();
        let store = SqliteTaskStore::open(directory.path().join("tasks.sqlite3")).unwrap();
        let created = create_detached(&store, "unverifiable-worker", chrono::Utc::now());
        let pid = std::process::id() as i32;
        let pgid = pgid_of(pid).unwrap();
        let started_at = crate::task::proc::process_start_time(pid).unwrap();
        let running = store
            .attach_controller(
                LOCAL_TASK_OWNER,
                &created.id,
                created.revision,
                created.executor_epoch,
                ControllerRef::ProcessGroup {
                    pid,
                    pgid,
                    started_at,
                    // Models an old row or a transiently unavailable procfs
                    // fingerprint: signalling fails closed, reading must not.
                    birth_fingerprint: None,
                },
                None,
                chrono::Utc::now(),
            )
            .unwrap();
        let paths = TaskPaths::new(&directory.path().join("tasks"), &created.id);

        let observed = reconcile_detached_process(&store, &paths, running.clone()).unwrap();
        assert_eq!(observed, running);
        assert_eq!(
            store.get(LOCAL_TASK_OWNER, &created.id).unwrap().unwrap(),
            running
        );

        let _ = refuse_unverifiable_cancel(
            &store,
            &created.id,
            &paths,
            "could not read process birth fingerprint",
            "during test",
        )
        .await
        .unwrap();
        assert_eq!(
            store.get(LOCAL_TASK_OWNER, &created.id).unwrap().unwrap(),
            running,
            "a failed identity probe must not terminalize a live worker"
        );
    }

    #[test]
    fn stopped_matching_harness_is_unlinked_but_a_replacement_stays_pending() {
        let directory = TempDir::new().unwrap();
        let paths = TaskPaths::new(&directory.path().join("tasks"), "nested-observation");
        paths.ensure_dir().unwrap();
        let old = HarnessControllerFile {
            schema: HARNESS_CONTROLLER_SCHEMA,
            pid: 101,
            pgid: 101,
            started_at: chrono::Utc::now(),
            birth_fingerprint: Some("old-birth".into()),
        };
        paths.write_harness_controller(&old).unwrap();

        assert!(!observed_nested_harness_group_pending(
            &paths,
            &old,
            IdentityCheck::Match,
            false,
        ));
        assert!(
            !paths.harness_controller().exists(),
            "a sentinel that dies between identity and group probes must be unlinked"
        );

        let new = HarnessControllerFile {
            pid: 202,
            pgid: 202,
            birth_fingerprint: Some("new-birth".into()),
            ..old.clone()
        };
        paths.write_harness_controller(&new).unwrap();
        assert!(observed_nested_harness_group_pending(
            &paths,
            &old,
            IdentityCheck::Match,
            false,
        ));
        assert_eq!(paths.read_harness_controller().unwrap(), new);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn dead_sentinel_never_authorizes_a_live_numeric_pgid() {
        let directory = TempDir::new().unwrap();
        let paths = TaskPaths::new(&directory.path().join("tasks"), "reused-group");
        paths.ensure_dir().unwrap();
        let grandchild_pid = directory.path().join("released-grandchild.pid");
        let script = format!(
            "( exec >/dev/null 2>&1; trap '' TERM; while :; do /bin/sleep 1; done ) & echo $! > '{}'; /bin/sleep 0.2; exit 0",
            grandchild_pid.display()
        );
        let mut command = std::process::Command::new("/bin/sh");
        command
            .args(["-c", &script])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        crate::task::proc::install_process_group(&mut command);
        let mut leader = command.spawn().unwrap();
        let pid = leader.id() as i32;
        let controller = HarnessControllerFile {
            schema: HARNESS_CONTROLLER_SCHEMA,
            pid,
            pgid: pid,
            started_at: crate::task::proc::process_start_time(pid).unwrap(),
            birth_fingerprint: Some(process_birth_fingerprint(pid).unwrap()),
        };
        paths.write_harness_controller(&controller).unwrap();
        assert!(
            wait_until(Duration::from_secs(2), Duration::from_millis(20), || {
                grandchild_pid.exists()
            })
            .await
            .is_some()
        );
        assert!(leader.wait().unwrap().success());
        assert!(process_group_alive(pid));
        assert!(
            signal_nested_harness(&paths, SIGKILL).is_err(),
            "a dead sentinel must fail closed while the numeric PGID is live"
        );
        assert!(process_group_alive(pid), "unproven group was signalled");
        assert!(paths.harness_controller().exists());

        signal_group(pid, SIGKILL);
        assert!(
            wait_until(Duration::from_secs(5), Duration::from_millis(20), || {
                !nested_harness_group_pending(&paths)
            })
            .await
            .is_some()
        );
        assert!(!paths.harness_controller().exists());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn terminal_race_after_running_snapshot_cleans_stopped_outer_and_nested() {
        let directory = TempDir::new().unwrap();
        let tasks_root = directory.path().join("tasks");
        let paths = TaskPaths::new(&tasks_root, "terminal-request-race");
        paths.ensure_dir().unwrap();
        let store = SqliteTaskStore::open(directory.path().join("tasks.sqlite3")).unwrap();

        let mut outer_command = std::process::Command::new("/bin/sleep");
        outer_command
            .arg("30")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        crate::task::proc::install_process_group(&mut outer_command);
        let mut outer = outer_command.spawn().unwrap();
        let outer_pid = outer.id() as i32;

        let mut nested_command = std::process::Command::new("/bin/sleep");
        nested_command
            .arg("30")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        crate::task::proc::install_process_group(&mut nested_command);
        let mut nested = nested_command.spawn().unwrap();
        let nested_pid = nested.id() as i32;

        let created = create_detached(&store, "terminal-request-race", chrono::Utc::now());
        let running = store
            .attach_controller(
                LOCAL_TASK_OWNER,
                &created.id,
                created.revision,
                created.executor_epoch,
                ControllerRef::ProcessGroup {
                    pid: outer_pid,
                    pgid: outer_pid,
                    started_at: chrono::Utc::now(),
                    birth_fingerprint: Some(process_birth_fingerprint(outer_pid).unwrap()),
                },
                None,
                chrono::Utc::now(),
            )
            .unwrap();
        paths
            .write_harness_controller(&HarnessControllerFile {
                schema: HARNESS_CONTROLLER_SCHEMA,
                pid: nested_pid,
                pgid: nested_pid,
                started_at: chrono::Utc::now(),
                birth_fingerprint: Some(process_birth_fingerprint(nested_pid).unwrap()),
            })
            .unwrap();

        signal_group(outer_pid, 19); // SIGSTOP: the exact outer cannot settle itself.
        assert_eq!(
            verify_controller_identity(
                outer_pid,
                outer_pid,
                chrono::Utc::now(),
                match running.controller.as_ref().unwrap() {
                    ControllerRef::ProcessGroup {
                        birth_fingerprint, ..
                    } => birth_fingerprint.as_deref(),
                    ControllerRef::InProcess { .. } => None,
                },
            ),
            IdentityCheck::Match
        );

        // `running` is the stale snapshot already read by task cancel. The
        // terminal write wins immediately before request_cancel_current, which
        // must treat its returned terminal record as a cleanup obligation.
        store
            .interrupt(
                LOCAL_TASK_OWNER,
                &running.id,
                running.revision,
                running.executor_epoch,
                FailureCode::ControlUnavailable,
                chrono::Utc::now(),
            )
            .unwrap();
        let outer_wait = std::thread::spawn(move || outer.wait());
        let nested_wait = std::thread::spawn(move || nested.wait());

        let id = running.id.clone();
        let exit = run_durable_task_cancel(&id, &paths, &store, running)
            .await
            .unwrap();
        assert_eq!(exit, ExitCode::SUCCESS);
        assert!(outer_wait.join().unwrap().is_ok());
        assert!(nested_wait.join().unwrap().is_ok());
        assert!(!process_group_alive(outer_pid));
        assert!(!process_group_alive(nested_pid));
        assert!(!paths.harness_controller().exists());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn unverifiable_reread_of_terminal_still_cleans_exact_outer() {
        let directory = TempDir::new().unwrap();
        let paths = TaskPaths::new(&directory.path().join("tasks"), "terminal-refusal-race");
        paths.ensure_dir().unwrap();
        let store = SqliteTaskStore::open(directory.path().join("tasks.sqlite3")).unwrap();
        let created = create_detached(&store, "terminal-refusal-race", chrono::Utc::now());

        let mut command = std::process::Command::new("/bin/sleep");
        command
            .arg("30")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        crate::task::proc::install_process_group(&mut command);
        let mut outer = command.spawn().unwrap();
        let pid = outer.id() as i32;
        let running = store
            .attach_controller(
                LOCAL_TASK_OWNER,
                &created.id,
                created.revision,
                created.executor_epoch,
                ControllerRef::ProcessGroup {
                    pid,
                    pgid: pid,
                    started_at: chrono::Utc::now(),
                    birth_fingerprint: Some(process_birth_fingerprint(pid).unwrap()),
                },
                None,
                chrono::Utc::now(),
            )
            .unwrap();
        signal_group(pid, 19);
        store
            .interrupt(
                LOCAL_TASK_OWNER,
                &running.id,
                running.revision,
                running.executor_epoch,
                FailureCode::ControlUnavailable,
                chrono::Utc::now(),
            )
            .unwrap();
        let wait = std::thread::spawn(move || outer.wait());

        let exit = refuse_unverifiable_cancel(
            &store,
            &running.id,
            &paths,
            "simulated transient identity failure",
            "during terminal race",
        )
        .await
        .unwrap();
        assert_eq!(exit, ExitCode::SUCCESS);
        assert!(wait.join().unwrap().is_ok());
        assert!(!process_group_alive(pid));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn spawn_failure_settles_only_the_exact_attached_controller() {
        let directory = TempDir::new().unwrap();
        let store = SqliteTaskStore::open(directory.path().join("tasks.sqlite3")).unwrap();
        let pid = std::process::id() as i32;
        let pgid = pgid_of(pid).unwrap();
        let fingerprint = process_birth_fingerprint(pid).unwrap();

        let created = create_detached(&store, "attached-epipe", chrono::Utc::now());
        store
            .attach_controller(
                LOCAL_TASK_OWNER,
                &created.id,
                created.revision,
                created.executor_epoch,
                ControllerRef::ProcessGroup {
                    pid,
                    pgid,
                    started_at: chrono::Utc::now(),
                    birth_fingerprint: Some(fingerprint.clone()),
                },
                None,
                chrono::Utc::now(),
            )
            .unwrap();
        let exact = SpawnedWorker {
            pid,
            pgid,
            birth_fingerprint: Some(fingerprint),
        };
        let settled = settle_spawn_failure(&store, &created, Some(&exact)).unwrap();
        assert_eq!(settled.state, DurableTaskState::Failed);
        assert_eq!(settled.failure_code, Some(FailureCode::SpawnFailed));

        let foreign = create_detached_for_owner(
            &store,
            "other-owner",
            "foreign-controller",
            chrono::Utc::now(),
        );
        store
            .attach_controller(
                "other-owner",
                &foreign.id,
                foreign.revision,
                foreign.executor_epoch,
                ControllerRef::ProcessGroup {
                    pid,
                    pgid,
                    started_at: chrono::Utc::now(),
                    birth_fingerprint: Some("foreign-birth".into()),
                },
                None,
                chrono::Utc::now(),
            )
            .unwrap();
        assert!(settle_spawn_failure(&store, &foreign, Some(&exact)).is_err());
        assert_eq!(
            store
                .get("other-owner", &foreign.id)
                .unwrap()
                .unwrap()
                .state,
            DurableTaskState::Running
        );
    }
}
