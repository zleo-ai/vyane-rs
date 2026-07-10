use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use vyane_config::ResolvedConfig;
use vyane_core::{
    AdapterTransport, BoundTarget, CancellationToken, Harness, HarnessKind, ProviderId, RunQuery,
    RunStatus, SessionRef, TaskSpec,
};
use vyane_harness::{ClaudeCodeHarness, CodexCliHarness};
use vyane_service::{VyaneService, resolve_target_chain, split_targets};
use vyane_workflow::{StepEvent, TargetResolver, Workflow, WorkflowEngine, WorkflowError};

use crate::app::{LoadedConfig, Runtime, StoragePaths, load_config};
use crate::cli::{
    BroadcastArgs, Cli, Command, DispatchArgs, HistoryArgs, ReviewArgs, RouteArgs, ServeArgs,
    TaskCancelArgs, TaskCommand, TaskListArgs, TaskStatusArgs, WorkerArgs, WorkflowCommand,
    WorkflowResumeArgs, WorkflowRunArgs,
};
use crate::output::{BroadcastJson, BroadcastRow, RunJson};
use crate::task::proc::{
    IdentityCheck, SIGKILL, SIGTERM, pgid_of, pid_alive, signal_group, verify_identity,
};
use crate::task::store::{JobSpec, StatusFile, TaskPaths, TaskState, interpret_state, list_tasks};

/// The production identity probe used by read-side orphan detection: a
/// still-`running` status is only trusted if the recorded process still
/// validates as its own worker (see [`verify_identity`]).
fn identity_probe(pid: i32, pgid: i32, started_at: chrono::DateTime<chrono::Utc>) -> IdentityCheck {
    verify_identity(pid, pgid, started_at)
}

pub async fn run(cli: Cli) -> Result<ExitCode> {
    match cli.command {
        Command::Check => run_check(cli.config).await,
        Command::Dispatch(args) => run_dispatch(cli.config, args).await,
        Command::Broadcast(args) => run_broadcast(cli.config, args).await,
        Command::History(args) => run_history(args).await,
        Command::Sessions(args) => run_sessions(args).await,
        Command::Workflow(command) => match command {
            WorkflowCommand::Run(args) => run_workflow(cli.config, args).await,
            WorkflowCommand::Resume(args) => resume_workflow(cli.config, args).await,
            WorkflowCommand::List(args) => list_workflows(args).await,
        },
        Command::Review(args) => run_review_command(cli.config, args).await,
        Command::Route(args) => run_route_command(cli.config, args).await,
        Command::Task(task) => run_task(task).await,
        Command::Serve(args) => run_serve(cli.config, args).await,
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
    let service = VyaneService::load(config_path.as_deref())?;
    let addr: SocketAddr = args.addr.parse().context("invalid --addr")?;
    eprintln!("vyane serve listening on {addr}");
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
        let chain = resolve_target_chain(&loaded, &args.target)?;
        // Prove the whole spec parses (labels included) up front. Discarded —
        // the online path rebuilds it below, the detached path re-parses in the
        // worker; this call's only job is to fail early on invalid input.
        // `task_base` runs the fallible `--label` key=value parsing; session is
        // infallible (`SessionRef::new`) so it needs no pre-validation here.
        let _ = task_base(
            args.task.clone(),
            args.workdir.clone(),
            args.sandbox.into(),
            args.system.clone(),
            args.timeout,
            args.label.clone(),
        )?;
        Ok((loaded, chain))
    });
    let (loaded, chain) = match phase {
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
        return spawn_detached_dispatch(config_path, args);
    }

    let json = args.json;
    let want_stream = args.stream;
    let task = task_from_dispatch(args)?;
    let runtime = Runtime::new(loaded.config, StoragePaths::resolve()?)?;
    // Built once and shared by both the streaming attempt and the
    // non-streaming fallback below, so a ctrl-c during the streaming path is
    // still honored if control falls through to `Dispatcher::dispatch`.
    let cancel = cancellation_token();

    if want_stream {
        match streamable_target(&chain, &task) {
            Some(bound) => {
                let bound = bound.clone();
                match run_dispatch_streaming(&runtime, &task, &bound, json, cancel.clone()).await? {
                    Some(code) => return Ok(code),
                    // `Unsupported` from the client itself: fall through to
                    // the non-streaming path below with the same chain.
                    None => eprintln!(
                        "notice: {} does not support streaming; falling back to non-streaming",
                        bound.target
                    ),
                }
            }
            None => eprintln!(
                "notice: --stream only applies to a single direct-HTTP target with no --session; falling back to non-streaming"
            ),
        }
    }

    let outcome = runtime.dispatcher.dispatch(&task, chain, cancel).await?;
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

/// The chain qualifies for `--stream` only when it is a single direct-HTTP
/// target (no failover, no harness) and the task names no session — the
/// assignment scopes streaming to exactly that case, and the streaming CLI
/// path has no session continuity (see WP-09.md's non-goals): falling back to
/// non-streaming for `--session --stream` avoids half-applying session
/// semantics (tagging a `RunRecord.session_id` while never touching the
/// session store's transcript/run_count).
fn streamable_target<'a>(chain: &'a [BoundTarget], task: &TaskSpec) -> Option<&'a BoundTarget> {
    if task.session.is_some() {
        return None;
    }
    match chain {
        [bound] if bound.transport == AdapterTransport::DirectHttp => Some(bound),
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
    bound: &BoundTarget,
    json: bool,
    cancel: CancellationToken,
) -> Result<Option<ExitCode>> {
    let outcome = runtime
        .dispatcher
        .dispatch_stream(task, bound, cancel, |event| match event {
            vyane_kernel::StreamDispatchEvent::Delta(delta) => {
                if !json {
                    print!("{delta}");
                    let _ = std::io::stdout().flush();
                }
            }
            vyane_kernel::StreamDispatchEvent::ReasoningDelta(_) => {
                // Reasoning deltas are not printed to stdout.
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

/// Freeze the dispatch request into a task directory and spawn a detached
/// worker to run it. Prints the run id and returns exit 0 without waiting.
///
/// The target chain was already resolved by the caller (so config errors have
/// exited 2 before we get here); we re-serialize the raw selector string into
/// the job so the worker re-resolves it identically.
fn spawn_detached_dispatch(config_path: Option<PathBuf>, args: DispatchArgs) -> Result<ExitCode> {
    let (_paths_root, tasks_dir) = crate::app::resolve_paths_with_tasks()?;
    let run_id = uuid::Uuid::now_v7().to_string();
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
    };
    let paths = TaskPaths::new(&tasks_dir, &run_id);
    crate::task::spawn::spawn_detached(&paths, &job)?;

    if args.json {
        println!("{}", serde_json::json!({ "id": run_id }));
    } else {
        println!("{run_id}");
    }
    Ok(ExitCode::SUCCESS)
}

/// The detached worker: re-resolve the frozen job, mark the run `running`,
/// dispatch it through the normal kernel path, then finalize `status.json` and
/// write `output.txt`. A `SIGTERM` (from `task cancel`) cancels the kernel so
/// the run finalizes as `cancelled` with its `RunRecord` still on the ledger.
///
/// The worker owns its own process group (installed by the parent via
/// `setsid`); it never re-exec's further.
///
/// ## Two ordering invariants this shell enforces
///
/// 1. **Cancellation handler is armed before any `running` state is
///    observable.** The `SIGTERM` → [`CancellationToken`] handler is installed
///    here, *before* [`worker_body`] runs (and it is `worker_body` that writes
///    the first `running` status). Because `task cancel` only signals a task
///    whose status file already reads `running`, and `running` cannot appear
///    until after this handler exists, any signal a canceller could deliver is
///    guaranteed to be caught and turned into a clean kernel cancellation —
///    never a raw process teardown mid-run. (The handler being live strictly
///    *before* the state write is what closes the race; the code structure —
///    handler on line A, `worker_body` call on the next line — encodes it.)
///
/// 2. **No path ever exits leaving `running` behind.** [`worker_body`] returns
///    `Err` for *any* setup or dispatch failure (corrupt `job.json`, config
///    resolution failure, spec/runtime assembly failure, kernel dispatch
///    error). This shell converts that `Err` into a terminal `error`
///    `status.json` (keyed by the worker's own run id, with the message), so a
///    reader always sees a definitive terminal state, not a stuck `running`.
async fn run_worker(config_path: Option<PathBuf>, args: WorkerArgs) -> Result<ExitCode> {
    let (_storage, tasks_dir) = crate::app::resolve_paths_with_tasks()?;
    let paths = TaskPaths::new(&tasks_dir, &args.id);

    // Invariant (1): arm the cancellation handler BEFORE running any body that
    // could publish `running`. See the doc comment above.
    let cancel = worker_cancellation_token();

    match worker_body(config_path, &args, &paths, cancel).await {
        Ok(code) => Ok(code),
        Err(error) => {
            // Invariant (2): convert any setup/dispatch failure into a terminal
            // `error` status so nothing is ever left observably `running`.
            let pid = std::process::id() as i32;
            let pgid = pgid_of(pid).unwrap_or(pid);
            let mut status = StatusFile::running(&args.id, pid, pgid, "-", None);
            status.state = TaskState::Error;
            status.finished_at = Some(chrono::Utc::now());
            status.error = Some(format!("{error:#}"));
            // Best-effort: if even this write fails there is nothing more we can
            // do (the read side will fall back to `stale`/`died`).
            let _ = paths.write_status(&status);
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
    cancel: CancellationToken,
) -> Result<ExitCode> {
    let job = paths
        .read_job()
        .with_context(|| format!("read job spec for {}", args.id))?;
    // The job's own recorded config override wins over any inherited flag; the
    // parent always writes it (possibly `None`).
    let config_path = job.config.clone().or(config_path);

    // Re-resolve config + target chain exactly as an online dispatch would. The
    // parent already validated this, but the worker is a fresh process, so it
    // resolves independently. A failure propagates as `Err` → terminal error.
    let (loaded, chain) = load_config(config_path.as_deref())
        .and_then(|loaded| resolve_target_chain(&loaded, &job.target).map(|c| (loaded, c)))
        .with_context(|| format!("resolve config/target for {}", args.id))?;

    let pid = std::process::id() as i32;
    let pgid = pgid_of(pid).unwrap_or(pid);
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

    // Announce `running` up front (atomic write) so `task list/status` observe
    // the run the instant the worker is live. The cancellation handler is
    // already armed by `run_worker`, so any observable `running` implies a
    // canceller's SIGTERM will be caught (ordering invariant 1).
    let running = StatusFile::running(&job.run_id, pid, pgid, &target_label, workdir.clone());
    paths.write_status(&running)?;

    // From here, a failure still lands as a terminal `error` (invariant 2): the
    // `?` bubbles up to `run_worker`, which overwrites this `running` file.
    let task = task_from_job(&job)?;
    let runtime = Runtime::new(loaded.config, StoragePaths::resolve()?)?;
    let outcome = runtime.dispatcher.dispatch(&task, chain, cancel).await?;
    let record = outcome.record;
    let output = outcome.output;

    // Persist the answer (if any) beside the status, then finalize status.
    if let Some(text) = output.as_deref() {
        std::fs::write(paths.output(), text)
            .with_context(|| format!("write {}", paths.output().display()))?;
    }

    let state = run_status_to_task_state(record.status);
    let mut final_status = running;
    final_status.state = state;
    final_status.finished_at = Some(record.finished_at);
    final_status.ledger_run_id = Some(record.run_id.clone());
    final_status.error = record.error.clone();
    paths.write_status(&final_status)?;

    Ok(if record.status == RunStatus::Success {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

async fn run_task_list(args: TaskListArgs) -> Result<ExitCode> {
    let (_storage, tasks_dir) = crate::app::resolve_paths_with_tasks()?;
    let rows = list_tasks(&tasks_dir, &identity_probe);

    if args.json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
    } else if rows.is_empty() {
        println!("no detached runs");
    } else {
        crate::output::print_task_table(&rows);
    }
    Ok(ExitCode::SUCCESS)
}

async fn run_task_status(args: TaskStatusArgs) -> Result<ExitCode> {
    let (_storage, tasks_dir) = crate::app::resolve_paths_with_tasks()?;
    let paths = TaskPaths::new(&tasks_dir, &args.id);

    let status = match paths.read_status() {
        Ok(status) => status,
        Err(_) => {
            // No readable status. Distinguish a *stale* run (the parent froze a
            // job but the worker never published status — a spawn likely
            // failed) from a genuinely unknown id, so the failure is
            // explainable rather than a bare "no such run".
            if paths.job_mtime().is_some() {
                eprintln!(
                    "{}: stale — worker never wrote status (spawn may have failed); see {}",
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
    let (_storage, tasks_dir) = crate::app::resolve_paths_with_tasks()?;
    let paths = TaskPaths::new(&tasks_dir, &args.id);

    let status = match paths.read_status() {
        Ok(status) => status,
        Err(_) => {
            eprintln!("no such detached run: {}", args.id);
            return Ok(ExitCode::from(1));
        }
    };

    // Already finished: nothing to signal.
    if status.state.is_terminal() {
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
            // The worker is already gone without finalizing — an orphan, which
            // the read side shows as `died`. Nothing to signal.
            eprintln!(
                "{}: worker process is gone (died); nothing to cancel",
                args.id
            );
            return Ok(ExitCode::from(1));
        }
        IdentityCheck::Mismatch(reason) => {
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

    // Give the worker up to 5s to exit its process group cleanly; escalate to
    // SIGKILL if the group is still alive after the grace.
    if wait_until(Duration::from_secs(5), Duration::from_millis(100), || {
        !pid_alive(status.pid)
    })
    .await
    .is_none()
    {
        signal_group(pgid, SIGKILL);
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

/// A cancellation token wired to `SIGTERM` (in addition to ctrl-c). `task
/// cancel` SIGTERMs the worker's group; catching it here cancels the kernel so
/// the dispatch unwinds and records a `cancelled` run.
fn worker_cancellation_token() -> CancellationToken {
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
        let term_token = token.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{SignalKind, signal};
            if let Ok(mut term) = signal(SignalKind::terminate()) {
                term.recv().await;
                term_token.cancel();
            }
        });
    }

    token
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

async fn run_sessions(args: crate::cli::SessionsArgs) -> Result<ExitCode> {
    let runtime = Runtime::new(ResolvedConfig::default(), StoragePaths::resolve()?)?;
    let sessions = runtime.sessions.list(Some("local")).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&sessions)?);
    } else {
        for session in &sessions {
            crate::output::print_session_line(session);
        }
    }
    Ok(ExitCode::SUCCESS)
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
        extra_tags,
        changed_files: args.changed_files,
        dependency_edges: args.dependency_edges,
        retry_count: args.retry_count,
        candidate_profiles,
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

fn task_from_dispatch(args: DispatchArgs) -> Result<TaskSpec> {
    let mut task = task_base(
        args.task,
        args.workdir,
        args.sandbox.into(),
        args.system,
        args.timeout,
        args.label,
    )?;
    if let Some(session) = args.session {
        task.session = Some(SessionRef::new(session));
    }
    Ok(task)
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

struct CliWorkflowResolver {
    loaded: LoadedConfig,
}

impl TargetResolver for CliWorkflowResolver {
    fn resolve(&self, target: &str) -> vyane_core::Result<Vec<BoundTarget>> {
        resolve_target_chain(&self.loaded, target).map_err(|error| {
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
