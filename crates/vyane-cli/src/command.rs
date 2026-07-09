use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use futures::StreamExt;
use vyane_config::ResolvedConfig;
use vyane_core::{
    AdapterTransport, Attempt, AttemptOutcome, BoundTarget, CancellationToken, ErrorKind, Harness,
    HarnessKind, ProviderId, RunQuery, RunRecord, RunStatus, SessionRef, StreamEvent, TaskSpec,
    Usage, VyaneError,
};
use vyane_harness::{ClaudeCodeHarness, CodexCliHarness};
use vyane_service::{VyaneService, resolve_target_chain, split_targets};
use vyane_workflow::{StepEvent, TargetResolver, Workflow, WorkflowEngine, WorkflowError};

use crate::app::{LoadedConfig, Runtime, StoragePaths, load_config};
use crate::cli::{
    BroadcastArgs, Cli, Command, DispatchArgs, HistoryArgs, ServeArgs, TaskCancelArgs, TaskCommand,
    TaskListArgs, TaskStatusArgs, WorkerArgs, WorkflowCommand, WorkflowResumeArgs, WorkflowRunArgs,
};
use crate::factory::direct_http_client;
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

/// First [`TASK_PREVIEW_CHARS`] characters of the prompt kept as a
/// human-scannable preview on the hand-built streaming `RunRecord`. Mirrors
/// `vyane-kernel::dispatch`'s own constant so a streaming and non-streaming
/// run of the same prompt preview identically in `vyane history`.
const TASK_PREVIEW_CHARS: usize = 120;

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

/// Drive the single-target streaming path: build the protocol client via the
/// same mapping `AssemblerFactory` uses, stream deltas to stdout (flushed per
/// delta), then append one `RunRecord` through the same `Ledger` the
/// non-streaming path uses.
///
/// Returns `Ok(Some(exit_code))` when the run was handled here (streamed
/// successfully, or failed after already starting to stream — either way a
/// `RunRecord` was recorded and the caller returns immediately). Returns
/// `Ok(None)` only when the client itself declined streaming
/// (`ErrorKind::Unsupported`, no HTTP call attempted yet) so the caller can
/// fall back to `Dispatcher::dispatch` on the untouched chain.
///
/// `task.timeout` and `cancel` are honored exactly the way
/// `vyane_kernel::dispatch`'s `drive` helper honors them for a non-streaming
/// attempt: cancellation is checked up front (a pre-cancelled token never
/// calls the client), and the client-construction-through-event-loop future is
/// raced against the caller-specified timeout and the cancellation token via a
/// biased `tokio::select!` (cancel wins ties). Either outcome still produces
/// exactly one recorded `RunRecord` — the invariant this whole function
/// exists to uphold.
///
/// This duplicates a slice of `vyane-kernel::dispatch`'s record-assembly
/// logic (digest, attempt shape, status mapping) because the kernel has no
/// streaming entry point and is frozen for this work package — see
/// `docs/plan/WP-09.md`'s "known seam" section and `docs/plan/feedback-wp09.md`.
async fn run_dispatch_streaming(
    runtime: &Runtime,
    task: &TaskSpec,
    bound: &BoundTarget,
    json: bool,
    cancel: CancellationToken,
) -> Result<Option<ExitCode>> {
    let started_at = Utc::now();
    let attempt_start = Instant::now();

    // Mirrors `vyane_kernel::dispatch::drive`'s "check before doing anything"
    // rule: an already-cancelled token never builds the client or calls the
    // network, and yields a deterministic `Cancelled` record.
    if cancel.is_cancelled() {
        let record = build_stream_record(
            task,
            bound,
            started_at,
            attempt_start,
            Err(&VyaneError::cancelled()),
        );
        append_ledger(runtime, &record).await;
        print_stream_result(json, &record, None)?;
        return Ok(Some(exit_code_for(record.status)));
    }

    let attempt = stream_attempt(runtime, task, bound, json, started_at, attempt_start);
    tokio::pin!(attempt);

    let outcome = match task.timeout {
        Some(duration) => {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => StreamAttemptOutcome::Cancelled,
                _ = tokio::time::sleep(duration) => StreamAttemptOutcome::TimedOut,
                result = &mut attempt => result,
            }
        }
        None => {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => StreamAttemptOutcome::Cancelled,
                result = &mut attempt => result,
            }
        }
    };

    match outcome {
        StreamAttemptOutcome::Unsupported => Ok(None),
        StreamAttemptOutcome::Recorded(code) => Ok(Some(code)),
        StreamAttemptOutcome::TimedOut | StreamAttemptOutcome::Cancelled => {
            let error = match outcome {
                StreamAttemptOutcome::TimedOut => VyaneError::new(
                    ErrorKind::Timeout,
                    format!(
                        "attempt exceeded timeout of {}ms",
                        task.timeout.unwrap_or_default().as_millis()
                    ),
                ),
                _ => VyaneError::cancelled(),
            };
            let record = build_stream_record(task, bound, started_at, attempt_start, Err(&error));
            append_ledger(runtime, &record).await;
            print_stream_result(json, &record, None)?;
            Ok(Some(exit_code_for(record.status)))
        }
    }
}

/// Outcome of racing [`stream_attempt`] against timeout/cancellation in
/// [`run_dispatch_streaming`]. `TimedOut`/`Cancelled` are produced by the
/// *other* arms of the `select!`, never by `stream_attempt` itself — it has no
/// way to observe either condition mid-flight, which is exactly why the
/// `select!` exists one level up.
enum StreamAttemptOutcome {
    /// The client declined streaming outright (`ErrorKind::Unsupported`, no
    /// HTTP call attempted) — caller falls back to non-streaming dispatch.
    Unsupported,
    /// A `RunRecord` was already built, ledger-appended, and printed; carries
    /// the exit code to return.
    Recorded(ExitCode),
    TimedOut,
    Cancelled,
}

/// Build the client, run the request, and consume the event stream to
/// completion — the part of the streaming attempt that the outer
/// `tokio::select!` can preempt for timeout/cancellation. Every path that
/// reaches a terminal state *other than* `Unsupported` here already appends
/// the `RunRecord` and prints the result before returning, including a
/// failure to construct the client itself.
async fn stream_attempt(
    runtime: &Runtime,
    task: &TaskSpec,
    bound: &BoundTarget,
    json: bool,
    started_at: chrono::DateTime<Utc>,
    attempt_start: Instant,
) -> StreamAttemptOutcome {
    let client = match direct_http_client(bound) {
        Ok(client) => client,
        Err(error) if error.kind == ErrorKind::Unsupported => {
            return StreamAttemptOutcome::Unsupported;
        }
        Err(error) => {
            // Constructing the client failed for a reason other than
            // "unsupported" (e.g. a malformed endpoint) — this is a real
            // attempt failure, not a signal to fall back silently, so it gets
            // the same recorded-attempt treatment as a failed HTTP call.
            let record = build_stream_record(task, bound, started_at, attempt_start, Err(&error));
            append_ledger(runtime, &record).await;
            let _ = print_stream_result(json, &record, None);
            return StreamAttemptOutcome::Recorded(exit_code_for(record.status));
        }
    };

    // Direct-chat message shape: system (if any) then the current user
    // message — same assembly `vyane_kernel::dispatch` uses for a fresh
    // (non-session) direct-chat attempt. The streaming CLI path carries no
    // `--session` continuity (see WP-09.md's non-goals), so there is no prior
    // transcript to replay here.
    let mut messages = Vec::new();
    if let Some(system) = task.system.as_ref() {
        messages.push(vyane_core::ChatMessage::system(system.clone()));
    }
    messages.push(vyane_core::ChatMessage::user(task.prompt.clone()));
    let req = vyane_core::ChatRequest {
        model: bound.target.model.clone(),
        messages,
        params: bound.params.clone(),
    };

    let mut stream = match client.stream(req).await {
        Ok(stream) => stream,
        Err(error) if error.kind == ErrorKind::Unsupported => {
            return StreamAttemptOutcome::Unsupported;
        }
        Err(error) => {
            let record = build_stream_record(task, bound, started_at, attempt_start, Err(&error));
            append_ledger(runtime, &record).await;
            let _ = print_stream_result(json, &record, None);
            return StreamAttemptOutcome::Recorded(exit_code_for(record.status));
        }
    };

    let mut text = String::new();
    let mut usage: Option<Usage> = None;
    let stdout_is_human = !json;
    let mut stream_error = None;

    loop {
        match stream.next().await {
            Some(Ok(StreamEvent::Delta(delta))) => {
                text.push_str(&delta);
                if stdout_is_human {
                    print!("{delta}");
                    // Flush per delta so a human watching the terminal sees
                    // text arrive live rather than buffered in bursts.
                    let _ = std::io::stdout().flush();
                }
            }
            Some(Ok(StreamEvent::ReasoningDelta(_))) => {
                // Reasoning deltas are not part of the recorded answer text
                // and may be entirely absent — never required for liveness.
            }
            Some(Ok(StreamEvent::Usage(u))) => {
                usage.get_or_insert_with(Usage::default).add(&u);
            }
            Some(Ok(StreamEvent::Done { .. })) => break,
            Some(Err(error)) => {
                stream_error = Some(error);
                break;
            }
            None => break,
        }
    }
    if stdout_is_human && !text.is_empty() {
        println!();
    }

    let record = match stream_error {
        Some(error) => build_stream_record(task, bound, started_at, attempt_start, Err(&error)),
        None => build_stream_record(
            task,
            bound,
            started_at,
            attempt_start,
            Ok((text.clone(), usage)),
        ),
    };
    append_ledger(runtime, &record).await;
    let output = if record.status == RunStatus::Success {
        Some(text)
    } else {
        None
    };
    let _ = print_stream_result(json, &record, output.as_deref());
    StreamAttemptOutcome::Recorded(exit_code_for(record.status))
}

/// Assemble one `RunRecord` for the streaming path — single attempt, no
/// failover chain (the caller already restricted itself to a one-target
/// chain). Field-for-field, this matches what
/// `vyane_kernel::dispatch::Dispatcher::dispatch` would have produced for the
/// same single-attempt outcome (see that module's `RunRecord` construction).
fn build_stream_record(
    task: &TaskSpec,
    bound: &BoundTarget,
    started_at: chrono::DateTime<Utc>,
    attempt_start: Instant,
    outcome: std::result::Result<(String, Option<Usage>), &vyane_core::VyaneError>,
) -> RunRecord {
    let duration_ms = attempt_start.elapsed().as_millis() as u64;
    let finished_at = Utc::now();
    let workdir = task
        .workdir
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned());
    let session_id = task.session.as_ref().map(|s| s.as_str().to_string());

    let (status, usage, output_chars, error_msg, attempt_outcome) = match outcome {
        Ok((text, usage)) => (
            RunStatus::Success,
            usage,
            Some(text.chars().count() as u64),
            None,
            AttemptOutcome::Ok,
        ),
        Err(error) => (
            status_for_error(error.kind),
            None,
            None,
            Some(error.to_string()),
            AttemptOutcome::Err {
                kind: error.kind,
                message: error.message.clone(),
                // Single-attempt path: there is no next target to fail over
                // to, so this is always `false` regardless of eligibility.
                failed_over: false,
            },
        ),
    };

    RunRecord {
        run_id: uuid::Uuid::now_v7().to_string(),
        owner: "local".to_string(),
        started_at,
        finished_at,
        task_digest: vyane_kernel::task_digest(&task.prompt),
        task_preview: Some(task_preview(&task.prompt)),
        workdir,
        sandbox: task.sandbox,
        target: bound.target.clone(),
        transport: bound.transport,
        attempts: vec![Attempt {
            target: bound.target.clone(),
            transport: bound.transport,
            started_at,
            duration_ms,
            outcome: attempt_outcome,
        }],
        status,
        usage,
        cost_usd: None,
        session_id,
        output_chars,
        error: error_msg,
        labels: task.labels.clone(),
    }
}

/// Map a terminal error kind onto a run status, mirroring
/// `vyane_kernel::dispatch::status_for_error` (private to the kernel, so this
/// is a deliberate, minimal duplication for the streaming shortcut).
fn status_for_error(kind: ErrorKind) -> RunStatus {
    match kind {
        ErrorKind::Timeout => RunStatus::Timeout,
        ErrorKind::Cancelled => RunStatus::Cancelled,
        _ => RunStatus::Error,
    }
}

/// First [`TASK_PREVIEW_CHARS`] characters of the prompt, on a char boundary
/// — mirrors `vyane_kernel::dispatch::task_preview` (private to the kernel).
fn task_preview(prompt: &str) -> String {
    prompt.chars().take(TASK_PREVIEW_CHARS).collect()
}

/// Best-effort ledger append — matches `Dispatcher::dispatch`'s rule that a
/// completed run is never demoted to a caller-visible failure by a ledger
/// write failure.
async fn append_ledger(runtime: &Runtime, record: &RunRecord) {
    if let Err(e) = runtime.ledger.append(record).await {
        tracing::warn!(
            run_id = %record.run_id,
            error = %e,
            "ledger append failed after streaming run completed; returning run anyway"
        );
    }
}

fn exit_code_for(status: RunStatus) -> ExitCode {
    if status == RunStatus::Success {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

/// Print the streaming run's result. In JSON mode this matches
/// `print_run_json`'s shape exactly (`{ "record", "output" }`) so `--stream
/// --json` output is a drop-in replacement for the non-streaming JSON output.
/// In human mode the deltas already printed live to stdout during the run;
/// this only prints an error line on failure (mirroring the non-streaming
/// path's `eprintln!(&record.error)` branch).
fn print_stream_result(json: bool, record: &RunRecord, output: Option<&str>) -> Result<()> {
    if json {
        print_run_json(record.clone(), output.map(ToString::to_string))?;
    } else if record.status != RunStatus::Success {
        if let Some(error) = &record.error {
            eprintln!("{error}");
        }
    }
    Ok(())
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
