use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use chrono::Utc;
use futures::StreamExt;
use vyane_config::{ConfigLayers, ResolvedConfig};
use vyane_core::{
    AdapterTransport, Attempt, AttemptOutcome, BoundTarget, CancellationToken, ErrorKind, Harness,
    HarnessKind, ProviderId, RunQuery, RunRecord, RunStatus, SessionRef, StreamEvent, TaskSpec,
    Usage, VyaneError,
};
use vyane_harness::{ClaudeCodeHarness, CodexCliHarness};
use vyane_workflow::{StepEvent, TargetResolver, Workflow, WorkflowEngine, WorkflowError};

use crate::app::{LoadedConfig, Runtime, StoragePaths, load_config};
use crate::cli::{
    BroadcastArgs, Cli, Command, DispatchArgs, HistoryArgs, WorkflowCommand, WorkflowResumeArgs,
    WorkflowRunArgs,
};
use crate::factory::direct_http_client;
use crate::output::{BroadcastJson, BroadcastRow, RunJson};

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
    }
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
    // Config-phase failures exit 2 (mirroring `check`), so wrappers can tell
    // "fix your config" apart from "the run failed" (exit 1).
    let phase = load_config(config_path.as_deref())
        .and_then(|loaded| resolve_target_chain(&loaded, &args.target).map(|c| (loaded, c)));
    let (loaded, chain) = match phase {
        Ok(value) => value,
        Err(error) => {
            eprintln!("config error: {error:#}");
            return Ok(ExitCode::from(2));
        }
    };
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

fn resolve_target_chain(loaded: &LoadedConfig, raw: &str) -> Result<Vec<BoundTarget>> {
    if let Some((provider, model)) = parse_provider_model(raw) {
        let root = provider_model_config(&loaded.config, provider, model)?;
        return resolve_temp_profile(root, "__cli_target", loaded);
    }
    Ok(loaded
        .config
        .resolve_failover_with(raw, &|key| loaded.env_lookup(key))?)
}

fn provider_model_config(
    config: &ResolvedConfig,
    provider: &str,
    model: &str,
) -> Result<vyane_config::RawRoot> {
    let provider_config = config.providers.get(provider)?;
    let profile = vyane_config::ProfilePatch {
        provider: Some(provider.to_string()),
        protocol: Some(provider_config.protocol),
        harness: Some("none".to_string()),
        model: Some(vyane_core::ModelId::new(model)),
        sandbox: None,
        params: None,
        failover: None,
    };
    let mut profiles = BTreeMap::new();
    profiles.insert("__cli_target".to_string(), profile);
    Ok(vyane_config::RawRoot {
        providers: BTreeMap::new(),
        profiles,
    })
}

fn resolve_temp_profile(
    root: vyane_config::RawRoot,
    profile: &str,
    loaded: &LoadedConfig,
) -> Result<Vec<BoundTarget>> {
    let mut layers = ConfigLayers {
        providers: loaded.config.providers.clone(),
        profiles: loaded.config.profiles.clone(),
    };
    layers.merge(&root)?;
    let config: ResolvedConfig = layers.into();
    Ok(vec![config.resolve_profile_with(profile, &|key| {
        loaded.env_lookup(key)
    })?])
}

fn parse_provider_model(raw: &str) -> Option<(&str, &str)> {
    let (provider, model) = raw.split_once('/')?;
    (!provider.is_empty() && !model.is_empty()).then_some((provider, model))
}

fn split_targets(raw: &str) -> Result<Vec<String>> {
    let targets = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if targets.is_empty() {
        bail!("--targets must include at least one target");
    }
    Ok(targets)
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
