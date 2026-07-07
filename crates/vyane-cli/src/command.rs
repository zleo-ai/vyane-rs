use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use vyane_config::{ConfigLayers, ResolvedConfig};
use vyane_core::{
    BoundTarget, CancellationToken, Harness, HarnessKind, ProviderId, RunQuery, RunStatus,
    SessionRef, TaskSpec,
};
use vyane_harness::{ClaudeCodeHarness, CodexCliHarness};

use crate::app::{LoadedConfig, Runtime, StoragePaths, load_config};
use crate::cli::{
    BroadcastArgs, Cli, Command, DispatchArgs, HistoryArgs, TaskCancelArgs, TaskCommand,
    TaskListArgs, TaskStatusArgs, WorkerArgs,
};
use crate::output::{BroadcastJson, BroadcastRow, RunJson};
use crate::task::proc::{SIGKILL, SIGTERM, pgid_of, pid_alive, signal_group};
use crate::task::store::{
    JobSpec, StatusFile, TaskPaths, TaskState, interpret_state, list_tasks,
};

pub async fn run(cli: Cli) -> Result<ExitCode> {
    match cli.command {
        Command::Check => run_check(cli.config).await,
        Command::Dispatch(args) => run_dispatch(cli.config, args).await,
        Command::Broadcast(args) => run_broadcast(cli.config, args).await,
        Command::History(args) => run_history(args).await,
        Command::Sessions(args) => run_sessions(args).await,
        Command::Task(task) => run_task(task).await,
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
    // "fix your config" apart from "the run failed" (exit 1). This validation
    // runs FIRST — before `--detach` spawns anything — so a bad config never
    // leaves a stray task directory behind.
    let phase = load_config(config_path.as_deref())
        .and_then(|loaded| resolve_target_chain(&loaded, &args.target).map(|c| (loaded, c)));
    let (loaded, chain) = match phase {
        Ok(value) => value,
        Err(error) => {
            eprintln!("config error: {error:#}");
            return Ok(ExitCode::from(2));
        }
    };

    // Detached path: freeze the request and hand it to a re-exec'd worker, then
    // return immediately. Config is already validated above, so reaching here
    // means the target resolves.
    if args.detach {
        return spawn_detached_dispatch(config_path, args);
    }

    let json = args.json;
    let task = task_from_dispatch(args)?;
    let runtime = Runtime::new(loaded.config, StoragePaths::resolve()?)?;
    let cancel = cancellation_token();
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

/// Freeze the dispatch request into a task directory and spawn a detached
/// worker to run it. Prints the run id and returns exit 0 without waiting.
///
/// The target chain was already resolved by the caller (so config errors have
/// exited 2 before we get here); we re-serialize the raw selector string into
/// the job so the worker re-resolves it identically.
fn spawn_detached_dispatch(config_path: Option<PathBuf>, args: DispatchArgs) -> Result<ExitCode> {
    let paths_root = StoragePaths::resolve()?;
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
    let paths = TaskPaths::new(&paths_root.tasks_dir, &run_id);
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
async fn run_worker(config_path: Option<PathBuf>, args: WorkerArgs) -> Result<ExitCode> {
    let storage = StoragePaths::resolve()?;
    let paths = TaskPaths::new(&storage.tasks_dir, &args.id);

    let job = paths.read_job()?;
    // The job's own recorded config override wins over any inherited flag; the
    // parent always writes it (possibly `None`).
    let config_path = job.config.clone().or(config_path);

    // Re-resolve config + target chain exactly as an online dispatch would.
    // The parent already validated this, but the worker is a fresh process, so
    // it resolves independently. A failure here is recorded as an error status.
    let resolved = load_config(config_path.as_deref())
        .and_then(|loaded| resolve_target_chain(&loaded, &job.target).map(|c| (loaded, c)));

    let pid = std::process::id() as i32;
    let pgid = pgid_of(pid).unwrap_or(pid);
    let workdir = job.workdir.as_ref().map(|p| p.to_string_lossy().into_owned());

    let (loaded, chain) = match resolved {
        Ok(value) => value,
        Err(error) => {
            // Config could not be resolved in the worker: record a terminal
            // error status so `task status` explains why, and exit nonzero.
            let mut status =
                StatusFile::running(&job.run_id, pid, pgid, &job.target, workdir.clone());
            status.state = TaskState::Error;
            status.finished_at = Some(chrono::Utc::now());
            status.error = Some(format!("config error: {error:#}"));
            paths.write_status(&status)?;
            eprintln!("config error: {error:#}");
            return Ok(ExitCode::from(1));
        }
    };

    // The status target is a best-effort label: the first resolved target's
    // identity reads better than the raw selector, but falls back to it.
    let target_label = chain
        .first()
        .map(|bound| bound.target.to_string())
        .unwrap_or_else(|| job.target.clone());

    // 1) Announce `running` up front (atomic write) so `task list/status`
    //    observe the run the instant the worker is live.
    let running = StatusFile::running(&job.run_id, pid, pgid, &target_label, workdir.clone());
    paths.write_status(&running)?;

    // 2) A SIGTERM handler cancels the kernel token, so `task cancel` lets the
    //    dispatch unwind cleanly (RunRecord lands, status becomes `cancelled`)
    //    instead of the process being torn down mid-run.
    let cancel = worker_cancellation_token();

    let task = task_from_job(&job)?;
    let runtime = Runtime::new(loaded.config, StoragePaths::resolve()?)?;
    let outcome = runtime.dispatcher.dispatch(&task, chain, cancel).await?;
    let record = outcome.record;
    let output = outcome.output;

    // 3) Persist the answer (if any) beside the status, then finalize status.
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
    let storage = StoragePaths::resolve()?;
    let rows = list_tasks(&storage.tasks_dir, pid_alive);

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
    let storage = StoragePaths::resolve()?;
    let paths = TaskPaths::new(&storage.tasks_dir, &args.id);

    let status = match paths.read_status() {
        Ok(status) => status,
        Err(_) => {
            eprintln!("no such detached run: {}", args.id);
            return Ok(ExitCode::from(1));
        }
    };
    let displayed = interpret_state(&status, pid_alive);

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
    let storage = StoragePaths::resolve()?;
    let paths = TaskPaths::new(&storage.tasks_dir, &args.id);

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

    let pgid = if status.pgid > 0 {
        status.pgid
    } else {
        pgid_of(status.pid).unwrap_or(status.pid)
    };

    // SIGTERM the whole group: the worker catches it and finalizes; any harness
    // grandchildren it spawned die with the group.
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
        eprintln!(
            "{}: kill delivered; worker did not finalize",
            args.id
        );
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
