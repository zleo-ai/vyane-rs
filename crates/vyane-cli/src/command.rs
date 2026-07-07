use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use vyane_config::{ConfigLayers, ResolvedConfig};
use vyane_core::{
    BoundTarget, CancellationToken, Harness, HarnessKind, ProviderId, RunQuery, RunStatus,
    SessionRef, TaskSpec,
};
use vyane_harness::{ClaudeCodeHarness, CodexCliHarness};
use vyane_workflow::{StepEvent, TargetResolver, Workflow, WorkflowEngine, WorkflowError};

use crate::app::{LoadedConfig, Runtime, StoragePaths, load_config};
use crate::cli::{
    BroadcastArgs, Cli, Command, DispatchArgs, HistoryArgs, WorkflowCommand, WorkflowResumeArgs,
    WorkflowRunArgs,
};
use crate::output::{BroadcastJson, BroadcastRow, RunJson};

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
