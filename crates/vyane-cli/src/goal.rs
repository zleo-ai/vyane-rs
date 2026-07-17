use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context as _, Result, anyhow, bail};
use chrono::Utc;
use serde::Serialize;
use vyane_core::CancellationToken;
use vyane_goal::{
    AcceptanceCriterion, AcceptanceVerification, AcceptanceVerifier, CriterionStatus, GoalEvent,
    GoalPursuer, GoalPursuitCheckpoint, GoalQuery, GoalRecord, GoalStatus, GoalStore,
    GoalVerificationArtifact, NewGoal, PursuitCheckpointStatus, PursuitConfig, PursuitOutcome,
    PursuitStatus, SqliteGoalStore,
};
use vyane_service::VyaneService;

use crate::app::StoragePaths;
use crate::cli::{
    GoalClaimArgs, GoalClaimNextArgs, GoalCommand, GoalCommonArgs, GoalCreateArgs, GoalDoneArgs,
    GoalFailArgs, GoalGetArgs, GoalIdArgs, GoalListArgs, GoalNextArgs, GoalProgressArgs,
    GoalPursueArgs, GoalReasonArgs, GoalResumeArgs, GoalSatisfyArgs, GoalStatusArg, GoalVerifyArgs,
};
use crate::goal_runtime::DispatchGoalRuntime;

#[derive(Debug, Serialize)]
struct GoalOutput {
    status: &'static str,
    goal: GoalRecord,
    db: String,
}

#[derive(Debug, Serialize)]
struct GoalDetailOutput {
    status: &'static str,
    goal: GoalRecord,
    events: Vec<GoalEvent>,
    verifications: Vec<GoalVerificationArtifact>,
    pursuit_checkpoint: Option<PursuitCheckpointView>,
    db: String,
}

#[derive(Debug, Serialize)]
struct PursuitCheckpointView {
    checkpoint_revision: u64,
    goal_revision: u64,
    claim_generation: u64,
    started_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
    segments_started: u16,
    segments_completed: u16,
    consecutive_failures: u16,
    status: PursuitCheckpointStatus,
    last_run_id: Option<String>,
    last_verification_id: Option<String>,
}

impl From<GoalPursuitCheckpoint> for PursuitCheckpointView {
    fn from(checkpoint: GoalPursuitCheckpoint) -> Self {
        Self {
            checkpoint_revision: checkpoint.checkpoint_revision,
            goal_revision: checkpoint.goal_revision,
            claim_generation: checkpoint.claim_generation,
            started_at: checkpoint.started_at,
            updated_at: checkpoint.updated_at,
            segments_started: checkpoint.segments_started,
            segments_completed: checkpoint.segments_completed,
            consecutive_failures: checkpoint.consecutive_failures,
            status: checkpoint.status,
            last_run_id: checkpoint.last_run_id,
            last_verification_id: checkpoint.last_verification_id,
        }
    }
}

#[derive(Debug, Serialize)]
struct GoalListOutput {
    status: &'static str,
    goals: Vec<GoalRecord>,
    count: usize,
    db: String,
}

#[derive(Debug, Serialize)]
struct GoalNextOutput {
    status: &'static str,
    goal: Option<GoalRecord>,
    db: String,
}

#[derive(Debug, Serialize)]
struct ProgressOutput {
    status: &'static str,
    goal: GoalRecord,
    event: GoalEvent,
    db: String,
}

#[derive(Debug, Serialize)]
struct VerifyOutput {
    status: &'static str,
    verification: AcceptanceVerification,
    artifact: GoalVerificationArtifact,
    goal: GoalRecord,
    db: String,
}

#[derive(Debug, Serialize)]
struct PursueOutput {
    status: &'static str,
    pursuit: PursuitOutcome,
    goal: GoalRecord,
    db: String,
}

#[derive(Debug, Serialize)]
struct ErrorOutput<'a> {
    status: &'static str,
    error: &'a str,
}

pub async fn run(config_path: Option<PathBuf>, command: GoalCommand) -> Result<ExitCode> {
    let json = common(&command).json;
    let result = match command {
        GoalCommand::Create(args) => create(args),
        GoalCommand::Get(args) => get(args),
        GoalCommand::List(args) => list(args),
        GoalCommand::Next(args) => next(args),
        GoalCommand::Start(args) => start(args),
        GoalCommand::Claim(args) => claim(args),
        GoalCommand::ClaimNext(args) => claim_next(args),
        GoalCommand::Renew(args) => renew(args),
        GoalCommand::Reclaim(args) => reclaim(args),
        GoalCommand::Satisfy(args) => satisfy(args),
        GoalCommand::Verify(args) => verify(args),
        GoalCommand::Pursue(args) => pursue(config_path, args).await,
        GoalCommand::Progress(args) => progress(args),
        GoalCommand::Pause(args) => pause(args),
        GoalCommand::Resume(args) => resume(args),
        GoalCommand::Done(args) => done(args),
        GoalCommand::Fail(args) => fail(args),
        GoalCommand::Cancel(args) => cancel(args),
    };
    match result {
        Ok(code) => Ok(code),
        Err(error) => {
            let message = format!("{error:#}");
            if json {
                if let Err(write_error) = print_json(&ErrorOutput {
                    status: "error",
                    error: &message,
                }) {
                    eprintln!("goal error: {message}; could not write JSON error: {write_error:#}");
                }
            } else {
                eprintln!("goal error: {message}");
            }
            Ok(ExitCode::from(2))
        }
    }
}

async fn pursue(config_path: Option<PathBuf>, args: GoalPursueArgs) -> Result<ExitCode> {
    if args.common.owner != "local" {
        bail!("goal pursue currently requires the local single-user owner scope");
    }
    let (store, db) = open_store(&args.common)?;
    let goal = require_goal(&store, &args.common.owner, &args.id)?;
    if goal.status != GoalStatus::InProgress {
        bail!(
            "goal `{}` must be in_progress before pursuit; current status is {}",
            args.id,
            goal.status
        );
    }
    if !goal.lease_active(Utc::now()) {
        bail!(
            "goal `{}` requires an active worker lease before pursuit",
            args.id
        );
    }
    if goal.claimed_by.as_deref() != Some(args.worker.as_str()) {
        bail!(
            "goal `{}` has an active lease held by `{}`; pass the matching --worker",
            args.id,
            goal.claimed_by.as_deref().unwrap_or("unknown")
        );
    }
    let workdir = match args.workdir {
        Some(workdir) => workdir,
        None => std::env::current_dir().context("resolve pursuit workdir")?,
    };
    let workdir = std::fs::canonicalize(&workdir).context("canonicalize pursuit workdir")?;
    let verifier = AcceptanceVerifier::new(
        &workdir,
        std::time::Duration::from_secs(args.verifier_timeout_seconds),
    )
    .context("construct pursuit verifier")?;
    let config = PursuitConfig {
        workdir,
        runtime: args.target.clone(),
        worker_id: args.worker,
        overall_timeout: std::time::Duration::from_secs(args.overall_timeout_seconds),
        segment_timeout: std::time::Duration::from_secs(args.segment_timeout_seconds),
        max_segments: args.max_segments,
        max_failures: args.max_failures,
    };
    config.validate().context("validate goal pursuit")?;
    let service =
        Arc::new(VyaneService::load(config_path.as_deref()).context("load pursuit runtime")?);
    if !args.target.eq_ignore_ascii_case("auto") {
        service
            .resolve(&args.target)
            .context("resolve pursuit target")?;
    }
    let (cancel, signal_task) = cancellation_token();
    let runtime = DispatchGoalRuntime::new(service, args.target.clone(), args.sandbox.into());
    let pursuer =
        GoalPursuer::new(&store, &runtime, &verifier, config).context("construct goal pursuer")?;
    let outcome = pursuer
        .pursue_with_cancel(&args.common.owner, &args.id, cancel)
        .await;
    signal_task.abort();
    let _ = signal_task.await;
    let outcome = outcome.context("pursue goal")?;
    let goal = require_goal(&store, &args.common.owner, &args.id)?;
    let (response_status, code) = match outcome.status {
        PursuitStatus::Achieved => ("success", ExitCode::SUCCESS),
        PursuitStatus::Paused => ("paused", ExitCode::from(3)),
        PursuitStatus::Stopped => ("stopped", ExitCode::from(4)),
    };
    if args.common.json {
        print_json(&PursueOutput {
            status: response_status,
            pursuit: outcome,
            goal,
            db: path_text(&db),
        })?;
    } else {
        println!("{}", terminal_safe(&outcome.summary));
    }
    Ok(code)
}

fn cancellation_token() -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let token = CancellationToken::new();
    let child = token.clone();
    let task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            child.cancel();
        }
    });
    (token, task)
}

fn create(args: GoalCreateArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let mut new_goal = NewGoal::new(args.title, Utc::now());
    new_goal.id = args.id;
    new_goal.description = args.description;
    new_goal.priority = args.priority;
    new_goal.parent_goal_id = args.parent;
    new_goal.acceptance_criteria = parse_acceptance(&args.acceptance)?;
    new_goal.continuity_policy = args
        .continuity_policy_json
        .as_deref()
        .map(serde_json::from_str::<vyane_goal::GoalContinuityPolicy>)
        .transpose()
        .context("parse continuity policy JSON")?;
    let goal = store
        .create(&args.common.owner, new_goal)
        .context("create goal")?;
    print_goal_result(&args.common, &db, goal)
}

fn get(args: GoalGetArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = require_goal(&store, &args.common.owner, &args.id)?;
    let events = store
        .events(&args.common.owner, &args.id)
        .context("read goal events")?;
    let verifications = store
        .verifications(&args.common.owner, &args.id)
        .context("read goal verification artifacts")?;
    if args.common.json {
        let pursuit_checkpoint = store
            .pursuit_checkpoint(&args.common.owner, &args.id)
            .context("read goal pursuit checkpoint")?
            .map(PursuitCheckpointView::from);
        print_json(&GoalDetailOutput {
            status: "success",
            goal,
            events,
            verifications,
            pursuit_checkpoint,
            db: path_text(&db),
        })?;
    } else {
        print_goal_line(&goal)?;
        for event in events {
            println!(
                "{}\t{}\t{}",
                event.revision,
                event.occurred_at.to_rfc3339(),
                event_kind_text(event.kind)
            );
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn list(args: GoalListArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let query = GoalQuery {
        statuses: args.states.into_iter().map(GoalStatus::from).collect(),
        parent_goal_id: args.parent,
        limit: args.limit,
    };
    let goals = store
        .list(&args.common.owner, &query)
        .context("list goals")?;
    if args.common.json {
        let count = goals.len();
        print_json(&GoalListOutput {
            status: "success",
            goals,
            count,
            db: path_text(&db),
        })?;
    } else {
        for goal in goals {
            print_goal_line(&goal)?;
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn next(args: GoalNextArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let mut goal = store
        .next_queued(&args.common.owner)
        .context("select next queued goal")?;
    if args.auto_start {
        goal = match goal {
            Some(selected) => Some(
                store
                    .start(&args.common.owner, &selected.id, Utc::now())
                    .context("auto-start next queued goal")?,
            ),
            None => None,
        };
    }
    if args.common.json {
        print_json(&GoalNextOutput {
            status: "success",
            goal,
            db: path_text(&db),
        })?;
    } else if let Some(goal) = goal {
        print_goal_line(&goal)?;
    }
    Ok(ExitCode::SUCCESS)
}

fn start(args: GoalIdArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = store
        .start(&args.common.owner, &args.id, Utc::now())
        .context("start goal")?;
    print_goal_result(&args.common, &db, goal)
}

fn claim(args: GoalClaimArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = store
        .claim(
            &args.common.owner,
            &args.id,
            &args.worker,
            args.lease_seconds,
            Utc::now(),
        )
        .context("claim goal")?;
    print_goal_result(&args.common, &db, goal)
}

fn claim_next(args: GoalClaimNextArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = store
        .claim_next(
            &args.common.owner,
            &args.worker,
            args.lease_seconds,
            Utc::now(),
        )
        .context("claim next queued goal")?;
    if args.common.json {
        print_json(&GoalNextOutput {
            status: "success",
            goal,
            db: path_text(&db),
        })?;
    } else if let Some(goal) = goal {
        print_goal_line(&goal)?;
    }
    Ok(ExitCode::SUCCESS)
}

fn renew(args: GoalClaimArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = store
        .renew_lease(
            &args.common.owner,
            &args.id,
            &args.worker,
            args.lease_seconds,
            Utc::now(),
        )
        .context("renew goal lease")?;
    print_goal_result(&args.common, &db, goal)
}

fn reclaim(args: GoalClaimArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = store
        .reclaim(
            &args.common.owner,
            &args.id,
            &args.worker,
            args.lease_seconds,
            Utc::now(),
        )
        .context("reclaim goal")?;
    print_goal_result(&args.common, &db, goal)
}

fn satisfy(args: GoalSatisfyArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = store
        .satisfy_criterion(
            &args.common.owner,
            &args.id,
            args.worker.as_deref(),
            args.index,
            Utc::now(),
        )
        .context("satisfy acceptance criterion")?;
    print_goal_result(&args.common, &db, goal)
}

fn verify(args: GoalVerifyArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = require_goal(&store, &args.common.owner, &args.id)?;
    let preflight_at = chrono::Utc::now();
    if goal.status != GoalStatus::InProgress {
        bail!(
            "goal `{}` must be in_progress before verification; current status is {}",
            args.id,
            goal.status
        );
    }
    if goal.lease_active(preflight_at) && args.worker.as_deref() != goal.claimed_by.as_deref() {
        bail!(
            "goal `{}` has an active lease held by `{}`; pass the matching --worker",
            args.id,
            goal.claimed_by.as_deref().unwrap_or("unknown")
        );
    }
    let workdir = args
        .workdir
        .unwrap_or(std::env::current_dir().context("resolve acceptance workdir")?);
    let verifier = AcceptanceVerifier::new(
        workdir,
        std::time::Duration::from_secs(args.timeout_seconds),
    )
    .context("construct acceptance verifier")?;
    let verification = verifier.verify(&goal);
    let verified_at = chrono::Utc::now();
    let artifact = store
        .record_verification(
            &args.common.owner,
            &args.id,
            args.worker.as_deref(),
            &verification,
            verified_at,
        )
        .context("persist verification artifact")?;
    for result in &verification.results {
        if result.status == CriterionStatus::Satisfied
            && goal
                .acceptance_criteria
                .get(result.criterion_index)
                .is_some_and(|criterion| criterion.satisfied_at.is_none())
        {
            store
                .satisfy_criterion(
                    &args.common.owner,
                    &args.id,
                    args.worker.as_deref(),
                    result.criterion_index,
                    verified_at,
                )
                .with_context(|| {
                    format!("persist satisfied criterion {}", result.criterion_index)
                })?;
        }
    }
    let goal = require_goal(&store, &args.common.owner, &args.id)?;
    let status = if verification.all_satisfied {
        "success"
    } else {
        "inconclusive"
    };
    let all_satisfied = verification.all_satisfied;
    if args.common.json {
        print_json(&VerifyOutput {
            status,
            verification,
            artifact,
            goal,
            db: path_text(&db),
        })?;
    } else {
        println!("{}", terminal_safe(&verification.summary));
    }
    Ok(if all_satisfied {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(3)
    })
}

fn progress(args: GoalProgressArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let event = store
        .progress(
            &args.common.owner,
            &args.id,
            &args.stage,
            &args.detail,
            Utc::now(),
        )
        .context("record goal progress")?;
    let goal = require_goal(&store, &args.common.owner, &args.id)?;
    if args.common.json {
        print_json(&ProgressOutput {
            status: "success",
            goal,
            event,
            db: path_text(&db),
        })?;
    } else {
        println!("{}", terminal_safe(&event.event_id));
    }
    Ok(ExitCode::SUCCESS)
}

fn pause(args: GoalReasonArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = store
        .pause(
            &args.common.owner,
            &args.id,
            args.worker.as_deref(),
            args.reason.as_deref(),
            Utc::now(),
        )
        .context("pause goal")?;
    print_goal_result(&args.common, &db, goal)
}

fn resume(args: GoalResumeArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = store
        .resume(
            &args.common.owner,
            &args.id,
            args.worker.as_deref(),
            Utc::now(),
        )
        .context("resume goal")?;
    print_goal_result(&args.common, &db, goal)
}

fn done(args: GoalDoneArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = store
        .done(
            &args.common.owner,
            &args.id,
            args.worker.as_deref(),
            args.summary.as_deref(),
            args.waive.as_deref(),
            Utc::now(),
        )
        .context("complete goal")?;
    print_goal_result(&args.common, &db, goal)
}

fn fail(args: GoalFailArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = store
        .fail(
            &args.common.owner,
            &args.id,
            args.worker.as_deref(),
            &args.reason,
            Utc::now(),
        )
        .context("fail goal")?;
    print_goal_result(&args.common, &db, goal)
}

fn cancel(args: GoalReasonArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = store
        .cancel(
            &args.common.owner,
            &args.id,
            args.worker.as_deref(),
            args.reason.as_deref(),
            Utc::now(),
        )
        .context("cancel goal")?;
    print_goal_result(&args.common, &db, goal)
}

fn parse_acceptance(values: &[String]) -> Result<Vec<AcceptanceCriterion>> {
    values
        .iter()
        .map(|value| {
            let Some((kind, target)) = value.split_once(':') else {
                bail!("--acceptance must be KIND:TARGET");
            };
            let kind = kind.trim();
            let target = target.trim();
            if kind.is_empty() || target.is_empty() {
                bail!("--acceptance kind and target must not be empty");
            }
            Ok(AcceptanceCriterion::new(kind, target))
        })
        .collect()
}

fn require_goal(store: &SqliteGoalStore, owner: &str, id: &str) -> Result<GoalRecord> {
    store
        .get(owner, id)
        .context("read goal")?
        .ok_or_else(|| anyhow!("goal `{id}` was not found"))
}

fn print_goal_result(common: &GoalCommonArgs, db: &Path, goal: GoalRecord) -> Result<ExitCode> {
    if common.json {
        print_json(&GoalOutput {
            status: "success",
            goal,
            db: path_text(db),
        })?;
    } else {
        print_goal_line(&goal)?;
    }
    Ok(ExitCode::SUCCESS)
}

fn print_goal_line(goal: &GoalRecord) -> Result<()> {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    writeln!(
        stdout,
        "{}\t{}\t{}\t{}",
        terminal_safe(&goal.id),
        goal.status,
        goal.priority,
        terminal_safe(&goal.title)
    )
    .context("write goal response")?;
    stdout.flush().context("flush goal response")
}

fn open_store(common: &GoalCommonArgs) -> Result<(SqliteGoalStore, PathBuf)> {
    let path = match &common.db {
        Some(path) => path.clone(),
        None => StoragePaths::resolve()?.goal_db_path(),
    };
    let store = SqliteGoalStore::open(&path)
        .with_context(|| format!("open goal database {}", path.display()))?;
    Ok((store, path))
}

fn common(command: &GoalCommand) -> &GoalCommonArgs {
    match command {
        GoalCommand::Create(args) => &args.common,
        GoalCommand::Get(args) => &args.common,
        GoalCommand::List(args) => &args.common,
        GoalCommand::Next(args) => &args.common,
        GoalCommand::Start(args) => &args.common,
        GoalCommand::Claim(args) | GoalCommand::Renew(args) | GoalCommand::Reclaim(args) => {
            &args.common
        }
        GoalCommand::ClaimNext(args) => &args.common,
        GoalCommand::Satisfy(args) => &args.common,
        GoalCommand::Verify(args) => &args.common,
        GoalCommand::Pursue(args) => &args.common,
        GoalCommand::Progress(args) => &args.common,
        GoalCommand::Pause(args) => &args.common,
        GoalCommand::Resume(args) => &args.common,
        GoalCommand::Done(args) => &args.common,
        GoalCommand::Fail(args) => &args.common,
        GoalCommand::Cancel(args) => &args.common,
    }
}

fn print_json(value: &impl Serialize) -> Result<()> {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer(&mut stdout, value).context("write JSON response")?;
    stdout.write_all(b"\n").context("finish JSON response")?;
    stdout.flush().context("flush JSON response")
}

fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn terminal_safe(value: &str) -> String {
    value.chars().flat_map(char::escape_default).collect()
}

const fn event_kind_text(kind: vyane_goal::GoalEventKind) -> &'static str {
    match kind {
        vyane_goal::GoalEventKind::Created => "created",
        vyane_goal::GoalEventKind::Started => "started",
        vyane_goal::GoalEventKind::Claimed => "claimed",
        vyane_goal::GoalEventKind::LeaseRenewed => "lease_renewed",
        vyane_goal::GoalEventKind::Reclaimed => "reclaimed",
        vyane_goal::GoalEventKind::Progress => "progress",
        vyane_goal::GoalEventKind::CriterionSatisfied => "criterion_satisfied",
        vyane_goal::GoalEventKind::CriteriaWaived => "criteria_waived",
        vyane_goal::GoalEventKind::Paused => "paused",
        vyane_goal::GoalEventKind::Resumed => "resumed",
        vyane_goal::GoalEventKind::Completed => "completed",
        vyane_goal::GoalEventKind::Failed => "failed",
        vyane_goal::GoalEventKind::Cancelled => "cancelled",
    }
}

impl From<GoalStatusArg> for GoalStatus {
    fn from(value: GoalStatusArg) -> Self {
        match value {
            GoalStatusArg::Queued => Self::Queued,
            GoalStatusArg::InProgress => Self::InProgress,
            GoalStatusArg::Paused => Self::Paused,
            GoalStatusArg::Completed => Self::Completed,
            GoalStatusArg::Failed => Self::Failed,
            GoalStatusArg::Cancelled => Self::Cancelled,
        }
    }
}

#[cfg(test)]
mod tests {
    use vyane_core::RunStatus;
    use vyane_goal::PursuitSegmentStatus;

    use crate::goal_runtime::pursuit_segment_status;

    #[test]
    fn every_run_status_has_an_exact_pursuit_status() {
        for (run, pursuit) in [
            (RunStatus::Success, PursuitSegmentStatus::Success),
            (RunStatus::Timeout, PursuitSegmentStatus::Timeout),
            (RunStatus::Cancelled, PursuitSegmentStatus::Cancelled),
            (RunStatus::Error, PursuitSegmentStatus::Error),
        ] {
            assert_eq!(pursuit_segment_status(run), pursuit);
        }
    }
}
