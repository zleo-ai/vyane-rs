use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context as _, Result, anyhow, bail};
use chrono::Utc;
use serde::Serialize;
use vyane_goal::{
    AcceptanceCriterion, GoalEvent, GoalQuery, GoalRecord, GoalStatus, GoalStore, NewGoal,
    SqliteGoalStore,
};

use crate::app::StoragePaths;
use crate::cli::{
    GoalClaimArgs, GoalClaimNextArgs, GoalCommand, GoalCommonArgs, GoalCreateArgs, GoalDoneArgs,
    GoalFailArgs, GoalGetArgs, GoalIdArgs, GoalListArgs, GoalNextArgs, GoalProgressArgs,
    GoalReasonArgs, GoalSatisfyArgs, GoalStatusArg,
};

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
    db: String,
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
struct ErrorOutput<'a> {
    status: &'static str,
    error: &'a str,
}

pub fn run(command: GoalCommand) -> Result<ExitCode> {
    let json = common(&command).json;
    match run_inner(command) {
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

fn run_inner(command: GoalCommand) -> Result<ExitCode> {
    match command {
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
        GoalCommand::Progress(args) => progress(args),
        GoalCommand::Pause(args) => pause(args),
        GoalCommand::Resume(args) => resume(args),
        GoalCommand::Done(args) => done(args),
        GoalCommand::Fail(args) => fail(args),
        GoalCommand::Cancel(args) => cancel(args),
    }
}

fn create(args: GoalCreateArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let mut new_goal = NewGoal::new(args.title, Utc::now());
    new_goal.id = args.id;
    new_goal.description = args.description;
    new_goal.priority = args.priority;
    new_goal.parent_goal_id = args.parent;
    new_goal.acceptance_criteria = parse_acceptance(&args.acceptance)?;
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
    if args.common.json {
        print_json(&GoalDetailOutput {
            status: "success",
            goal,
            events,
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
        .satisfy_criterion(&args.common.owner, &args.id, args.index, Utc::now())
        .context("satisfy acceptance criterion")?;
    print_goal_result(&args.common, &db, goal)
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
            args.reason.as_deref(),
            Utc::now(),
        )
        .context("pause goal")?;
    print_goal_result(&args.common, &db, goal)
}

fn resume(args: GoalIdArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = store
        .resume(&args.common.owner, &args.id, Utc::now())
        .context("resume goal")?;
    print_goal_result(&args.common, &db, goal)
}

fn done(args: GoalDoneArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = store
        .done(
            &args.common.owner,
            &args.id,
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
        .fail(&args.common.owner, &args.id, &args.reason, Utc::now())
        .context("fail goal")?;
    print_goal_result(&args.common, &db, goal)
}

fn cancel(args: GoalReasonArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = store
        .cancel(
            &args.common.owner,
            &args.id,
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
