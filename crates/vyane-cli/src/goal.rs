use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context as _, Result, anyhow, bail};
use chrono::Utc;
use serde::Serialize;
use vyane_core::{CancellationToken, RunStatus, Sandbox};
use vyane_goal::{
    AcceptanceCriterion, AcceptanceVerification, AcceptanceVerifier, CriterionStatus,
    GoalContinuityNextAction, GoalContinuityReviewCheck, GoalContinuitySignal,
    GoalContinuitySignalKind, GoalContinuitySignalResult, GoalEvent, GoalPursuer,
    GoalPursuitCheckpoint, GoalQuery, GoalRecord, GoalStatus, GoalStore, GoalVerificationArtifact,
    NewGoal, PursuitCheckpointStatus, PursuitConfig, PursuitOutcome, PursuitStatus,
    SqliteGoalStore, TakeoverApproval, TakeoverApprovalRequest, TakeoverApprovalStatus,
    TakeoverBoundTarget, TakeoverDecision, TakeoverFinish, TakeoverRunStatus, TakeoverSandbox,
    project_continuity_next_action,
};
use vyane_service::{DispatchParams, VyaneService};

use crate::app::StoragePaths;
use crate::cli::{
    GoalClaimArgs, GoalClaimNextArgs, GoalCommand, GoalCommonArgs, GoalContinuityDecisionArgs,
    GoalContinuityExecuteArgs, GoalContinuityQueueArgs, GoalContinuitySignalArgs, GoalCreateArgs,
    GoalDoneArgs, GoalFailArgs, GoalGetArgs, GoalIdArgs, GoalListArgs, GoalNextArgs,
    GoalProgressArgs, GoalPursueArgs, GoalReasonArgs, GoalResumeArgs, GoalSatisfyArgs,
    GoalStatusArg, GoalVerifyArgs, SandboxArg,
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
struct TakeoverApprovalOutput {
    status: &'static str,
    approval: TakeoverApproval,
    db: String,
}

#[derive(Debug, Serialize)]
struct ContinuitySignalOutput {
    status: &'static str,
    changed: bool,
    result: GoalContinuitySignalResult,
    db: String,
}

#[derive(Debug, Serialize)]
struct ContinuityNextOutput {
    status: &'static str,
    next_action: GoalContinuityNextAction,
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
        GoalCommand::ContinuityNext(args) => continuity_next(args),
        GoalCommand::ContinuityQueue(args) => continuity_queue(args),
        GoalCommand::ContinuityDecide(args) => continuity_decide(args),
        GoalCommand::ContinuityExecute(args) => continuity_execute(config_path, args).await,
        GoalCommand::ContinuitySignal(args) => continuity_signal(args),
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
                    eprintln!(
                        "{}",
                        crate::output::format_goal_error_line(&format!(
                            "{message}; could not write JSON error: {write_error:#}"
                        ))
                    );
                }
            } else {
                eprintln!("{}", crate::output::format_goal_error_line(&message));
            }
            Ok(ExitCode::from(2))
        }
    }
}

fn continuity_next(args: GoalIdArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let snapshot = store
        .continuity_projection_snapshot(&args.common.owner, &args.id)
        .context("read continuity projection snapshot")?
        .ok_or_else(|| anyhow!("goal `{}` was not found", args.id))?;
    let next_action = project_continuity_next_action(&snapshot.goal, &snapshot.approvals)
        .context("project continuity next action")?;
    // Kind-only pure continuity status/mode on next-action projection (WP-401).
    if let Some(state) = snapshot.goal.continuity_state.as_ref() {
        tracing::info!(
            status = state.state.as_str(),
            "{}",
            crate::output::format_goal_continuity_status_line(state.state)
        );
        // Step-level pure status for next-ready and projected step (WP-403).
        let plan = &state.handoff_plan;
        if !plan.next_ready_step.is_empty()
            && let Some(step) = plan
                .steps
                .iter()
                .find(|step| step.id == plan.next_ready_step)
        {
            tracing::info!(
                status = step.status.as_str(),
                "{}",
                crate::output::format_goal_continuity_step_status_line(step.status)
            );
        }
        if let Some(step_id) = next_action.step_id.as_deref()
            && step_id != plan.next_ready_step
            && let Some(step) = plan.steps.iter().find(|step| step.id == step_id)
        {
            tracing::info!(
                status = step.status.as_str(),
                "{}",
                crate::output::format_goal_continuity_step_status_line(step.status)
            );
        }
    }
    if let Some(policy) = snapshot.goal.continuity_policy.as_ref() {
        tracing::info!(
            mode = policy.mode.as_str(),
            "{}",
            crate::output::format_goal_continuity_mode_line(policy.mode)
        );
    }
    // Kind-only pure projected next-action kind (WP-406).
    tracing::info!(
        action = next_action.action.as_str(),
        "{}",
        crate::output::format_goal_continuity_next_action_kind_line(next_action.action)
    );
    // Kind-only pure ready signal kinds from durable continuity state (WP-451).
    if let Some(state) = snapshot.goal.continuity_state.as_ref() {
        for signal in &state.ready_signals {
            tracing::info!(
                kind = signal.kind.as_str(),
                "{}",
                crate::output::format_goal_continuity_signal_kind_line(signal.kind)
            );
        }
    }
    // Kind-only pure accepted signal kinds when projected (WP-446).
    for kind in &next_action.accepted_signals {
        tracing::info!(
            kind = kind.as_str(),
            "{}",
            crate::output::format_goal_continuity_signal_kind_line(*kind)
        );
    }
    // Kind-only pure operator command when projected (WP-409).
    if let Some(command) = next_action.command {
        tracing::info!(
            command = command.as_str(),
            "{}",
            crate::output::format_goal_continuity_operator_command_line(command)
        );
    }
    if args.common.json {
        print_json(&ContinuityNextOutput {
            status: "success",
            next_action,
            db: path_text(&db),
        })?;
    } else {
        print!("{}", format_continuity_next_action(&next_action));
    }
    Ok(ExitCode::SUCCESS)
}

fn continuity_signal(args: GoalContinuitySignalArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let kind = GoalContinuitySignalKind::from(args.signal);
    let review_check = match (
        args.repository,
        args.pull_request,
        args.observation_id,
        args.observation_sequence,
    ) {
        (
            Some(repository),
            Some(pull_request),
            Some(observation_id),
            Some(observation_sequence),
        ) => Some(GoalContinuityReviewCheck {
            repository,
            pull_request,
            observation_id,
            observation_sequence,
        }),
        (None, None, None, None) => None,
        _ => bail!(
            "--repository, --pull-request, --observation-id and --observation-sequence must be supplied together"
        ),
    };
    let signal = GoalContinuitySignal {
        kind,
        quota_event_id: args.quota_event_id,
        provider: args.provider,
        harness: args.harness,
        model: args.model,
        observed_at: Utc::now(),
        source: args.source,
        review_check,
    };
    let result = store
        .record_continuity_signal(&args.common.owner, &args.id, &signal, Utc::now())
        .context("record continuity signal")?;
    // Kind-only pure signal kind on continuity-signal settle (WP-408).
    tracing::info!(
        kind = kind.as_str(),
        "{}",
        crate::output::format_goal_continuity_signal_kind_line(kind)
    );
    if args.common.json {
        print_json(&ContinuitySignalOutput {
            status: "success",
            changed: result.changed,
            result,
            db: path_text(&db),
        })?;
    } else {
        println!(
            "{}",
            format_continuity_signal_line(
                &args.id,
                result.changed,
                &result.state.handoff_plan.next_ready_step,
            )
        );
    }
    Ok(ExitCode::SUCCESS)
}

fn continuity_queue(args: GoalContinuityQueueArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let goal = require_goal(&store, &args.common.owner, &args.id)?;
    let state = goal
        .continuity_state
        .as_ref()
        .context("goal has no visible continuity state")?;
    let step = state
        .handoff_plan
        .steps
        .iter()
        .find(|step| {
            matches!(
                (step.id.as_str(), step.kind.as_str()),
                ("takeover", "start_takeover")
                    | ("review_takeover", "review_takeover_work")
                    | ("repair_failed_review", "repair_review_failure")
                    | ("resume_primary", "resume_primary_after_reset")
            ) && step.status == vyane_goal::GoalContinuityStepStatus::Ready
                && step.requires_approval
        })
        .context("goal has no supported approval-required ready continuity step")?;
    let target = step
        .target
        .as_ref()
        .context("ready continuity step has no target")?;
    let workdir =
        std::fs::canonicalize(&args.workdir).context("canonicalize continuity workdir")?;
    let upstream_step = match step.id.as_str() {
        "review_takeover" => Some(("takeover", "start_takeover")),
        "repair_failed_review" => Some(("review_takeover", "review_takeover_work")),
        "resume_primary"
            if state.wait_for_review_checks_before_resume
                && state
                    .ready_signals
                    .iter()
                    .any(|signal| signal.kind == GoalContinuitySignalKind::ReviewChecksFailed) =>
        {
            Some(("repair_failed_review", "repair_review_failure"))
        }
        "resume_primary" if state.require_review_before_resume => {
            Some(("review_takeover", "review_takeover_work"))
        }
        _ => None,
    };
    let upstream = if let Some((upstream_step_id, upstream_step_kind)) = upstream_step {
        store
            .list_takeover_approvals(&args.common.owner, Some(&goal.id))
            .context("list continuity predecessor evidence")?
            .into_iter()
            .rev()
            .find(|approval| {
                approval.quota_event_id == state.quota_event_id
                    && approval.step_id == upstream_step_id
                    && approval.step_kind == upstream_step_kind
                    && approval.status == TakeoverApprovalStatus::Done
                    && approval.run_status == Some(TakeoverRunStatus::Success)
                    && approval.run_id.is_some()
            })
            .with_context(|| {
                if step.id == "review_takeover" {
                    "review step has no exact successful takeover run evidence".to_owned()
                } else {
                    format!(
                        "{} step has no exact successful {} run evidence",
                        step.id, upstream_step_id
                    )
                }
            })?
            .into()
    } else {
        None
    };
    let (upstream_approval_id, upstream_run_id, upstream_run_status) =
        upstream.map_or((None, None, None), |approval: TakeoverApproval| {
            (
                Some(approval.approval_id),
                approval.run_id,
                approval.run_status,
            )
        });
    let request = TakeoverApprovalRequest {
        goal_id: goal.id.clone(),
        step_id: step.id.clone(),
        step_kind: step.kind.clone(),
        quota_event_id: state.quota_event_id.clone(),
        target: TakeoverBoundTarget::from_execution(target),
        workdir,
        sandbox: takeover_sandbox(args.sandbox),
        timeout: std::time::Duration::from_secs(args.timeout_seconds),
        goal_revision: goal.revision,
        plan_snapshot: state.clone(),
        upstream_approval_id,
        upstream_run_id,
        upstream_run_status,
    };
    // Kind-only pure ready step status selected for queue (WP-452).
    tracing::info!(
        status = step.status.as_str(),
        "{}",
        crate::output::format_goal_continuity_step_status_line(step.status)
    );
    // Kind-only pure upstream run status when predecessor evidence is present (WP-453).
    if let Some(run_status) = upstream_run_status {
        tracing::info!(
            status = run_status.as_str(),
            "{}",
            crate::output::format_takeover_run_status_line(run_status)
        );
    }
    let approval = store
        .queue_takeover_approval(&args.common.owner, &request, Utc::now())
        .context("queue takeover approval")?;
    // Kind-only pure sandbox frozen on queue (WP-410).
    tracing::info!(
        sandbox = approval.sandbox.as_str(),
        "{}",
        crate::output::format_takeover_sandbox_line(approval.sandbox)
    );
    let status = approval.status.as_str();
    print_takeover_result(&args.common, &db, status, approval)
}

fn continuity_decide(args: GoalContinuityDecisionArgs) -> Result<ExitCode> {
    let (store, db) = open_store(&args.common)?;
    let decision = TakeoverDecision::from(args.decision);
    let approval = store
        .decide_takeover_approval(
            &args.common.owner,
            &args.approval_id,
            decision,
            &args.decided_by,
            args.reason.as_deref(),
            Utc::now(),
        )
        .context("decide takeover approval")?;
    // Kind-only pure lines for decision + approval status (WP-393).
    tracing::info!(
        decision = decision.as_str(),
        status = approval.status.as_str(),
        "{}; {}",
        crate::output::format_takeover_decision_line(decision),
        crate::output::format_takeover_approval_status_line(approval.status)
    );
    let status = match approval.status {
        TakeoverApprovalStatus::Approved => "approved",
        TakeoverApprovalStatus::Rejected => "rejected",
        _ => "success",
    };
    print_takeover_result(&args.common, &db, status, approval)
}

async fn continuity_execute(
    config_path: Option<PathBuf>,
    args: GoalContinuityExecuteArgs,
) -> Result<ExitCode> {
    if args.common.owner != "local" {
        bail!("goal continuity execute currently requires the local single-user owner scope");
    }
    let (store, db) = open_store(&args.common)?;
    let approval = store
        .get_takeover_approval(&args.common.owner, &args.approval_id)
        .context("read takeover approval")?
        .with_context(|| format!("takeover approval `{}` was not found", args.approval_id))?;
    // Kind-only pure approval status at execute admission (WP-457).
    tracing::info!(
        status = approval.status.as_str(),
        "{}",
        crate::output::format_takeover_approval_status_line(approval.status)
    );
    if approval.status != TakeoverApprovalStatus::Approved {
        bail!(
            "takeover approval `{}` is {} and cannot be executed",
            approval.approval_id,
            approval.status
        );
    }
    let goal = require_goal(&store, &args.common.owner, &approval.goal_id)?;
    let evidence = store
        .list_takeover_approvals(&args.common.owner, Some(&goal.id))
        .context("list continuity evidence for execution")?;
    let prompt = continuity_prompt(&goal, &approval, &evidence);
    let selector = approval.target.selector();
    let service =
        Arc::new(VyaneService::load(config_path.as_deref()).context("load continuity runtime")?);
    let resolved = service
        .resolve(&selector)
        .context("resolve approved continuity target")?;
    let [bound] = resolved.chain.as_slice() else {
        bail!("approved continuity target must resolve to exactly one target");
    };
    validate_resolved_continuity(bound, &approval.target)?;
    let current_workdir = std::fs::canonicalize(&approval.workdir)
        .context("revalidate approved continuity workdir")?;
    if current_workdir != approval.workdir {
        bail!("approved continuity workdir changed before execution");
    }

    let approval = store
        .consume_takeover_approval(&args.common.owner, &args.approval_id, Utc::now())
        .context("consume takeover approval")?;
    // Kind-only pure approval status after consume → InFlight (WP-458).
    tracing::info!(
        status = approval.status.as_str(),
        "{}",
        crate::output::format_takeover_approval_status_line(approval.status)
    );
    let (cancel, signal_task) = cancellation_token();
    let outcome = service
        .dispatch(
            DispatchParams {
                task: prompt,
                target: selector,
                workdir: Some(approval.workdir.clone()),
                sandbox: core_sandbox(approval.sandbox),
                session: None,
                system: None,
                timeout_secs: Some(approval.timeout_secs),
                labels: vec![
                    "source=goal-continuity".into(),
                    format!("continuity_step={}", approval.step_id),
                ],
            },
            cancel,
        )
        .await;
    signal_task.abort();
    let _ = signal_task.await;
    let (finish, exit) = match outcome {
        Ok(outcome) => {
            let run_status = takeover_run_status(outcome.record.status);
            let exit = if run_status.is_success() {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(4)
            };
            // Kind-only pure line for takeover run status (WP-395).
            tracing::info!(
                status = run_status.as_str(),
                "{}",
                crate::output::format_takeover_run_status_line(run_status)
            );
            (
                TakeoverFinish {
                    run_id: Some(outcome.record.run_id),
                    run_status,
                    detail: format!(
                        "continuity {} dispatch finished with {}",
                        approval.step_id,
                        run_status.as_str()
                    ),
                },
                exit,
            )
        }
        Err(error) => {
            // Kind-only pure line; free-form detail remains for operator envelope (WP-395).
            tracing::info!(
                status = TakeoverRunStatus::Error.as_str(),
                "{}",
                crate::output::format_takeover_run_status_line(TakeoverRunStatus::Error)
            );
            (
                TakeoverFinish {
                    run_id: None,
                    run_status: TakeoverRunStatus::Error,
                    detail: format!("continuity {} dispatch failed: {error:#}", approval.step_id),
                },
                ExitCode::from(4),
            )
        }
    };
    let settled = store
        .finish_takeover_approval(&args.common.owner, &args.approval_id, &finish, Utc::now())
        .context("settle takeover approval")?;
    // Kind-only pure settled approval status after finish (Done/Blocked) (WP-458).
    // Matches print_takeover_result pure residual used by queue/decide (WP-397).
    tracing::info!(
        status = settled.status.as_str(),
        "{}",
        crate::output::format_takeover_approval_status_line(settled.status)
    );
    if args.common.json {
        print_json(&TakeoverApprovalOutput {
            status: if exit == ExitCode::SUCCESS {
                "success"
            } else {
                "blocked"
            },
            approval: settled,
            db: path_text(&db),
        })?;
    } else {
        println!(
            "{}",
            format_takeover_approval_line(
                &settled.approval_id,
                settled.status,
                &settled.goal_id,
                &settled.step_id,
            )
        );
    }
    Ok(exit)
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
    let sandbox: Sandbox = args.sandbox.into();
    // Kind-only pure sandbox frozen for pursuit segments (WP-411).
    tracing::info!(
        sandbox = sandbox.as_str(),
        "{}",
        crate::output::format_sandbox_line(sandbox)
    );
    let runtime = DispatchGoalRuntime::new(service, args.target.clone(), sandbox);
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
    // Kind-only pure pursuit status on CLI pursue settle (WP-399).
    tracing::info!(
        status = outcome.status.as_str(),
        "{}",
        crate::output::format_pursuit_status_line(outcome.status)
    );
    // Kind-only pure final goal status from the durable outcome (WP-412).
    tracing::info!(
        status = outcome.final_goal_status.as_str(),
        "{}",
        crate::output::format_goal_status_line(outcome.final_goal_status)
    );
    if args.common.json {
        print_json(&PursueOutput {
            status: response_status,
            pursuit: outcome,
            goal,
            db: path_text(&db),
        })?;
    } else {
        print!("{}", format_pursue_outcome(&outcome));
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
    let pursuit_checkpoint = store
        .pursuit_checkpoint(&args.common.owner, &args.id)
        .context("read goal pursuit checkpoint")?;
    // Kind-only pure checkpoint status when a durable checkpoint exists (WP-398).
    if let Some(checkpoint) = pursuit_checkpoint.as_ref() {
        tracing::info!(
            status = checkpoint.status.as_str(),
            "{}",
            crate::output::format_pursuit_checkpoint_status_line(checkpoint.status)
        );
    }
    // Kind-only pure event kinds for each durable event (WP-407).
    for event in &events {
        tracing::info!(
            kind = event.kind.as_str(),
            "{}",
            crate::output::format_goal_event_kind_line(event.kind)
        );
    }
    if args.common.json {
        print_json(&GoalDetailOutput {
            status: "success",
            goal,
            events,
            verifications,
            pursuit_checkpoint: pursuit_checkpoint.map(PursuitCheckpointView::from),
            db: path_text(&db),
        })?;
    } else {
        print_goal_line(&goal)?;
        for event in events {
            println!("{}", format_goal_event_line(&event));
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
    // Kind-only pure line for explicit satisfy (WP-394).
    tracing::info!(
        status = CriterionStatus::Satisfied.as_str(),
        "{}",
        crate::output::format_criterion_status_line(CriterionStatus::Satisfied)
    );
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
        // Kind-only pure line per criterion result (WP-394).
        tracing::info!(
            status = result.status.as_str(),
            "{}",
            crate::output::format_criterion_status_line(result.status)
        );
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
        print!("{}", format_verify_result(&verification));
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
    // Kind-only pure progress event kind (WP-407).
    tracing::info!(
        kind = event.kind.as_str(),
        "{}",
        crate::output::format_goal_event_kind_line(event.kind)
    );
    if args.common.json {
        print_json(&ProgressOutput {
            status: "success",
            goal,
            event,
            db: path_text(&db),
        })?;
    } else {
        println!("{}", format_progress_event_line(&event));
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

/// Human one-line goal projection (id, status, priority, title).
/// Status column is the domain kind token via `as_str` (WP-400).
fn format_goal_line(goal: &GoalRecord) -> String {
    format!(
        "{}\t{}\t{}\t{}",
        terminal_safe(&goal.id),
        goal.status.as_str(),
        goal.priority,
        terminal_safe(&goal.title)
    )
}

/// Human one-line goal progress event: event id, kind token, goal id,
/// revision.
fn format_progress_event_line(event: &GoalEvent) -> String {
    format!(
        "{}\t{}\t{}\t{}",
        terminal_safe(&event.event_id),
        event.kind.as_str(),
        terminal_safe(&event.goal_id),
        event.revision
    )
}

/// Human one-line goal detail event row (revision, timestamp, kind token).
/// Kind tokens come from domain `GoalEventKind::as_str` — no private CLI match
/// table (WP-400).
fn format_goal_event_line(event: &GoalEvent) -> String {
    format!(
        "{}\t{}\t{}",
        event.revision,
        event.occurred_at.to_rfc3339(),
        event.kind.as_str()
    )
}

/// Human multi-line goal verify result: success|inconclusive, goal id, and
/// terminal-safe acceptance summary.
fn format_verify_result(verification: &AcceptanceVerification) -> String {
    format!(
        "result:    {}\ngoal:      {}\nsummary:   {}\n",
        if verification.all_satisfied {
            "success"
        } else {
            "inconclusive"
        },
        terminal_safe(&verification.goal_id),
        terminal_safe(&verification.summary),
    )
}

/// Human multi-line goal pursue outcome: status, goal id, segment counters,
/// and terminal-safe summary (reason only when non-empty and distinct).
/// Status field is the domain kind token via `as_str` (WP-400).
fn format_pursue_outcome(outcome: &PursuitOutcome) -> String {
    let mut out = format!(
        "status:    {}\ngoal:      {}\nsegments:  started={} completed={} failures={}\nsummary:   {}\n",
        outcome.status.as_str(),
        terminal_safe(&outcome.goal_id),
        outcome.segments_started,
        outcome.segments_completed,
        outcome.consecutive_failures,
        terminal_safe(&outcome.summary),
    );
    if !outcome.reason.is_empty() && outcome.reason != outcome.summary {
        out.push_str(&format!("reason:    {}\n", terminal_safe(&outcome.reason)));
    }
    out
}

/// Human one-line continuity-signal result: goal id, recorded|unchanged,
/// and the durable next_ready_step token from the handoff plan.
fn format_continuity_signal_line(goal_id: &str, changed: bool, next_ready_step: &str) -> String {
    format!(
        "{}\t{}\t{}",
        terminal_safe(goal_id),
        if changed { "recorded" } else { "unchanged" },
        terminal_safe(next_ready_step)
    )
}

/// Human multi-line projection of the WP-79 continuity next-action contract.
///
/// Always prints goal / revision / quota / action / reason with snake_case
/// action tokens via `as_str` (WP-401). Optional command, step, and approval
/// lines are included only when the projection carries those durable fields.
fn format_continuity_next_action(next: &GoalContinuityNextAction) -> String {
    let mut out = format!(
        "goal:      {}\nrevision:  {}\nquota:     {}\naction:    {}\n",
        terminal_safe(&next.goal_id),
        next.goal_revision,
        terminal_safe(&next.quota_event_id),
        next.action.as_str()
    );
    if let Some(command) = next.command {
        out.push_str(&format!("command:   {}\n", command.as_str()));
    }
    if let Some(step_id) = next.step_id.as_deref() {
        out.push_str(&format!("step:      {}\n", terminal_safe(step_id)));
    }
    if let Some(approval_id) = next.approval_id.as_deref() {
        out.push_str(&format!("approval:  {}\n", terminal_safe(approval_id)));
    }
    out.push_str(&format!("reason:    {}\n", terminal_safe(&next.reason)));
    out
}

fn print_goal_line(goal: &GoalRecord) -> Result<()> {
    // Kind-only pure status line alongside human projection (WP-396).
    tracing::info!(
        status = goal.status.as_str(),
        "{}",
        crate::output::format_goal_status_line(goal.status)
    );
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "{}", format_goal_line(goal)).context("write goal response")?;
    stdout.flush().context("flush goal response")
}

/// Human one-line takeover/continuity approval projection used by queue/decide
/// (and any other path that settles an approval without a multi-line body).
///
/// Fields: approval_id, status (snake_case Display), goal_id, step_id — free
/// text is terminal-safe so operators can parse tab columns safely.
fn format_takeover_approval_line(
    approval_id: &str,
    status: TakeoverApprovalStatus,
    goal_id: &str,
    step_id: &str,
) -> String {
    format!(
        "{}\t{}\t{}\t{}",
        terminal_safe(approval_id),
        terminal_safe(status.as_str()),
        terminal_safe(goal_id),
        terminal_safe(step_id)
    )
}

fn print_takeover_result(
    common: &GoalCommonArgs,
    db: &Path,
    status: &'static str,
    approval: TakeoverApproval,
) -> Result<ExitCode> {
    // Kind-only pure approval status on human/json settle paths (WP-397).
    tracing::info!(
        status = approval.status.as_str(),
        "{}",
        crate::output::format_takeover_approval_status_line(approval.status)
    );
    if common.json {
        print_json(&TakeoverApprovalOutput {
            status,
            approval,
            db: path_text(db),
        })?;
    } else {
        println!(
            "{}",
            format_takeover_approval_line(
                &approval.approval_id,
                approval.status,
                &approval.goal_id,
                &approval.step_id,
            )
        );
    }
    Ok(ExitCode::SUCCESS)
}

const fn takeover_sandbox(value: SandboxArg) -> TakeoverSandbox {
    match value {
        SandboxArg::ReadOnly => TakeoverSandbox::ReadOnly,
        SandboxArg::Write => TakeoverSandbox::Write,
        SandboxArg::Full => TakeoverSandbox::Full,
    }
}

const fn core_sandbox(value: TakeoverSandbox) -> Sandbox {
    match value {
        TakeoverSandbox::ReadOnly => Sandbox::ReadOnly,
        TakeoverSandbox::Write => Sandbox::Write,
        TakeoverSandbox::Full => Sandbox::Full,
    }
}

const fn takeover_run_status(value: RunStatus) -> TakeoverRunStatus {
    match value {
        RunStatus::Success => TakeoverRunStatus::Success,
        RunStatus::Error => TakeoverRunStatus::Error,
        RunStatus::Timeout => TakeoverRunStatus::Timeout,
        RunStatus::Cancelled => TakeoverRunStatus::Cancelled,
    }
}

fn validate_resolved_continuity(
    resolved: &vyane_core::BoundTarget,
    approved: &TakeoverBoundTarget,
) -> Result<()> {
    let harness = resolved
        .target
        .harness
        .as_ref()
        .map_or("none", |value| value.as_str());
    if resolved.target.provider.as_str() != approved.provider
        || resolved.target.protocol.to_string() != approved.protocol
        || harness != approved.harness
        || resolved.target.model.as_str() != approved.model
    {
        bail!(
            "resolved continuity target does not match the approved provider/protocol/harness/model boundary"
        );
    }
    Ok(())
}

fn continuity_prompt(
    goal: &GoalRecord,
    approval: &TakeoverApproval,
    approvals: &[TakeoverApproval],
) -> String {
    let reason = match approval.step_id.as_str() {
        "takeover" => "primary quota blocked",
        "review_takeover" => "review the completed takeover before primary handback",
        "repair_failed_review" => "repair the failed review checks before primary handback",
        "resume_primary" if approval.plan_snapshot.require_review_before_resume => {
            "review and quota-reset dependencies are satisfied"
        }
        "resume_primary" => "quota-reset dependency is satisfied",
        _ => "approved continuity step is ready",
    };
    let mut prompt = format!(
        "Continue goal {}: {}\n\nContinuity step: {} ({})\nReason: {}.\n\
         Approved target provider: {}\nApproved target protocol: {}\n\
         Approved target harness: {}\nApproved target model: {}\n",
        goal.id,
        goal.title,
        approval.step_id,
        approval.step_kind,
        reason,
        approval.target.provider,
        approval.target.protocol,
        approval.target.harness,
        approval.target.model,
    );
    if !goal.description.trim().is_empty() {
        prompt.push_str("\nGoal description:\n");
        prompt.push_str(goal.description.trim());
        prompt.push('\n');
    }
    if approval.step_id == "review_takeover" {
        prompt.push_str("\nTakeover evidence to review:\n");
        if let Some(id) = &approval.upstream_approval_id {
            prompt.push_str(&format!("- approval_id: {id}\n"));
        }
        if let Some(id) = &approval.upstream_run_id {
            prompt.push_str(&format!("- run_id: {id}\n"));
        }
        if let Some(status) = approval.upstream_run_status {
            prompt.push_str(&format!("- run_status: {}\n", status.as_str()));
        }
        prompt.push_str(
            "Review the completed takeover before primary handback. Report required fixes as blockers.\n",
        );
    }
    if approval.step_id == "resume_primary" {
        prompt.push_str("\nPrimary resume evidence:\n");
        append_approval_chain(&mut prompt, approval, approvals);
        append_signal_evidence(&mut prompt, approval);
        if approval.plan_snapshot.require_review_before_resume {
            prompt.push_str(
                "Resume the approved primary target only after verifying the reviewed takeover handback and quota-reset evidence.\n",
            );
        } else {
            prompt.push_str(
                "Resume the approved primary target only after verifying the takeover handback and quota-reset evidence.\n",
            );
        }
    }
    if approval.step_id == "repair_failed_review" {
        prompt.push_str("\nReview repair evidence:\n");
        append_approval_chain(&mut prompt, approval, approvals);
        append_signal_evidence(&mut prompt, approval);
        prompt.push_str(
            "Repair the failed review checks and report the new verification evidence before returning control.\n",
        );
    }
    prompt.push_str(
        "\nWork only on this approved continuity step. Report changes, verification, and blockers before returning control.",
    );
    prompt
}

fn append_approval_chain(
    prompt: &mut String,
    approval: &TakeoverApproval,
    approvals: &[TakeoverApproval],
) {
    let by_id = |id: &str| {
        approvals
            .iter()
            .find(|candidate| candidate.approval_id == id)
    };
    let direct = approval.upstream_approval_id.as_deref().and_then(by_id);
    let repair = direct.filter(|candidate| candidate.step_id == "repair_failed_review");
    if let Some(repair) = repair {
        append_approval_evidence(prompt, "repair", repair);
    }
    let review = if let Some(repair) = repair {
        repair.upstream_approval_id.as_deref().and_then(by_id)
    } else {
        direct.filter(|candidate| candidate.step_id == "review_takeover")
    };
    if let Some(review) = review {
        append_approval_evidence(prompt, "review", review);
    }
    let takeover = if let Some(review) = review {
        review.upstream_approval_id.as_deref().and_then(by_id)
    } else if direct.is_some_and(|candidate| candidate.step_id == "takeover") {
        direct
    } else {
        approvals.iter().find(|candidate| {
            candidate.quota_event_id == approval.quota_event_id
                && candidate.step_id == "takeover"
                && candidate.step_kind == "start_takeover"
                && candidate.status == TakeoverApprovalStatus::Done
                && candidate.run_status == Some(TakeoverRunStatus::Success)
        })
    };
    if let Some(takeover) = takeover {
        append_approval_evidence(prompt, "takeover", takeover);
    }
}

fn append_approval_evidence(prompt: &mut String, label: &str, approval: &TakeoverApproval) {
    prompt.push_str(&format!(
        "- {label}.approval_id: {}\n",
        approval.approval_id
    ));
    if let Some(run_id) = &approval.run_id {
        prompt.push_str(&format!("- {label}.run_id: {run_id}\n"));
    }
    if let Some(status) = approval.run_status {
        // Kind-only pure run status frozen into execute prompt evidence (WP-456).
        tracing::info!(
            status = status.as_str(),
            "{}",
            crate::output::format_takeover_run_status_line(status)
        );
        prompt.push_str(&format!("- {label}.run_status: {}\n", status.as_str()));
    }
}

fn append_signal_evidence(prompt: &mut String, approval: &TakeoverApproval) {
    for signal in &approval.plan_snapshot.ready_signals {
        // Kind-only pure ready signal kinds frozen into execute prompt evidence (WP-454).
        tracing::info!(
            kind = signal.kind.as_str(),
            "{}",
            crate::output::format_goal_continuity_signal_kind_line(signal.kind)
        );
        prompt.push_str(&format!(
            "- signal.{}: observed at {} by {}\n",
            signal.kind.as_str(),
            signal.observed_at.to_rfc3339(),
            signal.source
        ));
        if let Some(review_check) = &signal.review_check {
            prompt.push_str(&format!(
                "  review: {}#{}\n",
                review_check.repository, review_check.pull_request
            ));
        }
    }
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
        GoalCommand::ContinuityNext(args) => &args.common,
        GoalCommand::ContinuityQueue(args) => &args.common,
        GoalCommand::ContinuityDecide(args) => &args.common,
        GoalCommand::ContinuityExecute(args) => &args.common,
        GoalCommand::ContinuitySignal(args) => &args.common,
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
    use chrono::{TimeZone, Utc};
    use vyane_core::RunStatus;
    use vyane_goal::{
        AcceptanceVerification, GoalContinuityNextAction, GoalContinuityNextActionKind,
        GoalContinuityOperatorCommand, GoalEvent, GoalEventKind, GoalRecord, GoalStatus,
        PursuitOutcome, PursuitSegmentStatus, PursuitStatus, TakeoverApprovalStatus,
    };

    use super::{
        format_continuity_next_action, format_continuity_signal_line, format_goal_event_line,
        format_goal_line, format_progress_event_line, format_pursue_outcome,
        format_takeover_approval_line, format_verify_result,
    };
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

    fn sample_goal(id: &str, title: &str, status: GoalStatus, priority: u8) -> GoalRecord {
        let at = Utc.with_ymd_and_hms(2026, 8, 2, 14, 0, 0).unwrap();
        GoalRecord {
            id: id.into(),
            owner: "local".into(),
            title: title.into(),
            description: String::new(),
            status,
            priority,
            parent_goal_id: None,
            acceptance_criteria: vec![],
            continuity_policy: None,
            continuity_state: None,
            created_at: at,
            started_at: None,
            updated_at: at,
            finished_at: None,
            revision: 1,
            completion_summary: None,
            failure_reason: None,
            pause_reason: None,
            cancel_reason: None,
            claimed_by: None,
            claim_expires_at: None,
            claim_generation: 0,
        }
    }

    #[test]
    fn human_goal_line_prints_status_priority_and_terminal_safe_fields() {
        let goal = sample_goal(
            "g1\n\u{1b}[31m",
            "title\twith\ttabs",
            GoalStatus::InProgress,
            2,
        );
        let text = format_goal_line(&goal);
        assert!(text.contains("in_progress"), "{text}");
        assert!(text.contains('\t'), "tab-separated fields:\n{text}");
        assert!(text.contains("2"), "{text}");
        // control sequences escaped
        assert!(!text.contains('\u{1b}'), "no raw ESC:\n{text}");
        assert!(text.contains("\\n") || text.contains("\\u{1b}"), "{text}");
        let parts: Vec<&str> = text.split('\t').collect();
        assert_eq!(parts.len(), 4, "id status priority title:\n{text}");
        assert_eq!(parts[2], "2", "{text}");
    }

    #[test]
    fn human_goal_line_uses_status_as_str() {
        let goal = sample_goal("goal-a", "plain", GoalStatus::Paused, 0);
        let text = format_goal_line(&goal);
        assert!(text.starts_with("goal-a\tpaused\t0\t"), "{text}");
        assert!(text.ends_with("plain"), "{text}");
    }

    fn sample_next_action(
        action: GoalContinuityNextActionKind,
        command: Option<GoalContinuityOperatorCommand>,
        step_id: Option<&str>,
        approval_id: Option<&str>,
    ) -> GoalContinuityNextAction {
        GoalContinuityNextAction {
            goal_id: "goal-1\n\u{1b}[31m".into(),
            goal_revision: 7,
            quota_event_id: "quota-evt\t1".into(),
            action,
            command,
            step_id: step_id.map(str::to_string),
            step_kind: None,
            approval_id: approval_id.map(str::to_string),
            accepted_signals: vec![],
            required_inputs: vec![],
            reason: "needs decision\tnow".into(),
        }
    }

    #[test]
    fn human_continuity_next_uses_snake_case_action_not_debug() {
        let next = sample_next_action(
            GoalContinuityNextActionKind::DecideApproval,
            Some(GoalContinuityOperatorCommand::ContinuityDecide),
            Some("takeover"),
            Some("appr-9"),
        );
        let text = format_continuity_next_action(&next);
        assert!(text.contains("action:    decide_approval"), "{text}");
        assert!(
            !text.contains("DecideApproval"),
            "must not use Debug:\n{text}"
        );
        assert!(text.contains("command:   continuity_decide"), "{text}");
        assert!(text.contains("step:      takeover"), "{text}");
        assert!(text.contains("approval:  appr-9"), "{text}");
        assert!(text.contains("revision:  7"), "{text}");
        assert!(!text.contains('\u{1b}'), "no raw ESC:\n{text}");
        assert!(text.contains("\\n") || text.contains("\\u{1b}"), "{text}");
        assert!(text.contains("needs decision"), "{text}");
    }

    #[test]
    fn human_continuity_next_omits_optional_fields_when_absent() {
        let next = sample_next_action(
            GoalContinuityNextActionKind::WaitForDependency,
            None,
            None,
            None,
        );
        let text = format_continuity_next_action(&next);
        assert!(text.contains("action:    wait_for_dependency"), "{text}");
        assert!(!text.contains("command:"), "{text}");
        assert!(!text.contains("step:"), "{text}");
        assert!(!text.contains("approval:"), "{text}");
        assert!(text.contains("quota:"), "{text}");
        assert!(text.contains("reason:"), "{text}");
    }

    #[test]
    fn human_takeover_approval_line_prints_status_goal_and_step() {
        let text = format_takeover_approval_line(
            "appr-1\n\u{1b}[31m",
            TakeoverApprovalStatus::Pending,
            "goal-9",
            "takeover",
        );
        assert!(text.contains("pending"), "{text}");
        assert!(text.contains("goal-9"), "{text}");
        assert!(text.contains("takeover"), "{text}");
        assert!(!text.contains('\u{1b}'), "no raw ESC:\n{text}");
        let parts: Vec<&str> = text.split('\t').collect();
        assert_eq!(
            parts.len(),
            4,
            "approval_id status goal_id step_id:\n{text}"
        );
        assert_eq!(parts[1], "pending", "{text}");
        assert_eq!(parts[2], "goal-9", "{text}");
        assert_eq!(parts[3], "takeover", "{text}");
    }

    #[test]
    fn human_takeover_approval_line_uses_status_display() {
        let text = format_takeover_approval_line(
            "appr-ok",
            TakeoverApprovalStatus::InFlight,
            "g",
            "resume_primary",
        );
        assert_eq!(text, "appr-ok\tin_flight\tg\tresume_primary", "{text}");
    }

    #[test]
    fn human_continuity_signal_line_marks_recorded_and_next_step() {
        let text = format_continuity_signal_line("goal-1\n\u{1b}[31m", true, "resume_primary\tnow");
        assert!(text.contains("recorded"), "{text}");
        assert!(!text.contains("unchanged"), "{text}");
        assert!(!text.contains('\u{1b}'), "no raw ESC:\n{text}");
        let parts: Vec<&str> = text.split('\t').collect();
        assert_eq!(parts.len(), 3, "goal changed next_step:\n{text}");
        assert_eq!(parts[1], "recorded", "{text}");
        assert!(parts[2].contains("resume_primary"), "{text}");
    }

    #[test]
    fn human_continuity_signal_line_marks_unchanged() {
        let text = format_continuity_signal_line("goal-a", false, "takeover");
        assert_eq!(text, "goal-a\tunchanged\ttakeover", "{text}");
    }

    #[test]
    fn human_pursue_outcome_prints_status_segments_and_safe_summary() {
        let outcome = PursuitOutcome {
            goal_id: "goal-9\n\u{1b}[31m".into(),
            status: PursuitStatus::Paused,
            final_goal_status: GoalStatus::Paused,
            segments_started: 2,
            segments_completed: 1,
            consecutive_failures: 1,
            summary: "paused after segment\tfail".into(),
            reason: "quota blocked".into(),
            last_verification: None,
        };
        let text = format_pursue_outcome(&outcome);
        assert!(text.contains("status:    paused"), "{text}");
        assert!(
            text.contains("segments:  started=2 completed=1 failures=1"),
            "{text}"
        );
        assert!(!text.contains('\u{1b}'), "no raw ESC:\n{text}");
        assert!(text.contains("summary:"), "{text}");
        assert!(text.contains("reason:    quota blocked"), "{text}");
    }

    #[test]
    fn human_pursue_outcome_omits_reason_when_same_as_summary() {
        let outcome = PursuitOutcome {
            goal_id: "g".into(),
            status: PursuitStatus::Achieved,
            final_goal_status: GoalStatus::Completed,
            segments_started: 1,
            segments_completed: 1,
            consecutive_failures: 0,
            summary: "all criteria satisfied".into(),
            reason: "all criteria satisfied".into(),
            last_verification: None,
        };
        let text = format_pursue_outcome(&outcome);
        assert!(text.contains("status:    achieved"), "{text}");
        assert!(!text.contains("reason:"), "{text}");
        assert!(text.contains("summary:   all criteria satisfied"), "{text}");
    }

    #[test]
    fn human_verify_result_marks_success_when_all_satisfied() {
        let verification = AcceptanceVerification {
            goal_id: "goal-ok".into(),
            all_satisfied: true,
            results: vec![],
            summary: "1/1 satisfied".into(),
        };
        let text = format_verify_result(&verification);
        assert!(text.contains("result:    success"), "{text}");
        assert!(text.contains("goal:      goal-ok"), "{text}");
        assert!(text.contains("summary:   1/1 satisfied"), "{text}");
    }

    #[test]
    fn human_verify_result_marks_inconclusive_and_escapes_summary() {
        let verification = AcceptanceVerification {
            goal_id: "goal-x\n".into(),
            all_satisfied: false,
            results: vec![],
            summary: "0/2\u{1b}[31m failed".into(),
        };
        let text = format_verify_result(&verification);
        assert!(text.contains("result:    inconclusive"), "{text}");
        assert!(!text.contains('\u{1b}'), "no raw ESC:\n{text}");
        assert!(text.contains("summary:"), "{text}");
    }

    #[test]
    fn human_progress_event_line_prints_kind_goal_and_revision() {
        let at = Utc.with_ymd_and_hms(2026, 8, 2, 16, 0, 0).unwrap();
        let event = GoalEvent {
            sequence: 3,
            event_id: "evt-1\n\u{1b}[31m".into(),
            owner: "local".into(),
            goal_id: "goal-p".into(),
            revision: 4,
            occurred_at: at,
            kind: GoalEventKind::Progress,
            from_status: Some(GoalStatus::InProgress),
            to_status: GoalStatus::InProgress,
            stage: Some("build".into()),
            detail: Some("nudge".into()),
        };
        let text = format_progress_event_line(&event);
        assert!(text.contains("progress"), "{text}");
        assert!(text.contains("goal-p"), "{text}");
        assert!(text.contains('\t'), "{text}");
        assert!(!text.contains('\u{1b}'), "no raw ESC:\n{text}");
        let parts: Vec<&str> = text.split('\t').collect();
        assert_eq!(parts.len(), 4, "event_id kind goal_id revision:\n{text}");
        assert_eq!(parts[1], "progress", "{text}");
        assert_eq!(parts[2], "goal-p", "{text}");
        assert_eq!(parts[3], "4", "{text}");
    }

    #[test]
    fn human_progress_event_line_uses_kind_display() {
        let at = Utc.with_ymd_and_hms(2026, 8, 2, 16, 0, 0).unwrap();
        let event = GoalEvent {
            sequence: 1,
            event_id: "e".into(),
            owner: "local".into(),
            goal_id: "g".into(),
            revision: 1,
            occurred_at: at,
            kind: GoalEventKind::CriterionSatisfied,
            from_status: None,
            to_status: GoalStatus::InProgress,
            stage: None,
            detail: None,
        };
        let text = format_progress_event_line(&event);
        assert_eq!(text, "e\tcriterion_satisfied\tg\t1", "{text}");
    }

    #[test]
    fn human_goal_event_line_uses_domain_kind_display() {
        let at = Utc.with_ymd_and_hms(2026, 8, 2, 16, 30, 0).unwrap();
        let event = GoalEvent {
            sequence: 2,
            event_id: "evt".into(),
            owner: "local".into(),
            goal_id: "g1".into(),
            revision: 9,
            occurred_at: at,
            kind: GoalEventKind::LeaseRenewed,
            from_status: Some(GoalStatus::InProgress),
            to_status: GoalStatus::InProgress,
            stage: None,
            detail: None,
        };
        let text = format_goal_event_line(&event);
        assert!(text.starts_with("9\t"), "{text}");
        assert!(text.contains(at.to_rfc3339().as_str()), "{text}");
        assert!(text.ends_with("\tlease_renewed"), "{text}");
        // Domain as_str is the single source of kind tokens (no CLI match table).
        assert_eq!(event.kind.as_str(), "lease_renewed");
    }
}
