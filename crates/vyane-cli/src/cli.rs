use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use vyane_core::{RunStatus, Sandbox};
use vyane_workflow::WorkflowRunId;

/// Dispatch, fan out, and inspect Vyane model runs.
#[derive(Debug, Parser)]
#[command(
    name = "vyane",
    version,
    about = "Run configured AI targets with failover and a local run ledger.",
    long_about = "Vyane assembles configured providers, protocols, CLI harnesses, the dispatch kernel, and local storage into one command-line tool."
)]
pub struct Cli {
    /// Config file to load instead of the default user + project files.
    #[arg(long, global = true, value_name = "PATH")]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Validate configuration and show local readiness.
    Check,
    /// Run one task against a profile, provider/model, or auto-routed target.
    Dispatch(DispatchArgs),
    /// Run one task against several targets concurrently.
    Broadcast(BroadcastArgs),
    /// Show recent run ledger records.
    History(HistoryArgs),
    /// List saved session records.
    Sessions(SessionsArgs),
    /// Inspect and safely reset local continuity sessions.
    #[command(subcommand)]
    Session(SessionCommand),
    /// Run, resume, and list declarative workflows.
    #[command(subcommand)]
    Workflow(WorkflowCommand),
    /// Run a built-in multi-model review pipeline (implement → review → synthesize).
    Review(ReviewArgs),
    /// Show what the router would pick for a task (dry-run, no dispatch).
    Route(RouteArgs),
    /// Inspect and manage detached background runs.
    #[command(subcommand)]
    Task(TaskCommand),
    /// Start the bearer-authenticated loopback HTTP API server.
    Serve(ServeArgs),
    /// Run and control the authenticated local workflow daemon.
    #[command(subcommand)]
    Daemon(DaemonCommand),
    /// Use a local SQLite inbox; this is not authenticated A2A protocol transport.
    #[command(subcommand)]
    A2a(A2aCommand),
    /// Create, inspect, and advance owner-scoped local goals.
    #[command(subcommand)]
    Goal(GoalCommand),
    /// Run the MCP server over stdio (for use as an MCP tool server).
    Mcp,
    /// Internal: execute a detached run. Not for direct use.
    #[command(name = "__worker", hide = true)]
    Worker(WorkerArgs),
}

#[derive(Debug, Subcommand)]
pub enum A2aCommand {
    /// Queue one local message for an agent mailbox.
    Send(A2aSendArgs),
    /// List one local agent mailbox without changing delivery state.
    Inbox(A2aInboxArgs),
    /// Deliver one exact mailbox message, then acknowledge after stdout flushes.
    Read(A2aReadArgs),
}

#[derive(Debug, Subcommand)]
pub enum GoalCommand {
    /// Create a queued goal.
    Create(GoalCreateArgs),
    /// Show one goal and its append-only events.
    Get(GoalGetArgs),
    /// List goals by priority and recent activity.
    List(GoalListArgs),
    /// Show the highest-priority queued goal.
    Next(GoalNextArgs),
    /// Move a queued or paused goal to in_progress.
    Start(GoalIdArgs),
    /// Atomically claim a queued goal for a worker under a lease.
    Claim(GoalClaimArgs),
    /// Atomically select and claim the highest-priority queued goal.
    ClaimNext(GoalClaimNextArgs),
    /// Heartbeat: extend the lease currently held by a worker.
    Renew(GoalClaimArgs),
    /// Take over a goal whose lease has expired.
    Reclaim(GoalClaimArgs),
    /// Record that an acceptance criterion was actually verified.
    Satisfy(GoalSatisfyArgs),
    /// Run bounded local acceptance checks and persist successful criteria.
    Verify(GoalVerifyArgs),
    /// Repeatedly verify and dispatch fresh bounded work segments until achieved or paused.
    Pursue(GoalPursueArgs),
    /// Queue one approval bound to the current ready takeover or review step; never dispatches.
    ContinuityQueue(GoalContinuityQueueArgs),
    /// Explicitly approve or reject one pending continuity approval.
    ContinuityDecide(GoalContinuityDecisionArgs),
    /// Consume one approved continuity approval and execute exactly once.
    ContinuityExecute(GoalContinuityExecuteArgs),
    /// Record one exact external continuity readiness signal; never dispatches.
    ContinuitySignal(GoalContinuitySignalArgs),
    /// Append a progress event without changing lifecycle state.
    Progress(GoalProgressArgs),
    /// Pause an in-progress goal; releases any lease (holder-only while leased).
    Pause(GoalReasonArgs),
    /// Resume a paused goal; resumed goals are always unleased.
    Resume(GoalResumeArgs),
    /// Mark an in-progress goal completed.
    Done(GoalDoneArgs),
    /// Mark an in-progress goal failed.
    Fail(GoalFailArgs),
    /// Cancel a queued, in-progress, or paused goal.
    Cancel(GoalReasonArgs),
}

#[derive(Debug, Args)]
pub struct GoalCommonArgs {
    /// SQLite goal database; defaults to the standard Vyane data directory.
    #[arg(long, value_name = "PATH")]
    pub db: Option<PathBuf>,
    /// Caller-selected local storage scope; not authenticated authority.
    #[arg(long, alias = "owner-user-id", default_value = "local")]
    pub owner: String,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct GoalCreateArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Goal title.
    #[arg(long)]
    pub title: String,
    /// Long-form goal description.
    #[arg(long, default_value = "")]
    pub description: String,
    /// Priority from 0 (urgent) through 4 (backlog).
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u8).range(0..=4))]
    pub priority: u8,
    /// Optional umbrella goal id.
    #[arg(long)]
    pub parent: Option<String>,
    /// Acceptance descriptor in KIND:TARGET form; repeatable.
    #[arg(long, value_name = "KIND:TARGET")]
    pub acceptance: Vec<String>,
    /// Typed quota-continuity policy as one JSON object; records intent only.
    #[arg(long, value_name = "JSON")]
    pub continuity_policy_json: Option<String>,
    /// Explicit goal id; generated when omitted.
    #[arg(long)]
    pub id: Option<String>,
}

#[derive(Debug, Args)]
pub struct GoalContinuityQueueArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Goal whose current supported ready continuity step should be queued.
    pub id: String,
    /// Existing worktree or workspace bound into the approval.
    #[arg(long, value_name = "PATH")]
    pub workdir: PathBuf,
    /// Workspace permission bound into the approval.
    #[arg(long, value_enum, default_value_t = SandboxArg::Write)]
    pub sandbox: SandboxArg,
    /// Hard one-shot execution timeout, in seconds.
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum GoalTakeoverDecisionArg {
    Approve,
    Reject,
}

#[derive(Debug, Args)]
pub struct GoalContinuityDecisionArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Exact durable approval id.
    pub approval_id: String,
    /// Explicit operator decision.
    #[arg(long, value_enum)]
    pub decision: GoalTakeoverDecisionArg,
    /// Auditable local operator identity.
    #[arg(long)]
    pub decided_by: String,
    /// Optional decision rationale.
    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Debug, Args)]
pub struct GoalContinuityExecuteArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Exact approved, unconsumed approval id. No execution option may be overridden here.
    pub approval_id: String,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum GoalContinuitySignalArg {
    #[value(name = "quota-reset", alias = "quota_reset")]
    QuotaReset,
    #[value(name = "review-checks-passed", alias = "review_checks_passed")]
    ReviewChecksPassed,
    #[value(name = "review-checks-failed", alias = "review_checks_failed")]
    ReviewChecksFailed,
}

#[derive(Debug, Args)]
pub struct GoalContinuitySignalArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Goal whose current continuity plan receives the signal.
    pub id: String,
    /// Closed signal kind.
    #[arg(value_enum)]
    pub signal: GoalContinuitySignalArg,
    /// Exact quota event currently visible on the goal.
    #[arg(long)]
    pub quota_event_id: String,
    /// Exact primary provider observed available again.
    #[arg(long)]
    pub provider: String,
    /// Exact primary harness observed available again.
    #[arg(long)]
    pub harness: String,
    /// Exact primary model observed available again.
    #[arg(long)]
    pub model: String,
    /// Bounded non-secret observer identifier.
    #[arg(long)]
    pub source: String,
    /// Public repository whose review checks were observed.
    #[arg(long, requires_all = ["pull_request", "observation_id", "observation_sequence"])]
    pub repository: Option<String>,
    /// Pull request whose review checks were observed.
    #[arg(long, requires_all = ["repository", "observation_id", "observation_sequence"], value_parser = clap::value_parser!(u64).range(1..))]
    pub pull_request: Option<u64>,
    /// Stable identifier for this distinct review-check observation.
    #[arg(long, requires_all = ["repository", "pull_request", "observation_sequence"])]
    pub observation_id: Option<String>,
    /// Strictly increasing sequence for review-check observations on this PR.
    #[arg(long, requires_all = ["repository", "pull_request", "observation_id"], value_parser = clap::value_parser!(u64).range(1..))]
    pub observation_sequence: Option<u64>,
}

#[derive(Debug, Args)]
pub struct GoalGetArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Exact goal id.
    pub id: String,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum GoalStatusArg {
    Queued,
    #[value(name = "in_progress")]
    InProgress,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Args)]
pub struct GoalListArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Filter by lifecycle state; repeatable.
    #[arg(long = "state")]
    pub states: Vec<GoalStatusArg>,
    /// Filter by parent goal id.
    #[arg(long)]
    pub parent: Option<String>,
    /// Maximum rows; 0 means all, up to 1000.
    #[arg(long, default_value_t = 50)]
    pub limit: usize,
}

#[derive(Debug, Args)]
pub struct GoalNextArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Transition the selected goal to in_progress before returning it.
    #[arg(long)]
    pub auto_start: bool,
}

#[derive(Debug, Args)]
pub struct GoalIdArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Exact goal id.
    pub id: String,
}

#[derive(Debug, Args)]
pub struct GoalProgressArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Exact goal id.
    pub id: String,
    /// Stable progress stage label.
    #[arg(long)]
    pub stage: String,
    /// Human-readable progress detail.
    #[arg(long)]
    pub detail: String,
}

#[derive(Debug, Args)]
pub struct GoalReasonArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Exact goal id.
    pub id: String,
    /// Optional pause or cancellation reason.
    #[arg(long)]
    pub reason: Option<String>,
    /// Caller-supplied worker identity; required while a lease is active.
    #[arg(long)]
    pub worker: Option<String>,
}

#[derive(Debug, Args)]
pub struct GoalResumeArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Exact goal id.
    pub id: String,
    /// Caller-supplied worker identity; required while a lease is active.
    #[arg(long)]
    pub worker: Option<String>,
}

#[derive(Debug, Args)]
pub struct GoalDoneArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Exact goal id.
    pub id: String,
    /// Optional completion summary.
    #[arg(long)]
    pub summary: Option<String>,
    /// Explicitly waive unsatisfied acceptance criteria, recording an audit event.
    #[arg(long, value_name = "REASON")]
    pub waive: Option<String>,
    /// Caller-supplied worker identity; required while a lease is active.
    #[arg(long)]
    pub worker: Option<String>,
}

#[derive(Debug, Args)]
pub struct GoalClaimArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Exact goal id.
    pub id: String,
    /// Caller-supplied worker identity; not authenticated authority.
    #[arg(long)]
    pub worker: String,
    /// Lease duration in seconds before the claim can be reclaimed.
    #[arg(long, default_value_t = 300)]
    pub lease_seconds: u64,
}

#[derive(Debug, Args)]
pub struct GoalClaimNextArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Caller-supplied worker identity; not authenticated authority.
    #[arg(long)]
    pub worker: String,
    /// Lease duration in seconds before the claim can be reclaimed.
    #[arg(long, default_value_t = 300)]
    pub lease_seconds: u64,
}

#[derive(Debug, Args)]
pub struct GoalSatisfyArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Exact goal id.
    pub id: String,
    /// Zero-based acceptance criterion index.
    #[arg(long)]
    pub index: usize,
    /// Caller-supplied worker identity; required while a lease is active.
    #[arg(long)]
    pub worker: Option<String>,
}

#[derive(Debug, Args)]
pub struct GoalVerifyArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Exact goal id.
    pub id: String,
    /// Workdir for `cmd:` acceptance criteria; defaults to the current directory.
    #[arg(long, value_name = "PATH")]
    pub workdir: Option<PathBuf>,
    /// Maximum runtime per command, in seconds (1 through 300).
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=300))]
    pub timeout_seconds: u64,
    /// Caller-supplied worker identity; required while a lease is active.
    #[arg(long)]
    pub worker: Option<String>,
}

#[derive(Debug, Args)]
pub struct GoalPursueArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Exact in-progress goal id.
    pub id: String,
    /// Profile, provider/model pair, or auto target for each fresh segment.
    #[arg(long, value_name = "TARGET")]
    pub target: String,
    /// Canonical workdir shared by verifier commands and runtime segments.
    #[arg(long, value_name = "PATH")]
    pub workdir: Option<PathBuf>,
    /// Workspace permission for each fresh segment.
    #[arg(long, value_enum, default_value_t = SandboxArg::Write)]
    pub sandbox: SandboxArg,
    /// Overall pursuit budget in seconds (1 through 86400).
    #[arg(long, default_value_t = 3600, value_parser = clap::value_parser!(u64).range(1..=vyane_goal::MAX_PURSUIT_TIMEOUT.as_secs()))]
    pub overall_timeout_seconds: u64,
    /// Runtime budget per fresh segment in seconds (1 through 3600).
    #[arg(long, default_value_t = 900, value_parser = clap::value_parser!(u64).range(1..=vyane_goal::MAX_SEGMENT_TIMEOUT.as_secs()))]
    pub segment_timeout_seconds: u64,
    /// Runtime budget per acceptance command in seconds (1 through 300).
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=300))]
    pub verifier_timeout_seconds: u64,
    /// Maximum fresh runtime segments (1 through 64).
    #[arg(long, default_value_t = 6, value_parser = clap::value_parser!(u16).range(1..=i64::from(vyane_goal::MAX_PURSUIT_SEGMENTS)))]
    pub max_segments: u16,
    /// Pause after this many consecutive verifier/runtime failures (1 through 16).
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u16).range(1..=i64::from(vyane_goal::MAX_PURSUIT_FAILURES)))]
    pub max_failures: u16,
    /// Exact active lease holder; pursuit never runs without a lease.
    #[arg(long)]
    pub worker: String,
}

#[derive(Debug, Args)]
pub struct GoalFailArgs {
    #[command(flatten)]
    pub common: GoalCommonArgs,
    /// Exact goal id.
    pub id: String,
    /// Required failure reason.
    #[arg(long)]
    pub reason: String,
    /// Caller-supplied worker identity; required while a lease is active.
    #[arg(long)]
    pub worker: Option<String>,
}

#[derive(Debug, Args)]
pub struct A2aCommonArgs {
    /// SQLite message database; defaults to the standard Vyane data directory.
    #[arg(long, value_name = "PATH")]
    pub db: Option<PathBuf>,
    /// Caller-selected local storage scope; not authenticated authority.
    #[arg(long, alias = "owner-user-id", default_value = "local")]
    pub owner: String,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct A2aSendArgs {
    #[command(flatten)]
    pub common: A2aCommonArgs,
    /// Recipient agent mailbox id.
    pub to: String,
    /// Message body words; stdin is read when omitted.
    #[arg(num_args = 0..)]
    pub body: Vec<String>,
    /// Caller-supplied sender label; not an authenticated identity.
    #[arg(long = "from", alias = "from-code", value_name = "AGENT")]
    pub from: String,
    /// Message kind, such as message, handoff, or review.
    #[arg(long, default_value = "message")]
    pub kind: String,
    /// Delay delivery by this many whole seconds.
    #[arg(long, value_name = "SECONDS")]
    pub delay_seconds: Option<u64>,
    /// Optional conversation/thread id.
    #[arg(long = "thread-id", alias = "conversation", value_name = "ID")]
    pub thread_id: Option<String>,
    /// Optional trace id.
    #[arg(long, value_name = "ID")]
    pub trace_id: Option<String>,
    /// Payload item as KEY=VALUE or a JSON object; repeatable.
    #[arg(long, value_name = "ITEM")]
    pub payload: Vec<String>,
}

#[derive(Debug, Args)]
pub struct A2aInboxArgs {
    #[command(flatten)]
    pub common: A2aCommonArgs,
    /// Recipient agent mailbox id.
    pub to: String,
    /// Include acknowledged messages.
    #[arg(long)]
    pub include_read: bool,
    /// Include messages whose availability time is still in the future.
    #[arg(long)]
    pub include_future: bool,
    /// Maximum number of rows to return.
    #[arg(long, default_value_t = 100)]
    pub limit: usize,
}

#[derive(Debug, Args)]
pub struct A2aReadArgs {
    #[command(flatten)]
    pub common: A2aCommonArgs,
    /// Recipient agent mailbox id. Required to prevent cross-mailbox reads.
    pub to: String,
    /// Exact message id to acknowledge.
    pub message_id: String,
}

#[derive(Debug, Subcommand)]
pub enum WorkflowCommand {
    /// Run a workflow TOML file.
    Run(WorkflowRunArgs),
    /// Submit a workflow source bundle to the resident daemon.
    Submit(WorkflowSubmitArgs),
    /// Show one daemon-owned workflow task.
    Status(WorkflowStatusArgs),
    /// Request cancellation of one daemon-owned workflow task.
    Cancel(WorkflowCancelArgs),
    /// Resume a workflow run from its journal.
    Resume(WorkflowResumeArgs),
    /// Replay a journal-recorded all-success prefix into a new workflow run.
    Replay(WorkflowReplayArgs),
    /// List workflow journals.
    List(WorkflowListArgs),
}

#[derive(Debug, Subcommand)]
pub enum TaskCommand {
    /// List detached runs, most recent first.
    List(TaskListArgs),
    /// Show one detached run's status and recent log lines.
    Status(TaskStatusArgs),
    /// Terminate a detached run's process group.
    Cancel(TaskCancelArgs),
}

#[derive(Debug, Subcommand)]
pub enum SessionCommand {
    /// List redacted, revision-aware continuity sessions.
    List(SessionsArgs),
    /// Inspect one redacted continuity session.
    Inspect(SessionInspectArgs),
    /// Remove native harness continuity using revision-fenced compare-and-swap.
    ResetNative(SessionResetNativeArgs),
}

#[derive(Debug, Subcommand)]
pub enum DaemonCommand {
    /// Run the daemon in the foreground.
    Run(DaemonRunArgs),
    /// Start the daemon in a detached session and wait for readiness.
    Start(DaemonStartArgs),
    /// Verify the recorded daemon process and authenticated health endpoint.
    Status(DaemonStatusArgs),
    /// Gracefully stop the exact recorded daemon process.
    Stop,
}

#[derive(Debug, Args)]
pub struct TaskListArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct TaskStatusArgs {
    /// The detached run id.
    pub id: String,
    /// Print the captured answer (output.txt) instead of the status view.
    #[arg(long)]
    pub output: bool,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct TaskCancelArgs {
    /// The detached run id.
    pub id: String,
}

#[derive(Debug, Args)]
pub struct WorkerArgs {
    /// The detached run id whose job spec to execute.
    pub id: String,
}

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Loopback listen address. Non-loopback addresses are rejected.
    #[arg(long, default_value = "127.0.0.1:9721")]
    pub addr: String,
}

#[derive(Debug, Args)]
pub struct DaemonRunArgs {
    /// Loopback listen address. Port 0 selects an ephemeral port.
    #[arg(long, default_value = "127.0.0.1:9722")]
    pub addr: String,
    #[command(flatten)]
    pub goals: DaemonGoalArgs,
}

#[derive(Debug, Args)]
pub struct DaemonStartArgs {
    /// Loopback listen address. Port 0 selects an ephemeral port.
    #[arg(long, default_value = "127.0.0.1:9722")]
    pub addr: String,
    #[command(flatten)]
    pub goals: DaemonGoalArgs,
}

#[derive(Debug, Clone, Args)]
pub struct DaemonGoalArgs {
    /// Opt in to bounded resident pursuit of local goals.
    #[arg(
        long,
        requires_all = ["goal_target", "goal_workdir"]
    )]
    pub goal_auto_pursue: bool,
    /// Target used for each fresh automatic pursuit segment.
    #[arg(long, value_name = "TARGET", requires = "goal_auto_pursue")]
    pub goal_target: Option<String>,
    /// Canonical workdir shared by automatic verifier commands and segments.
    #[arg(long, value_name = "PATH", requires = "goal_auto_pursue")]
    pub goal_workdir: Option<PathBuf>,
    /// Workspace permission for each fresh automatic pursuit segment.
    #[arg(
        long,
        value_enum,
        default_value_t = SandboxArg::ReadOnly,
        requires = "goal_auto_pursue"
    )]
    pub goal_sandbox: SandboxArg,
    /// Overall budget for one automatic pursuit invocation, in seconds.
    #[arg(long, default_value_t = 3600, value_parser = clap::value_parser!(u64).range(1..=vyane_goal::MAX_PURSUIT_TIMEOUT.as_secs()), requires = "goal_auto_pursue")]
    pub goal_overall_timeout_seconds: u64,
    /// Runtime budget per fresh automatic segment, in seconds.
    #[arg(long, default_value_t = 900, value_parser = clap::value_parser!(u64).range(1..=vyane_goal::MAX_SEGMENT_TIMEOUT.as_secs()), requires = "goal_auto_pursue")]
    pub goal_segment_timeout_seconds: u64,
    /// Runtime budget per automatic acceptance command, in seconds.
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=300), requires = "goal_auto_pursue")]
    pub goal_verifier_timeout_seconds: u64,
    /// Maximum lifetime fresh segments for one goal.
    #[arg(long, default_value_t = 6, value_parser = clap::value_parser!(u16).range(1..=i64::from(vyane_goal::MAX_PURSUIT_SEGMENTS)), requires = "goal_auto_pursue")]
    pub goal_max_segments: u16,
    /// Pause after this many consecutive verifier/runtime failures.
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u16).range(1..=i64::from(vyane_goal::MAX_PURSUIT_FAILURES)), requires = "goal_auto_pursue")]
    pub goal_max_failures: u16,
    /// Delay between idle scans for eligible goals, in milliseconds.
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u64).range(50..=60_000), requires = "goal_auto_pursue")]
    pub goal_poll_millis: u64,
}

#[derive(Debug, Args)]
pub struct DaemonStatusArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct DispatchArgs {
    /// Task text to submit.
    pub task: String,
    /// Target profile, provider/model pair, or `auto`. Use `profile:auto` to
    /// select a profile literally named `auto`.
    #[arg(long, value_name = "TARGET")]
    pub target: String,
    /// Working directory for harness runs.
    #[arg(long, value_name = "PATH")]
    pub workdir: Option<PathBuf>,
    /// Workspace permission level for harness runs.
    #[arg(long, value_enum, default_value_t = SandboxArg::ReadOnly)]
    pub sandbox: SandboxArg,
    /// Continue or create a logical session id.
    #[arg(long, value_name = "ID")]
    pub session: Option<String>,
    /// System prompt for direct HTTP, appended instructions for harnesses.
    #[arg(long, value_name = "TEXT")]
    pub system: Option<String>,
    /// Attempt timeout in seconds; absent means no timeout.
    #[arg(long, value_name = "SECS")]
    pub timeout: Option<u64>,
    /// Label to store on the run record; repeatable as key=value. Router output
    /// fields such as routing.provider and routing.effort are reserved.
    #[arg(long, value_name = "k=v")]
    pub label: Vec<String>,
    /// For `--target auto`, prohibit frontier-tier routing.
    #[arg(long)]
    pub no_frontier: bool,
    /// Run in the background: spawn a detached worker, print its id, and exit
    /// without waiting. Inspect it later with `vyane task`.
    #[arg(long)]
    pub detach: bool,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
    /// Stream deltas to stdout as they arrive for one direct-HTTP or CLI-harness
    /// target; falls back to non-streaming for sessions or failover chains.
    #[arg(long)]
    pub stream: bool,
}

#[derive(Debug, Args)]
pub struct BroadcastArgs {
    /// Task text to submit to each target.
    pub task: String,
    /// Comma-separated target list; each target is a profile or provider/model.
    #[arg(long, value_name = "a,b,c")]
    pub targets: String,
    /// Working directory for harness runs.
    #[arg(long, value_name = "PATH")]
    pub workdir: Option<PathBuf>,
    /// Workspace permission level for harness runs.
    #[arg(long, value_enum, default_value_t = SandboxArg::ReadOnly)]
    pub sandbox: SandboxArg,
    /// System prompt for direct HTTP, appended instructions for harnesses.
    #[arg(long, value_name = "TEXT")]
    pub system: Option<String>,
    /// Attempt timeout in seconds; absent means no timeout.
    #[arg(long, value_name = "SECS")]
    pub timeout: Option<u64>,
    /// Label to store on each run record; repeatable as key=value.
    #[arg(long, value_name = "k=v")]
    pub label: Vec<String>,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct HistoryArgs {
    /// Maximum number of records to show.
    #[arg(long, default_value_t = 20)]
    pub limit: usize,
    /// Filter by run status.
    #[arg(long, value_enum)]
    pub status: Option<RunStatusArg>,
    /// Filter by provider id.
    #[arg(long, value_name = "PROVIDER")]
    pub provider: Option<String>,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct SessionsArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct SessionInspectArgs {
    /// Logical continuity session id.
    pub id: String,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct SessionResetNativeArgs {
    /// Logical continuity session id.
    pub id: String,
    /// Exact revision returned by `session inspect` or `session list`.
    #[arg(long, value_name = "REVISION")]
    pub expected_revision: u64,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct WorkflowRunArgs {
    /// Workflow TOML file to run.
    pub file: PathBuf,
    /// Workflow variable; repeatable as key=value.
    #[arg(long = "var", value_name = "k=v")]
    pub vars: Vec<String>,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct WorkflowSubmitArgs {
    /// Workflow TOML file whose bounded sources are sent to the daemon.
    pub file: PathBuf,
    /// Canonical lowercase UUIDv7 to reuse for an idempotent retry.
    #[arg(long, value_name = "UUIDV7")]
    pub id: Option<WorkflowRunId>,
    /// Workflow variable; repeatable as key=value.
    #[arg(long = "var", value_name = "k=v")]
    pub vars: Vec<String>,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct WorkflowStatusArgs {
    /// Canonical lowercase UUIDv7 workflow id.
    pub wf_run_id: WorkflowRunId,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct WorkflowCancelArgs {
    /// Canonical lowercase UUIDv7 workflow id.
    pub wf_run_id: WorkflowRunId,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct WorkflowResumeArgs {
    /// Workflow run id to resume.
    pub wf_run_id: String,
    /// Workflow TOML file used by the original run.
    #[arg(long, value_name = "FILE")]
    pub file: PathBuf,
    /// Workflow variables are loaded from the journal on resume; passing this is an error.
    #[arg(long = "var", value_name = "k=v")]
    pub vars: Vec<String>,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct WorkflowReplayArgs {
    /// Prior canonical UUIDv7 workflow run to replay.
    pub source_wf_run_id: WorkflowRunId,
    /// Workflow TOML file used by the source run.
    #[arg(long, value_name = "FILE")]
    pub file: PathBuf,
    /// Canonical UUIDv7 for the new run; generated when omitted.
    #[arg(long, value_name = "UUIDV7")]
    pub id: Option<WorkflowRunId>,
    /// Workflow variables are loaded from the source journal; passing this is an error.
    #[arg(long = "var", value_name = "k=v")]
    pub vars: Vec<String>,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct WorkflowListArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

/// Arguments for the built-in review pipeline.
#[derive(Debug, Args)]
pub struct ReviewArgs {
    /// Task / prompt text to implement and review.
    pub task: String,
    /// Target profile for the implementation step.
    #[arg(long, value_name = "TARGET")]
    pub implementer: String,
    /// Comma-separated reviewer target profiles (e.g. "opus,gpt,sonnet").
    #[arg(long, value_name = "a,b,c")]
    pub reviewers: String,
    /// Target profile for the synthesis step.
    #[arg(long, value_name = "TARGET")]
    pub synthesizer: String,
    /// Working directory for harness runs.
    #[arg(long, value_name = "PATH")]
    pub workdir: Option<PathBuf>,
    /// Per-attempt timeout in seconds.
    #[arg(long, value_name = "SECS")]
    pub timeout_secs: Option<u64>,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

/// Arguments for the `vyane route` dry-run command.
#[derive(Debug, Args)]
pub struct RouteArgs {
    /// Task text to analyze.
    pub task: String,
    /// Override the routing stage (e.g. "plan", "review", "architecture").
    #[arg(long, value_name = "STAGE")]
    pub stage: Option<String>,
    /// Override the tier directly: economy, mainline, or frontier.
    #[arg(long, value_name = "TIER")]
    pub tier: Option<String>,
    /// Extra tags beyond what's inferred from the task text (comma-separated).
    #[arg(long, value_name = "a,b,c")]
    pub tags: Option<String>,
    /// Restrict routing to these profile names only (comma-separated).
    #[arg(long, value_name = "a,b,c")]
    pub candidates: Option<String>,
    /// Number of changed files (routing signal).
    #[arg(long, value_name = "N")]
    pub changed_files: Option<usize>,
    /// Number of cross-file dependency edges (routing signal).
    #[arg(long, value_name = "N")]
    pub dependency_edges: Option<usize>,
    /// Retry count — how many times this task has been retried (routing signal).
    #[arg(long, value_name = "N")]
    pub retry_count: Option<usize>,
    /// Prohibit frontier-tier routing.
    #[arg(long)]
    pub no_frontier: bool,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SandboxArg {
    ReadOnly,
    Write,
    Full,
}

impl std::fmt::Display for SandboxArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let raw = match self {
            SandboxArg::ReadOnly => "read-only",
            SandboxArg::Write => "write",
            SandboxArg::Full => "full",
        };
        f.write_str(raw)
    }
}

impl From<SandboxArg> for Sandbox {
    fn from(value: SandboxArg) -> Self {
        match value {
            SandboxArg::ReadOnly => Sandbox::ReadOnly,
            SandboxArg::Write => Sandbox::Write,
            SandboxArg::Full => Sandbox::Full,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum RunStatusArg {
    Success,
    Error,
    Timeout,
    Cancelled,
}

impl From<RunStatusArg> for RunStatus {
    fn from(value: RunStatusArg) -> Self {
        match value {
            RunStatusArg::Success => RunStatus::Success,
            RunStatusArg::Error => RunStatus::Error,
            RunStatusArg::Timeout => RunStatus::Timeout,
            RunStatusArg::Cancelled => RunStatus::Cancelled,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::path::Path;

    use super::*;

    const RUN_ID: &str = "01890f3e-7b7c-7cc2-98d2-3f9a2b6c7d8e";

    #[test]
    fn workflow_daemon_commands_parse_the_expected_surface() {
        let submit = Cli::try_parse_from([
            "vyane",
            "workflow",
            "submit",
            "workflow.toml",
            "--id",
            RUN_ID,
            "--var",
            "topic=rust",
            "--json",
        ])
        .unwrap();
        let Command::Workflow(WorkflowCommand::Submit(args)) = submit.command else {
            panic!("expected workflow submit");
        };
        assert_eq!(args.file, PathBuf::from("workflow.toml"));
        assert_eq!(args.id.as_ref().map(WorkflowRunId::as_str), Some(RUN_ID));
        assert_eq!(args.vars, ["topic=rust"]);
        assert!(args.json);

        for command in ["status", "cancel"] {
            assert!(Cli::try_parse_from(["vyane", "workflow", command, RUN_ID, "--json"]).is_ok());
        }
    }

    #[test]
    fn workflow_daemon_ids_are_validated_by_clap() {
        for invalid in [
            "../../not-a-run-id",
            "550e8400-e29b-41d4-a716-446655440000",
            "01890F3E-7B7C-7CC2-98D2-3F9A2B6C7D8E",
        ] {
            assert!(
                Cli::try_parse_from([
                    "vyane",
                    "workflow",
                    "submit",
                    "workflow.toml",
                    "--id",
                    invalid,
                ])
                .is_err()
            );
        }

        for command in ["status", "cancel"] {
            assert!(
                Cli::try_parse_from(["vyane", "workflow", command, "../../not-a-run-id"]).is_err()
            );
            assert!(
                Cli::try_parse_from([
                    "vyane",
                    "workflow",
                    command,
                    "550e8400-e29b-41d4-a716-446655440000"
                ])
                .is_err()
            );
            assert!(
                Cli::try_parse_from([
                    "vyane",
                    "workflow",
                    command,
                    "01890F3E-7B7C-7CC2-98D2-3F9A2B6C7D8E"
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn session_control_surface_is_narrow_and_revision_fenced() {
        let legacy = Cli::try_parse_from(["vyane", "sessions", "--json"]).unwrap();
        let Command::Sessions(args) = legacy.command else {
            panic!("expected legacy sessions command");
        };
        assert!(args.json);

        let list = Cli::try_parse_from(["vyane", "session", "list", "--json"]).unwrap();
        let Command::Session(SessionCommand::List(args)) = list.command else {
            panic!("expected session list");
        };
        assert!(args.json);

        let inspect =
            Cli::try_parse_from(["vyane", "session", "inspect", "logical", "--json"]).unwrap();
        let Command::Session(SessionCommand::Inspect(args)) = inspect.command else {
            panic!("expected session inspect");
        };
        assert_eq!(args.id, "logical");
        assert!(args.json);

        let reset = Cli::try_parse_from([
            "vyane",
            "session",
            "reset-native",
            "logical",
            "--expected-revision",
            "7",
            "--json",
        ])
        .unwrap();
        let Command::Session(SessionCommand::ResetNative(args)) = reset.command else {
            panic!("expected session reset-native");
        };
        assert_eq!(args.id, "logical");
        assert_eq!(args.expected_revision, 7);
        assert!(args.json);

        assert!(Cli::try_parse_from(["vyane", "session", "reset-native", "logical"]).is_err());
        for forbidden in ["--owner", "--native-id", "--domain", "--digest"] {
            assert!(
                Cli::try_parse_from([
                    "vyane",
                    "session",
                    "reset-native",
                    "logical",
                    "--expected-revision",
                    "7",
                    forbidden,
                    "value",
                ])
                .is_err(),
                "forbidden option {forbidden} parsed"
            );
        }
        assert!(
            Cli::try_parse_from([
                "vyane",
                "session",
                "reset-native",
                "logical",
                "--expected-revision",
                "7",
                "--force",
            ])
            .is_err()
        );
    }

    #[test]
    fn daemon_goal_pursuit_is_explicit_and_bounded_at_parse_time() {
        assert!(Cli::try_parse_from(["vyane", "daemon", "run"]).is_ok());
        assert!(Cli::try_parse_from(["vyane", "daemon", "run", "--goal-auto-pursue"]).is_err());
        assert!(
            Cli::try_parse_from([
                "vyane",
                "daemon",
                "run",
                "--goal-target",
                "builder",
                "--goal-workdir",
                ".",
            ])
            .is_err()
        );

        for mode in ["run", "start"] {
            let parsed = Cli::try_parse_from([
                "vyane",
                "daemon",
                mode,
                "--goal-auto-pursue",
                "--goal-target",
                "builder",
                "--goal-workdir",
                ".",
                "--goal-sandbox",
                "read-only",
                "--goal-max-segments",
                "3",
                "--goal-poll-millis",
                "50",
            ])
            .unwrap();
            let goals = match parsed.command {
                Command::Daemon(DaemonCommand::Run(args)) => args.goals,
                Command::Daemon(DaemonCommand::Start(args)) => args.goals,
                _ => panic!("expected daemon command"),
            };
            assert!(goals.goal_auto_pursue);
            assert_eq!(goals.goal_target.as_deref(), Some("builder"));
            assert_eq!(goals.goal_workdir.as_deref(), Some(Path::new(".")));
            assert_eq!(goals.goal_max_segments, 3);
            assert_eq!(goals.goal_poll_millis, 50);
        }

        for (flag, value) in [
            ("--goal-overall-timeout-seconds", "0"),
            ("--goal-segment-timeout-seconds", "0"),
            ("--goal-verifier-timeout-seconds", "301"),
            ("--goal-max-segments", "0"),
            ("--goal-max-failures", "0"),
            ("--goal-poll-millis", "49"),
        ] {
            let argv = vec![
                "vyane",
                "daemon",
                "run",
                "--goal-auto-pursue",
                "--goal-target",
                "builder",
                "--goal-workdir",
                ".",
                flag,
                value,
            ];
            assert!(
                Cli::try_parse_from(argv).is_err(),
                "{flag} accepted {value}"
            );
        }
    }
}
