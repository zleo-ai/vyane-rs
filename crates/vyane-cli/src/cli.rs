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
    /// Send and read owner-scoped messages through the local durable queue.
    #[command(subcommand)]
    A2a(A2aCommand),
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
    /// Mark one exact mailbox message as read.
    Read(A2aReadArgs),
}

#[derive(Debug, Args)]
pub struct A2aCommonArgs {
    /// SQLite message database; defaults to the standard Vyane data directory.
    #[arg(long, value_name = "PATH")]
    pub db: Option<PathBuf>,
    /// Explicit owner authority for this local queue operation.
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
    /// Sender agent id.
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
}

#[derive(Debug, Args)]
pub struct DaemonStartArgs {
    /// Loopback listen address. Port 0 selects an ephemeral port.
    #[arg(long, default_value = "127.0.0.1:9722")]
    pub addr: String,
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
}
