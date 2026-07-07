use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use vyane_core::{RunStatus, Sandbox};

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
    /// Run one task against a profile or provider/model target.
    Dispatch(DispatchArgs),
    /// Run one task against several targets concurrently.
    Broadcast(BroadcastArgs),
    /// Show recent run ledger records.
    History(HistoryArgs),
    /// List saved session records.
    Sessions(SessionsArgs),
    /// Run, resume, and list declarative workflows.
    #[command(subcommand)]
    Workflow(WorkflowCommand),
}

#[derive(Debug, Subcommand)]
pub enum WorkflowCommand {
    /// Run a workflow TOML file.
    Run(WorkflowRunArgs),
    /// Resume a workflow run from its journal.
    Resume(WorkflowResumeArgs),
    /// List workflow journals.
    List(WorkflowListArgs),
}

#[derive(Debug, Args)]
pub struct DispatchArgs {
    /// Task text to submit.
    pub task: String,
    /// Target profile name or provider/model pair.
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
    /// Label to store on the run record; repeatable as key=value.
    #[arg(long, value_name = "k=v")]
    pub label: Vec<String>,
    /// Emit machine-readable JSON.
    #[arg(long)]
    pub json: bool,
    /// Stream deltas to stdout as they arrive. Only meaningful for a
    /// single-target direct-HTTP dispatch; falls back to non-streaming
    /// (with a stderr notice) for harness targets or multi-target chains.
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
pub struct WorkflowListArgs {
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
