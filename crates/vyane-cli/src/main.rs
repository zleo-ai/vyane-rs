//! The `vyane` binary: the assembler that wires config, protocol clients,
//! harnesses, kernel orchestration and local persistence behind a CLI.

mod a2a;
#[cfg(target_os = "linux")]
mod agent_host;
#[cfg(target_os = "linux")]
mod agent_process;
#[cfg(target_os = "linux")]
mod agent_spool;
mod api;
mod app;
mod cli;
mod command;
mod daemon;
#[cfg(target_os = "linux")]
mod daemon_agent;
mod daemon_client;
mod daemon_goal;
mod daemon_workflow;
mod factory;
mod goal;
mod goal_runtime;
mod mcp_workflow;
#[cfg(target_os = "linux")]
#[allow(dead_code)]
mod native_agent;
#[cfg(target_os = "linux")]
#[allow(dead_code)]
mod native_agent_spool;
mod output;
mod review;
mod supervisor;
mod task;
mod workflow_control;

use std::process::ExitCode;

use clap::Parser;

#[tokio::main]
async fn main() -> ExitCode {
    app::init_tracing();
    match command::run(cli::Cli::parse()).await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::from(1)
        }
    }
}
