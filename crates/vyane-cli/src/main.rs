//! The `vyane` binary: the assembler that wires config, protocol clients,
//! harnesses, kernel orchestration and local persistence behind a CLI.

mod app;
mod cli;
mod command;
mod factory;
mod output;

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
