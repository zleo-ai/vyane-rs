//! The `vyane` binary — scaffold only, see `docs/ROADMAP.md`.

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "vyane", version, about = "Multi-model orchestration kernel")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Sanity-check the current install.
    Check,
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Check => println!("scaffold — see docs/ROADMAP.md"),
    }
    Ok(())
}
