//! `vyane liveness sandbox-orphans`: print a read-only Codex sandbox report.
//!
//! The command reads a procfs tree and writes text or JSON to stdout. It does
//! not signal or stop any process, and it does not load configuration.

use std::io::{self, Write as _};
use std::process::ExitCode;

use anyhow::Result;
use serde::Serialize;
use vyane_service::{
    CodexSandboxOrphanQuery, SubjectUid, render_sandbox_orphan_report, scan_codex_sandbox_orphans,
};

use crate::cli::{LivenessCommand, SandboxOrphansArgs};

pub fn run(json: bool, command: LivenessCommand) -> Result<ExitCode> {
    match command {
        LivenessCommand::SandboxOrphans(args) => Ok(sandbox_orphans(json, args)),
    }
}

fn sandbox_orphans(json: bool, args: SandboxOrphansArgs) -> ExitCode {
    let report = scan_codex_sandbox_orphans(&CodexSandboxOrphanQuery {
        proc_root: args.proc_root,
        now_unix_seconds: None,
        clk_tck: None,
        uid: SubjectUid::Current,
        warn_after_seconds: args.warn_after_seconds,
    });
    let exit = if report.available {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    };
    let wrote = if json {
        write_json(&report)
    } else {
        write_text(&render_sandbox_orphan_report(&report))
    };
    if wrote { exit } else { write_failed() }
}

fn write_json(value: &impl Serialize) -> bool {
    let mut bytes = match serde_json::to_vec_pretty(value) {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };
    bytes.push(b'\n');
    write_stdout(&bytes)
}

fn write_text(text: &str) -> bool {
    write_stdout(text.as_bytes())
}

fn write_stdout(bytes: &[u8]) -> bool {
    let mut stdout = io::stdout().lock();
    stdout.write_all(bytes).is_ok() && stdout.flush().is_ok()
}

fn write_failed() -> ExitCode {
    let _ = writeln!(io::stderr(), "sandbox orphan report could not be written");
    ExitCode::from(2)
}
