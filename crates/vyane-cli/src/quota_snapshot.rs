//! `vyane quota-snapshot`: print a read-only freshness view.
//!
//! This command does not load configuration, probe a provider, or write the
//! snapshot file.

use std::io::{self, Write as _};
use std::process::ExitCode;

use anyhow::Result;
use serde::Serialize;
use vyane_service::quota_snapshot_read_view;

use crate::cli::QuotaSnapshotArgs;

pub fn run(args: QuotaSnapshotArgs) -> Result<ExitCode> {
    let view = quota_snapshot_read_view(args.path.as_deref(), None);
    Ok(write_json(&view))
}

fn write_json(value: &impl Serialize) -> ExitCode {
    let mut bytes = match serde_json::to_vec(value) {
        Ok(bytes) => bytes,
        Err(_) => return write_failed(),
    };
    bytes.push(b'\n');
    if write_stdout(&bytes) {
        ExitCode::SUCCESS
    } else {
        write_failed()
    }
}

fn write_stdout(bytes: &[u8]) -> bool {
    let mut stdout = io::stdout().lock();
    stdout.write_all(bytes).is_ok() && stdout.flush().is_ok()
}

fn write_failed() -> ExitCode {
    let _ = writeln!(io::stderr(), "quota snapshot view could not be written");
    ExitCode::from(2)
}
