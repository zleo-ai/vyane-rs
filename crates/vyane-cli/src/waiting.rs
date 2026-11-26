//! `vyane waiting`: print the read-only waiting list.
//!
//! This command does not load configuration, call a model, or write a database.

use std::io::{self, Write as _};
use std::process::ExitCode;

use anyhow::Result;
use serde::Serialize;
use vyane_service::{
    WaitingQuery, WaitingQueryError, WaitingSourceName, collect_waiting_list, format_waiting_list,
};

use crate::cli::{WaitingArgs, WaitingWithout};

pub fn run(args: WaitingArgs) -> Result<ExitCode> {
    let without = args
        .without
        .iter()
        .copied()
        .map(WaitingWithout::source)
        .collect::<Vec<_>>();
    let list = match collect_waiting_list(&WaitingQuery {
        owner: &args.owner,
        goal_db: args.goal_db.as_deref(),
        kernel_db: args.kernel_db.as_deref(),
        without: &without,
    }) {
        Ok(list) => list,
        Err(WaitingQueryError::InvalidOwner) => {
            return Ok(reject_owner(args.json));
        }
    };
    if args.json {
        Ok(write_json(&list, ExitCode::SUCCESS))
    } else if write_stdout(format_waiting_list(&list).as_bytes()) {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(write_failed())
    }
}

impl WaitingWithout {
    const fn source(self) -> WaitingSourceName {
        match self {
            Self::Approvals => WaitingSourceName::Approvals,
            Self::Tasks => WaitingSourceName::Tasks,
            Self::Projections => WaitingSourceName::Projections,
        }
    }
}

#[derive(Serialize)]
struct ErrorOutput {
    status: &'static str,
    error: &'static str,
}

fn reject_owner(json: bool) -> ExitCode {
    if json {
        write_json(
            &ErrorOutput {
                status: "error",
                error: "owner is invalid",
            },
            ExitCode::from(2),
        )
    } else {
        let _ = writeln!(io::stderr(), "owner is invalid");
        ExitCode::from(2)
    }
}

fn write_json(value: &impl Serialize, code: ExitCode) -> ExitCode {
    let mut bytes = match serde_json::to_vec(value) {
        Ok(bytes) => bytes,
        Err(_) => return write_failed(),
    };
    bytes.push(b'\n');
    if write_stdout(&bytes) {
        code
    } else {
        write_failed()
    }
}

fn write_stdout(bytes: &[u8]) -> bool {
    let mut stdout = io::stdout().lock();
    stdout.write_all(bytes).is_ok() && stdout.flush().is_ok()
}

fn write_failed() -> ExitCode {
    let _ = writeln!(io::stderr(), "waiting list could not be written");
    ExitCode::from(2)
}
