//! CLI for the local presence table.
//!
//! The commands call `vyane_harness::presence` in this process. They do not
//! spawn another Vyane process, and they do not read or write the A2A mailbox.

use std::io::Write;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde::Serialize;
use serde_json::{Value, json};
use vyane_harness::presence::{
    CheckIn, Heartbeat, ListQuery, PresenceError, PresenceStore, resolve_presence_path,
};

use crate::cli::{
    PresenceCheckinArgs, PresenceCommand, PresenceCommonArgs, PresenceHeartbeatArgs,
    PresenceLeaveArgs, PresenceListArgs,
};

struct Rendered {
    human: String,
    json: Value,
}

pub fn run(command: PresenceCommand) -> Result<ExitCode> {
    let json = common_of(&command).json;
    match run_inner(command) {
        Ok(rendered) => {
            if json {
                print_json(&rendered.json)?;
            } else {
                println!("{}", rendered.human);
            }
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            eprintln!("presence error: {}: {error}", error.as_str());
            if json {
                print_json(&json!({
                    "status": "error",
                    "error": {
                        "code": error.as_str(),
                        "message": error.to_string(),
                    }
                }))?;
            }
            Ok(ExitCode::from(1))
        }
    }
}

fn run_inner(command: PresenceCommand) -> Result<Rendered, PresenceError> {
    match command {
        PresenceCommand::Checkin(args) => check_in(args),
        PresenceCommand::Heartbeat(args) => heartbeat(args),
        PresenceCommand::Leave(args) => leave(args),
        PresenceCommand::List(args) => list(args),
    }
}

fn check_in(args: PresenceCheckinArgs) -> Result<Rendered, PresenceError> {
    let now = resolve_now(args.common.now)?;
    let store = open_store(args.common.path.as_deref())?;
    let outcome = store.check_in(
        &CheckIn {
            session_id: args.session,
            label: args.label,
            repo: args.repo,
            card: args.card,
            status: args.status,
            harness: args.harness,
        },
        now,
    )?;
    let human = if let Some(agent) = outcome.agent.as_ref() {
        format!(
            "checked in: {} @ {} {}",
            if agent.label.is_empty() {
                agent.session_id.as_str()
            } else {
                agent.label.as_str()
            },
            display(&agent.repo),
            display(&agent.card)
        )
    } else {
        "presence unchanged".to_owned()
    };
    Ok(Rendered {
        human,
        json: flatten_success(&outcome)?,
    })
}

fn heartbeat(args: PresenceHeartbeatArgs) -> Result<Rendered, PresenceError> {
    let now = resolve_now(args.common.now)?;
    let store = open_store(args.common.path.as_deref())?;
    let outcome = store.heartbeat(
        &Heartbeat {
            session_id: args.session.clone(),
            label: args.label,
            repo: args.repo,
            harness: args.harness,
        },
        now,
    )?;
    let human = if outcome.written {
        format!("heartbeat: {}", args.session.trim())
    } else {
        "presence unchanged".to_owned()
    };
    Ok(Rendered {
        human,
        json: flatten_success(&outcome)?,
    })
}

fn leave(args: PresenceLeaveArgs) -> Result<Rendered, PresenceError> {
    let now = resolve_now(args.common.now)?;
    let store = open_store(args.common.path.as_deref())?;
    let outcome = store.leave(&args.session, now)?;
    let human = format!("left: removed={}", outcome.removed);
    Ok(Rendered {
        human,
        json: flatten_success(&outcome)?,
    })
}

fn list(args: PresenceListArgs) -> Result<Rendered, PresenceError> {
    let now = resolve_now(args.common.now)?;
    let store = open_store(args.common.path.as_deref())?;
    let listed = store.list(
        &ListQuery {
            session_id: args.session,
            include_self: args.all,
            include_offline: args.all,
        },
        now,
    )?;
    let human = if listed.agents.is_empty() {
        "no online peers".to_owned()
    } else {
        let mut lines = vec![format!("online agents ({}):", listed.agents.len())];
        for agent in &listed.agents {
            lines.push(format!(
                "  {} {} {} {} {}",
                agent.session_id,
                display(&agent.label),
                display(&agent.repo),
                display(&agent.card),
                display(&agent.status),
            ));
        }
        lines.join("\n")
    };
    let body = json!({
        "status": "success",
        "now": listed.now,
        "ttl_seconds": listed.ttl_seconds,
        "count": listed.agents.len(),
        "agents": listed.agents,
    });
    Ok(Rendered { human, json: body })
}

fn common_of(command: &PresenceCommand) -> &PresenceCommonArgs {
    match command {
        PresenceCommand::Checkin(args) => &args.common,
        PresenceCommand::Heartbeat(args) => &args.common,
        PresenceCommand::Leave(args) => &args.common,
        PresenceCommand::List(args) => &args.common,
    }
}

fn open_store(path: Option<&std::path::Path>) -> Result<PresenceStore, PresenceError> {
    Ok(PresenceStore::open(resolve_presence_path(path)?))
}

fn resolve_now(now: Option<f64>) -> Result<f64, PresenceError> {
    if let Some(now) = now {
        return if now.is_finite() {
            Ok(now)
        } else {
            Err(PresenceError::InvalidTime)
        };
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .map_err(|_| PresenceError::InvalidTime)
}

fn display(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

fn flatten_success(body: &impl Serialize) -> Result<Value, PresenceError> {
    let mut value = serde_json::to_value(body).map_err(|_| PresenceError::StoreCorrupt)?;
    if let Some(object) = value.as_object_mut() {
        object.insert("status".to_owned(), Value::String("success".to_owned()));
    }
    Ok(value)
}

fn print_json(value: &Value) -> Result<()> {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer(&mut stdout, value)?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}
