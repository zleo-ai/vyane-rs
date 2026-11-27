//! `vyane mcp-smoke`: local stdio MCP startup check.
//!
//! The command spawns this binary's `mcp` subcommand and speaks to it with the
//! rmcp client. It sends `initialize` and `tools/list` only. It does not call a
//! tool, so it does not dispatch or write the run ledger.
//!
//! The child gets a temporary config file and `VYANE_DATA_DIR`. A `--config`
//! path on this command is not opened, and the default home data directory is
//! not read or written. The child environment is a copy of this process, with
//! `PYTHONUNBUFFERED` defaulting to `1` and `VYANE_LOG_LEVEL` defaulting to
//! `WARNING` when those variables are unset. The child working directory is
//! this process's current directory.

use std::collections::HashMap;
use std::ffi::OsString;
use std::path::Path;
use std::process::{ExitCode, Stdio};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::Serialize;
use tokio::process::{Child, ChildStdin, ChildStdout};

use crate::cli::McpSmokeArgs;

const TRANSPORT: &str = "stdio";

pub async fn run(args: McpSmokeArgs) -> Result<ExitCode> {
    let json = args.json;
    match execute(args.timeout, args.tool_limit).await {
        Ok(payload) => {
            if emit(render_success(&payload, json)) {
                Ok(ExitCode::SUCCESS)
            } else {
                Ok(ExitCode::from(1))
            }
        }
        Err(failure) => {
            let _ = emit(render_failure(&failure, json));
            Ok(ExitCode::from(1))
        }
    }
}

async fn execute(timeout_seconds: f64, tool_limit: i64) -> Result<SmokeSuccess, SmokeFailure> {
    let timeout = read_timeout(timeout_seconds).await?;
    let root = tempfile::tempdir().map_err(failure_from)?;
    let config_path = root.path().join("config.toml");
    std::fs::write(&config_path, "# isolated mcp-smoke config\n").map_err(failure_from)?;
    let data_dir = root.path().join("data");
    std::fs::create_dir(&data_dir).map_err(failure_from)?;
    let cwd = std::env::current_dir().map_err(failure_from)?;
    let program = std::env::current_exe().map_err(failure_from)?;
    let server_args = [
        OsString::from("--config"),
        config_path.as_os_str().to_os_string(),
        OsString::from("mcp"),
    ];
    let env = prepare_child_env(std::env::vars_os().collect(), &data_dir);
    let (names, elapsed_ms) = probe(
        ServerCommand {
            program: &program,
            args: &server_args,
            cwd: &cwd,
            env: &env,
        },
        timeout,
    )
    .await?;
    let (tool_count, tools) = select_tools(names, tool_limit);
    Ok(SmokeSuccess {
        status: "success",
        transport: TRANSPORT,
        tool_count,
        tools,
        elapsed_ms,
    })
}

struct ServerCommand<'a> {
    program: &'a Path,
    args: &'a [OsString],
    cwd: &'a Path,
    env: &'a HashMap<OsString, OsString>,
}

async fn probe(
    server: ServerCommand<'_>,
    timeout: Duration,
) -> Result<(Vec<String>, u64), SmokeFailure> {
    let mut command = tokio::process::Command::new(server.program);
    command
        .args(server.args)
        .current_dir(server.cwd)
        .env_clear()
        .envs(server.env)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let started = Instant::now();
    let mut child = command.spawn().map_err(failure_from)?;
    let stdout = take_stdio(child.stdout.take(), "mcp server stdout was not piped")?;
    let stdin = take_stdio(child.stdin.take(), "mcp server stdin was not piped")?;
    let outcome = tokio::time::timeout(timeout, handshake(stdout, stdin)).await;
    reap(&mut child).await;
    let elapsed_ms = elapsed_millis(started.elapsed());
    match outcome {
        Ok(names) => Ok((names?, elapsed_ms)),
        Err(elapsed) => Err(failure_from(elapsed)),
    }
}

async fn handshake(stdout: ChildStdout, stdin: ChildStdin) -> Result<Vec<String>, SmokeFailure> {
    let client = <() as rmcp::ServiceExt<rmcp::RoleClient>>::serve((), (stdout, stdin))
        .await
        .map_err(failure_from)?;
    let listed = client.list_tools(None).await.map_err(failure_from)?;
    Ok(listed
        .tools
        .into_iter()
        .map(|tool| tool.name.into_owned())
        .collect())
}

async fn reap(child: &mut Child) {
    if tokio::time::timeout(Duration::from_secs(2), child.wait())
        .await
        .is_err()
    {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}

fn take_stdio<T>(value: Option<T>, message: &'static str) -> Result<T, SmokeFailure> {
    match value {
        Some(value) => Ok(value),
        None => Err(failure_from(std::io::Error::other(message))),
    }
}

fn prepare_child_env(
    mut env: HashMap<OsString, OsString>,
    data_dir: &Path,
) -> HashMap<OsString, OsString> {
    env.entry(OsString::from("PYTHONUNBUFFERED"))
        .or_insert_with(|| OsString::from("1"));
    env.entry(OsString::from("VYANE_LOG_LEVEL"))
        .or_insert_with(|| OsString::from("WARNING"));
    env.insert(
        OsString::from("VYANE_DATA_DIR"),
        data_dir.as_os_str().to_os_string(),
    );
    env
}

fn select_tools(mut names: Vec<String>, tool_limit: i64) -> (usize, Vec<String>) {
    names.sort();
    let tool_count = names.len();
    let limit = if tool_limit <= 0 {
        0
    } else {
        usize::try_from(tool_limit).unwrap_or(usize::MAX)
    };
    if names.len() > limit {
        names.truncate(limit);
    }
    (tool_count, names)
}

fn elapsed_millis(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

async fn read_timeout(seconds: f64) -> Result<Duration, SmokeFailure> {
    let timeout = match Duration::try_from_secs_f64(seconds) {
        Ok(timeout) => timeout,
        Err(error) => return Err(failure_from(error)),
    };
    // A zero duration cannot wait for a server response. Report the same
    // timeout error a client read would return, and do not start a process.
    if timeout.is_zero() {
        return match tokio::time::timeout(Duration::ZERO, std::future::pending::<()>()).await {
            Err(elapsed) => Err(failure_from(elapsed)),
            Ok(()) => Err(failure_from(std::io::Error::other(
                "timeout elapsed before the client could read",
            ))),
        };
    }
    Ok(timeout)
}

#[derive(Debug, Serialize)]
struct SmokeSuccess {
    status: &'static str,
    transport: &'static str,
    tool_count: usize,
    tools: Vec<String>,
    elapsed_ms: u64,
}

struct SmokeFailure {
    error_type: &'static str,
    error: String,
}

#[derive(Serialize)]
struct SmokeFailureJson<'a> {
    status: &'static str,
    transport: &'static str,
    error_type: &'a str,
    error: &'a str,
}

fn failure_from<E: std::error::Error>(error: E) -> SmokeFailure {
    SmokeFailure {
        error_type: short_type_name::<E>(),
        error: error.to_string(),
    }
}

fn short_type_name<T>() -> &'static str {
    let name = std::any::type_name::<T>();
    name.rsplit("::").next().unwrap_or(name)
}

fn render_success(payload: &SmokeSuccess, json: bool) -> Result<String, serde_json::Error> {
    if json {
        return Ok(format!("{}\n", serde_json::to_string_pretty(payload)?));
    }
    let mut text = format!(
        "MCP stdio smoke OK: {} tool(s), {}ms\n",
        payload.tool_count, payload.elapsed_ms
    );
    if !payload.tools.is_empty() {
        text.push_str("  ");
        text.push_str(&payload.tools.join(", "));
        text.push('\n');
    }
    Ok(text)
}

fn render_failure(failure: &SmokeFailure, json: bool) -> Result<String, serde_json::Error> {
    if json {
        let payload = SmokeFailureJson {
            status: "error",
            transport: TRANSPORT,
            error_type: failure.error_type,
            error: &failure.error,
        };
        return Ok(format!("{}\n", serde_json::to_string_pretty(&payload)?));
    }
    Ok(format!(
        "MCP stdio smoke failed: {}: {}\n",
        failure.error_type, failure.error
    ))
}

fn emit(text: Result<String, serde_json::Error>) -> bool {
    match text {
        Ok(text) => {
            print!("{text}");
            true
        }
        Err(error) => {
            println!(
                "MCP stdio smoke failed: {}: {error}",
                short_type_name::<serde_json::Error>()
            );
            false
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::ffi::OsString;
    use std::path::Path;
    use std::time::Duration;

    use super::{
        ServerCommand, SmokeFailure, SmokeSuccess, prepare_child_env, probe, read_timeout,
        render_failure, render_success, select_tools,
    };

    #[test]
    fn select_tools_sorts_then_truncates() {
        let (count, tools) = select_tools(vec!["b".into(), "a".into(), "c".into()], 2);
        assert_eq!(count, 3);
        assert_eq!(tools, ["a", "b"]);
    }

    #[test]
    fn select_tools_treats_non_positive_limit_as_empty() {
        for limit in [0, -4] {
            let (count, tools) = select_tools(vec!["b".into(), "a".into()], limit);
            assert_eq!(count, 2);
            assert!(tools.is_empty(), "limit {limit}");
        }
    }

    #[test]
    fn child_env_defaults_do_not_override_and_data_dir_is_replaced() {
        let mut env = HashMap::new();
        env.insert(OsString::from("PYTHONUNBUFFERED"), OsString::from("0"));
        env.insert(OsString::from("HOME"), OsString::from("/sentinel-home"));
        env.insert(
            OsString::from("VYANE_DATA_DIR"),
            OsString::from("/sentinel-data"),
        );
        env.insert(OsString::from("KEEP"), OsString::from("yes"));
        let prepared = prepare_child_env(env, Path::new("/tmp/vyane-mcp-smoke-data"));
        assert_eq!(env_value(&prepared, "PYTHONUNBUFFERED"), Some("0"));
        assert_eq!(env_value(&prepared, "VYANE_LOG_LEVEL"), Some("WARNING"));
        assert_eq!(
            env_value(&prepared, "VYANE_DATA_DIR"),
            Some("/tmp/vyane-mcp-smoke-data")
        );
        assert_eq!(env_value(&prepared, "KEEP"), Some("yes"));
        assert_eq!(env_value(&prepared, "HOME"), Some("/sentinel-home"));
    }

    #[test]
    fn renders_success_and_failure_text() {
        let payload = SmokeSuccess {
            status: "success",
            transport: "stdio",
            tool_count: 2,
            tools: vec!["vyane_check".into(), "vyane_dispatch".into()],
            elapsed_ms: 200,
        };
        assert_eq!(
            render_success(&payload, true).unwrap(),
            "{\n  \"status\": \"success\",\n  \"transport\": \"stdio\",\n  \"tool_count\": 2,\n  \"tools\": [\n    \"vyane_check\",\n    \"vyane_dispatch\"\n  ],\n  \"elapsed_ms\": 200\n}\n"
        );
        assert_eq!(
            render_success(&payload, false).unwrap(),
            "MCP stdio smoke OK: 2 tool(s), 200ms\n  vyane_check, vyane_dispatch\n"
        );
        let empty = SmokeSuccess {
            tools: Vec::new(),
            ..payload
        };
        assert_eq!(
            render_success(&empty, false).unwrap(),
            "MCP stdio smoke OK: 2 tool(s), 200ms\n"
        );
        let failure = SmokeFailure {
            error_type: "Elapsed",
            error: "deadline has elapsed".into(),
        };
        assert_eq!(
            render_failure(&failure, false).unwrap(),
            "MCP stdio smoke failed: Elapsed: deadline has elapsed\n"
        );
        assert_eq!(
            render_failure(&failure, true).unwrap(),
            "{\n  \"status\": \"error\",\n  \"transport\": \"stdio\",\n  \"error_type\": \"Elapsed\",\n  \"error\": \"deadline has elapsed\"\n}\n"
        );
    }

    #[tokio::test]
    async fn zero_and_negative_timeouts_fail_before_a_server_starts() {
        let zero = read_timeout(0.0).await.unwrap_err();
        assert_eq!(zero.error_type, "Elapsed");
        assert_eq!(zero.error, "deadline has elapsed");

        let negative = read_timeout(-1.0).await.unwrap_err();
        assert_eq!(negative.error_type, "TryFromFloatSecsError");
        assert!(negative.error.contains("negative"), "{}", negative.error);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bad_server_process_fails_and_leaves_the_directory_alone() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("sentinel"), "untouched").unwrap();
        let mut env = HashMap::new();
        env.insert(
            OsString::from("HOME"),
            root.path().as_os_str().to_os_string(),
        );
        env.insert(
            OsString::from("VYANE_DATA_DIR"),
            root.path().join("vyane-data").as_os_str().to_os_string(),
        );
        let error = probe(
            ServerCommand {
                program: Path::new("/bin/true"),
                args: &[],
                cwd: root.path(),
                env: &env,
            },
            Duration::from_secs(2),
        )
        .await
        .expect_err("a process that is not an MCP server");
        assert_eq!(error.error_type, "ClientInitializeError");
        assert!(!error.error.is_empty());
        assert_eq!(
            std::fs::read_to_string(root.path().join("sentinel")).unwrap(),
            "untouched"
        );
        let entries: BTreeSet<_> = std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(entries, BTreeSet::from([OsString::from("sentinel")]));
    }

    fn env_value<'a>(env: &'a HashMap<OsString, OsString>, key: &str) -> Option<&'a str> {
        env.get(std::ffi::OsStr::new(key))
            .and_then(|value| value.to_str())
    }
}
