//! Protocol fixture for `vyane acp`: a canned stub turn over stdio.
//!
//! The child receives an invalid config file and a cleared environment so a
//! config or credential read fails the fixture. The default command and
//! `--stub` stream the same plan, framed echo, and demo tool call. A
//! `session/cancel` during that turn resolves the prompt as `cancelled`.

#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

const PROMPT: &str = "fixture-echo: line one\nline two \"quoted\" 回声";
const ECHO_LEAD: &str = "Vyane ACP M1 stub agent — ";
const ECHO_NOTICE: &str = "this is a canned turn (no model was dispatched). ";
const ECHO_SAID: &str = "You said: ";

#[tokio::test]
async fn default_stub_streams_plan_echo_and_tool_call_without_touching_the_workdir()
-> anyhow::Result<()> {
    let root = common::private_tempdir();
    let config = root.path().join("invalid-config.toml");
    let config_contents = "this is not valid TOML\n";
    fs::write(&config, config_contents)?;
    let before = snapshot(root.path())?;

    let (updates, done, stderr, status) = dialogue(root.path(), &config, &[], PROMPT, None).await?;
    assert!(status.success(), "exit {status}, stderr {stderr}");
    assert_eq!(status.code(), Some(0));
    assert!(stderr.is_empty(), "stderr {stderr}");
    assert_canned_turn(&updates, &done, PROMPT);
    assert_eq!(fs::read_to_string(&config)?, config_contents);
    let after = snapshot(root.path())?;
    assert_eq!(before, after, "working directory gained or changed files");
    assert!(
        after.keys().all(|path| {
            let lower = path.to_ascii_lowercase();
            !lower.ends_with(".sqlite") && !lower.ends_with(".sqlite3") && !lower.ends_with(".db")
        }),
        "working directory gained a state database: {after:?}"
    );
    Ok(())
}

#[tokio::test]
async fn explicit_stub_flag_streams_the_same_canned_turn() -> anyhow::Result<()> {
    let root = common::private_tempdir();
    let config = root.path().join("invalid-config.toml");
    fs::write(&config, "this is not valid TOML\n")?;
    let (updates, done, stderr, status) =
        dialogue(root.path(), &config, &["--stub"], PROMPT, None).await?;
    assert!(status.success(), "exit {status}, stderr {stderr}");
    assert!(stderr.is_empty(), "stderr {stderr}");
    assert_canned_turn(&updates, &done, PROMPT);
    Ok(())
}

#[tokio::test]
async fn cancel_during_the_turn_resolves_that_prompt_as_cancelled() -> anyhow::Result<()> {
    let root = common::private_tempdir();
    let config = root.path().join("invalid-config.toml");
    fs::write(&config, "this is not valid TOML\n")?;
    let (updates, done, stderr, status) = dialogue(
        root.path(),
        &config,
        &[],
        "cancel me mid turn",
        Some(json!({
            "jsonrpc": "2.0",
            "method": "session/cancel",
            "params": { "sessionId": "__SESSION__" }
        })),
    )
    .await?;
    assert!(status.success(), "exit {status}, stderr {stderr}");
    assert!(stderr.is_empty(), "stderr {stderr}");
    assert!(
        !updates.is_empty(),
        "cancel resolved before any streamed frame"
    );
    assert_eq!(updates[0]["method"], "session/update");
    assert_eq!(updates[0]["params"]["update"]["sessionUpdate"], "plan");
    assert!(updates.iter().all(|frame| frame.get("id").is_none()));
    assert!(
        updates.len() < 7,
        "cancel arrived after every canned frame: {updates:?}"
    );
    let kinds: Vec<&str> = updates
        .iter()
        .map(|frame| {
            frame["params"]["update"]["sessionUpdate"]
                .as_str()
                .expect("sessionUpdate")
        })
        .collect();
    assert_eq!(
        kinds,
        [
            "plan",
            "agent_message_chunk",
            "agent_message_chunk",
            "agent_message_chunk",
            "agent_message_chunk",
            "tool_call",
            "tool_call_update"
        ][..kinds.len()]
    );
    assert_eq!(done["id"], 3);
    assert!(done.get("error").is_none(), "cancel failed: {done}");
    assert_eq!(done["result"]["stopReason"], "cancelled");
    Ok(())
}

#[test]
fn native_flag_is_rejected_before_the_shell_starts() {
    let root = common::private_tempdir();
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_vyane"));
    command
        .env_clear()
        .env("HOME", root.path())
        .env("XDG_CONFIG_HOME", root.path().join("xdg-config"))
        .env("XDG_DATA_HOME", root.path().join("xdg-data"))
        .env("VYANE_DATA_DIR", root.path().join("vyane-data"))
        .env("NO_COLOR", "1")
        .env("TERM", "dumb")
        .env("LC_ALL", "C")
        .current_dir(root.path())
        .args(["acp", "--native"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(profile_file) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile_file);
    }
    let output = command.output().expect("spawn vyane");
    assert_eq!(output.status.code(), Some(2), "status {}", output.status);
    assert!(output.stdout.is_empty(), "stdout {:?}", output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unexpected argument '--native'"),
        "stderr {stderr}"
    );
}

fn assert_canned_turn(updates: &[Value], done: &Value, prompt: &str) {
    assert_eq!(updates.len(), 7, "frames: {updates:?}");
    assert!(updates.iter().all(|frame| frame.get("id").is_none()));
    let session_id = updates[0]["params"]["sessionId"]
        .as_str()
        .expect("session id");
    assert!(!session_id.is_empty());
    assert!(
        updates
            .iter()
            .all(|frame| frame["params"]["sessionId"] == session_id)
    );

    let plan = &updates[0]["params"]["update"];
    assert_eq!(plan["sessionUpdate"], "plan");
    assert_eq!(
        plan["entries"],
        json!([
            {"content": "Receive the prompt", "priority": "medium", "status": "completed"},
            {"content": "Stream a canned echo turn", "priority": "medium", "status": "in_progress"},
            {"content": "Return a stopReason", "priority": "medium", "status": "pending"}
        ])
    );

    let chunks: Vec<&str> = updates[1..5]
        .iter()
        .map(|frame| {
            assert_eq!(
                frame["params"]["update"]["sessionUpdate"],
                "agent_message_chunk"
            );
            assert_eq!(frame["params"]["update"]["content"]["type"], "text");
            frame["params"]["update"]["content"]["text"]
                .as_str()
                .expect("chunk text")
        })
        .collect();
    assert_eq!(chunks, vec![ECHO_LEAD, ECHO_NOTICE, ECHO_SAID, prompt]);

    let tool = &updates[5]["params"]["update"];
    assert_eq!(tool["sessionUpdate"], "tool_call");
    assert_eq!(tool["toolCallId"], "tc-stub-echo");
    assert_eq!(tool["title"], "stub.echo");
    assert_eq!(tool["kind"], "other");
    assert_eq!(tool["status"], "in_progress");
    assert_eq!(tool["rawInput"]["prompt"], prompt);

    let finished = &updates[6]["params"]["update"];
    assert_eq!(finished["sessionUpdate"], "tool_call_update");
    assert_eq!(finished["toolCallId"], "tc-stub-echo");
    assert_eq!(finished["status"], "completed");
    assert_eq!(
        finished["content"],
        json!([{
            "type": "content",
            "content": { "type": "text", "text": "stub tool ok" }
        }])
    );

    assert_eq!(done["id"], 3);
    assert!(done.get("error").is_none(), "prompt failed: {done}");
    assert_eq!(done["result"]["stopReason"], "end_turn");
}

/// Run one initialize + session/new + prompt dialogue.
///
/// When `cancel` is set, its `sessionId` placeholder is replaced and the
/// notification is written after the first streamed frame.
async fn dialogue(
    root: &Path,
    config: &Path,
    acp_args: &[&str],
    prompt: &str,
    cancel: Option<Value>,
) -> anyhow::Result<(Vec<Value>, Value, String, std::process::ExitStatus)> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_vyane"));
    command
        .env_clear()
        .env("HOME", root)
        .env("XDG_CONFIG_HOME", root.join("xdg-config"))
        .env("XDG_DATA_HOME", root.join("xdg-data"))
        .env("VYANE_DATA_DIR", root.join("vyane-data"))
        .env("NO_COLOR", "1")
        .env("TERM", "dumb")
        .env("LC_ALL", "C")
        .current_dir(root)
        .arg("--config")
        .arg(config)
        .arg("acp")
        .args(acp_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(profile_file) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile_file);
    }
    let mut child = command.spawn()?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    let stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        stderr.read_to_end(&mut buf).await.map(|_| buf)
    });

    let dialogue = async {
        let mut stdout = BufReader::new(stdout);
        send(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": 1,
                    "clientCapabilities": {},
                    "clientInfo": { "name": "acp-fixture", "version": "0" }
                }
            }),
        )
        .await?;
        let init = read_message(&mut stdout).await?;
        assert!(init.get("error").is_none(), "initialize failed: {init}");
        assert_eq!(init["id"], 1);
        assert_eq!(init["result"]["protocolVersion"], 1);
        assert_eq!(init["result"]["agentInfo"]["name"], "vyane");
        assert_eq!(
            init["result"]["agentInfo"]["version"],
            env!("CARGO_PKG_VERSION")
        );
        assert_eq!(init["result"]["agentCapabilities"]["loadSession"], false);
        assert_eq!(
            init["result"]["agentCapabilities"]["promptCapabilities"],
            json!({ "image": false, "audio": false, "embeddedContext": false })
        );
        assert_eq!(init["result"]["authMethods"], json!([]));

        let cwd = root.to_string_lossy().into_owned();
        send(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "session/new",
                "params": {
                    "cwd": cwd,
                    "mcpServers": [{ "name": "unused", "command": "do-not-spawn" }]
                }
            }),
        )
        .await?;
        let created = read_message(&mut stdout).await?;
        assert!(
            created.get("error").is_none(),
            "session/new failed: {created}"
        );
        let session_id = created["result"]["sessionId"]
            .as_str()
            .expect("session id")
            .to_string();
        assert!(!session_id.is_empty());

        send(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [{ "type": "text", "text": prompt }]
                }
            }),
        )
        .await?;

        let mut updates = Vec::new();
        if let Some(mut cancel) = cancel {
            let first = read_message(&mut stdout).await?;
            updates.push(first);
            if let Some(params) = cancel.get_mut("params") {
                params["sessionId"] = json!(session_id);
            }
            send(&mut stdin, &cancel).await?;
        }

        let done = loop {
            let message = read_message(&mut stdout).await?;
            if message.get("id").is_some() {
                break message;
            }
            updates.push(message);
            assert!(updates.len() < 20, "too many updates: {updates:?}");
        };

        drop(stdin);
        let mut extra = String::new();
        stdout.read_to_string(&mut extra).await?;
        assert!(
            extra.trim().is_empty(),
            "unexpected stdout after the turn: {extra:?}"
        );
        anyhow::Ok((updates, done))
    };

    let dialogue = tokio::time::timeout(Duration::from_secs(10), dialogue).await;
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
    let stderr = stderr_task.await.expect("stderr task")?;
    let stderr = String::from_utf8_lossy(&stderr).into_owned();
    let status = status.expect("child wait timed out").expect("child wait");
    let (updates, done) = dialogue
        .expect("ACP dialogue timed out")
        .expect("ACP dialogue");
    Ok((updates, done, stderr, status))
}

async fn send(stdin: &mut tokio::process::ChildStdin, message: &Value) -> anyhow::Result<()> {
    let mut line = serde_json::to_vec(message)?;
    line.push(b'\n');
    stdin.write_all(&line).await?;
    stdin.flush().await?;
    Ok(())
}

async fn read_message(
    stdout: &mut BufReader<tokio::process::ChildStdout>,
) -> anyhow::Result<Value> {
    let mut line = String::new();
    let n = tokio::time::timeout(Duration::from_secs(5), stdout.read_line(&mut line)).await??;
    assert!(n > 0, "stdout closed before a JSON-RPC message");
    Ok(serde_json::from_str(line.trim())?)
}

fn snapshot(root: &Path) -> anyhow::Result<BTreeMap<String, Option<Vec<u8>>>> {
    fn visit(
        root: &Path,
        path: &Path,
        files: &mut BTreeMap<String, Option<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            let key = path.strip_prefix(root)?.to_string_lossy().into_owned();
            if entry.file_type()?.is_dir() {
                files.insert(key, None);
                visit(root, &path, files)?;
            } else {
                files.insert(key, Some(fs::read(path)?));
            }
        }
        Ok(())
    }
    let mut files = BTreeMap::new();
    visit(root, root, &mut files)?;
    Ok(files)
}
