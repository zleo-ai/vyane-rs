//! Protocol fixture for `vyane acp`: initialize, one echo turn, exit 0, and
//! no new state database. The child receives an invalid config file and a
//! cleared environment so a config or credential read fails the fixture.

#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

const PROMPT: &str = "fixture-echo: line one\nline two \"quoted\" 回声";

#[tokio::test]
async fn initialize_and_echo_round_trip_leaves_the_workdir_unchanged() -> anyhow::Result<()> {
    let root = common::private_tempdir();
    let config = root.path().join("invalid-config.toml");
    let config_contents = "this is not valid TOML\n";
    fs::write(&config, config_contents)?;
    let before = snapshot(root.path())?;

    let mut command = Command::new(env!("CARGO_BIN_EXE_vyane"));
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
        .arg("--config")
        .arg(&config)
        .arg("acp")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
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

        let cwd = root.path().to_string_lossy().into_owned();
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
                    "prompt": [{ "type": "text", "text": PROMPT }]
                }
            }),
        )
        .await?;
        let update = read_message(&mut stdout).await?;
        assert_eq!(update["method"], "session/update");
        assert!(update.get("id").is_none());
        assert_eq!(update["params"]["sessionId"], session_id);
        assert_eq!(
            update["params"]["update"]["sessionUpdate"],
            "agent_message_chunk"
        );
        assert_eq!(update["params"]["update"]["content"]["type"], "text");
        assert_eq!(update["params"]["update"]["content"]["text"], PROMPT);

        let done = read_message(&mut stdout).await?;
        assert_eq!(done["id"], 3);
        assert!(done.get("error").is_none(), "prompt failed: {done}");
        assert_eq!(done["result"]["stopReason"], "end_turn");

        drop(stdin);
        let mut extra = String::new();
        stdout.read_to_string(&mut extra).await?;
        assert!(
            extra.trim().is_empty(),
            "unexpected stdout after the echo turn: {extra:?}"
        );
        anyhow::Ok(())
    };

    let dialogue = tokio::time::timeout(Duration::from_secs(10), dialogue).await;
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
    let stderr = stderr_task.await.expect("stderr task")?;
    let stderr = String::from_utf8_lossy(&stderr);
    let status = status.expect("child wait timed out").expect("child wait");
    dialogue
        .expect("ACP dialogue timed out")
        .expect("ACP dialogue");
    assert!(status.success(), "exit {status}, stderr {stderr}");
    assert_eq!(status.code(), Some(0));
    assert!(stderr.is_empty(), "stderr {stderr}");
    assert_eq!(
        fs::read_to_string(&config)?,
        config_contents,
        "invalid config was rewritten"
    );
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
