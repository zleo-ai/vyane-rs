//! Agent Client Protocol echo shell.
//!
//! `vyane acp` speaks newline-delimited JSON-RPC on stdin and stdout. It
//! answers `initialize`, remembers `session/new` only in this process, and
//! echoes the text blocks of `session/prompt` as one `agent_message_chunk`.
//! Closing stdin ends the process with status 0.
//!
//! This module does not load configuration, read credentials, dispatch a
//! target, call a model, connect MCP servers, or create a state database.
//! It does not import the config, service, harness, or provider crates.

use std::collections::HashSet;
use std::io;
use std::process::ExitCode;

use anyhow::Result;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

const JSONRPC: &str = "2.0";
const PROTOCOL_VERSION: u64 = 1;
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

struct Shell {
    initialized: bool,
    sessions: HashSet<String>,
}

struct Fail {
    code: i64,
    message: &'static str,
}

impl Fail {
    fn not_found() -> Self {
        Self {
            code: METHOD_NOT_FOUND,
            message: "method not found",
        }
    }

    fn request(message: &'static str) -> Self {
        Self {
            code: INVALID_REQUEST,
            message,
        }
    }

    fn params(message: &'static str) -> Self {
        Self {
            code: INVALID_PARAMS,
            message,
        }
    }

    fn response(self, id: Value) -> Value {
        error_response(id, self.code, self.message)
    }
}

pub async fn run() -> Result<ExitCode> {
    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let stdout = tokio::io::stdout();
    serve(stdin, stdout).await?;
    Ok(ExitCode::SUCCESS)
}

async fn serve<R, W>(mut reader: R, mut writer: W) -> io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut shell = Shell {
        initialized: false,
        sessions: HashSet::new(),
    };
    loop {
        let Some(line) = read_message(&mut reader).await? else {
            return Ok(());
        };
        for message in dispatch_line(&mut shell, &line) {
            write_message(&mut writer, &message).await?;
        }
    }
}

async fn read_message<R>(reader: &mut R) -> io::Result<Option<String>>
where
    R: AsyncBufRead + Unpin,
{
    let mut buf = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unterminated ACP message",
            ));
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if buf.len().saturating_add(take) > MAX_MESSAGE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ACP message exceeds 1 MiB",
            ));
        }
        buf.extend_from_slice(&available[..take]);
        reader.consume(take);
        if buf.last() == Some(&b'\n') {
            break;
        }
    }
    if buf.last() == Some(&b'\n') {
        buf.pop();
    }
    if buf.last() == Some(&b'\r') {
        buf.pop();
    }
    String::from_utf8(buf)
        .map(Some)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "ACP message is not UTF-8"))
}

fn dispatch_line(shell: &mut Shell, line: &str) -> Vec<Value> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let value: Value = match serde_json::from_str(trimmed) {
        Ok(value) => value,
        Err(_) => return vec![error_response(Value::Null, PARSE_ERROR, "parse error")],
    };
    let Some(object) = value.as_object() else {
        return vec![error_response(
            Value::Null,
            INVALID_REQUEST,
            "invalid request",
        )];
    };
    let has_id = object.contains_key("id");
    let id = object.get("id").cloned().unwrap_or(Value::Null);
    if object.get("jsonrpc").and_then(Value::as_str) != Some(JSONRPC) {
        if has_id {
            return vec![error_response(id, INVALID_REQUEST, "invalid request")];
        }
        return Vec::new();
    }
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        if has_id {
            return vec![error_response(id, INVALID_REQUEST, "invalid request")];
        }
        return Vec::new();
    };
    let params = object.get("params").cloned().unwrap_or(Value::Null);
    handle(shell, has_id, id, method, &params)
}

fn handle(shell: &mut Shell, has_id: bool, id: Value, method: &str, params: &Value) -> Vec<Value> {
    if !has_id {
        if method == "session/cancel" {
            let _ = cancel(shell, params);
        }
        return Vec::new();
    }
    let result = match method {
        "initialize" => on_initialize(shell, &id, params),
        "session/new" => on_new_session(shell, &id, params),
        "session/prompt" => on_prompt(shell, &id, params),
        "session/cancel" => on_cancel(shell, &id, params),
        _ => Err(Fail::not_found()),
    };
    match result {
        Ok(messages) => messages,
        Err(fail) => vec![fail.response(id)],
    }
}

fn on_initialize(shell: &mut Shell, id: &Value, params: &Value) -> Result<Vec<Value>, Fail> {
    let params = params_object(params)?;
    let version = params
        .get("protocolVersion")
        .ok_or(Fail::params("invalid params"))?;
    if json_u64(version).is_none() {
        return Err(Fail::params("invalid params"));
    }
    if let Some(capabilities) = params.get("clientCapabilities")
        && !capabilities.is_null()
        && !capabilities.is_object()
    {
        return Err(Fail::params("invalid params"));
    }
    shell.initialized = true;
    Ok(vec![success(id.clone(), initialize_result())])
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "agentCapabilities": {
            "loadSession": false,
            "promptCapabilities": {
                "image": false,
                "audio": false,
                "embeddedContext": false
            }
        },
        "authMethods": [],
        "agentInfo": {
            "name": "vyane",
            "version": env!("CARGO_PKG_VERSION")
        }
    })
}

fn on_new_session(shell: &mut Shell, id: &Value, params: &Value) -> Result<Vec<Value>, Fail> {
    if !shell.initialized {
        return Err(Fail::request("initialize required"));
    }
    let params = params_object(params)?;
    let cwd = params
        .get("cwd")
        .and_then(Value::as_str)
        .ok_or(Fail::params("invalid params"))?;
    if cwd.is_empty() {
        return Err(Fail::params("invalid params"));
    }
    let servers = params
        .get("mcpServers")
        .ok_or(Fail::params("invalid params"))?;
    if !servers.is_array() {
        return Err(Fail::params("invalid params"));
    }
    // `cwd` and `mcpServers` are acknowledged and unused. The shell does not
    // open the directory or connect any named server.
    let session_id = Uuid::now_v7().to_string();
    shell.sessions.insert(session_id.clone());
    Ok(vec![success(
        id.clone(),
        json!({ "sessionId": session_id }),
    )])
}

fn on_prompt(shell: &mut Shell, id: &Value, params: &Value) -> Result<Vec<Value>, Fail> {
    if !shell.initialized {
        return Err(Fail::request("initialize required"));
    }
    let params = params_object(params)?;
    let session_id = params
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or(Fail::params("invalid params"))?;
    if !shell.sessions.contains(session_id) {
        return Err(Fail::params("unknown session"));
    }
    let blocks = params
        .get("prompt")
        .and_then(Value::as_array)
        .ok_or(Fail::params("invalid params"))?;
    let text = echo_text(blocks)?;
    let update = json!({
        "jsonrpc": JSONRPC,
        "method": "session/update",
        "params": {
            "sessionId": session_id,
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": text }
            }
        }
    });
    let result = success(id.clone(), json!({ "stopReason": "end_turn" }));
    Ok(vec![update, result])
}

fn on_cancel(shell: &mut Shell, id: &Value, params: &Value) -> Result<Vec<Value>, Fail> {
    cancel(shell, params)?;
    Ok(vec![success(id.clone(), json!({}))])
}

fn cancel(shell: &Shell, params: &Value) -> Result<(), Fail> {
    if !shell.initialized {
        return Err(Fail::request("initialize required"));
    }
    let params = params_object(params)?;
    let session_id = params
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or(Fail::params("invalid params"))?;
    if !shell.sessions.contains(session_id) {
        return Err(Fail::params("unknown session"));
    }
    // The echo turn finishes before the next stdin line is read, so a later
    // cancel has no in-flight model or tool work to stop.
    Ok(())
}

/// Concatenate `text` blocks in order. Resource blocks are accepted and not
/// opened. Any other block is rejected because this shell does not advertise
/// image, audio, or embedded-context prompt capabilities.
fn echo_text(blocks: &[Value]) -> Result<String, Fail> {
    let mut text = String::new();
    for block in blocks {
        let Some(object) = block.as_object() else {
            return Err(Fail::params("invalid content block"));
        };
        match object.get("type").and_then(Value::as_str) {
            Some("text") => {
                let piece = object
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or(Fail::params("invalid content block"))?;
                text.push_str(piece);
            }
            Some("resource" | "resource_link") => {}
            Some(_) | None => return Err(Fail::params("unsupported content block")),
        }
    }
    Ok(text)
}

fn params_object(params: &Value) -> Result<&Map<String, Value>, Fail> {
    params.as_object().ok_or(Fail::params("invalid params"))
}

fn json_u64(value: &Value) -> Option<u64> {
    value.as_u64()
}

fn success(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": JSONRPC,
        "id": id,
        "result": result
    })
}

fn error_response(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": JSONRPC,
        "id": id,
        "error": { "code": code, "message": message }
    })
}

async fn write_message<W>(writer: &mut W, message: &Value) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut line = serde_json::to_vec(message)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    line.push(b'\n');
    writer.write_all(&line).await?;
    writer.flush().await
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    use super::{MAX_MESSAGE_BYTES, Shell, dispatch_line, read_message, serve};

    fn shell() -> Shell {
        Shell {
            initialized: false,
            sessions: Default::default(),
        }
    }

    fn dispatch(shell: &mut Shell, message: serde_json::Value) -> Vec<serde_json::Value> {
        dispatch_line(shell, &message.to_string())
    }

    fn initialize(shell: &mut Shell) -> Vec<serde_json::Value> {
        dispatch(
            shell,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": 1,
                    "clientCapabilities": {},
                    "clientInfo": { "name": "fixture", "version": "0" }
                }
            }),
        )
    }

    fn new_session(shell: &mut Shell) -> String {
        let response = dispatch(
            shell,
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "session/new",
                "params": { "cwd": "/work", "mcpServers": [] }
            }),
        );
        assert_eq!(response.len(), 1);
        response[0]["result"]["sessionId"]
            .as_str()
            .expect("session id")
            .to_string()
    }

    fn prompt(
        shell: &mut Shell,
        session_id: &str,
        blocks: serde_json::Value,
    ) -> Vec<serde_json::Value> {
        dispatch(
            shell,
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session/prompt",
                "params": { "sessionId": session_id, "prompt": blocks }
            }),
        )
    }

    #[test]
    fn initialize_negotiates_version_one_without_session_load() {
        let mut shell = shell();
        let response = initialize(&mut shell);
        assert_eq!(response.len(), 1);
        assert_eq!(response[0]["id"], 1);
        assert_eq!(response[0]["result"]["protocolVersion"], 1);
        assert_eq!(response[0]["result"]["agentInfo"]["name"], "vyane");
        assert_eq!(
            response[0]["result"]["agentInfo"]["version"],
            env!("CARGO_PKG_VERSION")
        );
        assert_eq!(
            response[0]["result"]["agentCapabilities"]["loadSession"],
            false
        );
        assert_eq!(response[0]["result"]["authMethods"], json!([]));
        assert!(response[0].get("error").is_none());

        let negotiated = dispatch(
            &mut shell,
            json!({
                "jsonrpc": "2.0",
                "id": "later",
                "method": "initialize",
                "params": { "protocolVersion": 9 }
            }),
        );
        assert_eq!(negotiated[0]["id"], "later");
        assert_eq!(negotiated[0]["result"]["protocolVersion"], 1);
    }

    #[test]
    fn string_protocol_version_is_rejected() {
        let mut shell = shell();
        let response = dispatch(
            &mut shell,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": { "protocolVersion": "1" }
            }),
        );
        assert_eq!(response[0]["error"]["code"], -32602);
        assert!(!shell.initialized);
    }

    #[test]
    fn prompt_echo_matches_text_exactly_and_ignores_resource_blocks() {
        let mut shell = shell();
        initialize(&mut shell);
        let session_id = new_session(&mut shell);
        let input = "fixture-echo: line one\nline two \"quoted\" 回声  ";
        let response = prompt(
            &mut shell,
            &session_id,
            json!([
                { "type": "text", "text": input },
                { "type": "resource_link", "uri": "file:///not/opened", "name": "x" },
                { "type": "text", "text": "" }
            ]),
        );
        assert_eq!(response.len(), 2);
        assert!(response[0].get("id").is_none());
        assert_eq!(response[0]["method"], "session/update");
        assert_eq!(response[0]["params"]["sessionId"], session_id);
        assert_eq!(
            response[0]["params"]["update"]["sessionUpdate"],
            "agent_message_chunk"
        );
        assert_eq!(response[0]["params"]["update"]["content"]["text"], input);
        assert_eq!(response[1]["id"], 3);
        assert_eq!(response[1]["result"]["stopReason"], "end_turn");

        let second = prompt(
            &mut shell,
            &session_id,
            json!([{ "type": "text", "text": "second" }]),
        );
        assert_eq!(second[0]["params"]["update"]["content"]["text"], "second");
    }

    #[test]
    fn unsupported_content_does_not_echo() {
        let mut shell = shell();
        initialize(&mut shell);
        let session_id = new_session(&mut shell);
        let response = prompt(
            &mut shell,
            &session_id,
            json!([{ "type": "image", "data": "aaaa", "mimeType": "image/png" }]),
        );
        assert_eq!(response.len(), 1);
        assert_eq!(response[0]["error"]["code"], -32602);
    }

    #[test]
    fn session_load_is_method_not_found_and_prompt_needs_a_session() {
        let mut shell = shell();
        let early = dispatch(
            &mut shell,
            json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "session/prompt",
                "params": { "sessionId": "missing", "prompt": [] }
            }),
        );
        assert_eq!(early[0]["error"]["code"], -32600);

        initialize(&mut shell);
        let missing = dispatch(
            &mut shell,
            json!({
                "jsonrpc": "2.0",
                "id": 8,
                "method": "session/load",
                "params": { "sessionId": "missing", "cwd": "/work" }
            }),
        );
        assert_eq!(missing[0]["error"]["code"], -32601);

        let unknown = prompt(&mut shell, "missing", json!([]));
        assert_eq!(unknown[0]["error"]["code"], -32602);
        assert_eq!(unknown[0]["error"]["message"], "unknown session");
    }

    #[test]
    fn cancel_notification_has_no_response() {
        let mut shell = shell();
        initialize(&mut shell);
        let session_id = new_session(&mut shell);
        let response = dispatch(
            &mut shell,
            json!({
                "jsonrpc": "2.0",
                "method": "session/cancel",
                "params": { "sessionId": session_id }
            }),
        );
        assert!(response.is_empty());
        let again = prompt(
            &mut shell,
            &session_id,
            json!([{ "type": "text", "text": "still here" }]),
        );
        assert_eq!(
            again[0]["params"]["update"]["content"]["text"],
            "still here"
        );
    }

    #[test]
    fn blank_lines_are_ignored_and_garbage_is_a_parse_error() {
        let mut shell = shell();
        assert!(dispatch_line(&mut shell, "   ").is_empty());
        let response = dispatch_line(&mut shell, "{");
        assert_eq!(response[0]["id"], serde_json::Value::Null);
        assert_eq!(response[0]["error"]["code"], -32700);
    }

    #[tokio::test]
    async fn serve_writes_one_json_line_and_returns_on_eof() {
        let (agent, mut client) = tokio::io::duplex(4096);
        let (agent_read, agent_write) = tokio::io::split(agent);
        let server =
            tokio::spawn(async move { serve(BufReader::new(agent_read), agent_write).await });
        let request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": { "protocolVersion": 1 }
        });
        client
            .write_all(format!("{request}\n").as_bytes())
            .await
            .expect("write");
        client.flush().await.expect("flush");
        let mut reader = BufReader::new(client);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("read");
        let response: serde_json::Value = serde_json::from_str(line.trim()).expect("json");
        assert_eq!(response["result"]["protocolVersion"], 1);
        drop(reader);
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("server timed out")
            .expect("server task")
            .expect("serve");
    }

    #[tokio::test]
    async fn closed_stdin_ends_the_shell() {
        let (agent, client) = tokio::io::duplex(64);
        drop(client);
        let (agent_read, agent_write) = tokio::io::split(agent);
        serve(BufReader::new(agent_read), agent_write)
            .await
            .expect("eof");
    }

    #[tokio::test]
    async fn oversized_message_is_rejected() {
        let (read, write) = tokio::io::duplex(1024);
        let writer = tokio::spawn(async move {
            let mut writer = write;
            let payload = vec![b'a'; MAX_MESSAGE_BYTES];
            let _ = writer.write_all(&payload).await;
            let _ = writer.write_all(b"\n").await;
        });
        let error = read_message(&mut BufReader::new(read))
            .await
            .expect_err("oversized");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        let _ = writer.await;
    }
}
