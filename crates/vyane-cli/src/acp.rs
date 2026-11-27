//! Agent Client Protocol canned stub.
//!
//! `vyane acp` speaks newline-delimited JSON-RPC on stdin and stdout. It
//! answers `initialize`, remembers `session/new` only in this process, and
//! streams one canned `session/prompt` turn: a plan, framed echo chunks, and
//! a demo tool call. `session/cancel` during that turn stops it. Closing
//! stdin ends the process with status 0.
//!
//! `--stub` selects this same stub and is the default. This module does not
//! load configuration, read credentials, dispatch a target, call a model,
//! connect MCP servers, or create a state database. It does not import the
//! config, service, harness, or provider crates.

use std::collections::HashSet;
use std::io;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use uuid::Uuid;

const JSONRPC: &str = "2.0";
const PROTOCOL_VERSION: u64 = 1;
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

/// Gap between streamed frames so a cancel can land before the turn ends.
const STREAM_STEP: Duration = Duration::from_millis(30);
/// After the last frame, wait briefly so a late cancel still resolves this prompt.
const SETTLE: Duration = Duration::from_millis(250);
const RAW_INPUT_CHARS: usize = 200;

const ECHO_LEAD: &str = "Vyane ACP M1 stub agent — ";
const ECHO_NOTICE: &str = "this is a canned turn (no model was dispatched). ";
const ECHO_SAID: &str = "You said: ";
const EMPTY_PROMPT: &str = "(empty prompt)";
const TOOL_CALL_ID: &str = "tc-stub-echo";
const TOOL_TITLE: &str = "stub.echo";
const TOOL_RESULT: &str = "stub tool ok";

const PLAN_RECEIVE: &str = "Receive the prompt";
const PLAN_STREAM: &str = "Stream a canned echo turn";
const PLAN_STOP: &str = "Return a stopReason";

struct Shell {
    initialized: bool,
    sessions: HashSet<String>,
    inflight: Option<Inflight>,
}

struct Inflight {
    request_id: Value,
    session_id: String,
    gate: CancelGate,
}

#[derive(Clone)]
struct CancelGate {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl CancelGate {
    fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    fn cancel(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    /// Returns whether the turn is cancelled. A cancel already set, or one that
    /// arrives during `delay`, completes immediately.
    async fn pause(&self, delay: Duration) -> bool {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        // Register the waiter before reading the flag so a cancel in between
        // cannot be missed. `notify_waiters` does not store a permit.
        if notified.as_mut().enable() || self.is_cancelled() {
            return true;
        }
        tokio::select! {
            biased;
            _ = &mut notified => true,
            _ = tokio::time::sleep(delay) => self.is_cancelled(),
        }
    }
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

#[derive(Debug)]
enum Step {
    Messages(Vec<Value>),
    Prompt {
        id: Value,
        session_id: String,
        text: String,
    },
    Cancel {
        session_id: String,
        messages: Vec<Value>,
    },
}

enum TurnMsg {
    Frame(Value),
    Finished {
        id: Value,
        stop_reason: &'static str,
    },
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// `explicit_stub` is true when the caller passed `--stub`. Omitting the flag
/// selects this same canned stub.
pub async fn run(explicit_stub: bool) -> Result<ExitCode> {
    let _explicit_stub = explicit_stub;
    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let stdout = tokio::io::stdout();
    serve(stdin, stdout).await?;
    Ok(ExitCode::SUCCESS)
}

async fn serve<R, W>(mut reader: R, mut writer: W) -> io::Result<()>
where
    R: AsyncBufRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin,
{
    let (in_tx, mut in_rx) = mpsc::unbounded_channel();
    // The reader task owns the byte buffer. Cancelling a read from `select`
    // would drop a partial line, so inbound messages arrive only as whole lines.
    let reader_task = AbortOnDrop(tokio::spawn(async move {
        loop {
            let incoming = read_message(&mut reader).await;
            let stop = !matches!(incoming, Ok(Some(_)));
            if in_tx.send(incoming).is_err() {
                break;
            }
            if stop {
                break;
            }
        }
    }));
    let (frame_tx, mut frame_rx) = mpsc::unbounded_channel();
    // Keep the frame channel open between turns so `recv` waits instead of
    // returning `None` while no prompt is running.
    let _keep_frames = frame_tx.clone();

    let mut shell = Shell {
        initialized: false,
        sessions: HashSet::new(),
        inflight: None,
    };
    let mut turn_task: Option<AbortOnDrop> = None;
    let outcome = loop {
        tokio::select! {
            biased;
            frame = frame_rx.recv() => {
                match frame {
                    Some(TurnMsg::Frame(value)) => write_message(&mut writer, &value).await?,
                    Some(TurnMsg::Finished { id, stop_reason })
                        if shell
                            .inflight
                            .as_ref()
                            .is_some_and(|inflight| inflight.request_id == id) =>
                    {
                        shell.inflight = None;
                        write_message(
                            &mut writer,
                            &success(id, json!({ "stopReason": stop_reason })),
                        )
                        .await?;
                    }
                    Some(TurnMsg::Finished { .. }) | None => {}
                }
            }
            incoming = in_rx.recv() => {
                match incoming {
                    Some(Ok(Some(line))) => {
                        handle_line(&mut shell, &line, &frame_tx, &mut turn_task, &mut writer)
                            .await?;
                    }
                    Some(Ok(None)) => break Ok(()),
                    Some(Err(error)) => break Err(error),
                    None => {
                        break Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "ACP reader stopped",
                        ));
                    }
                }
            }
        }
    };
    drop(reader_task);
    drop(turn_task);
    outcome
}

async fn handle_line<W>(
    shell: &mut Shell,
    line: &str,
    frame_tx: &mpsc::UnboundedSender<TurnMsg>,
    turn_task: &mut Option<AbortOnDrop>,
    writer: &mut W,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match step(shell, line) {
        Step::Messages(messages) => write_messages(writer, &messages).await,
        Step::Cancel {
            session_id,
            messages,
        } => {
            if let Some(inflight) = &shell.inflight
                && inflight.session_id == session_id
            {
                inflight.gate.cancel();
            }
            write_messages(writer, &messages).await
        }
        Step::Prompt {
            id,
            session_id,
            text,
        } => {
            if shell.inflight.is_some() {
                return write_message(writer, &Fail::request("prompt in progress").response(id))
                    .await;
            }
            start_turn(shell, frame_tx, turn_task, id, session_id, text);
            Ok(())
        }
    }
}

fn start_turn(
    shell: &mut Shell,
    frame_tx: &mpsc::UnboundedSender<TurnMsg>,
    turn_task: &mut Option<AbortOnDrop>,
    id: Value,
    session_id: String,
    text: String,
) {
    let gate = CancelGate::new();
    shell.inflight = Some(Inflight {
        request_id: id.clone(),
        session_id: session_id.clone(),
        gate: gate.clone(),
    });
    let tx = frame_tx.clone();
    *turn_task = Some(AbortOnDrop(tokio::spawn(async move {
        let stop_reason = drive_stub_turn(&session_id, &text, &gate, |frame| {
            tx.send(TurnMsg::Frame(frame)).is_ok()
        })
        .await;
        let _ = tx.send(TurnMsg::Finished { id, stop_reason });
    })));
}

async fn drive_stub_turn(
    session_id: &str,
    text: &str,
    gate: &CancelGate,
    mut emit: impl FnMut(Value) -> bool,
) -> &'static str {
    if gate.is_cancelled() {
        return "cancelled";
    }
    if !emit(session_update(session_id, plan_body())) {
        return "cancelled";
    }
    for chunk in echo_chunks(text) {
        if gate.is_cancelled() {
            return "cancelled";
        }
        if !emit(session_update(session_id, message_chunk(&chunk))) {
            return "cancelled";
        }
        if gate.pause(STREAM_STEP).await {
            return "cancelled";
        }
    }
    if gate.is_cancelled() || !emit(session_update(session_id, tool_start(text))) {
        return "cancelled";
    }
    if gate.pause(STREAM_STEP).await {
        return "cancelled";
    }
    if !emit(session_update(session_id, tool_done())) {
        return "cancelled";
    }
    if gate.pause(SETTLE).await {
        return "cancelled";
    }
    "end_turn"
}

fn echo_chunks(text: &str) -> Vec<String> {
    let cleaned = text.trim();
    let body = if cleaned.is_empty() {
        EMPTY_PROMPT
    } else {
        cleaned
    };
    vec![
        ECHO_LEAD.to_string(),
        ECHO_NOTICE.to_string(),
        ECHO_SAID.to_string(),
        body.to_string(),
    ]
}

fn raw_prompt(text: &str) -> String {
    text.trim().chars().take(RAW_INPUT_CHARS).collect()
}

fn plan_body() -> Value {
    json!({
        "sessionUpdate": "plan",
        "entries": [
            plan_entry(PLAN_RECEIVE, "completed"),
            plan_entry(PLAN_STREAM, "in_progress"),
            plan_entry(PLAN_STOP, "pending")
        ]
    })
}

fn plan_entry(content: &str, status: &str) -> Value {
    json!({
        "content": content,
        "priority": "medium",
        "status": status
    })
}

fn message_chunk(text: &str) -> Value {
    json!({
        "sessionUpdate": "agent_message_chunk",
        "content": { "type": "text", "text": text }
    })
}

fn tool_start(text: &str) -> Value {
    json!({
        "sessionUpdate": "tool_call",
        "toolCallId": TOOL_CALL_ID,
        "title": TOOL_TITLE,
        "kind": "other",
        "status": "in_progress",
        "rawInput": { "prompt": raw_prompt(text) }
    })
}

fn tool_done() -> Value {
    json!({
        "sessionUpdate": "tool_call_update",
        "toolCallId": TOOL_CALL_ID,
        "status": "completed",
        "content": [{
            "type": "content",
            "content": { "type": "text", "text": TOOL_RESULT }
        }]
    })
}

fn session_update(session_id: &str, update: Value) -> Value {
    json!({
        "jsonrpc": JSONRPC,
        "method": "session/update",
        "params": {
            "sessionId": session_id,
            "update": update
        }
    })
}

#[cfg(test)]
fn stub_frames(session_id: &str, text: &str) -> Vec<Value> {
    let mut frames = vec![session_update(session_id, plan_body())];
    for chunk in echo_chunks(text) {
        frames.push(session_update(session_id, message_chunk(&chunk)));
    }
    frames.push(session_update(session_id, tool_start(text)));
    frames.push(session_update(session_id, tool_done()));
    frames
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

fn step(shell: &mut Shell, line: &str) -> Step {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Step::Messages(Vec::new());
    }
    let value: Value = match serde_json::from_str(trimmed) {
        Ok(value) => value,
        Err(_) => {
            return Step::Messages(vec![error_response(
                Value::Null,
                PARSE_ERROR,
                "parse error",
            )]);
        }
    };
    let Some(object) = value.as_object() else {
        return Step::Messages(vec![error_response(
            Value::Null,
            INVALID_REQUEST,
            "invalid request",
        )]);
    };
    let has_id = object.contains_key("id");
    let id = object.get("id").cloned().unwrap_or(Value::Null);
    if object.get("jsonrpc").and_then(Value::as_str) != Some(JSONRPC) {
        if has_id {
            return Step::Messages(vec![error_response(id, INVALID_REQUEST, "invalid request")]);
        }
        return Step::Messages(Vec::new());
    }
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        if has_id {
            return Step::Messages(vec![error_response(id, INVALID_REQUEST, "invalid request")]);
        }
        return Step::Messages(Vec::new());
    };
    let params = object.get("params").cloned().unwrap_or(Value::Null);
    if !has_id {
        if method == "session/cancel"
            && let Ok(session_id) = cancel(shell, &params)
        {
            return Step::Cancel {
                session_id,
                messages: Vec::new(),
            };
        }
        return Step::Messages(Vec::new());
    }
    match method {
        "initialize" => Step::Messages(result_messages(id, on_initialize(shell, &params))),
        "session/new" => Step::Messages(result_messages(id, on_new_session(shell, &params))),
        "session/prompt" => match prepare_prompt(shell, &params) {
            Ok((session_id, text)) => Step::Prompt {
                id,
                session_id,
                text,
            },
            Err(fail) => Step::Messages(vec![fail.response(id)]),
        },
        "session/cancel" => match cancel(shell, &params) {
            Ok(session_id) => Step::Cancel {
                session_id,
                messages: vec![success(id, json!({}))],
            },
            Err(fail) => Step::Messages(vec![fail.response(id)]),
        },
        _ => Step::Messages(vec![Fail::not_found().response(id)]),
    }
}

fn result_messages(id: Value, result: Result<Value, Fail>) -> Vec<Value> {
    match result {
        Ok(result) => vec![success(id, result)],
        Err(fail) => vec![fail.response(id)],
    }
}

fn on_initialize(shell: &mut Shell, params: &Value) -> Result<Value, Fail> {
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
    Ok(initialize_result())
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

fn on_new_session(shell: &mut Shell, params: &Value) -> Result<Value, Fail> {
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
    Ok(json!({ "sessionId": session_id }))
}

fn prepare_prompt(shell: &Shell, params: &Value) -> Result<(String, String), Fail> {
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
    Ok((session_id.to_string(), text))
}

fn cancel(shell: &Shell, params: &Value) -> Result<String, Fail> {
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
    Ok(session_id.to_string())
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

async fn write_messages<W>(writer: &mut W, messages: &[Value]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    for message in messages {
        write_message(writer, message).await?;
    }
    Ok(())
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

    use super::{
        CancelGate, ECHO_LEAD, ECHO_NOTICE, ECHO_SAID, EMPTY_PROMPT, MAX_MESSAGE_BYTES,
        PLAN_RECEIVE, PLAN_STOP, PLAN_STREAM, Shell, Step, TOOL_CALL_ID, TOOL_RESULT, TOOL_TITLE,
        drive_stub_turn, echo_chunks, read_message, serve, step, stub_frames, tool_start,
    };

    fn shell() -> Shell {
        Shell {
            initialized: false,
            sessions: Default::default(),
            inflight: None,
        }
    }

    fn messages(shell: &mut Shell, message: serde_json::Value) -> Vec<serde_json::Value> {
        match step(shell, &message.to_string()) {
            Step::Messages(messages) => messages,
            Step::Cancel { messages, .. } => messages,
            Step::Prompt { .. } => panic!("expected an immediate reply"),
        }
    }

    fn initialize(shell: &mut Shell) -> Vec<serde_json::Value> {
        messages(
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
        let response = messages(
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

    fn prompt_step(
        shell: &mut Shell,
        session_id: &str,
        blocks: serde_json::Value,
    ) -> (serde_json::Value, String, String) {
        match step(
            shell,
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session/prompt",
                "params": { "sessionId": session_id, "prompt": blocks }
            })
            .to_string(),
        ) {
            Step::Prompt {
                id,
                session_id,
                text,
            } => (id, session_id, text),
            Step::Messages(messages) => panic!("prompt failed: {messages:?}"),
            Step::Cancel { .. } => panic!("prompt was classified as cancel"),
        }
    }

    async fn read_json<R>(reader: &mut R) -> serde_json::Value
    where
        R: AsyncBufReadExt + Unpin,
    {
        let mut line = String::new();
        let n = tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut line))
            .await
            .expect("timed out")
            .expect("read");
        assert!(n > 0, "stdout closed before a JSON-RPC message");
        serde_json::from_str(line.trim()).expect("json")
    }

    struct Client {
        writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
        reader: BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
    }

    async fn with_server() -> (Client, tokio::task::JoinHandle<std::io::Result<()>>) {
        let (agent, client) = tokio::io::duplex(64 * 1024);
        let (agent_read, agent_write) = tokio::io::split(agent);
        let server =
            tokio::spawn(async move { serve(BufReader::new(agent_read), agent_write).await });
        let (client_read, client_write) = tokio::io::split(client);
        (
            Client {
                writer: client_write,
                reader: BufReader::new(client_read),
            },
            server,
        )
    }

    async fn send(client: &mut Client, message: &serde_json::Value) {
        let mut line = serde_json::to_vec(message).expect("json");
        line.push(b'\n');
        client.writer.write_all(&line).await.expect("write");
        client.writer.flush().await.expect("flush");
    }

    async fn open_session(client: &mut Client) -> String {
        send(
            client,
            &json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": { "protocolVersion": 1 }
            }),
        )
        .await;
        let init = read_json(&mut client.reader).await;
        assert_eq!(init["result"]["protocolVersion"], 1);
        send(
            client,
            &json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "session/new",
                "params": { "cwd": "/work", "mcpServers": [] }
            }),
        )
        .await;
        let created = read_json(&mut client.reader).await;
        created["result"]["sessionId"]
            .as_str()
            .expect("session id")
            .to_string()
    }

    async fn read_until_id(
        client: &mut Client,
        id: i64,
    ) -> (Vec<serde_json::Value>, serde_json::Value) {
        let mut updates = Vec::new();
        loop {
            let message = read_json(&mut client.reader).await;
            if message.get("id").is_some() {
                assert_eq!(message["id"], id);
                return (updates, message);
            }
            updates.push(message);
            assert!(updates.len() < 20, "too many updates: {updates:?}");
        }
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

        let negotiated = messages(
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
        let response = messages(
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
    fn canned_frames_echo_the_prompt_and_ignore_resource_blocks() {
        let mut shell = shell();
        initialize(&mut shell);
        let session_id = new_session(&mut shell);
        let input = "fixture-echo: line one\nline two \"quoted\" 回声  ";
        let (id, echoed_session, text) = prompt_step(
            &mut shell,
            &session_id,
            json!([
                { "type": "text", "text": input },
                { "type": "resource_link", "uri": "file:///not/opened", "name": "x" },
                { "type": "text", "text": "" }
            ]),
        );
        assert_eq!(id, 3);
        assert_eq!(echoed_session, session_id);
        assert_eq!(text, input);

        let frames = stub_frames(&session_id, &text);
        assert_eq!(frames.len(), 7);
        assert_eq!(frames[0]["params"]["update"]["sessionUpdate"], "plan");
        let entries = frames[0]["params"]["update"]["entries"]
            .as_array()
            .expect("entries");
        assert_eq!(entries[0]["content"], PLAN_RECEIVE);
        assert_eq!(entries[0]["status"], "completed");
        assert_eq!(entries[0]["priority"], "medium");
        assert_eq!(entries[1]["content"], PLAN_STREAM);
        assert_eq!(entries[1]["status"], "in_progress");
        assert_eq!(entries[2]["content"], PLAN_STOP);
        assert_eq!(entries[2]["status"], "pending");

        let chunks: Vec<&str> = frames[1..5]
            .iter()
            .map(|frame| {
                frame["params"]["update"]["content"]["text"]
                    .as_str()
                    .expect("chunk")
            })
            .collect();
        assert_eq!(
            chunks,
            vec![ECHO_LEAD, ECHO_NOTICE, ECHO_SAID, input.trim(),]
        );
        assert_eq!(
            chunks.concat(),
            format!("{ECHO_LEAD}{ECHO_NOTICE}{ECHO_SAID}{}", input.trim())
        );

        let tool = &frames[5]["params"]["update"];
        assert_eq!(tool["sessionUpdate"], "tool_call");
        assert_eq!(tool["toolCallId"], TOOL_CALL_ID);
        assert_eq!(tool["title"], TOOL_TITLE);
        assert_eq!(tool["kind"], "other");
        assert_eq!(tool["status"], "in_progress");
        assert_eq!(tool["rawInput"]["prompt"], input.trim());

        let done = &frames[6]["params"]["update"];
        assert_eq!(done["sessionUpdate"], "tool_call_update");
        assert_eq!(done["toolCallId"], TOOL_CALL_ID);
        assert_eq!(done["status"], "completed");
        assert_eq!(done["content"][0]["type"], "content");
        assert_eq!(done["content"][0]["content"]["text"], TOOL_RESULT);
        assert!(frames.iter().all(|frame| frame.get("id").is_none()));
    }

    #[test]
    fn empty_prompt_and_long_prompt_have_stable_canned_text() {
        assert_eq!(echo_chunks("   \n").last().expect("body"), EMPTY_PROMPT);
        assert_eq!(tool_start("   ")["rawInput"]["prompt"], "");
        let long = "y".repeat(250);
        assert_eq!(echo_chunks(&long).last().expect("body"), &long);
        assert_eq!(
            tool_start(&long)["rawInput"]["prompt"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            200
        );
    }

    #[test]
    fn unsupported_content_does_not_echo() {
        let mut shell = shell();
        initialize(&mut shell);
        let session_id = new_session(&mut shell);
        let response = messages(
            &mut shell,
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [{ "type": "image", "data": "aaaa", "mimeType": "image/png" }]
                }
            }),
        );
        assert_eq!(response.len(), 1);
        assert_eq!(response[0]["error"]["code"], -32602);
    }

    #[test]
    fn session_load_is_method_not_found_and_prompt_needs_a_session() {
        let mut shell = shell();
        let early = messages(
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
        let missing = messages(
            &mut shell,
            json!({
                "jsonrpc": "2.0",
                "id": 8,
                "method": "session/load",
                "params": { "sessionId": "missing", "cwd": "/work" }
            }),
        );
        assert_eq!(missing[0]["error"]["code"], -32601);

        let unknown = messages(
            &mut shell,
            json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "session/prompt",
                "params": { "sessionId": "missing", "prompt": [] }
            }),
        );
        assert_eq!(unknown[0]["error"]["code"], -32602);
        assert_eq!(unknown[0]["error"]["message"], "unknown session");
    }

    #[test]
    fn cancel_notification_has_no_response_and_keeps_the_session() {
        let mut shell = shell();
        initialize(&mut shell);
        let session_id = new_session(&mut shell);
        match step(
            &mut shell,
            &json!({
                "jsonrpc": "2.0",
                "method": "session/cancel",
                "params": { "sessionId": session_id }
            })
            .to_string(),
        ) {
            Step::Cancel {
                messages,
                session_id: cancelled,
            } => {
                assert!(messages.is_empty());
                assert_eq!(cancelled, session_id);
            }
            other => panic!("expected a cancel step: {other:?}"),
        }
        let request = messages(
            &mut shell,
            json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "session/cancel",
                "params": { "sessionId": session_id }
            }),
        );
        assert_eq!(
            request,
            vec![json!({
                "jsonrpc": "2.0",
                "id": 4,
                "result": {}
            })]
        );
        let (_id, _session, text) = prompt_step(
            &mut shell,
            &session_id,
            json!([{ "type": "text", "text": "still here" }]),
        );
        assert_eq!(text, "still here");
    }

    #[test]
    fn blank_lines_are_ignored_and_garbage_is_a_parse_error() {
        let mut shell = shell();
        assert!(matches!(
            step(&mut shell, "   "),
            Step::Messages(messages) if messages.is_empty()
        ));
        match step(&mut shell, "{") {
            Step::Messages(response) => {
                assert_eq!(response[0]["id"], serde_json::Value::Null);
                assert_eq!(response[0]["error"]["code"], -32700);
            }
            other => panic!("expected a parse error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_between_frames_omits_the_rest_of_the_turn() {
        let gate = CancelGate::new();
        let cancel_after = gate.clone();
        let mut frames = Vec::new();
        let stop = drive_stub_turn("sess", "cancel me", &gate, |frame| {
            frames.push(frame);
            if frames.len() == 2 {
                cancel_after.cancel();
            }
            true
        })
        .await;
        assert_eq!(stop, "cancelled");
        assert_eq!(frames, stub_frames("sess", "cancel me")[..2]);
        assert_eq!(frames[1]["params"]["update"]["content"]["text"], ECHO_LEAD);
    }

    #[tokio::test]
    async fn cancelled_before_the_first_frame_emits_nothing() {
        let gate = CancelGate::new();
        gate.cancel();
        let mut frames = Vec::new();
        let stop = drive_stub_turn("sess", "hi", &gate, |frame| {
            frames.push(frame);
            true
        })
        .await;
        assert_eq!(stop, "cancelled");
        assert!(frames.is_empty());
    }

    #[tokio::test]
    async fn serve_streams_the_canned_turn_then_ends() {
        let (mut client, server) = with_server().await;
        let session_id = open_session(&mut client).await;
        let text = "fixture-echo: line one\nline two \"quoted\" 回声";
        send(
            &mut client,
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [{ "type": "text", "text": text }]
                }
            }),
        )
        .await;
        let (updates, done) = read_until_id(&mut client, 3).await;
        assert_eq!(updates, stub_frames(&session_id, text));
        assert_eq!(done["result"]["stopReason"], "end_turn");
        assert!(done.get("error").is_none());

        send(
            &mut client,
            &json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [{ "type": "text", "text": "second" }]
                }
            }),
        )
        .await;
        let (second, done) = read_until_id(&mut client, 4).await;
        assert_eq!(second, stub_frames(&session_id, "second"));
        assert_eq!(done["result"]["stopReason"], "end_turn");
        drop(client);
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("server timed out")
            .expect("server task")
            .expect("serve");
    }

    #[tokio::test]
    async fn serve_cancel_stops_the_turn_before_later_frames() {
        let (mut client, server) = with_server().await;
        let session_id = open_session(&mut client).await;
        send(
            &mut client,
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [{ "type": "text", "text": "cancel me" }]
                }
            }),
        )
        .await;
        let first = read_json(&mut client.reader).await;
        assert_eq!(first["params"]["update"]["sessionUpdate"], "plan");
        send(
            &mut client,
            &json!({
                "jsonrpc": "2.0",
                "method": "session/cancel",
                "params": { "sessionId": session_id }
            }),
        )
        .await;
        let (updates, done) = read_until_id(&mut client, 3).await;
        let mut seen = vec![first];
        seen.extend(updates);
        let expected = stub_frames(&session_id, "cancel me");
        assert!(
            (2..expected.len()).contains(&seen.len()),
            "cancel should omit later frames, saw {} of {}",
            seen.len(),
            expected.len()
        );
        assert_eq!(seen, expected[..seen.len()]);
        assert_eq!(seen[1]["params"]["update"]["content"]["text"], ECHO_LEAD);
        assert_eq!(done["result"]["stopReason"], "cancelled");
        assert!(done.get("error").is_none());

        send(
            &mut client,
            &json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [{ "type": "text", "text": "after" }]
                }
            }),
        )
        .await;
        let (second, again) = read_until_id(&mut client, 5).await;
        assert_eq!(second, stub_frames(&session_id, "after"));
        assert_eq!(again["result"]["stopReason"], "end_turn");
        drop(client);
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("server timed out")
            .expect("server task")
            .expect("serve");
    }

    #[tokio::test]
    async fn second_prompt_while_a_turn_is_running_is_rejected() {
        let (mut client, server) = with_server().await;
        let session_id = open_session(&mut client).await;
        send(
            &mut client,
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [{ "type": "text", "text": "first" }]
                }
            }),
        )
        .await;
        let _plan = read_json(&mut client.reader).await;
        send(
            &mut client,
            &json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [{ "type": "text", "text": "overlap" }]
                }
            }),
        )
        .await;
        let mut saw_error = false;
        let mut saw_end = false;
        for _ in 0..12 {
            let message = read_json(&mut client.reader).await;
            if message["id"] == 9 {
                assert_eq!(message["error"]["code"], -32600);
                assert_eq!(message["error"]["message"], "prompt in progress");
                saw_error = true;
            } else if message["id"] == 3 {
                assert_eq!(message["result"]["stopReason"], "end_turn");
                saw_end = true;
            }
            if saw_error && saw_end {
                break;
            }
        }
        assert!(saw_error, "overlapping prompt was accepted");
        assert!(saw_end, "the first turn did not finish");
        drop(client);
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("server timed out")
            .expect("server task")
            .expect("serve");
    }

    #[tokio::test]
    async fn serve_writes_one_json_line_and_returns_on_eof() {
        let (mut client, server) = with_server().await;
        send(
            &mut client,
            &json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": { "protocolVersion": 1 }
            }),
        )
        .await;
        let response = read_json(&mut client.reader).await;
        assert_eq!(response["result"]["protocolVersion"], 1);
        drop(client);
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
