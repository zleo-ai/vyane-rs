//! Server-sent-event decoding for the streaming HTTP protocol clients.
//!
//! Every protocol is normalized onto the [`StreamEvent`] contract documented
//! in `vyane-core`: text and reasoning deltas as they arrive, then at most one
//! [`StreamEvent::Usage`] carrying the stream's total, then exactly one
//! [`StreamEvent::Done`] as the last event — or a single error instead of the
//! `Done`. Each protocol says differently where its stream ends:
//!
//! | protocol | finish reason | usage | end of stream |
//! | --- | --- | --- | --- |
//! | OpenAI Chat | `choices[].finish_reason` | a later `choices: []` chunk (`include_usage`) | `data: [DONE]` |
//! | Anthropic Messages | `message_delta.delta.stop_reason` | `message_start.message.usage`, then cumulative `message_delta.usage` | `message_stop` |
//! | OpenAI Responses | `response.incomplete` details | `response.completed` / `response.incomplete` | those two events |
//!
//! So a finish reason is only recorded when it is seen; `Done` is emitted when
//! the protocol's end-of-stream marker arrives. EOF before that marker is a
//! transport error (see [`SseDecoder::finish`]), except that an OpenAI Chat
//! stream that already reported a finish reason may omit `[DONE]`.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{BoxStream, Stream, StreamExt};
use reqwest::Response;
use serde::Deserialize;
use serde_json::Value;
use vyane_core::{ErrorKind, Result, StreamEvent, Usage, VyaneError};

use crate::http::{MAX_JSON_RESPONSE_BYTES, reqwest_error_kind};
use crate::wire;

#[derive(Debug, Clone, Copy)]
pub(crate) enum StreamProtocol {
    OpenAiChat,
    Anthropic,
    OpenAiResponses,
}

pub(crate) fn response_to_stream(
    response: Response,
    protocol: StreamProtocol,
) -> BoxStream<'static, Result<StreamEvent>> {
    let bytes = response
        .bytes_stream()
        .map(|chunk| {
            chunk
                .map(|bytes| bytes.to_vec())
                .map_err(stream_transport_error)
        })
        .boxed();
    SseStream::new(bytes, protocol).boxed()
}

/// Incremental SSE decoder for one response stream.
///
/// Frames are split at the first blank line, whichever of the `\n` / `\r\n`
/// line endings the peer uses (and in whatever mix). A single frame may not
/// exceed [`MAX_JSON_RESPONSE_BYTES`], the same cap the non-streaming path
/// applies to a whole JSON body; a peer that never sends a frame delimiter
/// therefore cannot grow the buffer without bound.
///
/// Once `Done` or an error has been emitted the decoder is closed: further
/// bytes are ignored and `finish` adds nothing.
#[derive(Default)]
pub(crate) struct SseDecoder {
    buffer: Vec<u8>,
    /// Offset into `buffer` where the next boundary scan resumes, so each
    /// pushed byte is scanned once instead of rescanning the whole buffer.
    scan_from: usize,
    /// First finish/stop reason the stream reported, held for `Done`.
    finish_reason: Option<String>,
    /// Latest usage total reported by an OpenAI stream (each report is a
    /// complete total, so a later one replaces an earlier one).
    usage: Option<Usage>,
    /// Anthropic usage merged from `message_start` and `message_delta`.
    anthropic_usage: Option<wire::anthropic::UsageResponse>,
    done: bool,
    failed: bool,
}

/// What one decoded SSE frame contributes to the stream.
enum Signal {
    /// Forwarded to the consumer as-is (a text or reasoning delta).
    Event(StreamEvent),
    /// A complete usage total that replaces any earlier one.
    UsageTotal(Usage),
    /// Anthropic usage counters. They are cumulative, so they are merged
    /// field by field rather than added.
    AnthropicUsage(wire::anthropic::UsageResponse),
    /// The model's finish/stop reason, reported later with `Done`.
    FinishReason(String),
    /// The protocol's own end-of-stream event.
    End,
}

impl SseDecoder {
    pub(crate) fn push(
        &mut self,
        chunk: &[u8],
        protocol: StreamProtocol,
    ) -> VecDeque<Result<StreamEvent>> {
        let mut out = VecDeque::new();
        if self.is_closed() {
            return out;
        }
        let mut buffer = std::mem::take(&mut self.buffer);
        buffer.extend_from_slice(chunk);

        let mut start = 0;
        let mut scan = self.scan_from;
        while !self.is_closed() {
            match next_frame_boundary(&buffer, scan) {
                FrameBoundary::Found { frame_end, next } => {
                    let frame = &buffer[start..frame_end];
                    if frame.len() > MAX_JSON_RESPONSE_BYTES {
                        self.fail(frame_too_large(), &mut out);
                        break;
                    }
                    self.decode_frame(frame, protocol, &mut out);
                    start = next;
                    scan = next;
                }
                FrameBoundary::NeedMore { resume } => {
                    scan = resume;
                    break;
                }
            }
        }
        if self.is_closed() {
            self.release_buffer();
            return out;
        }

        buffer.drain(..start);
        if buffer.len() > MAX_JSON_RESPONSE_BYTES {
            self.fail(frame_too_large(), &mut out);
            return out;
        }
        self.scan_from = scan - start;
        self.buffer = buffer;
        out
    }

    /// Signal EOF: decode any unterminated trailing frame, then close a
    /// stream that has not ended yet.
    ///
    /// Anthropic and Responses streams always end with an explicit event
    /// (`message_stop`, `response.completed` / `response.incomplete`), so
    /// EOF before it means the output was cut off and is an error. OpenAI
    /// Chat is accepted without `[DONE]` once a `finish_reason` has been
    /// seen (some compatible servers omit `[DONE]`), but not before.
    ///
    /// The error is [`ErrorKind::Transport`]: the connection closed
    /// mid-response, which at this layer is the same failure as a stream
    /// dropped by a proxy or upstream (the "broken stream" case that kind
    /// covers), and a mid-body read error already maps there. `Protocol`
    /// is kept for an endpoint that answered with malformed or error
    /// content. Both kinds are retried and fail over alike.
    pub(crate) fn finish(&mut self, protocol: StreamProtocol) -> VecDeque<Result<StreamEvent>> {
        let mut out = VecDeque::new();
        if self.is_closed() {
            return out;
        }
        let frame = std::mem::take(&mut self.buffer);
        self.scan_from = 0;
        if !frame.is_empty() {
            self.decode_frame(&frame, protocol, &mut out);
        }
        if self.is_closed() {
            return out;
        }
        let complete_at_eof = match protocol {
            StreamProtocol::OpenAiChat => self.finish_reason.is_some(),
            StreamProtocol::Anthropic | StreamProtocol::OpenAiResponses => false,
        };
        if complete_at_eof {
            self.end(&mut out);
        } else {
            self.fail(
                VyaneError::new(
                    ErrorKind::Transport,
                    "stream closed before its end-of-stream event",
                ),
                &mut out,
            );
        }
        out
    }

    /// Whether the stream has ended (`Done` or an error was emitted).
    pub(crate) fn is_closed(&self) -> bool {
        self.done || self.failed
    }

    fn release_buffer(&mut self) {
        self.buffer = Vec::new();
        self.scan_from = 0;
    }

    /// Record a fatal decode error. Nothing is decoded after it, and the
    /// buffered bytes are released.
    fn fail(&mut self, error: VyaneError, out: &mut VecDeque<Result<StreamEvent>>) {
        self.failed = true;
        self.release_buffer();
        out.push_back(Err(error));
    }

    /// End the stream: the usage total (if any was reported), then `Done`.
    fn end(&mut self, out: &mut VecDeque<Result<StreamEvent>>) {
        self.done = true;
        let usage = self
            .anthropic_usage
            .take()
            .map(wire::anthropic::usage_from_response)
            .or_else(|| self.usage.take());
        if let Some(usage) = usage {
            out.push_back(Ok(StreamEvent::Usage(usage)));
        }
        out.push_back(Ok(StreamEvent::Done {
            finish_reason: self.finish_reason.take(),
        }));
    }

    fn decode_frame(
        &mut self,
        frame: &[u8],
        protocol: StreamProtocol,
        out: &mut VecDeque<Result<StreamEvent>>,
    ) {
        let Ok(text) = std::str::from_utf8(frame) else {
            self.fail(
                VyaneError::new(ErrorKind::Protocol, "SSE frame was not valid UTF-8"),
                out,
            );
            return;
        };
        let data = collect_data_lines(text);
        if data.trim().is_empty() {
            return;
        }
        if data.trim() == "[DONE]" {
            self.end(out);
            return;
        }

        let parsed = match protocol {
            StreamProtocol::OpenAiChat => parse_openai_chat(&data),
            StreamProtocol::Anthropic => parse_anthropic(&data),
            StreamProtocol::OpenAiResponses => parse_openai_responses(&data),
        };
        let signals = match parsed {
            Ok(signals) => signals,
            Err(error) => {
                self.fail(error, out);
                return;
            }
        };
        for signal in signals {
            match signal {
                Signal::Event(event) => out.push_back(Ok(event)),
                Signal::UsageTotal(usage) => self.usage = Some(usage),
                Signal::AnthropicUsage(usage) => {
                    merge_anthropic_usage(&mut self.anthropic_usage, usage)
                }
                Signal::FinishReason(reason) => {
                    self.finish_reason.get_or_insert(reason);
                }
                Signal::End => {
                    self.end(out);
                    return;
                }
            }
        }
    }
}

/// Merge cumulative Anthropic usage counters.
///
/// `message_start` reports the input side (and an initial output count);
/// `message_delta` reports cumulative counts — the output total, and on
/// newer API versions the input fields again. Each field therefore keeps
/// the largest value reported, which never double-counts and tolerates a
/// later event that omits a field or reports it as zero.
fn merge_anthropic_usage(
    total: &mut Option<wire::anthropic::UsageResponse>,
    next: wire::anthropic::UsageResponse,
) {
    let Some(total) = total.as_mut() else {
        *total = Some(next);
        return;
    };
    fn latest(current: Option<u64>, next: Option<u64>) -> Option<u64> {
        match (current, next) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }
    }
    total.input_tokens = latest(total.input_tokens, next.input_tokens);
    total.output_tokens = latest(total.output_tokens, next.output_tokens);
    total.cache_read_input_tokens =
        latest(total.cache_read_input_tokens, next.cache_read_input_tokens);
    total.cache_creation_input_tokens = latest(
        total.cache_creation_input_tokens,
        next.cache_creation_input_tokens,
    );
}

struct SseStream {
    inner: BoxStream<'static, Result<Vec<u8>>>,
    decoder: SseDecoder,
    protocol: StreamProtocol,
    pending: VecDeque<Result<StreamEvent>>,
    finished: bool,
}

impl SseStream {
    fn new(inner: BoxStream<'static, Result<Vec<u8>>>, protocol: StreamProtocol) -> Self {
        Self {
            inner,
            decoder: SseDecoder::default(),
            protocol,
            pending: VecDeque::new(),
            finished: false,
        }
    }
}

impl Stream for SseStream {
    type Item = Result<StreamEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(event) = self.pending.pop_front() {
            return Poll::Ready(Some(event));
        }
        // After `Done` or an error the stream is over, even if the peer keeps
        // the connection open.
        if self.finished || self.decoder.is_closed() {
            self.finished = true;
            return Poll::Ready(None);
        }

        loop {
            match self.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    let protocol = self.protocol;
                    self.pending = self.decoder.push(&chunk, protocol);
                    if let Some(event) = self.pending.pop_front() {
                        return Poll::Ready(Some(event));
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    self.finished = true;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(None) => {
                    self.finished = true;
                    let protocol = self.protocol;
                    self.pending = self.decoder.finish(protocol);
                    if let Some(event) = self.pending.pop_front() {
                        return Poll::Ready(Some(event));
                    }
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

enum FrameBoundary {
    /// A frame ends at `frame_end` (exclusive); the next frame starts at
    /// `next`, after the blank-line delimiter.
    Found { frame_end: usize, next: usize },
    /// No complete delimiter yet; resume scanning at `resume` once more bytes
    /// arrive (a `\n` that may start a delimiter is not skipped past).
    NeedMore { resume: usize },
}

/// Find the earliest blank line at or after `from`.
///
/// A blank line is a line terminator (`\n` or `\r\n`) directly followed by
/// another one, so `\n\n`, `\r\n\r\n`, `\r\n\n` and `\n\r\n` all end a frame
/// and whichever occurs first wins.
fn next_frame_boundary(buffer: &[u8], from: usize) -> FrameBoundary {
    let mut index = from;
    while let Some(offset) = buffer
        .get(index..)
        .and_then(|rest| rest.iter().position(|byte| *byte == b'\n'))
    {
        let newline = index + offset;
        match buffer.get(newline + 1) {
            None => return FrameBoundary::NeedMore { resume: newline },
            Some(b'\n') => {
                return FrameBoundary::Found {
                    frame_end: newline,
                    next: newline + 2,
                };
            }
            Some(b'\r') => match buffer.get(newline + 2) {
                None => return FrameBoundary::NeedMore { resume: newline },
                Some(b'\n') => {
                    return FrameBoundary::Found {
                        frame_end: newline,
                        next: newline + 3,
                    };
                }
                Some(_) => index = newline + 1,
            },
            Some(_) => index = newline + 1,
        }
    }
    FrameBoundary::NeedMore {
        resume: buffer.len(),
    }
}

fn frame_too_large() -> VyaneError {
    VyaneError::new(
        ErrorKind::Protocol,
        format!("SSE frame exceeds {MAX_JSON_RESPONSE_BYTES} bytes"),
    )
}

fn collect_data_lines(frame: &str) -> String {
    frame
        .lines()
        .filter_map(|line| {
            let line = line.strip_suffix('\r').unwrap_or(line);
            line.strip_prefix("data:")
                .map(|data| data.strip_prefix(' ').unwrap_or(data))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Parse one OpenAI Chat Completions chunk.
///
/// `finish_reason` is only recorded: with `stream_options.include_usage`
/// the usage chunk (`choices: []`) arrives after it, and the stream ends at
/// `data: [DONE]`.
fn parse_openai_chat(data: &str) -> Result<Vec<Signal>> {
    let chunk: OpenAiChatChunk = serde_json::from_str(data).map_err(|e| {
        VyaneError::with_source(ErrorKind::Protocol, "malformed OpenAI chat SSE JSON", e)
    })?;
    let mut signals = Vec::new();
    if let Some(usage) = chunk.usage {
        signals.push(Signal::UsageTotal(wire::openai_chat::usage_from_response(
            usage,
        )));
    }
    for choice in chunk.choices {
        if let Some(delta) = choice.delta {
            if let Some(content) = delta.content {
                let text = content_text(content);
                if !text.is_empty() {
                    signals.push(Signal::Event(StreamEvent::Delta(text)));
                }
            }
            if let Some(reasoning) = delta.reasoning_content.or(delta.reasoning)
                && !reasoning.is_empty()
            {
                signals.push(Signal::Event(StreamEvent::ReasoningDelta(reasoning)));
            }
        }
        if let Some(reason) = choice.finish_reason {
            signals.push(Signal::FinishReason(reason));
        }
    }
    Ok(signals)
}

/// Parse one Anthropic Messages event.
///
/// Input-side usage is only reported in `message_start`; `message_delta`
/// carries the stop reason and cumulative usage; `message_stop` ends the
/// stream.
fn parse_anthropic(data: &str) -> Result<Vec<Signal>> {
    let value: Value = serde_json::from_str(data).map_err(|e| {
        VyaneError::with_source(ErrorKind::Protocol, "malformed Anthropic SSE JSON", e)
    })?;
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match kind {
        "message_start" => {
            let usage = value
                .get("message")
                .and_then(|message| message.get("usage"));
            Ok(anthropic_usage(usage)?.into_iter().collect())
        }
        "content_block_delta" => Ok(parse_anthropic_delta(value)),
        "message_delta" => parse_anthropic_message_delta(&value),
        "message_stop" => Ok(vec![Signal::End]),
        "error" => Err(VyaneError::new(
            ErrorKind::Protocol,
            "Anthropic stream returned an error event",
        )),
        _ => Ok(Vec::new()),
    }
}

fn parse_anthropic_delta(value: Value) -> Vec<Signal> {
    let delta = value.get("delta").cloned().unwrap_or(Value::Null);
    let kind = delta
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let text = delta
        .get("text")
        .or_else(|| delta.get("thinking"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if text.is_empty() {
        return Vec::new();
    }
    let event = match kind {
        "thinking_delta" => StreamEvent::ReasoningDelta(text),
        _ => StreamEvent::Delta(text),
    };
    vec![Signal::Event(event)]
}

fn parse_anthropic_message_delta(value: &Value) -> Result<Vec<Signal>> {
    let mut signals: Vec<Signal> = anthropic_usage(value.get("usage"))?.into_iter().collect();
    if let Some(reason) = value
        .get("delta")
        .and_then(|delta| delta.get("stop_reason"))
        .and_then(Value::as_str)
    {
        signals.push(Signal::FinishReason(reason.to_owned()));
    }
    Ok(signals)
}

/// Decode an Anthropic `usage` object. A malformed one is a protocol error,
/// not silently dropped.
fn anthropic_usage(usage: Option<&Value>) -> Result<Option<Signal>> {
    let Some(usage) = usage.filter(|usage| !usage.is_null()) else {
        return Ok(None);
    };
    let usage = wire::anthropic::UsageResponse::deserialize(usage).map_err(|e| {
        VyaneError::with_source(ErrorKind::Protocol, "malformed Anthropic usage JSON", e)
    })?;
    Ok(Some(Signal::AnthropicUsage(usage)))
}

/// Parse one OpenAI Responses SSE frame.
///
/// Responses streaming uses named events (`event: <type>` + `data: {...}`),
/// but the API duplicates the event name into the payload's own `"type"`
/// field, so the `event:` line itself never needs parsing — [`collect_data_lines`]
/// already keeps only the `data:` lines, and this function reads `type` out of
/// that JSON like `parse_anthropic` does.
///
/// Unrecognized event types (`response.created`, `response.in_progress`,
/// `response.output_item.*`, `response.content_part.*`, …) are ignored,
/// mirroring how `parse_anthropic`'s catch-all arm treats unknown types: they
/// are half of a long, evolving event vocabulary that is not needed to
/// produce `StreamEvent`s.
fn parse_openai_responses(data: &str) -> Result<Vec<Signal>> {
    let value: Value = serde_json::from_str(data).map_err(|e| {
        VyaneError::with_source(
            ErrorKind::Protocol,
            "malformed OpenAI Responses SSE JSON",
            e,
        )
    })?;
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match kind {
        "response.output_text.delta" => Ok(responses_text_delta(&value, StreamEvent::Delta)),
        "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
            Ok(responses_text_delta(&value, StreamEvent::ReasoningDelta))
        }
        "response.completed" => responses_terminal(&value, None),
        "response.incomplete" => responses_terminal(&value, responses_incomplete_reason(&value)),
        "response.failed" | "error" => Err(VyaneError::new(
            ErrorKind::Protocol,
            "OpenAI Responses stream returned a failure event",
        )),
        _ => Ok(Vec::new()),
    }
}

/// Extract the `delta` string field shared by every `response.*.delta` event
/// and wrap it in the given `StreamEvent` constructor. An empty/missing delta
/// yields no event (nothing to emit).
fn responses_text_delta(value: &Value, make: impl Fn(String) -> StreamEvent) -> Vec<Signal> {
    let text = value
        .get("delta")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if text.is_empty() {
        Vec::new()
    } else {
        vec![Signal::Event(make(text.to_string()))]
    }
}

/// `response.completed` and `response.incomplete` both end the stream and
/// carry the full `Response` object under `"response"`, including `usage`
/// when reported.
///
/// A malformed `usage` payload is a protocol error (propagated via `?`), not
/// silently dropped — mirroring the Anthropic usage handling.
fn responses_terminal(value: &Value, finish_reason: Option<String>) -> Result<Vec<Signal>> {
    let mut signals = Vec::new();
    let usage = value
        .get("response")
        .and_then(|response| response.get("usage"))
        .filter(|usage| !usage.is_null());
    if let Some(usage) = usage {
        let usage = wire::openai_responses::UsageResponse::deserialize(usage).map_err(|e| {
            VyaneError::with_source(ErrorKind::Protocol, "malformed Responses usage JSON", e)
        })?;
        signals.push(Signal::UsageTotal(
            wire::openai_responses::usage_from_response(usage),
        ));
    }
    if let Some(reason) = finish_reason {
        signals.push(Signal::FinishReason(reason));
    }
    signals.push(Signal::End);
    Ok(signals)
}

/// `response.incomplete` carries `response.incomplete_details.reason`,
/// mirroring the non-streaming `ChatOutcome::finish_reason` mapping in
/// `wire::openai_responses::TryFrom<Response>`.
fn responses_incomplete_reason(value: &Value) -> Option<String> {
    value
        .get("response")
        .and_then(|response| response.get("incomplete_details"))
        .and_then(|details| details.get("reason"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn stream_transport_error(error: reqwest::Error) -> VyaneError {
    let kind = reqwest_error_kind(&error);
    VyaneError::with_source(kind, "stream transport error", error)
}

fn content_text(content: OpenAiContent) -> String {
    match content {
        OpenAiContent::Text(text) => text,
        OpenAiContent::Parts(parts) => parts
            .into_iter()
            .filter_map(|part| part.text)
            .collect::<Vec<_>>()
            .join(""),
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiChatChunk {
    #[serde(default)]
    choices: Vec<OpenAiChoice>,
    usage: Option<wire::openai_chat::UsageResponse>,
}

#[derive(Debug, Deserialize)]
struct OpenAiChoice {
    delta: Option<OpenAiDelta>,
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenAiDelta {
    content: Option<OpenAiContent>,
    reasoning_content: Option<String>,
    reasoning: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum OpenAiContent {
    Text(String),
    Parts(Vec<OpenAiContentPart>),
}

#[derive(Debug, Deserialize)]
struct OpenAiContentPart {
    text: Option<String>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Mirror of the kernel's stream loop (`vyane-kernel` dispatch): consume
    /// events, sum usage, and stop at the first `Done`.
    fn consume_like_kernel(
        events: impl IntoIterator<Item = Result<StreamEvent>>,
    ) -> (String, u64, u64) {
        let mut text = String::new();
        let (mut input, mut output) = (0, 0);
        for event in events {
            match event.unwrap() {
                StreamEvent::Delta(d) => text.push_str(&d),
                StreamEvent::Usage(u) => {
                    input += u.input_tokens;
                    output += u.output_tokens;
                }
                StreamEvent::Done { .. } => break,
                StreamEvent::ReasoningDelta(_) => {}
            }
        }
        (text, input, output)
    }

    /// Decode a whole recorded stream, delivered in `chunk_size` pieces,
    /// then EOF.
    fn decode_in_chunks(
        bytes: &[u8],
        chunk_size: usize,
        protocol: StreamProtocol,
    ) -> Vec<Result<StreamEvent>> {
        let mut decoder = SseDecoder::default();
        let mut events = Vec::new();
        for chunk in bytes.chunks(chunk_size) {
            events.extend(decoder.push(chunk, protocol));
        }
        events.extend(decoder.finish(protocol));
        events
    }

    /// Decode a fixture whole and in several chunkings; every chunking must
    /// produce the same events, ending in exactly one `Done` preceded by at
    /// most one `Usage`.
    fn decode_fixture(bytes: &[u8], protocol: StreamProtocol) -> Vec<StreamEvent> {
        let whole: Vec<StreamEvent> = decode_in_chunks(bytes, bytes.len(), protocol)
            .into_iter()
            .map(Result::unwrap)
            .collect();
        for chunk_size in [1, 2, 3, 7, 64] {
            let chunked: Vec<StreamEvent> = decode_in_chunks(bytes, chunk_size, protocol)
                .into_iter()
                .map(Result::unwrap)
                .collect();
            assert_eq!(
                format!("{chunked:?}"),
                format!("{whole:?}"),
                "chunk size {chunk_size}"
            );
        }
        let dones = whole
            .iter()
            .filter(|event| matches!(event, StreamEvent::Done { .. }))
            .count();
        assert_eq!(dones, 1, "{whole:?}");
        assert!(matches!(whole.last(), Some(StreamEvent::Done { .. })));
        let usages: Vec<usize> = whole
            .iter()
            .enumerate()
            .filter(|(_, event)| matches!(event, StreamEvent::Usage(_)))
            .map(|(index, _)| index)
            .collect();
        assert!(usages.len() <= 1, "{whole:?}");
        if let Some(index) = usages.first() {
            assert_eq!(*index, whole.len() - 2, "usage comes right before Done");
        }
        whole
    }

    #[test]
    fn openai_split_frames_are_reassembled() {
        let mut decoder = SseDecoder::default();
        let mut events = decoder.push(
            br#"data: {"choices":[{"delta":{"content":"hel"},"finish_reason":null}]}"#,
            StreamProtocol::OpenAiChat,
        );
        assert!(events.is_empty());
        events.extend(decoder.push(
            br#"

data: {"choices":[{"delta":{"content":"lo"},"finish_reason":"stop"}]}

data: [DONE]

"#,
            StreamProtocol::OpenAiChat,
        ));
        let events: Vec<_> = events.into_iter().map(Result::unwrap).collect();
        assert_eq!(events.len(), 3);
        assert!(matches!(&events[0], StreamEvent::Delta(text) if text == "hel"));
        assert!(matches!(&events[1], StreamEvent::Delta(text) if text == "lo"));
        assert!(matches!(
            &events[2],
            StreamEvent::Done {
                finish_reason: Some(reason)
            } if reason == "stop"
        ));
    }

    #[test]
    fn malformed_openai_json_is_protocol_error() {
        let mut decoder = SseDecoder::default();
        let events = decoder.push(b"data: {nope}\n\n", StreamProtocol::OpenAiChat);
        let error = events.into_iter().next().unwrap().unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
    }

    #[test]
    fn responses_split_frames_are_reassembled() {
        let mut decoder = SseDecoder::default();
        // First chunk ends mid-frame (no blank-line terminator yet).
        let mut events = decoder.push(
            br#"data: {"type":"response.output_text.delta","delta":"hel"}"#,
            StreamProtocol::OpenAiResponses,
        );
        assert!(events.is_empty());
        // Second chunk completes the first frame and adds two more, including
        // a `response.completed` carrying usage.
        events.extend(decoder.push(
            br#"

data: {"type":"response.output_text.delta","delta":"lo"}

data: {"type":"response.completed","response":{"usage":{"input_tokens":3,"output_tokens":2}}}

"#,
            StreamProtocol::OpenAiResponses,
        ));
        let events: Vec<_> = events.into_iter().map(Result::unwrap).collect();
        assert_eq!(events.len(), 4);
        assert!(matches!(&events[0], StreamEvent::Delta(text) if text == "hel"));
        assert!(matches!(&events[1], StreamEvent::Delta(text) if text == "lo"));
        assert!(matches!(
            &events[2],
            StreamEvent::Usage(Usage {
                input_tokens: 3,
                output_tokens: 2,
                ..
            })
        ));
        assert!(matches!(
            &events[3],
            StreamEvent::Done {
                finish_reason: None
            }
        ));
    }

    #[test]
    fn responses_unknown_event_type_is_ignored() {
        let mut decoder = SseDecoder::default();
        let events = decoder.push(
            br#"data: {"type":"response.in_progress"}

"#,
            StreamProtocol::OpenAiResponses,
        );
        assert!(events.is_empty());
    }

    #[test]
    fn malformed_responses_json_is_protocol_error() {
        let mut decoder = SseDecoder::default();
        let events = decoder.push(b"data: {nope}\n\n", StreamProtocol::OpenAiResponses);
        let error = events.into_iter().next().unwrap().unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
    }

    #[test]
    fn responses_completed_with_malformed_usage_is_protocol_error() {
        // The envelope and event `type` are well-formed; only the nested
        // `usage` field fails to deserialize (wrong type for `input_tokens`).
        // This must surface as a `Protocol` error, not be silently dropped.
        let mut decoder = SseDecoder::default();
        let events = decoder.push(
            br#"data: {"type":"response.completed","response":{"usage":{"input_tokens":"nope"}}}

"#,
            StreamProtocol::OpenAiResponses,
        );
        let error = events.into_iter().next().unwrap().unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
    }

    #[test]
    fn anthropic_delta_and_usage_normalize() {
        let mut decoder = SseDecoder::default();
        let events = decoder.push(
            br#"data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}

data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":4,"output_tokens":2,"cache_read_input_tokens":30,"cache_creation_input_tokens":5}}

data: {"type":"message_stop"}

"#,
            StreamProtocol::Anthropic,
        );
        let events: Vec<_> = events.into_iter().map(Result::unwrap).collect();
        assert!(matches!(&events[0], StreamEvent::Delta(text) if text == "hi"));
        assert!(matches!(
            &events[1],
            StreamEvent::Usage(Usage {
                input_tokens: 39,
                output_tokens: 2,
                reasoning_tokens: None,
                cached_input_tokens: Some(30),
            })
        ));
        assert!(matches!(
            &events[2],
            StreamEvent::Done {
                finish_reason: Some(reason)
            } if reason == "end_turn"
        ));
    }

    /// A CRLF-delimited frame followed by an LF-delimited one is split at the
    /// earliest blank line, so neither frame swallows the other.
    #[test]
    fn mixed_crlf_and_lf_delimiters_split_frames_in_order() {
        let mut decoder = SseDecoder::default();
        let events = decoder.push(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\r\n\r\ndata: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n",
            StreamProtocol::OpenAiChat,
        );
        let events: Vec<_> = events.into_iter().collect();
        assert!(events.iter().all(Result::is_ok), "{events:?}");
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], Ok(StreamEvent::Delta(text)) if text == "a"));
        assert!(matches!(&events[1], Ok(StreamEvent::Delta(text)) if text == "b"));
    }

    /// A peer that never sends a frame delimiter cannot grow the buffer past
    /// the response size cap: the decoder reports a protocol error instead.
    #[test]
    fn unterminated_frame_over_size_cap_is_protocol_error() {
        let mut decoder = SseDecoder::default();
        let chunk = vec![b'a'; 1024 * 1024];
        let mut error = None;
        for _ in 0..9 {
            let events = decoder.push(&chunk, StreamProtocol::OpenAiChat);
            if let Some(Err(e)) = events.into_iter().find(Result::is_err) {
                error = Some(e);
                break;
            }
        }
        let error = error.unwrap_or_else(|| {
            panic!(
                "a single SSE frame larger than the response cap must be rejected; buffered {} bytes",
                decoder.buffer.len()
            )
        });
        assert_eq!(error.kind, ErrorKind::Protocol);
        assert!(decoder.buffer.is_empty(), "rejected bytes are released");
        // The decoder stays failed: later bytes and EOF add nothing.
        assert!(decoder.push(b"\n\n", StreamProtocol::OpenAiChat).is_empty());
        assert!(decoder.finish(StreamProtocol::OpenAiChat).is_empty());
    }

    /// A complete frame over the cap is rejected even when its delimiter
    /// arrives in the same chunk.
    #[test]
    fn delimited_frame_over_size_cap_is_protocol_error() {
        let mut decoder = SseDecoder::default();
        let mut chunk = vec![b'a'; MAX_JSON_RESPONSE_BYTES + 1];
        chunk.extend_from_slice(b"\n\n");
        let events: Vec<_> = decoder
            .push(&chunk, StreamProtocol::OpenAiChat)
            .into_iter()
            .collect();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].as_ref().unwrap_err().kind, ErrorKind::Protocol);
    }

    /// A blank-line delimiter split across chunks (`\n` | `\r\n` and
    /// `\r\n` | `\r` | `\n`) is still recognized once the rest arrives.
    #[test]
    fn delimiter_split_across_chunks_is_recognized() {
        let mut decoder = SseDecoder::default();
        let protocol = StreamProtocol::OpenAiChat;
        let mut events = decoder.push(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n",
            protocol,
        );
        assert!(events.is_empty());
        events.extend(decoder.push(b"\r\n", protocol));
        assert_eq!(events.len(), 1);
        events.extend(decoder.push(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\r\n",
            protocol,
        ));
        events.extend(decoder.push(b"\r", protocol));
        assert_eq!(events.len(), 1);
        events.extend(decoder.push(b"\n", protocol));
        let events: Vec<_> = events.into_iter().map(Result::unwrap).collect();
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], StreamEvent::Delta(text) if text == "a"));
        assert!(matches!(&events[1], StreamEvent::Delta(text) if text == "b"));
        assert!(decoder.buffer.is_empty());
    }

    /// OpenAI Chat with `stream_options.include_usage` (which the client
    /// always requests) sends the `finish_reason` chunk first and the usage
    /// chunk (`choices: []`) after it. A consumer that stops at `Done` must
    /// still see that usage.
    #[test]
    fn openai_chat_usage_after_finish_reason_is_reported_before_done() {
        let mut decoder = SseDecoder::default();
        let mut events = decoder.push(
            br#"data: {"choices":[{"delta":{"content":"hi"},"finish_reason":null}]}

data: {"choices":[{"delta":{},"finish_reason":"stop"}]}

data: {"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":3,"total_tokens":14}}

data: [DONE]

"#,
            StreamProtocol::OpenAiChat,
        );
        events.extend(decoder.finish(StreamProtocol::OpenAiChat));
        let (text, input, output) = consume_like_kernel(events);
        assert_eq!(text, "hi");
        assert_eq!(
            (input, output),
            (11, 3),
            "usage trailer after finish_reason must be observed before Done"
        );
    }

    /// Anthropic Messages streams report input tokens in `message_start`;
    /// `message_delta` carries the cumulative output tokens. Both reach the
    /// consumer, with output counted once.
    #[test]
    fn anthropic_message_start_input_tokens_are_counted() {
        let mut decoder = SseDecoder::default();
        let mut events = decoder.push(
            br#"event: message_start
data: {"type":"message_start","message":{"id":"m","type":"message","role":"assistant","content":[],"model":"x","usage":{"input_tokens":25,"output_tokens":1}}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}

event: message_stop
data: {"type":"message_stop"}

"#,
            StreamProtocol::Anthropic,
        );
        events.extend(decoder.finish(StreamProtocol::Anthropic));
        let (text, input, output) = consume_like_kernel(events);
        assert_eq!(text, "ok");
        assert_eq!(output, 2);
        assert_eq!(input, 25, "message_start input tokens must be reported");
    }

    /// OpenAI Chat Completions stream with `include_usage`, in the shape the
    /// API documents: a role chunk, content chunks, a `finish_reason` chunk,
    /// a `choices: []` usage chunk, then `[DONE]`.
    #[test]
    fn openai_chat_documented_stream_fixture() {
        let fixture = concat!(
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"model-x\",\"system_fingerprint\":\"fp_1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\",\"refusal\":null},\"logprobs\":null,\"finish_reason\":null}],\"usage\":null}\n\n",
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"model-x\",\"system_fingerprint\":\"fp_1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"},\"logprobs\":null,\"finish_reason\":null}],\"usage\":null}\n\n",
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"model-x\",\"system_fingerprint\":\"fp_1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"logprobs\":null,\"finish_reason\":null}],\"usage\":null}\n\n",
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"model-x\",\"system_fingerprint\":\"fp_1\",\"choices\":[{\"index\":0,\"delta\":{},\"logprobs\":null,\"finish_reason\":\"stop\"}],\"usage\":null}\n\n",
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"model-x\",\"system_fingerprint\":\"fp_1\",\"choices\":[],\"usage\":{\"prompt_tokens\":19,\"completion_tokens\":10,\"total_tokens\":29,\"prompt_tokens_details\":{\"cached_tokens\":4,\"audio_tokens\":0},\"completion_tokens_details\":{\"reasoning_tokens\":2,\"audio_tokens\":0,\"accepted_prediction_tokens\":0,\"rejected_prediction_tokens\":0}}}\n\n",
            "data: [DONE]\n\n",
        );
        let events = decode_fixture(fixture.as_bytes(), StreamProtocol::OpenAiChat);
        assert_eq!(events.len(), 4, "{events:?}");
        assert!(matches!(&events[0], StreamEvent::Delta(text) if text == "Hello"));
        assert!(matches!(&events[1], StreamEvent::Delta(text) if text == " world"));
        assert!(matches!(
            &events[2],
            StreamEvent::Usage(usage) if *usage == Usage {
                input_tokens: 19,
                output_tokens: 10,
                reasoning_tokens: Some(2),
                cached_input_tokens: Some(4),
            }
        ));
        assert!(matches!(
            &events[3],
            StreamEvent::Done { finish_reason: Some(reason) } if reason == "stop"
        ));
    }

    /// Anthropic Messages stream in the documented event sequence, CRLF
    /// line endings included: `message_start` (input-side usage),
    /// thinking and text blocks with `ping` and `signature_delta` noise,
    /// `message_delta` (stop reason, cumulative output usage), then
    /// `message_stop`.
    #[test]
    fn anthropic_documented_stream_fixture() {
        let fixture = concat!(
            "event: message_start\r\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"model-x\",\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":25,\"cache_creation_input_tokens\":3,\"cache_read_input_tokens\":10,\"output_tokens\":1}}}\r\n\r\n",
            "event: content_block_start\r\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"\"}}\r\n\r\n",
            "event: ping\r\n",
            "data: {\"type\":\"ping\"}\r\n\r\n",
            "event: content_block_delta\r\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"Plan.\"}}\r\n\r\n",
            "event: content_block_delta\r\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"c2ln\"}}\r\n\r\n",
            "event: content_block_stop\r\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\r\n\r\n",
            "event: content_block_start\r\n",
            "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\r\n\r\n",
            "event: content_block_delta\r\n",
            "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\r\n\r\n",
            "event: content_block_delta\r\n",
            "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"!\"}}\r\n\r\n",
            "event: content_block_stop\r\n",
            "data: {\"type\":\"content_block_stop\",\"index\":1}\r\n\r\n",
            "event: message_delta\r\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":15}}\r\n\r\n",
            "event: message_stop\r\n",
            "data: {\"type\":\"message_stop\"}\r\n\r\n",
        );
        let events = decode_fixture(fixture.as_bytes(), StreamProtocol::Anthropic);
        assert_eq!(events.len(), 5, "{events:?}");
        assert!(matches!(&events[0], StreamEvent::ReasoningDelta(text) if text == "Plan."));
        assert!(matches!(&events[1], StreamEvent::Delta(text) if text == "Hello"));
        assert!(matches!(&events[2], StreamEvent::Delta(text) if text == "!"));
        // Same accounting as the non-streaming response: input includes cache
        // creation and cache reads; output is the cumulative total, once.
        assert!(matches!(
            &events[3],
            StreamEvent::Usage(usage) if *usage == Usage {
                input_tokens: 38,
                output_tokens: 15,
                reasoning_tokens: None,
                cached_input_tokens: Some(10),
            }
        ));
        assert!(matches!(
            &events[4],
            StreamEvent::Done { finish_reason: Some(reason) } if reason == "end_turn"
        ));
    }

    /// OpenAI Responses stream in the documented named-event sequence,
    /// ending in `response.completed` with the response's usage.
    #[test]
    fn openai_responses_documented_stream_fixture() {
        let fixture = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"status\":\"in_progress\",\"output\":[],\"usage\":null}}\n\n",
            "event: response.in_progress\n",
            "data: {\"type\":\"response.in_progress\",\"sequence_number\":1,\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"status\":\"in_progress\",\"output\":[],\"usage\":null}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\",\"summary\":[]}}\n\n",
            "event: response.reasoning_summary_text.delta\n",
            "data: {\"type\":\"response.reasoning_summary_text.delta\",\"sequence_number\":3,\"item_id\":\"rs_1\",\"output_index\":0,\"summary_index\":0,\"delta\":\"Think.\"}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"sequence_number\":4,\"output_index\":1,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n",
            "event: response.content_part.added\n",
            "data: {\"type\":\"response.content_part.added\",\"sequence_number\":5,\"item_id\":\"msg_1\",\"output_index\":1,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\",\"annotations\":[]}}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"sequence_number\":6,\"item_id\":\"msg_1\",\"output_index\":1,\"content_index\":0,\"delta\":\"Hi\"}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"sequence_number\":7,\"item_id\":\"msg_1\",\"output_index\":1,\"content_index\":0,\"delta\":\" there\"}\n\n",
            "event: response.output_text.done\n",
            "data: {\"type\":\"response.output_text.done\",\"sequence_number\":8,\"item_id\":\"msg_1\",\"output_index\":1,\"content_index\":0,\"text\":\"Hi there\"}\n\n",
            "event: response.content_part.done\n",
            "data: {\"type\":\"response.content_part.done\",\"sequence_number\":9,\"item_id\":\"msg_1\",\"output_index\":1,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"Hi there\",\"annotations\":[]}}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"sequence_number\":10,\"output_index\":1,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Hi there\",\"annotations\":[]}]}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"sequence_number\":11,\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":12,\"input_tokens_details\":{\"cached_tokens\":4},\"output_tokens\":7,\"output_tokens_details\":{\"reasoning_tokens\":2},\"total_tokens\":19}}}\n\n",
        );
        let events = decode_fixture(fixture.as_bytes(), StreamProtocol::OpenAiResponses);
        assert_eq!(events.len(), 5, "{events:?}");
        assert!(matches!(&events[0], StreamEvent::ReasoningDelta(text) if text == "Think."));
        assert!(matches!(&events[1], StreamEvent::Delta(text) if text == "Hi"));
        assert!(matches!(&events[2], StreamEvent::Delta(text) if text == " there"));
        assert!(matches!(
            &events[3],
            StreamEvent::Usage(usage) if *usage == Usage {
                input_tokens: 12,
                output_tokens: 7,
                reasoning_tokens: Some(2),
                cached_input_tokens: Some(4),
            }
        ));
        assert!(matches!(
            &events[4],
            StreamEvent::Done {
                finish_reason: None
            }
        ));
    }

    /// Some OpenAI-compatible endpoints repeat the running usage total on
    /// several chunks. Each report replaces the previous one, so the stream
    /// reports the last total once instead of their sum.
    #[test]
    fn openai_chat_repeated_usage_totals_are_not_summed() {
        let events = decode_fixture(
            concat!(
                "data: {\"choices\":[{\"delta\":{\"content\":\"a\"},\"finish_reason\":null}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":1}}\n\n",
                "data: {\"choices\":[{\"delta\":{\"content\":\"b\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
                "data: [DONE]\n\n",
            )
            .as_bytes(),
            StreamProtocol::OpenAiChat,
        );
        assert_eq!(
            consume_like_kernel(events.into_iter().map(Ok)),
            ("ab".into(), 5, 2)
        );
    }

    /// Nothing follows `Done`: frames after the end-of-stream marker are
    /// ignored and EOF adds no second `Done`.
    #[test]
    fn frames_after_end_of_stream_are_ignored() {
        let mut decoder = SseDecoder::default();
        let protocol = StreamProtocol::Anthropic;
        let mut events = decoder.push(
            concat!(
                "data: {\"type\":\"message_stop\"}\n\n",
                "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"late\"}}\n\n",
            )
            .as_bytes(),
            protocol,
        );
        events.extend(decoder.push(b"data: {\"type\":\"message_stop\"}\n\n", protocol));
        events.extend(decoder.finish(protocol));
        let events: Vec<_> = events.into_iter().map(Result::unwrap).collect();
        assert_eq!(events.len(), 1, "{events:?}");
        assert!(matches!(
            &events[0],
            StreamEvent::Done {
                finish_reason: None
            }
        ));
        assert!(decoder.buffer.is_empty());
    }

    /// A decode error is final: later frames and EOF add nothing, so the
    /// error is never followed by a `Done`.
    #[test]
    fn decode_error_ends_the_stream() {
        let mut decoder = SseDecoder::default();
        let protocol = StreamProtocol::OpenAiChat;
        let mut events = decoder.push(
            b"data: {nope}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n",
            protocol,
        );
        events.extend(decoder.push(b"data: [DONE]\n\n", protocol));
        events.extend(decoder.finish(protocol));
        assert_eq!(events.len(), 1);
        assert_eq!(
            events.pop_front().unwrap().unwrap_err().kind,
            ErrorKind::Protocol
        );
    }

    /// A malformed `message_start` usage object is a protocol error, as it
    /// is on `message_delta`.
    #[test]
    fn anthropic_malformed_message_start_usage_is_protocol_error() {
        let mut decoder = SseDecoder::default();
        let events = decoder.push(
            br#"data: {"type":"message_start","message":{"usage":{"input_tokens":"many"}}}

"#,
            StreamProtocol::Anthropic,
        );
        let error = events.into_iter().next().unwrap().unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
    }

    /// A stream that closes before its end-of-stream event (`message_stop`,
    /// `response.completed`) is truncated output, not a successful run.
    #[test]
    fn eof_before_end_of_stream_event_is_transport_error() {
        for (protocol, frame) in [
            (
                StreamProtocol::Anthropic,
                &br#"data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"half"}}

"#[..],
            ),
            (
                StreamProtocol::OpenAiResponses,
                &br#"data: {"type":"response.output_text.delta","delta":"half"}

"#[..],
            ),
        ] {
            let mut decoder = SseDecoder::default();
            let mut events = decoder.push(frame, protocol);
            events.extend(decoder.finish(protocol));
            let events: Vec<_> = events.into_iter().collect();
            assert_eq!(events.len(), 2, "{protocol:?}: {events:?}");
            assert!(matches!(&events[0], Ok(StreamEvent::Delta(text)) if text == "half"));
            assert_eq!(
                events[1].as_ref().unwrap_err().kind,
                ErrorKind::Transport,
                "{protocol:?}"
            );
        }
    }

    /// The Anthropic stop reason alone does not end the stream; EOF between
    /// `message_delta` and `message_stop` is still truncation.
    #[test]
    fn anthropic_eof_after_stop_reason_without_message_stop_is_transport_error() {
        let events = decode_in_chunks(
            concat!(
                "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
                "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
            )
            .as_bytes(),
            usize::MAX,
            StreamProtocol::Anthropic,
        );
        assert_eq!(events.len(), 2, "{events:?}");
        assert_eq!(events[1].as_ref().unwrap_err().kind, ErrorKind::Transport);
    }

    /// OpenAI Chat EOF: accepted without `[DONE]` once a finish reason was
    /// reported (usage seen so far is still delivered), an error before it.
    #[test]
    fn openai_chat_eof_needs_a_finish_reason() {
        let events = decode_in_chunks(
            concat!(
                "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"length\"}]}\n\n",
                "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":1}}\n\n",
            )
            .as_bytes(),
            usize::MAX,
            StreamProtocol::OpenAiChat,
        );
        assert_eq!(
            consume_like_kernel(events.iter().map(|event| match event {
                Ok(event) => Ok(event.clone()),
                Err(error) => panic!("{error}"),
            })),
            ("hi".into(), 4, 1)
        );
        assert!(matches!(
            events.last(),
            Some(Ok(StreamEvent::Done { finish_reason: Some(reason) })) if reason == "length"
        ));

        let events = decode_in_chunks(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n",
            usize::MAX,
            StreamProtocol::OpenAiChat,
        );
        assert_eq!(events.len(), 2, "{events:?}");
        assert_eq!(events[1].as_ref().unwrap_err().kind, ErrorKind::Transport);
    }

    /// An empty body (no events at all) is not a successful empty answer.
    #[test]
    fn empty_stream_is_transport_error() {
        for protocol in [
            StreamProtocol::OpenAiChat,
            StreamProtocol::Anthropic,
            StreamProtocol::OpenAiResponses,
        ] {
            let events = decode_in_chunks(b"", 1, protocol);
            assert_eq!(events.len(), 1, "{protocol:?}");
            assert_eq!(events[0].as_ref().unwrap_err().kind, ErrorKind::Transport);
        }
    }
}
