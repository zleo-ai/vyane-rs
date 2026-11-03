use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{BoxStream, Stream, StreamExt};
use reqwest::Response;
use serde::Deserialize;
use serde_json::Value;
use vyane_core::{ErrorKind, Result, StreamEvent, VyaneError};

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
#[derive(Default)]
pub(crate) struct SseDecoder {
    buffer: Vec<u8>,
    /// Offset into `buffer` where the next boundary scan resumes, so each
    /// pushed byte is scanned once instead of rescanning the whole buffer.
    scan_from: usize,
    done_emitted: bool,
    failed: bool,
}

impl SseDecoder {
    pub(crate) fn push(
        &mut self,
        chunk: &[u8],
        protocol: StreamProtocol,
    ) -> VecDeque<Result<StreamEvent>> {
        let mut out = VecDeque::new();
        if self.failed {
            return out;
        }
        let mut buffer = std::mem::take(&mut self.buffer);
        buffer.extend_from_slice(chunk);

        let mut start = 0;
        let mut scan = self.scan_from;
        while !self.failed {
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
        if self.failed {
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

    pub(crate) fn finish(&mut self, protocol: StreamProtocol) -> VecDeque<Result<StreamEvent>> {
        let mut out = VecDeque::new();
        if self.failed {
            return out;
        }
        let frame = std::mem::take(&mut self.buffer);
        self.scan_from = 0;
        if !frame.is_empty() {
            self.decode_frame(&frame, protocol, &mut out);
        }
        if !self.done_emitted && !out.iter().any(Result::is_err) {
            self.push_done(None, &mut out);
        }
        out
    }

    /// Record a fatal decode error. Nothing is decoded after it, and the
    /// buffered bytes are released.
    fn fail(&mut self, error: VyaneError, out: &mut VecDeque<Result<StreamEvent>>) {
        self.failed = true;
        self.buffer = Vec::new();
        self.scan_from = 0;
        out.push_back(Err(error));
    }

    fn decode_frame(
        &mut self,
        frame: &[u8],
        protocol: StreamProtocol,
        out: &mut VecDeque<Result<StreamEvent>>,
    ) {
        let Ok(text) = std::str::from_utf8(frame) else {
            out.push_back(Err(VyaneError::new(
                ErrorKind::Protocol,
                "SSE frame was not valid UTF-8",
            )));
            return;
        };
        let data = collect_data_lines(text);
        if data.trim().is_empty() {
            return;
        }
        if data.trim() == "[DONE]" {
            self.push_done(None, out);
            return;
        }

        let parsed = match protocol {
            StreamProtocol::OpenAiChat => parse_openai_chat(&data),
            StreamProtocol::Anthropic => parse_anthropic(&data),
            StreamProtocol::OpenAiResponses => parse_openai_responses(&data),
        };
        match parsed {
            Ok(events) => {
                for event in events {
                    match event {
                        StreamEvent::Done { finish_reason } => self.push_done(finish_reason, out),
                        other => out.push_back(Ok(other)),
                    }
                }
            }
            Err(error) => out.push_back(Err(error)),
        }
    }

    fn push_done(
        &mut self,
        finish_reason: Option<String>,
        out: &mut VecDeque<Result<StreamEvent>>,
    ) {
        if !self.done_emitted {
            self.done_emitted = true;
            out.push_back(Ok(StreamEvent::Done { finish_reason }));
        }
    }
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
        if self.finished {
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
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
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

fn parse_openai_chat(data: &str) -> Result<Vec<StreamEvent>> {
    let chunk: OpenAiChatChunk = serde_json::from_str(data).map_err(|e| {
        VyaneError::with_source(ErrorKind::Protocol, "malformed OpenAI chat SSE JSON", e)
    })?;
    let mut events = Vec::new();
    if let Some(usage) = chunk.usage {
        events.push(StreamEvent::Usage(wire::openai_chat::usage_from_response(
            usage,
        )));
    }
    for choice in chunk.choices {
        if let Some(delta) = choice.delta {
            if let Some(content) = delta.content {
                let text = content_text(content);
                if !text.is_empty() {
                    events.push(StreamEvent::Delta(text));
                }
            }
            if let Some(reasoning) = delta.reasoning_content.or(delta.reasoning)
                && !reasoning.is_empty()
            {
                events.push(StreamEvent::ReasoningDelta(reasoning));
            }
        }
        if let Some(reason) = choice.finish_reason {
            events.push(StreamEvent::Done {
                finish_reason: Some(reason),
            });
        }
    }
    Ok(events)
}

fn parse_anthropic(data: &str) -> Result<Vec<StreamEvent>> {
    let value: Value = serde_json::from_str(data).map_err(|e| {
        VyaneError::with_source(ErrorKind::Protocol, "malformed Anthropic SSE JSON", e)
    })?;
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match kind {
        "content_block_delta" => parse_anthropic_delta(value),
        "message_delta" => parse_anthropic_message_delta(value),
        "message_stop" => Ok(vec![StreamEvent::Done {
            finish_reason: None,
        }]),
        "error" => Err(VyaneError::new(
            ErrorKind::Protocol,
            "Anthropic stream returned an error event",
        )),
        _ => Ok(Vec::new()),
    }
}

fn parse_anthropic_delta(value: Value) -> Result<Vec<StreamEvent>> {
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
        return Ok(Vec::new());
    }
    let event = match kind {
        "thinking_delta" => StreamEvent::ReasoningDelta(text),
        _ => StreamEvent::Delta(text),
    };
    Ok(vec![event])
}

fn parse_anthropic_message_delta(value: Value) -> Result<Vec<StreamEvent>> {
    let mut events = Vec::new();
    if let Some(usage) = value.get("usage").cloned() {
        let usage =
            serde_json::from_value::<wire::anthropic::UsageResponse>(usage).map_err(|e| {
                VyaneError::with_source(ErrorKind::Protocol, "malformed Anthropic usage JSON", e)
            })?;
        events.push(StreamEvent::Usage(wire::anthropic::usage_from_response(
            usage,
        )));
    }
    let finish_reason = value
        .get("delta")
        .and_then(|delta| delta.get("stop_reason"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    if finish_reason.is_some() {
        events.push(StreamEvent::Done { finish_reason });
    }
    Ok(events)
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
fn parse_openai_responses(data: &str) -> Result<Vec<StreamEvent>> {
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
        "response.completed" => responses_completed(value),
        "response.incomplete" => Ok(vec![StreamEvent::Done {
            finish_reason: responses_incomplete_reason(&value),
        }]),
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
fn responses_text_delta(value: &Value, make: impl Fn(String) -> StreamEvent) -> Vec<StreamEvent> {
    let text = value
        .get("delta")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if text.is_empty() {
        Vec::new()
    } else {
        vec![make(text.to_string())]
    }
}

/// `response.completed` carries the full `Response` object under `"response"`,
/// including `usage` when reported. Usage (if present) is emitted before the
/// terminal `Done` — same ordering as the Chat/Anthropic parsers.
///
/// A malformed `usage` payload is a protocol error (propagated via `?`), not
/// silently dropped — mirroring `parse_anthropic_message_delta`'s handling of
/// its own `usage` field.
fn responses_completed(value: Value) -> Result<Vec<StreamEvent>> {
    let mut events = Vec::new();
    let usage = value
        .get("response")
        .and_then(|response| response.get("usage"))
        .cloned();
    if let Some(usage) = usage {
        let usage = serde_json::from_value::<wire::openai_responses::UsageResponse>(usage)
            .map_err(|e| {
                VyaneError::with_source(ErrorKind::Protocol, "malformed Responses usage JSON", e)
            })?;
        events.push(StreamEvent::Usage(
            wire::openai_responses::usage_from_response(usage),
        ));
    }
    events.push(StreamEvent::Done {
        finish_reason: None,
    });
    Ok(events)
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
    use vyane_core::Usage;

    use super::*;

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
}

/// Red-team reproductions. Each test encodes the behavior the decoder
/// *should* have; they fail on the current implementation.
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod redteam_tests {
    use super::*;

    /// Mirror of the kernel's stream loop (`vyane-kernel` dispatch): consume
    /// events, sum usage, and stop at the first `Done`.
    fn kernel_view(events: impl IntoIterator<Item = Result<StreamEvent>>) -> (String, u64, u64) {
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

    /// OpenAI Chat with `stream_options.include_usage` (which the client
    /// always requests) sends the `finish_reason` chunk first and the usage
    /// chunk (`choices: []`) after it. The decoder emits `Done` on
    /// `finish_reason`, and the kernel breaks there, so usage is dropped.
    #[test]
    fn redteam_openai_chat_usage_trailer_survives_finish_reason() {
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
        let (text, input, output) = kernel_view(events);
        assert_eq!(text, "hi");
        assert_eq!(
            (input, output),
            (11, 3),
            "usage trailer after finish_reason must be observed before Done"
        );
    }

    /// Anthropic Messages streams report input tokens in `message_start`;
    /// `message_delta` carries only the (cumulative) output tokens. The
    /// decoder ignores `message_start`, so streamed input tokens are lost.
    #[test]
    fn redteam_anthropic_message_start_input_tokens_are_counted() {
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
        let (text, input, output) = kernel_view(events);
        assert_eq!(text, "ok");
        assert_eq!(output, 2);
        assert_eq!(input, 25, "message_start input tokens must be reported");
    }

    /// A stream that ends (clean EOF) before any terminal event is truncated
    /// output, but `finish` synthesizes a successful `Done`.
    #[test]
    fn redteam_eof_before_terminal_event_is_not_success() {
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
            assert!(
                events.iter().any(Result::is_err),
                "{protocol:?}: EOF without message_stop/response.completed must surface an error"
            );
        }
    }
}
