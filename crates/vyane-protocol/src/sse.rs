use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{BoxStream, Stream, StreamExt};
use reqwest::Response;
use serde::Deserialize;
use serde_json::Value;
use vyane_core::{ErrorKind, Result, StreamEvent, VyaneError};

use crate::http::reqwest_error_kind;
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

#[derive(Default)]
pub(crate) struct SseDecoder {
    buffer: Vec<u8>,
    done_emitted: bool,
}

impl SseDecoder {
    pub(crate) fn push(
        &mut self,
        chunk: &[u8],
        protocol: StreamProtocol,
    ) -> VecDeque<Result<StreamEvent>> {
        self.buffer.extend_from_slice(chunk);
        let mut out = VecDeque::new();

        while let Some(index) = frame_boundary(&self.buffer) {
            let frame = self.buffer.drain(..index).collect::<Vec<_>>();
            let delimiter_len = if self.buffer.starts_with(b"\r\n\r\n") {
                4
            } else {
                2
            };
            self.buffer.drain(..delimiter_len);
            self.decode_frame(&frame, protocol, &mut out);
        }

        out
    }

    pub(crate) fn finish(&mut self, protocol: StreamProtocol) -> VecDeque<Result<StreamEvent>> {
        let mut out = VecDeque::new();
        if !self.buffer.is_empty() {
            let frame = std::mem::take(&mut self.buffer);
            self.decode_frame(&frame, protocol, &mut out);
        }
        if !self.done_emitted && !out.iter().any(Result::is_err) {
            self.push_done(None, &mut out);
        }
        out
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

fn frame_boundary(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .or_else(|| buffer.windows(4).position(|window| window == b"\r\n\r\n"))
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
            if let Some(reasoning) = delta.reasoning_content.or(delta.reasoning) {
                if !reasoning.is_empty() {
                    events.push(StreamEvent::ReasoningDelta(reasoning));
                }
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
        "response.completed" => Ok(responses_completed(value)),
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
fn responses_completed(value: Value) -> Vec<StreamEvent> {
    let mut events = Vec::new();
    let usage = value
        .get("response")
        .and_then(|response| response.get("usage"))
        .cloned();
    if let Some(usage) = usage {
        if let Ok(usage) = serde_json::from_value::<wire::openai_responses::UsageResponse>(usage) {
            events.push(StreamEvent::Usage(
                wire::openai_responses::usage_from_response(usage),
            ));
        }
    }
    events.push(StreamEvent::Done {
        finish_reason: None,
    });
    events
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
    fn anthropic_delta_and_usage_normalize() {
        let mut decoder = SseDecoder::default();
        let events = decoder.push(
            br#"data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}

data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":4,"output_tokens":2}}

"#,
            StreamProtocol::Anthropic,
        );
        let events: Vec<_> = events.into_iter().map(Result::unwrap).collect();
        assert!(matches!(&events[0], StreamEvent::Delta(text) if text == "hi"));
        assert!(matches!(
            &events[1],
            StreamEvent::Usage(Usage {
                input_tokens: 4,
                output_tokens: 2,
                ..
            })
        ));
        assert!(matches!(
            &events[2],
            StreamEvent::Done {
                finish_reason: Some(reason)
            } if reason == "end_turn"
        ));
    }
}
