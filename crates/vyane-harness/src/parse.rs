//! Output parsing for each CLI's machine-readable format.
//!
//! * Claude Code: `--output-format json` emits a **single JSON object** (the
//!   final result envelope) carrying `result` (the answer text), `session_id`,
//!   `total_cost_usd`, `duration_ms`, and a `usage` sub-object.
//! * Codex CLI: `codex exec --json` emits **JSONL** (one event per line); the
//!   thread/session id and token usage are read from those events, while the
//!   final answer is taken from the `--output-last-message` file the harness
//!   points the CLI at.
//!
//! Only the final answer becomes [`vyane_core::HarnessOutcome::text`] — never
//! the event stream. `usage` is populated only when the CLI actually reports it.

use serde_json::Value;
use vyane_core::run::Usage;

/// Parsed pieces common to both harnesses.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct Parsed {
    pub text: String,
    pub native_session_id: Option<String>,
    pub usage: Option<Usage>,
}

fn as_u64(v: Option<&Value>) -> u64 {
    v.and_then(Value::as_u64).unwrap_or(0)
}

fn nonempty_string(v: Option<&Value>) -> Option<String> {
    match v.and_then(Value::as_str) {
        Some(s) if !s.is_empty() => Some(s.to_string()),
        _ => None,
    }
}

/// Parse Claude Code `--output-format json` output.
///
/// The document is a single JSON object. We read `result` as the answer,
/// `session_id` as the native id (for `--resume`), and assemble [`Usage`] from
/// the `usage` block. Claude reports cache-creation and cache-read input tokens
/// separately; `cached_input_tokens` captures the cache-read portion, while
/// `input_tokens` is the sum of direct + cache-creation + cache-read (so token
/// accounting isn't undercounted).
///
/// If the payload isn't the expected object (e.g. an error dump), `text` falls
/// back to the raw trimmed output so the caller still surfaces *something*, and
/// classification happens upstream from the exit code.
pub(crate) fn parse_claude_json(stdout: &str) -> Parsed {
    let trimmed = stdout.trim();
    let root: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(_) => {
            return Parsed {
                text: trimmed.to_string(),
                ..Default::default()
            };
        }
    };
    let obj = match root.as_object() {
        Some(o) => o,
        None => {
            return Parsed {
                text: trimmed.to_string(),
                ..Default::default()
            };
        }
    };

    let text = obj
        .get("result")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    let native_session_id = nonempty_string(obj.get("session_id"));

    let usage = obj.get("usage").and_then(Value::as_object).map(|u| {
        let direct = as_u64(u.get("input_tokens"));
        let cache_creation = as_u64(u.get("cache_creation_input_tokens"));
        let cache_read = as_u64(u.get("cache_read_input_tokens"));
        let output = as_u64(u.get("output_tokens"));
        Usage {
            input_tokens: direct + cache_creation + cache_read,
            output_tokens: output,
            reasoning_tokens: None,
            cached_input_tokens: if cache_read > 0 {
                Some(cache_read)
            } else {
                None
            },
        }
    });

    Parsed {
        text,
        native_session_id,
        usage,
    }
}

/// Parse Codex `--json` JSONL events for the native session id and token usage.
///
/// The final answer is NOT taken from here — it comes from the
/// `--output-last-message` file (see [`combine_codex`]). We scan events for:
/// * `thread_id` / `session_id` — the native id to resume with.
/// * a `turn.completed` event's `usage`, or a `token_count` event's nested
///   `info.total_token_usage`/`last_token_usage`, for [`Usage`].
///
/// Non-JSON lines (CLI banners, reconnect notices) are skipped.
pub(crate) fn parse_codex_events(stdout: &str) -> Parsed {
    let mut native_session_id: Option<String> = None;
    let mut usage: Option<Usage> = None;

    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let event: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let obj = match event.as_object() {
            Some(o) => o,
            None => continue,
        };

        // Session id: first non-empty `thread_id` (preferred) or `session_id`.
        if native_session_id.is_none() {
            if let Some(id) = nonempty_string(obj.get("thread_id")) {
                native_session_id = Some(id);
            } else if let Some(id) = nonempty_string(obj.get("session_id")) {
                native_session_id = Some(id);
            }
        }

        // Usage: prefer a `turn.completed.usage`; else a token_count event.
        if let Some(u) = extract_codex_usage(obj) {
            usage = Some(u);
        }
    }

    Parsed {
        text: String::new(),
        native_session_id,
        usage,
    }
}

/// Pull a [`Usage`] out of one Codex event object, if it carries token counts.
fn extract_codex_usage(obj: &serde_json::Map<String, Value>) -> Option<Usage> {
    let ty = obj.get("type").and_then(Value::as_str).unwrap_or("");

    let usage_val: Option<&Value> = if ty == "turn.completed" {
        obj.get("usage")
    } else if ty == "event_msg" {
        obj.get("payload")
            .and_then(Value::as_object)
            .filter(|p| p.get("type").and_then(Value::as_str) == Some("token_count"))
            .and_then(|p| p.get("info"))
            .and_then(Value::as_object)
            .and_then(|info| {
                info.get("total_token_usage")
                    .or_else(|| info.get("last_token_usage"))
                    .or_else(|| info.get("usage"))
            })
    } else {
        // Some Codex builds emit usage on a top-level `usage` key of other events.
        obj.get("usage")
    };

    let u = usage_val?.as_object()?;
    let input = as_u64(u.get("input_tokens"));
    let output = as_u64(u.get("output_tokens"));
    let cached = u.get("cached_input_tokens").and_then(Value::as_u64);
    if input == 0 && output == 0 {
        return None;
    }
    Some(Usage {
        input_tokens: input,
        output_tokens: output,
        reasoning_tokens: None,
        cached_input_tokens: cached,
    })
}

/// Combine the Codex `--output-last-message` file contents (the authoritative
/// final answer) with the session id / usage scraped from the JSONL stream.
pub(crate) fn combine_codex(last_message: &str, events: Parsed) -> Parsed {
    Parsed {
        text: last_message.trim().to_string(),
        native_session_id: events.native_session_id,
        usage: events.usage,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn claude_json_extracts_text_session_and_usage() {
        let out = r#"{
            "type":"result","subtype":"success","is_error":false,
            "result":"the answer is 42",
            "session_id":"abc-123",
            "total_cost_usd":0.01,"duration_ms":1234,
            "usage":{"input_tokens":10,"cache_creation_input_tokens":5,
                     "cache_read_input_tokens":3,"output_tokens":7}
        }"#;
        let p = parse_claude_json(out);
        assert_eq!(p.text, "the answer is 42");
        assert_eq!(p.native_session_id.as_deref(), Some("abc-123"));
        let u = p.usage.unwrap();
        assert_eq!(u.input_tokens, 18); // 10 + 5 + 3
        assert_eq!(u.output_tokens, 7);
        assert_eq!(u.cached_input_tokens, Some(3));
    }

    #[test]
    fn claude_json_non_object_falls_back_to_raw() {
        let p = parse_claude_json("not json at all");
        assert_eq!(p.text, "not json at all");
        assert!(p.native_session_id.is_none());
        assert!(p.usage.is_none());
    }

    #[test]
    fn claude_json_missing_usage_is_none() {
        let out = r#"{"result":"hi","session_id":"s1"}"#;
        let p = parse_claude_json(out);
        assert_eq!(p.text, "hi");
        assert_eq!(p.native_session_id.as_deref(), Some("s1"));
        assert!(p.usage.is_none());
    }

    #[test]
    fn codex_events_read_thread_id_and_turn_usage() {
        let out = "\
{\"type\":\"thread.started\",\"thread_id\":\"th-9\"}\n\
{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"ignored here\"}}\n\
{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":100,\"output_tokens\":40,\"cached_input_tokens\":12}}\n";
        let p = parse_codex_events(out);
        assert_eq!(p.native_session_id.as_deref(), Some("th-9"));
        let u = p.usage.unwrap();
        assert_eq!(u.input_tokens, 100);
        assert_eq!(u.output_tokens, 40);
        assert_eq!(u.cached_input_tokens, Some(12));
    }

    #[test]
    fn codex_events_read_token_count_event() {
        let out = "\
{\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":{\"input_tokens\":5,\"output_tokens\":6}}}}\n\
{\"type\":\"session.created\",\"session_id\":\"sess-1\"}\n";
        let p = parse_codex_events(out);
        assert_eq!(p.native_session_id.as_deref(), Some("sess-1"));
        let u = p.usage.unwrap();
        assert_eq!(u.input_tokens, 5);
        assert_eq!(u.output_tokens, 6);
    }

    #[test]
    fn codex_skips_non_json_noise() {
        let out = "\
Reconnecting... 1/3\n\
some banner text\n\
{\"thread_id\":\"th-x\"}\n";
        let p = parse_codex_events(out);
        assert_eq!(p.native_session_id.as_deref(), Some("th-x"));
    }

    #[test]
    fn combine_codex_uses_last_message_for_text() {
        let events = parse_codex_events("{\"thread_id\":\"t1\"}\n");
        let p = combine_codex("  final answer\n", events);
        assert_eq!(p.text, "final answer");
        assert_eq!(p.native_session_id.as_deref(), Some("t1"));
    }
}
