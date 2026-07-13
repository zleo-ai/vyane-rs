# WP-35: Harness Streaming — Architecture Design

> **Status:** implemented by WP-36. This document preserves the proposal and
> implementation sequence; [`../ARCHITECTURE.md`](../ARCHITECTURE.md) describes
> the current behavior. The implementation additionally uses real CLI event
> fixtures, forwards nested tool-use events, and treats a missing terminal
> Claude result envelope as failure.

## Problem

`Dispatcher::dispatch_stream` (WP-18) only supports direct-HTTP targets.
When the target is a harness (`Executor::Agent`), it returns `Ok(None)` — the
caller falls back to non-streaming `dispatch`, and the user sees no live output
until the entire harness run completes.

For coding-CLI harnesses (Claude Code, Codex CLI), the run can take minutes.
Streaming the CLI's stdout to the user as it arrives would provide the same
live-experience as running the CLI directly.

## Current architecture

### Harness trait (`vyane-core/src/traits.rs`)

```rust
#[async_trait]
pub trait Harness: Send + Sync {
    fn kind(&self) -> HarnessKind;
    async fn available(&self) -> bool;
    async fn run(&self, job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome>;
}
```

`run()` is a single async call that blocks until the harness exits, then returns
the parsed result. There is no way for the caller to observe intermediate output.

### Process spawning (`vyane-harness/src/spawn.rs`)

`run_capture()` spawns the child with `stdout(Stdio::piped())` and drains the
pipe into a `String` after the child exits. The comment at line 96 is explicit:
"v0.1 harnesses are one-shot, so this is not a streaming path."

### Output parsing (`vyane-harness/src/parse.rs`)

- **Claude Code** (`--output-format stream-json`): stdout is a sequence of
  JSON objects, one per line. Each line has a `type` field (`system`,
  `assistant`, `result`, `tool_use`, etc.). The `assistant` lines carry
  `message.content[].text` — these are the incremental text deltas.
  The final `result` line carries the session id and usage.

- **Codex CLI** (`--json`): stdout is NDJSON events. The `item.completed`
  event carries the final answer; intermediate events carry progress.

Both parsers currently work on the full stdout **string** after the process
exits. They would need to parse incrementally (line-by-line) for streaming.

### dispatch_stream harness handling (`vyane-kernel/src/dispatch.rs:404-407`)

```rust
let client = match executor {
    Executor::Chat(c) => c,
    Executor::Agent(_) => return Ok(None),  // ← not supported
};
```

## Proposed design

### 1. New trait method: `run_stream`

Add a default method to the `Harness` trait that returns `Unsupported`:

```rust
#[async_trait]
pub trait Harness: Send + Sync {
    fn kind(&self) -> HarnessKind;
    async fn available(&self) -> bool;

    async fn run(&self, job: HarnessJob, cancel: CancellationToken) -> Result<HarnessOutcome>;

    /// Run a job with streaming output. Default: unsupported.
    ///
    /// Implementations that support streaming should call `on_event` for each
    /// text fragment as it arrives, then return the final `HarnessOutcome`.
    async fn run_stream<F>(
        &self,
        job: HarnessJob,
        cancel: CancellationToken,
        on_event: F,
    ) -> Result<HarnessOutcome>
    where
        F: FnMut(HarnessStreamEvent) + Send + Sync,
    {
        let _ = (job, cancel, on_event);
        Err(VyaneError::unsupported(format!("{} does not support streaming", self.kind())))
    }
}
```

### 2. New event type: `HarnessStreamEvent`

```rust
/// A live event during a streaming harness run.
#[derive(Debug, Clone)]
pub enum HarnessStreamEvent {
    /// A fragment of the answer text (from the CLI's stdout).
    Delta(String),
    /// A tool-use notification (optional, for observability).
    /// Implementations may emit these for tool calls the agent makes.
    ToolUse { name: String, summary: String },
}
```

This mirrors `StreamDispatchEvent` (which only has `Delta` / `ReasoningDelta`)
but adds `ToolUse` — coding CLIs make tool calls (edit files, run commands)
that are visible in their stream output and valuable for the user to see live.

### 3. Streaming spawn: `run_stream_capture`

Add a new function to `vyane-harness/src/spawn.rs` alongside `run_capture`:

```rust
pub(crate) async fn run_stream_capture<F>(
    program: &str,
    args: &[String],
    cwd: Option<&Path>,
    env: &BTreeMap<String, String>,
    cancel: &CancellationToken,
    timeout: Option<Duration>,
    on_line: F,
) -> Result<RunResult>
where
    F: FnMut(&str) + Send,
```

Instead of draining stdout into a single `String` after exit, this function:
1. Spawns with `stdout(Stdio::piped())` (same as now)
2. Reads stdout **line-by-line** in a tokio task (`tokio::io::BufReader::lines`)
3. For each line, calls `on_line(line)` so the harness can parse + emit events
4. Still captures all lines into the final `RunResult.stdout` for post-run parsing
5. Same process-group kill on cancel/timeout (unchanged from `run_capture`)

The bounded-drain logic from `run_capture` (wait for stdout EOF after child
exits, then SIGKILL the group) is reused unchanged.

### 4. Claude Code implementation

Claude Code's `--output-format stream-json` already emits one JSON object per
line. The streaming implementation:

```rust
async fn run_stream<F>(&self, job: HarnessJob, cancel: CancellationToken, mut on_event: F)
    -> Result<HarnessOutcome>
where F: FnMut(HarnessStreamEvent) + Send + Sync
{
    let args = build_claude_args(&job);
    let env = materialize_env(&job);
    let result = run_stream_capture(
        "claude", &args, job.workdir.as_deref(), &env,
        &cancel, job.timeout,
        |line| {
            if let Ok(json) = serde_json::from_str::<Value>(line) {
                match json["type"].as_str() {
                    Some("assistant") => {
                        // Extract text from message.content[].text
                        if let Some(text) = extract_assistant_text(&json) {
                            on_event(HarnessStreamEvent::Delta(text));
                        }
                    }
                    Some("tool_use") => {
                        if let Some(name) = json["name"].as_str() {
                            on_event(HarnessStreamEvent::ToolUse {
                                name: name.to_string(),
                                summary: json["input"].to_string(),
                            });
                        }
                    }
                    _ => {}
                }
            }
        },
    ).await?;

    let parsed = parse_claude_json(&result.stdout);
    classify_claude_result(&result, &parsed)
}
```

### 5. Codex CLI implementation

Codex CLI's `--json` emits NDJSON. The streaming implementation parses each
line as an event and emits deltas from `item.content` / `item.delta` events,
plus the final `item.completed` for the outcome.

### 6. dispatch_stream integration

In `Dispatcher::dispatch_stream`, change the harness branch:

```rust
let executor = self.factory.make(bound)?;
match executor {
    Executor::Chat(client) => { /* existing HTTP streaming path */ }
    Executor::Agent(harness) => {
        // Try streaming; if unsupported, return Ok(None) for fallback.
        let prompt = compose_harness_prompt(&task.prompt, task.system.as_deref());
        let job = HarnessJob { prompt, /* ... same as dispatch ... */ };

        let mut text = String::new();
        let outcome = harness.run_stream(job, cancel.clone(), |event| {
            if let HarnessStreamEvent::Delta(delta) = event {
                text.push_str(&delta);
                on_event(StreamDispatchEvent::Delta(delta));
            }
        }).await;

        match outcome {
            Ok(outcome) => {
                // Same record assembly as the HTTP streaming path.
                let record = self.assemble_stream_record(...).await;
                return Ok(Some(DispatchOutcome { record, output: Some(text) }));
            }
            Err(e) if e.kind == ErrorKind::Unsupported => return Ok(None),
            Err(e) => { /* record the error */ }
        }
    }
}
```

The `StreamDispatchEvent::Delta` callback is reused — front-ends (CLI, SSE)
don't need to know whether the delta came from HTTP or a harness.

### 7. Optional: new `StreamDispatchEvent` variant

Add `ToolUse` to `StreamDispatchEvent` so harness tool calls surface to the
front-end:

```rust
pub enum StreamDispatchEvent {
    Delta(String),
    ReasoningDelta(String),
    ToolUse { name: String, summary: String },  // ← new
}
```

The REST SSE endpoint would emit these as:
```json
{"type":"tool_use","name":"Edit","summary":"src/lib.rs"}
```

## Compatibility

- **Trait evolution is additive.** `run_stream` has a default impl returning
  `Unsupported`. Existing harness implementations (and any third-party
  implementations) are unaffected — they simply don't support streaming until
  they override the method.
- **`run_capture` stays unchanged.** The existing non-streaming `run()` path
  continues to use the simpler capture-all-then-parse approach. No regression.
- **dispatch_stream fallback is unchanged.** When `run_stream` returns
  `Unsupported`, `dispatch_stream` returns `Ok(None)` and the caller falls back
  to `dispatch` (which calls `run`).
- **No config changes.** Harness streaming is automatic when the harness
  implementation supports it.

## Implementation plan

| step | scope | crate |
|------|-------|-------|
| 1 | Add `HarnessStreamEvent` + `run_stream` default to `Harness` trait | vyane-core |
| 2 | Add `run_stream_capture` to spawn module | vyane-harness |
| 3 | Implement `run_stream` for ClaudeCode | vyane-harness |
| 4 | Implement `run_stream` for CodexCli | vyane-harness |
| 5 | Wire harness branch into `dispatch_stream` | vyane-kernel |
| 6 | Add `ToolUse` to `StreamDispatchEvent` (optional) | vyane-kernel |
| 7 | Tests: mock harness with streaming, dispatch_stream harness path | vyane-kernel |
| 8 | CLI: surface ToolUse events in human output | vyane-cli |

## Non-goals

- **No bidirectional streaming.** The harness stdin stays `Stdio::null()`.
  Interactive sessions (approve/deny tool calls) are a separate, larger
  feature.
- **No structured tool-call parsing beyond what the CLI emits.** We surface
  the CLI's own stream events as-is; deep tool-call semantics (diff content,
  command output) are not parsed.
- **No mid-stream cancellation of individual tool calls.** The cancel token
  kills the whole process group, same as non-streaming `run`.
