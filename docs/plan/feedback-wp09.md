# WP-09 feedback

WP-09 was implemented **as specified, within the frozen `vyane-core`
interface** — no core change was required, and `vyane-kernel` was left
untouched. This file records the one design gap the assignment explicitly
asked to be flagged here, plus the conventions chosen where the spec left
latitude. Nothing here is a blocker.

## Proposal: give the kernel a streaming dispatch entry point

`vyane-kernel::Dispatcher` exposes exactly one execution shape:

```rust
pub async fn dispatch(
    &self,
    task: &TaskSpec,
    chain: Vec<BoundTarget>,
    cancel: CancellationToken,
) -> Result<DispatchOutcome>;
```

`DispatchOutcome` carries the finished `RunRecord` plus the fully-materialized
`output: Option<String>` — there is no point in that pipeline where a caller
can observe events *as they arrive*. `--stream`'s CLI implementation
(`crates/vyane-cli/src/command.rs::run_dispatch_streaming`) therefore cannot
go through `Dispatcher::dispatch` at all; it drives the protocol client
directly and hand-assembles a `RunRecord` that mirrors what
`vyane_kernel::dispatch`'s internals would have produced for the same
single-attempt outcome:

- the same digest (`vyane_kernel::task_digest`, already `pub`, reused as-is)
- the same `Attempt`/`AttemptOutcome` shape (one attempt, `failed_over: false`
  since there is no next target in a single-target chain)
- the same `status_for_error` mapping (`Timeout`/`Cancelled` kept, everything
  else → `Error`) — duplicated locally since `vyane_kernel::dispatch::
  status_for_error` and `task_preview` are private to the kernel crate
- the same best-effort ledger-append rule (a ledger write failure never
  demotes a completed run to a caller-visible error)

This is a **deliberate, scoped duplication** — not an argument that the CLI
should own record assembly going forward. It works today because streaming is
gated to exactly the case with no failover and no session continuity (a
single `DirectHttp` target, `--session` unset — see `docs/plan/WP-09.md`), so
there is only one attempt to assemble a record for. It would **not**
generalize cleanly to streaming *with* failover: if a streaming attempt fails
mid-flight after emitting partial output, there is currently no defined
behavior for "advance to the next target but the terminal already printed
half an answer from the failed one" — the CLI's failure path here treats a
mid-stream error as terminal for that target (matches the "once bytes start
flowing there is no retry" rule already used for HTTP-level retry, extended
one level up to failover), which is a reasonable per-target rule but was never
exercised against a *multi-target* streaming chain because that combination
is out of scope for WP-09.

Two shapes would let the kernel absorb this properly in a future work package,
without breaking `Dispatcher::dispatch`'s existing contract:

- **A new `Dispatcher::dispatch_stream` method** returning something like
  `Result<BoxStream<'static, Result<StreamEvent>>, then eventually a
  RunRecord>` — the awkward part is expressing "a stream of events, followed
  by exactly one terminal record" in a single return type. A plausible shape:
  return a stream whose *last* item is a sentinel carrying the `RunRecord`
  (`enum DispatchStreamEvent { Delta(StreamEvent), Finished(RunRecord) }`), so
  callers can `while let Some(event) = stream.next().await` and get both without
  a second channel.
- **Or**, narrower: keep `dispatch_stream` scoped to the no-failover,
  no-session case WP-09 already uses (a `BoundTarget` instead of a `Vec<
  BoundTarget>`), and leave multi-target streaming failover as an explicit
  non-goal until there is a concrete need — this would already remove the
  duplication described above without answering the harder mid-stream-failover
  question.

Either direction is additive to `vyane-kernel`; I did not pursue it here to
stay within the frozen crate boundary for this work package.

## Documented conventions (within spec latitude)

- **OpenAI Responses SSE event vocabulary.** The spec (WP-02) deferred full
  Responses streaming without pinning the exact event names. WP-09 implements
  against the real, documented Responses streaming event shape: named events
  (`event: <type>` + `data: {...}`) where the payload's own `"type"` field
  duplicates the event name — so the existing `data:`-only frame collector
  (`collect_data_lines`) needed no changes; only the JSON `"type"` dispatch in
  `parse_openai_responses` is new. Handled: `response.output_text.delta` →
  `Delta`, `response.reasoning_summary_text.delta` /
  `response.reasoning_text.delta` → `ReasoningDelta`, `response.completed`
  (usage then `Done`), `response.incomplete` (`Done` with
  `incomplete_details.reason` as the finish reason), `response.failed` /
  `error` → `Protocol` error. Everything else (`response.created`,
  `response.in_progress`, `response.output_item.*`, `response.content_part.*`,
  …) is ignored, mirroring how `parse_anthropic`'s catch-all arm already
  treats unrecognized `type`s.
- **`--stream` gates on "no `--session`" too, not just "single DirectHttp
  target".** The assignment's SCOPE section only names "single-target
  DirectHttp dispatches"; I additionally fall back to non-streaming when
  `--session` is set, because the streaming CLI path has no transcript replay
  or session-store update at all. Half-honoring `--session` (tag
  `RunRecord.session_id` but never touch the session store) would silently
  diverge from what `--session` means on every other command. Falling back
  with the same one-line stderr notice mechanism keeps this consistent rather
  than adding a second, different kind of surprise.
- **`--stream` always honors `TaskSpec.system`.** The assignment's CLI section
  didn't call this out explicitly, but `--system` is documented as applying
  "for direct HTTP" (see `DispatchArgs::system`'s help text) and the streaming
  path *is* direct HTTP, so the streamed request assembles `[system?, user]`
  exactly like `vyane_kernel::dispatch`'s non-session direct-chat path does.
- **Human-mode delta flushing.** Deltas print via `print!` + a per-delta
  `stdout().flush()` (not `println!`, which would insert a newline the model
  never produced); a single trailing newline is added at the end only if any
  text was printed, so a failed stream that produced zero deltas doesn't leave
  a stray blank line.
- **`--stream --json` mirrors non-streaming `--json` exactly.** Both go
  through `print_run_json` / the same `RunJson { record, output }` shape, so a
  wrapper script that already parses `vyane dispatch --json` output needs no
  branch for the streaming case.
