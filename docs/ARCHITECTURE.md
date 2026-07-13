# Architecture

This document expands on the design encoded in `vyane-core`. Where it describes
types, it mirrors the frozen source in `crates/vyane-core/src/`; that source is
the authority if the two ever disagree.

## The four-layer target model

Everything in Vyane is built on four independent layers. None can be derived
from another, so each is a separate field — in configuration, in the resolved
`BoundTarget` the kernel executes against, and in the run ledger.

| layer | question it answers | type | examples |
|-------|---------------------|------|----------|
| **provider** | who supplies the endpoint, key, quota and billing | `ProviderId` | an official vendor account, an OpenAI-compatible relay, a cloud platform |
| **protocol** | what the wire format is | `Protocol` | `OpenaiChat`, `OpenaiResponses`, `AnthropicMessages` |
| **harness** | which execution shell the model runs in — and hence whether it has files, shell, tools and long sessions | `Option<HarnessKind>` | `ClaudeCode`, `CodexCli`, `OpenCode`, `Other(..)`, or `None` for direct HTTP chat |
| **model** | which inference model actually runs | `ModelId` | a concrete model id string, valid only within its provider |

### Why conflation breaks real systems

Lazier tooling collapses these axes, and each collapse is a concrete failure:

- **Treating a relay as a protocol.** An OpenAI-compatible relay is a
  *provider* that speaks the OpenAI Chat *protocol*. Fold the two together and
  you can no longer express "the same protocol, a different account, different
  quota and billing" — which is exactly what a failover target usually is.
- **Treating a coding CLI as a provider.** A coding CLI (Claude Code, Codex
  CLI) is a *harness*: an execution shell. It still needs a provider to supply
  the endpoint and key. Conflate them and you cannot run the same harness
  against a different provider, nor the same provider without the harness.
- **Treating a model id as global.** A `ModelId` is only meaningful inside one
  provider's namespace. If failover carries a model id from provider A to
  provider B, at best the call 404s; at worst B silently serves a *different*
  model under a coincidentally-matching name. Vyane pairs every model id with
  its provider and never separates them (see [Failover semantics](#failover-semantics)).

Direct HTTP chat has **no** harness. That is expressed as
`Option<HarnessKind>::None` on `Target`, never as a sentinel `HarnessKind`
value — the absence of a workspace is a real, first-class state.

The resolved forms the kernel runs against:

- `Target` — the four layers pinned (`provider`, `protocol`,
  `harness: Option<HarnessKind>`, `model`). This is the *loggable identity* of
  where a run went; it carries no credentials and is safe to serialize.
- `Endpoint` — `base_url` plus an optional `AuthMaterial` (`AuthStyle` +
  `Secret`). `auth: None` means the harness authenticates natively (its own
  login / subscription).
- `AdapterTransport` — `CliWrap` (spawn a harness subprocess) or `DirectHttp`
  (speak the protocol over HTTP, no workspace).
- `BoundTarget` — `Target` + `AdapterTransport` + `Option<Endpoint>` +
  `GenParams`. Everything the kernel needs to execute one target.

`Secret` has a redacted `Debug`/`Display` and is deliberately **not**
`Serialize`, so credentials cannot reach the ledger or any other persisted
record even by accident.

## Crate map and dependency edges

```
                         vyane-core
 (types · traits · errors · env policy · workdir pin; no orchestration runtime)
        ▲     ▲        ▲         ▲          ▲        ▲
        │     │        │         │          │        │
 vyane-config │  vyane-protocol  │    vyane-harness  │
   │          │        │         │          │        │
 vyane-provider        │      vyane-ledger   │   vyane-kernel
        │              │         │           │   (traits only)
        └──────┬───────┴─────────┴───────────┴────────┘
               ▼
        vyane-service   ── (shared facade: config loading, selector
                             resolution, dispatch/broadcast/history/sessions)
          ▲           ▲
          │           │
   vyane-mcp     vyane-cli      (front-ends: MCP server, REST API,
   (rmcp tools)  (+ axum API)    CLI — all consume vyane-service)
                 (+ vyane-router)
```

The load-bearing rule: **the kernel depends only on `vyane-core` traits.** It
never names a concrete protocol client, harness, or ledger. Concrete types are
constructed and wired together in the CLI (assembler) layer and handed to the
kernel behind `Arc<dyn ChatClient>` / `Arc<dyn Harness>` / an `ExecutorFactory`.

Two consequences:

1. **Wave-1 crates are fully parallel.** `vyane-config`, `vyane-provider`,
   `vyane-protocol`, `vyane-harness`, `vyane-kernel` and `vyane-ledger` each
   depend on `vyane-core` and (mostly) not on each other, so they can be built
   independently and concurrently. Assembly is deferred to `vyane-cli`.
2. **Adapters are swappable, but mutation is explicit.** A text-only adapter
   satisfying a `vyane-core` trait remains source-compatible for `ReadOnly`
   dispatch. `Write` or `Full` additionally requires the trusted assembled
   factory to declare an editing capability; unknown/custom adapters default to
   chat-only and cannot self-assert it through project configuration.

| crate | depends on (beyond `vyane-core`) | role |
|-------|----------------------------------|------|
| `vyane-config` | `vyane-provider` | parse TOML, resolve profiles + failover chains → `BoundTarget` |
| `vyane-provider` | — | provider registry, endpoints, auth styles, env-injection rules |
| `vyane-protocol` | — | `ChatClient` over HTTP (OpenAI Chat / Responses, Anthropic Messages), including bounded non-streaming OpenAI Chat typed tool turns, a separately authorized OpenAI Chat turn path, and the shared HTTP base validator and endpoint-routing digest |
| `vyane-harness` | — | `Harness` wrapping coding CLIs, additive process-local execution context, pinned-workdir child handoff, process-group control, a guarded native tool-registry boundary, and an unwired bounded native turn driver |
| `vyane-kernel` | — (traits only) | early execution identity, whole-chain capability admission, prepared dispatch, dispatch / broadcast / failover state machine |
| `vyane-ledger` | — | JSONL `Ledger`, filesystem `SessionStore` with strict revisioned native-session snapshots/CAS transitions, cost table |
| `vyane-task` | — | owner-qualified SQLite task lifecycle snapshots/events, revision + executor-epoch CAS, transactional schema migration |
| `vyane-agent` | — (independent leaf crate) | owner-scoped SQLite AgentRun queue and worker-topology truth, fenced two-stage recovery admission, non-serializable active permits and native execution scopes with atomic durable revalidation, bounded tree cancellation, and body-free outbox |
| `vyane-message` | — (independent leaf crate) | owner-scoped SQLite message/delivery truth, fenced leases, external receipts, per-projector body-free outbox |
| `vyane-broker` | `vyane-agent`, `vyane-message`, `vyane-ledger` | bounded owner-bound delivery pump, replay-safe adapter boundary, maintenance, body-free message/AgentRun EventLog projectors, and an explicit unwired resident library driver |
| `vyane-router` | `vyane-core` | target selection / routing policy |
| `vyane-workflow` | `vyane-kernel` | declarative DAG execution, bounded source bundles, atomic journals, and explicit resume |
| `vyane-service` | `vyane-agent`, `vyane-kernel`, `vyane-config`, `vyane-ledger`, `vyane-message`, `vyane-broker` | shared facade plus principal-derived `OwnerScopedService`, allowlisted run/session views, owner-scoped session control, explicit projection construction, the fresh-sessionless authority bridge, fixed-owner one-shot drivers, a paired in-process backend and its resident library supervisor; ordinary dispatch constructs none of the optional AgentRun components |
| `vyane-mcp` | `vyane-service`, `rmcp` | six-tool MCP server over stdio: dispatch/broadcast/history/sessions plus two bounded diagnostics, route preview and static configuration check; generic success output has a 1 MiB cap |
| `vyane-cli` | `vyane-service`, `vyane-workflow`, `vyane-task`, `vyane-mcp`, `axum` | assembler: CLI + bearer-authenticated loopback-only REST API (`vyane serve`, Host/Origin checked and non-loopback rejected) + authenticated local workflow daemon + MCP launcher (`vyane mcp`) |

`ChatClient::complete_turn` is an additive typed boundary. Its default fallback
keeps text-only clients source-compatible but delegates only when the request
is lossless legacy text; request-side tool calls/results, reasoning and refusal
signals are never flattened. The non-streaming OpenAI Chat override also
preserves response-side tool, reasoning and refusal signals. Anthropic/Responses
typed turns, streaming tool envelopes and provider-specific quirk profiles
remain open. This transport contract is a prerequisite for a native model loop;
it is not itself a `NativeHarness` implementation.

`AuthorizedToolChatClient` is a separate trait with no fallback to the ordinary
chat path. Its OpenAI Chat implementation asks a live
`NativeExecutionAuthority` for every explicit wire attempt immediately before
`send`; an authority error is returned unchanged and cannot enter the HTTP
retry policy. Cancellation is observed while waiting for authority, sending,
reading a success body and backing off. The shared HTTP client follows no
redirects and performs no implicit client retries, leaving the explicit Vyane
loop as the sole owner of physical attempt numbering. This guarded entry point
still has no production native-loop caller.

## Execution identity and capability admission

Every kernel dispatch first allocates an `ExecutionScope` with a UUIDv7 id,
owner, start time and requested sandbox. Each admitted chain position receives
an `AttemptScope` containing that execution identity plus its original resolved
chain ordinal. These values are serializable audit evidence; they contain no
credential, lease token, approval or native execution authority.

Before constructing any executor, the kernel asks the trusted
`ExecutorFactory` for every target's side-effect-free `CapabilityManifest` and
preflights the whole chain:

- `ReadOnly` remains compatible with text-only clients and legacy factories.
- `Write` and `Full` require an existing caller workdir plus
  `CallerWorkdirEditing` and non-`None` declared isolation. The primary target is
  rejected if it cannot satisfy the request; an ineligible fallback is filtered
  before `make`, HTTP or subprocess execution and retains its original ordinal
  in admission evidence.
- The service assembler declares only the built-in local Claude Code and Codex
  CLI adapters as `CallerWorkdirEditing + AdapterDelegated`. Direct HTTP,
  remote/unknown adapters and custom factories using the default manifest are
  chat-only.

On Linux a mutating admission opens the requested directory first, derives its
canonical audit path and device/inode identity from that open handle, and keeps
the handle in a non-serializable `PinnedWorkdir`. Built-in harness children
inherit the same directory object, establish cwd through its descriptor, and address it through
`/proc/self/fd`; rename or symlink replacement after admission therefore cannot
redirect the run. Non-Linux mutating admission fails closed. This is workdir
identity enforcement, not an OS sandbox: `AdapterDelegated` does not confine a
same-UID process or prevent absolute-path access, and `Full` is deliberately
unrestricted.

`PreparedDispatch` is a one-shot, process-local execution plan. It is bound to
the admitting `Dispatcher` identity and owner; only that dispatcher or a
same-owner clone can consume it. Serializable capability snapshots can be
compared across the detached parent/worker boundary, but cannot recreate the
dispatcher provenance or the pinned directory authority.

## Native side-effect authority seam

`NativeExecutionAuthority` is a live, object-safe boundary that grants exactly
one `NativeSideEffect`: one model wire send, one tool-registry operation, one
checkpoint preparation/publication point, or one revision-fenced session
commit. These coordinates contain no request body, tool arguments, path,
credential or bearer token, and are deliberately not serializable. A successful
check cannot be cached for a later effect.

`vyane-agent` can validate an `ActiveExecutionPermit` together with a
`NativeExecutionScope` in one write-locked SQLite snapshot. Besides the active
run, worker generation, secret lease capability, deadline and lifecycle, the
predicate freezes the exact target, prompt digest, policy digest, logical
session and fresh-versus-resumed binding proof. Heartbeat and activity changes
may advance the row revision without invalidating that authority; cancellation,
terminal state, lease/deadline expiry or identity drift fail closed.

Two lower-level consumers exist, but remain unwired. The authorized OpenAI Chat
path revalidates every explicit physical send. The authorized tool-registry path
revalidates only after validation and permission evaluation select an allowed
call, immediately before the tool executor is polled; pure invalid, unknown,
deny, ask, cancelled and expired outcomes consume no authority. Revocation is
an outer execution error, not model-facing tool text. Registry authorization is
only the dispatch linearization point: a trusted tool that performs multiple
opens, publishes or spawns must revalidate each additional external effect.

`vyane-service::AgentRunModelToolAuthority` is the first concrete bridge from
an owned `ActiveExecutionPermit`, `NativeExecutionScope`, and `AgentStore` to
the abstract authority contract. Construction accepts only a fresh,
sessionless scope. Each one-based `ModelSend` or `ToolOperation` revalidation
runs the synchronous store check on Tokio's blocking pool. Session-bearing
scopes, checkpoint prepare/publish, and session commit fail closed. The type is
exported for explicit future assembly but is not registered in a production
factory, called by an existing runtime/native loop, or composed with
`SessionExecutionLease` and exact `NativeSessionDomain` authority. These
contracts therefore close bypasses at their individual boundaries without
enabling `NativeHarness`, native resume, or any built-in tool.

The paired in-process backend can additionally bind its lifetime-bound
`InProcessEffectAuthority` to one exact fresh sessionless scope. The bind
performs an immediate atomic native-permit validation; the returned opaque,
non-`Clone`, non-serializable `InProcessNativeAuthority` repeats it for every
positive one-based model send and tool operation. Raw store/permit access,
session/resume scopes, checkpoint/session-commit effects and unknown effects
remain closed. This makes the authority chain composable inside a future
operation but supplies no resolver, client, or concrete product operation.
The generic crash-consistent completion sink/receipt boundary is added
separately by [WP-53](plan/WP-53.md); it still does not assemble a product
native runtime. See [WP-52](plan/WP-52.md).

`vyane-harness::native::NativeTurnDriver` is a bounded, strictly serial caller
of those guarded boundaries. Its default limit is eight logical model turns and
its validated hard ceiling is 32. Each response may contain at most one tool
call. Before the first model send, the advertised tool-name set must exactly
equal the executable registry-name set; descriptions and JSON schemas remain
non-authoritative model guidance, so every `NativeTool` must validate the
actual arguments it receives. The driver validates the initial request, every
subsequent request, and every response. Before permission evaluation or tool
polling, it also proves that the assistant call plus a worst-case bounded tool
result fits the next complete transcript; duplicate call ids and message or
envelope exhaustion therefore stop before a tool side effect.

Every model request goes through `AuthorizedToolChatClient`, and every known
allowed call goes through `ToolRegistry::execute_authorized` with 1-based turn
and ordinal coordinates. Invalid JSON arguments become static, non-echoing
model-facing error text and never execute. Refusal, approval-required, parallel
calls, tool-choice violations, cancellation, timeout, and turn-budget
exhaustion are typed terminal stops. Usage counters use saturating addition.
Once a known allowed tool may have been polled, later model or transcript
failure becomes a redacted `AbortedAfterToolActivity` stop rather than an outer
failover-eligible error. `NativeTurnOutcome` is not serializable, and its custom
`Debug` omits prompt, reasoning, tool arguments/output, transcript, final reply,
and approval-plan contents.

This driver is still a dark library component. No factory, service operation,
CLI, daemon, or runtime constructs it; it is not a `Harness`. It provides no
trusted built-ins, session/domain authority, checkpoint or session-commit
consumer, approval resume, or native resume.

## Dispatch lifecycle

A single `dispatch(TaskSpec, Vec<BoundTarget>)` walks this state machine. A
configuration, capability or session-continuity failure before the attempt
phase returns `Err` and writes no `RunRecord`. Once the attempt phase begins,
the dispatch produces exactly one record for success, ordinary failure,
timeout or cancellation.

```
resolve selector → Vec<BoundTarget>        (config/service layer)
              │
              ▼
allocate ExecutionScope UUIDv7             (before session/factory/model)
              │
              ▼
inspect every trusted manifest
pin Linux mutating workdir
reject primary / filter ineligible fallback
              │ pre-execution error → Err, no RunRecord
              ▼
load and validate session continuity
              │ pre-execution error → Err, no RunRecord
              ▼
attempt admitted target ───────────────────────────────────────┐
  make_scoped → Chat(client) or Agent(harness)                 │
  record Attempt                                               │
              │                                                │
        success? ──yes──▶ status = Success ─────────┐          │
              │ no                                  │          │
              ▼                                     │          │
      classify ErrorKind                            │          │
              │                                     │          │
 failover_eligible() AND admitted target remains?  │          │
         yes ─┼── mark failed_over ─────────────────┼──────────┘
              │
          no  ▼
      status = Error / Timeout / Cancelled
              │
              ▼
assemble RunRecord
  run_id = early execution id
  attempts = targets actually executed
  target = last attempted target
              │
              ▼
ledger.append(record)                 (best effort after completion)
session_store update when applicable  (best effort after completion)
```

The failover gate is a single conjunction over the **admitted** chain: advance
only when the error's `ErrorKind::failover_eligible()` is true and another
admitted target remains. A capability-rejected fallback is not an `Attempt`, is
never constructed, and does not appear in `RunRecord.attempts`; its original
ordinal remains in the scoped admission evidence. Every target actually tried,
including the last failed one, is recorded. `RunRecord.target` is the last
attempted target (the one that produced the final outcome). If every potential
fallback was filtered, the primary error remains terminal with
`failed_over=false`.

**`broadcast`** runs N independent target chains concurrently, each through the
same dispatch machine, bounded by a concurrency semaphore, and returns results
**in input order** (index-preserving) even though they complete out of order. A
partial failure is just some chains ending in `Error` while others succeed —
each still produces its own `RunRecord`.

**Cancellation** propagates through a `CancellationToken` (re-exported from
`vyane-core`). After successful preflight, an already-cancelled token produces a
cancelled record without calling `make`; direct-HTTP attempts drop their
in-flight request, while harness attempts kill and reap the child *process
group*. A cancelled attempt still writes its `RunRecord`.

## EnvPolicy and clean child environments

Credential pollution across nested agent sessions is a **real, observed
failure mode**, not a hypothetical: spawn a coding CLI with the parent's full
environment and the *calling* agent's `*_API_KEY` / `*_BASE_URL` overrides leak
into the child, silently redirecting its authentication — the child meant to
use provider A inherits provider B's overrides and starts failing with 401s, or
worse, quietly bills the wrong account.

So harnesses spawn **scrubbed by default**. `EnvPolicy` is a pure function of
`(policy, parent_env)`:

- `mode: InheritMode` — `Scrub` (default) starts from `BASELINE_ENV` only;
  `Full` inherits the whole parent environment (opt-in).
- `allow: Vec<String>` — extra parent variables let through when scrubbing.
- `inject: BTreeMap<String, String>` — variables set for this run (auth, base
  URL, model). **Injection always wins**, even in `Full` mode.

`BASELINE_ENV` is the minimal set a well-behaved CLI needs to start (`PATH`,
`HOME`, `USER`, `SHELL`, `TERM`, locale vars, `TMPDIR`, `TZ`, the `XDG_*`
config/data/cache dirs) — and nothing that redirects model traffic. Harness
implementations **must** build the child environment exclusively through
`EnvPolicy::build`; that is how a run's environment stays self-contained and
reproducible, and how the parent session's keys are guaranteed never to reach
the child.

Authority-bearing built-in harness runs add a Unix start gate. The lifecycle
sentinel starts under a fixed minimal environment, while the complete target
environment crosses a private descriptor and is sourced only after the parent
revalidates cancellation, the absolute deadline, and `HarnessSpawnAuthority`.
The same live authority is checked before every physical wrapper spawn attempt.
Denial leaves the real target unstarted and kills/reaps the gated group. The
callback is trusted synchronous runtime code and must remain bounded. This
[WP-55](plan/WP-55.md) seam has no production AgentRun caller yet; it is not a
Process backend, host sandbox, durable controller, or resume mechanism.

## Failover semantics

`ErrorKind::failover_eligible()` is the single source of truth for whether an
error advances to the next target. This table mirrors
`crates/vyane-core/src/error.rs` exactly.

| `ErrorKind` | meaning | fails over? |
|-------------|---------|:-----------:|
| `Auth` | 401/403, bad key | ✅ |
| `RateLimited` | 429, quota exhausted | ✅ |
| `Timeout` | exceeded the caller-specified timeout | ✅ |
| `Transport` | DNS / connect / TLS / broken stream | ✅ |
| `Protocol` | 5xx, malformed response, refused request | ✅ |
| `SpawnFailed` | harness binary could not be spawned | ✅ |
| `HarnessFailed` | harness ran but exited unsuccessfully | ✅ |
| `Config` | invalid / missing configuration | ❌ |
| `Cancelled` | cancelled by the caller | ❌ |
| `Unsupported` | target lacks the requested capability (e.g. streaming) | ❌ |
| `NotFound` | referenced session / profile / run does not exist | ❌ |
| `Io` | local I/O failure (ledger, config files) | ❌ |
| `Other` | anything else | ❌ |

Primary capability rejection is a typed pre-execution `Unsupported` error. It
does not become an `Attempt` and cannot trigger ordinary failover; capability-
ineligible fallback legs have already been removed from the executable chain.

The rule behind the table: **deterministic caller-side mistakes and explicit
cancellation abort; transient or target-specific failures fail over.** Retrying
a `Config` or `NotFound` elsewhere cannot succeed; retrying a `Cancelled` or
`Unsupported` would do something the caller did not ask for. A wrong `ErrorKind`
classification silently changes failover behaviour, so adapters must map their
failures onto these kinds faithfully — that mapping is part of the kernel's
contract, not an implementation detail.

Independently, and just as important: a failover **chain** is a list of
fully-resolved targets. Each element carries its own provider *and* its own
model id together, so moving to the next target never carries a model id across
a provider boundary.

## Streaming dispatch

`Dispatcher::dispatch_stream` is the streaming counterpart to
`Dispatcher::dispatch`. It is scoped to a single target (no failover, no
session) — either a direct-HTTP client or a CLI harness implementing
`Harness::run_stream`. The method takes a callback for text, reasoning, and
tool-use events and returns the assembled, ledger-appended `DispatchOutcome`
when the stream completes:

```rust
pub async fn dispatch_stream<F>(
    &self,
    task: &TaskSpec,
    bound: &BoundTarget,
    cancel: CancellationToken,
    on_event: F,
) -> Result<Option<DispatchOutcome>>
where F: FnMut(StreamDispatchEvent) + Send;
```

- Preserves the public compatibility contract: it returns `Ok(None)` when the
  client itself declines streaming (`ErrorKind::Unsupported`). This legacy
  entrypoint deliberately probes through unscoped `ExecutorFactory::make`, so it
  does not expose a factory-observed execution id that its caller cannot reuse;
  a caller that then invokes ordinary `dispatch` starts a separate dispatch.
- Returns `Ok(Some(outcome))` with the ledger-appended `RunRecord`.
- Capability admission still runs before the probe, so a chat-only mutating
  target is rejected before `make`.
- Direct-HTTP timeout/cancellation use the kernel's biased `select!`. Harnesses
  own the child process group and must honour the forwarded token plus
  `HarnessJob.timeout`; the kernel awaits their cleanup instead of dropping the
  process-owning future.
- Record assembly (digest, attempt shape, status mapping, best-effort ledger
  append) is kernel-owned and front-ends must not duplicate it.

The additive `prepare` → `dispatch_stream_prepared` seam is for callers that
own both probe and fallback. An unsupported prepared probe moves its one-shot
plan into the sole fallback-ready state; passing that exact value to
`dispatch_prepared` preserves the execution id, admitted chain and pinned
workdir. A completed/cancelled/error stream consumes the plan, and a second
probe or dispatch is rejected. Prepared plans are also provenance-bound to the
admitting dispatcher and owner. The CLI `--stream` path uses this seam. The REST
SSE endpoint intentionally continues to use the legacy API and emit
`unsupported`, preserving its wire contract.

`Harness::run_scoped` and `run_stream_scoped` carry the process-local pinned
workdir without adding a required field to `HarnessJob`. Existing harness
implementations remain source-compatible; their defaults delegate when no pin
is present and fail closed rather than ignore a supplied pin.

The REST API's `POST /v1/dispatch/stream` bridges the callback to SSE via a
`tokio::sync::mpsc` channel, yielding `delta` / `reasoning_delta` / `tool_use` /
`finished` / `unsupported` events. Harness implementations parse the CLIs'
actual line-delimited event envelopes; the final answer and usage still come
from the terminal harness outcome rather than concatenating telemetry. Live
deltas are best-effort under SSE backpressure, while the bounded bridge waits
to deliver the terminal `finished` / `unsupported` event to a connected client.

## Executable routing

Routing policy remains outside the kernel. `vyane-service` turns a rendered
task plus route hints into a `DispatchPlan` containing the selected profile,
real provider/model decision, and concrete `BoundTarget` chain. The normalized
effort is applied before the kernel sees the chain, and canonical `routing.*`
labels make the decision auditable in the ordinary `RunRecord`.

The route preference carries an opaque selection key so profiles that share a
provider/model tuple cannot be confused. `allow_frontier=false` filters profile
eligibility and every failover leg rather than relying on provider-wide
classification. Router output labels are reserved, and detached workers verify
a secret-free target snapshot before executing frozen routing metadata.

An explicit selector resolves immediately. `auto` in a workflow is a deferred
single-target selector: static validation checks the DAG and templates, then
the resolver selects a target only after the step prompt has rendered. Deferred
fan-out is rejected because the current kernel broadcast accepts one shared
`TaskSpec` and therefore cannot preserve distinct per-target route metadata.
`WorkflowRouteHints` now includes a closed typed `effort`; all route hints are
valid only on that deferred single-target path. An explicit target or `fan_out`
with non-empty route hints fails validation before journal creation, task/process
admission, or network activity rather than silently discarding policy.

Effective effort precedence is explicit workflow effort, selected-profile
configured effort, then the decision-tier default. The selected value is
normalized into reserved `routing.effort`, applied to every failover leg, and
frozen in the recorded route. Recorded-route reconstruction, detached workers,
daemon idempotency checks, and same-journal resume therefore use the admitted
effective value rather than recomputing it from later configuration. Ordinary
labels cannot set the reserved key, a generic `effort` label is not an alias,
and invalid typed or recorded values fail with a bounded non-echoing diagnostic.
`profile:auto` disambiguates a literal profile with that name, while
`target:<provider>/<model>` escapes a provider id beginning with `profile:`.
Workflow resume
hashes include referenced `prompt_file` content, not only the TOML bytes.

This routing boundary is delivered in [WP-46](plan/WP-46.md). [WP-54](plan/WP-54.md)
adds the shared `WorkflowPlan` schema-v1 execution payload used by compile,
prepare, run, and resume. It is strict, bounded, filesystem-independent, and
checksum-bound, but contains execution-sensitive prompt/target/workdir data and
is not a public projection. Its digest detects drift; it does not authenticate
the caller, prove provenance, or grant authority. The embedded capability
manifest is only a requested pre-resolution summary and must not replace target
resolution or runtime admission.

[WP-58](plan/WP-58.md) adds exact-plan replay/fork as a distinct new-run
operation. A terminal source journal is read-only; a create-only UUIDv7 journal
copies its dependency-closed, journal-recorded all-success prefix and then
executes the remaining suffix live. This is explicit foreground continuation, never daemon
restart replay.

Dynamic control flow, nested workflows, shared budgets, a compatibility
frontend, changed-plan call matching, public CLI/REST/MCP plan transport, sanitized
cross-implementation route fixtures, and a production-complete model-tier
policy remain open.

## Static MCP diagnostics

The MCP front-end exposes six tools. Four execute or query the established
service surface (`vyane_dispatch`, `vyane_broadcast`, `vyane_history`, and
`vyane_sessions`). `vyane_route` is a Rust-specific deterministic route
preview extension; it is not evidence of a same-name tool or equivalent
product behavior in the fixed original repository. `vyane_check` is a
configuration-only diagnostic. It validates bounded provider/profile shapes,
assembler-supported transport/protocol/harness combinations, credential
*presence*, and the same HTTP base-URL contract used by protocol clients. It
does not open a network connection, probe a model, spawn a harness, or validate
whether a credential works.

Both diagnostic tools use strict schemas and bounded inputs, configuration
rows, identifiers, failover legs, metadata and serialized output. Their wire
responses are allowlisted and redacted: they do not return task text, endpoint
URLs, credential values, environment-variable names, local paths, or raw
internal errors. Every success result has a generic 1 MiB output ceiling, while
the two diagnostics retain smaller domain-specific caps; the older execution
tools do not yet share the diagnostics' uniform field-level input budgets. If
dispatch/broadcast execution completed but the detail is oversized, MCP returns
bounded index/run-id receipts with `operation_status=completed` and
`detail_omitted=true` instead of a limit error that could invite duplicate
execution. Run and session results cross the service/MCP boundary through
allowlisted views rather than raw ledger or session-store records. The endpoint-routing helper used by native-session domains is
also shared with `vyane-protocol`: it canonicalizes scheme, IDNA host,
effective port and base path, rejects userinfo/fragments, permits at most one
explicitly non-secret `api-version` / `api_version` routing query, and persists
only a versioned SHA-256 digest.

## Ledger record schema

Every dispatch that reaches the attempt phase appends exactly one `RunRecord`.
Capability/configuration/session-admission failures return before an attempt and
do not manufacture a record. Fields mirror `crates/vyane-core/src/run.rs`.

| field | type | notes |
|-------|------|-------|
| `run_id` | `String` | UUIDv7 — time-ordered, globally unique |
| `owner` | `String` | owner scope; `"local"` for single-user. Present from day one |
| `started_at` / `finished_at` | `DateTime<Utc>` | wall-clock bounds of the whole dispatch |
| `task_digest` | `String` | SHA-256 of the prompt, hex, first 16 chars — **not the prompt body** |
| `task_preview` | `Option<String>` | legacy/explicit opt-in field; new dispatches leave it `None` |
| `workdir` | `Option<String>` | harness working directory, when set |
| `sandbox` | `Sandbox` | `ReadOnly` / `Write` / `Full` |
| `target` | `Target` | the target that produced the final outcome (the last attempt) |
| `transport` | `AdapterTransport` | `CliWrap` / `DirectHttp` |
| `attempts` | `Vec<Attempt>` | admitted targets actually attempted, in order; a capability-filtered leg is not an attempt |
| `status` | `RunStatus` | `Success` / `Error` / `Timeout` / `Cancelled` |
| `usage` | `Option<Usage>` | input / output (/ reasoning / cached) tokens |
| `cost_usd` | `Option<f64>` | `None` when the model is not in the price table — never guessed |
| `session_id` | `Option<String>` | session this run belonged to / created |
| `output_chars` | `Option<u64>` | length of the produced answer |
| `error` | `Option<String>` | terminal error message on failure |
| `labels` | `BTreeMap<String,String>` | free-form labels copied from the task spec |

Each `Attempt` records its own `target`, `transport`, `started_at`,
`duration_ms`, and an `AttemptOutcome` (`Ok`, or `Err { kind, message,
failed_over }` where `failed_over` says whether *this* error moved the kernel to
the next target).

`ExecutionScope`, `AttemptScope`, capability evidence and detached capability
snapshots are separate audit/control types. The current `RunRecord` schema does
not persist a rejected-target admission trail, so `attempts` must not be
described as evidence for targets that were filtered before execution.

## Session continuity

There are **two** continuity mechanisms and they are genuinely different things
(`crates/vyane-core/src/session.rs`):

- **Native harness sessions.** A coding CLI owns native state. The additive
  `SessionSnapshot` contract classifies it as `Absent`, `LegacyUnbound`, or
  `Bound { NativeSessionBinding }`. A binding couples the native id to an exact
  `NativeSessionDomain`: runtime/harness, provider/protocol/model, secret-free
  endpoint-routing digest, canonical workdir identity, checkpoint
  namespace/schema, and account/runtime scope digests. This is persisted
  evidence, not authority to resume.
- **Transcript sessions.** Direct-HTTP chat has no native runtime state. Vyane
  stores the message transcript on the logical record and replays it as history.
  A pure direct-HTTP chain may continue that transcript regardless of stored
  native-state classification because it neither consumes nor rewrites the
  native binding.

`SessionRecord` deliberately keeps its original public fields so downstream
struct literals remain source-compatible. `FsSessionStore` instead writes a
strict schema-2 wrapper containing the record, a required `session_revision`,
and a required explicit native-state variant. New readers migrate legacy JSON
as revision-zero `LegacyUnbound`/`Absent`; the incompatible wrapper prevents an
old `SessionRecord` reader from silently ignoring authority fields and writing
them back out. Missing/unknown schema fields, unknown V2 fields, duplicate
authority, and simultaneous legacy/bound representations fail closed.

Every legacy save/update and every native transition advances the same
revision under the per-session lock. `Reset`, `ForkFresh`, and `Commit` use
revision CAS and atomically publish the logical update with native state;
file/directory synchronization and an explicit `Indeterminate` result cover
post-rename durability uncertainty. `load_snapshot` and `list_snapshots`
expose revision-aware state. Custom stores remain source-compatible through
defaults that project legacy records at revision zero and return `Unsupported`
for native mutations. The additive execution-lease method also has a default,
but that default returns `Unsupported`: a custom store cannot silently run a
session without implementing execution-period ownership.

Regular session dispatch acquires a live `SessionExecutionLease` for the exact
owner, logical session and kernel execution id before loading continuity. The
kernel validates both the lease identity and the loaded record identity before
factory construction, retains the lease across every failover attempt, and
uses the revision loaded under that lease for a one-shot completion CAS. The
filesystem implementation holds a private advisory-lock descriptor, conflicts
a competing same-session execution before `make` when the current holder
outlives the bounded admission wait, and otherwise admits it only after the
holder releases; the two executions never overlap. Direct save/update/reset
control mutations use the same lock. Aborting a control
future while it is waiting can no longer publish later: admission and mutation
are separate stages. If cancellation happens after a filesystem publish has
started, the outcome is indeterminate and the caller must reload before retry.
Process death closes the descriptor automatically. Detached parent admission
is read-only; the worker reacquires the lease and reloads immediately before
actual execution.

This is a strong single-host `FsSessionStore` fence, not a distributed lease:
there is no durable generation/token, TTL renewal, heartbeat or stale-holder
recovery protocol for remote/custom stores. Session persistence after an
already-completed model call also remains best-effort, so a commit failure is
warned and returned as a successful run rather than pretending the external
side effect did not happen. Regular dispatch may start fresh from `Absent`, but
still rejects `LegacyUnbound` or `Bound` native state before `make` whenever the
admitted chain contains a harness. Streaming dispatch does not acquire a lease
or load a snapshot; it rejects any session before session-store load,
capability probe, or `make`. Generic model-send and tool-registry authority
consumers, a fresh-sessionless AgentRun bridge, and a bounded dark turn driver
now exist, but production domain-bound resume remains disabled until a runtime
authority composes the
active permit with the live session lease and exact domain, and checkpoint/
session-commit consumers complete the chain. The
owner-local service/CLI exposes list, inspect, and revision-checked
reset-native, but no public fork, REST mutation, coherent harness/direct-chat
hopping, or end-to-end native resume.

## Principal-derived owner service scope

Protocol authentication and durable owner selection are separate boundaries.
`OwnerContextFactory` freezes an application-owned `PrincipalAuthenticator`
and `PrincipalOwnerResolver`. Protocol code can submit credential bytes but
cannot publicly construct `AuthenticatedPrincipal` from a request field. The
factory maps the verified principal to a canonical owner and mints an opaque
`OwnerContext`; authenticated resolution cannot enter the reserved `local`
namespace. Closed errors and `Debug` output reveal neither credentials,
principal, owner, nor adapter diagnostics.

`VyaneService::scope` consumes that context into an `OwnerScopedService`. The
same frozen owner configures its dispatcher, single-target streaming, and all
run-history, session-list, session-inspection, and revision-checked reset paths.
Foreign session ids remain absent-shaped. Static route/config inspection is
exposed for convenience but does not confer cross-owner record authority.
Administrative access is a future separate typed capability, never an owner
string or nullable owner.

This is phase A, not a multi-user API. Legacy CLI and MCP operations retain an
explicit trusted single-user `local` compatibility path. REST freezes all
dispatch, broadcast, run, session and streaming operations into one local
facade at router assembly, but bearer handling does not yet authenticate
distinct principals. Durable task truth is owner-qualified after WP-50; built-in task
control still selects only `local`, and workflow/message/AgentRun control
surfaces are not all migrated. See [WP-49](plan/WP-49.md).

## Durable AgentRun and worker truth

`vyane-agent` is the owner-scoped source of truth for worker topology and each
worker's ordered AgentRun queue. It is deliberately separate from
`vyane-task`, whose rows describe user-visible execution control, and from
`vyane-message`, which owns message bodies and delivery state. AgentRun rows
contain bounded identifiers, target/prompt/policy digests and lifecycle
metadata; prompt text, model output, credentials, raw errors and native session
identifiers have no representable field.

Root creation, revision-fenced child spawn and run enqueue are transactional.
Claim assigns a per-worker generation, secret capability and one fixed
execution deadline. Start, heartbeat, activity, native-session binding and
settlement require the exact owner, worker, generation, revision, lease owner
and token while both lease and deadline remain live. Heartbeats may renew the
controller lease but never extend the execution deadline. Recovery is a
separate two-stage operation: an expired lease/deadline or abandoned cancel is
first claimed with a fenced recovery ticket, and terminal state changes only
after the controller adapter affirmatively confirms the old controller is
gone.

Once a claimed run is exactly `Running`, the store can exchange its current
receipt and frozen policy digest for a non-cloneable, non-serializable
`ActiveExecutionPermit`. Every validation rechecks owner, run, worker
generation, secret token, lease/deadline, lifecycle and policy and returns only
a serializable, token-free audit snapshot. Heartbeat/activity revision changes
do not by themselves invalidate the permit. Native validation can additionally
freeze the target, prompt, policy, logical session and resume-binding proof in
a non-serializable `NativeExecutionScope`, then compare the complete predicate
in one write-locked snapshot. `AgentRunModelToolAuthority` bridges that
permit/store operation to the abstract trait only for fresh sessionless scopes
and only for model sends and tool operations, moving each synchronous
revalidation onto Tokio's blocking pool. No production factory/runtime/native
loop constructs it, and session, checkpoint and session-commit authority remain
fail closed, so this is not end-to-end native side-effect authority.

For the paired in-process path, a private shared permit state now backs both
permit-only proofs and a lifetime-bound native-scope wrapper. Binding and every
model/tool effect use the same atomic native predicate; invalid coordinates and
unassembled effects fail before store access. This closes the authority
composition seam but not request binding or durable result handback.

Topology snapshots and tree cancellation are bounded. A cancel operation
freezes its root, children-first worker order, exact run/action membership and
plan digest before any lifecycle mutation; retry tickets cannot expand, shrink
or move that scope. Resume creates a new queued run only from an eligible
interruption and requires the exact frozen logical/native-session binding plus
policy digest. Native session values are hashed transiently and never stored in
this crate.

Every worker/run lifecycle mutation represented by `AgentEvent` commits that
body-free event in the same SQLite transaction. Projector acknowledgement is a
separate persisted mutation and does not manufacture another lifecycle event.
Event sequences are contiguous per owner and projection acknowledgement is
isolated per projector. `vyane-broker::AgentEventProjector::project_once`
consumes one bounded owner/projector-scoped page, durably appends a body-free
lifecycle event to EventLog with the source event id unchanged, and only then
marks the outbox row projected. A crash between append and mark can repeat the
stable id, so consumers still deduplicate by event id. This remains a bounded
projector, not a unified resident timeline. The service's explicit
`AgentProjectionComponents::open` validates and freezes one owner before
opening `$VYANE_DATA_DIR/agent-runs.sqlite3`, keeps the raw store encapsulated,
and exposes only the projector backed by the shared EventLog directory.
Ordinary dispatch does not construct these components, open the AgentRun
database, or start background work. The broker's explicit resident library
driver can poll this projector, but no service, CLI or daemon production
assembly constructs that driver. The paired in-process backend below has a
separate resident execution/recovery library supervisor, but no concrete product
operation, Process/Remote integration, service/CLI execution API or message
handback wiring. Unix paths reject symlink traversal and unsafe parents and keep database
sidecars private; an equivalent non-Unix ACL contract remains future hardening.

### One-shot stale-controller recovery seam

`vyane-service::AgentRunRecoveryDriver` consumes the store's existing
two-stage recovery protocol without exposing the raw store or a
`RecoveryTicket`. It is an explicit fixed-owner, non-`Clone` value, and
`recover_once(self, cancel)` consumes it; constructing the value starts no
loop, task, channel, runtime or store operation. Construction validates and
freezes the owner, reconciler identity, options and adapter registrations.
Adapter names and kinds must be stable and non-secret, each `ControllerKind`
has at most one adapter, and an adapter receives only the exact body-free
`ControllerRef` plus a monotonic deadline.

The pass calls `claim_recovery_due` on Tokio's blocking pool, validates the
complete returned batch before any adapter is polled, and processes it without
spawning per-item tasks. Hard admission caps are 64 claimed tickets, 16
concurrent adapter futures, 60 seconds per adapter timeout and five minutes per
recovery-operation lease. The operation lease must be strictly longer than the
adapter timeout plus settlement margin. Before entering the blocking claim the
driver derives a conservative caller-local monotonic pass deadline from that
configured lease. Claim latency is therefore deducted, and each adapter starts
only if the same monotonic deadline can still fit its timeout and settlement
margin. Ticket wall-clock expiry remains solely a durable store fence; a custom
store clock cannot extend the driver's effect-admission window.

Controllerless tickets may go directly to settlement. Otherwise only
`ControllerRecoveryObservation::Gone` authorizes
`confirm_controller_gone`, which also runs on the blocking pool. `Gone` means
the exact controller is absent or was synchronously stopped and its exit was
observed; still-present, unavailable, missing-adapter, timeout, panic,
cancellation and insufficient-window results do not settle. Every adapter must
revalidate the complete controller identity immediately before each effect,
must return `Unavailable` without an effect when it cannot exclude identity
reuse, and must be repeat-safe or reconcilable after timeout, caller drop or
settlement failure. Reports, errors and `Debug` are body-free and omit run,
worker, controller, operation, ticket/token, raw store error and panic payload.

Cancellation observed before claim is non-mutating. After claim, it suppresses
buffered adapter calls that have not started, while calls already being polled
run only to the shared monotonic timeout and an affirmative result may still be
settled. Dropping the outer future is explicitly not graceful: Tokio cannot
abort a running custom-store blocking call, settlement may complete after its
async waiter is gone, and a non-abortable adapter operation can continue after
future timeout/drop. Such adapter work must be independently bounded,
exact-identity-safe and retry-safe; the async timeout is not proof that its
external effect stopped.

Standing alone this one-shot driver is not a resident supervisor. WP-51 composes
it only with the paired in-process backend; there is still no Process/Remote
adapter, session-aware resume, production factory/service/CLI/daemon assembly,
controller/message handback, live pause/resume or automatic replay. See
[WP-45](plan/WP-45.md).

### One-shot newly-due execution seam

`vyane-service::AgentRunExecutionDriver` is a separate fixed-owner,
non-`Clone`, consuming one-shot driver for newly due AgentRuns. Its monotonic
pass base starts before `claim_due`; the whole synchronous claim runs on
Tokio's blocking pool and is fully admitted before item processing. A pass is
capped at 64 claims and 16 concurrent item polls. The operation lease is at
most 300 seconds, and heartbeat cadence is between 100 milliseconds and 60
seconds while remaining strictly below the lease.

Before durable start, each item receives driver-generated, domain-separated
prospective controller material: an independent 256-bit identifier and 256-bit
fingerprint. The required transition order is claim, start exact controller,
issue exact `ActiveExecutionPermit`, complete a pre-effect heartbeat, then first
poll of the trusted executor. Permit possession is not blanket authority: the
executor contract requires fresh atomic validation at every external-effect
linearization point. The driver validates every custom-store transition against
the complete expected identity, state, fence and returned receipt.

One item future is the single writer of its current receipt, so
heartbeat renewal and terminal settlement cannot race stale receipt copies.
Only `AgentExecutorOutcome::Quiesced` proves that effects have stopped and
allows the driver to initiate closed settlement. Before that proof,
cancellation, timeout, panic, future drop, `Unknown`, and heartbeat failure
authorize no new settlement and can leave the run nonterminal in `Starting` or
`Running` for WP-45 recovery. A blocking settlement call, once started, is
fully awaited by a live driver but cannot be interrupted and may outlive a
dropped waiter. A custom store can mutate-then-error, so settlement failure is
reported as uncertain rather than treated as proof that no mutation occurred.

This is not a resident loop or production assembly. No concrete executor or
controller recovery adapter is supplied, and it does not compose session-aware
authority, message correlation/handback, live resume or automatic replay. See
[WP-47](plan/WP-47.md).

### Paired in-process execution and recovery backend

`InProcessAgentComponents` closes one narrow concrete adapter gap by binding an
owner-scoped structured operation, the same `AgentStore`, and one paired
`InProcess` execution/recovery backend. The components admit only one live
backend per owner process-wide, regardless of a competing store pointer. That
backend binds one store and operation and constructs both one-shot drivers from
the exact same state.

The backend registry matches an exact controller id and fingerprint. Recovery
signals only that exact controller's cancellation token and observes exit via
`Notify`; fingerprint mismatch cannot signal a replacement. An exact absent
observation installs a tombstone atomically against late registration. The
registry permits at most 4096 retired pairs and rejects same-id reuse or
capacity exhaustion without guessing controller state. Successful durable gone
confirmation reclaims the exact tombstone through the same adapter; failed or
uncertain confirmation retains it. A late registration after reclamation must
pass another durable permit validation while registered and before operation
code can run.

The operation resolves private input after durable `Running` admission. It
receives a lifetime-bound, non-`Clone`, non-serializable authority whose
single-use proof is minted only after a fresh blocking-pool permit revalidation
for each model/tool/other effect. The operation contract prohibits detached
work and requires future drop to synchronously end ownership of every effect.

This backend supports neither `Process` nor `Remote`. There is no concrete
operation, message input/correlation/handback, production service/CLI/daemon
assembly, or session-aware resume. See [WP-48](plan/WP-48.md).

### Resident paired in-process supervision

`InProcessAgentComponents::into_resident_supervisor` consumes the exact paired
owner/store/backend into a non-`Clone` `ResidentInProcessAgentSupervisor`.
Its consuming `run` future polls newly due execution and stale-controller
recovery in independent loops; each loop constructs and completely awaits one
existing bounded pass at a time. Empty batches wait, full healthy batches yield,
and degraded items, returned errors and caught panics use capped exponential
backoff. The supervisor creates no task, channel, runtime, payload queue or
replay policy, and reports only saturating body-free counters.

The host token is a drain signal, not AgentRun cancellation. It prevents a new
pass and interrupts a scheduling wait, while a pass already started receives an
independent token and drains. Forced drop forfeits that guarantee, and custom
blocking store calls prevent a fixed wall-clock drain promise. The recovery
loop never calls `enqueue_resume`; a resume-eligible interruption remains
interrupted. This is a dark library composition, not a concrete product
operation or production host. See [WP-51](plan/WP-51.md).

## Transactional message truth

`vyane-message` is the owner-scoped source of truth for messages and their
deliveries. It is deliberately independent from `vyane-task`: task rows remain
bounded, secret-free execution-control metadata, while immutable message bodies
and mutable delivery state live in separate SQLite tables. `vyane-broker` and
`vyane-ledger` event streams are consumers or projections of this store, never
a second message truth.

Enqueue is idempotent within `(owner, producer, idempotency_key)`. One message
can fan out to several delivery mailboxes; a claim over one or several selected
mailboxes preserves strict FIFO order across the eligible set in one SQLite
transaction. Delayed availability, delivery TTL, lease generation/token
fencing, renewal, reclaim, delivered, ack, retry/permanent nack, cancel, and
atomic reply-and-ack transitions are persisted with restart-safe contract
tests. Every transition also writes a body-free event in the same transaction.
Projection acknowledgement is tracked separately per projector, so an EventLog
projector and a broker projector do not share or overwrite progress.

External adapters can persist a batch of provider receipt IDs with the local
delivery transition and can reverse-resolve a receipt by transport, account,
destination, and external ID. This records and reconciles an effect already
observed at the remote provider; it does **not** make `remote send → local
SQLite receipt` distributed exactly-once. An adapter must use the delivery's
stable provider idempotency key (or an equivalent provider reconciliation key)
for the remote call and reconcile an uncertain outcome before retrying. A
provider with neither idempotency nor reconciliation remains at-least-once
across a crash between remote success and the local commit.

`vyane-broker` adds one-shot, owner-bound `publish`, bounded `pump_once`, and
`maintenance_once` operations. It rejects adapters that cannot promise stable
idempotency or reconciliation, never gives an adapter its lease token, catches
panic/timeout without guessing a settlement, and refuses to call an adapter
when delivery TTL shortened the actual lease below the configured execution
window. Retry/dead-letter reports are derived from the store's returned state,
not from the requested action. Reply creation and acknowledgement use the
message store's transaction. These remain bounded one-shot operations; only an
explicit caller or the resident library driver below repeats them.

The broker's message lifecycle projector consumes each projector's outbox in
sequence, durably appends the original stable event id, then marks that outbox
row projected. Append success followed by a lost projection acknowledgement can
therefore repeat the same event id; EventLog consumers must deduplicate it. The
projection excludes message body/payload, lease tokens, receipts, account and
destination scopes, and caller-controlled route, endpoint and conversation
identifiers. Only internal opaque message/delivery ids and bounded lifecycle
metadata cross into EventLog.

The AgentRun lifecycle projector follows the same append-then-mark and stable-
identity rule over `vyane-agent`'s transactional outbox, but maps only bounded
worker/run identity, revision, lifecycle/state, kind and source sequence. It
excludes prompt/target/policy data, logical sessions, task/trace ids, native
session material and raw bodies. Both projector types expose one-shot bounded
operations and start no background work by themselves.

### Explicit resident broker driver

`vyane-broker::ResidentBrokerSupervisor` is an explicit, non-`Clone` library
driver over those bounded primitives. Construction binds one `MessageBroker`,
one message projector, one AgentEvent projector and zero or more delivery lanes
to the same exact owner scope. The broker and message projector must also share
the same `Arc` message store. Lane ids are unique, mailbox sets are disjoint,
and every lane freezes its claim, lease, replay-safe adapter and pump policy
before `run` can begin.

`run(self, cancel)` concurrently polls all delivery lanes, maintenance, message
projection and AgentEvent projection. Batch sizes, aggregate delivery
concurrency, schedule delays and capped exponential error backoff are validated
up front. Every loop catches its own operation error or panic, reports only
body-free saturating counters, and backs off independently, so a broken lane or
projector does not stop unrelated progress. Busy loops yield and idle loops
sleep; there is no detached `spawn`, channel, internally discovered runtime,
global environment state or second pending queue. The embedding caller owns the
Tokio runtime, future and cancellation token.

Cancellation is a boundary between cycles, not an interruption inside a store
or adapter operation. Once a loop observes cancellation it starts no new cycle;
an already-started bounded cycle, including adapter settlement, is awaited
before the exit report is returned. An embedding application may add a wider
outer timeout, but dropping the run future forfeits graceful drain because an
already-running blocking store call can outlive its async waiter.

This is not yet a completed collaboration surface. `vyane-service` can open the
owner-bound message components and AgentRun projection components explicitly,
and `vyane-agent` provides the durable queue/topology store. An embedding caller
can explicitly assemble those compatible pieces into the broker driver, but no
service operation, CLI command or workflow daemon does so in production. The
driver is not an AgentRun execution or recovery supervisor and supplies no
controller/message handback glue, public execution API, A2A/Channels adapter,
live pause/resume or automatic replay.

## Durable task control

`vyane-task` is the shared control-plane ledger for REST async tasks, CLI
detached tasks, and daemon-owned workflows. `$VYANE_DATA_DIR/tasks.sqlite3`
contains only bounded metadata: task/owner/target identifiers, a task or
submission digest, lifecycle timestamps, controller identity, revision,
executor epoch, ledger link, and a closed failure code. Prompt/system/session
content, arbitrary labels, credentials, output, workflow source, variables,
and raw errors are structurally absent.

Every state change appends an event and updates its snapshot in one SQLite
transaction. Writers compare both `revision` and `executor_epoch`, so a stale
worker cannot settle a controller claimed by a newer owner. The canonical
lifecycle runs from `queued` to `running`, optionally through `cancelling`, and
then to `succeeded`, `failed`, `timed_out`, `cancelled`, or `interrupted`.
`queued` is durable before spawn, so a create-before-worker crash remains
visible and explicitly cancellable instead of disappearing. Age alone is not
treated as proof of worker loss: list/status reads mutate a task only after
affirmative dead or identity-mismatch evidence.

Schema v2 makes `(owner,id)` the snapshot key and gives task events the same
composite foreign key and revision uniqueness. Owner is a separate store
authority argument rather than a `NewTask` payload field. The v1 migration runs
under the immediate writer transaction, validates the exact source/target
manifests, preserves task/event counts and event-sequence high-water, checks
orphans and foreign keys, and rolls back on mismatch. Identical ids across
owners therefore remain isolated for reads, events, pagination, leases and CAS.

For a **new CLI `--detach` submission**, the parent completes capability and
session admission before creating the durable task row or spawning a worker. It
freezes a secret-free, execution-id-independent capability snapshot alongside
the existing target/config snapshot. A mutating Linux submission also
duplicates the already-open directory into fixed worker descriptor 7; the
worker rebuilds `PinnedWorkdir` from that descriptor, checks its device/inode
against the frozen identity, independently re-resolves every target/manifest,
compares the complete capability plan, and repeats session admission. It never
reopens the audit pathname. The built-in harness layer later duplicates the
same object to its own fixed descriptor and establishes cwd through that descriptor before exec.

The detached task id remains control-plane identity. The worker allocates a
fresh kernel execution id for the real dispatch and records it as
`ledger_run_id`; those ids are correlated, not required to be equal. Pre-envelope
`job.json` remains a read-only compatibility input with optional/defaulted new
fields. That legacy path can be read and executed under its historical contract
but is not evidence that every old job carried the new parent-side capability
snapshot or inherited directory descriptor.

REST keeps cancellation tokens and opaque Tokio task handles only in memory,
keyed by task id and executor epoch. `vyane serve` binds its socket, takes a
per-data-dir advisory supervisor lock, generates a fresh 256-bit bearer token,
and atomically publishes it to a private `serve.token` (mode `0600` inside a
mode-`0700` data directory on Unix) before opening the task store and marking
abandoned REST tasks interrupted; any startup failure
removes that exact token generation. Every route requires the bearer and a
loopback Host/Origin authority; cross-site browser requests are rejected to
close DNS-rebinding paths. It never replays payloads. Completed dispatches retain their
runtime ownership while exact settlement retries with bounded backoff. On
shutdown it closes admission and drains or exact-owner-interrupts background
dispatches before releasing that lock. Even if SQLite fails during drain, a
finally path cancels tokens, aborts and awaits every tracked future, then
returns the metadata error, so a replacement supervisor cannot race old work.
On non-Unix systems the caller must keep `VYANE_DATA_DIR` under a
platform-managed user-private directory; the process does not replace the
platform ACL on a caller-selected shared root.
CLI workers receive their one-shot request over piped stdin, record an exact
process-birth fingerprint, and keep only mode-0600 `task.log`/`output.txt`
artifacts plus an ephemeral mode-0600 nested-harness controller and its
mode-0600 advisory lock. Lock acquisition has a 500 ms budget, so a stopped
lock holder cannot suspend cancellation indefinitely. Started/Stopped updates
are serialized so an old Stop or reused PID/PGID cannot erase a newer birth
identity, and a new Started refuses to overwrite an unresolved controller from
an earlier failover attempt. A lifecycle-controlled
CLI waits behind a parent-held stdin start gate: the real executable cannot run
until `Started` is durably published, and parent death closes the gate without
launching it. The controlled shell remains the exact process-group leader while
the real CLI runs as its child. On target completion it sends the real exit code
through a private status pipe and kills its own group, including closed-stdio
descendants that remain in that group, before `Stopped` may remove the sidecar.
The sidecar contains only the sentinel's PID/PGID/start/birth identity. It lets an independent canceller
reach the harness's separate
process group even if the outer worker is stopped or has crashed. A status read
does not make an outer-dead row terminal while a nested group may still be
live, and explicit cancel consumes a verified sidecar even when metadata is
already terminal. Linux controller checks use the kernel birth fingerprint as
authority and therefore do not depend on wall-clock stability or `ps` in PATH.
The sentinel remains the non-reusable generation proof for the lifetime of the
target group; it never hands authority to an ordinary descendant that could
change groups or close an inherited descriptor. If that exact sentinel is dead
while the numeric PGID still appears live, control fails closed rather than
risk signalling a reused group. A
terminal detached row also does not suppress process control: explicit cancel
kills a still-exact outer controller so stopped workers can reap killed nested
children. Detached output uses unique-temp atomic replacement, so an I/O failure
cannot expose a partial success artifact.
REST successes likewise keep output outside SQLite as a mode-0600 artifact
exposed by `/v1/tasks/:id/output`. New output paths contain only
domain-separated SHA-256 owner/task segments. A read-only compatibility lookup
of the previous raw-id path is limited to a proven successful local REST row
with a UUID-parseable id. Pre-WP-39 `job.json` and `status.json`
directories remain read-only
compatibility inputs and are never a second truth source for new tasks.
Detached cancellation revalidates the outer worker before graceful TERM. It
gives nested cleanup and outer ledger/SQLite settlement separate grace windows,
then revalidates both outer-worker and current nested-harness identities before
any forced KILL. A gated, published and still-exact sentinel authorizes cleanup
of its process group; a group with no surviving sentinel authority is refused
after its leader disappears rather than risk signalling a reused process-group
id.

The sentinel is a crash-recovery/control mechanism, not a same-UID security
boundary. A deliberately hostile child running as the Vyane user can signal its
ancestors or modify that user's private files; workloads requiring protection
from such behavior need an OS identity, container, VM, or equivalent sandbox
boundary. Vyane fails closed if its exact sentinel is destroyed and never turns
that loss into a blind numeric-PGID signal.

## Resident workflow daemon

`vyane daemon run` owns managed workflow execution in the foreground;
`daemon start` launches the same supervisor in a new session, waits for an
authenticated readiness check, and then lets the submitting terminal exit.
`daemon status` requires both the exact recorded PID/PGID/start identity (plus
the Linux birth fingerprint) and a matching authenticated health response.
`daemon stop` revalidates that exact identity before TERM or KILL, so a stale
descriptor cannot signal a reused process. `vyane serve` and `daemon run`
intentionally contend for the same per-data-directory supervisor lock and
cannot mutate shared task ownership at the same time.

The daemon listener is loopback-only. Every endpoint, including `/health`,
requires a fresh per-start 256-bit bearer token. The mode-`0600` descriptor
contains the loopback address and exact process/instance identity; the token is
kept in a separate mode-`0600` file, and the data directory is mode `0700`.
Control is published only after the socket is bound, the supervisor lock and
task database are open, abandoned work has been recovered, the router is
built, and shutdown handlers are installed. The router does not install a
permissive CORS layer, has a fixed request-body bound, and returns bounded error
bodies that do not echo sources, variables, credentials, or raw model output.

The managed workflow command and HTTP surfaces map directly:

| CLI | authenticated daemon endpoint | operation |
|-----|-------------------------------|-----------|
| `vyane workflow submit <file> [--var k=v] [--id UUIDV7] [--json]` | `POST /v1/workflows` | validate and durably admit one workflow |
| `vyane workflow status <id> [--json]` | `GET /v1/workflows/:id` | read the daemon task plus available journal detail |
| `vyane workflow cancel <id> [--json]` | `POST /v1/workflows/:id/cancel` | durably request cancellation, then signal the matching live generation |

Submission is daemon-only and never falls back to foreground execution. The
client first verifies the daemon descriptor and bearer-authenticated health,
then reads the TOML and every declared `prompt_file` below the workflow file's
canonical directory. It rejects symlink escapes, traversal, absolute or
non-portable bundle paths, missing/extra/duplicate entries, non-regular files,
and non-UTF-8 content. The daemon materializes only the in-memory bundle and
never resolves a client path against its own filesystem. Source hashes use a
domain-separated, length-framed v1 encoding over the TOML and prompt entries in
deterministic path order. Exact legacy journal hashes can be accepted once and
atomically migrated; new journals always carry the v1 hash.

| submitted field | semantic limit |
|-----------------|----------------|
| workflow TOML | 1 MiB |
| each prompt | 4 MiB |
| complete source bundle | 16 MiB, including TOML and prompt paths/content |
| prompt entries / path | 128 entries / 4,096 bytes per path |
| workflow variables | 128 entries; 256-byte key; 1 MiB value; 4 MiB total |
| execution working directory | for a new submission, canonical absolute UTF-8 directory; at most 4,096 bytes |

The execution working directory is the client's canonical current directory at
submission time. A missing step `workdir` becomes that directory, a relative
one is joined to it, and an absolute one is preserved. It is part of the
submission identity along with the normalized source hash and variables sorted
by key.

The client generates a canonical lowercase UUIDv7 unless `--id` supplies one,
and writes and flushes that id to stderr before POST so ambiguous transport
failure can be reconciled. The workflow task id and journal id are exactly this
value.
An exact retry with the same id, daemon workflow scope, source, execution
directory, and variables returns the existing task without replay; a different
submission under that id returns a conflict. The equality check happens before
current target validation, so configuration drift does not prevent a client
from recovering the existing task view.

For a new id, validation precedes durable creation. The daemon then creates a
`queued` row, attaches its exact in-process controller and 30-second lease,
writes the initial journal, publishes the matching in-memory cancellation
token, and only then releases the workflow start gate. Lease renewal runs every
10 seconds and uses revision, executor epoch, owner, and unexpired lease checks.
Harness steps publish a set of exact PID/PGID/start/birth controller sidecars,
because fan-out can own several harness groups at once; cancellation and
recovery signal only identities that still verify.

On startup, the daemon pages through active `(owner=local, kind=Workflow,
origin=Daemon)` rows, best-effort cleans every exact nested controller, and
marks each abandoned row `interrupted` with `WorkerLost`. It does not resume a
journal or replay a source bundle. On cooperative shutdown it closes admission
as soon as the signal is observed, gives in-flight HTTP requests up to 10
seconds to finish, durably requests cancellation, signals live tokens, drains
workers/controllers/watchers, and performs a final exact interruption pass
before releasing the supervisor lock. The cooperative HTTP and workflow phase
budgets total 68 seconds; `daemon stop` revalidates identity and escalates after
75 seconds if a safety wait has not completed.

This bearer token and owner-only files reduce accidental local access; they do
not isolate mutually hostile processes running under the same OS user. The
daemon is not a remote or multi-user boundary.

## Non-goals

Explicitly out of scope for the current release, to keep the core honest:

- **No native harness or host-enforced sandbox yet.** Whole-chain capability
  admission and Linux pinned-workdir identity are implemented, but
  `AdapterDelegated` is not same-UID confinement, a container or a microVM.
  Mutating dispatch fails closed outside Linux. Guarded model-send and tool-
  registry entry points can consume an abstract live authority, and a concrete
  bridge now revalidates `ActiveExecutionPermit` plus the AgentRun store for
  fresh sessionless model/tool effects. A bounded serial turn driver consumes
  the abstract authorized model/tool boundaries, but neither component is
  wired into a production factory/runtime or registered as a `Harness`. The
  bridge rejects session-bearing, checkpoint and session-commit authority;
  trusted built-in tools and approval resume are also absent. The strict
  `NativeSessionDomain`
  persistence contract and
  reset/fresh-fork/commit CAS data contract exists, but neither legacy-unbound
  nor domain-bound native harness state is resumed. Regular dispatch rejects
  those native states before executor construction; streaming rejects any
  session before load/probe/construction. The local filesystem execution lease
  is implemented for regular dispatch and control mutations; session-aware
  production authority assembly, distributed lease/fencing protocol, public
  fork, REST mutation, and production resume remain absent.
- **No automatic daemon replay or live pause/resume.** The resident daemon owns
  admitted workflows after the client exits, but it does not persist a replay
  payload in SQLite, resume a live daemon task, or automatically re-execute a
  journal after restart. Recovery cleans exact controllers and marks abandoned
  work interrupted. Foreground `workflow resume` remains explicit.
- **No learned routing.** Target selection is deterministic (complexity
  scoring + tag/tier/stage preferences), not adaptive. History-based or
  feedback-driven routing is a future direction.
- **No model proxy.** Vyane dispatches to providers and harnesses; the REST
  API is a control surface for dispatch/broadcast/query, not a transparent
  HTTP proxy in front of your model endpoints.
- **No GUI.** CLI, REST API, and MCP only.
- **No interactive/bidirectional harness streaming.** Claude Code and Codex CLI
  stdout events are observable, but Vyane does not answer approval prompts or
  send mid-run input through this API. Sessions also remain on the ordinary
  non-streaming dispatch path.
- **No multi-user or hostile same-UID REST authority.** `vyane serve` rejects
  non-loopback bind/Host/Origin and cross-site browser requests, and every route
  requires a fresh per-start bearer published in a mode-`0600` token file. Run
  and session responses are allowlisted public views. A malicious process under
  the same OS identity can still read that token, so this is a local single-user
  capability boundary rather than tenant isolation. The separate workflow
  daemon uses its own per-start bearer and also rejects non-loopback binding.

## Roadmap

See [ROADMAP.md](ROADMAP.md) for the milestone breakdown of v0.1 through v0.4.
