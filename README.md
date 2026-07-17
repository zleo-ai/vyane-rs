# Vyane

**A multi-model agent-orchestration kernel, in Rust.**

[![CI](https://github.com/zleo-ai/vyane-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/zleo-ai/vyane-rs/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)
&nbsp;·&nbsp; [简体中文](README.zh-CN.md)

Vyane is one kernel — and one CLI over it — for dispatching, broadcasting and
failing over tasks across *both* coding-agent harnesses (Claude Code, Codex
CLI, …) and raw HTTP model endpoints. Point it at a task and a target and it
runs it; point it at several targets and it fans out or fails over. Whether a
run happens inside a coding CLI with a filesystem and tools, or as a plain
chat completion over HTTP, is one field on the target — not a different tool,
a different config file, or a different mental model.

The differentiator is the **four-layer target model**. *Provider*, *protocol*,
*harness* and *model* are independent axes and are never conflated. A provider
is who supplies the endpoint, key and billing; a protocol is the wire format; a
harness is the execution shell (or none, for direct chat); a model is the
inference model. So: **a relay is not a protocol** (it is a provider that
happens to speak one), **a coding CLI is not a provider** (it is a harness that
still needs one), and **a model id is only valid within one provider**. These
stay four separate fields from configuration all the way into the run ledger,
which is what lets Vyane do the correct thing at the boundaries where lazier
tools quietly break.

Concretely, that buys you: **clean-env subprocess spawning by construction** —
child agents are launched from a scrubbed baseline environment, so the calling
session's credentials and base-URL overrides never leak into them; **failover
chains that never leak a model id across providers** — each element of a chain
is resolved fully and independently, so a fallback always uses the model that
belongs to the provider it runs against; **whole-chain capability admission** —
`Write`/`Full` cannot silently run on a chat-only target, and Linux mutating
runs retain the admitted directory object across child spawn; and an
**append-only JSONL run ledger** that records prompt *digests* (not prompt
bodies), the full attempt
trail of every dispatch, and **owner-scoped records from day one** so
multi-user isolation never needs a schema retrofit.

## Origin

`vyane-rs` is the open-source Rust implementation of Vyane, a private personal
AI-OS execution substrate that has been running real multi-model development
pipelines since early 2026. It is being rebuilt here in the open, capability by
capability, tracking the private system as it evolves — so the design reflects
what actually held up in daily use, not a greenfield guess.

## How it's built

This repo is developed by an orchestrated fleet of AI coding agents: different
frontier models write the code, adversarially cross-review each other's work,
and fix what the review turns up, all under a human-owned architecture and
integration gate. Every merge passes independent cross-model review plus the
`cargo fmt` / `clippy` / `cargo test` gates described in
[CONTRIBUTING.md](CONTRIBUTING.md) — no change lands on the say-so of the
model that wrote it.

## Status

**The repository-local v0.1 through v0.3 milestones and v0.4 implementation
scope are delivered. External crates.io publication (to Rust's public package
registry) is a deferred distribution action, not a feature milestone, and
requires separate authorization; a tag or registry token is not that
authorization. The manual release workflow additionally requires a protected
`crates-io` environment approval by a reviewer other than the dispatcher and
requires the supplied release tag, current `main`, and workflow SHA to identify
one exact commit. The registry token is exposed only to the final publish step.
The 17-crate local package preflight passes, but no crate has
been published. This does not mean full parity with the original private Vyane
system.** In the current public integration baseline, the fixed cross-repository matrix
tracks 53 capabilities across eight domains: 7 implemented, 21 partial, 14
missing, 9 deliberately different or awaiting a decision, and 2 planned. It
records substantial native-harness, continuity, collaboration, governance,
observability, and interface work still to do. Unqualified “full parity” means
whole-system capability parity: private credentials and deployment details stay
out of this public repo, but their generic contracts and optional/private
adapter boundaries still need verifiable coverage. Live daemon pause/resume and
automatic restart replay are two explicit daemon limits, not the whole remaining
parity backlog. See the
[original-Vyane parity baseline](docs/parity/ORIGINAL-VYANE-PARITY.md). APIs
remain unstable pre-release; the CLI runs real dispatch/broadcast/failover
today. The public parity manifest now contains 25 sanitized cases across
classifier, failover, and automatic-routing suites; 15 match after their
declared normalization and ten remain explicit open differences or one-sided
Rust scope differences. Run `python3 .github/scripts/parity-report.py --format
markdown` to recompute current Rust behavior, validate the stored sanitized
attestation, and print an offline report. It does not execute the private
reference implementation.

| capability | crate | state |
|------------|-------|-------|
| core type system (four-layer model, traits, errors, env policy, process-local workdir pin) | `vyane-core` | [x] includes the non-serializable live native-side-effect authority contract |
| config & profiles | `vyane-config` | [x] |
| OpenAI-Chat + Responses + Anthropic-Messages clients | `vyane-protocol` | [x] baseline clients; [~] bounded typed tool turns and the per-wire authorized path currently cover non-streaming OpenAI Chat only |
| Claude Code + Codex CLI harnesses, including stdout event streaming | `vyane-harness` | [x] additive scoped execution carries the Linux pinned workdir and an optional live spawn authority; the Process AgentRun host constructs that authority for fresh sessionless CLI runs, and gated capture/streaming revalidate before wrapper spawn and real-target release. This remains adapter-delegated rather than a host sandbox |
| native permission/tool execution seam (not yet a `Harness` implementation) | `vyane-harness` + `vyane-service` | [~] atomic AgentRun scope validation, per-wire model authorization, an allowed-tool registry gate, a fresh-sessionless permit/store bridge, bounded serial turn driver, lifetime-bound in-process native-scope composition, and a generic crash-consistent completion handback boundary exist as dark components. [WP-65](docs/plan/WP-65.md) composes a private-spool, exact fresh-sessionless, tool-free OpenAI Chat operation and durable message-completion E2E; it remains dark and is not registered with a daemon or public API. Session-bearing authority, trusted built-ins, OS sandbox, checkpoint/session commit, approval resume, failover/replay and native resume remain absent |
| dispatch / broadcast / failover kernel | `vyane-kernel` | [x] early execution id, whole-chain trusted capability admission, one-shot prepared dispatch and original-ordinal failover evidence |
| append-only run ledger + owner-isolated session records | `vyane-ledger` | [x] direct-HTTP transcript continuation plus strict revisioned V2 snapshots, store-level CAS `Reset` / `ForkFresh` / `Commit`, and an exact local-filesystem execution-period lease; CLI/service control is limited to owner-local list/inspect/reset-native, with no public fork, REST mutation, distributed lease protocol, or production native resume |
| replayable owner-scoped event store | `vyane-ledger` | [~] storage/cursors, bounded message and AgentRun lifecycle projection, and owner-bound resident broker/projector assembly in the daemon now exist; delivery lanes, dispatch/workflow producers, subscription, retention and a unified timeline remain |
| durable, secret-free task metadata | `vyane-task` | [x] schema v2 keys snapshots, events and CAS by `(owner,id)` with transactional v1 migration; built-in frontends still select explicit `local` |
| durable owner-scoped AgentRun queue, worker topology and recovery truth | `vyane-agent` | [~] exact leases/deadlines, active permits, bounded tree cancel, body-free completion receipts/outbox, and resident execution/recovery/publication are production-assembled for a narrow Linux `Process` path with an authenticated loopback submit/status/output/cancel API. `Remote`, native production execution, sessions/resume, distinct principals, live pause/resume, and automatic replay remain absent |
| owner-scoped transactional message/delivery store | `vyane-message` | [~] multi-mailbox strict FIFO, delayed/idempotent delivery, fenced leases, TTL, ack/nack, body-free outbox, external-receipt reconciliation, hidden staged completion publication, bounded mailbox pages, and exact mailbox claim exist |
| owner-scoped goal lifecycle and progress truth | `vyane-goal` | [~] one SQLite transaction updates the current snapshot and appends an immutable event; WP-68 adds bounded local acceptance verification, WP-69 preserves immutable evidence, WP-70 adds explicit bounded manual pursuit through fresh production dispatch segments, WP-71 adds durable lease-fenced restart checkpoints, WP-72 composes opt-in single-goal automatic pursuit into the resident daemon, WP-73 adds typed policy plus visible idempotent quota-handoff state, WP-74 adds durable explicit approval plus exact-boundary one-shot takeover execution, and WP-75 binds review to exact successful takeover evidence before a quota-reset-gated handback; external quota ingestion, primary resume and an authenticated goal service remain future layers |
| bounded replay-safe delivery broker + body-free EventLog projectors | `vyane-broker` | [~] fake-adapter contracts, message/AgentRun lifecycle projection with stable source event IDs, and the explicit non-`Clone` `ResidentBrokerSupervisor` are assembled into the resident daemon with an intentionally empty delivery lane set; worker/message glue and remote A2A/Channels adapters remain absent |
| declarative workflow engine (DAG + journal/resume/replay) | `vyane-workflow` | [x] exact-plan replay creates a new run and reuses a journal-recorded all-success prefix |
| resident workflow and Process AgentRun daemon (authenticated local control) | `vyane-cli` | [x] workflow control plus fresh sessionless CLI-harness AgentRun submit/status/output/cancel on Linux; no automatic replay or live pause/resume |
| detached background runs (`--detach` + `task` commands) | `vyane-cli` | [x] |
| CLI (check / dispatch / broadcast / history / session / sessions / workflow / task / daemon / a2a / goal) | `vyane-cli` | [x] revision-aware session control, local `a2a send/inbox/read`, and owner-scoped `goal` lifecycle/progress commands; legacy `sessions` remains compatible |
| shared service layer | `vyane-service` | [x] `OwnerContextFactory` authenticates and resolves a reserved-local-safe authority; `OwnerScopedService` freezes dispatch/stream/query/session/reset. AgentRun components include prepared authorized harness dispatch, paired backends, exact message-completion handback, and the generic resident supervisor used by the daemon's Linux Process host; ordinary dispatch starts none of them |
| **REST API** (`vyane serve` — dispatch/broadcast/runs/sessions/health) | `vyane-cli` + `axum` | [x] per-start bearer capability, loopback Host/Origin enforcement, non-loopback bind rejection, allowlisted views, and one assembly-frozen local service scope; the bearer still is not a distinct principal or hostile same-UID/multi-user boundary |
| **MCP server** (`vyane mcp` — nine tools) | `vyane-mcp` + `rmcp` | [x] six base tools plus authenticated durable workflow submit/status/cancel; generic success output has a 1 MiB cap |
| pluggable routing | `vyane-router` | [x] |
| solution-review workflow (implement → fan-out review → synthesize) | `vyane-cli` (review module) | [x] not yet the original system's structured git diff/PR review product |

Capability admission is deliberately narrower than a sandbox. `ReadOnly` works
with chat or harness targets. `Write`/`Full` requires an existing workdir and a
trusted built-in Claude/Codex CLI editing manifest; direct HTTP and unknown
adapters are rejected before executor construction. Mutating dispatch currently
fails closed outside Linux. The pinned descriptor prevents workdir path
replacement, but does not confine a hostile same-UID child or absolute-path
access. The exact `NativeSessionDomain` storage contract and store-level CAS
transitions now exist, but they are evidence rather than resume authority.
Regular dispatch requires an exact `(owner, session_id, execution_id)` lease,
acquires it before loading continuity, and retains it through model execution
and a revision-CAS completion update. The filesystem store uses an owner-only
advisory lock, serializes direct control mutations against the same authority,
and prevents same-session runs from overlapping: a competitor either acquires
the lease after a bounded wait or receives `Conflict`, always before executor
construction. This is a local crash-released fence, not a distributed
generation/TTL protocol; the
post-model session commit also remains best-effort and must not be described as
strict durable continuation. Regular dispatch still rejects legacy-unbound or
domain-bound native harness state before executor construction. Streaming
dispatch rejects any session even earlier, before session-store load,
capability probe, or executor construction. Pure direct-HTTP transcript
continuation remains available only through regular dispatch. The owner-local
CLI/service exposes list, inspect, and revision-checked reset-native; it exposes
no public fork, REST mutation, or production native resume.

The native authority work remains an incomplete integration seam. A separate
OpenAI Chat typed-turn path revalidates immediately before every explicit wire
send; cancellation remains live through authority wait, send, response-body
read and retry backoff. The shared HTTP client follows no redirects and performs
no implicit retries, so one explicit attempt corresponds to one authority
check. `ToolRegistry::execute_authorized` similarly revalidates only an allowed
call immediately before executor polling; deny/ask/invalid/unknown/cancelled/
expired decisions remain pure and revocation stays outside model-facing tool
text. `AgentRunModelToolAuthority` is now a concrete bridge for a fresh,
sessionless scope: it owns the permit and scope, revalidates the AgentRun store
on Tokio's blocking pool for each one-based model send or tool operation, and
rejects session-bearing scopes, checkpoint effects, and session commits. It is
not registered by a production factory or called by a runtime/native loop, and
it does not combine a session lease with an exact native-session domain.

The paired in-process operation can now bind its lifetime-bound effect
authority to one exact fresh, sessionless `NativeExecutionScope`. Binding first
atomically validates owner/run/generation/lease/deadline/controller plus exact
target, prompt and policy digests; each subsequent one-based model send or tool
operation repeats that full native-scope validation. Session/resume scopes,
checkpoint/session-commit effects, raw store/permit access and cloning or
serialization remain closed. This completes an authority composition seam, not
a concrete native AgentRun operation or result handback. See
[WP-52](docs/plan/WP-52.md).

`NativeTurnDriver` now supplies a separate bounded dark model/tool loop. It
defaults to eight model turns with a hard ceiling of 32, permits at most one
tool call per turn, requires the initial advertised tool-name set to equal the
registry-name set, validates every request/response, and preflights the complete
next transcript with a worst-case bounded tool result before any permission or
tool future can run. Model sends and allowed tools use only the authorized
entry points. Refusal, approval-required, parallel calls, tool-choice
violations, cancellation, timeout and budget exhaustion are typed terminal
stops; usage addition saturates, and post-tool model failure becomes a redacted
non-replayable stop rather than an outer failover error. Invalid JSON arguments
produce static non-echo tool text and never execute. Tool descriptions and
schemas are non-authoritative model guidance; each `NativeTool` must validate
the actual arguments it receives.

The driver's outcome is non-serializable and has redacted `Debug`, but the
driver is not a `Harness` and no factory/runtime constructs it. There are still
no trusted built-ins, checkpoint/session-commit consumers, approval resume, or
native resume. Separately, `AgentProjectionComponents::open` provides an
explicit owner-bound path to the one-shot AgentRun projector while keeping the
raw store encapsulated. Ordinary dispatch neither opens that database nor
starts projection or other resident work.

[WP-65](docs/plan/WP-65.md) provides a dark composition slice: a private native
spool, exact fresh/sessionless scope, authorized OpenAI Chat client, tool-free
`NativeTurnDriver`, and exact durable message-completion acceptance path. It is
not a daemon, CLI, REST, or MCP native
target; it adds no trusted built-ins, sandbox, session/resume/checkpoint or
approval authority, failover, replay, or general production-parity claim.

`vyane-service::AgentRunRecoveryDriver` is another explicit fixed-owner,
non-`Clone` one-shot seam. Construction freezes the owner, injected store,
options and at most one trusted adapter per `ControllerKind`; `recover_once`
consumes the driver. Recovery claim and final confirmation run on Tokio's
blocking pool. One pass is capped at 64 tickets and 16 concurrent adapter
calls, each adapter timeout is at most 60 seconds, and the durable operation
lease is at most five minutes and must be strictly longer than the timeout plus
settlement margin. A conservative caller-local monotonic window starts before
the blocking claim, so claim latency is deducted and a custom store's wall
clock cannot extend adapter authority. Only a controllerless ticket or an
affirmative `Gone` observation for the exact controller can reach
`confirm_controller_gone`; reports and errors are body-free and recovery
tickets never cross the adapter boundary.

Standing alone this one-shot driver is not a resident worker-health or execution
loop. WP-51 first composed the paired in-process backend; WP-61 now uses the
generic supervisor with an exact Linux `Process` adapter in the workflow daemon.
There is still no `Remote` adapter, session-aware resume, live pause/resume, or
automatic replay. Controller adapters must revalidate the
complete identity before every effect, return `Unavailable` without an effect
when identity reuse cannot be excluded, and remain safe to repeat after
timeout, drop, or settlement failure. A custom store's blocking call cannot be
forcibly cancelled, and an adapter timeout bounds future polling rather than
proving a non-abortable external effect stopped. See
[WP-45](docs/plan/WP-45.md) for the exact boundary.

`vyane-service::AgentRunExecutionDriver` complements recovery with a separate
fixed-owner, non-`Clone`, consuming one-shot pass over newly due runs. A pass
admits its whole claim on Tokio's blocking pool and is capped at 64 runs, 16
concurrent polls, a five-minute lease, and heartbeat intervals from 100
milliseconds through 60 seconds that must remain below the lease. Its monotonic
base starts before claim. For every admitted item it generates independent
256-bit prospective controller id and fingerprint material, then orders claim,
durable start, permit issue, a pre-effect heartbeat, and only then the first
executor poll. The trusted executor must revalidate the permit at every actual
effect, and custom stores are checked after every transition.

Only a `Quiesced` closed outcome authorizes the driver to initiate terminal
settlement. A single item future owns and advances the receipt. Before that
proof, cancellation, timeout, panic, drop, `Unknown`, or heartbeat failure
authorizes no new settlement and may leave `Starting` or `Running` for WP-45
exact-identity recovery. Once a blocking settlement call has started it cannot
be interrupted and may outlive a dropped waiter; a custom store can also
mutate-then-error, so a reported settlement failure is uncertain. WP-47 remains
the generic one-shot contract. WP-61 production-assembles it with a fresh
sessionless CLI-harness executor, exact Process controller, and message
handback. That narrow assembly does not add native execution, `Remote`,
session-aware resume, live pause/resume, or automatic replay. See
[WP-47](docs/plan/WP-47.md) and [WP-61](docs/plan/WP-61.md).

`InProcessAgentComponents` now supplies one concrete pairing for those one-shot
drivers. It admits one live backend per owner process-wide, regardless of store
pointer; that backend binds one store and structured operation and mints both
`InProcess` executor/recovery drivers. Exact id/fingerprint matching,
`Notify`-based cancellation/exit observation, and a fail-closed 4096-entry
tombstone bound prevent a late or reused controller from being mistaken for the
one being recovered. Durable confirmation reclaims the exact tombstone; failed
or uncertain confirmation retains it, and a post-reclamation late registration
must revalidate its permit again before operation code runs. Operations receive
a lifetime-bound non-`Clone` effect authority and must consume a freshly
revalidated permit proof immediately before every effect.

`ResidentInProcessAgentSupervisor` can consume this exact pairing into separate
execution, recovery, and completion-publication polling loops. It validates poll/backoff bounds, applies
capped exponential backoff to degraded/error/panic cycles, creates no task,
channel, runtime, payload queue or replay policy, and never automatically
enqueues resume. Supervisor cancellation stops new cycles and is forwarded to
an executor already being polled; the Process backend terminates and reaps its
exact group, records the stopped lifecycle, and the driver awaits the item
before the completion loop's final drain. Forced drop still forfeits that
guarantee, and a custom blocking store means drain has no fixed wall-clock
bound. The generic
`ResidentAgentSupervisor` now also consumes a concrete Linux Process backend in
the daemon; the in-process operation itself remains a dark native seam. The
generic handback contract is defined in [WP-53](docs/plan/WP-53.md), the
original resident boundary is in [WP-51](docs/plan/WP-51.md), and the production
Process scope is in [WP-61](docs/plan/WP-61.md).

`ResidentAgentHost` is the bounded multi-lane service substrate above those
single-lane facades. It runs one exact durable-backend execution loop per lane
while sharing one validated recovery-adapter union and one completion
projector. Lane failures and backoff are isolated, but store ordering remains
cross-backend. The current daemon does not assemble this into native or Remote
production execution; it still installs only its Linux Process lane. See
[WP-64](docs/plan/WP-64.md).

The WP-61 host accepts authenticated loopback AgentRun requests only through
the workflow daemon. It freezes a private create-only prompt spool together
with the exact resolved target chain, capability plan, canonical workdir,
sandbox, timeout, system text and labels. Only fresh, sessionless CLI harness
targets are admitted. The executor revalidates the active permit and frozen
snapshots before wrapper spawn and every real-target release. A private
controller sidecar is durably reserved before `Running`, then advanced to the
exact Linux process identity before target release. Sequential
failover stops the old controller before starting the next. Success requires a
quiesced process lifecycle and typed terminal proof; in particular, Claude
exiting zero without a JSON `result` is a failure. Completion is staged under a
stable key and returned only after exact message publication.

The daemon exposes `POST /v1/agent-runs`, status, output and cancel routes under
its existing bearer and loopback restrictions. Startup performs exact stale
controller recovery without replay. Graceful shutdown first closes admission
and drains admitted submit/cancel initialization, then signals the AgentRun
supervisor and awaits it concurrently with workflow-supervisor drain. An active
Process group is cooperatively terminated and reaped before completion's final
drain. This is not a `Remote` or native production host, does not support
sessions, live pause/resume or automatic replay, and the local bearer still
does not represent distinct principals or hostile same-UID isolation. See
[WP-61](docs/plan/WP-61.md).

The service layer also has a principal-derived owner phase-A boundary.
`OwnerContextFactory` freezes a trusted authenticator and resolver, keeps
`AuthenticatedPrincipal` construction private, rejects authenticated entry to
the reserved `local` namespace, and mints a non-serializable redacted
`OwnerContext`. `OwnerScopedService` freezes that owner into dispatch,
single-target streaming, history, session inspection, and revision-checked
reset. Existing frontends explicitly retain trusted single-user `local`
compatibility; REST freezes all of its dispatch/broadcast/run/session/stream
operations into one such local scope at router assembly, but its bearer does
not yet represent distinct principals. See [WP-49](docs/plan/WP-49.md).

Durable task schema v2 now keys snapshots, events, leases and CAS by
`(owner,id)`, so two owners can safely reuse one task id. Its v1 migration is
transactional and verifies counts, foreign keys, schema and event-sequence high
water before commit. REST outputs use opaque owner/task-qualified artifact
segments with an exact local-UUID legacy read fallback. Built-in task control
still selects only `local`, and the old detached filesystem scaffold remains a
local compatibility subsystem; this is therefore storage isolation, not a
multi-user REST claim. See [WP-50](docs/plan/WP-50.md).

`vyane-broker::ResidentBrokerSupervisor` is a separate explicit, non-`Clone`
library driver. Its consuming `run` future concurrently owns disjoint
owner/store-bound delivery lanes, message maintenance, message projection and
AgentEvent projection. Batch sizes, aggregate delivery concurrency and
exponential error backoff are validated and bounded; one lane's error or panic
does not stop unrelated lanes or projectors. The driver creates no detached
task, channel, runtime or second queue: the embedding caller supplies a Tokio
runtime and cancellation token and must await the future. Cancellation is
observed only between cycles, so an operation already in progress drains its
current bounded cycle; dropping the outer future forfeits that graceful-drain
guarantee.

No service, CLI or daemon production assembly constructs this driver yet. It
does not execute or recover AgentRuns, provide controller/message handback,
implement A2A or Channels, or add live pause/resume or automatic replay. See
[WP-44](docs/plan/WP-44.md) for the exact boundary.

### Protocol entry points

Vyane supports three interchangeable front-ends, all sharing the same
`vyane-service` layer so dispatch semantics are identical:

| protocol | command | use case |
|----------|---------|----------|
| **CLI** | `vyane dispatch --target prod "task"` | interactive / scripted one-shot runs |
| **REST API** | `vyane serve --addr 127.0.0.1:9721` | programmatic access from any HTTP client |
| **MCP** | `vyane mcp` | let other agents (Claude, Codex, …) call vyane as a tool |

The production CLI MCP surface contains nine tools: dispatch, broadcast,
history, sessions, `vyane_route`, `vyane_check`, and authenticated durable
workflow submit/status/cancel. Generic success payloads have a 1 MiB cap;
the two new diagnostics use stricter, smaller bounds. `vyane_route` is a
Rust-side deterministic preview extension, not a same-name parity claim
against the fixed reference baseline. `vyane_check` performs static
configuration analysis only; it does not probe a network endpoint, spawn a
harness, or validate a live credential. Every success result shares the generic
output ceiling, but the legacy execution tools do not yet have the diagnostics'
uniform field-level input budgets. If execution completed but detail exceeded
the ceiling, dispatch/broadcast returns bounded run receipts with
`operation_status="completed"` and `detail_omitted=true`; callers must not retry
those receipts as though execution failed.

Workflow callers provide a canonical UUIDv7 plus a bounded self-contained
source bundle. The MCP layer cannot provide owner, controller, token, or an
execution path. The CLI freezes its own startup directory and re-authenticates
the exact resident daemon for every workflow operation. Responses contain only
the caller id, lifecycle state, and a closed failure code. Explicit step
workdirs and sandboxes above read-only are rejected before daemon contact. This is durable
workflow control, not general task, board, collaboration, or multi-principal
MCP parity.

### Local A2A inbox

The CLI exposes the transactional message store as a same-machine durable
inbox:

```sh
vyane a2a send reviewer --from builder --kind review "ready for review" --json
vyane a2a inbox reviewer --json
vyane a2a read reviewer <message-id> --json
```

`inbox` is non-mutating. `read` requires the recipient mailbox as well as the
message id, then uses the existing fenced claim → delivered → acknowledged
state machine. It writes and flushes the full response before acknowledging;
write failure remains reclaimable, while a crash after flush can produce a
duplicate instead of message loss. Its JSON payload is therefore a pre-ack
`delivered` snapshot; exit status zero confirms the subsequent acknowledgement.
`--delay-seconds`, `--include-future`, `--include-read`,
`--limit`, `--owner` (alias `--owner-user-id`), and `--db` provide explicit
local control. The default database is the standard `messages.sqlite3` below
the Vyane data directory.

This is not an A2A HTTP implementation. It has no Agent Card, discovery,
remote send/get/cancel, SSE, push, or Channels adapter. `--owner` and `--from`
are supplied by the caller: the former is a logical storage scope and the
latter is only a sender label, not authenticated principal authority or
identity. Do not expose this CLI directly as a hostile multi-user service.
See [WP-59](docs/plan/WP-59.md) for the exact boundary.

### Local goals

The CLI exposes an owner-scoped goal lifecycle over one SQLite source of truth:

```sh
vyane goal create --title "Ship a release" \
  --acceptance 'test-passes:cmd:cargo test --workspace' --json
vyane goal next --auto-start --json
vyane goal progress <goal-id> --stage implementation --detail "tests added" --json
vyane goal verify <goal-id> --workdir . --json
vyane goal claim <goal-id> --worker pursuer --json
vyane goal pursue <goal-id> --worker pursuer --target builder --workdir . --sandbox write --json
vyane goal done <goal-id> --summary "all checks passed" --json
```

`create/get/list/next/start/claim/progress/verify/pursue/pause/resume/done/fail/cancel` share stable
JSON success/error envelopes. Every mutation updates the current query snapshot
and appends an immutable revision event in the same transaction. Queue selection
orders priority ascending, then creation time ascending. The default database is
`goals.sqlite3` below the Vyane data directory; `--db` selects another local
file. `--owner` (alias `--owner-user-id`) is a caller-selected storage scope,
not authenticated authority. Foreign and absent ids are intentionally
indistinguishable at the store boundary.

Acceptance `KIND:TARGET` values remain persisted descriptors. `goal verify`
executes only explicit local `cmd:` checks under a canonical workdir, scrubbed
environment, bounded output/time and Unix process-group cleanup, then persists
passing criteria through the existing lease-fenced mutation. The goal must be
`in_progress`; an active lease requires the matching `--worker`. It does not
auto-complete or pursue goals, invoke network/board adapters, coordinate quota
handoff, or expose a goal REST/MCP service. See [WP-60](docs/plan/WP-60.md) and
[WP-68](docs/plan/WP-68.md) and [WP-69](docs/plan/WP-69.md). Each verification
attempt is retained as a private, immutable, digest-checked artifact and is
returned by `goal get --json`; it is evidence, not completion authority.

`goal pursue` is the explicit bounded outer loop: verify, dispatch one fresh
sessionless segment through the ordinary target/failover kernel, then reverify.
It requires and renews the exact active worker lease. A schema-v4 checkpoint
atomically preserves lifetime segment/failure counters, runtime/workdir identity,
and the last run and verification ids with each progress event. After pause and
database restart, an explicitly resumed goal can be claimed by a new worker and
continues from that checkpoint; stale revisions, workers and lease generations
fail closed. Only the pursuer may
complete after all criteria pass. Missing/manual criteria,
budgets, cancellation and repeated failures pause; external lifecycle changes stop. The
manual command does not auto-start a goal, resume a runtime-native session, or
perform quota/approval handoff. The resident daemon can separately opt in to the
same pursuer as described below.
The production CLI currently accepts only the `local` single-user owner so goal
and dispatch ledger authority cannot silently diverge.
See [WP-70](docs/plan/WP-70.md), [WP-71](docs/plan/WP-71.md),
[WP-72](docs/plan/WP-72.md), and [WP-73](docs/plan/WP-73.md).

### REST API

`vyane serve` exposes a JSON HTTP API with streaming (SSE) and async task
support:

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/v1/health` | health check |
| `POST` | `/v1/dispatch` | dispatch a task (synchronous, blocks until done) |
| `POST` | `/v1/dispatch/stream` | dispatch with SSE streaming (deltas as they arrive) |
| `POST` | `/v1/broadcast` | fan out to multiple targets concurrently |
| `POST` | `/v1/tasks` | submit async dispatch, returns task id immediately |
| `GET` | `/v1/tasks` | list durable REST task metadata |
| `GET` | `/v1/tasks/:id` | get one task's durable status and ledger link |
| `GET` | `/v1/tasks/:id/output` | read a successful task's separate mode-0600 output artifact |
| `POST` | `/v1/tasks/:id/cancel` | cancel a running task |
| `GET` | `/v1/runs` | query the run ledger (filter by status/provider) |
| `GET` | `/v1/sessions` | list saved sessions |

SSE streaming events (`POST /v1/dispatch/stream`):

```
data: {"type":"delta","text":"Here is "}

data: {"type":"tool_use","name":"Read","summary":"{\"path\":\"src/lib.rs\"}"}

data: {"type":"delta","text":"a function..."}

data: {"type":"finished","record":{...},"output":"Here is a function..."}
```

Async task lifecycle (`POST /v1/tasks` → poll `GET /v1/tasks/:id`):

```bash
# Use the exact mode-0600 token path printed by `vyane serve`. Feeding this
# header through curl config stdin keeps the bearer out of argv and the process
# environment. The token is removed after a clean shutdown.
TOKEN_FILE='<path printed by vyane serve>'
rest_auth() {
  printf 'header = "Authorization: Bearer %s"\n' "$(<"$TOKEN_FILE")"
}

# Submit
rest_auth | curl --config - -X POST http://localhost:9721/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task":"write tests","target":"openai/gpt-4o"}'
# → {"id":"0195...","state":"running","task_digest":"...",...}

# Poll
rest_auth | curl --config - http://localhost:9721/v1/tasks/0195...
# → {"id":"0195...","state":"succeeded","ledger_run_id":"0195...",...}

# Read the result body after success
rest_auth | curl --config - http://localhost:9721/v1/tasks/0195.../output
# → {"output":"..."}
```

Cancellation first reports the non-terminal `cancelling` state; keep polling
until the kernel records `cancelled`, `succeeded`, `failed`, `timed_out`, or
`interrupted` after process cleanup and ledger settlement. Task responses never
embed the prompt, output, or raw error; use `ledger_run_id` to correlate the
bounded task record with the run ledger.

The server rejects non-loopback bind addresses and every endpoint requires a
fresh 256-bit bearer capability published to the private `serve.token` path
printed at startup (mode `0600` inside a mode-`0700` data directory on Unix).
It also rejects non-loopback `Host`/`Origin` values and
cross-site browser requests. Run and session responses use allowlisted public
views rather than serializing internal storage records. This blocks remote
browser rebinding and other unauthenticated callers, but malicious code running
under the same OS identity can read the token; this remains a local single-user
control surface, not a hostile same-UID or multi-user boundary. On non-Unix
systems, keep `VYANE_DATA_DIR` under a platform-managed user-private directory;
Vyane does not replace the platform ACL on a caller-selected shared directory.

### Resident workflow daemon

The local-only workflow daemon is a separate control surface from `vyane serve`.
It keeps an admitted workflow running after the submitting CLI exits:

```sh
vyane daemon start                         # detached, waits for readiness
vyane daemon start --goal-auto-pursue \
  --goal-target builder --goal-workdir . --goal-sandbox write
vyane daemon status --json
vyane workflow submit workflow.toml --var env=dev
vyane workflow status <uuidv7> --json
vyane workflow cancel <uuidv7>
vyane daemon stop
```

`daemon run` keeps the same supervisor in the foreground. The listener accepts
only loopback addresses, and every endpoint — including `/health` — requires a
per-start 256-bit bearer token stored separately from the owner-only daemon
descriptor. The control API is `POST /v1/workflows`, `GET /v1/workflows/:id`,
and `POST /v1/workflows/:id/cancel`. On Linux it also exposes
`POST /v1/agent-runs`, `GET /v1/agent-runs/:id`,
`GET /v1/agent-runs/:id/output`, and `POST /v1/agent-runs/:id/cancel` for the
fresh sessionless Process host described in [WP-61](docs/plan/WP-61.md). There
is no permissive CORS layer. This
protects against accidental browser and cross-process access, but is not a
sandbox against hostile code running as the same OS user.

The client authenticates the exact recorded daemon before reading local
workflow sources. It sends the workflow TOML plus every declared `prompt_file`
as an in-memory source bundle; the daemon never resolves those source paths on
its own filesystem. The semantic limits are 1 MiB for the TOML, 4 MiB per
prompt, 16 MiB total, 128 prompt entries, and 4,096 bytes per bundle path.
Variables are limited to 128 entries, 256 bytes per key, 1 MiB per value, and
4 MiB total. The canonical submission working directory is also carried in the
request (maximum 4,096 bytes): a missing or relative step `workdir` is resolved
from it, while an absolute step `workdir` is preserved.

The client generates a canonical UUIDv7 by default and prints it to stderr
before the request is sent. `--id <uuidv7>` reuses an id for reconciliation or
an idempotent retry: the same daemon workflow scope, normalized source, working
directory, and variables returns the existing task; any mismatch is a conflict
and never replays the earlier payload. The task id and workflow journal id are
identical. On daemon restart, abandoned daemon-owned rows are cleaned by exact
controller identity and marked `interrupted`; they are not automatically
resumed or replayed. Foreground `workflow resume` remains an explicit,
journal-oriented command.

Goal pursuit is disabled by default. `--goal-auto-pursue` requires one explicit
target and canonical workdir; sandbox and verifier/segment/invocation limits
remain bounded flags. The daemon handles one `local` goal at a time, first
adopting eligible `in_progress` work and then atomically claiming the next
queued goal. A daemon replacement can continue its still-live stable-worker
lease and WP-71 checkpoint; an expired lease is reclaimed through the normal
generation fence. Semantic pauses, terminal goals, and goals under another
live worker are never auto-resumed or stolen. Shutdown cancels the fresh
runtime effect while leaving a running checkpoint, rather than recording a
false business pause. This adds no goal HTTP API, quota/approval handoff,
runtime-native session resume, remote owner, or parallel goal scheduler.

### Smart routing

`vyane route "task text"` previews a decision. `vyane dispatch --target auto`
executes the same decision and records the selected profile/provider/model,
tier, effort, score, intent, and tag on the ledger record. The router scores task complexity from structural signals
(changed files, dependency edges, retry count, prompt length, inferred
tags) and maps to an economy / mainline / frontier tier, then resolves
a preference based on profile `tier` and `tags` metadata:

```
vyane route "write an ADR for the system architecture" --changed-files 20
vyane route "simple task" --tier frontier
vyane dispatch "fix the parser" --target auto
vyane dispatch "review auth" --target auto --no-frontier
```

Routing hints can also be supplied as `routing.stage`, `routing.tier`,
`routing.tags`, `routing.candidates`, and `routing.allow_frontier` labels.
Decision fields such as `routing.provider` and `routing.effort` are reserved so
callers cannot forge audit metadata. A literal profile named `auto` remains
addressable as `profile:auto`; `target:<provider>/<model>` explicitly selects a
provider/model pair when a provider id begins with the reserved `profile:`
prefix. Workflow steps may use a
deferred auto target after prompt rendering:

```toml
[[step]]
id = "review"
target = "auto"
prompt = "Review {{steps.implement.output}}"
[step.route]
stage = "review"
tags = ["security"]
allow_frontier = false
effort = "high"
```

`[step.route].effort` is a closed typed value (`low`, `medium`, `high`, or
`xhigh`) available only on a deferred single target. Route hints on an explicit
target or `fan_out` fail before dispatch instead of being ignored. Effective
effort precedence is workflow explicit effort, then the selected profile's
configured effort, then the decision tier default. The resulting canonical
`routing.effort` is applied to every failover leg and frozen for recorded,
detached, daemon-idempotent, and journal-resume execution. Invalid values are
rejected without echoing their input, and an ordinary `effort` label cannot
forge the reserved decision field. See [WP-46](docs/plan/WP-46.md).

`WorkflowPlan` schema v1 is now the bounded, strict, filesystem-independent
execution payload shared by compile, prepare, run, and resume. It freezes the
materialized DAG, typed targets and route hints, lossless timeouts, requested
capability summary, source claim, and a canonical plan checksum. The payload is
not a safe public view, and its checksum is not authentication, provenance, or
execution authority. Plan-only continuation fails closed unless the journal has
the exact checksum; only the source-bearing compatibility API may migrate an
older journal after validating the exact source hash. See
[WP-54](docs/plan/WP-54.md).

[WP-58](docs/plan/WP-58.md) adds explicit exact-plan replay/fork. It reads a
terminal source journal, creates a new UUIDv7 journal, reuses only the
dependency-closed, journal-recorded all-success prefix, and executes the
remaining suffix live.
The source remains unchanged; daemon restart still never implies replay.

This closes the repository-local shared typed-plan prerequisite, not workflow
parity. Dynamic control flow, nested workflows, shared budgets, a compatibility
frontend, changed-plan call matching, CLI/REST/MCP plan transport, sanitized
cross-implementation fixtures, and a production-complete model-tier policy
remain open.

The frontier guard is fail-closed across the selected profile and its failover
chain. Detached auto-routed jobs send a secret-free target snapshot once over a
private worker stdin pipe and refuse to start if config changes would make the
worker execute a different provider/model/protocol/harness/parameter chain.
The snapshot is not persisted. Endpoint URLs, provider-specific extra values,
and harness env-injection values are represented only by safe digests/shape
metadata; raw credentials are never copied.

### Solution-review workflow

`vyane review` runs a three-step solution-review workflow on the existing
workflow engine: implement → fan-out review → synthesize. It is not yet the
original system's structured git diff/PR review, finding-artifact, verifier,
and publication pipeline.

```
vyane review 'implement a sorting function' \
  --implementer sonnet \
  --reviewers opus,gpt \
  --synthesizer opus
```

## Architecture

```
       CLI              REST API           MCP
   vyane dispatch     vyane serve       vyane mcp
        │                  │                  │
        └──────────┬───────┴──────────┬───────┘
                   ▼                  ▼
              vyane-service (shared facade)
                   │
                   ▼
            ┌─────────────┐
            │   kernel    │  early execution id,
            │  dispatch / │  whole-chain admission,
            │  broadcast  │  attempts + RunRecord
            └──────┬──────┘
      ┌────────────┴────────────┐
      ▼                         ▼
direct-http protocol         cli-wrap harnesses
clients (ChatClient)         (Harness, scrubbed env)
OpenAI Chat / Responses,     Claude Code, Codex CLI, …
Anthropic Messages                    │
      └────────────┬──────────────┘
                   ▼
        append-only JSONL ledger
         (digests, attempt trail)
           + session store
   SQLite transactional message store
 (messages, deliveries, receipt/outbox state)
      SQLite goal lifecycle store
 (snapshot + immutable events in one transaction)
       bounded delivery broker
 (replay-safe adapters + body-free EventLog projection)
    SQLite AgentRun / worker store
 (leases, topology, recovery, tree cancel, body-free outbox)
```

The kernel depends only on the traits and types in `vyane-core`; the concrete
clients, harnesses and ledger are assembled behind those traits in the service
layer. Seventeen crates:

| crate | responsibility |
|-------|----------------|
| `vyane-core` | four-layer target model, capability traits, error taxonomy, env policy, process-local workdir pin, and live native-side-effect authority vocabulary — the shared types everything else speaks |
| `vyane-config` | TOML config + profiles; resolves a profile (and its failover chain) to a `BoundTarget` |
| `vyane-provider` | provider registry; endpoints, auth styles, per-provider env-injection rules |
| `vyane-protocol` | `ChatClient` implementations for the HTTP protocols (OpenAI Chat / Responses, Anthropic Messages), a separately authorized OpenAI Chat typed-turn path, and the shared HTTP base validator and secret-free endpoint-routing digest |
| `vyane-harness` | `Harness` implementations wrapping coding CLIs headlessly, with additive scoped execution, pinned-workdir handoff and process-group control; also owns the guarded native tool-registry boundary and unwired bounded native turn driver |
| `vyane-kernel` | early execution identity, whole-chain capability admission, prepared streaming/dispatch, failover gating and run-record assembly |
| `vyane-ledger` | JSONL `Ledger` + filesystem `SessionStore`, including strict revisioned native-session snapshots and atomic CAS transitions; cost estimation |
| `vyane-task` | SQLite-backed, secret-free task lifecycle snapshots and CAS event history |
| `vyane-agent` | SQLite-backed, owner-scoped AgentRun queue, worker topology, fenced two-stage recovery admission, active permits and atomic native-scope revalidation, bounded tree cancellation, and body-free lifecycle outbox |
| `vyane-message` | SQLite-backed, owner-scoped transactional message and delivery truth, fenced leases, external receipts, and per-projector body-free outbox |
| `vyane-goal` | SQLite-backed, owner-scoped goal snapshots plus immutable lifecycle/progress events and persisted acceptance descriptors |
| `vyane-broker` | bounded owner-bound delivery pumps, replay-safety admission, fenced settlement, maintenance, and body-free message/AgentRun EventLog projection |
| `vyane-router` | target selection / routing policy (grows into pluggable routing) |
| `vyane-workflow` | declarative DAG execution, templates, atomic journals, and resume |
| `vyane-service` | shared construction and operation layer used by front-ends; also exports principal-derived owner scope, the unwired native fresh-sessionless bridge, explicit AgentRun projection assembly, one-shot recovery/execution drivers, paired backends, and the generic resident supervisor used by the daemon's Linux Process host |
| `vyane-mcp` | six base MCP tools plus an injectable, credential-free workflow-control port; the CLI exposes nine tools over stdio |
| `vyane-cli` | the assembler and entry point: wires the crates together behind a command-line UI |

## Usage

Configuration is a TOML file (a platform config directory for user defaults —
e.g. `~/Library/Application Support/vyane/config.toml` on macOS — merged with
`.vyane/config.toml` for project overrides). See
[`profiles.example.toml`](profiles.example.toml) for the full shape; a
provider and a profile look like:

```toml
[providers.anthropic]
base_url      = "https://api.anthropic.com"
api_key_env   = "ANTHROPIC_API_KEY"   # names an env var; no key material in config
auth_style    = "x_api_key"           # bearer | x_api_key
protocol      = "anthropic_messages"
default_model = "a-capable-anthropic-model"

# A named bundle of provider + protocol + harness + model → one BoundTarget.
[profiles.review]
provider = "anthropic"
protocol = "anthropic_messages"
harness  = "none"                     # "none" = direct HTTP chat, no workspace
model    = "a-capable-anthropic-model"
```

Then, from the shell:

```sh
# Validate config, resolve every profile, probe harness binaries and env vars.
vyane check

# Dispatch one task to one target (a profile name).
vyane dispatch "review this diff" --target myprofile

# Broadcast the same task to several target chains, concurrently.
vyane broadcast "compare approaches" --targets a,b,c
```

Sample `vyane check` output against a config with two providers and three
profiles (one profile's required env var deliberately left unset, to show
what a missing-key warning looks like):

```
config files:
  .vyane/config.toml (loaded)
providers:
  anthropic: anthropic_messages default_model=a-capable-anthropic-model
  openai: openai_chat default_model=a-fast-openai-model
profiles:
  builder: anthropic/a-capable-anthropic-model via claude-code (anthropic_messages)
  codex: warning: provider requires environment variable `OPENAI_API_KEY` for its API key, but it is not set
  review: anthropic/a-capable-anthropic-model (anthropic_messages)
harnesses:
  claude-code: available
  codex-cli: available
profile environment:
  builder: ANTHROPIC_API_KEY present
  codex: OPENAI_API_KEY missing
  review: ANTHROPIC_API_KEY present
```

## Design principles

- **Boring dependencies.** Widely-used, well-understood crates; no clever
  infrastructure where a plain file will do.
- **Secrets never serialize.** Credential types are non-serializable by
  construction; config stores the *name* of an env var, never the value.
- **Declaration and evidence are not authority.** Capability manifests and
  scopes are serializable audit data; prepared plans, pinned directory handles
  and active permits stay process-local and fail closed across provenance drift.
- **The ledger stores digests, not prompts.** New run accounting records a
  prompt digest and never copies a preview or prompt body by default.
- **Scrubbed child environment by default.** Harnesses spawn from a minimal
  baseline env plus an explicit per-target injection set; inheriting the full
  parent environment is opt-in.
- **No hidden timeouts.** Long agentic runs legitimately take hours; timeouts
  are opt-in per task, never a silent default.
- **Owner-aware records.** Every run and session carries an owner field from
  day one, so multi-user isolation is never a retrofit.

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — the four-layer model, crate map,
  dispatch lifecycle, env policy, failover and ledger semantics.
- [Architecture decisions](docs/adr/README.md) — accepted product differences
  and their still-open acceptance gates.
- [Roadmap](docs/ROADMAP.md) — milestones for v0.1 through v0.4.
- [Original-Vyane parity baseline](docs/parity/ORIGINAL-VYANE-PARITY.md) —
  fixed cross-repository capability matrix and acceptance gates.
- [Contributing](CONTRIBUTING.md) — toolchain, checks, and PR conventions.

## License

Licensed under either of [MIT](LICENSE-MIT) or
[Apache License, Version 2.0](LICENSE-APACHE) at your option. Unless you
explicitly state otherwise, any contribution you submit for inclusion is
dual-licensed as above, with no additional terms or conditions.
