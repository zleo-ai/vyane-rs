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
       (types · traits · errors · env policy — no runtime)
        ▲     ▲        ▲         ▲          ▲        ▲
        │     │        │         │          │        │
 vyane-config │  vyane-protocol  │    vyane-harness  │
   │          │        │         │          │        │
 vyane-provider        │      vyane-ledger   │   vyane-kernel
        │              │         │           │   (traits only)
        └──────┬───────┴─────────┴───────────┴────────┘
               ▼
           vyane-cli   ──── (assembler: constructs concrete
       (+ vyane-router)      clients/harnesses/ledger and injects
                             them into the kernel via a factory)
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
2. **Adapters are swappable.** Anything satisfying a `vyane-core` trait drops
   in without touching the kernel — that is the seam future routing, new
   protocols and new harnesses extend along.

| crate | depends on (beyond `vyane-core`) | role |
|-------|----------------------------------|------|
| `vyane-config` | `vyane-provider` | parse TOML, resolve profiles + failover chains → `BoundTarget` |
| `vyane-provider` | — | provider registry, endpoints, auth styles, env-injection rules |
| `vyane-protocol` | — | `ChatClient` over HTTP (OpenAI Chat / Responses, Anthropic Messages) |
| `vyane-harness` | — | `Harness` wrapping coding CLIs, process-group control |
| `vyane-kernel` | — (traits only) | dispatch / broadcast / failover state machine |
| `vyane-ledger` | — | JSONL `Ledger`, filesystem `SessionStore`, cost table |
| `vyane-router` | `vyane-core` | target selection / routing policy |
| `vyane-cli` | all of the above | assembler + command-line entry point |

## Dispatch lifecycle

A single `dispatch(TaskSpec, Vec<BoundTarget>)` walks this state machine and
always ends by producing exactly one `RunRecord`, whatever happened.

```
  ┌─────────────────────┐
  │ resolve selector    │  profile name / provider/model / chain
  │  → Vec<BoundTarget> │  (done in the config layer; kernel receives
  └──────────┬──────────┘   an already-resolved, ordered chain)
             ▼
  ┌─────────────────────┐
  │ attempt loop        │◀──────────────────────────────┐
  │  take next target   │                                │
  └──────────┬──────────┘                                │
             ▼                                            │
  ┌─────────────────────┐                                │
  │ execute one attempt │  Chat(client) or Agent(harness)│
  │  record Attempt     │  per BoundTarget.transport     │
  └──────────┬──────────┘                                │
             ▼                                            │
        success? ──yes──▶ status = Success ───┐          │
             │ no                             │          │
             ▼                                │          │
     classify ErrorKind                       │          │
             │                                │          │
   failover_eligible()  AND  targets remain?  │          │
             │                                │          │
        yes ─┼── mark attempt failed_over ────┼──────────┘
             │
         no  ▼
     status = Error / Timeout / Cancelled
             │
             ▼
  ┌─────────────────────────────────────────┐
  │ assemble RunRecord                       │
  │  run_id = uuidv7, full attempt trail,    │
  │  target = last attempt's target,         │
  │  usage, task_digest, status              │
  └──────────────────┬──────────────────────┘
                     ▼
        ledger.append(record)   (on success AND failure)
        session_store update    (native id / transcript, run_count++)
```

The failover gate is a single conjunction: advance to the next target **only
when** the error's `ErrorKind::failover_eligible()` is true **and** at least one
more target remains in the chain. Every attempt — including the last, failed
one — is recorded in `attempts`, so the record answers "what was tried, in what
order, and why we stopped" after the fact. `RunRecord.target` is the *last*
attempt's target (the one that produced the final outcome).

**`broadcast`** runs N independent target chains concurrently, each through the
same dispatch machine, bounded by a concurrency semaphore, and returns results
**in input order** (index-preserving) even though they complete out of order. A
partial failure is just some chains ending in `Error` while others succeed —
each still produces its own `RunRecord`.

**Cancellation** propagates through a `CancellationToken` (re-exported from
`vyane-core`). Direct-HTTP attempts drop their in-flight request; harness
attempts kill the child *process group* (see [EnvPolicy](#envpolicy-and-clean-child-environments)
neighbours in the harness spec — a bare child kill leaves grandchildren
running). A cancelled run still writes its `RunRecord`.

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

## Ledger record schema

Every dispatch appends exactly one `RunRecord`. Fields mirror
`crates/vyane-core/src/run.rs`.

| field | type | notes |
|-------|------|-------|
| `run_id` | `String` | UUIDv7 — time-ordered, globally unique |
| `owner` | `String` | owner scope; `"local"` for single-user. Present from day one |
| `started_at` / `finished_at` | `DateTime<Utc>` | wall-clock bounds of the whole dispatch |
| `task_digest` | `String` | SHA-256 of the prompt, hex, first 16 chars — **not the prompt body** |
| `task_preview` | `Option<String>` | first ~120 chars of the prompt, for human scanning; configurable off |
| `workdir` | `Option<String>` | harness working directory, when set |
| `sandbox` | `Sandbox` | `ReadOnly` / `Write` / `Full` |
| `target` | `Target` | the target that produced the final outcome (the last attempt) |
| `transport` | `AdapterTransport` | `CliWrap` / `DirectHttp` |
| `attempts` | `Vec<Attempt>` | full failover chain, in order; length 1 = no failover |
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

## Session continuity

There are **two** continuity mechanisms and they are genuinely different things
(`crates/vyane-core/src/session.rs`):

- **Native harness sessions.** A coding CLI keeps its own session state. Vyane
  stores the harness's `native_session_id` on the `SessionRecord` and passes
  the harness-appropriate resume flag on the next run. The transcript stays
  empty — the CLI owns the history.
- **Transcript sessions.** Direct-HTTP chat has no native state. Vyane itself
  stores the message `transcript` on the `SessionRecord` and replays it as
  history on the next call.

A single `SessionRecord` can carry both, so one logical session id can hop
between a harness target and a direct-chat target while staying coherent. Each
record also carries `owner`, `created_at` / `updated_at`, and a `run_count`.

## Non-goals for v0.1

Explicitly out of scope for the first release, to keep the core honest:

- **No daemon.** v0.1 is one-shot CLI + library only. (Long-running daemon and
  an async task registry are v0.2.)
- **No learned routing.** Target selection is explicit configuration, not a
  model that decides for you. (Pluggable routing is v0.3.)
- **No gateway / proxy server.** Vyane spawns and calls; it does not sit in
  front of your models as an HTTP gateway.
- **No GUI.** CLI and library only.

## Roadmap

See [ROADMAP.md](ROADMAP.md) for the milestone breakdown of v0.1, v0.2 and
v0.3.
