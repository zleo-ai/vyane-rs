# Roadmap

Scope, not schedule. Milestones list *what*, in dependency order; there are no
dates and no time estimates. `vyane-core` (the four-layer model, traits, error
taxonomy, env policy) is complete and underpins everything below.

These are repository-local milestones, not a claim of full feature parity with
the private Python system. Cross-repository status, deliberate differences, and
acceptance gates are tracked in
[`docs/parity/ORIGINAL-VYANE-PARITY.md`](parity/ORIGINAL-VYANE-PARITY.md).

## Active P0-P3 whole-system parity program

Unqualified “full parity” means whole-system capability parity, not merely public-core parity.
Private credentials, deployment details, endpoints, paths, and identity data
must not enter this public repository; the corresponding generic contracts and
optional/private adapter boundaries must nevertheless be verifiable. In the
current public integration baseline the 53 matrix items are 7 implemented, 20 partial,
16 missing, 8 different, and 2 planned.

| wave | active scope |
|------|--------------|
| **P0** | Keep the parity ledger honest: accepted-difference ADRs and a versioned public manifest pin selected, maintainer-attested classifier, failover, and automatic-routing behavior fixtures, their SHA-256/case digests, and explicit open differences without disclosing private repository provenance. [WP-57](plan/WP-57.md) adds automatic provider/model/effort precedence, rendered-task routing, no-eligible fail-closed, full-chain frontier guard, frozen replay, and an offline recompute/report command. Target resolution, production failover trails, run/session schemas, workflow migration and broader shadow coverage remain open. |
| **P1** | Complete the execution core. Early execution scopes, whole-chain trusted capability admission, Linux pinned-workdir handoff, bounded typed tool chat, the local-filesystem same-session execution lease, strict revisioned `NativeSessionDomain` transitions, and the AgentRun permit primitive are implemented. [WP-41](plan/WP-41.md) adds atomic permit-plus-native-scope validation and guarded model/tool boundaries; [WP-42](plan/WP-42.md) adds a fresh-sessionless bridge; [WP-43](plan/WP-43.md) adds a dark bounded serial turn driver. [WP-52](plan/WP-52.md) lets the paired in-process operation bind a lifetime-bound exact fresh scope and repeats atomic native validation for every model/tool effect. [WP-53](plan/WP-53.md) adds generic crash-consistent local completion handback and publication. No concrete product operation, production factory/runtime or registered `Harness` exists. Session-aware authority, production resume, trusted built-ins, OS sandboxing, checkpoint/session-commit and approval resume remain. [WP-46](plan/WP-46.md) delivers closed typed workflow effort routing; [WP-54](plan/WP-54.md) delivers the bounded typed workflow execution plan and exact plan-digest continuation binding; [WP-58](plan/WP-58.md) adds explicit new-run exact-plan replay/fork with journal-recorded all-success prefix reuse. Dynamic control flow, nested workflows, shared budgets, changed-plan call matching, compatibility frontend/transport, sanitized cross-implementation fixtures and a production-complete tier policy remain open. Repository-change review still needs immutable bounded diff acquisition, structured findings/verifier/artifacts and non-skippable least-privilege harness evidence. The current session lease is local and crash-released; distributed generation/TTL fencing remains separate. |
| **P2** | Build the common AI-OS substrate in dependency order: owner-safe event/session/message stores, principal-derived owner auth/policy, logical sessions/goals, worker topology/collaboration, then observability. EventLog/session/message/AgentRun stores, bounded projectors and broker/recovery/execution seams are partial foundations. [WP-48](plan/WP-48.md) pairs exact `InProcess` execution/recovery with permit/tombstone fencing. [WP-49](plan/WP-49.md) freezes authentication plus owner resolution; REST now freezes every service operation into one explicit local scope, but its bearer is not a distinct principal. [WP-50](plan/WP-50.md) makes durable task truth/artifacts owner-qualified. [WP-51](plan/WP-51.md) supplies bounded resident execution/recovery polling; [WP-53](plan/WP-53.md) extends it with completion publication, hidden message staging, and shutdown-safe exact recovery. [WP-55](plan/WP-55.md) adds a dark live-authority gate before built-in harness wrapper spawn and real-target release. [WP-59](plan/WP-59.md) exposes the transactional message store as an owner/mailbox-scoped local `a2a send/inbox/read` CLI with delayed pages, exact fenced reads, and stable JSON; its caller-selected owner is not authenticated authority. No production AgentRun caller constructs the live authority, and there is still no exact process sidecar, concrete Process/Remote integration, authenticated message protocol or production host. Public fork/REST mutation, distinct-principal protocol wiring, owner-safe workflow/AgentRun/event control, session-aware production resume, distributed fencing, production host assembly, authenticated A2A/Channels, remaining event producers, subscriptions, retention, live pause/resume and automatic replay remain open. |
| **P3** | Add private/platform capabilities through generic contracts plus verifiable optional/private adapters; do not copy private implementation data into the public core. Upstream quota/balance begins as a bounded, fixture-tested snapshot connector contract, not a durable ledger claim. |

## v0.1 — the kernel end to end

A single machine can configure targets, dispatch a task to one, broadcast to
several, fail over between them, and have every run recorded. Delivered as a
sequence of milestones, each a self-contained work package.

| milestone | scope |
|-----------|-------|
| **M1** | config + provider: TOML config, layered precedence, profiles resolving to `BoundTarget`, failover chains, per-provider env-injection rules. |
| **M2** | protocol clients: `ChatClient` for OpenAI Chat + Anthropic Messages (non-streaming + SSE), OpenAI Responses (non-streaming), with explicit retry/backoff and faithful error mapping. The shared client follows no redirects and performs no implicit client retries; the separately authorized OpenAI Chat typed-turn path revalidates each explicit wire attempt. |
| **M3** | harnesses: `Harness` for Claude Code + Codex CLI — headless one-shot, scrubbed child env via `EnvPolicy`, process-group spawn and group kill. |
| **M4** | kernel: the dispatch / broadcast / failover state machine over injected executors, assembling the full-attempt-trail `RunRecord`. |
| **M5** | ledger + sessions: append-only JSONL `Ledger` with advisory locking, owner-isolated filesystem `SessionStore` with strict revisioned snapshots, native-state CAS transitions, and an execution-period advisory lease shared by dispatch and control mutations; cost estimation from a price table. |
| **M6** | CLI + integration: `vyane check` / `dispatch` / `broadcast`, wiring all crates behind the command line, end-to-end tests. |

The M1–M5 work packages are specified in [`docs/plan/`](plan/) (WP-01 … WP-05);
they map one-to-one onto M1–M5. Because the kernel depends only on `vyane-core`
traits, the wave-1 packages are largely parallel — assembly happens at M6.

## v0.2 — pipelines and background execution

| milestone | scope |
|-----------|-------|
| ~~**workflow engine**~~ | ✅ declarative DAG pipelines with target/fan-out steps, template data flow, same-run resume, and exact-plan new-run replay/fork. |
| ~~**background task control**~~ | ✅ CLI detached, REST async, and daemon workflow tasks share durable SQLite lifecycle metadata, scoped CAS cancellation, restart interruption, and ledger correlation (WP-39, WP-40). New CLI detached submissions also freeze capability admission before the task row/process and transfer a Linux pinned workdir descriptor to the worker for identity revalidation. Automatic replay and live pause/resume remain future work. |
| ~~**daemon**~~ | ✅ A resident, loopback-only process owns admitted workflow execution after the submitting client exits, with authenticated submit/status/cancel and exact-controller recovery (WP-40). Restart marks abandoned work interrupted; it does not replay or resume it. |

## v0.3 — integration surface and smarter routing

| milestone | scope |
|-----------|-------|
| ~~**MCP server**~~ | ✅ expose six MCP tools over stdio: dispatch / broadcast / history / sessions plus two bounded diagnostics, deterministic `route` preview and static-only configuration `check`. Every success result has a generic 1 MiB output cap; route/check use smaller domain-specific caps, while legacy execution inputs do not share the diagnostics' uniform field-level budgets. Oversized completed dispatch/broadcast detail becomes bounded `operation_status=completed` run receipts, not a retry-inviting limit error. Route preview is a Rust extension, not a same-name reference-system parity claim; check performs no network probe, harness spawn, or live credential validation. |
| ~~**REST API**~~ | ✅ bearer-authenticated loopback-only HTTP JSON API (`vyane serve`, axum): `/v1/dispatch`, `/v1/broadcast`, `/v1/runs`, `/v1/sessions`, `/v1/health`; non-loopback bind/Host/Origin and cross-site requests are rejected, run/session results use allowlisted views, and the per-start token is mode `0600`. This is not hostile same-UID or multi-user isolation. |
| ~~**shared service layer**~~ | ✅ `vyane-service` crate: one `VyaneService` facade shared by CLI, REST, and MCP front-ends, with allowlisted run/session views and owner-local session list/inspect/reset-native. Optional owner-bound message and AgentRun projection-only components require explicit construction and do not alter ordinary dispatch. There is no public fork or REST/MCP reset mutation. |
| ~~**local A2A message CLI**~~ | ✅ `vyane a2a send/inbox/read` over the transactional message store, with explicit owner/mailbox scope, delayed visibility, bounded stable JSON pages and fenced read acknowledgement (WP-59). This is a same-machine queue, not authenticated multi-user authority or A2A HTTP compatibility. |
| ~~**solution-review workflow**~~ | ✅ built-in `vyane review` command: three-step workflow (implement → fan-out review → synthesize) on the existing engine. It is not yet the original structured git diff/PR review product. |
| ~~**pluggable routing**~~ | ✅ Deterministic complexity/tag/tier policy plus executable `--target auto`: the service preserves provider/profile identity, applies effort to HTTP and CLI harnesses, records the decision in ledger labels, and supports deferred workflow routing (WP-38). Closed typed workflow effort now uses explicit > selected-profile > tier-default precedence, freezes one canonical effective value across failover and replay surfaces, and rejects route hints outside deferred single targets (WP-46). Cross-implementation fixtures and production-complete tier/model policy remain open. |

## v0.4 — streaming, daemon, and package readiness

| milestone | scope |
|-----------|-------|
| ~~**kernel streaming API**~~ | ✅ `Dispatcher::dispatch_stream` — callback-based streaming with kernel-owned record assembly, timeout/cancellation, and the legacy `Ok(None)` unsupported contract. The additive prepared probe/fallback seam lets the CLI reuse one execution id without changing that public behavior (WP-18). |
| ~~**SSE streaming endpoint**~~ | ✅ `POST /v1/dispatch/stream` — Server-Sent Events for real-time delta delivery over HTTP (WP-19). |
| ~~**async task registry**~~ | ✅ `POST/GET /v1/tasks` + real cancellation-token propagation, now backed by secret-free durable metadata and restart recovery (WP-21, WP-39). |
| ~~**registry package preflight**~~ | ✅ 16-crate metadata complete, workspace path deps versioned, and the publish workflow is fail-closed (WP-20, WP-37, WP-39, WP-56). Manual dispatch requires an existing release tag on the exact current `main` SHA plus a protected `crates-io` environment with required reviewers and self-review prevention; the token reaches only the publish step. The 16-crate local preflight passes; no crate has been published. This is package readiness, not parity. |
| ~~**harness streaming**~~ | ✅ Claude Code / Codex CLI stdout events flow through `Harness::run_stream`, the kernel, CLI, and REST SSE, with fake-CLI protocol fixtures and cancellation coverage (WP-36). Additive scoped harness methods carry the Linux pinned-workdir handle; legacy harness implementations remain source-compatible and fail closed if handed a pin they do not implement. |
| **external distribution (deferred; not parity)** | crates.io upload is an external, hard-to-reverse action. A tag and `CARGO_REGISTRY_TOKEN` remain data and credential, not authority. The only repository workflow path is manual dispatch from current `main`, followed by approval in the protected `crates-io` environment by someone other than the dispatcher. Do not create the tag, approve the environment, or upload without separate explicit authorization. |
| ~~**daemon (resident process)**~~ | ✅ Persistent ownership for managed workflows, 30-second renewable leases, authenticated loopback control, bounded in-memory source bundles, client-generated UUIDv7 idempotency, exact controller cleanup, and graceful drain (WP-40). No live pause/resume or automatic restart replay. |
