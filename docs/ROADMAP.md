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
current public integration baseline the 53 matrix items are 7 implemented, 22 partial,
13 missing, 9 different, and 2 planned.

| wave | active scope |
|------|--------------|
| **P0** | Keep the parity ledger honest: accepted-difference ADRs and a versioned public manifest pin selected, maintainer-attested classifier, failover, and automatic-routing behavior fixtures, their SHA-256/case digests, and explicit open differences without disclosing private repository provenance. [WP-57](plan/WP-57.md) adds automatic provider/model/effort precedence, rendered-task routing, no-eligible fail-closed, full-chain frontier guard, frozen replay, and an offline recompute/report command. Target resolution, production failover trails, run/session schemas, workflow migration and broader shadow coverage remain open. |
| **P1** | Complete the execution core. Early execution scopes, whole-chain trusted capability admission, Linux pinned-workdir handoff, bounded typed tool chat, the local-filesystem same-session execution lease, strict revisioned `NativeSessionDomain` transitions, and the AgentRun permit primitive are implemented. [WP-41](plan/WP-41.md) adds atomic permit-plus-native-scope validation and guarded model/tool boundaries; [WP-42](plan/WP-42.md) adds a fresh-sessionless bridge; [WP-43](plan/WP-43.md) adds a dark bounded serial turn driver. [WP-52](plan/WP-52.md) lets the paired in-process operation bind a lifetime-bound exact fresh scope and repeats atomic native validation for every model/tool effect. [WP-53](plan/WP-53.md) adds generic crash-consistent local completion handback and publication. [WP-61](plan/WP-61.md) production-assembles a narrower fresh sessionless CLI-harness Process path with frozen dispatch authority. [WP-63](plan/WP-63.md) freezes a durable execution backend on every new AgentRun, filters claims without weakening per-worker FIFO, and migrates ambiguous v3 rows to a non-runnable sentinel. [WP-65](plan/WP-65.md) composes the existing native seams into a private-spool, exact fresh-sessionless, tool-free OpenAI Chat operation with durable message-completion evidence; that composition is dark in isolation. [WP-66](plan/WP-66.md) subsequently production-assembles the still tool-free, fresh-sessionless Native lane behind the resident recovery/completion host and exposes read-only native submit/status/output/cancel control. [WP-83](plan/WP-83.md) defines the Linux-first trusted filesystem tools and confinement train; [WP-84](plan/WP-84.md), [WP-85](plan/WP-85.md), [WP-86](plan/WP-86.md), and [WP-87](plan/WP-87.md) add separately authorized command execution, command HTTPS destinations, hosted web search, and bounded direct HTTPS text fetch. [WP-91](plan/WP-91.md) exposes bounded per-request and layered-config tool decisions: layer-local find-last rules compose by `deny > ask > allow`, cannot register an absent tool, and are frozen into the durable policy digest. `ask` still terminates without execution; owner-scoped approval storage and resume require a separate lifecycle decision. [WP-92](plan/WP-92.md) freezes and revalidates an ordered chain of up to sixteen direct OpenAI Chat targets and advances only on the shared failover-eligible taxonomy before any tool side effect; one fixed deadline covers the whole chain. [WP-93](plan/WP-93.md) adds independent, monotonic `read-only <= write <= full` ceilings for any chain containing a Claude Code or Codex CLI harness leg, rejecting over-broad requests before REST/workflow task identity, journal, dispatch persistence or spawn while retaining closed adapter-owned vendor mappings. Native protocols beyond this lane, session-aware authority, production resume, checkpoint/session-commit, approval resume and automatic replay remain open. [WP-46](plan/WP-46.md) delivers closed typed workflow effort routing; [WP-54](plan/WP-54.md) delivers the bounded typed workflow execution plan and exact plan-digest continuation binding; [WP-58](plan/WP-58.md) adds explicit new-run exact-plan replay/fork with journal-recorded all-success prefix reuse. Dynamic control flow, nested workflows, shared budgets, changed-plan call matching, compatibility frontend/transport, sanitized cross-implementation fixtures and a production-complete tier policy remain open. Repository-change review still needs immutable bounded diff acquisition, structured findings/verifier/artifacts and non-skippable least-privilege harness evidence. The current session lease is local and crash-released; distributed generation/TTL fencing remains separate. |
| **P2** | Build the common AI-OS substrate in dependency order: owner-safe event/session/message/goal stores, principal-derived owner auth/policy, worker topology/collaboration, then observability. EventLog/session/message/AgentRun stores, bounded projectors and broker/recovery/execution seams are partial foundations. [WP-48](plan/WP-48.md) pairs exact `InProcess` execution/recovery with permit/tombstone fencing. [WP-49](plan/WP-49.md) freezes authentication plus owner resolution; REST now freezes every service operation into one explicit local scope, but its bearer is not a distinct principal. [WP-50](plan/WP-50.md) makes durable task truth/artifacts owner-qualified. [WP-51](plan/WP-51.md) supplies bounded resident execution/recovery polling; [WP-53](plan/WP-53.md) extends it with completion publication, hidden message staging, and shutdown-safe exact recovery. [WP-55](plan/WP-55.md) adds a live-authority gate before built-in harness wrapper spawn and real-target release. [WP-59](plan/WP-59.md) exposes the transactional message store as an owner/mailbox-scoped local `a2a send/inbox/read` CLI. [WP-60](plan/WP-60.md) adds one owner-scoped SQLite goal truth with transactional snapshot/event revisions and local lifecycle/progress CLI. [WP-61](plan/WP-61.md) closes the first production AgentRun slice: an authenticated loopback Linux Process host with frozen input, exact sidecars/recovery, completion handback, cancel, and cooperative drain. [WP-63](plan/WP-63.md) adds durable backend routing, [WP-64](plan/WP-64.md) adds the bounded service substrate for exact-backend execution lanes around one recovery union and completion projector, and [WP-65](plan/WP-65.md) supplies the fresh/sessionless native operation. [WP-66](plan/WP-66.md) production-assembles Process and Native lanes behind one recovery/completion host, adds an explicit read-only native submit/status/output/cancel slice, and closes the daemon-level Native graceful-shutdown race with deadline-bound restart/no-replay acceptance. [WP-83](plan/WP-83.md) through [WP-88](plan/WP-88.md), plus [WP-90](plan/WP-90.md), add descriptor-bound filesystem read/write/edit, separately authorized sandboxed command execution, command HTTPS destinations, hosted web search, bounded direct HTTPS text fetch, layered permission ceilings, and descriptor-bound writable command roots. [WP-91](plan/WP-91.md) freezes configurable per-tool allow/ask/deny decisions, while [WP-92](plan/WP-92.md) freezes bounded pre-side-effect native target failover. [WP-67](plan/WP-67.md) assembles the owner-bound broker/projector supervisor into the resident daemon; its delivery lane set remains intentionally empty until an explicit adapter is integrated. [WP-68](plan/WP-68.md) adds bounded local goal acceptance verification and fenced persistence of successful criteria; [WP-69](plan/WP-69.md) preserves each complete bounded result as an immutable, owner-scoped, digest-checked artifact; [WP-70](plan/WP-70.md) adds the explicit bounded manual verify→fresh dispatch→reverify loop; [WP-71](plan/WP-71.md) makes its checkpoint durable and fenced; [WP-72](plan/WP-72.md) adds opt-in single-goal resident automatic pursuit and restart adoption; [WP-79](plan/WP-79.md) adds a read-only continuity next-action contract for operators and future separately authorized runners. [WP-80](plan/WP-80.md) exposes only that projection through an opt-in owner-frozen `GoalReadService` and bearer-authenticated loopback GET over one atomic allowlisted snapshot. [WP-81](plan/WP-81.md) adds a separate owner-frozen/source-bound observation mutation port and one-shot bounded typed watcher poller without a concrete adapter or periodic loop. [WP-82](plan/WP-82.md) adds a purpose-authenticated one-shot bounded projection runner with separately configured queue and approved-execution ports; decision remains manual and every port receives an exact revision/quota/step/approval fence. Concrete ports and periodic assembly remain open. The daemon remains fixed to one local single-user scope; native session resume, approval resume, generic child-process OS sandboxing, `Remote`, distinct-principal protocol wiring, owner-safe event control, distributed fencing, authenticated A2A/Channels, remaining event producers, subscriptions, retention, live pause/resume and general workflow/AgentRun automatic replay remain open. |
| **P3** | Add private/platform capabilities through generic contracts plus verifiable optional/private adapters; do not copy private implementation data into the public core. Upstream quota/balance begins as a bounded, fixture-tested snapshot connector contract, not a durable ledger claim. |

[WP-88](plan/WP-88.md) extends the P1 native lane with independent
user/project/managed permission ceilings. Configuration never grants an
optional tool and every configured ceiling can only narrow the explicit
AgentRun request. [WP-90](plan/WP-90.md) keeps writable command execution
separate and limits it to explicit descriptor-bound roots inside an otherwise
read-only workspace. [WP-94](plan/WP-94.md) adds a redacted static summary of
the composed CLI-harness ceiling and Native/Canto capability bounds to both
`vyane check` surfaces without turning configuration into a grant.
[WP-95](plan/WP-95.md) keeps daemon startup fail-closed before spawn while
allowing the bounded post-spawn readiness loop to retry only typed transient
control-lock contention.
[WP-96](plan/WP-96.md) makes forced-restart acceptance fixtures wait for the
old daemon PID to disappear instead of racing the production pre-spawn
liveness and authenticated-health gate.
[WP-97](plan/WP-97.md) gives the standard CI test job the same serialized
broker-supervisor fixture scheduling already used by coverage, without
skipping tests or reducing concurrency inside each fixture.
[WP-98](plan/WP-98.md) reconciles the README's stale Native
graceful-shutdown acceptance statement with WP-66 and the current tested
roadmap state.
[WP-99](plan/WP-99.md) refreshes the locked MCP SDK and macros from `rmcp`
3.0.0 to the compatible 3.0.1 patch while retaining the Rust 1.88 floor and
the existing stdio tool surface.
[WP-100](plan/WP-100.md) makes daemon terminal acceptance polling reuse one
short-timeout client and retry transient loopback timeouts only inside the
existing total terminal budget.
[WP-101](plan/WP-101.md) gives idempotent AgentRun acceptance submissions the
same bounded retry treatment when a loopback timeout leaves the response
ambiguous under suite load.
[WP-102](plan/WP-102.md) gives the pursuit overall-timeout fixture enough room
for its real verifier while still proving the segment receives only the
remaining total budget.
[WP-103](plan/WP-103.md) isolates the macOS broker-maintenance runtime and the
instrumented daemon-restart fixture in fresh test processes after exact
main-branch failures, without changing production lifecycle budgets.
[WP-104](plan/WP-104.md) moves every CI and publish checkout to the immutable
official `actions/checkout` v6.0.2 commit and explicitly narrows ordinary CI
workflow token access to read-only repository contents.
[WP-105](plan/WP-105.md) gives the service-owned no-lane resident broker
fixture a fresh process in normal and coverage gates after an exact macOS main
run hung at that runtime handoff.
[WP-106](plan/WP-106.md) gives the resident workflow acceptance fixture a
two-thread Tokio runtime so its blocking CLI polling cannot starve the mock
target serving the detached daemon.
[WP-107](plan/WP-107.md) gives the resident completion supervisor fixture
independent Tokio workers and a bounded final join after an exact coverage
process consumed the whole job timeout during shutdown.
[WP-108](plan/WP-108.md) gives the mixed process/native daemon fixture
independent Tokio workers after Ubuntu delayed its two 15-second terminal
pollers for roughly 90 seconds in the full acceptance-test process.
[WP-109](plan/WP-109.md) reserves a documented settlement margin on each
pursuit lease renewal so overall-timeout pause checkpoints can finish after
the runtime budget elapses, without extending execution timeouts.
[WP-110](plan/WP-110.md) adds a second, settlement-only lease heartbeat
immediately before terminal pause writes so pause settlement does not depend
on residual loop-start lease alone.
[WP-111](plan/WP-111.md) gives multi-mock native AgentRun fixtures (web-search
and failover) independent Tokio workers after Ubuntu and coverage left them
polling until the status budget expired with only the daemon listen line.
[WP-112](plan/WP-112.md) keeps resident workflow daemon fixtures schedulable:
two-worker MCP runtime, async workflow status polling, and a fresh CI process
for the long dual-path workflow fixture after macOS/Ubuntu suite stalls.
[WP-113](plan/WP-113.md) puts the remaining Wiremock-backed native AgentRun
acceptance fixtures on two-worker Tokio runtimes so suite handoffs cannot
starve mock targets during status polling.
[WP-114](plan/WP-114.md) puts Wiremock-backed `native_agent` unit fixtures on
two-worker Tokio runtimes for the same multi-mock scheduling class.
[WP-115](plan/WP-115.md) puts multi-mock CLI acceptance failover/stream
fixtures on two-worker Tokio runtimes.
[WP-116](plan/WP-116.md) keeps the long dual-path workflow fixture
schedulable under llvm-cov after main coverage left it `running` for the full
poll budget: more workers, non-blocking CLI I/O, hold-until-release mock gate,
daemon-log settlement panics, and incremental-cache cleanup before the
isolated coverage process.
[WP-117](plan/WP-117.md) puts authorized OpenAI Chat dual-mock redirect and
cancellation fixtures on two-worker Tokio runtimes so delayed targets and
cancel paths stay schedulable under suite load.
[WP-118](plan/WP-118.md) isolates the process-lane AgentRun success fixture in
a fresh CI process after suite handoff left Ubuntu submission retries with only
the daemon listen line.
[WP-119](plan/WP-119.md) isolates the concurrent process/native multi-lane
fixture the same way after main Ubuntu left its first submit with only the
daemon listen line inside the serialized suite.
[WP-120](plan/WP-120.md) puts remaining delayed MockServer timeout unit
fixtures on two-worker Tokio runtimes while leaving lease-sensitive restart
fixtures on the current-thread runtime.
[WP-121](plan/WP-121.md) puts authorized OpenAI Responses web-search
MockServer fixtures on two-worker Tokio runtimes so cancel/retry paths stay
schedulable under suite load.
[WP-122](plan/WP-122.md) puts long-delay detached CLI acceptance fixtures on
two-worker Tokio runtimes so delayed targets stay schedulable while the parent
returns from `--detach` or cancels the worker group.
[WP-123](plan/WP-123.md) isolates the native writable-command-root sandbox
fixture in a fresh CI process after coverage left its first submit with only
the daemon listen line.
[WP-124](plan/WP-124.md) puts the remaining authorized OpenAI Chat MockServer
unit fixtures on two-worker Tokio runtimes so the full suite shares one
scheduling class.
[WP-125](plan/WP-125.md) isolates the native command-permission sandbox
fixture in a fresh CI process after coverage left status polling with only the
daemon listen line under suite load.
[WP-126](plan/WP-126.md) puts the remaining MockServer-backed detached CLI
acceptance fixtures on two-worker Tokio runtimes so wiremock-backed dispatch
paths stay schedulable under suite load.
[WP-127](plan/WP-127.md) isolates the shared-resident native submit fixture in a
fresh CI process after suite handoff left AgentRun output HTTP timed out under
load.
[WP-128](plan/WP-128.md) isolates expired-controller restart settlement and
extends exact-controller restart recovery isolation to the normal CI job after
suite load still observed non-terminal restart states while the bodies stayed
stable alone.
[WP-129](plan/WP-129.md) isolates the plain-to-auto-pursuit daemon start fixture
in a fresh CI process after macOS suite handoff left plain daemon stop unclean.
[WP-130](plan/WP-130.md) puts the remaining MockServer-backed CLI acceptance
fixtures on two-worker Tokio runtimes so dispatch/workflow/stream/review wire
paths stay schedulable under suite load.
[WP-131](plan/WP-131.md) isolates graceful-stop restart settlement in a fresh CI
process, completing the long-delay restart fixture process isolations.
[WP-132](plan/WP-132.md) widens the cooperative pursuit-cancel wall-clock check
to two seconds after macOS suite load exceeded the prior one-second bound.
[WP-133](plan/WP-133.md) isolates the supervisor panic-backoff fixture in a
fresh CI process after macOS suite handoff left a `supervisor_cont` orphan.
[WP-134](plan/WP-134.md) puts MockServer-backed protocol client fixtures on
two-worker Tokio runtimes so complete/stream/retry wire paths stay schedulable
under suite load.
[WP-135](plan/WP-135.md) puts MockServer-backed daemon client unit fixtures on
two-worker Tokio runtimes so submit/control wire paths stay schedulable under
suite load.
[WP-136](plan/WP-136.md) isolates the native write-permission tool-lane fixture
in a fresh CI process alongside the other resident sandbox isolations.
[WP-137](plan/WP-137.md) isolates the native web-search frozen target fixture in
a fresh CI process with the other multi-thread native AgentRun isolations.
[WP-138](plan/WP-138.md) isolates the native multi-mock failover AgentRun fixture
in a fresh CI process under suite load.
[WP-139](plan/WP-139.md) isolates the native in-process cancel controller fixture
in a fresh CI process with the other multi-thread native AgentRun isolations.
[WP-140](plan/WP-140.md) isolates the configured tool-ask native AgentRun fixture
in a fresh CI process with the other multi-thread native AgentRun isolations.
[WP-141](plan/WP-141.md) isolates the read-only process-harness workdir fixture
in a fresh CI process after instrumented suite load left status polling with
only the daemon listen line.
[WP-142](plan/WP-142.md) isolates the configured native-ceiling AgentRun fixture
in a fresh CI process with the other multi-thread native AgentRun isolations.
[WP-143](plan/WP-143.md) isolates the exit-zero-without-terminal process fixture
in a fresh CI process with the other process-lane AgentRun isolations.
[WP-144](plan/WP-144.md) isolates the process-group cancel settlement fixture in
a fresh CI process with the other process-lane AgentRun isolations.
[WP-145](plan/WP-145.md) isolates the restart terminal-controller cleanup
fixture in a fresh CI process with the other process-lane AgentRun isolations.
[WP-146](plan/WP-146.md) isolates the MCP workflow resident-daemon fixture in a
fresh CI process with the other multi-thread daemon workflow isolations.
[WP-147](plan/WP-147.md) isolates the terminal polling budget fixture in a fresh
CI process with the other multi_thread Wiremock control-plane isolations.
[WP-148](plan/WP-148.md) isolates the submission retry budget fixture in a fresh
CI process with the other multi_thread Wiremock control-plane isolations.
[WP-149](plan/WP-149.md) isolates the resident native unit failover fixture in a
fresh CI process with the other multi_thread multi-mock native isolations.
[WP-150](plan/WP-150.md) isolates the resident native chain-drift unit fixture in
a fresh CI process with the other multi_thread multi-mock native isolations.
[WP-151](plan/WP-151.md) isolates the resident native post-tool no-failover unit
fixture in a fresh CI process, completing that multi_thread multi-mock unit
trio.
[WP-152](plan/WP-152.md) extends the authenticated durable workflow lifecycle
view with a single bounded success `output` / `output_omitted` projection so
MCP clients can complete submit → status → result without a new tool name or a
weaker redaction boundary.
[WP-153](plan/WP-153.md) isolates the detach fast-return success fixture in a
fresh CI process with the other multi_thread Wiremock control-plane isolations.
[WP-154](plan/WP-154.md) isolates the detach cancel finalize fixture in a fresh
CI process with the other multi_thread detach control-plane isolations.
[WP-155](plan/WP-155.md) isolates the detach fingerprint-cancel fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-156](plan/WP-156.md) isolates the detached auto-route freeze fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-157](plan/WP-157.md) isolates the detached no-frontier failover fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-158](plan/WP-158.md) isolates the detach config-error fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-159](plan/WP-159.md) isolates the detach bad-label fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-160](plan/WP-160.md) isolates the detached direct-HTTP write reject fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-161](plan/WP-161.md) isolates the legacy job-reader drift fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-162](plan/WP-162.md) isolates the legacy job JSON empty-stdin fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-163](plan/WP-163.md) isolates the task-list ordering fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.

WP-72 composes the P2 goal foundation into the resident daemon behind explicit
opt-in target/workdir/sandbox authority. One local goal is pursued at a time;
running checkpoints survive daemon replacement, while semantic pauses and live
foreign leases remain untouched. WP-73 adds only typed continuity policy and
visible idempotent quota-handoff state; it does not authorize a takeover
runtime. WP-80 adds only an opt-in owner-frozen continuity-next REST read over an
atomic redacted snapshot. WP-81 adds the typed observation ingress; WP-82 adds
only the purpose-authenticated bounded runner seam. General goal APIs, concrete
queue/execution ports and periodic assembly remain open.

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
| ~~**background task control**~~ | ✅ CLI detached, REST async, daemon workflow tasks, and the daemon's Linux Process and Native AgentRun lanes use durable SQLite lifecycle truth, scoped cancellation, and exact recovery (WP-39, WP-40, WP-61, WP-63–66). CLI detached submissions freeze capability admission before the task row/process and transfer a Linux pinned workdir descriptor to the worker; AgentRun submissions freeze a private prompt spool plus target/capability/workdir snapshots before queue creation. Native daemon-level graceful-shutdown race acceptance now covers cooperative cancellation, deadline-bound restart settlement, no replay/no output, and exact terminal residue cleanup; automatic replay and live pause/resume remain future work. |
| ~~**daemon**~~ | ✅ A resident, loopback-only process owns admitted workflow execution and, on Linux, both CLI-harness Process AgentRuns and fresh sessionless Native AgentRuns after the submitting client exits. It provides authenticated workflow control plus AgentRun submit/status/output/cancel, exact backend recovery, completion publication, and cooperative drain for Process and Native lanes (WP-40, WP-61, WP-63–66). Restart never replays or resumes AgentRun input. |

## v0.3 — integration surface and smarter routing

| milestone | scope |
|-----------|-------|
| ~~**MCP server**~~ | ✅ the library keeps six base tools; the CLI injects an authenticated `WorkflowControl` adapter and exposes nine over stdio, adding durable workflow submit/status/idempotent-cancel (WP-62). [WP-89](plan/WP-89.md) moves the transport and real-protocol fixtures to stable `rmcp` 3.0 with a Rust 1.88 MSRV. Workflow input is strictly bounded and allowlisted; [WP-152](plan/WP-152.md) adds the bounded success-output projection on the same lifecycle view. The MCP crate owns no daemon discovery or credential. Task, board, collaboration, and multi-principal owner context remain open. |
| ~~**REST API**~~ | ✅ bearer-authenticated loopback-only HTTP JSON API (`vyane serve`, axum): `/v1/dispatch`, `/v1/broadcast`, `/v1/runs`, `/v1/sessions`, `/v1/health`; non-loopback bind/Host/Origin and cross-site requests are rejected, run/session results use allowlisted views, and the per-start token is mode `0600`. This is not hostile same-UID or multi-user isolation. |
| ~~**shared service layer**~~ | ✅ `vyane-service` crate: one `VyaneService` facade shared by CLI, REST, and MCP front-ends, with allowlisted run/session views and owner-local session list/inspect/reset-native. Optional owner-bound message and AgentRun projection-only components require explicit construction and do not alter ordinary dispatch. There is no public fork or REST/MCP reset mutation. |
| ~~**local A2A message CLI**~~ | ✅ `vyane a2a send/inbox/read` over the transactional message store, with explicit owner/mailbox scope, delayed visibility, bounded stable JSON pages and fenced read acknowledgement (WP-59). This is a same-machine queue, not authenticated multi-user authority or A2A HTTP compatibility. |
| ~~**local goal lifecycle CLI**~~ | ✅ `vyane goal create/get/list/next/start/progress/pause/resume/done/fail/cancel` over one owner-scoped SQLite truth; snapshot and immutable event revisions commit atomically, with stable JSON and persisted acceptance descriptors (WP-60). Verification, pursuit, quota handoff and authenticated service exposure remain future work. |
| ~~**solution-review workflow**~~ | ✅ built-in `vyane review` command: three-step workflow (implement → fan-out review → synthesize) on the existing engine. It is not yet the original structured git diff/PR review product. |
| ~~**pluggable routing**~~ | ✅ Deterministic complexity/tag/tier policy plus executable `--target auto`: the service preserves provider/profile identity, applies effort to HTTP and CLI harnesses, records the decision in ledger labels, and supports deferred workflow routing (WP-38). Closed typed workflow effort now uses explicit > selected-profile > tier-default precedence, freezes one canonical effective value across failover and replay surfaces, and rejects route hints outside deferred single targets (WP-46). Cross-implementation fixtures and production-complete tier/model policy remain open. |

## v0.4 — streaming, daemon, and package readiness

| milestone | scope |
|-----------|-------|
| ~~**kernel streaming API**~~ | ✅ `Dispatcher::dispatch_stream` — callback-based streaming with kernel-owned record assembly, timeout/cancellation, and the legacy `Ok(None)` unsupported contract. The additive prepared probe/fallback seam lets the CLI reuse one execution id without changing that public behavior (WP-18). |
| ~~**SSE streaming endpoint**~~ | ✅ `POST /v1/dispatch/stream` — Server-Sent Events for real-time delta delivery over HTTP (WP-19). |
| ~~**async task registry**~~ | ✅ `POST/GET /v1/tasks` + real cancellation-token propagation, now backed by secret-free durable metadata and restart recovery (WP-21, WP-39). |
| ~~**registry package preflight**~~ | ✅ 17-crate metadata complete, workspace path deps versioned, and the publish workflow is fail-closed (WP-20, WP-37, WP-39, WP-56, WP-60). Manual dispatch requires an existing release tag on the exact current `main` SHA plus a protected `crates-io` environment with required reviewers and self-review prevention; the token reaches only the publish step. The 17-crate local preflight passes; no crate has been published. This is package readiness, not parity. |
| ~~**harness streaming**~~ | ✅ Claude Code / Codex CLI stdout events flow through `Harness::run_stream`, the kernel, CLI, and REST SSE, with fake-CLI protocol fixtures and cancellation coverage (WP-36). Additive scoped harness methods carry the Linux pinned-workdir handle; legacy harness implementations remain source-compatible and fail closed if handed a pin they do not implement. |
| **external distribution (deferred; not parity)** | crates.io upload is an external, hard-to-reverse action. A tag and `CARGO_REGISTRY_TOKEN` remain data and credential, not authority. The only repository workflow path is manual dispatch from current `main`, followed by approval in the protected `crates-io` environment by someone other than the dispatcher. Do not create the tag, approve the environment, or upload without separate explicit authorization. |
| ~~**daemon (resident process)**~~ | ✅ Persistent ownership for managed workflows plus Linux Process and fresh/sessionless Native AgentRun lanes; authenticated loopback control, client-generated UUIDv7 idempotency, exact backend cleanup/recovery, completion handback, and Process/Native graceful drain (WP-40, WP-61, WP-63–66). The Native shutdown race is covered through deadline-bound restart settlement without replay or output; there is no `Remote`, native session resume, live pause/resume, or automatic restart replay. |
