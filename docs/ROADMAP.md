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
[WP-164](plan/WP-164.md) raises CI test and coverage job timeout headroom from
20 to 30 minutes after sequential isolation suite growth.
[WP-165](plan/WP-165.md) isolates the daemon-client bearer/redirect fixture in a
fresh CI process with other multi_thread CLI unit isolations.
[WP-166](plan/WP-166.md) isolates the daemon-client oversized-response fixture in a
fresh CI process with other multi_thread CLI unit isolations.
[WP-167](plan/WP-167.md) isolates the daemon-client 4xx submission fixture in a
fresh CI process with other multi_thread CLI unit isolations.
[WP-168](plan/WP-168.md) isolates the daemon-client 5xx submission fixture in a
fresh CI process with other multi_thread CLI unit isolations.

[WP-169](plan/WP-169.md) isolates the daemon-client malformed-accepted
response fixture in a fresh CI process (suite skip + `--exact`).

[WP-170](plan/WP-170.md) isolates the daemon-client oversized-accepted
response fixture in a fresh CI process (suite skip + `--exact`).

[WP-171](plan/WP-171.md) isolates the daemon-client accepted-another-id
response fixture in a fresh CI process (suite skip + `--exact`).

[WP-172](plan/WP-172.md) isolates the daemon-client submission-timeout
fixture in a fresh CI process (suite skip + `--exact`).

[WP-173](plan/WP-173.md) isolates the daemon-client workflow-status-missing
fixture in a fresh CI process (suite skip + `--exact`).

[WP-174](plan/WP-174.md) isolates the daemon-client workflow-cancel-malformed
fixture in a fresh CI process (suite skip + `--exact`).

[WP-175](plan/WP-175.md) isolates the daemon-client workflow-auth-drift
fixture in a fresh CI process (suite skip + `--exact`).

[WP-176](plan/WP-176.md) isolates the daemon-workflow concurrent-creates
fixture in a fresh CI process (suite skip + `--exact`).

[WP-177](plan/WP-177.md) prints the WP-152 bounded success `output` (or
`output omitted`) on human-mode daemon `workflow status` / submit responses.

[WP-178](plan/WP-178.md) prints the durable `failure_code` already on the
daemon workflow task record in the same human-mode status / submit responses.

[WP-179](plan/WP-179.md) prints each detached `TaskRow.failure_code` on human
`task list` when present (JSON already serialized the field).

[WP-180](plan/WP-180.md) extracts a pure human durable `task status` formatter
and unit-tests `failure_code` snake_case lines on the shipped path.

[WP-181](plan/WP-181.md) extracts a pure human `workflow list` formatter and
unit-tests status names plus step-count totals on real journal summaries.

[WP-182](plan/WP-182.md) extracts a pure human broadcast result table formatter
and unit-tests success and error row projection.

[WP-183](plan/WP-183.md) extracts a pure human run-record line formatter and
unit-tests status, target, duration, and optional cost projection.

[WP-184](plan/WP-184.md) extracts a pure human session-view line formatter and
unit-tests native state / resume flags with terminal-safe fields.

[WP-185](plan/WP-185.md) extracts a pure human legacy session line formatter and
unit-tests id / target / run_count / updated_at projection.

[WP-186](plan/WP-186.md) extracts a pure human legacy `task status` formatter and
unit-tests displayed state vs file state plus optional error/log lines.

[WP-187](plan/WP-187.md) extracts a pure human workflow run summary formatter and
unit-tests status, path, optional replay, and step first-line output.

[WP-188](plan/WP-188.md) extracts a pure human goal list line formatter and
unit-tests status Display, priority, and terminal-safe id/title fields.

[WP-189](plan/WP-189.md) extracts a pure human continuity next-action formatter
with snake_case action/command tokens (no Debug) and optional durable field
lines for the WP-79 operator projection.

[WP-190](plan/WP-190.md) extracts a pure human `vyane route` result formatter
and unit-tests profile + RouteDecision fields (tier/effort as_str, score, safe
free text).

[WP-191](plan/WP-191.md) extracts a pure human takeover approval line formatter
for continuity queue/decide responses (safe id/goal/step + status Display).

[WP-192](plan/WP-192.md) extracts a pure human continuity-signal result line
formatter (safe goal id, recorded|unchanged, next_ready_step).

[WP-193](plan/WP-193.md) extracts a pure human `vyane check` permission summary
formatter over the redacted harness + native ceiling axes (P1 permission
operator surface).

[WP-194](plan/WP-194.md) extracts a pure human goal pursue outcome formatter
(status Display, segment counters, terminal-safe summary/reason).

[WP-195](plan/WP-195.md) extracts a pure human goal verify result formatter
(success|inconclusive, terminal-safe goal id and acceptance summary).

[WP-196](plan/WP-196.md) extracts a pure human goal progress event line formatter
(safe event/goal ids, kind Display, revision).

[WP-197](plan/WP-197.md) routes human `goal get` event rows through pure
`format_goal_event_line` and drops the private CLI `event_kind_text` table in
favor of domain `GoalEventKind` Display.

[WP-198](plan/WP-198.md) extracts the pure daemon workflow status/submit human
formatter into `output.rs` with the existing WP-152/WP-178 unit coverage.

[WP-199](plan/WP-199.md) extracts pure human local A2A send/inbox/read formatters
with terminal-safe ids, codes, kind, delivery status, and body.

[WP-200](plan/WP-200.md) extracts pure human cancel final-state lines for
detached tasks and daemon workflow cancel.

[WP-201](plan/WP-201.md) extracts pure human `vyane check` section formatters
for config files, providers, profiles, harnesses, and profile environment rows.

[WP-202](plan/WP-202.md) extracts pure human cancel idempotency lines
(`{id} already {state}`) for legacy and durable terminal cancel paths.

[WP-203](plan/WP-203.md) extracts pure human daemon lifecycle lines (already
running / started / running status / stopped).

[WP-204](plan/WP-204.md) extracts pure session-control error classification plus
empty task-list and detached run-id human lines.

[WP-205](plan/WP-205.md) extracts pure native ask-surface model outputs
(`approval_required_output` / `missing_approval_plan_output`) with safe plan
hash prefixes.

[WP-206](plan/WP-206.md) extracts pure deny / unknown / invalid native tool
model outputs used on the prepare path.

[WP-207](plan/WP-207.md) extracts pure cancel / timeout / panic / unavailable /
failed native tool execution outputs on ordinary and authorized paths.

[WP-208](plan/WP-208.md) extracts pure workflow error human prefix lines
(`config error:` vs `error:`).

[WP-209](plan/WP-209.md) extracts pure detached-task diagnostic lines (no such
run / no output / not local dispatch) with terminal-safe ids.

[WP-210](plan/WP-210.md) extracts pure `vyane serve` loopback/start lines and a
shared config-error prefix helper.

[WP-211](plan/WP-211.md) routes remaining CLI `config error:` stderr through
`format_config_error_line` (no residual ad-hoc literals in command.rs).

[WP-212](plan/WP-212.md) extracts pure generic `error:` and `worker error:`
CLI stderr prefix helpers for command/main paths.

[WP-213](plan/WP-213.md) composes workflow error human lines from the shared
config-error and error prefix helpers.

[WP-214](plan/WP-214.md) composes session-control error human lines from the
shared `format_error_line` helper.

[WP-215](plan/WP-215.md) extracts pure daemon listen/not-running banners and
goal CLI error prefix lines.

[WP-216](plan/WP-216.md) extracts pure A2A error / inbox-more and cancel
diagnostic lines (kill unfinalized, not-local frontend cancel).

[WP-217](plan/WP-217.md) extracts pure cancel ownership/diagnostic lines and
workflow observer step event lines.

[WP-218](plan/WP-218.md) extracts pure legacy cancel identity / nested-harness
cleanup diagnostics (pid-reuse refuse, incomplete process groups, and related
operator lines).

[WP-219](plan/WP-219.md) extracts pure durable cancel refuse-control and terminal
cleanup-failed diagnostics (process identity unavailable; reuse cleanup helper).

[WP-220](plan/WP-220.md) extracts pure worker/stale-detached diagnostics (stale
status, metadata settlement, output write, nested harness controller
write/cleanup).

[WP-221](plan/WP-221.md) extracts pure dispatch stream notices (unsupported /
not-applicable fallback) and tool-use progress lines with terminal-safe free
text.

[WP-222](plan/WP-222.md) extracts pure REST task-supervisor lifecycle operator
logs (init cleanup, duplicate runtime, dispatch fail/panic, output artifact).

[WP-223](plan/WP-223.md) extracts pure REST metadata settlement, goal-read,
task output read, and serve listening operator logs.

[WP-224](plan/WP-224.md) extracts pure REST handler label/stream/broadcast and
ledger/session query operator logs (remaining serve stderr ad-hoc paths).

[WP-225](plan/WP-225.md) extracts pure `vyane check` section header lines
(providers / profiles / harnesses / profile environment).

[WP-226](plan/WP-226.md) extracts pure check profile resolve-warning body and
dispatch run-failure stderr (no `error:` prefix contract frozen).

[WP-227](plan/WP-227.md) consolidates native turn-driver invalid-JSON tool
arguments into the pure tool ERROR helper surface (WP-205–207).

[WP-228](plan/WP-228.md) publishes domain `as_str`/`Display` for continuity
signal kinds, criterion status, and pursuit checkpoint/segment status; CLI
signal evidence uses domain tokens.

[WP-229](plan/WP-229.md) publishes domain `as_str`/`Display` for continuity
mode/status/step status and takeover decision tokens.

[WP-230](plan/WP-230.md) makes `GoalStatus::as_str` public (Display already
existed), aligning the goal status token API with sibling domain enums.

[WP-231](plan/WP-231.md) adds `Display` for `TakeoverRunStatus` (via existing
`as_str`), finishing the takeover status token surface.

[WP-232](plan/WP-232.md) publishes domain `as_str`/`Display` for
`WorkflowRunStatus` and `JournalStepStatus`; CLI `workflow_status_name` uses
domain tokens only.

[WP-233](plan/WP-233.md) routes workflow summary step status through domain
`JournalStepStatus::as_str` (no Debug + to_lowercase).

[WP-234](plan/WP-234.md) adds `Display` for `SessionNativeState` (via existing
`as_str`), matching the domain token train for session human lines.

[WP-235](plan/WP-235.md) publishes domain `as_str`/`Display` for
`NativePermissionAxisStatus`; CLI permission-check axis labels use domain
tokens only.

[WP-236](plan/WP-236.md) publishes domain `as_str`/`Display` for `RunStatus`
(snake_case) and `Sandbox` (kebab-case); CLI `status_name` / `sandbox_name`
use domain tokens only.

[WP-237](plan/WP-237.md) routes CLI `SandboxArg` Display through domain
`Sandbox::as_str` (no private kebab-case match table).

[WP-238](plan/WP-238.md) adds CLI `RunStatusArg` Display through domain
`RunStatus::as_str`.

[WP-239](plan/WP-239.md) adds CLI `GoalStatusArg` Display through domain
`GoalStatus::as_str` (including `in_progress`).

[WP-240](plan/WP-240.md) adds CLI takeover-decision and continuity-signal arg
Display through domain tokens; decide/signal command paths use `From`.

[WP-241](plan/WP-241.md) publishes `as_str` on all `vyane-task` string enums
via the shared macro (`TaskState`, origin, kind, failure code, event kind).

[WP-242](plan/WP-242.md) adds `Display` for `Effort` (via existing `as_str`),
matching the domain token train for routing human lines.

[WP-243](plan/WP-243.md) adds `Display` for `RouteTier` and `RouteEffort`
(via existing `as_str`).

[WP-244](plan/WP-244.md) adds `Display` for router `IntentCategory` (via
existing `as_str`, including multi-word `code_gen`).

[WP-245](plan/WP-245.md) publishes domain `as_str`/`Display` for configuration
diagnostic enums (check/credential/profile status and issue codes).

[WP-246](plan/WP-246.md) publishes domain `as_str`/`Display` for kernel
`ErrorKind` (including multi-word tokens and `other` for unknown serde kinds).

[WP-247](plan/WP-247.md) routes session-control error `kind_code` through
`ErrorKind::as_str` where the operator contract already matches domain tokens.

[WP-248](plan/WP-248.md) publishes domain `as_str`/`Display` for
`RouteSelectionBasis` on route-preview diagnostics.

[WP-249](plan/WP-249.md) publishes domain `as_str`/`Display` for goal
observation closed enums (`GoalObservationSignalKind`,
`GoalObservationStatus`, `GoalObservationWatcherErrorCode`,
`GoalObservationWatchStatus`).

[WP-250](plan/WP-250.md) publishes domain `as_str`/`Display` for quota closed
enums (`QuotaStatus`, `QuotaUnit`, `QuotaConnectorErrorCode`,
`QuotaSnapshotStatus`).

[WP-251](plan/WP-251.md) publishes domain `as_str`/`Display` for goal
next-action view enums (`GoalNextActionKind`, `GoalOperatorCommand`,
`GoalSignalKind`, `GoalNextReasonCode`), single-sourcing action/command/signal
tokens from the core projection enums.

[WP-252](plan/WP-252.md) publishes domain `as_str`/`Display` for
`GoalContinuityRunStatus` on the bounded continuity runner redacted result.

[WP-253](plan/WP-253.md) publishes public `as_str` for `vyane-message`
`string_enum!` tokens (`MessageDirection`, `EndpointKind`, `DeliveryStatus`,
`MessageEventKind`, `MessagePublicationStatus`), matching WP-241 for tasks.

[WP-254](plan/WP-254.md) publishes public `as_str` for `vyane-agent`
`string_enum!` tokens (`RunState`, `ExecutionBackend`, `AgentEventKind`, and
siblings), completing the store string-enum public token train with WP-241/253.

[WP-255](plan/WP-255.md) publishes domain `as_str`/`Display` for kernel
`AuthStyle` and `InheritMode` (`bearer` / `x_api_key`, `scrub` / `full`).

[WP-256](plan/WP-256.md) publishes domain `as_str`/`Display` for
`EnvInjectSource` and ledger event enums (`EventCategory`, `EventSource`,
`EventDurability`).

[WP-257](plan/WP-257.md) publishes domain `as_str`/`Display` for kernel
capability enums (`FilesystemCapability`, `IsolationStrength`,
`CapabilityRejectionReason`).

[WP-258](plan/WP-258.md) publishes domain `as_str`/`Display` for MCP workflow
projection enums (`WorkflowState`, `WorkflowFailureCode`).

[WP-259](plan/WP-259.md) publishes domain `as_str`/`Display` for config native
policy enums (`NativeToolPermissionEffectConfig`,
`NativeCommandNetworkRouteConfig`).

[WP-260](plan/WP-260.md) publishes public `as_str` for harness
`PermissionEffect` and domain tokens for `NativeCommandNetworkRoute`, aligning
runtime vocabulary with WP-259 config enums.

[WP-261](plan/WP-261.md) publishes domain `as_str`/`Display` for native
`ToolInvocationStatus`, including the permission-ask terminal
`approval_required`.

[WP-262](plan/WP-262.md) publishes domain `as_str`/`Display` for
`GoalContinuityPortResult` and completes `Display` for
`WebSearchContextSize`.

[WP-263](plan/WP-263.md) publishes domain kind tokens for `NativeTurnStop`
(`approval_required`, `budget_exhausted`, …) without leaking reply or
approval payloads.

[WP-264](plan/WP-264.md) wires native status tokens on shipped paths:
`ToolInvocationStatus::is_executed` drives turn-driver `is_error`, and native
agent settlement maps `NativeTurnStop::as_str` kinds onto durable failure codes.

[WP-265](plan/WP-265.md) locks config↔harness native permission/network token
parity on the service adapter path that composes permission ceilings.

[WP-266](plan/WP-266.md) adds pure human `format_tool_invocation_status_line`
over `ToolInvocationStatus` tokens (`approval_required`, `timed_out`, …).

[WP-267](plan/WP-267.md) wires that pure status line onto the CLI stream
`ToolUse` path (progress uses `executed`).

[WP-268](plan/WP-268.md) adds pure human `format_native_turn_stop_line` over
`NativeTurnStop` kind tokens and keeps it reachable from native settlement.

[WP-269](plan/WP-269.md) emits that pure stop line on the native settlement
failure path via `tracing::info!` (`stop_kind` / `failure_code` tokens only).

[WP-270](plan/WP-270.md) wires `PursuitStatus` / `GoalStatus` `as_str` tokens
onto the resident goal pursuit settlement diagnostic (replacing Debug `?`).

[WP-271](plan/WP-271.md) publishes `LifecycleObservation` kind tokens and wires
them onto Process AgentRun dispatch diagnostics (replacing Debug `?state`).

[WP-272](plan/WP-272.md) publishes completion-stage and controller-cleanup kind
tokens and replaces Debug `?staged` / `?report` daemon warn dumps with
`stage_error` / `unresolved` counts.

[WP-273](plan/WP-273.md) publishes `AgentExecutionItemStatus` /
`AgentExecutionError` kind tokens and logs the first degraded execution item
kind on supervisor backoff.

[WP-274](plan/WP-274.md) publishes recovery/completion projection status tokens
and logs first degraded kinds on those supervisor backoff paths.

[WP-275](plan/WP-275.md) publishes recovery/completion/supervisor error kind
tokens and logs them on failed AgentRun supervisor cycles (replacing discarded
`Err(_)`).

[WP-276](plan/WP-276.md) logs execution/recovery driver construction failures
with the same `as_str` error kind tokens (closing the WP-275 residual).

[WP-277](plan/WP-277.md) publishes goal-read / continuity-runner error kind
tokens and pure `format_goal_read_error_kind_line`, wired onto REST
continuity-next diagnostics.

[WP-278](plan/WP-278.md) adds pure continuity runner/authority error kind lines
and logs underlying `GoalReadError` kinds when the continuity runner cannot
open its goal reader.

[WP-279](plan/WP-279.md) publishes `GoalObservationIngressError` /
`GoalObservationRunnerError` kind tokens for the observation mutation seam.

[WP-280](plan/WP-280.md) adds pure observation ingress/runner error kind lines
and logs ingress open failures with those tokens.

[WP-281](plan/WP-281.md) logs body-free `reason = "panic"` on AgentRun
supervisor catch_unwind arms that increment `panicked_cycles`.

[WP-282](plan/WP-282.md) publishes `NativePermissionSetError` kind tokens for
native permission ceiling validation failures.

[WP-283](plan/WP-283.md) adds pure `format_native_permission_set_error_kind_line`
and logs that kind when daemon native permission ceiling application fails.

[WP-284](plan/WP-284.md) publishes in-process authority/completion/native-bind
error kind tokens for the P1 native execution authority seam.

[WP-285](plan/WP-285.md) publishes `InProcessAssemblyError` kind tokens for
assembly registry construction failures.

[WP-286](plan/WP-286.md) publishes `OwnerContextError` kind tokens for
principal-to-owner binding failures.

[WP-287](plan/WP-287.md) adds pure `format_owner_context_error_kind_line` and
logs authentication failure kinds on `OwnerContextFactory::authenticate`.

[WP-288](plan/WP-288.md) logs remaining owner-context construction kinds
(`invalid_principal`, `resolution_failed`, `invalid_owner`, `reserved_owner`).

[WP-289](plan/WP-289.md) publishes `AgentMessageCompletionReadError` and
`DiagnosticErrorKind` tokens.

[WP-290](plan/WP-290.md) publishes kind-only tokens for
`PermissionRuleError` / `ToolRegistryError` on the native ask registration
surface.

[WP-291](plan/WP-291.md) publishes kind-only tokens for `ToolContextError`,
`NativeCommandPolicyError`, and `NativeReadPolicyError`.

[WP-292](plan/WP-292.md) publishes kind tokens for write/filesystem/network/
web-fetch/web-search native policy errors.

[WP-293](plan/WP-293.md) publishes kind-only tokens for
`ToolChatValidationError` on the bounded typed tool-chat boundary.

[WP-294](plan/WP-294.md) publishes kind-only tokens for `EditError` on the
bounded trusted text-edit tool surface.

[WP-295](plan/WP-295.md) adds pure `format_tool_chat_validation_error_kind_line`
and logs validation kinds on the native turn driver request/response boundary.

[WP-296](plan/WP-296.md) logs preflight next-request validation kinds before
tool side effects (closing the WP-295 residual).

[WP-297](plan/WP-297.md) publishes kind tokens for `NativeTurnLimitError`,
host Unsupported, and `RegisterCommandToolError`.

[WP-298](plan/WP-298.md) publishes kind-only tokens for `AgentStoreError` on
the AgentRun durable store surface.

[WP-299](plan/WP-299.md) adds pure `format_agent_store_error_kind_line` and
logs store kinds when native authority maps store failures.

[WP-300](plan/WP-300.md) publishes kind-only tokens for web-fetch/web-search
tool registration errors.

[WP-301](plan/WP-301.md) publishes kind-only tokens for `GoalStoreError` on the
owner-scoped goal durable store surface.

[WP-302](plan/WP-302.md) adds pure `format_goal_store_error_kind_line` and logs
goal-store kinds on the resident goal daemon paths.

[WP-303](plan/WP-303.md) publishes kind-only tokens for `MessageStoreError` on
the owner-scoped message durable store surface.

[WP-304](plan/WP-304.md) publishes kind-only tokens for `TaskStoreError` on the
owner-scoped task durable store surface.

[WP-305](plan/WP-305.md) publishes kind-only tokens for quota validation, runner,
and transport error surfaces.

[WP-306](plan/WP-306.md) publishes kind-only tokens for `BrokerError` on the
local message/agent projection broker surface.

[WP-307](plan/WP-307.md) publishes kind-only tokens for `EventLogError` on the
durable event-log surface.

[WP-308](plan/WP-308.md) publishes kind-only tokens for `WorkflowError` on the
workflow engine / journal surface.

[WP-309](plan/WP-309.md) adds pure `format_workflow_error_kind_line` and logs
workflow kinds on the resident workflow daemon failure paths.

[WP-310](plan/WP-310.md) publishes kind-only tokens for `WorkflowControlError`
on the MCP/daemon workflow control adapter surface.

[WP-311](plan/WP-311.md) adds pure task/message store error lines and logs
task-store kinds on workflow lease renewal and completion retry.

[WP-312](plan/WP-312.md) adds pure event-log and workflow-control error kind
lines for operator diagnostics.

[WP-313](plan/WP-313.md) adds pure register web-fetch/web-search tool error kind
lines for operator diagnostics.

[WP-314](plan/WP-314.md) logs kind-only tokens when the resident goal supervisor
fails or shutdown handlers cannot be installed.

[WP-315](plan/WP-315.md) logs kind-only tokens on workflow controller open,
cleanup, join, and invalid-task recovery paths.

[WP-316](plan/WP-316.md) logs a fixed kind when harness lifecycle stop reporting
fails (closing the last workspace `error = %error` structured-log site).

[WP-317](plan/WP-317.md) publishes lowercase kind tokens for workflow step
`OnError` policy (`abort` / `continue`).

[WP-318](plan/WP-318.md) logs `ErrorKind` tokens on kernel post-run ledger
append and session update best-effort warnings.

[WP-319](plan/WP-319.md) publishes kind-only tokens for daemon workflow submit
and control client errors.

[WP-320](plan/WP-320.md) adds pure workflow submit and control-client error kind
lines for operator diagnostics.

[WP-321](plan/WP-321.md) adds pure `on_error` policy lines for workflow step
failure policy tokens.

[WP-322](plan/WP-322.md) adds pure `ErrorKind` lines for the kernel failover
taxonomy tokens.

[WP-323](plan/WP-323.md) adds pure `BrokerError` lines after wiring a direct
CLI dependency on `vyane-broker`.

[WP-324](plan/WP-324.md) adds pure quota validation/runner/transport error
lines after wiring a direct CLI dependency on `vyane-quota`.

[WP-325](plan/WP-325.md) publishes kind-only tokens for broker `PumpItemStatus`
and logs degraded pump items with those kinds.

[WP-326](plan/WP-326.md) publishes kind-only tokens for `NativeSideEffect` and
`InProcessAgentEffect` on the native revalidation surface.

[WP-327](plan/WP-327.md) adds pure native-side-effect and in-process-effect
lines for operator diagnostics.

[WP-328](plan/WP-328.md) publishes kind-only tokens for `GoalObservationKind`
and `NativeSessionState`.

[WP-329](plan/WP-329.md) adds pure goal-observation-kind and native-session-state
lines for operator diagnostics.

[WP-330](plan/WP-330.md) publishes kind-only tokens for message `NackDisposition`
and a pure nack diagnostic line.

[WP-331](plan/WP-331.md) logs kind-only nack disposition and pump status on
broker nack settlement.

[WP-332](plan/WP-332.md) logs workflow submit and control-client kinds when
mapping MCP control errors.

[WP-333](plan/WP-333.md) logs kind-only native side-effect rejections on
model/tool authority revalidation.

[WP-334](plan/WP-334.md) logs kind-only observation kind and status on goal
observation ingest.

[WP-335](plan/WP-335.md) logs kind-only workflow-control errors when MCP maps
them to public tool errors.

[WP-336](plan/WP-336.md) publishes kind-only tokens for agent `CancelOutcome`
and a pure CLI cancel-outcome line.

[WP-337](plan/WP-337.md) logs kind-only cancel outcomes after durable agent
cancel settlement.

[WP-338](plan/WP-338.md) publishes kind-only tokens for broker
`AdapterOutcome` / `AdapterFailure` and pure CLI adapter lines.

[WP-339](plan/WP-339.md) logs kind-only adapter outcomes and failures on the
broker pump delivery path.

[WP-340](plan/WP-340.md) publishes kind-only tokens for dispatch
`AttemptOutcome` and a pure CLI attempt-outcome line.

[WP-341](plan/WP-341.md) publishes kind-only tokens for
`ControllerRecoveryObservation` and a pure CLI recovery-observation line.

[WP-342](plan/WP-342.md) logs kind-only controller recovery observations on the
daemon agent cancel path.

[WP-343](plan/WP-343.md) publishes kind-only tokens for `AgentExecutorOutcome`
and `ReplaySafety`, plus pure CLI lines.

[WP-344](plan/WP-344.md) publishes kind-only tokens for agent `RunSettlement`
and task `TaskSettlement`, plus pure CLI settlement lines.

[WP-345](plan/WP-345.md) publishes kind-only tokens for
`AgentExecutionSettlement` and a pure CLI execution-settlement line.

[WP-346](plan/WP-346.md) publishes kind-only tokens for
`NativeSessionTransition` and a pure CLI session-transition line.

[WP-347](plan/WP-347.md) publishes kind-only tokens for
`AgentCompletionSinkObservation` / `AgentCompletionSinkTransition` and pure CLI
completion-sink lines.

[WP-348](plan/WP-348.md) logs kind-only completion-sink observations on the
agent recovery path.

[WP-349](plan/WP-349.md) publishes kind-only tokens for public
`RunAttemptOutcomeView` and a pure CLI run-attempt line.

[WP-350](plan/WP-350.md) logs kind-only agent recovery item statuses after each
recovered ticket, plus a pure CLI recovery-item line.

[WP-351](plan/WP-351.md) logs kind-only agent execution item statuses after each
executed claim, plus a pure CLI execution-item line.

[WP-352](plan/WP-352.md) logs kind-only agent completion projection item
statuses after each projected event, plus a pure CLI projection-status line.

[WP-353](plan/WP-353.md) adds pure CLI kind-only lines for lifecycle
`RunState` / `TaskState` / `GoalStatus` / `WorkflowRunStatus`.

[WP-354](plan/WP-354.md) adds pure CLI kind-only lines for
`DeliveryStatus` / `MessagePublicationStatus` / `RunCompletionStatus`.

[WP-355](plan/WP-355.md) adds pure CLI kind-only lines for goal
`PursuitStatus` / `CriterionStatus` / `GoalContinuityStatus` /
`TakeoverApprovalStatus`.

[WP-356](plan/WP-356.md) adds pure CLI kind-only lines for
`JournalStepStatus` / `WorkflowState` / `GoalContinuityStepStatus`.

[WP-357](plan/WP-357.md) adds pure CLI kind-only lines for
`MessageEventKind` / `RunStatus` / `GoalContinuityMode`.

[WP-358](plan/WP-358.md) adds pure CLI kind-only lines for
`TakeoverDecision` / `TakeoverRunStatus` / `PursuitCheckpointStatus` /
`PursuitSegmentStatus`.

[WP-359](plan/WP-359.md) adds pure CLI kind-only lines for goal observation
signal/status/watch kinds.

[WP-360](plan/WP-360.md) adds pure CLI kind-only lines for diagnostics
config/credential/profile/permission-axis statuses.

[WP-361](plan/WP-361.md) wires pure cancel-outcome and controller-recovery
observation lines onto daemon agent cancel logs.

[WP-362](plan/WP-362.md) wires pure pursuit and goal status lines onto resident
goal pursuit settlement logs.

[WP-363](plan/WP-363.md) wires pure goal/task store and workflow error lines onto
daemon and MCP mapping logs.

[WP-364](plan/WP-364.md) clears leftover `dead_code` allows on pure store/workflow
error lines wired by WP-363.

[WP-365](plan/WP-365.md) wires pure register web_fetch/web_search error lines onto
native AgentRun tool registration failures.

[WP-366](plan/WP-366.md) adds pure register run_command error lines and wires them
onto native AgentRun command-tool registration failures.

[WP-367](plan/WP-367.md) adds pure permission-rule error lines and wires them onto
native AgentRun permission-policy assembly failures.

[WP-368](plan/WP-368.md) adds pure native filesystem policy error lines and wires
them onto native AgentRun workspace registry assembly failures.

[WP-369](plan/WP-369.md) wires pure goal-store error lines onto soft-handled
resident goal quarantine pause failures.

[WP-370](plan/WP-370.md) wires pure goal-store error lines onto resident pursuit
config/construct/stop error logs.

[WP-371](plan/WP-371.md) adds pure completion-stage error lines and wires them
onto Process AgentRun completion staging failures.

[WP-372](plan/WP-372.md) adds pure lifecycle observation lines and wires them
onto Process AgentRun dispatch/stop-proof diagnostics.

[WP-373](plan/WP-373.md) wires pure run-status lines onto Process AgentRun
non-quiesced terminal status diagnostics.

[WP-374](plan/WP-374.md) wires pure executor/settlement lines onto Process
AgentRun final settle paths.

[WP-375](plan/WP-375.md) wires pure executor/settlement lines onto native
AgentRun settle paths (failure helper + completion stage).

[WP-376](plan/WP-376.md) wires pure agent-store error lines onto daemon AgentRun
create failures that cannot exact-retry.

[WP-377](plan/WP-377.md) wires pure agent-store error lines onto Process AgentRun
create failures that cannot exact-retry.

[WP-378](plan/WP-378.md) wires pure ErrorKind lines onto native AgentRun turn
failure and outer timeout mapping.

[WP-379](plan/WP-379.md) adds pure RunFailureCode lines and wires them onto native
and Process AgentRun quiesced failure settlements.

[WP-380](plan/WP-380.md) wires pure goal-store error lines onto resident goal
supervisor task failure.

[WP-381](plan/WP-381.md) wires pure MCP workflow-control error lines onto submit
and control error mapping.

[WP-382](plan/WP-382.md) wires pure workflow/task-store error lines onto daemon
workflow API failure mapping.

[WP-383](plan/WP-383.md) wires pure workflow-run status and task settlement lines
onto daemon workflow worker finish.

[WP-384](plan/WP-384.md) wires pure task settlement lines onto workflow finish
error and non-shutdown join settles.

[WP-385](plan/WP-385.md) wires pure run-state lines onto daemon AgentRun cancel
observation of terminal and cancelling states.

[WP-386](plan/WP-386.md) wires pure task-state lines onto daemon workflow cancel
observation of terminal and cancelling states.

[WP-387](plan/WP-387.md) wires pure MCP workflow-state lines onto successful
workflow cancel lifecycle observation.

[WP-388](plan/WP-388.md) wires pure run-completion status lines onto terminal
AgentRun view assembly when a completion record is present.

[WP-389](plan/WP-389.md) wires pure journal-step status lines onto daemon workflow
worker finish last-step observation.

[WP-390](plan/WP-390.md) wires pure attempt-outcome lines onto Process AgentRun
quiesced Error settlement when a last attempt is present.

[WP-391](plan/WP-391.md) wires pure delivery-status lines onto local A2A message
projection.

[WP-392](plan/WP-392.md) wires pure message-store error lines onto local A2A store
open failures.

[WP-393](plan/WP-393.md) wires pure takeover decision and approval status lines
onto goal continuity decide.

[WP-394](plan/WP-394.md) wires pure criterion status lines onto goal satisfy and
verify paths.

[WP-395](plan/WP-395.md) wires pure takeover-run status lines onto goal continuity
execute finish.

[WP-396](plan/WP-396.md) wires pure goal-status lines onto CLI goal human
projection.

[WP-397](plan/WP-397.md) wires pure takeover-approval status lines onto shared
takeover result projection and uses `as_str` in the human tab column.

[WP-398](plan/WP-398.md) wires pure pursuit-checkpoint status lines onto goal
CLI get when a durable checkpoint is present.

[WP-399](plan/WP-399.md) wires pure pursuit-status lines onto CLI goal pursue
settle (daemon already had them via WP-362).

[WP-400](plan/WP-400.md) makes goal human tab/status columns use explicit
`as_str` kind tokens instead of Display formatting.

[WP-401](plan/WP-401.md) wires pure continuity status/mode lines onto CLI
continuity-next and uses `as_str` for next-action/command human columns.

[WP-402](plan/WP-402.md) wires pure config-check, credential, profile, issue,
and native-permission-axis lines onto `vyane check`.

[WP-403](plan/WP-403.md) wires pure continuity step-status lines onto CLI
continuity-next for next-ready and projected plan steps.

[WP-404](plan/WP-404.md) wires pure pursuit-segment status lines onto the shared
DispatchGoalRuntime segment settle used by CLI pursue and daemon pursuit.

[WP-405](plan/WP-405.md) wires pure OnError policy lines onto review workflow
build for each frozen step policy.

[WP-406](plan/WP-406.md) wires pure continuity next-action kind lines onto CLI
continuity-next projection.

[WP-407](plan/WP-407.md) wires pure goal-event kind lines onto CLI goal get
event rows and progress settlement.

[WP-408](plan/WP-408.md) wires pure continuity signal-kind lines onto CLI
continuity-signal settle.

[WP-409](plan/WP-409.md) wires pure continuity operator-command lines onto CLI
continuity-next when a command is projected.

[WP-410](plan/WP-410.md) wires pure takeover-sandbox lines onto CLI continuity
queue after the approval freeze.

[WP-411](plan/WP-411.md) wires pure core sandbox lines onto CLI goal pursue
runtime freeze.

[WP-412](plan/WP-412.md) wires pure final goal-status lines onto CLI pursue
settle from the durable outcome field.

[WP-413](plan/WP-413.md) adds `Protocol::as_str` and wires pure protocol lines
onto `vyane check` per-provider rows.

[WP-414](plan/WP-414.md) wires pure harness-kind lines onto `vyane check`
harness availability rows.

[WP-415](plan/WP-415.md) wires pure permission-effect lines onto native
ApprovalRequired settle (ask surface).

[WP-416](plan/WP-416.md) wires pure auth-style lines onto native target freeze
when endpoint auth is present.

[WP-417](plan/WP-417.md) wires pure protocol lines onto native target freeze
(reusing WP-413 tokens).

[WP-418](plan/WP-418.md) wires pure web-search context-size lines onto native
web-search tool registration.

[WP-419](plan/WP-419.md) adds `AdapterTransport::as_str` and wires pure
transport lines onto native target freeze.

[WP-420](plan/WP-420.md) wires pure effort lines onto native genparams freeze
when effort is set.

[WP-421](plan/WP-421.md) wires pure task failure-code lines onto daemon
workflow Failed settle and interrupt paths.

[WP-422](plan/WP-422.md) wires pure execution-backend lines onto daemon
AgentRun submit freeze (process + native).

[WP-423](plan/WP-423.md) wires pure run-mode lines onto the same daemon
AgentRun submit freeze sites.

[WP-424](plan/WP-424.md) wires pure controller-kind lines onto daemon AgentRun
cancel dispatch (Process + InProcess).

[WP-425](plan/WP-425.md) wires pure task origin/kind lines onto daemon workflow
task create freeze.

[WP-426](plan/WP-426.md) reuses pure sandbox lines on process AgentRun submit
freeze.

[WP-427](plan/WP-427.md) wires pure session-native-state lines onto session
list/inspect human output.

[WP-428](plan/WP-428.md) wires pure native-session-transition lines onto
session reset-native success.

[WP-429](plan/WP-429.md) reuses pure error-kind lines on session control
failure paths.

[WP-430](plan/WP-430.md) wires pure task-settlement lines onto daemon workflow
lease-loss Forced Failed settle.

[WP-431](plan/WP-431.md) reuses pure task-state lines on durable task status
print.

[WP-432](plan/WP-432.md) reuses pure task origin/kind lines on durable task
status print.

[WP-433](plan/WP-433.md) reuses pure task failure-code lines on durable task
status print when a code is present.

[WP-434](plan/WP-434.md) reuses pure task state/origin/kind/failure lines on
daemon workflow status print.

[WP-435](plan/WP-435.md) reuses pure workflow-run-status lines on daemon
workflow status print when a journal is present.

[WP-436](plan/WP-436.md) reuses pure workflow-run-status lines on workflow list
print.

[WP-437](plan/WP-437.md) reuses pure workflow-run-status lines on local workflow
summary print.

[WP-438](plan/WP-438.md) reuses pure run-status lines on history record print.

[WP-439](plan/WP-439.md) reuses pure run-status lines on broadcast table print
for successful rows.

[WP-440](plan/WP-440.md) wires pure legacy detached task-state lines on task
status print.

[WP-441](plan/WP-441.md) wires pure route-tier and route-effort lines on
`vyane route` human print.

[WP-442](plan/WP-442.md) reuses pure sandbox lines on permission-check print
for the harness max sandbox.

[WP-443](plan/WP-443.md) reuses pure protocol lines on legacy session list
print.

[WP-444](plan/WP-444.md) reuses pure task state/origin/kind/failure lines when
projecting durable records into task-list rows.

[WP-445](plan/WP-445.md) reuses pure legacy task-state lines when projecting
legacy detached list rows into task-list rows.

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
