# Work-package history index

This page keeps the per-work-package narrative that used to sit in the
[roadmap](../ROADMAP.md), from WP-66 through WP-467, in its original order.
It is history, not an active plan: the roadmap's active product program and
P0-P3 table say what is current, and the roadmap no longer continues this
line item by item. Each linked `WP-NN.md` file in this directory is the plan
for that work package. WP-18, WP-19, WP-20, WP-21 and WP-36 are cited by the
roadmap milestone tables but have no separate plan file.

[WP-88](WP-88.md) extends the P1 native lane with independent
user/project/managed permission ceilings. Configuration never grants an
optional tool and every configured ceiling can only narrow the explicit
AgentRun request. [WP-90](WP-90.md) keeps writable command execution
separate and limits it to explicit descriptor-bound roots inside an otherwise
read-only workspace. [WP-94](WP-94.md) adds a redacted static summary of
the composed CLI-harness ceiling and Native/Canto capability bounds to both
`vyane check` surfaces without turning configuration into a grant.
[WP-95](WP-95.md) keeps daemon startup fail-closed before spawn while
allowing the bounded post-spawn readiness loop to retry only typed transient
control-lock contention.
[WP-96](WP-96.md) makes forced-restart acceptance fixtures wait for the
old daemon PID to disappear instead of racing the production pre-spawn
liveness and authenticated-health gate.
[WP-97](WP-97.md) gives the standard CI test job the same serialized
broker-supervisor fixture scheduling already used by coverage, without
skipping tests or reducing concurrency inside each fixture.
[WP-98](WP-98.md) reconciles the README's stale Native
graceful-shutdown acceptance statement with WP-66 and the current tested
roadmap state.
[WP-99](WP-99.md) refreshes the locked MCP SDK and macros from `rmcp`
3.0.0 to the compatible 3.0.1 patch while retaining the Rust 1.88 floor and
the existing stdio tool surface.
[WP-467](WP-467.md) refreshes that lock to `rmcp` 3.3.0 and pins the
stdio server to MCP 2026 dual-era negotiation: `initialize` stays on
2025-11-25, while `server/discover` can negotiate 2026-07-28. Streamable HTTP,
OAuth, and Tasks remain unused.
[WP-100](WP-100.md) makes daemon terminal acceptance polling reuse one
short-timeout client and retry transient loopback timeouts only inside the
existing total terminal budget.
[WP-101](WP-101.md) gives idempotent AgentRun acceptance submissions the
same bounded retry treatment when a loopback timeout leaves the response
ambiguous under suite load.
[WP-102](WP-102.md) gives the pursuit overall-timeout fixture enough room
for its real verifier while still proving the segment receives only the
remaining total budget.
[WP-103](WP-103.md) isolates the macOS broker-maintenance runtime and the
instrumented daemon-restart fixture in fresh test processes after exact
main-branch failures, without changing production lifecycle budgets.
[WP-104](WP-104.md) moves every CI and publish checkout to the immutable
official `actions/checkout` v6.0.2 commit and explicitly narrows ordinary CI
workflow token access to read-only repository contents.
[WP-105](WP-105.md) gives the service-owned no-lane resident broker
fixture a fresh process in normal and coverage gates after an exact macOS main
run hung at that runtime handoff.
[WP-106](WP-106.md) gives the resident workflow acceptance fixture a
two-thread Tokio runtime so its blocking CLI polling cannot starve the mock
target serving the detached daemon.
[WP-107](WP-107.md) gives the resident completion supervisor fixture
independent Tokio workers and a bounded final join after an exact coverage
process consumed the whole job timeout during shutdown.
[WP-108](WP-108.md) gives the mixed process/native daemon fixture
independent Tokio workers after Ubuntu delayed its two 15-second terminal
pollers for roughly 90 seconds in the full acceptance-test process.
[WP-109](WP-109.md) reserves a documented settlement margin on each
pursuit lease renewal so overall-timeout pause checkpoints can finish after
the runtime budget elapses, without extending execution timeouts.
[WP-110](WP-110.md) adds a second, settlement-only lease heartbeat
immediately before terminal pause writes so pause settlement does not depend
on residual loop-start lease alone.
[WP-111](WP-111.md) gives multi-mock native AgentRun fixtures (web-search
and failover) independent Tokio workers after Ubuntu and coverage left them
polling until the status budget expired with only the daemon listen line.
[WP-112](WP-112.md) keeps resident workflow daemon fixtures schedulable:
two-worker MCP runtime, async workflow status polling, and a fresh CI process
for the long dual-path workflow fixture after macOS/Ubuntu suite stalls.
[WP-113](WP-113.md) puts the remaining Wiremock-backed native AgentRun
acceptance fixtures on two-worker Tokio runtimes so suite handoffs cannot
starve mock targets during status polling.
[WP-114](WP-114.md) puts Wiremock-backed `native_agent` unit fixtures on
two-worker Tokio runtimes for the same multi-mock scheduling class.
[WP-115](WP-115.md) puts multi-mock CLI acceptance failover/stream
fixtures on two-worker Tokio runtimes.
[WP-116](WP-116.md) keeps the long dual-path workflow fixture
schedulable under llvm-cov after main coverage left it `running` for the full
poll budget: more workers, non-blocking CLI I/O, hold-until-release mock gate,
daemon-log settlement panics, and incremental-cache cleanup before the
isolated coverage process.
[WP-117](WP-117.md) puts authorized OpenAI Chat dual-mock redirect and
cancellation fixtures on two-worker Tokio runtimes so delayed targets and
cancel paths stay schedulable under suite load.
[WP-118](WP-118.md) isolates the process-lane AgentRun success fixture in
a fresh CI process after suite handoff left Ubuntu submission retries with only
the daemon listen line.
[WP-119](WP-119.md) isolates the concurrent process/native multi-lane
fixture the same way after main Ubuntu left its first submit with only the
daemon listen line inside the serialized suite.
[WP-120](WP-120.md) puts remaining delayed MockServer timeout unit
fixtures on two-worker Tokio runtimes while leaving lease-sensitive restart
fixtures on the current-thread runtime.
[WP-121](WP-121.md) puts authorized OpenAI Responses web-search
MockServer fixtures on two-worker Tokio runtimes so cancel/retry paths stay
schedulable under suite load.
[WP-122](WP-122.md) puts long-delay detached CLI acceptance fixtures on
two-worker Tokio runtimes so delayed targets stay schedulable while the parent
returns from `--detach` or cancels the worker group.
[WP-123](WP-123.md) isolates the native writable-command-root sandbox
fixture in a fresh CI process after coverage left its first submit with only
the daemon listen line.
[WP-124](WP-124.md) puts the remaining authorized OpenAI Chat MockServer
unit fixtures on two-worker Tokio runtimes so the full suite shares one
scheduling class.
[WP-125](WP-125.md) isolates the native command-permission sandbox
fixture in a fresh CI process after coverage left status polling with only the
daemon listen line under suite load.
[WP-126](WP-126.md) puts the remaining MockServer-backed detached CLI
acceptance fixtures on two-worker Tokio runtimes so wiremock-backed dispatch
paths stay schedulable under suite load.
[WP-127](WP-127.md) isolates the shared-resident native submit fixture in a
fresh CI process after suite handoff left AgentRun output HTTP timed out under
load.
[WP-128](WP-128.md) isolates expired-controller restart settlement and
extends exact-controller restart recovery isolation to the normal CI job after
suite load still observed non-terminal restart states while the bodies stayed
stable alone.
[WP-129](WP-129.md) isolates the plain-to-auto-pursuit daemon start fixture
in a fresh CI process after macOS suite handoff left plain daemon stop unclean.
[WP-130](WP-130.md) puts the remaining MockServer-backed CLI acceptance
fixtures on two-worker Tokio runtimes so dispatch/workflow/stream/review wire
paths stay schedulable under suite load.
[WP-131](WP-131.md) isolates graceful-stop restart settlement in a fresh CI
process, completing the long-delay restart fixture process isolations.
[WP-132](WP-132.md) widens the cooperative pursuit-cancel wall-clock check
to two seconds after macOS suite load exceeded the prior one-second bound.
[WP-133](WP-133.md) isolates the supervisor panic-backoff fixture in a
fresh CI process after macOS suite handoff left a `supervisor_cont` orphan.
[WP-134](WP-134.md) puts MockServer-backed protocol client fixtures on
two-worker Tokio runtimes so complete/stream/retry wire paths stay schedulable
under suite load.
[WP-135](WP-135.md) puts MockServer-backed daemon client unit fixtures on
two-worker Tokio runtimes so submit/control wire paths stay schedulable under
suite load.
[WP-136](WP-136.md) isolates the native write-permission tool-lane fixture
in a fresh CI process alongside the other resident sandbox isolations.
[WP-137](WP-137.md) isolates the native web-search frozen target fixture in
a fresh CI process with the other multi-thread native AgentRun isolations.
[WP-138](WP-138.md) isolates the native multi-mock failover AgentRun fixture
in a fresh CI process under suite load.
[WP-139](WP-139.md) isolates the native in-process cancel controller fixture
in a fresh CI process with the other multi-thread native AgentRun isolations.
[WP-140](WP-140.md) isolates the configured tool-ask native AgentRun fixture
in a fresh CI process with the other multi-thread native AgentRun isolations.
[WP-141](WP-141.md) isolates the read-only process-harness workdir fixture
in a fresh CI process after instrumented suite load left status polling with
only the daemon listen line.
[WP-142](WP-142.md) isolates the configured native-ceiling AgentRun fixture
in a fresh CI process with the other multi-thread native AgentRun isolations.
[WP-143](WP-143.md) isolates the exit-zero-without-terminal process fixture
in a fresh CI process with the other process-lane AgentRun isolations.
[WP-144](WP-144.md) isolates the process-group cancel settlement fixture in
a fresh CI process with the other process-lane AgentRun isolations.
[WP-145](WP-145.md) isolates the restart terminal-controller cleanup
fixture in a fresh CI process with the other process-lane AgentRun isolations.
[WP-146](WP-146.md) isolates the MCP workflow resident-daemon fixture in a
fresh CI process with the other multi-thread daemon workflow isolations.
[WP-147](WP-147.md) isolates the terminal polling budget fixture in a fresh
CI process with the other multi_thread Wiremock control-plane isolations.
[WP-148](WP-148.md) isolates the submission retry budget fixture in a fresh
CI process with the other multi_thread Wiremock control-plane isolations.
[WP-149](WP-149.md) isolates the resident native unit failover fixture in a
fresh CI process with the other multi_thread multi-mock native isolations.
[WP-150](WP-150.md) isolates the resident native chain-drift unit fixture in
a fresh CI process with the other multi_thread multi-mock native isolations.
[WP-151](WP-151.md) isolates the resident native post-tool no-failover unit
fixture in a fresh CI process, completing that multi_thread multi-mock unit
trio.
[WP-152](WP-152.md) extends the authenticated durable workflow lifecycle
view with a single bounded success `output` / `output_omitted` projection so
MCP clients can complete submit → status → result without a new tool name or a
weaker redaction boundary.
[WP-153](WP-153.md) isolates the detach fast-return success fixture in a
fresh CI process with the other multi_thread Wiremock control-plane isolations.
[WP-154](WP-154.md) isolates the detach cancel finalize fixture in a fresh
CI process with the other multi_thread detach control-plane isolations.
[WP-155](WP-155.md) isolates the detach fingerprint-cancel fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-156](WP-156.md) isolates the detached auto-route freeze fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-157](WP-157.md) isolates the detached no-frontier failover fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-158](WP-158.md) isolates the detach config-error fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-159](WP-159.md) isolates the detach bad-label fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-160](WP-160.md) isolates the detached direct-HTTP write reject fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-161](WP-161.md) isolates the legacy job-reader drift fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-162](WP-162.md) isolates the legacy job JSON empty-stdin fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-163](WP-163.md) isolates the task-list ordering fixture in a
fresh CI process with the other multi_thread detach control-plane isolations.
[WP-164](WP-164.md) raises CI test and coverage job timeout headroom from
20 to 30 minutes after sequential isolation suite growth.
[WP-165](WP-165.md) isolates the daemon-client bearer/redirect fixture in a
fresh CI process with other multi_thread CLI unit isolations.
[WP-166](WP-166.md) isolates the daemon-client oversized-response fixture in a
fresh CI process with other multi_thread CLI unit isolations.
[WP-167](WP-167.md) isolates the daemon-client 4xx submission fixture in a
fresh CI process with other multi_thread CLI unit isolations.
[WP-168](WP-168.md) isolates the daemon-client 5xx submission fixture in a
fresh CI process with other multi_thread CLI unit isolations.

[WP-169](WP-169.md) isolates the daemon-client malformed-accepted
response fixture in a fresh CI process (suite skip + `--exact`).

[WP-170](WP-170.md) isolates the daemon-client oversized-accepted
response fixture in a fresh CI process (suite skip + `--exact`).

[WP-171](WP-171.md) isolates the daemon-client accepted-another-id
response fixture in a fresh CI process (suite skip + `--exact`).

[WP-172](WP-172.md) isolates the daemon-client submission-timeout
fixture in a fresh CI process (suite skip + `--exact`).

[WP-173](WP-173.md) isolates the daemon-client workflow-status-missing
fixture in a fresh CI process (suite skip + `--exact`).

[WP-174](WP-174.md) isolates the daemon-client workflow-cancel-malformed
fixture in a fresh CI process (suite skip + `--exact`).

[WP-175](WP-175.md) isolates the daemon-client workflow-auth-drift
fixture in a fresh CI process (suite skip + `--exact`).

[WP-176](WP-176.md) isolates the daemon-workflow concurrent-creates
fixture in a fresh CI process (suite skip + `--exact`).

[WP-177](WP-177.md) prints the WP-152 bounded success `output` (or
`output omitted`) on human-mode daemon `workflow status` / submit responses.

[WP-178](WP-178.md) prints the durable `failure_code` already on the
daemon workflow task record in the same human-mode status / submit responses.

[WP-179](WP-179.md) prints each detached `TaskRow.failure_code` on human
`task list` when present (JSON already serialized the field).

[WP-180](WP-180.md) extracts a pure human durable `task status` formatter
and unit-tests `failure_code` snake_case lines on the shipped path.

[WP-181](WP-181.md) extracts a pure human `workflow list` formatter and
unit-tests status names plus step-count totals on real journal summaries.

[WP-182](WP-182.md) extracts a pure human broadcast result table formatter
and unit-tests success and error row projection.

[WP-183](WP-183.md) extracts a pure human run-record line formatter and
unit-tests status, target, duration, and optional cost projection.

[WP-184](WP-184.md) extracts a pure human session-view line formatter and
unit-tests native state / resume flags with terminal-safe fields.

[WP-185](WP-185.md) extracts a pure human legacy session line formatter and
unit-tests id / target / run_count / updated_at projection.

[WP-186](WP-186.md) extracts a pure human legacy `task status` formatter and
unit-tests displayed state vs file state plus optional error/log lines.

[WP-187](WP-187.md) extracts a pure human workflow run summary formatter and
unit-tests status, path, optional replay, and step first-line output.

[WP-188](WP-188.md) extracts a pure human goal list line formatter and
unit-tests status Display, priority, and terminal-safe id/title fields.

[WP-189](WP-189.md) extracts a pure human continuity next-action formatter
with snake_case action/command tokens (no Debug) and optional durable field
lines for the WP-79 operator projection.

[WP-190](WP-190.md) extracts a pure human `vyane route` result formatter
and unit-tests profile + RouteDecision fields (tier/effort as_str, score, safe
free text).

[WP-191](WP-191.md) extracts a pure human takeover approval line formatter
for continuity queue/decide responses (safe id/goal/step + status Display).

[WP-192](WP-192.md) extracts a pure human continuity-signal result line
formatter (safe goal id, recorded|unchanged, next_ready_step).

[WP-193](WP-193.md) extracts a pure human `vyane check` permission summary
formatter over the redacted harness + native ceiling axes (P1 permission
operator surface).

[WP-194](WP-194.md) extracts a pure human goal pursue outcome formatter
(status Display, segment counters, terminal-safe summary/reason).

[WP-195](WP-195.md) extracts a pure human goal verify result formatter
(success|inconclusive, terminal-safe goal id and acceptance summary).

[WP-196](WP-196.md) extracts a pure human goal progress event line formatter
(safe event/goal ids, kind Display, revision).

[WP-197](WP-197.md) routes human `goal get` event rows through pure
`format_goal_event_line` and drops the private CLI `event_kind_text` table in
favor of domain `GoalEventKind` Display.

[WP-198](WP-198.md) extracts the pure daemon workflow status/submit human
formatter into `output.rs` with the existing WP-152/WP-178 unit coverage.

[WP-199](WP-199.md) extracts pure human local A2A send/inbox/read formatters
with terminal-safe ids, codes, kind, delivery status, and body.

[WP-200](WP-200.md) extracts pure human cancel final-state lines for
detached tasks and daemon workflow cancel.

[WP-201](WP-201.md) extracts pure human `vyane check` section formatters
for config files, providers, profiles, harnesses, and profile environment rows.

[WP-202](WP-202.md) extracts pure human cancel idempotency lines
(`{id} already {state}`) for legacy and durable terminal cancel paths.

[WP-203](WP-203.md) extracts pure human daemon lifecycle lines (already
running / started / running status / stopped).

[WP-204](WP-204.md) extracts pure session-control error classification plus
empty task-list and detached run-id human lines.

[WP-205](WP-205.md) extracts pure native ask-surface model outputs
(`approval_required_output` / `missing_approval_plan_output`) with safe plan
hash prefixes.

[WP-206](WP-206.md) extracts pure deny / unknown / invalid native tool
model outputs used on the prepare path.

[WP-207](WP-207.md) extracts pure cancel / timeout / panic / unavailable /
failed native tool execution outputs on ordinary and authorized paths.

[WP-208](WP-208.md) extracts pure workflow error human prefix lines
(`config error:` vs `error:`).

[WP-209](WP-209.md) extracts pure detached-task diagnostic lines (no such
run / no output / not local dispatch) with terminal-safe ids.

[WP-210](WP-210.md) extracts pure `vyane serve` loopback/start lines and a
shared config-error prefix helper.

[WP-211](WP-211.md) routes remaining CLI `config error:` stderr through
`format_config_error_line` (no residual ad-hoc literals in command.rs).

[WP-212](WP-212.md) extracts pure generic `error:` and `worker error:`
CLI stderr prefix helpers for command/main paths.

[WP-213](WP-213.md) composes workflow error human lines from the shared
config-error and error prefix helpers.

[WP-214](WP-214.md) composes session-control error human lines from the
shared `format_error_line` helper.

[WP-215](WP-215.md) extracts pure daemon listen/not-running banners and
goal CLI error prefix lines.

[WP-216](WP-216.md) extracts pure A2A error / inbox-more and cancel
diagnostic lines (kill unfinalized, not-local frontend cancel).

[WP-217](WP-217.md) extracts pure cancel ownership/diagnostic lines and
workflow observer step event lines.

[WP-218](WP-218.md) extracts pure legacy cancel identity / nested-harness
cleanup diagnostics (pid-reuse refuse, incomplete process groups, and related
operator lines).

[WP-219](WP-219.md) extracts pure durable cancel refuse-control and terminal
cleanup-failed diagnostics (process identity unavailable; reuse cleanup helper).

[WP-220](WP-220.md) extracts pure worker/stale-detached diagnostics (stale
status, metadata settlement, output write, nested harness controller
write/cleanup).

[WP-221](WP-221.md) extracts pure dispatch stream notices (unsupported /
not-applicable fallback) and tool-use progress lines with terminal-safe free
text.

[WP-222](WP-222.md) extracts pure REST task-supervisor lifecycle operator
logs (init cleanup, duplicate runtime, dispatch fail/panic, output artifact).

[WP-223](WP-223.md) extracts pure REST metadata settlement, goal-read,
task output read, and serve listening operator logs.

[WP-224](WP-224.md) extracts pure REST handler label/stream/broadcast and
ledger/session query operator logs (remaining serve stderr ad-hoc paths).

[WP-225](WP-225.md) extracts pure `vyane check` section header lines
(providers / profiles / harnesses / profile environment).

[WP-226](WP-226.md) extracts pure check profile resolve-warning body and
dispatch run-failure stderr (no `error:` prefix contract frozen).

[WP-227](WP-227.md) consolidates native turn-driver invalid-JSON tool
arguments into the pure tool ERROR helper surface (WP-205–207).

[WP-228](WP-228.md) publishes domain `as_str`/`Display` for continuity
signal kinds, criterion status, and pursuit checkpoint/segment status; CLI
signal evidence uses domain tokens.

[WP-229](WP-229.md) publishes domain `as_str`/`Display` for continuity
mode/status/step status and takeover decision tokens.

[WP-230](WP-230.md) makes `GoalStatus::as_str` public (Display already
existed), aligning the goal status token API with sibling domain enums.

[WP-231](WP-231.md) adds `Display` for `TakeoverRunStatus` (via existing
`as_str`), finishing the takeover status token surface.

[WP-232](WP-232.md) publishes domain `as_str`/`Display` for
`WorkflowRunStatus` and `JournalStepStatus`; CLI `workflow_status_name` uses
domain tokens only.

[WP-233](WP-233.md) routes workflow summary step status through domain
`JournalStepStatus::as_str` (no Debug + to_lowercase).

[WP-234](WP-234.md) adds `Display` for `SessionNativeState` (via existing
`as_str`), matching the domain token train for session human lines.

[WP-235](WP-235.md) publishes domain `as_str`/`Display` for
`NativePermissionAxisStatus`; CLI permission-check axis labels use domain
tokens only.

[WP-236](WP-236.md) publishes domain `as_str`/`Display` for `RunStatus`
(snake_case) and `Sandbox` (kebab-case); CLI `status_name` / `sandbox_name`
use domain tokens only.

[WP-237](WP-237.md) routes CLI `SandboxArg` Display through domain
`Sandbox::as_str` (no private kebab-case match table).

[WP-238](WP-238.md) adds CLI `RunStatusArg` Display through domain
`RunStatus::as_str`.

[WP-239](WP-239.md) adds CLI `GoalStatusArg` Display through domain
`GoalStatus::as_str` (including `in_progress`).

[WP-240](WP-240.md) adds CLI takeover-decision and continuity-signal arg
Display through domain tokens; decide/signal command paths use `From`.

[WP-241](WP-241.md) publishes `as_str` on all `vyane-task` string enums
via the shared macro (`TaskState`, origin, kind, failure code, event kind).

[WP-242](WP-242.md) adds `Display` for `Effort` (via existing `as_str`),
matching the domain token train for routing human lines.

[WP-243](WP-243.md) adds `Display` for `RouteTier` and `RouteEffort`
(via existing `as_str`).

[WP-244](WP-244.md) adds `Display` for router `IntentCategory` (via
existing `as_str`, including multi-word `code_gen`).

[WP-245](WP-245.md) publishes domain `as_str`/`Display` for configuration
diagnostic enums (check/credential/profile status and issue codes).

[WP-246](WP-246.md) publishes domain `as_str`/`Display` for kernel
`ErrorKind` (including multi-word tokens and `other` for unknown serde kinds).

[WP-247](WP-247.md) routes session-control error `kind_code` through
`ErrorKind::as_str` where the operator contract already matches domain tokens.

[WP-248](WP-248.md) publishes domain `as_str`/`Display` for
`RouteSelectionBasis` on route-preview diagnostics.

[WP-249](WP-249.md) publishes domain `as_str`/`Display` for goal
observation closed enums (`GoalObservationSignalKind`,
`GoalObservationStatus`, `GoalObservationWatcherErrorCode`,
`GoalObservationWatchStatus`).

[WP-250](WP-250.md) publishes domain `as_str`/`Display` for quota closed
enums (`QuotaStatus`, `QuotaUnit`, `QuotaConnectorErrorCode`,
`QuotaSnapshotStatus`).

[WP-251](WP-251.md) publishes domain `as_str`/`Display` for goal
next-action view enums (`GoalNextActionKind`, `GoalOperatorCommand`,
`GoalSignalKind`, `GoalNextReasonCode`), single-sourcing action/command/signal
tokens from the core projection enums.

[WP-252](WP-252.md) publishes domain `as_str`/`Display` for
`GoalContinuityRunStatus` on the bounded continuity runner redacted result.

[WP-253](WP-253.md) publishes public `as_str` for `vyane-message`
`string_enum!` tokens (`MessageDirection`, `EndpointKind`, `DeliveryStatus`,
`MessageEventKind`, `MessagePublicationStatus`), matching WP-241 for tasks.

[WP-254](WP-254.md) publishes public `as_str` for `vyane-agent`
`string_enum!` tokens (`RunState`, `ExecutionBackend`, `AgentEventKind`, and
siblings), completing the store string-enum public token train with WP-241/253.

[WP-255](WP-255.md) publishes domain `as_str`/`Display` for kernel
`AuthStyle` and `InheritMode` (`bearer` / `x_api_key`, `scrub` / `full`).

[WP-256](WP-256.md) publishes domain `as_str`/`Display` for
`EnvInjectSource` and ledger event enums (`EventCategory`, `EventSource`,
`EventDurability`).

[WP-257](WP-257.md) publishes domain `as_str`/`Display` for kernel
capability enums (`FilesystemCapability`, `IsolationStrength`,
`CapabilityRejectionReason`).

[WP-258](WP-258.md) publishes domain `as_str`/`Display` for MCP workflow
projection enums (`WorkflowState`, `WorkflowFailureCode`).

[WP-259](WP-259.md) publishes domain `as_str`/`Display` for config native
policy enums (`NativeToolPermissionEffectConfig`,
`NativeCommandNetworkRouteConfig`).

[WP-260](WP-260.md) publishes public `as_str` for harness
`PermissionEffect` and domain tokens for `NativeCommandNetworkRoute`, aligning
runtime vocabulary with WP-259 config enums.

[WP-261](WP-261.md) publishes domain `as_str`/`Display` for native
`ToolInvocationStatus`, including the permission-ask terminal
`approval_required`.

[WP-262](WP-262.md) publishes domain `as_str`/`Display` for
`GoalContinuityPortResult` and completes `Display` for
`WebSearchContextSize`.

[WP-263](WP-263.md) publishes domain kind tokens for `NativeTurnStop`
(`approval_required`, `budget_exhausted`, …) without leaking reply or
approval payloads.

[WP-264](WP-264.md) wires native status tokens on shipped paths:
`ToolInvocationStatus::is_executed` drives turn-driver `is_error`, and native
agent settlement maps `NativeTurnStop::as_str` kinds onto durable failure codes.

[WP-265](WP-265.md) locks config↔harness native permission/network token
parity on the service adapter path that composes permission ceilings.

[WP-266](WP-266.md) adds pure human `format_tool_invocation_status_line`
over `ToolInvocationStatus` tokens (`approval_required`, `timed_out`, …).

[WP-267](WP-267.md) wires that pure status line onto the CLI stream
`ToolUse` path (progress uses `executed`).

[WP-268](WP-268.md) adds pure human `format_native_turn_stop_line` over
`NativeTurnStop` kind tokens and keeps it reachable from native settlement.

[WP-269](WP-269.md) emits that pure stop line on the native settlement
failure path via `tracing::info!` (`stop_kind` / `failure_code` tokens only).

[WP-270](WP-270.md) wires `PursuitStatus` / `GoalStatus` `as_str` tokens
onto the resident goal pursuit settlement diagnostic (replacing Debug `?`).

[WP-271](WP-271.md) publishes `LifecycleObservation` kind tokens and wires
them onto Process AgentRun dispatch diagnostics (replacing Debug `?state`).

[WP-272](WP-272.md) publishes completion-stage and controller-cleanup kind
tokens and replaces Debug `?staged` / `?report` daemon warn dumps with
`stage_error` / `unresolved` counts.

[WP-273](WP-273.md) publishes `AgentExecutionItemStatus` /
`AgentExecutionError` kind tokens and logs the first degraded execution item
kind on supervisor backoff.

[WP-274](WP-274.md) publishes recovery/completion projection status tokens
and logs first degraded kinds on those supervisor backoff paths.

[WP-275](WP-275.md) publishes recovery/completion/supervisor error kind
tokens and logs them on failed AgentRun supervisor cycles (replacing discarded
`Err(_)`).

[WP-276](WP-276.md) logs execution/recovery driver construction failures
with the same `as_str` error kind tokens (closing the WP-275 residual).

[WP-277](WP-277.md) publishes goal-read / continuity-runner error kind
tokens and pure `format_goal_read_error_kind_line`, wired onto REST
continuity-next diagnostics.

[WP-278](WP-278.md) adds pure continuity runner/authority error kind lines
and logs underlying `GoalReadError` kinds when the continuity runner cannot
open its goal reader.

[WP-279](WP-279.md) publishes `GoalObservationIngressError` /
`GoalObservationRunnerError` kind tokens for the observation mutation seam.

[WP-280](WP-280.md) adds pure observation ingress/runner error kind lines
and logs ingress open failures with those tokens.

[WP-281](WP-281.md) logs body-free `reason = "panic"` on AgentRun
supervisor catch_unwind arms that increment `panicked_cycles`.

[WP-282](WP-282.md) publishes `NativePermissionSetError` kind tokens for
native permission ceiling validation failures.

[WP-283](WP-283.md) adds pure `format_native_permission_set_error_kind_line`
and logs that kind when daemon native permission ceiling application fails.

[WP-284](WP-284.md) publishes in-process authority/completion/native-bind
error kind tokens for the P1 native execution authority seam.

[WP-285](WP-285.md) publishes `InProcessAssemblyError` kind tokens for
assembly registry construction failures.

[WP-286](WP-286.md) publishes `OwnerContextError` kind tokens for
principal-to-owner binding failures.

[WP-287](WP-287.md) adds pure `format_owner_context_error_kind_line` and
logs authentication failure kinds on `OwnerContextFactory::authenticate`.

[WP-288](WP-288.md) logs remaining owner-context construction kinds
(`invalid_principal`, `resolution_failed`, `invalid_owner`, `reserved_owner`).

[WP-289](WP-289.md) publishes `AgentMessageCompletionReadError` and
`DiagnosticErrorKind` tokens.

[WP-290](WP-290.md) publishes kind-only tokens for
`PermissionRuleError` / `ToolRegistryError` on the native ask registration
surface.

[WP-291](WP-291.md) publishes kind-only tokens for `ToolContextError`,
`NativeCommandPolicyError`, and `NativeReadPolicyError`.

[WP-292](WP-292.md) publishes kind tokens for write/filesystem/network/
web-fetch/web-search native policy errors.

[WP-293](WP-293.md) publishes kind-only tokens for
`ToolChatValidationError` on the bounded typed tool-chat boundary.

[WP-294](WP-294.md) publishes kind-only tokens for `EditError` on the
bounded trusted text-edit tool surface.

[WP-295](WP-295.md) adds pure `format_tool_chat_validation_error_kind_line`
and logs validation kinds on the native turn driver request/response boundary.

[WP-296](WP-296.md) logs preflight next-request validation kinds before
tool side effects (closing the WP-295 residual).

[WP-297](WP-297.md) publishes kind tokens for `NativeTurnLimitError`,
host Unsupported, and `RegisterCommandToolError`.

[WP-298](WP-298.md) publishes kind-only tokens for `AgentStoreError` on
the AgentRun durable store surface.

[WP-299](WP-299.md) adds pure `format_agent_store_error_kind_line` and
logs store kinds when native authority maps store failures.

[WP-300](WP-300.md) publishes kind-only tokens for web-fetch/web-search
tool registration errors.

[WP-301](WP-301.md) publishes kind-only tokens for `GoalStoreError` on the
owner-scoped goal durable store surface.

[WP-302](WP-302.md) adds pure `format_goal_store_error_kind_line` and logs
goal-store kinds on the resident goal daemon paths.

[WP-303](WP-303.md) publishes kind-only tokens for `MessageStoreError` on
the owner-scoped message durable store surface.

[WP-304](WP-304.md) publishes kind-only tokens for `TaskStoreError` on the
owner-scoped task durable store surface.

[WP-305](WP-305.md) publishes kind-only tokens for quota validation, runner,
and transport error surfaces.

[WP-306](WP-306.md) publishes kind-only tokens for `BrokerError` on the
local message/agent projection broker surface.

[WP-307](WP-307.md) publishes kind-only tokens for `EventLogError` on the
durable event-log surface.

[WP-308](WP-308.md) publishes kind-only tokens for `WorkflowError` on the
workflow engine / journal surface.

[WP-309](WP-309.md) adds pure `format_workflow_error_kind_line` and logs
workflow kinds on the resident workflow daemon failure paths.

[WP-310](WP-310.md) publishes kind-only tokens for `WorkflowControlError`
on the MCP/daemon workflow control adapter surface.

[WP-311](WP-311.md) adds pure task/message store error lines and logs
task-store kinds on workflow lease renewal and completion retry.

[WP-312](WP-312.md) adds pure event-log and workflow-control error kind
lines for operator diagnostics.

[WP-313](WP-313.md) adds pure register web-fetch/web-search tool error kind
lines for operator diagnostics.

[WP-314](WP-314.md) logs kind-only tokens when the resident goal supervisor
fails or shutdown handlers cannot be installed.

[WP-315](WP-315.md) logs kind-only tokens on workflow controller open,
cleanup, join, and invalid-task recovery paths.

[WP-316](WP-316.md) logs a fixed kind when harness lifecycle stop reporting
fails (closing the last workspace `error = %error` structured-log site).

[WP-317](WP-317.md) publishes lowercase kind tokens for workflow step
`OnError` policy (`abort` / `continue`).

[WP-318](WP-318.md) logs `ErrorKind` tokens on kernel post-run ledger
append and session update best-effort warnings.

[WP-319](WP-319.md) publishes kind-only tokens for daemon workflow submit
and control client errors.

[WP-320](WP-320.md) adds pure workflow submit and control-client error kind
lines for operator diagnostics.

[WP-321](WP-321.md) adds pure `on_error` policy lines for workflow step
failure policy tokens.

[WP-322](WP-322.md) adds pure `ErrorKind` lines for the kernel failover
taxonomy tokens.

[WP-323](WP-323.md) adds pure `BrokerError` lines after wiring a direct
CLI dependency on `vyane-broker`.

[WP-324](WP-324.md) adds pure quota validation/runner/transport error
lines after wiring a direct CLI dependency on `vyane-quota`.

[WP-325](WP-325.md) publishes kind-only tokens for broker `PumpItemStatus`
and logs degraded pump items with those kinds.

[WP-326](WP-326.md) publishes kind-only tokens for `NativeSideEffect` and
`InProcessAgentEffect` on the native revalidation surface.

[WP-327](WP-327.md) adds pure native-side-effect and in-process-effect
lines for operator diagnostics.

[WP-328](WP-328.md) publishes kind-only tokens for `GoalObservationKind`
and `NativeSessionState`.

[WP-329](WP-329.md) adds pure goal-observation-kind and native-session-state
lines for operator diagnostics.

[WP-330](WP-330.md) publishes kind-only tokens for message `NackDisposition`
and a pure nack diagnostic line.

[WP-331](WP-331.md) logs kind-only nack disposition and pump status on
broker nack settlement.

[WP-332](WP-332.md) logs workflow submit and control-client kinds when
mapping MCP control errors.

[WP-333](WP-333.md) logs kind-only native side-effect rejections on
model/tool authority revalidation.

[WP-334](WP-334.md) logs kind-only observation kind and status on goal
observation ingest.

[WP-335](WP-335.md) logs kind-only workflow-control errors when MCP maps
them to public tool errors.

[WP-336](WP-336.md) publishes kind-only tokens for agent `CancelOutcome`
and a pure CLI cancel-outcome line.

[WP-337](WP-337.md) logs kind-only cancel outcomes after durable agent
cancel settlement.

[WP-338](WP-338.md) publishes kind-only tokens for broker
`AdapterOutcome` / `AdapterFailure` and pure CLI adapter lines.

[WP-339](WP-339.md) logs kind-only adapter outcomes and failures on the
broker pump delivery path.

[WP-340](WP-340.md) publishes kind-only tokens for dispatch
`AttemptOutcome` and a pure CLI attempt-outcome line.

[WP-341](WP-341.md) publishes kind-only tokens for
`ControllerRecoveryObservation` and a pure CLI recovery-observation line.

[WP-342](WP-342.md) logs kind-only controller recovery observations on the
daemon agent cancel path.

[WP-343](WP-343.md) publishes kind-only tokens for `AgentExecutorOutcome`
and `ReplaySafety`, plus pure CLI lines.

[WP-344](WP-344.md) publishes kind-only tokens for agent `RunSettlement`
and task `TaskSettlement`, plus pure CLI settlement lines.

[WP-345](WP-345.md) publishes kind-only tokens for
`AgentExecutionSettlement` and a pure CLI execution-settlement line.

[WP-346](WP-346.md) publishes kind-only tokens for
`NativeSessionTransition` and a pure CLI session-transition line.

[WP-347](WP-347.md) publishes kind-only tokens for
`AgentCompletionSinkObservation` / `AgentCompletionSinkTransition` and pure CLI
completion-sink lines.

[WP-348](WP-348.md) logs kind-only completion-sink observations on the
agent recovery path.

[WP-349](WP-349.md) publishes kind-only tokens for public
`RunAttemptOutcomeView` and a pure CLI run-attempt line.

[WP-350](WP-350.md) logs kind-only agent recovery item statuses after each
recovered ticket, plus a pure CLI recovery-item line.

[WP-351](WP-351.md) logs kind-only agent execution item statuses after each
executed claim, plus a pure CLI execution-item line.

[WP-352](WP-352.md) logs kind-only agent completion projection item
statuses after each projected event, plus a pure CLI projection-status line.

[WP-353](WP-353.md) adds pure CLI kind-only lines for lifecycle
`RunState` / `TaskState` / `GoalStatus` / `WorkflowRunStatus`.

[WP-354](WP-354.md) adds pure CLI kind-only lines for
`DeliveryStatus` / `MessagePublicationStatus` / `RunCompletionStatus`.

[WP-355](WP-355.md) adds pure CLI kind-only lines for goal
`PursuitStatus` / `CriterionStatus` / `GoalContinuityStatus` /
`TakeoverApprovalStatus`.

[WP-356](WP-356.md) adds pure CLI kind-only lines for
`JournalStepStatus` / `WorkflowState` / `GoalContinuityStepStatus`.

[WP-357](WP-357.md) adds pure CLI kind-only lines for
`MessageEventKind` / `RunStatus` / `GoalContinuityMode`.

[WP-358](WP-358.md) adds pure CLI kind-only lines for
`TakeoverDecision` / `TakeoverRunStatus` / `PursuitCheckpointStatus` /
`PursuitSegmentStatus`.

[WP-359](WP-359.md) adds pure CLI kind-only lines for goal observation
signal/status/watch kinds.

[WP-360](WP-360.md) adds pure CLI kind-only lines for diagnostics
config/credential/profile/permission-axis statuses.

[WP-361](WP-361.md) wires pure cancel-outcome and controller-recovery
observation lines onto daemon agent cancel logs.

[WP-362](WP-362.md) wires pure pursuit and goal status lines onto resident
goal pursuit settlement logs.

[WP-363](WP-363.md) wires pure goal/task store and workflow error lines onto
daemon and MCP mapping logs.

[WP-364](WP-364.md) clears leftover `dead_code` allows on pure store/workflow
error lines wired by WP-363.

[WP-365](WP-365.md) wires pure register web_fetch/web_search error lines onto
native AgentRun tool registration failures.

[WP-366](WP-366.md) adds pure register run_command error lines and wires them
onto native AgentRun command-tool registration failures.

[WP-367](WP-367.md) adds pure permission-rule error lines and wires them onto
native AgentRun permission-policy assembly failures.

[WP-368](WP-368.md) adds pure native filesystem policy error lines and wires
them onto native AgentRun workspace registry assembly failures.

[WP-369](WP-369.md) wires pure goal-store error lines onto soft-handled
resident goal quarantine pause failures.

[WP-370](WP-370.md) wires pure goal-store error lines onto resident pursuit
config/construct/stop error logs.

[WP-371](WP-371.md) adds pure completion-stage error lines and wires them
onto Process AgentRun completion staging failures.

[WP-372](WP-372.md) adds pure lifecycle observation lines and wires them
onto Process AgentRun dispatch/stop-proof diagnostics.

[WP-373](WP-373.md) wires pure run-status lines onto Process AgentRun
non-quiesced terminal status diagnostics.

[WP-374](WP-374.md) wires pure executor/settlement lines onto Process
AgentRun final settle paths.

[WP-375](WP-375.md) wires pure executor/settlement lines onto native
AgentRun settle paths (failure helper + completion stage).

[WP-376](WP-376.md) wires pure agent-store error lines onto daemon AgentRun
create failures that cannot exact-retry.

[WP-377](WP-377.md) wires pure agent-store error lines onto Process AgentRun
create failures that cannot exact-retry.

[WP-378](WP-378.md) wires pure ErrorKind lines onto native AgentRun turn
failure and outer timeout mapping.

[WP-379](WP-379.md) adds pure RunFailureCode lines and wires them onto native
and Process AgentRun quiesced failure settlements.

[WP-380](WP-380.md) wires pure goal-store error lines onto resident goal
supervisor task failure.

[WP-381](WP-381.md) wires pure MCP workflow-control error lines onto submit
and control error mapping.

[WP-382](WP-382.md) wires pure workflow/task-store error lines onto daemon
workflow API failure mapping.

[WP-383](WP-383.md) wires pure workflow-run status and task settlement lines
onto daemon workflow worker finish.

[WP-384](WP-384.md) wires pure task settlement lines onto workflow finish
error and non-shutdown join settles.

[WP-385](WP-385.md) wires pure run-state lines onto daemon AgentRun cancel
observation of terminal and cancelling states.

[WP-386](WP-386.md) wires pure task-state lines onto daemon workflow cancel
observation of terminal and cancelling states.

[WP-387](WP-387.md) wires pure MCP workflow-state lines onto successful
workflow cancel lifecycle observation.

[WP-388](WP-388.md) wires pure run-completion status lines onto terminal
AgentRun view assembly when a completion record is present.

[WP-389](WP-389.md) wires pure journal-step status lines onto daemon workflow
worker finish last-step observation.

[WP-390](WP-390.md) wires pure attempt-outcome lines onto Process AgentRun
quiesced Error settlement when a last attempt is present.

[WP-391](WP-391.md) wires pure delivery-status lines onto local A2A message
projection.

[WP-392](WP-392.md) wires pure message-store error lines onto local A2A store
open failures.

[WP-393](WP-393.md) wires pure takeover decision and approval status lines
onto goal continuity decide.

[WP-394](WP-394.md) wires pure criterion status lines onto goal satisfy and
verify paths.

[WP-395](WP-395.md) wires pure takeover-run status lines onto goal continuity
execute finish.

[WP-396](WP-396.md) wires pure goal-status lines onto CLI goal human
projection.

[WP-397](WP-397.md) wires pure takeover-approval status lines onto shared
takeover result projection and uses `as_str` in the human tab column.

[WP-398](WP-398.md) wires pure pursuit-checkpoint status lines onto goal
CLI get when a durable checkpoint is present.

[WP-399](WP-399.md) wires pure pursuit-status lines onto CLI goal pursue
settle (daemon already had them via WP-362).

[WP-400](WP-400.md) makes goal human tab/status columns use explicit
`as_str` kind tokens instead of Display formatting.

[WP-401](WP-401.md) wires pure continuity status/mode lines onto CLI
continuity-next and uses `as_str` for next-action/command human columns.

[WP-402](WP-402.md) wires pure config-check, credential, profile, issue,
and native-permission-axis lines onto `vyane check`.

[WP-403](WP-403.md) wires pure continuity step-status lines onto CLI
continuity-next for next-ready and projected plan steps.

[WP-404](WP-404.md) wires pure pursuit-segment status lines onto the shared
DispatchGoalRuntime segment settle used by CLI pursue and daemon pursuit.

[WP-405](WP-405.md) wires pure OnError policy lines onto review workflow
build for each frozen step policy.

[WP-406](WP-406.md) wires pure continuity next-action kind lines onto CLI
continuity-next projection.

[WP-407](WP-407.md) wires pure goal-event kind lines onto CLI goal get
event rows and progress settlement.

[WP-408](WP-408.md) wires pure continuity signal-kind lines onto CLI
continuity-signal settle.

[WP-409](WP-409.md) wires pure continuity operator-command lines onto CLI
continuity-next when a command is projected.

[WP-410](WP-410.md) wires pure takeover-sandbox lines onto CLI continuity
queue after the approval freeze.

[WP-411](WP-411.md) wires pure core sandbox lines onto CLI goal pursue
runtime freeze.

[WP-412](WP-412.md) wires pure final goal-status lines onto CLI pursue
settle from the durable outcome field.

[WP-413](WP-413.md) adds `Protocol::as_str` and wires pure protocol lines
onto `vyane check` per-provider rows.

[WP-414](WP-414.md) wires pure harness-kind lines onto `vyane check`
harness availability rows.

[WP-415](WP-415.md) wires pure permission-effect lines onto native
ApprovalRequired settle (ask surface).

[WP-416](WP-416.md) wires pure auth-style lines onto native target freeze
when endpoint auth is present.

[WP-417](WP-417.md) wires pure protocol lines onto native target freeze
(reusing WP-413 tokens).

[WP-418](WP-418.md) wires pure web-search context-size lines onto native
web-search tool registration.

[WP-419](WP-419.md) adds `AdapterTransport::as_str` and wires pure
transport lines onto native target freeze.

[WP-420](WP-420.md) wires pure effort lines onto native genparams freeze
when effort is set.

[WP-421](WP-421.md) wires pure task failure-code lines onto daemon
workflow Failed settle and interrupt paths.

[WP-422](WP-422.md) wires pure execution-backend lines onto daemon
AgentRun submit freeze (process + native).

[WP-423](WP-423.md) wires pure run-mode lines onto the same daemon
AgentRun submit freeze sites.

[WP-424](WP-424.md) wires pure controller-kind lines onto daemon AgentRun
cancel dispatch (Process + InProcess).

[WP-425](WP-425.md) wires pure task origin/kind lines onto daemon workflow
task create freeze.

[WP-426](WP-426.md) reuses pure sandbox lines on process AgentRun submit
freeze.

[WP-427](WP-427.md) wires pure session-native-state lines onto session
list/inspect human output.

[WP-428](WP-428.md) wires pure native-session-transition lines onto
session reset-native success.

[WP-429](WP-429.md) reuses pure error-kind lines on session control
failure paths.

[WP-430](WP-430.md) wires pure task-settlement lines onto daemon workflow
lease-loss Forced Failed settle.

[WP-431](WP-431.md) reuses pure task-state lines on durable task status
print.

[WP-432](WP-432.md) reuses pure task origin/kind lines on durable task
status print.

[WP-433](WP-433.md) reuses pure task failure-code lines on durable task
status print when a code is present.

[WP-434](WP-434.md) reuses pure task state/origin/kind/failure lines on
daemon workflow status print.

[WP-435](WP-435.md) reuses pure workflow-run-status lines on daemon
workflow status print when a journal is present.

[WP-436](WP-436.md) reuses pure workflow-run-status lines on workflow list
print.

[WP-437](WP-437.md) reuses pure workflow-run-status lines on local workflow
summary print.

[WP-438](WP-438.md) reuses pure run-status lines on history record print.

[WP-439](WP-439.md) reuses pure run-status lines on broadcast table print
for successful rows.

[WP-440](WP-440.md) wires pure legacy detached task-state lines on task
status print.

[WP-441](WP-441.md) wires pure route-tier and route-effort lines on
`vyane route` human print.

[WP-442](WP-442.md) reuses pure sandbox lines on permission-check print
for the harness max sandbox.

[WP-443](WP-443.md) reuses pure protocol lines on legacy session list
print.

[WP-444](WP-444.md) reuses pure task state/origin/kind/failure lines when
projecting durable records into task-list rows.

[WP-445](WP-445.md) reuses pure legacy task-state lines when projecting
legacy detached list rows into task-list rows.

[WP-446](WP-446.md) reuses pure continuity signal-kind lines on CLI
continuity-next for each projected accepted signal.

[WP-447](WP-447.md) adds pure allowlisted goal-signal lines on REST
continuity-next success for each projected accepted signal.

[WP-448](WP-448.md) adds pure allowlisted next-action lines on REST
continuity-next success for the projected action kind.

[WP-449](WP-449.md) adds pure allowlisted operator-command lines on REST
continuity-next success when a command is projected.

[WP-450](WP-450.md) adds pure allowlisted next-reason-code lines on REST
continuity-next success for the projected reason taxonomy.

[WP-451](WP-451.md) reuses pure continuity signal-kind lines on CLI
continuity-next for each durable ready signal on the continuity state.

[WP-452](WP-452.md) reuses pure continuity step-status lines on CLI
continuity-queue for the selected ready approval-required step.

[WP-453](WP-453.md) reuses pure takeover run-status lines on CLI
continuity-queue when upstream predecessor evidence is present.

[WP-454](WP-454.md) reuses pure continuity signal-kind lines on CLI
continuity-execute when ready signals are assembled into prompt evidence.

[WP-455](WP-455.md) reuses pure journal step-status lines on local workflow
summary print for each journal step.

[WP-456](WP-456.md) reuses pure takeover run-status lines on CLI
continuity-execute when approval evidence is assembled into the prompt.

[WP-457](WP-457.md) reuses pure takeover approval-status lines on CLI
continuity-execute admission before the approved check.

[WP-458](WP-458.md) reuses pure takeover approval-status lines on CLI
continuity-execute after consume (InFlight) and finish settle (Done/Blocked).

[WP-459](WP-459.md) reuses pure takeover-sandbox lines on CLI
continuity-execute for the frozen sandbox before dispatch.

[WP-460](WP-460.md) reuses pure tool-invocation-status lines on native
AgentRun ApprovalRequired settle (`approval_required` ask surface).

[WP-461](WP-461.md) pure-logs native turn-stop / error-kind tokens on the
cancel Unknown settlement path (no false failure code).

[WP-462](WP-462.md) reuses pure tool-invocation-status lines on native
AgentRun tool-lane TimedOut settle.

[WP-463](WP-463.md) reuses pure tool-invocation-status lines on native
AgentRun cancel Unknown settle (`cancelled` tool status).

[WP-464](WP-464.md) pure-logs nested ErrorKind tokens on native
AgentRun AbortedAfterToolActivity settle.

[WP-465](WP-465.md) pure-logs ErrorKind tokens on native AgentRun
failover-eligible intermediate continue (before next bound).

[WP-466](WP-466.md) gives the uncertain native AgentRun recovery fixture
its own CI process after exact-main macOS exposed a Tokio runtime handoff stall.

WP-72 composes the P2 goal foundation into the resident daemon behind explicit
opt-in target/workdir/sandbox authority. One local goal is pursued at a time;
running checkpoints survive daemon replacement, while semantic pauses and live
foreign leases remain untouched. WP-73 adds only typed continuity policy and
visible idempotent quota-handoff state; it does not authorize a takeover
runtime. WP-80 adds only an opt-in owner-frozen continuity-next REST read over an
atomic redacted snapshot. WP-81 adds the typed observation ingress; WP-82 adds
only the purpose-authenticated bounded runner seam. General goal APIs, concrete
queue/execution ports and periodic assembly remain open.
