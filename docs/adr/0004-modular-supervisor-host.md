# ADR 0004: compose narrow supervisors instead of recreating a god daemon

- Status: accepted
- Date: 2026-07-10
- Parity rows: INT-04, CON-06, COL-03, COL-05, GOV-01

## Context

The fixed original daemon owns worker/session registries, scheduling, events,
messages, channels, goals, memory and platform integrations in one broad
process. The Rust daemon currently owns only admitted workflow execution and has
strong controller leases and fail-closed restart handling. The names match, but
the products do not.

Expanding the current daemon by directly copying every original subsystem would
recreate coupled lifecycle code and multiple sources of truth. Keeping only the
workflow daemon forever would leave collaboration and worker-health parity
unaddressed.

## Decision

1. Rust keeps narrow domain stores and supervisors: workflow, AgentRun/worker,
   message delivery, event projection and optional adapters each own one
   lifecycle and cancellation boundary.
2. A future resident host composes selected supervisors behind one authenticated
   local control plane. It coordinates admission and graceful drain but does not
   merge their records into a universal task row.
3. `vyane-task` remains execution-control metadata; message bodies stay in
   `vyane-message`; worker topology and active execution live in `vyane-agent`;
   external channels remain optional adapters.
4. Restart recovery is component-specific and fenced by exact revision,
   generation, lease token and controller identity. Age alone never proves that
   a worker is dead, and a session id alone never authorizes automatic resume.
5. The host must be modular in code and configuration. A disabled or failed
   optional adapter cannot prevent unrelated core supervisors from starting.

`vyane-broker::ResidentBrokerSupervisor` is the first concrete narrow resident
driver under this decision. It is an explicit non-`Clone` library value whose
consuming future concurrently owns disjoint owner/store-bound delivery lanes,
message maintenance, message projection and AgentEvent projection. It validates
batch and aggregate-concurrency bounds and isolates each loop's failure with
capped exponential backoff. It does not spawn tasks, create a channel or
runtime, or keep a second queue.

This delivery does not instantiate the future resident host. No service, CLI or
daemon production path constructs the driver, and it is not the AgentRun
execution/recovery supervisor, controller/message glue, A2A/Channels surface,
live pause/resume or automatic replay described by the broader decision.

`vyane-service::AgentRunRecoveryDriver` is a second narrow control seam under
this decision, but deliberately not a resident supervisor. It is a fixed-owner,
non-`Clone` one-shot value whose consuming call performs one bounded stale-
controller recovery pass. It uses the durable AgentRun store's two-stage claim
and confirm protocol without exposing the raw store or ticket. Claim and confirm
run on the blocking pool; adapter admission uses a conservative monotonic window
started before claim, and only a controllerless ticket or exact-controller
`Gone` proof can confirm. The driver has no concrete controller adapter,
execution loop or production host assembly, so its existence does not close the
resident-host or session-aware recovery decision.

`vyane-service::AgentRunExecutionDriver` is the corresponding one-shot seam for
newly due work, not by itself a resident supervisor. It is fixed-owner,
non-`Clone` and consuming; one bounded pass claims on the blocking pool, creates
independent prospective controller identity and fingerprint material, and
orders durable start, permit issue and a pre-effect heartbeat before the first
executor poll. Only a `Quiesced` executor outcome may initiate settlement.
Before that proof, uncertain exits authorize no settlement and can leave
`Starting` or `Running` for recovery. A blocking settlement already in flight
cannot be cancelled and a custom store may mutate-then-error, so its reported
failure remains uncertain. Standing alone, neither driver supplies a concrete
executor/controller adapter or production host.

`InProcessAgentComponents` pairs both one-shot drivers for one narrow
controller domain. It admits one live backend per owner process-wide even for a
different store pointer; that backend binds one store and structured operation
and mints the exact-fingerprint `InProcess` executor/recovery pair.
`Notify` coordinates cancel/exit observation and a bounded tombstone set closes
late-registration races. Only successful durable gone confirmation reclaims an
exact tombstone, and a later registration must revalidate before operation
code. `ResidentInProcessAgentSupervisor` can consume that exact pairing into
separate bounded execution/recovery polling loops. Host cancellation stops new
cycles but is not forwarded as AgentRun cancellation to a pass already in
progress; that pass drains with an independent token. Degraded/error/panic
cycles back off, and the supervisor never enqueues resume. It is still a dark
library composition with no concrete operation, Process/Remote backend,
message handback or production assembly.

Owner propagation now also has a service-layer phase-A contract.
`OwnerContextFactory` freezes trusted authentication and resolution, keeps
principal construction private, reserves `local`, and `OwnerScopedService`
freezes that owner across dispatch, streaming, query and session mutation.
Built-in frontends deliberately retain `local` compatibility; REST freezes all
service operations into one local scope at router assembly. Durable task truth
is owner-qualified, but REST and resident control still
select the explicit local compatibility owner. The future resident host still
needs one distinct-principal authenticated context at its control plane.

## Consequences

- The existing workflow daemon remains a valid narrow supervisor rather than a
  false claim of original-daemon parity.
- Durable AgentRun state and bounded tree cancellation now exist at the store
  boundary; worker supervision and message pumping can be composed around them
  without creating a second message/task truth.
- The resident broker driver proves that independently failing bounded loops can
  share one explicit caller-owned cancellation and drain boundary without a god
  daemon. Cancellation is observed between cycles; an in-progress bounded cycle
  drains, while dropping the outer future explicitly forfeits that guarantee.
- The one-shot AgentRun recovery seam proves that controller proof and durable
  settlement can remain owner-bound, bounded and body-free before a resident
  execution supervisor exists. Adapter timeout/drop and custom-store blocking
  remain uncertain boundaries: exact-identity effects must be repeat-safe, and
  timeout never proves a non-abortable external effect stopped.
- The one-shot execution seam proves bounded claim/start/heartbeat/effect
  ordering and single-writer receipt renewal without inventing a second queue
  or resident runtime. Per-effect permit revalidation remains a trusted executor
  obligation. Pre-`Quiesced` uncertainty is recovery-owned; an already-started
  blocking settlement may complete even after its waiter is dropped.
- The paired in-process resident supervisor proves independent execution and
  recovery polling, bounded backoff and shutdown drain without conflating host
  cancellation with AgentRun cancellation. Forced drop and blocking custom
  stores remain uncertain, and no resume is automatically enqueued.
- INT-04 remains `different` until the composed host has executable contracts;
  this ADR defines the accepted architecture, not completion.

## Acceptance gates

- Hermetic restart tests prove exact ownership fencing and component-local
  reconciliation for workflow, AgentRun and message pumps.
- Owner authentication is established once at the control plane and propagated
  as an explicit `OwnerContext`; phase A proves service propagation, while REST,
  task control and future resident-host wiring remain acceptance work. Stores
  still enforce owner-scoped lookup.
- Every supervisor documents its cycle and drain bounds. A host may enforce a
  wider outer timeout, but a forced drop is not graceful drain; any
  controller-owning supervisor must additionally fence replacement ownership so
  abandoned work cannot authorize a stale controller.
- Fake A2A/Channels adapters can be enabled independently and cannot access
  another owner's topology, messages or event cursor.
