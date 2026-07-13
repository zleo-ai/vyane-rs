# ADR 0005: execution identity, capability admission and authority are separate

- Status: accepted
- Date: 2026-07-10
- Parity rows: EXE-07, GOV-03, GOV-04, GOV-05, CON-01, CON-06

## Context

Before the capability-admission implementation, the dispatcher created a run
id only after all attempts had finished. Factories and executors therefore
could not share one pre-existing execution identity with lifecycle events,
checkpoints or a future native tool loop. `TaskSpec.sandbox` was passed to CLI
harnesses, but a
direct-HTTP target ignored `Write` or `Full`; a failover chain could consequently
accept a task that claimed filesystem authority and execute it in a chat-only
runtime. At that baseline, native session ids were also stored without an
exact runtime/workspace domain.

The fixed original Vyane baseline has a useful result-level rule: a write task
must not fail over to a chat-only or different-filesystem executor. Its
capability flag is only a declaration, however, and its native sandbox and
session/checkpoint authority are not strong enough to copy unchanged.

## Decision

1. One kernel-dispatch execution id is allocated before session loading, capability inspection,
   factory construction or model execution. An `ExecutionScope` and each
   chain-position `AttemptScope` carry this stable, serializable audit identity.
   They contain no credential, lease token or approval capability and are never
   treated as authority. A workflow run, detached task or AgentRun keeps its own
   lifecycle identity and correlates it to one or more kernel-dispatch ids; the
   ids are not required to be equal.
2. Executors expose a trusted `CapabilityManifest` through the assembled
   factory boundary. Unknown/custom executors default to chat-only. Ordinary
   project configuration cannot self-assert filesystem or host-sandbox power.
3. The kernel preflights the whole resolved chain before its first attempt.
   Admission returns serializable evidence and structured rejections, not a
   grant. A rejected fallback is never constructed or called; if the primary
   later fails, its original error is preserved when no admitted fallback
   remains.
4. `Write` and `Full` require the same canonical execution-workdir domain.
   Direct HTTP, remote/A2A filesystems and unknown runtimes cannot satisfy that
   requirement. Existing local coding-CLI harnesses may declare adapter-
   delegated editing capability, but that declaration is not evidence of OS
   confinement and its isolation strength is recorded separately. A future
   native harness must declare host-enforced sandboxing and will initially
   expose no native `Full` capability.
5. Actual native side-effect authority is a non-serializable, redacted
   `ActiveExecutionPermit` issued from an exact running AgentRun receipt. Every
   model turn, side-effecting tool, checkpoint write and native-session commit
   revalidates owner, run, worker generation, secret token, lease/deadline and
   frozen policy. Scope and admission digests cannot substitute for that
   permit.
6. A `NativeSessionDomain` binds a native session to runtime/harness,
   provider/protocol/model, a canonical endpoint-routing digest, canonical
   workdir, checkpoint namespace/schema and account/runtime scope. The endpoint
   digest includes scheme, host, effective port and normalized base path;
   userinfo and fragments are rejected, while the only permitted routing query
   names, `api-version` and `api_version`, are normalized into the digest without
   logging plaintext. Session id and domain are persisted atomically. A legacy
   id without a domain remains readable but is not resumable in place by
   default. A future trusted execution path may reset it or atomically commit a
   fresh fork only after the new native session has actually been created; the
   persisted transition is not itself authority to execute. Any execution that
   names a logical session must first acquire a live, non-serializable lease
   bound to the exact owner/session/execution id, load continuity under it, and
   retain it through the final revision-fenced mutation. A local filesystem
   descriptor lock can satisfy this on one host; distributed stores require
   their own generation, expiry and stale-holder fencing protocol.
7. `NativeHarness` remains unavailable in config/factory registration until
   scoped identity, chain admission, active authority, domain-bound session
   persistence and an OS-enforced sandbox have executable acceptance coverage.

## Consequences

- A serializable execution object can be logged and replayed as evidence
  without becoming a bearer credential.
- Write-capability mismatches fail before executor construction (`make`), HTTP or
  subprocess side effects, including on fallback legs. Preflight does invoke the
  trusted factory's contractually side-effect-free `capability_manifest`.
- Heartbeat/activity revision changes need not invalidate an active permit, but
  cancellation, generation/token change, policy drift, terminal state and
  lease/deadline expiry do.
- Existing read-only direct chat and legacy executor traits can migrate through
  additive scoped methods; stronger capabilities fail closed until declared by
  a trusted assembler.
- This ADR fixes the target architecture. Implemented and open portions are
  recorded below; accepting the decision is not evidence that every layer is
  executable.

## Implementation status

In the clean public integration baseline:

- Decisions 1–4 are implemented. `ExecutionScope`/`AttemptScope`, whole-chain
  trusted manifest admission, stable original ordinals and process-local
  `PreparedDispatch` provenance are enforced before executor construction.
  Linux mutating dispatches open-first and retain one directory object through
  foreground, prepared streaming and detached worker/harness handoff. Other
  platforms fail closed for mutating dispatches.
- Decision 5 now has a live authority contract and the first dark consumers,
  but it is not assembled end to end. `vyane-agent` can issue a non-
  serializable, redacted `ActiveExecutionPermit`; its SQLite store validates
  that permit and an owned `NativeExecutionScope` against the exact target,
  prompt, policy, logical-session and resume-binding identity in one write-
  locked snapshot. `vyane-core::NativeExecutionAuthority` grants one explicitly
  identified side effect at a time. The separate `AuthorizedToolChatClient`
  boundary and the OpenAI Chat implementation revalidate every explicit wire
  send, while `ToolRegistry::execute_authorized` revalidates an allowed call
  immediately before polling its executor. These entry points are intentionally
  unwired. `vyane-service::AgentRunModelToolAuthority` is a concrete bridge
  from an owned permit/scope and `AgentStore` to the live trait only for fresh,
  sessionless scopes and only for `ModelSend`/`ToolOperation`. It moves each
  synchronous store revalidation onto Tokio's blocking pool and fails closed
  for session-bearing scope, checkpoint prepare/publish, and session commit.
  `vyane-harness::native::NativeTurnDriver` now supplies a separate dark,
  strictly serial consumer: default eight and at most 32 model turns, at most
  one call per turn, exact advertised/registry tool-name sets, validation at
  every request and response boundary, and worst-case next-transcript preflight
  before permission or tool polling. It uses only the authorized model/tool
  entries, aggregates usage with saturation, and converts approval, refusal,
  unsupported parallel calls, tool-choice violations, cancellation, timeout,
  budget exhaustion, and post-tool failure into typed non-replayable stops.
  Invalid JSON arguments receive static non-echo text and never execute. Tool
  descriptions and schemas are non-authoritative model guidance; each tool
  remains responsible for validating actual arguments. No production factory,
  runtime, service operation, or `Harness` registration constructs either the
  bridge or driver, and neither composes the session lease or exact native-
  session domain.
- Decision 6 is implemented at the data-contract and local-filesystem execution
  boundary.
  The public `SessionRecord` shape remains source-compatible; additive
  revisioned snapshots classify native state as absent, legacy-unbound, or
  domain-bound. `FsSessionStore` uses a strict schema-2 wrapper, rejects
  missing/unknown/duplicate authority, advances one revision across legacy and
  native mutations, and atomically applies CAS `Reset`, `ForkFresh`, and
  `Commit`. The shared protocol helper validates/canonicalizes HTTP endpoint
  routing and accepts only one explicitly non-secret `api-version` or
  `api_version` query before producing a versioned digest. This still does not
  authorize native execution. Regular dispatch acquires an exact
  owner/session/execution `SessionExecutionLease` before the continuity read,
  validates lease and snapshot identity before `make`, holds the local advisory
  lock through failover/model execution, and commits once with the loaded
  revision. Direct control mutations use the same lock; aborted waiters cannot
  commit after the active execution releases it. Streaming rejects any session
  before session-store load, capability probe or `make`. Pure direct-HTTP
  transcript continuation remains available through regular dispatch. The
  current fence is local and descriptor-owned, not a distributed TTL/generation
  protocol, and post-model session persistence remains best-effort. The owner-
  local service/CLI exposes list, inspect and revision-checked reset-native
  only; there is no public fork, REST mutation or production resume.
- Decision 7 remains in force: there is no registered `NativeHarness`, and the
  current pinned-workdir handoff is adapter-delegated workdir identity, not a
  host-enforced OS sandbox or same-UID security boundary.

## Acceptance gates

- **Implemented:** foreground, broadcast/workflow dispatch, the CLI's prepared
  streaming fallback and detached worker execution allocate one early kernel
  execution id per dispatch and retain stable original-chain ordinals. The
  legacy public `dispatch_stream` contract still returns `Ok(None)` on
  unsupported and does not expose an orphan scoped id.
- **Implemented:** whole-chain preflight occurs before any `make`, HTTP request
  or subprocess; direct-HTTP `Write`/`Full` is rejected, ineligible fallback
  legs are filtered, and a local CLI write failure can fail over only to another
  admitted local workdir executor. Read-only chat failover remains compatible.
- **Implemented at the persistence/admission boundary:** strict V2 migration,
  snapshot listing, revision CAS, store-level reset/fresh-fork/commit, domain-
  field validation, endpoint digest canonicalization, corruption/downgrade
  rejection, and concurrent-writer tests cover the data contract. The local
  execution lease is exact-scoped, single-commit, abort-safe while waiting and
  shared with control mutations; a production-like kernel test proves a second
  same-session dispatch conflicts before factory construction when the holder
  outlives the bounded admission timeout. Forged lease or snapshot identities
  also fail before transcript/model exposure. Kernel tests
  prove that legacy-unbound and exact bound states stop regular harness dispatch
  before `make`/`run`; streaming acceptance proves any session stops before
  load/probe/`make`. Direct-HTTP transcript continuation remains available on
  the regular path. CLI/service tests cover owner-local list, inspect and
  revision-checked reset-native without exposing fork or a REST/MCP mutation.
- **Implemented at the authority-store boundary:** permit tests cover issue-
  only-while-running, heartbeat/activity survival, cancellation and terminal
  revocation, generation/token/policy mismatch, expiry, restart, non-
  serialization and redacted debug output. Native-scope tests additionally
  cover target, prompt, policy, logical-session and resume-proof drift, with
  the complete predicate evaluated in one write-locked SQLite snapshot.
- **Implemented at two guarded consumer boundaries:** OpenAI Chat's authorized
  typed-turn path revalidates every explicit physical send, does not retry an
  authority error, disables implicit client retries and redirects, and observes
  cancellation while waiting for authority, sending, reading the body and
  backing off. The authorized tool-registry path consumes authority only for an
  allowed call; invalid, unknown, denied, approval-required, cancelled and
  expired decisions remain side-effect free. Revocation is an outer execution
  error rather than model-facing tool text.
- **Implemented as a narrow concrete bridge:** the fresh-sessionless
  `AgentRunModelToolAuthority` owns an `ActiveExecutionPermit` and
  `NativeExecutionScope`, sends every model/tool revalidation through the
  blocking pool, preserves redacted errors, and rejects unsupported effect
  kinds before consulting the store.
- **Implemented as a bounded dark turn driver:** initial advertised tool names
  must exactly equal registry names; request, response and pre-tool next-
  transcript validation precede tool side effects; one-based authorized model
  and tool operations are serial; all suspended/terminal conditions are typed;
  usage saturates; post-tool model failure cannot escape as failover-eligible;
  and the non-serializable outcome redacts sensitive content from `Debug`.
- **Implemented at the AgentRun execution boundary:** the fixed-owner,
  non-`Clone`, consuming `AgentRunExecutionDriver` claims a whole bounded batch
  on the blocking pool, generates independent 256-bit prospective controller
  identity and fingerprint material, and orders claim, durable start, permit,
  pre-effect heartbeat and first executor poll. One item future owns the current
  receipt, custom-store results are validated after every transition, and only
  a proved `Quiesced` outcome may initiate settlement. The trusted executor must
  still revalidate the permit at every actual effect. Before `Quiesced`, an
  uncertain exit authorizes no settlement and may leave `Starting` or `Running`
  for exact-identity recovery. An already-started blocking settlement cannot be
  cancelled and a custom store may mutate-then-error, so failure is uncertain.
- **Implemented for one paired in-process controller domain:**
  `InProcessAgentComponents` admits one live backend per owner process-wide,
  regardless of store pointer; that backend binds one store/operation and mints
  both drivers. Exact id/fingerprint observation,
  `Notify` cancellation/exit coordination and a bounded tombstone set close
  replacement and late-registration races. Successful durable confirmation
  reclaims the exact tombstone; a post-reclamation registration revalidates
  before operation code. A lifetime-bound non-`Clone` effect authority
  revalidates the durable permit for every single-use effect proof.
  The operation can now bind that authority to one exact fresh sessionless
  native scope; bind and every model/tool effect atomically revalidate the full
  scope. Raw store/permit access, session/resume, checkpoint and session commit
  remain closed. This is a trusted port with no concrete operation,
  Process/Remote backend, message input or production assembly.
- **Open:** no production assembler/runtime constructs the bridge and driver or
  composes AgentRun authority with `SessionExecutionLease` and exact
  `NativeSessionDomain` validation. There is no registered native `Harness`, trusted
  built-in tool set, checkpoint prepare/publish consumer or revision-fenced
  session-commit consumer. There is also no concrete product operation,
  Process/Remote AgentRun backend, production host, crash-consistent message
  handback, or session-aware resume composition. Registry-level authorization also does not authorize
  an arbitrary tool implementation to perform multiple opens, publishes or
  spawns without further revalidation. Production native resume and end-to-end
  domain-drift acceptance therefore remain intentionally disabled.
