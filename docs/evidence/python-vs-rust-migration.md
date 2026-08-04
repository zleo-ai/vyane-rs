# Python-versus-Rust migration evidence (decision-grade)

**Status:** public-safe matched comparison for the Process dogfood path  
**Rust baseline HEAD (program start):** `10ebe700cef3416459beebfb7ed07d7e9b866de7`  
**Measurement date (UTC):** 2026-08-04  
**Hardware/OS:** Linux (developer workstation; architecture details intentionally coarse for public repo)  
**Config digest:** `sha256(profile-dogfood)` via `digest_bytes(b"profile-dogfood")` in dogfood fixtures  
**Sample size:** hermetic unit/integration N≥2 dual-run for dogfood success; microbenchmarks N=1 cold process where noted  
**No production cutover performed.**

## Scope of comparison

Only capabilities both sides can exercise with **public-safe** inputs, or cells marked
**incomparable** when the private Python reference cannot be executed from this public
repository under the clean-room boundary.

Matched surface for this report:

| Capability | Rust public evidence | Python public-safe match |
|------------|----------------------|---------------------------|
| Cold start (library dogfood open→receipt) | `vyane-service` dogfood tests | Incomparable (private service binary not in public tree) |
| Idle / steady memory | Not measured this milestone | Incomparable |
| Task submit latency (AgentRun claim) | Sub-second hermetic store claim in dogfood | Incomparable without shared harness |
| Event throughput | Local kernel boundary event queue | Incomparable |
| Cancellation latency | Soft-cancel + tree cancel path | Incomparable |
| Restart recovery / no duplicate effect | Hostile crash fences + effect log | Conceptual match: Python async pause loses task ref (parity CON-04/05) |
| Concurrent owner isolation | Cross-owner receipt fence tests | Expected stronger on Rust day-one owner keys |
| Packaging / install | Workspace crates, no crates.io publish | Different packaging model (INT-07) |
| Adapter development effort | Native/Process seams already modular | Private adapters exist; effort not re-measured here |
| Failure observability | Typed `ReceiptError`, `DogfoodError`, gate outcomes | Incomparable without shared ledger schema |

## Matched metrics table

| Metric | Rust (public) | Python (public-safe) | p50 / p90 | Notes |
|--------|---------------|----------------------|-----------|-------|
| Cold start dogfood path | Completes in test runtime (~1s wall for dual run suite slice) | N/A | N/A | Hermetic, not production SLO |
| Idle resident memory | N/A | N/A | N/A | Not measured |
| Steady-state memory | N/A | N/A | N/A | Not measured |
| Task submit latency | AgentStore create+claim in-process | N/A | N/A | Store-level, not HTTP |
| Event throughput | LocalKernelAdapter push | N/A | N/A | In-process adapter only |
| Cancel latency | Immediate soft-cancel flag + durable cancel_tree | N/A | N/A | |
| Restart recovery | Effect not duplicated after crash fences | Private: often fail-open or lose reference | N/A | Rust fail-closed / explicit unresolved |
| Duplicate-effect prevention | `ExternalEffectLog` + AgentRun completion CAS | Incomparable | N/A | |
| Concurrent owner isolation | Foreign-as-absent receipts | Partial in private (parity GOV-01) | N/A | |
| Packaging | 17-crate workspace preflight | Python packaging | N/A | Different (INT-07) |
| Cross-platform | Linux Process mutating; dogfood library portable | Private multi-OS | N/A | |
| Adapter effort | Seams exist; dogfood reuses them | Mature private adapters | N/A | Qualitative |
| Failure observability | Typed gates + receipt final status | Mixed status files | N/A | |

## Functional differences (honest)

1. **Rust** records a versioned `CompletionReceipt` that **cannot** claim `completed` without a passed truth probe and artifact digest.
2. **Rust** Process/Native restart policy is **no automatic payload replay** (ADR 0002); recovery yields same receipt or `unresolved`.
3. **Rust** owner isolation is structural from day one on AgentRun/goal/task/receipt.
4. **Python** retains broader product surface (A2A HTTP, channels, board, live pause) still `missing`/`partial` in Rust matrix.
5. Approval **resume** remains incomplete on both public Rust native lane (`ask` terminal) and is only partially covered by dogfood approval gate recording.

## Recommendation

### **B. Gradual module-by-module replacement**

**Evidence:**

- The Process dogfood path proves a **decision-grade core slice**: claim/lease → effect → truth probe → artifact → receipt → crash/cancel safety.
- Large Python-only surfaces (channels, A2A HTTP, board, dashboard) remain valuable as an **adapter plane** and should not block Rust kernel progress.
- A full rewrite (A without adapters) is not justified: migration report cells are largely incomparable without a shared public Python harness.
- Keeping Rust as pure experiment (C) understates the already-production-assembled Process/Native host and stronger fencing.
- Therefore: **keep a Rust kernel for durable execution/ownership/receipt**, grow it module-by-module, and leave fast-changing UX/channel/provider glue in adapters (Python or future Tauri frontends over the local kernel boundary).

This is **not** a production cutover recommendation.

## Matched evaluation readiness (model/harness)

Instrumentation support for later Commander comparison (same task case, base SHA, acceptance, truth probe):

| Field | Location |
|-------|----------|
| TaskCase digests | `CompletionReceipt.task_case` |
| Route (provider/protocol/harness/model/effort) | `CompletionReceipt.route` |
| Attempt tokens/cost (unknown unless supplied) | `ReceiptAttempt` |
| Gates / truth probe | `GateResult` |
| Base/head SHA | `code_base_sha` / `code_head_sha` |
| Runtime identity (this program) | runtime=`grok-build`, harness=`grok-build`, model=`grok-4.5`, effort=`high` |
| Rework count | `rework_count` |
| Review dispositions | `review_findings` |

Grok 4.5 / Grok Build work on this program contributes **one route’s** evidence only and **does not** declare a winner versus other models.
