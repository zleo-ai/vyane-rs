# Kernel alignment audit — current public impl vs Python / Horus / Tauri plans

**Status:** public-safe gap map for the next kernel work package  
**Kind:** evidence / planning input. This document does **not** implement a work
package, change the 53-item parity headline counts, merge anything, or switch
production kernels.

## Identity

| Field | Value |
|-------|--------|
| Program | Long-running public Rust kernel (determinism, durable resume, CompletionReceipt, approval resume, harness/model evidence, verifiable delivery) |
| Rust baseline (this audit) | `c040333f2b4b8d2c4c37430cc7b2ac8ea1fb7fb5` (`main` = `#181`) |
| Planning-log SHA named on the parent card | `c511393b5d4bfdc3ea34466221042a7b05a6f54a` (`#180`; parent of `#181`) |
| Card backfill SHA | `86eb3949ace8b510824399a0db8e37803df6db8a` (`#173`) |
| Decision-grade program start | `10ebe700cef3416459beebfb7ed07d7e9b866de7` |
| Kernel-pilot baseline (post `#169`/`#170`) | `a3e24ba6a3195b482fccce9ca4fef527623fa222` |
| Measurement date (UTC) | 2026-08-20 |
| runtime / harness | `grok-build` / `grok-build` |
| model | `grok-4.6` |
| Private Python / Horus / Tauri trees | **not executed** (clean-room; incomparable cells stay incomparable) |

Prior receipts this audit refreshes against, without replacing:

- [`DECISION-GRADE-KERNEL.md`](../plan/DECISION-GRADE-KERNEL.md)
- [`KERNEL-INTEGRATION-PILOT.md`](../plan/KERNEL-INTEGRATION-PILOT.md)
- [`KERNEL-INTEGRATION-RECEIPT.md`](./KERNEL-INTEGRATION-RECEIPT.md)
- [`python-vs-rust-migration.md`](./python-vs-rust-migration.md)
- [`kernel-tauri-adapter-example.md`](./kernel-tauri-adapter-example.md)
- [`ORIGINAL-VYANE-PARITY.md`](../parity/ORIGINAL-VYANE-PARITY.md)

## 1. What this slice answers

The parent card's order after backfilling `#169` / `#170` / `#172` / `#173` is:
**audit current public implementation against the Python Vyane / Horus / Tauri
plans, then pick the next work package.** This file is that audit. The next
package is **recommended here and not implemented in this change**.

Non-goals (unchanged):

- file-by-file Python port
- second authority besides `kernel.sqlite` on the dogfood path
- production kernel cutover, crates.io, tags, or release
- WP-179–465 formatter residual train (hard stop)
- inflating `implemented` counts without matrix rules + hermetic evidence

## 2. Method

1. Read the public tree at `c040333f2b4b8d2c4c37430cc7b2ac8ea1fb7fb5`.
2. Classify each card-mainline theme as **closed on the chosen Process dogfood
   path**, **closed in store but open on the Horus/Tauri boundary**, or **open**.
3. Compare to the already-published 53-item matrix and to the Tauri command map.
   Private source, private paths, credentials, and deploy identifiers stay out.
4. Name one next kernel work package that is already a hole in *this* public
   tree, with an existing bounded child PR when one exists.

Recompute of sanitized public fixtures remains
`python3 .github/scripts/parity-report.py --format markdown`. That tool does
not execute the private reference.

## 3. Card mainline vs current public kernel

Chosen vertical path is still `ProcessLaneAutonomousDelivery` (see the decision-grade
plan). Counts on the 53-item matrix remain
**7 implemented / 23 partial / 12 missing / 9 different / 2 planned**.

| Theme | Public evidence at this SHA | Residual |
|-------|-----------------------------|----------|
| Deterministic routing | `vyane-router` + ADR 0001; whole-chain admission before spawn | History/feedback learning signals remain out of public-core default (EXE-05 `different`) |
| Durable resume | `KernelStore` (`kernel.sqlite`) CAS for receipt / effect / approval / artifact / lease fence / delivery phase; AgentRun claim/lease stays in `agent.sqlite`; restart **does not** replay payload (ADR 0002) | Live pause/resume (CON-04) and automatic payload replay (CON-05) stay `planned`. Native session resume is still fail-closed. |
| CompletionReceipt | `vyane-core::receipt` cannot complete without a passed truth probe + artifact digest; dogfood persists receipts in `KernelStore`; `#176` reconciles AgentRun `Succeeded` vs receipt still `Open` after crash-between-commit | Not a multi-tenant receipt service. Cost stays absent unless a provider supplies it (never written as `0`). |
| Approval resume | Dogfood `grant_approval` / `deny_approval` write `KernelStore` with digest/revision/generation binding; deny-then-grant fail-closed; grant-then-crash-before-effect does not duplicate | **Horus/Tauri boundary still stubs approve/deny.** Native lane `ask` is still a typed non-replayable stop (GOV-05 `partial`). |
| Harness / model evidence | Receipt `RouteConfig` (provider / protocol / harness / model / effort); hermetic `vyane_harness_lifecycle` binary; CLI-harness Process lane is the dogfood host | Formal Claude/Codex/Grok product-harness wiring stays in the adapter plane. |
| Verifiable delivery | Dogfood fail-then-pass truth probe; independent goal verifier + durable artifacts (`#175` / `#180`) | Goal pursuit is not the Process dogfood engine; do not treat goal completion as kernel-boundary approval resume. |

### 3.1 The live Horus / Tauri hole (verified in-tree)

`crates/vyane-service/src/kernel_boundary.rs` documents the split and still
implements it:

- **Durable facts:** `DriveDogfood` → `KernelStore` (`kernel.sqlite`).
- **Boundary `DecideApproval` / `DenyApproval`:** push in-process
  `Approved` / `Denied` events. They do not open `KernelStore`, do not require a
  pending ask + digest binding, and do not fail closed on missing store /
  deny-after-grant / grant-after-deny.

`Status` / `ReadReceipt` *can* rebuild from `kernel.sqlite` when `dogfood_root`
or a registered durable root is supplied (`#173`). That does not make stub
approve/deny durable. The same residual is already written in
[`kernel-tauri-adapter-example.md`](./kernel-tauri-adapter-example.md).

Dogfood and `KernelStore::{grant_approval,deny_approval}` already encode the
product FSM (`approval_fsm.rs`: deny terminal, grant binds task/run/owner/
revision/digest). The missing slice is **the versioned shell command path**
using that store, not a second FSM.

## 4. Python Vyane (public-safe)

The July 2026 parity baseline plus the 2026-08-04 migration report still hold
for this SHA:

1. Rust owns a **stronger** Process-lane core: owner keys from day one, CAS,
   fail-closed unknown schema, no automatic payload replay, truth-gated
   CompletionReceipt.
2. Python still owns a **broader** product surface (A2A HTTP, channels, board,
   live pause, dashboard). Those stay `missing` / `partial` / `different` on
   the matrix. They are adapter-plane work, not the next kernel package.
3. Matched performance cells remain **incomparable** from this public
   repository (no shared public Python harness).
4. Migration recommendation remains **B — gradual module-by-module replacement
   of the Rust kernel surface**. Not A (no single Python capability selected
   for shadow migration). Not C (kernel store + receipt + dogfood approval are
   past a pure experiment). Not a production cutover.

This audit does **not** refresh private-side evidence and does **not** move any
matrix row. LED-05 and GOV-05 stay `partial` even if the next work package
lands: the matrix forbids an unqualified jump to `implemented` without the
listed product-store / native-resume acceptance.

## 5. Horus / Tauri plan

The public plan is a **thin adapter** over versioned `KernelCommand` /
`KernelEvent` / `KernelProjection`. No Tauri types belong in the kernel.
`display_hint` is never authoritative. Principal bind happens in the adapter;
payload cannot override owner.

Command map at this SHA (unchanged): Submit, Status, Approve, Deny, Cancel,
ReadArtifact, ReadReceipt, DriveDogfood.

The blocker for a future shell is not “missing command names”. It is that
Approve/Deny on `LocalKernelAdapter` are event stubs, so a UI that called them
would see an in-process grant that another process / restart cannot rebuild.
That is exactly the Horus/Tauri risk the kernel-pilot residual called out.

## 6. Public main since the card backfill (`86eb394` → `c040333`)

| SHA | PR | Kernel relevance |
|-----|----|------------------|
| `915905a` / `#174` | isolate uncertain execution recovery fixture | CI fixture isolation; not a kernel capability |
| `#175` | verifier-backed goal completion + lease safety | Adjacent GoalStore honesty; not kernel-boundary approval |
| `11d330d` / `#176` | finalize open receipt after succeeded AgentRun | Closes the crash window where AgentRun was `Succeeded` and the receipt stayed `Open` |
| `730d65c` / `#177` | detach SIGKILL ESRCH wait | CLI lifecycle flake; not kernel store |
| `c511393` / `#180` | GoalStore residual P2 contracts | Goal verifier tests; not kernel store |
| `c040333` / `#181` | exclusive tempdirs for MCP workflow-port fixtures | macOS main flake that had been blocking other PRs; **merged** |

Open, out of this document's implementation scope:

| Item | State at audit time | Notes |
|------|---------------------|-------|
| `#178` persist kernel-boundary approve/deny in `KernelStore` | Open; merge state `UNSTABLE` (macOS test failed 2026-08-15; Ubuntu cancelled). Reviewer quota exhausted; no independent review. | Child of the approval-resume hole in §3.1. Needs rebase onto post-`#181` `main` and a real review. **Not merged from this slice.** |
| `#179` Cloud Agent environment | Draft | Out of kernel scope |
| Review-pipeline / CONTRIBUTING tiering | Process remainder on a different card | Not a kernel work package |

## 7. Recommended next work package (not this PR)

**Name:** bind `LocalKernelAdapter` `DecideApproval` / `DenyApproval` to the
existing `KernelStore` (no second FSM).

**Why this one, now:**

1. It is the only remaining hole on the card's approval-resume + Horus/Tauri
   mainline that is already implemented in the store and stubbed at the
   published shell boundary.
2. `#181` removed the macOS workflow-port flake that the parent-card log
   treated as the merge-gate blocker for `#178`.
3. Determinism, CompletionReceipt, dogfood durable resume, and hermetic
   harness evidence are already on `main` for the Process path.

**Acceptance shape (already written on the child card; restated so this audit
stands alone):**

1. Baseline probe on current `main`: approve without a store emits `Approved`
   and is invisible after reopen / in another process; deny-then-grant still
   passes the stub.
2. Bound grant/deny require a resolvable durable root, a pending ask, and a
   matching revision/digest. Missing store, wrong binding, deny-after-grant,
   and grant-after-deny fail closed.
3. Successful grant rebuilds `Status` / `ReadReceipt` from `kernel.sqlite`.
   Crash-before-effect does not duplicate the effect. Reuse `KernelStore` /
   dogfood FSM.
4. Do **not** move GOV-05 / LED-05 from `partial` to `implemented` unless the
   matrix rules are met; residual notes only.
5. Independent review + fmt/clippy/test; merge is a later gate.

**Deferred (explicitly not next):**

| Candidate | Why not now |
|-----------|-------------|
| Native session resume / checkpoint commit | EXE-07 / CON-01 still require session-aware authority; larger than one slice |
| Live pause/resume (CON-04) | Intentionally planned; dogfood uses cancel + restart adoption |
| Automatic payload replay (CON-05) | Fail-closed by ADR 0002 |
| A2A HTTP / board / dashboard | Adapter plane; not the kernel vertical |
| rmcp patch bump | Interface crate, not kernel mainline |
| Review-channel / CONTRIBUTING tiers | Process, not kernel |
| Formatter residual train WP-179–465 | Hard stop |

## 8. Stop conditions (none hit)

No payment/credential expansion, no production deploy, no crates.io/tag, no
private-tree copy, no second overlapping writer on this docs slice, no failed
truth-probe reproduction. A slow or quota-exhausted GitHub review bot is not a
stop.

## 9. How to use this file

1. Treat §7 as the next kernel work package unless new hermetic evidence
   contradicts §3.1.
2. Keep recommendation **B**.
3. Do not claim whole-system Python parity from this audit.
4. Record runtime / harness / model / SHA on the implementing PR the same way
   the kernel-pilot receipt does.
