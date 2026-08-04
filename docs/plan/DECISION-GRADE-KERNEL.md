# Decision-grade Rust kernel — product path and milestone plan

**Status:** active product program (not a micro-WP train)  
**Baseline HEAD:** `10ebe700cef3416459beebfb7ed07d7e9b866de7` (main = origin/main at goal start)  
**Runtime identity for this program:** runtime=`grok-build`, harness=`grok-build`, model=`grok-4.5`, reasoning_effort=`high`  
**Non-goal:** resume WP-179–465 pure-formatter residual work (hard stop remains)

## 1. Why this program exists

`vyane-rs` already has many partial kernel capabilities. The product question is
not “how many small pure formatters remain,” but whether one **complete,
restart-safe autonomous delivery path** is decision-grade enough to decide:

1. which responsibilities should move from Python Vyane to Rust;
2. which should stay in a fast-changing adapter plane;
3. whether a Rust kernel helps a future Horus/Tauri desktop architecture;
4. what compatibility boundary permits gradual migration without a flag day.

## 2. Chosen end-to-end vertical path

**Name:** `ProcessLaneAutonomousDelivery` (short: **Process dogfood path**)

```
TaskCase
  → explicit RouteConfig (provider / protocol / harness / model / effort)
  → owner-scoped AgentRun create or adopt
  → atomic claim + execution lease (ActiveExecutionPermit)
  → Linux CLI-harness Process lane execution
  → tool permission ceiling decision (and approval-required stop when ask)
  → acceptance truth probe (must fail on broken baseline, pass after fix)
  → immutable output artifact (digest-checked)
  → CompletionReceipt (truth-gated; no complete without verified gates)
  → cancel / crash / daemon restart adoption with duplicate-effect prevention
```

### Why Process lane (not Native-first, not full goal pursuit)

| Candidate | Fit | Why not chosen as primary |
|-----------|-----|---------------------------|
| **Process AgentRun** (WP-61+) | Has production submit/status/output/cancel, exact lease, completion handback, restart no-replay, cancel, residue cleanup | **Chosen.** Shortest path that already closes external effect + recovery with hermetic fake harness binaries. |
| Native/Canto fresh sessionless | Strong authority/tool surface; approval-required exists as terminal ask | Secondary: needs mock HTTP + more moving parts for the same recovery evidence. Still exercised for approval/tool ceiling gates on the dogfood suite. |
| Goal pursuit (WP-70–82) | Real claim/lease, verifier, artifact, continuity | Not the primary execution engine; dogfood **imports** goal acceptance verification + immutable artifact contracts rather than re-implementing pursuit. |
| Full Python parity / Remote | Out of scope | Non-goal; no production cutover. |

### Explicit composition (reuse, do not fork a parallel runtime)

- **Route:** four-layer `Target` + requested/effective effort (`vyane-core` / config resolve).
- **Ownership:** `vyane-agent` owner-scoped queue, lease generation, active permit.
- **Execution:** resident Process backend (`vyane-cli` / `vyane-service` host).
- **Approval/tool:** native permission ceiling + configured `ask` as required gate evidence (Process path records ceiling; Native path supplies live ask-stop when needed).
- **Verification:** model-free `AcceptanceVerifier` style truth probe over synthetic/public fixtures.
- **Artifact:** digest-checked immutable blob (goal artifact pattern / message completion sink).
- **Receipt:** new public-safe `CompletionReceipt` schema (this program, Phase 1).
- **Recovery:** existing restart no-replay + completion publisher idempotency + hostile tests (Phase 3).

## 3. Parity matrix classification (53 items)

Counts remain honest: **7 implemented / 23 partial / 12 missing / 9 different / 2 planned**
(LED-05 advanced to `partial` on receipt/dogfood hermetic evidence).

Classification is relative to the **Process dogfood path**, not whole-system parity.

| Class | Meaning for this program |
|-------|--------------------------|
| **(a) required by chosen path** | Must be present or closed on the dogfood path before goal complete. |
| **(b) future Python compatibility** | Needed for gradual migration evidence; not a dogfood blocker if marked incomparable. |
| **(c) useful, not product-critical now** | Defer without blocking decision-grade path. |
| **(d) intentionally different** | Keep; document; do not “fix” by cloning Python debt. |
| **(e) obsolete / duplicated** | Do not expand; stop micro-trains. |

### (a) Required by chosen path

| ID | State | Role on dogfood path |
|----|-------|----------------------|
| EXE-01 | implemented | Four-layer route identity on receipt |
| EXE-02 | implemented | Dispatch/failover evidence when route uses chain |
| EXE-07 | partial | Tool/permission/approval gates (Native slice for ask/tools; Process for harness effect) |
| EXE-09 | different | Detached/resident Process AgentRun is the execution host |
| CON-06 | partial | Lease, recovery admission, restart no-replay |
| COL-03 | partial | Durable AgentRun queue/claim/lease |
| COL-05 | partial | Tree cancel / process-group cancel |
| LED-01 | implemented | Attempt trail / usage fields when available |
| LED-05 | missing | Approvals + artifacts as receipt gates (generic schema in-repo) |
| GOV-01 | partial | Owner isolation on receipt store |
| GOV-02 | implemented | Clean-env, process group, controller recovery |
| GOV-03 | partial | Capability/sandbox ceiling on route |
| GOV-05 | partial | allow/ask/deny + approval-required evidence |
| QUA-03 | implemented | Local gates / truth probe harness |
| OBS-02 | implemented | Terminal status visibility |
| INT-04 | different | Narrow Process host is the dogfood control plane |

### (b) Future Python compatibility / migration evidence

EXE-03, EXE-04, EXE-05, EXE-06, CON-01, CON-03, CON-07, COL-01, COL-02, LED-02, LED-04, LED-06, GOV-04, QUA-01, QUA-04, OBS-01, OBS-03, INT-01, INT-02, INT-03, INT-08

### (c) Useful but not product-critical for this decision

COL-06, LED-03, GOV-06, GOV-07, QUA-02, OBS-04, OBS-05, OBS-06, INT-05, INT-06

### (d) Intentionally different (keep)

EXE-05, EXE-06, EXE-09, CON-03, LED-02, LED-06, QUA-01, INT-04, INT-07 — ADRs already bound several of these.

### (e) Obsolete / do not expand under this goal

- Pure human formatter residual train (WP-179–465 and similar status-string extraction).
- Per-fixture process-isolation micro-WPs unless a new hostile lifecycle gap is proven.
- crates.io publish / tag / release actions (require separate Maple approval).

### Planned items touching path recovery story

| ID | State | Handling |
|----|-------|----------|
| CON-04 | planned | Live pause/resume **not** required; dogfood uses cancel + restart adoption instead. |
| CON-05 | planned | Automatic payload replay remains fail-closed (ADR 0002); dogfood proves **no duplicate effect**, not silent replay. |

## 4. Semantic milestones (not micro-WPs)

| Milestone | User-visible capability | Primary surfaces |
|-----------|-------------------------|------------------|
| **M0** | Decision-oriented path + classification (this document + roadmap pointer) | `docs/` |
| **M1** | Typed provenance + CompletionReceipt contract | `vyane-core` receipt module + tests |
| **M2** | One local dogfood path → truth-verified receipt | compose Process + verifier + receipt; acceptance tests |
| **M3** | Recovery, lifecycle, hostile idempotency | cancel/crash/restart/orphan/lease races |
| **M4** | Tauri-ready local kernel boundary | versioned commands/events/projections (no UI) |
| **M5** | Migration evidence + eval readiness + final receipt | `docs/` report, parity honesty, instrumentation |

Large PRs carry a review map (domains, invariants, transitions, security, validation, deferred).

## 5. Assumptions

1. Public clean-room: no private Python source, paths, credentials, or Beacon IDs in commits.
2. Synthetic/public fixtures and fake harness binaries only for hermetic dogfood.
3. Process lane remains Linux-first for mutating harness work (existing product rule).
4. `ask` may still lack full approval-resume product; dogfood records approval-required as a **required gate** and implements the minimum durable approval decision needed for criterion coverage without dropping the requirement.
5. Cost fields stay unknown unless a provider supplies them; never claim zero cost from silence.
6. Formatter residual train stays stopped.
7. No production Vyane switch, data migration, or registry publish without Maple approval.

## 6. Stop conditions

Pause only for:

- payment / new paid provider / credential expansion;
- production deploy or data migration;
- destructive irreversible action;
- public release/tag/crate publish;
- architecture that would make Python compatibility impossible;
- two consecutive attempts at the same blocker with no new evidence;
- baseline truth probe cannot reproduce the claimed failure;
- overlapping writer ownership;
- required external service unavailable and no local evidence can advance.

A slow review or optional GitHub bot is **not** a stop.

## 7. Terminal completeness (goal)

1. One real e2e task reaches truth-verified CompletionReceipt.
2. Cancel, crash, restart recovery demonstrated.
3. Duplicate effects prevented or explicitly detected.
4. Route/model/harness/effort provenance durable.
5. Tauri-ready local contract documented and tested.
6. Parity matrix updated honestly (no inflated `implemented`).
7. Decision-grade Python-vs-Rust report with recommendation A/B/C/D.
8. Milestone PRs have required CI + review evidence.
9. No release/production switch without Maple approval.
10. Final CompletionReceipt lists ready / partial / recommended migration path.

## 8. Capability count honesty

This program **does not** change the 53-item headline counts until hermetic evidence
justifies a status move. Shipping CompletionReceipt + dogfood may advance LED-05 /
GOV-05 / COL-03 evidence as `partial` improvements with explicit residual notes —
never an unqualified jump to whole-system `implemented` without the matrix rules.
