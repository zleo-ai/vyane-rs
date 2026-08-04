# Goal CompletionReceipt — Vyane Rust Kernel Integration Pilot

## Identity

| Field | Value |
|-------|--------|
| Goal | Vyane Rust Kernel Integration Pilot — durable multi-process store, approval resume, hermetic harness lifecycle |
| runtime | grok-build |
| harness | grok-build |
| model | grok-4.5 |
| reasoning_effort | high |
| role | Developer |
| Baseline SHA | `a3e24ba6a3195b482fccce9ca4fef527623fa222` (main post #169/#170) |
| Final SHA | *(filled at merge)* |
| started_at (UTC) | 2026-08-04T14:53:14Z |
| ended_at (UTC) | *(filled at merge)* |
| distinct attempts | 1 primary implementation train |
| corrective commits | tracked on feature branch |
| first-pass | pending independent review + CI |
| rework_count | 0 (post-skeptic of prior program; this pilot is new) |
| cost / tokens | **unknown** (never claimed as 0) |

## Ready (proven hermetically)

1. **SQLite KernelStore** (`kernel.sqlite`): transactional CAS for CompletionReceipt, effect identity, approval decision, artifact metadata, lease fence, delivery phase, schema version. Multi-process effect race test: exactly one first apply. Unknown schema version fail closed. Unknown cost fields stay absent (not 0).
2. **Crash/restart no-duplicate-effects**: dogfood reopen after `AfterEffectBeforeReceipt` completes once; effect count remains 1. Grant-then-crash-before-effect resumes without duplicate.
3. **Approval FSM product path**: Running → ApprovalRequired → Approved|Denied → Resuming → … → Completed. Ask-only entry; deny terminal; grant binds task/run/owner/revision/digest; idempotent re-grant; wrong revision fails closed; approval gate on receipt.
4. **Hermetic harness lifecycle binary** `vyane_harness_lifecycle`: spawn → events → approval wait → grant resume → effect → artifact → completed; dual-run consistency; kill/cancel; crash-after-start injector. **Not** formal vendor harness integration.
5. **Versioned kernel boundary**: commands submit/status/approve/deny/cancel/read artifact|receipt/DriveDogfood; events covering accepted through completion_receipt_finalized / canceled|failed; display_hint non-authoritative; principal authz before mutation. **Durable multi-process path** is DriveDogfood → KernelStore; in-process approve/deny without dogfood are event stubs (see residual).
6. **Composition**: reuses `SqliteAgentStore` for AgentRun claim/lease; does **not** fork a second runtime.

## Partial / residual (pilot, not production)

- Formal Claude/Codex/Grok product harness wiring remains in adapter plane / existing daemon acceptance.
- LocalKernelAdapter event queue is process-local; in-process `DecideApproval`/`DenyApproval` without KernelStore are **not** multi-process durable authority (dogfood/KernelStore is).
- Effect apply is record-then-side-effect (at-most-once / no duplicate); crash between CAS and OS child may leave identity without side effect — recovery does not invent success without truth probe.
- Multi-writer race probe uses concurrent threads on one SQLite file (Immediate txn); true OS multi-process open race covered by idempotent schema init + Immediate effect apply.
- No multi-tenant production service, no production cutover, no crates.io/tag/release.
- Live pause/resume (CON-04) and automatic payload replay (CON-05) remain intentionally out.

## Fail-mode truth probes

| Probe | Baseline intent | Status |
|-------|-----------------|--------|
| TP-MP-01 concurrent effect race | only one first apply | pass (`multi_process_effect_race_only_one_first` — concurrent threads + Immediate txn) |
| TP-MP-02 stale lease generation overwrite | fail closed | pass (`stale_lease_generation_cannot_overwrite_fence`) |
| TP-RE-01 reopen no duplicate | effect count 1 after restart | pass (`crash_after_effect_true_process_restart_does_not_duplicate`) |
| TP-CR-01 crash after effect before receipt | recover without second effect | pass (same as TP-RE-01) |
| TP-CR-02 crash after receipt | terminal retained; no re-effect | pass via complete-then-reopen foreign complete fail-closed + effect count |
| TP-AP-01 deny then grant | fail closed | pass (`approval_deny_then_grant_fails_closed`) |
| TP-AP-02 wrong binding | fail closed | pass (`approval_grant_binding_and_deny_final` + fence check on grant) |
| TP-AP-03 grant crash before effect | resume once | pass (`approval_grant_then_crash_before_effect_no_duplicate_on_resume`) |
| TP-SCHEMA-01 unknown schema | fail closed | pass (`unknown_schema_version_fails_closed`) |
| TP-COST-01 unknown cost ≠ 0 | pass | pass (`unknown_cost_not_zero_on_receipt_attempt_path`) |

## Migration recommendation (evidence-based)

### **B — Continue gradual module-by-module replacement of the Rust kernel surface**

Not A (no single Python capability selected for shadow migration without shared public harness evidence).  
Not C (Rust already owns more than pure Process experiment: receipt, approval, multi-process kernel store, boundary).  
Not D (pilot unblocked next kernel infrastructure work).

Evidence: multi-process SQLite authority closed the #169/#170 JSON multi-process gap; approval resume product path exists; hermetic lifecycle protocol binary exists; adapter plane (providers/UI/vendor harness) still belongs outside the kernel.

## Reviewer

- Implementer must not self-serve as independent review. Cross-model review required before merge claim. Same-model findings (if any) labeled same-model.

## Cleanup

- Dogfood uses `kernel.sqlite` + `agent.sqlite` under durable root; ephemeral markers removed on terminal paths; no production data.

## Stop conditions hit

None (no payment, production switch, release publish, private tree, or dual-attempt blocker).
