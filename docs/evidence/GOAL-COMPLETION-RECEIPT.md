# Goal CompletionReceipt — decision-grade Rust kernel program

## Identity

| Field | Value |
|-------|--------|
| Milestone / goal | Decision-grade Rust kernel (Process dogfood path) |
| User-visible capability | One restart-safe path to a truth-verified CompletionReceipt + Tauri-ready boundary + migration recommendation B |
| runtime | grok-build |
| harness | grok-build |
| model | grok-4.5 |
| reasoning_effort | high |
| Prepared baseline | `10ebe700cef3416459beebfb7ed07d7e9b866de7` |
| Implementation HEAD | `78ddfa5` (branch tip; see PR #169) |

## Ready

1. **Phase 0** decision path documented (`docs/plan/DECISION-GRADE-KERNEL.md`, roadmap pointer).
2. **Phase 1** public typed contract: TaskCase, RouteConfig, ReceiptAttempt, GateResult, CompletionReceipt with schema versioning, owner isolation, revision CAS, no complete without truth probe (`vyane-core::receipt`).
3. **Phase 2** Process-lane dogfood composition using AgentStore claim/lease + effect log + truth probe fail-then-pass + immutable artifact + receipt (`vyane-service::dogfood`).
4. **Phase 3** hostile coverage: crash after effect, crash after artifact, cancel race, duplicate command, newer schema, partial tree cleanup visibility.
5. **Phase 4** transport-neutral local kernel boundary + LocalKernelAdapter tests (`vyane-service::kernel_boundary`).
6. **Phase 5–6** migration report with recommendation **B** (gradual module-by-module); eval fields on receipt; parity LED-05 → partial honestly.
7. No release/tag/crate publish; no production switch.

## Partial / remaining

- Full daemon Process e2e still relies on existing `daemon_agent_acceptance` for live harness spawn; dogfood library path is the product composition under test here.
- Approval **resume** (not only ask-stop) remains open (GOV-05 residual).
- Durable multi-process receipt store (beyond MemoryReceiptLedger) not shipped.
- Python matched performance numbers largely **incomparable** under clean-room.
- Live pause/resume (CON-04) and automatic payload replay (CON-05) intentionally not productized.

## Recommended migration path

**B — Gradual module-by-module replacement:** Rust kernel owns execution fencing, ownership, completion truth; adapter plane retains fast-changing channels/UI/providers until each module has hermetic public evidence.

## Reviewer

- Same-model note: primary implementation by grok-4.5 / grok-build; independent cross-model review required before merge of the milestone PR (label same-model if same model re-reads).

## Cleanup

- Dogfood declares lifecycle scopes; ephemeral markers removed on terminal paths; no production data touched.

## Stop conditions hit

- None of the hard stop conditions (payment, production, release publish, etc.).

## Same-model review dispositions (PR #169)

Reviewer: grok-4.5 / grok-build (same-model, independent explore agent).

| Finding | Disposition |
|---------|-------------|
| Lifecycle orphan dead code | Fixed: prepare_cleanup_state before terminal + inventory after |
| LED-05 plan table drift | Fixed: partial in DECISION-GRADE-KERNEL.md |
| UnknownFieldForbidden dead | Fixed: deny_unknown_fields + test; removed unused error variant |
| Soft crash recovery assert | Fixed: require Completed on designed recover-to-success path |
| Criterion 10 typed fields | Partial: remaining_risks + validation_summary on dogfood complete; human GOAL-COMPLETION-RECEIPT remains |
| Synthetic effect residual | Accepted residual (documented) |
| display_hint test weak | Accepted nit for follow-up |

Disposition after fixes: **APPROVE** pending required CI (same-model label retained; cross-model still preferred).
