# Kernel Integration Pilot — durable multi-process store, approval resume, hermetic harness

**Status:** active product program (G0–G6)  
**Baseline HEAD:** `a3e24ba6a3195b482fccce9ca4fef527623fa222` (main = origin/main; post #169/#170)  
**Runtime identity:** runtime=`grok-build`, harness=`grok-build`, model=`grok-4.5`, effort=`high`, role=`Developer`  
**Non-goals:** WP-179–465 formatter train; file-by-file Python port; private data/creds; crates.io/tag/release; production switch

## 1. Why this pilot

PR #169/#170 delivered a **single-process** decision-grade Process dogfood path with
filesystem JSON durability (`effects.json`, `receipts/`, `lease-snapshot.json`).
That is restart-safe within one host process lifecycle, but it is **not** a
transactional multi-process authority suitable for a future Horus/Tauri local shell.

This program advances dogfood into a **versioned, restart-safe Rust kernel service pilot**:
SQLite-backed identities, approval resume product path, hermetic CLI-lifecycle harness
test program, and a complete local command/event/projection boundary.

## 2. G0 state map (baseline at `a3e24ba`)

| Fact | Authority today | Multi-process safe? | Target under this pilot |
|------|-----------------|---------------------|-------------------------|
| AgentRun / claim / lease generation | `agent.sqlite` (`SqliteAgentStore`) | Yes (WAL + lock) | Keep; reuse |
| CompletionReceipt | `MemoryReceiptLedger` + optional JSON dir | No (process-local + last-writer JSON) | `kernel.sqlite` receipts CAS |
| External effect identity | `effects.json` + in-process Mutex | No | `kernel.sqlite` effects unique |
| Lease snapshot for reopen | `lease-snapshot.json` | No | `kernel.sqlite` lease_fence |
| Approval grant | in-memory `approval_granted: bool` | No | durable approval decision + FSM |
| Artifact bytes | workdir files | Content-addressed files OK | metadata in kernel.sqlite; bytes on disk |
| Kernel boundary events | in-process VecDeque | No (local adapter only) | durable facts authoritative; events rebuildable |
| Delivery phase (Running/Ask/…) | implicit flags | No | explicit `delivery_phase` column |

### Memory-only (acceptable for pilot)

- Local event queue in `LocalKernelAdapter` (projection rebuild from durable store).
- In-process cancel flag before durable cancel_tree.
- Hermetic harness child process lifetime (OS truth, not SQLite).

### JSON files (must not remain multi-process authority)

- `effects.json`, `receipts/**.json`, `lease-snapshot.json` are retired as write authority
  once `kernel.sqlite` is online. May remain as debug export only if needed — not the CAS source.

## 3. Fail modes and truth probes (written before fix)

| ID | Fail mode | Probe (baseline fail → fix pass) |
|----|-----------|----------------------------------|
| TP-MP-01 | Two processes steal same effect identity | concurrent apply_effect; second fails closed or idempotent same digest |
| TP-MP-02 | Stale generation / wrong owner advances receipt | CAS fence returns typed error; state unchanged |
| TP-RE-01 | Reopen re-executes effect | after effect + crash, reopen applies once only |
| TP-AP-01 | deny then grant resumes | grant after deny rejected |
| TP-AP-02 | grant without binding (wrong revision/digest) | rejected |
| TP-AP-03 | grant then crash before effect | resume once; effect count = 1 |
| TP-CR-01 | crash after effect before receipt | recover completes without second effect |
| TP-CR-02 | crash after receipt before client response | reopen shows terminal receipt; no re-effect |
| TP-SCHEMA-01 | unknown schema version / unknown required JSON | fail closed |
| TP-COST-01 | unknown cost never written as 0 | receipt attempt cost fields stay null/absent |

## 4. Architecture boundary (unchanged axes)

**Rust kernel owns:** task/run identity, owner + lease, generation/revision CAS,
durable approval, effect dedupe, immutable artifact metadata, truth-gated
CompletionReceipt, versioned command/event/projection, crash recovery.

**Adapter plane owns:** provider API, model alias, harness argv differences, UI copy,
Horus/Tauri chrome, private deploy config, Python compatibility adapter.

Stop condition: if a change requires the kernel to understand vendor model names,
UI page trees, or private Python objects — halt and re-check boundary.

## 5. Milestones

| ID | Capability |
|----|------------|
| **G0** | This map + fail modes + plan (no capability headline inflation) |
| **G1** | SQLite `KernelStore` (receipt/approval/effect/artifact/revision/lease fence/schema) |
| **G2** | Approval FSM product path bound into receipt gates |
| **G3** | Hermetic harness lifecycle test executable (not formal vendor integration) |
| **G4** | Versioned commands/events/projections + authz-before-store |
| **G5** | Hostile matrix (lease race, crash windows, cancel/complete, digest mismatch, …) |
| **G6** | Docs, parity honesty, A\|B\|C\|D from evidence, Goal CompletionReceipt, CI + independent review |

## 6. Composition (reuse, no second runtime)

```
LocalKernelAdapter / future Tauri thin adapter
        │ versioned command/event
        ▼
DogfoodPath / ProcessLaneAutonomousDelivery
        │
        ├── SqliteAgentStore (agent.sqlite) — AgentRun claim/lease
        ├── KernelStore (kernel.sqlite) — receipt/effect/approval/artifact/phase/lease fence
        └── OS child / hermetic harness protocol binary — external effect lifecycle
```

## 7. Parity honesty

Do **not** change the 53-item headline counts unless matrix rules and hermetic
evidence justify a status move. LED-05 / GOV-05 / CON-06 may gain residual notes
as `partial` improvements only.

## 8. Stop conditions

Same as `DECISION-GRADE-KERNEL.md` §6 plus: overlapping writer ownership;
truth probe cannot fail on intentional broken baseline; paid provider required.
