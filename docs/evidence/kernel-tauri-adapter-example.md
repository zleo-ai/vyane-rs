# Tauri / Horus thin adapter example (non-binding sketch)

Public-safe sketch only. No Tauri crate dependency in the kernel.
Future shell commands map 1:1 onto versioned [`KernelCommand`] / [`KernelEvent`].

```text
UI / Horus
   │  invoke("kernel_submit" | "kernel_approve" | "kernel_drive_dogfood", …)
   ▼
thin adapter (Tauri command)  — authz principal bind here
   │
   ├─ DriveDogfood / dogfood path  → KernelStore (kernel.sqlite) + AgentStore
   │                                 durable multi-process authority
   │
   └─ Status / event subscribe     → LocalKernelAdapter projection rebuild
                                     (events process-local; facts from store)
   ▼
KernelEvent / KernelProjection  — UI may show display_hint but MUST use projection
```

**Honest residual:** `DecideApproval` / `DenyApproval` persist through KernelStore
only when the command carries a resolvable durable root and a matching pending
ask (plus grant binding for approve). Missing store or binding fails closed.
Do not treat an Approved/Denied event as authority without the store row.
Denied is the approval decision; it does not fail the receipt. Delivery-phase
update is best-effort and is not native resume.

## Command map (minimum)

| Shell action | `KernelCommandKind` |
|--------------|---------------------|
| Submit | `SubmitTask` |
| Status | `Status` / `GetProjection` |
| Approve | `DecideApproval` (`approval_granted: true`) |
| Deny | `DenyApproval` |
| Cancel | `CancelTask` |
| Read artifact | `ReadArtifact` |
| Read receipt | `ReadReceipt` |
| Dogfood e2e (dev) | `DriveDogfood` |

## Authority rules

1. Principal owner is bound by the adapter; payload cannot override owner.
2. `display_hint` is never used for branching on task state.
3. Client cache may drop; rebuild from durable store via Status/ReadReceipt
   **with `dogfood_root` (or a registered durable root after DriveDogfood)**.
   `LocalKernelAdapter` loads `kernel.sqlite` under that root — not memory alone.
4. Duplicate command_id handling is idempotent or fail-closed Conflict — never a second effect.
5. `DecideApproval` requires `approval_binding` (digest, revision, lease owner, generation).
   `DenyApproval` may omit the binding only when a pending KernelStore row already exists.

## Not in kernel

Provider model alias tables, UI page trees, private Python objects, paid provider calls.
