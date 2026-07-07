# Feedback — WP-08 (detached background runs)

WP-08 is fully implemented within the frozen surface; nothing outside
`crates/vyane-cli/**` was touched. This note records one design seam the CLI
layer could not close on its own, plus a couple of smaller observations, for a
future kernel revision to consider. **None of these blocked WP-08** — the
implementation works around them and all acceptance tests pass.

## 1. The detached task id is not the ledger `run_id` (kernel owns run_id)

The spec has the parent "allocate run id (uuidv7)" for the job/task directory,
and `status.json` carries both `run_id` and `ledger_run_id`. In this
implementation those are **two different UUIDv7 values**:

- **task id** = the id the parent allocates; it names `tasks/<id>/` and is the
  `status.json` `run_id`. It is what `vyane task list/status/cancel` address.
- **ledger `run_id`** = a *fresh* UUIDv7 the kernel mints itself, at
  `crates/vyane-kernel/src/dispatch.rs` (`run_id: uuid::Uuid::now_v7()...`),
  when it assembles the `RunRecord`. The worker records it into
  `status.ledger_run_id` so the task → ledger link is explicit.

The kernel's `Dispatcher::dispatch(&TaskSpec, chain, cancel)` has no parameter
for a caller-supplied run id, and `RunRecord` is a frozen type, so the CLI
cannot make the two ids equal without changing a frozen crate. The status
file's separate `ledger_run_id` field (which the spec already anticipates)
bridges the gap, and the acceptance tests assert the link via that field rather
than by id equality.

**If a future revision wants task id == ledger run_id**, the minimal change is
an *optional* caller-supplied run id on the dispatch entry point, e.g.:

```rust
// vyane-kernel
pub struct DispatchOptions { pub run_id: Option<String>, /* … */ }
pub async fn dispatch_with(&self, task, chain, cancel, opts) -> Result<…>;
```

with `dispatch` delegating to `dispatch_with(.., DispatchOptions::default())`
so existing callers are unaffected. The worker would then pass the frozen job
id through, and `status.run_id == status.ledger_run_id == RunRecord.run_id`.
This is a clean additive change; it was simply out of scope for a CLI-only WP.

## 2. `Sandbox` is `#[non_exhaustive]` but currently has three variants

`SandboxSpec` in `crates/vyane-cli/src/task/store.rs` is a local serializable
mirror of `vyane_core::Sandbox` so the job spec stays self-owned (the core enum
is `#[non_exhaustive]` and could gain variants). The `From<Sandbox>` impl today
compiles as an exhaustive match over the three known variants; if `Sandbox`
gains a variant, that impl will fail to compile in the CLI and flag the mapping
to update — a deliberate compile-time reminder rather than a silent default.
No action needed now; noting it so the coupling is visible.

## 3. Worker `dispatch()` error path leaves status `running`

The worker propagates a `dispatch()` `Err` with `?`. Per the kernel contract
that arm is reserved for an **empty chain** — impossible here because the parent
resolved a non-empty chain before spawning and the worker re-resolves the same
selector. If it somehow occurred, the status would stay `running` and read-side
orphan detection would surface it as `died`, which matches the spec's stated
behaviour for a worker that dies without finalizing ("the file simply stays
running — see orphan detection"). Acceptable as-is; documented for completeness.
