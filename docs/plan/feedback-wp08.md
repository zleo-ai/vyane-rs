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

> **Superseded by review round 2, fix 3** (see below): the worker no longer
> leaves `running` behind on *any* setup/dispatch error — it now converts every
> such failure into a terminal `error` status. This item is retained only as
> history of the original shape.

---

# Review round 2 — applied fixes (independent reviewer REQUEST_CHANGES)

An independent review (GPT) issued `REQUEST_CHANGES` on WP-08 with one blocker
and four major/minor items. All were applied within the frozen surface (only
`crates/vyane-cli/**`). Summary of what changed and why.

## Fix 1 (BLOCKER) — process-identity validation before ANY signal

**Problem:** `task cancel` and orphan detection acted on a bare recorded pid.
Between the worker recording its pid and a later cancel/list, the OS can recycle
that pid onto an unrelated process; group-signalling it (`kill(-pgid, …)`) could
kill something we never launched, and orphan detection could mislabel a reused
pid as a live run.

**Fix:** a new `proc::verify_identity(pid, pgid, started_at) -> IdentityCheck`
validates **both**, before any signal:
- **(a) process group** — `getpgid(pid) == recorded pgid` (the worker is a group
  leader; a reused pid almost always lives in a different group).
- **(b) start time** — the process's actual start time, obtained via
  `ps -p <pid> -o etime=` (elapsed time; `now - etime = start`), must match the
  recorded `started_at` within **±10s** (`IDENTITY_START_TOLERANCE_SECS`). The
  worker stamps `started_at = Utc::now()` right after exec, so the recorded
  value and the kernel-reported start refer to the same instant; the tolerance
  covers only the fork→stamp gap and `ps`'s whole-second resolution. Critically
  this compares *start-time to start-time*, so it stays correct no matter how
  long the worker has been running — a reused pid necessarily started later, by
  far more than the tolerance.
- Chose **`etime`** over `lstart`: `etime` is a fixed, locale/timezone-independent
  duration (`[[DD-]HH:]MM:SS`), so parsing is robust; `lstart` is a
  locale-dependent absolute timestamp. Documented on `process_start_time`.

`task cancel` now: `Match` → signal the verified group (`status.pgid`);
`Mismatch` → `process identity mismatch (…; pid likely reused); refusing to
signal`, exit 1; `Dead` → report the worker is gone (died), exit 1 — no signal.
Orphan detection (`interpret_state`) uses the same probe: a still-`running`
status whose pid is dead **or reused** now reads as `died`.

**Tested** (`proc.rs`, real spawned processes): `verify_identity_matches_a_real
_worker_like_child` (live child with matching pgid+start → Match; wrong pgid →
Mismatch; start time an hour off → Mismatch), `process_start_time_of_real_child
_is_recent`, `verify_identity_reports_dead_for_absent_pid`, `parse_etime_*`.
`store.rs`: `interpret_state_marks_reused_pid_running_as_died`.

## Fix 2 (major) — arm the SIGTERM→CancellationToken handler BEFORE `running`

**Problem:** the worker published `state:"running"` and only *then* installed
the SIGTERM handler, so a cancel racing that window could tear the process down
before the handler existed.

**Fix:** `run_worker` now installs the cancellation handler (`worker_cancellation
_token`) **before** calling `worker_body`, and it is `worker_body` that writes
the first `running` status. Since `task cancel` only signals a task whose status
already reads `running`, and `running` cannot appear until after the handler
exists, any deliverable signal is guaranteed to be caught. The invariant is
encoded in code structure (handler on the line above the body call) and
documented at length on `run_worker`. (A deterministic race test is not feasible;
the ordering is enforced structurally, per the fix-list guidance.)

## Fix 3 (major) — validate full TaskSpec in the parent; never leave `running`

**Problem:** `--label bad` (no `=`) was only rejected inside the worker, after
the parent had already created a task dir. And a worker setup error left the
status `running`.

**Fix, parent side:** `run_dispatch` now validates the **full TaskSpec**
(including `--label` key=value parsing, via `task_base`) in the same up-front
phase as config resolution — *before* the `--detach` branch. Invalid input exits
2 with no task dir, exactly like a config error.

**Fix, worker side:** the worker is split into `run_worker` (thin shell) +
`worker_body` (real work). `worker_body` returns `Err` for *any* setup/dispatch
failure (corrupt `job.json`, config failure, spec/runtime assembly, kernel
dispatch error); `run_worker` converts that into a terminal `error` status
(keyed by the worker's run id, with the message). No path exits leaving
`running`. This supersedes feedback item 3 above.

**Tested** (`detach_acceptance.rs`): `detach_bad_label_exits_two_and_creates_no
_task_dir` (`--label bad --detach` → exit 2, no task dir), `worker_setup_failure
_finalizes_error_not_running` (crafted corrupt `job.json` → worker writes
`state:"error"`, exit 1, `task status` shows error).

## Fix 4 (major) — a `job.json`-without-`status.json` dir must be VISIBLE

**Problem:** a task dir the parent created but whose worker never wrote status
(a failed spawn) was silently skipped by `task list` and reported as a bare "no
such run" by `task status`.

**Fix:** a new synthetic read-side state `TaskState::Stale` (never persisted).
`list_tasks` now renders such a dir as `stale`, with `started_at` from the
`job.json` mtime (`TaskPaths::job_mtime`). `task status <id>` on it exits 1 with
`stale — worker never wrote status (spawn may have failed); see …task.log`. A
dir with *neither* file is still skipped as transient scaffolding. (`Stale` is
distinct from the Fix-3 `error` case: `error` = the worker ran and its setup
failed; `stale` = the worker never came up at all.)

**Tested:** `job_without_status_shows_stale_and_explains` (acceptance),
`list_tasks_surfaces_job_without_status_as_stale` + `stale_state_serializes_and
_names` (unit).

## Fix 5 (minor) — group-kill proof: a child in the group dies too

**Problem:** the cancel acceptance test only asserted the direct worker pid died,
not the whole group.

**Fix:** two-pronged.
- **Unit** (`proc.rs`, `signal_group_reaps_a_child_in_the_group`): spawn a real
  group-leader shell (`setsid`, `pgid == pid`) that forks a `sleep` grandchild
  in the same group and prints its pid; `signal_group(stored_pgid, SIGKILL)`;
  assert the **grandchild** (not just the direct child) is dead and the group is
  empty. This is the faithful group-kill proof.
- **Acceptance** (`cancel_finalizes_cancelled_and_kills_group`): additionally
  asserts the whole recorded process **group** is empty after cancel
  (`kill(-pgid, 0)` → ESRCH) using the **stored `pgid`** — a group-level check,
  not just the worker pid. (An `openai_chat` worker forks no children, so the
  independent grandchild-death proof lives in the unit test above; the
  acceptance test proves group-level teardown via the stored pgid.)

## Test counts after round 2

- `vyane-cli` unit tests: 24 (was fewer) — added identity, etime parsing,
  group-kill-reaps-child, reused-pid orphan, stale-row tests.
- `detach_acceptance.rs`: 8 (was 5) — added bad-label exit-2, worker-setup-error,
  job-without-status stale; strengthened cancel to a group-level assertion.
- Full workspace `cargo test`: green; `cargo fmt --all` + `cargo clippy
  --workspace --all-targets -- -D warnings`: green.
