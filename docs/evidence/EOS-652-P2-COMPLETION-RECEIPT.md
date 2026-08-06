# CompletionReceipt — EOS-652 residual P2

## Identity

| Field | Value |
|-------|--------|
| Card | EOS-652 residual P2 (independent-verifier completion, progress lease fence, future-ts lease safety) |
| runtime / harness | grok-build |
| model | grok-4.5 |
| Branch | `grok/EOS-652-verifier-goal` |
| Worktree | `/home/maple/AIOS/worktrees/vyane-rs/eos652-verifier-goal` |
| Base (origin/main) | `915905a14f4f7cb810fad90fd87aec49f91f6b36` |
| Head (implementation) | `6c217a2b985b7a27c3a42201e9a0d6ef03a0c573` |
| Branch tip | `89bdf2936f9e8f6c2f82905b993bacc70728e2fe` (includes CLI lifecycle + receipt pin commits) |
| Push / PR | **none** (bounded local commit only) |

## Audit (main baseline)

Full matrix: session scratch `eos652-audit.md` (also under implementer scratch).

| Residual P2 item | Baseline status | Disposition |
|------------------|-----------------|-------------|
| (a) `completed` ↔ independent verifier | **missing** (partial infra: artifacts + pursuit path present; bare `satisfy_criterion` still unlocked `done`) | Implemented: `done` / Achieved require durable `CriterionStatus::Satisfied` artifacts or waiver |
| (b) `progress` lease fence + status guard | **missing** | Implemented: optional `worker_id`, `ensure_lease_holder`, `InProgress`-only |
| (c) future-ts × monotonic clamp false-kills lease | **missing** | Implemented: mutations pass `lease_at=occurred_at` separate from `effective_at`; lease ops use `lease_at` |

Already-present P1 left intact: claim/generation fencing, owner isolation, pause/terminal lease release, criteria waiver atomicity (waiver still does not forge `satisfied_at`).

## Files touched

- `crates/vyane-goal/src/sqlite.rs` — progress fence/status, verifier-backed completion, dual-time lease evaluation, `verifier_satisfied_indices`
- `crates/vyane-goal/src/store.rs` — `progress` signature + docs; `done` docs
- `crates/vyane-goal/src/pursuit.rs` — wall-clock store `at` for lease-safe checkpoint/verification writes
- `crates/vyane-goal/src/lib.rs` — module docs
- `crates/vyane-cli/src/cli.rs`, `crates/vyane-cli/src/goal.rs` — optional `--worker` on `goal progress`
- `crates/vyane-cli/tests/goal_acceptance.rs` — lifecycle public path uses verify→done (durable artifacts)
- `crates/vyane-goal/tests/eos652_p2.rs` — residual P2 regressions (new)
- `crates/vyane-goal/tests/claim_lease.rs`, `store_contract.rs`, `continuity_projection.rs`, `takeover_approval.rs` — call-site + completion-path updates
- `docs/evidence/EOS-652-P2-COMPLETION-RECEIPT.md` — this receipt

## Baseline red evidence

Command (signature stub only; fence/completion/future-ts behavior still baseline-missing):

```text
cargo test -p vyane-goal --test eos652_p2
```

Observed failures (4 red / 2 green on positive waiver+verifier paths that already worked under partial semantics):

- `done_rejects_bare_self_report_without_independent_verifier_results` — bare satisfy still completed
- `progress_is_status_guarded_against_queued_and_terminal` — progress on queued succeeded
- `progress_is_lease_fenced_for_non_holder_and_anonymous` — non-holder progress succeeded
- `future_progress_timestamp_does_not_false_kill_active_lease` — reclaim after future progress succeeded (false expiry)

Log: implementer scratch `baseline-red.log`.

## Final verification

Environment note: local umask `002` makes default tempdirs group-writable; GoalStore rejects that. Tests run with `umask 077` and/or fixture `chmod 700`.

| Gate | Result |
|------|--------|
| `cargo test -p vyane-goal --test eos652_p2` | **6 passed** |
| `cargo test -p vyane-goal --test claim_lease --test store_contract --test pursuit` | **15 + 26 + 25 passed** |
| `cargo test -p vyane-goal` | **all green** (lib + integration) |
| `cargo test -p vyane-cli --test goal_acceptance` | **25 passed** (lifecycle uses verify→done durable artifacts) |
| CLI goal-related unit filters | **green** |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets -- -D warnings` | exit 0 |

Logs: implementer scratch `directed-green.log`, `related-green.log`, `fmt-clippy-tests.log`, `cli-goal-acceptance-green.log`.

### Skeptic gap closure
- CLI `lifecycle_round_trip_has_stable_json_and_persisted_acceptance` previously bare-satisfied then `done` (exit 2 under P2 gate). Updated to machine-checkable criteria + `goal verify` (durable artifact) then `done`; full `goal_acceptance` re-run green.

## Behavior summary

1. **Completion**: criteria without durable independent verification (`goal_verifications` with `Satisfied` for that index) block `done` / Achieved unless explicitly waived; waiver audit event retained; `satisfied_at` not forged by waiver.
2. **Progress**: only `in_progress`; active lease requires holder `worker_id`; rejects queued/terminal appends.
3. **Lease time**: event monotonicity still uses `effective_at = max(occurred_at, updated_at)`; lease activity / renew / reclaim / fences use unclamped `occurred_at` (`lease_at`).

## Residual risks

- Caller-supplied future `at` on **lease-sensitive** APIs (reclaim/renew) remains trusted wall-clock input; residual fix addresses clamp-induced false expiry, not full clock-authority security.
- Manual criteria still need either a durable Satisfied verification artifact (operator/CLI path) or `--waive`; bare `satisfy` alone no longer completes.
- `record_verification` does not auto-write `satisfied_at` (CLI verify still chains satisfy); completion gates on artifacts, not `satisfied_at`.
- WAL sidecar umask (card P2, out of scope) unchanged.

## Independent review suggestions

1. Mutation review of `done` / Achieved dual gate: verify waiver listing includes all non-verified indices and does not clear bare `satisfied_at`.
2. Confirm progress CLI `--worker` docs/help and any external callers of `GoalStore::progress` after the signature change.
3. Adversarial: future-dated `renew_lease` / `reclaim` sequences vs residual (c); ensure no regression of intentional expired-window anonymous write.
4. Cross-model review preferred before any PR (same-model implementer).
