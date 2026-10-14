# Explicit targets and dispatch trails

The `target_resolution` and `dispatch_trail` suites add a supplementary
`execution-behavior-v1` capture to the existing parity manifest. They do not
refresh the older classifier, taxonomy or automatic-routing reference outputs,
and do not change the 53-capability implementation counts.

Run the existing entry point from the repository root:

```sh
python3 .github/scripts/parity-report.py --format markdown
```

By default this runs the Rust `parity_manifest` integration tests before
validating fixture digests and reporting matches and open differences.
`--skip-rust-test` checks stored data only; it does not recompute behavior.

The offline report also pins the 53 canonical capability identifiers, separately
from their status counts. Replacing a row with an unknown identifier or a
noncanonical number is an error even when all counts stay the same. Intentional
coverage changes require an explicit baseline update; this check does not change
implementation status or establish additional parity.

Exercise this report boundary without compiling Rust:

```sh
python3 .github/scripts/test-parity-report.py
```

## What runs

Eight target cases exercise `ResolvedConfig::resolve_failover_with`: direct
Chat/Responses/Messages coordinates, provider identity distinct from a CLI
harness, provider-scoped model defaults, a profile protocol override, and an
ordered chain with a different model on every leg. Credential lookup is an
injected function that panics if called. Synthetic endpoints are never contacted.

Eleven dispatch cases resolve their configured chains, then run the real
`Dispatcher` against scripted harness outcomes and a real temporary JSONL ledger.
They compare every attempt's four target coordinates, position, status, error
category and `failed_over`, along with final status, output and the actual model
received by each harness invocation. The final target must equal the last
attempt, and the single persisted record must equal the returned record.

Cases include primary success, one or two failed legs before success, mixed
failure categories, an exhausted chain, a single failed target, timeout, rate
limit, quota exhaustion, authentication failure and a blocked outcome. The
adapter returns typed failures immediately: these cases do not measure timeout
clocks, HTTP error parsing, process cleanup or sandbox enforcement.

The reference capture executed its actual explicit-target resolver and dispatch
entry with synthetic configuration, supplied candidate order and fake adapter
results. Its real history record supplied the attempt trail; an independent
invocation list checked which models were actually called. Only allowlisted
synthetic fields were retained. Source provenance and the capture harness remain
in the controlled maintainer record, outside this public repository. Public
readers can recompute Rust against the pinned capture, not rerun that private
implementation from this repository.

## Normalization and remaining differences

Provider, protocol and model are preserved. Reference `direct-chat` becomes
`none`, and both outputs normalize `codex-cli` to `codex`. Fixture inputs use
the canonical built-in `codex-cli`, and the resolved harness kind is asserted
before execution so an unknown harness cannot pass through the scripted factory.
Reference provider defaults are expressed through its per-provider configuration;
this does not compare implicit catalogs.
Generated ids, timestamps, durations, paths and unrelated runtime metadata are
excluded. Reference `failed_over` is derived from whether another recorded
attempt follows; its source failure trigger remains pinned and is checked
separately. Rust `Transport` maps to `execution_error`, and `RateLimited` to
`rate_limit`; other categories retain their names.

The eight target cases and seven dispatch cases are normalized matches. Four
dispatch differences remain explicit:

| Case | Reference capture | Rust |
| --- | --- | --- |
| Typed timeout | Stops after the timed-out adapter | Advances to the next eligible target |
| Quota exhaustion | Records `quota_exhausted` | Records the combined `RateLimited` category |
| Authentication failure | Advances, recording `execution_error` | Advances, retaining `Auth` |
| Blocked outcome | Preserves `blocked` / `policy_blocked` | Preserves `error` / `Unsupported` and stops |

Both matched and differing cases must recompute their recorded Rust output.
Differences are evidence for a later compatibility decision, not instructions to
weaken a fail-closed rule or silently change production policy.

Scope is explicit configuration and fresh, sessionless attempts with supplied
candidate order and no same-target retries. Disabled-target admission, implicit
provider inference, live protocol/harness behavior, session recovery, candidate
ranking, cancellation and retry timing still require separate acceptance.
An entire attempt sequence within this scope is not whole-system parity.
