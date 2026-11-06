# C0 behavior contract

This file freezes an incremental Python-versus-Rust behavior contract.
The original five suites and their 44 cases stay the baseline in
`docs/parity/fixtures/v1/manifest.json`. Nothing in this contract edits
those fixtures, their dispositions, or the 53-capability implementation
counts.

Python expectations are authored pins (`python_expectation:
authored_contract_pin`). Replaying the corpus does not read a private
repository, credentials, or a network model. A pin is not evidence that
the public repository executed the private Python tree.

Every C0 case is replayed by
`crates/vyane-cli/tests/parity_c0.rs::c0_corpus_replays_offline`.
The input, normalization object, authored oracle, and Rust output live
in the suite fixture. The sibling manifest
`docs/parity/fixtures/v1/c0-manifest.json` locks each fixture's SHA-256.

## Dispositions

| JSON | Meaning |
| --- | --- |
| `aligned` | The authored normalized oracle and the Rust replay are equal. `blocker` is null. |
| `intentional_retain` | They differ, and Rust keeps the stronger or safer behavior. `blocker` starts with `RETAIN `. |
| `pending` | They differ, and the product decision is not frozen. `blocker` starts with `PENDING `. |

An alias can still be `aligned`. For example, Rust `rate_limited` is
pinned next to the Python label `rate_limit` inside one comparison
object. The raw tokens are not claimed to be identical.

## Normalization

Shared rules for every C0 suite:

- Newlines are compared as `\n`.
- Paths, endpoints, version text, timestamps, and transcript bodies are
  omitted from pinned output.
- Credential material is not part of any pin. A resolved target must
  not carry auth material.
- The oracle locator is exactly `{"fn":"authored_contract_pin","args":{}}`.

Suite-specific rules are the `normalization` object in each fixture.

## Baseline open differences

These 14 rows stay `open_difference` in the v1 fixtures. This table only
records the C0 disposition. It does not rewrite the fixtures.

| Case | C0 disposition | Why |
| --- | --- | --- |
| `automatic-routing.routed-defaults` | pending | The reference dispatch entry does not consume the Rust candidate shape. |
| `automatic-routing.explicit-effort` | pending | Explicit effort is accepted, but the selected tuple is one-sided. |
| `automatic-routing.explicit-model` | pending | Rust has no typed explicit-model override in that entry. |
| `automatic-routing.explicit-selector` | pending | No same-shaped selector-to-profile contract on the reference entry. |
| `automatic-routing.rendered-prompt` | pending | Both route rendered text; the selected tuple is not reference-equivalent. |
| `automatic-routing.full-chain-frontier-guard` | intentional retain | Rust fails closed across the whole failover chain. |
| `automatic-routing.ambiguous-direct-frontier` | intentional retain | Rust fails closed when a direct leg cannot be proved non-frontier. |
| `automatic-routing.frozen-replay` | pending | Frozen automatic-chain replay is not the Python prefix cache. |
| `failover.reason.policy-blocked` | pending | Rust has no `policy_blocked` kind. `Unsupported` is not that label. |
| `failover.reason.quota-exhausted` | pending | Rust folds quota into `RateLimited`. |
| `dispatch-trail.timeout` | pending | Trail shape still differs after a timeout. |
| `dispatch-trail.quota` | pending | The reference records `quota_exhausted`; Rust records `rate_limit`. |
| `dispatch-trail.auth` | pending | Both advance after authentication failure. Rust keeps `Auth`; the reference label is `execution_error`. This is not declared retained. |
| `dispatch-trail.policy-blocked` | pending | The reference policy stop is not a Rust kind. |

## `cli_error` (27)

Fixture: `docs/parity/fixtures/v1/c0-cli-error.json`.

Normalization: CRLF becomes LF, tracing lines are dropped, help
inventory keeps indent-2 command names, version text is unpinned, and
feedback JSON keeps serde_json map key order.
Feedback cases set `VYANE_DATA_DIR` under the worktree temp directory.
Storage may be created there. The pin does not require that directory
to stay empty, and it does not echo the path.

| Case | Disposition | Input and expectation |
| --- | --- | --- |
| `cli-error.kind.timeout` | aligned | `ErrorKind::Timeout` is `timeout`, failover eligible. |
| `cli-error.kind.rate-limited` | aligned | `rate_limited` aliases to `rate_limit`, failover eligible. |
| `cli-error.kind.spawn-failed` | aligned | `spawn_failed` aliases to `runtime_unavailable`, failover eligible. |
| `cli-error.kind.harness-failed` | aligned | `harness_failed` aliases to `runtime_crash`, failover eligible. |
| `cli-error.kind.other` | aligned | `other` aliases to `execution_error`, not failover eligible. |
| `cli-error.kind.transport` | pending | Rust `transport` versus authored `execution_error`. Both are failover eligible. |
| `cli-error.kind.protocol` | pending | Rust `protocol` versus authored `execution_error`. Both are failover eligible. |
| `cli-error.kind.auth` | pending | Rust `auth` versus authored `execution_error`. Both are failover eligible. |
| `cli-error.kind.unsupported` | pending | Rust `unsupported` versus authored `policy_blocked`. Neither fails over. |
| `cli-error.kind.cancelled` | intentional retain | Cancellation must not fail over. |
| `cli-error.kind.config` | intentional retain | Configuration failure must not fail over. |
| `cli-error.kind.not-found` | intentional retain | A missing entity is not a missing runtime binary. |
| `cli-error.kind.conflict` | intentional retain | Conflict must be reloaded, not failed over. |
| `cli-error.kind.io` | intentional retain | Local I/O must not fail over. |
| `cli-error.kind.indeterminate` | intentional retain | An indeterminate publication must not be retried. |
| `cli-error.unmapped.overloaded` | pending | No Rust kind. Python treats `overloaded` as failover eligible. |
| `cli-error.unmapped.policy-blocked` | pending | No Rust kind. Python retry policy maps it to config, not a retry. |
| `cli-error.diagnostic.invalid-input` | pending | Diagnostic token `invalid_input` has no Python twin. |
| `cli-error.diagnostic.config-invalid` | pending | Diagnostic token `config_invalid` has no Python twin. |
| `cli-error.diagnostic.budget-exceeded` | pending | Diagnostic token `budget_exceeded` has no Python twin. |
| `cli-error.feedback.missing-human` | pending | `vyane feedback rate` without arguments exits 2 with the fixed config line. Python uses `vyane feedback` and defaults the rating to 0. |
| `cli-error.feedback.missing-json` | pending | The same missing arguments in `--json` are one config error object. |
| `cli-error.feedback.rating-range` | pending | Rating `9` exits 2: the rating must be an integer from 1 to 5. |
| `cli-error.command.unknown` | aligned | An unknown command exits 2, class `usage`, stdout empty, stderr nonempty. The clap text is not pinned. |
| `cli-error.command.version` | pending | `--version` exits 0, program `vyane`, one line. Python exposes `vyane version`. The version string is not pinned. |
| `cli-error.command.session` | pending | Help names are `list`, `inspect`, `reset-native`. |
| `cli-error.command.workflow` | pending | Help names are `plan`, `run`, `submit`, `status`, `cancel`, `resume`, `replay`, `list`. There is no `tail`. |

## `selector` (13)

Fixture: `docs/parity/fixtures/v1/c0-selector.json`.

The replay calls `vyane_service::resolve_target_chain` and
`split_targets` on one neutral config embedded in the test. Providers
`relay` and `backup` have no credential-env field. Profiles are
`primary` (`relay/model-a`, failover `backup/model-b`), `auto`
(`relay/model-auto`), and `effort-high` (effort `high`). Projected
fields are provider, protocol, harness (`none`), model, transport, and
effort. Endpoints are not pinned. Declared failover order is kept.

| Case | Disposition | Input and expectation |
| --- | --- | --- |
| `selector.direct-model` | aligned | `relay/model-a` is one direct HTTP leg. |
| `selector.target-prefix` | aligned | `target:relay/model-a` is the same leg. |
| `selector.profile-chain` | aligned | `profile:primary` is relay then backup, declared order. |
| `selector.bare-profile` | aligned | Bare `primary` is the same chain. |
| `selector.bare-auto` | intentional retain | Bare `auto` resolves the profile named `auto`. This entry does not call the router. |
| `selector.profile-auto` | intentional retain | `profile:auto` resolves that profile and does not call the router. |
| `selector.profile-slash` | intentional retain | `profile:relay/model-z` looks up a profile of that name and is `not_found`. |
| `selector.direct-slash` | aligned | `relay/model-z` selects that provider and model. |
| `selector.empty-profile` | aligned | `profile:` is `selector_syntax`. |
| `selector.split-trim` | aligned | `" a, ,b "` splits to `a`, `b`. |
| `selector.split-empty` | aligned | A blank list is `selector_syntax`. |
| `selector.unknown-profile` | pending | Unknown profile `missing` is `not_found`. The authored Python pin says `config`. |
| `selector.profile-effort` | aligned | `effort-high` projects effort `high`. |

## `effort` (9)

Fixture: `docs/parity/fixtures/v1/c0-effort.json`.

`vyane_core::Effort` accepts only `low`, `medium`, `high`, and `xhigh`.
`parse_effort` stays crate-private. Collapse of `max` and `none` is
observed through public `route_task` with explicit tier `economy`,
provider `relay`, and task `ping`. Empty preference effort inherits
`effort_for_tier`: frontier `high`, mainline `medium`, economy `low`.

| Case | Disposition | Input and expectation |
| --- | --- | --- |
| `effort.serde-closed` | aligned | The four closed tokens round-trip. |
| `effort.serde-max` | pending | `max` fails deserialize. Python accepts it. |
| `effort.serde-none` | pending | `none` fails deserialize. Python accepts it. |
| `effort.route-max` | pending | Preference `max` becomes `medium` on tier `economy`. |
| `effort.route-none` | pending | Preference `none` becomes `medium` on tier `economy`. |
| `effort.route-high` | aligned | Preference `HIGH` stays `high` on tier `economy`. |
| `effort.route-xhigh` | aligned | Preference `xhigh` stays `xhigh` on tier `frontier`. |
| `effort.route-inherit` | aligned | Empty effort inherits low, medium, and high from the three tiers. |
| `effort.toml-max` | pending | Profile `effort = "max"` fails config load as `config`. |

## `usage` (8)

Fixture: `docs/parity/fixtures/v1/c0-usage.json`.

`estimate_configured_cost` returns `unknown` when the rate or the usage
is absent. It never substitutes 0 for an unknown price. A present rate
with zero tokens is `priced` at 0 micro-USD. Amounts are
`round(cost * 1_000_000)` integer micro-USD. Reasoning is a subset of
output and cached input is a subset of input. A subset larger than its
parent is clamped. A missing subset rate bills the parent count once.

| Case | Disposition | Input and expectation |
| --- | --- | --- |
| `usage.absent-rate` | aligned | No rate is `unknown`. |
| `usage.absent-usage` | aligned | No usage is `unknown`. |
| `usage.zero-tokens` | aligned | Known rate and zero tokens are priced 0. |
| `usage.parent-rates` | aligned | 1e6 input at 2.5 and 5e5 output at 10 are 7500000 micro-USD. |
| `usage.subset-rates` | pending | Rust bills subsets inside the parent counts (4590). The Python helper bills cache and reasoning as disjoint volume (6290). |
| `usage.missing-subset-rates` | aligned | Missing subset rates bill the parent counts once (3000). |
| `usage.clamp-subset` | pending | Cached 1000 against input 100 clamps to 10 micro-USD. The disjoint pin is 100. |
| `usage.saturating-add` | intentional retain | `u64::MAX + 1` stays saturated. The counter must not wrap. |

## `session` (9)

Fixture: `docs/parity/fixtures/v1/c0-session.json`.

Pins carry counts, owner, provider, and model. They omit timestamps,
transcript text, and native binding digests. The native-resume case
records public state tokens and the fail-closed policy. It does not
call private `validate_native_resume`. The kernel evidence is
`crates/vyane-kernel/tests/acceptance.rs`:
`harness_legacy_native_resume_is_rejected_even_for_read_only` and
`native_resume_rejects_when_any_admitted_fallback_is_a_harness`.

| Case | Disposition | Input and expectation |
| --- | --- | --- |
| `session.id-empty` | aligned | An empty id is `config`. |
| `session.id-nul` | aligned | An id containing NUL is `config`. |
| `session.id-space` | pending | A single space is accepted. |
| `session.id-exact-cap` | aligned | 1024 UTF-8 bytes are accepted. |
| `session.id-overlong` | pending | 1025 UTF-8 bytes are `config`. |
| `session.native-resume` | intentional retain | Absent falls through. Legacy and bound harness resume are `unsupported`. |
| `session.transitions` | pending | Public tokens are `reset`, `fork_fresh`, and `commit`. |
| `session.apply-update` | aligned | Two updates end at run count 2, transcript length 2, and the native id from the first update. |
| `session.chat-roles` | aligned | Constructors yield `system`, `user`, and `assistant`. |

## `workflow` (15)

Fixture: `docs/parity/fixtures/v1/c0-workflow.json`.

`WorkflowPlan::feature_support` reports schema v1 representability.
`supported` for worktree isolation means the plan can name it. It is
not a Windows execution-parity claim. JavaScript is parsed as workflow
TOML and is not executed. Graph cases use inline prompts only, so the
replay does not open prompt files. Display paths are not pinned.

| Case | Disposition | Input and expectation |
| --- | --- | --- |
| `workflow.feature.declarative-dag` | aligned | Declarative DAG is `supported`. |
| `workflow.feature.worktree` | aligned | Worktree isolation is `supported` as schema v1 only. |
| `workflow.feature.dynamic` | intentional retain | Dynamic control flow is unsupported. This DAG is not a Deno runner. |
| `workflow.feature.nested` | intentional retain | Nested workflow calls are unsupported. This DAG is not a Deno runner. |
| `workflow.feature.shared-budget` | pending | Shared budgets are unsupported in schema v1. |
| `workflow.parse.javascript` | intentional retain | JavaScript source is `parse_workflow` and is not executed. |
| `workflow.parse.ignored-script` | pending | A step `script` key is ignored. The prompt-and-target step still validates. |
| `workflow.graph.cycle` | aligned | Cycle text is `workflow dependency cycle detected involving: a, b`. |
| `workflow.graph.duplicate` | aligned | The only problem names the duplicate step id `one`. |
| `workflow.graph.both-prompts` | aligned | Step 1 cannot set both `prompt` and `prompt_file`. |
| `workflow.graph.blank-name` | aligned | A blank name reports `[workflow].name is required`. |
| `workflow.graph.both-targets` | aligned | Step `one` cannot set both `target` and `fan_out`. |
| `workflow.graph.topo` | aligned | `b` needs `a`, so the order is `a`, `b`. |
| `workflow.journal.reset` | intentional retain | `Success` stays `success`. Any other status becomes `pending`. This is not the Python changed-plan prefix cache. |
| `workflow.on-error` | aligned | Tokens are `abort` and `continue`. |

## Replay

From the repository root, with the MSVC linker ahead of any other
`link.exe`:

```sh
cargo test -p vyane-cli --test parity_manifest --locked
cargo test -p vyane-cli --test parity_c0 --locked
python3 .github/scripts/test-parity-report.py
```

`parity-report.py` validates the original 44 cases and then the C0
suites. It does not replace the baseline counts.
