# Changelog

User-visible changes in the public repository. Versions follow
[VERSIONING.md](VERSIONING.md) and the [release policy](RELEASING.md).
A git tag names a source snapshot. It is not a crates.io publication.
This is a curated history, not a complete reconstruction of all development.

## Unreleased

### Added

- Read one saved session's redacted metadata through `GET /v1/sessions/:id`
  or MCP `vyane_session_inspect`. Inspection leaves legacy files and permissions
  unchanged and returns not-found for valid missing long IDs. Custom session
  stores opt in through `SessionStore::inspect_snapshot`; execution loads keep
  their existing migration behavior. Base and CLI MCP tool counts are eight and
  eleven respectively.

- `vyane broadcast --compare` adds a local lexical comparison of successful
  text results: term counts, vocabulary overlap, and measured speed rankings.
  With `--json`, the opt-in result is `{ results, comparison }`; ordinary
  broadcast output and exit codes remain compatible. No additional model call
  is made. Comparison accepts at most 64 targets.

### Fixed

- A workflow variable key that contains a control character is rejected.
  The error names the position, not the key. A key with an internal space
  stays valid, and a value may still contain a newline.
- A goal observation whose goal id contains a control character is rejected
  before it is recorded. The receipt omits the id. An id with an internal
  space still reaches lookup.

## 0.1.0 - 2026-10-23

Source snapshot `v0.1.0`. The tag is created on the `main` commit that
contains this section. It does not upload packages.

### Added

- Dispatch stores a run's USD cost when `[cost.models]` gives that model an
  explicit rate and the run has usage. Input and output rates are required
  USD amounts per 1,000,000 tokens; optional reasoning and cache-read rates,
  when set, bill those subsets at the special rate instead of the ordinary
  output or input rate. Subsets are not extra tokens. A missing rate, an
  unlisted model, or missing usage leaves `cost_usd` unset. Built-in prices
  are not used, and an unknown price is never stored as zero. Non-finite,
  negative, or unknown rate fields are rejected when the config is loaded
  ([#227](https://github.com/zleo-ai/vyane-rs/pull/227)).

- Tests cover Codex usage JSON shapes the harness parser already reads:
  `token_count.info.last_token_usage`, `token_count.info.usage`, and a
  top-level `usage` object on a non-turn event. `total_token_usage` still
  takes precedence over `last_token_usage`. Reported USD and event
  `duration_ms` stay unparsed, and outcome duration remains the measured run
  time. Parsing behavior is unchanged
  ([#228](https://github.com/zleo-ai/vyane-rs/pull/228)).

- History statistics summarize saved USD costs across CLI, REST and MCP,
  with explicit unknown/invalid coverage and overflow instead of treating
  missing prices as free runs. No price lookup or ledger rewrite occurs.
- Add saved token-usage statistics to CLI, REST and MCP history stats, with
  per-field known/missing counts, exact partial totals and explicit overflow.

- `vyane check --json` exposes bounded static configuration diagnostics for
  scripts, with redacted reports and fixed JSON errors. It never probes a
  provider or starts a harness; the existing text check stays unchanged.

- MCP `vyane_workflow_plan` compiles caller-supplied workflow sources into the
  existing static plan schema without filesystem lookup or execution. The
  bounded response includes supplied prompt text; base and CLI MCP tool counts
  are now seven and ten respectively.

- `vyane workflow plan FILE [--json]` inspects a static workflow before
  execution. Human output lists dependencies and requested capabilities;
  JSON exports the existing full plan payload, including prompt text
  ([#214](https://github.com/zleo-ai/vyane-rs/pull/214)).
- Local history filters for session and labels, measured whole-run duration
  statistics, and durations grouped by final provider
  ([#208](https://github.com/zleo-ai/vyane-rs/pull/208),
  [#209](https://github.com/zleo-ai/vyane-rs/pull/209),
  [#210](https://github.com/zleo-ai/vyane-rs/pull/210)).
- A source-build quickstart, seven README translations, a shared translation
  policy, security-reporting guidance and a documented version/release checklist.

### Changed

- Lead all eight READMEs with user scenarios, a reproducible local CLI demo and
  a shorter first-run path; retain full setup and migration detail in
  [QUICKSTART.md](QUICKSTART.md) and [PROJECT-STATUS.md](PROJECT-STATUS.md).

- Move detailed CLI, REST, MCP and workflow examples from the README into
  [CLI-GUIDE.md](CLI-GUIDE.md). The README now distinguishes source capabilities,
  release availability and migration limits.
- Raise the `async-trait` minimum to `0.1.92` to avoid generated redundant
  `#[must_use]` warnings on newer Rust toolchains
  ([#197](https://github.com/zleo-ai/vyane-rs/pull/197)).

### Fixed

- Malformed `secrets.env` lines no longer include the line text in the
  config error. The message names the file, the line number, and whether the
  line lacks `=` or has an empty key. A normal `KEY=VALUE` line, including a
  value that contains `=`, still loads
  ([#280](https://github.com/zleo-ai/vyane-rs/pull/280)).
- Empty, whitespace-only, oversized, or control-character identifiers and
  paths fail at the checks that already rejected a nearby invalid form.
  This covers session ids, owners, event ids, workflow names and targets,
  route values, source and workdir paths, evidence URIs, hosted-search
  domains, and permission patterns. Text with an internal space stays valid.
  The error names the field and does not echo the rejected value.
- MCP dispatch and broadcast reject oversized task, system, workdir, target,
  and route inputs before the service runs, using the same per-field limits
  as the workflow plan fields they stand in for. A value at the limit still
  passes. Each broadcast selector is also capped at the workflow target
  length, in addition to the existing list bounds. Session ids and free-form
  labels are unchanged
  ([#226](https://github.com/zleo-ai/vyane-rs/pull/226)).

- Skip and count JSONL run-record lines containing invalid UTF-8 instead of
  silently replacing bad bytes inside JSON strings and accepting altered data.
  Valid Unicode and neighboring complete records remain readable; reads do not
  rewrite the ledger.

- Bound repeated workflow prompt-file expansion before materialization and
  legacy hashing, and reject oversized source metadata before diagnostic
  expansion, using the existing plan safety budgets.

- Include `vyane-quota` in the workspace publication order so all 18 crates
  pass the membership and dependency-order checks before packaging; add offline
  preflight regression coverage with synthetic archives
  ([#215](https://github.com/zleo-ai/vyane-rs/pull/215)).
- Anthropic input-usage normalization includes ordinary, cache-creation and
  cache-read input. Cache-read subcounts include reads only; cache creation still
  uses the ordinary input rate for estimates. Specialized cache/reasoning rates
  deduct their subcounts from totals before billing, with anomalous subcounts
  capped to totals to avoid double charging
  ([#194](https://github.com/zleo-ai/vyane-rs/pull/194)).
- Security routing recognizes inflections of `authenticate` and `authorize`
  while retaining word boundaries to avoid unrelated-word matches
  ([#195](https://github.com/zleo-ai/vyane-rs/pull/195)).
- Routing diagnostics and preferences normalize labels such as `front end` and
  `front-end` consistently while preserving the original matched label
  ([#196](https://github.com/zleo-ai/vyane-rs/pull/196)).
- A zero local-history limit returns before storage access
  ([#211](https://github.com/zleo-ai/vyane-rs/pull/211)).
- Offline parity reports pin capability identities and run their report
  regression tests in CI
  ([#212](https://github.com/zleo-ai/vyane-rs/pull/212)).

### Known limitations

- Full Python Vyane parity is not delivered. `v0.1.0` is a source snapshot,
  not that alignment. This tag does not retitle the capability matrix.
  Decision-layer rows and optional adapters stay open.
  See the [capability matrix](parity/ORIGINAL-VYANE-PARITY.md).
- Real package and registry readiness remain unverified. The publication-order
  regression uses synthetic archives; its success and source installation do
  not establish publishable artifacts. See [RELEASING.md](RELEASING.md).
- README translations are machine-assisted drafts pending native-speaker review;
  structural checks do not establish linguistic or semantic accuracy.
