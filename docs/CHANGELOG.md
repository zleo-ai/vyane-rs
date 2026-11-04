# Changelog

User-visible changes in the public repository. Versions follow
[VERSIONING.md](VERSIONING.md) and the [release policy](RELEASING.md).
A git tag names a source snapshot. It is not a crates.io publication.
This is a curated history, not a complete reconstruction of all development.

## Unreleased

### Added

- The Anthropic Messages client implements non-streaming typed tool turns
  (`complete_turn`) and a separately authorized turn path
  (`AuthorizedToolChatClient`). They keep tool calls and results, thinking
  text, `stop_reason: "refusal"` (as a structured refusal), every system
  message and the shared resource limits. This is not full parity with the
  OpenAI Chat typed turn: thinking signatures and `redacted_thinking` data
  are not kept, so replaying an assistant turn that carried thinking fails
  with a `Config` error, which makes multi-turn tool loops with `effort` set
  unsupported for now. Ordinary `complete` stays text-only.
- `vyane feedback rate|list`, REST `POST /v1/runs/{run_id}/feedback` /
  `GET /v1/feedback`, and MCP `vyane_feedback` record an owner-scoped 1–5
  rating for one of your runs. Ratings are append-only; the latest rating per
  run and source wins. No free-text comment is stored.
- `vyane history --outcomes`, REST `GET /v1/outcomes`, and MCP
  `vyane_outcomes` group your runs by provider, protocol, harness, model,
  effort, profile or status and report success, failover, duration, token,
  cost and average human rating. Machine-derived ratings are excluded unless
  requested. Routing decisions do not read ratings.
- A workflow step can set `isolation = "worktree"` (with optional
  `isolation_base`) to run each write-capable target in its own git worktree
  under the Vyane data directory. A worktree with no changes is removed; one
  with uncommitted changes or new commits is kept and its branch, path and
  base/head commits are recorded in the journal and printed. A worktree whose
  index holds a submodule is kept as `unknown_state`; settlement never runs
  git inside a nested repository. If a run stops on an internal error (for
  example a failed journal write) while isolated steps are still running,
  their worktrees stay `active` in the journal and a later `resume` reports
  them as kept. Read-only steps and the resident daemon reject the option
  instead of ignoring it. Default behavior is unchanged.

### Changed

- `ChatClient::complete_turn` on the Anthropic Messages client no longer falls
  back to the text `complete` path, even for a tool-free request. Unknown
  content blocks and a malformed `tool_use` input are `Protocol` errors.
  Reserved fields in `params.extra` (such as `thinking`, `system` or
  `max_tokens`), a thinking budget not below `max_output_tokens`, and a
  non-default temperature together with `effort` are `Config` errors.
- A read-only Claude Code dispatch now passes `--permission-mode dontAsk`
  and denies the Bash, Edit, Write and NotebookEdit tools, so a permissive
  `.claude/settings.json` in the workspace can no longer let the model edit
  files. Read-only shell commands such as `git log` are no longer available
  to it either. Hooks configured in those settings files still run, as in
  an interactive session.
- Rust embedders: `JournalStep` gains `worktrees`, `WorkflowPlanStep` and
  `WorkflowStep` gain `isolation` and `isolation_base`, `StoragePaths` gains
  `worktrees_dir`, `vyane_workflow::WorkflowError` gains
  `IsolationUnsupported`, and `vyane_mcp::WorkflowControlError` gains
  `IsolationUnsupported` (public MCP code `isolation_unsupported`). Struct
  literals and exhaustive matches on these types need the new member.
  `WorkflowPlanFeature` gains `WorktreeIsolation`; it is `#[non_exhaustive]`,
  so matches on it are unaffected. Plans and journals without isolation keep
  their serialized form.

### Fixed

- The run-status filter of `vyane history`, REST `GET /v1/history` and
  `GET /v1/outcomes`, and MCP `vyane_history` and `vyane_outcomes` uses one
  parser. A rejected status gets a fixed message that does not echo it (the
  CLI still exits 2), and `canceled` is accepted as a spelling of `cancelled`
  everywhere.
- A workflow variable key that contains a control character is rejected.
  The error names the position, not the key. A key with an internal space
  stays valid, and a value may still contain a newline.
- A goal observation whose goal id contains a control character is rejected
  before it is recorded. The receipt omits the id. An id with an internal
  space still reaches lookup.
- Native `write_file` and `edit_file` refuse any path with a `.git`,
  `.vyane`, `.codex`, `.claude` or `.mcp.json` component, at any depth and
  in any ASCII case, matching the rule for command writable roots. Write
  exclusions cannot relax it. Files there, such as git hooks, harness
  settings or Claude Code's project MCP servers, run or are trusted outside
  the sandbox.
- Claude Code dispatch passes the task after a `--` marker, so a task that
  starts with `-` is no longer parsed as a CLI option.
- Environment values injected into a harness child, including an endpoint
  API key, are held as secrets. Debug output of a harness job lists only the
  variable names, and a serialized environment policy omits the injected
  set.
- A streaming HTTP response splits events at the first blank line even when
  the endpoint mixes `\r\n\r\n` and `\n\n` delimiters, instead of merging
  two events into malformed JSON. A single streamed event larger than the
  8 MiB response cap now fails with a protocol error instead of buffering
  without limit.
- Streaming OpenAI Chat runs record their token usage. The usage report
  arrives after the finish reason, and the stream used to end at the finish
  reason, so these runs recorded zero tokens.
- Streaming Anthropic runs record input and cache tokens, which the API
  reports only at the start of the stream. Output tokens, reported as a
  running total, are counted once. Streamed and non-streamed runs now
  report the same usage.
- A streaming Anthropic or OpenAI Responses run whose connection closes
  before the stream's end event (`message_stop`, `response.completed` or
  `response.incomplete`) fails with a transport error instead of being
  recorded as a success with truncated output. A streaming OpenAI Chat run
  must report a finish reason before the connection closes; `[DONE]` may
  still be omitted.
- Adding token usage together saturates at the largest count instead of
  panicking in debug builds or wrapping to a small count in release builds
  when an endpoint reports an extreme value.
- Kernel-store owners and effect ids, and message-store owners, ids,
  routes, consumers and idempotency keys, reject surrounding whitespace and
  control characters (NUL included), the same character rule the agent
  store and owner resolution already apply. Length limits are unchanged, so
  a kernel-store owner is still limited to 128 bytes. An id with an internal
  space stays valid. Message-store errors name the field, not the value. A
  row written by an earlier build with such an id or owner is rejected when
  a read path re-validates it.
- Dispatch labels from CLI `--label`, REST `labels` and MCP `labels` share
  one parser. A key must be non-blank, at most 256 bytes, without
  surrounding whitespace or control characters; a value is a single line
  without control characters (newline, tab and NUL included). A dispatch
  carries at most 128 labels. A label error names the label's position,
  not its text. MCP `session` and `labels` now have schema length limits.

### Missed in the 0.1.0 notes

These changes shipped in 0.1.0 but were not listed in its section.

- A config file that sets `schema_version` to anything other than the
  integer `1` is rejected while it is parsed, before any layer is merged.
  Omitting `schema_version` still loads the file as version 1. Earlier
  builds ignored the key, so a config that carried another value now fails
  to load. ([#248](https://github.com/zleo-ai/vyane-rs/pull/248))
- Every dispatch that records a run also appends one `dispatch.terminal`
  event (run id and status only) to the run owner's event log. The data
  directory therefore gains `events/<owner digest>/dispatch.jsonl` and a
  lock file beside it. A failed event append is logged and does not fail
  the dispatch. ([#242](https://github.com/zleo-ai/vyane-rs/pull/242))

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
