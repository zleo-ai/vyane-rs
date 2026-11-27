# Changelog

User-visible changes in the public repository. Versions follow
[VERSIONING.md](VERSIONING.md) and the [release policy](RELEASING.md).
A git tag names a source snapshot. It is not a crates.io publication.
This is a curated history, not a complete reconstruction of all development.

## Unreleased

### Added

- `vyane mcp-smoke` checks the local stdio MCP server with a real MCP client. It starts this binary's `mcp` subcommand, sends `initialize` and `tools/list`, and does not call a tool. `--timeout` is the client read timeout in seconds (default 10). `--tool-limit` is how many tool names to keep after sorting (default 12); zero or a negative value keeps none, and `tool_count` is still the full count. `--json` prints one JSON object. Success has `status` `success`, `transport` `stdio`, `tool_count`, `tools`, and `elapsed_ms`. Plain success prints `MCP stdio smoke OK: {tool_count} tool(s), {elapsed_ms}ms` and, when `tools` is not empty, a second line of two spaces plus the names joined by `, `. Failure has `status` `error`, `transport` `stdio`, `error_type`, and `error`. `--json` prints that object; otherwise the command prints `MCP stdio smoke failed: {error_type}: {error}` and exits 1. The server is started with a temporary config file and a temporary `VYANE_DATA_DIR`, so the command does not read or write the default home data directory. A `--config` path passed to `mcp-smoke` is not opened. The child environment is a copy of the parent environment; `PYTHONUNBUFFERED` defaults to `1` and `VYANE_LOG_LEVEL` defaults to `WARNING` when those variables are unset. The child working directory is the caller's current directory.
- `history_archive` is an explicit monthly archive for one JSONL ledger. `maybe_rotate` runs when the live file is larger than `10 * 1024 * 1024` bytes and moves the older half of its records into `<ledger directory>/history-archive/YYYY-MM.jsonl.gz`. The month is the UTC year-month of `started_at` (RFC 3339). A line with no usable `started_at`, including a non-JSON line, goes to `undated.jsonl.gz`. Each rotation appends one gzip member for the new segment and does not recompress the month. Plain `YYYY-MM.jsonl` files already in that directory are still read. Readers walk archives in month order, then the live file. `history-archive/.pending/committed.json` stores the sha256 and byte length of a prefix that was archived before the live file was replaced; a live file that still starts with those bytes drops them on read, and a live row that only has the same text as an archived row is kept. A failed commit leaves the live file in place. A failed cleanup does not append that segment again. The next `maybe_rotate` finishes `.pending` before planning another cut. `.pending` is not ledger data. `JsonlLedger::append` does not rotate, and `vyane history` does not read the archive.
- `vyane export` writes a read-only report of the live run ledger (`ledger.jsonl`) as CSV, JSON, or Markdown. `--format` / `-f` is `csv` (the default), `json`, or `md`. `--hours` defaults to 0, which means no hour cutoff; a positive value keeps runs whose `started_at` is inside that window. `--provider` matches a provider id. `-n` / `--limit` defaults to 1000 and keeps the newest matching rows; zero writes an empty report without opening the ledger. `--output` / `-o` writes a file instead of stdout. CSV columns are `timestamp` (the row's `started_at`), `provider`, `status`, `duration_seconds` (`finished_at` minus `started_at`), `task_digest`, `task_preview`, `model`, `run_id`, `input_tokens`, `output_tokens`, and `cost_usd`. `task_preview` is empty unless that row already stored one. The command does not reconstruct a prompt, does not add a `source` column, and does not read `history-archive/`. JSON has `exported_at`, `count`, `statistics` (the same aggregates as `vyane history --stats` for the exported rows), and `entries`. Markdown is titled "Vyane Report" and caps its recent table at 50 rows. The command reads the local owner's rows only and does not modify `ledger.jsonl`. An unknown format is rejected. `vyane history` is unchanged.
- `vyane clean` deletes the local run ledger and feedback file. `what` is `all` (the default), `history`, or `feedback`. `history` removes `ledger.jsonl` and a sibling temporary rotate file whose name ends in `.rotate` and belongs to that ledger, such as `ledger.jsonl.rotate`. `feedback` removes `feedback.jsonl`. `all` is those two. `--dry-run` prints what would be removed and deletes nothing. Without `--dry-run`, `--yes` is required; otherwise the command exits 2 and leaves every file in place. `history-archive/` is never deleted, including a directory placed by hand. `sessions/`, `workflows/`, `worktrees/`, and the goal, task, agent, and message SQLite files are not targets. `audit`, `status`, and `benchmark` are rejected.
- `vyane liveness sandbox-orphans` prints a read-only procfs scan of Codex Linux sandbox processes reparented to the current user's `systemd --user`, PID 1, or a non-Codex child subreaper (`codex-sandbox-orphans.v1`). A candidate is `codex-linux-sandbox` (Linux `comm` may be truncated to `codex-linux-san`) or a direct `bwrap` / `bubblewrap` parent whose sandbox child did not start earlier and still names that parent. An orphan is recorded only when the visible ancestor chain, including a visible PID 1, does not contain the `codex` binary. An incomplete chain is skipped. A missing `ChildSubreaper` field is not treated as a subreaper. `--proc-root` selects the tree (default `/proc`). `--json` prints the report. `--warn-after-seconds` defaults to 21600; `status` is `warn` only when the oldest known age is greater than that threshold. The command does not signal, stop, or clean up any process. On a non-Linux host, a missing user id, or a proc root that is missing or unreadable, `available` is false and the process exits 1. This command does not read a worker registry or build the broader liveness report.
- `CodebuddyHarness` (`HarnessKind::Other("codebuddy")`) runs one `codebuddy -p <prompt> --model <id> --output-format json --setting-sources user` shot and maps the JSON onto `HarnessOutcome`. The prompt is an argument. Stdin is closed. The model is the job model when that id is non-empty, otherwise `params.extra["model"]` when that string is non-empty, otherwise `deepseek-v4.1-flash`. `task_type` and `chain_index` are ignored. `Write` passes `--permission-mode acceptEdits`. `Full` passes `-y`. `ReadOnly` passes no sandbox flag. `job.resume` is `--resume`. Optional extras are `effort`, `allowed_tools`, `disallowed_tools`, `add_dir`, and `fallback_model`; `fallback_model` is omitted unless the caller supplied a truthy value. Other extra keys, including `owner_user_id`, `bound_owner`, and `run_id`, are ignored. An argument of 128 KiB or more is rejected before spawn. No environment name is removed after the env policy is applied. An endpoint on the job is not injected. On Windows, an npm `codebuddy.cmd` forwarding shim is unwrapped to `node.exe` plus its script. SGR color is stripped. Only the first JSON value is decoded (an event array or one result object); a second JSON value fails. The last `type=result` event supplies the answer, `session_id` or `sessionId`, and `usage.input_tokens` / `usage.output_tokens`. `is_error` true, or a `subtype` other than `success`, fails the run. Truncated JSON and a missing result fail closed. Credit fields are not read. Callers construct the adapter directly. Config profiles and the service assembler keep the Claude Code and Codex CLI support matrix. Auto, failover, and broadcast do not gain a CodeBuddy default pool.
- `readonly_view` builds a bubblewrap read-only view prefix and the checks that keep that prefix closed. `validate_view_paths` requires an explicit existing workdir that is not the filesystem root, refuses a workdir that would re-expose `/tmp`, `/var/tmp`, or `/run`, and refuses a workdir stacked on `/dev` or `/proc`. Writable directories must be absolute paths that already exist and must not contain the workdir or the home directory. `readonly_view_prefix` resolves `bwrap` on `PATH` and returns `bwrap --ro-bind / / --dev /dev --proc /proc`, then `--tmpfs` on `/tmp`, `/var/tmp`, and `/run`. When `/etc/resolv.conf` resolves to a regular file under one of those mounts, only that file is bound read-only. The workdir is bound read-only and each writable directory is bound with `--bind`. Hidden paths are planned and validated after those binds: directories become `--tmpfs`, and regular files and sockets become `--ro-bind /dev/null`. The prefix ends with `--die-with-parent --new-session --chdir <workdir> --`. `--unsetenv` lists only `DBUS_SESSION_BUS_ADDRESS`, `DISPLAY`, `WAYLAND_DISPLAY`, `SSH_AUTH_SOCK`, `VYANE_DAEMON_API_TOKEN`, `VYANE_DAEMON_WORKER_TOKEN`, `VYANE_DAEMON_OWNER_TOKENS`, `VYANE_MEMORY_PRINCIPAL_TOKENS`, `VYANE_DASHBOARD_API_TOKEN`, `VYANE_DASHBOARD_OWNER_TOKENS`, `VYANE_DASHBOARD_INTERNAL_API_TOKEN`, and `VYANE_ENV_FILE`. Environment values are not read to discover aliases. A missing `bwrap` is an error, so the caller cannot fall through to an unisolated command. `assert_no_hardlink_escape` refuses a regular file in a writable directory whose link count is greater than 1. `assert_readonly_view_spawn` requires `close_fds` and rejects caller-supplied `pass_fds`. A hardlink runtime guard can warn when a new hardlink appears after a successful check; it does not signal a process. On Windows the link count is read from the file handle, and a count that cannot be read fails the check instead of assuming one link. The module does not start a D-Bus proxy, does not add daemon memory roots, and is not called by the Grok Build, Claude Code, Codex, or other adapters.
- `OpencodeHarness` runs one `opencode run <prompt> --format json` turn and maps the output onto `HarnessOutcome`. The prompt is an argument and stdin is closed. `--model` comes from the job model when that id is non-empty, otherwise from `params.extra["model"]`. A non-empty `params.extra["provider"]` is joined in front when the chosen model does not already contain `/`. `job.resume` is `--session`. Remaining `params.extra` entries follow passthrough rules in the map's iteration order: snake case becomes a kebab flag, true emits the flag alone, false and null are skipped, a string that starts with `-` is dropped, and a non-scalar value is refused before spawn. Identity keys (`owner_user_id`, `bound_owner`, `run_id`, and their kebab aliases) are never flags. `format` in extra does not replace `--format json`. No sandbox tier adds a flag, and no environment name is removed after the env policy is applied. An endpoint on the job is neither injected nor refused. Answer text is accumulated from non-JSON lines and from `part.text` on `type=text` events. Chat markers (`<|assistant|>`, `<|user|>`, `<|system|>`, `<|tool|>`, `<|end|>`, `<|endoftext|>`) and tool markers (`<tool_call>`, `</tool_call>`, `<tool_calls>`, `</tool_calls>`) are removed. A non-empty `sessionID` is the session id, and a non-JSON `Session:` or `session:` line is a fallback. `type=error` and `event_error` fail the run. `type=step_finish` `part.tokens.input` and `part.tokens.output` map to usage. `total` is not stored: `Usage` has no total field. A zero exit with empty answer text also fails. A non-zero exit fails. The harness kind is `HarnessKind::Other` with token `opencode`; `HarnessKind::OpenCode` is unchanged. Callers construct the adapter directly. Config profiles and the service assembler keep the Claude Code and Codex CLI support matrix. Auto, failover, and broadcast do not gain an OpenCode default pool.
- `GenericCliHarness` runs one caller-supplied command from an argv template. `{task}`, `{workdir}`, `{sandbox}`, and `{session_id}` come from the job. Other keys on `params.extra` are substituted in the map's key order; those four names are ignored, so extras cannot replace them. `{task}` matches that placeholder only, not a longer token such as `{task_extra}`. An empty template is `{task}`. String extras are their text, and any other JSON value is its JSON text. The command string is not a template. The child's stdout is the answer, including when it is empty, with no session id and no token usage. A non-zero exit is a harness failure. The harness kind is `Other` with the name given to the constructor, including when that name matches a built-in harness. The child environment is only the job's environment policy; an endpoint is not injected. Callers construct the adapter directly. Provider command configuration, the service assembler, and the auto, failover, and broadcast pools do not select it.
- `vyane quota-snapshot` prints a read-only JSON view of the local subscription snapshot (`quota-snapshot-read.v1`). `state` is `fresh`, `stale`, or `missing`, with reasons `file_missing`, `unreadable`, `schema_mismatch`, `never_succeeded`, `future_timestamp`, `too_old`, `probe_failed`, and `invalidated`. When the path is omitted, the file is `$XDG_STATE_HOME/aios/quota-snapshot.json`, or `~/.local/state/aios/quota-snapshot.json` when `XDG_STATE_HOME` is unset or empty. A path argument reads that file instead. The command reads a regular file of at most 1 MiB, leaves the file unchanged, and does not probe a provider or change automatic routing. A stale or unreadable snapshot is reported as such; `vyane route` and `vyane dispatch --target auto` still keep the unfiltered route in those cases.
- `KimiCodeHarness` (`HarnessKind::Other("kimi-code")`) runs one existing Kimi Code CLI turn (`kimi -p <prompt> --output-format stream-json`, optional `-m`, optional `-S`) and maps the stream onto `HarnessOutcome`. The prompt is an argument. There is no stdin prompt and no `--prompt-file` flag. Non-empty assistant `content` values are joined into the answer. `tool_calls` and `role=tool` events are skipped. The session id is the last non-empty `session_id` on `role=meta` / `type=session.resume_hint`. A `role=error` line, or a truthy top-level `error` on any other JSON object, fails the run. Output with no JSON object is kept as raw text. Token usage stays empty. `ReadOnly`, `Write`, and `Full` add no sandbox flag. The workspace is the child working directory. Caller `params.extra` values pass through as kebab flags (`add_dir` becomes `--add-dir`); identity keys and `provider` do not, and a string value that starts with `-` is dropped. An argument of 128 KiB or more is rejected before spawn. `KIMI_API_KEY` and `KIMI_MODEL_API_KEY` are removed from the child environment, including a different ASCII case of the same name, and are never injected. An endpoint base URL or credential on the job is not copied into the environment or the argument list. Callers construct the adapter directly. Config profiles and the service assembler keep the existing Claude Code and Codex CLI support matrix. Auto, failover, and broadcast do not gain a Kimi Code default pool.
- `GrokBuildHarness` runs one existing Grok Build CLI turn (`grok --prompt-file /dev/stdin --output-format json`) and maps the JSON document onto `HarnessOutcome`. The prompt is the child stdin, not an argument. `text` and a non-empty `sessionId` are kept. `usage.input_tokens` and `usage.output_tokens` stay the raw fields; `reasoning_tokens` and `cache_read_input_tokens` are kept beside them and are not folded into the input count. `ReadOnly` is refused before spawn: vendor `--sandbox` and `--permission-mode` are not treated as enforcement, and this adapter does not build a bubblewrap read-only view, so the bare CLI is not started. `Write` and `Full` spawn with no sandbox flag. A non-empty endpoint base URL or any endpoint credential is refused before spawn. `XAI_API_KEY` is removed from the child environment and is never injected. Callers construct the adapter directly. Config profiles and the service assembler keep the existing Claude Code and Codex CLI support matrix.
- `vyane route` and `vyane dispatch --target auto` read a local JSON snapshot of subscription pace. When `--quota-snapshot` is omitted, the file is `$XDG_STATE_HOME/aios/quota-snapshot.json`, or `~/.local/state/aios/quota-snapshot.json` when `XDG_STATE_HOME` is unset or empty. `--quota-snapshot PATH` reads that path instead. When one subscription is busy and another has more room, automatic routing leaves the busy profile out. A missing or unreadable snapshot leaves the route unchanged. A named dispatch target does not open the file. Provider ids `claude`, `codex`, and `grok-build` also match snapshot providers `anthropic`, `openai-codex`, and `xai-oauth` when the profile id has no subscription group of its own.
- `vyane acp` serves a canned Agent Client Protocol stub over stdio. `--stub` is optional and selects that same stub, which is the default. It completes `initialize`, stores `session/new` only in memory, and streams one canned turn: a `plan`, the prompt split across several `agent_message_chunk` frames, and a demo `tool_call` completed by `tool_call_update`. `session/cancel` during the turn stops it with `stopReason` `cancelled`. It does not load configuration, read credentials, dispatch, call a model, or create a state database.
- `vyane waiting` prints a read-only list assembled from three existing
  sources: pending kernel approval decisions, kernel deliveries whose phase
  is `approval_required`, and goal continuity projections whose next action
  is `decide_approval` or `manual_decision`. Every source is named. A source
  that is turned off, missing, or unreadable stays in the report with a
  status, and the other sources are still listed. The command does not
  migrate or write either database. It is not a `GET /api/waiting` route and
  does not scan a board, notifications, or secrets, and it does not snooze.
- `CursorCliHarness` runs one existing Cursor CLI print and maps the result object onto `HarnessOutcome`. The program is the Windows standalone `cursor-agent` (print flags on that program; no `agent` subcommand). Read-only passes `--mode ask` and does not pass `--force`, `--sandbox`, or `--trust`. `Write` is refused before spawn. `Full` stays `--sandbox disabled --force`. `result` and `session_id` are kept; the envelope publishes no token usage, so `usage` stays empty. `CURSOR_API_KEY` and `CURSOR_API_ENDPOINT` are removed after the child environment is built, including a different case of the same name, so a parent value, an allow entry, an inject entry, or endpoint auth cannot replace the local subscription login. A non-empty endpoint base URL is refused before spawn. Config profiles and the service assembler keep the existing Claude Code and Codex CLI support matrix. Auto, failover, and broadcast do not gain a Cursor default pool.
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

- A failed `git worktree add -b` no longer leaves its branch behind, so a
  resumed run can retry the same attempt instead of failing forever on
  "branch already exists". A generated branch that already existed before the
  call is never touched and the attempt is skipped like an existing path.
  When the leftovers cannot be cleared (a locked registration for a missing
  directory, or a symlink planted at the planned path), they are reported as
  left in place and recorded in the journal rather than removed through.
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
  space stays valid. Message-store errors name the field, not the value.
  Rows an earlier build wrote under its looser rule (padded or
  control-character text, including transport receipt batches whose digest
  covers that text) still open, audit and read back; only new writes are
  held to the tightened rule. Known trade-off: such a row is readable but
  cannot be addressed by its own value through an input that now applies
  the strict rule (for example a conversation listing or a transport
  receipt lookup keyed by a padded scope), which is rejected as invalid
  input rather than matched.
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
