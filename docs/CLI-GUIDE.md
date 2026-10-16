# CLI and service guide

Detailed examples extracted from the README. Start with the [quickstart](../README.md#quickstart).
Commands here assume the Rust `vyane` binary is on your PATH; if Python Vyane is
also installed, use the explicit Rust binary path from the quickstart.

## Protocol entry points

Vyane supports three front-ends that share the `vyane-service` layer. Each
exposes a different subset of operations; they are not interchangeable:

| protocol | command | use case |
|----------|---------|----------|
| **CLI** | `vyane dispatch --target prod "task"` | interactive / scripted one-shot runs |
| **REST API** | `vyane serve --addr 127.0.0.1:9721` | programmatic access from any HTTP client |
| **MCP** | `vyane mcp` | let other agents (Claude, Codex, …) call vyane as a tool |

The production CLI MCP surface contains ten tools: dispatch, broadcast,
history, sessions, `vyane_route`, `vyane_check`, `vyane_workflow_plan`, and
authenticated durable workflow submit/status/cancel. The library's base server
contains seven tools; only submit/status/cancel require an injected control port.
Generic success payloads have a 1 MiB cap;
the two new diagnostics use stricter, smaller bounds. `vyane_route` is a
Rust-side deterministic preview extension, not a same-name parity claim
against the fixed reference baseline. `vyane_check` performs static
configuration analysis only; it does not probe a network endpoint, spawn a
harness, or validate a live credential. Every success result shares the generic
output ceiling, but the legacy execution tools do not yet have the diagnostics'
uniform field-level input budgets. If execution completed but detail exceeded
the ceiling, dispatch/broadcast returns bounded run receipts with
`operation_status="completed"` and `detail_omitted=true`; callers must not retry
those receipts as though execution failed.

Workflow submission callers provide a canonical UUIDv7 plus a bounded self-contained
source bundle. The MCP layer cannot provide owner, controller, token, or an
execution path. The CLI freezes its own startup directory and re-authenticates
the exact resident daemon for every workflow operation. Responses contain only
the caller id, lifecycle state, and a closed failure code. Explicit step
workdirs and sandboxes above read-only are rejected before daemon contact. This is durable
workflow control, not general task, board, collaboration, or multi-principal
MCP parity.

## Static workflow plans over MCP

`vyane_workflow_plan` accepts a required nonempty `workflow_toml` string and
optional `prompt_files` (default `[]`), each containing exactly `path` and
`content`. Every declared prompt file must be supplied in memory; host files
are never consulted. Extra, missing, duplicate, noncanonical or absolute prompt
paths fail validation. Unknown fields, including `vars`, `caller_id`, `owner`,
`execution_cwd` and credentials, are rejected.

The existing source budgets apply: 1 MiB TOML, 128 prompt entries, 4 MiB per
prompt, 4,096 bytes per canonical relative path, and 16 MiB total UTF-8 source
bytes including paths. Source materialization also rejects more than 4,096
steps or 16 MiB of expanded prompt content before repeated-reference hashing or
allocation. Existing step-id, per-step list and global dependency/target/route
limits are also checked before diagnostics can repeat metadata; compiled plans
retain their existing total-content checks.
Runtime checks enforce byte limits; JSON Schema string
length hints alone do not enforce the UTF-8 byte budget.

```json
{
  "workflow_toml": "[workflow]\nname='preview'\n[[step]]\nid='one'\ntarget='unconfigured'\nprompt='Inspect {{vars.topic}}'"
}
```

The successful JSON text payload is the same `WorkflowPlan` schema v1 exported
by `workflow plan --json`: materialized DAG, unresolved targets, unrendered
templates, requested capabilities, and deterministic source/plan checksums.
It includes caller-supplied prompts, system text and workdirs, so it is **not a
redacted view**. Workdirs and requested sandboxes are represented without
granting permission or establishing execution readiness. The checksum is a
drift check, not authentication or authorization.

This tool performs bounded synchronous compilation only: no target resolution,
template rendering, daemon contact, model call, run/journal creation or separate
background task. Repeating the same sources produces the same plan. There is
no execution to retry or cancel. The MCP server retains its existing startup
configuration requirements and local scope; this tool adds no principal or
owner authority and accesses no stored workflow.

Errors use the existing successful MCP-result envelope with `status="error"`:
`invalid_argument` for invalid sources or schema, and `limit_exceeded` if the
serialized plan exceeds 1 MiB. Error messages are fixed and never include raw
parse errors or input text. An oversized plan is not truncated or executed;
reduce the sources to retrieve it through MCP. Existing tools and plan schema
remain compatible; clients enumerating tools now see one additional tool.

## Local A2A inbox

The CLI exposes the transactional message store as a same-machine durable
inbox:

```sh
vyane a2a send reviewer --from builder --kind review "ready for review" --json
vyane a2a inbox reviewer --json
vyane a2a read reviewer <message-id> --json
```

`inbox` is non-mutating. `read` requires the recipient mailbox as well as the
message id, then uses the existing fenced claim → delivered → acknowledged
state machine. It writes and flushes the full response before acknowledging;
write failure remains reclaimable, while a crash after flush can produce a
duplicate instead of message loss. Its JSON payload is therefore a pre-ack
`delivered` snapshot; exit status zero confirms the subsequent acknowledgement.
`--delay-seconds`, `--include-future`, `--include-read`,
`--limit`, `--owner` (alias `--owner-user-id`), and `--db` provide explicit
local control. The default database is the standard `messages.sqlite3` below
the Vyane data directory.

This is not an A2A HTTP implementation. It has no Agent Card, discovery,
remote send/get/cancel, SSE, push, or Channels adapter. `--owner` and `--from`
are supplied by the caller: the former is a logical storage scope and the
latter is only a sender label, not authenticated principal authority or
identity. Do not expose this CLI directly as a hostile multi-user service.
See [WP-59](plan/WP-59.md) for the exact boundary.

## Local goals

The CLI exposes an owner-scoped goal lifecycle over one SQLite source of truth:

```sh
vyane goal create --title "Ship a release" \
  --acceptance 'test-passes:cmd:cargo test --workspace' --json
vyane goal next --auto-start --json
vyane goal progress <goal-id> --stage implementation --detail "tests added" --json
vyane goal verify <goal-id> --workdir . --json
vyane goal claim <goal-id> --worker pursuer --json
vyane goal pursue <goal-id> --worker pursuer --target builder --workdir . --sandbox write --json
vyane goal continuity-next <goal-id> --json
vyane goal done <goal-id> --summary "all checks passed" --json
```

`create/get/list/next/start/claim/progress/verify/pursue/pause/resume/done/fail/cancel` share stable
JSON success/error envelopes. Every mutation updates the current query snapshot
and appends an immutable revision event in the same transaction. Queue selection
orders priority ascending, then creation time ascending. The default database is
`goals.sqlite3` below the Vyane data directory; `--db` selects another local
file. `--owner` (alias `--owner-user-id`) is a caller-selected storage scope,
not authenticated authority. Foreign and absent ids are intentionally
indistinguishable at the store boundary.

Acceptance `KIND:TARGET` values remain persisted descriptors. `goal verify`
executes only explicit local `cmd:` checks under a canonical workdir, scrubbed
environment, bounded output/time and Unix process-group cleanup, then persists
passing criteria through the existing lease-fenced mutation. The goal must be
`in_progress`; an active lease requires the matching `--worker`. It does not
auto-complete or pursue goals, invoke network/board adapters, coordinate quota
handoff, or expose a goal REST/MCP service. See [WP-60](plan/WP-60.md) and
[WP-68](plan/WP-68.md) and [WP-69](plan/WP-69.md). Each verification
attempt is retained as a private, immutable, digest-checked artifact and is
returned by `goal get --json`; it is evidence, not completion authority.

`goal pursue` is the explicit bounded outer loop: verify, dispatch one fresh
sessionless segment through the ordinary target/failover kernel, then reverify.
It requires and renews the exact active worker lease. A schema-v4 checkpoint
atomically preserves lifetime segment/failure counters, runtime/workdir identity,
and the last run and verification ids with each progress event. After pause and
database restart, an explicitly resumed goal can be claimed by a new worker and
continues from that checkpoint; stale revisions, workers and lease generations
fail closed. Only the pursuer may
complete after all criteria pass. Missing/manual criteria,
budgets, cancellation and repeated failures pause; external lifecycle changes stop. The
manual command does not auto-start a goal, resume a runtime-native session, or
perform quota/approval handoff. The resident daemon can separately opt in to the
same pursuer as described below.
The production CLI currently accepts only the `local` single-user owner so goal
and dispatch ledger authority cannot silently diverge.
See [WP-70](plan/WP-70.md), [WP-71](plan/WP-71.md),
[WP-72](plan/WP-72.md), and [WP-73](plan/WP-73.md).

`goal continuity-next` is a read-only operator projection over the exact current
goal revision, quota boundary, handoff steps, and durable approval evidence. It
returns one closed action such as queue, decide, execute, record a signal, wait,
resolve a blocked execution, or continuity complete. It never performs that
action and never invents required workdir, decision, or signal evidence. See
[WP-79](plan/WP-79.md).

The REST assembler explicitly opts into the goal read component and exposes the
same projection as `GET /v1/goals/{id}/continuity-next` on the
bearer-authenticated loopback server. That REST instance of `GoalReadService`
freezes the `local` owner at construction, observes the goal and its approval
evidence in one read transaction, and returns only an allowlisted redacted DTO. A request cannot
select an owner. The component has no goal list/get, MCP, queue, decision,
execution, signal or runner surface and cannot complete a goal. The bearer is
not a distinct principal and does not provide hostile multi-user or same-UID
isolation. This fresh continuity view is also not runtime-native session
resume. See [WP-80](plan/WP-80.md).

`GoalObservationIngress` is a separate opt-in mutation component. It freezes an
authority-supplied owner, binds a trusted non-secret watcher identity as the
signal source, and accepts only typed quota-reset or sequenced review-check
observations for an exact current quota/primary-target boundary. The one-shot
`GoalObservationRunner` bounds watcher count, concurrency, timeout and batch
size, isolates failures, and applies accepted batches in deterministic watcher
order. It has no HTTP/MCP/CLI endpoint, concrete connector, periodic loop,
approval or execution authority; it cannot complete a goal. See
[WP-81](plan/WP-81.md).

`GoalContinuityRunner` consumes that closed next-action view for a bounded goal
set. Read, queue and execute credentials are authenticated through separate
purpose policies and must resolve to one owner. Each port retains that same
opaque owner context, and assembly rejects a mismatched binding. Queue and
approved-execution ports receive only the exact goal revision, quota boundary,
step and approval fence; there is no decision port, raw dispatch input, retry or periodic loop.
The crate supplies no concrete queue/execution adapter, so assembly remains
explicit. See [WP-82](plan/WP-82.md).

## REST API

`vyane serve` exposes a JSON HTTP API with streaming (SSE) and async task
support:

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/v1/health` | health check |
| `POST` | `/v1/dispatch` | dispatch a task (synchronous, blocks until done) |
| `POST` | `/v1/dispatch/stream` | dispatch with SSE streaming (deltas as they arrive) |
| `POST` | `/v1/broadcast` | fan out to multiple targets concurrently |
| `POST` | `/v1/tasks` | submit async dispatch, returns task id immediately |
| `GET` | `/v1/tasks` | list durable REST task metadata |
| `GET` | `/v1/tasks/:id` | get one task's durable status and ledger link |
| `GET` | `/v1/tasks/:id/output` | read a successful task's separate mode-0600 output artifact |
| `POST` | `/v1/tasks/:id/cancel` | cancel a running task |
| `GET` | `/v1/runs` | query the run ledger (filter by status/provider/start time) |
| `GET` | `/v1/sessions` | list saved sessions |

History accepts an inclusive start-time cutoff through
`vyane history --since 2026-01-01T12:00:00Z`, the REST query parameter
`since`, and the MCP `vyane_history` argument `since`. Supply an RFC 3339
timestamp with a timezone (`Z` or an explicit offset); percent-encode `+` as
`%2B` in a REST query. The cutoff compares `started_at` and combines with
provider/status filters before the record limit, retaining reverse append
order. Omit it to query all time. This reuses the run ledger; source filtering and automatic cost attribution remain open parity work.

An inclusive upper bound is available as `vyane history --until TIMESTAMP`,
REST `until`, and the MCP `vyane_history` argument `until`. It uses the same
timezone-required RFC 3339 format and compares `started_at` before pagination.
For example, `--since 2026-01-01T12:00:00Z --until 2026-01-01T12:00:00Z`
includes runs started at that exact instant. Either bound may be omitted;
`since` later than `until` is rejected. Both bounds combine with provider/status
filters and also apply to statistics.

The local CLI additionally accepts `--session SESSION` and repeatable
`--label KEY=VALUE` predicates over its existing `owner=local` records. For example,
`vyane history --session example-session --label purpose=example --stats --json`
counts every matching local run. Session ids, label keys and label values match
exactly and case-sensitively; repeated labels are ANDed, including repeated keys.
A session id and label key must be nonempty. Label values may be empty or contain
`=`. Predicates combine with provider/status/time bounds before a list limit;
statistics count all matches. The unfiltered list defaults and response shapes
remain unchanged. These predicates are local CLI only: REST/MCP do not accept
these arguments or expose the hidden session/label metadata.

A local CLI list with `--limit 0` returns no records (JSON: `[]`) without opening
local storage. Its filter arguments are still validated first. Positive limits,
list defaults and statistics retain their behavior; REST/MCP keep their existing
positive-limit validation.

Use `vyane history --stats --json`, `GET /v1/runs?stats=true`, or
`vyane_history` with `{"stats": true}` to count every matching ledger run.
The same provider, status and inclusive `since`/`until` filters apply. Statistics mode
rejects an explicit `limit`; the usual list defaults do not truncate counts.
The result contains `total`, `success`, `error`, `timeout`, `cancelled`, and
`by_provider` with the same five counts for each final provider. A failover run
counts once, regardless of its attempt count. Missing or empty storage returns
zero counts and an empty provider map. Statistics contain no run ids, prompts,
paths, labels or raw errors. Saved costs are summarized separately below.

The overall `duration` object derives whole-run wall-clock elapsed time from
saved `started_at` and `finished_at`: `samples` counts nonnegative spans and
`invalid` counts reversed timestamps. Each valid span is truncated to whole
milliseconds; `total_ms` sums those samples and `mean_ms` averages them, rounded
to one decimal millisecond. All run statuses contribute valid timings, and the
existing filters apply before measurement. Empty or all-invalid samples return
`null` for both values (CLI text: `n/a`); a measured zero stays `0`. An aggregate
total outside `u64` is `null`, while its sample count and mean remain available.
Provider groups retain their existing counts and success rates. The separate
`by_provider_duration` map uses the same final-provider keys and the same four
duration fields for each group. Every sample covers the whole recorded run span,
including preceding failover attempts; grouping assigns that span to the final
provider. Filters are applied first, and each mean uses its own valid sample count.
All-invalid groups remain present with null measurements; no matches produce an
empty map. CLI text adds the same duration columns to its provider table.

Each count object also includes `success_rate`: successful runs as a percentage
of all matching runs, rounded to one decimal. Error, timeout and cancelled runs
remain in the denominator. Provider groups use their own counts, and all filters
apply before computing rates. With no matching runs the overall rate is `null`
(CLI text: `n/a`); a nonempty group with no successes has a measured rate of `0.0`.

Statistics also include a `cost` object with `known`, `unknown`, `invalid`,
`known_total_usd`, and `overflowed`. It sums only existing `cost_usd` values:
finite nonnegative amounts (including explicit zero) are known; missing values
are unknown; negative or nonfinite in-memory values are invalid. The three
counts partition the matching runs. `known_total_usd` is the partial sum of
known amounts, not an estimate of missing prices or an invoice. It is `null`
when there are no known amounts or their sum exceeds finite `f64` range; the
latter sets `overflowed=true`. JSON keeps approximate floating-point amounts
without decimal rounding, and human output labels the sum as known saved cost.

The same additive object is returned by CLI, REST and MCP statistics. Owner and
request filters apply first, and each matching run contributes once regardless
of status or attempt count. No price table, model call, historical repricing or
ledger write occurs. Current dispatch does not automatically populate
`cost_usd`, so its records remain unknown here. Existing list responses and
provider groups keep their shape; this is not per-attempt or provider billing.

SSE streaming events (`POST /v1/dispatch/stream`):

```
data: {"type":"delta","text":"Here is "}

data: {"type":"tool_use","name":"Read","summary":"{\"path\":\"src/lib.rs\"}"}

data: {"type":"delta","text":"a function..."}

data: {"type":"finished","record":{...},"output":"Here is a function..."}
```

Async task lifecycle (`POST /v1/tasks` → poll `GET /v1/tasks/:id`):

```bash
# Use the exact mode-0600 token path printed by `vyane serve`. Feeding this
# header through curl config stdin keeps the bearer out of argv and the process
# environment. The token is removed after a clean shutdown.
TOKEN_FILE='<path printed by vyane serve>'
rest_auth() {
  printf 'header = "Authorization: Bearer %s"\n' "$(<"$TOKEN_FILE")"
}

# Submit
rest_auth | curl --config - -X POST http://localhost:9721/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task":"write tests","target":"openai/gpt-4o"}'
# → {"id":"0195...","state":"running","task_digest":"...",...}

# Poll
rest_auth | curl --config - http://localhost:9721/v1/tasks/0195...
# → {"id":"0195...","state":"succeeded","ledger_run_id":"0195...",...}

# Read the result body after success
rest_auth | curl --config - http://localhost:9721/v1/tasks/0195.../output
# → {"output":"..."}
```

Cancellation first reports the non-terminal `cancelling` state; keep polling
until the kernel records `cancelled`, `succeeded`, `failed`, `timed_out`, or
`interrupted` after process cleanup and ledger settlement. Task responses never
embed the prompt, output, or raw error; use `ledger_run_id` to correlate the
bounded task record with the run ledger.

The server rejects non-loopback bind addresses and every endpoint requires a
fresh 256-bit bearer capability published to the private `serve.token` path
printed at startup (mode `0600` inside a mode-`0700` data directory on Unix).
It also rejects non-loopback `Host`/`Origin` values and
cross-site browser requests. Run and session responses use allowlisted public
views rather than serializing internal storage records. This blocks remote
browser rebinding and other unauthenticated callers, but malicious code running
under the same OS identity can read the token; this remains a local single-user
control surface, not a hostile same-UID or multi-user boundary. On non-Unix
systems, keep `VYANE_DATA_DIR` under a platform-managed user-private directory;
Vyane does not replace the platform ACL on a caller-selected shared directory.

## Resident workflow daemon

The local-only workflow daemon is a separate control surface from `vyane serve`.
It keeps an admitted workflow running after the submitting CLI exits:

```sh
vyane daemon start                         # detached, waits for readiness
vyane daemon start --goal-auto-pursue \
  --goal-target builder --goal-workdir . --goal-sandbox write
vyane daemon status --json
vyane workflow submit workflow.toml --var env=dev
vyane workflow status <uuidv7> --json
vyane workflow cancel <uuidv7>
vyane daemon stop
```

`daemon run` keeps the same supervisor in the foreground. The listener accepts
only loopback addresses, and every endpoint — including `/health` — requires a
per-start 256-bit bearer token stored separately from the owner-only daemon
descriptor. The control API is `POST /v1/workflows`, `GET /v1/workflows/:id`,
and `POST /v1/workflows/:id/cancel`. On Linux it also exposes
`POST /v1/agent-runs`, `GET /v1/agent-runs/:id`,
`GET /v1/agent-runs/:id/output`, and `POST /v1/agent-runs/:id/cancel` for the
fresh/sessionless Process and Native lanes described in
[WP-61](plan/WP-61.md) and [WP-66](plan/WP-66.md). There is no
permissive CORS layer. This
protects against accidental browser and cross-process access, but is not a
sandbox against hostile code running as the same OS user.

The client authenticates the exact recorded daemon before reading local
workflow sources. It sends the workflow TOML plus every declared `prompt_file`
as an in-memory source bundle; the daemon never resolves those source paths on
its own filesystem. The semantic limits are 1 MiB for the TOML, 4 MiB per
prompt, 16 MiB total, 128 prompt entries, and 4,096 bytes per bundle path.
Variables are limited to 128 entries, 256 bytes per key, 1 MiB per value, and
4 MiB total. The canonical submission working directory is also carried in the
request (maximum 4,096 bytes): a missing or relative step `workdir` is resolved
from it, while an absolute step `workdir` is preserved.

The client generates a canonical UUIDv7 by default and prints it to stderr
before the request is sent. `--id <uuidv7>` reuses an id for reconciliation or
an idempotent retry: the same daemon workflow scope, normalized source, working
directory, and variables returns the existing task; any mismatch is a conflict
and never replays the earlier payload. The task id and workflow journal id are
identical. On daemon restart, abandoned daemon-owned rows are cleaned by exact
controller identity and marked `interrupted`; they are not automatically
resumed or replayed. Foreground `workflow resume` remains an explicit,
journal-oriented command.

Goal pursuit is disabled by default. `--goal-auto-pursue` requires one explicit
target and canonical workdir; sandbox and verifier/segment/invocation limits
remain bounded flags. The daemon handles one `local` goal at a time, first
adopting eligible `in_progress` work and then atomically claiming the next
queued goal. A daemon replacement can continue its still-live stable-worker
lease and WP-71 checkpoint; an expired lease is reclaimed through the normal
generation fence. Semantic pauses, terminal goals, and goals under another
live worker are never auto-resumed or stolen. Shutdown cancels the fresh
runtime effect while leaving a running checkpoint, rather than recording a
false business pause. This adds no goal HTTP API, quota/approval handoff,
runtime-native session resume, remote owner, or parallel goal scheduler.

## Smart routing

`vyane route "task text"` previews a decision. `vyane dispatch --target auto`
executes the same decision and records the selected profile/provider/model,
tier, effort, score, intent, and tag on the ledger record. The router scores task complexity from structural signals
(changed files, dependency edges, retry count, prompt length, inferred
tags) and maps to an economy / mainline / frontier tier, then resolves
a preference based on profile `tier` and `tags` metadata:

```
vyane route "write an ADR for the system architecture" --changed-files 20
vyane route "simple task" --tier frontier
vyane dispatch "fix the parser" --target auto
vyane dispatch "review auth" --target auto --no-frontier
```

Routing hints can also be supplied as `routing.stage`, `routing.tier`,
`routing.tags`, `routing.candidates`, and `routing.allow_frontier` labels.
Decision fields such as `routing.provider` and `routing.effort` are reserved so
callers cannot forge audit metadata. A literal profile named `auto` remains
addressable as `profile:auto`; `target:<provider>/<model>` explicitly selects a
provider/model pair when a provider id begins with the reserved `profile:`
prefix. Workflow steps may use a
deferred auto target after prompt rendering:

```toml
[[step]]
id = "review"
target = "auto"
prompt = "Review {{steps.implement.output}}"
[step.route]
stage = "review"
tags = ["security"]
allow_frontier = false
effort = "high"
```

`[step.route].effort` is a closed typed value (`low`, `medium`, `high`, or
`xhigh`) available only on a deferred single target. Route hints on an explicit
target or `fan_out` fail before dispatch instead of being ignored. Effective
effort precedence is workflow explicit effort, then the selected profile's
configured effort, then the decision tier default. The resulting canonical
`routing.effort` is applied to every failover leg and frozen for recorded,
detached, daemon-idempotent, and journal-resume execution. Invalid values are
rejected without echoing their input, and an ordinary `effort` label cannot
forge the reserved decision field. See [WP-46](plan/WP-46.md).

`WorkflowPlan` schema v1 is now the bounded, strict, filesystem-independent
execution payload shared by compile, prepare, run, and resume. It freezes the
materialized DAG, typed targets and route hints, lossless timeouts, requested
capability summary, source claim, and a canonical plan checksum. The payload is
not a safe public view, and its checksum is not authentication, provenance, or
execution authority. Plan-only continuation fails closed unless the journal has
the exact checksum; only the source-bearing compatibility API may migrate an
older journal after validating the exact source hash. See
[WP-54](plan/WP-54.md).

[WP-58](plan/WP-58.md) adds explicit exact-plan replay/fork. It reads a
terminal source journal, creates a new UUIDv7 journal, reuses only the
dependency-closed, journal-recorded all-success prefix, and executes the
remaining suffix live.
The source remains unchanged; daemon restart still never implies replay.

Inspect a workflow before execution with `vyane workflow plan workflow.toml`.
The default output lists steps, dependencies, unresolved target names, requested
sandbox levels and the plan digest. Add `--json` to export the existing schema v1
payload, including prompt text, system instructions and workdirs; it is not a
sanitized public view. Planning reads the workflow and declared prompt files,
but does not load runtime configuration, resolve targets, render templates,
call models or create a journal. Requested capabilities do not authorize
execution or establish runtime readiness.

This closes the repository-local shared typed-plan prerequisite and CLI static
plan inspection/export plus MCP source-to-plan compilation, not workflow parity.
Dynamic control flow, nested
workflows, shared budgets, a compatibility frontend, changed-plan call matching,
plan import and REST plan transport, sanitized cross-implementation fixtures,
and a production-complete model-tier policy remain open.

The frontier guard is fail-closed across the selected profile and its failover
chain. Detached auto-routed jobs send a secret-free target snapshot once over a
private worker stdin pipe and refuse to start if config changes would make the
worker execute a different provider/model/protocol/harness/parameter chain.
The snapshot is not persisted. Endpoint URLs, provider-specific extra values,
and harness env-injection values are represented only by safe digests/shape
metadata; raw credentials are never copied.

## Solution-review workflow

`vyane review` runs a three-step solution-review workflow on the existing
workflow engine: implement → fan-out review → synthesize. It is not yet the
original system's structured git diff/PR review, finding-artifact, verifier,
and publication pipeline.

```
vyane review 'implement a sorting function' \
  --implementer sonnet \
  --reviewers opus,gpt \
  --synthesizer opus
```
