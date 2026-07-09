# Roadmap

Scope, not schedule. Milestones list *what*, in dependency order; there are no
dates and no time estimates. `vyane-core` (the four-layer model, traits, error
taxonomy, env policy) is complete and underpins everything below.

## v0.1 — the kernel end to end

A single machine can configure targets, dispatch a task to one, broadcast to
several, fail over between them, and have every run recorded. Delivered as a
sequence of milestones, each a self-contained work package.

| milestone | scope |
|-----------|-------|
| **M1** | config + provider: TOML config, layered precedence, profiles resolving to `BoundTarget`, failover chains, per-provider env-injection rules. |
| **M2** | protocol clients: `ChatClient` for OpenAI Chat + Anthropic Messages (non-streaming + SSE), OpenAI Responses (non-streaming), with retry/backoff and faithful error mapping. |
| **M3** | harnesses: `Harness` for Claude Code + Codex CLI — headless one-shot, scrubbed child env via `EnvPolicy`, process-group spawn and group kill. |
| **M4** | kernel: the dispatch / broadcast / failover state machine over injected executors, assembling the full-attempt-trail `RunRecord`. |
| **M5** | ledger + sessions: append-only JSONL `Ledger` with advisory locking, filesystem `SessionStore`, cost estimation from a price table. |
| **M6** | CLI + integration: `vyane check` / `dispatch` / `broadcast`, wiring all crates behind the command line, end-to-end tests. |

The M1–M5 work packages are specified in [`docs/plan/`](plan/) (WP-01 … WP-05);
they map one-to-one onto M1–M5. Because the kernel depends only on `vyane-core`
traits, the wave-1 packages are largely parallel — assembly happens at M6.

## v0.2 — pipelines and background execution

| milestone | scope |
|-----------|-------|
| **workflow engine** | declarative multi-stage pipelines, each stage bound to its own target (or target chain), passing results between stages. |
| **async task registry** | track, query, pause, resume and cancel long-running dispatches that outlive a single CLI invocation. |
| **daemon** | a resident process hosting the task registry and workflow execution, so runs continue independently of any one client. |

## v0.3 — integration surface and smarter routing

| milestone | scope |
|-----------|-------|
| ~~**MCP server**~~ | ✅ expose dispatch / broadcast / history / sessions as MCP tools (`vyane mcp`, rmcp SDK, stdio transport). |
| ~~**REST API**~~ | ✅ HTTP JSON API (`vyane serve`, axum): `/v1/dispatch`, `/v1/broadcast`, `/v1/runs`, `/v1/sessions`, `/v1/health`. |
| ~~**shared service layer**~~ | ✅ `vyane-service` crate: one `VyaneService` facade shared by CLI, REST, and MCP front-ends. |
| **review pipeline** | a built-in multi-model review workflow (independent reviewers, cross-model comparison) on top of the workflow engine. |
| ~~**pluggable routing**~~ | ✅ `vyane-router` crate: deterministic complexity scoring, tag inference, tier mapping, preference resolution. Wired into `vyane-service` via `RoutePreferenceTable` built from profile `tier`/`tags` metadata. |
