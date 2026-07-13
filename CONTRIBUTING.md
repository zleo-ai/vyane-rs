# Contributing

Vyane is early-bootstrap and its APIs are unstable, but the bar for merged code
is already firm. Thanks for helping build it.

## Toolchain

- **Stable Rust**, **edition 2024** (see `rust-toolchain.toml`; MSRV is pinned
  in the workspace `Cargo.toml`).
- Install the pinned toolchain with `rustup` — it will pick up
  `rust-toolchain.toml` and the `rustfmt` + `clippy` components automatically.

## Checks that must pass

Every change must be clean under all three before review:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

The workspace Cargo config caps libtest at four threads because process-control
acceptance tests create and signal real child groups; unbounded host-core
parallelism can make those tests interfere on large development machines.

CI also runs `cargo check --workspace --all-targets --locked` on the exact
workspace MSRV (`1.85.0`). Local stable-only checks do not prove MSRV
compatibility.

Clippy runs as an error gate. `unsafe_code` is denied workspace-wide and
`unwrap_used` is warned — prefer explicit error handling over `unwrap`/`expect`
in non-test code.

## Crate map

```
vyane-core        types, traits, errors, env policy — no runtime
vyane-message     owner-safe transactional messages, deliveries, leases, outbox
vyane-goal        owner-scoped goal snapshots and immutable lifecycle/progress events
vyane-task        secret-free durable task lifecycle metadata
vyane-provider    provider registry, endpoints, auth styles
vyane-config      TOML parsing, profile + failover chain resolution
vyane-protocol    ChatClient over HTTP (OpenAI Chat/Responses, Anthropic Messages)
vyane-harness     coding-CLI Harnesses + native permission/tool execution seam
vyane-ledger      run/event ledgers, owner-isolated SessionStore, cost table
vyane-kernel      dispatch / broadcast / failover state machine + streaming
vyane-router      deterministic routing: complexity scoring, tag inference, tier mapping
vyane-workflow    declarative workflow engine (DAG + journal/resume)
vyane-service     shared facade: config loading, selector resolution, routing adapter
vyane-mcp         MCP server (rmcp SDK, 4 tools over stdio)
vyane-cli         front-end: CLI + REST API (axum) + MCP launcher
```

The load-bearing rule: **the kernel depends only on `vyane-core` traits.** It
never names a concrete protocol client, harness, or ledger. Concrete types are
constructed and wired together in `vyane-service` and injected through the
`ExecutorFactory` seam.

## Protocol entry points

Vyane has three front-ends for the shared service operations. Workflow and
detached-task control are currently CLI/REST-specific surfaces rather than
interchangeable operations:

| protocol | command | new code location |
|----------|---------|-------------------|
| CLI | `vyane dispatch/broadcast/review/route/task/workflow/a2a/goal` | `vyane-cli/src/command.rs` |
| REST API | `vyane serve` (axum, 10 endpoints + SSE) | `vyane-cli/src/api.rs` |
| MCP | `vyane mcp` (rmcp, 4 tools over stdio) | `vyane-mcp/src/lib.rs` |

When adding a shared operation, implement it in `vyane-service` and expose it
through each applicable front-end. If an operation is intentionally
front-end-specific, document that boundary and keep reusable execution logic in
its owning crate; do not duplicate orchestration logic across front-ends.

## Streaming

The kernel owns streaming via `Dispatcher::dispatch_stream`. It takes a callback
for text, reasoning, and harness tool-use events and returns the assembled,
ledger-appended `DispatchOutcome`. Direct-HTTP clients and CLI harnesses meet at
this API. The CLI `--stream` flag and REST `/v1/dispatch/stream` SSE endpoint both
call it — do not hand-roll streaming logic in front-ends. Protocol fixtures must
match real CLI event envelopes; a green compile alone is not an adapter test.

## Routing

`vyane-router` is a standalone crate (depends only on `serde`) implementing
deterministic target selection: complexity scoring, tag inference, tier mapping,
and preference resolution. `vyane-service/src/routing.rs` is the adapter that
builds a `RoutePreferenceTable` from configured profiles (tier/tags/stage
metadata) and maps the router's decision back to a profile name. Use
`vyane route` for dry-run testing and `vyane dispatch --target auto` for the
executable path. `RouteDecision.provider` must always remain a real provider id;
the selected config profile belongs in `RouteResult.profile`. New adapters must
prove effort propagation through both direct-HTTP and CLI-harness fixtures.

## Tests accompany code

New behaviour ships with its tests in the same change. Follow the patterns the
work packages call for — HTTP clients are tested against `wiremock`, harnesses
against fake CLI shell scripts (no real external CLIs in CI; real-CLI smoke
tests live behind `#[ignore]`).

Test placement:
- **Unit tests** (`#[cfg(test)] mod tests` inside each source file) for
  serialization, parsing, pure logic.
- **Integration tests** (`crates/*/tests/*.rs`) for multi-crate flows,
  dispatch/broadcast/broadcast/failover, workflow engine, ledger roundtrips.
- **Acceptance tests** (`crates/vyane-cli/tests/*.rs`) for end-to-end CLI +
  API behavior against wiremock servers.

## Pull requests

- **Small and single-purpose.** One logical change per PR; split unrelated work
  apart. Large sweeping PRs are hard to review and will be asked to be broken up.
- **Conventional commits.** Use `type(scope): summary` — e.g.
  `feat(protocol): add Anthropic Messages streaming`, `fix(harness): kill the
  process group on cancel`, `docs(architecture): clarify failover gate`.
- **Keep the docs in sync.** If you change a public type or config shape, update
  the affected docs and `profiles.example.toml` in the same PR.

## Releases

Crates.io publication is manual and requires authority distinct from merging,
creating a tag, or possessing a registry credential:

1. Configure the GitHub environment named exactly `crates-io` with at least one
   required reviewer, enable **Prevent self-review**, and store
   `CARGO_REGISTRY_TOKEN` as an environment secret rather than a repository-wide
   secret. Do not define a repository or organization secret with the same
   name: GitHub's expression syntax does not let the workflow prove which
   secret scope won precedence. The workflow refuses to proceed if reviewer
   protections are absent or unreadable.
2. Create the intended `vMAJOR.MINOR.PATCH` tag on the exact current `main`
   commit only after separate publication authorization has been requested.
3. Dispatch the Publish workflow from `main` and supply that existing tag as
   the required `release_tag` input. A different dispatch ref, stale `main`,
   dirty checkout, missing tag, or tag pointing anywhere else fails closed. The
   token-free preflight runs all checks and package verification first.
4. After preflight succeeds, a reviewer other than the dispatcher approves the
   `crates-io` deployment. The publish job revalidates its environment and exact
   source, then injects the registry token only into the final publish step.

Do not use a tag push as a release trigger, and do not test this mechanism by
performing a real upload. The hermetic authorization checks run with
`bash .github/scripts/test-release-gate.sh`.

## Branch model

This repository uses GitHub Flow rather than Git Flow. `main` is the single
integration branch and must remain releasable; there is no long-lived `dev`
branch. Make changes on short-lived topic branches, open them for independent
review, and delete them after a fast-forward or reviewed merge. Use a worktree
only when parallel changes need filesystem isolation, then remove it when its
branch is integrated. Release stability comes from the protected manual release
gate and exact tags, not from keeping an empty or divergent production branch.

## Licensing

By submitting a contribution you agree that it is dual-licensed under
**MIT OR Apache-2.0**, matching the project license, with no additional terms.
