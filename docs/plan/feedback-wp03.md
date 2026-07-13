# WP-03 Feedback

## Codex custom-provider wire API is represented in `HarnessJob`

`HarnessJob` originally carried an optional endpoint without the resolved
protocol. A Codex custom provider can additionally require a
`model_providers.<name>.wire_api` value (`chat` or `responses`), so endpoint
configuration alone was insufficient.

This gap is closed: `HarnessJob::protocol` now routes Codex custom endpoints to
the matching wire API, while `anthropic_messages` with `codex-cli` fails closed
as unsupported.

## Version-sensitive Claude Code behavior

The CLI permission flags are upstream-version-sensitive and remain
adapter-delegated controls, not a host security boundary:

- `Sandbox::Full` maps to `--dangerously-skip-permissions`. Opt-in ignored
  smoke tests verify that the supported CLI version accepts the flag in
  headless mode. Compatibility with a newly installed CLI version must be
  rechecked before relying on it operationally.
- `Sandbox::ReadOnly` intentionally passes no permission flag. The supported
  default configuration denies mutating tool calls without hanging, but a
  permissive user-level CLI setting can change that behavior. The harness
  cannot reliably restore an upstream factory default through argv alone.

Therefore `Sandbox::ReadOnly` on this adapter must not be described as OS-level
filesystem confinement. Callers that require a security boundary need the
kernel capability gate plus a host-enforced sandbox; the ignored real-CLI smoke
tests provide compatibility evidence only and never consume persisted account
or machine-specific details as fixtures.
