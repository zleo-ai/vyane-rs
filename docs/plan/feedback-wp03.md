# WP-03 Feedback

## Codex custom-provider wire API is not represented in HarnessJob

`HarnessJob` carries `endpoint: Option<Endpoint>`, but `Endpoint` only contains
`base_url` and `auth`. The Codex CLI self-contained provider config can also
need a `model_providers.<name>.wire_api` value (`"chat"` or `"responses"`).

Within the frozen interface, `vyane-harness` defaults Codex custom endpoints to
`responses`, matching Codex's native path. If the kernel must support
Chat-Completions-only Codex endpoints, `HarnessJob` should carry the resolved
`Protocol` (or `Endpoint` should carry a wire API hint) so the harness can emit
the correct per-run `-c model_providers.<name>.wire_api=...` override.

## Known version-sensitive behaviors (needs real-CLI verification)

Claude Code sandbox behavior is version-sensitive and must be checked against
the real CLI before relying on it operationally:

- `Sandbox::Full` maps to `--dangerously-skip-permissions`. Some Claude Code
  versions or installations may require an additional opt-in such as
  `--allow-dangerously-skip-permissions` before that mode is accepted
  headlessly. Verification test:
  `real_claude_smoke_full_headless` via `cargo test -- --ignored`.
- `Sandbox::ReadOnly` intentionally passes no permission flag in headless print
  mode. Whether mutating tool attempts are denied automatically or would prompt
  on every supported Claude Code version is not yet verified. Verification
  test: `real_claude_smoke_read_only_headless` via `cargo test -- --ignored`.
