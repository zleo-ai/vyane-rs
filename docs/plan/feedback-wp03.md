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
