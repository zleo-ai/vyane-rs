# WP-06 Feedback

## Frozen API gaps found while assembling the CLI

- `vyane-harness` currently exports only crate docs, not concrete Claude Code
  and Codex CLI harness types or constructors. WP-06 therefore has to provide
  local CLI harness wrappers inside `vyane-cli` to satisfy the assembler role.
  Those wrappers implement the frozen `vyane_core::Harness` trait and use
  `EnvPolicy::build`, but the intended reusable harness implementations should
  live in `vyane-harness`.
- `Dispatcher::dispatch` and `Dispatcher::broadcast` return `RunRecord` values
  without the successful answer text. WP-06 needs to print the answer in human
  mode and include it in JSON mode, so the CLI captures adapter output in
  wrapper clients/harnesses. A kernel return type that carries both
  `RunRecord` and `Option<String>` would remove this extra capture layer and
  would be safer for duplicate targets in broadcast.
