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

2026-07-07: CLOSED by adding `HarnessJob::protocol` and routing Codex custom
endpoints to `chat` or `responses`; `anthropic_messages` with `codex-cli` now
returns `Unsupported`.

## Known version-sensitive behaviors (needs real-CLI verification)

Claude Code sandbox behavior is version-sensitive and must be checked against
the real CLI before relying on it operationally:

- `Sandbox::Full` maps to `--dangerously-skip-permissions`. Some Claude Code
  versions or installations may require an additional opt-in such as
  `--allow-dangerously-skip-permissions` before that mode is accepted
  headlessly. Verification test:
  `real_claude_smoke_full_headless` via `cargo test -- --ignored`.

  2026-07-07: CLOSED, confirmed-safe. Verified against real, authenticated
  Claude Code `2.1.201 (Claude Code)` on macOS.
  `--dangerously-skip-permissions` works fully **standalone** — no
  `--allow-dangerously-skip-permissions` companion flag is required. Evidence:
  - `real_claude_smoke_full_headless` (`cargo test -p vyane-harness --
    --ignored real_claude`): passed, exit 0, 7.60s for all 3 real-CLI smokes.
  - Direct CLI check, `--dangerously-skip-permissions` alone,
    `--setting-sources project,local` (excludes this machine's personal
    `bypassPermissions` override so the flag is the only thing granting
    access): `claude -p 'Reply exactly: OK' --output-format json
    --dangerously-skip-permissions --setting-sources project,local` → exit 0,
    `"is_error":false`, `"result":"OK"`. A mutating-tool prompt under the same
    flag (`Create a file named mutation-test4.txt...`) also succeeded with
    `"permission_denials":[]` and the file was actually created — i.e. the
    flag grants full bypass alone, immediately, with no extra opt-in gate on
    this version. `--allow-dangerously-skip-permissions` is a different knob
    per `claude --help` ("Enable bypassing all permission checks as an
    option, without it being enabled by default") — it is not a required
    prerequisite for `--dangerously-skip-permissions` on the CLI.
  - No mapping change was needed in `sandbox_args` / `build_argv`.

- `Sandbox::ReadOnly` intentionally passes no permission flag in headless print
  mode. Whether mutating tool attempts are denied automatically or would prompt
  on every supported Claude Code version is not yet verified. Verification
  test: `real_claude_smoke_read_only_headless` via `cargo test -- --ignored`.

  2026-07-07: CLOSED, confirmed-safe. Verified against real, authenticated
  Claude Code `2.1.201 (Claude Code)` on macOS.
  A headless `-p` run with no permission flag never hangs on a
  tool-permission prompt — it always completes and exits — for read prompts,
  tool-using read prompts, and mutating-tool prompts alike. Evidence:
  - `real_claude_smoke_read_only_headless` (`cargo test -p vyane-harness --
    --ignored real_claude`): passed, exit 0.
  - Direct CLI checks (`claude -p ... --output-format json`), all exit 0,
    `is_error:false`, no hang, each completing in single-digit-to-teens of
    seconds:
    - Plain reply prompt: `"result":"OK"`.
    - Read-only tool prompt (`list the files in this directory`):
      `num_turns:2` (a tool was actually used), accurate listing returned,
      `"permission_denials":[]` — reads are auto-allowed, not merely
      non-hanging.
    - Mutating-tool prompt (`Create a file named ...`), tested under
      `--setting-sources project,local` to get Anthropic's actual shipped
      default rather than this machine's personal `bypassPermissions`
      override (see caveat below): `"permission_denials":[{"tool_name":
      "Write", ...}]`, file **not** created, model surfaces the denial as
      text ("I've requested permission to create ... Please approve the
      write to proceed.") and terminates normally — denied-and-completes, not
      denied-and-hangs. Same result for a `Bash`-tool mutation attempt.
  - **Caveat surfaced during verification, not a defect in the mapping**:
    this development machine's own `~/.claude/settings.json` sets
    `"permissions": {"defaultMode": "bypassPermissions"}` (a personal,
    machine-wide override, unrelated to `vyane-harness`). Under that ambient
    setting, "no permission flag" silently allows mutations too — confirmed
    by direct reproduction (`Create a file...` with no flag → file created,
    `permission_denials:[]`). This does not affect any Claude Code
    installation using Anthropic's shipped default (`defaultMode` unset),
    which denies-and-completes as described above. Anyone relying on
    `Sandbox::ReadOnly` for actual isolation must not also carry a personal
    `bypassPermissions` default in their own `~/.claude/settings.json` — the
    harness has no way to override a user-level Claude Code setting from
    argv, since there is no `--permission-mode default`/`"auto"`-as-shipped
    flag that out-forces a `bypassPermissions` user default back to
    Anthropic's factory default. This is a note for anyone standing up a
    `vyane-harness` ReadOnly sandbox on a machine with permissive personal
    Claude Code settings, not a mapping bug.
  - No mapping change was needed in `sandbox_args` / `build_argv`.
