# WP-10 Feedback

## `EnvPolicy::BASELINE_ENV` (vyane-core) has no proxy passthrough, which can make the real-CLI smoke tests fail on a machine that requires an HTTP(S) proxy for outbound network access

`crates/vyane-core/src/env.rs`'s `BASELINE_ENV` allowlist (`PATH`, `HOME`,
`USER`, `SHELL`, `TERM`, `LANG`, `LC_ALL`, `LC_CTYPE`, `TMPDIR`, `TZ`,
`XDG_CONFIG_HOME`, `XDG_DATA_HOME`, `XDG_CACHE_HOME`) does not include
`HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY`. This is unrelated to the two
sandbox-flag behaviors WP-10 was scoped to verify, but it surfaced while
running them for real.

On a development machine that requires a proxy for outbound network access,
`ClaudeCodeHarness::run` with the default `EnvPolicy::scrubbed()` cannot reach
the Anthropic API at all — auth itself fails, before any permission-mode
behavior is exercised. Reproduced directly:

```
env -i PATH="$PATH" HOME="$HOME" USER="$USER" SHELL="$SHELL" TERM="$TERM" \
    LANG="$LANG" TMPDIR="$TMPDIR" \
    claude -p 'Reply exactly: T1' --output-format json
# → exit 1, "is_error":true, "result":"Failed to authenticate. API Error:
#   403 Request not allowed", "api_error_status":403
```

Adding only `HTTPS_PROXY`/`HTTP_PROXY`/`NO_PROXY` on top of the same baseline
fixes it (confirmed reproducible, re-tested back to back, not flaky):

```
env -i PATH="$PATH" HOME="$HOME" USER="$USER" SHELL="$SHELL" TERM="$TERM" \
    LANG="$LANG" TMPDIR="$TMPDIR" \
    HTTPS_PROXY="$HTTPS_PROXY" HTTP_PROXY="$HTTP_PROXY" NO_PROXY="$NO_PROXY" \
    claude -p 'Reply exactly: T3' --output-format json
# → exit 0, "is_error":false, "result":"T3"
```

This is a real, environment-dependent gap in the frozen scrub baseline, not a
bug in `vyane-harness`'s Claude Code mapping. `vyane-harness` already exposes
the sanctioned per-job escape hatch (`EnvPolicy.allow: Vec<String>`, a public
field on the frozen `EnvPolicy` type), so WP-10 worked around it **without any
`vyane-core` edit** by widening `allow` on a small `real_cli_job()` test
helper local to `crates/vyane-harness/tests/fake_cli.rs`, used only by the two
real-CLI smoke tests — not by `base_job()` generally, so no other test's
environment changed. Both real-CLI smokes then passed cleanly (see
`feedback-wp03.md` for the sandbox-flag verdicts this unblocked).

If `vyane-harness` (or any downstream caller) should behave sanely by default
on proxy-only networks without every caller having to know to widen `allow`,
`BASELINE_ENV` is the natural place to add `HTTP_PROXY` / `HTTPS_PROXY` /
`NO_PROXY` (all three are read-only network routing hints, not credentials —
consistent with the existing baseline's rationale of "everything a
well-behaved CLI needs to start, nothing that redirects model traffic"; a
proxy redirects transport, not model/auth destination, and per-run `inject`
still wins over anything env-scrubbed). That edit is out of `vyane-harness`'s
frozen-crate boundary for this work package, so it is recorded here rather
than applied.
