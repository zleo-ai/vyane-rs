# CLI help and exit-code snapshots

`crates/vyane-cli/tests/cli_contract.rs` invokes the real `vyane` binary and
compares complete stdout, stderr and numeric exit codes with the checked-in
`tests/fixtures/cli-contract.json` fixture. Run it with:

```sh
cargo test --locked -p vyane-cli --test cli_contract
```

The initial fixture was captured from commit
`4ac9ac7f273cabdccd1530b429610ea0463bc299`. Its 30 cases cover root short/long
help, all 15 visible top-level command help pages, the static workflow-plan
help page, nine argument errors, an empty zero-limit JSON history result and
two history validation errors. Successful help and the empty query exit with
0 and write only stdout; argument errors exit with 2 and write only stderr;
the two application validation errors exit with 1 and write only stderr.

Each invocation has an isolated working directory, HOME, configuration and
data paths, a cleared environment, disabled color and a ten-second timeout.
An explicitly selected invalid configuration must remain unread by these
paths, and the fixture directory must remain unchanged. Coverage subprocesses
inherit only the collector's `LLVM_PROFILE_FILE` in addition to the fixed test
environment. No execution, model, daemon or remote endpoint is invoked.

These are Rust CLI regression contracts. They do not establish equivalence
with another implementation, freeze every nested command, or complete INT-01.
No runtime behavior is changed by this test slice. Intentional command or
wording changes require reviewing the corresponding fixture diff; the test
never rewrites snapshots or accepts new output automatically. When updating a
case, reproduce the command under the fixture environment, inspect the full
stdout/stderr and exit code, and explain the intended contract change in the PR.

For a test-only change, the unchanged baseline is expected to pass. Regression
sensitivity is checked by temporarily changing production help text, the
application-error exit code and the application-error output stream: each
mutation must fail this real-binary test, and restored source must pass.
Record those mutation commands and results with the delivery evidence.
