# Windows quickstart and CI coverage

## Build with MSVC

Use stable Rust with the `x86_64-pc-windows-msvc` host, Visual Studio 2022
Build Tools (Desktop development with C++), and a Windows SDK. From PowerShell
in the checkout root:

```powershell
. ./scripts/windows-msvc.ps1
rustc -vV
cargo -V
cargo build --workspace --locked
```

The script discovers the installed tools with `vswhere`, enters the x64 VS
developer environment (including SDK libraries), and explicitly selects MSVC
`link.exe` for Cargo. It changes only the current process environment, never
the system PATH. Git's `usr/bin/link.exe` is GNU's file-link utility and cannot
link MSVC objects. Setting just a linker path without SDK/library discovery is
not sufficient. Missing prerequisites are reported; the script installs nothing.

## Preview routing offline in PowerShell

After building, run these commands from the checkout root. They use the
bundled [offline configuration](examples/offline.toml).

```powershell
$vyane = '.\target\debug\vyane.exe'
$config = '.\docs\examples\offline.toml'

& $vyane --config $config check --json
& $vyane --config $config route 'Explain this function' --json
```

The first command reports static configuration diagnostics without probing
harnesses. The second previews the routing decision. Look for the `offline`
profile in the check output and `"selection_key": "offline"` in the route
output. Both commands run without provider credentials or model requests and
consume no model quota. The example uses a deliberately invalid endpoint.
Use this configuration only for checks and routing previews.

## Test locally

After initializing MSVC, run the normal contributor checks:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --no-fail-fast --locked
```

CI isolates `TEMP`, `TMP`, `TMPDIR`, user configuration directories and
`VYANE_DATA_DIR` beneath a dedicated runner temporary root. For local tests,
set those variables to disposable directories too, while preserving
`CARGO_HOME` and `RUSTUP_HOME` so the installed compiler remains discoverable.
The portable native CLI fixture compiles a std-only Rust executable with the
installed `rustc`; it needs no vendor CLI, account or shell script.

Windows CI runs the workspace without `vyane-cli`, then runs that package with
`--test-threads=1`. CLI fixtures launch real binaries; overlapping those
processes can exhaust Windows memory commitment on large hosts. Use the same
two invocations if a local full-suite run reports OS error 1455 or
`STATUS_COMMITMENT_LIMIT`. Every applicable test still runs.
Under compiler memory pressure, set `$env:CARGO_BUILD_JOBS = '1'` in the
current shell to reduce peak build memory without changing system settings.

```powershell
cargo test --workspace --exclude vyane-cli --no-fail-fast --locked
if ($LASTEXITCODE -ne 0) { throw 'Workspace tests failed' }
cargo test -p vyane-cli --no-fail-fast --locked -- --test-threads=1
if ($LASTEXITCODE -ne 0) { throw 'CLI tests failed' }
```

## CI coverage and platform boundaries

The CI test matrix runs on Linux, macOS and Windows. The Windows lane is a
hard failure gate for the x64 MSVC workspace build and applicable tests,
including SQLite, JSONL, session CAS and locking, routing, direct HTTP and CLI
contracts. Linux and macOS use their own test splits.

- **Unix process control.** Command-verifier success and error probes require
  Unix process-group cleanup and `waitid`. Windows tests cover inconclusive
  verification, lifecycle/store behavior and manual waivers. Detached process
  control and the resident daemon lifecycle depend on Unix process identity
  and control. Windows daemon control reads fail closed and retain
  unverifiable sidecars.
- **Linux native isolation.** Pinned workdirs and native sandbox fixtures are
  Linux-only. Windows storage tests cover basic round trips. Windows
  reparse-point, ACL and sandbox safety are outside that coverage.
- **Workflow worktrees.** Workflow worktree isolation is unsupported on Windows.
