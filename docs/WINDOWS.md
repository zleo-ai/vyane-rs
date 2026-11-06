# Windows build and test gate

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

The compilation/test gate does not imply full Windows feature support.
Unix process-group command verification, detached process control, Linux pinned
workdirs and native sandboxing still have platform restrictions. Basic storage
round trips do not establish Windows ACL or sandbox equivalence.

## Offline checks and platform coverage

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

Windows runs the applicable SQLite, JSONL, session CAS/lock, routing, direct
HTTP and CLI contracts. Platform exclusions remain explicit in source:

- Unix command-verifier success/error probes (including command-dependent
  pursuit and CLI lifecycle cases) require process-group cleanup and waitid.
  Windows instead checks inconclusive verification without marking criteria
  satisfied, and keeps lifecycle/store and manual-waiver tests.
- Detached latency, group cancellation, orphan PID detection and resident
  daemon lifecycle require Unix process identity/control. Windows daemon control
  reads fail closed and retain unverifiable sidecars.
- Symlink/permission probes use Unix APIs; Linux pinned-directory and native
  sandbox fixtures remain Linux-only. These exclusions do not establish
  Windows reparse-point, ACL or sandbox safety.

The Windows CI lane is a hard failure gate. Linux/macOS retain their existing
test process splits and Linux sandbox prerequisites.
