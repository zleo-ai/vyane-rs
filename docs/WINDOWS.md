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
