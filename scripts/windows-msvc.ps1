# Dot-source from PowerShell before running Cargo. Changes only this process.
$ErrorActionPreference = 'Stop'
$vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio/Installer/vswhere.exe'
if (-not (Test-Path -LiteralPath $vswhere)) {
    throw 'Install Visual Studio Build Tools with Desktop development with C++ and a Windows SDK.'
}
$installation = & $vswhere -latest -products '*' -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
if ($LASTEXITCODE -ne 0 -or -not $installation) {
    throw 'No Visual Studio installation with the MSVC x64 tools was found.'
}
Import-Module (Join-Path $installation 'Common7/Tools/Microsoft.VisualStudio.DevShell.dll')
# Some developer-shell SDK discovery steps invoke vswhere by name.
$env:PATH = "$(Split-Path -Parent $vswhere);$env:PATH"
Enter-VsDevShell -VsInstallPath $installation -SkipAutomaticLocation -DevCmdArguments '-arch=amd64 -host_arch=amd64' | Out-Null
$linker = Join-Path $env:VCToolsInstallDir 'bin/Hostx64/x64/link.exe'
if (-not (Test-Path -LiteralPath $linker) -or -not $env:WindowsSdkDir) {
    throw 'The MSVC x64 linker and Windows SDK must both be available.'
}
# Pin the linker too: Git usr/bin/link.exe is a different program.
$env:CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER = $linker
& $linker /? | Select-Object -First 2
if ($LASTEXITCODE -ne 0) { throw 'MSVC linker verification failed.' }
