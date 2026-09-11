#!/usr/bin/env bash
# Author-run evidence for EOS-690 vyane-rs agent entry. CI is not Done.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
fail=0
for f in AGENTS.md CLAUDE.md; do
  if [[ ! -f "$f" ]]; then
    echo "MISSING $f"
    fail=1
  else
    echo "OK exists $f"
  fi
done
if ! grep -q '@AGENTS.md' CLAUDE.md; then
  echo "FAIL CLAUDE.md must import AGENTS.md"
  fail=1
else
  echo "OK CLAUDE.md imports AGENTS.md"
fi
if ! grep -q 'AGENTS.md' README.md; then
  echo "FAIL README.md must point at AGENTS.md"
  fail=1
else
  echo "OK README.md points at AGENTS.md"
fi
# Local $GIT_DIR/info/exclude used to list /AGENTS.md, so an unforced
# `git add` left the entry untracked. Existence on disk is not enough.
if ! git ls-files --error-unmatch AGENTS.md >/dev/null 2>&1; then
  echo "FAIL AGENTS.md is not tracked (git add -f; check info/exclude)"
  fail=1
else
  echo "OK AGENTS.md is tracked"
fi
if ! git ls-files --error-unmatch CLAUDE.md >/dev/null 2>&1; then
  echo "FAIL CLAUDE.md is not tracked"
  fail=1
else
  echo "OK CLAUDE.md is tracked"
fi
# Public-repo hygiene: no private device/effort facts.
if grep -E -n '/home/maple|\\\\wsl\$|~/AIOS/|默认 xhigh|事实源在 Meridian|beacon\.sqlite|hzlhu@qq\.com' AGENTS.md CLAUDE.md; then
  echo "FAIL forbidden private/stale phrases"
  fail=1
else
  echo "OK no private path / xhigh default / Meridian fact-source / email"
fi
if git cat-file -e origin/main:AGENTS.md 2>/dev/null; then
  echo "NOTE origin/main already has AGENTS.md (gap closed on remote)"
else
  echo "OK origin/main still lacks AGENTS.md (this PR is the gap)"
fi
exit "$fail"
