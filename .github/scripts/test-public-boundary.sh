#!/usr/bin/env bash
# Exercises public-boundary.py against throwaway repositories. Violating
# strings are assembled at runtime so this file never trips the gate itself.
set -euo pipefail

readonly SCRIPT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/public-boundary.py"
readonly TEMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TEMP_ROOT"' EXIT

readonly BOARD_ID="EOS""-1234"
readonly HOME_PATH="/home/someone/""AIOS/vyane"
readonly MAIL="someone@""qq.com"

fresh_repo() {
  local dir="$TEMP_ROOT/$1"
  git init -q "$dir"
  printf 'clean public text\n' >"$dir/README.md"
  echo "$dir"
}

commit_all() {
  git -C "$1" add -A
}

expect_clean() {
  if ! env -u PR_TITLE -u PR_BODY -u PR_HEAD_REF "$@" >"$TEMP_ROOT/out" 2>&1; then
    echo "public boundary test: expected clean: $*" >&2
    cat "$TEMP_ROOT/out" >&2
    exit 1
  fi
}

expect_rule() {
  local rule="$1"
  shift
  if "$@" >"$TEMP_ROOT/out" 2>&1; then
    echo "public boundary test: expected [$rule] failure: $*" >&2
    exit 1
  fi
  grep -q -- "\[$rule\]" "$TEMP_ROOT/out" || {
    echo "public boundary test: missing [$rule] in output" >&2
    cat "$TEMP_ROOT/out" >&2
    exit 1
  }
}

# A clean tree passes, and so does this repository.
repo="$(fresh_repo clean)"
commit_all "$repo"
expect_clean python3 "$SCRIPT" --root "$repo"
expect_clean python3 "$SCRIPT"

# Content rules.
repo="$(fresh_repo board)"
printf 'fixes %s\n' "$BOARD_ID" >"$repo/notes.md"
commit_all "$repo"
expect_rule internal-board-id env -u PR_TITLE -u PR_BODY python3 "$SCRIPT" --root "$repo"

repo="$(fresh_repo home)"
printf 'path=%s\n' "$HOME_PATH" >"$repo/run.sh"
commit_all "$repo"
expect_rule private-home-path env -u PR_TITLE -u PR_BODY python3 "$SCRIPT" --root "$repo"

repo="$(fresh_repo mail)"
printf 'author %s\n' "$MAIL" >"$repo/AUTHORS"
commit_all "$repo"
expect_rule personal-email env -u PR_TITLE -u PR_BODY python3 "$SCRIPT" --root "$repo"

# Untracked files are not part of the public tree.
repo="$(fresh_repo untracked)"
commit_all "$repo"
printf 'fixes %s\n' "$BOARD_ID" >"$repo/scratch.md"
expect_clean python3 "$SCRIPT" --root "$repo"

# Path rules.
repo="$(fresh_repo deliver)"
printf 'hand-off\n' >"$repo/DELIVER-RESULT.md"
commit_all "$repo"
expect_rule delivery-note env -u PR_TITLE -u PR_BODY python3 "$SCRIPT" --root "$repo"

repo="$(fresh_repo pathid)"
mkdir -p "$repo/tests"
printf '// test\n' >"$repo/tests/eos""652_p2.rs"
commit_all "$repo"
expect_rule internal-board-id-in-path env -u PR_TITLE -u PR_BODY python3 "$SCRIPT" --root "$repo"

# Pull request metadata: title and body fail, branch only warns.
repo="$(fresh_repo pr)"
commit_all "$repo"
expect_rule internal-board-id env PR_TITLE="feat($BOARD_ID): x" python3 "$SCRIPT" --root "$repo"
expect_rule private-home-path env PR_BODY="see $HOME_PATH" python3 "$SCRIPT" --root "$repo"
env -u PR_TITLE -u PR_BODY PR_HEAD_REF="codex/$BOARD_ID-x" \
  python3 "$SCRIPT" --root "$repo" >"$TEMP_ROOT/out" 2>&1 || {
  echo "public boundary test: branch name must only warn" >&2
  exit 1
}
grep -q '::warning::' "$TEMP_ROOT/out" || {
  echo "public boundary test: branch name warning missing" >&2
  exit 1
}

echo "public boundary tests passed"
