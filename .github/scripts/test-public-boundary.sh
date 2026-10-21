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
readonly SAMPLE_HOME_PATH="/home/""ubuntu/project"
readonly NO_PR=(env -u PR_TITLE -u PR_BODY -u PR_HEAD_REF -u PR_BASE_SHA -u PR_HEAD_SHA)

fresh_repo() {
  local dir="$TEMP_ROOT/$1"
  git init -q "$dir"
  printf 'clean public text\n' >"$dir/README.md"
  echo "$dir"
}

# The scanner reads the index, so staging is enough to make a file tracked.
stage_all() {
  git -C "$1" add -A
}

expect_clean() {
  if ! "${NO_PR[@]}" "$@" >"$TEMP_ROOT/out" 2>&1; then
    echo "public boundary test: expected clean: $*" >&2
    cat "$TEMP_ROOT/out" >&2
    exit 1
  fi
}

expect_rule() {
  local rule="$1"
  shift
  if "${NO_PR[@]}" "$@" >"$TEMP_ROOT/out" 2>&1; then
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
stage_all "$repo"
expect_clean python3 "$SCRIPT" --root "$repo"
expect_clean python3 "$SCRIPT"

# Content rules: each sample must trip exactly the named rule.
content_case() {
  local rule="$1" text="$2" file="${3:-notes.md}"
  local repo
  repo="$(fresh_repo "case$RANDOM$RANDOM")"
  printf '%s\n' "$text" >"$repo/$file"
  stage_all "$repo"
  expect_rule "$rule" python3 "$SCRIPT" --root "$repo"
}

content_case internal-board-id "fixes $BOARD_ID"
content_case internal-board-id "fixes eos""-1234 in lower case"
content_case internal-board-id "branch_EOS""_652 and more"
content_case internal-board-id "cargo test --test eos""652_p2"
content_case private-home-path "path=$HOME_PATH" run.sh
content_case private-home-path "cache=/home/""maintainer/.cache/x"
content_case private-home-path "mac=/Users/""maintainer/src"
content_case private-home-path "dir=~/""AIOS/vyane"
content_case private-home-path 'dir=$HOME/''AIOS/vyane'
content_case private-home-path 'share=\\''wsl$\Ubuntu'
content_case private-system-name "db=beacon"".sqlite"
content_case private-system-name "moved from Meri""dian"
content_case personal-email "author $MAIL" AUTHORS

# Placeholder prefixes do not exempt a different account name.
content_case private-home-path "/home/""runner-other/work"
content_case private-home-path "/Users/""runner.private/work"
content_case private-home-path "/home/""ubuntu-private/work"
content_case private-home-path "/home/""ubuntu.private/work"
content_case private-home-path "/home/""ubuntu_private/work"
content_case private-home-path "/home/""ubuntu/""AIOS/vyane"

# Placeholder accounts and ordinary words stay clean.
repo="$(fresh_repo placeholders)"
printf '%s\n' 'HOME=/home/u' '/Users/runner/work' 'eos_token_id = 2' 'EOS2 marker' '/home/''ubuntu' "$SAMPLE_HOME_PATH" >"$repo/notes.md"
stage_all "$repo"
expect_clean python3 "$SCRIPT" --root "$repo"

# An ordinary example home path also stays clean as a symlink target.
repo="$(fresh_repo samplehome-symlink)"
ln -s "$SAMPLE_HOME_PATH" "$repo/link"
stage_all "$repo"
expect_clean python3 "$SCRIPT" --root "$repo"

# A symlink is checked by its target; UTF-16 text is decoded.
repo="$(fresh_repo symlink)"
ln -s "$HOME_PATH" "$repo/link"
stage_all "$repo"
expect_rule private-home-path python3 "$SCRIPT" --root "$repo"

repo="$(fresh_repo utf16)"
printf 'fixes %s\n' "$BOARD_ID" | iconv -f UTF-8 -t UTF-16 >"$repo/notes.txt"
stage_all "$repo"
expect_rule internal-board-id python3 "$SCRIPT" --root "$repo"

# Oversized files are reported instead of silently skipped.
repo="$(fresh_repo large)"
head -c 3000000 /dev/zero | tr '\0' 'a' >"$repo/big.txt"
stage_all "$repo"
expect_clean python3 "$SCRIPT" --root "$repo"
grep -q '::warning::.*not scanned' "$TEMP_ROOT/out" || {
  echo "public boundary test: oversized file must be reported" >&2
  exit 1
}

# Untracked files are not part of the public tree.
repo="$(fresh_repo untracked)"
stage_all "$repo"
printf 'fixes %s\n' "$BOARD_ID" >"$repo/scratch.md"
expect_clean python3 "$SCRIPT" --root "$repo"

# Path rules.
repo="$(fresh_repo deliver)"
printf 'hand-off\n' >"$repo/DELIVER-RESULT.md"
stage_all "$repo"
expect_rule delivery-note python3 "$SCRIPT" --root "$repo"

repo="$(fresh_repo pathid)"
mkdir -p "$repo/tests"
printf '// test\n' >"$repo/tests/eos""652_p2.rs"
stage_all "$repo"
expect_rule internal-board-id-in-path python3 "$SCRIPT" --root "$repo"

repo="$(fresh_repo eostoken)"
mkdir -p "$repo/t"
printf '{}\n' >"$repo/t/v.eos2.json"
stage_all "$repo"
expect_clean python3 "$SCRIPT" --root "$repo"

# Pull request metadata: title, body and commits fail; the branch only warns.
repo="$(fresh_repo pr)"
stage_all "$repo"
expect_label() {
  grep -q -- "$1" "$TEMP_ROOT/out" || {
    echo "public boundary test: missing '$1' in output" >&2
    cat "$TEMP_ROOT/out" >&2
    exit 1
  }
}
expect_clean env PR_TITLE="docs: $SAMPLE_HOME_PATH" PR_BODY="Example: $SAMPLE_HOME_PATH" \
  python3 "$SCRIPT" --root "$repo"
expect_rule private-home-path env PR_BODY="/home/""ubuntu-private/work" python3 "$SCRIPT" --root "$repo"
expect_label "pull request body:"
expect_rule internal-board-id env PR_TITLE="feat($BOARD_ID): x" python3 "$SCRIPT" --root "$repo"
expect_label "pull request title:"
expect_rule private-home-path env PR_BODY="see $HOME_PATH" python3 "$SCRIPT" --root "$repo"
expect_label "pull request body:"
"${NO_PR[@]}" PR_HEAD_REF="codex/$BOARD_ID-x" \
  python3 "$SCRIPT" --root "$repo" >"$TEMP_ROOT/out" 2>&1 || {
  echo "public boundary test: branch name must only warn" >&2
  exit 1
}
expect_label "::warning::.*pull request branch"

# Commit messages and emails in base..head are checked.
git -C "$repo" -c user.name=t -c user.email=t@example.invalid commit -q -m "base"
base="$(git -C "$repo" rev-parse HEAD)"
git -C "$repo" -c user.name=t -c user.email=t@example.invalid commit -q --allow-empty -m "docs: $SAMPLE_HOME_PATH"
expect_clean env PR_BASE_SHA="$base" PR_HEAD_SHA="$(git -C "$repo" rev-parse HEAD)" \
  python3 "$SCRIPT" --root "$repo"
base="$(git -C "$repo" rev-parse HEAD)"
git -C "$repo" -c user.name=t -c user.email=t@example.invalid commit -q --allow-empty -m "path=/home/""ubuntu-private/work"
expect_rule private-home-path env PR_BASE_SHA="$base" PR_HEAD_SHA="$(git -C "$repo" rev-parse HEAD)" \
  python3 "$SCRIPT" --root "$repo"
expect_label "message:"
base="$(git -C "$repo" rev-parse HEAD)"
git -C "$repo" -c user.name=t -c user.email=t@example.invalid commit -q --allow-empty -m "fix($BOARD_ID): x"
expect_rule internal-board-id env PR_BASE_SHA="$base" PR_HEAD_SHA="$(git -C "$repo" rev-parse HEAD)" \
  python3 "$SCRIPT" --root "$repo"
expect_label "message:"
git -C "$repo" -c user.name=t -c user.email="$MAIL" commit -q --allow-empty -m "clean"
expect_rule personal-email env PR_BASE_SHA="$(git -C "$repo" rev-parse HEAD~1)" \
  PR_HEAD_SHA="$(git -C "$repo" rev-parse HEAD)" python3 "$SCRIPT" --root "$repo"
expect_label "author:"

echo "public boundary tests passed"
