#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/release-gate.py"
readonly PUBLISH_SCRIPT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/publish-workspace.sh"
readonly TEMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TEMP_ROOT"' EXIT

expect_rejected() {
  if "$@" >"$TEMP_ROOT/rejected.out" 2>"$TEMP_ROOT/rejected.err"; then
    echo "release gate test: unexpectedly accepted: $*" >&2
    exit 1
  fi
}

grep -Fq 'cargo publish --locked --no-verify --package "$crate"' "$PUBLISH_SCRIPT" || {
  echo "release gate test: token-bearing cargo publish must not rebuild package code" >&2
  exit 1
}

cat >"$TEMP_ROOT/environment-valid.json" <<'JSON'
{
  "name": "crates-io",
  "protection_rules": [
    {
      "type": "required_reviewers",
      "prevent_self_review": true,
      "reviewers": [{"type": "User", "reviewer": {"login": "reviewer"}}]
    }
  ]
}
JSON

python3 "$SCRIPT" verify-environment "$TEMP_ROOT/environment-valid.json"

for invalid in no-reviewers bad-reviewer duplicate-rules self-review wrong-name rules-not-list top-not-object malformed; do
  case "$invalid" in
    no-reviewers)
      payload='{"name":"crates-io","protection_rules":[]}'
      ;;
    bad-reviewer)
      payload='{"name":"crates-io","protection_rules":[{"type":"required_reviewers","prevent_self_review":true,"reviewers":[{}]}]}'
      ;;
    duplicate-rules)
      payload='{"name":"crates-io","protection_rules":[{"type":"required_reviewers","prevent_self_review":true,"reviewers":[{"type":"User","reviewer":{"login":"one"}}]},{"type":"required_reviewers","prevent_self_review":true,"reviewers":[{"type":"User","reviewer":{"login":"two"}}]}]}'
      ;;
    self-review)
      payload='{"name":"crates-io","protection_rules":[{"type":"required_reviewers","prevent_self_review":false,"reviewers":[{}]}]}'
      ;;
    wrong-name)
      payload='{"name":"production","protection_rules":[{"type":"required_reviewers","prevent_self_review":true,"reviewers":[{}]}]}'
      ;;
    rules-not-list)
      payload='{"name":"crates-io","protection_rules":{}}'
      ;;
    top-not-object)
      payload='[]'
      ;;
    malformed)
      payload='{'
      ;;
  esac
  printf '%s\n' "$payload" >"$TEMP_ROOT/environment-$invalid.json"
  expect_rejected python3 "$SCRIPT" verify-environment "$TEMP_ROOT/environment-$invalid.json"
done

git init --bare --quiet "$TEMP_ROOT/origin.git"
git init --quiet "$TEMP_ROOT/repository"
git -C "$TEMP_ROOT/repository" config user.name "Release Gate Test"
git -C "$TEMP_ROOT/repository" config user.email "release-gate@example.invalid"
git -C "$TEMP_ROOT/repository" checkout --quiet -b main
printf 'fixture\n' >"$TEMP_ROOT/repository/file.txt"
git -C "$TEMP_ROOT/repository" add file.txt
git -C "$TEMP_ROOT/repository" commit --quiet -m fixture
git -C "$TEMP_ROOT/repository" remote add origin "$TEMP_ROOT/origin.git"
git -C "$TEMP_ROOT/repository" tag v0.2.0
printf 'current\n' >>"$TEMP_ROOT/repository/file.txt"
git -C "$TEMP_ROOT/repository" commit --quiet -am current
git -C "$TEMP_ROOT/repository" tag v0.1.0
git -C "$TEMP_ROOT/repository" tag -a v0.1.1 -m "annotated release fixture"
git -C "$TEMP_ROOT/repository" push --quiet -u origin main --tags
git -C "$TEMP_ROOT/repository" fetch --quiet origin main --tags
readonly SHA="$(git -C "$TEMP_ROOT/repository" rev-parse HEAD)"

(
  cd "$TEMP_ROOT/repository"
  python3 "$SCRIPT" verify-source \
    --tag v0.1.0 \
    --event workflow_dispatch \
    --ref refs/heads/main \
    --workflow-sha "$SHA"
  python3 "$SCRIPT" verify-source \
    --tag v0.1.1 \
    --event workflow_dispatch \
    --ref refs/heads/main \
    --workflow-sha "$SHA"

  expect_rejected python3 "$SCRIPT" verify-source \
    --tag v0.1.0 --event push --ref refs/heads/main --workflow-sha "$SHA"
  expect_rejected python3 "$SCRIPT" verify-source \
    --tag v0.1.0 --event workflow_dispatch --ref refs/heads/dev --workflow-sha "$SHA"
  expect_rejected python3 "$SCRIPT" verify-source \
    --tag 'v0.1' --event workflow_dispatch --ref refs/heads/main --workflow-sha "$SHA"
  expect_rejected python3 "$SCRIPT" verify-source \
    --tag 'v01.1.1' --event workflow_dispatch --ref refs/heads/main --workflow-sha "$SHA"
  expect_rejected python3 "$SCRIPT" verify-source \
    --tag v9.9.9 --event workflow_dispatch --ref refs/heads/main --workflow-sha "$SHA"
  expect_rejected python3 "$SCRIPT" verify-source \
    --tag v0.1.0 --event workflow_dispatch --ref refs/heads/main --workflow-sha "${SHA^^}"

  expect_rejected python3 "$SCRIPT" verify-source \
    --tag v0.2.0 --event workflow_dispatch --ref refs/heads/main --workflow-sha "$SHA"

  git checkout --quiet --detach HEAD^
  expect_rejected python3 "$SCRIPT" verify-source \
    --tag v0.1.0 --event workflow_dispatch --ref refs/heads/main --workflow-sha "$SHA"
  git checkout --quiet main

  git update-ref refs/remotes/origin/main HEAD^
  expect_rejected python3 "$SCRIPT" verify-source \
    --tag v0.1.0 --event workflow_dispatch --ref refs/heads/main --workflow-sha "$SHA"
  git update-ref refs/remotes/origin/main "$SHA"

  printf 'untracked\n' >untracked.txt
  expect_rejected python3 "$SCRIPT" verify-source \
    --tag v0.1.0 --event workflow_dispatch --ref refs/heads/main --workflow-sha "$SHA"
  rm untracked.txt

  printf 'dirty\n' >>file.txt
  expect_rejected python3 "$SCRIPT" verify-source \
    --tag v0.1.0 --event workflow_dispatch --ref refs/heads/main --workflow-sha "$SHA"
)

echo "release gate tests: all fail-closed cases passed"
