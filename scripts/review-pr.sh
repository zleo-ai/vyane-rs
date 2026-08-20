#!/usr/bin/env bash
# Local independent PR review for zleo-ai/vyane-rs.
#
# Purpose: run one independent review round on a developer machine that
# already has model credentials, and post the result as a GitHub review
# comment (event=COMMENT; verdict on the first line).
#
# When to run: after the PR is opened, and again before merge if HEAD moved.
# Not a GitHub Actions job — credentials stay off repository secrets, and
# this public repo does not attach a self-hosted runner.
#
# Strict tier depends on the maintainer-side vyane review pipeline
# (`VYANE_PROJECT`, overridable). External contributors cannot run it
# themselves; a maintainer runs that round on their behalf.
#
# Usage:
#   scripts/review-pr.sh <PR> [--tier light|standard|strict]
#                              [--reviewer claude|codex|grok]
#                              [--dry-run]
set -euo pipefail

# Developer shells (and some agent harnesses) export CLICOLOR_FORCE/FORCE_COLOR.
# gh would then paint --json output with ANSI even when redirected, which
# breaks JSON parsing. Pin a colorless, non-pager gh for this process.
export NO_COLOR=1
export CLICOLOR=0
export GH_PAGER=cat
unset CLICOLOR_FORCE FORCE_COLOR GH_FORCE_TTY || true

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly REPO="zleo-ai/vyane-rs"
readonly VYANE_PROJECT="${VYANE_PROJECT:-/home/maple/AIOS/vyane}"
readonly MAX_DIFF_BYTES=$((400 * 1024))
readonly GITHUB_BODY_LIMIT=60000

usage() {
  cat >&2 <<'EOF'
usage: scripts/review-pr.sh <PR> [--tier light|standard|strict] [--reviewer claude|codex|grok] [--dry-run]

Run one independent review round locally and post it as a PR review comment.

  <PR>                 Pull request number
  --tier               Override the path heuristic (light|standard|strict)
  --reviewer           Standard-tier CLI (default: claude; ignored on light/strict)
  --dry-run            Build the review body and print it; do not post

Trigger: after the PR is opened, before merge, on a machine with model credentials.
EOF
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

die_usage() {
  printf 'error: %s\n' "$*" >&2
  usage
  exit 2
}

# --- args -------------------------------------------------------------------

PR=""
TIER_OVERRIDE=""
REVIEWER="claude"
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      usage
      exit 0
      ;;
    --tier)
      [[ $# -ge 2 ]] || die_usage "--tier requires light|standard|strict"
      TIER_OVERRIDE="$2"
      shift 2
      ;;
    --tier=*)
      TIER_OVERRIDE="${1#--tier=}"
      shift
      ;;
    --reviewer)
      [[ $# -ge 2 ]] || die_usage "--reviewer requires claude|codex|grok"
      REVIEWER="$2"
      shift 2
      ;;
    --reviewer=*)
      REVIEWER="${1#--reviewer=}"
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --)
      shift
      break
      ;;
    -*)
      die_usage "unknown option: $1"
      ;;
    *)
      if [[ -z "$PR" ]]; then
        PR="$1"
        shift
      else
        die_usage "unexpected argument: $1"
      fi
      ;;
  esac
done

[[ -n "$PR" ]] || die_usage "missing PR number"
[[ "$PR" =~ ^[1-9][0-9]*$ ]] || die_usage "PR must be a positive integer, got ${PR@Q}"

case "$REVIEWER" in
  claude | codex | grok) ;;
  *) die_usage "--reviewer must be claude, codex, or grok" ;;
esac

if [[ -n "$TIER_OVERRIDE" ]]; then
  case "$TIER_OVERRIDE" in
    light | standard | strict) ;;
    *) die_usage "--tier must be light, standard, or strict" ;;
  esac
fi

command -v gh >/dev/null 2>&1 || die "gh CLI not found"
command -v python3 >/dev/null 2>&1 || die "python3 not found"

# --- workspace --------------------------------------------------------------

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/vyane-rs-review-pr.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

PR_JSON="${WORKDIR}/pr.json"
DIFF_FILE="${WORKDIR}/pr.diff"
PROMPT_FILE="${WORKDIR}/prompt.txt"
REVIEW_RAW="${WORKDIR}/reviewer.txt"
REVIEW_ERR="${WORKDIR}/reviewer.err"
BODY_FILE="${WORKDIR}/body.md"
STRICT_OUT="${WORKDIR}/vyane-review"

# --- fetch PR metadata ------------------------------------------------------

gh pr view "$PR" --repo "$REPO" --json title,body,headRefOid,baseRefName,files \
  >"$PR_JSON" || die "gh pr view failed for ${REPO}#${PR}"
[[ -s "$PR_JSON" ]] || die "gh pr view returned empty JSON for ${REPO}#${PR}"

eval "$(
  python3 - "$PR_JSON" <<'PY'
import json, re, shlex, sys
from pathlib import Path

raw = Path(sys.argv[1]).read_text(encoding="utf-8")
raw = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", raw)
data = json.loads(raw)
files = []
for entry in data.get("files") or []:
    if isinstance(entry, dict):
        path = entry.get("path")
        if path:
            files.append(path)


def emit(name: str, value: str) -> None:
    print(f"{name}={shlex.quote(value)}")


emit("PR_TITLE", data.get("title") or "")
emit("PR_BODY", data.get("body") or "")
emit("HEAD_SHA", data.get("headRefOid") or "")
emit("BASE_REF", data.get("baseRefName") or "")
print("PR_FILES=(")
for path in files:
    print(f"  {shlex.quote(path)}")
print(")")
PY
)"

[[ -n "$HEAD_SHA" ]] || die "PR ${PR} has no head SHA"

# --- path heuristic ---------------------------------------------------------
# Advisory only. Mixed PRs take the highest tier. Human/agent judgment wins.

path_is_light() {
  local p="$1"
  case "$p" in
    *.md | docs | docs/* | LICENSE*) return 0 ;;
  esac
  return 1
}

path_is_strict() {
  local p="$1"
  local lower="${p,,}"
  case "$p" in
    *publish.yml | .github/scripts/release* | .github/scripts/publish*)
      return 0
      ;;
  esac
  case "$lower" in
    *auth* | *credential* | *secret* | *token* | *sandbox* | *policy*)
      return 0
      ;;
  esac
  return 1
}

suggest_tier() {
  local path has_strict=0 has_non_light=0
  if [[ ${#PR_FILES[@]} -eq 0 ]]; then
    printf 'light'
    return
  fi
  for path in "${PR_FILES[@]}"; do
    if path_is_strict "$path"; then
      has_strict=1
    elif ! path_is_light "$path"; then
      has_non_light=1
    fi
  done
  if ((has_strict)); then
    printf 'strict'
  elif ((has_non_light)); then
    printf 'standard'
  else
    printf 'light'
  fi
}

tier_label_zh() {
  case "$1" in
    light) printf '轻量' ;;
    standard) printf '标准' ;;
    strict) printf '严格' ;;
    *) printf '%s' "$1" ;;
  esac
}

SUGGESTED_TIER="$(suggest_tier)"
TIER="${TIER_OVERRIDE:-$SUGGESTED_TIER}"
TIER_ZH="$(tier_label_zh "$TIER")"

echo "PR: ${REPO}#${PR}"
echo "HEAD: ${HEAD_SHA}"
echo "Base: ${BASE_REF}"
echo "Files (${#PR_FILES[@]}):"
if [[ ${#PR_FILES[@]} -eq 0 ]]; then
  echo "  (none)"
else
  printf '  %s\n' "${PR_FILES[@]}"
fi
echo "启发式建议档：${SUGGESTED_TIER}"
echo "启发式建议，最终档以人工/agent 判断为准，混合按最高档。"
if [[ -n "$TIER_OVERRIDE" ]]; then
  echo "采用档：${TIER}（--tier 覆盖）"
else
  echo "采用档：${TIER}"
fi

if [[ "$TIER" != "standard" && "$REVIEWER" != "claude" ]]; then
  echo "note: --reviewer is only used on standard tier; ignoring for ${TIER}" >&2
fi

# --- light ------------------------------------------------------------------

if [[ "$TIER" == "light" ]]; then
  echo "轻量档：CI 绿即合，无需独立审查"
  exit 0
fi

# --- diff (standard + strict posting metadata) ------------------------------

if ! gh pr diff "$PR" --repo "$REPO" >"$DIFF_FILE"; then
  die "gh pr diff failed for ${REPO}#${PR}"
fi
# Strip ANSI in case a parent environment forced colored gh output.
python3 - "$DIFF_FILE" <<'PY'
from pathlib import Path
import re, sys
path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8", errors="replace")
path.write_text(re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text), encoding="utf-8")
PY

DIFF_BYTES="$(wc -c <"$DIFF_FILE" | tr -d ' ')"
TRUNCATED=0
if [[ "$DIFF_BYTES" -gt "$MAX_DIFF_BYTES" ]]; then
  TRUNCATED=1
  head -c "$MAX_DIFF_BYTES" "$DIFF_FILE" >"${DIFF_FILE}.kept"
  mv "${DIFF_FILE}.kept" "$DIFF_FILE"
  {
    echo
    echo "[diff truncated: original ${DIFF_BYTES} bytes, prompt keeps first ${MAX_DIFF_BYTES} bytes (400KB) to avoid prompt explosion]"
  } >>"$DIFF_FILE"
  echo "note: diff truncated from ${DIFF_BYTES} to ${MAX_DIFF_BYTES} bytes" >&2
fi

extract_verdict() {
  local file="$1"
  local line
  line="$(
    tr -d '\r' <"$file" | grep -E '^结论：(APPROVE|REQUEST_CHANGES|COMMENT)$' | head -n1 || true
  )"
  if [[ -n "$line" ]]; then
    printf '%s' "${line#结论：}"
  else
    printf ''
  fi
}

wrap_body() {
  local verdict="$1"
  local reviewer_name="$2"
  local raw="$3"
  local extra_note="${4:-}"
  local findings="${WORKDIR}/findings.md"
  # Drop a leading 结论/档 pair so the wrapper owns the machine-readable header.
  python3 - "$raw" "$findings" <<'PY'
from pathlib import Path
import sys

src = Path(sys.argv[1]).read_text(encoding="utf-8")
lines = src.splitlines()
i = 0
if i < len(lines) and lines[i].startswith("结论："):
    i += 1
if i < len(lines) and lines[i].startswith("档："):
    i += 1
while i < len(lines) and lines[i].strip() == "":
    i += 1
Path(sys.argv[2]).write_text("\n".join(lines[i:]) + ("\n" if lines[i:] else ""), encoding="utf-8")
PY
  {
    echo "结论：${verdict}"
    echo "档：${TIER_ZH}"
    echo
    echo "- Reviewer: ${reviewer_name}"
    echo "- HEAD: \`${HEAD_SHA}\`"
    echo "- Tier: ${TIER} (heuristic suggested ${SUGGESTED_TIER}; advisory — human/agent judgment is authoritative; mixed PRs take the highest tier)"
    if ((TRUNCATED)); then
      echo "- Diff: truncated to 400KB for the review prompt (original ${DIFF_BYTES} bytes)"
    fi
    if [[ -n "$extra_note" ]]; then
      echo
      echo "$extra_note"
    fi
    echo
    echo '---'
    echo
    cat "$findings"
  } >"$BODY_FILE"
}

post_or_print() {
  if ((DRY_RUN)); then
    echo
    echo "===== dry-run: review body (not posted) ====="
    cat "$BODY_FILE"
    echo "===== end dry-run ====="
    return 0
  fi
  python3 - "$BODY_FILE" "$GITHUB_BODY_LIMIT" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
limit = int(sys.argv[2])
raw = path.read_bytes()
if len(raw) <= limit:
    raise SystemExit(0)
notice = "\n[review body truncated to GitHub's size limit]\n"
budget = max(0, limit - len(notice.encode("utf-8")))
# Slice by bytes, then drop a trailing incomplete UTF-8 sequence.
text = raw[:budget].decode("utf-8", errors="ignore")
path.write_text(text + notice, encoding="utf-8")
PY
  gh api "repos/${REPO}/pulls/${PR}/reviews" \
    -f event=COMMENT \
    -f "commit_id=${HEAD_SHA}" \
    -F "body=@${BODY_FILE}" \
    >/dev/null \
    || die "failed to post review comment on ${REPO}#${PR}"
  echo "posted COMMENT review on ${REPO}#${PR} (HEAD ${HEAD_SHA})"
}

require_cmd() {
  local name="$1"
  command -v "$name" >/dev/null 2>&1 || die "${name} CLI not found (no silent fallback)"
}

# --- standard ---------------------------------------------------------------

run_standard() {
  local contributing="${REPO_ROOT}/CONTRIBUTING.md"
  local claude_md="${REPO_ROOT}/CLAUDE.md"
  {
    cat <<EOF
你是 zleo-ai/vyane-rs 的独立代码审查者。只审查下面内嵌的 PR diff。
不要修改任何文件。不要调用工具；diff 与 CONTRIBUTING 已全部内嵌，直接写审查结果。

输出格式（必须遵守）：
- 第 1 行：结论：APPROVE  或  结论：REQUEST_CHANGES  或  结论：COMMENT
- 第 2 行：档：${TIER_ZH}
- 其后写 findings。每条 finding 必须包含：file:line、类别（规范合规 / Bug / 架构影响）、置信度 0-100、说明。
- 丢弃置信度 <75 的条目，不要写进 findings。
- 忽略 lint / 格式 / 纯类型问题（CI 负责）。
- 无合格 finding 时用 结论：APPROVE；有必须改的 bug 或明确违规用 结论：REQUEST_CHANGES；其余（仅 nit、仅 follow-up、不确定）用 结论：COMMENT。
- 不要伪造未观察到的问题。

三维度：
A. 规范合规 — 对照仓内 CONTRIBUTING.md（如下内嵌）。本仓若无 CLAUDE.md 则只对照 CONTRIBUTING 与 diff 内可见约定。
B. Bug 扫描 — 只看 diff 内变更，不把预存问题当成本 PR 引入。
C. 架构影响 — 是否破坏 crate 边界、kernel 对具体 client/harness 的隔离、向后兼容。

PR ${REPO}#${PR}
Title: ${PR_TITLE}
Base: ${BASE_REF}
HEAD: ${HEAD_SHA}

PR body:
EOF
    if [[ -n "$PR_BODY" ]]; then
      printf '%s\n' "$PR_BODY"
    else
      echo "(empty)"
    fi
    echo
    echo "----- CONTRIBUTING.md -----"
    if [[ -f "$contributing" ]]; then
      cat "$contributing"
    else
      echo "(CONTRIBUTING.md not found in repo root)"
    fi
    echo "----- end CONTRIBUTING.md -----"
    echo
    if [[ -f "$claude_md" ]]; then
      echo "----- CLAUDE.md -----"
      cat "$claude_md"
      echo "----- end CLAUDE.md -----"
      echo
    else
      echo "CLAUDE.md: not present in this repository."
      echo
    fi
    if ((TRUNCATED)); then
      echo "WARNING: the diff below is truncated to the first ${MAX_DIFF_BYTES} bytes (original ${DIFF_BYTES} bytes)."
      echo
    fi
    echo "----- PR DIFF -----"
    cat "$DIFF_FILE"
    echo "----- end PR DIFF -----"
  } >"$PROMPT_FILE"

  case "$REVIEWER" in
    claude)
      require_cmd claude
      set +e
      claude -p --output-format text <"$PROMPT_FILE" \
        >"$REVIEW_RAW" 2>"$REVIEW_ERR"
      local status=$?
      set -e
      if [[ $status -ne 0 ]]; then
        echo "error: claude exited ${status} (no silent fallback)" >&2
        cat "$REVIEW_ERR" >&2 || true
        exit 1
      fi
      ;;
    codex)
      require_cmd codex
      set +e
      # `codex exec --help`: PROMPT omitted or `-` reads instructions from
      # stdin. Do not pass both a prompt argument and a pipe — stdin would
      # then be appended as a `<stdin>` block instead of replacing argv.
      # --output-last-message captures the review text instead of event noise.
      codex exec --sandbox read-only --color never \
        --output-last-message "$REVIEW_RAW" \
        - <"$PROMPT_FILE" \
        >"${WORKDIR}/codex.stdout" 2>"$REVIEW_ERR"
      local status=$?
      set -e
      if [[ $status -ne 0 ]]; then
        echo "error: codex exited ${status} (no silent fallback)" >&2
        cat "$REVIEW_ERR" >&2 || true
        cat "${WORKDIR}/codex.stdout" >&2 || true
        exit 1
      fi
      if [[ ! -s "$REVIEW_RAW" && -s "${WORKDIR}/codex.stdout" ]]; then
        cp "${WORKDIR}/codex.stdout" "$REVIEW_RAW"
      fi
      ;;
    grok)
      require_cmd grok
      # Specified shape is `grok --prompt-file … --output-format plain --max-turns 1`.
      # In practice grok still has always-on MCP meta-tools; a one-turn cap then
      # exits 1 with "max turns reached" after the first tool call. Allow a
      # few turns, strip the default filesystem/web/agent tools, and fail loud
      # if grok still returns non-zero.
      set +e
      grok --prompt-file "$PROMPT_FILE" --output-format plain --max-turns 8 \
        --no-subagents --disable-web-search \
        --tools "todo_write" \
        --disallowed-tools "todo_write,run_terminal_cmd,grep,read_file,search_replace,list_dir,web_search,web_fetch,task,Agent" \
        >"$REVIEW_RAW" 2>"$REVIEW_ERR"
      local status=$?
      set -e
      if [[ $status -ne 0 ]]; then
        echo "error: grok exited ${status} (no silent fallback)" >&2
        cat "$REVIEW_ERR" >&2 || true
        exit 1
      fi
      ;;
  esac

  if [[ ! -s "$REVIEW_RAW" ]]; then
    die "${REVIEWER} produced empty review output (no silent fallback)"
  fi

  local verdict extra=""
  verdict="$(extract_verdict "$REVIEW_RAW")"
  if [[ -z "$verdict" ]]; then
    verdict="COMMENT"
    extra="审查输出未给出首行 \`结论：APPROVE|REQUEST_CHANGES|COMMENT\`；未伪造通过/阻断，按 COMMENT 记录。"
  fi
  wrap_body "$verdict" "$REVIEWER" "$REVIEW_RAW" "$extra"
  post_or_print
}

# --- strict -----------------------------------------------------------------

run_strict() {
  echo "严格档要求对抗式多轮审查，且开发 / 审查 / 合并必须是三次独立的 agent run；本脚本只承担其中一轮。"
  command -v uv >/dev/null 2>&1 || die "uv not found (required for strict-tier vyane review)"
  [[ -d "$VYANE_PROJECT" ]] || die "vyane project not found: ${VYANE_PROJECT}"
  mkdir -p "$STRICT_OUT"

  set +e
  uv run --project "$VYANE_PROJECT" vyane review \
    --pr "$PR" \
    --workdir "$REPO_ROOT" \
    --output "$STRICT_OUT" \
    >"${WORKDIR}/vyane.stdout" 2>"${WORKDIR}/vyane.stderr"
  local status=$?
  set -e

  if [[ -s "${WORKDIR}/vyane.stderr" ]]; then
    cat "${WORKDIR}/vyane.stderr" >&2
  fi

  case "$status" in
    0 | 1)
      echo "vyane review exited ${status} (0=clean, 1=findings)"
      ;;
    2)
      die "vyane review exited 2 (pipeline error); not posting"
      ;;
    3)
      die "vyane review exited 3 (partial/degraded); not posting"
      ;;
    *)
      die "vyane review exited ${status} (not a completed 0/1 run); not posting"
      ;;
  esac

  local md="${STRICT_OUT}/review.md"
  if [[ ! -f "$md" ]]; then
    if [[ -s "${WORKDIR}/vyane.stdout" ]]; then
      cp "${WORKDIR}/vyane.stdout" "$md"
    else
      die "vyane review produced no review.md (no silent fallback)"
    fi
  fi

  local verdict="APPROVE"
  if [[ "$status" -eq 1 ]]; then
    verdict="REQUEST_CHANGES"
  fi
  wrap_body "$verdict" "vyane review" "$md" \
    "严格档要求对抗式多轮 + 开发/审查/合并三分离；脚本只承担其中一轮。"
  post_or_print
}

case "$TIER" in
  standard) run_standard ;;
  strict) run_strict ;;
  *) die "internal: unexpected tier ${TIER}" ;;
esac
