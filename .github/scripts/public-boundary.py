#!/usr/bin/env python3
"""Keep private maintainer context out of this public repository.

Checks every tracked file path and text file, and optionally the pull request
title, body, and head branch passed through the environment:

    PR_TITLE, PR_BODY   failures when they carry private context
    PR_HEAD_REF         reported as a warning only

The patterns below are written so that this file never matches itself.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MAX_TEXT_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class Rule:
    name: str
    pattern: re.Pattern[str]
    hint: str


CONTENT_RULES = (
    Rule(
        "internal-board-id",
        re.compile(r"\bEOS-\d+\b", re.IGNORECASE),
        "internal tracker IDs belong in the private tracker, not in public files",
    ),
    Rule(
        "private-home-path",
        re.compile(r"/home/[^/\s]+/AIOS\b|~[/]AIOS\b"),
        "use repository-relative paths or a documented environment variable",
    ),
    Rule(
        "personal-email",
        re.compile(r"[A-Za-z0-9._%+-]+@qq\.com\b", re.IGNORECASE),
        "use the GitHub noreply address for public attribution",
    ),
)

PATH_RULES = (
    Rule(
        "delivery-note",
        re.compile(r"(^|/)DELIVER-[^/]*$", re.IGNORECASE),
        "agent delivery notes are hand-off material and must not be committed",
    ),
    Rule(
        "internal-board-id-in-path",
        re.compile(r"(^|[/_.-])eos[-_]?\d+", re.IGNORECASE),
        "rename the file without the internal tracker ID",
    ),
)


@dataclass(frozen=True)
class Finding:
    where: str
    rule: Rule
    excerpt: str

    def render(self) -> str:
        return f"{self.where}: [{self.rule.name}] {self.excerpt!r} — {self.rule.hint}"


def tracked_files(root: Path) -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z"],
        check=True,
        capture_output=True,
    ).stdout
    return [p for p in out.decode("utf-8", "surrogateescape").split("\0") if p]


def read_text(path: Path) -> str | None:
    try:
        if path.is_symlink() or not path.is_file():
            return None
        if path.stat().st_size > MAX_TEXT_BYTES:
            return None
        data = path.read_bytes()
    except OSError:
        return None
    if b"\0" in data:
        return None
    return data.decode("utf-8", "replace")


def scan_text(where: str, text: str) -> list[Finding]:
    findings = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        for rule in CONTENT_RULES:
            match = rule.pattern.search(line)
            if match:
                findings.append(Finding(f"{where}:{lineno}", rule, match.group(0)))
    return findings


def scan_repository(root: Path) -> list[Finding]:
    findings = []
    for rel in tracked_files(root):
        for rule in PATH_RULES:
            if rule.pattern.search(rel):
                findings.append(Finding(rel, rule, rel))
        text = read_text(root / rel)
        if text is not None:
            findings.extend(scan_text(rel, text))
    return findings


def scan_pull_request(env: dict[str, str]) -> tuple[list[Finding], list[Finding]]:
    failures = []
    for key, label in (("PR_TITLE", "pull request title"), ("PR_BODY", "pull request body")):
        value = env.get(key, "")
        if value:
            failures.extend(scan_text(label, value))
    warnings = []
    head_ref = env.get("PR_HEAD_REF", "")
    if head_ref:
        warnings.extend(scan_text("pull request branch", head_ref))
    return failures, warnings


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=REPO_ROOT)
    args = parser.parse_args(argv)

    failures = scan_repository(args.root.resolve())
    pr_failures, warnings = scan_pull_request(dict(os.environ))
    failures.extend(pr_failures)

    for finding in warnings:
        print(f"::warning::{finding.render()}")
    for finding in failures:
        print(f"::error::{finding.render()}")
    if failures:
        print(f"public boundary: {len(failures)} finding(s)", file=sys.stderr)
        return 1
    print("public boundary: clean")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
