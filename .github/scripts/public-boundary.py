#!/usr/bin/env python3
"""Keep private maintainer context out of this public repository.

Checks every tracked file path and text file (symlinks by target), and
optionally pull request metadata passed through the environment:

    PR_TITLE, PR_BODY          failures when they carry private context
    PR_BASE_SHA, PR_HEAD_SHA   commits in base..head: messages and emails
    PR_HEAD_REF                reported as a warning only

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


# Placeholder account names that tests and docs may use in example paths.
SAMPLE_HOME_NAMES = (
    "u|user|username|runner|someone|example|me|you|alice|bob|name|test|tester|"
    "vyane|dev|ci|root|nobody"
)

CONTENT_RULES = (
    Rule(
        "internal-board-id",
        re.compile(r"(?<![A-Za-z0-9])EOS[-_]?\d{2,}(?![0-9])", re.IGNORECASE),
        "internal tracker IDs belong in the private tracker, not in public files",
    ),
    Rule(
        "private-home-path",
        re.compile(
            r"(?:/home|/Users)/(?!(?:" + SAMPLE_HOME_NAMES + r")(?:/|\b))[A-Za-z0-9._-]+(?:/|\b)"
            r"|(?:/home|/Users)/[^/\s]+[/]AIOS\b"
            r"|(?:~|\$HOME|\$\{HOME\})[/]AIOS\b"
            r"|\\\\wsl[$]"
        ),
        "use repository-relative paths, a placeholder account, or an environment variable",
    ),
    Rule(
        "private-system-name",
        re.compile(r"\bbeacon[.]sqlite\b|\bMeridian\b"),
        "private systems are not part of the public kernel",
    ),
    Rule(
        "personal-email",
        re.compile(r"[A-Za-z0-9._%+-]+@(?:qq|163|126|foxmail)[.]com\b", re.IGNORECASE),
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
        re.compile(r"(^|[/_.-])eos[-_]?\d{2,}(?=[_./-]|$)", re.IGNORECASE),
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


def read_text(path: Path, warnings: list[str]) -> str | None:
    """Return the text to scan, or None for binary files.

    A symlink is scanned by its target string, which is what git stores.
    Anything skipped for a reason other than being binary is reported.
    """
    try:
        if path.is_symlink():
            return os.readlink(path)
        if not path.is_file():
            return None
        if path.stat().st_size > MAX_TEXT_BYTES:
            warnings.append(f"{path}: larger than {MAX_TEXT_BYTES} bytes, not scanned")
            return None
        data = path.read_bytes()
    except OSError as err:
        warnings.append(f"{path}: unreadable ({err}), not scanned")
        return None
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        return data.decode("utf-16", "replace")
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


def scan_repository(root: Path, warnings: list[str]) -> list[Finding]:
    findings = []
    for rel in tracked_files(root):
        for rule in PATH_RULES:
            if rule.pattern.search(rel):
                findings.append(Finding(rel, rule, rel))
        text = read_text(root / rel, warnings)
        if text is not None:
            findings.extend(scan_text(rel, text))
    return findings


def scan_commits(root: Path, base: str, head: str) -> list[Finding]:
    """Scan messages and author/committer emails of the commits in a PR."""
    out = subprocess.run(
        ["git", "-C", str(root), "log", "--format=%H%x00%ae%x00%ce%x00%B%x1e", f"{base}..{head}"],
        check=True,
        capture_output=True,
    ).stdout.decode("utf-8", "replace")
    findings = []
    for record in out.split("\x1e"):
        record = record.strip("\n")
        if not record:
            continue
        sha, author, committer, body = record.split("\0", 3)
        where = f"commit {sha[:12]}"
        findings.extend(scan_text(f"{where} author", author))
        findings.extend(scan_text(f"{where} committer", committer))
        findings.extend(scan_text(f"{where} message", body))
    return findings


def scan_pull_request(root: Path, env: dict[str, str]) -> tuple[list[Finding], list[Finding]]:
    failures = []
    for key, label in (("PR_TITLE", "pull request title"), ("PR_BODY", "pull request body")):
        value = env.get(key, "")
        if value:
            failures.extend(scan_text(label, value))
    base, head = env.get("PR_BASE_SHA", ""), env.get("PR_HEAD_SHA", "")
    if base and head:
        failures.extend(scan_commits(root, base, head))
    warnings = []
    head_ref = env.get("PR_HEAD_REF", "")
    if head_ref:
        warnings.extend(scan_text("pull request branch", head_ref))
    return failures, warnings


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=REPO_ROOT)
    args = parser.parse_args(argv)

    root = args.root.resolve()
    skipped: list[str] = []
    failures = scan_repository(root, skipped)
    pr_failures, warnings = scan_pull_request(root, dict(os.environ))
    failures.extend(pr_failures)

    for message in skipped:
        print(f"::warning::{message}")
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
