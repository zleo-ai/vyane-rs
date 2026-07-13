#!/usr/bin/env python3
"""Fail-closed checks for the manual crates.io release workflow."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import NoReturn


RELEASE_TAG = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\Z")
HEX_SHA = re.compile(r"[0-9a-f]{40}\Z")


def fail(message: str) -> NoReturn:
    print(f"release gate: {message}", file=sys.stderr)
    raise SystemExit(1)


def load_environment(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        fail(f"cannot read environment response: {error}")


def verify_environment(path: Path, expected_name: str) -> None:
    document = load_environment(path)
    if not isinstance(document, dict):
        fail("environment response is not an object")
    if document.get("name") != expected_name:
        fail(f"environment name is not {expected_name!r}")

    rules = document.get("protection_rules")
    if not isinstance(rules, list):
        fail("environment protection_rules is missing or invalid")
    reviewer_rules = [
        rule
        for rule in rules
        if isinstance(rule, dict) and rule.get("type") == "required_reviewers"
    ]
    if len(reviewer_rules) != 1:
        fail("environment must have exactly one required_reviewers rule")

    rule = reviewer_rules[0]
    reviewers = rule.get("reviewers")
    if not isinstance(reviewers, list) or not reviewers:
        fail("environment required_reviewers rule has no reviewers")
    if not all(
        isinstance(entry, dict)
        and entry.get("type") in {"User", "Team"}
        and isinstance(entry.get("reviewer"), dict)
        and bool(entry["reviewer"])
        for entry in reviewers
    ):
        fail("environment required_reviewers entries are invalid")
    if rule.get("prevent_self_review") is not True:
        fail("environment must enable prevent_self_review")

    print(
        f"release gate: environment {expected_name!r} has required reviewers "
        "and prevents self-review"
    )


def git(*arguments: str) -> str:
    try:
        completed = subprocess.run(
            ["git", *arguments],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", "") or str(error)
        fail(f"git {' '.join(arguments)} failed: {detail.strip()}")
    return completed.stdout.strip()


def commit_for(revision: str) -> str:
    commit = git("rev-parse", "--verify", f"{revision}^{{commit}}")
    if not HEX_SHA.fullmatch(commit):
        fail(f"{revision!r} did not resolve to a full commit SHA")
    return commit


def verify_source(
    release_tag: str,
    event_name: str,
    github_ref: str,
    workflow_sha: str,
) -> None:
    if event_name != "workflow_dispatch":
        fail("publication is allowed only from workflow_dispatch")
    if github_ref != "refs/heads/main":
        fail("the release workflow must be dispatched from refs/heads/main")
    if not RELEASE_TAG.fullmatch(release_tag):
        fail("release_tag must have the exact form vMAJOR.MINOR.PATCH")
    if not HEX_SHA.fullmatch(workflow_sha):
        fail("GITHUB_SHA is not a full lowercase commit SHA")

    head = commit_for("HEAD")
    main = commit_for("refs/remotes/origin/main")
    tag = commit_for(f"refs/tags/{release_tag}")

    if workflow_sha != main:
        fail("the dispatched workflow SHA is not the fetched origin/main SHA")
    if head != workflow_sha:
        fail("the checked-out HEAD is not the dispatched workflow SHA")
    if tag != workflow_sha:
        fail("release_tag does not resolve to the exact dispatched main commit")
    if git("status", "--porcelain", "--untracked-files=all"):
        fail("release checkout is dirty")

    print(f"release gate: {release_tag} and origin/main resolve exactly to {workflow_sha}")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)

    environment = commands.add_parser("verify-environment")
    environment.add_argument("response", type=Path)
    environment.add_argument("--name", default="crates-io")

    source = commands.add_parser("verify-source")
    source.add_argument("--tag", required=True)
    source.add_argument("--event", required=True)
    source.add_argument("--ref", required=True)
    source.add_argument("--workflow-sha", required=True)
    return root


def main() -> None:
    arguments = parser().parse_args()
    if arguments.command == "verify-environment":
        verify_environment(arguments.response, arguments.name)
    else:
        verify_source(
            arguments.tag,
            arguments.event,
            arguments.ref,
            arguments.workflow_sha,
        )


if __name__ == "__main__":
    main()
