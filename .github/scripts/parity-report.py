#!/usr/bin/env python3
"""Validate public parity fixtures and print a deterministic offline report."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = REPO_ROOT / "docs" / "parity" / "fixtures" / "v1"
MANIFEST_PATH = FIXTURE_ROOT / "manifest.json"
CAPABILITY_MATRIX_PATH = REPO_ROOT / "docs" / "parity" / "ORIGINAL-VYANE-PARITY.md"
CAPABILITY_STATUS_BASELINE = {
    "total": 53,
    "implemented": 7,
    "partial": 25,
    "missing": 10,
    "different": 9,
    "planned": 2,
}
# Keep identity coverage independent of status counts: renaming one row must
# not silently replace a capability while retaining the same totals.
CAPABILITY_ID_BASELINE = frozenset(
    f"{domain}-{index:02d}"
    for domain, count in {
        "EXE": 9,
        "CON": 7,
        "COL": 6,
        "LED": 6,
        "GOV": 7,
        "QUA": 4,
        "OBS": 6,
        "INT": 8,
    }.items()
    for index in range(1, count + 1)
)
CAPABILITY_STATUS_CLAIMS = {
    CAPABILITY_MATRIX_PATH: re.compile(
        r"当前 (?P<total>\d+) 项计数为：\s*"
        r"implemented (?P<implemented>\d+)、partial (?P<partial>\d+)、"
        r"missing (?P<missing>\d+)、different (?P<different>\d+)、"
        r"planned (?P<planned>\d+)。"
    ),
    REPO_ROOT / "README.md": re.compile(
        r"tracks (?P<total>\d+) capabilities across eight domains: "
        r"(?P<implemented>\d+) implemented, (?P<partial>\d+) partial, "
        r"(?P<missing>\d+) missing, (?P<different>\d+) deliberately different "
        r"or awaiting a decision, and (?P<planned>\d+) planned\."
    ),
    REPO_ROOT / "README.zh-CN.md": re.compile(
        r"矩阵按 8 个域追踪 (?P<total>\d+) 个能力项："
        r"(?P<implemented>\d+) 个 implemented、(?P<partial>\d+) 个 partial、"
        r"(?P<missing>\d+) 个 missing、\s*(?P<different>\d+) 个刻意不同或待决策、"
        r"(?P<planned>\d+) 个 planned。"
    ),
    REPO_ROOT / "docs" / "ROADMAP.md": re.compile(
        r"the (?P<total>\d+) matrix items are (?P<implemented>\d+) implemented, "
        r"(?P<partial>\d+) partial, (?P<missing>\d+) missing, "
        r"(?P<different>\d+) different, and (?P<planned>\d+) planned\."
    ),
}


def fail(message: str) -> None:
    raise SystemExit(f"parity report: {message}")


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {path.relative_to(REPO_ROOT)}: {error}")
    if not isinstance(value, dict):
        fail(f"{path.relative_to(REPO_ROOT)} must contain a JSON object")
    return value


def require_keys(value: dict[str, Any], expected: set[str], context: str) -> None:
    actual = set(value)
    if actual != expected:
        fail(
            f"{context} keys differ: missing={sorted(expected - actual)}, "
            f"unknown={sorted(actual - expected)}"
        )


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as error:
        fail(f"cannot read {path.relative_to(REPO_ROOT)}: {error}")


def normalized_markdown(path: Path) -> str:
    return " ".join(read_text(path).replace("`", "").split())


def validate_capability_status_counts() -> None:
    matrix = read_text(CAPABILITY_MATRIX_PATH)
    counts = Counter()
    seen_ids: set[str] = set()
    for line in matrix.splitlines():
        match = re.match(r"^\| ([A-Z]+-\d+) \|.*?\| `([^`]+)` \|", line)
        if match is None:
            continue
        capability_id, status = match.groups()
        if capability_id in seen_ids:
            fail(f"capability matrix contains duplicate id {capability_id}")
        if status not in CAPABILITY_STATUS_BASELINE or status == "total":
            fail(f"capability {capability_id} has unknown status {status}")
        seen_ids.add(capability_id)
        counts[status] += 1

    if seen_ids != CAPABILITY_ID_BASELINE:
        fail(
            "capability matrix id set drifted: "
            f"missing={sorted(CAPABILITY_ID_BASELINE - seen_ids)}, "
            f"unknown={sorted(seen_ids - CAPABILITY_ID_BASELINE)}"
        )

    actual = {"total": len(seen_ids)}
    actual.update(
        {
            status: counts[status]
            for status in CAPABILITY_STATUS_BASELINE
            if status != "total"
        }
    )
    if actual != CAPABILITY_STATUS_BASELINE:
        fail(
            "capability matrix status counts drifted: "
            f"expected={CAPABILITY_STATUS_BASELINE}, actual={actual}"
        )

    for path, pattern in CAPABILITY_STATUS_CLAIMS.items():
        match = pattern.search(normalized_markdown(path))
        relative = path.relative_to(REPO_ROOT)
        if match is None:
            fail(f"capability status declaration is missing or malformed in {relative}")
        declared = {key: int(value) for key, value in match.groupdict().items()}
        if declared != CAPABILITY_STATUS_BASELINE:
            fail(
                f"capability status declaration drifted in {relative}: "
                f"expected={CAPABILITY_STATUS_BASELINE}, actual={declared}"
            )


def fixture_path(raw: object) -> Path:
    if not isinstance(raw, str):
        fail("normalized_fixture must be a string")
    path = Path(raw)
    if path.is_absolute() or ".." in path.parts:
        fail("normalized_fixture must be repository-relative without traversal")
    resolved = REPO_ROOT / path
    if resolved.parent != FIXTURE_ROOT:
        fail("normalized_fixture must stay in docs/parity/fixtures/v1")
    return resolved


C0_MANIFEST_PATH = FIXTURE_ROOT / "c0-manifest.json"
C0_DISPOSITIONS = {"aligned", "intentional_retain", "pending"}
C0_RUST_TEST = "crates/vyane-cli/tests/parity_c0.rs::c0_corpus_replays_offline"
FORBIDDEN_FIXTURE_SNIPPETS = (
    "beacon.sqlite",
    ".config/vyane",
    ".config\\vyane",
    "sk-",
    "api_key",
    "AKIA",
    "github_pat_",
    "BEGIN PRIVATE",
    "C:/Users/",
    "C:\\Users\\",
    "/Users/",
)


def validate_disposition(case: dict[str, Any]) -> None:
    case_id = case.get("id")
    if not isinstance(case_id, str) or not case_id:
        fail("fixture case id must be a non-empty string")
    disposition = case.get("disposition")
    blocker = case.get("blocker")
    raw = case.get("oracle_raw_output", case.get("sanitized_oracle_output"))
    normalized = case.get("normalized_oracle_output")
    rust = case.get("rust_output")
    if disposition == "exact":
        if blocker is not None or raw != rust or normalized != rust:
            fail(f"exact case {case_id} is not exactly equal")
    elif disposition == "normalized_exact":
        if blocker is not None or normalized != rust:
            fail(f"normalized_exact case {case_id} drifted")
    elif disposition == "open_difference":
        if normalized == rust:
            fail(f"open_difference case {case_id} is now equal")
        if not isinstance(blocker, str) or not blocker.startswith("BLOCKER "):
            fail(f"open_difference case {case_id} lacks a public blocker")
    elif disposition == "aligned":
        if blocker is not None or normalized != rust:
            fail(f"aligned case {case_id} drifted")
    elif disposition == "intentional_retain":
        if normalized == rust:
            fail(f"intentional_retain case {case_id} is now equal")
        if not isinstance(blocker, str) or not blocker.startswith("RETAIN "):
            fail(f"intentional_retain case {case_id} lacks a RETAIN blocker")
    elif disposition == "pending":
        if normalized == rust:
            fail(f"pending case {case_id} is now equal")
        if not isinstance(blocker, str) or not blocker.startswith("PENDING "):
            fail(f"pending case {case_id} lacks a PENDING blocker")
    else:
        fail(f"case {case_id} has unknown disposition")


def reject_private_snippets(path: Path, payload: str) -> None:
    for snippet in FORBIDDEN_FIXTURE_SNIPPETS:
        if snippet in payload:
            fail(f"{path.relative_to(REPO_ROOT)} contains forbidden snippet {snippet!r}")


def build_report() -> dict[str, Any]:
    validate_capability_status_counts()
    manifest = load_json(MANIFEST_PATH)
    require_keys(
        manifest,
        {"schema_version", "normalization_version", "reference", "suites"},
        "manifest",
    )
    if manifest["schema_version"] != 2:
        fail("manifest schema_version must be 2")
    if manifest["normalization_version"] != "vyane-cross-repo-v1":
        fail("manifest normalization_version is unsupported")
    reference = manifest["reference"]
    if not isinstance(reference, dict):
        fail("manifest reference must be an object")
    require_keys(reference, {"snapshot", "disclosure"}, "manifest reference")
    if reference != {
        "snapshot": "behavioral-baseline-v1",
        "disclosure": "sanitized_behavior_only",
    }:
        fail("manifest reference metadata drifted")
    suites = manifest.get("suites")
    if not isinstance(suites, list) or not suites:
        fail("manifest suites must be a non-empty list")

    report_suites: list[dict[str, Any]] = []
    seen_suites: set[str] = set()
    total = Counter()
    for suite in suites:
        if not isinstance(suite, dict):
            fail("manifest suite entries must be objects")
        require_keys(
            suite,
            {"id", "fixture_sha256", "scope", "normalized_fixture", "cases"},
            "manifest suite",
        )
        suite_id = suite.get("id")
        if not isinstance(suite_id, str) or not suite_id or suite_id in seen_suites:
            fail("manifest suite ids must be unique non-empty strings")
        seen_suites.add(suite_id)
        path = fixture_path(suite.get("normalized_fixture"))
        try:
            fixture_bytes = path.read_bytes()
        except OSError as error:
            fail(f"cannot read {path.relative_to(REPO_ROOT)}: {error}")
        digest = hashlib.sha256(fixture_bytes).hexdigest()
        if digest != suite.get("fixture_sha256"):
            fail(f"fixture digest drifted for {suite_id}")
        fixture = load_json(path)
        require_keys(
            fixture,
            {"schema_version", "suite", "normalization", "cases"},
            f"fixture {suite_id}",
        )
        if fixture["schema_version"] != 1:
            fail(f"fixture {suite_id} schema_version must be 1")
        if not isinstance(fixture["normalization"], dict) or not fixture["normalization"]:
            fail(f"fixture {suite_id} normalization must be a non-empty object")
        if fixture.get("suite") != suite_id:
            fail(f"fixture suite id drifted for {suite_id}")
        cases = fixture.get("cases")
        manifest_cases = suite.get("cases")
        if not isinstance(cases, list) or not isinstance(manifest_cases, list):
            fail(f"suite {suite_id} cases must be lists")
        fixture_by_id = {
            case.get("id"): case for case in cases if isinstance(case, dict)
        }
        manifest_by_id = {
            case.get("id"): case for case in manifest_cases if isinstance(case, dict)
        }
        if len(fixture_by_id) != len(cases) or len(manifest_by_id) != len(manifest_cases):
            fail(f"suite {suite_id} contains duplicate or malformed case ids")
        if fixture_by_id.keys() != manifest_by_id.keys():
            fail(f"manifest and fixture case sets differ for {suite_id}")

        counts = Counter()
        differences = []
        for case_id in sorted(fixture_by_id):
            case = fixture_by_id[case_id]
            manifest_case = manifest_by_id[case_id]
            require_keys(
                manifest_case,
                {"id", "disposition", "blocker"},
                f"manifest case {case_id}",
            )
            common = {
                "id",
                "oracle_locator",
                "normalized_oracle_output",
                "rust_output",
                "disposition",
                "blocker",
            }
            if "sanitized_oracle_output" in case:
                expected_case_keys = common | {
                    "input",
                    "sanitized_oracle_output",
                }
            else:
                expected_case_keys = common | {
                    "oracle_raw_output",
                    "rust_input",
                }
                if "rust_failover_eligible" in case:
                    expected_case_keys.add("rust_failover_eligible")
            require_keys(case, expected_case_keys, f"fixture case {case_id}")
            if case.get("disposition") != manifest_case.get("disposition"):
                fail(f"case disposition drifted for {case_id}")
            if case.get("blocker") != manifest_case.get("blocker"):
                fail(f"case blocker drifted for {case_id}")
            validate_disposition(case)
            disposition = case["disposition"]
            counts[disposition] += 1
            total[disposition] += 1
            if disposition == "open_difference":
                differences.append(
                    {"id": case_id, "blocker": case["blocker"]}
                )
        report_suites.append(
            {
                "id": suite_id,
                "fixture_sha256": digest,
                "cases": len(cases),
                "dispositions": dict(sorted(counts.items())),
                "open_differences": differences,
            }
        )

    validate_c0(report_suites, total)
    return {
        "schema_version": 1,
        "normalization_version": manifest.get("normalization_version"),
        "suites": report_suites,
        "totals": {
            "cases": sum(item["cases"] for item in report_suites),
            "dispositions": dict(sorted(total.items())),
        },
    }


def validate_c0(report_suites: list[dict[str, Any]], total: Counter) -> None:
    manifest = load_json(C0_MANIFEST_PATH)
    require_keys(
        manifest,
        {"schema_version", "normalization_version", "reference", "suites"},
        "c0 manifest",
    )
    if manifest["schema_version"] != 1:
        fail("c0 manifest schema_version must be 1")
    if manifest["normalization_version"] != "vyane-c0-contract-v1":
        fail("c0 manifest normalization_version is unsupported")
    reference = manifest["reference"]
    if reference != {
        "snapshot": "c0-contract-2026-11-05",
        "disclosure": "sanitized_behavior_only",
        "python_expectation": "authored_contract_pin",
    }:
        fail("c0 manifest reference metadata drifted")
    reject_private_snippets(C0_MANIFEST_PATH, C0_MANIFEST_PATH.read_text(encoding="utf-8"))
    suites = manifest["suites"]
    if not isinstance(suites, list) or not suites:
        fail("c0 manifest suites must be a non-empty list")
    seen = {suite["id"] for suite in report_suites}
    for suite in suites:
        if not isinstance(suite, dict):
            fail("c0 suite entries must be objects")
        require_keys(
            suite,
            {"id", "fixture_sha256", "scope", "normalized_fixture", "cases"},
            "c0 suite",
        )
        suite_id = suite["id"]
        if not isinstance(suite_id, str) or not suite_id or suite_id in seen:
            fail("c0 suite ids must be unique non-empty strings")
        seen.add(suite_id)
        path = fixture_path(suite["normalized_fixture"])
        try:
            fixture_bytes = path.read_bytes()
        except OSError as error:
            fail(f"cannot read {path.relative_to(REPO_ROOT)}: {error}")
        digest = hashlib.sha256(fixture_bytes).hexdigest()
        if digest != suite["fixture_sha256"]:
            fail(f"fixture digest drifted for {suite_id}")
        reject_private_snippets(path, fixture_bytes.decode("utf-8"))
        fixture = load_json(path)
        require_keys(
            fixture,
            {"schema_version", "suite", "normalization", "cases"},
            f"fixture {suite_id}",
        )
        if fixture["schema_version"] != 1 or fixture["suite"] != suite_id:
            fail(f"fixture identity drifted for {suite_id}")
        if not isinstance(fixture["normalization"], dict) or not fixture["normalization"]:
            fail(f"fixture {suite_id} normalization must be a non-empty object")
        cases = fixture["cases"]
        manifest_cases = suite["cases"]
        if not isinstance(cases, list) or not isinstance(manifest_cases, list):
            fail(f"suite {suite_id} cases must be lists")
        fixture_by_id = {case.get("id"): case for case in cases if isinstance(case, dict)}
        manifest_by_id = {
            case.get("id"): case for case in manifest_cases if isinstance(case, dict)
        }
        if len(fixture_by_id) != len(cases) or fixture_by_id.keys() != manifest_by_id.keys():
            fail(f"c0 manifest and fixture case sets differ for {suite_id}")
        counts: Counter = Counter()
        differences = []
        for case_id in sorted(fixture_by_id):
            case = fixture_by_id[case_id]
            manifest_case = manifest_by_id[case_id]
            require_keys(
                manifest_case,
                {"id", "disposition", "blocker"},
                f"c0 manifest case {case_id}",
            )
            require_keys(
                case,
                {
                    "id",
                    "input",
                    "oracle_locator",
                    "oracle_raw_output",
                    "normalized_oracle_output",
                    "rust_output",
                    "disposition",
                    "blocker",
                    "rust_test",
                },
                f"c0 fixture case {case_id}",
            )
            locator = case["oracle_locator"]
            if locator != {"fn": "authored_contract_pin", "args": {}}:
                fail(f"c0 case {case_id} must use the authored contract locator")
            if case["rust_test"] != C0_RUST_TEST:
                fail(f"c0 case {case_id} rust_test drifted")
            if case["disposition"] != manifest_case["disposition"]:
                fail(f"case disposition drifted for {case_id}")
            if case["blocker"] != manifest_case["blocker"]:
                fail(f"case blocker drifted for {case_id}")
            if case["disposition"] not in C0_DISPOSITIONS:
                fail(f"c0 case {case_id} has a baseline disposition")
            validate_disposition(case)
            counts[case["disposition"]] += 1
            total[case["disposition"]] += 1
            if case["disposition"] != "aligned":
                differences.append({"id": case_id, "blocker": case["blocker"]})
        report_suites.append(
            {
                "id": suite_id,
                "fixture_sha256": digest,
                "cases": len(cases),
                "dispositions": dict(sorted(counts.items())),
                "open_differences": differences,
            }
        )


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Public parity report",
        "",
        "| Suite | Cases | Exact | Normalized exact | Open differences | Aligned | Retained | Pending |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for suite in report["suites"]:
        counts = suite["dispositions"]
        lines.append(
            f"| `{suite['id']}` | {suite['cases']} | {counts.get('exact', 0)} | "
            f"{counts.get('normalized_exact', 0)} | {counts.get('open_difference', 0)} | "
            f"{counts.get('aligned', 0)} | {counts.get('intentional_retain', 0)} | "
            f"{counts.get('pending', 0)} |"
        )
    differences = [
        difference
        for suite in report["suites"]
        for difference in suite["open_differences"]
    ]
    if differences:
        lines.extend(["", "## Open differences", ""])
        lines.extend(
            f"- `{item['id']}`: {item['blocker']}" for item in differences
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-rust-test",
        action="store_true",
        help="validate stored fixtures only; do not recompute Rust behavior",
    )
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    args = parser.parse_args()

    if not args.skip_rust_test:
        cargo = shutil.which("cargo")
        if cargo is None:
            conventional = Path.home() / ".cargo" / "bin" / "cargo"
            if conventional.is_file():
                cargo = str(conventional)
            else:
                fail("cargo was not found on PATH or in the conventional user toolchain")
        for test_name in ("parity_manifest", "parity_c0"):
            completed = subprocess.run(
                [
                    cargo,
                    "test",
                    "-p",
                    "vyane-cli",
                    "--test",
                    test_name,
                    "--locked",
                ],
                cwd=REPO_ROOT,
                check=False,
            )
            if completed.returncode != 0:
                return completed.returncode

    report = build_report()
    if args.format == "markdown":
        sys.stdout.write(render_markdown(report))
    else:
        json.dump(report, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
