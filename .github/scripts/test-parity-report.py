#!/usr/bin/env python3
"""Exercise the offline parity report against isolated public evidence copies."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
REPORT_SCRIPT = REPO_ROOT / ".github" / "scripts" / "parity-report.py"


class CapabilityIdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="parity-report-tests-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for relative in ("README.md", "README.zh-CN.md", "docs/ROADMAP.md"):
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(REPO_ROOT / relative, target)
        shutil.copytree(REPO_ROOT / "docs/parity", self.root / "docs/parity")
        self.script = self.root / ".github/scripts/parity-report.py"
        self.script.parent.mkdir(parents=True)
        shutil.copyfile(REPORT_SCRIPT, self.script)
        self.matrix = self.root / "docs/parity/ORIGINAL-VYANE-PARITY.md"

    def report(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(self.script), "--skip-rust-test", "--format", "json"],
            capture_output=True,
            text=True,
            check=False,
        )

    def replace_id(self, replacement: str) -> None:
        text = self.matrix.read_text(encoding="utf-8")
        original = "| EXE-01 |"
        self.assertEqual(text.count(original), 1)
        self.matrix.write_text(text.replace(original, f"| {replacement} |"), encoding="utf-8")

    def assert_report_unchanged(self) -> None:
        result = self.report()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        baseline_ids = [
            "routing",
            "automatic_routing",
            "failover",
            "target_resolution",
            "dispatch_trail",
        ]
        self.assertEqual(
            [suite["id"] for suite in report["suites"][:5]],
            baseline_ids,
        )
        baseline = {"normalized_exact": 0, "open_difference": 0}
        baseline_cases = 0
        for suite in report["suites"][:5]:
            baseline_cases += suite["cases"]
            for name, count in suite["dispositions"].items():
                self.assertIn(name, baseline)
                baseline[name] += count
        self.assertEqual(baseline_cases, 44)
        self.assertEqual(baseline, {"normalized_exact": 30, "open_difference": 14})
        c0_ids = []
        for suite in report["suites"][5:]:
            c0_ids.append(suite["id"])
            self.assertGreater(suite["cases"], 0)
            self.assertTrue(
                set(suite["dispositions"]) <= {"aligned", "intentional_retain", "pending"}
            )
        self.assertEqual(
            c0_ids,
            ["cli_error", "selector", "effort", "usage", "session", "workflow"],
        )
        self.assertEqual(report["totals"]["cases"], sum(suite["cases"] for suite in report["suites"]))

    def assert_id_rejected(self, replacement: str) -> None:
        self.replace_id(replacement)
        result = self.report()
        self.assertNotEqual(result.returncode, 0, "same-count replacement was accepted")
        self.assertEqual(result.stdout, "", "invalid matrix emitted a successful report")
        self.assertIn("capability matrix id set drifted", result.stderr)
        self.assertIn("EXE-01", result.stderr)
        self.assertIn(replacement, result.stderr)

    def test_current_public_evidence_still_reports_44_cases(self) -> None:
        self.assert_report_unchanged()

    def test_same_domain_unknown_number_cannot_replace_a_capability(self) -> None:
        self.assert_id_rejected("EXE-99")

    def test_unknown_domain_cannot_replace_a_capability(self) -> None:
        self.assert_id_rejected("XYZ-01")

    def test_noncanonical_number_cannot_replace_a_capability(self) -> None:
        self.assert_id_rejected("EXE-1")

    def test_capability_row_order_does_not_change_the_report(self) -> None:
        lines = self.matrix.read_text(encoding="utf-8").splitlines()
        positions = [
            index for index, line in enumerate(lines)
            if any(line.startswith(f"| {domain}-") for domain in (
                "EXE", "CON", "COL", "LED", "GOV", "QUA", "OBS", "INT"
            ))
        ]
        self.assertEqual(len(positions), 53)
        rows = [lines[index] for index in reversed(positions)]
        for index, row in zip(positions, rows):
            lines[index] = row
        self.matrix.write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.assert_report_unchanged()

    def test_duplicate_identity_remains_rejected(self) -> None:
        self.replace_id("EXE-02")
        result = self.report()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("duplicate id EXE-02", result.stderr)

    def test_missing_capability_remains_rejected(self) -> None:
        text = self.matrix.read_text(encoding="utf-8")
        lines = [line for line in text.splitlines() if not line.startswith("| EXE-01 |")]
        self.matrix.write_text("\n".join(lines) + "\n", encoding="utf-8")
        result = self.report()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("parity report:", result.stderr)


if __name__ == "__main__":
    unittest.main()
