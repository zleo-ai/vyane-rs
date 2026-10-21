#!/usr/bin/env python3
"""Exercise the real release preflight with offline metadata and fake packaging.

Only `cargo metadata --offline` runs against Cargo. The publication script sees
stub cargo/curl executables; any publish or network attempt fails the test.
"""

import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
PUBLISH = ROOT / ".github/scripts/publish-workspace.sh"

CARGO_STUB = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
with open(os.environ["STUB_CALLS"], "a", encoding="utf-8") as log:
    log.write(json.dumps(["cargo", *args]) + "\n")
metadata = json.loads(Path(os.environ["STUB_METADATA"]).read_text())
if args == ["metadata", "--locked", "--format-version", "1", "--no-deps"]:
    print(json.dumps(metadata))
elif args and args[0] == "package":
    expected = ["package", "--workspace", "--locked", "--target-dir",
                str(Path(metadata["target_directory"]) / "package-target"), "--allow-dirty"]
    if args != expected:
        raise SystemExit("unexpected package arguments")
    members = set(metadata["workspace_members"])
    directory = Path(metadata["target_directory"]) / "package-target" / "package"
    directory.mkdir(parents=True)
    for package in metadata["packages"]:
        if package["id"] in members:
            name = f"{package['name']}-{package['version']}.crate"
            (directory / name).write_bytes(("fake package " + name).encode())
else:
    raise SystemExit("forbidden cargo operation: " + repr(args))
'''

CURL_STUB = r'''#!/usr/bin/env python3
import json
import os
import sys
with open(os.environ["STUB_CALLS"], "a", encoding="utf-8") as log:
    log.write(json.dumps(["curl", *sys.argv[1:]]) + "\n")
raise SystemExit("network is forbidden in this test")
'''


class PublishWorkspaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        result = subprocess.run(
            ["cargo", "metadata", "--offline", "--locked", "--format-version", "1", "--no-deps"],
            cwd=ROOT, text=True, capture_output=True, check=True,
        )
        cls.metadata = json.loads(result.stdout)
        cls.source = PUBLISH.read_text()
        ids = set(cls.metadata["workspace_members"])
        cls.packages = [p for p in cls.metadata["packages"] if p["id"] in ids]

    def preflight(self, source):
        with tempfile.TemporaryDirectory(prefix="publish-workspace-test-") as directory:
            root = Path(directory)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            metadata = copy.deepcopy(self.metadata)
            metadata["target_directory"] = str(root / "target")
            metadata_file = root / "metadata.json"
            metadata_file.write_text(json.dumps(metadata))
            calls_file = root / "calls.jsonl"
            calls_file.touch()
            for name, text in [("cargo", CARGO_STUB), ("curl", CURL_STUB)]:
                path = bin_dir / name
                path.write_text(text)
                path.chmod(0o755)
            (bin_dir / "python3").symlink_to(sys.executable)
            script = root / "publish-workspace.sh"
            script.write_text(source)
            environment = {
                "PATH": str(bin_dir) + os.pathsep + os.defpath,
                "HOME": str(root),
                "TMPDIR": str(root),
                "STUB_METADATA": str(metadata_file),
                "STUB_CALLS": str(calls_file),
            }
            result = subprocess.run(
                [shutil.which("bash"), str(script), "preflight", "--tag",
                 "v" + self.packages[0]["version"], "--allow-dirty"],
                cwd=root, env=environment, capture_output=True, text=True, timeout=30,
            )
            calls = [json.loads(line) for line in calls_file.read_text().splitlines()]
            self.assertTrue(all(call[0] == "cargo" and call[1] in {"metadata", "package"}
                                for call in calls), calls)
            archives = sorted(p.name for p in (root / "target/package-target/package").glob("*.crate"))
            return result, calls, archives

    def test_current_workspace_preflight_covers_every_crate_in_dependency_order(self):
        result, calls, archives = self.preflight(self.source)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual([call[1] for call in calls], ["metadata", "package"])
        self.assertEqual(archives, sorted(f"{p['name']}-{p['version']}.crate" for p in self.packages))
        self.assertIn(f"packaged and verified {len(self.packages)} crates", result.stdout)
        self.assertIn("published-version identity verifier self-check passed", result.stdout)

    def assert_rejected_before_packaging(self, source, diagnostic):
        result, calls, archives = self.preflight(source)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(diagnostic, result.stderr)
        self.assertEqual([call[1] for call in calls], ["metadata"])
        self.assertEqual(archives, [])

    def test_missing_workspace_member_is_rejected(self):
        self.assert_rejected_before_packaging(
            self.source.replace("  vyane-core\n", "", 1), "missing from PUBLISH_ORDER",
        )

    def test_duplicate_plan_entry_is_rejected(self):
        self.assert_rejected_before_packaging(
            self.source.replace("  vyane-core\n", "  vyane-core\n  vyane-core\n", 1),
            "PUBLISH_ORDER contains a duplicate",
        )

    def test_stale_plan_entry_is_rejected(self):
        self.assert_rejected_before_packaging(
            self.source.replace("  vyane-core\n", "  vyane-core\n  unused-crate\n", 1),
            "PUBLISH_ORDER entries absent from workspace",
        )

    def test_dependency_after_dependent_is_rejected(self):
        source = self.source.replace("  vyane-core\n", "", 1)
        source = source.replace("  vyane-task\n", "  vyane-task\n  vyane-core\n", 1)
        self.assert_rejected_before_packaging(source, "before its workspace dependency vyane-core")


if __name__ == "__main__":
    unittest.main(verbosity=2)
