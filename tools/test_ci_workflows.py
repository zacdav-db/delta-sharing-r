#!/usr/bin/env python3
"""Focused structural checks for cross-platform R package workflows."""

from __future__ import annotations

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
R_CHECK = ROOT / ".github" / "workflows" / "r-cmd-check.yaml"
ARTIFACTS = ROOT / ".github" / "workflows" / "package-artifacts.yaml"


class PackageWorkflowTests(unittest.TestCase):
    def test_full_r_check_includes_linux_arm64_release(self) -> None:
        workflow = R_CHECK.read_text(encoding="utf-8")
        arm_entry = re.compile(
            r"- os: ubuntu-24\.04-arm\s+"
            r"r: release\s+"
            r"label: Linux arm64 \(R release\)"
        )
        self.assertRegex(workflow, arm_entry)
        self.assertIn("uses: r-lib/actions/check-r-package@", workflow)

    def test_artifact_matrix_covers_every_supported_host(self) -> None:
        workflow = ARTIFACTS.read_text(encoding="utf-8")
        expected = {
            "ubuntu-24.04": "linux-x86_64",
            "ubuntu-24.04-arm": "linux-arm64",
            "macos-15": "macos-arm64",
            "macos-15-intel": "macos-x86_64",
            "windows-2025": "windows-x86_64",
        }
        for runner, artifact in expected.items():
            entry = re.compile(
                rf"- os: {re.escape(runner)}\s+"
                rf"label: [^\n]+\s+"
                rf"rust-target: [^\n]+\s+"
                rf"artifact: {re.escape(artifact)}"
            )
            self.assertRegex(workflow, entry)

    def test_exact_source_precedes_binary_reinstall(self) -> None:
        workflow = ARTIFACTS.read_text(encoding="utf-8")
        required_fragments = (
            "python3 tools/rust_vendor.py generate",
            "python3 tools/rust_vendor.py check",
            "python3 tools/dependency_licenses.py check",
            "name: package-source",
            "needs: source",
            "R CMD INSTALL \\",
            "--build \\",
            'Rscript tools/verify-installed-package.R "$source_library"',
            'PATH="$no_native_tools:$PATH" R CMD INSTALL',
            'Rscript tools/verify-installed-package.R "$binary_library"',
            "name: package-binary-${{ matrix.config.artifact }}",
        )
        for fragment in required_fragments:
            self.assertIn(fragment, workflow)

        source_upload = workflow.index("name: package-source")
        source_download = workflow.rindex("name: package-source")
        binary_install = workflow.index("Install binary without native build tools")
        self.assertLess(source_upload, source_download)
        self.assertLess(source_download, binary_install)


if __name__ == "__main__":
    unittest.main()
