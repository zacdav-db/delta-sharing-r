#!/usr/bin/env python3
"""Structural regression tests for the installed lifecycle sanitizer gate."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
RUST_WORKFLOW = ROOT / ".github" / "workflows" / "rust.yaml"
LIFECYCLE_GATE = ROOT / "tools" / "installed_sanitizer_gate.R"
MAKEVARS = ROOT / "src" / "Makevars"


class InstalledSanitizerGateTests(unittest.TestCase):
    def test_workflow_sanitizes_the_installed_package_boundary(self) -> None:
        workflow = RUST_WORKFLOW.read_text(encoding="utf-8")
        required = (
            "R CMD INSTALL",
            "CFLAGS += -O1 -g -fsanitize=address",
            "CARGO_BUILD_TARGET: ${{ env.RUST_SANITIZER_TARGET }}",
            "RUSTFLAGS: -Zsanitizer=address -Zexternal-clangrt",
            "ldd \"$native_library\"",
            "ASAN_OPTIONS: detect_leaks=1:halt_on_error=1",
            "LSAN_OPTIONS: exitcode=23",
            "Rscript --vanilla tools/installed_sanitizer_gate.R",
        )
        for fragment in required:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, workflow)

    def test_gate_covers_each_installed_ownership_exit(self) -> None:
        gate = LIFECYCLE_GATE.read_text(encoding="utf-8")
        required = (
            'find.package("delta.sharing")',
            "native_test_stream",
            "native_snapshot_stream",
            "materialize_data_frame",
            "explicit$release()",
            "error_after = 1L",
            "panic_after = 0L",
            "nanoarrow_pointer_is_valid",
            "synthetic finalizer iteration",
            "Kernel early release iteration",
            "Kernel exhaustion iteration",
            "current$active_streams",
            "current$pending_cleanups",
        )
        for fragment in required:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, gate)

    def test_target_archive_uses_the_portable_shell_recipe(self) -> None:
        makevars = MAKEVARS.read_text(encoding="utf-8")
        self.assertIn('if test -n "$$CARGO_BUILD_TARGET"', makevars)
        self.assertIn(
            '"$(TARGET_DIR)/$$CARGO_BUILD_TARGET/release/'
            'libdelta_sharing_native.a"',
            makevars,
        )
        self.assertNotIn("$(if", makevars)

    def test_target_archive_is_copied_to_the_r_link_location(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binaries = root / "bin"
            binaries.mkdir()
            cargo = binaries / "cargo"
            cargo.write_text(
                """#!/bin/sh
set -eu
target_dir=
for argument in "$@"; do
  case "$argument" in
    --target-dir=*) target_dir=${argument#--target-dir=} ;;
  esac
done
test -n "$target_dir"
archive="$target_dir/$CARGO_BUILD_TARGET/release/libdelta_sharing_native.a"
mkdir -p "$(dirname "$archive")"
: > "$archive"
""",
                encoding="utf-8",
            )
            cargo.chmod(0o755)

            environment = os.environ.copy()
            environment["CARGO_BUILD_TARGET"] = "x86_64-unknown-linux-gnu"
            environment["PATH"] = (
                f"{binaries}{os.pathsep}{environment['PATH']}"
            )
            link_archive = (
                root
                / "rust"
                / "target"
                / "r-package"
                / "release"
                / "libdelta_sharing_native.a"
            )
            subprocess.run(
                [
                    "make",
                    "-f",
                    str(MAKEVARS),
                    "./rust/target/r-package/release/"
                    "libdelta_sharing_native.a",
                ],
                cwd=root,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertTrue(link_archive.is_file())


if __name__ == "__main__":
    unittest.main()
