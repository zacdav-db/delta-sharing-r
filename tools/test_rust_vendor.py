#!/usr/bin/env python3
"""Focused tests for deterministic Rust vendor archive handling."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import rust_vendor


class RustVendorToolTests(unittest.TestCase):
    def test_archive_bytes_are_reproducible_and_metadata_is_normalized(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            vendor = root / "vendor"
            package = vendor / "example-1.0.0"
            package.mkdir(parents=True)
            (package / "Cargo.toml").write_text(
                "[package]\nname='example'\nversion='1.0.0'\n",
                encoding="utf-8",
            )
            first = root / "first.tar.xz"
            second = root / "second.tar.xz"

            rust_vendor.write_deterministic_archive(vendor, first)
            rust_vendor.write_deterministic_archive(vendor, second)

            self.assertEqual(first.read_bytes(), second.read_bytes())
            with tarfile.open(first, "r:xz") as archive:
                members = archive.getmembers()
            self.assertEqual(
                [member.name for member in members],
                [
                    "vendor",
                    "vendor/example-1.0.0",
                    "vendor/example-1.0.0/Cargo.toml",
                ],
            )
            self.assertTrue(all(member.mtime == 0 for member in members))
            self.assertTrue(all(member.uid == member.gid == 0 for member in members))

    def test_unsafe_archive_member_is_rejected(self) -> None:
        member = tarfile.TarInfo("../outside")
        member.type = tarfile.REGTYPE
        member.size = 0
        member.uid = 0
        member.gid = 0
        member.uname = ""
        member.gname = ""
        member.mtime = 0
        member.mode = 0o644
        with self.assertRaises(rust_vendor.VendorError):
            rust_vendor.validate_member(member, None)

    def test_config_must_replace_crates_io_with_local_vendor(self) -> None:
        valid = """
[source.crates-io]
replace-with = "vendored-sources"

[source.vendored-sources]
directory = "vendor"
"""
        self.assertEqual(
            rust_vendor.normalize_vendor_config(valid),
            valid.strip() + "\n",
        )
        invalid = valid.replace('directory = "vendor"', 'directory = "/tmp/vendor"')
        with self.assertRaises(rust_vendor.VendorError):
            rust_vendor.normalize_vendor_config(invalid)

    def test_no_vendor_artifacts_use_normal_locked_build(self) -> None:
        makevars = rust_vendor.REPOSITORY_ROOT / "src" / "Makevars"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cargo = root / "bin" / "cargo"
            cargo.parent.mkdir()
            cargo.write_text(
                '#!/bin/sh\nprintf "%s\\n" "$*" > "$CARGO_ARGS_LOG"\n',
                encoding="utf-8",
            )
            cargo.chmod(0o755)
            log = root / "cargo-args"
            environment = os.environ.copy()
            environment["CARGO_ARGS_LOG"] = str(log)
            environment["PATH"] = f"{cargo.parent}{os.pathsep}{environment['PATH']}"

            subprocess.run(
                [
                    "make",
                    "-f",
                    str(makevars),
                    "./rust/target/r-package/release/libdelta_sharing_native.a",
                ],
                cwd=root,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )

            arguments = log.read_text(encoding="utf-8")
            self.assertIn("build --locked", arguments)
            self.assertNotIn("--frozen", arguments)

        for filename in ("Makevars", "Makevars.win"):
            recipe = (rust_vendor.REPOSITORY_ROOT / "src" / filename).read_text(
                encoding="utf-8"
            )
            self.assertIn('cargo_flags="--locked"', recipe)
            self.assertIn(
                "test -f rust/vendor.tar.xz || "
                "test -f rust/vendor-config.toml",
                recipe,
            )
            self.assertIn('cargo_flags="--frozen"', recipe)

    def test_macos_build_uses_rust_target_default_and_preserves_override(self) -> None:
        makevars = rust_vendor.REPOSITORY_ROOT / "src" / "Makevars"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary_directory = root / "bin"
            binary_directory.mkdir()

            cargo = binary_directory / "cargo"
            cargo.write_text(
                '#!/bin/sh\nprintf "%s\\n" "$MACOSX_DEPLOYMENT_TARGET" '
                '> "$DEPLOYMENT_TARGET_LOG"\n',
                encoding="utf-8",
            )
            cargo.chmod(0o755)

            uname = binary_directory / "uname"
            uname.write_text("#!/bin/sh\nprintf 'Darwin\\n'\n", encoding="utf-8")
            uname.chmod(0o755)

            rustc = binary_directory / "rustc"
            rustc.write_text(
                "#!/bin/sh\n"
                "test \"$1\" = '--print=deployment-target'\n"
                "printf 'MACOSX_DEPLOYMENT_TARGET=11.0\\n'\n",
                encoding="utf-8",
            )
            rustc.chmod(0o755)

            log = root / "deployment-target"
            environment = os.environ.copy()
            environment.pop("MACOSX_DEPLOYMENT_TARGET", None)
            environment["DEPLOYMENT_TARGET_LOG"] = str(log)
            environment["PATH"] = (
                f"{binary_directory}{os.pathsep}{environment['PATH']}"
            )
            command = [
                "make",
                "-f",
                str(makevars),
                "./rust/target/r-package/release/libdelta_sharing_native.a",
            ]

            subprocess.run(
                command,
                cwd=root,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(log.read_text(encoding="utf-8"), "11.0\n")

            environment["MACOSX_DEPLOYMENT_TARGET"] = "14.0"
            subprocess.run(
                command,
                cwd=root,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(log.read_text(encoding="utf-8"), "14.0\n")


if __name__ == "__main__":
    unittest.main()
