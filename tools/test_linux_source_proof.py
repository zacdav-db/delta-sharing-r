#!/usr/bin/env python3
"""Focused tests for the isolated Linux source-install proof wrapper."""

from __future__ import annotations

import io
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().with_name("linux-source-proof.sh")
DIGEST_IMAGE = "example.invalid/delta-sharing-proof@sha256:" + "a" * 64


def write_source_archive(path: Path, include_vendor: bool = True) -> None:
    with tarfile.open(path, "w:gz") as archive:
        entries = ["delta.sharing/DESCRIPTION"]
        if include_vendor:
            entries.extend(
                [
                    "delta.sharing/src/rust/vendor.tar.xz",
                    "delta.sharing/src/rust/vendor-config.toml",
                ]
            )
        for name in entries:
            payload = b"test"
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))


class LinuxSourceProofTests(unittest.TestCase):
    def test_requires_digest_pinned_image(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            archive = Path(temporary) / "package.tar.gz"
            write_source_archive(archive)
            completed = subprocess.run(
                [str(SCRIPT), str(archive)],
                env={**os.environ, "DELTA_SHARING_LINUX_IMAGE": "example:latest"},
                capture_output=True,
                text=True,
                check=False,
            )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("must use an exact sha256 digest", completed.stderr)

    def test_rejects_source_without_offline_vendor_pair(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "package.tar.gz"
            write_source_archive(archive, include_vendor=False)
            engine = root / "engine"
            engine.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
            engine.chmod(0o755)
            completed = subprocess.run(
                [str(SCRIPT), str(archive)],
                env={
                    **os.environ,
                    "CONTAINER_ENGINE": str(engine),
                    "DELTA_SHARING_LINUX_IMAGE": DIGEST_IMAGE,
                },
                capture_output=True,
                text=True,
                check=False,
            )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("missing src/rust/vendor.tar.xz", completed.stderr)

    def test_runs_without_network_against_read_only_source_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "package.tar.gz"
            write_source_archive(archive)
            argument_log = root / "arguments"
            engine = root / "engine"
            engine.write_text(
                '#!/bin/sh\nprintf "%s\\n" "$@" > "$ARGUMENT_LOG"\n',
                encoding="utf-8",
            )
            engine.chmod(0o755)
            subprocess.run(
                [str(SCRIPT), str(archive)],
                env={
                    **os.environ,
                    "ARGUMENT_LOG": str(argument_log),
                    "CONTAINER_ENGINE": str(engine),
                    "DELTA_SHARING_LINUX_IMAGE": DIGEST_IMAGE,
                },
                capture_output=True,
                text=True,
                check=True,
            )
            argument_text = argument_log.read_text(encoding="utf-8")
            arguments = argument_text.splitlines()

        self.assertIn("--network", arguments)
        self.assertIn("none", arguments)
        self.assertIn("--read-only", arguments)
        self.assertIn(DIGEST_IMAGE, arguments)
        mount = next(value for value in arguments if value.startswith("type=bind,"))
        self.assertIn("destination=/proof/package.tar.gz", mount)
        self.assertTrue(mount.endswith(",readonly"))
        self.assertIn("CARGO_NET_OFFLINE=true", argument_text)
        self.assertIn("R CMD INSTALL --preclean", argument_text)


if __name__ == "__main__":
    unittest.main()
