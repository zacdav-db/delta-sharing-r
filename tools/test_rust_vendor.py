import io
import hashlib
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

import rust_vendor


class RustVendorTests(unittest.TestCase):
    def test_archive_is_reproducible(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            vendor = root / "vendor"
            package = vendor / "example-1.0.0"
            package.mkdir(parents=True)
            (package / "Cargo.toml").write_text("[package]\nname='example'\n")

            first = root / "first.tar.xz"
            second = root / "second.tar.xz"
            rust_vendor.write_archive(vendor, first)
            rust_vendor.write_archive(vendor, second)

            self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_archive_rejects_parent_paths(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive_path = root / "vendor.tar.xz"
            with tarfile.open(archive_path, "w:xz") as archive:
                vendor = tarfile.TarInfo("vendor")
                vendor.type = tarfile.DIRTYPE
                archive.addfile(vendor)
                unsafe = tarfile.TarInfo("vendor/../outside")
                unsafe.size = 1
                archive.addfile(unsafe, io.BytesIO(b"x"))

            with self.assertRaises(rust_vendor.VendorError):
                rust_vendor.extract_archive(archive_path, root / "extract")

    def test_config_must_use_the_relative_vendor_directory(self):
        self.assertEqual(
            rust_vendor.normalize_vendor_config(rust_vendor.CONFIG),
            rust_vendor.CONFIG,
        )
        with self.assertRaises(rust_vendor.VendorError):
            rust_vendor.normalize_vendor_config(
                rust_vendor.CONFIG.replace(
                    'directory = "vendor"',
                    'directory = "/tmp/vendor"',
                )
            )

    def test_vendored_files_must_match_lock_checksums(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            package = root / "vendor" / "example-1.0.0"
            package.mkdir(parents=True)
            manifest = package / "Cargo.toml"
            manifest.write_text("[package]\nname='example'\nversion='1.0.0'\n")
            file_digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
            package_digest = "1" * 64
            (package / ".cargo-checksum.json").write_text(
                json.dumps(
                    {
                        "files": {"Cargo.toml": file_digest},
                        "package": package_digest,
                    }
                )
            )
            lock = root / "Cargo.lock"
            lock.write_text(
                "version = 4\n\n"
                "[[package]]\n"
                'name = "example"\n'
                'version = "1.0.0"\n'
                'source = "registry+https://github.com/rust-lang/crates.io-index"\n'
                f'checksum = "{package_digest}"\n'
            )

            self.assertEqual(
                rust_vendor.verify_vendored_checksums(root / "vendor", lock),
                1,
            )
            manifest.write_text("tampered\n")
            with self.assertRaises(rust_vendor.VendorError):
                rust_vendor.verify_vendored_checksums(root / "vendor", lock)


if __name__ == "__main__":
    unittest.main()
