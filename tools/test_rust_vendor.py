import io
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


if __name__ == "__main__":
    unittest.main()
