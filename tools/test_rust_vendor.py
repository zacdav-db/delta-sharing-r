import io
import hashlib
import json
from pathlib import Path
import tarfile
import tempfile
import tomllib
import unittest
from unittest.mock import patch
import subprocess

import rust_vendor


class RustVendorTests(unittest.TestCase):
    def write_crate(self, root, name="example"):
        package = root / "vendor" / f"{name}-1.0.0"
        files = {
            "Cargo.toml": (
                f'[package]\nname = "{name}"\nversion = "1.0.0"\n'
                'build = "build.rs"\nlinks = "example"\ndefault-run = "example"\n'
                'license-file = "docs/terms.txt"\n'
                '[lib]\npath = "src/original.rs"\n'
                '[[bin]]\nname = "example"\npath = "src/main.rs"\n'
                '[features]\noptional = ["dep:other"]\n'
                '[dependencies.other]\nversion = "1"\noptional = true\n'
            ),
            "src/original.rs": "pub fn example() {}\n",
            "src/main.rs": "fn main() {}\n",
            "src/tests.rs": "// Not a top-level test directory.\n",
            "build.rs": "fn main() {}\n",
            "README.md": "crate readme\n",
            ".cargo_vcs_info.json": "{}",
            "AUTHORS": "original authors\n",
            "docs/terms.txt": "declared legal text\n",
            "docs/guide.md": "unused guide\n",
            "tests/data/COPYING": "fixture copyright\n",
            "tests/data/input.txt": "unused test fixture\n",
            "tests/licenses/third-party.txt": "third-party terms\n",
            "benches/read.rs": "benchmark\n",
        }
        for relative, content in files.items():
            destination = package / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(content, encoding="utf-8")
        (package / ".cargo-checksum.json").write_text(json.dumps({
            "package": "1" * 64,
            "files": {key: hashlib.sha256(value.encode()).hexdigest() for key, value in files.items()},
        }))
        lock = root / "Cargo.lock"
        lock.write_text(
            f'[[package]]\nname = "{name}"\nversion = "1.0.0"\n'
            'source = "registry+https://github.com/rust-lang/crates.io-index"\n'
            f'checksum = "{"1" * 64}"\n'
        )
        return package, lock, files

    def test_selected_crate_keeps_build_inputs_and_all_legal_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            package, lock, files = self.write_crate(Path(temporary))
            rust_vendor.filter_package(package, selected=True)
            for relative in (
                "Cargo.toml", "build.rs", "src/original.rs", "src/tests.rs",
                "AUTHORS", "docs/terms.txt", "tests/data/COPYING",
                "tests/licenses/third-party.txt",
            ):
                self.assertEqual((package / relative).read_text(), files[relative])
            for relative in ("docs/guide.md", "tests/data/input.txt", "benches/read.rs"):
                self.assertFalse((package / relative).exists())
            self.assertEqual(rust_vendor.verify_vendored_checksums(package.parent, lock), 1)

    def test_zerocopy_keeps_documentation_compile_inputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            package, lock, files = self.write_crate(Path(temporary), name="zerocopy")
            rust_vendor.filter_package(package, selected=True)
            for relative, content in files.items():
                self.assertEqual((package / relative).read_text(), content)
            self.assertEqual(rust_vendor.verify_vendored_checksums(package.parent, lock), 1)

    def test_inactive_crate_keeps_resolver_metadata_and_cannot_compile(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            package, lock, files = self.write_crate(root)
            rust_vendor.filter_package(package, selected=False)
            manifest = tomllib.loads((package / "Cargo.toml").read_text())
            original = tomllib.loads(files["Cargo.toml"])
            for key in ("features", "dependencies"):
                self.assertEqual(manifest[key], original[key])
            self.assertFalse(manifest["package"]["build"])
            self.assertNotIn("links", manifest["package"])
            self.assertNotIn("default-run", manifest["package"])
            self.assertNotIn("bin", manifest)
            self.assertEqual(manifest["lib"]["path"], "src/lib.rs")
            for relative in ("AUTHORS", "docs/terms.txt", "tests/data/COPYING", "README.md"):
                self.assertEqual((package / relative).read_text(), files[relative])
            self.assertFalse((package / "src/original.rs").exists())
            self.assertFalse((package / "build.rs").exists())
            self.assertEqual(rust_vendor.verify_vendored_checksums(package.parent, lock), 1)
            result = subprocess.run(
                ["rustc", "--crate-type=lib", str(package / "src/lib.rs"), "--out-dir", str(root)],
                capture_output=True, text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("outside the supported release targets", result.stderr)

    def test_filtering_is_idempotent_and_reproducible(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            package, _, _ = self.write_crate(root)
            rust_vendor.filter_package(package, selected=False)
            first = root / "first.tar.xz"
            second = root / "second.tar.xz"
            rust_vendor.write_archive(package.parent, first)
            rust_vendor.filter_package(package, selected=False)
            rust_vendor.write_archive(package.parent, second)
            self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_legal_files_cannot_escape_the_crate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            package, _, _ = self.write_crate(root)
            (root / "outside").write_text("not part of this crate")
            with self.assertRaisesRegex(rust_vendor.VendorError, "leaves package root"):
                rust_vendor.legal_source_files(package, "../../outside")
            with self.assertRaisesRegex(rust_vendor.VendorError, "does not exist"):
                rust_vendor.legal_source_files(package, "missing-license")

    def test_selection_uses_the_target_union_not_the_host(self):
        outputs = [subprocess.CompletedProcess([], 0, f"crate{i} v1.0.0\n", "")
                   for i in range(len(rust_vendor.RELEASE_TARGETS))]
        with patch("rust_vendor.subprocess.run", side_effect=outputs) as run:
            selected = rust_vendor.selected_packages(Path("/temporary/vendor"))
        self.assertEqual(selected, {f"crate{i}-1.0.0" for i in range(len(outputs))})
        for call, target in zip(run.call_args_list, rust_vendor.RELEASE_TARGETS):
            command = call.args[0]
            self.assertEqual(command[command.index("--target") + 1], target)
            self.assertIn("--frozen", command)
            self.assertIn("--all-features", command)
            self.assertIn("normal,build", command)

    def test_failed_selection_cannot_silently_omit_dependencies(self):
        with patch("rust_vendor.subprocess.run", return_value=subprocess.CompletedProcess([], 1, "", "failed")):
            with self.assertRaisesRegex(rust_vendor.VendorError, "selection failed"):
                rust_vendor.selected_packages(Path("/temporary/vendor"))
        with patch("rust_vendor.subprocess.run", return_value=subprocess.CompletedProcess([], 0, "unexpected output\n", "")):
            with self.assertRaisesRegex(rust_vendor.VendorError, "unexpected Cargo"):
                rust_vendor.selected_packages(Path("/temporary/vendor"))

    def test_kernel_engine_uses_only_local_io_features(self):
        with (rust_vendor.RUST_ROOT / "Cargo.toml").open("rb") as stream:
            manifest = tomllib.load(stream)

        engine = manifest["dependencies"]["delta_kernel_default_engine"]
        self.assertFalse(engine["default-features"])
        self.assertEqual(engine["features"], ["arrow-58"])

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
