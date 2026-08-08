#!/usr/bin/env python3
"""Focused tests for deterministic dependency-license inventory handling."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import dependency_licenses


class DependencyLicenseTests(unittest.TestCase):
    def write_fixture(
        self,
        root: Path,
        *,
        registry_has_license: bool = True,
        include_override: bool = False,
    ) -> dict[str, Path | dict]:
        vendor = root / "vendor"
        crate = vendor / "example-1.0.0"
        crate.mkdir(parents=True, exist_ok=True)
        manifest = crate / "Cargo.toml"
        manifest.write_text(
            "[package]\nname='example'\nversion='1.0.0'\nlicense='MIT'\n",
            encoding="utf-8",
        )
        if registry_has_license:
            (crate / "LICENSE-MIT").write_text("example license\n", encoding="utf-8")
        else:
            (crate / ".cargo_vcs_info.json").write_text(
                json.dumps({"git": {"sha1": "a" * 40}}),
                encoding="utf-8",
            )

        native = root / "rust"
        native.mkdir(exist_ok=True)
        native_manifest = native / "Cargo.toml"
        native_manifest.write_text(
            "[package]\n"
            "name='delta_sharing_native'\n"
            "version='0.2.0'\n"
            "license='Apache-2.0'\n",
            encoding="utf-8",
        )
        lock = native / "Cargo.lock"
        lock.write_text(
            "version = 4\n\n"
            "[[package]]\n"
            "name = \"delta_sharing_native\"\n"
            "version = \"0.2.0\"\n\n"
            "[[package]]\n"
            "name = \"example\"\n"
            "version = \"1.0.0\"\n"
            "source = \"registry+https://github.com/rust-lang/crates.io-index\"\n"
            f"checksum = \"{'1' * 64}\"\n",
            encoding="utf-8",
        )
        description = root / "DESCRIPTION"
        description.write_text(
            "Package: fixture\n"
            "Depends: R (>= 4.3.0)\n"
            "Imports:\n"
            "  alpha (>= 1.0),\n"
            "  beta\n"
            "Suggests: gamma\n"
            "Config/Needs/check: rcmdcheck\n",
            encoding="utf-8",
        )
        package_license = root / "LICENSE"
        package_license.write_text("package license\n", encoding="utf-8")
        override_files = root / "override-files"
        override_files.mkdir(exist_ok=True)
        override_content = b"pinned upstream license\n"
        (override_files / "example-LICENSE").write_bytes(override_content)
        overrides = root / "overrides.json"
        override_items = []
        if include_override:
            override_items.append(
                {
                    "target": "example@1.0.0",
                    "kind": "pinned-vcs",
                    "repository": "https://example.test/example",
                    "revision": "a" * 40,
                    "reason": "The fixture crate omits its repository license.",
                    "files": [
                        {
                            "local": "example-LICENSE",
                            "upstream_path": "LICENSE",
                            "sha256": dependency_licenses.sha256_bytes(
                                override_content
                            ),
                            "source_url": (
                                "https://example.test/example/"
                                + "a" * 40
                                + "/LICENSE"
                            ),
                        }
                    ],
                }
            )
        overrides.write_text(
            json.dumps(
                {
                    "schema_version": dependency_licenses.SCHEMA_VERSION,
                    "overrides": override_items,
                }
            ),
            encoding="utf-8",
        )
        metadata = {
            "packages": [
                {
                    "name": "delta_sharing_native",
                    "version": "0.2.0",
                    "manifest_path": str(native_manifest),
                    "source": None,
                    "license": "Apache-2.0",
                    "license_file": None,
                    "repository": None,
                    "authors": [],
                },
                {
                    "name": "example",
                    "version": "1.0.0",
                    "manifest_path": str(manifest),
                    "source": (
                        "registry+https://github.com/rust-lang/crates.io-index"
                    ),
                    "license": "MIT",
                    "license_file": None,
                    "repository": "https://example.test/example",
                    "authors": ["Example Authors"],
                },
            ]
        }
        return {
            "vendor": vendor,
            "native": native,
            "lock": lock,
            "description": description,
            "package_license": package_license,
            "overrides": overrides,
            "override_files": override_files,
            "metadata": metadata,
        }

    def build(self, paths: dict[str, Path | dict]):
        return dependency_licenses.build_inventory(
            metadata=paths["metadata"],
            vendor_root=paths["vendor"],
            native_root=paths["native"],
            lock_path=paths["lock"],
            description_path=paths["description"],
            package_license_path=paths["package_license"],
            overrides_path=paths["overrides"],
            override_files_root=paths["override_files"],
        )

    def test_inventory_and_bundle_are_byte_reproducible(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inventory, corpus = self.build(self.write_fixture(root))
            first_inventory, first_bundle = dependency_licenses.render_outputs(
                inventory, corpus, root / "first"
            )
            second_inventory, second_bundle = dependency_licenses.render_outputs(
                inventory, corpus, root / "second"
            )

            self.assertEqual(
                first_inventory.read_bytes(),
                second_inventory.read_bytes(),
            )
            self.assertEqual(first_bundle.read_bytes(), second_bundle.read_bytes())
            rendered = json.loads(first_inventory.read_text(encoding="utf-8"))
            self.assertEqual(rendered["rust"]["resolved_package_count"], 2)
            self.assertEqual(rendered["rust"]["registry_package_count"], 1)
            self.assertEqual(
                [
                    (item["name"], item["relationship"])
                    for item in rendered["r_dependencies"]
                ],
                [
                    ("R", "Depends"),
                    ("alpha", "Imports"),
                    ("beta", "Imports"),
                    ("gamma", "Suggests"),
                    ("rcmdcheck", "Config/Needs/check"),
                ],
            )
            with tarfile.open(first_bundle, "r:xz") as archive:
                names = archive.getnames()
            self.assertEqual(names, sorted(names))
            self.assertEqual(
                len(names),
                rendered["rust"]["unique_legal_text_count"] + 1,
            )

    def test_tampered_inventory_and_bundle_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inventory, corpus = self.build(self.write_fixture(root))
            expected_inventory, expected_bundle = dependency_licenses.render_outputs(
                inventory, corpus, root / "expected"
            )
            actual_inventory, actual_bundle = dependency_licenses.render_outputs(
                inventory, corpus, root / "actual"
            )
            actual_inventory.write_bytes(actual_inventory.read_bytes() + b" ")
            with self.assertRaisesRegex(
                dependency_licenses.LicenseInventoryError,
                "stale or tampered",
            ):
                dependency_licenses.assert_generated_matches(
                    actual_inventory,
                    actual_bundle,
                    expected_inventory,
                    expected_bundle,
                )

            actual_inventory.write_bytes(expected_inventory.read_bytes())
            actual_bundle.write_bytes(actual_bundle.read_bytes()[:-1] + b"!")
            with self.assertRaisesRegex(
                dependency_licenses.LicenseInventoryError,
                "stale or tampered",
            ):
                dependency_licenses.assert_generated_matches(
                    actual_inventory,
                    actual_bundle,
                    expected_inventory,
                    expected_bundle,
                )

    def test_missing_crate_license_requires_revision_pinned_override(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = self.write_fixture(root, registry_has_license=False)
            with self.assertRaisesRegex(
                dependency_licenses.LicenseInventoryError,
                "no license/notice evidence or override",
            ):
                self.build(paths)

            paths["overrides"].unlink()
            complete = self.write_fixture(
                root,
                registry_has_license=False,
                include_override=True,
            )
            inventory, corpus = self.build(complete)
            example = next(
                package
                for package in inventory["rust"]["packages"]
                if package["name"] == "example"
            )
            self.assertEqual(
                example["legal_files"][0]["source"],
                "pinned-vcs-override",
            )
            self.assertIn(example["legal_files"][0]["sha256"], corpus)

    def test_metadata_must_exactly_match_lockfile(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = self.write_fixture(root)
            paths["metadata"]["packages"].pop()
            with self.assertRaisesRegex(
                dependency_licenses.LicenseInventoryError,
                "differs from Cargo.lock",
            ):
                self.build(paths)

    def test_metadata_source_must_match_lockfile(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = self.write_fixture(root)
            paths["metadata"]["packages"][1]["source"] = (
                "registry+https://example.test/unreviewed"
            )
            with self.assertRaisesRegex(
                dependency_licenses.LicenseInventoryError,
                "source differs from Cargo.lock",
            ):
                self.build(paths)

    def test_pinned_override_checksum_tamper_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = self.write_fixture(
                root,
                registry_has_license=False,
                include_override=True,
            )
            (paths["override_files"] / "example-LICENSE").write_text(
                "tampered\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                dependency_licenses.LicenseInventoryError,
                "checksum failed",
            ):
                self.build(paths)

    def test_stale_override_is_rejected_when_crate_ships_license(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = self.write_fixture(root, include_override=True)
            with self.assertRaisesRegex(
                dependency_licenses.LicenseInventoryError,
                "override is stale",
            ):
                self.build(paths)


if __name__ == "__main__":
    unittest.main()
