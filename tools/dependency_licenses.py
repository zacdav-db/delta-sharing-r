#!/usr/bin/env python3
"""Generate and verify dependency-license materials for package distribution.

The Rust inventory is derived from Cargo.lock, frozen offline Cargo metadata,
and the checksum-verified vendor archive.  Verbatim license, notice, authorship,
and patent files are deduplicated by SHA-256 into a deterministic xz archive.
Pinned VCS overrides cover crates whose published archive omitted a
workspace-root license file.

R dependencies are not bundled into delta.sharing.  Their exact DESCRIPTION
requirements and dependency roles are recorded so the installed dependency's
own DESCRIPTION and license files remain the authority for its license.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import rust_vendor


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DESCRIPTION_PATH = REPOSITORY_ROOT / "DESCRIPTION"
PACKAGE_LICENSE_PATH = REPOSITORY_ROOT / "LICENSE"
RUST_ROOT = REPOSITORY_ROOT / "src" / "rust"
LOCK_PATH = RUST_ROOT / "Cargo.lock"
VENDOR_ARCHIVE_PATH = RUST_ROOT / "vendor.tar.xz"
VENDOR_CONFIG_PATH = RUST_ROOT / "vendor-config.toml"
OVERRIDES_PATH = REPOSITORY_ROOT / "tools" / "rust-license-overrides.json"
OVERRIDE_FILES_ROOT = REPOSITORY_ROOT / "tools" / "rust-license-overrides"
OUTPUT_ROOT = REPOSITORY_ROOT / "inst" / "dependency-licenses"
INVENTORY_PATH = OUTPUT_ROOT / "dependency-inventory.json"
LICENSE_BUNDLE_PATH = OUTPUT_ROOT / "rust-license-texts.tar.xz"
LICENSE_BUNDLE_ROOT = "rust-license-texts"
SCHEMA_VERSION = 1

LEGAL_BASENAME = re.compile(
    r"^(?:"
    r"licen[cs]e|copying|notice|copyright|unlicense|authors?|contributors?|"
    r"patents?|third[-_.]?party(?:[-_.]?notices?)?"
    r")(?:[-_.].*)?$",
    re.IGNORECASE,
)
R_DEPENDENCY_ENTRY = re.compile(
    r"^(?P<name>[A-Za-z][A-Za-z0-9.]*)"
    r"(?:\s*\((?P<requirement>[^)]+)\))?$"
)
R_DEPENDENCY_FIELDS = ("Depends", "Imports", "Suggests", "LinkingTo", "Enhances")


class LicenseInventoryError(RuntimeError):
    """A deterministic, user-facing inventory validation failure."""


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sha256_file(path: Path) -> str:
    return rust_vendor.sha256_file(path)


def package_id(name: str, version: str) -> str:
    return f"{name}@{version}"


def parse_dcf(path: Path) -> dict[str, str]:
    fields: dict[str, str] = {}
    current: str | None = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if raw_line.startswith((" ", "\t")):
            if current is None:
                raise LicenseInventoryError("DESCRIPTION has an orphan continuation")
            fields[current] += "\n" + raw_line.strip()
            continue
        if not raw_line:
            current = None
            continue
        if ":" not in raw_line:
            raise LicenseInventoryError(f"invalid DESCRIPTION field: {raw_line}")
        current, value = raw_line.split(":", 1)
        current = current.strip()
        fields[current] = value.strip()
    return fields


def r_dependency_inventory(description_path: Path) -> list[dict[str, Any]]:
    fields = parse_dcf(description_path)
    dependencies: list[dict[str, Any]] = []
    dependency_fields = list(R_DEPENDENCY_FIELDS) + sorted(
        field for field in fields if field.startswith("Config/Needs/")
    )
    relationship_order = {
        relationship: index
        for index, relationship in enumerate(dependency_fields)
    }
    for relationship in dependency_fields:
        value = fields.get(relationship)
        if value is None:
            continue
        for raw_entry in value.replace("\n", " ").split(","):
            entry = raw_entry.strip()
            if not entry:
                continue
            matched = R_DEPENDENCY_ENTRY.fullmatch(entry)
            if matched is None:
                raise LicenseInventoryError(
                    f"cannot parse DESCRIPTION {relationship} entry: {entry}"
                )
            name = matched.group("name")
            dependencies.append(
                {
                    "name": name,
                    "relationship": relationship,
                    "requirement": matched.group("requirement"),
                    "distribution": (
                        "R runtime"
                        if relationship == "Depends" and name == "R"
                        else "external R package; code is not bundled"
                    ),
                    "license_authority": (
                        "R's distribution terms"
                        if relationship == "Depends" and name == "R"
                        else (
                            "the installed dependency's DESCRIPTION License field "
                            "and license files"
                        )
                    ),
                }
            )
    return sorted(
        dependencies,
        key=lambda item: (relationship_order[item["relationship"]], item["name"]),
    )


def read_lock_packages(lock_path: Path) -> dict[str, dict[str, Any]]:
    with lock_path.open("rb") as stream:
        lock = tomllib.load(stream)
    packages: dict[str, dict[str, Any]] = {}
    for package in lock.get("package", []):
        identifier = package_id(package["name"], package["version"])
        if identifier in packages:
            raise LicenseInventoryError(
                f"Cargo.lock has a duplicate name/version package: {identifier}"
            )
        source = package.get("source")
        checksum = package.get("checksum")
        if source is not None:
            if not source.startswith("registry+"):
                raise LicenseInventoryError(
                    f"Cargo.lock has an unreviewed non-registry source: {identifier}"
                )
            if not isinstance(checksum, str):
                raise LicenseInventoryError(
                    f"Cargo.lock registry package has no checksum: {identifier}"
                )
        elif checksum is not None:
            raise LicenseInventoryError(
                f"Cargo.lock local package unexpectedly has a checksum: {identifier}"
            )
        packages[identifier] = {
            "name": package["name"],
            "version": package["version"],
            "source": source,
            "checksum": checksum,
        }
    if not packages:
        raise LicenseInventoryError("Cargo.lock contains no packages")
    return packages


def load_overrides(path: Path) -> dict[str, dict[str, Any]]:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise LicenseInventoryError(
            "Rust license overrides are missing or invalid"
        ) from error
    if parsed.get("schema_version") != SCHEMA_VERSION:
        raise LicenseInventoryError("Rust license override schema is unsupported")
    overrides: dict[str, dict[str, Any]] = {}
    for override in parsed.get("overrides", []):
        target = override.get("target")
        if not isinstance(target, str) or target in overrides:
            raise LicenseInventoryError("Rust license override targets must be unique")
        overrides[target] = override
    return overrides


def normalize_repository(url: str | None) -> str | None:
    if url is None:
        return None
    normalized = url.rstrip("/")
    marker = "/tree/"
    if marker in normalized:
        normalized = normalized.split(marker, 1)[0]
    if normalized.endswith(".git"):
        normalized = normalized[:-4]
    return normalized


def legal_source_files(
    package_root: Path,
    explicit_license_file: str | None,
) -> list[Path]:
    explicit: Path | None = None
    if explicit_license_file is not None:
        candidate = (package_root / explicit_license_file).resolve()
        try:
            candidate.relative_to(package_root.resolve())
        except ValueError as error:
            raise LicenseInventoryError(
                f"license-file leaves package root: {explicit_license_file}"
            ) from error
        if not candidate.is_file():
            raise LicenseInventoryError(
                f"declared license-file does not exist: {explicit_license_file}"
            )
        explicit = candidate

    files: list[Path] = []
    for candidate in package_root.rglob("*"):
        if not candidate.is_file():
            continue
        try:
            candidate.resolve().relative_to(package_root.resolve())
        except ValueError as error:
            raise LicenseInventoryError(
                f"legal-file link leaves package root: "
                f"{candidate.relative_to(package_root)}"
            ) from error
        relative = candidate.relative_to(package_root)
        in_license_directory = any(
            part.lower() in ("license", "licenses") for part in relative.parts[:-1]
        )
        if (
            LEGAL_BASENAME.fullmatch(candidate.name)
            or in_license_directory
            or (explicit is not None and candidate.resolve() == explicit)
        ):
            files.append(candidate)
    return sorted(
        set(files),
        key=lambda item: item.relative_to(package_root).as_posix(),
    )


def vcs_revision(package_root: Path) -> str | None:
    path = package_root / ".cargo_vcs_info.json"
    if not path.is_file():
        return None
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise LicenseInventoryError(
            f"invalid .cargo_vcs_info.json in {package_root.name}"
        ) from error
    revision = parsed.get("git", {}).get("sha1")
    return revision if isinstance(revision, str) and revision else None


def add_corpus_file(
    corpus: dict[str, bytes],
    content: bytes,
) -> tuple[str, str]:
    digest = sha256_bytes(content)
    existing = corpus.setdefault(digest, content)
    if existing != content:
        raise LicenseInventoryError(f"SHA-256 collision for license text {digest}")
    return digest, f"{LICENSE_BUNDLE_ROOT}/{digest}"


def direct_file_record(
    package_root: Path,
    path: Path,
    corpus: dict[str, bytes],
    source_kind: str = "vendored-crate",
) -> dict[str, Any]:
    content = path.read_bytes()
    digest, member = add_corpus_file(corpus, content)
    return {
        "path": path.relative_to(package_root).as_posix(),
        "sha256": digest,
        "bundle_member": member,
        "source": source_kind,
    }


def override_file_records(
    target: str,
    override: dict[str, Any],
    package_roots: dict[str, Path],
    metadata_by_id: dict[str, dict[str, Any]],
    corpus: dict[str, bytes],
    override_files_root: Path,
) -> list[dict[str, Any]]:
    kind = override.get("kind")
    reason = override.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        raise LicenseInventoryError(f"license override has no reason: {target}")
    records: list[dict[str, Any]] = []
    if kind == "registry-sibling":
        source_package = override.get("source_package")
        if source_package not in package_roots:
            raise LicenseInventoryError(
                f"license override source package is not locked: {target}"
            )
        if normalize_repository(metadata_by_id[target].get("repository")) != (
            normalize_repository(metadata_by_id[source_package].get("repository"))
        ):
            raise LicenseInventoryError(
                f"license override sibling repository differs: {target}"
            )
        source_root = package_roots[source_package]
        allowed = {
            path.relative_to(source_root).as_posix(): path
            for path in legal_source_files(
                source_root,
                metadata_by_id[source_package].get("license_file"),
            )
        }
        for relative in override.get("files", []):
            if relative not in allowed:
                raise LicenseInventoryError(
                    f"license override sibling file is not legal evidence: "
                    f"{source_package}/{relative}"
                )
            record = direct_file_record(
                source_root,
                allowed[relative],
                corpus,
                source_kind=f"locked-registry-sibling:{source_package}",
            )
            record["override_reason"] = reason
            records.append(record)
    elif kind == "pinned-vcs":
        revision = override.get("revision")
        repository = override.get("repository")
        if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise LicenseInventoryError(
                f"license override revision is not pinned: {target}"
            )
        if vcs_revision(package_roots[target]) != revision:
            raise LicenseInventoryError(
                f"license override revision differs from .cargo_vcs_info: {target}"
            )
        if normalize_repository(metadata_by_id[target].get("repository")) != (
            normalize_repository(repository)
        ):
            raise LicenseInventoryError(
                f"license override repository differs from Cargo metadata: {target}"
            )
        for item in override.get("files", []):
            relative = item.get("local")
            upstream_path = item.get("upstream_path")
            expected_digest = item.get("sha256")
            source_url = item.get("source_url")
            if not all(
                isinstance(value, str)
                for value in (relative, upstream_path, expected_digest, source_url)
            ):
                raise LicenseInventoryError(f"invalid pinned override file: {target}")
            if revision not in source_url:
                raise LicenseInventoryError(
                    f"pinned override URL omits exact revision: "
                    f"{target}/{upstream_path}"
                )
            path = (override_files_root / relative).resolve()
            try:
                path.relative_to(override_files_root.resolve())
            except ValueError as error:
                raise LicenseInventoryError(
                    f"pinned override file leaves override root: {target}"
                ) from error
            if not path.is_file() or sha256_file(path) != expected_digest:
                raise LicenseInventoryError(
                    f"pinned override file checksum failed: {target}/{upstream_path}"
                )
            digest, member = add_corpus_file(corpus, path.read_bytes())
            records.append(
                {
                    "path": upstream_path,
                    "sha256": digest,
                    "bundle_member": member,
                    "source": "pinned-vcs-override",
                    "source_url": source_url,
                    "revision": revision,
                    "override_reason": reason,
                }
            )
    else:
        raise LicenseInventoryError(f"unknown license override kind: {target}")
    if not records:
        raise LicenseInventoryError(f"license override contains no files: {target}")
    return sorted(records, key=lambda item: (item["path"], item["sha256"]))


def build_inventory(
    *,
    metadata: dict[str, Any],
    vendor_root: Path,
    native_root: Path,
    lock_path: Path,
    description_path: Path,
    package_license_path: Path,
    overrides_path: Path,
    override_files_root: Path,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    locked = read_lock_packages(lock_path)
    metadata_packages = metadata.get("packages")
    if not isinstance(metadata_packages, list):
        raise LicenseInventoryError("Cargo metadata has no package list")
    metadata_by_id: dict[str, dict[str, Any]] = {}
    for package in metadata_packages:
        identifier = package_id(package["name"], package["version"])
        if identifier in metadata_by_id:
            raise LicenseInventoryError(
                f"Cargo metadata has a duplicate package: {identifier}"
            )
        metadata_by_id[identifier] = package
    if set(metadata_by_id) != set(locked):
        missing = sorted(set(locked) - set(metadata_by_id))
        extra = sorted(set(metadata_by_id) - set(locked))
        raise LicenseInventoryError(
            f"Cargo metadata differs from Cargo.lock; missing={missing}, extra={extra}"
        )

    package_roots: dict[str, Path] = {}
    for identifier, locked_package in locked.items():
        if metadata_by_id[identifier].get("source") != locked_package["source"]:
            raise LicenseInventoryError(
                f"Cargo metadata source differs from Cargo.lock: {identifier}"
            )
        if locked_package["source"] is None:
            root = native_root
        else:
            root = vendor_root / (
                f"{locked_package['name']}-{locked_package['version']}"
            )
        manifest = root / "Cargo.toml"
        if not manifest.is_file():
            raise LicenseInventoryError(
                f"package source is missing from verified inputs: {identifier}"
            )
        metadata_manifest = Path(metadata_by_id[identifier]["manifest_path"]).resolve()
        if metadata_manifest != manifest.resolve():
            raise LicenseInventoryError(
                f"Cargo metadata package root differs from verified source: "
                f"{identifier}"
            )
        package_roots[identifier] = root

    overrides = load_overrides(overrides_path)
    corpus: dict[str, bytes] = {}
    package_records: list[dict[str, Any]] = []
    packages_requiring_overrides: set[str] = set()
    for identifier in sorted(locked):
        locked_package = locked[identifier]
        package = metadata_by_id[identifier]
        license_expression = package.get("license")
        if not isinstance(license_expression, str) or not license_expression.strip():
            raise LicenseInventoryError(
                f"Cargo package has no declared license: {identifier}"
            )
        root = package_roots[identifier]
        legal_files = legal_source_files(root, package.get("license_file"))
        records = [
            direct_file_record(root, path, corpus)
            for path in legal_files
        ]
        if locked_package["source"] is None:
            if not package_license_path.is_file():
                raise LicenseInventoryError("the R package LICENSE file is missing")
            digest, member = add_corpus_file(corpus, package_license_path.read_bytes())
            records.append(
                {
                    "path": "LICENSE",
                    "sha256": digest,
                    "bundle_member": member,
                    "source": "R-package-license",
                }
            )
        elif not records:
            packages_requiring_overrides.add(identifier)
            if identifier not in overrides:
                raise LicenseInventoryError(
                    f"vendored crate has no license/notice evidence or override: "
                    f"{identifier}"
                )
            records.extend(
                override_file_records(
                    identifier,
                    overrides[identifier],
                    package_roots,
                    metadata_by_id,
                    corpus,
                    override_files_root,
                )
            )
        elif identifier in overrides:
            raise LicenseInventoryError(
                f"license override is stale because the crate now ships evidence: "
                f"{identifier}"
            )

        source = locked_package["source"]
        record = {
            "name": locked_package["name"],
            "version": locked_package["version"],
            "source": source,
            "checksum": locked_package["checksum"],
            "declared_license": license_expression,
            "license_file": package.get("license_file"),
            "repository": package.get("repository"),
            "authors": package.get("authors", []),
            "vcs_revision": vcs_revision(root),
            "legal_files": sorted(
                records,
                key=lambda item: (
                    item["path"],
                    item["source"],
                    item["sha256"],
                ),
            ),
        }
        package_records.append(record)

    if set(overrides) != packages_requiring_overrides:
        stale = sorted(set(overrides) - packages_requiring_overrides)
        missing = sorted(packages_requiring_overrides - set(overrides))
        raise LicenseInventoryError(
            f"license override coverage differs; stale={stale}, missing={missing}"
        )

    inventory = {
        "schema_version": SCHEMA_VERSION,
        "policy": {
            "rust": (
                "All locked Rust packages are inventoried. Verbatim legal files "
                "come from checksum-verified crate archives, exact locked sibling "
                "crates, or revision-pinned VCS overrides."
            ),
            "r": (
                "R dependencies are separately distributed packages, not bundled "
                "code. Their installed DESCRIPTION and license files govern."
            ),
        },
        "r_dependencies": r_dependency_inventory(description_path),
        "rust": {
            "cargo_lock_sha256": sha256_file(lock_path),
            "resolved_package_count": len(package_records),
            "registry_package_count": sum(
                record["source"] is not None for record in package_records
            ),
            "package_count_requiring_override": len(packages_requiring_overrides),
            "pinned_vcs_override_package_count": sum(
                override["kind"] == "pinned-vcs" for override in overrides.values()
            ),
            "locked_sibling_override_package_count": sum(
                override["kind"] == "registry-sibling"
                for override in overrides.values()
            ),
            "unique_legal_text_count": len(corpus),
            "packages": package_records,
        },
    }
    return inventory, corpus


def write_bundle(corpus: dict[str, bytes], destination: Path) -> None:
    with tempfile.TemporaryDirectory(
        prefix="delta-sharing-r-license-bundle-"
    ) as temporary:
        root = Path(temporary) / LICENSE_BUNDLE_ROOT
        root.mkdir()
        for digest, content in sorted(corpus.items()):
            if digest != sha256_bytes(content):
                raise LicenseInventoryError(
                    f"license corpus digest differs from content: {digest}"
                )
            (root / digest).write_bytes(content)
        rust_vendor.write_deterministic_archive(root, destination)


def render_outputs(
    inventory: dict[str, Any],
    corpus: dict[str, bytes],
    destination: Path,
) -> tuple[Path, Path]:
    destination.mkdir(parents=True, exist_ok=True)
    bundle = destination / LICENSE_BUNDLE_PATH.name
    write_bundle(corpus, bundle)
    inventory = json.loads(json.dumps(inventory))
    inventory["rust"]["license_bundle"] = {
        "path": LICENSE_BUNDLE_PATH.name,
        "bytes": bundle.stat().st_size,
        "sha256": sha256_file(bundle),
    }
    inventory_path = destination / INVENTORY_PATH.name
    inventory_path.write_text(
        json.dumps(inventory, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return inventory_path, bundle


def frozen_metadata(
    source_root: Path,
    native_root: Path,
) -> dict[str, Any]:
    cargo_home = source_root.parent / "cargo-home"
    cargo_home.mkdir()
    environment = os.environ.copy()
    environment.update(
        {
            "CARGO_HOME": str(cargo_home),
            "CARGO_NET_OFFLINE": "true",
        }
    )
    completed = subprocess.run(
        [
            "cargo",
            "metadata",
            "--manifest-path",
            str(native_root / "Cargo.toml"),
            "--format-version",
            "1",
            "--all-features",
            "--frozen",
        ],
        cwd=source_root,
        env=environment,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return json.loads(completed.stdout)


def build_expected_outputs(destination: Path) -> tuple[Path, Path]:
    if not VENDOR_ARCHIVE_PATH.is_file() or not VENDOR_CONFIG_PATH.is_file():
        raise LicenseInventoryError(
            "generate and check the Rust vendor pair before dependency licenses"
        )
    config_text = rust_vendor.normalize_vendor_config(
        VENDOR_CONFIG_PATH.read_text(encoding="utf-8")
    )
    source_root = destination / "source"
    source_root.mkdir()
    vendor_root = rust_vendor.extract_verified_archive(
        VENDOR_ARCHIVE_PATH,
        source_root,
    )
    rust_vendor.verify_vendored_checksums(vendor_root, LOCK_PATH)
    native_root = source_root / "rust"
    shutil.copytree(
        RUST_ROOT,
        native_root,
        ignore=shutil.ignore_patterns(
            "target",
            "vendor.tar.xz",
            "vendor-config.toml",
        ),
    )
    cargo_config = source_root / ".cargo" / "config.toml"
    cargo_config.parent.mkdir()
    cargo_config.write_text(config_text, encoding="utf-8", newline="\n")
    metadata = frozen_metadata(source_root, native_root)
    inventory, corpus = build_inventory(
        metadata=metadata,
        vendor_root=vendor_root,
        native_root=native_root,
        lock_path=native_root / "Cargo.lock",
        description_path=DESCRIPTION_PATH,
        package_license_path=PACKAGE_LICENSE_PATH,
        overrides_path=OVERRIDES_PATH,
        override_files_root=OVERRIDE_FILES_ROOT,
    )
    return render_outputs(inventory, corpus, destination / "expected")


def generate() -> None:
    with tempfile.TemporaryDirectory(
        prefix="delta-sharing-r-dependency-licenses-"
    ) as temporary:
        expected_inventory, expected_bundle = build_expected_outputs(Path(temporary))
        OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
        os.replace(expected_inventory, INVENTORY_PATH)
        os.replace(expected_bundle, LICENSE_BUNDLE_PATH)
    inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    print(f"rust_packages={inventory['rust']['resolved_package_count']}")
    print(f"registry_packages={inventory['rust']['registry_package_count']}")
    print(
        "license_overrides="
        f"{inventory['rust']['package_count_requiring_override']}"
    )
    print(f"unique_legal_texts={inventory['rust']['unique_legal_text_count']}")
    print(f"inventory_bytes={INVENTORY_PATH.stat().st_size}")
    print(f"bundle_bytes={LICENSE_BUNDLE_PATH.stat().st_size}")
    print(f"bundle_sha256={sha256_file(LICENSE_BUNDLE_PATH)}")


def assert_generated_matches(
    actual_inventory: Path,
    actual_bundle: Path,
    expected_inventory: Path,
    expected_bundle: Path,
) -> None:
    for actual, expected in (
        (actual_inventory, expected_inventory),
        (actual_bundle, expected_bundle),
    ):
        if not actual.is_file() or actual.read_bytes() != expected.read_bytes():
            try:
                display = actual.relative_to(REPOSITORY_ROOT)
            except ValueError:
                display = actual
            raise LicenseInventoryError(
                f"generated dependency-license output is stale or tampered: "
                f"{display}"
            )


def check() -> None:
    if not INVENTORY_PATH.is_file() or not LICENSE_BUNDLE_PATH.is_file():
        raise LicenseInventoryError("generated dependency-license outputs are missing")
    with tempfile.TemporaryDirectory(
        prefix="delta-sharing-r-dependency-license-check-"
    ) as temporary:
        expected_inventory, expected_bundle = build_expected_outputs(Path(temporary))
        assert_generated_matches(
            INVENTORY_PATH,
            LICENSE_BUNDLE_PATH,
            expected_inventory,
            expected_bundle,
        )
    inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    print(f"rust_packages={inventory['rust']['resolved_package_count']}")
    print(f"registry_packages={inventory['rust']['registry_package_count']}")
    print(f"inventory_sha256={sha256_file(INVENTORY_PATH)}")
    print(f"bundle_sha256={sha256_file(LICENSE_BUNDLE_PATH)}")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("generate", "check"))
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    try:
        if arguments.command == "generate":
            generate()
        else:
            check()
    except (
        LicenseInventoryError,
        OSError,
        subprocess.CalledProcessError,
        json.JSONDecodeError,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
