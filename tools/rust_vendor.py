#!/usr/bin/env python3
"""Build and verify the locked Rust source archive used by CRAN."""

from __future__ import annotations

import argparse
import hashlib
import json
import lzma
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import tomllib


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
RUST_ROOT = REPOSITORY_ROOT / "src" / "rust"
ARCHIVE_PATH = RUST_ROOT / "vendor.tar.xz"
CONFIG_PATH = RUST_ROOT / "vendor-config.toml"
CONFIG = """\
[source.crates-io]
replace-with = "vendored-sources"

[source.vendored-sources]
directory = "vendor"
"""


class VendorError(RuntimeError):
    """A release archive could not be generated or verified."""


def sha256(path: Path) -> str:
    """Return the SHA-256 digest of a file."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_file(path: Path) -> str:
    """Return the SHA-256 digest of a file."""
    return sha256(path)


def archive_paths(vendor_root: Path) -> list[Path]:
    """Return the vendor tree in a stable archive order."""
    return [vendor_root, *sorted(vendor_root.rglob("*"))]


def normalized_mode(path: Path) -> int:
    """Return a portable mode for an archived path."""
    mode = path.lstat().st_mode
    if stat.S_ISDIR(mode):
        return 0o755
    return 0o755 if mode & 0o111 else 0o644


def write_archive(vendor_root: Path, destination: Path) -> None:
    """Write a reproducible xz-compressed vendor archive."""
    with destination.open("wb") as raw_stream:
        with lzma.LZMAFile(raw_stream, "w", preset=9) as compressed:
            with tarfile.open(
                fileobj=compressed,
                mode="w",
                format=tarfile.PAX_FORMAT,
            ) as archive:
                for path in archive_paths(vendor_root):
                    if path.is_symlink():
                        raise VendorError(f"vendor tree contains a link: {path.name}")
                    name = path.relative_to(vendor_root.parent).as_posix()
                    info = archive.gettarinfo(path, arcname=name)
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    info.mtime = 0
                    info.mode = normalized_mode(path)
                    if path.is_file() and not path.is_symlink():
                        with path.open("rb") as source:
                            archive.addfile(info, source)
                    else:
                        archive.addfile(info)


def write_deterministic_archive(vendor_root: Path, destination: Path) -> None:
    """Write an archive with stable ordering and metadata."""
    write_archive(vendor_root, destination)


def normalize_vendor_config(config_text: str) -> str:
    """Accept only the relative Cargo source replacement shipped by the package."""
    normalized = "\n".join(
        line.rstrip() for line in config_text.replace("\r\n", "\n").splitlines()
    ).strip() + "\n"
    if normalized != CONFIG:
        raise VendorError("vendor-config.toml is not the expected relative source map")
    return normalized


def locked_registry_packages(lock_path: Path) -> dict[str, str]:
    """Return versioned vendor directories and checksums from Cargo.lock."""
    with lock_path.open("rb") as stream:
        lock = tomllib.load(stream)

    expected: dict[str, str] = {}
    for package in lock.get("package", []):
        source = package.get("source")
        if source is None:
            continue
        if not source.startswith("registry+"):
            raise VendorError(
                "the locked graph contains an unsupported non-registry dependency"
            )
        checksum = package.get("checksum")
        if not isinstance(checksum, str):
            raise VendorError(
                f"locked registry package {package['name']} "
                f"{package['version']} has no checksum"
            )
        directory = f"{package['name']}-{package['version']}"
        if directory in expected:
            raise VendorError(f"duplicate versioned vendor directory: {directory}")
        expected[directory] = checksum
    return expected


def verify_vendored_checksums(vendor_root: Path, lock_path: Path) -> int:
    """Match every vendored crate and file to Cargo.lock checksum metadata."""
    expected = locked_registry_packages(lock_path)
    actual = {
        path.name
        for path in vendor_root.iterdir()
        if path.is_dir() and not path.is_symlink()
    }
    if actual != set(expected):
        missing = sorted(set(expected) - actual)
        extra = sorted(actual - set(expected))
        raise VendorError(
            "vendor directories do not match Cargo.lock; "
            f"missing={missing}, extra={extra}"
        )

    for directory, package_checksum in sorted(expected.items()):
        package_root = vendor_root / directory
        checksum_path = package_root / ".cargo-checksum.json"
        try:
            checksum = json.loads(checksum_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise VendorError(f"invalid checksum metadata for {directory}") from error
        if checksum.get("package") != package_checksum:
            raise VendorError(f"package checksum differs from Cargo.lock: {directory}")

        files = checksum.get("files")
        if not isinstance(files, dict):
            raise VendorError(f"vendored file checksums are missing: {directory}")
        actual_files = {
            path.relative_to(package_root).as_posix()
            for path in package_root.rglob("*")
            if path.is_file() and path.name != ".cargo-checksum.json"
        }
        if actual_files != set(files):
            raise VendorError(f"vendored file list differs for {directory}")
        for relative, expected_digest in files.items():
            if sha256_file(package_root / relative) != expected_digest:
                raise VendorError(f"vendored file checksum failed: {directory}/{relative}")
    return len(expected)


def extract_archive(archive_path: Path, destination: Path) -> Path:
    """Validate archive paths before extracting the vendor tree."""
    with tarfile.open(archive_path, "r:xz") as archive:
        members = archive.getmembers()
        if not members or members[0].name != "vendor":
            raise VendorError("vendor archive has no vendor root")
        for member in members:
            path = PurePosixPath(member.name)
            if path.is_absolute() or path.parts[0] != "vendor":
                raise VendorError(f"unsafe vendor archive path: {member.name}")
            if any(part in ("", ".", "..") for part in path.parts):
                raise VendorError(f"unsafe vendor archive path: {member.name}")
            if not (member.isdir() or member.isfile()):
                raise VendorError(f"unsupported vendor archive entry: {member.name}")
        archive.extractall(destination, members=members)
    return destination / "vendor"


def extract_verified_archive(archive_path: Path, destination: Path) -> Path:
    """Extract an archive after validating its paths and entry types."""
    return extract_archive(archive_path, destination)


def copied_rust_tree(destination: Path) -> Path:
    """Copy only the native crate inputs required for offline resolution."""
    copied = destination / "src" / "rust"
    shutil.copytree(
        RUST_ROOT,
        copied,
        ignore=shutil.ignore_patterns(
            "target",
            "vendor.tar.xz",
            "vendor-config.toml",
        ),
    )
    return copied


def verify_archive(archive_path: Path, config_path: Path) -> int:
    """Resolve the copied crate using only the archived dependencies."""
    config = normalize_vendor_config(config_path.read_text(encoding="utf-8"))

    with tempfile.TemporaryDirectory(prefix="delta-sharing-r-vendor-check-") as temporary:
        root = Path(temporary)
        rust_root = copied_rust_tree(root / "source")
        source_root = rust_root.parent
        vendor_root = extract_verified_archive(archive_path, source_root)
        package_count = verify_vendored_checksums(vendor_root, rust_root / "Cargo.lock")
        cargo_config = source_root / ".cargo" / "config.toml"
        cargo_config.parent.mkdir()
        cargo_config.write_text(config, encoding="utf-8")

        cargo_home = root / "cargo-home"
        cargo_home.mkdir()
        environment = os.environ.copy()
        environment.update(
            {
                "CARGO_HOME": str(cargo_home),
                "CARGO_NET_OFFLINE": "true",
                "CARGO_TARGET_DIR": str(root / "target"),
            }
        )
        result = subprocess.run(
            [
                "cargo",
                "metadata",
                "--manifest-path",
                str(rust_root / "Cargo.toml"),
                "--format-version",
                "1",
                "--frozen",
                "--all-features",
            ],
            cwd=source_root,
            env=environment,
            check=False,
            stdout=subprocess.DEVNULL,
        )
        if result.returncode:
            raise VendorError("Cargo could not resolve the vendor archive offline")

        return package_count


def generate() -> None:
    """Generate, verify, and publish the release-only archive files."""
    with tempfile.TemporaryDirectory(prefix="delta-sharing-r-vendor-") as temporary:
        root = Path(temporary)
        vendor_root = root / "vendor"
        result = subprocess.run(
            [
                "cargo",
                "vendor",
                "--manifest-path",
                str(RUST_ROOT / "Cargo.toml"),
                "--locked",
                "--offline",
                "--respect-source-config",
                "--versioned-dirs",
                str(vendor_root),
            ],
            cwd=REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode:
            raise VendorError(
                "cargo vendor failed; run cargo fetch --locked before generating:\n"
                f"{result.stderr.strip()}"
            )

        archive = root / ARCHIVE_PATH.name
        config = root / CONFIG_PATH.name
        write_archive(vendor_root, archive)
        config.write_text(CONFIG, encoding="utf-8")
        package_count = verify_archive(archive, config)
        os.replace(archive, ARCHIVE_PATH)
        os.replace(config, CONFIG_PATH)

    describe(package_count)


def check() -> None:
    """Verify existing release archive files."""
    if not ARCHIVE_PATH.is_file() or not CONFIG_PATH.is_file():
        raise VendorError("vendor.tar.xz and vendor-config.toml are both required")
    describe(verify_archive(ARCHIVE_PATH, CONFIG_PATH))


def describe(package_count: int) -> None:
    """Print stable release evidence for CI logs."""
    print(f"vendor_packages={package_count}")
    print(f"archive_bytes={ARCHIVE_PATH.stat().st_size}")
    print(f"archive_sha256={sha256(ARCHIVE_PATH)}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("generate", "check"))
    arguments = parser.parse_args()
    try:
        if arguments.command == "generate":
            generate()
        else:
            check()
    except (OSError, VendorError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
