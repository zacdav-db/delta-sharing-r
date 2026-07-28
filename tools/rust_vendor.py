#!/usr/bin/env python3
"""Generate and verify the locked Rust source-vendor archive.

The release archive is intentionally generated, not committed on development
branches. `generate` uses `cargo vendor --locked --versioned-dirs`, writes a
normalized Cargo source replacement, and creates a byte-reproducible xz tar.
`check` validates archive shape and checksums before asking Cargo to resolve the
copied crate with a fresh, frozen, offline Cargo home.
"""

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
from typing import Iterable


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
RUST_ROOT = REPOSITORY_ROOT / "src" / "rust"
MANIFEST_PATH = RUST_ROOT / "Cargo.toml"
LOCK_PATH = RUST_ROOT / "Cargo.lock"
ARCHIVE_PATH = RUST_ROOT / "vendor.tar.xz"
CONFIG_PATH = RUST_ROOT / "vendor-config.toml"
VENDOR_DIRECTORY = "vendor"
XZ_PRESET = 9


class VendorError(RuntimeError):
    """A safe, user-facing vendor validation failure."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalized_mode(path: Path) -> int:
    mode = path.lstat().st_mode
    if stat.S_ISDIR(mode):
        return 0o755
    if stat.S_ISLNK(mode):
        return 0o777
    return 0o755 if mode & 0o111 else 0o644


def archive_paths(vendor_root: Path) -> Iterable[Path]:
    yield vendor_root
    yield from sorted(
        vendor_root.rglob("*"),
        key=lambda path: path.relative_to(vendor_root.parent).as_posix(),
    )


def write_deterministic_archive(vendor_root: Path, destination: Path) -> None:
    """Write stable metadata, ordering, and xz bytes for one vendor tree."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as raw_stream:
        with lzma.LZMAFile(
            raw_stream,
            mode="w",
            format=lzma.FORMAT_XZ,
            check=lzma.CHECK_CRC64,
            preset=XZ_PRESET,
        ) as compressed:
            with tarfile.open(
                fileobj=compressed,
                mode="w",
                format=tarfile.PAX_FORMAT,
            ) as archive:
                for path in archive_paths(vendor_root):
                    relative = path.relative_to(vendor_root.parent).as_posix()
                    info = tarfile.TarInfo(relative)
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    info.mtime = 0
                    info.mode = normalized_mode(path)

                    if path.is_symlink():
                        info.type = tarfile.SYMTYPE
                        info.linkname = os.readlink(path)
                        archive.addfile(info)
                    elif path.is_dir():
                        info.type = tarfile.DIRTYPE
                        archive.addfile(info)
                    elif path.is_file():
                        info.type = tarfile.REGTYPE
                        info.size = path.stat().st_size
                        with path.open("rb") as source:
                            archive.addfile(info, source)
                    else:
                        raise VendorError(
                            f"unsupported vendored filesystem entry: {relative}"
                        )


def normalize_vendor_config(config_text: str) -> str:
    """Accept Cargo's emitted config only when it targets `vendor`."""
    normalized = "\n".join(
        line.rstrip() for line in config_text.replace("\r\n", "\n").splitlines()
    ).strip()
    if not normalized:
        raise VendorError("cargo vendor did not emit a source replacement")
    try:
        parsed = tomllib.loads(normalized)
    except tomllib.TOMLDecodeError as error:
        raise VendorError("cargo vendor emitted invalid TOML") from error

    sources = parsed.get("source")
    if not isinstance(sources, dict):
        raise VendorError("cargo vendor config has no source replacement")
    vendored = sources.get("vendored-sources")
    if not isinstance(vendored, dict) or vendored.get("directory") != VENDOR_DIRECTORY:
        raise VendorError("cargo vendor config does not target the package vendor directory")
    crates_io = sources.get("crates-io")
    if not isinstance(crates_io, dict) or crates_io.get("replace-with") != (
        "vendored-sources"
    ):
        raise VendorError("cargo vendor config does not replace crates.io")

    if str(REPOSITORY_ROOT) in normalized or tempfile.gettempdir() in normalized:
        raise VendorError("cargo vendor config contains a machine-local path")
    return normalized + "\n"


def locked_registry_packages(lock_path: Path) -> dict[str, str]:
    with lock_path.open("rb") as stream:
        lock = tomllib.load(stream)
    expected: dict[str, str] = {}
    for package in lock.get("package", []):
        source = package.get("source")
        if source is None:
            continue
        if not source.startswith("registry+"):
            raise VendorError(
                "the locked graph contains a non-registry dependency; "
                "review source replacement policy before vendoring"
            )
        name = package["name"]
        version = package["version"]
        checksum = package.get("checksum")
        if not isinstance(checksum, str):
            raise VendorError(f"locked registry package {name} {version} has no checksum")
        directory = f"{name}-{version}"
        if directory in expected:
            raise VendorError(f"duplicate versioned vendor directory: {directory}")
        expected[directory] = checksum
    return expected


def validate_link(member_name: PurePosixPath, link_name: str) -> None:
    target = PurePosixPath(link_name)
    if target.is_absolute():
        raise VendorError(f"archive link is absolute: {member_name}")
    resolved = member_name.parent.joinpath(target)
    depth = 0
    for component in resolved.parts:
        if component == "..":
            depth -= 1
        elif component not in ("", "."):
            depth += 1
        if depth < 1:
            raise VendorError(f"archive link leaves vendor root: {member_name}")


def validate_member(member: tarfile.TarInfo, previous_name: str | None) -> str:
    name = PurePosixPath(member.name)
    if name.is_absolute() or not name.parts or name.parts[0] != VENDOR_DIRECTORY:
        raise VendorError(f"archive member is outside vendor root: {member.name}")
    if any(part in ("", ".", "..") for part in name.parts):
        raise VendorError(f"archive member has an unsafe path: {member.name}")
    if previous_name is not None and member.name <= previous_name:
        raise VendorError("archive members are duplicated or not sorted")
    if member.uid != 0 or member.gid != 0 or member.uname or member.gname:
        raise VendorError(f"archive ownership metadata is not normalized: {member.name}")
    if member.mtime != 0:
        raise VendorError(f"archive timestamp is not normalized: {member.name}")
    expected_mode = (
        0o755
        if member.isdir()
        else 0o777
        if member.issym()
        else 0o755
        if member.mode & 0o111
        else 0o644
    )
    if member.mode != expected_mode:
        raise VendorError(f"archive mode is not normalized: {member.name}")
    if not (member.isdir() or member.isfile() or member.issym()):
        raise VendorError(f"archive contains an unsupported entry: {member.name}")
    if member.issym():
        validate_link(name, member.linkname)
    return member.name


def extract_verified_archive(archive_path: Path, destination: Path) -> Path:
    """Validate first, then extract without tarfile.extractall()."""
    seen: set[str] = set()
    previous_name: str | None = None
    with tarfile.open(archive_path, mode="r:xz") as archive:
        members = archive.getmembers()
        if not members or members[0].name != VENDOR_DIRECTORY:
            raise VendorError("archive does not start with the vendor root")
        for member in members:
            previous_name = validate_member(member, previous_name)
            if member.name in seen:
                raise VendorError(f"archive contains a duplicate member: {member.name}")
            seen.add(member.name)

        for member in members:
            target = destination.joinpath(*PurePosixPath(member.name).parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                target.chmod(member.mode)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                source = archive.extractfile(member)
                if source is None:
                    raise VendorError(f"archive member cannot be read: {member.name}")
                with target.open("wb") as output:
                    shutil.copyfileobj(source, output)
                target.chmod(member.mode)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.symlink_to(member.linkname)
    return destination / VENDOR_DIRECTORY


def verify_vendored_checksums(vendor_root: Path, lock_path: Path) -> int:
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
            f"vendor directories do not match Cargo.lock; missing={missing}, extra={extra}"
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
            raise VendorError(f"vendored file list differs from checksum data: {directory}")
        for relative, expected_digest in files.items():
            if sha256_file(package_root / relative) != expected_digest:
                raise VendorError(f"vendored file checksum failed: {directory}/{relative}")
    return len(expected)


def copy_native_crate(destination: Path) -> Path:
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


def frozen_metadata_check(vendor_root: Path, config_text: str, temp_root: Path) -> int:
    copied_rust = copy_native_crate(temp_root)
    copied_src = copied_rust.parent
    shutil.copytree(vendor_root, copied_src / VENDOR_DIRECTORY, symlinks=True)
    cargo_config = copied_src / ".cargo" / "config.toml"
    cargo_config.parent.mkdir(parents=True)
    cargo_config.write_text(config_text, encoding="utf-8", newline="\n")
    cargo_home = temp_root / "cargo-home"
    cargo_home.mkdir()
    environment = os.environ.copy()
    environment.update(
        {
            "CARGO_HOME": str(cargo_home),
            "CARGO_NET_OFFLINE": "true",
        }
    )
    command = [
        "cargo",
        "metadata",
        "--manifest-path",
        str(copied_rust / "Cargo.toml"),
        "--format-version",
        "1",
        "--all-features",
        "--frozen",
    ]
    completed = subprocess.run(
        command,
        cwd=copied_src,
        env=environment,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    metadata = json.loads(completed.stdout)
    return len(metadata.get("packages", []))


def generate() -> None:
    if not MANIFEST_PATH.is_file() or not LOCK_PATH.is_file():
        raise VendorError("the native Cargo manifest and lockfile are required")
    with tempfile.TemporaryDirectory(prefix="delta-sharing-r-vendor-") as temporary:
        temp_root = Path(temporary)
        copied_rust = copy_native_crate(temp_root)
        vendor_root = temp_root / VENDOR_DIRECTORY
        command = [
            "cargo",
            "vendor",
            "--manifest-path",
            str(copied_rust / "Cargo.toml"),
            "--locked",
            "--offline",
            "--respect-source-config",
            "--versioned-dirs",
            VENDOR_DIRECTORY,
        ]
        completed = subprocess.run(
            command,
            cwd=temp_root,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if completed.returncode != 0:
            raise VendorError(
                "cargo vendor failed; ensure every locked crate is cached "
                "with `cargo fetch --locked`"
            )
        config_text = normalize_vendor_config(completed.stdout)
        package_count = verify_vendored_checksums(vendor_root, copied_rust / "Cargo.lock")

        staged_archive = temp_root / "vendor.tar.xz"
        write_deterministic_archive(vendor_root, staged_archive)
        verification_root = temp_root / "verify"
        verification_root.mkdir()
        extracted = extract_verified_archive(staged_archive, verification_root)
        verify_vendored_checksums(extracted, copied_rust / "Cargo.lock")
        frozen_count = frozen_metadata_check(
            extracted,
            config_text,
            temp_root / "offline-check",
        )

        staged_config = temp_root / "vendor-config.toml"
        staged_config.write_text(config_text, encoding="utf-8", newline="\n")
        ARCHIVE_PATH.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged_archive, ARCHIVE_PATH)
        os.replace(staged_config, CONFIG_PATH)

    print(f"lock_sha256={sha256_file(LOCK_PATH)}")
    print(f"registry_packages={package_count}")
    print(f"resolved_packages={frozen_count}")
    print(f"archive_bytes={ARCHIVE_PATH.stat().st_size}")
    print(f"archive_sha256={sha256_file(ARCHIVE_PATH)}")
    print(f"config_bytes={CONFIG_PATH.stat().st_size}")


def check() -> None:
    if not ARCHIVE_PATH.is_file() or not CONFIG_PATH.is_file():
        raise VendorError(
            "both src/rust/vendor.tar.xz and vendor-config.toml are required"
        )
    config_text = normalize_vendor_config(CONFIG_PATH.read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory(prefix="delta-sharing-r-vendor-check-") as temporary:
        temp_root = Path(temporary)
        vendor_root = extract_verified_archive(ARCHIVE_PATH, temp_root / "archive")
        package_count = verify_vendored_checksums(vendor_root, LOCK_PATH)
        resolved_count = frozen_metadata_check(
            vendor_root,
            config_text,
            temp_root / "offline",
        )
    print(f"lock_sha256={sha256_file(LOCK_PATH)}")
    print(f"registry_packages={package_count}")
    print(f"resolved_packages={resolved_count}")
    print(f"archive_bytes={ARCHIVE_PATH.stat().st_size}")
    print(f"archive_sha256={sha256_file(ARCHIVE_PATH)}")
    print(f"config_bytes={CONFIG_PATH.stat().st_size}")


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
    except (VendorError, OSError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
