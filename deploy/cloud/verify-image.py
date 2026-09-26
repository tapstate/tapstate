#!/usr/bin/env python3
"""Inspect both Cloud OCI platforms before a release is approved or pushed."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import posixpath
import re
import sys
import tarfile
from pathlib import Path
from typing import Any

from importlib.util import module_from_spec, spec_from_file_location


STAGING_SCRIPT = Path(__file__).with_name("stage-connectors.py")
SPEC = spec_from_file_location("stage_connectors", STAGING_SCRIPT)
assert SPEC is not None and SPEC.loader is not None
STAGING = module_from_spec(SPEC)
SPEC.loader.exec_module(STAGING)


class ImageError(Exception):
    pass


def blob(layout: Path, digest: str) -> bytes:
    if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ImageError("OCI descriptor has an invalid SHA-256 digest")
    path = layout / "blobs" / "sha256" / digest.removeprefix("sha256:")
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise ImageError(f"OCI blob is missing: {digest}") from exc
    if "sha256:" + hashlib.sha256(data).hexdigest() != digest:
        raise ImageError(f"OCI blob digest mismatch: {digest}")
    return data


def object_from_bytes(data: bytes, description: str) -> dict[str, Any]:
    try:
        value = json.loads(data)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ImageError(f"{description} is not JSON") from exc
    if not isinstance(value, dict):
        raise ImageError(f"{description} is not a JSON object")
    return value


def normalized(name: str) -> str:
    path = posixpath.normpath(name.removeprefix("./"))
    if path.startswith("../") or path.startswith("/") or path in {".", ".."}:
        raise ImageError("OCI layer has an unsafe path")
    return path


def selected_files(layout: Path, layers: list[Any], platform: str) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for descriptor in layers:
        if not isinstance(descriptor, dict):
            raise ImageError(f"{platform} has a malformed OCI layer descriptor")
        layer = blob(layout, descriptor.get("digest"))
        try:
            with tarfile.open(fileobj=io.BytesIO(layer), mode="r:*") as archive:
                for member in archive:
                    path = normalized(member.name)
                    wanted = path == "opt/tapstate/tapstate.jar" or path.startswith(
                        ("opt/tapstate/connectors/", "opt/tapstate/release/")
                    )
                    if not wanted or member.isdir():
                        continue
                    if path in files or not member.isfile() or "/.wh." in path:
                        raise ImageError(f"{platform} has a duplicate, linked, or deleted image input: {path}")
                    stream = archive.extractfile(member)
                    if stream is None:
                        raise ImageError(f"{platform} cannot read image input: {path}")
                    files[path] = stream.read()
        except (tarfile.TarError, OSError) as exc:
            raise ImageError(f"{platform} has an unreadable OCI layer") from exc
    return files


def verify_platform(layout: Path, descriptor: dict[str, Any], platform: str,
                    entries: list[dict[str, Any]], license_files: list[dict[str, Any]],
                    lock_bytes: bytes) -> tuple[dict[str, str], str]:
    manifest = object_from_bytes(blob(layout, descriptor.get("digest")), f"{platform} manifest")
    config_descriptor = manifest.get("config")
    if not isinstance(config_descriptor, dict):
        raise ImageError(f"{platform} has no OCI config descriptor")
    config = object_from_bytes(blob(layout, config_descriptor.get("digest")), f"{platform} config")
    labels = config.get("config", {}).get("Labels")
    if not isinstance(labels, dict):
        raise ImageError(f"{platform} has no image labels")
    relevant_labels = {}
    for key in ("org.opencontainers.image.version", "org.opencontainers.image.revision",
                "org.opencontainers.image.licenses", "io.tapstate.web.revision",
                "io.tapstate.web.files.sha256"):
        if not isinstance(labels.get(key), str) or not labels[key]:
            raise ImageError(f"{platform} is missing image label {key}")
        relevant_labels[key] = labels[key]
    layers = manifest.get("layers")
    if not isinstance(layers, list) or not layers:
        raise ImageError(f"{platform} has no OCI layers")
    files = selected_files(layout, layers, platform)
    expected_jar_paths = {f"opt/tapstate/connectors/{entry['id']}-connector.jar" for entry in entries}
    actual_jar_paths = {path for path in files if path.startswith("opt/tapstate/connectors/")}
    if actual_jar_paths != expected_jar_paths:
        raise ImageError(f"{platform} connector seed differs from the seven locked IDs")
    expected_release_paths = {
        "opt/tapstate/release/connectors.lock.json",
        "opt/tapstate/release/connectors.sha256",
    }
    expected_release_paths.update(
        f"opt/tapstate/release/licenses/{entry['name']}" for entry in license_files
    )
    actual_release_paths = {path for path in files if path.startswith("opt/tapstate/release/")}
    if actual_release_paths != expected_release_paths:
        raise ImageError(f"{platform} release metadata has missing or unexpected files")
    if "opt/tapstate/tapstate.jar" not in files:
        raise ImageError(f"{platform} has no Boot JAR")
    boot_jar_sha256 = hashlib.sha256(files["opt/tapstate/tapstate.jar"]).hexdigest()
    if files.get("opt/tapstate/release/connectors.lock.json") != lock_bytes:
        raise ImageError(f"{platform} embeds a different connector lock")
    expected_checksums = "".join(
        f"{entry['sha256']}  {entry['id']}-connector.jar\n" for entry in entries
    ).encode("ascii")
    if files.get("opt/tapstate/release/connectors.sha256") != expected_checksums:
        raise ImageError(f"{platform} embeds incorrect connector checksums")
    for entry in entries:
        path = f"opt/tapstate/connectors/{entry['id']}-connector.jar"
        jar = files[path]
        if len(jar) != entry["bytes"] or hashlib.sha256(jar).hexdigest() != entry["sha256"]:
            raise ImageError(f"{platform} {entry['id']} JAR differs from the release lock")
    for entry in license_files:
        path = f"opt/tapstate/release/licenses/{entry['name']}"
        license_bytes = files[path]
        if len(license_bytes) != entry["bytes"] or hashlib.sha256(license_bytes).hexdigest() != entry["sha256"]:
            raise ImageError(f"{platform} {entry['name']} companion license differs from the release lock")
    return relevant_labels, boot_jar_sha256


def verify(layout: Path, lock_path: Path, boot_jar_path: Path | None = None) -> str:
    try:
        entries, license_files = STAGING.read_lock(lock_path)
        lock_bytes = lock_path.read_bytes()
        root = object_from_bytes((layout / "index.json").read_bytes(), "OCI root index")
    except (OSError, STAGING.StageError) as exc:
        raise ImageError(f"cannot load image or lock: {exc}") from exc
    roots = root.get("manifests")
    if not isinstance(roots, list) or len(roots) != 1 or not isinstance(roots[0], dict):
        raise ImageError("OCI archive must have one top-level multi-platform index")
    top_digest = roots[0].get("digest")
    nested = object_from_bytes(blob(layout, top_digest), "OCI multi-platform index")
    descriptors = nested.get("manifests")
    if not isinstance(descriptors, list):
        raise ImageError("OCI archive has no platform manifests")
    platforms = {}
    for descriptor in descriptors:
        if not isinstance(descriptor, dict):
            raise ImageError("OCI archive has a malformed platform descriptor")
        info = descriptor.get("platform") or {}
        if not isinstance(info, dict):
            raise ImageError("OCI archive has malformed platform metadata")
        if info.get("os") != "linux" or info.get("architecture") not in {"amd64", "arm64"}:
            continue
        platform = f"linux/{info['architecture']}"
        if platform in platforms:
            raise ImageError(f"OCI archive has duplicate platform {platform}")
        platforms[platform] = verify_platform(
            layout, descriptor, platform, entries, license_files, lock_bytes
        )
    if set(platforms) != {"linux/amd64", "linux/arm64"}:
        raise ImageError(f"OCI archive platforms are {sorted(platforms)}, expected amd64 and arm64")
    if platforms["linux/amd64"][1] != platforms["linux/arm64"][1]:
        raise ImageError("OCI platforms have different Boot JAR bytes")
    if platforms["linux/amd64"][0] != platforms["linux/arm64"][0]:
        raise ImageError("OCI platforms have different release or Web provenance labels")
    if boot_jar_path is not None:
        try:
            expected_sha256, _ = STAGING.sha256_and_size(boot_jar_path)
        except OSError as exc:
            raise ImageError("cannot read the expected Boot JAR") from exc
        if platforms["linux/amd64"][1] != expected_sha256:
            raise ImageError("OCI Boot JAR differs from the verified build input")
    return top_digest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oci-layout", required=True, type=Path)
    parser.add_argument("--lock", required=True, type=Path)
    parser.add_argument("--boot-jar", required=True, type=Path)
    args = parser.parse_args()
    try:
        digest = verify(args.oci_layout, args.lock, args.boot_jar)
    except ImageError as exc:
        print(f"Cloud image verification refused: {exc}", file=sys.stderr)
        return 1
    print(f"verified linux/amd64 and linux/arm64 Cloud connector seed: {digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
