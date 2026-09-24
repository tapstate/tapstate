#!/usr/bin/env python3
"""Create or verify release provenance by reading the OCI archive, image labels, and Boot JAR."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import sys
import tarfile
import zipfile
from pathlib import Path
from posixpath import normpath
from typing import Any


class ProvenanceError(Exception):
    pass


def sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProvenanceError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ProvenanceError(f"{path} must contain a JSON object")
    return value


def blob_path(layout: Path, digest: str) -> Path:
    algorithm, separator, encoded = digest.partition(":")
    if not separator or algorithm != "sha256" or len(encoded) != 64:
        raise ProvenanceError(f"unsupported OCI digest: {digest}")
    path = layout / "blobs" / algorithm / encoded
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise ProvenanceError(f"OCI blob is missing: {digest}") from exc
    if sha256(payload) != digest:
        raise ProvenanceError(f"OCI blob content does not match digest {digest}")
    return path


def properties_from_jar(jar_bytes: bytes, platform: str) -> dict[str, str]:
    try:
        with zipfile.ZipFile(io.BytesIO(jar_bytes)) as jar:
            raw = jar.read("META-INF/tapstate-web.properties").decode("utf-8")
            files_manifest = jar.read("META-INF/tapstate-web.files.sha256")
            static_files = {
                name.removeprefix("BOOT-INF/classes/static/"): jar.read(name)
                for name in jar.namelist()
                if name.startswith("BOOT-INF/classes/static/") and not name.endswith("/")
            }
    except (zipfile.BadZipFile, KeyError, UnicodeDecodeError) as exc:
        raise ProvenanceError(f"{platform} Boot JAR has no valid Web provenance metadata") from exc
    props: dict[str, str] = {}
    for line in raw.splitlines():
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key or key in props:
            raise ProvenanceError(f"{platform} Boot JAR metadata has a malformed or duplicate entry")
        props[key] = value
    expected_files: dict[str, str] = {}
    try:
        for line in files_manifest.decode("utf-8").splitlines():
            digest, separator, name = line.partition("  ")
            if not separator or len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
                raise ProvenanceError(f"{platform} Web file manifest contains a malformed entry")
            normalized = normpath(name.removeprefix("*"))
            if normalized.startswith("../") or normalized.startswith("/") or normalized in {".", ".."}:
                raise ProvenanceError(f"{platform} Web file manifest contains an unsafe path")
            path = normalized.removeprefix("./")
            if path in expected_files:
                raise ProvenanceError(f"{platform} Web file manifest contains a duplicate path")
            expected_files[path] = digest
    except UnicodeDecodeError as exc:
        raise ProvenanceError(f"{platform} Web file manifest is not UTF-8") from exc
    if not expected_files:
        raise ProvenanceError(f"{platform} Web file manifest is empty")
    if set(static_files) != set(expected_files):
        raise ProvenanceError(f"{platform} packaged static files do not match the Web file manifest")
    for name, digest in expected_files.items():
        if hashlib.sha256(static_files[name]).hexdigest() != digest:
            raise ProvenanceError(f"{platform} packaged Web file does not match its manifest: {name}")
    if hashlib.sha256(files_manifest).hexdigest() != props.get("files.sha256"):
        raise ProvenanceError(f"{platform} Web manifest content disagrees with files.sha256 metadata")
    return props


def metadata_from_layer(layer: bytes, platform: str) -> dict[str, str]:
    try:
        with tarfile.open(fileobj=io.BytesIO(layer), mode="r:*") as archive:
            for entry in archive:
                normalized = normpath(entry.name.removeprefix("./"))
                if entry.isfile() and normalized == "opt/tapstate/tapstate.jar":
                    stream = archive.extractfile(entry)
                    if stream is None:
                        break
                    return properties_from_jar(stream.read(), platform)
    except (tarfile.TarError, OSError) as exc:
        raise ProvenanceError(f"cannot inspect {platform} OCI layer: {exc}") from exc
    raise ProvenanceError(f"{platform} OCI image does not contain /opt/tapstate/tapstate.jar")


def image_metadata(layout: Path) -> tuple[str, dict[str, dict[str, str]]]:
    root = load_json(layout / "index.json")
    roots = root.get("manifests")
    if not isinstance(roots, list) or len(roots) != 1 or not isinstance(roots[0], dict):
        raise ProvenanceError("OCI archive must contain one top-level multi-platform index descriptor")
    top = roots[0]
    digest = top.get("digest")
    if not isinstance(digest, str):
        raise ProvenanceError("OCI archive top-level descriptor has no digest")
    nested = load_json(blob_path(layout, digest))
    descriptors = nested.get("manifests")
    if not isinstance(descriptors, list):
        raise ProvenanceError("OCI top-level descriptor is not a multi-platform image index")

    metadata: dict[str, dict[str, str]] = {}
    for descriptor in descriptors:
        if not isinstance(descriptor, dict):
            continue
        platform_info = descriptor.get("platform") or {}
        if platform_info.get("os") != "linux" or platform_info.get("architecture") not in {"amd64", "arm64"}:
            continue
        platform = f"linux/{platform_info['architecture']}"
        if platform in metadata:
            raise ProvenanceError(f"OCI archive contains duplicate platform {platform}")
        manifest = load_json(blob_path(layout, descriptor.get("digest", "")))
        config_descriptor = manifest.get("config", {})
        config = load_json(blob_path(layout, config_descriptor.get("digest", "")))
        labels = config.get("config", {}).get("Labels")
        if not isinstance(labels, dict):
            raise ProvenanceError(f"{platform} image has no OCI config labels")
        required_labels = {
            "org.opencontainers.image.version": "release.version",
            "org.opencontainers.image.revision": "tapstate.revision",
            "io.tapstate.web.revision": "revision",
            "io.tapstate.web.files.sha256": "files.sha256",
        }
        layer_descriptors = manifest.get("layers")
        if not isinstance(layer_descriptors, list):
            raise ProvenanceError(f"{platform} image has no layers")
        jar_props = None
        for layer_descriptor in layer_descriptors:
            if not isinstance(layer_descriptor, dict):
                raise ProvenanceError(f"{platform} image has a malformed layer descriptor")
            layer_path = blob_path(layout, layer_descriptor.get("digest", ""))
            try:
                jar_props = metadata_from_layer(layer_path.read_bytes(), platform)
            except ProvenanceError as exc:
                if "does not contain" not in str(exc):
                    raise
        if jar_props is None:
            raise ProvenanceError(f"{platform} image has no Boot JAR provenance")
        for label, property_name in required_labels.items():
            if not labels.get(label):
                raise ProvenanceError(f"{platform} image is missing label {label}")
            if labels[label] != jar_props.get(property_name):
                raise ProvenanceError(f"{platform} label {label} disagrees with Boot JAR metadata")
        metadata[platform] = jar_props

    if set(metadata) != {"linux/amd64", "linux/arm64"}:
        raise ProvenanceError(f"OCI archive platforms are {sorted(metadata)}, expected linux/amd64 and linux/arm64")
    if metadata["linux/amd64"] != metadata["linux/arm64"]:
        raise ProvenanceError("amd64 and arm64 images carry different Web release metadata")
    return digest, metadata["linux/amd64"]


def expected_metadata(version: str, tapstate_revision: str, web_revision: str) -> dict[str, str]:
    return {
        "repository": "tapstate/tapstate-web",
        "revision": web_revision,
        "tapstate.revision": tapstate_revision,
        "release.version": version,
    }


def create(args: argparse.Namespace) -> None:
    digest, props = image_metadata(args.oci_layout)
    for key, expected in expected_metadata(args.version, args.tapstate_revision, args.web_revision).items():
        if props.get(key) != expected:
            raise ProvenanceError(f"image metadata {key} is {props.get(key)!r}, expected {expected!r}")
    files_digest = props.get("files.sha256", "")
    if len(files_digest) != 64 or any(char not in "0123456789abcdef" for char in files_digest):
        raise ProvenanceError("Boot JAR metadata has no valid Web file manifest digest")
    provenance = {
        "schemaVersion": 1,
        "releaseVersion": args.version,
        "tapstate": {"repository": "tapstate/tapstate", "revision": args.tapstate_revision},
        "web": {
            "repository": "tapstate/tapstate-web",
            "revision": args.web_revision,
            "filesSha256": files_digest,
        },
        "image": {
            "repository": "ghcr.io/tapstate/tapstate",
            "tag": args.version,
            "manifestDigest": digest,
            "platforms": ["linux/amd64", "linux/arm64"],
        },
    }
    verify_values(provenance, digest, props)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def verify_values(provenance: dict[str, Any], digest: str, props: dict[str, str]) -> None:
    try:
        version = provenance["releaseVersion"]
        tapstate_revision = provenance["tapstate"]["revision"]
        web_revision = provenance["web"]["revision"]
        files_digest = provenance["web"]["filesSha256"]
        image_digest = provenance["image"]["manifestDigest"]
        platforms = provenance["image"]["platforms"]
    except (KeyError, TypeError) as exc:
        raise ProvenanceError("provenance asset is missing required fields") from exc
    if type(provenance.get("schemaVersion")) is not int or provenance["schemaVersion"] != 1:
        raise ProvenanceError("provenance schemaVersion must be 1")
    tapstate = provenance.get("tapstate")
    web = provenance.get("web")
    image = provenance.get("image")
    if not isinstance(tapstate, dict) or not isinstance(web, dict) or not isinstance(image, dict):
        raise ProvenanceError("provenance repository, Web, and image fields must be objects")
    if tapstate.get("repository") != "tapstate/tapstate":
        raise ProvenanceError("provenance Tapstate repository is unexpected")
    if web.get("repository") != "tapstate/tapstate-web":
        raise ProvenanceError("provenance Web repository is unexpected")
    if image.get("repository") != "ghcr.io/tapstate/tapstate":
        raise ProvenanceError("provenance image repository is unexpected")
    if image.get("tag") != version:
        raise ProvenanceError("provenance image tag disagrees with the release version")
    if not isinstance(version, str) or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-.][0-9A-Za-z.-]+)?", version):
        raise ProvenanceError("provenance release version is malformed")
    for label, revision in (("Tapstate", tapstate_revision), ("Web", web_revision)):
        if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise ProvenanceError(f"provenance {label} revision is not a full commit id")
    for label, value in (("release version", version), ("Tapstate revision", tapstate_revision), ("Web revision", web_revision)):
        if not isinstance(value, str) or not value:
            raise ProvenanceError(f"provenance {label} is empty")
    if not isinstance(files_digest, str) or len(files_digest) != 64 or any(char not in "0123456789abcdef" for char in files_digest):
        raise ProvenanceError("provenance Web file manifest digest is malformed")
    if image_digest != digest:
        raise ProvenanceError("provenance manifest digest disagrees with the OCI archive")
    expected = expected_metadata(version, tapstate_revision, web_revision)
    expected["files.sha256"] = files_digest
    for key, value in expected.items():
        if props.get(key) != value:
            raise ProvenanceError(f"provenance {key} disagrees with the image")
    if not isinstance(platforms, list) or platforms != ["linux/amd64", "linux/arm64"]:
        raise ProvenanceError("provenance platform list is missing or out of canonical order")


def verify(args: argparse.Namespace) -> None:
    provenance = load_json(args.provenance)
    digest, props = image_metadata(args.oci_layout)
    verify_values(provenance, digest, props)
    print(f"verified OCI archive {digest} against Boot JAR metadata and {args.provenance}")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    make = commands.add_parser("create")
    make.add_argument("--oci-layout", type=Path, required=True)
    make.add_argument("--output", type=Path, required=True)
    make.add_argument("--version", required=True)
    make.add_argument("--tapstate-revision", required=True)
    make.add_argument("--web-revision", required=True)
    make.set_defaults(run=create)
    check = commands.add_parser("verify")
    check.add_argument("--oci-layout", type=Path, required=True)
    check.add_argument("--provenance", type=Path, required=True)
    check.set_defaults(run=verify)
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        args.run(args)
    except ProvenanceError as exc:
        print(f"web-provenance: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
