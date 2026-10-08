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
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from posixpath import normpath
from typing import Any


PROFILE_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "web-assets-profile.py"
PROFILE_SPEC = spec_from_file_location("web_assets_profile", PROFILE_SCRIPT)
assert PROFILE_SPEC is not None and PROFILE_SPEC.loader is not None
PROFILE = module_from_spec(PROFILE_SPEC)
PROFILE_SPEC.loader.exec_module(PROFILE)


class ProvenanceError(Exception):
    pass


def sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def normalize_cloud_console_url(value: Any) -> str:
    try:
        return PROFILE.normalize_cloud_console_url(value)
    except PROFILE.WebAssetsError as exc:
        raise ProvenanceError("Cloud Console URL is not a valid explicit HTTPS URL") from exc


def declared_profile(web_profile: str, cloud_console_url: str | None) -> tuple[str, str | None]:
    if web_profile not in {"cloud", "onprem"}:
        raise ProvenanceError("a known Web profile must be declared explicitly")
    if web_profile == "cloud":
        if cloud_console_url is None:
            raise ProvenanceError("the declared Cloud profile requires a Cloud Console URL")
        return web_profile, normalize_cloud_console_url(cloud_console_url)
    if cloud_console_url is not None:
        raise ProvenanceError("the declared on-prem profile must not contain a Cloud Console URL")
    return web_profile, None


def validate_profile_metadata(props: dict[str, str], description: str) -> tuple[str, str | None]:
    profile = props.get("web.profile")
    if profile not in {"cloud", "onprem"}:
        raise ProvenanceError(f"{description} is missing a known Web profile")
    if profile == "onprem":
        if "cloud.console.url" in props:
            raise ProvenanceError(f"{description} on-prem metadata contains a Cloud Console URL")
        return profile, None
    url = props.get("cloud.console.url")
    normalized_url = normalize_cloud_console_url(url)
    if url != normalized_url:
        raise ProvenanceError(f"{description} Cloud Console URL is not canonical")
    return profile, normalized_url


def verify_declared_profile(props: dict[str, str], web_profile: str,
                            cloud_console_url: str | None) -> None:
    expected_profile, expected_url = declared_profile(web_profile, cloud_console_url)
    actual_profile, actual_url = validate_profile_metadata(props, "Boot JAR")
    if actual_profile != expected_profile:
        raise ProvenanceError("Boot JAR profile disagrees with the declared build input")
    if actual_url != expected_url:
        raise ProvenanceError("Boot JAR Cloud Console URL disagrees with the declared build input")


def verify_declared_boot_jar(props: dict[str, str], boot_jar_sha256: str) -> None:
    if not isinstance(boot_jar_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", boot_jar_sha256):
        raise ProvenanceError("declared Boot JAR SHA-256 must be 64 lowercase hexadecimal characters")
    if props.get("image.boot-jar.sha256") != boot_jar_sha256:
        raise ProvenanceError("embedded Boot JAR bytes disagree with the declared build input")


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_json_object)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProvenanceError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ProvenanceError(f"{path} must contain a JSON object")
    return value


def unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProvenanceError("JSON provenance or OCI metadata contains a duplicate field")
        result[key] = value
    return result


def blob_path(layout: Path, digest: str) -> Path:
    if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ProvenanceError("OCI descriptor has an invalid SHA-256 digest")
    algorithm, encoded = digest.split(":", 1)
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
            if len(jar.namelist()) != len(set(jar.namelist())):
                raise ProvenanceError(f"{platform} Boot JAR contains a duplicate ZIP entry")
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
    validate_profile_metadata(props, f"{platform} Boot JAR")
    props["image.boot-jar.sha256"] = hashlib.sha256(jar_bytes).hexdigest()
    return props


def metadata_from_layer(layer: bytes, platform: str, jar_seen: bool = False) -> dict[str, str]:
    result = None
    try:
        with tarfile.open(fileobj=io.BytesIO(layer), mode="r:*") as archive:
            for entry in archive:
                normalized = normpath(entry.name.removeprefix("./"))
                if normalized in {"opt", "opt/tapstate"} and not entry.isdir():
                    raise ProvenanceError(f"{platform} OCI image has a linked or non-directory Boot JAR parent")
                if (jar_seen or result is not None) and normalized in {
                        ".wh.opt", "opt/.wh.tapstate", "opt/tapstate/.wh.tapstate.jar",
                        ".wh..wh..opq", "opt/.wh..wh..opq", "opt/tapstate/.wh..wh..opq"}:
                    raise ProvenanceError(f"{platform} OCI image deletes a Boot JAR input")
                if normalized == "opt/tapstate/tapstate.jar":
                    if not entry.isfile() or result is not None:
                        raise ProvenanceError(f"{platform} OCI image has a linked or duplicate Boot JAR input")
                    stream = archive.extractfile(entry)
                    if stream is None:
                        break
                    jar_bytes = stream.read()
                    result = properties_from_jar(jar_bytes, platform)
    except (tarfile.TarError, OSError) as exc:
        raise ProvenanceError(f"cannot inspect {platform} OCI layer: {exc}") from exc
    if result is None:
        raise ProvenanceError(f"{platform} OCI image does not contain /opt/tapstate/tapstate.jar")
    return result


def validate_image_labels(props: dict[str, str], labels: dict[str, Any], platform: str) -> None:
    required_labels = {
        "org.opencontainers.image.version": "release.version",
        "org.opencontainers.image.revision": "tapstate.revision",
        "io.tapstate.web.revision": "revision",
        "io.tapstate.web.files.sha256": "files.sha256",
        "io.tapstate.web.profile": "web.profile",
    }
    for label, property_name in required_labels.items():
        if not labels.get(label):
            raise ProvenanceError(f"{platform} image is missing label {label}")
        if labels[label] != props.get(property_name):
            raise ProvenanceError(f"{platform} label {label} disagrees with Boot JAR metadata")
    profile, console_url = validate_profile_metadata(props, f"{platform} Boot JAR")
    distribution = labels.get("io.tapstate.distribution")
    if not isinstance(distribution, str) or distribution not in {"cloud", "onprem"}:
        raise ProvenanceError(f"{platform} image is missing a known distribution")
    if distribution != profile:
        raise ProvenanceError(f"{platform} image distribution disagrees with the Boot JAR profile")
    url_label = labels.get("io.tapstate.web.cloud-console-url")
    if profile == "cloud" and url_label != console_url:
        raise ProvenanceError(f"{platform} image Cloud Console URL label disagrees with Boot JAR metadata")
    if profile == "onprem" and url_label is not None and url_label != "":
        raise ProvenanceError(f"{platform} on-prem image has a Cloud Console URL label")
    props["image.distribution"] = distribution


def image_metadata(layout: Path) -> tuple[str, dict[str, str]]:
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
        if not isinstance(platform_info, dict):
            raise ProvenanceError("OCI archive has malformed platform metadata")
        architecture = platform_info.get("architecture")
        if architecture is not None and not isinstance(architecture, str):
            raise ProvenanceError("OCI archive has malformed platform metadata")
        if platform_info.get("os") != "linux" or architecture not in {"amd64", "arm64"}:
            continue
        platform = f"linux/{platform_info['architecture']}"
        if platform in metadata:
            raise ProvenanceError(f"OCI archive contains duplicate platform {platform}")
        manifest = load_json(blob_path(layout, descriptor.get("digest", "")))
        config_descriptor = manifest.get("config", {})
        if not isinstance(config_descriptor, dict):
            raise ProvenanceError(f"{platform} image has no valid OCI config descriptor")
        config = load_json(blob_path(layout, config_descriptor.get("digest", "")))
        image_config = config.get("config")
        if not isinstance(image_config, dict):
            raise ProvenanceError(f"{platform} image has no valid OCI config")
        labels = image_config.get("Labels")
        if not isinstance(labels, dict):
            raise ProvenanceError(f"{platform} image has no OCI config labels")
        layer_descriptors = manifest.get("layers")
        if not isinstance(layer_descriptors, list):
            raise ProvenanceError(f"{platform} image has no layers")
        jar_props = None
        for layer_descriptor in layer_descriptors:
            if not isinstance(layer_descriptor, dict):
                raise ProvenanceError(f"{platform} image has a malformed layer descriptor")
            layer_path = blob_path(layout, layer_descriptor.get("digest", ""))
            try:
                candidate = metadata_from_layer(layer_path.read_bytes(), platform, jar_props is not None)
            except ProvenanceError as exc:
                if "does not contain" not in str(exc):
                    raise
            else:
                if jar_props is not None:
                    raise ProvenanceError(f"{platform} image contains duplicate Boot JAR inputs across layers")
                jar_props = candidate
        if jar_props is None:
            raise ProvenanceError(f"{platform} image has no Boot JAR provenance")
        validate_image_labels(jar_props, labels, platform)
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


def image_repository(props: dict[str, str]) -> str:
    distribution = props.get("image.distribution")
    if distribution not in {"cloud", "onprem"}:
        raise ProvenanceError("image has an unknown distribution")
    return "ghcr.io/tapstate/tapstate-cloud" if distribution == "cloud" else "ghcr.io/tapstate/tapstate"


def create(args: argparse.Namespace) -> None:
    web_profile, console_url = declared_profile(args.web_profile, args.cloud_console_url)
    digest, props = image_metadata(args.oci_layout)
    verify_declared_profile(props, web_profile, console_url)
    verify_declared_boot_jar(props, args.boot_jar_sha256)
    for key, expected in expected_metadata(args.version, args.tapstate_revision, args.web_revision).items():
        if props.get(key) != expected:
            raise ProvenanceError(f"image metadata {key} is {props.get(key)!r}, expected {expected!r}")
    files_digest = props.get("files.sha256", "")
    if len(files_digest) != 64 or any(char not in "0123456789abcdef" for char in files_digest):
        raise ProvenanceError("Boot JAR metadata has no valid Web file manifest digest")
    provenance = {
        "schemaVersion": 1,
        "releaseVersion": args.version,
        "bootJarSha256": args.boot_jar_sha256,
        "tapstate": {"repository": "tapstate/tapstate", "revision": args.tapstate_revision},
        "web": {
            "repository": "tapstate/tapstate-web",
            "revision": args.web_revision,
            "filesSha256": files_digest,
            "profile": web_profile,
        },
        "image": {
            "repository": image_repository(props),
            "tag": args.version,
            "manifestDigest": digest,
            "platforms": ["linux/amd64", "linux/arm64"],
        },
    }
    if console_url is not None:
        provenance["web"]["cloudConsoleUrl"] = console_url
    verify_values(provenance, digest, props, web_profile, console_url, args.boot_jar_sha256)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def verify_values(provenance: dict[str, Any], digest: str, props: dict[str, str],
                  web_profile: str, cloud_console_url: str | None, boot_jar_sha256: str) -> None:
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
    declared, declared_url = declared_profile(web_profile, cloud_console_url)
    verify_declared_profile(props, declared, declared_url)
    verify_declared_boot_jar(props, boot_jar_sha256)
    if provenance.get("bootJarSha256") != boot_jar_sha256:
        raise ProvenanceError("provenance Boot JAR SHA-256 disagrees with the declared build input")
    if web.get("profile") != declared:
        raise ProvenanceError("provenance Web profile disagrees with the declared build input")
    if declared == "cloud":
        raw_url = web.get("cloudConsoleUrl")
        normalized_url = normalize_cloud_console_url(raw_url)
        if raw_url != normalized_url or normalized_url != declared_url:
            raise ProvenanceError("provenance Cloud Console URL disagrees with the declared build input")
    elif "cloudConsoleUrl" in web:
        raise ProvenanceError("on-prem provenance must not contain a Cloud Console URL")
    if image.get("repository") != image_repository(props):
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
    verify_values(provenance, digest, props, args.web_profile, args.cloud_console_url,
                  args.boot_jar_sha256)
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
    make.add_argument("--web-profile", choices=("cloud", "onprem"), required=True)
    make.add_argument("--cloud-console-url")
    make.add_argument("--boot-jar-sha256", required=True)
    make.set_defaults(run=create)
    check = commands.add_parser("verify")
    check.add_argument("--oci-layout", type=Path, required=True)
    check.add_argument("--provenance", type=Path, required=True)
    check.add_argument("--web-profile", choices=("cloud", "onprem"), required=True)
    check.add_argument("--cloud-console-url")
    check.add_argument("--boot-jar-sha256", required=True)
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
