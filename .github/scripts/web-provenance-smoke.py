#!/usr/bin/env python3
"""Exercise the OCI, Boot JAR, and release provenance cross-checks with a tiny local image."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path


SCRIPT = Path(__file__).with_name("web-provenance.py")
VERSION = "0.0.0-smoke"
TAPSTATE_REVISION = "0123456789abcdef0123456789abcdef01234567"
WEB_REVISION = "abcdef0123456789abcdef0123456789abcdef01"
HTML = b"<!doctype html><title>Smoke</title>\n"
ASSET = b"window.__tapstateWebSmoke = true;\n"


def digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def json_bytes(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


def add_blob(layout: Path, payload: bytes) -> str:
    value = digest(payload)
    algorithm, encoded = value.split(":", 1)
    path = layout / "blobs" / algorithm / encoded
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return value


def boot_jar(html: bytes = HTML, asset: bytes = ASSET) -> bytes:
    files = {"index.html": html, "assets/app.js": asset}
    manifest = "".join(f"{hashlib.sha256(content).hexdigest()}  ./{name}\n" for name, content in sorted(files.items())).encode()
    props = (
        "repository=tapstate/tapstate-web\n"
        f"revision={WEB_REVISION}\n"
        f"files.sha256={hashlib.sha256(manifest).hexdigest()}\n"
        f"tapstate.revision={TAPSTATE_REVISION}\n"
        f"release.version={VERSION}\n"
    ).encode()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as jar:
        jar.writestr("META-INF/tapstate-web.properties", props)
        jar.writestr("META-INF/tapstate-web.files.sha256", manifest)
        for name, content in files.items():
            jar.writestr(f"BOOT-INF/classes/static/{name}", content)
    return output.getvalue()


def tamper_asset(jar_bytes: bytes) -> bytes:
    source = zipfile.ZipFile(io.BytesIO(jar_bytes))
    output = io.BytesIO()
    with source, zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as altered:
        for name in source.namelist():
            payload = b"tampered asset\n" if name == "BOOT-INF/classes/static/assets/app.js" else source.read(name)
            altered.writestr(name, payload)
    return output.getvalue()


def image_layer(jar_bytes: bytes) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as layer:
        info = tarfile.TarInfo("opt/tapstate/tapstate.jar")
        info.size = len(jar_bytes)
        layer.addfile(info, io.BytesIO(jar_bytes))
    return output.getvalue()


def write_layout(layout: Path, jar_bytes: bytes = b"") -> None:
    if not jar_bytes:
        jar_bytes = boot_jar()
    with zipfile.ZipFile(io.BytesIO(jar_bytes)) as jar:
        web_files_sha256 = hashlib.sha256(jar.read("META-INF/tapstate-web.files.sha256")).hexdigest()
    platforms = []
    for architecture in ("amd64", "arm64"):
        layer_digest = add_blob(layout, image_layer(jar_bytes))
        labels = {
            "org.opencontainers.image.version": VERSION,
            "org.opencontainers.image.revision": TAPSTATE_REVISION,
            "io.tapstate.web.revision": WEB_REVISION,
            "io.tapstate.web.files.sha256": web_files_sha256,
        }
        config_digest = add_blob(layout, json_bytes({"config": {"Labels": labels}}))
        image_manifest_digest = add_blob(
            layout,
            json_bytes(
                {
                    "schemaVersion": 2,
                    "config": {"digest": config_digest},
                    "layers": [{"digest": layer_digest}],
                }
            ),
        )
        platforms.append(
            {
                "digest": image_manifest_digest,
                "platform": {"os": "linux", "architecture": architecture},
            }
        )
    index_digest = add_blob(layout, json_bytes({"schemaVersion": 2, "manifests": platforms}))
    (layout / "index.json").write_text(json.dumps({"schemaVersion": 2, "manifests": [{"digest": index_digest}]}))


def invoke(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(SCRIPT), *args], text=True, capture_output=True, check=False)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="tapstate-web-provenance-") as temporary:
        root = Path(temporary)
        layout = root / "oci"
        layout.mkdir()
        write_layout(layout)
        provenance = root / "release.json"
        created = invoke(
            "create", "--oci-layout", str(layout), "--output", str(provenance),
            "--version", VERSION, "--tapstate-revision", TAPSTATE_REVISION, "--web-revision", WEB_REVISION,
        )
        if created.returncode:
            print(created.stderr, file=sys.stderr)
            return 1
        verified = invoke("verify", "--oci-layout", str(layout), "--provenance", str(provenance))
        if verified.returncode:
            print(verified.stderr, file=sys.stderr)
            return 1
        print("ok - matching OCI archive, Boot JAR, labels, and provenance verify")

        value = json.loads(provenance.read_text())
        value["image"]["manifestDigest"] = "sha256:" + "0" * 64
        provenance.write_text(json.dumps(value))
        tampered = invoke("verify", "--oci-layout", str(layout), "--provenance", str(provenance))
        if tampered.returncode == 0:
            print("not ok - a mismatched manifest digest is rejected", file=sys.stderr)
            return 1
        print("ok - a mismatched manifest digest is rejected")

        invalid_layout = root / "invalid-oci"
        invalid_layout.mkdir()
        write_layout(invalid_layout, tamper_asset(boot_jar()))
        invalid = root / "invalid.json"
        rejected = invoke(
            "create", "--oci-layout", str(invalid_layout), "--output", str(invalid),
            "--version", VERSION, "--tapstate-revision", TAPSTATE_REVISION, "--web-revision", WEB_REVISION,
        )
        if rejected.returncode == 0:
            print("not ok - an asset that disagrees with the JAR manifest is rejected", file=sys.stderr)
            return 1
        print("ok - an asset that disagrees with the JAR manifest is rejected")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
