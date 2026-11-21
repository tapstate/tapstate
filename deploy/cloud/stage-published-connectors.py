#!/usr/bin/env python3
"""Select and stage official Maven connector releases using the packaged host's compatibility rules."""
from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urlsplit

SPEC = importlib.util.spec_from_file_location("connector_stage", Path(__file__).with_name("stage-connectors.py"))
assert SPEC is not None and SPEC.loader is not None
STAGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(STAGE)
ROOT = Path(__file__).resolve().parents[2]


def select(boot_jar: Path, profile: str, java: str, extra: tuple[str, ...] = ()) -> list[dict]:
    command = [java, "-Dloader.main=io.tapstate.app.ConnectorBundleSelection", "-cp", str(boot_jar),
               "org.springframework.boot.loader.launch.PropertiesLauncher", profile, *extra]
    try:
        process = subprocess.run(command, capture_output=True, check=True, timeout=300)
        selected = json.loads(process.stdout, object_pairs_hook=STAGE.unique_object)
    except (OSError, subprocess.SubprocessError, ValueError) as failure:
        diagnostic = (failure.stderr or "").decode(errors="replace") if isinstance(
            failure, subprocess.CalledProcessError) else ""
        line = next((line for line in diagnostic.splitlines() if "Required connector" in line
                     or "No compatible published JAR" in line), "")
        raise STAGE.StageError(line.strip() or "packaged connector selection failed") from failure
    if not isinstance(selected, dict) or set(selected) != {"schemaVersion", "connectors"} or selected["schemaVersion"] != 1:
        raise STAGE.StageError("packaged connector selector returned an invalid response")
    return selected["connectors"]


def download(url: str, destination: Path) -> None:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise STAGE.StageError("connector selection contains an invalid download URL")
    process = subprocess.run([
        "curl", "--fail", "--silent", "--show-error", "--location", "--proto", "=https", "--proto-redir", "=https",
        "--max-time", "120", "--max-filesize", "128000000", "--output", str(destination), url,
    ], capture_output=True, timeout=125, check=False)
    if process.returncode != 0:
        raise STAGE.StageError(f"{destination.name}: Maven artifact download failed (curl exit={process.returncode})")


def inspect(path: Path, selected: dict) -> dict:
    digest, size = STAGE.sha256_and_size(path)
    if digest != selected["sha256"]:
        raise STAGE.StageError(f"{selected['id']}: JAR SHA-256 differs from the selected release")
    with zipfile.ZipFile(path) as jar:
        headers = STAGE.manifest_headers(jar.read("META-INF/MANIFEST.MF"))
        paths = []
        for name in jar.namelist():
            if "/" in name or not name.endswith(".json") or "spec" not in name.lower():
                continue
            try:
                tree = json.loads(jar.read(name))
            except (ValueError, UnicodeError):
                continue
            if isinstance(tree, dict) and isinstance(tree.get("properties"), dict) and tree["properties"].get("id") == selected["id"]:
                paths.append(name)
        if len(paths) != 1:
            raise STAGE.StageError(f"{selected['id']}: expected exactly one matching connector spec")
    if headers.get("Implementation-Version") != selected["version"]:
        raise STAGE.StageError(f"{selected['id']}: JAR version differs from the selected Maven release")
    return {"id": selected["id"], "bytes": size, "sha256": digest,
            "upstreamRevision": headers.get("Git-Commit-Id"), "pdkApiVersion": selected["pdkApiVersion"],
            "specPath": paths[0], **{key: selected[key] for key in STAGE.COORDINATE_KEYS}}


def stage(boot_jar: Path, profile: str, output: Path, java: str = "java", *,
          selection: list[dict] | None = None, extra: tuple[str, ...] = ()) -> None:
    if output.exists() or output.is_symlink():
        raise STAGE.StageError("stage directory already exists; refusing to overwrite it")
    if profile == "op":
        required = tuple(dict.fromkeys(("mongodb", *extra)))
    else:
        required = STAGE.CLOUD_IDS if profile == "cloud" else STAGE.PUBLISHED_ONPREM_IDS
    selected = select(boot_jar, profile, java, extra) if selection is None else selection
    if not isinstance(selected, list) or len(selected) != len(required) or {entry.get("id") for entry in selected} != set(required):
        raise STAGE.StageError("packaged connector selection has an incomplete or duplicate profile")
    with tempfile.TemporaryDirectory(prefix="maven-connector-bundle-") as temporary:
        jars = Path(temporary)
        entries = []
        for entry in selected:
            path = jars / f"{entry['id']}-connector.jar"
            download(entry["url"], path)
            entries.append(inspect(path, entry))
        licenses = []
        if profile != "cloud":
            for name in STAGE.REQUIRED_LICENSE_FILES:
                source = ROOT / "deploy/connectors/licenses" / name
                shutil.copyfile(source, jars / name)
                digest, size = STAGE.sha256_and_size(source)
                licenses.append({"name": name, "bytes": size, "sha256": digest})
        lock = jars / "connectors.lock.json"
        lock.write_text(json.dumps({"schemaVersion": 3, "connectors": entries, "licenseFiles": licenses}, indent=2) + "\n")
        STAGE.stage(lock, jars, output, required)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--boot-jar", required=True, type=Path)
    parser.add_argument("--profile", required=True, choices=("onprem", "cloud"))
    parser.add_argument("--stage-dir", required=True, type=Path)
    parser.add_argument("--java", default="java")
    args = parser.parse_args()
    try:
        stage(args.boot_jar, args.profile, args.stage_dir, args.java)
    except (STAGE.StageError, OSError, KeyError, zipfile.BadZipFile, subprocess.SubprocessError) as failure:
        print(f"published connector staging refused: {failure}", file=sys.stderr)
        return 1
    print(f"staged verified {args.profile} Maven connector releases in {args.stage_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
