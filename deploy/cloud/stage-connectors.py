#!/usr/bin/env python3
"""Verify a seven-connector release lock and stage immutable Cloud image inputs.

The lock is supplied by the release process after publication and license review.
This script does not download, approve, or publish connector artifacts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Any


REQUIRED_IDS = (
    "mysql", "mongodb", "postgres", "oracle", "sqlserver", "mongodb-atlas", "aws-rds-mysql"
)
ENTRY_KEYS = {"id", "bytes", "sha256", "upstreamRevision", "pdkApiVersion", "specPath"}
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
REVISION = re.compile(r"[0-9a-f]{40}\Z")
VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
SPEC_PATH = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\.json\Z")


class StageError(Exception):
    pass


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise StageError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def read_lock(path: Path) -> list[dict[str, Any]]:
    try:
        lock = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_object)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise StageError(f"cannot read connector lock: {exc}") from exc
    if not isinstance(lock, dict) or set(lock) != {"schemaVersion", "connectors"}:
        raise StageError("connector lock must have only schemaVersion and connectors")
    if type(lock["schemaVersion"]) is not int or lock["schemaVersion"] != 1:
        raise StageError("connector lock schemaVersion must be 1")
    entries = lock["connectors"]
    if not isinstance(entries, list) or len(entries) != len(REQUIRED_IDS):
        raise StageError("connector lock must contain exactly seven entries")
    ids = []
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != ENTRY_KEYS:
            raise StageError("connector entry has missing or unexpected fields")
        connector_id = entry["id"]
        if not isinstance(connector_id, str) or connector_id not in REQUIRED_IDS:
            raise StageError(f"unexpected connector id: {connector_id!r}")
        if type(entry["bytes"]) is not int or not 1_000_000 <= entry["bytes"] <= 128_000_000:
            raise StageError(f"{connector_id}: invalid shaded JAR length")
        if not isinstance(entry["sha256"], str) or not SHA256.fullmatch(entry["sha256"]):
            raise StageError(f"{connector_id}: invalid SHA-256")
        if not isinstance(entry["upstreamRevision"], str) or not REVISION.fullmatch(entry["upstreamRevision"]):
            raise StageError(f"{connector_id}: invalid upstream revision")
        if not isinstance(entry["pdkApiVersion"], str) or not VERSION.fullmatch(entry["pdkApiVersion"]):
            raise StageError(f"{connector_id}: invalid PDK API version")
        if not isinstance(entry["specPath"], str) or not SPEC_PATH.fullmatch(entry["specPath"]):
            raise StageError(f"{connector_id}: unsafe spec path")
        ids.append(connector_id)
    if sorted(ids) != sorted(REQUIRED_IDS):
        raise StageError("connector lock has a duplicate or missing id")
    return entries


def manifest_headers(raw: bytes) -> dict[str, str]:
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeError as exc:
        raise StageError("JAR manifest is not UTF-8") from exc
    unfolded = []
    for line in lines:
        if not line:
            break
        if line.startswith(" "):
            if not unfolded:
                raise StageError("JAR manifest starts with a continuation")
            unfolded[-1] += line[1:]
        else:
            unfolded.append(line)
    headers = {}
    for line in unfolded:
        key, separator, value = line.partition(": ")
        if not separator or key in headers:
            raise StageError("JAR manifest has malformed or duplicate headers")
        headers[key] = value
    return headers


def sha256_and_size(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
            size += len(block)
    return digest.hexdigest(), size


def verify_jar(path: Path, entry: dict[str, Any]) -> None:
    connector_id = entry["id"]
    try:
        mode = path.lstat().st_mode
    except OSError as exc:
        raise StageError(f"{connector_id}: JAR is missing") from exc
    if not stat.S_ISREG(mode):
        raise StageError(f"{connector_id}: JAR must be a regular file")
    actual_hash, actual_size = sha256_and_size(path)
    if (actual_hash, actual_size) != (entry["sha256"], entry["bytes"]):
        raise StageError(f"{connector_id}: JAR bytes differ from the release lock")
    try:
        with zipfile.ZipFile(path) as jar:
            names = jar.namelist()
            for required in ("META-INF/MANIFEST.MF", entry["specPath"]):
                if names.count(required) != 1:
                    raise StageError(f"{connector_id}: JAR has missing or duplicate {required}")
            headers = manifest_headers(jar.read("META-INF/MANIFEST.MF"))
            spec = json.loads(jar.read(entry["specPath"]).decode("utf-8"))
    except (OSError, zipfile.BadZipFile, UnicodeError, json.JSONDecodeError) as exc:
        raise StageError(f"{connector_id}: JAR manifest or spec is unreadable") from exc
    # The published SQL Server JAR is built from the upstream mssql module, while its PDK id is sqlserver.
    implementation_title = "mssql-connector" if connector_id == "sqlserver" else f"{connector_id}-connector"
    expected_headers = {
        "Implementation-Title": implementation_title,
        "Git-Commit-Id": entry["upstreamRevision"],
        "PDK-API-Version": entry["pdkApiVersion"],
    }
    for key, expected in expected_headers.items():
        if headers.get(key) != expected:
            raise StageError(f"{connector_id}: JAR {key} disagrees with the release lock")
    if not isinstance(spec, dict) or not isinstance(spec.get("properties"), dict):
        raise StageError(f"{connector_id}: JAR spec has no properties")
    if spec["properties"].get("id") != connector_id:
        raise StageError(f"{connector_id}: JAR spec id disagrees with the release lock")


def stage(lock_path: Path, jar_dir: Path, stage_dir: Path) -> None:
    entries = read_lock(lock_path)
    expected_names = {f"{entry['id']}-connector.jar" for entry in entries}
    if not jar_dir.is_dir():
        raise StageError("connector JAR directory is missing")
    actual_names = {path.name for path in jar_dir.iterdir() if path.suffix == ".jar"}
    if actual_names != expected_names:
        raise StageError(f"connector JAR set differs from lock: missing={sorted(expected_names - actual_names)}, "
                         f"extra={sorted(actual_names - expected_names)}")
    if stage_dir.exists() or stage_dir.is_symlink():
        raise StageError("stage directory already exists; refusing to overwrite it")
    for entry in entries:
        verify_jar(jar_dir / f"{entry['id']}-connector.jar", entry)

    stage_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".cloud-connectors-", dir=stage_dir.parent))
    try:
        connectors = temporary / "connectors"
        release = temporary / "release"
        connectors.mkdir()
        release.mkdir()
        for entry in entries:
            filename = f"{entry['id']}-connector.jar"
            destination = connectors / filename
            shutil.copyfile(jar_dir / filename, destination)
            verify_jar(destination, entry)
        shutil.copyfile(lock_path, release / "connectors.lock.json")
        (release / "connectors.sha256").write_text(
            "".join(f"{entry['sha256']}  {entry['id']}-connector.jar\n" for entry in entries),
            encoding="ascii",
        )
        os.rename(temporary, stage_dir)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", required=True, type=Path)
    parser.add_argument("--jar-dir", required=True, type=Path)
    parser.add_argument("--stage-dir", required=True, type=Path)
    args = parser.parse_args()
    try:
        stage(args.lock, args.jar_dir, args.stage_dir)
    except StageError as exc:
        print(f"cloud connector staging refused: {exc}", file=sys.stderr)
        return 1
    print(f"staged seven locked connectors in {args.stage_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
