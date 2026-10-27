#!/usr/bin/env python3
"""Fetch pinned public Cloud connector inputs for startup tests, without connecting to any database."""

from __future__ import annotations

import argparse
import importlib.util
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


SPEC = importlib.util.spec_from_file_location("cloud_connector_stage", Path(__file__).with_name("stage-connectors.py"))
assert SPEC is not None and SPEC.loader is not None
STAGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(STAGE)
PUBLIC_RELEASE = "https://github.com/tapstate/tapstate/releases/download/connectors-preview"


def download(url: str, destination: Path, maximum: int) -> None:
    # Use the same OS-trusted transport as release preparation; do not disable certificate checks
    # or depend on a developer Python installation's separate certificate bundle.
    try:
        result = subprocess.run([
            "curl", "--fail", "--silent", "--show-error", "--location",
            "--proto", "=https", "--proto-redir", "=https", "--max-time", "60",
            "--retry", "2", "--retry-max-time", "120", "--max-filesize", str(maximum),
            "--output", str(destination), url,
        ], capture_output=True, timeout=125, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise STAGE.StageError(f"{destination.name}: public input transport unavailable") from None
    if result.returncode != 0:
        # curl's raw stderr can contain redirect URLs or proxy details. Keep only its numeric exit.
        raise STAGE.StageError(f"{destination.name}: public input download failed (curl exit={result.returncode})")
    if destination.stat().st_size > maximum:
        raise STAGE.StageError(f"{destination.name}: downloaded bytes exceed lock length")


def prepare(lock: Path, output: Path, base_url: str = PUBLIC_RELEASE) -> None:
    entries, licenses = STAGE.read_lock(lock)
    if output.exists() or output.is_symlink():
        raise STAGE.StageError("test input output already exists; refusing to overwrite it")
    # Only this owned directory is cleaned up; successfully verified output is retained for the tests.
    with tempfile.TemporaryDirectory(prefix="cloud-public-test-inputs-") as temporary:
        jars = Path(temporary)
        for entry in entries + licenses:
            name = f"{entry['id']}-connector.jar" if "id" in entry else entry["name"]
            download(f"{base_url.rstrip('/')}/{name}", jars / name, entry["bytes"])
        STAGE.stage(lock, jars, output)


def prepare_mongodb(lock: Path, output: Path, base_url: str = PUBLIC_RELEASE) -> None:
    """Prepare one verified test dependency, not a partial Cloud release bundle."""
    entries, licenses = STAGE.read_lock(lock)
    expected = next(entry for entry in entries if entry["id"] == "mongodb")
    if output.exists() or output.is_symlink():
        raise STAGE.StageError("test input output already exists; refusing to overwrite it")
    with tempfile.TemporaryDirectory(prefix="mongodb-public-test-input-") as temporary:
        staged = Path(temporary)
        jar = staged / "mongodb-connector.jar"
        download(f"{base_url.rstrip('/')}/{jar.name}", jar, expected["bytes"])
        STAGE.verify_jar(jar, expected)
        for entry in licenses:
            license_file = staged / entry["name"]
            download(f"{base_url.rstrip('/')}/{license_file.name}", license_file, entry["bytes"])
            STAGE.verify_companion_license(license_file, entry)
        # Claim a new output only after all inputs verify. A failed copy leaves an explicit,
        # incomplete output that a later invocation refuses to replace.
        output.mkdir(parents=True, exist_ok=False)
        for source in staged.iterdir():
            shutil.copyfile(source, output / source.name)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", type=Path, default=Path(__file__).with_name("connectors.lock.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mongodb-only", action="store_true", help="prepare the managed-target test JAR and licenses instead of a Cloud release bundle")
    args = parser.parse_args()
    try:
        if args.mongodb_only:
            prepare_mongodb(args.lock, args.output)
        else:
            prepare(args.lock, args.output)
    except STAGE.StageError as failure:
        print(f"Cloud test inputs refused: {failure}", file=sys.stderr)
        return 1
    kind = "the verified MongoDB test dependency" if args.mongodb_only else "eight verified public connector inputs"
    print(f"Prepared {kind} in {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
