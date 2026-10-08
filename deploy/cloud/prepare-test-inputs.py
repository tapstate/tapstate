#!/usr/bin/env python3
"""Fetch pinned public Cloud connector inputs for startup tests, without connecting to any database."""

from __future__ import annotations

import argparse
import importlib.util
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", type=Path, default=Path(__file__).with_name("connectors.lock.json"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        prepare(args.lock, args.output)
    except STAGE.StageError as failure:
        print(f"Cloud test inputs refused: {failure}", file=sys.stderr)
        return 1
    print(f"Prepared seven verified public connectors in {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
