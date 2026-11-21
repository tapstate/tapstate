#!/usr/bin/env python3
"""Build, repackage, or verify the Tapstate OP archive."""

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("published_connectors", HERE.parent / "cloud/stage-published-connectors.py")
PUBLISHED = importlib.util.module_from_spec(spec)
spec.loader.exec_module(PUBLISHED)

PLATFORMS = ("linux-amd64", "darwin-arm64")
ROOT = "tapstate-op"
DIGEST = re.compile(r"[a-f0-9]{64}")
VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-.][A-Za-z0-9]+)*")
CONNECTOR_ID = re.compile(r"[a-z][a-z0-9-]*")


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=PUBLISHED.STAGE.unique_object)


def extract(archive, destination, root):
    with tarfile.open(archive, "r:gz") as tar:
        names = set()
        for member in tar:
            path = PurePosixPath(member.name)
            if (not path.parts or path.parts[0] != root or ".." in path.parts or path in names
                    or "\\" in member.name or not (member.isdir() or member.isfile())):
                raise ValueError(f"unsafe or unexpected archive member {member.name!r}")
            names.add(path)
        tar.extractall(destination, filter="data")
    return destination / root


def checked_file(package, entry):
    # The archive may be extracted over a package with files from a different version.
    path = package / entry["path"]
    if not DIGEST.fullmatch(entry["sha256"]) or sha(path) != entry["sha256"]:
        raise ValueError(f"wrong checksum for {entry['path']}")
    return path


def verified(root, version, expected_platforms, expected_boot=None):
    manifest = read_json(root / "op-manifest.json")
    if manifest["schemaVersion"] != 1 or manifest["version"] != version:
        raise ValueError("OP manifest schema or release version does not match")
    server = manifest["server"]
    if server["path"] != f"lib/app-{version}-boot.jar":
        raise ValueError("unexpected server JAR location")
    checked_file(root, server)
    if expected_boot and server["sha256"] != expected_boot:
        raise ValueError("OP server JAR differs from the release image")
    controllers = manifest["controllers"]
    if set(controllers) != set(expected_platforms):
        raise ValueError("unexpected platform set in OP manifest")
    for platform, entry in controllers.items():
        if entry["path"] != f"bin/{platform}/tapstate-ctl" or not os.access(checked_file(root, entry), os.X_OK):
            raise ValueError(f"invalid controller path or mode for {platform}")
    lock_path = root / "release/connectors.lock.json"
    if manifest["connectorLockSha256"] != sha(lock_path):
        raise ValueError("connector lock checksum does not match")
    connectors = read_json(lock_path)["connectors"]
    if connectors != manifest["connectors"]:
        raise ValueError("connector manifests disagree")
    ids = [item["id"] for item in connectors]
    if ("mongodb" not in ids or len(set(ids)) != len(ids)
            or any(not CONNECTOR_ID.fullmatch(key) for key in ids)):
        raise ValueError("invalid OP connector selection")
    for entry in connectors:
        checked_file(root, {"path": f"connectors/{entry['id']}-connector.jar", "sha256": entry["sha256"]})
    expected = {server["path"], *(entry["path"] for entry in controllers.values()),
                "op-manifest.json", "release/connectors.lock.json", "release/connectors.sha256",
                "bin/tapstate-server", "README.md", "NOTICE", "LICENSE",
                *(f"connectors/{key}-connector.jar" for key in ids)}
    for license_file in read_json(lock_path)["licenseFiles"]:
        name = license_file["name"]
        if name not in PUBLISHED.STAGE.REQUIRED_LICENSE_FILES:
            raise ValueError(f"unexpected bundled license {name!r}")
        path = f"release/licenses/{name}"
        checked_file(root, {"path": path, "sha256": license_file["sha256"]})
        expected.add(path)
    files = {path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()}
    if not expected <= files or any(not name.startswith("conf/") for name in files - expected):
        raise ValueError("OP archive contains missing or unexpected files")
    if len(expected_platforms) == len(PLATFORMS) and ids != ["mongodb"]:
        raise ValueError("official OP archive must contain exactly the MongoDB connector")
    return manifest


def release_checksum(archive, checksum):
    fields = checksum.read_text(encoding="ascii").split()
    if (len(fields) != 2 or fields[1].lstrip("*") != archive.name
            or not DIGEST.fullmatch(fields[0]) or sha(archive) != fields[0]):
        raise ValueError(f"OP archive checksum does not match {archive.name}")
    return fields[0]


def write_package(root, version, platforms, bundle, output, source=None):
    for directory in ("connectors", "release"):
        shutil.copytree(bundle / directory, root / directory)
    lock = root / "release/connectors.lock.json"
    boot = f"lib/app-{version}-boot.jar"
    controllers = {}
    for platform in platforms:
        path = f"bin/{platform}/tapstate-ctl"
        controllers[platform] = {"path": path, "sha256": sha(root / path)}
    manifest = {"schemaVersion": 1, "version": version,
                "server": {"path": boot, "sha256": sha(root / boot)},
                "controllers": controllers, "connectorLockSha256": sha(lock),
                "connectors": read_json(lock)["connectors"]}
    if source:
        manifest["sourceArchiveSha256"] = source
    (root / "op-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    verified(root, version, platforms)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent) as temporary:
        staged = Path(temporary) / output.name
        with tarfile.open(staged, "w:gz") as archive:
            archive.add(root, arcname=ROOT)
        checksum = staged.with_name(staged.name + ".sha256")
        checksum.write_text(f"{sha(staged)}  {staged.name}\n", encoding="ascii")
        staged.replace(output)
        checksum.replace(output.with_name(output.name + ".sha256"))


def official(args, work):
    release = extract(args.assembly, work, f"tapstate-server-{args.version}")
    root = release.rename(work / ROOT)
    boot = root / f"lib/app-{args.version}-boot.jar"
    if args.boot_sha256 and sha(boot) != args.boot_sha256:
        raise ValueError("server Assembly JAR differs from the release image")
    for platform in PLATFORMS:
        target = root / "bin" / platform / "tapstate-ctl"
        target.parent.mkdir()
        shutil.copyfile(args.controllers / platform / "tapstate-ctl", target)
        target.chmod(0o755)
    for name in ("README.md", "LICENSE", "NOTICE"):
        source = HERE / name if name == "README.md" else HERE.parents[1] / name
        shutil.copyfile(source, root / name)
    bundle = work / "bundle"
    PUBLISHED.stage(boot, "op", bundle, args.java)
    write_package(root, args.version, PLATFORMS, bundle, args.output)


def manual(args, work):
    source_digest = release_checksum(args.archive, args.checksum)
    root = extract(args.archive, work, ROOT)
    manifest = verified(root, args.version, PLATFORMS)
    extra = tuple(dict.fromkeys(re.split(r"[,\s]+", args.connectors.strip()))) if args.connectors.strip() else ()
    if any(not CONNECTOR_ID.fullmatch(value) for value in extra):
        raise ValueError("invalid connector ID in the manual selection")
    extra = tuple(value for value in extra if value != "mongodb")
    bundle = work / "bundle"
    PUBLISHED.stage(root / manifest["server"]["path"], "op", bundle, args.java, extra=extra)
    for platform in PLATFORMS:
        if platform != args.platform:
            shutil.rmtree(root / "bin" / platform)
    for directory in ("connectors", "release"):
        shutil.rmtree(root / directory)
    write_package(root, args.version, (args.platform,), bundle, args.output, source_digest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    for action in ("official", "manual", "verify"):
        command = actions.add_parser(action)
        command.add_argument("--version", required=True)
        if action in ("official", "manual"):
            command.add_argument("--java", default="java")
            command.add_argument("--output", type=Path, required=True)
        if action in ("manual", "verify"):
            command.add_argument("--archive", type=Path, required=True)
            command.add_argument("--checksum", type=Path, required=True)
        if action in ("official", "verify"):
            command.add_argument("--boot-sha256")
        if action == "manual":
            command.add_argument("--platform", choices=PLATFORMS, required=True)
            command.add_argument("--connectors", default="")
        if action == "official":
            command.add_argument("--assembly", type=Path, required=True)
            command.add_argument("--controllers", type=Path, required=True)
    args = parser.parse_args()
    try:
        if not VERSION.fullmatch(args.version):
            raise ValueError("invalid Tapstate release version")
        with tempfile.TemporaryDirectory(prefix="tapstate-op-") as temporary:
            work = Path(temporary)
            if args.action == "verify":
                release_checksum(args.archive, args.checksum)
                verified(extract(args.archive, work, ROOT), args.version, PLATFORMS, args.boot_sha256)
            elif args.action == "official":
                official(args, work)
            else:
                manual(args, work)
    except (ValueError, OSError, KeyError, TypeError, tarfile.TarError, PUBLISHED.STAGE.StageError) as failure:
        print(f"OP packaging refused: {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
