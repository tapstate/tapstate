#!/usr/bin/env python3
"""Bind real container probes to their packaged Web, SDK and connector inputs."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("runtime_web_provenance", ROOT / ".github/scripts/web-provenance.py")
assert SPEC is not None and SPEC.loader is not None
PROVENANCE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROVENANCE)
def sdk_entry(version: str) -> str:
    # Maven keeps the base snapshot name in the Boot JAR even for an exact timestamp request.
    if not re.fullmatch(r'[0-9]+\.[0-9]+(?:\.[0-9]+)?-[0-9a-f]{8}-(?:SNAPSHOT|[0-9]{8}\.[0-9]{6}-[1-9][0-9]*)', version):
        raise ValueError("invalid locked Cloud SDK coordinate")
    base = re.sub(r'-[0-9]{8}\.[0-9]{6}-[1-9][0-9]*$', '-SNAPSHOT', version)
    return f"BOOT-INF/lib/cloud-control-plane-sdk-{base}.jar"


SDK_VERSION = ET.parse(ROOT / "pom.xml").findtext(
    "{http://maven.apache.org/POM/4.0.0}properties/{http://maven.apache.org/POM/4.0.0}cloud.sdk.version")
SDK = sdk_entry(SDK_VERSION or "")
SDK_SHA = "e74bbb9d24dec2a91a8ba6fc23201b0e9cb67c1537e35da5581ecec3b723f255"


def metadata(jar_path: Path, image_path: Path, profile: str, lock: Path) -> dict:
    contents = jar_path.read_bytes()
    props = PROVENANCE.properties_from_jar(contents, profile)
    if props["web.profile"] != profile:
        raise ValueError("image probe profile does not match its packaged Web")
    inspected = json.loads(image_path.read_text())
    if len(inspected) != 1:
        raise ValueError("expected one immutable local image")
    config = inspected[0]["Config"]
    PROVENANCE.validate_image_labels(props, config["Labels"], profile)
    if config.get("Entrypoint") != ["java", "-jar", "/opt/tapstate/tapstate.jar"]:
        raise ValueError("runtime smoke must execute the actual JVM entrypoint")
    user = config.get("User")
    if profile == "cloud":
        if user not in ["root", "0", "root:root", "0:0"]:
            raise ValueError("Cloud runtime image must retain its root user")
    elif user != "tapstate":
        raise ValueError("on-prem runtime image must retain its unprivileged user")
    healthcheck = config.get("Healthcheck")
    command = healthcheck.get("Test") if isinstance(healthcheck, dict) else None
    if (not isinstance(command, list) or len(command) < 2
            or command[0] not in ["CMD", "CMD-SHELL"]
            or any(not isinstance(part, str) or not part.strip() for part in command[1:])):
        raise ValueError("runtime image must retain its enabled healthcheck")
    environment = dict(item.split("=", 1) for item in config.get("Env", []))
    if any(key in environment for key in ["TAPSTATE_CLOUD_BASE_URL", "TAPSTATE_CLOUD_TOKEN",
                                         "TAPSTATE_CLOUD_ATLAS_URI", "TAPSTATE_CLOUD_CLUSTER_ID", "CLUSTER_ID"]):
        raise ValueError("deployment configuration must not be baked into the image")
    if (environment.get("TAPSTATE_CONNECTORS_SEED_DIR") == "/opt/tapstate/connectors") != (profile == "cloud"):
        raise ValueError("the image connector seed configuration has the wrong profile")
    with zipfile.ZipFile(jar_path) as jar:
        if hashlib.sha256(jar.read(SDK)).hexdigest() != SDK_SHA:
            raise ValueError("runtime smoke requires the locked actual SDK bytes")
        if jar.read("META-INF/tapstate/connectors.lock.json") != lock.read_bytes():
            raise ValueError("runtime Boot JAR does not carry the selected connector lock")
        manifest = jar.read("META-INF/tapstate-web.files.sha256").decode()
    web_files = {name.removeprefix("./"): digest for digest, name in
                 (line.split("  ", 1) for line in manifest.splitlines())}
    return {"imageId": inspected[0]["Id"], "version": props["release.version"],
            "tapstateRevision": props["tapstate.revision"], "webRevision": props["revision"],
            "webProfile": profile, "webFilesSha256": props["files.sha256"],
            "bootJarSha256": props["image.boot-jar.sha256"],
            "cloudConsoleUrl": props.get("cloud.console.url"), "webFiles": web_files,
            "connectorIds": [row["id"] for row in json.loads(lock.read_text())["connectors"]]}


def paired(cloud: dict, onprem: dict) -> None:
    for key in ["version", "tapstateRevision", "webRevision"]:
        if cloud[key] != onprem[key]:
            raise ValueError("the runtime pair must use the same source revisions and version")
    for key in ["imageId", "bootJarSha256", "webFilesSha256"]:
        if cloud[key] == onprem[key]:
            raise ValueError("the runtime pair must not share compiled profile bytes")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["cloud-jar", "onprem-jar", "cloud-image", "onprem-image", "lock", "output"]:
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    cloud = metadata(args.cloud_jar, args.cloud_image, "cloud", args.lock)
    onprem = metadata(args.onprem_jar, args.onprem_image, "onprem", args.lock)
    paired(cloud, onprem)
    for profile, value in [("cloud", cloud), ("onprem", onprem)]:
        (args.output / f"{profile}.json").write_text(json.dumps(value, sort_keys=True) + "\n")
    print("PASS: real paired image IDs, Web profiles/digests, locked SDK and connector inputs")


if __name__ == "__main__":
    main()
