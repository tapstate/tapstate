#!/usr/bin/env python3
"""Drive the real local-owner OP installation contract in an isolated image smoke fixture."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid

DIRECTORY = "/var/lib/tapstate/installation"
JAR = "/opt/tapstate/tapstate.jar"


def command(*args, body=None, timeout=180, label="command"):
    result = subprocess.run(args, input=body, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)
    if result.returncode:
        # Request arguments and output may carry the fixture capability. Never echo either.
        raise RuntimeError(f"native installation smoke {label} failed (exit {result.returncode})")
    return result.stdout


def prepare(args):
    release = Path(args.release)
    lock = json.loads((release / "release/connectors.lock.json").read_text())
    expected = next(item for item in lock["connectors"] if item["id"] == "mongodb")
    seed = release / "connectors/mongodb-connector.jar"
    content = seed.read_bytes()
    if len(content) != expected["bytes"] or hashlib.sha256(content).hexdigest() != expected["sha256"]:
        raise RuntimeError("the Mongo connector seed does not match its retained release lock")
    mount = f"type=volume,src={args.volume},dst=/var/lib/tapstate"
    owned_container("--mount", mount, "-e", "JAVA_TOOL_OPTIONS=-Xmx768m -XX:ActiveProcessorCount=2", args.image,
            "installation", "--initialize", f"--tapstate.installation.directory={DIRECTORY}")
    owned_container("-i", "--mount", mount, "--entrypoint", "sh", args.image,
            "-ec", "cat > /var/lib/tapstate/connectors/mongodb-connector.jar", body=content)
    print("Prepared owner-only installation files and the verified Mongo test seed")


def owned_container(*args, body=None):
    owner = uuid.uuid4().hex
    name = "tapstate-native-install-" + owner
    label = "io.tapstate.native-install-smoke.owner"
    try:
        identity = command("docker", "create", "--name", name, "--label", label + "=" + owner, *args).decode().strip()
        if not re.fullmatch(r"[0-9a-f]{64}", identity):
            raise RuntimeError("Docker did not return an exact created container identity")
        flags = ("--attach", "--interactive") if body is not None else ("--attach",)
        command("docker", "start", *flags, identity, body=body)
    finally:
        # The preassigned name also finds a create whose successful response was lost.
        try:
            result = subprocess.run(["docker", "inspect", "-f",
                    '{{index .Config.Labels "' + label + '"}} {{.Id}}', name],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False)
        except (OSError, subprocess.TimeoutExpired):
            raise RuntimeError("could not verify smoke helper cleanup; inspect " + name) from None
        fields = result.stdout.decode().strip().split()
        if result.returncode == 0 and len(fields) == 2 and fields[0] == owner and re.fullmatch(r"[0-9a-f]{64}", fields[1]):
            command("docker", "rm", "--force", "--volumes", fields[1])
        elif result.returncode == 0:
            raise RuntimeError("refusing to remove an unowned smoke helper container")
        else:
            raise RuntimeError("could not verify smoke helper cleanup; inspect " + name)


def document(server, filename):
    return json.loads(command("docker", "exec", server, "cat", f"{DIRECTORY}/{filename}.json"))


def request(server, path, body=None, stage=None, capability=None, label=None):
    origin = stage["managementUrl"] if stage else "http://127.0.0.1:8080"
    if not origin.startswith("http://127.0.0.1:"):
        raise RuntimeError("the smoke must use the retained independent loopback management listener")
    args = ["docker", "exec", "-i", server, "curl", "--noproxy", "*", "--silent", "--show-error",
            "--max-time", "60", "--write-out", "\n%{http_code}", origin + path]
    if stage:
        args += ["-H", "Origin: " + origin, "-H", "X-Tapstate-Setup-Capability: " + capability["token"]]
    encoded = None
    if body is not None:
        args += ["-H", "Content-Type: application/json", "--data-binary", "@-"]
        encoded = json.dumps(body).encode()
    phase = label or path
    try:
        response = command(*args, body=encoded, timeout=70, label=phase)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"native installation smoke {phase} timed out") from None
    except OSError:
        raise RuntimeError(f"native installation smoke {phase} transport unavailable") from None
    content, separator, status = response.rpartition(b"\n")
    if not separator or not re.fullmatch(rb"[1-5][0-9]{2}", status):
        raise RuntimeError(f"native installation smoke {phase} returned no HTTP status")
    if not status.startswith(b"2"):
        # Only catalog codes may leave a failed response; never echo its message or parameters.
        code = "unavailable"
        try:
            value = json.loads(content)
            catalog = Path(__file__).resolve().parents[2] / "arch-tests/src/test/resources/error-codes.golden"
            known = {line.strip() for line in catalog.read_text().splitlines() if not line.startswith("#")}
            if isinstance(value, dict) and isinstance(value.get("code"), str) and value["code"] in known:
                code = value["code"]
        except (ValueError, OSError):
            pass
        raise RuntimeError(f"native installation smoke {phase} failed (HTTP {status.decode()}, code {code})")
    return json.loads(content)


def await_entry(server, expected):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            if request(server, "/auth/storage-entry").get("entry") == expected:
                return
        except (RuntimeError, json.JSONDecodeError):
            pass
        time.sleep(1)
    raise RuntimeError(f"the OP image did not reach {expected} within its startup bound")


def install(args):
    stage = document(args.server, "stage")
    capability = document(args.server, "capability")
    if capability["purpose"] != "SETUP" or any(stage[key] != capability[key]
            for key in ("clusterId", "installationId", "operationId", "generation")):
        raise RuntimeError("the original installation scope and SETUP capability do not match")
    candidate = {}
    for layer in ("configuration", "cache", "master"):
        database = "image_smoke_" + layer
        candidate[layer] = {"kind": "MONGODB", "uri": args.mongo.format(database=database), "database": database}
    original = {"operationId": capability["operationId"], "generation": capability["generation"], "candidate": candidate}
    stamps = {}
    for layer in ("CONFIG", "CACHE", "MASTER"):
        tested = request(args.server, "/api/storage/test", {**original, "layer": layer}, stage, capability,
                label="test-" + layer)
        stamps[layer] = tested["testStamp"]
    committed = request(args.server, "/api/storage/setup/commit", {**original, "testStamps": stamps}, stage, capability)
    if committed["setupPhase"] != "COMPLETE":
        raise RuntimeError("the original three-store setup did not complete")
    await_entry(args.server, "RESTART_REQUIRED")
    command("docker", "stop", "--time", "30", args.server)
    if command("docker", "inspect", "-f", "{{.State.ExitCode}}", args.server).strip() != b"0":
        raise RuntimeError("the setup process did not exit orderly before replacement")
    command("docker", "start", args.server)
    await_entry(args.server, "FIRST_ADMIN_REQUIRED")
    command("docker", "exec", args.server, "java", "-jar", JAR,
            "installation", "--authorize", f"--tapstate.installation.directory={DIRECTORY}")
    stage = document(args.server, "stage")
    capability = document(args.server, "capability")
    if capability["purpose"] != "FIRST_ADMIN":
        raise RuntimeError("the restarted installation did not issue a FIRST_ADMIN capability")
    created = request(args.server, "/installation/first-admin",
            {"username": args.username, "password": args.password}, stage, capability)
    if created["phase"] != "COMPLETE":
        raise RuntimeError("the first administrator was not created")
    await_entry(args.server, "LOGIN")
    print("Verified three-store setup, orderly restart, first administrator and normal login entry")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    setup = commands.add_parser("prepare")
    setup.add_argument("--image", required=True)
    setup.add_argument("--volume", required=True)
    setup.add_argument("--release", default=os.environ.get("TAPSTATE_TEST_CLOUD_RELEASE_DIR"))
    configured = commands.add_parser("install")
    configured.add_argument("--server", required=True)
    configured.add_argument("--mongo", required=True)
    configured.add_argument("--username", required=True)
    configured.add_argument("--password", required=True)
    args = parser.parse_args()
    if args.action == "prepare" and not args.release:
        parser.error("set TAPSTATE_TEST_CLOUD_RELEASE_DIR to verified public test inputs")
    try:
        (prepare if args.action == "prepare" else install)(args)
    except (RuntimeError, OSError, ValueError, KeyError, StopIteration, subprocess.TimeoutExpired) as failure:
        detail = str(failure) if isinstance(failure, RuntimeError) else type(failure).__name__
        print(f"Native installation smoke failed: {detail}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
