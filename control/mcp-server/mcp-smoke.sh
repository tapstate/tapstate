#!/usr/bin/env bash
# Black-box stdio smoke for the native sidecar or the runnable Boot jar.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ARTIFACT="${1:-}"

if [[ -z "$ARTIFACT" ]]; then
  if [[ -x "$REPO_ROOT/control/mcp-server/target/tapstate-mcp" ]]; then
    ARTIFACT="$REPO_ROOT/control/mcp-server/target/tapstate-mcp"
  else
    ARTIFACT="$(ls -t "$REPO_ROOT"/control/mcp-server/target/mcp-server-*-boot.jar 2>/dev/null | head -1 || true)"
  fi
fi

if [[ -z "$ARTIFACT" || ! -e "$ARTIFACT" ]]; then
  echo "MCP artifact not found; package control/mcp-server first" >&2
  exit 2
fi

if [[ "$ARTIFACT" == *.jar ]]; then
  COMMAND=(java -jar "$ARTIFACT")
else
  COMMAND=("$ARTIFACT")
fi

TAPSTATE_MCP_SMOKE_COMMAND="$(printf '%q ' "${COMMAND[@]}")" python3 - <<'PY'
import json
import os
import select
import shlex
import subprocess

command = shlex.split(os.environ["TAPSTATE_MCP_SMOKE_COMMAND"])
environment = dict(os.environ)
environment["TAPSTATE_TOKEN"] = "smoke-token"
environment["TAPSTATE_SERVER_URL"] = "http://127.0.0.1:1"
process = subprocess.Popen(
    command,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    env=environment,
)

def send(message):
    process.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
    process.stdin.flush()

def receive(timeout=5):
    readable, _, _ = select.select([process.stdout], [], [], timeout)
    if not readable:
        process.kill()
        _, stderr = process.communicate()
        raise RuntimeError(f"MCP process did not respond within {timeout}s; stderr: {stderr!r}")
    line = process.stdout.readline()
    if not line:
        raise RuntimeError("MCP process closed stdout before responding")
    try:
        return json.loads(line)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"non-protocol stdout frame: {line!r}") from error

try:
    send({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "tapstate-smoke", "version": "1"},
        },
    })
    assert receive()["result"]["protocolVersion"] == "2025-06-18"
    send({"jsonrpc": "2.0", "method": "notifications/initialized"})
    send({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    tools = receive()["result"]["tools"]
    tool_names = {tool["name"] for tool in tools}
    # The read-only face, named tool by tool rather than counted. A count moves silently when a verb
    # is promoted onto this face: artifact.get became visible here and the expected number did not
    # follow, so this smoke asked for one tool fewer than the server offered -- and since no lane but
    # the release run executes it, the whole release lane was what reported the mismatch. A set names
    # the tool that appeared or vanished, which is the part a number cannot say.
    #
    # It happened again, the same way, with the three data-browser tools: they were added on 2026-08-20
    # and this list was not, and nothing said so until a release was cut five days later. Naming the
    # tool rather than counting it made the failure legible in one line -- but a legible failure that
    # only a release can produce still costs a release to find. It happened a third time, with
    # system_version, and cost a third release run before anything said so. The list below was never
    # the fix for that; being executed before a release is, so the build job on every pull request now
    # runs this smoke as well.
    expected = {
        "artifact_get",
        "artifact_validate",
        "connection_schema",
        "data_browser_collections",
        "data_browser_find",
        "data_browser_stats",
        "connection_test_result",
        "connector_get",
        "connector_list",
        "pipeline_logs",
        "pipeline_metrics",
        "pipeline_snapshot",
        "pipeline_status",
        "source_draft",
        "system_version",
    }
    assert tool_names == expected, (
        f"unexpected tools: {sorted(tool_names - expected)}; "
        f"missing tools: {sorted(expected - tool_names)}"
    )
    # Write verbs stay off a face that was not granted them, and the retired Source CRUD stays retired.
    assert tool_names.isdisjoint({"source_create", "source_list", "source_get", "source_update", "source_delete"})

    process.stdin.close()
    process.wait(timeout=5)
    assert process.returncode == 0
    stderr = process.stderr.read()
    assert "smoke-token" not in stderr
    print(f"mcp smoke: initialize, {len(expected)} read tools, clean EOF, no credential leak")
finally:
    if process.poll() is None:
        process.kill()
        process.wait(timeout=5)
PY
