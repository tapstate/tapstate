#!/usr/bin/env bash
# Build both image targets from isolated synthetic inputs and inspect a two-platform Cloud OCI archive.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEMP_ROOT="$(mktemp -d /private/tmp/cloud-image-smoke.XXXXXX)"
SERVER_TAG="tapstate:server-image-smoke-$$"
CLOUD_TAG="tapstate:cloud-image-smoke-$$"

cleanup() {
    docker image rm -f "$SERVER_TAG" "$CLOUD_TAG" >/dev/null 2>&1 || true
    case "$TEMP_ROOT" in
        /private/tmp/cloud-image-smoke.*) rm -rf -- "$TEMP_ROOT" ;;
        *) echo "refusing to remove unexpected smoke directory: $TEMP_ROOT" >&2 ;;
    esac
}
trap cleanup EXIT

mkdir -p "$TEMP_ROOT/context/app/target" "$TEMP_ROOT/jars"
cp "$REPO_ROOT/.dockerignore" "$TEMP_ROOT/context/.dockerignore"
printf 'synthetic-boot-jar' > "$TEMP_ROOT/context/app/target/app-smoke-boot.jar"

python3 - "$TEMP_ROOT" <<'PY'
import hashlib
import json
import sys
import zipfile
from pathlib import Path

root = Path(sys.argv[1])
ids = ("mysql", "mongodb", "postgres", "oracle", "sqlserver", "mongodb-atlas", "aws-rds-mysql")
spec_paths = {
    "mongodb": "spec.json",
    "postgres": "spec_postgres.json",
    "oracle": "spec_oracle.json",
    "sqlserver": "mssql-spec.json",
    "mongodb-atlas": "atlas-spec.json",
}
entries = []
for connector_id in ids:
    jar_path = root / "jars" / f"{connector_id}-connector.jar"
    spec_path = spec_paths.get(connector_id, f"{connector_id}-spec.json")
    title = "mssql-connector" if connector_id == "sqlserver" else f"{connector_id}-connector"
    manifest = (
        "Manifest-Version: 1.0\r\n"
        f"Implementation-Title: {title}\r\n"
        f"Git-Commit-Id: {'a' * 40}\r\n"
        "PDK-API-Version: 2.0.5-SNAPSHOT\r\n\r\n"
    )
    with zipfile.ZipFile(jar_path, "w", compression=zipfile.ZIP_STORED) as jar:
        jar.writestr("META-INF/MANIFEST.MF", manifest)
        jar.writestr(spec_path, json.dumps({"properties": {"id": connector_id}}))
        jar.writestr("classes/SyntheticDependency.class", b"x" * 1_000_000)
    content = jar_path.read_bytes()
    entries.append({
        "id": connector_id,
        "bytes": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "upstreamRevision": "a" * 40,
        "pdkApiVersion": "2.0.5-SNAPSHOT",
        "specPath": spec_path,
    })
(root / "connectors.lock.json").write_text(
    json.dumps({"schemaVersion": 1, "connectors": entries}), encoding="utf-8"
)
PY

PYTHONDONTWRITEBYTECODE=1 python3 "$REPO_ROOT/deploy/cloud/stage-connectors.py" \
    --lock "$TEMP_ROOT/connectors.lock.json" \
    --jar-dir "$TEMP_ROOT/jars" \
    --stage-dir "$TEMP_ROOT/staged"

if ! docker buildx build --progress=plain --load --target server \
    -f "$REPO_ROOT/deploy/docker/Dockerfile" -t "$SERVER_TAG" \
    "$TEMP_ROOT/context" >"$TEMP_ROOT/server-build.log" 2>&1; then
    tail -80 "$TEMP_ROOT/server-build.log" >&2
    exit 1
fi
docker run --rm --entrypoint sh "$SERVER_TAG" -ec \
    'test ! -e /opt/tapstate/connectors; test -z "${TAPSTATE_CONNECTORS_SEED_DIR:-}"'
test "$(docker image inspect "$SERVER_TAG" --format '{{ index .Config.Labels "org.opencontainers.image.licenses" }}')" = Apache-2.0

if ! docker buildx build --progress=plain --load --target cloud \
    --build-context "cloud_connectors=$TEMP_ROOT/staged" \
    -f "$REPO_ROOT/deploy/docker/Dockerfile" -t "$CLOUD_TAG" \
    "$TEMP_ROOT/context" >"$TEMP_ROOT/cloud-build.log" 2>&1; then
    tail -80 "$TEMP_ROOT/cloud-build.log" >&2
    exit 1
fi
docker run --rm --entrypoint sh "$CLOUD_TAG" -ec \
    'test "$TAPSTATE_CONNECTORS_SEED_DIR" = /opt/tapstate/connectors; \
     cd /opt/tapstate/connectors; \
     sha256sum -c /opt/tapstate/release/connectors.sha256; \
     test "$(find . -maxdepth 1 -type f -name "*-connector.jar" | wc -l | tr -d " ")" = 7'
test "$(docker image inspect "$CLOUD_TAG" --format '{{ index .Config.Labels "org.opencontainers.image.licenses" }}')" = NOASSERTION

if ! docker buildx build --progress=plain --target cloud \
    --platform linux/amd64,linux/arm64 \
    --build-context "cloud_connectors=$TEMP_ROOT/staged" \
    -f "$REPO_ROOT/deploy/docker/Dockerfile" \
    --output "type=oci,dest=$TEMP_ROOT/cloud-image.tar" \
    "$TEMP_ROOT/context" >"$TEMP_ROOT/cloud-multiarch-build.log" 2>&1; then
    tail -80 "$TEMP_ROOT/cloud-multiarch-build.log" >&2
    exit 1
fi
mkdir "$TEMP_ROOT/oci-layout"
tar -xf "$TEMP_ROOT/cloud-image.tar" -C "$TEMP_ROOT/oci-layout"
PYTHONDONTWRITEBYTECODE=1 python3 "$REPO_ROOT/deploy/cloud/verify-image.py" \
    --oci-layout "$TEMP_ROOT/oci-layout" --lock "$TEMP_ROOT/connectors.lock.json"

echo "PASS: default server target remains connector-free; Cloud target and both OCI platforms carry seven locked synthetic JARs"
