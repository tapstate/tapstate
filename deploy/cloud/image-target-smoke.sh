#!/usr/bin/env bash
# Build both image targets from isolated synthetic inputs and inspect a two-platform Cloud OCI archive.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEMP_ROOT="$(mktemp -d /tmp/cloud-image-smoke.XXXXXX)"
SERVER_TAG="tapstate:server-image-smoke-$$"
CLOUD_TAG="tapstate:cloud-image-smoke-$$"
SERVER_CONTEXT="$TEMP_ROOT/onprem-context"
CLOUD_CONTEXT="$TEMP_ROOT/cloud-context"
SMOKE_RELEASE_VERSION=0.0.0-smoke
SMOKE_TAPSTATE_REVISION=0123456789abcdef0123456789abcdef01234567
SMOKE_WEB_REVISION=abcdef0123456789abcdef0123456789abcdef01

cleanup() {
    docker image rm -f "$SERVER_TAG" "$CLOUD_TAG" >/dev/null 2>&1 || true
    case "$TEMP_ROOT" in
        /tmp/cloud-image-smoke.*|/private/tmp/cloud-image-smoke.*) rm -rf -- "$TEMP_ROOT" ;;
        *) echo "refusing to remove unexpected smoke directory: $TEMP_ROOT" >&2 ;;
    esac
}
trap cleanup EXIT

SMOKE_CLOUD_CONSOLE_URL="$(PYTHONDONTWRITEBYTECODE=1 python3 "$REPO_ROOT/scripts/web-assets-profile.py" \
    normalize-url --value HTTPS://Console.Example.Test:443)"
mkdir -p "$SERVER_CONTEXT/app/target" "$CLOUD_CONTEXT/app/target" "$TEMP_ROOT/jars"
cp "$REPO_ROOT/.dockerignore" "$SERVER_CONTEXT/.dockerignore"
cp "$REPO_ROOT/.dockerignore" "$CLOUD_CONTEXT/.dockerignore"

python3 - "$TEMP_ROOT" "$SMOKE_TAPSTATE_REVISION" "$SMOKE_WEB_REVISION" \
    "$SMOKE_RELEASE_VERSION" "$SMOKE_CLOUD_CONSOLE_URL" <<'PY'
import hashlib
import json
import sys
import zipfile
from pathlib import Path

root = Path(sys.argv[1])
tapstate_revision, web_revision, release_version, cloud_console_url = sys.argv[2:]
# These ZIPs exercise packaging metadata, not application execution or a real Web compiler.
fixtures = {}
for profile in ("onprem", "cloud"):
    files = {
        "index.html": f"<!doctype html><title>Tapstate {profile} image fixture</title>\n".encode(),
        "assets/app.js": f'window.__tapstateImageFixtureProfile = "{profile}";\n'.encode(),
    }
    web_manifest = "".join(
        f"{hashlib.sha256(content).hexdigest()}  ./{name}\n"
        for name, content in sorted(files.items())
    ).encode()
    web_files_sha256 = hashlib.sha256(web_manifest).hexdigest()
    properties = (
        "repository=tapstate/tapstate-web\n"
        f"revision={web_revision}\n"
        f"files.sha256={web_files_sha256}\n"
        f"tapstate.revision={tapstate_revision}\n"
        f"release.version={release_version}\n"
        f"web.profile={profile}\n"
    )
    if profile == "cloud":
        properties += f"cloud.console.url={cloud_console_url}\n"
    boot_jar = root / f"{profile}-context" / "app" / "target" / "app-smoke-boot.jar"
    with zipfile.ZipFile(boot_jar, "w", compression=zipfile.ZIP_STORED) as jar:
        jar.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\r\n\r\n")
        jar.writestr("META-INF/tapstate-web.properties", properties)
        jar.writestr("META-INF/tapstate-web.files.sha256", web_manifest)
        for name, content in files.items():
            jar.writestr(f"BOOT-INF/classes/static/{name}", content)
    fixtures[profile] = {
        "webFilesSha256": web_files_sha256,
        "bootJarSha256": hashlib.sha256(boot_jar.read_bytes()).hexdigest(),
    }
if fixtures["onprem"]["bootJarSha256"] == fixtures["cloud"]["bootJarSha256"]:
    raise AssertionError("the two image targets must not share a Boot JAR")
(root / "fixtures.json").write_text(json.dumps(fixtures), encoding="utf-8")
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
license_files = []
for name in ("MICROSOFT-MIT-LICENSE.txt", "ORACLE-FREE-USE-TERMS.txt"):
    content = f"synthetic terms for {name}\n".encode("utf-8")
    (root / "jars" / name).write_bytes(content)
    license_files.append({
        "name": name,
        "bytes": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
    })
(root / "connectors.lock.json").write_text(
    json.dumps({"schemaVersion": 2, "connectors": entries, "licenseFiles": license_files}),
    encoding="utf-8"
)
PY

ONPREM_FILES_SHA256="$(jq -er '.onprem.webFilesSha256' "$TEMP_ROOT/fixtures.json")"
CLOUD_FILES_SHA256="$(jq -er '.cloud.webFilesSha256' "$TEMP_ROOT/fixtures.json")"
CLOUD_BOOT_SHA256="$(jq -er '.cloud.bootJarSha256' "$TEMP_ROOT/fixtures.json")"
COMMON_BUILD_ARGS=(
    --build-arg "TAPSTATE_RELEASE_VERSION=$SMOKE_RELEASE_VERSION"
    --build-arg "TAPSTATE_REVISION=$SMOKE_TAPSTATE_REVISION"
    --build-arg "TAPSTATE_WEB_REVISION=$SMOKE_WEB_REVISION"
)
SERVER_BUILD_ARGS=("${COMMON_BUILD_ARGS[@]}"
    --build-arg "TAPSTATE_WEB_FILES_SHA256=$ONPREM_FILES_SHA256"
    --build-arg TAPSTATE_WEB_PROFILE=onprem
)
CLOUD_BUILD_ARGS=("${COMMON_BUILD_ARGS[@]}"
    --build-arg "TAPSTATE_WEB_FILES_SHA256=$CLOUD_FILES_SHA256"
    --build-arg TAPSTATE_WEB_PROFILE=cloud
    --build-arg "TAPSTATE_WEB_CLOUD_CONSOLE_URL=$SMOKE_CLOUD_CONSOLE_URL"
)

PYTHONDONTWRITEBYTECODE=1 python3 "$REPO_ROOT/deploy/cloud/stage-connectors.py" \
    --lock "$TEMP_ROOT/connectors.lock.json" \
    --jar-dir "$TEMP_ROOT/jars" \
    --stage-dir "$TEMP_ROOT/staged"

if ! docker buildx build --progress=plain --load --target server \
    "${SERVER_BUILD_ARGS[@]}" \
    -f "$REPO_ROOT/deploy/docker/Dockerfile" -t "$SERVER_TAG" \
    "$SERVER_CONTEXT" >"$TEMP_ROOT/server-build.log" 2>&1; then
    tail -80 "$TEMP_ROOT/server-build.log" >&2
    exit 1
fi
docker run --rm --entrypoint sh "$SERVER_TAG" -ec \
    'test ! -e /opt/tapstate/connectors; test -z "${TAPSTATE_CONNECTORS_SEED_DIR:-}"'
test "$(docker image inspect "$SERVER_TAG" --format '{{ index .Config.Labels "org.opencontainers.image.licenses" }}')" = Apache-2.0
test "$(docker image inspect "$SERVER_TAG" --format '{{ index .Config.Labels "org.opencontainers.image.source" }}')" = https://github.com/tapstate/tapstate
test "$(docker image inspect "$SERVER_TAG" --format '{{ index .Config.Labels "io.tapstate.distribution" }}')" = onprem
test "$(docker image inspect "$SERVER_TAG" --format '{{ index .Config.Labels "io.tapstate.web.profile" }}')" = onprem
test "$(docker image inspect "$SERVER_TAG" --format '{{ index .Config.Labels "io.tapstate.web.cloud-console-url" }}')" = ""

if ! docker buildx build --progress=plain --load --target cloud \
    "${CLOUD_BUILD_ARGS[@]}" \
    --build-context "cloud_connectors=$TEMP_ROOT/staged" \
    -f "$REPO_ROOT/deploy/docker/Dockerfile" -t "$CLOUD_TAG" \
    "$CLOUD_CONTEXT" >"$TEMP_ROOT/cloud-build.log" 2>&1; then
    tail -80 "$TEMP_ROOT/cloud-build.log" >&2
    exit 1
fi
docker run --rm --entrypoint sh "$CLOUD_TAG" -ec \
    'test "$TAPSTATE_CONNECTORS_SEED_DIR" = /opt/tapstate/connectors; \
     cd /opt/tapstate/connectors; \
     sha256sum -c /opt/tapstate/release/connectors.sha256; \
     test "$(find . -maxdepth 1 -type f -name "*-connector.jar" | wc -l | tr -d " ")" = 7'
test "$(docker image inspect "$CLOUD_TAG" --format '{{ index .Config.Labels "org.opencontainers.image.licenses" }}')" = NOASSERTION
test "$(docker image inspect "$CLOUD_TAG" --format '{{ index .Config.Labels "io.tapstate.web.profile" }}')" = cloud
test "$(docker image inspect "$CLOUD_TAG" --format '{{ index .Config.Labels "io.tapstate.web.cloud-console-url" }}')" = "$SMOKE_CLOUD_CONSOLE_URL"
docker image inspect "$CLOUD_TAG" --format '{{json .Config.Labels}}' \
    | jq -e '(."org.opencontainers.image.source" // "") == "" and ."io.tapstate.distribution" == "cloud"' >/dev/null

if ! docker buildx build --progress=plain --target cloud \
    "${CLOUD_BUILD_ARGS[@]}" \
    --platform linux/amd64,linux/arm64 \
    --build-context "cloud_connectors=$TEMP_ROOT/staged" \
    -f "$REPO_ROOT/deploy/docker/Dockerfile" \
    --output "type=oci,dest=$TEMP_ROOT/cloud-image.tar" \
    "$CLOUD_CONTEXT" >"$TEMP_ROOT/cloud-multiarch-build.log" 2>&1; then
    tail -80 "$TEMP_ROOT/cloud-multiarch-build.log" >&2
    exit 1
fi
mkdir "$TEMP_ROOT/oci-layout"
tar -xf "$TEMP_ROOT/cloud-image.tar" -C "$TEMP_ROOT/oci-layout"
PYTHONDONTWRITEBYTECODE=1 python3 "$REPO_ROOT/deploy/cloud/verify-image.py" \
    --oci-layout "$TEMP_ROOT/oci-layout" --lock "$TEMP_ROOT/connectors.lock.json" \
    --boot-jar "$CLOUD_CONTEXT/app/target/app-smoke-boot.jar" \
    --web-profile cloud --cloud-console-url "$SMOKE_CLOUD_CONSOLE_URL"
PYTHONDONTWRITEBYTECODE=1 python3 "$REPO_ROOT/.github/scripts/web-provenance.py" create \
    --oci-layout "$TEMP_ROOT/oci-layout" --output "$TEMP_ROOT/cloud-provenance.json" \
    --version "$SMOKE_RELEASE_VERSION" --tapstate-revision "$SMOKE_TAPSTATE_REVISION" \
    --web-revision "$SMOKE_WEB_REVISION" --web-profile cloud \
    --cloud-console-url "$SMOKE_CLOUD_CONSOLE_URL" --boot-jar-sha256 "$CLOUD_BOOT_SHA256"
PYTHONDONTWRITEBYTECODE=1 python3 "$REPO_ROOT/.github/scripts/web-provenance.py" verify \
    --oci-layout "$TEMP_ROOT/oci-layout" --provenance "$TEMP_ROOT/cloud-provenance.json" \
    --web-profile cloud --cloud-console-url "$SMOKE_CLOUD_CONSOLE_URL" \
    --boot-jar-sha256 "$CLOUD_BOOT_SHA256"

echo "PASS: independent onprem/Cloud Web JARs have matching profile provenance; default server target remains connector-free; both Cloud OCI platforms carry seven locked synthetic JARs and companion licenses"
