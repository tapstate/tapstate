#!/usr/bin/env bash
# Start the actual, profile-specific images without rebuilding or publishing them.
# Native-platform container/HTTP evidence is separate from browser and real-provider acceptance.
set -euo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PREFIX GIT_QUARANTINE_PATH

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
[[ $# = 2 ]] || { echo 'usage: cloud-image-smoke.sh CLOUD_IMAGE ONPREM_IMAGE' >&2; exit 2; }
docker info >/dev/null
for command in node python3 curl unzip openssl keytool; do command -v "$command" >/dev/null; done
# Resolve the two refs once: every later create/run uses this immutable local config ID.
CLOUD="$(docker image inspect "$1" --format '{{.Id}}')"
ONPREM="$(docker image inspect "$2" --format '{{.Id}}')"
[[ "$CLOUD" != "$ONPREM" ]]
WORK="$(mktemp -d "${TMPDIR:-/tmp}/tapstate-cloud-runtime.XXXXXX")"
chmod 700 "$WORK"
OWNER="$(node -e 'process.stdout.write(require("node:crypto").randomUUID())')"
NETWORK="tapstate-cloud-runtime-$OWNER"
CONTAINERS=()
VOLUMES=()
label=io.tapstate.runtime-smoke.owner
LABELS=(--label "$label=$OWNER")
if [[ -n "${TAPSTATE_ISSUE_LOOP_OWNER:-}" ]]; then
  LABELS+=(--label "io.tapstate.issue-loop.owner=$TAPSTATE_ISSUE_LOOP_OWNER")
fi
cleanup() {
  local resource
  for resource in "${CONTAINERS[@]}"; do
    if [[ "$(docker inspect -f "{{index .Config.Labels \"$label\"}}" "$resource" 2>/dev/null || true)" = "$OWNER" ]]; then
      docker logs "$resource" >"$WORK/$resource.log" 2>&1 || true
      docker rm -fv "$resource" >/dev/null 2>&1 || true
    fi
  done
  for resource in "${VOLUMES[@]}"; do
    if [[ "$(docker volume inspect -f "{{index .Labels \"$label\"}}" "$resource" 2>/dev/null || true)" = "$OWNER" ]]; then
      docker volume rm "$resource" >/dev/null 2>&1 || true
    fi
  done
  if [[ "$(docker network inspect -f "{{index .Labels \"$label\"}}" "$NETWORK" 2>/dev/null || true)" = "$OWNER" ]]; then
    docker network rm "$NETWORK" >/dev/null 2>&1 || true
  fi
  # Retain this one owned directory for failure attribution; it contains only local fixture data.
  echo "runtime smoke evidence: $WORK"
}
trap cleanup EXIT

new_container() {
  CURRENT="tapstate-runtime-$1-$OWNER"
  CONTAINERS+=("$CURRENT")
}
new_volume() {
  VOLUME="tapstate-runtime-$1-$OWNER"
  docker volume create "${LABELS[@]}" "$VOLUME" >/dev/null || return 1
  VOLUMES+=("$VOLUME")
}
logs() { docker logs "$1" >"$WORK/$2.log" 2>&1; }
ready() {
  local container="$1" deadline=$((SECONDS + 120)) started
  started="$(docker inspect -f '{{.State.StartedAt}}' "$container")"
  while (( SECONDS < deadline )); do
    if [[ "$(docker inspect -f '{{.State.Running}}' "$container")" != true ]]; then
      logs "$container" startup-failed
      echo 'FAIL: real runtime exited before application readiness' >&2
      return 1
    fi
    # The health endpoint alone precedes runner completion and is not the Ready gate.
    if docker logs --since "$started" "$container" 2>&1 | grep -F 'Tapstate application is ready' >/dev/null \
        && [[ "$(docker inspect -f '{{.State.Health.Status}}' "$container")" = healthy ]]; then
      return 0
    fi
    sleep 1
  done
  logs "$container" startup-timeout
  echo 'FAIL: actual image did not reach Ready and healthy in the startup bound' >&2
  return 1
}
url() { printf 'http://127.0.0.1:%s' "$(docker port "$1" "$2/tcp" | sed -n 's/^127\.0\.0\.1://p')"; }
BASE_ARGS=(--role=all --tapstate.hz.jet.cooperative-thread-count=2)
CLOUD_ENV=(-e TAPSTATE_CLOUD_BASE_URL=http://gateway:3000 \
  -e TAPSTATE_CLOUD_TOKEN=image-runtime-static-token-sentinel \
  -e TAPSTATE_CLOUD_CLUSTER_ID=image-runtime-cluster)

# Read the packaged Web metadata and lock, not a guessed asset name or fixture SPA.
for profile in cloud onprem; do
  image="$CLOUD"
  [[ "$profile" != onprem ]] || image="$ONPREM"
  new_container "export-$profile"
  docker create --name "$CURRENT" "${LABELS[@]}" "$image" >/dev/null
  docker cp "$CURRENT:/opt/tapstate/tapstate.jar" "$WORK/$profile.jar"
  docker image inspect "$image" >"$WORK/$profile-image.json"
done
PYTHONDONTWRITEBYTECODE=1 python3 "$ROOT/deploy/cloud/image-runtime-metadata.py" \
  --cloud-jar "$WORK/cloud.jar" --onprem-jar "$WORK/onprem.jar" \
  --cloud-image "$WORK/cloud-image.json" --onprem-image "$WORK/onprem-image.json" \
  --lock "$ROOT/deploy/cloud/connectors.lock.json" --output "$WORK"
VERSION="$(node -e 'process.stdout.write(JSON.parse(require("node:fs").readFileSync(process.argv[1])).version)' "$WORK/cloud.json")"

docker network create "${LABELS[@]}" "$NETWORK" >/dev/null
mkdir "$WORK/tls"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj '/CN=mongo' \
  -addext 'subjectAltName=DNS:mongo,DNS:localhost,IP:127.0.0.1' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -keyout "$WORK/tls/key.pem" -out "$WORK/tls/ca.pem" >"$WORK/tls-generation.log" 2>&1
cat "$WORK/tls/key.pem" "$WORK/tls/ca.pem" >"$WORK/tls/server.pem"
openssl rand -base64 756 >"$WORK/tls/keyfile"
# The real connector uses the JVM's normal trust when no client certificate is configured.
# Trust only the owned test CA; keep TLS and hostname validation enabled.
keytool -importcert -noprompt -storetype PKCS12 -alias runtime-mongo -file "$WORK/tls/ca.pem" \
  -keystore "$WORK/tls/truststore.p12" -storepass changeit >>"$WORK/tls-generation.log" 2>&1
new_volume mongo-data
MONGO_DATA="$VOLUME"
new_volume mongo-config
MONGO_CONFIG="$VOLUME"
new_container mongo
MONGO="$CURRENT"
docker run -d --name "$MONGO" "${LABELS[@]}" --network "$NETWORK" --network-alias mongo \
  --mount "type=volume,src=$MONGO_DATA,dst=/data/db" \
  --mount "type=volume,src=$MONGO_CONFIG,dst=/data/configdb" \
  --mount "type=bind,src=$WORK/tls,dst=/fixture,readonly" --entrypoint bash mongo:7.0 -ec '
    cp /fixture/keyfile /tmp/mongo-keyfile; chmod 600 /tmp/mongo-keyfile
    cp /fixture/server.pem /tmp/server.pem; cp /fixture/ca.pem /tmp/ca.pem
    chown 999:999 /tmp/mongo-keyfile /tmp/server.pem /tmp/ca.pem
    exec /usr/local/bin/docker-entrypoint.sh mongod --replSet rs0 --bind_ip_all \
      --keyFile /tmp/mongo-keyfile --tlsMode allowTLS --tlsCertificateKeyFile /tmp/server.pem \
      --tlsCAFile /tmp/ca.pem --tlsAllowConnectionsWithoutCertificates' >/dev/null
deadline=$((SECONDS + 60))
until docker exec "$MONGO" mongosh --quiet --eval 'db.runCommand({ping:1}).ok' 2>/dev/null | grep -q '^1$'; do
  (( SECONDS < deadline )) || { echo 'FAIL: owned Mongo did not start' >&2; exit 1; }
  sleep 1
done
docker exec "$MONGO" mongosh --quiet --eval \
  'rs.initiate({_id:"rs0",members:[{_id:0,host:"mongo:27017"}]})' >/dev/null
deadline=$((SECONDS + 60))
until docker exec "$MONGO" mongosh --quiet --eval 'db.hello().isWritablePrimary' 2>/dev/null | grep -q '^true$'; do
  (( SECONDS < deadline )) || { echo 'FAIL: owned Mongo did not elect a primary' >&2; exit 1; }
  sleep 1
done
docker exec "$MONGO" mongosh --quiet admin --eval '
  db.createUser({user:"fixture-root",pwd:"fixture-root-password",roles:["root"]})' >/dev/null
docker exec "$MONGO" mongosh --quiet admin -u fixture-root -p fixture-root-password --eval '
  db.createUser({user:"fixture",pwd:"fixture-mongo-password",roles:
    ["image_cloud","image_cache","image_master","image_smoke_configuration","image_smoke_cache","image_smoke_master"]
      .map(db=>({role:"readWrite",db}))})' >/dev/null
export TAPSTATE_RUNTIME_SMOKE_CANDIDATE="$WORK/cloud-candidate.json"
python3 - "$WORK/tls/ca.pem" "$TAPSTATE_RUNTIME_SMOKE_CANDIDATE" <<'PY_CANDIDATE'
import json, pathlib, sys
ca = pathlib.Path(sys.argv[1]).read_text()
candidate = {layer: {"kind": "ATLAS", "database": "image_" + layer,
    "uri": "mongodb://fixture:fixture-mongo-password@mongo:27017/image_" + layer + "?replicaSet=rs0&authSource=admin", "caPem": ca}
    for layer in ("cache", "master")}
pathlib.Path(sys.argv[2]).write_text(json.dumps(candidate))
PY_CANDIDATE
chmod 600 "$TAPSTATE_RUNTIME_SMOKE_CANDIDATE"
new_container gateway
GATEWAY="$CURRENT"
docker run -d --name "$GATEWAY" "${LABELS[@]}" --network "$NETWORK" --network-alias gateway \
  -p 127.0.0.1::3000 --mount "type=bind,src=$ROOT/deploy/cloud/image-runtime-probe.mjs,dst=/probe.mjs,readonly" \
  node:22-alpine node /probe.mjs serve "$VERSION" >/dev/null
FIXTURE_URL="$(url "$GATEWAY" 3000)"
deadline=$((SECONDS + 30))
until curl --noproxy '*' -fsS --max-time 2 "$FIXTURE_URL/healthz" >/dev/null 2>&1; do
  (( SECONDS < deadline )) || { echo 'FAIL: controlled SDK peer did not start' >&2; exit 1; }
  sleep 1
done

new_volume cloud
CLOUD_VOLUME="$VOLUME"
new_container cloud
SERVER="$CURRENT"
docker run -d --name "$SERVER" "${LABELS[@]}" --network "$NETWORK" \
  -e TZ=Etc/UTC \
  --mount "type=bind,src=$WORK/tls/truststore.p12,dst=/fixture-truststore.p12,readonly" \
  -e 'JAVA_TOOL_OPTIONS=-Xmx1g -XX:ActiveProcessorCount=2 -Djavax.net.ssl.trustStore=/fixture-truststore.p12 -Djavax.net.ssl.trustStorePassword=changeit' \
  -p 127.0.0.1::8080 --mount "type=volume,src=$CLOUD_VOLUME,dst=/var/lib/tapstate" \
  "${CLOUD_ENV[@]}" -e 'TAPSTATE_CLOUD_ATLAS_URI=mongodb://fixture:fixture-mongo-password@mongo:27017/image_cloud?replicaSet=rs0&authSource=admin' \
  -e 'TAPSTATE_STORE_MONGO_URI=mongodb://unreachable.invalid:27017/no_fallback' \
  "$CLOUD" "${BASE_ARGS[@]}" >/dev/null
ready "$SERVER"
[[ "$(docker exec "$SERVER" id -u)" = 0 ]]
docker exec "$SERVER" sh -ec 'test "$(cat /proc/1/comm)" = java; \
    if ! ldconfig -p | grep -F "libxml2.so.2"; then echo "FAIL: Cloud runtime lacks libxml2.so.2" >&2; exit 1; fi; \
    if [ "${LD_PRELOAD:-}" != libstdc++.so.6 ]; then echo "FAIL: Cloud runtime lacks the shared C++ preload" >&2; exit 1; fi; \
    if ! grep -F "libstdc++.so.6" /proc/1/maps; then echo "FAIL: Cloud Java process has not loaded shared libstdc++.so.6" >&2; exit 1; fi; \
    cd /opt/tapstate/connectors; sha256sum -c /opt/tapstate/release/connectors.sha256' \
  >"$WORK/seed-bytes.log"
SERVER_URL="$(url "$SERVER" 8080)"
FAILED=0
cloud_lifecycle() {
  ORIGINAL_PROCESS="$(docker inspect -f '{{.State.Pid}} {{.State.StartedAt}} {{.RestartCount}}' "$SERVER")" || return 1
  node "$ROOT/deploy/cloud/image-runtime-probe.mjs" verify cloud "$SERVER_URL" "$FIXTURE_URL" "$WORK/cloud.json" "$WORK/cloud-state.json" || return 1
  [[ "$(docker inspect -f '{{.State.Pid}} {{.State.StartedAt}} {{.RestartCount}}' "$SERVER")" = "$ORIGINAL_PROCESS" ]] || return 1
  docker stop --time 30 "$SERVER" >/dev/null || return 1
  [[ "$(docker inspect -f '{{.State.ExitCode}}' "$SERVER")" = 0 ]] || return 1
  logs "$SERVER" cloud-first || return 1
  grep -F 'Server time zone: UTC (default)' "$WORK/cloud-first.log" >/dev/null || return 1
  node "$ROOT/deploy/cloud/image-runtime-probe.mjs" verify cloud-stopped "$SERVER_URL" "$FIXTURE_URL" "$WORK/cloud.json" "$WORK/cloud-state.json" || return 1
  docker start "$SERVER" >/dev/null || return 1
  ready "$SERVER" || return 1
  SERVER_URL="$(url "$SERVER" 8080)" || return 1
  node "$ROOT/deploy/cloud/image-runtime-probe.mjs" verify restart "$SERVER_URL" "$FIXTURE_URL" "$WORK/cloud.json" "$WORK/cloud-state.json" || return 1
  node "$ROOT/deploy/cloud/image-runtime-probe.mjs" verify logout "$SERVER_URL" "$FIXTURE_URL" "$WORK/cloud.json" "$WORK/cloud-state.json" || return 1
  docker stop --time 30 "$SERVER" >/dev/null || return 1
  [[ "$(docker inspect -f '{{.State.ExitCode}}' "$SERVER")" = 0 ]] || return 1
  logs "$SERVER" cloud-restart || return 1
  curl --noproxy '*' -fsS --max-time 10 "$FIXTURE_URL/proof" >"$WORK/proof-after-cloud.json" || return 1
}
CLOUD_OK=1
if ! cloud_lifecycle; then
  FAILED=1; CLOUD_OK=0
  echo 'FAIL: Cloud lifecycle; dependent Cloud phases are blocked' >&2
  docker stop --time 30 "$SERVER" >/dev/null || true
fi
curl --noproxy '*' -fsS --max-time 10 "$FIXTURE_URL/proof" >"$WORK/proof-after-cloud.json"

onprem_lifecycle() {
  mkdir -p "$WORK/op-input/connectors" "$WORK/op-input/release" || return 1
  docker cp "$SERVER:/opt/tapstate/connectors/mongodb-connector.jar" "$WORK/op-input/connectors/" || return 1
  cp "$ROOT/deploy/cloud/connectors.lock.json" "$WORK/op-input/release/" || return 1
  new_volume onprem || return 1
  python3 "$ROOT/deploy/docker/native-install-smoke.py" prepare --image "$ONPREM" --volume "$VOLUME" --release "$WORK/op-input" || return 1
  new_container onprem || return 1
  OP_SERVER="$CURRENT" || return 1
  docker run -d --name "$OP_SERVER" "${LABELS[@]}" --network "$NETWORK" \
    -p 127.0.0.1::8080 --mount "type=volume,src=$VOLUME,dst=/var/lib/tapstate" \
    "$ONPREM" "${BASE_ARGS[@]}" --tapstate.installation.directory=/var/lib/tapstate/installation >/dev/null || return 1
  ready "$OP_SERVER" || return 1
  [[ "$(docker exec "$OP_SERVER" id -u)" = 10001 ]] || return 1
  docker exec "$OP_SERVER" sh -ec 'test ! -e /opt/tapstate/connectors; test -z "${TAPSTATE_CONNECTORS_SEED_DIR:-}"' || return 1
  python3 "$ROOT/deploy/docker/native-install-smoke.py" install --server "$OP_SERVER" \
    --mongo 'mongodb://fixture:fixture-mongo-password@mongo:27017/{database}?replicaSet=rs0&authSource=admin' \
    --username image-runtime-admin --password image-runtime-onprem-password-sentinel || return 1
  node "$ROOT/deploy/cloud/image-runtime-probe.mjs" verify onprem "$(url "$OP_SERVER" 8080)" "$FIXTURE_URL" "$WORK/onprem.json" "$WORK/onprem-state.json" || return 1
  docker stop --time 30 "$OP_SERVER" >/dev/null || return 1
  [[ "$(docker inspect -f '{{.State.ExitCode}}' "$OP_SERVER")" = 0 ]] || return 1
  docker start "$OP_SERVER" >/dev/null || return 1
  ready "$OP_SERVER" || return 1
  node "$ROOT/deploy/cloud/image-runtime-probe.mjs" verify onprem-restart "$(url "$OP_SERVER" 8080)" "$FIXTURE_URL" "$WORK/onprem.json" "$WORK/onprem-state.json" || return 1
  docker stop --time 30 "$OP_SERVER" >/dev/null || return 1
  logs "$OP_SERVER" onprem || return 1
}
if ! onprem_lifecycle; then
  FAILED=1
  echo 'FAIL: on-prem lifecycle; dependent on-prem phases are blocked' >&2
  if [[ -n "${OP_SERVER:-}" ]]; then docker stop --time 30 "$OP_SERVER" >/dev/null || true; fi
fi

curl --noproxy '*' -fsS --max-time 10 "$FIXTURE_URL/proof" >"$WORK/proof-before-rejections.json"
cmp "$WORK/proof-after-cloud.json" "$WORK/proof-before-rejections.json" || FAILED=1

refuses() {
  local scenario="$1" image="$2" code="$3"; shift 3
  new_container "$scenario"
  docker run -d --name "$CURRENT" "${LABELS[@]}" --network "$NETWORK" \
    "$@" "$image" "${BASE_ARGS[@]}" >/dev/null || return 1
  local deadline=$((SECONDS + 120))
  while [[ "$(docker inspect -f '{{.State.Running}}' "$CURRENT")" = true ]]; do
    (( SECONDS < deadline )) || { echo "FAIL: $scenario did not fail within the bound" >&2; return 1; }
    sleep 1
  done
  [[ "$(docker inspect -f '{{.State.ExitCode}}' "$CURRENT")" != 0 ]] || return 1
  logs "$CURRENT" "$scenario" || return 1
  grep -qF "$code" "$WORK/$scenario.log" || return 1
  if grep -qF 'Tapstate application is ready' "$WORK/$scenario.log"; then
    echo "FAIL: $scenario reached Ready" >&2
    return 1
  fi
  case "$scenario" in
    cloud-without-config|cloud-partial-config|onprem-with-cloud-config)
      if grep -E 'MongoClient with metadata|Members \{|Tomcat started on port' "$WORK/$scenario.log" >/dev/null; then
        echo "FAIL: $scenario opened runtime infrastructure before rejecting the configuration" >&2
        return 1
      fi ;;
  esac
  echo "PASS: real image refuses $scenario before Ready"
}
refuses cloud-without-config "$CLOUD" boot.web-profile-mode-mismatch || FAILED=1
refuses cloud-partial-config "$CLOUD" boot.cloud-config-incomplete "${CLOUD_ENV[@]}" || FAILED=1
refuses onprem-with-cloud-config "$ONPREM" boot.web-profile-mode-mismatch "${CLOUD_ENV[@]}" \
  -e 'TAPSTATE_CLOUD_ATLAS_URI=mongodb://mongo:27017/image_crossed?replicaSet=rs0' || FAILED=1
mkdir "$WORK/missing" "$WORK/damaged"
docker cp "$SERVER:/opt/tapstate/connectors/." "$WORK/damaged/"
# Mutate an owned copy, never the input image or the shared published artifacts.
printf 'changed-owned-fixture' >>"$WORK/damaged/mongodb-atlas-connector.jar"
# Seed validation belongs to retained business startup, after real storage initialization.
# The diagnostic root remains available; it must never publish the business HTTP face.
seed_refuses() {
  local scenario="$1" seed="$2"
  new_container "cloud-$scenario-seed"
  docker run -d --name "$CURRENT" "${LABELS[@]}" --network "$NETWORK" \
    -p 127.0.0.1::8080 --mount "type=volume,src=$CLOUD_VOLUME,dst=/var/lib/tapstate" \
    --mount "type=bind,src=$seed,dst=/opt/tapstate/connectors,readonly" \
    "${CLOUD_ENV[@]}" \
    -e 'JAVA_TOOL_OPTIONS=-Xmx1g -XX:ActiveProcessorCount=2' \
    -e 'TAPSTATE_CLOUD_ATLAS_URI=mongodb://fixture:fixture-mongo-password@mongo:27017/image_cloud?replicaSet=rs0&authSource=admin' \
    "$CLOUD" "${BASE_ARGS[@]}" >/dev/null || return 1
  ready "$CURRENT" || return 1
  logs "$CURRENT" "cloud-$scenario-seed" || return 1
  grep -qF 'boot.cloud-connectors-invalid' "$WORK/cloud-$scenario-seed.log" || return 1
  python3 - "$WORK/cloud-$scenario-seed.log" <<'PY_MEMBER' || return 1
import pathlib, sys
text = pathlib.Path(sys.argv[1]).read_text()
if 'Node session maintenance adopted' in text:
    assert text.rfind('is SHUTDOWN') > text.rfind('is STARTED')
PY_MEMBER
  node "$ROOT/deploy/cloud/image-runtime-probe.mjs" verify seed-refused "$(url "$CURRENT" 8080)" \
    "$FIXTURE_URL" "$WORK/cloud.json" "$WORK/cloud-state.json" || return 1
  docker stop --time 30 "$CURRENT" >/dev/null || return 1
  [[ "$(docker inspect -f '{{.State.ExitCode}}' "$CURRENT")" = 0 ]] || return 1
  echo "PASS: real image refuses cloud-$scenario-seed before business readiness"
}
if [[ "$CLOUD_OK" = 1 ]]; then
  for scenario in missing damaged; do
    if ! seed_refuses "$scenario" "$WORK/$scenario"; then
      FAILED=1
      echo "FAIL: cloud-$scenario-seed refusal" >&2
      docker stop --time 30 "$CURRENT" >/dev/null || true
    fi
  done
else
  echo 'FAIL: seed refusal checks blocked by incomplete Cloud initialization' >&2
  FAILED=1
fi
curl --noproxy '*' -fsS --max-time 10 "$FIXTURE_URL/proof" >"$WORK/proof-after-rejections.json"
cmp "$WORK/proof-before-rejections.json" "$WORK/proof-after-rejections.json" || FAILED=1
for sentinel in image-runtime-static-token-sentinel image-runtime-one-time-code-sentinel image-runtime-formal-code-sentinel image-runtime-onprem-password-sentinel fixture-mongo-password; do
  if grep -Fq "$sentinel" "$WORK/"*.log; then
    echo 'FAIL: a fixture credential escaped into runtime logs' >&2
    exit 1
  fi
done
[[ "$FAILED" = 0 ]] || exit 1
echo 'PASS: real Cloud/onprem images, locked registrations, restart/session/C2, shutdown and five fail-closed scenarios'
