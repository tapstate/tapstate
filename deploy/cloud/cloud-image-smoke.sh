#!/usr/bin/env bash
# Start the actual, profile-specific images without rebuilding or publishing them.
# Native-platform container/HTTP evidence is separate from browser and real-provider acceptance.
set -euo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PREFIX GIT_QUARANTINE_PATH

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
[[ $# = 2 ]] || { echo 'usage: cloud-image-smoke.sh CLOUD_IMAGE ONPREM_IMAGE' >&2; exit 2; }
docker info >/dev/null
for command in node python3 curl unzip; do command -v "$command" >/dev/null; done
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
  docker volume create --label "$label=$OWNER" "$VOLUME" >/dev/null
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
  docker create --name "$CURRENT" --label "$label=$OWNER" "$image" >/dev/null
  docker cp "$CURRENT:/opt/tapstate/tapstate.jar" "$WORK/$profile.jar"
  docker image inspect "$image" >"$WORK/$profile-image.json"
done
PYTHONDONTWRITEBYTECODE=1 python3 "$ROOT/deploy/cloud/image-runtime-metadata.py" \
  --cloud-jar "$WORK/cloud.jar" --onprem-jar "$WORK/onprem.jar" \
  --cloud-image "$WORK/cloud-image.json" --onprem-image "$WORK/onprem-image.json" \
  --lock "$ROOT/deploy/cloud/connectors.lock.json" --output "$WORK"
VERSION="$(node -e 'process.stdout.write(JSON.parse(require("node:fs").readFileSync(process.argv[1])).version)' "$WORK/cloud.json")"

docker network create --label "$label=$OWNER" "$NETWORK" >/dev/null
new_container mongo
MONGO="$CURRENT"
docker run -d --name "$MONGO" --label "$label=$OWNER" --network "$NETWORK" --network-alias mongo \
  mongo:7.0 --replSet rs0 --bind_ip_all >/dev/null
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
new_container gateway
GATEWAY="$CURRENT"
docker run -d --name "$GATEWAY" --label "$label=$OWNER" --network "$NETWORK" --network-alias gateway \
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
docker run -d --name "$SERVER" --label "$label=$OWNER" --network "$NETWORK" \
  -p 127.0.0.1::8080 --mount "type=volume,src=$CLOUD_VOLUME,dst=/var/lib/tapstate" \
  "${CLOUD_ENV[@]}" -e 'TAPSTATE_CLOUD_ATLAS_URI=mongodb://mongo:27017/image_cloud?replicaSet=rs0' \
  -e 'TAPSTATE_STORE_MONGO_URI=mongodb://unreachable.invalid:27017/no_fallback' \
  "$CLOUD" "${BASE_ARGS[@]}" >/dev/null
ready "$SERVER"
[[ "$(docker exec "$SERVER" id -u)" = 10001 ]]
docker exec "$SERVER" sh -ec 'test "$(cat /proc/1/comm)" = java; cd /opt/tapstate/connectors; sha256sum -c /opt/tapstate/release/connectors.sha256' \
  >"$WORK/seed-bytes.log"
SERVER_URL="$(url "$SERVER" 8080)"
node "$ROOT/deploy/cloud/image-runtime-probe.mjs" verify cloud "$SERVER_URL" "$FIXTURE_URL" "$WORK/cloud.json" "$WORK/cloud-state.json"
docker stop --time 30 "$SERVER" >/dev/null
[[ "$(docker inspect -f '{{.State.ExitCode}}' "$SERVER")" = 0 ]]
logs "$SERVER" cloud-first
node "$ROOT/deploy/cloud/image-runtime-probe.mjs" verify cloud-stopped "$SERVER_URL" "$FIXTURE_URL" "$WORK/cloud.json" "$WORK/cloud-state.json"
docker start "$SERVER" >/dev/null
ready "$SERVER"
SERVER_URL="$(url "$SERVER" 8080)"
node "$ROOT/deploy/cloud/image-runtime-probe.mjs" verify restart "$SERVER_URL" "$FIXTURE_URL" "$WORK/cloud.json" "$WORK/cloud-state.json"
node "$ROOT/deploy/cloud/image-runtime-probe.mjs" verify logout "$SERVER_URL" "$FIXTURE_URL" "$WORK/cloud.json" "$WORK/cloud-state.json"
docker stop --time 30 "$SERVER" >/dev/null
[[ "$(docker inspect -f '{{.State.ExitCode}}' "$SERVER")" = 0 ]]
logs "$SERVER" cloud-restart
curl --noproxy '*' -fsS --max-time 10 "$FIXTURE_URL/proof" >"$WORK/proof-after-cloud.json"

new_volume onprem
new_container onprem
OP_SERVER="$CURRENT"
docker run -d --name "$OP_SERVER" --label "$label=$OWNER" --network "$NETWORK" \
  -p 127.0.0.1::8080 --mount "type=volume,src=$VOLUME,dst=/var/lib/tapstate" \
  -e 'TAPSTATE_STORE_MONGO_URI=mongodb://mongo:27017/image_onprem?replicaSet=rs0' \
  "$ONPREM" "${BASE_ARGS[@]}" >/dev/null
ready "$OP_SERVER"
[[ "$(docker exec "$OP_SERVER" id -u)" = 10001 ]]
docker exec "$OP_SERVER" sh -ec 'test ! -e /opt/tapstate/connectors; test -z "${TAPSTATE_CONNECTORS_SEED_DIR:-}"'
[[ "$(docker exec "$OP_SERVER" curl -sS --max-time 10 -o /dev/null -w '%{http_code}' \
  -X POST http://127.0.0.1:8080/auth/bootstrap -H 'Content-Type: application/json' \
  -d '{"username":"image-runtime-admin","password":"image-runtime-onprem-password-sentinel"}')" = 204 ]]
node "$ROOT/deploy/cloud/image-runtime-probe.mjs" verify onprem "$(url "$OP_SERVER" 8080)" "$FIXTURE_URL" "$WORK/onprem.json" "$WORK/onprem-state.json"
docker stop --time 30 "$OP_SERVER" >/dev/null
[[ "$(docker inspect -f '{{.State.ExitCode}}' "$OP_SERVER")" = 0 ]]
docker start "$OP_SERVER" >/dev/null
ready "$OP_SERVER"
node "$ROOT/deploy/cloud/image-runtime-probe.mjs" verify onprem-restart "$(url "$OP_SERVER" 8080)" "$FIXTURE_URL" "$WORK/onprem.json" "$WORK/onprem-state.json"
docker stop --time 30 "$OP_SERVER" >/dev/null
logs "$OP_SERVER" onprem
curl --noproxy '*' -fsS --max-time 10 "$FIXTURE_URL/proof" >"$WORK/proof-before-rejections.json"
cmp "$WORK/proof-after-cloud.json" "$WORK/proof-before-rejections.json"

refuses() {
  local scenario="$1" image="$2" code="$3"; shift 3
  new_container "$scenario"
  docker run -d --name "$CURRENT" --label "$label=$OWNER" --network "$NETWORK" \
    "$@" "$image" "${BASE_ARGS[@]}" >/dev/null
  local deadline=$((SECONDS + 120))
  while [[ "$(docker inspect -f '{{.State.Running}}' "$CURRENT")" = true ]]; do
    (( SECONDS < deadline )) || { echo "FAIL: $scenario did not fail within the bound" >&2; return 1; }
    sleep 1
  done
  [[ "$(docker inspect -f '{{.State.ExitCode}}' "$CURRENT")" != 0 ]]
  logs "$CURRENT" "$scenario"
  grep -qF "$code" "$WORK/$scenario.log"
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
refuses cloud-without-config "$CLOUD" boot.web-profile-mode-mismatch
refuses cloud-partial-config "$CLOUD" boot.cloud-config-incomplete "${CLOUD_ENV[@]}"
refuses onprem-with-cloud-config "$ONPREM" boot.web-profile-mode-mismatch "${CLOUD_ENV[@]}" \
  -e 'TAPSTATE_CLOUD_ATLAS_URI=mongodb://mongo:27017/image_crossed?replicaSet=rs0'
mkdir "$WORK/missing" "$WORK/damaged"
docker cp "$SERVER:/opt/tapstate/connectors/." "$WORK/damaged/"
# Mutate an owned copy, never the input image or the shared published artifacts.
printf 'changed-owned-fixture' >>"$WORK/damaged/mongodb-atlas-connector.jar"
for scenario in missing damaged; do
  refuses "cloud-$scenario-seed" "$CLOUD" boot.cloud-connectors-invalid "${CLOUD_ENV[@]}" \
    -e "TAPSTATE_CLOUD_ATLAS_URI=mongodb://mongo:27017/image_$scenario?replicaSet=rs0" \
    --mount "type=bind,src=$WORK/$scenario,dst=/opt/tapstate/connectors,readonly"
done
curl --noproxy '*' -fsS --max-time 10 "$FIXTURE_URL/proof" >"$WORK/proof-after-rejections.json"
cmp "$WORK/proof-before-rejections.json" "$WORK/proof-after-rejections.json"
for sentinel in image-runtime-static-token-sentinel image-runtime-one-time-code-sentinel image-runtime-onprem-password-sentinel; do
  if grep -Fq "$sentinel" "$WORK/"*.log; then
    echo 'FAIL: a fixture credential escaped into runtime logs' >&2
    exit 1
  fi
done
echo 'PASS: real Cloud/onprem images, eight locked registrations, restart/session/C2, shutdown and five fail-closed startup scenarios'
