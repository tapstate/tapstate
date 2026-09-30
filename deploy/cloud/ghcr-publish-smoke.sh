#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PUBLISHER="$ROOT/deploy/cloud/ghcr-publish.sh"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/ghcr-publish-smoke.XXXXXX")"
trap 'rm -rf -- "$WORK"' EXIT
passed=0 failed=0
ok() { printf '  ok    %s\n' "$1"; passed=$((passed + 1)); }
bad() { printf '  FAIL  %s\n' "$1"; failed=$((failed + 1)); }
mkdir -p "$WORK/layout/blobs/sha256" "$WORK/bin"
printf '{"manifests":[],"schemaVersion":2}' > "$WORK/manifest.json"
DIGEST="sha256:$(sha256sum "$WORK/manifest.json" | awk '{print $1}')"
cp "$WORK/manifest.json" "$WORK/layout/blobs/sha256/${DIGEST#sha256:}"
printf '{"manifests":[{"digest":"%s"}],"schemaVersion":2}\n' "$DIGEST" > "$WORK/layout/index.json"
tar -cf "$WORK/cloud-image.tar" -C "$WORK/layout" .
TOKEN=private-publisher-test-sentinel
cat > "$WORK/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'gh %s\n' "$*" >> "$STUB_LOG"
if [[ "$2" == user ]]; then
    printf '%s\n' "${STUB_ACTOR:-publisher}"
elif [[ "$2" == --paginate ]]; then
    printf '%s' "${STUB_EXISTING_DIGEST:-}"
elif [[ "${STUB_MISSING:-false}" == true && ! -f "$STUB_PUSHED" ]]; then
    if [[ "${STUB_NOTFOUND_STRING:-false}" == true ]]; then
        printf '{"status":"404","message":"Package not found"}\n'
    else
        printf '{"status":404,"message":"Package not found"}\n'
    fi
    exit 1
elif [[ "${STUB_API_FAIL:-false}" == true ]]; then
    exit 1
else
    visibility="${STUB_VISIBILITY:-private}"
    if [[ -f "$STUB_PUSHED" ]]; then visibility="${STUB_VISIBILITY_AFTER:-$visibility}"; fi
    printf '{"visibility":"%s","repository":%s}\n' "$visibility" "${STUB_REPOSITORY:-null}"
fi
EOF
cat > "$WORK/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'docker %s\n' "$*" >> "$STUB_LOG"
case "$1 $2 $3" in
    'login ghcr.io --username')
        IFS= read -r token || true
        [[ "$token" == "$STUB_TOKEN" && "${STUB_LOGIN_FAIL:-false}" != true ]]
        ;;
    'buildx imagetools create')
        [[ "${STUB_PUSH_FAIL:-false}" != true ]]
        touch "$STUB_PUSHED"
        ;;
    'buildx imagetools inspect')
        [[ "${STUB_READ_FAIL:-false}" != true ]]
        if [[ "${STUB_DIGEST_MISMATCH:-false}" == true ]]; then printf other; else cat "$STUB_MANIFEST"; fi
        ;;
    *) exit 98 ;;
esac
EOF
chmod +x "$WORK/bin/gh" "$WORK/bin/docker"

run_publish() {
    : > "$WORK/commands.log"
    rm -f "$WORK/pushed"
    env PATH="$WORK/bin:$PATH" CLOUD_GHCR_USERNAME=publisher \
        CLOUD_GHCR_TOKEN="${TEST_TOKEN-$TOKEN}" GITHUB_OUTPUT= \
        STUB_TOKEN="$TOKEN" STUB_ACTOR="${STUB_ACTOR:-publisher}" \
        STUB_LOG="$WORK/commands.log" STUB_PUSHED="$WORK/pushed" STUB_MANIFEST="$WORK/manifest.json" \
        STUB_MISSING="${STUB_MISSING:-false}" STUB_API_FAIL="${STUB_API_FAIL:-false}" \
        STUB_NOTFOUND_STRING="${STUB_NOTFOUND_STRING:-false}" \
        STUB_VISIBILITY="${STUB_VISIBILITY:-private}" STUB_VISIBILITY_AFTER="${STUB_VISIBILITY_AFTER:-private}" \
        STUB_REPOSITORY="${STUB_REPOSITORY:-null}" STUB_EXISTING_DIGEST="${STUB_EXISTING_DIGEST:-}" \
        STUB_LOGIN_FAIL="${STUB_LOGIN_FAIL:-false}" STUB_PUSH_FAIL="${STUB_PUSH_FAIL:-false}" \
        STUB_READ_FAIL="${STUB_READ_FAIL:-false}" STUB_DIGEST_MISMATCH="${STUB_DIGEST_MISMATCH:-false}" \
        bash "$PUBLISHER" publish --archive "$WORK/cloud-image.tar" \
            --version "${TEST_VERSION:-0.6.0}" --expected-digest "${TEST_DIGEST:-$DIGEST}"
}

expect_failure() {
    local name="$1" text="$2"
    if run_publish > "$WORK/out" 2> "$WORK/err"; then bad "$name"
    elif grep -qF "$text" "$WORK/err"; then ok "$name"
    else bad "$name"; fi
}
if STUB_MISSING=true run_publish > "$WORK/out" 2> "$WORK/err" \
        && grep -qF "ghcr.io/tapstate/tapstate-cloud@$DIGEST" "$WORK/out" \
        && grep -qF 'ghcr.io/tapstate/tapstate-cloud:0.6.0' "$WORK/commands.log"; then
    ok 'a new private Cloud package publishes exactly the checked release digest'
else bad 'a new private Cloud package publishes exactly the checked release digest'; fi
if STUB_MISSING=true STUB_NOTFOUND_STRING=true run_publish > "$WORK/out" 2> "$WORK/err" \
        && grep -qF "ghcr.io/tapstate/tapstate-cloud@$DIGEST" "$WORK/out"; then
    ok 'the real API string-valued 404 allows private first publication'
else
    bad 'the real API string-valued 404 allows private first publication'
    grep -F 'could not be verified' "$WORK/err" || true
fi
TEST_TOKEN='' expect_failure 'missing dedicated credentials are refused' 'username and token'
STUB_ACTOR=other expect_failure 'publishing identity must match the configured account' 'differs'
STUB_VISIBILITY=public expect_failure 'a public package is refused before writing' 'must be private'
if [[ ! -f "$WORK/pushed" ]]; then ok 'public packages cause no registry write'; else bad 'public packages cause no registry write'; fi
STUB_REPOSITORY='{"private":false}' expect_failure 'public repository association is refused' 'not linked'
STUB_API_FAIL=true expect_failure 'unknown permissions fail closed' 'could not be verified'
STUB_VISIBILITY_AFTER=public expect_failure 'visibility is checked again after publication' 'must be private'
STUB_EXISTING_DIGEST=sha256:other expect_failure 'a release tag cannot overwrite different bytes' 'overwriting is forbidden'
if [[ ! -f "$WORK/pushed" ]]; then ok 'tag conflicts cause no registry write'; else bad 'tag conflicts cause no registry write'; fi
if STUB_EXISTING_DIGEST="$DIGEST" run_publish > "$WORK/out" 2> "$WORK/err" \
        && ! grep -qF 'imagetools create' "$WORK/commands.log"; then
    ok 'the same digest is idempotent and still checked remotely'
else bad 'the same digest is idempotent and still checked remotely'; fi
TEST_VERSION=latest expect_failure 'floating tags are refused' 'numeric release version'
TEST_VERSION=0.6.0-cloud expect_failure 'Cloud and OP use the same numeric version convention' 'numeric release version'
TEST_DIGEST="sha256:$(printf other | sha256sum | awk '{print $1}')" \
    expect_failure 'an archive digest mismatch is refused before authentication' 'archive differs'
if [[ ! -s "$WORK/commands.log" ]]; then ok 'wrong archive causes no external operation'; else bad 'wrong archive causes no external operation'; fi
STUB_LOGIN_FAIL=true expect_failure 'failed login refuses publication' 'login failed'
STUB_PUSH_FAIL=true expect_failure 'failed push is not reported as success' 'push failed'
STUB_READ_FAIL=true expect_failure 'failed readback is not reported as success' 'read back'
STUB_DIGEST_MISMATCH=true expect_failure 'a remote manifest mismatch is refused' 'manifest digest differs'
if ! grep -qF "$TOKEN" "$WORK/out" "$WORK/err" "$WORK/commands.log"; then
    ok 'diagnostics and commands never contain the token'
else bad 'diagnostics and commands never contain the token'; fi
if [[ ! -e "$ROOT/.github/workflows/cloud-ecr.yml" ]]; then
    ok 'the independent upload workflow is removed'
else bad 'the independent upload workflow is removed'; fi
printf '\n%s passed, %s failed\n' "$passed" "$failed"
[[ "$failed" -eq 0 ]]
