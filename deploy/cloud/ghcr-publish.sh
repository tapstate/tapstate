#!/usr/bin/env bash
set -euo pipefail

fail() { printf 'Cloud GHCR publication refused: %s\n' "$1" >&2; exit 1; }
[[ "${1:-}" == publish ]] || fail 'usage: ghcr-publish.sh publish --archive FILE --version VERSION --expected-digest SHA256'
shift
archive='' version='' expected_digest=''
while [[ $# -gt 0 ]]; do
    [[ $# -ge 2 ]] || fail 'a publish argument is missing its value'
    case "$1" in
        --archive) archive="$2" ;;
        --version) version="$2" ;;
        --expected-digest) expected_digest="$2" ;;
        *) fail 'unknown publish argument' ;;
    esac
    shift 2
done
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail 'a numeric release version is required; floating tags are forbidden'
[[ "$expected_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || fail 'expected OCI digest is missing or invalid'
[[ -f "$archive" ]] || fail 'verified OCI archive is missing'
[[ -n "${CLOUD_GHCR_USERNAME:-}" && -n "${CLOUD_GHCR_TOKEN:-}" ]] || fail 'the dedicated GHCR username and token are required'
for tool in docker gh jq sha256sum tar; do
    command -v "$tool" >/dev/null 2>&1 || fail "required command is unavailable: $tool"
done

image=ghcr.io/tapstate/tapstate-cloud
endpoint=orgs/tapstate/packages/container/tapstate-cloud
scratch="$(mktemp -d "${RUNNER_TEMP:-/tmp}/tapstate-ghcr-publish.XXXXXX")"
cleanup() { rm -rf -- "$scratch"; }
trap cleanup EXIT
mkdir "$scratch/layout" "$scratch/docker"
# Do not overwrite or log out a developer's normal Docker credentials. Preserve only plugin
# discovery when the buildx binary is installed in the default Docker config directory.
plugin_root="${DOCKER_CONFIG:-${HOME}/.docker}/cli-plugins"
if [[ -d "$plugin_root" ]]; then ln -s "$plugin_root" "$scratch/docker/cli-plugins"; fi
export DOCKER_CONFIG="$scratch/docker"
export GH_TOKEN="$CLOUD_GHCR_TOKEN"

tar -xf "$archive" -C "$scratch/layout"
archive_digest="$(jq -er '
    if .schemaVersion == 2 and (.manifests | type) == "array" and (.manifests | length) == 1
    then .manifests[0].digest else error("invalid OCI root index") end
' "$scratch/layout/index.json")" || fail 'OCI archive has an invalid root index'
[[ "$archive_digest" == "$expected_digest" ]] || fail 'OCI archive differs from the verified digest'
blob="$scratch/layout/blobs/sha256/${expected_digest#sha256:}"
[[ -f "$blob" ]] || fail 'OCI archive is missing its verified manifest'
[[ "sha256:$(sha256sum "$blob" | awk '{print $1}')" == "$expected_digest" ]] \
    || fail 'OCI manifest bytes differ from the verified digest'

actor="$(gh api user --jq .login 2>"$scratch/api-error")" || fail 'GHCR account authentication failed'
[[ "$actor" == "$CLOUD_GHCR_USERNAME" ]] || fail 'the publishing account differs from the configured username'

package_exists=false
check_private() {
    if gh api "$endpoint" >"$scratch/package.json" 2>"$scratch/api-error"; then
        jq -e '.visibility == "private" and (.repository == null or .repository.private == true)' \
            "$scratch/package.json" >/dev/null \
            || fail 'the Cloud package must be private and not linked to a public repository'
        package_exists=true
    elif [[ "$1" == before ]] && jq -e '.status == 404' "$scratch/package.json" >/dev/null 2>&1; then
        # New GHCR packages start private. Cloud images have no source label that would
        # automatically associate the package with the public code repository.
        package_exists=false
    else
        fail 'Cloud package permissions or visibility could not be verified'
    fi
}
check_private before

if [[ "$package_exists" == true ]]; then
    existing="$(gh api --paginate "$endpoint/versions?per_page=100" \
        --jq ".[] | select((.metadata.container.tags // []) | index(\"$version\")) | .name" \
        2>"$scratch/api-error")" || fail 'existing Cloud release tags could not be read'
    [[ -z "$existing" || "$existing" == "$expected_digest" ]] \
        || fail 'the release version already names different bytes; overwriting is forbidden'
else
    existing=
fi

printf '%s' "$CLOUD_GHCR_TOKEN" \
    | docker login ghcr.io --username "$CLOUD_GHCR_USERNAME" --password-stdin \
        >"$scratch/login-out" 2>"$scratch/login-error" || fail 'GHCR login failed'
if [[ -z "$existing" ]]; then
    docker buildx imagetools create --tag "$image:$version" \
        "oci-layout://$scratch/layout@$expected_digest" \
        >"$scratch/push-out" 2>"$scratch/push-error" || fail 'GHCR image push failed'
fi
docker buildx imagetools inspect --raw "$image:$version" \
    >"$scratch/remote-manifest.json" 2>"$scratch/read-error" \
    || fail 'the published Cloud image could not be read back'
actual="sha256:$(sha256sum "$scratch/remote-manifest.json" | awk '{print $1}')"
[[ "$actual" == "$expected_digest" ]] || fail 'the registry manifest digest differs from the verified archive'
check_private after

deployable="$image@$expected_digest"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    printf 'image=%s\ntag=%s\n' "$deployable" "$image:$version" >> "$GITHUB_OUTPUT"
fi
printf 'Published private Cloud image %s as %s\n' "$deployable" "$image:$version"
