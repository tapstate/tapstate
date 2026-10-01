#!/usr/bin/env bash
set -euo pipefail
umask 077

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
for tool in docker gh jq curl sha256sum tar; do
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

check_public() {
    if gh api "$endpoint" >"$scratch/package.json" 2>"$scratch/api-error"; then
        jq -e '.visibility == "public"' \
            "$scratch/package.json" >/dev/null \
            || fail 'the Cloud package must be Public'
    elif [[ "$1" == before ]] && jq -e '(.status | tostring) == "404"' "$scratch/package.json" >/dev/null 2>&1; then
        # Initial package visibility must be configured separately. Never write an image
        # here when its intended anonymous distribution cannot be verified beforehand.
        fail 'the Cloud package must already be Public; first publication requires visibility setup'
    else
        fail 'Cloud package permissions or visibility could not be verified'
    fi
}
check_public before

existing="$(gh api --paginate "$endpoint/versions?per_page=100" \
    --jq ".[] | select((.metadata.container.tags // []) | index(\"$version\")) | .name" \
    2>"$scratch/api-error")" || fail 'existing Cloud release tags could not be read'
[[ -z "$existing" || "$existing" == "$expected_digest" ]] \
    || fail 'the release version already names different bytes; overwriting is forbidden'

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
check_public after

# Bypass user curl configuration and obtain a pull-only token without the publishing
# identity. Keep that token out of command arguments and all diagnostic output.
curl --disable --silent --show-error --fail --connect-timeout 10 --max-time 30 \
    --proto '=https' --output "$scratch/anonymous-token.json" \
    'https://ghcr.io/token?service=ghcr.io&scope=repository:tapstate/tapstate-cloud:pull' \
    2>"$scratch/anonymous-error" || fail 'anonymous pull authorization is unavailable'
anonymous_token="$(jq -er '
    .token | select(type == "string" and length > 0 and length <= 16384)
    | select(test("^[A-Za-z0-9._~+/-]+=*$"))
' "$scratch/anonymous-token.json" 2>"$scratch/anonymous-error")" \
    || fail 'anonymous pull authorization is invalid'
printf 'Authorization: Bearer %s\n' "$anonymous_token" > "$scratch/anonymous-header"
unset anonymous_token
curl --disable --silent --show-error --fail --connect-timeout 10 --max-time 30 \
    --proto '=https' --header "@$scratch/anonymous-header" \
    --header 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
    --output "$scratch/anonymous-manifest.json" \
    "https://ghcr.io/v2/tapstate/tapstate-cloud/manifests/$expected_digest" \
    2>"$scratch/anonymous-error" || fail 'the published Cloud image could not be read anonymously'
anonymous_digest="sha256:$(sha256sum "$scratch/anonymous-manifest.json" | awk '{print $1}')"
[[ "$anonymous_digest" == "$expected_digest" ]] \
    || fail 'the anonymous manifest digest differs from the verified archive'

deployable="$image@$expected_digest"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    printf 'image=%s\ntag=%s\n' "$deployable" "$image:$version" >> "$GITHUB_OUTPUT"
fi
printf 'Published Public Cloud image %s as %s\n' "$deployable" "$image:$version"
