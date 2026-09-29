#!/usr/bin/env bash

set -euo pipefail

fail() {
    printf 'Cloud ECR publication refused: %s\n' "$1" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "required command is unavailable: $1"
}

auth_mode() {
    local role="${AWS_ROLE_ARN:-}"
    local access_key="${ECR_STATIC_AWS_ACCESS_KEY_ID:-}"
    local secret_key="${ECR_STATIC_AWS_SECRET_ACCESS_KEY:-}"
    local session_token="${ECR_STATIC_AWS_SESSION_TOKEN:-}"
    local mode

    if [[ -n "$role" && ( -n "$access_key" || -n "$secret_key" || -n "$session_token" ) ]]; then
        fail "OIDC role and static credentials are mutually exclusive"
    fi
    if [[ -n "$role" ]]; then
        [[ "$role" =~ ^arn:aws:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+$ ]] \
            || fail "AWS_ROLE_ARN is not a valid IAM role ARN"
        mode=oidc
    else
        [[ -n "$access_key" || -n "$secret_key" || -n "$session_token" ]] \
            || fail "configure either AWS_ROLE_ARN or a complete static credential pair"
        [[ -n "$access_key" && -n "$secret_key" ]] \
            || fail "static AWS credentials require both access key id and secret access key"
        mode=static
    fi

    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
        printf 'mode=%s\n' "$mode" >> "$GITHUB_OUTPUT"
    else
        printf 'mode=%s\n' "$mode"
    fi
}

publish() {
    local archive=""
    local tag=""
    local expected_digest=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --archive) archive="${2:-}"; shift 2 ;;
            --tag) tag="${2:-}"; shift 2 ;;
            --expected-digest) expected_digest="${2:-}"; shift 2 ;;
            *) fail "unknown publish argument: $1" ;;
        esac
    done

    local region="${AWS_REGION:-}"
    local account="${AWS_ACCOUNT_ID:-}"
    local repository="${ECR_REPOSITORY:-}"
    [[ "$region" =~ ^[a-z]{2}(-gov)?-[a-z]+-[0-9]+$ ]] || fail "AWS_REGION is missing or invalid"
    [[ "$account" =~ ^[0-9]{12}$ ]] || fail "AWS_ACCOUNT_ID must be a 12-digit account id"
    [[ "$repository" =~ ^[a-z0-9]+([._/-][a-z0-9]+)*$ ]] || fail "ECR_REPOSITORY is missing or invalid"
    [[ "$tag" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] || fail "image tag is missing or invalid"
    [[ "$tag" != latest ]] || fail "the floating latest tag is not allowed"
    [[ "$expected_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "expected OCI digest is missing or invalid"
    [[ -f "$archive" ]] || fail "verified OCI archive is missing"

    require_command aws
    require_command docker
    require_command jq
    require_command sha256sum
    require_command tar

    local scratch
    scratch="$(mktemp -d "${RUNNER_TEMP:-/tmp}/tapstate-ecr-publish.XXXXXX")"
    local registry="${account}.dkr.ecr.${region}.amazonaws.com"
    local repository_uri="${registry}/${repository}"
    local tagged_image="${repository_uri}:${tag}"
    local logged_in=false
    cleanup() {
        if [[ "$logged_in" == true ]]; then
            docker logout "$registry" >/dev/null 2>&1 || true
        fi
        rm -rf -- "$scratch"
    }
    trap cleanup EXIT

    mkdir "$scratch/layout"
    tar -xf "$archive" -C "$scratch/layout"
    local archive_digest
    archive_digest="$(jq -er '
        if (.manifests | type) == "array" and (.manifests | length) == 1
           and (.manifests[0].digest | type) == "string"
        then .manifests[0].digest else error("invalid OCI root index") end
    ' "$scratch/layout/index.json")" || fail "OCI archive has an invalid root index"
    [[ "$archive_digest" == "$expected_digest" ]] || fail "OCI archive digest differs from the verified digest"
    local manifest_blob="$scratch/layout/blobs/sha256/${expected_digest#sha256:}"
    [[ -f "$manifest_blob" ]] || fail "OCI archive is missing its verified manifest"
    local local_digest
    local_digest="sha256:$(sha256sum "$manifest_blob" | awk '{print $1}')"
    [[ "$local_digest" == "$expected_digest" ]] || fail "OCI manifest bytes differ from the verified digest"

    local caller_account
    caller_account="$(aws sts get-caller-identity --query Account --output text)" \
        || fail "AWS identity lookup failed"
    [[ "$caller_account" == "$account" ]] || fail "AWS identity belongs to a different account"

    local actual_repository_uri
    actual_repository_uri="$(aws ecr describe-repositories --region "$region" \
        --repository-names "$repository" --query 'repositories[0].repositoryUri' --output text)" \
        || fail "the configured ECR repository does not exist or is not readable"
    [[ "$actual_repository_uri" == "$repository_uri" ]] \
        || fail "ECR repository URI differs from the configured account, region, or repository"

    aws ecr get-login-password --region "$region" \
        | docker login --username AWS --password-stdin "$registry" >/dev/null \
        || fail "ECR login failed"
    logged_in=true

    docker buildx imagetools create --tag "$tagged_image" \
        "oci-layout://$scratch/layout@$expected_digest" >/dev/null \
        || fail "ECR image push failed"
    docker buildx imagetools inspect --raw "$tagged_image" > "$scratch/remote-manifest.json" \
        || fail "the pushed image could not be read back"
    local inspected_digest
    inspected_digest="sha256:$(sha256sum "$scratch/remote-manifest.json" | awk '{print $1}')"
    [[ "$inspected_digest" == "$expected_digest" ]] || fail "the registry manifest digest differs from the verified archive"

    local ecr_digest
    ecr_digest="$(aws ecr describe-images --region "$region" --repository-name "$repository" \
        --image-ids "imageTag=$tag" --query 'imageDetails[0].imageDigest' --output text)" \
        || fail "ECR did not return the pushed tag"
    [[ "$ecr_digest" == "$expected_digest" ]] || fail "ECR reports a different image digest"

    local deployable="${repository_uri}@${expected_digest}"
    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
        printf 'image=%s\n' "$deployable" >> "$GITHUB_OUTPUT"
        printf 'tag=%s\n' "$tagged_image" >> "$GITHUB_OUTPUT"
    fi
    printf 'Published verified Cloud image %s as %s\n' "$deployable" "$tagged_image"
    cleanup
    trap - EXIT
}

case "${1:-}" in
    auth-mode)
        shift
        [[ $# -eq 0 ]] || fail "auth-mode takes no arguments"
        auth_mode
        ;;
    publish)
        shift
        publish "$@"
        ;;
    *)
        fail "usage: ecr-publish.sh {auth-mode|publish ...}"
        ;;
esac
