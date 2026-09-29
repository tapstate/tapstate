#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PUBLISHER="$ROOT/deploy/cloud/ecr-publish.sh"
TEMP="$(mktemp -d /private/tmp/ecr-publish-smoke.XXXXXX)"
trap 'rm -rf -- "$TEMP"' EXIT

passed=0
failed=0
ok() { printf '  ok    %s\n' "$1"; passed=$((passed + 1)); }
bad() { printf '  FAIL  %s\n        %s\n' "$1" "$2"; failed=$((failed + 1)); }

expect_auth_failure() {
    local name="$1"
    local expected="$2"
    shift 2
    if env -i PATH="$PATH" "$@" bash "$PUBLISHER" auth-mode >"$TEMP/out" 2>"$TEMP/err"; then
        bad "$name" "unexpected success"
    elif grep -qF "$expected" "$TEMP/err"; then
        ok "$name"
    else
        bad "$name" "missing diagnostic: $expected"
    fi
}

expect_auth_failure "auth is required" "configure either" \
    AWS_ROLE_ARN= ECR_STATIC_AWS_ACCESS_KEY_ID= ECR_STATIC_AWS_SECRET_ACCESS_KEY=
expect_auth_failure "static credentials are a complete pair" "require both" \
    ECR_STATIC_AWS_ACCESS_KEY_ID=only-id
expect_auth_failure "OIDC and static credentials cannot coexist" "mutually exclusive" \
    AWS_ROLE_ARN=arn:aws:iam::123456789012:role/tapstate-ecr \
    ECR_STATIC_AWS_ACCESS_KEY_ID=id ECR_STATIC_AWS_SECRET_ACCESS_KEY=secret

if env -i PATH="$PATH" AWS_ROLE_ARN=arn:aws:iam::123456789012:role/tapstate-ecr \
        bash "$PUBLISHER" auth-mode | grep -qx 'mode=oidc'; then
    ok "a valid role selects OIDC"
else
    bad "a valid role selects OIDC" "mode output differs"
fi
if env -i PATH="$PATH" ECR_STATIC_AWS_ACCESS_KEY_ID=id ECR_STATIC_AWS_SECRET_ACCESS_KEY=secret \
        bash "$PUBLISHER" auth-mode | grep -qx 'mode=static'; then
    ok "a complete key pair selects static auth"
else
    bad "a complete key pair selects static auth" "mode output differs"
fi

mkdir -p "$TEMP/layout/blobs/sha256" "$TEMP/bin"
printf '{"manifests":[],"mediaType":"application/vnd.oci.image.index.v1+json","schemaVersion":2}' \
    > "$TEMP/manifest.json"
DIGEST="sha256:$(sha256sum "$TEMP/manifest.json" | awk '{print $1}')"
cp "$TEMP/manifest.json" "$TEMP/layout/blobs/sha256/${DIGEST#sha256:}"
printf '{"manifests":[{"digest":"%s"}],"schemaVersion":2}\n' "$DIGEST" > "$TEMP/layout/index.json"
tar -cf "$TEMP/cloud-image.tar" -C "$TEMP/layout" .

cat > "$TEMP/bin/aws" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'aws %s\n' "$*" >> "$STUB_LOG"
case "$1 $2" in
    'sts get-caller-identity') printf '%s\n' "$STUB_ACCOUNT" ;;
    'ecr describe-repositories')
        [[ "${STUB_REPOSITORY_MISSING:-false}" != true ]]
        printf '%s\n' "$STUB_REPOSITORY_URI"
        ;;
    'ecr get-login-password') printf 'temporary-password\n' ;;
    'ecr describe-images') printf '%s\n' "$STUB_ECR_DIGEST" ;;
    *) exit 97 ;;
esac
EOF
cat > "$TEMP/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'docker %s\n' "$*" >> "$STUB_LOG"
case "$1 $2 $3" in
    'login --username AWS') read -r password; [[ "$password" == temporary-password ]] ;;
    'logout  '* ) ;;
    'buildx imagetools create') [[ "${STUB_PUSH_FAIL:-false}" != true ]] ;;
    'buildx imagetools inspect') cat "$STUB_MANIFEST" ;;
    *)
        if [[ "$1" == logout ]]; then exit 0; fi
        exit 98
        ;;
esac
EOF
chmod +x "$TEMP/bin/aws" "$TEMP/bin/docker"

ACCOUNT=123456789012
REGION=ap-southeast-1
REPOSITORY=tapstate-runtime-validation
REPOSITORY_URI="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/$REPOSITORY"

run_publish() {
    env PATH="$TEMP/bin:$PATH" \
        AWS_REGION="$REGION" AWS_ACCOUNT_ID="$ACCOUNT" ECR_REPOSITORY="$REPOSITORY" \
        STUB_ACCOUNT="${STUB_ACCOUNT_OVERRIDE:-$ACCOUNT}" \
        STUB_REPOSITORY_URI="${STUB_REPOSITORY_URI_OVERRIDE:-$REPOSITORY_URI}" \
        STUB_ECR_DIGEST="${STUB_ECR_DIGEST_OVERRIDE:-$DIGEST}" \
        STUB_REPOSITORY_MISSING="${STUB_REPOSITORY_MISSING:-false}" \
        STUB_PUSH_FAIL="${STUB_PUSH_FAIL:-false}" \
        STUB_MANIFEST="$TEMP/manifest.json" STUB_LOG="$TEMP/commands.log" \
        bash "$PUBLISHER" publish --archive "$TEMP/cloud-image.tar" \
            --tag "${PUBLISH_TAG:-validation-deadbeef-42-1}" --expected-digest "$DIGEST"
}

: > "$TEMP/commands.log"
if run_publish >"$TEMP/out" 2>"$TEMP/err" \
        && grep -qF "$REPOSITORY_URI@$DIGEST" "$TEMP/out" \
        && ! grep -qE 'create-repository|:latest' "$TEMP/commands.log"; then
    ok "the verified archive is pushed to the existing repository by digest"
else
    bad "the verified archive is pushed to the existing repository by digest" "$(cat "$TEMP/err")"
fi

if STUB_ACCOUNT_OVERRIDE=999999999999 run_publish >"$TEMP/out" 2>"$TEMP/err"; then
    bad "a different STS account is refused before login" "unexpected success"
elif grep -qF "different account" "$TEMP/err"; then
    ok "a different STS account is refused before login"
else
    bad "a different STS account is refused before login" "missing safe account diagnostic"
fi

if STUB_REPOSITORY_MISSING=true run_publish >"$TEMP/out" 2>"$TEMP/err"; then
    bad "the publisher never creates a missing repository" "unexpected success"
elif grep -qF "does not exist" "$TEMP/err"; then
    ok "the publisher never creates a missing repository"
else
    bad "the publisher never creates a missing repository" "missing repository diagnostic"
fi

OTHER_DIGEST="sha256:$(printf other | sha256sum | awk '{print $1}')"
if STUB_ECR_DIGEST_OVERRIDE="$OTHER_DIGEST" run_publish >"$TEMP/out" 2>"$TEMP/err"; then
    bad "an ECR digest mismatch is refused" "unexpected success"
elif grep -qF "different image digest" "$TEMP/err"; then
    ok "an ECR digest mismatch is refused"
else
    bad "an ECR digest mismatch is refused" "missing digest diagnostic"
fi

if PUBLISH_TAG=latest run_publish >"$TEMP/out" 2>"$TEMP/err"; then
    bad "the floating latest tag is refused" "unexpected success"
elif grep -qF "latest tag is not allowed" "$TEMP/err"; then
    ok "the floating latest tag is refused"
else
    bad "the floating latest tag is refused" "missing tag diagnostic"
fi

WORKFLOW="$ROOT/.github/workflows/cloud-ecr.yml"
if grep -qE '^  workflow_dispatch:' "$WORKFLOW" \
        && grep -qE '^      tapstate_revision:' "$WORKFLOW" \
        && grep -qE '^      web_revision:' "$WORKFLOW"; then
    ok "the independent workflow requires exact Tapstate and Web revisions"
else
    bad "the independent workflow requires exact Tapstate and Web revisions" "immutable inputs are missing"
fi
# shellcheck disable=SC2016  # This asserts the workflow expression literally.
if grep -qF 'environment: ${{ inputs.target_environment }}' "$WORKFLOW" \
        && grep -qF 'ecr-publish.sh auth-mode' "$WORKFLOW"; then
    ok "the independent workflow reads credentials only in its environment-scoped publish job"
else
    bad "the independent workflow reads credentials only in its environment-scoped publish job" \
        "environment or authentication gate is missing"
fi
if grep -qF 'deploy/cloud/ecr-publish.sh publish' "$WORKFLOW" \
        && grep -qF -- '--expected-digest' "$WORKFLOW" \
        && ! grep -qE 'gh release (create|upload)|:latest' "$WORKFLOW"; then
    ok "the independent workflow publishes the checked digest without creating a release or latest tag"
else
    bad "the independent workflow publishes the checked digest without creating a release or latest tag" \
        "the workflow bypasses the shared publisher or has a forbidden side effect"
fi

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[[ "$failed" -eq 0 ]]
