#!/usr/bin/env bash
#
# Stage a verified tapstate-web production bundle as generated App resources.
#
# The release workflow invokes this after building the pinned satellite checkout and before Maven
# repackages the Boot JAR. It never builds from a default branch, accepts a dirty checkout, or reuses
# a destination that may contain assets from an earlier build.

set -euo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PREFIX GIT_QUARANTINE_PATH

usage() {
    cat <<'USAGE'
Usage: scripts/prepare-web-assets.sh --web-root <path> --web-revision <sha> \
       --web-profile <cloud|onprem> [--cloud-console-url <https-url>] \
       --tapstate-revision <sha> --release-version <version> --output <path>

Build the clean pinned checkout with scripts/build-web-assets.sh using matching profile and URL inputs.
Cloud requires an explicit HTTPS Console URL; onprem must not receive one.
The output directory must not exist; it receives static/ and META-INF/tapstate-web.properties.
USAGE
}

die() {
    echo "prepare-web-assets.sh: $*" >&2
    exit 2
}

web_root=""
web_revision=""
web_profile=""
cloud_console_url=""
cloud_console_url_supplied=false
tapstate_revision=""
release_version=""
output=""

while [ $# -gt 0 ]; do
    case "$1" in
        --web-root|--web-revision|--web-profile|--cloud-console-url|--tapstate-revision|--release-version|--output)
            [ $# -ge 2 ] || die "$1 requires a value" ;;
    esac
    case "$1" in
        --web-root) web_root="${2:-}"; shift 2 ;;
        --web-revision) web_revision="${2:-}"; shift 2 ;;
        --web-profile) web_profile="${2:-}"; shift 2 ;;
        --cloud-console-url) cloud_console_url="${2:-}"; cloud_console_url_supplied=true; shift 2 ;;
        --tapstate-revision) tapstate_revision="${2:-}"; shift 2 ;;
        --release-version) release_version="${2:-}"; shift 2 ;;
        --output) output="${2:-}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; die "unknown argument '$1'" ;;
    esac
done

[ -n "$web_root" ] || die "--web-root is required"
[ -n "$web_revision" ] || die "--web-revision is required"
[ -n "$web_profile" ] || die "--web-profile is required"
[[ "$web_profile" = cloud || "$web_profile" = onprem ]] || die "Web profile must be cloud or onprem"
if [ "$web_profile" = cloud ]; then
    [ "$cloud_console_url_supplied" = true ] && [ -n "$cloud_console_url" ] \
        || die "--cloud-console-url is required for Cloud Web"
elif [ "$cloud_console_url_supplied" = true ]; then
    die "onprem Web must not receive a Cloud Console URL"
fi
[ -n "$tapstate_revision" ] || die "--tapstate-revision is required"
[ -n "$release_version" ] || die "--release-version is required"
[ -n "$output" ] || die "--output is required"
[[ "$tapstate_revision" =~ ^[0-9a-f]{40}$ ]] \
    || die "Tapstate revision must be a full lowercase SHA-1 commit id"
[[ "$web_revision" =~ ^[0-9a-f]{40}$ ]] \
    || die "Web revision must be a full lowercase SHA-1 commit id"
[[ "$release_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]] \
    || die "release version must be a numeric x.y.z version with an optional prerelease suffix"
[ -d "$web_root" ] || die "web checkout '$web_root' does not exist"
[ ! -e "$output" ] && [ ! -L "$output" ] \
    || die "output '$output' already exists; clean the Maven target directory before staging assets"

web_root="$(cd "$web_root" && pwd -P)"
repo_root="$(git -C "$web_root" rev-parse --show-toplevel 2>/dev/null)" \
    || die "'$web_root' is not a Git checkout"
[ "$repo_root" = "$web_root" ] || die "--web-root must name the checkout root, not a subdirectory"

expected="$(git -C "$web_root" rev-parse --verify "${web_revision}^{commit}" 2>/dev/null)" \
    || die "web revision '$web_revision' is not a commit in '$web_root'"
actual="$(git -C "$web_root" rev-parse HEAD)"
[ "$actual" = "$expected" ] || die "web checkout is at $actual, expected $expected"
git -C "$web_root" diff --quiet || die "web checkout has unstaged changes"
git -C "$web_root" diff --cached --quiet || die "web checkout has staged changes"
[ -z "$(git -C "$web_root" status --porcelain --untracked-files=all)" ] \
    || die "web checkout has untracked files"

[ -f "$web_root/package.json" ] || die "web checkout has no package.json"
[ -f "$web_root/pnpm-lock.yaml" ] || die "web checkout has no pnpm-lock.yaml"
[ -z "$(find "$web_root/apps/web/dist" -type l -print -quit)" ] \
    || die "production bundle contains symlinks"
[ -f "$web_root/apps/web/dist/index.html" ] || die "production bundle has no apps/web/dist/index.html"
[ -n "$(find "$web_root/apps/web/dist" -type f -print -quit)" ] \
    || die "production bundle contains no files"

script_directory="$(cd "$(dirname "$0")" && pwd -P)"
profile_arguments=(--web-root "$web_root" --web-revision "$web_revision" --web-profile "$web_profile")
if [ "$web_profile" = cloud ]; then
    cloud_console_url="$(python3 "$script_directory/web-assets-profile.py" normalize-url --value "$cloud_console_url")" \
        || die "Cloud Console URL does not satisfy the packaging contract"
    profile_arguments+=(--cloud-console-url "$cloud_console_url")
fi
expected_files_digest="$(python3 "$script_directory/web-assets-profile.py" verify "${profile_arguments[@]}")" \
    || die "production bundle build attestation does not satisfy the packaging contract"

mkdir -p "$output/static" "$output/META-INF"
cp -R "$web_root/apps/web/dist/." "$output/static/"
rm "$output/static/.tapstate-web-build.json"

manifest="$output/META-INF/tapstate-web.files.sha256"
(
    cd "$output/static"
    find . -type f -print | LC_ALL=C sort | while IFS= read -r file; do
        shasum -a 256 "$file"
    done
) >"$manifest"

files_digest="$(shasum -a 256 "$manifest" | awk '{print $1}')"
[ "$files_digest" = "$expected_files_digest" ] || die "staged bundle bytes changed after build verification"
cat >"$output/META-INF/tapstate-web.properties" <<EOF
repository=tapstate/tapstate-web
revision=$actual
files.sha256=$files_digest
tapstate.revision=$tapstate_revision
release.version=$release_version
web.profile=$web_profile
EOF
if [ "$web_profile" = cloud ]; then
    printf 'cloud.console.url=%s\n' "$cloud_console_url" >>"$output/META-INF/tapstate-web.properties"
fi

test -s "$output/static/index.html" || die "staged index.html is empty"
test -s "$manifest" || die "staged file manifest is empty"
echo "staged tapstate-web $actual in $output"
