#!/usr/bin/env bash
#
# Stage a verified tapstate-web production bundle as generated App resources.
#
# The release workflow invokes this after building the pinned satellite checkout and before Maven
# repackages the Boot JAR. It never builds from a default branch, accepts a dirty checkout, or reuses
# a destination that may contain assets from an earlier build.

set -euo pipefail

usage() {
    cat <<'USAGE'
Usage: scripts/prepare-web-assets.sh --web-root <path> --web-revision <sha> --output <path>

The web checkout must be clean, checked out at --web-revision, and contain apps/web/dist/index.html.
The output directory must not exist; it receives static/ and META-INF/tapstate-web.properties.
USAGE
}

die() {
    echo "prepare-web-assets.sh: $*" >&2
    exit 2
}

web_root=""
web_revision=""
output=""

while [ $# -gt 0 ]; do
    case "$1" in
        --web-root) web_root="${2:-}"; shift 2 ;;
        --web-revision) web_revision="${2:-}"; shift 2 ;;
        --output) output="${2:-}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; die "unknown argument '$1'" ;;
    esac
done

[ -n "$web_root" ] || die "--web-root is required"
[ -n "$web_revision" ] || die "--web-revision is required"
[ -n "$output" ] || die "--output is required"
[ -d "$web_root" ] || die "web checkout '$web_root' does not exist"
[ ! -e "$output" ] || die "output '$output' already exists; clean the Maven target directory before staging assets"

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

[ -f "$web_root/package.json" ] || die "web checkout has no package.json"
[ -f "$web_root/pnpm-lock.yaml" ] || die "web checkout has no pnpm-lock.yaml"
[ -f "$web_root/apps/web/dist/index.html" ] || die "production bundle has no apps/web/dist/index.html"
[ -n "$(find "$web_root/apps/web/dist" -type f -print -quit)" ] \
    || die "production bundle contains no files"

mkdir -p "$output/static" "$output/META-INF"
cp -R "$web_root/apps/web/dist/." "$output/static/"

manifest="$output/META-INF/tapstate-web.files.sha256"
(
    cd "$output/static"
    find . -type f -print | LC_ALL=C sort | while IFS= read -r file; do
        shasum -a 256 "$file"
    done
) >"$manifest"

files_digest="$(shasum -a 256 "$manifest" | awk '{print $1}')"
cat >"$output/META-INF/tapstate-web.properties" <<EOF
repository=tapstate/tapstate-web
revision=$actual
files.sha256=$files_digest
EOF

test -s "$output/static/index.html" || die "staged index.html is empty"
test -s "$manifest" || die "staged file manifest is empty"
echo "staged tapstate-web $actual in $output"
