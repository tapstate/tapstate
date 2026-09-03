#!/usr/bin/env bash
#
# Cases for release-manifest.sh. Each stages a complete release and then removes exactly one thing, so
# every assertion is shown failing on its own. A case that only stages a complete release proves the
# check can say yes, which is the half that was never in doubt.

set -eu

here="$(cd "$(dirname "$0")" && pwd)"
script="$here/release-manifest.sh"
repo="$(cd "$here/../.." && pwd)"
failures=0

pass_case() {   # name, root, staging, oci
    local name="$1"; shift
    if ( cd "$1" && "$script" 9.9.9 "$2" "$3" >/dev/null 2>&1 ); then
        echo "ok   - $name"
    else
        echo "FAIL - $name: the check refused a complete release"
        echo "       $( cd "$1" && "$script" 9.9.9 "$2" "$3" 2>&1 | tail -3 )"
        failures=$((failures + 1))
    fi
}

fail_case() {   # name, root, staging, oci, text the refusal must name
    local name="$1" root="$2" staging="$3" oci="$4" needle="$5" out
    out="$( cd "$root" && "$script" 9.9.9 "$staging" "$oci" 2>&1 )" && {
        echo "FAIL - $name: the check passed"
        failures=$((failures + 1))
        return
    }
    if printf '%s' "$out" | grep -q "$needle"; then
        echo "ok   - $name"
    else
        echo "FAIL - $name: refused, but without naming '$needle': $(printf '%s' "$out" | tail -1)"
        failures=$((failures + 1))
    fi
}

# A repository root the check can ask its questions of: the real installer, so the platform list under
# test is the one a user's machine actually consults, and a compose file pinning the version.
make_root() {   # version pinned in compose
    local dir; dir="$(mktemp -d)"
    mkdir -p "$dir/install" "$dir/deploy/quickstart"
    cp "$repo/install/install.sh" "$dir/install/install.sh"
    printf 'services:\n  tapstate:\n    image: ghcr.io/tapstate/tapstate:%s\n' "$1" > "$dir/deploy/quickstart/docker-compose.yml"
    echo "$dir"
}

# One release asset, built the way the assembly descriptor builds one: a versioned directory holding
# the native CLI and its MCP sidecar. Real tar.gz, because the check opens them -- a fixture of text
# files named .tar.gz would pass an existence check and nothing else, which is the state this replaced.
make_asset() {   # staging dir, platform, [omit: bin | mcp | nothing]
    local dir="$1" platform="$2" omit="${3:-}" build
    build="$(mktemp -d)"
    mkdir -p "$build/tapstate-cli-9.9.9/bin" "$build/tapstate-cli-9.9.9/libexec"
    [ "$omit" = bin ] || echo "native CLI for $platform" > "$build/tapstate-cli-9.9.9/bin/tapstate"
    [ "$omit" = mcp ] || echo "sidecar for $platform"    > "$build/tapstate-cli-9.9.9/libexec/tapstate-mcp.jar"
    tar -czf "$dir/tapstate-9.9.9-$platform.tar.gz" -C "$build" tapstate-cli-9.9.9
    echo "sha for $platform" > "$dir/tapstate-9.9.9-$platform.tar.gz.sha256"
    rm -rf "$build"
}

make_staging() {   # every asset a complete release has
    local dir; dir="$(mktemp -d)"
    for p in darwin-arm64 darwin-x64 linux-arm64 linux-x64; do
        make_asset "$dir" "$p"
    done
    ( cd "$dir" && ls tapstate-*.tar.gz > checksums.txt )
    echo "linux glibc 2.35" > "$dir/platform-minimums.txt"
    echo "$dir"
}

# An OCI layout the way buildx writes one: index.json points at a manifest list blob, and that blob is
# where the architectures are declared.
make_oci() {   # architectures, space separated
    local dir; dir="$(mktemp -d)"
    mkdir -p "$dir/blobs/sha256"
    local entries="" sep=""
    for arch in $1; do
        entries="$entries$sep{\"platform\":{\"os\":\"linux\",\"architecture\":\"$arch\"},\"digest\":\"sha256:dead\"}"
        sep=","
    done
    printf '{"manifests":[%s]}' "$entries" > "$dir/blobs/sha256/listdigest"
    printf '{"manifests":[{"digest":"sha256:listdigest","mediaType":"application/vnd.oci.image.index.v1+json"}]}' > "$dir/index.json"
    echo "$dir"
}

root="$(make_root 9.9.9)"; staging="$(make_staging)"; oci="$(make_oci "amd64 arm64")"
pass_case "a complete release passes" "$root" "$staging" "$oci"

# Named outright because nothing consumes them. No automated path fetches either file, so their going
# missing is invisible everywhere else -- which is the whole reason they are listed by name.
rm -f "$staging/checksums.txt"
fail_case "a missing checksums.txt is caught" "$root" "$staging" "$oci" "checksums.txt"
( cd "$staging" && ls tapstate-*.tar.gz > checksums.txt )

rm -f "$staging/platform-minimums.txt"
fail_case "a missing platform-minimums.txt is caught" "$root" "$staging" "$oci" "platform-minimums.txt"
echo "linux glibc 2.35" > "$staging/platform-minimums.txt"

# Asked of the installer rather than restated here. A platform it offers with no asset behind it is a
# 404 for whoever runs on it, and nothing else in the release would notice.
rm -f "$staging/tapstate-9.9.9-linux-arm64.tar.gz"
fail_case "a platform the installer offers with no asset is caught" "$root" "$staging" "$oci" "linux-arm64"
make_asset "$staging" linux-arm64

rm -f "$staging/tapstate-9.9.9-darwin-x64.tar.gz.sha256"
fail_case "a missing per-asset checksum is caught" "$root" "$staging" "$oci" "darwin-x64"
echo "sha for darwin-x64" > "$staging/tapstate-9.9.9-darwin-x64.tar.gz.sha256"

# The other direction. An asset for a platform the installer does not offer cannot be reached by any
# documented path, so it is either a leftover or the list is wrong; both want a person to look.
make_asset "$staging" freebsd-x64
fail_case "an asset for a platform nobody offers is caught" "$root" "$staging" "$oci" "freebsd-x64"
rm -f "$staging/tapstate-9.9.9-freebsd-x64.tar.gz"

# The third thing named outright. Nothing pulls the arm64 server image on any automated path, so a
# build that quietly produced one architecture would go out looking complete.
oci_one="$(make_oci amd64)"
fail_case "a server image missing an architecture is caught" "$root" "$staging" "$oci_one" "arm64"
rm -rf "$oci_one"

# What is inside an asset, not only that one is there. install.sh refuses a bundle missing either of
# these, but that refusal runs on a user's machine after the release is public; before this point
# nothing opens an asset at all. Dropping the sidecar from the assembly descriptor renames nothing and
# breaks no build -- the tarball is still produced, still checksummed, still the right size to look
# right -- and `tapstate mcp` is simply gone for everyone who installs that version.
rm -f "$staging/tapstate-9.9.9-linux-x64.tar.gz"
make_asset "$staging" linux-x64 mcp
fail_case "an asset built without its MCP sidecar is caught" "$root" "$staging" "$oci" "linux-x64"
rm -f "$staging/tapstate-9.9.9-linux-x64.tar.gz"
make_asset "$staging" linux-x64

rm -f "$staging/tapstate-9.9.9-darwin-arm64.tar.gz"
make_asset "$staging" darwin-arm64 bin
fail_case "an asset built without the CLI itself is caught" "$root" "$staging" "$oci" "darwin-arm64"
rm -f "$staging/tapstate-9.9.9-darwin-arm64.tar.gz"
make_asset "$staging" darwin-arm64

# The shape the old fixture had by accident: a file with the right name that is not an archive. A
# packaging step that wrote an error message where the tarball should be produces exactly this.
echo "not an archive" > "$staging/tapstate-9.9.9-linux-arm64.tar.gz"
fail_case "an asset that is not readable as an archive is caught" "$root" "$staging" "$oci" "linux-arm64"
rm -f "$staging/tapstate-9.9.9-linux-arm64.tar.gz"
make_asset "$staging" linux-arm64

# Asked of the compose file, which is what a quickstart user actually runs.
root_wrong="$(make_root 9.9.8)"
fail_case "a compose file pinning another version is caught" "$root_wrong" "$staging" "$oci" "9.9.8"
rm -rf "$root_wrong"

pass_case "the complete release still passes after every repair" "$root" "$staging" "$oci"

rm -rf "$root" "$staging" "$oci"

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed" >&2
    exit 1
fi
echo "all cases passed"
