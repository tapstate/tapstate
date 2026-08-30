#!/usr/bin/env bash
#
# Is this release complete? Run from the repository root, against what a release run has just built:
#
#   release-manifest.sh <version> <staging-dir> <oci-dir>
#
# The expectation is never restated here. Restating it would mean the release checks itself against a
# list it also wrote, which is worth nothing. It comes from two places instead:
#
#   Asked of the consumers        the platforms are whatever `install.sh --print-platforms` offers,
#                                 because that is the list a user's machine consults; the image
#                                 reference is whatever the quickstart compose file pins, because that
#                                 is what a quickstart user actually runs. Either one moving without a
#                                 matching asset is a 404 for somebody, and this is where it surfaces.
#
#   Named outright                checksums.txt, platform-minimums.txt, the linux/arm64 server image,
#                                 and what has to be inside each CLI asset. Nothing automated fetches
#                                 the first three, so nothing else would ever notice them missing.
#                                 The fourth has one consumer and it is on the wrong side of the
#                                 release: install.sh refuses a bundle without an executable and a
#                                 sidecar, but that refusal runs on a user's machine after the
#                                 release is public. Naming them is the only coverage they have
#                                 before it, and it is why this list is written by hand rather than
#                                 derived from what the build produced.
#
# Both directions are checked. An asset with no platform offering it is as wrong as a platform with no
# asset: one of the two is a mistake, and which one it is needs a person.
#
# Known boundary, not a gap: connector jars are not part of a versioned release. They are carried by a
# floating prerelease that is republished on its own schedule, so they are neither expected here nor
# counted as missing. That is a temporary arrangement, and while it holds this check would otherwise
# have to choose between reporting them missing every time and not looking at connectors at all.
#
# Reports everything wrong before exiting, rather than stopping at the first. A release is assembled
# once and looked at by a person once; handing back one problem at a time wastes both.

set -eu

version="${1:-}"
staging="${2:-}"
oci="${3:-}"

if [ -z "$version" ] || [ -z "$staging" ] || [ -z "$oci" ]; then
    echo "usage: release-manifest.sh <version> <staging-dir> <oci-dir>" >&2
    exit 1
fi

problems=0
problem() {
    echo "  missing or wrong: $1" >&2
    problems=$((problems + 1))
}

# --- asked of the installer: every platform it offers has an asset behind it ---------------------
platforms="$(sh install/install.sh --print-platforms)"
for platform in $platforms; do
    asset="tapstate-${version}-${platform}.tar.gz"
    [ -f "$staging/$asset" ]         || problem "$asset (the installer offers $platform)"
    [ -f "$staging/$asset.sha256" ]  || problem "$asset.sha256 (the installer verifies every download)"
done

# --- and the other direction: no asset for a platform nobody offers ------------------------------
for built in "$staging"/tapstate-"$version"-*.tar.gz; do
    [ -f "$built" ] || continue
    name="$(basename "$built")"
    platform="${name#tapstate-"${version}"-}"
    platform="${platform%.tar.gz}"
    case "
$platforms
" in
        *"
$platform
"*) ;;
        *) problem "$name is built for $platform, which the installer does not offer" ;;
    esac
done

# --- named outright: an asset being present is not an asset being complete -----------------------
# The archive is opened, not weighed. A packaging step that dropped the sidecar, or wrote an error
# message where the tarball should be, produces a file of plausible name and size either way, and
# every other check here is satisfied by the name alone.
for platform in $platforms; do
    asset="$staging/tapstate-${version}-${platform}.tar.gz"
    [ -f "$asset" ] || continue    # already reported above as the asset that is not there at all
    if ! listing="$(tar -tzf "$asset" 2>/dev/null)"; then
        problem "tapstate-${version}-${platform}.tar.gz is not readable as a gzipped archive"
        continue
    fi
    printf '%s\n' "$listing" | grep -qE '(^|/)bin/tapstate$' \
        || problem "tapstate-${version}-${platform}.tar.gz has no bin/tapstate (the CLI itself)"
    printf '%s\n' "$listing" | grep -qE '(^|/)libexec/tapstate-mcp(\.jar)?$' \
        || problem "tapstate-${version}-${platform}.tar.gz has no libexec MCP sidecar (install.sh refuses a bundle without one)"
done

# --- named outright: nothing fetches these, so nothing else would report them gone ---------------
[ -s "$staging/checksums.txt" ] || problem "checksums.txt (the only verification path that does not go through our own tooling)"
[ -s "$staging/platform-minimums.txt" ] || problem "platform-minimums.txt (the installer reads it to warn about an old system)"

if [ -s "$staging/checksums.txt" ]; then
    for platform in $platforms; do
        grep -q "tapstate-${version}-${platform}.tar.gz" "$staging/checksums.txt" \
            || problem "checksums.txt does not cover tapstate-${version}-${platform}.tar.gz"
    done
fi

# --- the server image, read out of the archive the release built and has not pushed yet ----------
if [ ! -f "$oci/index.json" ]; then
    problem "$oci/index.json (no server image archive was produced)"
else
    digest="$(jq -r '.manifests[0].digest' "$oci/index.json")"
    blob="$oci/blobs/${digest%%:*}/${digest#*:}"
    if [ ! -f "$blob" ]; then
        problem "the image archive's manifest list blob ($digest)"
    else
        architectures="$(jq -r '.manifests[] | select(.platform.architecture != null and .platform.architecture != "unknown") | .platform.architecture' "$blob" | sort -u)"
        for want in amd64 arm64; do
            printf '%s\n' "$architectures" | grep -qx "$want" \
                || problem "the server image has no linux/$want (built: $(printf '%s' "$architectures" | tr '\n' ' '))"
        done
    fi
fi

# --- asked of the compose file the quickstart runs -----------------------------------------------
pinned="$(sed -n 's|.*image: ghcr.io/tapstate/tapstate:\([^ ]*\).*|\1|p' deploy/quickstart/docker-compose.yml | head -1)"
[ "$pinned" = "$version" ] \
    || problem "the quickstart compose file runs ghcr.io/tapstate/tapstate:${pinned:-<none>}, not :$version"

if [ "$problems" -ne 0 ]; then
    echo "$problems item(s) wrong - this release is not complete" >&2
    exit 1
fi

echo "release $version is complete: $(printf '%s' "$platforms" | tr '\n' ' ')CLI assets carrying their sidecar, both server image architectures, and the two files nothing fetches"
