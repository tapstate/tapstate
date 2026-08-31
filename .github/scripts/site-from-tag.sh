#!/usr/bin/env bash
#
# Assemble the two scripts install.tapstate.dev serves, for one release tag:
#
#   site-from-tag.sh <tag> <outdir>
#
# The content comes from the tag. The version does not: it comes from the tag's *name*, and is
# written into the extracted copies before they are published.
#
# That distinction is the whole point of this script. A release is cut by naming a bump, not a
# version -- the number is counted up at release time and written into the tree by set-version.sh
# inside the runner, which never commits. The tag is then created on the commit that CI actually
# validated, which is the commit as it was before that write. So the tree a release tag points at
# carries the *previous* release's pins, by construction and on purpose.
#
# Publishing those two files straight out of the tag therefore ships an installer that pins the
# release before the one being announced. Nothing reports it: the file is present, correct in every
# other respect, and installs a version that genuinely exists, so both the deployment check and the
# quickstart lane pass. The one-liner in the README simply hands people the old release, forever.
#
# On a tag cut the old way -- when the bump was committed before tagging -- writing the version in is
# a no-op, because the pins already say it. The operation is idempotent, and correct for both eras.
#
# set-version.sh is used rather than a second set of substitutions here. It knows all six pins and
# refuses when one of them stops matching, and duplicating two of its patterns in this file is the
# exact drift it exists to catch. It comes from this checkout, not from the tag: it is a tool that
# runs on the release, not part of what the release ships.

set -eu

tag="${1:-}"
outdir="${2:-}"
here="$(cd "$(dirname "$0")" && pwd)"

if [ -z "$tag" ] || [ -z "$outdir" ]; then
    echo "usage: site-from-tag.sh <tag> <outdir>" >&2
    exit 1
fi

# Checked before it reaches git. This runs in the job that holds the credentials for the domain the
# README tells people to pipe into sh, and the tag can arrive from a dispatch input.
git check-ref-format "refs/tags/$tag" \
    || { echo "refusing a tag that is not a valid ref name: $tag" >&2; exit 1; }

version="${tag#v}"
case "$version" in
    *[!0-9.]* | '' ) bad=yes ;;
    *) bad=no ;;
esac
if [ "$bad" = yes ] || [ "$(echo "$version" | tr -cd . | wc -c)" -ne 2 ]; then
    echo "'$tag' does not name a release version - expected vX.Y.Z, as in v0.4.0" >&2
    exit 1
fi

src="$(mktemp -d)"
trap 'rm -rf "$src"' EXIT
git archive "$tag" | tar -x -C "$src" \
    || { echo "cannot read the tree at $tag" >&2; exit 1; }

# Refuses rather than falling back. A tag too old to carry every pin cannot be the newest release
# again, and publishing an installer whose pinned version we could not write is silent in a way that
# a failed job is not -- this is the file people pipe into a shell.
( cd "$src" && "$here/set-version.sh" "$version" >/dev/null ) \
    || { echo "$tag: cannot write $version into its tree - too old to publish from" >&2; exit 1; }

mkdir -p "$outdir"
cp "$src/deploy/quickstart/quickstart.sh" "$outdir/quickstart.sh"
cp "$src/install/install.sh" "$outdir/install.sh"

# The copy is checked, not assumed. set-version.sh proves it wrote the tree; this proves the two
# files that reach the edge are the ones it wrote.
grep -qF "PINNED_VERSION=\"$version\"" "$outdir/install.sh" \
    || { echo "install.sh reached the site without pinning $version" >&2; exit 1; }
grep -qF "CLI_VERSION=\"$version\"" "$outdir/quickstart.sh" \
    || { echo "quickstart.sh reached the site without pinning $version" >&2; exit 1; }

echo "$version"
