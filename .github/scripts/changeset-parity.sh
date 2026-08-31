#!/usr/bin/env bash
#
# A patch release must ship the same system-data changesets as the release it is a patch of:
#
#   changeset-parity.sh <base-ref> <release-ref>
#
# Changeset numbers are globally monotonic and only ever go out with a MINOR from the default branch.
# A fix for an older line is cherry-picked, and if a changeset comes along with it -- 0.5.1 taking the
# database to changeset 8 -- then 0.6.0, which only knows up to 7, refuses to start against every
# instance 0.5.1 upgraded. The version gate fires inside a single MAJOR, against users who did nothing
# wrong, and nothing anywhere reports it on the way: both releases build, both install, and the two
# only meet on somebody's server.
#
# Read from the tree rather than from a built CLI. `tapstate migrate --list` is the eventual reading
# of this same list, but asking it would mean building two versions of the product at release time to
# compare them, and the answer is already in the source at each ref.
#
# THIS CHECK IS VACUOUS TODAY AND SAYS SO. No changeset exists yet in any tree, so both lists are
# empty and it can only pass. It is written now because the release path that would carry a changeset
# onto an old line is being built now, and a check added after the first patch release is a check that
# was not there for it. What it must never become is quietly vacuous: if changesets land somewhere
# other than the package below, an empty list would go on looking like agreement forever. Hence the
# refusal when the migration runner exists and the changeset package does not -- that combination
# means this script is looking in the wrong place.

set -eu

base="${1:-}"
release="${2:-}"

if [ -z "$base" ] || [ -z "$release" ]; then
    echo "usage: changeset-parity.sh <base-ref> <release-ref>" >&2
    exit 1
fi

changesets_dir=adapters/adapter-mongo-store/src/main/java/io/tapstate/adapters/mongostore/migration

# The changesets a ref carries, one class name per line.
changesets_at() {
    git ls-tree -r --name-only "$1" -- "$changesets_dir" 2>/dev/null \
        | sed -n 's|.*/\([A-Za-z0-9_]*\)\.java$|\1|p' \
        | grep -v '^MigrationRunner$' \
        | sort
}

for ref in "$base" "$release"; do
    git rev-parse --verify --quiet "$ref^{commit}" >/dev/null \
        || { echo "'$ref' is not a commit this repository knows" >&2; exit 1; }
done

base_list="$(changesets_at "$base")"
release_list="$(changesets_at "$release")"

# A migration framework with no changesets where this script looks means they are somewhere it cannot
# see, and every answer it gives from here is meaningless rather than reassuring.
if [ -z "$release_list" ] && git ls-tree -r --name-only "$release" | grep -q '/MigrationRunner\.java$'; then
    echo "$release has a migration runner but no changesets under $changesets_dir." >&2
    echo "They have moved, and this check has been comparing two empty lists. Point it at the new location." >&2
    exit 1
fi

if [ "$base_list" = "$release_list" ]; then
    if [ -z "$release_list" ]; then
        echo "no changesets exist in either tree - this check is vacuous until they do (base $base, release $release)"
    else
        echo "$(printf '%s\n' "$release_list" | wc -l | tr -d ' ') changeset(s), the same in both: $(printf '%s' "$release_list" | tr '\n' ' ')"
    fi
    exit 0
fi

echo "this patch does not carry the same changesets as $base:" >&2
printf '%s\n' "$release_list" | comm -23 - <(printf '%s\n' "$base_list") | sed 's/^/  only in the release: /' >&2
printf '%s\n' "$base_list" | comm -23 - <(printf '%s\n' "$release_list") | sed 's/^/  only in the base:    /' >&2
echo "A changeset ships with a MINOR from the default branch, never on a cherry-picked patch." >&2
exit 1
