#!/usr/bin/env bash
#
# The version a release is about to become, worked out rather than typed.
#
#   next-version.sh <major|minor|patch> [<commit>]
#
# Prints three lines, in the shape a workflow appends straight to its step output:
#
#   version=0.6.1
#   tag=v0.6.1
#   is_latest=false
#   base=v0.6.0
#
# Counts up from the newest version tag that is an ANCESTOR of the commit being released, not from
# the newest tag in the repository. Those differ exactly when they matter: once 0.6.0 is out, a fix
# for someone still on the 0.5 line is built from a commit whose ancestry ends at v0.5.0, and it has
# to become 0.5.1. Reading the repository-wide newest tag would answer 0.6.1 -- a number that is
# well-formed, plausible, on the wrong line, and that nothing downstream has any reason to question.
#
# is_latest answers the other half of that same split: whether this release is ahead of every version
# already tagged. Two irreversible things read it -- whether the floating pointer moves, and whether
# the version write-back is opened against the default branch. Both are wrong for a fix on an older
# line, and both are wrong quietly: a user installing from the official entry point would land on the
# older line, and the write-back would walk the default branch's version backwards in a pull request
# where every pin agrees with every other and nothing looks amiss.
#
# base is the tag it counted up from, handed back rather than left to be worked out a second time.
# Two things downstream need it -- the release-notes range and the changeset parity check -- and both
# need the same ancestry rule, so a second `git describe` elsewhere is a second implementation of the
# split this script exists to get right.
#
# Refuses rather than guessing, in three places: an unknown bump, an ancestry with no version tag on
# it, and a version that already exists. A guessed version cannot be taken back once it is published.

set -eu

bump="${1:-}"
ref="${2:-HEAD}"

case "$bump" in
    major|minor|patch) ;;
    *) echo "expected a bump of major, minor or patch - got '${bump}'" >&2; exit 1 ;;
esac

base="$(git describe --tags --abbrev=0 --match 'v[0-9]*.[0-9]*.[0-9]*' "$ref" 2>/dev/null || true)"
if [ -z "$base" ]; then
    echo "no version tag is an ancestor of '$ref' - there is nothing to count up from" >&2
    exit 1
fi

base_tag="$base"
base="${base#v}"
major="${base%%.*}"
rest="${base#*.}"
minor="${rest%%.*}"
patch="${rest#*.}"

case "$bump" in
    major) version="$((major + 1)).0.0" ;;
    minor) version="${major}.$((minor + 1)).0" ;;
    patch) version="${major}.${minor}.$((patch + 1))" ;;
esac

highest="$(git tag --list 'v[0-9]*.[0-9]*.[0-9]*' | sed 's/^v//' | sort -V | tail -1)"

if [ "$version" = "$highest" ] || git rev-parse -q --verify "refs/tags/v$version" >/dev/null; then
    echo "v$version already exists - a version is spent once it is tagged" >&2
    exit 1
fi

# Ahead of everything already tagged, or a fix on a line that has been overtaken.
if [ "$(printf '%s\n%s\n' "$version" "$highest" | sort -V | tail -1)" = "$version" ]; then
    is_latest=true
else
    is_latest=false
fi

echo "version=$version"
echo "tag=v$version"
echo "is_latest=$is_latest"
echo "base=$base_tag"
