#!/usr/bin/env bash
#
# The repositories that are tagged alongside a tapstate release, and the four things done to them.
#
#   satellites.sh reach                              the token can write to every one of them
#   satellites.sh branch <version>                   create ws/release-<version> in each
#   satellites.sh release <version> --notes-url <u>  tag that branch and publish a release pointing at u
#   satellites.sh unbranch <version>                 delete ws/release-<version> from each
#
# Why the list is a file and not written into this script: it is read twice, once when a release is
# started and once when it is published, and the two readers are in different workflows. Two copies
# of a list drift in exactly one direction -- a repository present in one and missing from the other
# is a repository that gets a branch and never gets a tag, and nothing anywhere goes red for it.
#
# Why an empty token is refused rather than skipped. This repository has already shipped that bug
# once: a cross-repository job read a secret that was not there, and did nothing -- no write, no
# error -- because an empty token is not a failure until something tries to use it. So `reach` fails
# on an empty token before it asks anything, and it runs as a release gate, before the tag exists.
# That ordering is the whole point: tapstate's tag is irreversible, and a token that turns out not to
# reach the documentation repository is worth knowing about while the release is still a draft.
# Discovering it afterwards leaves a published release whose documentation never went public.
#
# Why an empty list is refused too. It is the same failure one level up: a list that resolves to
# nothing makes every verb here a successful no-op, and "there were no satellites" and "the file did
# not parse" look identical from the outside.
#
# Why the tag goes on the release branch's tip and not on the default branch. Pinning four
# repositories at the start of a release is only worth doing if the thing tagged at the end is what
# was pinned. Tagging the default branch would let anything merged during the release -- in any of
# the four -- into the release, which is the situation the branch was introduced to end.
#
# Nothing here decides the version number. It arrives already worked out from the tapstate side,
# where it is computed from the commit's ancestry; a satellite has no tags of its own to compute one
# from, and a second opinion about the version is a second version.

set -uo pipefail

verb="${1:-}"
[ $# -gt 0 ] && shift

version=""
list=".github/release-satellites.txt"
plan=0
notes_url=""

case "$verb" in
    branch|release|unbranch)
        case "${1:-}" in
            ""|--*) ;;
            *) version="$1"; shift ;;
        esac
        ;;
esac

while [ $# -gt 0 ]; do
    case "$1" in
        --list) list="${2:-}"; shift 2 ;;
        --plan) plan=1; shift ;;
        --notes-url) notes_url="${2:-}"; shift 2 ;;
        *) echo "satellites.sh: unknown argument '$1'" >&2; exit 2 ;;
    esac
done

die() { echo "satellites.sh: $1" >&2; exit "${2:-1}"; }

case "$verb" in
    reach|branch|release|unbranch) ;;
    "") die "no verb. Expected one of: reach, branch, release, unbranch" 2 ;;
    *)  die "unknown verb '$verb'. Expected one of: reach, branch, release, unbranch" 2 ;;
esac

case "$verb" in
    branch|release|unbranch)
        [ -n "$version" ] || die "$verb needs a version, as x.y.z" 2
        case "$version" in
            *[!0-9.]*|*..*|.*|*.) die "'$version' is not a version. Expected x.y.z, with no leading v" 2 ;;
        esac
        case "$version" in
            *.*.*) case "$version" in *.*.*.*) die "'$version' is not a version. Expected x.y.z" 2 ;; esac ;;
            *) die "'$version' is not a version. Expected x.y.z, with no leading v" 2 ;;
        esac
        ;;
esac

[ "$verb" != release ] || [ -n "$notes_url" ] || \
    die "release needs --notes-url: the link back to the tapstate release is the entire body of a satellite release" 2

[ -f "$list" ] || die "no satellite list at '$list'"

repos="$(sed 's/#.*//' "$list" | tr -d '\r' | awk 'NF')"
[ -n "$repos" ] || die "'$list' names no repositories; refusing to treat that as 'there are none'"

branch="ws/release-$version"
tag="v$version"
rc=0

for repo in $repos; do
    case "$repo" in
        */*) ;;
        *) die "'$repo' in $list is not owner/repo" ;;
    esac

    case "$verb" in
        reach)
            if [ -z "${GH_TOKEN:-}${GITHUB_TOKEN:-}" ]; then
                die "no token in GH_TOKEN. An empty token writes nothing and reports nothing"
            fi
            push="$(gh api "repos/$repo" --jq '.permissions.push' 2>/dev/null || true)"
            if [ "$push" = true ]; then
                echo "$repo  reachable, push permitted"
            else
                echo "$repo  NOT writable by this token (permissions.push=${push:-unreadable})" >&2
                rc=1
            fi
            ;;
        branch)
            if [ "$plan" = 1 ]; then
                echo "$repo  create refs/heads/$branch at its default branch tip"
                continue
            fi
            head="$(gh api "repos/$repo" --jq '.default_branch' 2>/dev/null)"
            [ -n "$head" ] || { echo "$repo  cannot read the default branch" >&2; rc=1; continue; }
            sha="$(gh api "repos/$repo/git/ref/heads/$head" --jq '.object.sha' 2>/dev/null)"
            [ -n "$sha" ] || { echo "$repo  cannot read $head" >&2; rc=1; continue; }
            if gh api "repos/$repo/git/refs" -f "ref=refs/heads/$branch" -f "sha=$sha" >/dev/null 2>&1; then
                echo "$repo  $branch created at ${sha%"${sha#???????}"}"
            else
                echo "$repo  could not create $branch" >&2
                rc=1
            fi
            ;;
        release)
            if [ "$plan" = 1 ]; then
                echo "$repo  tag $tag at $branch, release body links to $notes_url"
                continue
            fi
            sha="$(gh api "repos/$repo/git/ref/heads/$branch" --jq '.object.sha' 2>/dev/null)"
            [ -n "$sha" ] || { echo "$repo  has no $branch to tag" >&2; rc=1; continue; }
            if gh release create "$tag" --repo "$repo" --target "$sha" --title "$tag" \
                 --notes "This repository is released alongside tapstate $tag. What changed, and everything else worth reading, is in the tapstate release: $notes_url" \
                 >/dev/null 2>&1; then
                echo "$repo  released $tag"
            else
                echo "$repo  could not release $tag" >&2
                rc=1
            fi
            ;;
        unbranch)
            if [ "$plan" = 1 ]; then
                echo "$repo  delete refs/heads/$branch"
                continue
            fi
            if gh api -X DELETE "repos/$repo/git/refs/heads/$branch" >/dev/null 2>&1; then
                echo "$repo  $branch deleted"
            else
                echo "$repo  could not delete $branch (already gone?)" >&2
            fi
            ;;
    esac
done

exit "$rc"
