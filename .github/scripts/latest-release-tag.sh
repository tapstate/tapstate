#!/usr/bin/env bash
#
# The newest published release, as a version tag.
#
# Three workflows need this answer: the quickstart lane runs against it, the install lane compares
# the edge scripts to it, and the install-site publisher falls back to it when a maintainer
# dispatches a run without naming a tag. All three used to work it out themselves, and two of the
# three got it wrong the same way.
#
# The wrong way is to take the first entry of the release list. Releases arrive newest-first by
# publication date, and not every release is a version: `connectors-preview` is a release and a
# floating git tag, so re-pushing it puts it at the front. What the callers then do with that answer
# is worse than an error would be -- the install lane compares the live edge against a months-old
# script and reports a difference that is not one, and the publisher deploys that months-old script
# to the install site and reports success, because its check compares the site against the very
# thing it just uploaded.
#
# So: keep only the tags that name a version, then take the newest of those.
#
# Newest here means newest by publication date, which is the order the list already arrives in. That
# is not the same question as which release line is current -- a fix shipped onto an older line is
# published later than the newer line it does not belong to. The floating pointer that answers that
# question is set where the release is made, not here.
#
# Refuses rather than answering with nothing. An empty tag flows on into `git show "$tag:path"` and
# into a deploy, and fails somewhere far enough away that the cause is not obvious.
#
# Pass a file holding the release list as JSON to resolve against that instead of asking GitHub;
# that is how the cases build lists this repository does not currently produce.

set -eu

list_file="${1:-}"

if [ -n "$list_file" ]; then
    releases="$(cat "$list_file")"
else
    releases="$(gh release list --limit 30 --exclude-drafts --json tagName)"
fi

tag="$(printf '%s' "$releases" | jq -r 'map(select(.tagName | startswith("v"))) | first | .tagName // empty')"

if [ -z "$tag" ]; then
    echo "no published release carries a version tag - only non-version tags such as connectors-preview" >&2
    exit 1
fi

echo "$tag"
