#!/usr/bin/env bash
# The body of a release, assembled at the moment it is cut.
#
#   release-notes.sh --version <v> --base <tag> --sha <commit> --macos-req <sentence> --glibc-req <sentence>
#
# Three layers, and each one is answering a different way of misleading a reader:
#
#   1. Fixed prose, written here.        Not saying it is concealment. What this runtime does to an
#                                        insert-only target, and what it can miss while a change
#                                        stream positions itself, have no scenario covering them, so
#                                        a reader who is not told has no other way to find out.
#   2. Harvested and then edited by       Not saying it is wasted work. The sentence describing a
#      whoever approves the release.      change is written by whoever made it, in their pull
#                                        request, and copied here unedited.
#   3. Appended by GitHub afterwards.     Not saying it is an omission. Whatever nobody wrote a
#      (`generate_release_notes`)         sentence for is still listed, derived from the range.
#
# Layer 3 is why nothing here writes a "## What's Changed": GitHub appends its list after this body,
# so emitting one would put it in the middle and give a reader two lists of different lengths.
#
# The supported-versions link at the end is an absolute URL on purpose. A release body is rendered on
# the releases page, not from a file in the tree, so a relative path resolves against that page and
# 404s. `blob/HEAD` names no branch, so it follows the default branch and keeps pointing at the
# current promise rather than at the one this release happened to ship with.
#
# The range is `<base>..<sha>`, and base is the version tag on this commit's own ancestry -- handed
# in rather than worked out again, because working it out twice is two chances to take the newest tag
# in the repository instead. On a fix to a line that has been overtaken those differ, and the notes
# for 0.5.1 would list everything 0.6.0 shipped.
#
# Two sections come out as an empty prompt rather than as content, and that is deliberate. Neither
# can be decided by a machine, and the record they would be filled from is in a private repository:
# fetching it would mean handing a public workflow a key to that repository, and what came back --
# decision numbers, document paths -- would sit in the release body's HTML comments, invisible on the
# page and plainly readable in the API response.
set -uo pipefail

version=""; base=""; sha=""; macos_req=""; glibc_req=""
while [ $# -gt 0 ]; do
  case "$1" in
    --version) version="${2:-}"; shift 2 ;;
    --base) base="${2:-}"; shift 2 ;;
    --sha) sha="${2:-}"; shift 2 ;;
    --macos-req) macos_req="${2:-}"; shift 2 ;;
    --glibc-req) glibc_req="${2:-}"; shift 2 ;;
    *) echo "unknown argument '$1'" >&2; exit 2 ;;
  esac
done
[ -n "$version" ] || { echo "--version is required" >&2; exit 2; }
[ -n "$base" ] || { echo "--base is required: the range this release covers starts at the version tag it counts up from" >&2; exit 2; }
[ -n "$sha" ] || { echo "--sha is required" >&2; exit 2; }

git rev-parse -q --verify "${base}^{commit}" >/dev/null 2>&1 || {
  echo "'${base}' is not a commit in this repository, so there is no range to collect notes over" >&2
  exit 1
}

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=.github/scripts/_pr-section.sh
. "$here/_pr-section.sh"

# Every pull request merged in the range, from the commit subjects. Both merge styles leave the
# number in the subject -- "title (#12)" when squashed, "Merge pull request #12 from ..." when not --
# so neither style has to be configured anywhere for this to find them.
numbers="$(git log --format='%s' "${base}..${sha}" 2>/dev/null \
  | grep -oE '#[0-9]+' | tr -d '#' | sort -un || true)"

news=""; fixes=""; other=""
for n in $numbers; do
  # A number in a subject is not always a pull request in this repository: it can be an issue, or
  # another repository's. Asking and being refused is the answer, not a failure.
  body="$(gh pr view "$n" --json body --jq '.body' 2>/dev/null)" || continue
  [ -n "$body" ] || continue
  note="$(section_body "Release note" "###")"
  [ -n "$note" ] || continue
  # The classification comes off first. It is a field inside the section, and the section is carried
  # whole into a bullet, so leaving it in glues "**Kind:** fix" onto the front of the sentence it was
  # only meant to file.
  kind="$(printf '%s' "$(field_value Kind "$note")" | tr '[:upper:]' '[:lower:]')"
  note="$(without_field Kind "$note")"
  [ -n "$note" ] || continue
  # "none" is a conclusion the author reached, with or without a reason after it, and it produces no
  # entry. Left as a bullet with nothing in it, it would read as a change nobody could describe.
  is_none "$note" && continue
  line="* $(printf '%s' "$note" | tr '\n' ' ' | sed 's/[ \t]*$//')
"
  case "$kind" in
    fix*) fixes="${fixes}${line}" ;;
    new*) news="${news}${line}" ;;
    # No classification at all: every pull request merged before the field existed, and any later
    # one whose author deleted it. Carried, and filed as neither -- reading an unclassified change
    # as a new capability is the one outcome worth refusing, because it is the half a reader
    # scanning for what broke would never think to check.
    *)    other="${other}${line}" ;;
  esac
done

# A heading only where there is something under it. An empty "Fixes" reads as a claim that this
# release fixed nothing, which is a different statement from not having said.
group() {   # <heading> <entries>
  [ -n "$2" ] || return 0
  printf '## %s\n\n%s\n' "$1" "$2"
}
grouped="$(group "What's new" "$news"; group "Fixes" "$fixes"; group "Other changes" "$other")"

cat <<EOF
Preview build — single-node, in-memory runtime, not for production.

Native CLI binaries for macOS (arm64 / x64) and Linux (x64 / arm64). Verify a download against
its \`.sha256\` or \`checksums.txt\` before use.

${macos_req} ${glibc_req} (\`sw_vers -productVersion\` and \`ldd --version\` report what a machine
has.) Both are measured from these binaries and follow the build machines, so they can move between
releases; an older system may refuse to launch them. They are recommendations, not gates — the
installer names them and carries on rather than blocking you. \`platform-minimums.txt\` carries the
same numbers in machine-readable form.

### Known limits in this preview

These are disclosed rather than fixed, and no shipped scenario covers either one. For a change
the connector has read, delivery is at-least-once — never exactly-once. The second limit below
is a change the connector never reads at all, which no delivery guarantee can cover.

* **Duplicate rows on an insert-only target.** Where the target does not upsert, the snapshot
  read and the change-data-capture read overlap, and a row seen by both is written twice.
* **Changes lost while the change stream positions itself.** The position is recorded before
  the snapshot runs and is not handed to the connector's stream read until afterwards, so a
  change written to the source inside that window can be missed.

<!-- Breaking changes: what somebody upgrading has to DO, not what we changed. Delete this
     section if this release breaks nothing. A rolling 0.x minor is allowed to break
     compatibility, which is exactly why its absence here cannot be assumed. -->

<!-- What's new: the harvested sentences are below. Promote anything worth reading out of the
     generated list at the bottom, rewrite it from the reader's seat, and leave that list
     alone — shortening it puts back the omissions this whole arrangement exists to prevent. -->

${grouped}
<!-- Known issues: paste the user-visible defects this release is knowingly shipping with, and
     delete this section if there are none. The list of them is not fetched for you and must not
     be: it lives outside this repository, and anything copied from it would name records that
     have no business in a public release body. Look it up where it is kept, and write only what
     a user of this build would recognise. -->

---

Which releases still receive fixes: [Supported versions](https://github.com/tapstate/tapstate/blob/HEAD/SECURITY.md#supported-versions).
EOF
