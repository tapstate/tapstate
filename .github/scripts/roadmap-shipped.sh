#!/usr/bin/env bash
# What this release shipped, marked Shipped on the roadmap board.
#
#   roadmap-shipped.sh --version <v> --base <tag> --sha <commit> [--dry-run]
#
# The board is written, not the issues. By the time a release goes out the execution issue for the
# work in it is already closed -- closing out a line happens when its pull request merges, which is
# before the release that carries it -- so "it shipped in 0.4.0" has nowhere to live except a field.
# Two fields, together: a status alone leaves a roadmap that says something shipped without saying
# in which release, and under rolling minors that is the one thing a reader wants.
#
# What went out is read from the range, the same way the notes are: every pull request between the
# two commits, and in each one the `Refs #N` its author wrote. A pull request with no `Refs` line is
# not on the roadmap and is not meant to be -- a drive-by fix, an outside contribution -- so it is
# passed over in silence rather than reported as something missing.
#
# Not every `Refs #N` names an issue. A release pull request can accurately reference the pull
# request before it, and that number resolves to no issue at all -- so it is reported as passed
# over rather than failed, because the board holds work items and a red roadmap is meant to mean
# something needs looking at.
#
# Items are added rather than expected: an issue nobody put on the board is still work this release
# shipped, and adding it is how the board learns about it. `addProjectV2ItemById` returns the
# existing item when there already is one, so this is safe to run twice over the same release.
#
# Every refusal names the piece that is missing. This step is `continue-on-error` in the workflow --
# a roadmap a version behind is worth less than a release held back for it -- and a step that failed
# without saying which of the board, the field, the option or the credential was the problem would
# be reported once and then ignored on every release after.
set -uo pipefail

owner="${PROJECT_OWNER:-tapstate}"
number="${PROJECT_NUMBER:-1}"
status_field="${PROJECT_STATUS_FIELD:-Status}"
status_option="${PROJECT_SHIPPED_OPTION:-Shipped}"
released_field="${PROJECT_RELEASED_FIELD:-Released in}"

version=""; base=""; sha=""; dry=0
while [ $# -gt 0 ]; do
  case "$1" in
    --version) version="${2:-}"; shift 2 ;;
    --base) base="${2:-}"; shift 2 ;;
    --sha) sha="${2:-}"; shift 2 ;;
    --dry-run) dry=1; shift ;;
    *) echo "unknown argument '$1'" >&2; exit 2 ;;
  esac
done
[ -n "$version" ] || { echo "--version is required: the release whose name goes into the board" >&2; exit 2; }
[ -n "$base" ] || { echo "--base is required: the range starts at the version tag this release counts up from" >&2; exit 2; }
[ -n "$sha" ] || { echo "--sha is required" >&2; exit 2; }

repo="${GITHUB_REPOSITORY:-}"
[ -n "$repo" ] || { echo "GITHUB_REPOSITORY is not set - there is no repository whose issues these are" >&2; exit 2; }
repo_owner="${repo%%/*}"; repo_name="${repo##*/}"

git rev-parse -q --verify "${base}^{commit}" >/dev/null 2>&1 || {
  echo "'${base}' is not a commit in this repository, so there is no range to read" >&2
  exit 1
}

# The pull requests in the range, then the execution issue each one says it is part of.
issues=""
for n in $(git log --format='%s' "${base}..${sha}" 2>/dev/null | grep -oE '#[0-9]+' | tr -d '#' | sort -un); do
  body="$(gh pr view "$n" --json body --jq '.body' 2>/dev/null)" || continue
  refs="$(printf '%s' "$body" | grep -oiE '(^|[^a-z])Refs[[:space:]]+#[0-9]+' | grep -oE '[0-9]+')"
  [ -n "$refs" ] || continue
  issues="${issues}${refs}
"
done
issues="$(printf '%s' "$issues" | grep -E '^[0-9]+$' | sort -un)"

if [ -z "$issues" ]; then
  echo "No pull request in ${base}..${sha} carries a 'Refs #N' line, so this release moves nothing on the board."
  exit 0
fi

# shellcheck disable=SC2016  # the $ names are GraphQL variables, bound by the -f flags above
board="$(gh api graphql -f owner="$owner" -F number="$number" -f query='
  query($owner:String!, $number:Int!) {
    organization(login:$owner) { projectV2(number:$number) {
      id
      fields(first:50) { nodes {
        ... on ProjectV2Field { id name }
        ... on ProjectV2SingleSelectField { id name options { id name } }
      } } } } }' 2>&1)"
project_id="$(printf '%s' "$board" | jq -r '.data.organization.projectV2.id // empty' 2>/dev/null)"
if [ -z "$project_id" ]; then
  echo "Cannot read project ${owner}/#${number}. Either the credential has no access to organization projects, or the board is not there:" >&2
  printf '%s\n' "$board" >&2
  exit 1
fi

field_id() { printf '%s' "$board" | jq -r --arg n "$1" '.data.organization.projectV2.fields.nodes[] | select(.name == $n) | .id // empty'; }
status_id="$(field_id "$status_field")"
released_id="$(field_id "$released_field")"
shipped_id="$(printf '%s' "$board" | jq -r --arg f "$status_field" --arg o "$status_option" \
  '.data.organization.projectV2.fields.nodes[] | select(.name == $f) | .options[]? | select(.name == $o) | .id // empty')"

missing=""
add_missing() { if [ -z "$missing" ]; then missing="$1"; else missing="${missing}, $1"; fi; }
[ -n "$status_id" ] || add_missing "a '${status_field}' field"
[ -n "$shipped_id" ] || add_missing "a '${status_option}' option on '${status_field}'"
[ -n "$released_id" ] || add_missing "a '${released_field}' field"
if [ -n "$missing" ]; then
  echo "Project ${owner}/#${number} is missing: ${missing}. Add them on the board; nothing here creates them, because a field this invents would be one no view is built on." >&2
  exit 1
fi

set_field() {   # item, field id, the value expression jq built
  gh api graphql -f project="$project_id" -f item="$1" -f field="$2" -f value="$3" -f query="
    mutation(\$project:ID!, \$item:ID!, \$field:ID!, \$value:String!) {
      updateProjectV2ItemFieldValue(input:{projectId:\$project, itemId:\$item, fieldId:\$field, value:{${4}:\$value}}) {
        projectV2Item { id } } }" >/dev/null
}

# What the board says about one item now. The mutations report on the request, not on the state that
# survives it: a project workflow triggered by an item being added lands after them and leaves the
# item at its own value, with nothing in either response to say so.
# shellcheck disable=SC2016  # the $ names are GraphQL variables, bound by the -f flags
read_back() {   # item -> the status on line 1, the released-in text on line 2
  gh api graphql -f item="$1" -f query='
    query($item:ID!) { node(id:$item) { ... on ProjectV2Item { fieldValues(first:50) { nodes {
      ... on ProjectV2ItemFieldSingleSelectValue { name field { ... on ProjectV2SingleSelectField { name } } }
      ... on ProjectV2ItemFieldTextValue { text field { ... on ProjectV2Field { name } } } } } } } }' 2>/dev/null \
    | jq -r --arg s "$status_field" --arg r "$released_field" '
        (.data.node.fieldValues.nodes // []) | map(select(.field.name != null))
        | ((map(select(.field.name == $s)) | first | .name) // ""),
          ((map(select(.field.name == $r)) | first | .text) // "")'
}

failed=0
for i in $issues; do
  # gh writes the response body to stdout even when the query errored, so a lookup that found
  # nothing does not come back empty -- it comes back as the NOT_FOUND body, which is what the
  # guard below is reading. Take the exit status: without it that body went on as a node id, and
  # every mutation after reported on a global id that was a blob of JSON.
  # shellcheck disable=SC2016  # GraphQL variables again
  node="$(gh api graphql -f owner="$repo_owner" -f repo="$repo_name" -F number="$i" -f query='
    query($owner:String!, $repo:String!, $number:Int!) {
      repository(owner:$owner, name:$repo) { issue(number:$number) { id title } } }' \
    --jq '.data.repository.issue.id' 2>/dev/null)" || node=""
  if [ -z "$node" ]; then
    # A pull request resolves to no issue here, and the reference that named it is honest when it
    # does -- a release pull request saying `Refs #N` about the one that wrote the version pins.
    # The board is a list of work items, so this is passed over: out loud, because a step silent
    # about what it skipped is one nobody can check, and not red, because a red roadmap is supposed
    # to mean something needs looking at and this needs none.
    if gh pr view "$i" --json number >/dev/null 2>&1; then
      echo "  #${i}: a pull request, not an issue -- passed over, the board holds work items"
      continue
    fi
    # Two readings, and this cannot tell them apart: the number is wrong, or the credential cannot
    # read issues at all. Passing over it in silence made the second one report success having moved
    # nothing -- the exact shape a roadmap step must not have, since nobody reads a green job.
    echo "  #${i}: not an issue in ${repo} -- either the number is wrong, or this credential cannot read issues" >&2
    failed=1
    continue
  fi
  if [ "$dry" = 1 ]; then
    echo "  #${i}: would be set to ${status_option}, ${released_field} = ${version}"
    continue
  fi
  # shellcheck disable=SC2016  # GraphQL variables again
  item="$(gh api graphql -f project="$project_id" -f content="$node" -f query='
    mutation($project:ID!, $content:ID!) {
      addProjectV2ItemById(input:{projectId:$project, contentId:$content}) { item { id } } }' \
    --jq '.data.addProjectV2ItemById.item.id' 2>/dev/null)" || item=""   # the error body is on stdout here too
  if [ -z "$item" ]; then
    echo "  #${i}: could not be put on the board" >&2
    failed=1
    continue
  fi
  set_field "$item" "$status_id" "$shipped_id" singleSelectOptionId
  set_field "$item" "$released_id" "$version" text
  # Asked, not assumed. Measured on the first real release: both mutations answered success for
  # fifteen issues and the board kept seven of them -- the workflow fires when the item is added, so
  # it lands after the write and the response carries no sign of it. Writing again wins because that
  # trigger does not fire twice; three looks bound it, and a value that still will not hold is a
  # failure rather than a line claiming it moved.
  status_now=""; released_now=""; attempt=0
  while [ "$attempt" -lt 3 ]; do
    attempt=$((attempt + 1))
    board_now="$(read_back "$item")"
    status_now="$(printf '%s\n' "$board_now" | sed -n 1p)"
    released_now="$(printf '%s\n' "$board_now" | sed -n 2p)"
    if [ "$status_now" = "$status_option" ] && [ "$released_now" = "$version" ]; then break; fi
    [ "$attempt" -lt 3 ] || break
    set_field "$item" "$status_id" "$shipped_id" singleSelectOptionId
    set_field "$item" "$released_id" "$version" text
  done
  if [ "$status_now" = "$status_option" ] && [ "$released_now" = "$version" ]; then
    echo "  #${i}: ${status_option}, ${released_field} = ${version}"
  else
    echo "  #${i}: the board says '${status_now:-<unset>}' / '${released_now:-<unset>}' after ${attempt} attempt(s), not '${status_option}' / '${version}'" >&2
    failed=1
  fi
done

exit "$failed"
