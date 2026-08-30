#!/usr/bin/env bash
# Every pull request in a release range answered its documentation impact, and the ones that said
# documentation was needed have it.
#
#   docs-gate.sh --base <tag> --sha <commit> --bump <major|minor|patch>
#
# The first half is the same judgement the pull-request check makes, made again over a whole range.
# It is asked twice on purpose: the check at pull-request time is what makes the answer exist, and
# this is what makes it count. A body cannot be repaired here -- by now every one of these pull
# requests is merged -- so this reads a record rather than asking for one, which is why a range
# containing a pull request that predates the check refuses instead of passing.
#
# The second half exists only at release time: for a pull request labelled `docs-needed`, the
# follow-up issue that its merge opened in the documentation repository. Two strictnesses, taken from
# this release's own bump rather than from anything about the pull request:
#
#   minor, major   the issue must be closed  -- a release that adds something a user can see ships
#                                               with the page that explains it
#   patch          the issue must exist      -- a compatible fix is not held up for documentation,
#                                               but it does not get to lose the follow-up either
#
# The axis is the bump because the bump is already the one semantic thing a person states when
# dispatching a release, and it is what the documentation policy is actually written in terms of.
set -uo pipefail

base=""; sha=""; bump=""
while [ $# -gt 0 ]; do
  case "$1" in
    --base) base="${2:-}"; shift 2 ;;
    --sha) sha="${2:-}"; shift 2 ;;
    --bump) bump="${2:-}"; shift 2 ;;
    *) echo "unknown argument '$1'" >&2; exit 2 ;;
  esac
done
[ -n "$base" ] || { echo "--base is required" >&2; exit 2; }
[ -n "$sha" ] || { echo "--sha is required" >&2; exit 2; }
case "$bump" in
  major|minor|patch) ;;
  *) echo "--bump must be major, minor or patch - got '${bump}'" >&2; exit 2 ;;
esac

here="$(cd "$(dirname "$0")" && pwd)"
docs_repo="${DOCS_FOLLOWUP_REPO:-tapstate/docs}"

git rev-parse -q --verify "${base}^{commit}" >/dev/null 2>&1 || {
  echo "'${base}' is not a commit in this repository, so there is no range to check" >&2
  exit 1
}

numbers="$(git log --format='%s' "${base}..${sha}" 2>/dev/null \
  | grep -oE '#[0-9]+' | tr -d '#' | sort -un || true)"
if [ -z "$numbers" ]; then
  echo "clean: the range ${base}..${sha} contains no pull requests to check."
  exit 0
fi

fail=0
seen=0
for n in $numbers; do
  # A number in a commit subject can be an issue, or another repository's pull request. Being
  # refused is the answer; it is not this gate's business and never was.
  pr="$(gh pr view "$n" --json body,labels,author,url 2>/dev/null)" || continue
  [ -n "$pr" ] || continue
  seen=$((seen + 1))

  body="$(printf '%s' "$pr" | jq -r '.body // ""')"
  labels="$(printf '%s' "$pr" | jq -r '[.labels[].name] | join(",")')"
  actor="$(printf '%s' "$pr" | jq -r '.author.login // ""')"
  url="$(printf '%s' "$pr" | jq -r '.url // ""')"

  if ! verdict="$(PR_BODY="$body" PR_LABELS="$labels" PR_ACTOR="$actor" bash "$here/docs-impact.sh" 2>&1)"; then
    echo "::error::#${n} did not answer its documentation impact, and by now nobody can fix it there — ${url}"
    printf '%s\n' "$verdict" | sed 's/^::error:://; s/^/    /'
    fail=1
    continue
  fi

  printf '%s' "$labels" | tr ',' '\n' | grep -qx docs-needed || continue

  # The issue `docs-followup.yml` opens on merge, found by the link back to this pull request that it
  # writes into the body. Looked up by that link rather than by title: a title is edited.
  if ! issue="$(gh issue list --repo "$docs_repo" --state all --search "$url" --json number,state 2>&1)"; then
    # Not the same thing as finding no issue, and reported as itself. The credential that can read
    # that repository is a separate one; without it this call fails, and folded into "no follow-up
    # issue" it would send somebody to open an issue that is already there.
    echo "::error::could not read ${docs_repo} to look for #${n}'s follow-up issue: ${issue}"
    echo "    That is a missing credential, not a missing issue. Nothing here says whether the documentation exists."
    fail=1
    continue
  fi
  number="$(printf '%s' "${issue:-[]}" | jq -r 'first(.[].number) // empty' 2>/dev/null)"
  if [ -z "$number" ]; then
    echo "::error::#${n} carries \`docs-needed\` and has no follow-up issue in ${docs_repo} — the label is what opens it, so the documentation owner was never told — ${url}"
    fail=1
    continue
  fi
  state="$(printf '%s' "$issue" | jq -r 'first(.[].state) // empty' | tr '[:lower:]' '[:upper:]')"
  if [ "$bump" != "patch" ] && [ "$state" != "CLOSED" ]; then
    echo "::error::#${n}'s follow-up ${docs_repo}#${number} is still open, and a ${bump} release ships with the pages that explain what it added — ${url}"
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "Nothing was released. Each pull request named above is already merged, so the fix is in the documentation repository or in the label, not in the pull request body."
  exit 1
fi

echo "clean: ${seen} pull request(s) in ${base}..${sha} answered their documentation impact, and every follow-up a ${bump} needs is in place."
