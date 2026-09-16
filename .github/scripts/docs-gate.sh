#!/usr/bin/env bash
# Every pull request in a release range answered its documentation impact, and the ones that said
# documentation was needed have it.
#
#   docs-gate.sh --base <tag> --sha <commit> --bump <major|minor|patch> [--list-open <path>]
#   docs-gate.sh --base <tag> --sha <commit> --report-only --list-open <path>
#
# The first half is the same judgement the pull-request check makes, made again over a whole range.
# It is asked twice on purpose: the check at pull-request time is what makes the answer exist, and
# this is what makes it count. Every pull request it reads is merged by now, which does not make
# them unfixable -- a merged body and its labels are both still editable -- but it does mean nobody
# is looking any more, which is why the check at pull-request time exists and why a range containing
# a pull request that predates it refuses rather than passing.
#
# The second half exists only at release time: for a pull request labelled `docs-needed`, the
# follow-up issue that its merge opened in the documentation repository. Two questions, and only one
# of them waits on somebody outside this repository:
#
#   every bump     the issue must exist      -- absence means the label never reached anybody. That
#                                               is this repository's automation failing, not another
#                                               person's morning, so it is a refusal at every bump
#   major          the issue must be closed  -- the release that changes what we promise does not go
#                                               out ahead of the pages explaining it
#   minor, patch   open is allowed           -- the release is not held. The still-open ones are
#                                               carried into the request this release already makes
#                                               of the documentation site, so the person who owes
#                                               them is told which ones, in an issue already in
#                                               front of them
#
# The axis is the bump because the bump is already the one semantic thing a person states when
# dispatching a release. The line sits at major because everything else a release needs is machine-
# checkable and refuses when it is not satisfied, while this one waits on a person: refusing on it
# makes the whole train hostage to one calendar, which is the same reason the documentation site is
# coordinated through an issue rather than pinned like a satellite.
#
# `--list-open <path>` writes the still-open ones for whoever asks next, as `<pr>TAB<issue>TAB<url>`.
# `--report-only` is the same scan reaching no verdict, for the end of a release: what was open when
# it started is not what is open when it publishes.
set -uo pipefail

base=""; sha=""; bump=""; list_open=""; report_only=0
while [ $# -gt 0 ]; do
  case "$1" in
    --base) base="${2:-}"; shift 2 ;;
    --sha) sha="${2:-}"; shift 2 ;;
    --bump) bump="${2:-}"; shift 2 ;;
    --list-open) list_open="${2:-}"; shift 2 ;;
    --report-only) report_only=1; shift ;;
    *) echo "unknown argument '$1'" >&2; exit 2 ;;
  esac
done
[ -n "$base" ] || { echo "--base is required" >&2; exit 2; }
[ -n "$sha" ] || { echo "--sha is required" >&2; exit 2; }
# The two modes refuse each other rather than one being a quieter setting of the other. A verdict
# comes from a bump and from nothing else, so `--report-only --bump minor` is not a softer gate: it
# is two instructions that contradict. Written as an ordinary flag, `--report-only` would be exactly
# the shape this repository does not give a gate -- one argument, added to the release workflow by
# anybody, that makes it reach no verdict while still looking like it ran.
if [ "$report_only" = 1 ]; then
  [ -z "$bump" ] || { echo "--report-only reaches no verdict, so it refuses --bump" >&2; exit 2; }
  [ -n "$list_open" ] || { echo "--report-only needs --list-open <path>; listing is all it does" >&2; exit 2; }
else
  case "$bump" in
    major|minor|patch) ;;
    *) echo "--bump must be major, minor or patch - got '${bump}'" >&2; exit 2 ;;
  esac
fi
# Truncated here rather than appended to. A file an earlier step left behind reads exactly like this
# run's answer, and what reads it is an issue somebody is asked to act on.
if [ -n "$list_open" ] && ! : > "$list_open"; then
  echo "--list-open: cannot write ${list_open}" >&2; exit 2
fi

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
carried=0
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

  # Not asked under --report-only: that mode reaches no verdict on anything, and what it is after is
  # the follow-ups. A pull request that never answered its section still carries whatever label it
  # carries, and dropping it from the list would hide the one most likely to need a page.
  if [ "$report_only" = 0 ] && ! verdict="$(PR_BODY="$body" PR_LABELS="$labels" PR_ACTOR="$actor" bash "$here/docs-impact.sh" 2>&1)"; then
    echo "::error::#${n} did not answer its documentation impact — ${url}"
    printf '%s\n' "$verdict" | sed 's/^::error:://; s/^/    /'
    fail=1
    continue
  fi

  # Matched against a built list rather than piped into `grep -q`: grep exiting on the first match
  # kills the writer behind it, and under `pipefail` the pipeline reports that instead of the match.
  grep -qx docs-needed <<<"$(printf '%s\n' "$labels" | tr ',' '\n')" || continue

  # The issue `docs-followup.yml` opens on merge, found by the link back to this pull request that it
  # writes into the body. Looked up by that link rather than by title: a title is edited.
  if ! issue="$(gh issue list --repo "$docs_repo" --state all --search "$url" --json number,state,body 2>&1)"; then
    if [ "$report_only" = 1 ]; then
      # This mode runs after the release is already out, and the most it can cost is a line missing
      # from an issue. Refusing here would redden a job whose remaining steps are what open that
      # issue at all.
      echo "could not read ${docs_repo} while looking for #${n}'s follow-up: ${issue}"
      continue
    fi
    # Not the same thing as finding no issue, and reported as itself. The credential that can read
    # that repository is a separate one; without it this call fails, and folded into "no follow-up
    # issue" it would send somebody to open an issue that is already there.
    echo "::error::could not read ${docs_repo} to look for #${n}'s follow-up issue: ${issue}"
    echo "    That is a missing credential, not a missing issue. Nothing here says whether the documentation exists."
    fail=1
    continue
  fi
  # `--search` is a tokenized full-text search, so the answer is every issue that *mentions* this
  # number anywhere -- another follow-up whose prose refers back to this pull request, a body
  # quoting a `SHA-256` digest against pull request 256 -- ranked by relevance, which is not the
  # same question as "which issue links here". Taking the first one reads a stranger's state, and
  # the expensive direction is a closed stranger ranked above the real follow-up: the release then
  # hears "the documentation has landed" for pages nobody wrote. So keep only the issues whose body
  # actually carries this pull request's URL. The character after the link is checked because
  # `/pull/23` is a prefix of `/pull/231`.
  linked="$(printf '%s' "${issue:-[]}" | jq -c --arg u "$url" \
              'map(select((.body // "") | split($u) | .[1:] | map(.[0:1] | test("^[0-9]") | not) | any))' 2>/dev/null)"
  number="$(printf '%s' "${linked:-[]}" | jq -r 'first(.[].number) // empty' 2>/dev/null)"
  if [ -z "$number" ]; then
    if [ "$report_only" = 1 ]; then
      echo "#${n} carries \`docs-needed\` and nothing in ${docs_repo} links back to it"
      continue
    fi
    echo "::error::#${n} carries \`docs-needed\` and has no follow-up issue in ${docs_repo} — the label is what opens it, so the documentation owner was never told — ${url}"
    fail=1
    continue
  fi
  # Several issues can link one pull request, and the documentation is written when every one of
  # them is closed -- so the open one is what gets named, not whichever came back first.
  open_issue="$(printf '%s' "${linked:-[]}" | jq -r '[.[] | select((.state // "" | ascii_upcase) != "CLOSED") | .number] | first // empty' 2>/dev/null)"
  [ -n "$open_issue" ] || continue
  carried=$((carried + 1))
  # Written before the verdict, and written whatever the verdict is. What the list is for is the
  # bumps that do not refuse: the page is owed either way, and the only thing that changes is whether
  # anybody is stopped over it.
  [ -z "$list_open" ] || printf '%s\t%s\t%s\n' "$n" "$open_issue" "$url" >> "$list_open"
  if [ "$report_only" = 0 ] && [ "$bump" = "major" ]; then
    echo "::error::#${n}'s follow-up ${docs_repo}#${open_issue} is still open, and a ${bump} release ships with the pages that explain what it added — ${url}"
    fail=1
  fi
done

if [ "$report_only" = 1 ]; then
  echo "report-only: ${carried} unfinished follow-up(s) across ${seen} pull request(s) in ${base}..${sha}. No verdict was reached."
  exit 0
fi

if [ "$fail" -ne 0 ]; then
  echo "Nothing was released. Each one named above is merged, but a merged pull request's body and labels are both still editable, and that is where these are answered."
  exit 1
fi

# Said plainly, not as a warning. A ${bump} is not held for these, and a run that shouts about
# something it has decided not to stop for teaches whoever reads it to skip the lines that do stop it.
[ "$carried" = 0 ] || echo "${carried} follow-up(s) are still open. A ${bump} release does not wait for them; they go into the request this release makes of the documentation site, where the person who owes them will read them."
echo "clean: ${seen} pull request(s) in ${base}..${sha} answered their documentation impact, and every follow-up a ${bump} needs is in place."
