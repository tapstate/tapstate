#!/usr/bin/env bash
# Named checks ran on one commit, and were green.
#
#   checks-on-commit.sh --sha <sha> --required <name>[,<name>...]
#   checks-on-commit.sh --sha <sha> --from-ruleset <branch>
#
# "CI is green" has two readings that are far apart at the moment a release is cut: the last run was
# green, and this commit was checked. Only the second one says anything about what is about to ship,
# so this asks the commit.
#
# Three ways of getting that wrong are all in this repository's history, and each one is answered
# above rather than argued about:
#
#   - Asking the pull request or the branch returns the previous push's runs alongside the current
#     ones, the same job listed twice with different answers. The request here names the commit.
#   - A check that never ran leaves no record. Fetch the runs, filter for failures, count zero, and
#     "nothing failed" is what a lane that was never dispatched looks like. So the names are checked
#     for being present before any conclusion is looked at, and absence is refused in its own words.
#   - Writing the required set out here is a copy of a setting that lives elsewhere, and copies
#     diverge silently in the direction of passing. `--from-ruleset` reads it from the branch's rules
#     at the moment of the release, and refuses an empty answer rather than passing over one.
#
# An in-progress run is not green either. It is refused separately from a red one, and with a
# different exit code: 3 when everything wrong with the commit is still unsettled -- a check that has
# not started, or has not finished -- and 1 once at least one of them has concluded something other
# than success. A caller waiting for a lane to finish loops on 3 and stops on 1. Without the split it
# would keep polling a lane that already failed, for as long as its deadline allows, printing that it
# is waiting while the answer has been in for an hour.
set -uo pipefail

sha=""
required=""
branch=""

while [ $# -gt 0 ]; do
  case "$1" in
    --sha) sha="${2:-}"; shift 2 ;;
    --required) required="${2:-}"; shift 2 ;;
    --from-ruleset) branch="${2:-}"; shift 2 ;;
    *) echo "unknown argument '$1'" >&2; exit 2 ;;
  esac
done

repo="${GITHUB_REPOSITORY:-}"
[ -n "$repo" ] || { echo "GITHUB_REPOSITORY is not set - there is no repository to ask" >&2; exit 2; }
[ -n "$sha" ] || { echo "--sha is required: a release is checked at the commit it ships, not on a branch" >&2; exit 2; }
if [ -z "$required" ] && [ -z "$branch" ]; then
  echo "one of --required <names> or --from-ruleset <branch> is required" >&2
  exit 2
fi

names=""
if [ -n "$branch" ]; then
  # Every rule that applies to the branch, of which the interesting one carries the contexts. Read
  # through the rules endpoint rather than the classic protection one: protection here is a ruleset,
  # and the classic endpoint answers 404 for a branch that is in fact protected.
  if ! ruleset="$(gh api "repos/${repo}/rules/branches/${branch}" \
      --jq '[.[] | select(.type == "required_status_checks") | .parameters.required_status_checks[].context] | .[]' 2>&1)"; then
    echo "::error::could not read the branch rules for ${branch}: ${ruleset}" >&2
    exit 1
  fi
  names="$(printf '%s\n' "$ruleset" | sed 's/^[ \t]*//; s/[ \t]*$//' | grep -v '^$' || true)"
  if [ -z "$names" ]; then
    echo "::error::the rules on '${branch}' name no required status checks, so there is nothing to verify" >&2
    echo "That reads identically to a green release. Either the ruleset lost its checks, or this is asking the wrong branch." >&2
    exit 1
  fi
else
  names="$(printf '%s' "$required" | tr ',' '\n' | sed 's/^[ \t]*//; s/[ \t]*$//' | grep -v '^$' || true)"
fi

if ! observed="$(gh api --paginate "repos/${repo}/commits/${sha}/check-runs" \
    --jq '.check_runs[] | [.name, .status, .conclusion] | @tsv' 2>&1)"; then
  echo "::error::could not read the checks on ${sha}: ${observed}" >&2
  exit 1
fi

fail=0
unsettled=0
while IFS= read -r name; do
  [ -n "$name" ] || continue
  line="$(printf '%s\n' "$observed" | awk -F'\t' -v want="$name" '$1 == want { print; exit }')"
  if [ -z "$line" ]; then
    echo "::error::'${name}' never ran on ${sha} — a check that was not dispatched leaves no record, so this cannot be read off the failures"
    unsettled=1
    continue
  fi
  status="$(printf '%s' "$line" | cut -f2)"
  conclusion="$(printf '%s' "$line" | cut -f3)"
  if [ "$status" != "completed" ]; then
    echo "::error::'${name}' is still running on ${sha} (${status}) — this is not a failure to fix, it is a result to wait for"
    unsettled=1
  elif [ "$conclusion" != "success" ]; then
    echo "::error::'${name}' concluded ${conclusion} on ${sha}"
    fail=1
  fi
done <<EOF
$names
EOF

if [ "$fail" -ne 0 ]; then
  echo "Nothing was released. Fix or re-run what is named above against ${sha}; a release is built from a commit that was checked, not from one that was checked once."
  exit 1
fi
if [ "$unsettled" -ne 0 ]; then
  echo "Nothing above has concluded anything yet, so this says nothing about ${sha} either way."
  exit 3
fi

echo "clean: $(printf '%s\n' "$names" | wc -l | tr -d ' ') named check(s) ran on ${sha} and are green."
