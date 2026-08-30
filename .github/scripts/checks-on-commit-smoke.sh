#!/usr/bin/env bash
# Cases for "these checks ran on this commit, and they were green", driven against a stub `gh`.
#
# The case this exists for is the second one: a check that never ran leaves no record at all, so a
# gate that fetches the check-runs for a commit and counts the failures among them reads "nothing
# failed" off a lane that was never dispatched. Both of the checks this drives -- the required set
# on the release commit, and the real-connector lane -- can be absent exactly that way, and absence
# is the likelier of the two failures for both of them. So every "it is red" case here is paired
# with an "it is not there" case, and the two must not be reported in the same words.
#
# Run it from anywhere. Exits 0 if every case holds.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/checks-on-commit.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
export SMOKE_SCRATCH="$scratch"
mkdir -p "$scratch/bin"
passed=0
failed=0

# `gh` answers from whatever the case staged, chosen by which endpoint was asked for, and records
# that it was asked. A staged file that is absent is an API that answered nothing, which is a state
# both endpoints really have.
cat > "$scratch/bin/gh" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$SMOKE_SCRATCH/gh-log"
[ "$(cat "$SMOKE_SCRATCH/gh-mode" 2>/dev/null || echo ok)" = fail ] && exit 1
case "$*" in
  *check-runs*) cat "$SMOKE_SCRATCH/check-runs" 2>/dev/null || true ;;
  *rules/branches*) cat "$SMOKE_SCRATCH/ruleset" 2>/dev/null || true ;;
  *) exit 1 ;;
esac
STUB
chmod +x "$scratch/bin/gh"
PATH="$scratch/bin:$PATH"
export PATH
export GITHUB_REPOSITORY=tapstate/tapstate

runs() { printf '%s\n' "$@" > "$scratch/check-runs"; }
ruleset() { printf '%s\n' "$@" > "$scratch/ruleset"; }
reset() { rm -f "$scratch/check-runs" "$scratch/ruleset" "$scratch/gh-log"; echo ok > "$scratch/gh-mode"; }

# `--` before the needle: the two usage cases below look for text that starts with a dash, and
# without it grep reads the needle as one of its own flags and answers about something else.
expect() {
  local name="$1" want_code="$2" want_text="$3"; shift 3
  local out code
  out="$(bash "$gate" "$@" 2>&1)"
  code=$?
  if [ "$code" = "$want_code" ] && printf '%s' "$out" | grep -qF -- "$want_text"; then
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        wanted exit %s containing %s\n        got exit %s: %s\n' \
      "$name" "$want_code" "$want_text" "$code" "$out"
    failed=$((failed + 1))
  fi
}

refute() {
  local name="$1" unwanted="$2"; shift 2
  local out
  out="$(bash "$gate" "$@" 2>&1)"
  if printf '%s' "$out" | grep -qF -- "$unwanted"; then
    printf '  FAIL  %s\n        did not want %s, got: %s\n' "$name" "$unwanted" "$out"
    failed=$((failed + 1))
  else
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  fi
}

sha=346afd52

# --- the named set --------------------------------------------------------------------------------
reset
runs $'build\tcompleted\tsuccess' $'no-cjk\tcompleted\tsuccess'
expect "every named check present and green"     0 "clean:" --sha "$sha" --required build,no-cjk

reset
runs $'build\tcompleted\tfailure' $'no-cjk\tcompleted\tsuccess'
expect "a red check refuses"                     1 "concluded failure" --sha "$sha" --required build,no-cjk
expect "and it names the red one"                1 "build"             --sha "$sha" --required build,no-cjk
refute "and does not call the green one absent"    "no-cjk never ran"  --sha "$sha" --required build,no-cjk

# The whole reason this script exists rather than a `--jq 'select(.conclusion != "success")' | wc -l`.
reset
runs $'build\tcompleted\tsuccess'
expect "a check that never ran refuses"          1 "never ran"         --sha "$sha" --required build,no-cjk
expect "and it names the absent one"             1 "no-cjk"            --sha "$sha" --required build,no-cjk
refute "absence is not reported as a conclusion"   "concluded"         --sha "$sha" --required build,no-cjk

reset
runs $'build\tin_progress\t'
expect "a check still running is not green"      1 "still running"     --sha "$sha" --required build
refute "and that is not called never ran"          "never ran"         --sha "$sha" --required build

reset
runs $'build\tcompleted\tskipped'
expect "a skipped check is not green"            1 "concluded skipped" --sha "$sha" --required build

# The commit is asked for by SHA, never the branch or the pull request: a branch-wide listing carries
# the previous push's runs, so the same job name appears twice and the stale one can be the green.
reset
runs $'build\tcompleted\tsuccess'
expect "the commit is asked for by its sha"      0 "clean:" --sha "$sha" --required build
if grep -q "commits/$sha/check-runs" "$scratch/gh-log"; then
  printf '  ok    %s\n' "and the request names that sha"; passed=$((passed + 1))
else
  printf '  FAIL  %s\n        gh was called as: %s\n' "and the request names that sha" "$(cat "$scratch/gh-log")"; failed=$((failed + 1))
fi

# --- the set read from the branch ruleset ----------------------------------------------------------
reset
ruleset $'build\nno-cjk'
runs $'build\tcompleted\tsuccess' $'no-cjk\tcompleted\tsuccess' $'extra\tcompleted\tfailure'
expect "the required set comes from the ruleset" 0 "clean:" --sha "$sha" --from-ruleset main
refute "a check outside the required set is not judged" "extra" --sha "$sha" --from-ruleset main

reset
ruleset $'build\nsonarqube'
runs $'build\tcompleted\tsuccess'
expect "a required check missing on the commit refuses" 1 "sonarqube" --sha "$sha" --from-ruleset main

# An empty required set is the failure this repository has already had once, in another gate: an
# input nobody produces reads exactly like a check that looked and found nothing wrong.
reset
ruleset ''
runs $'build\tcompleted\tsuccess'
expect "an empty ruleset refuses rather than passing" 1 "no required status checks" --sha "$sha" --from-ruleset main

reset
echo fail > "$scratch/gh-mode"
expect "an unreadable ruleset refuses"           1 "could not read" --sha "$sha" --from-ruleset main

reset
echo fail > "$scratch/gh-mode"
expect "unreadable check-runs refuse"            1 "could not read" --sha "$sha" --required build

reset
runs $'build\tcompleted\tsuccess'
# Exit 2, not 1, and the difference is worth pinning: 1 is a verdict about the commit, 2 is this
# script having been called wrong. Both fail the step that runs it, so nothing ships either way --
# but a release blocked because a gate was miswired must not read as a release blocked by a red CI.
expect "a missing --sha is a usage error"        2 "--sha"      --required build
expect "neither source of names is a usage error" 2 "--required" --sha "$sha"

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
