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
# A commit path with no sha in it is a malformed request, and the real API answers 404. The stub
# has to as well: answering it like any other commit is what let a guard against an empty sha go
# unwitnessed -- the case passed, and would have passed with the guard deleted.
case "$*" in
  *commits//*) exit 1 ;;
  *"$SMOKE_HEAD_SHA"*check-runs*) cat "$SMOKE_SCRATCH/check-runs-head" 2>/dev/null || true ;;
  *check-runs*) cat "$SMOKE_SCRATCH/check-runs" 2>/dev/null || true ;;
  *rules/branches*) cat "$SMOKE_SCRATCH/ruleset" 2>/dev/null || true ;;
  *pulls*) cat "$SMOKE_SCRATCH/pulls" 2>/dev/null || true ;;
  *"$SMOKE_HEAD_SHA"*) cat "$SMOKE_SCRATCH/tree-head" 2>/dev/null || true ;;
  *commits/*) cat "$SMOKE_SCRATCH/tree" 2>/dev/null || true ;;
  *) exit 1 ;;
esac
STUB
chmod +x "$scratch/bin/gh"
PATH="$scratch/bin:$PATH"
export PATH
export GITHUB_REPOSITORY=tapstate/tapstate

runs() { printf '%s\n' "$@" > "$scratch/check-runs"; }
ruleset() { printf '%s\n' "$@" > "$scratch/ruleset"; }
# The pull request a commit belongs to, and the two trees. Staged separately from the check-runs
# because the fallback has to be able to find a pull request and still refuse on what it finds.
head_sha=91c0e4a7
export SMOKE_HEAD_SHA="$head_sha"
runs_head() { printf '%s\n' "$@" > "$scratch/check-runs-head"; }
pulls() { printf '%s\n' "$@" > "$scratch/pulls"; }
trees() { printf '%s\n' "$1" > "$scratch/tree"; printf '%s\n' "$2" > "$scratch/tree-head"; }
reset() { rm -f "$scratch/check-runs" "$scratch/check-runs-head" "$scratch/ruleset" \
  "$scratch/pulls" "$scratch/tree" "$scratch/tree-head" "$scratch/gh-log"; echo ok > "$scratch/gh-mode"; }

# `--` before the needle: the two usage cases below look for text that starts with a dash, and
# without it grep reads the needle as one of its own flags and answers about something else.
expect() {
  local name="$1" want_code="$2" want_text="$3"; shift 3
  local out code
  out="$(bash "$gate" "$@" 2>&1)"
  code=$?
  if [ "$code" = "$want_code" ] && grep -qF -- "$want_text" <<<"$out"; then
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
  if grep -qF -- "$unwanted" <<<"$out"; then
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
expect "a check that never ran refuses"          3 "never ran"         --sha "$sha" --required build,no-cjk
expect "and it names the absent one"             3 "no-cjk"            --sha "$sha" --required build,no-cjk
# Narrower than "the word concluded does not appear": the summary line legitimately says nothing has
# concluded yet. What must not happen is the absent check being given one.
refute "absence is not reported as a conclusion"   "no-cjk' concluded" --sha "$sha" --required build,no-cjk

reset
runs $'build\tin_progress\t'
expect "a check still running is not green"      3 "still running"     --sha "$sha" --required build
refute "and that is not called never ran"          "never ran"         --sha "$sha" --required build

reset
runs $'build\tcompleted\tskipped'
expect "a skipped check is not green"            1 "concluded skipped" --sha "$sha" --required build

# 3 means "nothing has concluded yet", 1 means "something concluded and it was not success". A
# caller waiting for a lane loops on the first and stops on the second; folded together, a lane that
# already failed is polled until its deadline while the log says it is waiting. A commit carrying
# both a red check and an absent one has an answer, so it is 1.
reset
runs $'build\tcompleted\tfailure'
expect "one red and one absent is a verdict, not a wait" 1 "never ran" --sha "$sha" --required build,no-cjk
refute "and it does not claim nothing concluded"          "says nothing about" --sha "$sha" --required build,no-cjk

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
expect "a required check missing on the commit refuses" 3 "sonarqube" --sha "$sha" --from-ruleset main

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

# --- a required check that cannot run on this commit at all -----------------------------------------
# Two of the contexts the branch ruleset requires are `pull_request`-only workflows, so they never
# produce a check-run on a commit that sits on the default branch, and a release cut from one used to
# stop here forever. The answer is on the pull request whose merge produced this commit -- but only
# when that pull request's head carries the same tree, because then there is no combination of
# changes that the head was not itself checked with. A different tree is a different thing, and is
# refused exactly as before.

reset
runs $'build\tcompleted\tsuccess'
runs_head $'build\tcompleted\tsuccess' $'dco\tcompleted\tsuccess'
pulls "$head_sha"
trees deadbeef deadbeef
expect "a check absent here but green on the merged head passes" 0 "clean:" --sha "$sha" --required build,dco
expect "and it says which commit answered for it"                0 "$head_sha" --sha "$sha" --required build,dco
refute "and it is not reported as never having run"                "never ran" --sha "$sha" --required build,dco

# The guard. If the merge produced a tree the head never had, the head's green says nothing about
# what is being released, and the refusal has to come back.
reset
runs $'build\tcompleted\tsuccess'
runs_head $'build\tcompleted\tsuccess' $'dco\tcompleted\tsuccess'
pulls "$head_sha"
trees deadbeef 0ther777
expect "a head carrying a different tree does not answer"        3 "never ran" --sha "$sha" --required build,dco

reset
runs $'build\tcompleted\tsuccess'
trees deadbeef deadbeef
expect "no pull request for this commit still refuses"           3 "never ran" --sha "$sha" --required build,dco

reset
runs $'build\tcompleted\tsuccess'
runs_head $'build\tcompleted\tsuccess'
pulls "$head_sha"
trees deadbeef deadbeef
expect "absent on the head as well still refuses"                3 "never ran" --sha "$sha" --required build,dco

# Red on the head is a verdict, not an absence, and the two must not share an exit code.
reset
runs $'build\tcompleted\tsuccess'
runs_head $'build\tcompleted\tsuccess' $'dco\tcompleted\tfailure'
pulls "$head_sha"
trees deadbeef deadbeef
expect "red on the merged head is reported as red"               1 "concluded failure" --sha "$sha" --required build,dco

# `[ \t]` is a tab only as a GNU extension; inside a bracket expression BSD sed reads it as "space,
# backslash or the letter t" and eats the trailing `t` of a name. Every required context here whose
# name ends in one was then looked up under a name that does not exist and reported as never having
# run -- a fabricated refusal, on a maintainer's machine only.
#
# Say what this case does and does not catch: on a GNU runner it passes either way, so what it
# guards there is only the trimming being dropped altogether. On macOS it is the whole witness.
reset
ruleset '  no-agent-footprint  '
runs $'no-agent-footprint\tcompleted\tsuccess'
expect "a name is trimmed without losing its last letter" 0 "clean:" --sha "$sha" --from-ruleset main

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
