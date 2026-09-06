#!/usr/bin/env bash
# Cases for the witness gate's own decision, driven against a scratch tree shaped like each one.
#
# The gate exists because a build's aggregate counts cannot carry absence, and it is itself a shell
# script that nothing else checks. Each case below is a way for it to go green over a release that
# verified less than it claims: a manifest quietly narrowed to drop an inconvenient scenario, a line
# left behind after its example was deleted, a witness that skipped or aborted while the build stayed
# green. Every one of those was measured against the real gate before being written down here.
#
# Both halves of every answer are checked - the exit code and the reason given. A gate that refuses
# for the wrong reason is not refusing, it is coincidentally red.
#
# Run it from anywhere. Exits 0 if every case holds.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/witness-gate.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
passed=0
failed=0

# A tree the gate is happy with: two published examples, a manifest naming both on both tiers, and a
# ledger recording all four as executed and passed. Every case differs from it by exactly one thing.
fresh_tree() {
  rm -rf "${scratch:?}/tree"
  mkdir -p "$scratch/tree/e2e/examples/first-example" "$scratch/tree/e2e/examples/second-example" \
      "$scratch/tree/e2e/target"
  cd "$scratch/tree" || exit 1
  cat > e2e/witness-manifest.txt <<'MANIFEST'
# A comment, and a blank line below, so the gate is seen to ignore both.

first-example on IN_PROCESS
first-example on REAL_PROCESS
second-example on IN_PROCESS
second-example on REAL_PROCESS
MANIFEST
  cat > e2e/target/witness-ledger.txt <<'LEDGER'
first-example on IN_PROCESS
first-example on REAL_PROCESS
second-example on IN_PROCESS
second-example on REAL_PROCESS
LEDGER
}

# Runs the gate over whatever the tree now holds, and checks the exit code and the reason together.
expect() {
  local name="$1" want_code="$2" want_text="$3" arg="${4:-}"
  local out code
  # Branching rather than an unquoted expansion: the argument is optional, and passing an empty one
  # is not the same call as passing none - the gate reads "" as "no argument" but a word-split empty
  # variable would still be a word here on some shells.
  if [ -n "$arg" ]; then
    out="$(bash "$gate" "$arg" 2>&1)"
  else
    out="$(bash "$gate" 2>&1)"
  fi
  code=$?
  if [ "$code" = "$want_code" ] && grep -qF "$want_text" <<<"$out"; then
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  else
    printf '  FAIL  %s: wanted exit %s containing "%s", got exit %s:\n' \
      "$name" "$want_code" "$want_text" "$code"
    printf '%s\n' "$out" | sed 's/^/        /'
    failed=$((failed + 1))
  fi
}

echo "witness-gate cases:"

fresh_tree
expect "an agreeing tree passes" 0 "all 4 witnesses executed and passed"

fresh_tree
expect "an agreeing tree passes the manifest-only check" 0 "agree on all 4 witnesses" --manifest-only

# The narrowing this gate exists to refuse: drop a scenario from the contract, leave its example
# published, and every count in the build stays exactly as green as it was.
fresh_tree
grep -v '^second-example on REAL_PROCESS$' e2e/witness-manifest.txt > e2e/m.tmp
mv e2e/m.tmp e2e/witness-manifest.txt
expect "a published example missing from the manifest fails" 1 "second-example on REAL_PROCESS"

# The other direction, which a one-way check would miss: the example is gone and its lines are not.
fresh_tree
rm -rf e2e/examples/second-example
expect "a manifest line naming no published example fails" 1 "naming no published example"

# The shape from this project's own history: the witness did not run, and nothing in the build said
# so. Skipped, aborted and never-discovered all arrive here as the same absence.
fresh_tree
grep -v '^first-example on IN_PROCESS$' e2e/target/witness-ledger.txt > e2e/target/l.tmp
mv e2e/target/l.tmp e2e/target/witness-ledger.txt
expect "a manifest witness absent from the ledger fails" 1 "first-example on IN_PROCESS"

# A whole class aborting in @BeforeAll writes no ledger at all, and the build reports success.
fresh_tree
rm -f e2e/target/witness-ledger.txt
expect "a missing ledger fails rather than passing vacuously" 1 "no witness recorded anything"

# Checks 1 and 2 must not need a run; if they did they could not gate a pull request.
fresh_tree
rm -f e2e/target/witness-ledger.txt
expect "the manifest-only check needs no ledger" 0 "agree on all 4 witnesses" --manifest-only

fresh_tree
rm -f e2e/witness-manifest.txt
expect "a missing manifest fails" 1 "the release contract itself is gone"

fresh_tree
expect "an unknown argument is refused rather than ignored" 2 "unknown argument" --manifest-onyl

echo "witness-gate cases: $passed passed, $failed failed"
[ "$failed" -eq 0 ]
