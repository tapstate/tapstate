#!/usr/bin/env bash
# The witness gate: every named scenario in the manifest really ran and passed, checked one by one.
#
# Aggregate counts cannot carry this: a class that aborts before its first test reports
# "Tests run: 0, Skipped: 0", and twelve witnesses running eleven still sum to a positive number
# inside a BUILD SUCCESS. So the gate reads names, not counts, from two files:
#
#   e2e/witness-manifest.txt   - the contract: every scenario a release must have seen pass
#   e2e/target/witness-ledger.txt - the run's own record: one line per witness that executed and
#                                   held every assertion; skips and aborts write nothing
#
# Three checks, each failing loudly on its own:
#   1. every published example (on every tier) is in the manifest - a new example cannot ship unlisted;
#   2. every manifest line names a published example - the manifest cannot be padded with ghosts, and
#      a line surviving its example's deletion is caught rather than ignored;
#   3. every manifest line is in the ledger - a witness that was skipped, aborted, or never discovered
#      reads as absence, and absence fails the gate. An environment that could not run a witness is a
#      gate failure, not an exemption.
#
# Run it after the verify that produced the ledger, from the repository root:
#   .github/scripts/witness-gate.sh
set -euo pipefail

manifest="e2e/witness-manifest.txt"
ledger="e2e/target/witness-ledger.txt"
tiers="IN_PROCESS REAL_PROCESS"

[ -f "$manifest" ] || { echo "witness-gate: $manifest is missing - the release contract itself is gone"; exit 1; }
[ -f "$ledger" ] || { echo "witness-gate: $ledger is missing - no witness recorded anything; was the sweep run?"; exit 1; }

expected=$(for d in e2e/examples/*/; do
  name=$(basename "$d")
  for t in $tiers; do echo "$name on $t"; done
done | LC_ALL=C sort)

listed=$(grep -v '^#' "$manifest" | sed '/^[[:space:]]*$/d' | LC_ALL=C sort)

missing_from_manifest=$(LC_ALL=C comm -23 <(echo "$expected") <(echo "$listed"))
if [ -n "$missing_from_manifest" ]; then
  echo "witness-gate: published examples missing from $manifest:"
  echo "$missing_from_manifest" | sed 's/^/  /'
  exit 1
fi

ghosts=$(LC_ALL=C comm -13 <(echo "$expected") <(echo "$listed"))
if [ -n "$ghosts" ]; then
  echo "witness-gate: manifest lines naming no published example:"
  echo "$ghosts" | sed 's/^/  /'
  exit 1
fi

never_ran=$(LC_ALL=C comm -23 <(echo "$listed") <(LC_ALL=C sort "$ledger"))
if [ -n "$never_ran" ]; then
  echo "witness-gate: manifest witnesses that never ran to a pass (skipped, aborted, or absent):"
  echo "$never_ran" | sed 's/^/  /'
  exit 1
fi

count=$(echo "$listed" | wc -l | tr -d ' ')
echo "witness-gate: all $count witnesses executed and passed"
