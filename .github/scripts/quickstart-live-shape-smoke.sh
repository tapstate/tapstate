#!/usr/bin/env bash
# The quickstart lanes' shape, checked as a text.
#
# Both lanes here run nightly against published releases, so nothing they cover is watched on a pull
# request. That is the right schedule -- a red caused by a stale release is not a red about the change
# under review -- but it means the edits that would quietly hollow them out merge with nothing looking:
#
#   * the arm64 leg. It is one matrix entry and one architecture assertion, and losing either leaves
#     three legs passing while two of them run the same amd64 image. The arm64 half of the published
#     image was built and pushed for four releases and started by nothing; this is how that came back.
#   * where the upgrade lane starts. Resolved with --previous, it moves between two releases; resolved
#     without, it installs the newest and "upgrades" to the newest, and every assertion downstream then
#     compares a version against itself. In the log the two are indistinguishable.
#   * the volumes it upgrades across. `docker compose down -v` in place of `docker compose down` turns
#     the upgrade into a fresh install, and the data assertions go on passing because a stack that
#     started from nothing agrees with itself too.
#   * the install edge it upgrades through. A CLI taken from the checkout would prove a path no user
#     has, and would keep proving it after the published one broke.
#
# Reads the workflow as text, attributing lines to the job or step they sit under. Whole-line comments
# are dropped first, and that is not tidiness: the file explains these rules in prose, so a check that
# matched the comments would answer about the explanation. "`down`, not `down -v`" is a sentence
# containing `down -v`.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
workflow="$here/../workflows/quickstart-live.yml"
failed=0

ok()  { echo "ok   - $1"; }
bad() { echo "FAIL - $1"; failed=$((failed + 1)); }

[ -f "$workflow" ] || { echo "no quickstart workflow at $workflow"; exit 1; }

# Every line of one job, from its two-space key to the next one, comments dropped.
job() {
  awk -v want="  $1:" '
    $0 == want { inside = 1; next }
    /^  [a-z][a-z0-9_-]*:[ \t]*$/ { inside = 0 }
    inside && $0 !~ /^[ \t]*#/ { print }
  ' "$workflow"
}

# Every line of one named step, from its `- name:` to the next step, comments dropped.
step() {
  awk -v want="      - name: $1" '
    $0 == want { inside = 1; next }
    /^      - / { inside = 0 }
    inside && $0 !~ /^[ \t]*#/ { print }
  ' "$workflow"
}

has() {   # $1 = description, $2 = text, $3 = extended regex
  if printf '%s' "$2" | grep -qE "$3"; then ok "$1"; else bad "$1"; fi
}

lacks() { # $1 = description, $2 = text, $3 = extended regex
  if printf '%s' "$2" | grep -qE "$3"; then bad "$1"; else ok "$1"; fi
}

# Vacuity first. Every case below reads a job or a step by name, and a rename would empty all of them
# at once -- silently, and in the direction that reports success.
jobs_list="$(awk '/^  [a-z][a-z0-9_-]*:[ \t]*$/ { gsub(/[ :]/, ""); print }' "$workflow")"
for want in quickstart-live quickstart-upgrade; do
  if printf '%s\n' "$jobs_list" | grep -qx "$want"; then
    ok "the $want job is here"
  else
    bad "no $want job in the workflow -- every case about it below would pass over nothing"
  fi
done

live="$(job quickstart-live)"
upgrade="$(job quickstart-upgrade)"
if [ -z "$live" ] || [ -z "$upgrade" ]; then
  echo "one of the two jobs read back empty" >&2
  exit 1
fi

# --- the arm64 leg -------------------------------------------------------------------------------
has "the live lane has an arm64 leg, on an arm runner" "$live" \
  'server: published,[[:space:]]*arch: arm64,[[:space:]]*runner: ubuntu-24\.04-arm'
has "it keeps the amd64 published leg beside it" "$live" \
  'server: published,[[:space:]]*arch: amd64'
has "it keeps the from-source leg beside both" "$live" \
  'server: from-source'

arch_step="$(step 'The server came from where this leg says it did')"
has "the image's architecture is read off the image, not assumed from the runner" "$arch_step" \
  'docker image inspect .* --format .\{\{\.Architecture\}\}'
has "and held to the leg's own architecture" "$arch_step" \
  'matrix\.arch'

# --- the two halves of the release agree ------------------------------------------------------------
cli_step="$(step "The CLI the quickstart installed is the release's own")"
has "the installed CLI is held to the image tag this leg started" "$cli_step" \
  'PREPARED##\*:'

# --- the connector jars --------------------------------------------------------------------------
jars_step="$(step 'The connector jars the quickstart fetched are readable jars')"
has "the fetched jars are opened, not just weighed" "$jars_step" 'zipfile'
has "and their entries are read back" "$jars_step" 'testzip'

# --- where the upgrade starts --------------------------------------------------------------------
resolve_step="$(step 'Resolve the two releases this leg moves between')"
has "the upgrade starts from the release before the newest" "$resolve_step" \
  'latest-release-tag\.sh --previous'
# The two patterns below are regexes matched against the workflow's text, so the `$` is a dollar the
# workflow contains and not one this script expands.
# shellcheck disable=SC2016
has "and refuses if both ends resolve to the same release" "$resolve_step" \
  'from" != "\$to'
has "and refuses if the two come back in the wrong version order" "$resolve_step" \
  'sort -V'

# --- what it upgrades across ---------------------------------------------------------------------
upgrade_step="$(step 'Upgrade in place, following the documented steps')"
has "the upgrade stops the stack" "$upgrade_step" 'docker compose down'
lacks "and keeps its volumes doing it" "$upgrade_step" 'docker compose down .*-v'
has "the new CLI comes through the published install edge" "$upgrade_step" \
  'install\.tapstate\.dev'
# shellcheck disable=SC2016
has "and the image the stack runs is moved to the new version" "$upgrade_step" \
  'ghcr\.io/tapstate/tapstate:\$TO_VER'

# --- and it will not stay vacuous quietly ---------------------------------------------------------
has "the lane refuses once a migration runner exists and it still asserts nothing about one" \
  "$upgrade" 'MigrationRunner'

if [ "$failed" -ne 0 ]; then
  echo "$failed case(s) failed" >&2
  exit 1
fi
echo "all cases passed"
