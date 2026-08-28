#!/usr/bin/env bash
# The two workflows that carry SonarQube to a pull request from a fork, asserted at the file level.
#
# Why file level and not behaviour: the property that matters about fork-sonar-notice is a property
# of the FILE - that it contains no checkout. `pull_request_target` runs with a writable token and
# with this repository's own workflow code; the moment such a file also checks out the contributor's
# branch, that token is theirs. No amount of runtime testing establishes the absence of a step, and
# the day someone adds one "just to read the diff" is the day this matters.
#
# Read the directives, not the prose. Every comment is stripped before anything is matched, because
# the file explains at length why it has no checkout - and a check that cannot tell an explanation
# from an instruction fails on the explanation.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
wf="$here/../workflows"
scratch="$(mktemp -d)"; trap 'rm -rf "$scratch"' EXIT
passed=0; failed=0

check() { # check <name> <0-or-1>
  if [ "$2" = 0 ]; then printf '  ok    %s\n' "$1"; passed=$((passed + 1))
  else printf '  FAIL  %s\n' "$1"; failed=$((failed + 1)); fi
}
directives() { grep -v '^[[:space:]]*#' "$1" > "$scratch/$(basename "$1")"; printf '%s' "$scratch/$(basename "$1")"; }

echo "fork-sonar cases"

notice="$wf/fork-sonar-notice.yml"
check "the notice workflow exists" "$([ -f "$notice" ] && echo 0 || echo 1)"
nd="$(directives "$notice")"
check "it never checks out the pull request" \
  "$(grep -q 'actions/checkout' "$nd" && echo 1 || echo 0)"
check "it runs on pull_request_target, which is why the rule above exists" \
  "$(grep -q 'pull_request_target' "$nd" && echo 0 || echo 1)"
check "it fires only for a fork" \
  "$(grep -qF 'head.repo.full_name != github.repository' "$nd" && echo 0 || echo 1)"
check "it can write, or it could not label at all" \
  "$(grep -q 'pull-requests: write' "$nd" && echo 0 || echo 1)"
check "the label it applies is the one the maintainer scans for" \
  "$(grep -q 'needs-sonar-review' "$nd" && echo 0 || echo 1)"

analyze="$wf/sonar-pr.yml"
check "the analysis workflow exists" "$([ -f "$analyze" ] && echo 0 || echo 1)"
ad="$(directives "$analyze")"
# All three, and each for a different reason: without key the run is filed as a branch analysis and
# overwrites the project's own state; without branch and base the pull request has no shape on the
# server. The plugin autodetects them from a pull_request event, and this is a workflow_dispatch.
for prop in sonar.pullrequest.key sonar.pullrequest.branch sonar.pullrequest.base; do
  check "it passes $prop explicitly" "$(grep -qF "$prop" "$ad" && echo 0 || echo 1)"
done
check "it is triggered by a person, not by the pull request" \
  "$(grep -q 'workflow_dispatch' "$ad" && echo 0 || echo 1)"
check "it checks out the merge result, not the head" \
  "$(grep -qF '/merge' "$ad" && echo 0 || echo 1)"
# The mirror of the notice workflow's rule: this one DOES build the contributor's code, which is
# exactly why it may not be triggered by the pull request itself.
check "and it is not a pull_request_target" \
  "$(grep -q 'pull_request_target' "$ad" && echo 1 || echo 0)"

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ]
