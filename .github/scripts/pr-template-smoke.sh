#!/usr/bin/env bash
# Cases for the pull-request template gate. The shapes that matter are the ones where a body looks
# answered and is not: the template's own comment left in place (which is what "I scrolled past it"
# produces), and a heading deleted rather than filled in. The mirror matters just as much — a gate
# that cannot tell an answered section from an unanswered one is as useless when it refuses
# everything as when it admits everything, so every unfilled case is paired with a filled one that
# must pass, and one case answers exactly one of the two sections to check it names that one alone.
#
# Run it from anywhere. Exits 0 if every case holds.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/pr-template.sh"
passed=0
failed=0

# Both halves of the answer: the exit code, and the reason. A gate that refuses for the wrong reason
# is not refusing — it is failing to notice something else.
expect() {
  local name="$1" want_code="$2" want_text="$3" pr_body="$4"
  local out code
  out="$(PR_BODY="$pr_body" bash "$gate" 2>&1)"
  code=$?
  if [ "$code" = "$want_code" ] && grep -qF "$want_text" <<<"$out"; then
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        wanted exit %s containing %s\n        got exit %s: %s\n' \
      "$name" "$want_code" "$want_text" "$code" "$out"
    failed=$((failed + 1))
  fi
}

# Refuses to name a section that is in fact answered.
refute() {
  local name="$1" unwanted="$2" pr_body="$3"
  local out
  out="$(PR_BODY="$pr_body" bash "$gate" 2>&1)"
  if grep -qF "$unwanted" <<<"$out"; then
    printf '  FAIL  %s\n        did not want %s, got: %s\n' "$name" "$unwanted" "$out"
    failed=$((failed + 1))
  else
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  fi
}

answered=$'## Linked issue\n\nRefs #12. The design is in the issue body, under "Design".\n\n## Live verification scenario\n\nStart the demo workspace, apply the pipeline, insert a row, watch it land in the sink.\n\n\n### Release note\n\n**Kind:** new\n\nas an operator, I can see which build I am running, so that a bug report names it without asking.\n'

untouched=$'## Linked issue\n\n<!--\nWhich issue, and where its design is written.\n-->\n\n## Live verification scenario\n\n<!-- How a maintainer sees this work by hand. -->\n'

half=$'## Linked issue\n\nRefs #12.\n\n## Live verification scenario\n\n<!-- How a maintainer sees this work by hand. -->\n'

deleted=$'## What changed\n\nA fix.\n\n## Live verification scenario\n\nRun the CLI against the sample workspace.\n'

inline=$'## Linked issue\n\nRefs #12 <!-- not Fixes, on purpose -->\n\n## Live verification scenario\n\nRun it <!-- see below --> against the demo workspace.\n\n\n### Release note\n\n**Kind:** new\n\nas an operator, I can see which build I am running, so that a bug report names it without asking.\n'

other_sections=$'## Linked issue\n\nRefs #12.\n\n## Live verification scenario\n\nRun it by hand.\n\n## Checks\n\n<!-- unfilled, and none of this gate\x27s business -->\n\n\n### Release note\n\n**Kind:** new\n\nas an operator, I can see which build I am running, so that a bug report names it without asking.\n'

# The section the gate is being taught. `none` is an answer and must pass -- a release-tooling
# change has no user-visible effect, and a sentence invented for it travels all the way to a user.
relnote_missing=$'## Linked issue\n\nRefs #12.\n\n## Live verification scenario\n\nRun it by hand.\n'

relnote_unfilled=$'## Linked issue\n\nRefs #12.\n\n## Live verification scenario\n\nRun it by hand.\n\n### Release note\n\n<!--\nOne sentence, as a user story.\n-->\n'

relnote_none=$'## Linked issue\n\nRefs #12.\n\n## Live verification scenario\n\nRun it by hand.\n\n### Release note\n\nnone -- release tooling only, nothing a user sees.\n'

expect "an answered body passes"                0 "clean:"            "$answered"
expect "the template left untouched is refused" 1 "Linked issue"      "$untouched"
expect "and it names the second section too"    1 "Live verification" "$untouched"
expect "answering one is not answering both"    1 "Live verification" "$half"
refute "and the answered one is not named"        "\"## Linked issue\" is still" "$half"
expect "a deleted heading is refused by name"   1 "has no \"## Linked issue\"" "$deleted"
expect "an empty body is refused"               1 "has no"            ""
expect "an answer beside a comment passes"      0 "clean:"            "$inline"
expect "other unfilled sections are not ours"   0 "clean:"            "$other_sections"

# A sentence has to say which kind it is; `none` does not, because there is nothing to file.
relnote_nokind=$'## Linked issue\n\nRefs #12.\n\n## Live verification scenario\n\nRun it by hand.\n\n### Release note\n\nas an operator, I can see the version.\n'

relnote_badkind=$'## Linked issue\n\nRefs #12.\n\n## Live verification scenario\n\nRun it by hand.\n\n### Release note\n\n**Kind:** bugfix\n\nas an operator, I can see the version.\n'

relnote_fix=$'## Linked issue\n\nRefs #12.\n\n## Live verification scenario\n\nRun it by hand.\n\n### Release note\n\n**Kind:** fix\n\nas an operator, I no longer see a stale row.\n'

expect "a missing release note is refused by name" 1 'has no "### Release note"' "$relnote_missing"
expect "a release note left as the prompt refused" 1 "### Release note" "$relnote_unfilled"
# The control, and the reason `none` stays legal: without it, "refuse anything that is not a
# sentence" passes both cases above. It does NOT discriminate this change on its own -- it passed
# before the gate existed too -- and that is said here rather than left to be assumed.
expect "a written none is an answer and passes"    0 "clean:" "$relnote_none"

# The gate above is driven by fixtures, so nothing in it looks at the template this repository
# actually ships. That file is where the failure came from: while it ended the section with the
# literal word `none`, an untouched body and a considered one were the same body, and the gate
# would pass every pull request while reporting itself clean. So the template is a case.
expect "the shipped template does not pre-answer" 1 '"### Release note" is still' "$(cat "$here/../PULL_REQUEST_TEMPLATE.md")"

expect "a sentence without a Kind is refused"      1 "Kind" "$relnote_nokind"
expect "a Kind that is neither is refused"         1 "bugfix" "$relnote_badkind"
expect "fix is a Kind and passes"                  0 "clean:" "$relnote_fix"
# The control that keeps the escape hatch open: `none` needs no classification, so demanding one
# everywhere would refuse exactly the pull requests the escape hatch was written for.
refute "a written none is not asked for a Kind"      "Kind" "$relnote_none"

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
