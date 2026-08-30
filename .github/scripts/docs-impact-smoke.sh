#!/usr/bin/env bash
# Cases for the documentation-impact gate. The pair that carries the whole gate is the first two:
# a body that answered nothing and a body that answered "none" are, in this repository today,
# character-for-character identical in everything a label-reading check can see — neither carries
# `docs-needed`. Any implementation that looks only at the label admits the first, and admitting the
# first is the reason this gate exists.
#
# The other two refusals are the inconsistent pairs, and they are kept apart on purpose: they are
# repaired in opposite directions, so a gate that reported one sentence for both would be telling
# the author to do the wrong thing half the time.
#
# Run it from anywhere. Exits 0 if every case holds.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/docs-impact.sh"
passed=0
failed=0

# Both halves of the answer: the exit code, and which of the four refusals it is. A gate refusing for
# the wrong reason has not noticed the thing it was pointed at.
expect() {
  local name="$1" want_code="$2" want_text="$3" pr_body="$4" labels="$5" actor="${6:-someone}"
  local out code
  out="$(PR_BODY="$pr_body" PR_LABELS="$labels" PR_ACTOR="$actor" bash "$gate" 2>&1)"
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
  local name="$1" unwanted="$2" pr_body="$3" labels="$4"
  local out
  out="$(PR_BODY="$pr_body" PR_LABELS="$labels" PR_ACTOR=someone bash "$gate" 2>&1)"
  if grep -qF -- "$unwanted" <<<"$out"; then
    printf '  FAIL  %s\n        did not want %s, got: %s\n' "$name" "$unwanted" "$out"
    failed=$((failed + 1))
  else
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  fi
}

# shellcheck disable=SC2016  # the backticks are markdown in the body being built, not a substitution
section() {   # $1 = draft field, $2 = public field -- written the way the template writes them
  printf '## What changed\n\nA change.\n\n## Documentation impact\n\n- [ ] This change needs documentation follow-up (add the `docs-needed` label)\n- **Draft in this repository:** %s\n- **Public page it is headed for:** %s\n\n## Checks\n\n- [ ] something else\n' "$1" "$2"
}

# The template exactly as it ships: both fields carry only their prompt.
untouched="$(section '<!-- path under docs/, or "none" -->' '<!-- URL under https://tapstate.dev/docs, or "none" -->')"
# Answered "none", deliberately. Indistinguishable from the above to anything reading labels.
declined="$(section 'none' 'none')"
answered="$(section 'docs/nest-merge.md' 'https://tapstate.dev/docs/nest-merge')"
# A draft written but not yet aimed at a page. Follow-up still exists, so the label still has to.
half_answered="$(section 'docs/nest-merge.md' 'none')"
one_blank="$(section 'docs/nest-merge.md' '<!-- URL under https://tapstate.dev/docs, or "none" -->')"
no_section=$'## What changed\n\nA change.\n\n## Checks\n\n- [ ] something\n'

expect "answered and labelled passes"              0 "clean:"          "$answered"      "docs-needed"
expect "an explicit none with no label passes"     0 "clean:"          "$declined"      ""
expect "a draft with no public page still needs the label" 0 "clean:" "$half_answered" "docs-needed"

expect "the template left untouched is refused"    1 "answered neither" "$untouched"    ""
expect "and it is refused with the label on too"   1 "answered neither" "$untouched"    "docs-needed"
expect "one field left as its prompt is refused"   1 "Public page it is headed for" "$one_blank" "docs-needed"
refute "and the answered field is not named"         "\"Draft in this repository\" is still" "$one_blank" "docs-needed"

expect "a page named without the label is refused" 1 "no \`docs-needed\` label" "$answered" ""
expect "a draft named without the label is refused" 1 "no \`docs-needed\` label" "$half_answered" ""
expect "none twice with the label is refused"      1 "carries \`docs-needed\`"  "$declined" "docs-needed"

# What people actually write when they mean none, taken from pull requests already merged here: a
# full stop after it, or a sentence saying why. Comparing the whole field to the word "none" reads
# every one of those as a documentation path, and then refuses the pull request for not carrying a
# label it must not carry.
expect "'none.' is none"                           0 "clean:"          "$(section 'none.' 'none.')" ""
expect "none with a reason after it is none"       0 "clean:"          "$(section 'none -- the lane documents itself' 'none')" ""
expect "and the capitalised one too"               0 "clean:"          "$(section 'None' 'NONE')" ""
# The whole first word, not a prefix: a real answer can begin with those four letters.
expect "'nonetheless ...' is not none"             1 "no \`docs-needed\` label" "$(section 'nonetheless docs/a.md' 'none')" ""

expect "a deleted section is refused by name"      1 "no \"## Documentation impact\"" "$no_section" ""
expect "an empty body is refused"                  1 "no \"## Documentation impact\"" ""          ""

# The two refusals that are inconsistencies are repaired in opposite directions, so they must not
# share a sentence: one says add the label, the other says the label is wrong.
refute "the missing-label refusal does not say the label is wrong" "carries \`docs-needed\`" "$answered" ""
refute "the spurious-label refusal does not ask for the label"     "no \`docs-needed\` label" "$declined" "docs-needed"

# Bots do not get the template, so a gate that refuses them only teaches people to switch it off.
expect "a bot author is exempt"                    0 "not asked of"    "$untouched"     "" "dependabot[bot]"
expect "and a human with the same body is not"     1 "answered neither" "$untouched"    "" "someone"

# Labels arrive as a list; the gate must not match a different label that contains the word.
expect "a lookalike label is not the label"        1 "no \`docs-needed\` label" "$answered" "docs-needed-later"
expect "the label is found among others"           0 "clean:"          "$answered"      "bug,docs-needed,area/cli"

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
