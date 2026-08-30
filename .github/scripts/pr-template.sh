#!/usr/bin/env bash
# Two pull-request sections a reviewer cannot reconstruct from the diff.
#
#   Linked issue                -> where this change was decided, and where its design is written.
#                                  For an external contribution the issue is the only context there
#                                  is: the planning that produced internal work is not public.
#   Live verification scenario  -> how to watch it work by hand. CI proves the assertions still
#                                  hold; it does not prove anyone has seen the thing run.
#
# This is a weak gate and is meant to stay one. It checks that the two sections are present and that
# something was written under them — not that what was written is any good, which is the reviewer's
# call and cannot be delegated to a grep. What it buys is that neither section can be silently left
# as the template shipped it, which is how both of them get skipped: not by refusing, by scrolling.
#
# Reads the body from PR_BODY. Exits 1 naming every section that is missing or unfilled.
set -uo pipefail

body="${PR_BODY:-}"
required=("Linked issue" "Live verification scenario")

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=.github/scripts/_pr-section.sh
. "$here/_pr-section.sh"
fail=0
for name in "${required[@]}"; do
  if ! printf '%s\n' "$body" | grep -qE "^## ${name}[[:space:]]*$"; then
    echo "::error::the pull request body has no \"## ${name}\" section — it is in the template, and removing it does not answer it"
    fail=1
    continue
  fi
  if [ -z "$(section_body "$name")" ]; then
    echo "::error::\"## ${name}\" is still the template's prompt, with nothing written under it"
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "Both sections are short to answer and expensive to leave out. See .github/PULL_REQUEST_TEMPLATE.md,"
  echo "and CONTRIBUTING.md -> External contributions for what a linked issue is expected to carry."
  exit 1
fi

echo "clean: the linked issue and the live verification scenario are both answered."
