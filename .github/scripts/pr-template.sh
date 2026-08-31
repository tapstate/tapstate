#!/usr/bin/env bash
# Two pull-request sections a reviewer cannot reconstruct from the diff.
#
#   Linked issue                -> where this change was decided, and where its design is written.
#                                  For an external contribution the issue is the only context there
#                                  is: the planning that produced internal work is not public.
#   Live verification scenario  -> how to watch it work by hand. CI proves the assertions still
#                                  hold; it does not prove anyone has seen the thing run.
#   Release note                -> the one sentence a release is assembled from. `none` is a real
#                                  answer here and passes; what does not pass is the section left
#                                  as the template shipped it. Until the template stopped ending
#                                  that section with the literal word `none`, those two were the
#                                  same body, and the change simply never reached a release note.
#
# This is a weak gate and is meant to stay one. It checks that the two sections are present and that
# something was written under them — not that what was written is any good, which is the reviewer's
# call and cannot be delegated to a grep. What it buys is that neither section can be silently left
# as the template shipped it, which is how both of them get skipped: not by refusing, by scrolling.
#
# Reads the body from PR_BODY. Exits 1 naming every section that is missing or unfilled.
set -uo pipefail

body="${PR_BODY:-}"
# Heading level and name together. The release note is a SUBSECTION, and a loop that assumed
# one level would look for it under a heading that is not there and pass every body forever.
required=("## Linked issue" "## Live verification scenario" "### Release note")

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=.github/scripts/_pr-section.sh
. "$here/_pr-section.sh"
fail=0
for entry in "${required[@]}"; do
  marker="${entry%% *}"
  name="${entry#* }"
  # Not piped into `grep -q`: it closes the pipe on its first match, the writer behind it dies of
  # that, and under `pipefail` the pipeline reports the signal rather than the match.
  if ! grep -qE "^${marker} ${name}[[:space:]]*$" <<<"$body"; then
    echo "::error::the pull request body has no \"${entry}\" section — it is in the template, and removing it does not answer it"
    fail=1
    continue
  fi
  if [ -z "$(section_body "$name" "$marker")" ]; then
    echo "::error::\"${entry}\" is still the template's prompt, with nothing written under it"
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "All three are short to answer and expensive to leave out. See .github/PULL_REQUEST_TEMPLATE.md,"
  echo "and CONTRIBUTING.md -> External contributions for what a linked issue is expected to carry."
  exit 1
fi

echo "clean: the linked issue, the live verification scenario and the release note are answered."
