#!/usr/bin/env bash
# Developer Certificate of Origin: every commit in an external pull request carries a
# `Signed-off-by:` line.
#
# What the line is for, since it looks like the attribution footers this repository rejects: it is a
# statement about *origin*, not authorship — the submitter certifying they have the right to send
# this code under the project's license. Nothing here verifies that claim; the certificate buys a
# clear line of responsibility, not a technical guarantee, and review is still what catches code
# that came from somewhere it should not have.
#
# Three deliberate narrowings, each of which would otherwise produce a red nobody can act on:
#
#   - Only external pull requests. A branch in this repository was pushed by someone with write
#     access, whose identity the repository already establishes; asking them for the line as well
#     adds a flag to remember and no assurance.
#   - Merge commits are exempt. A contributor merging `main` into their branch gets a commit GitHub
#     wrote, not one they authored.
#   - Presence and shape only — never that the sign-off matches the commit author. The certificate
#     explicitly covers passing on a patch you received from someone else, so those two differing is
#     a case it provides for, not a violation.
#
# The failure message is the whole point of the check. A contributor who forgot `-s` has to rewrite
# history and force-push, and if the red does not say exactly how, this stops being a gate and
# becomes an unexplained wall on someone's first contribution.
#
# Reads PR_IS_FORK, and the range from DCO_RANGE or BASE_REF. Exits 1 naming every unsigned commit.
set -uo pipefail

if [ "${PR_IS_FORK:-false}" != "true" ]; then
  echo "not required: this branch lives in this repository, so its author already has write access here."
  exit 0
fi

range="${DCO_RANGE:-origin/${BASE_REF:-main}..HEAD}"
signoff='^[[:space:]]*Signed-off-by:[[:space:]]+.+[[:space:]]+<[^<>[:space:]]+@[^<>[:space:]]+>[[:space:]]*$'

# Ask for the range explicitly rather than piping rev-list into the loop. A range that does not
# resolve makes rev-list fail and print nothing, and a loop over nothing reports every commit signed
# — a required check that goes green precisely when it could not look at anything.
if ! commits="$(git rev-list --no-merges "$range" 2>&1)"; then
  echo "::error::cannot list the commits in ${range}, so nothing was checked: ${commits}"
  exit 1
fi

unsigned=()
while read -r sha; do
  [ -n "$sha" ] || continue
  if ! git log -1 --format='%B' "$sha" | grep -qE "$signoff"; then
    unsigned+=("$(git log -1 --format='%h %s' "$sha")")
  fi
done <<< "$commits"

if [ "${#unsigned[@]}" -eq 0 ]; then
  echo "clean: every commit is signed off."
  exit 0
fi

echo "::error::commits without a Signed-off-by line:"
printf '  %s\n' "${unsigned[@]}"
cat <<'HOWTO'

Every commit needs a line of the form:

  Signed-off-by: Your Name <your@email>

It certifies that you have the right to submit this code under the project's license
(the full text is one paragraph, at https://developercertificate.org/). Git writes it
for you with `git commit -s`.

To add it to what you already pushed, then force-push to your fork — which is harmless,
it is your branch:

  git commit --amend -s               # the last commit only
  git rebase --signoff <base>         # every commit on your branch, e.g. --signoff main
  git push --force-with-lease

See CONTRIBUTING.md, "Sign your commits (DCO)".
HOWTO
exit 1
