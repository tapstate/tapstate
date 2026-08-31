#!/usr/bin/env bash
# Cases for the release-notes assembly.
#
# Two ends of one regular expression are where this goes wrong, and both produce a body that looks
# written: swallowing the heading that follows a section, and leaving a section that said nothing as
# an empty bullet. So the cases that matter are not "a pull request with a note produces a line" --
# they are the two that produce nothing, and the one whose note is followed by another heading.
#
# The other half is the empty slots. What the machine hands the approver is a prompt, never material:
# the governance record it would otherwise be filled from lives in a private repository, and the
# identifiers in it must not travel into a public release body -- where a comment block is invisible
# on the page and plainly readable in the API response.
#
# Builds its own repository. The inherited git environment is cleared first: a script that makes its
# own repository and is ever started from a hook takes refs and remotes from the repository being
# pushed while its working tree is the sandbox.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PREFIX GIT_QUARANTINE_PATH
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
script="$here/release-notes.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
export SMOKE_SCRATCH="$scratch"
mkdir -p "$scratch/bin" "$scratch/bodies"
passed=0
failed=0

# `gh pr view <n>` answers with whatever body the case staged for that number, and refuses for a
# number nothing was staged for -- which is what a reference to a pull request in another repository,
# or to an issue, really does.
cat > "$scratch/bin/gh" <<'STUB'
#!/usr/bin/env bash
for a in "$@"; do case "$a" in [0-9]*) n="$a"; break ;; esac; done
[ -f "$SMOKE_SCRATCH/bodies/$n" ] || exit 1
cat "$SMOKE_SCRATCH/bodies/$n"
STUB
chmod +x "$scratch/bin/gh"
PATH="$scratch/bin:$PATH"
export PATH
export GITHUB_REPOSITORY=tapstate/tapstate

body() { printf '%s' "$2" > "$scratch/bodies/$1"; }

repo="$scratch/repo"
mkdir -p "$repo"
git -C "$repo" init -q -b main
git -C "$repo" config user.email t@example.com
git -C "$repo" config user.name t
seed() {   # a commit whose subject carries a pull-request reference the way each merge style writes it
  echo "$RANDOM$1" > "$repo/f"
  git -C "$repo" add f
  git -C "$repo" commit -q -m "$1"
}
echo base > "$repo/f"; git -C "$repo" add f; git -C "$repo" commit -q -m "base"
git -C "$repo" tag v0.3.0

# The section as it is written now: a Kind line, then the sentence. Called with one argument it
# writes a section with no Kind at all, which is every pull request merged before the field existed
# -- those must still be carried, and must not be filed as though somebody had classified them.
note() {
  if [ $# -ge 2 ]; then
    printf '## What changed\n\nx\n\n### Release note\n\n**Kind:** %s\n\n%s\n\n## Checks\n\n- [ ] x\n' "$2" "$1"
  else
    printf '## What changed\n\nx\n\n### Release note\n\n%s\n\n## Checks\n\n- [ ] x\n' "$1"
  fi
}

body 11 "$(note 'You can assemble tables from MySQL and PostgreSQL into one object, without creating a view.' new)"
body 12 "$(note 'none')"
body 13 "$(printf '## What changed\n\nInternal only.\n\n## Checks\n\n- [ ] x\n')"
body 14 "$(note 'none -- build configuration only.')"
body 15 "$(note 'You can read a task write-back position from the CLI, so that a stalled task can be told apart from a slow one.' new)"
body 16 "$(note 'Rows deleted at the source while a task was down no longer survive a reload, so that a purge is not needed by hand.' fix)"
body 17 "$(note 'You can pass a workspace directory to the CLI, so that a demo need not run from the current directory.')"

seed "Assemble across sources (#11)"
seed "Refactor the registry (#12)"
seed "Bump a dependency (#13)"
seed "CI: cache maven (#14)"
seed "Merge pull request #15 from contributor/write-back-position"
seed "Delete-during-downtime survives a reload (#16)"
seed "Take a workspace directory (#17)"

out="$(cd "$repo" && bash "$script" --version 0.4.0 --base v0.3.0 --sha HEAD \
  --macos-req 'Recommended macOS: 15.0 or newer.' --glibc-req 'Recommended glibc: 2.34 or newer.' 2>&1)"
code=$?

has() {
  local name="$1" needle="$2"
  if grep -qF -- "$needle" <<<"$out"; then
    printf '  ok    %s\n' "$name"; passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        wanted: %s\n' "$name" "$needle"; failed=$((failed + 1))
  fi
}
hasnt() {
  local name="$1" needle="$2"
  if grep -qF -- "$needle" <<<"$out"; then
    printf '  FAIL  %s\n        did not want: %s\n' "$name" "$needle"; failed=$((failed + 1))
  else
    printf '  ok    %s\n' "$name"; passed=$((passed + 1))
  fi
}

if [ "$code" = 0 ]; then
  printf '  ok    %s\n' "it assembles"; passed=$((passed + 1))
else
  printf '  FAIL  it assembles: exit %s\n%s\n' "$code" "$out"; failed=$((failed + 1))
fi

# Layer 1 -- fixed prose that has to survive every assembly, because nothing else discloses it.
has  "the preview disclaimer is in"        "not for production"
has  "duplicate rows are disclosed"        "Duplicate rows on an insert-only target"
has  "the positioning window is disclosed" "Changes lost while the change stream positions itself"
has  "delivery is not claimed exactly-once" "at-least-once"
has  "the measured macOS floor is in"      "Recommended macOS: 15.0 or newer."
has  "the measured glibc floor is in"      "Recommended glibc: 2.34 or newer."

# Layer 2 -- harvested, verbatim.
has  "a written note is carried over verbatim" "* You can assemble tables from MySQL and PostgreSQL into one object, without creating a view."
has  "a merge-commit subject is harvested too" "* You can read a task write-back position from the CLI"
hasnt "the section's next heading is not swallowed" "## Checks"
# The three that must produce nothing, counted rather than searched for. Their absence cannot be
# checked by looking for their text -- a pull request that contributes no entry contributes no text
# at all, so "the body does not contain it" is true before the script has done anything. Two entries
# from five pull requests is the assertion; anything that leaks one of the three makes it three.
whats_new="$(printf '%s\n' "$out" | awk '/^## What.s new$/ { inside = 1; next } /^## / { inside = 0 } /^<!--/ { inside = 0 } inside')"
bullets="$(printf '%s\n' "$whats_new" | grep -c '^\* ')"
if [ "$bullets" = 2 ]; then
  printf '  ok    %s\n' "the three that say nothing produce no entries (2 bullets from 5 pull requests)"
  passed=$((passed + 1))
else
  printf '  FAIL  %s\n        wanted 2 bullets, got %s:\n%s\n' \
    "the three that say nothing produce no entries" "$bullets" "$whats_new"
  failed=$((failed + 1))
fi
hasnt "a none with a reason is not carried over" "build configuration only"
if grep -qE '^\*[[:space:]]*$' <<<"$whats_new"; then
  printf '  FAIL  %s\n' "and none of them leaves an empty bullet"; failed=$((failed + 1))
else
  printf '  ok    %s\n' "and none of them leaves an empty bullet"; passed=$((passed + 1))
fi

# --- the grouping. A flat list mixes a new capability with a fix, and a reader looking for what
# broke has to read every line to find out none of them is about that. The category comes from the
# author, beside the sentence they are already writing -- not from a label, because a label is a
# second place to keep it and, measured across the sixteen pull requests of the first release, not
# one of them carried `bug` or `enhancement`. Grouping on that would have put everything in one
# bucket while reporting itself grouped.
bucket() {   # the bullets under one heading
  printf '%s\n' "$out" | awk -v h="## $1" '$0 == h { inside = 1; next } /^## / { inside = 0 } inside' | grep -c '^\* '
}
ck_bucket() {
  local name="$1" heading="$2" want="$3" got
  got="$(bucket "$heading")"
  if [ "$got" = "$want" ]; then printf '  ok    %s\n' "$name"; passed=$((passed + 1))
  else printf '  FAIL  %s\n        wanted %s bullet(s) under "## %s", got %s\n' "$name" "$want" "$heading" "$got"; failed=$((failed + 1)); fi
}
ck_bucket "the two new capabilities are under What's new" "What's new" 2
ck_bucket "the fix is under its own heading"              "Fixes"      1
ck_bucket "an unclassified note is filed as neither"      "Other changes" 1
has  "the fix's sentence is carried"      "* Rows deleted at the source while a task was down"
has  "the unclassified one is carried"    "* You can pass a workspace directory to the CLI"
# The pollution guard, and the reason the field could not simply be another line in the section:
# the whole section used to become one bullet, so a Kind line would have ridden into every entry.
hasnt "the Kind line never reaches a bullet" "* **Kind:**"
hasnt "and it is not glued onto a sentence" "fix You can"
hasnt "nor onto the one before it"          "new You can assemble"

# The slots the approver fills. A prompt, never material.
has  "there is a breaking-changes slot"    "Breaking changes"
has  "there is a known-issues slot"        "Known issues"
if grep -qE 'ADR-[0-9]|adr/|plans/|progress/' <<<"$out"; then
  printf '  FAIL  %s\n' "the slots name no ADR and no docs path"; failed=$((failed + 1))
else
  printf '  ok    %s\n' "the slots name no ADR and no docs path"; passed=$((passed + 1))
fi

# Layer 3 is appended by GitHub after this body. Emitting it here would put it in the middle.
hasnt "the generated list is not written here" "## What's Changed"

# The one link out of the body. A relative path here 404s -- a release body is rendered on the
# releases page, not from a file in the tree -- so the case is the whole URL, not the filename.
has "the supported-versions link is in the body" \
    "https://github.com/tapstate/tapstate/blob/HEAD/SECURITY.md#supported-versions"

# An empty range still has to produce a valid body: a release with no harvested note is normal.
empty="$(cd "$repo" && bash "$script" --version 0.4.0 --base HEAD --sha HEAD \
  --macos-req 'Recommended macOS: 15.0 or newer.' --glibc-req 'Recommended glibc: 2.34 or newer.' 2>&1)"
empty_code=$?
if [ "$empty_code" = 0 ] && grep -qF -- "not for production" <<<"$empty" && grep -qF -- "What's new" <<<"$empty"; then
  printf '  ok    %s\n' "an empty range still assembles a body"; passed=$((passed + 1))
else
  printf '  FAIL  an empty range still assembles a body:\n%s\n' "$empty"; failed=$((failed + 1))
fi

# A range whose base does not exist is a mis-wired release, not an empty one.
bad="$(cd "$repo" && bash "$script" --version 0.4.0 --base v9.9.9 --sha HEAD --macos-req a --glibc-req b 2>&1)"
bad_code=$?
if [ "$bad_code" != 0 ] && grep -qF -- "v9.9.9" <<<"$bad"; then
  printf '  ok    %s\n' "an unknown base refuses by name"; passed=$((passed + 1))
else
  printf '  FAIL  an unknown base refuses by name: %s\n' "$bad"; failed=$((failed + 1))
fi

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
