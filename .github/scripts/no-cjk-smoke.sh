#!/usr/bin/env bash
# Cases for the character check, driven against scratch repositories shaped like each one.
#
# This check spent two years as inline shell in its workflow, which is why it is the one that
# actually went silently green: a doubled percent sign in a `--format` string made git print the
# format instead of the messages, the commit-message scan covered nothing, and it reported clean
# on every push afterwards. Nothing catches a scan that stopped scanning except a case that is red
# when it does, which is why the cases below seed real offending input rather than only checking
# that a clean repository passes.
#
# Two groups carry most of the weight. The five characters the old CJK-range form let straight
# through - kana, hangul, a Cyrillic look-alike, a zero-width space, a right-to-left override -
# are each their own case, because "the ranges were incomplete" is invisible until something names
# what they missed. And the allow-list's own failures are cases, because a missing allow-list and
# a repository full of unknown characters are both red, and only one of them tells you what broke.
#
# Every character these cases need is written as bytes, never as a literal, because this file is
# itself scanned. A case file that reddens the check it tests is not a case, it is an outage.
#
# Run it from anywhere. Exits 0 if every case holds.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/no-cjk.sh"
real_allowlist="$here/../charset-allowlist.txt"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
passed=0
failed=0

han="$(printf '\xE4\xB8\xAD')"          # U+4E2D   a Chinese ideograph
fullwidth="$(printf '\xEF\xBC\x8C')"    # U+FF0C   full-width comma
kana="$(printf '\xE3\x81\x82')"         # U+3042   HIRAGANA LETTER A
hangul="$(printf '\xED\x95\x9C')"       # U+D55C   HANGUL SYLLABLE HAN
cyrillic="$(printf '\xD0\xB0')"         # U+0430   CYRILLIC SMALL LETTER A, a look-alike for 'a'
zwsp="$(printf '\xE2\x80\x8B')"         # U+200B   ZERO WIDTH SPACE
rtl="$(printf '\xE2\x80\xAE')"          # U+202E   RIGHT-TO-LEFT OVERRIDE
emdash="$(printf '\xE2\x80\x94')"       # U+2014   EM DASH, on the allow-list
# U+00ED is not on the allow-list, and that is what makes the cases below discriminate: with the
# trailer exemption deleted they go red on it. U+00E9 IS on the list - a JSON escaping fixture needs
# it - so it carries none of that weight. Do not reduce this name to the e-acute half.
jose="Jos$(printf '\xC3\xA9') Garc$(printf '\xC3\xADa')"   # U+00E9 (allowed), U+00ED (not)

fresh_repo() {
  rm -rf "${scratch:?}/repo"
  mkdir -p "$scratch/repo/.github"
  cp "$real_allowlist" "$scratch/repo/.github/charset-allowlist.txt"
  cd "$scratch/repo" || exit 1
  git init -q -b main .
  git config user.email cases@example.invalid
  git config user.name "A Contributor"
  echo base > file.txt
  git add -A && git commit -qm base
}

expect() { # expect <name> <mode> <want code> <want text>
  local name="$1" mode="$2" want_code="$3" want_text="$4" out code
  out="$(bash "$gate" "$mode" 2>&1)"
  code=$?
  if [ "$code" = "$want_code" ] && printf '%s' "$out" | grep -qF "$want_text"; then
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        wanted exit %s containing %s\n        got exit %s: %s\n' \
      "$name" "$want_code" "$want_text" "$code" "$out"
    failed=$((failed + 1))
  fi
}

refute() { # refute <name> <mode> <unwanted text>
  local name="$1" mode="$2" unwanted="$3" out
  out="$(bash "$gate" "$mode" 2>&1)"
  if printf '%s' "$out" | grep -qF "$unwanted"; then
    printf '  FAIL  %s\n        did not want %s, got: %s\n' "$name" "$unwanted" "$out"
    failed=$((failed + 1))
  else
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  fi
}

seed_file() { # seed_file <name> <char>
  printf 'a %s b\n' "$2" > "$1"
  git add -A && git commit -qm "seed $1"
}

echo "tracked files"

fresh_repo
expect "an ASCII-only tree passes" files 0 "clean: every character in tracked files"

fresh_repo
seed_file doc.md "$han"
expect "an unknown character is refused" files 1 "not on the allow-list"
expect "and it is named by code point" files 1 "U+4E2D"
expect "and located by file and line" files 1 "doc.md:1"

fresh_repo
seed_file punct.md "$fullwidth"
expect "full-width punctuation is refused" files 1 "U+FF0C"

# The five the CJK-range form admitted. Each is its own case: a range list is incomplete in ways
# only an enumeration makes visible, and these five were what it happened to miss.
fresh_repo; seed_file kana.md "$kana"
expect "Japanese kana is refused" files 1 "U+3042"
fresh_repo; seed_file hangul.md "$hangul"
expect "Korean hangul is refused" files 1 "U+D55C"
fresh_repo; seed_file cyr.md "$cyrillic"
expect "a Cyrillic look-alike is refused" files 1 "U+0430"
fresh_repo; seed_file zwsp.md "$zwsp"
expect "a zero-width space is refused" files 1 "U+200B"
fresh_repo; seed_file rtl.md "$rtl"
expect "a right-to-left override is refused" files 1 "U+202E"

# The other half of the same claim: moving to an allow-list must not cost the repository a single
# existing character. Seeded from the allow-list itself, so it cannot drift away from it.
fresh_repo
perl -CSD -ne 'next if /^\s*(#|$)/; my ($t) = split " "; $t =~ s/^U\+//; print chr(hex($t))' \
  .github/charset-allowlist.txt > legit.txt
printf '\n' >> legit.txt
git add -A && git commit -qm "every allowed character at once"
expect "a file holding every allowed character passes" files 0 "clean: every character in tracked files"

fresh_repo
seed_file doc.md "$han"
echo 'doc.md' > .cjk-allowlist
git add -A && git commit -qm allowlist
expect "a path on the path allow-list is exempt" files 0 "clean: every character in tracked files"

fresh_repo
seed_file doc.md "$han"
printf '# doc.md\n\n' > .cjk-allowlist
git add -A && git commit -qm allowlist
expect "a commented-out path allow-list line exempts nothing" files 1 "doc.md"

fresh_repo
printf 'a %s b\n' "$han" > untracked.md
expect "an untracked file is outside the scan" files 0 "clean: every character in tracked files"

# A name is not in any file's bytes. git grep reads content, so a tree whose file names carried
# kana - or a right-to-left override, which is the whole reason to name that one - was reported
# clean, and every case here seeded content and so none of them could see it.
fresh_repo
printf 'plain ascii content\n' > "kana-${kana}.md"
git add -A && git commit -qm "a file whose name is not ASCII"
expect "an unknown character in a file NAME is refused" files 1 "U+3042"
expect "and the report says it was a name" files 1 "in the name(s) of tracked files"

# The control: the name pass must not redden a tree it has no business reddening. Without it the
# case above is satisfied by a pass that reddens everything, which is the shape the detector
# self-checks exist to catch in the content scan and this pass would otherwise reintroduce.
fresh_repo
seed_file "kana-${kana}.md" b
# Left untracked on purpose: committing it would put the kana into a scanned file's CONTENT, and
# the case would then be red for a reason that has nothing to do with the name it is about.
printf 'kana-%s.md\n' "$kana" > .cjk-allowlist
expect "a name on the path allow-list is exempt like a content match" files 0 "clean: every character in tracked files"

# git reads this one as binary and reports it as `Binary file <path> matches`, with no line number.
# Every report line without one is dropped, so its bytes used to leave no trace at all and the mode
# printed the clean line. It cannot be read as text, so it is named as unscanned instead.
fresh_repo
seed_file asset.bin "$han"
head -c 8 /dev/zero >> asset.bin
git add -A && git commit -qm "a file git reads as binary"
expect "a file that cannot be read as text is refused, not passed over" files 1 "cannot read as text"
expect "and it is named, so it can be reviewed onto the path allow-list" files 1 "asset.bin"

# The path allow-list holds git pathspecs, and a pathspec may hold a space or a hash. Deleting
# every space in the line and cutting at the first hash rewrote both into a different path than the
# reviewer approved - silently, and in the widening direction for the hash.
fresh_repo
seed_file "release notes.md" "$han"
printf 'release notes.md\n' > .cjk-allowlist
git add -A && git commit -qm allowlist
expect "a path allow-list entry holding a space exempts that file" files 0 "clean: every character in tracked files"

fresh_repo
seed_file 'c#-notes.md' "$han"
printf 'c#-notes.md\n' > .cjk-allowlist
git add -A && git commit -qm allowlist
expect "a path allow-list entry is not cut short at a hash" files 0 "clean: every character in tracked files"

fresh_repo
seed_file doc.md "$han"
printf '   # doc.md\n' > .cjk-allowlist
git add -A && git commit -qm allowlist
expect "an indented comment in the path allow-list is still a comment" files 1 "doc.md:1"

# Big enough that the report does not fit in a pipe buffer, which is the only size at which this
# goes wrong. The findings are truncated to fifty and the count of the rest, and the advice line
# after them says what to do about it - and both of those are printed AFTER the truncation, so a
# report long enough to fill the buffer used to end at line fifty with neither. A reader then sees
# exactly fifty findings, no sign that anything was cut, and no advice; the run also ends 141
# rather than 1. Every other fixture here is a handful of lines, so nothing reached it.
fresh_repo
yes "a ${han} b" | head -3000 > big.md
git add -A && git commit -qm "a large finding set"
expect "a report too big for a pipe still says how many were cut" files 1 "and 2950 more"
expect "and still prints the advice after it" files 1 "add the character to"

fresh_repo
base_sha="$(git rev-parse HEAD)"
echo more > file.txt; git add -A
git commit -q -m "$(yes "subject ${han}" | head -3000)"
EVENT_NAME=push EVENT_BEFORE="$base_sha" EVENT_SHA="$(git rev-parse HEAD)" \
  expect "the same holds for a large commit-message report" messages 1 "Rewrite the message"
EVENT_NAME=push EVENT_BEFORE="$base_sha" EVENT_SHA="$(git rev-parse HEAD)" \
  expect "and it says how many findings were cut, as the file report does" messages 1 "and 2950 more"

echo "the allow-list itself"

# A missing allow-list and a repository full of unknown characters are both red. Only one of them
# says what to fix, and letting an empty allow set "naturally" redden everything gives the other.
fresh_repo
rm .github/charset-allowlist.txt
expect "a missing allow-list is refused as itself" files 1 "allow-list not found"
refute "and not reported as unknown characters" files "not on the allow-list"

fresh_repo
: > .github/charset-allowlist.txt
expect "an emptied allow-list is refused as itself" files 1 "below the floor"

fresh_repo
head -30 "$real_allowlist" > .github/charset-allowlist.txt
expect "an allow-list truncated below the floor is refused" files 1 "below the floor"

fresh_repo
printf 'U+2014 em dash\nnot-a-code-point oops\n' > .github/charset-allowlist.txt
expect "an allow-list line that is not a code point is refused" files 1 "not a code point"
expect "and the offending line is quoted back" files 1 "not-a-code-point oops"

echo "commit messages"

fresh_repo
base_sha="$(git rev-parse HEAD)"
echo more > file.txt; git add -A; git commit -qm "an ordinary English subject"
EVENT_NAME=push EVENT_BEFORE="$base_sha" EVENT_SHA="$(git rev-parse HEAD)" \
  expect "English commit messages pass" messages 0 "clean: every character in the commit messages"

# An allowed character has to pass HERE too, not only in a tracked file. The messages in this
# repository do carry em dashes, and until this case existed the only thing pinning that was a
# fixture in the pull-request-body mode - which is gone, because that mode was reading
# conversational text a required check has no business reading.
fresh_repo
base_sha="$(git rev-parse HEAD)"
echo more > file.txt; git add -A; git commit -qm "$(printf 'a subject %s with an em dash' "$emdash")"
EVENT_NAME=push EVENT_BEFORE="$base_sha" EVENT_SHA="$(git rev-parse HEAD)" \
  expect "an allowed character in a commit message passes" messages 0 "clean: every character in the commit messages"

# The case the extraction exists for. Reverting the format string to the doubled-percent form
# makes git emit the format verbatim, so nothing is scanned and this goes green - which is exactly
# what shipped once and reported clean for as long as it was there.
fresh_repo
base_sha="$(git rev-parse HEAD)"
echo more > file.txt; git add -A; git commit -qm "$(printf 'subject with %s in it' "$han")"
EVENT_NAME=push EVENT_BEFORE="$base_sha" EVENT_SHA="$(git rev-parse HEAD)" \
  expect "an unknown character in a commit message is refused" messages 1 "U+4E2D"

# A first push carries the all-zero SHA as the previous commit. The range was built from it
# anyway, git log failed, the failure was discarded, and the scan reported clean having read
# nothing - the same silent green as the format-string defect, by a different route.
fresh_repo
echo more > file.txt; git add -A; git commit -qm "$(printf 'subject with %s in it' "$han")"
EVENT_NAME=push EVENT_BEFORE=0000000000000000000000000000000000000000 EVENT_SHA="$(git rev-parse HEAD)" \
  expect "a first push, whose previous commit is all zeros, is still scanned" messages 1 "U+4E2D"

fresh_repo
git checkout -qb feature
echo more > file.txt; git add -A; git commit -qm "$(printf 'subject with %s in it' "$han")"
git branch -q -f origin/main main
EVENT_NAME=pull_request BASE_REF=main \
  expect "a pull request scans its own commits" messages 1 "U+4E2D"

# The mirror, and the one that proves the range narrows at all: the offending message is behind
# the base, so a check that quietly scanned everything would be red here and indistinguishable
# from one that works.
fresh_repo
echo more > file.txt; git add -A; git commit -qm "$(printf 'subject with %s in it' "$han")"
git branch -q -f origin/main HEAD
git checkout -qb feature
echo yet more > file.txt; git add -A; git commit -qm "an ordinary English subject"
EVENT_NAME=pull_request BASE_REF=main \
  expect "commits already on the base branch are not this pull request's problem" \
  messages 0 "clean: every character in the commit messages"

# A range that cannot be resolved must be refused, not reported clean: a loop over no commits
# finds nothing wrong, which is the shape this whole check keeps being broken into.
fresh_repo
EVENT_NAME=pull_request BASE_REF=no-such-branch \
  expect "an unresolvable range is refused, not reported clean" messages 1 "no commit message was checked"

echo "the detector itself"

# A pattern that flags nothing and a clean repository produce the same output; so do a pattern
# that flags everything and a repository full of unknown characters. The third control is what the
# allow-list form adds: a list that was read but never reached the pattern reddens every
# legitimate character in the repository, which no amount of scanning can tell from real breakage.
fresh_repo
CHARSET_UNKNOWN_PATTERN='zzzz-matches-nothing' \
  expect "a pattern that flags nothing refuses to report" files 1 "did not flag a character that is not on the allow-list"
CHARSET_UNKNOWN_PATTERN='.' \
  expect "a pattern that flags plain ASCII refuses to report" files 1 "flagged plain ASCII"
CHARSET_UNKNOWN_PATTERN='[^\x00-\x7F]' \
  expect "a pattern the allow-list never reached refuses to report" files 1 "never reached the pattern"

echo "author and sign-off lines"

# A person's name is not ours to constrain, and this project asks contributors to sign their
# commits - so a check that reddened on the name they signed with would turn that into a door.
# U+00ED is deliberately not on the allow-list: if it were, these cases would pass for the wrong
# reason and would keep passing after the exemption was deleted.
fresh_repo
echo more > file.txt; git add -A
git commit -q -m "an ordinary English subject

Signed-off-by: ${jose} <jose@example.invalid>"
base_sha="$(git rev-parse HEAD~1)"
EVENT_NAME=push EVENT_BEFORE="$base_sha" EVENT_SHA="$(git rev-parse HEAD)" \
  expect "a sign-off in any script passes" messages 0 "clean: every character in the commit messages"

fresh_repo
echo more > file.txt; git add -A
git commit -q -m "an ordinary English subject

Co-authored-by: ${jose} <jose@example.invalid>"
base_sha="$(git rev-parse HEAD~1)"
EVENT_NAME=push EVENT_BEFORE="$base_sha" EVENT_SHA="$(git rev-parse HEAD)" \
  expect "a co-author trailer in any script passes" messages 0 "clean: every character in the commit messages"

fresh_repo
echo more > file.txt; git add -A
git commit -q -m "an ordinary English subject

signed-off-by: ${jose} <jose@example.invalid>"
base_sha="$(git rev-parse HEAD~1)"
EVENT_NAME=push EVENT_BEFORE="$base_sha" EVENT_SHA="$(git rev-parse HEAD)" \
  expect "the trailer is recognised whatever its capitalisation" messages 0 "clean: every character in the commit messages"

# The negative half, and the reason the exemption is by line shape rather than by adding these
# characters to the allow-list: the same name in the message body is still refused.
fresh_repo
echo more > file.txt; git add -A
git commit -q -m "a subject mentioning ${jose} in prose"
base_sha="$(git rev-parse HEAD~1)"
EVENT_NAME=push EVENT_BEFORE="$base_sha" EVENT_SHA="$(git rev-parse HEAD)" \
  expect "the same name in the message body is still refused" messages 1 "U+00ED"

# The exemption is anchored at the start of the line. Unanchored, a sentence that merely mentions
# the trailer would exempt itself - and prose is exactly where someone would mention it.
fresh_repo
echo more > file.txt; git add -A
git commit -q -m "I added a Signed-off-by: line for ${jose}"
base_sha="$(git rev-parse HEAD~1)"
EVENT_NAME=push EVENT_BEFORE="$base_sha" EVENT_SHA="$(git rev-parse HEAD)" \
  expect "prose that merely mentions the trailer is not exempt" messages 1 "U+00ED"

fresh_repo
printf 'Signed-off-by: %s <jose@example.invalid>\n' "$jose" > patch.txt
git add -A && git commit -qm "a file holding a sign-off line"
expect "a sign-off line inside a tracked file passes" files 0 "clean: every character in tracked files"

fresh_repo
printf 'Signed-off-by: %s <jose@example.invalid>\nand %s again, in prose\n' "$jose" "$jose" > patch.txt
git add -A && git commit -qm "a file holding a sign-off line and prose"
expect "and the exemption does not leak to the next line" files 1 "patch.txt:2"

fresh_repo
printf '%s <jose@example.invalid>\n' "$jose" > AUTHORS
git add -A && git commit -qm "AUTHORS"
expect "the AUTHORS file carries names in any script" files 0 "clean: every character in tracked files"

# Exactly that path, not anything starting with it: a wildcard here would exempt whatever someone
# names AUTHORS-something, which is a different decision from the one that was made.
fresh_repo
printf '%s <jose@example.invalid>\n' "$jose" > AUTHORS.md
git add -A && git commit -qm "AUTHORS.md"
expect "a file merely named like AUTHORS is not exempt" files 1 "AUTHORS.md:1"

echo "the workflow wiring"

# The one invariant this check inherits rather than decides: a required check named for characters
# reads tracked files and commit messages, and nothing else. Both repositories had a step reading
# pull request text - one the body, narrowed to same-repository pull requests, the other title and
# body both - and both are gone. Nothing but this stops one coming back, and it comes back easily,
# because reading the body is the obvious way to check a pull request's own text.
#
# It judges TEXT, not behaviour, and saying so is part of the case. Somebody who copies the field
# into an environment variable in one step and uses it in another goes around this, and no grep can
# tell. What it buys is that the direct way back in is red.
#
# Scope is the workflow that RUNS this check, located by looking for it rather than named here, so
# the same script makes the same statement in a repository whose workflow is called something else.
# That narrowing carries weight in the other direction too: this repository has workflows that read
# pull request text for their own reasons, and none of them is this check.

workflow_dir() { rm -rf wf && mkdir -p wf; }

clean_gate_workflow() {
  cat > wf/no-cjk.yml <<'YML'
name: no-cjk
on: [push, pull_request]
jobs:
  no-cjk:
    steps:
      - run: bash .github/scripts/no-cjk.sh files
      - run: bash .github/scripts/no-cjk.sh messages
YML
}

fresh_repo; workflow_dir; clean_gate_workflow
WORKFLOW_DIR=wf expect "a character check that reads no pull request text passes" \
  workflows 0 "clean: no workflow that runs the character check"

fresh_repo; workflow_dir
cat > wf/no-cjk.yml <<'YML'
name: no-cjk
on: [push, pull_request]
jobs:
  no-cjk:
    steps:
      - run: bash .github/scripts/no-cjk.sh files
      - env:
          PR_BODY: ${{ github.event.pull_request.body }}
        run: printf '%s' "$PR_BODY" | bash .github/scripts/no-cjk.sh text
YML
WORKFLOW_DIR=wf expect "a step reading the pull request body is refused" workflows 1 "pull_request.body"
WORKFLOW_DIR=wf expect "and the file holding it is named" workflows 1 "wf/no-cjk.yml"

fresh_repo; workflow_dir
cat > wf/no-cjk.yml <<'YML'
name: no-cjk
on: [push, pull_request]
jobs:
  no-cjk:
    steps:
      - env:
          PR_TITLE: ${{ github.event.pull_request.title }}
        run: printf '%s' "$PR_TITLE" | bash .github/scripts/no-cjk.sh text
YML
WORKFLOW_DIR=wf expect "a step reading the pull request title is refused" workflows 1 "pull_request.title"

# The narrowing, as its own case. Without it this mode reddens every workflow that reads a pull
# request for a reason of its own - the template check and the agent-footprint check both do - and
# a mode that cannot be satisfied gets deleted rather than obeyed.
fresh_repo; workflow_dir; clean_gate_workflow
cat > wf/pr-template.yml <<'YML'
name: pr-template
on: pull_request
jobs:
  pr-template:
    steps:
      - env:
          PR_BODY: ${{ github.event.pull_request.body }}
        run: bash .github/scripts/pr-template.sh
YML
WORKFLOW_DIR=wf expect "a workflow that is not this check may read pull request text" \
  workflows 0 "clean: no workflow that runs the character check"

# A comment cannot read anything. Without this the first person to write down WHY this workflow does
# not read the body reddens the check by saying so - and the quickest way out of that is to delete
# the check, which is how a gate that cannot be satisfied dies. Whole-line comments only: text after
# a `run:` line is shell, not YAML, and is not treated as a comment here.
fresh_repo; workflow_dir
cat > wf/no-cjk.yml <<'YML'
name: no-cjk
on: [push, pull_request]
# This check deliberately does not read ${{ github.event.pull_request.body }}: a title and a body
# are conversation, and they are refused before they are published.
jobs:
  no-cjk:
    steps:
      - run: bash .github/scripts/no-cjk.sh files
YML
WORKFLOW_DIR=wf expect "a comment saying the field is not read is not a use of it" \
  workflows 0 "clean: no workflow that runs the character check"

# The anti-hollow control, and the reason this mode is not simply a grep. Its scope is found by
# looking, so a repository where the wiring was renamed away has nothing in scope - and "no
# workflow reads pull request text" would then be true of an empty set and print the same green.
fresh_repo; workflow_dir
cat > wf/build.yml <<'YML'
name: build
on: [push]
jobs:
  build:
    steps:
      - run: mvn -B verify
YML
WORKFLOW_DIR=wf expect "a workflow directory that no longer runs this check is refused, not passed" \
  workflows 1 "nothing to check"

fresh_repo
WORKFLOW_DIR=no-such-dir expect "a missing workflow directory is refused" workflows 1 "no workflow directory"

# The regression guard proper: this repository's own workflows, as they stand right now. Every case
# above runs against a fixture, and a fixture cannot go red when somebody edits the real thing.
fresh_repo
WORKFLOW_DIR="$here/../workflows" expect "this repository's own workflows pass" \
  workflows 0 "clean: no workflow that runs the character check"

echo "invocation"

fresh_repo
expect "an unknown mode is refused rather than assumed" bogus 1 "needs a mode"
expect "the mode that read the pull request body is gone" pr-body 1 "needs a mode"
out="$(bash "$gate" 2>&1)"; code=$?
if [ "$code" = 1 ] && printf '%s' "$out" | grep -qF "needs a mode"; then
  printf '  ok    %s\n' "no mode at all is refused"
  passed=$((passed + 1))
else
  printf '  FAIL  %s\n        got exit %s: %s\n' "no mode at all is refused" "$code" "$out"
  failed=$((failed + 1))
fi

# git grep says 1 for "nothing matched" and 128 for "there is no work tree here". Both used to be
# discarded alike, so a scan that never ran printed the same clean line as a clean tree - which is
# what the commit-message mode refuses by name, and what this mode did anyway. Driven from outside
# a repository because that is the one trigger needing no fixture: a build without PCRE and a
# pathspec git cannot parse arrive at the same place.
mkdir -p "$scratch/norepo" && cd "$scratch/norepo" || exit 1
printf 'a %s b\n' "$han" > doc.md
out="$(CHARSET_ALLOWLIST="$real_allowlist" bash "$gate" files 2>&1)"; code=$?
if [ "$code" = 1 ] && printf '%s' "$out" | grep -qF "could not run"; then
  printf '  ok    %s\n' "a scan that could not run is refused, not reported clean"
  passed=$((passed + 1))
else
  printf '  FAIL  %s\n        got exit %s: %s\n' \
    "a scan that could not run is refused, not reported clean" "$code" "$out"
  failed=$((failed + 1))
fi

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
