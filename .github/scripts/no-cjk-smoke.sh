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
jose="Jos$(printf '\xC3\xA9') Garc$(printf '\xC3\xADa')"   # U+00E9, U+00ED - neither on the list

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

echo "invocation"

fresh_repo
expect "an unknown mode is refused rather than assumed" bogus 1 "needs a mode"
out="$(bash "$gate" 2>&1)"; code=$?
if [ "$code" = 1 ] && printf '%s' "$out" | grep -qF "needs a mode"; then
  printf '  ok    %s\n' "no mode at all is refused"
  passed=$((passed + 1))
else
  printf '  FAIL  %s\n        got exit %s: %s\n' "no mode at all is refused" "$code" "$out"
  failed=$((failed + 1))
fi

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
