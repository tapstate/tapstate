#!/usr/bin/env bash
# Cases for the no-CJK gate, driven against scratch repositories shaped like each one.
#
# This gate spent two years as inline shell in its workflow, which is why it is the one that
# actually went silently green: a doubled percent sign in a `--format` string made git print the
# format instead of the messages, the commit-message scan covered nothing, and it reported clean
# on every push afterwards. Nothing here catches a scan that stops scanning except a case that is
# red when it does - which is what the commit-message cases below are, and why they seed a real
# CJK message rather than only checking that a clean repository passes.
#
# Every CJK character these cases need is written as bytes, never as a literal, because the gate
# also scans this file. A case file that trips the gate it tests is not a case, it is an outage.
#
# Run it from anywhere. Exits 0 if every case holds.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/no-cjk.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
passed=0
failed=0

han="$(printf '\xE4\xB8\xAD')"          # U+4E2D, a Chinese ideograph
fullwidth="$(printf '\xEF\xBC\x8C')"    # U+FF0C, a full-width comma

fresh_repo() {
  rm -rf "${scratch:?}/repo"
  mkdir -p "$scratch/repo"
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

echo "tracked files"

fresh_repo
expect "an ASCII-only tree passes" files 0 "clean: no CJK in tracked files."

fresh_repo
printf 'title %s here\n' "$han" > doc.md
git add -A && git commit -qm "add doc"
expect "a CJK ideograph in a tracked file is refused" files 1 "CJK character(s) found"
expect "and the refusal names the file it is in" files 1 "doc.md"

fresh_repo
printf 'a%sb\n' "$fullwidth" > punct.md
git add -A && git commit -qm "add punct"
expect "full-width punctuation is refused too" files 1 "punct.md"

fresh_repo
printf 'title %s here\n' "$han" > doc.md
echo 'doc.md' > .cjk-allowlist
git add -A && git commit -qm "add doc and allowlist"
expect "a path listed in .cjk-allowlist is exempt" files 0 "clean: no CJK in tracked files."

fresh_repo
printf 'title %s here\n' "$han" > doc.md
printf '# doc.md\n\n' > .cjk-allowlist
git add -A && git commit -qm "add doc and a commented allowlist"
expect "a commented-out allowlist line exempts nothing" files 1 "doc.md"

# Without this one, an allowlist that had silently swallowed every path would look identical to a
# clean tree: both print the same line. This seeds a file the gate must not be looking at anyway.
fresh_repo
printf 'title %s here\n' "$han" > untracked.md
expect "an untracked file is outside the scan" files 0 "clean: no CJK in tracked files."

echo "commit messages"

fresh_repo
base_sha="$(git rev-parse HEAD)"
echo more > file.txt
git add -A && git commit -qm "an ordinary English subject"
EVENT_NAME=push EVENT_BEFORE="$base_sha" EVENT_SHA="$(git rev-parse HEAD)" \
  expect "English commit messages pass" messages 0 "clean: no CJK in commit messages."

# The case the whole extraction exists for. Reverting the format string to the doubled-percent
# form makes git emit the format verbatim, so `msgs` holds no CJK and this goes green - which is
# exactly what shipped once and reported clean for as long as it was there.
fresh_repo
base_sha="$(git rev-parse HEAD)"
echo more > file.txt
git add -A && git commit -qm "$(printf 'subject with %s in it' "$han")"
EVENT_NAME=push EVENT_BEFORE="$base_sha" EVENT_SHA="$(git rev-parse HEAD)" \
  expect "a CJK commit message is refused" messages 1 "CJK found in commit message(s)"

fresh_repo
git checkout -qb feature
echo more > file.txt
git add -A && git commit -qm "$(printf 'subject with %s in it' "$han")"
git branch -q -f origin/main main
EVENT_NAME=pull_request BASE_REF=main \
  expect "a pull request scans its own commits" messages 1 "CJK found in commit message(s)"

# The mirror, and the one that proves the range narrows at all: the offending message is behind
# the base, so a gate that quietly scanned everything would be red here and indistinguishable
# from one that works.
fresh_repo
echo more > file.txt
git add -A && git commit -qm "$(printf 'subject with %s in it' "$han")"
git branch -q -f origin/main HEAD
git checkout -qb feature
echo yet more > file.txt
git add -A && git commit -qm "an ordinary English subject"
EVENT_NAME=pull_request BASE_REF=main \
  expect "commits already on the base branch are not this pull request's problem" \
  messages 0 "clean: no CJK in commit messages."

echo "pull request body"

fresh_repo
PR_BODY="$(printf 'body with %s in it' "$han")" \
  expect "a CJK pull request body is refused" pr-body 1 "CJK found in the pull request body."
PR_BODY="an ordinary English body" \
  expect "an English pull request body passes" pr-body 0 "clean: no CJK in the pull request body."
PR_BODY="" \
  expect "an empty pull request body passes" pr-body 0 "clean: no CJK in the pull request body."

echo "the detector itself"

# A detector that matches nothing and a clean repository produce the same output; so do a detector
# that matches everything and a repository full of CJK. Neither is something a later step can tell
# from the truth, so the gate answers on two known values before it is trusted on unknown ones.
fresh_repo
CJK='zzzz-matches-nothing' \
  expect "a detector that misses known CJK refuses to report" files 1 "missed a known CJK control value"
# It has to match the CJK probe too, or it trips the first check and never reaches this one.
CJK='.' \
  expect "a detector that matches ASCII refuses to report" files 1 "rejected an ASCII control value"

# It guards every mode, not only the one it was copied from. pr-body with a clean body is the
# shape where a broken detector would otherwise pass with nothing to show for it.
PR_BODY="an ordinary English body" CJK='zzzz-matches-nothing' \
  expect "the detector is checked in every mode" pr-body 1 "missed a known CJK control value"

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
