#!/usr/bin/env bash
# Cases for the zero-binary-bytes gate, driven against a scratch repository shaped like each one.
#
# The case that matters most is not "a NUL is refused" -- it is everything the gate lets through,
# because each of those is indistinguishable from the gate being broken. `git grep` answers "no
# match" and "I could not run" with the same exit status, so a clean tree and a detector that
# stopped working print the same `clean:` line. Every green case below is therefore paired with a
# red one that proves the green was a decision rather than an outage.
#
# Run it from anywhere. Exits 0 if every case holds.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/no-binary-bytes.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
passed=0
failed=0

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

# A NUL byte cannot be written by a here-doc or by `echo`: the shell terminates its own
# strings on it. printf with an explicit octal escape is the only portable way in.
seed_nul() { # seed_nul <path>
  # mkdir first. Without it the redirect fails, the file never exists, and every
  # "allowlisted path is excluded" case goes green with nothing to find -- a fixture
  # bug that reads exactly like a working allowlist. It was the paired red case that
  # caught it, which is the reason each green here has one.
  mkdir -p "$(dirname "$1")"
  printf 'before\000after\n' > "$1"
  git add -A && git commit -qm "seed $1" >/dev/null 2>&1
  [ -s "$1" ] || { printf '  FAIL  fixture: %s was not created\n' "$1"; exit 1; }
}

expect() { # expect <name> <want code> <want text>
  local name="$1" want_code="$2" want_text="$3" out code
  bash "$gate" > "$scratch/out" 2>&1; code=$?
  out="$(tr -d '\000' < "$scratch/out")"
  if [ "$code" = "$want_code" ] && printf '%s' "$out" | grep -qF "$want_text"; then
    printf '  ok    %s\n' "$name"; passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        wanted exit %s containing %s\n        got exit %s: %s\n' \
      "$name" "$want_code" "$want_text" "$code" "$out"
    failed=$((failed + 1))
  fi
}

# --- a clean tree passes. Paired with the case below: on its own it is also what a
# --- detector that cannot run at all would print.
fresh_repo
expect "a tree with no NUL byte passes" 0 "clean:"

# --- a tracked NUL is refused, and the file is named. Without this, the case above
# --- passes just as well against a gate that always says clean.
seed_nul tainted.txt
expect "a tracked NUL byte is refused" 1 "NUL byte(s) found"
expect "and the offending file is named" 1 "tainted.txt"

# --- an UNTRACKED file is out of scope on purpose: this gate is about what the
# --- repository carries. Asserting it proves the green above is scoped, not blind.
fresh_repo
printf 'before\000after\n' > untracked.txt
expect "an untracked NUL byte is not this gate's business" 0 "clean:"

# --- the allowlist excludes a path...
fresh_repo
seed_nul assets/logo.bin
printf 'assets/logo.bin\n' > .binary-allowlist
git add -A && git commit -qm allowlist >/dev/null 2>&1
expect "an allowlisted path is excluded" 0 "clean:"

# --- ...and removing that line brings the red back. This is the discriminating half:
# --- without it, "allowlist works" is also satisfied by a gate that stopped scanning.
printf '\n' > .binary-allowlist
git add -A && git commit -qm empty-allowlist >/dev/null 2>&1
expect "removing the allowlist line reddens it again" 1 "NUL byte(s) found"

# --- comments and blank lines are stripped rather than turned into pathspecs. A `#`
# --- comment that survived would become a literal path that excludes nothing, and the
# --- symptom is a red nobody can explain -- so assert the parse, not just the outcome.
printf '# assets are allowed\n\n   assets/logo.bin   \n' > .binary-allowlist
git add -A && git commit -qm commented-allowlist >/dev/null 2>&1
expect "comments and whitespace in the allowlist are ignored" 0 "clean:"

# --- the allowlist path is a parameter, so a vendored copy can point elsewhere.
fresh_repo
seed_nul assets/logo.bin
printf 'assets/logo.bin\n' > elsewhere.txt
git add -A && git commit -qm alt >/dev/null 2>&1
BINARY_ALLOWLIST=elsewhere.txt bash "$gate" > "$scratch/out" 2>&1; code=$?
out="$(tr -d '\000' < "$scratch/out")"
if [ "$code" = 0 ]; then
  printf '  ok    %s\n' "BINARY_ALLOWLIST relocates the allowlist"; passed=$((passed + 1))
else
  printf '  FAIL  %s\n        got exit %s: %s\n' "BINARY_ALLOWLIST relocates the allowlist" "$code" "$out"
  failed=$((failed + 1))
fi

# --- the liveness control fires when the matcher cannot run at all.
# Shadowed with a real executable that fails, not by trimming PATH: a PATH entry that does
# not exist makes the shell fall through to the real binary, so the test would quietly
# measure nothing. This is the failure the control exists for -- a git without PCRE support
# exits non-zero with a message, which the scan alone would read as "no match, all clean".
fresh_repo
shadow="$scratch/shadow"; mkdir -p "$shadow"
printf '#!/bin/sh\necho "git: simulated failure" >&2\nexit 1\n' > "$shadow/git"
chmod +x "$shadow/git"
PATH="$shadow:$PATH" bash "$gate" > "$scratch/out" 2>&1; code=$?
out="$(tr -d '\000' < "$scratch/out")"
if [ "$code" != 0 ] && printf '%s' "$out" | grep -qF "did not match a control file"; then
  printf '  ok    %s\n' "a matcher that cannot run reddens instead of reporting clean"; passed=$((passed + 1))
else
  printf '  FAIL  %s\n        got exit %s: %s\n' \
    "a matcher that cannot run reddens instead of reporting clean" "$code" "$out"; failed=$((failed + 1))
fi

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ]
