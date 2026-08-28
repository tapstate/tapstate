#!/usr/bin/env bash
# Cases for the DCO gate, driven against a scratch repository shaped like each one.
#
# The shape that matters most is not "an unsigned commit is refused" — it is everything the gate
# deliberately lets through, because each of those is indistinguishable from the gate being broken.
# A check that admits every internal pull request and every merge commit looks exactly like a check
# that admits everything, right up to the first external contribution nobody signed. So the exempt
# cases are paired here with the refusals that prove the exemption is a decision and not an outage.
#
# Run it from anywhere. Exits 0 if every case holds.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/dco.sh"
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
  git checkout -q -b feature
}

n=0
commit() { # commit <subject> [-s]
  n=$((n + 1))
  echo "change $n" > "feature-$n.txt"
  git add -A
  if [ "${2:-}" = "-s" ]; then git commit -qs -m "$1"; else git commit -q -m "$1"; fi
}

expect() { # expect <name> <fork?> <want code> <want text>
  local name="$1" fork="$2" want_code="$3" want_text="$4"
  local out code
  out="$(PR_IS_FORK="$fork" DCO_RANGE="main..HEAD" bash "$gate" 2>&1)"
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

refute() { # refute <name> <fork?> <unwanted text>
  local name="$1" fork="$2" unwanted="$3" out
  out="$(PR_IS_FORK="$fork" DCO_RANGE="main..HEAD" bash "$gate" 2>&1)"
  if printf '%s' "$out" | grep -qF "$unwanted"; then
    printf '  FAIL  %s\n        did not want %s, got: %s\n' "$name" "$unwanted" "$out"
    failed=$((failed + 1))
  else
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  fi
}

fresh_repo
commit "signed one" -s
commit "signed two" -s
expect "an external pull request with every commit signed passes" true 0 "clean:"

fresh_repo
commit "signed one" -s
commit "forgot the flag"
expect "one unsigned commit is refused" true 1 "forgot the flag"
refute "and the signed one is not named" true "signed one"
expect "the refusal carries the amend command" true 1 "git commit --amend -s"
expect "the refusal carries the rebase command" true 1 "git rebase --signoff"
expect "the refusal points at where it is documented" true 1 "CONTRIBUTING.md"

fresh_repo
commit "forgot the flag"
expect "the same branch inside this repository is not asked" false 0 "not required"

fresh_repo
commit "signed one" -s
git checkout -q main
echo other > on-main.txt
git add -A && git commit -qm "main moved on"
git checkout -q feature
git merge -q --no-ff main -m "Merge branch 'main' into feature"
# Without this the case is vacuous: a merge that conflicted leaves no merge commit, and the gate then
# passes on the signed commit alone while appearing to have exempted something.
git log -1 --format=%p | grep -q ' ' || { echo "  FAIL  the merge case built no merge commit"; exit 1; }
expect "a merge commit the contributor did not author is exempt" true 0 "clean:"

fresh_repo
printf 'x\n' > anon.txt
git add -A
git commit -q -m "a sign-off with no address

Signed-off-by: Anonymous"
expect "a sign-off without an address does not count" true 1 "a sign-off with no address"

fresh_repo
commit "signed by someone else" -s
git commit -q --amend -m "$(git log -1 --format=%s)

Signed-off-by: Someone Else <else@example.invalid>"
expect "signing off a patch received from another person is allowed" true 0 "clean:"

# The failure mode a green cannot be told from: rev-list fails, prints nothing, and a loop over
# nothing finds every commit signed. Worth its own case because it is the one shape where the gate
# reports success for having looked at zero commits.
fresh_repo
commit "signed one" -s
out="$(PR_IS_FORK=true DCO_RANGE="no-such-base..HEAD" bash "$gate" 2>&1)"
code=$?
if [ "$code" = 1 ] && printf '%s' "$out" | grep -qF "nothing was checked"; then
  printf '  ok    %s\n' "an unresolvable range is refused, not reported clean"
  passed=$((passed + 1))
else
  printf '  FAIL  %s\n        got exit %s: %s\n' "an unresolvable range is refused, not reported clean" "$code" "$out"
  failed=$((failed + 1))
fi

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
