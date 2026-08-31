#!/usr/bin/env bash
# Cases for the zero-agent-footprint gate, one group per mode.
#
# The greens carry the weight here. "A CLAUDE.md is refused" is satisfied by a gate that refuses
# everything; what says the gate is a decision rather than an outage is that README.md and
# CONTRIBUTING.md go through, and that a clean tree does. So each refusal below is paired with the
# nearest thing that must not be refused.
#
# Run it from anywhere. Exits 0 if every case holds.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/no-agent-footprint.sh"
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

commit_msg() { # commit_msg <message>
  n=$((${n:-0} + 1))
  echo "change $n" > "f-$n.txt"
  git add -A && git commit -q -m "$1"
}

expect() { # expect <name> <mode> <want code> <want text>
  local name="$1" mode="$2" want_code="$3" want_text="$4" out code
  bash "$gate" "$mode" > "$scratch/out" 2>&1; code=$?
  out="$(cat "$scratch/out")"
  if [ "$code" = "$want_code" ] && printf '%s' "$out" | grep -qF "$want_text"; then
    printf '  ok    %s\n' "$name"; passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        wanted exit %s containing %s\n        got exit %s: %s\n' \
      "$name" "$want_code" "$want_text" "$code" "$out"
    failed=$((failed + 1))
  fi
}

echo "-- mode: files"
fresh_repo
expect "a clean tree passes"                       files 0 "clean:"
echo readme > README.md; echo contrib > CONTRIBUTING.md; git add -A && git commit -qm docs
expect "README.md and CONTRIBUTING.md pass"        files 0 "clean:"
echo x > CLAUDE.md; git add -A && git commit -qm claude
expect "a CLAUDE.md at the root is refused"        files 1 "agent instruction file"
expect "and the path is named"                     files 1 "./CLAUDE.md"
fresh_repo
mkdir -p docs/deep; echo x > docs/deep/AGENTS.md; git add -A && git commit -qm nested
expect "an AGENTS.md at any depth is refused"      files 1 "docs/deep/AGENTS.md"
fresh_repo
echo x > claude.md; git add -A && git commit -qm lower
expect "the match is case-insensitive"             files 1 "agent instruction file"

echo "-- mode: dir"
fresh_repo
expect "no .claude directory passes"               dir 0 "clean:"
mkdir -p .claude; echo x > .claude/settings.json; git add -A && git commit -qm dotclaude
expect "a .claude directory is refused"            dir 1 ".claude directory found"
fresh_repo
mkdir -p sub/.claude; echo x > sub/.claude/a; git add -A && git commit -qm nested
expect "a nested .claude directory is refused"     dir 1 ".claude directory found"

echo "-- mode: messages"
fresh_repo
commit_msg "an ordinary subject"
EVENT_BEFORE="$(git rev-parse HEAD~1)" EVENT_SHA="$(git rev-parse HEAD)" \
  bash "$gate" messages > "$scratch/out" 2>&1
if grep -qF "clean:" "$scratch/out"; then
  printf '  ok    %s\n' "an ordinary commit message passes"; passed=$((passed + 1))
else
  printf '  FAIL  %s: %s\n' "an ordinary commit message passes" "$(cat "$scratch/out")"; failed=$((failed + 1))
fi

msg_case() { # msg_case <name> <commit message> <want code> <want text>
  local name="$1" message="$2" want_code="$3" want_text="$4" code out
  fresh_repo >/dev/null
  n=0
  commit_msg "$message" >/dev/null
  EVENT_BEFORE="$(git rev-parse HEAD~1)" EVENT_SHA="$(git rev-parse HEAD)" \
    bash "$gate" messages > "$scratch/out" 2>&1; code=$?
  out="$(cat "$scratch/out")"
  if [ "$code" = "$want_code" ] && printf '%s' "$out" | grep -qF "$want_text"; then
    printf '  ok    %s\n' "$name"; passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        wanted exit %s containing %s\n        got exit %s: %s\n' \
      "$name" "$want_code" "$want_text" "$code" "$out"; failed=$((failed + 1))
  fi
}
msg_case "a Co-authored-by Claude footer is refused" \
         "subject

Co-authored-by: Claude <noreply@anthropic.com>" 1 "agent footer found in commit message"
msg_case "a Generated with Claude Code footer is refused" \
         "subject

Generated with [Claude Code](https://claude.com/claude-code)" 1 "agent footer found in commit message"
msg_case "a Co-authored-by a person is NOT refused" \
         "subject

Co-authored-by: A Person <person@example.invalid>" 0 "clean:"

# The PR body is the other half of this mode.
fresh_repo >/dev/null
n=0; commit_msg "plain subject" >/dev/null
PR_BODY="Generated with [Claude Code](https://claude.com/claude-code)" \
EVENT_BEFORE="$(git rev-parse HEAD~1)" EVENT_SHA="$(git rev-parse HEAD)" \
  bash "$gate" messages > "$scratch/out" 2>&1; code=$?
if [ "$code" = 1 ] && grep -qF "agent footer found in the PR body" "$scratch/out"; then
  printf '  ok    %s\n' "a footer in the PR body is refused"; passed=$((passed + 1))
else
  printf '  FAIL  %s: rc=%s %s\n' "a footer in the PR body is refused" "$code" "$(cat "$scratch/out")"; failed=$((failed + 1))
fi
PR_BODY="An ordinary description." \
EVENT_BEFORE="$(git rev-parse HEAD~1)" EVENT_SHA="$(git rev-parse HEAD)" \
  bash "$gate" messages > "$scratch/out" 2>&1; code=$?
if [ "$code" = 0 ]; then
  printf '  ok    %s\n' "an ordinary PR body passes"; passed=$((passed + 1))
else
  printf '  FAIL  %s: rc=%s %s\n' "an ordinary PR body passes" "$code" "$(cat "$scratch/out")"; failed=$((failed + 1))
fi

# --- THE DEFECT, pinned as it behaves TODAY so that changing it has to change this case.
# On a first push to a branch, github.event.before is the all-zero SHA. The range built from it
# does not resolve, `git log` fails, the failure is discarded, `msgs` is empty, and the step
# reports clean having scanned nothing. This is the same shape that was fixed in the sibling
# character check; here it is still live. The case asserts the CURRENT answer, green, and the
# commit that fixes it must flip this assertion -- which is the point of writing it now.
fresh_repo >/dev/null
n=0; commit_msg "subject

Co-authored-by: Claude <noreply@anthropic.com>" >/dev/null
EVENT_BEFORE="0000000000000000000000000000000000000000" EVENT_SHA="$(git rev-parse HEAD)" \
  bash "$gate" messages > "$scratch/out" 2>&1; code=$?
if [ "$code" = 0 ]; then
  printf '  ok    %s\n' "KNOWN DEFECT: a first push scans nothing and reports clean"; passed=$((passed + 1))
else
  printf '  FAIL  %s\n        this case pins today behaviour; if it just changed, flip it: rc=%s\n' \
    "KNOWN DEFECT: a first push scans nothing and reports clean" "$code"; failed=$((failed + 1))
fi

echo "-- liveness controls"
# Same idea, same reason: shadow `find` with a real executable that finds nothing. Without
# the control, both modes would print `clean:` and exit 0 -- which is what a repository with
# no agent files looks like, and is why the two are indistinguishable without this.
fresh_repo
shadow="$scratch/shadow"; mkdir -p "$shadow"
printf '#!/bin/sh\nexit 0\n' > "$shadow/find"
chmod +x "$shadow/find"
for mode in files dir; do
  PATH="$shadow:$PATH" bash "$gate" "$mode" > "$scratch/out" 2>&1; code=$?
  if [ "$code" != 0 ] && grep -qF "did not match a control" "$scratch/out"; then
    printf '  ok    a %s detector that finds nothing at all reddens\n' "$mode"; passed=$((passed + 1))
  else
    printf '  FAIL  a %s detector that finds nothing at all reddens\n        got exit %s: %s\n' \
      "$mode" "$code" "$(cat "$scratch/out")"; failed=$((failed + 1))
  fi
done

echo "-- dispatch"
expect "an unknown mode is a usage error" bogus 2 "usage:"

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ]
