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
  if [ "$code" = "$want_code" ] && grep -qF "$want_text" <<<"$out"; then
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

# --- an IGNORED file is not something this repository carries. Paired with the tracked case
# --- above, which must stay red: the pair is what keeps this narrowing from becoming a hole.
fresh_repo
printf 'CLAUDE.md\n' > .gitignore
git add -A && git commit -qm ignore
echo "personal notes" > CLAUDE.md          # never added: git considers the tree clean
expect "an ignored CLAUDE.md is not carried by the repository" files 0 "clean:"
git add -f CLAUDE.md && git commit -qm forced
expect "but the same file tracked is still refused"          files 1 "agent instruction file"

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
  if [ "$code" = "$want_code" ] && grep -qF "$want_text" <<<"$out"; then
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

# --- THE RANGE, which used to be the hole in this mode.
# On a first push to a branch github.event.before is the all-zero SHA. The range built from
# it resolved to nothing, `git log` failed, the failure was discarded, and the step reported
# clean having read no commit at all -- a required check that went green precisely when it
# could not look. Both cases below were green before the fix; each is the reason the guard
# has the shape it has.
range_case() { # range_case <name> <before>
  local name="$1" before="$2" code
  fresh_repo >/dev/null
  n=0
  commit_msg "subject

Co-authored-by: Claude <noreply@anthropic.com>" >/dev/null
  EVENT_BEFORE="$before" EVENT_SHA="$(git rev-parse HEAD)" \
    bash "$gate" messages > "$scratch/out" 2>&1; code=$?
  if [ "$code" != 0 ] && grep -qF "agent footer found in commit message" "$scratch/out"; then
    printf '  ok    %s\n' "$name"; passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        got exit %s: %s\n' "$name" "$code" "$(cat "$scratch/out")"; failed=$((failed + 1))
  fi
}
range_case "a first push (all-zero before) is still scanned" \
           "0000000000000000000000000000000000000000"
# The same guard, reached the other way: a previous commit this clone does not have. It is
# not a hypothetical -- a force-push leaves `before` naming a commit that no longer exists.
range_case "a before this clone does not have is still scanned" \
           "1234567890123456789012345678901234567890"

# And the failure that must NOT be silent: a range naming a ref that cannot resolve at all
# has to red saying nothing was checked, rather than print clean.
fresh_repo >/dev/null
n=0; commit_msg "plain" >/dev/null
EVENT_NAME=pull_request BASE_REF=no-such-branch \
  bash "$gate" messages > "$scratch/out" 2>&1; code=$?
if [ "$code" != 0 ] && grep -qF "so no commit message was checked" "$scratch/out"; then
  printf '  ok    %s\n' "an unresolvable range reds instead of reporting clean"; passed=$((passed + 1))
else
  printf '  FAIL  %s\n        got exit %s: %s\n' \
    "an unresolvable range reds instead of reporting clean" "$code" "$(cat "$scratch/out")"; failed=$((failed + 1))
fi

echo "-- liveness controls"
# Same idea, same reason: shadow the tool the detector actually uses with a real executable
# that fails. It is `git` since these modes read tracked files -- the earlier version of this
# case shadowed `find`, and kept passing after the detector stopped calling it, which is the
# case testing the wrong binary rather than the gate being right.
#
# Without the control both modes would print `clean:` and exit 0, which is exactly what a
# repository with no agent files looks like.
fresh_repo
shadow="$scratch/shadow"; mkdir -p "$shadow"
printf '#!/bin/sh\necho "git: simulated failure" >&2\nexit 1\n' > "$shadow/git"
chmod +x "$shadow/git"
for mode in files dir; do
  PATH="$shadow:$PATH" bash "$gate" "$mode" > "$scratch/out" 2>&1; code=$?
  # Either cause is a pass -- what must never happen is exit 0 with `clean:`. Both messages
  # name the real problem; asserting on one of them specifically would make this case depend
  # on which guard happens to fire first.
  if [ "$code" != 0 ] && grep -qE 'cannot list the tracked files|did not match a control|could not build the control repository' "$scratch/out"; then
    printf '  ok    a %s detector whose tool cannot run reddens\n' "$mode"; passed=$((passed + 1))
  else
    printf '  FAIL  a %s detector whose tool cannot run reddens\n        got exit %s: %s\n' \
      "$mode" "$code" "$(cat "$scratch/out")"; failed=$((failed + 1))
  fi
done

# A git that can list but cannot init. Two guards can produce a red here -- the control
# repository failing to build, and the tracked listing failing -- and with git broken outright
# only the second one is ever reached, so removing the first changes nothing observable. This
# stub separates them: `ls-files` works, `init` does not, so only the first guard can speak.
fresh_repo
shadow2="$scratch/shadow2"; mkdir -p "$shadow2"
real_git="$(command -v git)"
printf '#!/bin/sh\ncase " $* " in *" init "*) exit 1 ;; esac\nexec %s "$@"\n' "$real_git" > "$shadow2/git"
chmod +x "$shadow2/git"
PATH="$shadow2:$PATH" bash "$gate" files > "$scratch/out" 2>&1; code=$?
if [ "$code" != 0 ] && grep -qF "could not build the control repository" "$scratch/out"; then
  printf '  ok    a control repository that cannot be built reddens, and says so\n'; passed=$((passed + 1))
else
  printf '  FAIL  a control repository that cannot be built reddens, and says so\n        got exit %s: %s\n' \
    "$code" "$(cat "$scratch/out")"; failed=$((failed + 1))
fi

echo "-- dispatch"
expect "an unknown mode is a usage error" bogus 2 "usage:"

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ]
