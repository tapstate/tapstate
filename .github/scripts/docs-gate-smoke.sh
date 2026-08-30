#!/usr/bin/env bash
# Cases for the release-time documentation gate.
#
# It asks the same question of every pull request in a release range that the pull-request check
# asked of each one as it opened, plus the half that only exists later: for the ones that said
# documentation was needed, was it actually written. Two strictnesses, chosen by the release's own
# bump, and the pair that proves they are two is a `docs-needed` pull request whose follow-up issue
# is still open -- it must pass a patch and fail a minor. Without that pair a gate that is simply
# always strict, or always lax, passes every case here.
#
# Builds its own repository. The inherited git environment is cleared first: a script that makes its
# own repository and is ever started from a hook takes refs and remotes from the repository being
# pushed while its working tree is the sandbox.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PREFIX GIT_QUARANTINE_PATH
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
script="$here/docs-gate.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
export SMOKE_SCRATCH="$scratch"
mkdir -p "$scratch/bin" "$scratch/pr" "$scratch/issue"
passed=0
failed=0

# `gh pr view` answers the staged pull request as JSON; `gh issue list` answers whatever follow-up
# issue was staged for that pull request's URL, and an empty list where none was -- which is what
# "the label was added and the issue was never opened" really looks like.
cat > "$scratch/bin/gh" <<'STUB'
#!/usr/bin/env bash
case "$1" in
  pr)
    for a in "$@"; do case "$a" in [0-9]*) n="$a"; break ;; esac; done
    [ -f "$SMOKE_SCRATCH/pr/$n" ] || exit 1
    cat "$SMOKE_SCRATCH/pr/$n" ;;
  issue)
    for a in "$@"; do case "$a" in *"/pull/"*) u="${a##*/pull/}" ;; esac; done
    cat "$SMOKE_SCRATCH/issue/${u:-none}" 2>/dev/null || echo '[]' ;;
  *) exit 1 ;;
esac
STUB
chmod +x "$scratch/bin/gh"
PATH="$scratch/bin:$PATH"
export PATH
export GITHUB_REPOSITORY=tapstate/tapstate

# A pull request as the gate reads it: body, labels, author, url.
pr() {   # number, draft field, public field, labels, [author]
  printf '{"body":%s,"labels":[%s],"author":{"login":"%s"},"url":"https://github.com/tapstate/tapstate/pull/%s"}\n' \
    "$(printf '## Documentation impact\n\n- **Draft in this repository:** %s\n- **Public page it is headed for:** %s\n' "$2" "$3" \
       | jq -Rs .)" \
    "$(printf '%s' "$4" | awk -F, 'NF{for(i=1;i<=NF;i++){printf "%s{\"name\":\"%s\"}", (i>1?",":""), $i}}')" \
    "${5:-someone}" "$1" > "$scratch/pr/$1"
}
followup() { printf '[{"number":7,"state":"%s"}]\n' "$2" > "$scratch/issue/$1"; }

prompt='<!-- path under docs/, or "none" -->'

pr 21 "$prompt" "$prompt" ""                        # answered nothing
pr 22 none none ""                                   # judged: nothing to document
pr 23 docs/a.md https://tapstate.dev/docs/a docs-needed
pr 24 docs/b.md https://tapstate.dev/docs/b docs-needed
followup 23 OPEN
followup 24 CLOSED

repo="$scratch/repo"; mkdir -p "$repo"
git -C "$repo" init -q -b main
git -C "$repo" config user.email t@example.com; git -C "$repo" config user.name t
echo base > "$repo/f"; git -C "$repo" add f; git -C "$repo" commit -q -m base; git -C "$repo" tag v0.3.0
seed() { echo "$RANDOM" > "$repo/f"; git -C "$repo" add f; git -C "$repo" commit -q -m "$1"; }

run() { ( cd "$repo" && bash "$script" --base v0.3.0 --sha HEAD --bump "$1" 2>&1 ); }

expect() {   # name, bump, want exit, text
  local out code
  out="$(run "$2")"; code=$?
  if [ "$code" = "$3" ] && printf '%s' "$out" | grep -qF -- "$4"; then
    printf '  ok    %s\n' "$1"; passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        wanted exit %s containing %s\n        got exit %s: %s\n' "$1" "$3" "$4" "$code" "$out"
    failed=$((failed + 1))
  fi
}
refute() {   # name, bump, text
  local out
  out="$(run "$2")"
  if printf '%s' "$out" | grep -qF -- "$3"; then
    printf '  FAIL  %s\n        did not want %s, got: %s\n' "$1" "$3" "$out"; failed=$((failed + 1))
  else
    printf '  ok    %s\n' "$1"; passed=$((passed + 1))
  fi
}

# The pair that carries the gate: identical to anything reading labels, opposite verdicts.
seed "Refactor (#22)"
expect "a judged none passes a patch"              patch 0 "clean:"
expect "and passes a minor"                        minor 0 "clean:"

seed "Add a thing (#21)"
expect "an unanswered section blocks a patch"      patch 1 "#21"
expect "and blocks a minor"                        minor 1 "#21"
expect "and it says which of the two it is"        patch 1 "answered neither"
refute "the judged one is not named alongside it"  patch "#22"

# The strictness split. Same pull request, same follow-up issue, two answers.
rm -f "$scratch/pr/21"; git -C "$repo" reset -q --hard HEAD~1
seed "Assemble across sources (#23)"
expect "an open follow-up passes a patch"          patch 0 "clean:"
expect "and blocks a minor"                        minor 1 "#23"
expect "the minor refusal says the issue is open"  minor 1 "still open"

seed "Another (#24)"
expect "a closed follow-up passes a minor"         minor 1 "#23"
refute "and #24 is not what blocked it"            minor "#24 "

git -C "$repo" reset -q --hard HEAD~2
seed "Another (#24)"
expect "a closed follow-up passes a minor on its own" minor 0 "clean:"

# The label was added and the issue was never opened -- nothing in this repository reports that.
rm -f "$scratch/issue/24"
expect "a missing follow-up issue blocks a minor"  minor 1 "no follow-up issue"
expect "and blocks a patch too"                    patch 1 "no follow-up issue"

# An empty range is a real release shape, not a pass to be manufactured.
git -C "$repo" reset -q --hard v0.3.0
expect "an empty range passes and says it was empty" minor 0 "no pull requests"

git -C "$repo" reset -q --hard v0.3.0
seed "Bump (#99)"
expect "a number that is not a pull request here is skipped" minor 0 "clean:"

expect "an unknown bump is a usage error"          sideways 2 "--bump"

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
