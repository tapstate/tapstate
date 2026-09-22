#!/usr/bin/env bash
# Cases for the release-time documentation gate.
#
# It asks the same question of every pull request in a release range that the pull-request check
# asked of each one as it opened, plus the half that only exists later: for the ones that said
# documentation was needed, was it actually written. The pair that carries this file is a
# `docs-needed` pull request whose follow-up issue is still open -- it must pass a minor and fail a
# major. Without that pair a gate that is simply always strict, or always lax, passes every case
# here.
#
# The second pair is the one that pays for the first. A minor that does not refuse must still say
# what it let past, so every "passes" case about an open follow-up is checked twice: the exit status,
# and the line it wrote to --list-open. A gate that quietly dropped the list would pass on exit
# status alone, and the release would go out with an issue that asks the documentation owner for
# nothing in particular.
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
# One element of what `gh issue list` answers. A follow-up is identified by the link in its body:
# `docs-followup.yml` opens every one of them with a line naming the merged pull request's URL, and
# that link is the only thing about the issue nobody edits into pointing somewhere else.
entry() {   # issue number, state, the pull request its body links
  printf '{"number":%s,"state":"%s","body":"Documentation follow-up for merged PR https://github.com/tapstate/tapstate/pull/%s -- Title"}' \
    "$1" "$2" "$3"
}
# An issue the search returns that is somebody else's follow-up: it came back because its prose
# mentions the queried number, which is what a tokenized search matches on.
decoy() {   # issue number, state, the pull request its body links, the number it merely mentions
  printf '{"number":%s,"state":"%s","body":"Documentation follow-up for merged PR https://github.com/tapstate/tapstate/pull/%s -- the original feature PR #%s has also been corrected"}' \
    "$1" "$2" "$3" "$4"
}
followup() { printf '[%s]\n' "$(entry 7 "$2" "$1")" > "$scratch/issue/$1"; }

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

run() { ( cd "$repo" && bash "$script" --base v0.3.0 --sha HEAD --bump "$1" \
            --list-open "$scratch/open.tsv" 2>&1 ); }
run_report() { ( cd "$repo" && bash "$script" --base v0.3.0 --sha HEAD --report-only \
            --list-open "$scratch/open.tsv" 2>&1 ); }

expect() {   # name, bump, want exit, text
  local out code
  out="$(run "$2")"; code=$?
  if [ "$code" = "$3" ] && grep -qF -- "$4" <<<"$out"; then
    printf '  ok    %s\n' "$1"; passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        wanted exit %s containing %s\n        got exit %s: %s\n' "$1" "$3" "$4" "$code" "$out"
    failed=$((failed + 1))
  fi
}
expect_report() {   # name, want exit, text
  local out code
  out="$(run_report)"; code=$?
  if [ "$code" = "$2" ] && grep -qF -- "$3" <<<"$out"; then
    printf '  ok    %s\n' "$1"; passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        wanted exit %s containing %s\n        got exit %s: %s\n' "$1" "$2" "$3" "$code" "$out"
    failed=$((failed + 1))
  fi
}
# What the last run wrote to --list-open. Asserted separately from the exit status because on the
# bumps that do not refuse it is the only thing that carries the answer anywhere.
listed() {   # name, pull request, issue
  local want; want="$(printf '%s\t%s\t' "$2" "$3")"
  if grep -qF -- "$want" "$scratch/open.tsv" 2>/dev/null; then
    printf '  ok    %s\n' "$1"; passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        no "%s %s" line in: %s\n' "$1" "$2" "$3" "$(cat "$scratch/open.tsv" 2>&1)"
    failed=$((failed + 1))
  fi
}
listed_nothing() {   # name
  if [ -s "$scratch/open.tsv" ]; then
    printf '  FAIL  %s\n        wanted an empty list, got: %s\n' "$1" "$(cat "$scratch/open.tsv")"
    failed=$((failed + 1))
  else
    printf '  ok    %s\n' "$1"; passed=$((passed + 1))
  fi
}
# A usage error, which is what the two modes answer each other with.
refuses() {   # name, text, args...
  local name="$1" want="$2"; shift 2
  local out code
  out="$( ( cd "$repo" && bash "$script" "$@" 2>&1 ) )"; code=$?
  if [ "$code" = 2 ] && grep -qF -- "$want" <<<"$out"; then
    printf '  ok    %s\n' "$name"; passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        wanted exit 2 containing %s\n        got exit %s: %s\n' "$name" "$want" "$code" "$out"
    failed=$((failed + 1))
  fi
}
refute() {   # name, bump, text
  local out
  out="$(run "$2")"
  if grep -qF -- "$3" <<<"$out"; then
    printf '  FAIL  %s\n        did not want %s, got: %s\n' "$1" "$3" "$out"; failed=$((failed + 1))
  else
    printf '  ok    %s\n' "$1"; passed=$((passed + 1))
  fi
}
refute_report() {   # name, text
  local out
  out="$(run_report)"
  if grep -qF -- "$2" <<<"$out"; then
    printf '  FAIL  %s\n        did not want %s, got: %s\n' "$1" "$2" "$out"; failed=$((failed + 1))
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

# The strictness split. Same pull request, same follow-up issue, two answers -- and the line sits at
# major: a minor is not held for an issue somebody else has to close, it carries it instead.
rm -f "$scratch/pr/21"; git -C "$repo" reset -q --hard HEAD~1
seed "Assemble across sources (#23)"
expect "an open follow-up passes a patch"          patch 0 "clean:"
listed "and the patch still writes it down"        23 7
expect "and passes a minor"                        minor 0 "clean:"
expect "the minor says what it let past"           minor 0 "does not wait for them"
listed "and writes it down for the request"        23 7
refute "and does not raise a warning over it"      minor "::warning"
expect "but a major is blocked by it"              major 1 "#23"
expect "the major refusal says the issue is open"  major 1 "still open"

seed "Another (#24)"
expect "a closed follow-up is not what blocks a major" major 1 "#23"
refute "and #24 is not what blocked it"                major "#24 "

git -C "$repo" reset -q --hard HEAD~2
seed "Another (#24)"
expect "a closed follow-up passes a major on its own" major 0 "clean:"
listed_nothing "and leaves the list empty"

# The label was added and the issue was never opened -- nothing in this repository reports that.
rm -f "$scratch/issue/24"
expect "a missing follow-up issue blocks a minor"  minor 1 "no follow-up issue"
expect "and blocks a patch too"                    patch 1 "no follow-up issue"

# "The documentation repository could not be read" and "there is no follow-up issue" are the same
# empty list, and only one of them is about documentation. Sending somebody to open an issue that is
# already there is the cost of folding them together.
cat > "$scratch/bin/gh" <<'STUB'
#!/usr/bin/env bash
case "$1" in
  pr)
    for a in "$@"; do case "$a" in [0-9]*) n="$a"; break ;; esac; done
    [ -f "$SMOKE_SCRATCH/pr/$n" ] || exit 1
    cat "$SMOKE_SCRATCH/pr/$n" ;;
  issue) echo "HTTP 404: Not Found" >&2; exit 1 ;;
  *) exit 1 ;;
esac
STUB
expect "an unreadable docs repository is not a missing issue" minor 1 "could not read"
refute "and it is not reported as a missing issue"            minor "no follow-up issue"
# The same failure with no verdict to reach. --report-only runs in the job that publishes the
# release, one step before the one that opens the issue this list feeds: a refusal there would cost
# the notification it exists to send, on a release that has already gone out.
expect_report "report-only survives an unreadable docs repository" 0 "could not read"

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

# An empty range is a real release shape, not a pass to be manufactured.
git -C "$repo" reset -q --hard v0.3.0
expect "an empty range passes and says it was empty" minor 0 "no pull requests"

git -C "$repo" reset -q --hard v0.3.0
seed "Bump (#99)"
expect "a number that is not a pull request here is skipped" minor 0 "clean:"

# Which issue is "the follow-up" is decided by the link in its body, never by where the search put
# it. The lookup is a tokenized full-text search, so it answers with every issue that mentions the
# number -- another follow-up whose prose refers back to this pull request, a body quoting a
# `SHA-256` digest against pull request 256 -- ranked by relevance, which is a different question.
# The pair below is what makes it two questions: same stranger ranked first both times, opposite
# verdicts, and neither of them is the stranger's own state.
git -C "$repo" reset -q --hard v0.3.0
seed "Assemble across sources (#23)"

# The expensive direction. Reading the first one here says the pages were written, and a minor
# release goes out over a follow-up nobody has started.
printf '[%s,%s]\n' "$(decoy 65 CLOSED 368 23)" "$(entry 7 OPEN 23)" > "$scratch/issue/23"
expect "a closed stranger ranked first still blocks a major" major 1 "still open"
expect "and the refusal names the issue that links here"     major 1 "tapstate/docs#7"
refute "not the one the search ranked first"                 major "tapstate/docs#65"

# The other direction, which is what a fix that simply preferred a closed issue would get wrong.
printf '[%s,%s]\n' "$(decoy 65 OPEN 368 23)" "$(entry 7 CLOSED 23)" > "$scratch/issue/23"
expect "an open stranger ranked first does not block a major" major 0 "clean:"
listed_nothing "and a stranger is not carried into the request"

# `/pull/23` is a prefix of `/pull/231`, and 231's follow-up is not this pull request's.
printf '[%s]\n' "$(entry 81 CLOSED 231)" > "$scratch/issue/23"
expect "a longer number is not this pull request's follow-up" minor 1 "no follow-up issue"

# More than one issue can link one pull request, and the pages exist when every one of them is
# closed. Reading whichever came back first is how the closed half answers for the open half.
printf '[%s,%s]\n' "$(entry 7 CLOSED 23)" "$(entry 9 OPEN 23)" > "$scratch/issue/23"
expect "of two links, the open one is what blocks and is named" major 1 "tapstate/docs#9"
listed "and the open one is the one carried"                   23 9

followup 23 OPEN

# --report-only: the same scan at the end of a release. What was open when the release started is
# not what is open when it publishes, and the issue this feeds is the last thing anyone will read
# about this release's documentation.
expect_report "report-only counts the unfinished ones" 0 "1 unfinished"
listed "and writes the same line the gate writes"      23 7
expect_report "and says no verdict was reached"        0 "No verdict"
refute_report "and refuses nothing"                    "::error"

# The half it deliberately does not ask. An unanswered section is a refusal in every other mode;
# here the pull request is read for its label alone, because a release that is already out cannot be
# stopped and the page is owed whatever the body says.
pr 21 "$prompt" "$prompt" docs-needed
followup 21 OPEN
seed "Add a thing (#21)"
expect_report "an unanswered section does not stop report-only" 0 "2 unfinished"
listed "and its follow-up is carried anyway"                    21 7
rm -f "$scratch/pr/21" "$scratch/issue/21"; git -C "$repo" reset -q --hard HEAD~1

# The two modes answer each other with a usage error rather than one quietly winning. A flag that
# turned the verdict off while the step still looked like it ran is the one way this gate could be
# retired by accident.
refuses "--report-only refuses --bump" "--bump" \
  --base v0.3.0 --sha HEAD --report-only --list-open "$scratch/open.tsv" --bump minor
refuses "--report-only needs somewhere to write it" "--list-open" \
  --base v0.3.0 --sha HEAD --report-only

expect "an unknown bump is a usage error"          sideways 2 "--bump"

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
