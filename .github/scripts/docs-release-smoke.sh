#!/usr/bin/env bash
#
# Cases for docs-release.sh. Every write verb runs under --plan, and the state of the release issue
# is injected with --assume-state, because the thing being tested is a decision about somebody
# else's repository that cannot be set up here.
#
# Run with `bash`, and drive the script with `bash` too: `sh` is dash on the runners and bash on a
# developer's Mac, so `sh "$script"` passes locally and exits 2 in CI the moment the script uses
# anything dash does not have.

set -eu

here="$(cd "$(dirname "$0")" && pwd)"
script="$here/docs-release.sh"
failures=0

pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1: $2"; failures=$((failures + 1)); }

# A refusal and a script that is not there -- or one that will not start -- are the same non-zero
# exit with something on stderr, so answer that here once rather than in each case.
if [ ! -f "$script" ]; then
    echo "FAIL - docs-release.sh is not at $script; every case below would pass against an absent file"
    exit 1
fi
if ! bash "$script" 2>/dev/null; then :; fi
if [ "$(bash "$script" open 9.9.9 --plan 2>/dev/null; echo $?)" = 2 ]; then
    echo "FAIL - the script does not start under bash; every refusal case below would pass anyway"
    exit 1
fi

url=https://github.com/tapstate/tapstate/releases/tag/v0.4.0
plan() { bash "$script" "$@" --plan 2>&1; }

refuses() {
    local name="$1"; shift
    local out rc
    set +e
    out="$("$@" 2>&1)"; rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then fail "$name" "exited 0; refusal expected. output: $out"
    elif [ -z "$out" ]; then fail "$name" "refused with exit $rc but said nothing"
    else pass "$name"; fi
}

contains() {
    local name="$1" needle="$2" haystack="$3"
    case "$haystack" in
        *"$needle"*) pass "$name" ;;
        *) fail "$name" "no '$needle' in: $haystack" ;;
    esac
}

# --- refusals -------------------------------------------------------------------------------

refuses "no verb is refused"            bash "$script"
refuses "an unknown verb is refused"    bash "$script" frobnicate 0.4.0
refuses "a missing version is refused"  bash "$script" open --plan
refuses "a version with a leading v is refused" bash "$script" open v0.4.0 --plan
refuses "settle without --notes-url is refused" bash "$script" settle 0.4.0 --plan
refuses "--assume-state outside --plan is refused" \
    bash "$script" settle 0.4.0 --notes-url "$url" --assume-state closed

# --- what each verb would do ----------------------------------------------------------------

out="$(plan open 0.4.0)"
contains "open names the docs repository" "tapstate/docs" "$out"
contains "open names the person asked"    "heywalter"     "$out"

closed="$(plan settle 0.4.0 --notes-url "$url" --assume-state closed)"
contains "a published site gets the tag from us" "tag v0.4.0" "$closed"
contains "and the tag goes on main"              "main"       "$closed"

still="$(plan settle 0.4.0 --notes-url "$url" --assume-state open)"
contains "an unpublished site gets asked again" "second issue" "$still"
contains "and is asked to tag it themselves"    "tag v0.4.0"   "$still"

# The coupling that decides everything: `open` writes a title and `settle`, in a different workflow
# dispatch, finds it again by that exact string. Nothing carries a number between the two, so if the
# two halves ever disagree about the title, settle silently never finds it and every release ends in
# the reminder -- which looks exactly like a documentation owner who is always late.
title="Release 0.4.0: publish the documentation site"
contains "open writes the title settle looks for" "$title" "$(plan open 0.4.0)"
# And it is built from a single definition, so the two halves cannot drift apart. The source carries
# the unexpanded form -- searching for the expanded one here would only prove that $version expands.
# shellcheck disable=SC2016  # the literal $version is what is being searched for, unexpanded
defs="$(grep -cF 'title="Release $version: publish the documentation site"' "$script" || true)"
if [ "$defs" = 1 ]; then
    pass "the title has exactly one definition in the script"
else
    fail "the title has exactly one definition in the script" "found $defs"
fi

# --- retire: withdrawing a request for a version that is not coming ----------------------------

# `retire` has to be an accepted verb before any case below means anything. Under `set -e` an
# unknown one aborts this file at the first command substitution: no FAIL line, no summary, just a
# run that stops -- red in CI, but red without saying what. Measured on a mutation that removed the
# verb from the validation: exit 2, and the last line printed was an ok.
if ! bash "$script" retire 0.4.0 --plan --assume-state open >/dev/null 2>&1; then
    fail "retire is an accepted verb" "the script refuses it; every case below would abort silently"
    echo
    echo "docs-release-smoke: $failures case(s) failed"
    exit 1
fi
pass "retire is an accepted verb"

refuses "retire without a version is refused" bash "$script" retire --plan

open_req="$(plan retire 0.4.0 --assume-state open)"
contains "retire withdraws the open request" "withdraw" "$open_req"
contains "and names the docs repository"     "tapstate/docs" "$open_req"

done_req="$(plan retire 0.4.0 --assume-state closed)"
contains "an already-closed request is left alone" "already closed" "$done_req"

# It looks for the same title the other two use, so a request opened by `open` is the one withdrawn.
# Without this, retire could withdraw nothing for ever and read exactly like a version whose request
# somebody had already closed.
contains "retire looks for the title open wrote" "$title" "$(plan retire 0.4.0 --assume-state open)"

# retire needs no --notes-url: there is no release to link to, which is the whole reason it runs.
if bash "$script" retire 0.4.0 --plan --assume-state open >/dev/null 2>&1; then
    pass "retire needs no --notes-url"
else
    fail "retire needs no --notes-url" "refused without one"
fi

# The same rule the settle half lives by, for the same reason and a sharper one: this runs in the job
# that cleans up after every ending, so a step that can go red here leaves the rest of the cleanup --
# three branches in three repositories -- undone.
# shellcheck disable=SC2016  # the anchor is the script's own literal text, searched unexpanded
retire_half="$(sed -n '/^if \[ "\$verb" = retire \]; then$/,/^# settle$/p' "$script")"
if [ -z "$retire_half" ]; then
    fail "nothing in the retire half refuses" "could not find the retire half; the anchor moved"
else
    n="$(printf '%s' "$retire_half" | grep -cE 'exit [1-9]' || true)"
    if [ "$n" = 0 ]; then pass "nothing in the retire half refuses"
    else fail "nothing in the retire half refuses" "$n non-zero exit(s); the cleanup must not stop for this"; fi
fi

for st in closed open; do
    set +e
    bash "$script" retire 0.4.0 --plan --assume-state "$st" >/dev/null 2>&1
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then pass "retire exits 0 when the issue is $st"
    else fail "retire exits 0 when the issue is $st" "exited $rc; the cleanup must not stop for this"; fi
done

# --- the whole point: it never blocks ---------------------------------------------------------

# Three of these read the source rather than run it, and that is deliberate. What they cover cannot
# be reached from here: --plan exits 0 down every path by construction, and the issue lookup needs an
# issue in somebody else's repository. A mutation run found all three blind, so they are assertions
# about the shape of the file -- weaker than behaviour, and the honest thing to have rather than
# cases that look like they cover it.

# The settle half must have no refusal in it once the state is known. --plan returns 0 whatever it
# is asked, so the cases below prove nothing about the live path; this does.
settle_half="$(sed -n '/^# settle$/,$p' "$script")"
n="$(printf '%s' "$settle_half" | grep -cE 'exit [1-9]' || true)"
if [ "$n" = 0 ]; then pass "nothing in the settle half refuses"
else fail "nothing in the settle half refuses" "$n non-zero exit(s); a release must not stop for this"; fi

# Exactly one assignment to the title, whatever it says. Counting one known string cannot see a
# second assignment added next to it, which is precisely how the two halves would drift apart.
n="$(grep -cE '^[[:space:]]*title=' "$script" || true)"
if [ "$n" = 1 ]; then pass "the title has exactly one assignment"
else fail "the title has exactly one assignment" "found $n"; fi

# When the lookup cannot name a single issue, the fallback has to be "not done". Guessing the other
# way tags a site nobody published.
if grep -qF 'state="open"' "$script" && ! grep -q 'state="closed"' "$script"; then
    pass "an unreadable lookup falls back to not-done"
else
    fail "an unreadable lookup falls back to not-done" "the fallback does not read as state=open"
fi

for st in closed open; do
    set +e
    bash "$script" settle 0.4.0 --notes-url "$url" --plan --assume-state "$st" >/dev/null 2>&1
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then pass "settle exits 0 when the issue is $st"
    else fail "settle exits 0 when the issue is $st" "exited $rc; a release must not stop for this"; fi
done

# --- retire's live path: what it says when the write does not land ------------------------------
#
# Every case above runs under --plan, which never reaches the branch where the write fails -- and
# that branch is the one that was silent. Measured on a real rejected release: retire could neither
# comment nor close, the step reported success, and the only trace was a line on stderr. The workflow
# tees stdout into the step summary, so an outcome written to stderr is not merely quiet, it is
# invisible in the one place anybody looks.
#
# gh is replaced for these. The stub is a real file on PATH and records every call: a PATH that does
# not take would otherwise fall through to the real gh and write to somebody's repository, which is
# not hypothetical. The guard below refuses rather than let that pass unnoticed.
stub_dir="$(mktemp -d)"
cat > "$stub_dir/gh" <<'STUB'
#!/bin/sh
echo "$*" >> "$GH_STUB_LOG"
case "$*" in
  "issue list"*)
    printf '[{"title":"%s","state":"OPEN","number":29}]\n' "$GH_STUB_TITLE"
    exit 0 ;;
  "issue comment"*) exit "${GH_STUB_COMMENT_RC:-0}" ;;
  "issue close"*)   exit "${GH_STUB_CLOSE_RC:-0}" ;;
esac
exit 0
STUB
chmod +x "$stub_dir/gh"

# stdout and stderr kept apart on purpose: which stream the outcome lands on is the whole subject.
retire_live() {
    GH_STUB_LOG="$1" \
    GH_STUB_TITLE="Release 0.4.1: publish the documentation site" \
    GH_STUB_COMMENT_RC="$2" GH_STUB_CLOSE_RC="$3" \
    PATH="$stub_dir:$PATH" \
    bash "$script" retire 0.4.1 2>/dev/null
}

log="$stub_dir/calls-close-fails"
: > "$log"
out="$(retire_live "$log" 0 1)"
if ! grep -q 'issue list' "$log"; then
    fail "the gh stub is the one that ran" "no calls recorded; the real gh may have been used"
elif printf '%s' "$out" | grep -q '#29'; then
    pass "a close that fails says so on stdout, naming the issue"
else
    fail "a close that fails says so on stdout, naming the issue" "stdout was: $out"
fi

log="$stub_dir/calls-comment-fails"
: > "$log"
out="$(retire_live "$log" 1 0)"
# Not merely "#29 appears": the plain success line names the issue too, so asking only for the number
# passed against the very code this case exists to change. A request that is closed with no reason on
# it is worse for its owner than one still open, so the warning is the assertion.
if printf '%s' "$out" | grep -q '::warning::' && printf '%s' "$out" | grep -q '#29'; then
    pass "a closed request whose explanation did not land says so too"
else
    fail "a closed request whose explanation did not land says so too" "stdout was: $out"
fi

# The control: when both land, the summary says it withdrew the request and raises no warning.
# Without it, a script that shouted on every run would pass both cases above while saying nothing.
log="$stub_dir/calls-ok"
: > "$log"
out="$(retire_live "$log" 0 0)"
if printf '%s' "$out" | grep -q 'withdrew' && ! printf '%s' "$out" | grep -q '::warning::'; then
    pass "a withdrawal that lands reports plainly, with no warning"
else
    fail "a withdrawal that lands reports plainly, with no warning" "stdout was: $out"
fi
rm -rf "$stub_dir"

echo
if [ "$failures" -eq 0 ]; then
    echo "docs-release-smoke: all cases passed"
else
    echo "docs-release-smoke: $failures case(s) failed"
    exit 1
fi
