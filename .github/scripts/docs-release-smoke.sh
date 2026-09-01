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

echo
if [ "$failures" -eq 0 ]; then
    echo "docs-release-smoke: all cases passed"
else
    echo "docs-release-smoke: $failures case(s) failed"
    exit 1
fi
