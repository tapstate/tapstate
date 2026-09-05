#!/usr/bin/env bash
#
# Cases for roadmap-shipped.sh. This script had none: it does nothing on a branch, in a pull
# request or in a rehearsal, so a published release was its only exercise and the first one found
# it broken. What is tested here is the half that is silent when it is wrong -- a run that reports
# every issue moved and exits 0 while the board says something else.
#
# The board is a concurrent writer. A project workflow that fires when an item is added lands after
# the mutations this script makes, so the write reports success and the item ends at the workflow's
# value. Nothing in the mutation response says so; only asking the board again does.
#
# gh is replaced. The stub is a real file on PATH and records every call: a PATH that does not take
# falls through to the real gh and writes to the real board, which is not hypothetical.

set -eu

here="$(cd "$(dirname "$0")" && pwd)"
script="$here/roadmap-shipped.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
failures=0

if [ ! -f "$script" ]; then
    echo "FAIL - roadmap-shipped.sh is not at $script; every case below would pass against an absent file"
    exit 1
fi

pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1: $2"; failures=$((failures + 1)); }

# A repository with one merge commit in range, so the script has a pull request number to read.
repo="$tmp/repo"
mkdir -p "$repo"
git -C "$repo" init -q
git -C "$repo" config user.email t@t; git -C "$repo" config user.name t
git -C "$repo" commit -q --allow-empty -m "base"
git -C "$repo" tag v0
git -C "$repo" commit -q --allow-empty -m "Merge pull request #7 from tapstate/ws/x"
sha="$(git -C "$repo" rev-parse HEAD)"

stub_dir="$tmp/stub"
mkdir -p "$stub_dir"
cat > "$stub_dir/gh" <<'STUB'
#!/bin/sh
echo "$*" >> "$GH_STUB_LOG"
case "$*" in
  "pr view"*"--json number"*)
    # Is this number a pull request? Asked only once the issue lookup has already come back empty.
    [ "${GH_STUB_TARGET:-issue}" = pr ] || exit 1
    echo '{"number":42}'
    exit 0 ;;
  "pr view"*)
    echo "Refs #42"
    exit 0 ;;
  *"projectV2(number:"*)
    cat <<'JSON'
{"data":{"organization":{"projectV2":{"id":"PVT_1","fields":{"nodes":[
  {"id":"F_status","name":"Status","options":[{"id":"O_shipped","name":"Shipped"},{"id":"O_done","name":"Done"}]},
  {"id":"F_released","name":"Released in"}]}}}}}
JSON
    exit 0 ;;
  *"repository(owner:"*)
    # A lookup that finds no issue is not an empty capture. gh writes the response body to stdout
    # even when the query errored and exits non-zero, so a stub that answers with nothing tests a
    # shape the real gh never produces -- and hides the one that put a JSON blob through as a node
    # id. 'missing' and 'pr' are identical here on purpose: this query cannot tell them apart.
    if [ "${GH_STUB_TARGET:-issue}" = issue ]; then echo "I_42"; exit 0; fi
    echo '{"data":{"repository":{"issue":null}},"errors":[{"type":"NOT_FOUND","path":["repository","issue"],"message":"Could not resolve to an Issue with the number of 42."}]}'
    exit 1 ;;
  *addProjectV2ItemById*)
    # A credential that can read the board and not write it. Same shape as the failed issue lookup:
    # the body lands on stdout, so an unguarded capture carries it forward as an item id.
    if [ "${GH_STUB_ADD_FAILS:-0}" = 1 ]; then
      echo '{"data":{"addProjectV2ItemById":null},"errors":[{"type":"FORBIDDEN","message":"Resource not accessible by personal access token"}]}'
      exit 1
    fi
    echo "PVTI_1"
    exit 0 ;;
  *updateProjectV2ItemFieldValue*)
    echo '{}'
    exit 0 ;;
  *"node(id:"*)
    # What the board says now. The first read answers with GH_STUB_FIRST, every later one with
    # GH_STUB_LATER -- which is how a workflow that overwrites once differs from one that keeps
    # overwriting.
    n=$(cat "$GH_STUB_READS" 2>/dev/null || echo 0); [ -n "$n" ] || n=0
    echo $((n + 1)) > "$GH_STUB_READS"
    if [ "$n" = 0 ]; then st="$GH_STUB_FIRST"; else st="$GH_STUB_LATER"; fi
    printf '{"data":{"node":{"fieldValues":{"nodes":[{"name":"%s","field":{"name":"Status"}},{"text":"0.4.0","field":{"name":"Released in"}}]}}}}\n' "$st"
    exit 0 ;;
esac
exit 0
STUB
chmod +x "$stub_dir/gh"

run() {   # first-read-status  later-read-status  target(issue|pr|missing)  add-fails -> OUT, RC, LOG
    LOG="$tmp/calls.$$.$RANDOM"; : > "$LOG"
    : > "$tmp/reads"
    set +e
    OUT="$(cd "$repo" && GH_STUB_LOG="$LOG" GH_STUB_READS="$tmp/reads" \
        GH_STUB_FIRST="$1" GH_STUB_LATER="$2" GH_STUB_TARGET="${3:-issue}" \
        GH_STUB_ADD_FAILS="${4:-0}" \
        GITHUB_REPOSITORY=tapstate/tapstate PATH="$stub_dir:$PATH" \
        bash "$script" --version 0.4.0 --base v0 --sha "$sha" 2>&1)"
    RC=$?
    set -e
    if ! grep -q 'projectV2(number:' "$LOG"; then
        fail "the gh stub is the one that ran" "no calls recorded; the real gh may have been used"
        return 1
    fi
    return 0
}

writes() { grep -c 'updateProjectV2ItemFieldValue' "$LOG" || true; }

# The control. Without it a script that failed on every release would pass the two cases below.
if run Shipped Shipped; then
    if [ "$RC" = 0 ] && [ "$(writes)" = 2 ]; then
        pass "a board that keeps the write is reported once and not written again"
    else
        fail "a board that keeps the write is reported once and not written again" "rc=$RC writes=$(writes): $OUT"
    fi
fi

# The measured case: the item ends at Done although both mutations returned success.
if run Done Shipped; then
    if [ "$RC" = 0 ] && [ "$(writes)" -gt 2 ] && printf '%s' "$OUT" | grep -q '#42'; then
        pass "a write the board overwrote once is written again and then holds"
    else
        fail "a write the board overwrote once is written again and then holds" "rc=$RC writes=$(writes): $OUT"
    fi
fi

# The same thing when rewriting does not win: this must be red, not a green run that moved nothing.
if run Done Done; then
    if [ "$RC" != 0 ] && printf '%s' "$OUT" | grep -q '#42'; then
        pass "an item the board will not keep at Shipped fails, naming the issue"
    else
        fail "an item the board will not keep at Shipped fails, naming the issue" "rc=$RC: $OUT"
    fi
fi

# A Refs line whose issue cannot be resolved: a wrong number, or a credential without issue access.
# Passing over it in silence is how a run with no issues permission reports success and moves none.
if run Shipped Shipped missing; then
    if [ "$RC" != 0 ] && printf '%s' "$OUT" | grep -q '#42'; then
        pass "an unresolvable Refs target fails, naming the issue"
    else
        fail "an unresolvable Refs target fails, naming the issue" "rc=$RC: $OUT"
    fi
fi

# A Refs target that is a pull request. It gets there honestly -- a release pull request saying
# `Refs #N` about the write-back pull request before it is accurate prose -- and the board is a list
# of work items, so the reference is passed over and said out loud. Two ways to be wrong, so both
# are asserted: red here reddens the roadmap job on every release carrying such a reference, and a
# board write here puts a pull request on the roadmap as noise that nobody removes.
if run Shipped Shipped pr; then
    if [ "$RC" = 0 ] && printf '%s' "$OUT" | grep -q '#42' \
        && [ "$(writes)" = 0 ] && ! grep -q addProjectV2ItemById "$LOG"; then
        pass "a Refs target that is a pull request is passed over, and nothing is written"
    else
        fail "a Refs target that is a pull request is passed over, and nothing is written" \
             "rc=$RC writes=$(writes) added=$(grep -c addProjectV2ItemById "$LOG" || true): $OUT"
    fi
fi

# The item could not be put on the board at all. The failure has to be reported as that, and the
# field writes that follow must not happen: an unguarded capture sends six mutations against an item
# id that is an error body, and the run then blames the board for a value it was never asked to hold.
if run Shipped Shipped issue 1; then
    if [ "$RC" != 0 ] && printf '%s' "$OUT" | grep -q 'could not be put on the board' \
        && [ "$(writes)" = 0 ]; then
        pass "an item the board refuses is reported as that, with no field writes after it"
    else
        fail "an item the board refuses is reported as that, with no field writes after it" \
             "rc=$RC writes=$(writes): $OUT"
    fi
fi

echo
if [ "$failures" = 0 ]; then echo "all cases passed"; else echo "$failures case(s) failed"; fi
exit $((failures > 0))
