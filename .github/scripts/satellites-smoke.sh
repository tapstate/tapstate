#!/usr/bin/env bash
#
# Cases for satellites.sh. The satellite list is handed in as a file and every write verb is run
# under --plan, so each case states outright which repositories exist and asserts the exact refs
# that would be touched -- without that, the only way to test a verb that creates a tag in another
# repository is to create one.
#
# --plan is not a substitute for exercising the real calls. `reach` does hit the API and runs as a
# release gate before anything irreversible, so the credential path is proven every release. What
# is tested here is the half that decides *what* to touch, which is where a wrong answer is silent:
# a satellite dropped from the list is a repository that never gets tagged, and nothing goes red.

set -eu

here="$(cd "$(dirname "$0")" && pwd)"
script="$here/satellites.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
failures=0

# A refusal and a script that is not there produce the same non-zero exit with a message on stderr,
# so every case below would pass against an absent file. Answer that here, once, rather than in each
# case: from this line on, a non-zero exit is the script's own.
if [ ! -f "$script" ]; then
    echo "FAIL - satellites.sh is not at $script; every case below would pass against an absent file"
    exit 1
fi

pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1: $2"; failures=$((failures + 1)); }

# The list every positive case runs against.
list="$tmp/list.txt"
cat > "$list" <<'LIST'
# One owner/repo per line. Comments and blank lines are ignored.
tapstate/tapstate-web

tapstate/tapstate-skills
tapstate/docs
LIST

: > "$tmp/empty.txt"
printf '# comment only\n\n' > "$tmp/comments.txt"

plan() { GH_TOKEN=t sh "$script" "$@" --list "$list" --plan 2>&1; }

contains() {
    local name="$1" needle="$2" haystack="$3"
    case "$haystack" in
        *"$needle"*) pass "$name" ;;
        *) fail "$name" "no '$needle' in: $haystack" ;;
    esac
}

refuses() {
    local name="$1"; shift
    local out rc
    set +e
    out="$("$@" 2>&1)"; rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
        fail "$name" "exited 0; refusal expected. output: $out"
    elif [ -z "$out" ]; then
        fail "$name" "refused with exit $rc but said nothing"
    else
        pass "$name"
    fi
}

# --- refusals -------------------------------------------------------------------------------

refuses "an empty token is refused, not skipped" \
    env GH_TOKEN= sh "$script" reach --list "$list"

refuses "an empty satellite list is refused" \
    env GH_TOKEN=t sh "$script" branch 0.4.0 --list "$tmp/empty.txt"

refuses "a list file that does not exist is refused" \
    env GH_TOKEN=t sh "$script" branch 0.4.0 --list "$tmp/nope.txt"

refuses "a version that is not x.y.z is refused" \
    env GH_TOKEN=t sh "$script" branch v0.4.0 --list "$list" --plan

refuses "a missing version is refused" \
    env GH_TOKEN=t sh "$script" branch --list "$list" --plan

refuses "an unknown verb is refused" \
    env GH_TOKEN=t sh "$script" frobnicate --list "$list"

refuses "release without --notes-url is refused" \
    env GH_TOKEN=t sh "$script" release 0.4.0 --list "$list" --plan

refuses "a list of only comments is refused" \
    env GH_TOKEN=t sh "$script" branch 0.4.0 --list "$tmp/comments.txt" --plan

# --- what each verb would touch -------------------------------------------------------------

out="$(plan branch 0.4.0)"
for r in tapstate/tapstate-web tapstate/tapstate-skills tapstate/docs; do
    contains "branch names $r" "$r" "$out"
done
contains "branch names the ref it creates" "ws/release-0.4.0" "$out"

out="$(plan unbranch 0.4.0)"
contains "unbranch names the same ref" "ws/release-0.4.0" "$out"
contains "unbranch says it deletes" "delete" "$out"

out="$(plan release 0.4.0 --notes-url https://github.com/tapstate/tapstate/releases/tag/v0.4.0)"
contains "release names the tag" "v0.4.0" "$out"
contains "release carries the link back to tapstate" "tapstate/tapstate/releases/tag/v0.4.0" "$out"
contains "release tags the release branch, not the default branch" "ws/release-0.4.0" "$out"

# A satellite dropped from the list is the silent failure this file exists to catch.
one="$tmp/one.txt"; echo "tapstate/docs" > "$one"
out="$(GH_TOKEN=t sh "$script" branch 0.4.0 --list "$one" --plan 2>&1)"
case "$out" in
    *tapstate-skills*) fail "the list is what decides, not a built-in default" "skills appeared for a one-line list" ;;
    *) pass "the list is what decides, not a built-in default" ;;
esac

echo
if [ "$failures" -eq 0 ]; then
    echo "satellites-smoke: all cases passed"
else
    echo "satellites-smoke: $failures case(s) failed"
    exit 1
fi
