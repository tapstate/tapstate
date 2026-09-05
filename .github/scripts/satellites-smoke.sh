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

plan() { GH_TOKEN=t bash "$script" "$@" --list "$list" --plan 2>&1; }

contains() {
    local name="$1" needle="$2" haystack="$3"
    case "$haystack" in
        *"$needle"*) pass "$name" ;;
        *) fail "$name" "no '$needle' in: $haystack" ;;
    esac
}

excludes() {
    local name="$1" needle="$2" haystack="$3"
    case "$haystack" in
        *"$needle"*) fail "$name" "'$needle' leaked into: $haystack" ;;
        *) pass "$name" ;;
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
    env GH_TOKEN= bash "$script" reach --list "$list"

refuses "an empty satellite list is refused" \
    env GH_TOKEN=t bash "$script" branch 0.4.0 --list "$tmp/empty.txt"

refuses "a list file that does not exist is refused" \
    env GH_TOKEN=t bash "$script" branch 0.4.0 --list "$tmp/nope.txt"

refuses "a version that is not x.y.z is refused" \
    env GH_TOKEN=t bash "$script" branch v0.4.0 --list "$list" --plan

refuses "a missing version is refused" \
    env GH_TOKEN=t bash "$script" branch --list "$list" --plan

refuses "an unknown verb is refused" \
    env GH_TOKEN=t bash "$script" frobnicate --list "$list"

refuses "release without --notes-url is refused" \
    env GH_TOKEN=t bash "$script" release 0.4.0 --list "$list" --plan

refuses "a list of only comments is refused" \
    env GH_TOKEN=t bash "$script" branch 0.4.0 --list "$tmp/comments.txt" --plan

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
out="$(GH_TOKEN=t bash "$script" branch 0.4.0 --list "$one" --plan 2>&1)"
case "$out" in
    *tapstate-skills*) fail "the list is what decides, not a built-in default" "skills appeared for a one-line list" ;;
    *) pass "the list is what decides, not a built-in default" ;;
esac


# --- when the API call itself fails -----------------------------------------------------------

# `gh api` writes the error body to stdout, not stderr, when a request fails. An output taken
# without its exit status is therefore that body: non-empty, and not the value that was asked for.
# Every reader below either compares the result against a literal or tests it for emptiness, and a
# JSON blob quietly passes the emptiness test -- so the precise sentence each site was written to
# print can never fire, and the blob is printed in place of the value instead. A token with no
# access at all to a repository gets a 404, which is the single most likely reason for any of these
# branches to run, and the release gate is where they run.
stub="$tmp/bin"
mkdir -p "$stub"
cat > "$stub/gh" <<'STUB'
#!/usr/bin/env bash
# Fails the way the real gh does: the error body on stdout, a line on stderr, exit 1.
# GH_STUB_FAIL is a substring of the arguments to fail on; empty means fail every call.
if [ -z "${GH_STUB_FAIL:-}" ] || [[ "$*" == *"$GH_STUB_FAIL"* ]]; then
    echo '{"message":"Not Found","documentation_url":"https://docs.github.com/rest","status":"404"}'
    echo "gh: Not Found (HTTP 404)" >&2
    exit 1
fi
echo "${GH_STUB_OK:-main}"
STUB
chmod +x "$stub/gh"

# A stub that is not the gh being run would make every case below pass against the real one.
if [ "$(PATH="$stub:$PATH" command -v gh)" = "$stub/gh" ]; then
    pass "the stub is the gh these cases run"
else
    fail "the stub is the gh these cases run" "command -v gh found something else"
fi

# A repository that does not exist, so a stub that somehow is not picked up still writes nothing.
solo="$tmp/solo.txt"; echo "tapstate/satellites-smoke-no-such-repo" > "$solo"

# <fail-substring> <verb> [args...] -- output of one run against the stub, exit status dropped.
stubbed() {
    local f="$1"; shift
    local out
    set +e
    out="$(PATH="$stub:$PATH" GH_TOKEN=t GH_STUB_FAIL="$f" bash "$script" "$@" --list "$solo" 2>&1)"
    set -e
    printf '%s' "$out"
}

out="$(stubbed "" reach)"
contains "reach reports an unreadable permission as unreadable" "permissions.push=unreadable" "$out"
excludes "reach does not print the error body as the permission" "Not Found" "$out"

refuses "reach still refuses when the call fails" \
    env PATH="$stub:$PATH" GH_TOKEN=t GH_STUB_FAIL= bash "$script" reach --list "$solo"

out="$(stubbed "" branch 0.4.0)"
contains "branch says which read failed" "cannot read the default branch" "$out"

out="$(stubbed "git/ref" branch 0.4.0)"
contains "branch says which ref it could not read" "cannot read main" "$out"

out="$(stubbed "" release 0.4.0 --notes-url https://example.invalid/notes)"
contains "release says the branch it would tag is not there" "has no ws/release-0.4.0 to tag" "$out"
echo
if [ "$failures" -eq 0 ]; then
    echo "satellites-smoke: all cases passed"
else
    echo "satellites-smoke: $failures case(s) failed"
    exit 1
fi
