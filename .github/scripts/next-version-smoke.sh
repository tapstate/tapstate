#!/usr/bin/env bash
#
# Cases for next-version.sh. Each builds a throwaway repository whose tags and branches are stated
# outright: the case that matters -- a fix on a line that is no longer the newest -- cannot be
# reproduced by waiting for one to happen.

set -eu

script="$(cd "$(dirname "$0")" && pwd)/next-version.sh"
failures=0

check() {
    local name="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        echo "ok   - $name"
    else
        echo "FAIL - $name: expected '$expected', got '$actual'"
        failures=$((failures + 1))
    fi
}

commit() {   # dir, message
    git -C "$1" commit -q --allow-empty -m "$2"
}

# main carrying v0.5.0 then v0.6.0, plus a branch off v0.5.0 with a fix on it. This is the shape a
# patch to an older line has, and the one a repository-wide "newest tag" reads wrong.
two_lines() {
    local dir
    dir="$(mktemp -d)"
    git -C "$dir" init -q -b main
    git -C "$dir" config user.email tapstate@example.com
    git -C "$dir" config user.name tapstate
    commit "$dir" "0.5.0"; git -C "$dir" tag v0.5.0
    git -C "$dir" branch line-0.5
    commit "$dir" "0.6.0"; git -C "$dir" tag v0.6.0
    git -C "$dir" checkout -q line-0.5
    commit "$dir" "a fix for the 0.5 line"
    git -C "$dir" checkout -q main
    echo "$dir"
}

answer() {   # dir, bump, commit, field
    ( cd "$1" && "$script" "$2" "$3" ) | sed -n "s/^$4=//p"
}

dir="$(two_lines)"

check "a minor from main counts up from the tag on main"  "0.7.0" "$(answer "$dir" minor main version)"
check "a patch from main counts up from the tag on main"  "0.6.1" "$(answer "$dir" patch main version)"
check "a major from main counts up from the tag on main"  "1.0.0" "$(answer "$dir" major main version)"

# The case this script exists for. The newest tag in the repository is v0.6.0, and reading it instead
# of walking the ancestry produces 0.6.1 -- a perfectly plausible number, on the wrong line, that
# nothing downstream would question.
check "a patch on the older line counts up from that line" "0.5.1" "$(answer "$dir" patch line-0.5 version)"

# The same split decides the floating pointer and the write-back. A patch to an older line must not
# claim to be the latest release, or a new user installs an old line from the official entry point.
check "a release ahead of every tag is the latest"        "true"  "$(answer "$dir" minor main is_latest)"
check "a fix on an older line is not the latest"          "false" "$(answer "$dir" patch line-0.5 is_latest)"

rm -rf "$dir"

# A repository with no version tag at all has nothing to count up from. Answering 0.0.1 or 1.0.0 would
# both be guesses, and a guessed version is spent the moment it is published.
dir="$(mktemp -d)"
git -C "$dir" init -q -b main
git -C "$dir" config user.email tapstate@example.com
git -C "$dir" config user.name tapstate
commit "$dir" "no tags here"
if ( cd "$dir" && "$script" minor main >/dev/null 2>&1 ); then
    echo "FAIL - a repository with no version tag must refuse"
    failures=$((failures + 1))
else
    echo "ok   - a repository with no version tag refuses"
fi
rm -rf "$dir"

# A bump that is not one of the three has to stop the run rather than fall through to some default.
dir="$(two_lines)"
if ( cd "$dir" && "$script" nudge main >/dev/null 2>&1 ); then
    echo "FAIL - an unknown bump must refuse"
    failures=$((failures + 1))
else
    echo "ok   - an unknown bump refuses"
fi
rm -rf "$dir"

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed" >&2
    exit 1
fi
echo "all cases passed"
