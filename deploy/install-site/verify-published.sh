#!/bin/sh
# Assert that what the install domain serves right now is exactly what this tree would deploy.
#
# Why byte equality and not "the version looks right": the installer e2e lane used to assert only that
# the served script did not say SNAPSHOT, which a script pinned to any stale release passes. That let a
# published one-liner install 0.2.0 for an entire release cycle while every checked-in pin said 0.2.1,
# and nothing went red. A digest cannot be stale-but-plausible.
#
# Every entry point is checked, never just one. They are two different files -- / and its alias /cli
# are the CLI-only installer, /demo is the full-stack quickstart -- and each carries its own version
# pin, so a deploy that refreshed one and missed the other is a real and observed shape. The alias is
# checked as its own route: a rewrite that points it at the wrong file is a deploy defect too.
#
# Cache is bypassed on purpose: the edge serves these with a 300s max-age, so a check that accepts a
# cached body can report success about content the origin no longer has.
#
# Usage: verify-published.sh <base-url> <expected-dir>
#   e.g. verify-published.sh https://install.tapstate.dev "$(mktemp -d)"   (after assemble.sh)
set -eu

base="${1:?usage: verify-published.sh <base-url> <expected-dir>}"
expected="${2:?usage: verify-published.sh <base-url> <expected-dir>}"

# Refused up front rather than inside digest(). Every call is `x="$(digest ...)"`, and an exit there
# ends the command substitution's subshell, not this script: both sides would come back empty, the
# comparison of two empty strings would hold, and the verifier would print ok having compared nothing.
# A checker that passes when it cannot check is worse than no checker.
if command -v sha256sum >/dev/null 2>&1; then
    DIGEST_TOOL="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
    DIGEST_TOOL="shasum -a 256"
else
    echo "no sha256 tool (sha256sum or shasum) is available; refusing to report a result" >&2
    exit 1
fi

# The pipe to awk decides the status, so a checksum command that fails still leaves a successful,
# empty result -- and two empty digests compare equal, which is the same way this script could once
# report a match having compared nothing. The checksum runs on its own, its status is taken, and an
# empty digest is refused rather than returned.
digest() {
    # shellcheck disable=SC2086
    _raw="$($DIGEST_TOOL "$1")" || {
        echo "could not hash $1" >&2
        return 1
    }
    _sum="$(printf '%s' "$_raw" | awk '{print $1}')"
    [ -n "$_sum" ] || {
        echo "hashing $1 produced nothing" >&2
        return 1
    }
    printf '%s' "$_sum"
}

# The pin each script carries, echoed on failure so the message names the skew instead of only
# reporting that two digests differ.
pin_of() {
    sed -n 's/^PINNED_VERSION="\(.*\)"$/\1/p;s/^CLI_VERSION="\(.*\)"$/\1/p' "$1" | head -1
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

failed=0
check() {
    path="$1"
    file="$2"
    url="$base$path"

    curl -fsSL -H 'Cache-Control: no-cache' -H 'Pragma: no-cache' "$url" -o "$tmp/served" || {
        echo "FAIL $url could not be fetched" >&2
        failed=1
        return
    }

    want="$(digest "$expected/$file")" || { failed=1; return; }
    got="$(digest "$tmp/served")" || { failed=1; return; }
    if [ "$want" = "$got" ]; then
        printf 'ok   %s == %s (pin %s, sha256 %.12s...)\n' "$url" "$file" "$(pin_of "$expected/$file")" "$got"
        return
    fi

    echo "FAIL $url is not what this tree would deploy" >&2
    echo "     served  pin $(pin_of "$tmp/served")  sha256 $got" >&2
    echo "     tree    pin $(pin_of "$expected/$file")  sha256 $want  ($file)" >&2
    echo "     the edge is serving an older deployment: re-run the publish-install-site workflow" >&2
    failed=1
}

check "/"     install.sh
check "/cli"  install.sh
check "/demo" quickstart.sh

[ "$failed" -eq 0 ] || exit 1
echo "all entry points match the tree"
