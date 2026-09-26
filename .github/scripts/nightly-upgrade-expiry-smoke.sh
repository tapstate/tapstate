#!/usr/bin/env bash
# The rollback witness must remain runnable after the startup gate reaches a release tag.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
workflow="$repo/.github/workflows/nightly-upgrade.yml"
marker=adapters/adapter-mongo-store/src/main/java/io/tapstate/adapters/mongostore/MigrationError.java
base="$(git -C "$repo" describe --tags --abbrev=0 HEAD)"

if [ ! -f "$workflow" ]; then
    echo "test setup: no upgrade workflow at $workflow" >&2
    exit 2
fi
if ! git -C "$repo" cat-file -e "$base:$marker"; then
    echo "test setup: $base does not contain $marker" >&2
    exit 2
fi

gate="$(git -C "$repo" log --diff-filter=A --format=%H "$base"..HEAD -- "$marker")"
active_run="$(awk '
    /^        run: \|$/ { in_run = 1; next }
    in_run && /^[[:space:]]*$/ { next }
    in_run && /^          / {
        sub(/^          /, "")
        if ($0 !~ /^[[:space:]]*#/) print
        next
    }
    in_run { in_run = 0 }
' "$workflow")"
# shellcheck disable=SC2016
if [ -z "$gate" ] && grep -Fq 'gate="$(git log --diff-filter=A --format=%H "$base"..HEAD -- "$marker")"' <<< "$active_run"; then
    echo "FAIL - $base already contains $marker, but the upgrade lane still searches for its adding commit after $base." >&2
    echo "expected exactly one commit adding $marker since $base, found: none." >&2
    exit 1
fi

echo "ok - the upgrade lane does not use the expired old-binary recipe"
