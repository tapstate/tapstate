#!/usr/bin/env bash
#
# Cases for latest-release-tag.sh. The release list is handed in as a file, so every case states
# outright which releases exist and in what order -- waiting for a real re-push would make the one
# case that matters unreproducible.

set -eu

here="$(cd "$(dirname "$0")" && pwd)"
script="$here/latest-release-tag.sh"
workflows="$(cd "$here/../workflows" && pwd)"
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

refuses() {
    local name="$1" list="$2" file
    file="$(mktemp)"
    printf '%s' "$list" > "$file"
    if "$script" "$file" >/dev/null 2>&1; then
        echo "FAIL - $name: answered instead of refusing"
        failures=$((failures + 1))
    else
        echo "ok   - $name"
    fi
    rm -f "$file"
}

resolve() {
    local list="$1" file out
    file="$(mktemp)"
    printf '%s' "$list" > "$file"
    out="$("$script" "$file")" || out="<refused>"
    rm -f "$file"
    printf '%s' "$out"
}

# The case this script exists for. `connectors-preview` is a release and a git tag, and re-pushing it
# sorts it to the front of the list by publication date. Taking the first entry then hands back a tag
# that is not a version at all -- and every consumer of that answer goes on to do something plausible
# with it. Nothing in the real list produces this order today, so a case that does not build the list
# outright is blind to the defect by construction.
check "a non-version tag at the front is not the newest release" "v0.3.0" \
    "$(resolve '[{"tagName":"connectors-preview"},{"tagName":"v0.3.0"},{"tagName":"v0.2.1"}]')"

# The control: the same shape with nothing to skip. Both cases have to hold, or the filter could be
# passing by refusing everything.
check "an all-version list answers with its first entry" "v0.3.0" \
    "$(resolve '[{"tagName":"v0.3.0"},{"tagName":"v0.2.1"}]')"

check "a non-version tag anywhere else is skipped too" "v0.2.1" \
    "$(resolve '[{"tagName":"v0.2.1"},{"tagName":"connectors-preview"}]')"

# Refusing matters more than it looks: an empty answer flows into `git show "$tag:path"` and into a
# deploy step, where it turns into a confusing failure far from here -- or, on the publish path, into
# a comparison of a thing against itself that reports success.
refuses "a list with no version tag refuses" '[{"tagName":"connectors-preview"}]'
refuses "an empty list refuses" '[]'

# The other half of this task: one resolution, not three. A workflow that keeps its own copy is not
# covered by any case above, and the two that had one were both wrong.
for wf in quickstart-live.yml install-e2e.yml publish-install-site.yml; do
    if [ ! -f "$workflows/$wf" ]; then
        echo "FAIL - $wf: not found at $workflows -- the two checks below would pass vacuously"
        failures=$((failures + 1))
        continue
    fi
    if grep -q 'latest-release-tag\.sh' "$workflows/$wf"; then
        echo "ok   - $wf calls latest-release-tag.sh"
    else
        echo "FAIL - $wf does not call latest-release-tag.sh"
        failures=$((failures + 1))
    fi
    if grep -q 'gh release list' "$workflows/$wf"; then
        echo "FAIL - $wf still resolves the newest release itself"
        failures=$((failures + 1))
    else
        echo "ok   - $wf keeps no copy of the resolution"
    fi
done

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed" >&2
    exit 1
fi
echo "all cases passed"
