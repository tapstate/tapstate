#!/usr/bin/env bash
# One release attempt, with a retired request retained throughout its lifecycle.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
script="$here/docs-release.sh"
command -v jq >/dev/null || { echo "SETUP: jq is required" >&2; exit 2; }
[ -f "$script" ] || { echo "SETUP: docs-release.sh is missing" >&2; exit 2; }

scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
: > "$scratch/failures"
: > "$scratch/stub-errors"

# The retired request's reason and timestamps match the reported release. Only the
# current request changes below, when its human owner completes publication.
cat > "$scratch/issues.json" <<'JSON'
[
  {
    "number": 117,
    "title": "Release 0.7.0: publish the documentation site",
    "state": "CLOSED",
    "stateReason": "NOT_PLANNED",
    "createdAt": "2026-10-15T12:56:28Z",
    "closedAt": "2026-10-15T13:02:12Z"
  },
  {
    "number": 119,
    "title": "Release 0.7.0: publish the documentation site",
    "state": "OPEN",
    "stateReason": "",
    "createdAt": "2026-10-16T06:14:09Z",
    "closedAt": null
  }
]
JSON

# Stub only the GitHub boundary. Honor state and field selection so the fixture
# cannot supply lifecycle metadata that a real gh call did not request.
cat > "$scratch/gh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
jq -cn --args '$ARGS.positional' -- "$@" >> "$GH_STUB_LOG" || {
    echo "Could not record the gh invocation" >> "$GH_STUB_ERRORS"
    exit 2
}
unsupported() {
    printf 'Unsupported gh call: %s\n' "$*" >> "$GH_STUB_ERRORS"
    exit 2
}
case "${1:-} ${2:-}" in
    "issue list")
        shift 2
        state=open
        fields=""
        while [ "$#" -gt 0 ]; do
            case "$1" in
                --state|-s) state="$2"; shift 2 ;;
                --json) fields="$2"; shift 2 ;;
                --repo|-R|--limit|-L|--search|-S) shift 2 ;;
                *) unsupported "$@" ;;
            esac
        done
        jq --arg state "$state" --arg fields "$fields" '
            map(select($state == "all" or (.state | ascii_downcase) == $state))
            | map(with_entries(select(.key as $key | $fields | split(",") | index($key))))
        ' "$GH_STUB_ISSUES" || {
            echo "Could not read the GitHub issue fixture" >> "$GH_STUB_ERRORS"
            exit 2
        }
        ;;
    "api repos/tapstate/docs/git/ref/heads/main")
        echo 1111111111111111111111111111111111111111 ;;
    "issue create") echo https://github.com/tapstate/docs/issues/120 ;;
    "issue edit"|"issue comment"|"release create") ;;
    *) unsupported "$@" ;;
esac
STUB
chmod +x "$scratch/gh"
export PATH="$scratch:$PATH"
export GH_STUB_ISSUES="$scratch/issues.json" GH_STUB_ERRORS="$scratch/stub-errors"
export DOCS_REPO=tapstate/docs DOCS_OWNER=heywalter
[ "$(command -v gh)" = "$scratch/gh" ] || { echo "SETUP: gh stub is not selected" >&2; exit 2; }

fail() { printf '  %s\n' "$*" >> "$scratch/failures"; }
check_calls() {
    if ! jq -se "$2" "$GH_STUB_LOG" >/dev/null; then fail "$1"; fi
}
settle() {
    export GH_STUB_LOG="$scratch/$1.calls"
    : > "$GH_STUB_LOG"
    if ! bash "$script" settle 0.7.0 \
            --notes-url https://github.com/tapstate/tapstate/releases/tag/v0.7.0 \
            > "$scratch/$1.output" 2>&1; then
        fail "settlement must leave the runtime release unblocked ($1)"
    fi
    check_calls "settlement must query GitHub ($1)" 'any(.[]; .[0:2] == ["issue", "list"])'
    if grep -q 'treating as not done' "$scratch/$1.output"; then
        fail "the retired request must not make the current request ambiguous ($1)"
    fi
    check_calls "settlement must leave retired request #117 untouched ($1)" \
        'all(.[]; (.[0] == "issue" and .[1] != "list" and .[2] == "117") | not)'
}

settle open
check_calls "an open current request still needs the human publication reminder" \
    'any(.[]; .[0:2] == ["issue", "create"] and index("Still to do: publish the documentation for v0.7.0") != null)'
check_calls "a retired closed request is not evidence that the current site is published" \
    'all(.[]; .[0:2] != ["release", "create"])'

# The human completes the current request; the retired request remains available.
jq 'map(if .number == 119 then
        .state = "CLOSED" | .stateReason = "COMPLETED" | .closedAt = "2026-10-16T08:00:00Z"
    else . end)' "$scratch/issues.json" > "$scratch/completed.json"
mv "$scratch/completed.json" "$scratch/issues.json"

settle completed
check_calls "the completed current request must settle without another publication reminder" \
    'all(.[]; .[0:2] != ["issue", "create"])'
# shellcheck disable=SC2016 # $i is a jq variable, not a shell expansion.
check_calls "completion must create exactly one documentation tag on the site's main commit" \
    '[.[] | select(.[0:3] == ["release", "create", "v0.7.0"])
        | select(index("--target") as $i | $i != null and .[$i + 1] == "1111111111111111111111111111111111111111")]
     | length == 1'
check_calls "the publication note belongs on current request #119" \
    'any(.[]; .[0:3] == ["issue", "comment", "119"])'

if [ -s "$scratch/stub-errors" ]; then
    echo "SETUP: the GitHub stub did not support the invocation" >&2
    cat "$scratch/stub-errors" >&2
    exit 2
fi
if [ -s "$scratch/failures" ]; then
    echo "FAIL - a retired request must not hide the current release attempt through completion"
    cat "$scratch/failures"
    printf '\nOpen current request:\n'
    cat "$scratch/open.output"
    printf '\nCompleted current request:\n'
    cat "$scratch/completed.output"
    exit 1
fi
echo "ok - a retired request does not hide the current release attempt through completion"
