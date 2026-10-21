#!/usr/bin/env bash
# Offline cases for the shared PR reader and the release scans that use it.
set -euo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PREFIX GIT_QUARANTINE_PATH

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=.github/scripts/_pr-read.sh
. "$here/_pr-read.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
export PR_READ_SMOKE_SCRATCH="$scratch"
export GITHUB_REPOSITORY=tapstate/tapstate
mkdir -p "$scratch/bin" "$scratch/repo"
passed=0
failed=0

cat > "$scratch/bin/gh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$PR_READ_SMOKE_SCRATCH/calls"
[ "${1:-}" = pr ] && [ "${2:-}" = view ] || exit 2
attempt="$(wc -l < "$PR_READ_SMOKE_SCRATCH/calls")"
case "$PR_READ_SMOKE_MODE" in
  healthy)
    echo 'a warning from a successful read' >&2 ;;
  transient)
    if [ "$attempt" -lt 3 ]; then
      echo 'partial stdout from a failed read'
      echo 'HTTP 502: Bad Gateway (https://api.github.com/graphql)' >&2
      exit 1
    fi ;;
  missing)
    echo "GraphQL: Could not resolve to a PullRequest with the number of $3. (repository.pullRequest)" >&2
    exit 1 ;;
  unavailable)
    echo 'partial stdout from a failed read'
    echo 'HTTP 502: Bad Gateway (https://api.github.com/graphql)' >&2
    exit 1 ;;
  denied)
    echo 'HTTP 401: Bad credentials (https://api.github.com/graphql)' >&2
    exit 4 ;;
  repository-missing)
    echo "GraphQL: Could not resolve to a Repository with the name 'tapstate/tapstate'. (repository)" >&2
    exit 1 ;;
  mixed)
    echo "GraphQL: Could not resolve to a PullRequest with the number of $3. (repository.pullRequest)" >&2
    echo 'HTTP 502: Bad Gateway (https://api.github.com/graphql)' >&2
    exit 1 ;;
  silent) exit 1 ;;
  empty) exit 0 ;;
  *) exit 2 ;;
esac
cat "$PR_READ_SMOKE_SCRATCH/payload"
STUB
cat > "$scratch/bin/sleep" <<'STUB'
#!/bin/sh
printf '%s\n' "$1" >> "$PR_READ_SMOKE_SCRATCH/delays"
STUB
chmod +x "$scratch/bin/gh" "$scratch/bin/sleep"
export PATH="$scratch/bin:$PATH"
printf '%s' '{"body":"A release note.\nA second line.","labels":[],"author":{"login":"t"},"url":"https://github.com/tapstate/tapstate/pull/485"}' > "$scratch/payload"

run() {
  : > "$scratch/calls"
  : > "$scratch/delays"
  status=0
  "$@" > "$scratch/stdout" 2> "$scratch/stderr" || status=$?
  reads="$(wc -l < "$scratch/calls" | tr -d '[:space:]')"
}
record() {
  if [ "$2" = 0 ]; then
    printf 'ok - %s\n' "$1"
    passed=$((passed + 1))
  else
    printf 'FAIL - %s (exit %s, %s read(s))\n' "$1" "$status" "$reads"
    cat "$scratch/stdout" "$scratch/stderr" "$scratch/calls" "$scratch/delays"
    failed=$((failed + 1))
  fi
}
read_case() {
  local name="$1" mode="$2" want_status="$3" want_reads="$4" verdict=0
  export PR_READ_SMOKE_MODE="$mode"
  run read_pr 485 --json body,labels,author,url
  [ "$status" = "$want_status" ] && [ "$reads" = "$want_reads" ] || verdict=1
  if [ "$mode" = healthy ] || [ "$mode" = transient ]; then
    cmp -s "$scratch/payload" "$scratch/stdout" || verdict=1
  else
    [ ! -s "$scratch/stdout" ] || verdict=1
  fi
  if [ "$want_reads" = 3 ]; then
    [ "$(cat "$scratch/delays")" = $'1\n2' ] || verdict=1
    grep -qF '#485' "$scratch/stderr" || verdict=1
    if [ "$want_status" = 1 ]; then
      grep -qF 'after 3 attempts' "$scratch/stderr" || verdict=1
    fi
  else
    [ ! -s "$scratch/delays" ] && [ ! -s "$scratch/stderr" ] || verdict=1
  fi
  if [ "$mode" = unavailable ] || [ "$mode" = transient ] || [ "$mode" = mixed ]; then
    grep -qF 'HTTP 502' "$scratch/stderr" || verdict=1
  fi
  if [ "$mode" = denied ]; then
    grep -qF 'HTTP 401' "$scratch/stderr" || verdict=1
  fi
  if [ "$mode" = silent ]; then
    grep -qF 'no diagnostic from gh' "$scratch/stderr" || verdict=1
  fi
  [ "$(sort -u "$scratch/calls")" = 'pr view 485 --json body,labels,author,url' ] || verdict=1
  record "$name" "$verdict"
}

read_case 'a successful read preserves stdout and JSON flags without stderr pollution' healthy 0 1
read_case 'two transient failures recover on the third read' transient 0 3
read_case 'an explicit non-PR number is skipped without a retry' missing 0 1
read_case 'a persistent server failure refuses without emitting partial stdout' unavailable 1 3
read_case 'an authentication failure is not mistaken for an absent PR' denied 1 3
read_case 'an inaccessible repository is not mistaken for an absent PR' repository-missing 1 3
read_case 'a non-PR diagnostic alongside another error is not silently skipped' mixed 1 3
read_case 'a failure with no diagnostic still refuses and names the PR' silent 1 3
read_case 'a successfully read empty body is not retried' empty 0 1

# The same failed read must stop the other range scans before they claim success or write a board.
repo="$scratch/repo"
git -C "$repo" init -q -b main
git -C "$repo" config user.email t@example.com
git -C "$repo" config user.name t
git -C "$repo" config commit.gpgSign false
git -C "$repo" config core.hooksPath /dev/null
git -C "$repo" commit -q --allow-empty -m base
git -C "$repo" tag v0.5.0
git -C "$repo" commit -q --allow-empty -m 'A change with a release note (#485)'
export PR_READ_SMOKE_MODE=unavailable
scan() {
  local script="$1"
  shift
  (cd "$repo" && bash "$here/$script" "$@")
}

run scan docs-gate.sh --base v0.5.0 --sha HEAD --bump minor
verdict=0
[ "$status" = 1 ] && [ "$reads" = 3 ] && [ ! -s "$scratch/stdout" ] && \
  grep -qF '#485' "$scratch/stderr" || verdict=1
record 'the documentation gate refuses an incomplete PR scan' "$verdict"

run scan docs-gate.sh --base v0.5.0 --sha HEAD --report-only --list-open "$scratch/open.tsv"
verdict=0
[ "$status" = 0 ] && [ "$reads" = 3 ] && \
  grep -qF 'No verdict was reached.' "$scratch/stdout" && \
  grep -qF '#485' "$scratch/stderr" || verdict=1
record 'the best-effort documentation report makes the failed read visible' "$verdict"

run scan roadmap-shipped.sh --base v0.5.0 --sha HEAD --version 0.6.0 --dry-run
verdict=0
[ "$status" = 1 ] && [ "$reads" = 3 ] && [ ! -s "$scratch/stdout" ] && \
  grep -qF '#485' "$scratch/stderr" || verdict=1
record 'the roadmap refuses an incomplete scan before reaching the board' "$verdict"

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
