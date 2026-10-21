#!/usr/bin/env bash
# Read a pull request without treating an unavailable response as an absent pull request.
# Explicit non-PR numbers return an empty answer; other failures are retried, then refused.
read_pr() {
  local number="$1" error_file result error read_status attempt
  shift
  error_file="$(mktemp)" || {
    printf '::error::cannot capture the read error for pull request #%s\n' "$number" >&2
    return 1
  }
  for attempt in 1 2 3; do
    if result="$(gh pr view "$number" "$@" 2> "$error_file")"; then
      rm -f "$error_file"
      printf '%s' "$result"
      return 0
    else
      read_status=$?
    fi
    error="$(cat "$error_file")"
    # Match the complete diagnostic, including the requested number and GraphQL field.
    # Repository, authentication and transport failures do not establish that this is not a PR.
    if [ "$error" = "GraphQL: Could not resolve to a PullRequest with the number of ${number}. (repository.pullRequest)" ]; then
      rm -f "$error_file"
      return 0
    fi
    printf 'could not read pull request #%s (attempt %s/3, exit %s): %s\n' \
      "$number" "$attempt" "$read_status" "${error:-no diagnostic from gh}" >&2
    if [ "$attempt" -lt 3 ] && ! sleep "$attempt"; then
      rm -f "$error_file"
      return 1
    fi
  done
  rm -f "$error_file"
  printf '::error::could not read pull request #%s after 3 attempts; refusing incomplete release data\n' "$number" >&2
  return 1
}
