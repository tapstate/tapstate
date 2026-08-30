#!/usr/bin/env bash
# No-CJK gate: this repository is English-only, and neither its tracked files nor its commit
# messages may contain CJK characters - Chinese/Japanese/Korean ideographs, CJK punctuation, or
# full-width / half-width forms. No directory is exempt by default; .cjk-allowlist at the repo
# root is the only escape hatch, and it is a reviewed decision per path.
#
# The decision used to live inline in the workflow, which made it the one gate here with no cases
# able to reach it. That is not a hypothetical: a `--format` string was once written with doubled
# percent signs, git printed the format verbatim instead of the messages, and the commit-message
# scan went permanently empty - reporting clean forever. It was caught by someone reading a diff.
# A false green and a real green are the same text, so the only thing that tells them apart is a
# case that fails when the decision is broken.
#
# Three modes, one per thing that gets scanned, so each keeps its own step and its own narrowing:
#
#   files      scan tracked files. Honours .cjk-allowlist (git pathspec, one per line).
#   messages   scan commit messages. Reads EVENT_NAME, BASE_REF, EVENT_BEFORE, EVENT_SHA.
#   pr-body    scan a pull request body. Reads PR_BODY.
#
# Exits 0 with a "clean:" line, or 1 naming what it found.
set -euo pipefail

# A UTF-8 locale is required so PCRE treats the pattern as code points rather than bytes. Set,
# never defaulted to: under LC_ALL=C the pattern below is rejected outright as out-of-range code
# points, so inheriting whatever the caller had turns the caller's locale into part of the gate.
export LC_ALL=C.UTF-8

# CJK Unified Ideographs + Ext-A + Ext-B, CJK symbols/punctuation, and full-width / half-width
# forms. Defined once for every mode: separate copies drift, and a gate whose steps disagree about
# what CJK is goes green on the character one of them stopped catching. Overridable only so the
# cases can drive a deliberately broken detector at the self-check below.
if [ -z "${CJK:-}" ]; then
  CJK='[\x{3000}-\x{303f}\x{3400}-\x{4dbf}\x{4e00}-\x{9fff}\x{ff00}-\x{ffef}\x{20000}-\x{2a6df}]'
fi

# The detector answers correctly on two known values before it is trusted on unknown ones. A
# pattern that has stopped matching anything, or that matches everything, produces exactly the
# output of a clean repository and of a hopelessly dirty one respectively - neither of which any
# later step can tell from the truth. The probe is written as bytes so that this file, which the
# gate also scans, stays free of the characters it is looking for.
probe="$(printf '\xE4\xB8\xAD')"
if ! printf '%s' "$probe" | grep -qP "$CJK"; then
  echo "::error::CJK detector missed a known CJK control value, so nothing it reports can be trusted."
  exit 1
fi
if printf '%s' "Tapstate" | grep -qP "$CJK"; then
  echo "::error::CJK detector rejected an ASCII control value."
  exit 1
fi

case "${1:-}" in
  files)
    # Build pathspec exclusions from the allowlist; ignore comments / blanks.
    pathspec=(':(top)')
    if [ -f .cjk-allowlist ]; then
      while IFS= read -r line; do
        line="${line%%#*}"
        line="$(printf '%s' "$line" | tr -d '[:space:]')"
        [ -z "$line" ] && continue
        pathspec+=(":(top,exclude)$line")
      done < .cjk-allowlist
    fi

    if git grep -nP "$CJK" -- "${pathspec[@]}"; then
      echo "::error::CJK character(s) found in an English-only repo (matches above)."
      echo "tapstate is English-only. If a file legitimately needs CJK, add its path to .cjk-allowlist."
      exit 1
    fi
    echo "clean: no CJK in tracked files."
    ;;

  messages)
    # A commit message is repository content: this repository squashes with the commit-message
    # mode, so a message written in another language lands in main's history verbatim and the
    # merge does not launder it. Rewriting one afterwards means a force-push. So this scan, like
    # the tracked-files one above, applies to everyone with no exception.
    #
    # The range selection below is carried over unchanged from the inline step, swallowed errors
    # included: on a first push to a branch EVENT_BEFORE is the all-zero SHA, git log fails, the
    # failure is discarded and the scan silently covers nothing. Repairing that changes what the
    # gate decides and belongs to a change that brings its own case; this one only moves code.
    if [ "${EVENT_NAME:-}" = "pull_request" ]; then
      base="origin/${BASE_REF:-main}"
      git rev-parse --verify --quiet "$base" >/dev/null 2>&1 \
        || git fetch --quiet --no-tags origin "${BASE_REF:-main}" || true
      range="$base..HEAD"
    else
      range="${EVENT_BEFORE:-}..${EVENT_SHA:-}"
    fi

    msgs="$(git log --format='%H %B' "$range" 2>/dev/null || true)"
    if [ -n "$msgs" ] && printf '%s' "$msgs" | grep -qP "$CJK"; then
      echo "::error::CJK found in commit message(s):"
      printf '%s' "$msgs" | grep -nP "$CJK" || true
      exit 1
    fi
    echo "clean: no CJK in commit messages."
    ;;

  pr-body)
    # A pull request body is conversational text, and that is a different thing from repository
    # content even though the same gate checks both. Which pull requests reach this mode is the
    # caller's decision, not this script's.
    if [ -n "${PR_BODY:-}" ] && printf '%s' "$PR_BODY" | grep -qP "$CJK"; then
      echo "::error::CJK found in the pull request body."
      exit 1
    fi
    echo "clean: no CJK in the pull request body."
    ;;

  *)
    echo "::error::no-cjk.sh needs a mode: files | messages | pr-body (got '${1:-}')."
    exit 1
    ;;
esac
