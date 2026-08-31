#!/usr/bin/env bash
# Zero-agent-footprint gate. This is a public repository that must never carry
# agent-governance artifacts: the governance lives in a private repository, and a copy
# of it here would leak that structure and invite edits nothing upstream would see.
#
# Modes, one per decision, so a test can call each on its own and a red says which:
#   files      agent instruction files at any depth (CLAUDE.md / AGENTS.md / GEMINI.md /
#              CODEX.md / COPILOT.md). Human-facing README.md and CONTRIBUTING.md are
#              not matched -- they are documentation, not instructions to a tool.
#   dir        a .claude/ directory at any depth.
#   messages   agent-generated footers in the pushed commit messages, and in the pull
#              request body. Reads EVENT_NAME, BASE_REF, EVENT_BEFORE, EVENT_SHA, PR_BODY.
#
# Extracted verbatim from .github/workflows/no-agent-footprint.yml: the three decisions
# are unchanged, including the way `messages` builds its range. What changed is that
# they now live somewhere a test can reach them.
set -euo pipefail

MODE="${1:-}"

# Common agent footers; case-insensitive. At the top because the liveness control below
# has to run this exact string -- a control with its own copy of the pattern proves the
# copy works and says nothing about the one that decides.
pattern='Generated with \[?Claude Code|🤖 Generated with|Co-[Aa]uthored-[Bb]y:.*(Claude|GPT|Codex|Copilot|Gemini)|noreply@anthropic\.com'

find_instruction_files() { # <root>
  find "$1" -path "$1/.git" -prune -o -type f \
    \( -iname 'CLAUDE.md' -o -iname 'AGENTS.md' -o -iname 'GEMINI.md' \
       -o -iname 'CODEX.md' -o -iname 'COPILOT.md' \) -print
}

find_claude_dirs() { # <root>
  find "$1" -path "$1/.git" -prune -o -type d -name '.claude' -print
}

# ---- liveness controls ------------------------------------------------------------
#
# Every mode here reports a clean repository by printing nothing and exiting 0, which is
# also exactly what it does when the detector itself has stopped working -- a mistyped
# pattern, a `find` predicate that silently matches nothing, a grep without the flag it
# needs. The sibling character check shipped for two years with a broken commit-message
# scan for that reason: the step said `clean:` on every commit and a person reading a
# diff is what caught it.
#
# So before each mode decides anything about this repository, it decides something about
# a case it already knows the answer to: one value that MUST match and one that MUST NOT.
# Both halves are needed. A detector that matches everything passes the first.
die_detector() { echo "::error::$1"; echo "Nothing this step says about the repository can be trusted, so it is a failure rather than a pass."; exit 1; }

detector_alive_files() {
  local d hit; d="$(mktemp -d)"
  : > "$d/CLAUDE.md"; : > "$d/README.md"
  hit="$(find_instruction_files "$d")"
  rm -rf "$d"
  case "$hit" in *CLAUDE.md*) : ;; *) die_detector "the instruction-file detector did not match a control CLAUDE.md." ;; esac
  case "$hit" in *README.md*) die_detector "the instruction-file detector matched a control README.md, so it matches more than it should." ;; esac
}

detector_alive_dir() {
  local d hit; d="$(mktemp -d)"
  mkdir -p "$d/.claude" "$d/docs"
  hit="$(find_claude_dirs "$d")"
  rm -rf "$d"
  case "$hit" in *.claude*) : ;; *) die_detector "the .claude detector did not match a control .claude directory." ;; esac
  case "$hit" in *docs*) die_detector "the .claude detector matched a control docs directory, so it matches more than it should." ;; esac
}

detector_alive_messages() {
  printf '%s' 'Co-authored-by: Claude <noreply@anthropic.com>' | grep -qEi "$pattern" \
    || die_detector "the footer pattern did not match a control footer."
  if printf '%s' 'Tapstate is a data integration product.' | grep -qEi "$pattern"; then
    die_detector "the footer pattern matched a control sentence with no footer in it, so it matches more than it should."
  fi
}

case "$MODE" in
  files)
    detector_alive_files
    hits="$(find_instruction_files .)"
    if [ -n "$hits" ]; then
      echo "::error::agent instruction file(s) found in a zero-footprint repo:"
      echo "$hits"
      exit 1
    fi
    echo "clean: no agent instruction files."
    ;;

  dir)
    detector_alive_dir
    hits="$(find_claude_dirs .)"
    if [ -n "$hits" ]; then
      echo "::error::.claude directory found in a zero-footprint repo:"
      echo "$hits"
      exit 1
    fi
    echo "clean: no .claude directory."
    ;;

  messages)
    detector_alive_messages
    fail=0

    # The range is resolved, never assumed. On a first push to a branch the previous commit
    # is the all-zero SHA; the range built from it resolves to nothing, `git log` fails, and
    # the failure used to be discarded -- so the scan covered no commit at all and the step
    # said it was clean. Every failure below is fatal instead: a scan that could not look is
    # not a scan that found nothing. Same shape as the sibling character check, deliberately,
    # because a second spelling of this is how the two drift apart again.
    fetch_err=""
    if [ "${EVENT_NAME:-}" = "pull_request" ]; then
      base="origin/${BASE_REF:-main}"
      # Fetching is best-effort and its failure is not fatal here -- but only because the
      # range is resolved for real below, and that failure is. The old form swallowed both.
      if ! git rev-parse --verify --quiet "$base" >/dev/null 2>&1; then
        fetch_err="$(git fetch --quiet --no-tags origin "${BASE_REF:-main}" 2>&1)" || true
      fi
      range="$base..HEAD"
    elif [ -n "${EVENT_BEFORE:-}" ] && git cat-file -e "${EVENT_BEFORE}^{commit}" 2>/dev/null; then
      range="${EVENT_BEFORE}..${EVENT_SHA:-HEAD}"
    else
      # First push, or a previous commit this clone does not have: the pushed commit itself.
      range="${EVENT_SHA:-HEAD}"
      range="${range}~1..${range}"
      git rev-parse --verify --quiet "${range%%~*}~1" >/dev/null 2>&1 || range="${EVENT_SHA:-HEAD}"
    fi

    if ! msgs="$(git log --format='%H %B' "$range" 2>&1)"; then
      echo "::error::cannot list the commits in ${range}, so no commit message was checked: ${msgs}"
      [ -n "$fetch_err" ] && echo "fetching the base branch had already failed: ${fetch_err}"
      exit 1
    fi

    if [ -n "$msgs" ] && printf '%s' "$msgs" | grep -qEi "$pattern"; then
      echo "::error::agent footer found in commit message(s):"
      printf '%s' "$msgs" | grep -Ei "$pattern" || true
      fail=1
    fi

    if [ -n "${PR_BODY:-}" ] && printf '%s' "$PR_BODY" | grep -qEi "$pattern"; then
      echo "::error::agent footer found in the PR body."
      fail=1
    fi

    if [ "$fail" -ne 0 ]; then exit 1; fi
    echo "clean: no agent footers."
    ;;

  *)
    echo "::error::usage: no-agent-footprint.sh <files|dir|messages>"
    exit 2
    ;;
esac
