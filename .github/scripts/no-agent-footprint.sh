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

case "$MODE" in
  files)
    hits="$(find . -path ./.git -prune -o -type f \
      \( -iname 'CLAUDE.md' -o -iname 'AGENTS.md' -o -iname 'GEMINI.md' \
         -o -iname 'CODEX.md' -o -iname 'COPILOT.md' \) -print)"
    if [ -n "$hits" ]; then
      echo "::error::agent instruction file(s) found in a zero-footprint repo:"
      echo "$hits"
      exit 1
    fi
    echo "clean: no agent instruction files."
    ;;

  dir)
    hits="$(find . -path ./.git -prune -o -type d -name '.claude' -print)"
    if [ -n "$hits" ]; then
      echo "::error::.claude directory found in a zero-footprint repo:"
      echo "$hits"
      exit 1
    fi
    echo "clean: no .claude directory."
    ;;

  messages)
    # Common agent footers; case-insensitive.
    pattern='Generated with \[?Claude Code|🤖 Generated with|Co-[Aa]uthored-[Bb]y:.*(Claude|GPT|Codex|Copilot|Gemini)|noreply@anthropic\.com'
    fail=0

    if [ "${EVENT_NAME:-}" = "pull_request" ]; then
      base="origin/${BASE_REF:-main}"
      git rev-parse --verify --quiet "$base" >/dev/null 2>&1 \
        || git fetch --quiet --no-tags origin "${BASE_REF:-main}" || true
      range="$base..HEAD"
    else
      range="${EVENT_BEFORE:-}..${EVENT_SHA:-HEAD}"
    fi

    msgs="$(git log --format='%H %B' "$range" 2>/dev/null || true)"
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
