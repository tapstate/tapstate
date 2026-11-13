#!/usr/bin/env bash
set -euo pipefail

shellcheck .github/scripts/checks-on-commit.sh .github/scripts/checks-on-commit-smoke.sh \
  .github/scripts/docs-gate.sh .github/scripts/docs-gate-smoke.sh \
  .github/scripts/release-notes.sh .github/scripts/release-notes-smoke.sh \
  .github/scripts/release-notes-read-failure-smoke.sh \
  .github/scripts/_pr-read.sh .github/scripts/_pr-read-smoke.sh \
  .github/scripts/docs-impact.sh .github/scripts/_pr-section.sh
bash .github/scripts/_pr-read-smoke.sh
bash .github/scripts/checks-on-commit-smoke.sh
bash .github/scripts/docs-gate-smoke.sh
bash .github/scripts/release-notes-smoke.sh
bash .github/scripts/release-notes-read-failure-smoke.sh
