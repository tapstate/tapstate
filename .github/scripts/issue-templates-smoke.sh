#!/usr/bin/env bash
# The issue chooser, asserted at the file level - it is data, and data regresses silently.
#
# Both properties below were once wrong, and neither showed up as a failure anywhere:
#
#   - The proposal template carried no label, so `scripts/issue-list.sh` filed a proposal under
#     "triaged, nobody took it" and nobody owed it a reply. Measured on a real proposal fifteen
#     minutes old; nothing was red, because nothing was looking.
#   - A hand-written contact link duplicated the entry GitHub adds by itself when private
#     vulnerability reporting is on, so the chooser showed "Report a security vulnerability" twice,
#     pointing both times at the same place. To someone who came here to report a vulnerability,
#     that is what a broken page looks like.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
tpl="$here/../ISSUE_TEMPLATE"
passed=0; failed=0
check() { if [ "$2" = 0 ]; then printf '  ok    %s\n' "$1"; passed=$((passed+1));
          else printf '  FAIL  %s\n' "$1"; failed=$((failed+1)); fi; }

echo "issue template cases"

for t in intake proposal; do
  check "the $t template exists" "$([ -f "$tpl/$t.yml" ] && echo 0 || echo 1)"
  # Both lanes, not just the bug one: both are somebody outside asking, and the triage list keys on
  # this label. A lane without it is invisible to the only thing that surfaces untriaged reports.
  check "the $t lane is labelled needs-triage" \
    "$(grep -q 'needs-triage' "$tpl/$t.yml" && echo 0 || echo 1)"
done

check "blank issues stay off" \
  "$(grep -q 'blank_issues_enabled: false' "$tpl/config.yml" && echo 0 || echo 1)"
# Directives, not prose: config.yml explains at length why there are none, and a check that cannot
# tell an explanation from an instruction fails on the explanation.
check "no hand-written contact link duplicates GitHub's own security entry" \
  "$(grep -v '^[[:space:]]*#' "$tpl/config.yml" | grep -q 'contact_links' && echo 1 || echo 0)"

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ]
