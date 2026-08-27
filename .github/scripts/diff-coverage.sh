#!/usr/bin/env bash
# Coverage of the lines this change adds, computed from the JaCoCo reports the build already wrote.
#
# Why this exists at all: the new-code coverage condition lives on the SonarQube server, and a
# workflow run from a fork is never given the token that reaches it. So the one number a
# contributor most needs - "is the code I just wrote tested?" - is precisely the number they
# cannot see, and the SonarQube check goes green beside their pull request having measured
# nothing. This reads the reports `mvn verify` produced locally and answers the same question
# without a secret, a server, or a network call.
#
# It is a report, not a gate. It always exits 0. Blocking on it would put a threshold in front of
# outside contributions that our own pull requests are measured against somewhere else entirely,
# and two thresholds that drift apart are worse than one.
#
# Three deliberate choices, each of which is the difference between a number and a useful number:
#
#   - The denominator is executable lines, not added lines. JaCoCo reports only lines that carry
#     instructions; blank lines, comments, imports, package declarations and lone braces are
#     absent from it. Counting those as uncovered makes the figure drop when someone adds a
#     comment, which teaches everyone to ignore it.
#   - Only `src/main/java`. A path with no `src/main/java/` segment is skipped, which is how test
#     sources stay out of their own coverage figure.
#   - "Could not look" is never rendered as "found nothing". A range that does not resolve makes
#     git print nothing and fail, and nothing is also what a change with no Java looks like. The
#     two get different words, because the first is a broken invocation and the second is fine.
#
# Reads DIFF_RANGE (default origin/main...HEAD) and, optionally, JACOCO_REPORTS - a space-separated
# list of report paths, otherwise they are discovered under */target/site/jacoco*/. Writes markdown
# on stdout; the caller appends it to $GITHUB_STEP_SUMMARY.
set -uo pipefail

range="${DIFF_RANGE:-origin/main...HEAD}"

not_measured() { # not_measured <reason>
  printf '### Diff coverage: not measured\n\n%s\n' "$1"
  exit 0
}

# --- the added lines --------------------------------------------------------------------------
# --unified=0 so a hunk header spans exactly the added lines and nothing around them.
if ! diff_out="$(git diff --unified=0 --diff-filter=d "$range" -- '*.java' 2>&1)"; then
  not_measured "The diff range \`${range}\` could not be resolved, so no lines were examined. This is a broken invocation, not a change without new code."
fi

added="$(printf '%s\n' "$diff_out" | awk '
  /^\+\+\+ / {
    path = substr($0, 7)                      # strip "+++ b/"
    if (path == "/dev/null") path = ""
    # The report keys on the package path, which is what follows src/main/java/.
    i = index(path, "src/main/java/")
    key = (i > 0) ? substr(path, i + length("src/main/java/")) : ""
    next
  }
  /^@@ / {
    if (key == "") next
    split($3, h, ",")                         # $3 is "+start,count" (count omitted means 1)
    start = substr(h[1], 2) + 0
    count = (h[2] == "") ? 1 : h[2] + 0
    for (n = 0; n < count; n++) print key, start + n
  }
')"

if [ -z "$added" ]; then
  # shellcheck disable=SC2016 # the backticks are markdown for the summary, not a substitution
  printf '### Diff coverage: no new executable lines in this change\n\nNothing under `src/main/java` was added, so there is nothing to measure.\n'
  exit 0
fi

# --- the reports ------------------------------------------------------------------------------
reports="${JACOCO_REPORTS:-}"
if [ -z "$reports" ]; then
  reports="$(find . -type f -path '*/target/site/jacoco*/jacoco.xml' 2>/dev/null | sort)"
fi
if [ -z "$reports" ]; then
  not_measured "No JaCoCo report was found under \`*/target/site/jacoco*/\`. The build has to run \`mvn verify\` before this can say anything."
fi

# JaCoCo writes its XML as one long line, so split it a tag per line before reading it - and then
# strip the leading indentation, because a report that has been through a formatter arrives with
# the tags already on their own lines and the anchors below would miss every one of them.
# shellcheck disable=SC2086 # the list is newline-separated paths this script produced itself
covered_lines="$(cat $reports 2>/dev/null | sed -e 's/></>\n</g' -e 's/^[[:space:]]*//' | awk '
  function attr(s, name,   m) {
    if (match(s, name "=\"[^\"]*\"") == 0) return ""
    m = substr(s, RSTART + length(name) + 2, RLENGTH - length(name) - 3)
    return m
  }
  /^<package /      { pkg = attr($0, "name"); next }
  /^<sourcefile /   { sf  = attr($0, "name"); next }
  /^<\/sourcefile>/ { sf  = ""; next }
  /^<line /         { if (sf != "") print pkg "/" sf, attr($0, "nr"), attr($0, "ci") }
')"

# --- the join ---------------------------------------------------------------------------------
printf '%s\n' "$covered_lines" > "${TMPDIR:-/tmp}/dc-cov.$$"
result="$(printf '%s\n' "$added" | awk -v covfile="${TMPDIR:-/tmp}/dc-cov.$$" '
  BEGIN {
    while ((getline line < covfile) > 0) {
      n = split(line, f, " ")
      if (n < 3) continue
      k = f[1] SUBSEP f[2]
      v = f[3] + 0
      # The highest count across every report, never the last one read. The same line appears in
      # more than one report - jacoco/ and jacoco-it/ at least - and each of those describes only
      # the runs that wrote it, so a line exercised by an integration test reads as missed in the
      # unit report. Keeping whichever file `find | sort` happened to end on makes the figure a
      # property of the filenames.
      if (!(k in seen) || v > ci[k]) ci[k] = v
      seen[k] = 1
      reported[f[1]] = 1
    }
  }
  {
    k = $1 SUBSEP $2
    if (!(k in seen)) {
      # Two different things reach here, and only one of them is "this line is not executable".
      # A file no report mentions at all was never looked at, and folding that into the same
      # silence renders "could not look" as "found nothing" - one level below the guard above,
      # which only fires when there is no report anywhere.
      if (!($1 in reported)) nofile[$1]++
      next
    }
    total++
    if (ci[k] > 0) hit++; else { miss++; per[$1]++ }
  }
  END {
    printf "%d %d\n", total + 0, hit + 0
    for (src in nofile) printf "NOFILE %s %d\n", src, nofile[src]
    # Not `f`: the BEGIN block split into an array by that name, and awk refuses to reuse it as
    # a scalar - on stderr, while still exiting 0 and printing a table with no rows in it.
    for (src in per) printf "FILE %s %d\n", src, per[src]
  }
')"
rm -f "${TMPDIR:-/tmp}/dc-cov.$$"

total="$(printf '%s' "$result" | head -n1 | cut -d' ' -f1)"
hit="$(printf '%s' "$result" | head -n1 | cut -d' ' -f2)"

# Files that added lines and appear in no report at all - a module whose own tests never ran, so
# nothing wrote its exec file and `jacoco:report` skipped it. Named, never counted: putting them
# in the denominator would report untested code and code nobody measured as the same thing.
nofile="$(printf '%s\n' "$result" | awk '$1 == "NOFILE" { printf "`%s`\n", $2 }' | sort)"

if [ "${total:-0}" -eq 0 ]; then
  if [ -n "$nofile" ]; then
    not_measured "$(printf 'These files added lines and appear in no JaCoCo report, so nothing here was measured:\n\n%s\n\nA module whose own tests did not run writes no execution data, and its report is skipped. This is not the same as a change that added no executable code.' "$nofile")"
  fi
  printf '### Diff coverage: no new executable lines in this change\n\nThe added lines are comments, blanks or declarations that JaCoCo does not instrument.\n'
  exit 0
fi

pct=$(( hit * 100 / total ))
printf '### Diff coverage: **%d of %d new executable lines covered (%d%%)**\n\n' "$hit" "$total" "$pct"

if [ "$hit" -lt "$total" ]; then
  printf 'New lines with no test exercising them:\n\n| File | Uncovered new lines |\n|---|---|\n'
  printf '%s\n' "$result" | awk '$1 == "FILE" { printf "| `%s` | %s |\n", $2, $3 }' | sort
  printf '\n'
fi

if [ -n "$nofile" ]; then
  printf 'Not measured at all - these files appear in no JaCoCo report, so they are in neither number above:\n\n'
  printf '%s\n' "$nofile" | sed 's/^/- /'
  printf '\n'
fi

# shellcheck disable=SC2016 # the backticks are markdown for the summary, not a substitution
printf '_Measured from the JaCoCo reports this run wrote, against `%s`. This is a report, not a gate._\n' "$range"
