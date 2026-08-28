#!/usr/bin/env bash
# Cases for the diff-coverage summary, driven against a scratch repository and a hand-written
# JaCoCo report shaped like each one.
#
# Two of these carry almost all the weight, and neither is "it computes a percentage".
#
# The first is the denominator. A change adds blank lines, comments, imports and closing braces
# along with its code, and none of those are executable - JaCoCo does not report them at all. An
# implementation that treats "added line absent from the report" as uncovered produces a number
# that falls when you add a comment, which is worse than no number: it is a number people learn to
# ignore. So a case here adds exactly two executable lines among several that are not, and pins
# the denominator to two.
#
# The second is what happens when the range cannot be resolved. A diff against a base that is not
# there produces no output and exits non-zero, and a loop over no output finds no added lines -
# which renders as "nothing new to measure", the same words as a change that genuinely added no
# code. That is the failure this file exists to catch: the summary has to say it could not look,
# in different words from having looked and found nothing.
#
# Run it from anywhere. Exits 0 if every case holds.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/diff-coverage.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
passed=0
failed=0

# A source file whose executable lines - by the report written beside it - are 5 and 7.
widget_src() {
  cat <<'JAVA'
package io.tapstate.demo;

// A comment, which is not executable and must not reach the denominator.
final class Widget {
    static int covered() { return 1; }

    static int missed() { return 2; }

}
JAVA
}

# JaCoCo reports only executable lines. `ci` is instructions covered; zero means missed.
jacoco_report() { # jacoco_report <path> <pkg> <file> <nr:ci> ...
  local path="$1" pkg="$2" file="$3"
  shift 3
  mkdir -p "$(dirname "$path")"
  {
    printf '<?xml version="1.0" encoding="UTF-8"?>\n<report name="cases">\n'
    printf '  <package name="%s">\n    <sourcefile name="%s">\n' "$pkg" "$file"
    local pair
    for pair in "$@"; do
      printf '      <line nr="%s" mi="0" ci="%s" mb="0" cb="0"/>\n' "${pair%%:*}" "${pair##*:}"
    done
    printf '    </sourcefile>\n  </package>\n</report>\n'
  } > "$path"
}

fresh_repo() {
  rm -rf "${scratch:?}/repo"
  mkdir -p "$scratch/repo"
  cd "$scratch/repo" || exit 1
  git init -q -b main .
  git config user.email cases@example.invalid
  git config user.name "A Contributor"
  echo base > README.md
  git add -A && git commit -qm base
  git checkout -q -b feature
}

add_widget() { # the whole file is new, so every one of its lines is an added line
  mkdir -p core/src/main/java/io/tapstate/demo
  widget_src > core/src/main/java/io/tapstate/demo/Widget.java
  git add -A && git commit -qm "add a widget"
}

run() { # run [range] -> stdout of the gate
  DIFF_RANGE="${1:-main...HEAD}" bash "$gate" 2>&1
}

expect() { # expect <name> <want text> [range]
  local name="$1" want="$2" out code
  out="$(run "${3:-}")"
  code=$?
  if [ "$code" = 0 ] && printf '%s' "$out" | grep -qF "$want"; then
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        wanted exit 0 containing %s\n        got exit %s: %s\n' \
      "$name" "$want" "$code" "$out"
    failed=$((failed + 1))
  fi
}

refute() { # refute <name> <unwanted text> [range]
  local name="$1" unwanted="$2" out
  out="$(run "${3:-}")"
  if printf '%s' "$out" | grep -qF "$unwanted"; then
    printf '  FAIL  %s\n        did not want %s, got: %s\n' "$name" "$unwanted" "$out"
    failed=$((failed + 1))
  else
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  fi
}

echo "diff-coverage cases"

# --- the denominator is executable lines, not added lines --------------------------------------
fresh_repo
add_widget
jacoco_report core/target/site/jacoco/jacoco.xml io/tapstate/demo Widget.java 5:3 7:0
expect "counts only the lines JaCoCo reports" "1 of 2"
refute "does not count comments and blanks as missed" "1 of 9"
expect "states the percentage" "50%"
expect "names the file that has uncovered new lines" "Widget.java"

# --- a change whose new lines are all covered ---------------------------------------------------
fresh_repo
add_widget
jacoco_report core/target/site/jacoco/jacoco.xml io/tapstate/demo Widget.java 5:3 7:4
expect "all covered reads as full" "2 of 2"
refute "full coverage lists no uncovered file" "Widget.java"

# --- a change whose new lines are all missed ----------------------------------------------------
fresh_repo
add_widget
jacoco_report core/target/site/jacoco/jacoco.xml io/tapstate/demo Widget.java 5:0 7:0
expect "none covered reads as zero" "0 of 2"
expect "zero coverage still reports a percentage" "0%"

# --- no report at all: not measured, and it says why --------------------------------------------
fresh_repo
add_widget
expect "no JaCoCo report is 'not measured'" "not measured"
refute "absent coverage is not reported as full" "of 0 new"

# --- a change that touches no Java ---------------------------------------------------------------
fresh_repo
echo more >> README.md
git add -A && git commit -qm "docs only"
jacoco_report core/target/site/jacoco/jacoco.xml io/tapstate/demo Widget.java 5:3 7:0
expect "a change with no new Java says so" "no new executable lines"

# --- two reports disagree about the same line ----------------------------------------------------
# The one that decides is the highest count, never the one `find | sort` ended on. Without that the
# figure is a property of the filenames: `jacoco-it/` sorts before `jacoco/`, so the unit report
# overwrites the integration one and a line only an integration test reaches reads as missed.
fresh_repo
add_widget
jacoco_report core/target/site/jacoco/jacoco.xml    io/tapstate/demo Widget.java 5:0 7:0
jacoco_report core/target/site/jacoco-it/jacoco.xml io/tapstate/demo Widget.java 5:3 7:4
expect "a line covered only by the integration report counts as covered" "2 of 2"
refute "the last report read does not decide" "0 of 2"

# --- a file no report mentions is not 'nothing to measure' ---------------------------------------
# The mirror of the no-report-at-all case above, one level down: reports were found, just none that
# has ever heard of this file. A module whose own tests never ran writes no execution data, so
# `jacoco:report` skips it while every other module reports normally. Folding that into "the added
# lines are comments and blanks" is the same collapse this file exists to catch.
fresh_repo
add_widget
jacoco_report other/target/site/jacoco/jacoco.xml io/tapstate/other Other.java 3:4
expect "a file in no report says it was not measured" "not measured"
expect "and it names the file nobody measured" "Widget.java"
refute "it is not called comments and blanks" "comments, blanks"
refute "and it invents no percentage for it" "%"

# --- measured and unmeasured in the same change --------------------------------------------------
# The mirror that keeps the case above from being satisfied by a gate that gives up on everything:
# the file the reports do cover still gets its number, and the one they do not is named beside it
# rather than counted as uncovered.
fresh_repo
add_widget
mkdir -p tool/src/main/java/io/tapstate/tool
printf 'package io.tapstate.tool;\n\nfinal class Tool {\n    static int go() { return 1; }\n}\n' \
  > tool/src/main/java/io/tapstate/tool/Tool.java
git add -A && git commit -qm "add a tool"
jacoco_report core/target/site/jacoco/jacoco.xml io/tapstate/demo Widget.java 5:3 7:0
expect "the measured file still reports its number" "1 of 2"
expect "the unmeasured file is named beside it" "Tool.java"
refute "the unmeasured file is not counted as uncovered" "1 of 3"

# --- an unresolvable range must not read as 'nothing new' ----------------------------------------
fresh_repo
add_widget
jacoco_report core/target/site/jacoco/jacoco.xml io/tapstate/demo Widget.java 5:3 7:0
expect "an unresolvable range says it could not look" "not measured" "no-such-base...HEAD"
refute "an unresolvable range is not 'nothing new'" "no new executable lines" "no-such-base...HEAD"
refute "an unresolvable range invents no percentage" "%" "no-such-base...HEAD"

cd /
printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ]
