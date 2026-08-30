#!/usr/bin/env bash
# Reading one "## <heading>" section out of a pull-request body, with HTML comments removed.
#
# Sourced, never run. Two gates need it and they need it to answer identically: the template gate
# asks whether a section was left as it shipped, and the documentation-impact gate asks whether a
# field says "none" or says nothing. Both questions reduce to the same one -- what is left once the
# template's own guidance comments are taken out -- and a second implementation of that reduction is
# a second set of edge cases at the two ends where it is easy to get wrong: swallowing the next
# heading, or leaving a stripped comment behind as a blank that reads as an answer.
#
# Reads the body from the caller's `body` variable.

# shellcheck disable=SC2154  # `body` belongs to whoever sources this

# The text under one heading, comments removed and blank lines dropped. The level defaults to "##";
# pass "###" for a subsection. A section ends at the next heading of the same level or shallower, so
# a subsection nested inside one is part of it and does not cut it short.
section_body() {
  local marker="${2:-##}"
  printf '%s\n' "$body" | awk -v want="$marker $1" -v depth="${#marker}" '
    { heading = $0; sub(/[ \t\r]+$/, "", heading) }
    heading == want { inside = 1; next }
    inside && match(heading, /^#+ /) && RLENGTH - 1 <= depth { inside = 0 }
    inside { print }
  ' | strip_comments
}

# HTML comments out, surrounding whitespace out, empty lines dropped. Spans multiple lines, because
# the template's guidance does.
strip_comments() {
  awk '
    {
      line = $0
      if (comment) {
        if (index(line, "-->")) { sub(/.*-->/, "", line); comment = 0 } else { line = "" }
      }
      if (!comment) {
        while (match(line, /<!--.*-->/)) { sub(/<!--.*-->/, "", line) }
        if (index(line, "<!--")) { sub(/<!--.*/, "", line); comment = 1 }
      }
      gsub(/^[ \t\r]+|[ \t\r]+$/, "", line)
      if (line != "") print line
    }
  '
}
