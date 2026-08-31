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
      # Index-based rather than a regex: awk has no lazy quantifier, and a greedy <!--.*--> spans
      # from the first opener to the last closer, taking whatever the author wrote between two
      # comments with it. The same applies to closing an open comment on a line that opens another.
      if (comment) {
        e = index(line, "-->")
        if (e) { line = substr(line, e + 3); comment = 0 } else { line = "" }
      }
      if (!comment) {
        while ((s = index(line, "<!--")) > 0) {
          rest = substr(line, s + 4)
          e = index(rest, "-->")
          if (e == 0) { line = substr(line, 1, s - 1); comment = 1; break }
          line = substr(line, 1, s - 1) substr(rest, e + 3)
        }
      }
      gsub(/^[ \t\r]+|[ \t\r]+$/, "", line)
      if (line != "") print line
    }
  '
}

# Whether what an author wrote amounts to "none". Shared because two gates ask it -- the check on a
# pull request, and the collection of release notes -- and they must not answer differently: a field
# read as an answer by one and as a value by the other refuses a pull request that is correct, and
# tells its author to add a label they must not add.
#
# The first word decides, so a full stop or a reason after it still counts. Real answers look like
# "none.", "none -- the lane documents itself in its header", "None"; all of them are the author
# having judged it. "nonetheless" is not, which is why it is the whole first word and not a prefix.
is_none() {
  [ "$(printf '%s' "$1" | head -1 | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z].*//')" = none ]
}
