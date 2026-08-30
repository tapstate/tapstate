#!/usr/bin/env bash
# Character check for an English-only repository.
#
# It asks whether every character is one this repository has agreed to use, and names the ones
# that are not. It used to ask the opposite question - whether any character fell inside a list of
# CJK ranges - and the difference is which way it fails. A range nobody thought to enumerate is
# admitted silently; the ranges in use here admitted Japanese kana, Korean hangul, Cyrillic
# look-alikes, the zero-width space and the right-to-left override, and a broken pattern admitted
# everything. An unknown character now stops a merge and gets looked at, which is a decision
# someone makes rather than a gap nobody sees.
#
# What is allowed: ASCII, plus the code points enumerated in the allow-list file. That file is the
# only place they are written down, and this script refuses to run rather than guess when it
# cannot be read - "the allow-list is missing" and "this repository is full of unknown characters"
# are both red, but only one of them tells you what to fix.
#
# The decision used to live inline in the workflow, which made it the one check here with no cases
# able to reach it. A `--format` string was once written with doubled percent signs, git printed
# the format instead of the messages, and the commit-message scan went permanently empty -
# reporting clean until a person happened to read the diff. A false green and a real green are the
# same text, so the cases are the only thing that tells them apart.
#
# Three modes, one per thing that gets asked about, so each keeps its own step and its own
# narrowing:
#
#   files      scan tracked files. Honours the path allow-list (git pathspec, one per line).
#   messages   scan commit messages. Reads EVENT_NAME, BASE_REF, EVENT_BEFORE, EVENT_SHA.
#   text       name the unknown characters in stdin, and exit 0 either way. A question, not a
#              gate - see the mode itself for why that difference is load-bearing.
#
# Paths are read from CHARSET_ALLOWLIST and PATH_ALLOWLIST so a copy of this script can run in a
# repository that keeps them elsewhere.
#
# The scanning modes exit 0 with a "clean:" line, or 1 naming every unknown character with its
# code point and where it is. `text` is the exception and says so where it is defined.
set -euo pipefail

# A UTF-8 locale is required so PCRE treats the pattern as code points rather than bytes. Set,
# never defaulted to: under LC_ALL=C the pattern built below is rejected outright as out-of-range
# code points, so inheriting whatever the caller had turns the caller's locale into part of this.
export LC_ALL=C.UTF-8

CHARSET_ALLOWLIST="${CHARSET_ALLOWLIST:-.github/charset-allowlist.txt}"
PATH_ALLOWLIST="${PATH_ALLOWLIST:-.cjk-allowlist}"

# The count the allow-list is not allowed to fall below. A floor, never an equality: legitimate
# characters arrive - one did between this list being measured and being written down - and an
# exact count would turn the next honest addition into a red on the day it lands.
ALLOWLIST_FLOOR=26

# --- the allow-list ------------------------------------------------------------------------
# Read before anything is scanned. An unreadable, empty or malformed list is reported as itself
# and nothing is scanned. The tempting alternative - let an empty allow set make every non-ASCII
# character unknown, so it "naturally" goes red - is an untested assumption of exactly the kind
# this check exists to stop relying on, and its red points at thousands of innocent em dashes
# rather than at the file that went missing.
if [ ! -f "$CHARSET_ALLOWLIST" ]; then
  echo "::error::character allow-list not found at ${CHARSET_ALLOWLIST}, so nothing was scanned."
  exit 1
fi
if [ ! -r "$CHARSET_ALLOWLIST" ]; then
  echo "::error::character allow-list at ${CHARSET_ALLOWLIST} cannot be read, so nothing was scanned."
  exit 1
fi

allowed_hex=""
count=0
# Default IFS on purpose: `read` then strips the leading whitespace and hands back the first
# token, which is the whole of the format. Nothing here word-splits an unquoted variable, so a
# note containing a `*` cannot glob against the working directory.
while read -r token rest || [ -n "$token" ]; do
  [ -n "$token" ] || continue
  case "$token" in \#*) continue ;; esac
  case "$token" in
    U+[0-9A-F][0-9A-F][0-9A-F][0-9A-F] | \
    U+[0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F] | \
    U+[0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F][0-9A-F]) ;;
    *)
      echo "::error::character allow-list ${CHARSET_ALLOWLIST} has a line that is not a code point: ${token} ${rest}"
      echo "each entry starts with U+ and 4 to 6 uppercase hex digits; the rest of the line is a note."
      exit 1
      ;;
  esac
  allowed_hex="${allowed_hex}${allowed_hex:+ }${token#U+}"
  count=$((count + 1))
done < "$CHARSET_ALLOWLIST"

if [ "$count" -lt "$ALLOWLIST_FLOOR" ]; then
  echo "::error::character allow-list ${CHARSET_ALLOWLIST} holds ${count} entries, below the floor of ${ALLOWLIST_FLOOR}."
  echo "that is a truncated or emptied list, not a repository that stopped using these characters."
  exit 1
fi

# The negated class the scan looks for: anything that is neither ASCII nor on the list.
UNKNOWN='[^\x00-\x7F'
for h in $allowed_hex; do UNKNOWN="${UNKNOWN}\\x{${h}}"; done
UNKNOWN="${UNKNOWN}]"

# Overridable only so the cases can drive a deliberately broken detector at the three checks
# below. Written as an if rather than ${VAR:-$UNKNOWN}: the pattern contains braces of its own,
# and inside a ${...:-default} the first of them ends the expansion and silently truncates it.
if [ -n "${CHARSET_UNKNOWN_PATTERN:-}" ]; then
  UNKNOWN="$CHARSET_UNKNOWN_PATTERN"
fi

# Turn "file:line:text" on stdin into one report line per unknown character. The same function
# serves every mode, so a character is named the same way wherever it was found - and so the
# one exemption below holds everywhere rather than in whichever mode someone remembered.
#
# A person's name is not English prose and is not ours to constrain. Sign-off and authorship
# trailers therefore carry any script at all: this project asks contributors to sign their commits,
# and a check that reddens on the name they signed with would turn that into a door. The exemption
# is by line shape, never by widening the allowed characters - widening would let the same
# characters through anywhere in the file, which is the thing being kept out. It costs one hole,
# stated rather than discovered: prose written on a line that begins with one of these trailers is
# not scanned.
name_unknown() {
  ALLOWED_HEX="$allowed_hex" perl -CSD -ne '
    BEGIN { %ok = map { hex($_) => 1 } split " ", $ENV{ALLOWED_HEX} }
    next unless /^(.*?):(\d+):(.*)$/;
    my ($where, $ln, $text) = ($1, $2, $3);
    next if $text =~ /^\s*(?:Signed-off-by|Co-authored-by)\s*:/i;
    for my $c (split //, $text) {
      my $o = ord $c;
      next if $o < 0x80 || $ok{$o};
      printf("  U+%04X  %s  %s:%d\n", $o, $c, $where, $ln);
    }
  '
}

# --- the detector answers on known values before it is trusted on unknown ones ---------------
# A pattern that matches nothing looks exactly like a clean repository; one that matches
# everything looks like a repository full of unknown characters. Neither is something a later step
# can tell from the truth. The third control is the one the white-list form adds: an allow-list
# that was read but never reached the pattern would redden every legitimate character in the
# repository, and it is taken from the list itself so that editing the list cannot invalidate it.
probe="$(printf '\xE4\xB8\xAD')"   # U+4E2D, written as bytes so this file stays free of it
first_allowed="$(printf '%s' "$allowed_hex" | cut -d' ' -f1)"
allowed_char="$(perl -CSD -e 'print chr(hex($ARGV[0]))' "$first_allowed")"

if ! printf '%s' "$probe" | grep -qP "$UNKNOWN"; then
  echo "::error::the check did not flag a character that is not on the allow-list, so nothing it reports can be trusted."
  exit 1
fi
if printf '%s' "Tapstate" | grep -qP "$UNKNOWN"; then
  echo "::error::the check flagged plain ASCII, so nothing it reports can be trusted."
  exit 1
fi
if printf '%s' "$allowed_char" | grep -qP "$UNKNOWN"; then
  echo "::error::the check flagged U+${first_allowed}, which is on the allow-list: the list was read but never reached the pattern."
  exit 1
fi

case "${1:-}" in
  files)
    # Build pathspec exclusions from the path allow-list; ignore comments / blanks. AUTHORS is
    # excluded ahead of it and not through it: the path allow-list is the per-file escape hatch
    # somebody reviews, and a file whose every line is a person's name is the same decision as the
    # trailer exemption above rather than an exception to the rule.
    pathspec=(':(top)' ':(top,exclude)AUTHORS')
    if [ -f "$PATH_ALLOWLIST" ]; then
      while IFS= read -r line; do
        line="${line%%#*}"
        line="$(printf '%s' "$line" | tr -d '[:space:]')"
        [ -z "$line" ] && continue
        pathspec+=(":(top,exclude)$line")
      done < "$PATH_ALLOWLIST"
    fi

    hits="$(git grep -nP "$UNKNOWN" -- "${pathspec[@]}" || true)"
    found="$(printf '%s' "$hits" | name_unknown)"
    if [ -n "$found" ]; then
      echo "::error::character(s) not on the allow-list, in tracked files:"
      printf '%s\n' "$found" | head -50
      total="$(printf '%s\n' "$found" | wc -l | tr -d ' ')"
      [ "$total" -gt 50 ] && echo "  ... and $((total - 50)) more"
      echo "add the character to ${CHARSET_ALLOWLIST} if English typesetting here needs it, or add the file to ${PATH_ALLOWLIST}."
      exit 1
    fi
    echo "clean: every character in tracked files is on the allow-list."
    ;;

  messages)
    # A commit message is repository content: this repository squashes with the commit-message
    # mode, so a message written in another language lands in main's history verbatim and the
    # merge does not launder it. Rewriting one afterwards means a force-push. So this scan, like
    # the tracked-files one, applies to everyone with no exception.
    #
    # The range is resolved, never assumed. On a first push to a branch the previous commit is the
    # all-zero SHA, which resolves to nothing; the range used to be built from it anyway and the
    # resulting failure was discarded, so the scan covered nothing and said it was clean. Every
    # failure below is fatal instead, because a scan that could not look is not a scan that found
    # nothing.
    fetch_err=""
    if [ "${EVENT_NAME:-}" = "pull_request" ]; then
      base="origin/${BASE_REF:-main}"
      # Fetching is best-effort and its failure is deliberately not fatal here - but only because
      # the range is resolved for real a few lines down, and that failure is. The old form
      # swallowed both, which is how a scan that could not reach its base reported clean.
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

    if ! shas="$(git rev-list "$range" 2>&1)"; then
      echo "::error::cannot list the commits in ${range}, so no commit message was checked: ${shas}"
      [ -n "$fetch_err" ] && echo "fetching the base branch had already failed: ${fetch_err}"
      exit 1
    fi

    found=""
    for sha in $shas; do
      msg="$(git log -1 --format='%B' "$sha" | awk -v s="$sha" '{printf "%s:%d:%s\n", s, NR, $0}' | name_unknown)"
      # Command substitution eats the trailing newline, so put one back per commit. Without it two
      # commits' reports run together on one line, and so does the advice printed after them.
      [ -n "$msg" ] && found="${found}${msg}"$'\n'
    done
    if [ -n "$found" ]; then
      echo "::error::character(s) not on the allow-list, in commit message(s):"
      printf '%s' "$found" | head -50
      echo "commit messages here are English. Rewrite the message with git commit --amend or git rebase."
      exit 1
    fi
    echo "clean: every character in the commit messages is on the allow-list."
    ;;

  text)
    # Answers a question; it does not gate. Reads text on stdin and prints the unknown characters
    # in it, one per line, and nothing at all when there are none. Exit status says only whether
    # the check could run: a non-zero exit here means the allow-list guards above refused, never
    # that the text was dirty. Read the output for that.
    #
    # The one caller is the intake translator, which asks "was this report written in English" and
    # is a courtesy beside somebody's bug report - it must never fail, so a mode that reddened on
    # finding something would be unusable to it. It also has to tell "this text is foreign" from
    # "nobody could look", and a gate collapses those two into one non-zero exit.
    #
    # It goes through name_unknown like every other mode rather than asking the same question a
    # second way. Two spellings of "which characters are English here" drift, and the drift shows
    # up as one of them quietly answering for the other.
    awk '{printf "(text):%d:%s\n", NR, $0}' | name_unknown
    ;;

  *)
    echo "::error::no-cjk.sh needs a mode: files | messages | text (got '${1:-}')."
    exit 1
    ;;
esac
