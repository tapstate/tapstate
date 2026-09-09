#!/usr/bin/env bash
# No pointers to a design record this repository does not carry.
#
# The reasoning behind the decisions here is written up in a private repository, and that
# is deliberate. What is not deliberate is naming that write-up by its identifier from a
# tracked file: the identifier resolves to nothing for anyone reading this repository,
# while still saying where to go looking. A comment's job is to state its constraint --
# what holds, and what must be true. A pointer to a record the reader cannot open replaces
# the constraint with an errand.
#
# So the rule is about the identifier, not about the reasoning. Keep the reasoning, restate
# it self-containedly, drop the pointer. Same footing as the sibling zero-footprint gate:
# both refuse a trace of the private repository's structure, not the structure itself.
#
# What is deliberately NOT refused: the bare section-sign convention where it appears with
# no identifier next to it. It names no document and resolves to nothing either way, so it
# is noise rather than a pointer, and refusing it would be a prose preference wearing a
# gate's clothes. The control below holds the scan to both halves of that.
set -euo pipefail

# The pointer: a record identifier, case-insensitively. At the top because the liveness
# control has to run this exact string -- a control with its own copy proves the copy works
# and says nothing about the one that decides.
POINTER='ADR-[0-9]'

# This file and nothing else. The gate cannot state what it refuses without spelling it,
# so it would otherwise be its own first offence. Named as a path rather than smuggled
# past the pattern, because an exemption that is visible can be argued with.
SELF='.github/scripts/no-private-record-pointers.sh'

die_detector() {
  echo "::error::$1"
  echo "Nothing this gate says about the repository can be trusted, so it is a failure rather than a pass."
  exit 1
}

# Tracked files, not the working tree: what this gate is about is what the repository
# carries, and an ignored file in somebody's checkout reaches no push and no reader.
# Binary files are skipped (-I) -- a byte sequence that happens to spell the pattern is
# not a pointer anyone reads.
#
# Two exit statuses have to be told apart, and `set -euo pipefail` hides both by killing
# the script with no output at all. 1 means "no match", which is the good news; anything
# above it means the scan could not look, and a scan that could not look is not a scan
# that found nothing.
scan() { # <root>
  local out status
  out="$(git -C "$1" grep -nIE -i "$POINTER" -- .)" && status=0 || status=$?
  case "$status" in
    0 | 1) ;;
    *) echo "::error::git grep failed under $1 (status $status), so nothing was scanned." >&2; exit 2 ;;
  esac
  [ -n "$out" ] && printf '%s\n' "$out"
  return 0
}

# A clean repository reports nothing and exits 0 -- which is also exactly what this does
# when the scan itself has stopped working. So before it decides anything about this
# repository it decides something about a case whose answer is already known: one file
# that MUST be caught and one that MUST NOT. Both halves are needed; a detector that
# matches everything passes the first.
#
# The number in the control is a shape, not a reference: no record carries it, and the
# control exists to be matched rather than to be looked up.
detector_alive() {
  local d hit; d="$(mktemp -d)"
  mkdir -p "$d/src" "$d/corpus"
  printf '%s\n' '/** Embed mode of a nest child (ADR-0000 §5.1): 1:N array vs 1:1 object. */' > "$d/src/Pointer.java"
  printf '%s\n' 'adr: "ADR-0000 §2"' > "$d/corpus/expected.yml"
  printf '%s\n' '/** Embed mode of a nest child (§5.1): 1:N array vs 1:1 object. */' > "$d/src/BareSection.java"
  { git -C "$d" init -q -b main . \
    && git -C "$d" config user.email control@example.invalid \
    && git -C "$d" config user.name control \
    && git -C "$d" add -A; } >/dev/null 2>&1 \
    || die_detector "could not build the control repository, so the scan was never checked (is git working?)."
  hit="$(scan "$d")"
  rm -rf "$d"
  case "$hit" in *Pointer.java*) : ;; *) die_detector "the scan did not match a control comment carrying a record identifier." ;; esac
  case "$hit" in *expected.yml*) : ;; *) die_detector "the scan did not match a control fixture sidecar carrying a record identifier." ;; esac
  case "$hit" in *BareSection.java*) die_detector "the scan matched a control carrying only a bare section sign, which names no document and is not a pointer." ;; esac
}

detector_alive

hits="$(scan . | grep -v "^${SELF}:" || true)"
if [ -n "$hits" ]; then
  files="$(printf '%s\n' "$hits" | cut -d: -f1 | sort -u | wc -l | tr -d ' ')"
  echo "::error::pointer(s) to a design record this repository does not carry, in ${files} file(s):"
  printf '%s\n' "$hits"
  echo
  echo "State the constraint self-containedly instead: keep the reasoning, drop the identifier."
  exit 1
fi
echo "clean: no pointers to a design record this repository does not carry."
