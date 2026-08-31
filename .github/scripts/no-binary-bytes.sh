#!/usr/bin/env bash
# Zero-binary-bytes gate: no tracked file may contain a NUL byte.
#
# Why a NUL specifically, rather than "binary content" generally: a single NUL makes
# grep treat the whole file as binary, so `grep -I` (the default in several tools here)
# skips it silently and exits 0. One accidental raw 0x00 in a source file therefore
# turns other scanners off for that file without anything reporting it.
#
# Paths are read from BINARY_ALLOWLIST so a copy of this script can run against a
# different repository layout without being edited.
#
# Extracted verbatim from .github/workflows/no-binary-bytes.yml. The decision is
# unchanged; what changed is that it now lives somewhere a test can call it.
set -euo pipefail

# Byte semantics, not text semantics: this check is about one specific byte, so the
# pattern must not be reinterpreted as code points in some locale. The workflow sets
# this at job level too. The script sets it as well, because a unit that only behaves
# correctly when its caller remembers to set a variable is not the unit under test.
export LC_ALL=C

BINARY_ALLOWLIST="${BINARY_ALLOWLIST:-.binary-allowlist}"

# ---- liveness control -------------------------------------------------------------
#
# `git grep` answers "found nothing" and "could not run" with the same exit status, so a
# clean repository and a detector that has stopped working print the same `clean:` line.
# That is not hypothetical here: the pattern needs git's PCRE support, and a git built
# without it fails with a message and a non-zero status that this script would read as
# "no match". Measured while writing this: a pathspec typo produced `fatal: ... outside
# the directory tree`, and read exactly like a clean answer.
#
# So before deciding anything about this repository, decide a case whose answer is known:
# one file that MUST match and one that MUST NOT. Both halves, because a detector that
# matches everything passes the first on its own. --no-index runs the same git matcher
# against files that are not in any repository.
# Written with `if !` rather than `cmd; rc=$?`: under `set -e` a bare subshell that exits
# non-zero kills the script before the next line runs, and the negative control's whole job
# is to exit non-zero. The first draft of this function did exactly that and the script died
# with no output at all -- a liveness check that fails silently is worse than none.
detector_alive() {
  local d; d="$(mktemp -d)"
  printf 'before\000after\n' > "$d/has-nul"
  printf 'beforeafter\n'      > "$d/no-nul"
  if ! ( cd "$d" && git grep --no-index -aqP '\x00' -- has-nul ); then
    rm -rf "$d"
    echo "::error::the NUL detector did not match a control file that contains one."
    echo "Nothing this step says about the repository can be trusted, so it is a failure rather than a pass."
    exit 1
  fi
  if ( cd "$d" && git grep --no-index -aqP '\x00' -- no-nul ); then
    rm -rf "$d"
    echo "::error::the NUL detector matched a control file that contains none, so it matches more than it should."
    echo "Nothing this step says about the repository can be trusted, so it is a failure rather than a pass."
    exit 1
  fi
  rm -rf "$d"
}
detector_alive

# Build pathspec exclusions from the allowlist; ignore comments / blanks.
pathspec=(':(top)')
if [ -f "$BINARY_ALLOWLIST" ]; then
  while IFS= read -r line; do
    line="${line%%#*}"
    line="$(printf '%s' "$line" | tr -d '[:space:]')"
    [ -z "$line" ] && continue
    pathspec+=(":(top,exclude)$line")
  done < "$BINARY_ALLOWLIST"
fi

# -a forces every file to be read as text; see the note at the top.
if git grep -anP '\x00' -- "${pathspec[@]}"; then
  echo "::error::NUL byte(s) found in a text-only repo (matches above; the NUL itself prints as a blank)."
  echo "A NUL byte makes the whole file read as binary, so grep -I skips it silently and exits 0."
  # printf, not echo: shellcheck (SC2028) is right that `echo` is not portable for a
  # backslash, and inline in a workflow nothing was there to say so. The bytes printed
  # are unchanged -- the A/B against the inline block asserts that, output included.
  printf '%s\n' "Write the character as an escape instead (for example '\\0' in Java, rather than a raw 0x00)."
  echo "If a file is legitimately binary, add its path to ${BINARY_ALLOWLIST}."
  exit 1
fi
echo "clean: no NUL bytes in tracked files."
