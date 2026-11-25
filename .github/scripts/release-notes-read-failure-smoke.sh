#!/usr/bin/env bash
# A pull request that cannot be read must not silently disappear from a release body.
set -euo pipefail
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PREFIX GIT_QUARANTINE_PATH

here="$(cd "$(dirname "$0")" && pwd)"
script="$here/release-notes.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
export RELEASE_NOTES_READ_FAILURE_SCRATCH="$scratch"
export GITHUB_REPOSITORY=tapstate/tapstate
mkdir -p "$scratch/bin" "$scratch/bodies" "$scratch/repo"

cat > "$scratch/bin/gh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
[ "${1:-}" = pr ] && [ "${2:-}" = view ] || exit 2
n="${3:?pull request number is required}"
if [ "$n" = 485 ] && [ -f "$RELEASE_NOTES_READ_FAILURE_SCRATCH/fail-read" ]; then
  printf '%s\n' "$n" >> "$RELEASE_NOTES_READ_FAILURE_SCRATCH/failed-reads"
  echo 'HTTP 502: Bad Gateway (https://api.github.com/graphql)' >&2
  exit 1
fi
cat "$RELEASE_NOTES_READ_FAILURE_SCRATCH/bodies/$n"
STUB
chmod +x "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH"

kept_note='You can read a task write-back position from the CLI.'
missing_note='As an operator, I can run the Web application from the Tapstate server image and reload a bookmarked client route, so the UI stays version-aligned with the server and its source revisions can be traced.'
printf '### Release note\n\n**Kind:** new\n\n%s\n\n## Checks\n' "$kept_note" > "$scratch/bodies/484"
printf '### Release note\n\n**Kind:** new\n\n%s\n\n## Checks\n' "$missing_note" > "$scratch/bodies/485"

repo="$scratch/repo"
git -C "$repo" init -q -b main
git -C "$repo" config user.email t@example.com
git -C "$repo" config user.name t
git -C "$repo" config commit.gpgSign false
git -C "$repo" config core.hooksPath /dev/null
git -C "$repo" commit -q --allow-empty -m base
git -C "$repo" tag v0.5.0
git -C "$repo" commit -q --allow-empty -m 'Read a task write-back position (#484)'
git -C "$repo" commit -q --allow-empty -m 'Serve the Web application from the server image (#485)'

assemble() {
  (cd "$repo" && bash "$script" --version 0.6.0 --base v0.5.0 --sha HEAD \
    --macos-req 'Recommended macOS: 15.0 or newer.' \
    --glibc-req 'Recommended glibc: 2.34 or newer.')
}

# Validate the range and extraction before changing only the outcome of the read.
assemble > "$scratch/complete.md" 2> "$scratch/complete.stderr"
if ! grep -qFx -- "* $kept_note" "$scratch/complete.md" || \
   ! grep -qFx -- "* $missing_note" "$scratch/complete.md"; then
  echo 'SETUP FAIL: healthy reads did not assemble both fixture notes' >&2
  cat "$scratch/complete.stderr" >&2
  exit 2
fi

touch "$scratch/fail-read"
status=0
assemble > "$scratch/release-notes.md" 2> "$scratch/stderr" || status=$?
if [ ! -s "$scratch/failed-reads" ]; then
  echo 'SETUP FAIL: the collector did not encounter the injected PR #485 read failure' >&2
  cat "$scratch/stderr" >&2
  exit 2
fi

if [ "$status" -ne 0 ] && [ ! -s "$scratch/release-notes.md" ] && \
   grep -qF -- '485' "$scratch/stderr"; then
  echo 'ok - an unreadable pull request blocks release-note assembly and is named'
  exit 0
fi

echo 'FAIL - an unreadable pull request must block release-note assembly and be named' >&2
printf '  collector exit status: %s (expected non-zero)\n' "$status" >&2
if [ -s "$scratch/release-notes.md" ]; then
  echo '  Gate 5 test -s release-notes.md accepts the incomplete body' >&2
fi
if grep -qFx -- "* $kept_note" "$scratch/release-notes.md"; then
  echo '  PR #484 note is present' >&2
fi
if ! grep -qF -- "$missing_note" "$scratch/release-notes.md"; then
  echo '  PR #485 note is missing despite an unchanged body and a valid release-note section' >&2
fi
if [ ! -s "$scratch/stderr" ]; then
  echo '  collector stderr is empty despite HTTP 502 from gh pr view 485' >&2
else
  cat "$scratch/stderr" >&2
fi
exit 1
