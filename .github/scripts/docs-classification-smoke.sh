#!/usr/bin/env bash
# Cases for the documentation classification gate's own decision, driven against a scratch repository
# shaped like each one. The shapes that matter are the ones nobody would notice going wrong: a page
# whose classification words appear in its prose rather than in its header, a half-filled header that
# names a status but no destination, and a page that claims both destinations at once. Each of those
# turns the gate green over a page that says nothing reliable about where it is going.
#
# Run it from anywhere. Exits 0 if every case holds.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/docs-classification.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
passed=0
failed=0

# A repository whose base branch already carries an unclassified page and an executable asset, so a
# case about touching what was already there differs from the base by exactly that.
fresh_repo() {
  rm -rf "${scratch:?}/repo"
  mkdir -p "$scratch/repo"
  cd "$scratch/repo" || exit 1
  git init -q -b main .
  git config user.email cases@example.invalid
  git config user.name cases
  mkdir -p docs/tutorials/an-existing-one src/main/java
  printf '# An existing page\n\nWritten before the rule.\n' > docs/tutorials/an-existing-one/README.md
  printf 'select 1;\n' > docs/tutorials/an-existing-one/data.sql
  echo base > src/main/java/App.java
  git add -A && git commit -qm base
  git update-ref refs/remotes/origin/main main
  git checkout -q -b feature
}

# Writes a page with the header a draft is supposed to carry.
draft_page() {
  mkdir -p "$(dirname "$1")"
  cat > "$1" <<'EOF'
---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/nest/assembling-documents
---

# A page

Body.
EOF
}

# Runs the gate over whatever the branch now holds, and checks both halves of its answer: the exit
# code, and the reason it gave. A gate that refuses for the wrong reason is not refusing.
expect() {
  local name="$1" want_code="$2" want_text="$3"
  local out code
  out="$(BASE_REF=main bash "$gate" 2>&1)"
  code=$?
  if [ "$code" = "$want_code" ] && grep -qF "$want_text" <<<"$out"; then
    printf '  ok    %s\n' "$name"
    passed=$((passed + 1))
  else
    printf '  FAIL  %s: wanted exit %s containing "%s", got exit %s:\n' \
      "$name" "$want_code" "$want_text" "$code"
    printf '%s\n' "$out" | sed 's/^/        /'
    failed=$((failed + 1))
  fi
}

fresh_repo
echo changed >> src/main/java/App.java
git add -A && git commit -qm product
expect "a change touching no page is out of scope" 0 "not in scope"

fresh_repo
draft_page docs/nest/a-new-page.md
git add -A && git commit -qm draft
expect "a new page classified as a draft passes" 0 "classified"

fresh_repo
mkdir -p docs/nest
cat > docs/nest/a-new-page.md <<'EOF'
---
status: canonical-pointer
canonical_url: https://tapstate.dev/docs/nest/assembling-documents
---

# A page

The published version lives at the URL above.
EOF
git add -A && git commit -qm pointer
expect "a new page classified as a pointer passes" 0 "classified"

fresh_repo
mkdir -p docs/nest
printf '# A page\n\nBody.\n' > docs/nest/a-new-page.md
git add -A && git commit -qm unclassified
expect "a new page with no header is refused" 1 "carries no classification"

fresh_repo
mkdir -p docs/nest
cat > docs/nest/a-new-page.md <<'EOF'
---
status: engineering-draft
publication: handoff
---

# A page
EOF
git add -A && git commit -qm targetless
expect "a draft that names no target is refused" 1 "names no target"

fresh_repo
mkdir -p docs/nest
cat > docs/nest/a-new-page.md <<'EOF'
---
status: canonical-pointer
---

# A page
EOF
git add -A && git commit -qm urlless
expect "a pointer that names no canonical url is refused" 1 "names no canonical_url"

fresh_repo
mkdir -p docs/nest
cat > docs/nest/a-new-page.md <<'EOF'
---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/nest/assembling-documents
canonical_url: https://tapstate.dev/docs/nest/assembling-documents
---

# A page
EOF
git add -A && git commit -qm mixed
expect "a page claiming both destinations is refused" 1 "mixes"

fresh_repo
mkdir -p docs/nest
cat > docs/nest/a-new-page.md <<'EOF'
---
status: published
---

# A page
EOF
git add -A && git commit -qm unknown
expect "a status outside the two words is refused" 1 "not a status this repository knows"

# The shape a reader of the gate would never think of: the words are present, but as prose. A gate
# that greps the whole file admits a page that has classified nothing.
fresh_repo
mkdir -p docs/nest
cat > docs/nest/a-new-page.md <<'EOF'
# A page

A page that explains the rule quotes the header in full, which is the whole point of this
case: the same lines, at column one, and none of them classifying this page.

```
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/somewhere
```
EOF
git add -A && git commit -qm prose
expect "the words in prose do not classify a page" 1 "carries no classification"

fresh_repo
echo changed >> src/main/java/App.java
git add -A && git commit -qm untouched
expect "an existing page nobody touched stays grandfathered" 0 "not in scope"

fresh_repo
echo 'One more line.' >> docs/tutorials/an-existing-one/README.md
git add -A && git commit -qm touched
expect "an existing page that is touched loses its grandfathering" 1 "carries no classification"

fresh_repo
git rm -q docs/tutorials/an-existing-one/README.md
git commit -qm deleted
expect "deleting a page is not authoring one" 0 "not in scope"

fresh_repo
echo 'select 2;' >> docs/tutorials/an-existing-one/data.sql
git add -A && git commit -qm asset
expect "an executable asset is out of scope" 0 "not in scope"

fresh_repo
draft_page docs/nest/a-new-page.md
printf '# A page\n' > docs/nest/another-new-page.md
git add -A && git commit -qm two
expect "one classified page does not cover an unclassified one" 1 "another-new-page.md"

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
