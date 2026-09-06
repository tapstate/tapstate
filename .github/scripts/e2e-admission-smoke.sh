#!/usr/bin/env bash
# Cases for the admission gate's own decision, driven against a scratch repository shaped like each
# one. A gate that has only been configured is not a gate that has been seen to work, and the shapes
# that matter most are the ones nobody would notice going wrong: a deleted specification counting as
# a case, a label that merely contains the waiver word, a case that has sat on the base branch all
# along. Each of those turns the gate green while it verifies nothing.
#
# Run it from anywhere. Exits 0 if every case holds.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/e2e-admission.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
passed=0
failed=0

# A repository whose base branch already carries product source, a specification and a document, so
# every case differs from it by exactly the change it is about.
fresh_repo() {
  rm -rf "${scratch:?}/repo"
  mkdir -p "$scratch/repo"
  cd "$scratch/repo" || exit 1
  git init -q -b main .
  git config user.email cases@example.invalid
  git config user.name cases
  mkdir -p app/src/main/java e2e/examples/an-existing-case e2e/src/test/java/io/tapstate/e2e docs
  echo base > app/src/main/java/App.java
  echo base > e2e/examples/an-existing-case/spec.e2e.yml
  echo base > docs/readme.md
  git add -A && git commit -qm base
  git update-ref refs/remotes/origin/main main
  git checkout -q -b feature
}

# Runs the gate over whatever the branch now holds, and checks both halves of its answer: the exit
# code, and the reason it gave. A gate that refuses for the wrong reason is not refusing.
expect() {
  local name="$1" want_code="$2" want_text="$3" labels="${4:-}"
  local out code
  out="$(BASE_REF=main PR_LABELS="$labels" bash "$gate" 2>&1)"
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
echo more >> docs/readme.md
git add -A && git commit -qm docs
expect "a documentation change is out of scope" 0 "not in scope"

fresh_repo
echo x > e2e/src/test/java/io/tapstate/e2e/SomethingIT.java
git add -A && git commit -qm test
expect "a test-only change is out of scope" 0 "not in scope"

fresh_repo
echo changed >> app/src/main/java/App.java
git add -A && git commit -qm product
expect "product source with no case is refused" 1 "brings no end-to-end case"

fresh_repo
echo changed >> app/src/main/java/App.java
mkdir -p e2e/examples/a-new-case
echo spec > e2e/examples/a-new-case/spec.e2e.yml
git add -A && git commit -qm both
expect "a new specification admits it" 0 "e2e/examples/a-new-case/spec.e2e.yml"

fresh_repo
echo changed >> app/src/main/java/App.java
echo more >> e2e/examples/an-existing-case/spec.e2e.yml
git add -A && git commit -qm both
expect "extending an existing specification admits it" 0 "e2e/examples/an-existing-case/spec.e2e.yml"

fresh_repo
echo changed >> app/src/main/java/App.java
echo x > e2e/src/test/java/io/tapstate/e2e/NewThingIT.java
git add -A && git commit -qm both
expect "a bespoke end-to-end test admits it" 0 "e2e/src/test/java/io/tapstate/e2e/NewThingIT.java"

fresh_repo
echo changed >> app/src/main/java/App.java
git rm -q e2e/examples/an-existing-case/spec.e2e.yml
git add -A && git commit -qm delete
expect "deleting a specification does not admit it" 1 "brings no end-to-end case"

fresh_repo
echo changed >> app/src/main/java/App.java
mkdir -p app/src/test/java
echo x > app/src/test/java/StoreIT.java
git add -A && git commit -qm both
expect "an integration test outside the e2e module does not admit it" 1 "brings no end-to-end case"

fresh_repo
echo changed >> app/src/main/java/App.java
git add -A && git commit -qm product
expect "the waiver label admits it" 0 "waived by the no-e2e label" "no-e2e"

fresh_repo
echo changed >> app/src/main/java/App.java
git add -A && git commit -qm product
expect "an unrelated label does not waive" 1 "brings no end-to-end case" "bug,enhancement"

fresh_repo
echo changed >> app/src/main/java/App.java
git add -A && git commit -qm product
expect "a label merely containing the waiver word does not waive" 1 "brings no end-to-end case" \
  "needs-no-e2e-someday"

fresh_repo
echo changed >> app/src/main/java/App.java
git add -A && git commit -qm product
expect "a case already on the base branch does not admit it" 1 "brings no end-to-end case"

# The release's own write-back changes a version pin that lives under src/main, so the gate sees
# product source and demands a case the change cannot carry: a version constant has no behaviour to
# witness. Left alone, every release opens a pull request that is red on arrival and can only be
# merged by waiving a gate -- which is a maintainer's act, performed on a bot's mechanical edit.
# Scope is decided by what changed, not by who or which branch: a pin list is not spoofable.
fresh_repo
mkdir -p cli/src/main/java/io/tapstate/cli
echo 'VERSION = "tapstate 0.0.0"' > cli/src/main/java/io/tapstate/cli/Cli.java
git add -A && git commit -qm "pin exists"
git update-ref refs/remotes/origin/main HEAD
echo 'VERSION = "tapstate 0.4.1"' > cli/src/main/java/io/tapstate/cli/Cli.java
git add -A && git commit -qm "write the version in"
expect "a pull request that only rewrites version pins is out of scope" 0 "only version pins"

# The control, and the reason the exemption is by path set rather than by branch: the same pull
# request carrying one more product file is in scope again. Without this, "it touched a pin" would
# be a way to smuggle any change past the gate.
fresh_repo
mkdir -p cli/src/main/java/io/tapstate/cli
echo 'VERSION = "tapstate 0.0.0"' > cli/src/main/java/io/tapstate/cli/Cli.java
git add -A && git commit -qm "pin exists"
git update-ref refs/remotes/origin/main HEAD
echo 'VERSION = "tapstate 0.4.1"' > cli/src/main/java/io/tapstate/cli/Cli.java
echo changed >> app/src/main/java/App.java
git add -A && git commit -qm "a version pin and something else"
expect "a version pin next to any other product change is still in scope" 1 "brings no end-to-end case"

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
