#!/usr/bin/env bash
# Cases for the install-telemetry fence gate, driven against a scratch repository shaped like each one.
#
# The gate's own liveness control decides its controls one function at a time. These run the whole
# gate the way a pull request does: the workflows git tracks, read as YAML, and what it prints and the
# exit status a merge depends on. What reaches the installer is decided by scope. An `env:` block on a
# job reaches that job's steps and no other job's, so a workflow whose only fence sits on a job that
# installs nothing still installs unfenced from the job that does: the send goes to the production
# endpoint under the community default, and lands in the figure the funnel divides by. A scan of the
# whole file finds both names in that workflow and calls it clean.
#
# Run it from anywhere. Exits 0 if every case holds.
#
# Builds its own repository. The inherited git environment is cleared first: a script that makes its
# own repository and is ever started from a hook takes refs and remotes from the repository being
# pushed while its working tree is the sandbox.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PREFIX GIT_QUARANTINE_PATH
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
gate="$here/no-unfenced-install-telemetry.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
passed=0
failed=0

# The gate reads the workflows git tracks, so staging is enough. Nothing here commits, and so nothing
# here depends on an identity or a signing setup.
fresh_repo() {
  rm -rf "${scratch:?}/repo"
  mkdir -p "$scratch/repo/.github/workflows"
  cd "$scratch/repo" || exit 1
  git init -q -b main .
}

workflow() { # workflow <path>; the body on stdin
  cat > "$1"
  git add -- "$1"
}

expect() { # expect <name> <want code> <want text>
  local name="$1" want_code="$2" want_text="$3" out code
  bash "$gate" > "$scratch/out" 2>&1; code=$?
  out="$(cat "$scratch/out")"
  if [ "$code" = "$want_code" ] && grep -qF "$want_text" <<<"$out"; then
    printf '  ok    %s\n' "$name"; passed=$((passed + 1))
  else
    printf '  FAIL  %s\n        wanted exit %s containing %s\n        got exit %s: %s\n' \
      "$name" "$want_code" "$want_text" "$code" "$out"
    failed=$((failed + 1))
  fi
}

# --- two jobs, and the fence on the one that installs nothing. Both names are present, spelled the
# --- way the gate spells them and outside any comment; the install step runs in `install`, which
# --- inherits neither of them.
fresh_repo
workflow .github/workflows/two-jobs.yml <<'EOF'
name: two-jobs
on: workflow_dispatch
jobs:
  build:
    runs-on: ubuntu-latest
    env:
      TAPSTATE_TELEMETRY_URL: http://127.0.0.1:1/e
      TAPSTATE_TELEMETRY_CHANNEL: internal
    steps:
      - run: echo "nothing is installed in this job"
  install:
    runs-on: ubuntu-latest
    steps:
      - run: curl -sSL https://install.tapstate.dev/cli | sh
EOF
expect "a fence on a job that installs nothing does not cover the job that does" 1 ".github/workflows/two-jobs.yml"

# --- the same workflow with the fence moved onto the job that installs: clean, and exit 0. Without
# --- this, a gate that refused every repository would pass the case above.
fresh_repo
workflow .github/workflows/two-jobs.yml <<'EOF'
name: two-jobs
on: workflow_dispatch
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo "nothing is installed in this job"
  install:
    runs-on: ubuntu-latest
    env:
      TAPSTATE_TELEMETRY_URL: http://127.0.0.1:1/e
      TAPSTATE_TELEMETRY_CHANNEL: internal
    steps:
      - run: curl -sSL https://install.tapstate.dev/cli | sh
EOF
expect "a fence on the job that installs is accepted" 0 "clean:"

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ]
