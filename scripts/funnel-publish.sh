#!/bin/sh
#
# Commit the captured series and push it.
#
# The series is small -- a year of daily counters is about 33 KB -- so a file in a private git
# repository is a better store than any database: versioned, diffable, backed up with the repository,
# no new vendor and no new credential, and the commit history is itself the audit trail of what was
# observed when.
#
# Commits only when something actually changed. The job runs weekly forever, and an unconditional
# commit would add one empty commit per week, burying the real ones; `git commit` with nothing staged
# also exits non-zero, which would turn every quiet week into a failed run.
#
# Usage: funnel-publish.sh --repo-dir <checkout> [--message <text>] [--branch <name>]
set -eu

repo_dir=""
message=""
branch="main"
die() { printf 'funnel-publish: %s\n' "$1" >&2; exit 1; }

while [ $# -gt 0 ]; do
    case "$1" in
        --repo-dir) repo_dir="${2:?--repo-dir needs a path}"; shift 2 ;;
        --message)  message="${2:?--message needs text}"; shift 2 ;;
        --branch)   branch="${2:?--branch needs a name}"; shift 2 ;;
        *) die "unknown argument: $1" ;;
    esac
done
[ -n "$repo_dir" ] || die "--repo-dir is required"
[ -d "$repo_dir/.git" ] || die "$repo_dir is not a git checkout"

: "${message:=funnel: capture $(date -u +%Y-%m-%d)}"

git -C "$repo_dir" add -A

# --quiet --exit-code: 0 when the index matches HEAD, 1 when it does not. Nothing staged means the
# capture observed nothing new, which is a normal week, not a failure.
if git -C "$repo_dir" diff --cached --quiet --exit-code; then
    printf 'funnel-publish: nothing changed; no commit\n'
    exit 0
fi

# Identity comes from the environment when the caller set one (CI does); otherwise a local fallback
# keeps this runnable on a workstation without touching the user's global git config.
if [ -z "$(git -C "$repo_dir" config user.email || true)" ]; then
    git -C "$repo_dir" config user.email "funnel-capture@tapstate.local"
    git -C "$repo_dir" config user.name "funnel capture"
fi

git -C "$repo_dir" commit -q -m "$message"
git -C "$repo_dir" push -q origin "HEAD:$branch"
printf 'funnel-publish: committed and pushed to %s\n' "$branch"
