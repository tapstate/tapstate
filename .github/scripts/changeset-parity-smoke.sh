#!/usr/bin/env bash
#
# Cases for changeset-parity.sh.
#
# The check is vacuous against the real repository -- no changeset exists yet -- so a case that ran it
# there would pass without exercising a single comparison. Every case below seeds changesets that do
# not exist anywhere yet, which is the only way to know the check will work on the day they do.
#
# Builds its own repository. Every git command is scoped to it, and the inherited git environment is
# cleared first: a script that makes its own repository and is ever started from a hook takes refs and
# remotes from the repository being pushed while its working tree is the sandbox.

unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PREFIX GIT_QUARANTINE_PATH
set -eu

here="$(cd "$(dirname "$0")" && pwd)"
script="$here/changeset-parity.sh"
repo="$(cd "$here/../.." && pwd)"
pkg=adapters/adapter-mongo-store/src/main/java/io/tapstate/adapters/mongostore/migration
failures=0

passes() {   # name, repo, base, release, text the output must contain
    local name="$1" where="$2" out
    if ! out="$( cd "$where" && "$script" "$3" "$4" 2>&1 )"; then
        echo "FAIL - $name: it refused: $(printf '%s' "$out" | head -1)"
        failures=$((failures + 1))
        return
    fi
    if printf '%s' "$out" | grep -q "$5"; then
        echo "ok   - $name"
    else
        echo "FAIL - $name: passed, but without saying '$5': $out"
        failures=$((failures + 1))
    fi
}

refuses() {   # name, repo, base, release, text the refusal must name
    local name="$1" where="$2" out
    if out="$( cd "$where" && "$script" "$3" "$4" 2>&1 )"; then
        echo "FAIL - $name: it did not refuse"
        failures=$((failures + 1))
        return
    fi
    if printf '%s' "$out" | grep -q "$5"; then
        echo "ok   - $name"
    else
        echo "FAIL - $name: refused without naming '$5': $(printf '%s' "$out" | tail -1)"
        failures=$((failures + 1))
    fi
}

git_c() { git -C "$1" -c user.email=t@t -c user.name=t "${@:2}"; }

seed() {   # repo dir, branch, changeset class names...
    local dir="$1" branch="$2"; shift 2
    git_c "$dir" checkout --quiet -B "$branch"
    rm -rf "${dir:?}/$pkg"
    if [ "$#" -gt 0 ]; then
        mkdir -p "$dir/$pkg"
        for c in "$@"; do printf 'class %s {}\n' "$c" > "$dir/$pkg/$c.java"; done
    fi
    git_c "$dir" add -A
    git_c "$dir" commit --quiet --allow-empty -m "$branch: $*"
}

work="$(mktemp -d)/repo"; mkdir -p "$work"
git -C "$work" init --quiet
printf 'x\n' > "$work/README"; git_c "$work" add -A; git_c "$work" commit --quiet -m root

# The state of the world today: no changeset anywhere. It has to pass, and it has to say why it could
# not have done anything else -- a silent pass here is what turns into "we have been checking" later.
seed "$work" empty-base
seed "$work" empty-release
passes "an empty tree on both sides passes, and says the check was vacuous" \
       "$work" empty-base empty-release "vacuous"

# The shape it exists to catch. A patch cherry-picked onto an older line brings a changeset with it,
# so the line it was picked from does not know that number, and every instance the patch upgrades is
# refused by the next MINOR.
seed "$work" line-base V1 V2
seed "$work" line-patch V1 V2 V3
refuses "a patch carrying a changeset the base does not have is caught" \
        "$work" line-base line-patch "only in the release: V3"

# The other direction. Not the failure above, but the base and the release disagreeing at all means
# one of the two trees is not what somebody thinks it is.
refuses "a patch missing a changeset the base has is caught" \
        "$work" line-patch line-base "only in the base:    V3"

passes "identical non-empty lists pass, and say how many" \
       "$work" line-base line-base "2 changeset(s)"

# The guard against staying vacuous forever. If changesets land somewhere other than the package this
# script reads, both lists are empty and every release looks fine -- indistinguishable from the state
# in the first case above, which is why the runner is what tells them apart.
git_c "$work" checkout --quiet -B moved line-base
rm -rf "${work:?}/$pkg"
mkdir -p "$work/adapters/adapter-mongo-store/src/main/java/io/tapstate/adapters/mongostore"
printf 'class MigrationRunner {}\n' > "$work/adapters/adapter-mongo-store/src/main/java/io/tapstate/adapters/mongostore/MigrationRunner.java"
git_c "$work" add -A; git_c "$work" commit --quiet -m "runner, changesets elsewhere"
refuses "a migration runner with no changesets where this looks is caught, not read as agreement" \
        "$work" empty-base moved "moved"

refuses "a ref this repository does not know is refused" \
        "$work" empty-base v9.9.9-nope "not a commit"

# What the counting is actually counting. It is files in a directory, not classes implementing
# anything -- it reads a tree, and a tree has no types in it. So a helper parked next to the runner is
# a changeset as far as this is concerned, the two lines stop agreeing for a reason that has nothing to
# do with a changeset, and a release is refused with a message about one.
seed "$work" helper-base V1
git_c "$work" checkout --quiet -B helper-beside helper-base
printf 'class LockDocument {}\n' > "$work/$pkg/LockDocument.java"
git_c "$work" add -A; git_c "$work" commit --quiet -m "a helper parked with the changesets"
refuses "a helper parked beside the changesets is counted as one" \
        "$work" helper-base helper-beside "only in the release: LockDocument"

# And a subdirectory does not get around it: the listing is recursive, so a helper one package down is
# counted exactly the same. Worth a case of its own because "put it in a subpackage" is the obvious
# thing to reach for and it does not work.
git_c "$work" checkout --quiet -B helper-below helper-base
mkdir -p "$work/$pkg/support"
printf 'class LockDocument {}\n' > "$work/$pkg/support/LockDocument.java"
git_c "$work" add -A; git_c "$work" commit --quiet -m "a helper one package down"
refuses "a helper in a subpackage is counted just the same" \
        "$work" helper-base helper-below "only in the release: LockDocument"

# The standing consequence for this repository: everything in that package is either the runner or a
# changeset. Read from the real tree, because the two cases above show what it costs when it is not,
# and nothing else would notice a helper landing there until a release refused itself.
# Read from the working tree, not from HEAD. A file added but not yet committed is exactly the state
# somebody is in while making this mistake, and a check that reads HEAD passes for them and fails in
# CI -- the one place the answer arrives too late to be useful. Measured: with a helper class sitting
# in the package, the HEAD-reading form of this case reported it clean.
strays="$( find "$repo/$pkg" -name '*.java' -type f 2>/dev/null \
    | sed -n 's|.*/\([A-Za-z0-9_]*\)\.java$|\1|p' \
    | grep -v '^MigrationRunner$' | grep -v '^V[0-9]' || true )"
if [ -z "$strays" ]; then
    echo "ok   - the migration package holds only the runner and the changesets"
else
    echo "FAIL - these are neither the runner nor a changeset, and each is counted as one: $strays"
    failures=$((failures + 1))
fi

# The release workflow actually calls it, and only for a patch. A check nothing invokes cannot fail.
wf="$repo/.github/workflows/release.yml"
if grep -q 'changeset-parity.sh' "$wf"; then
    echo "ok   - release.yml calls changeset-parity.sh"
else
    echo "FAIL - release.yml does not call changeset-parity.sh"
    failures=$((failures + 1))
fi
if grep -B4 'changeset-parity.sh' "$wf" | grep -q "inputs.bump == 'patch'"; then
    echo "ok   - and only when the bump is a patch"
else
    echo "FAIL - release.yml runs changeset-parity.sh without gating it on a patch bump"
    failures=$((failures + 1))
fi

rm -rf "$(dirname "$work")"

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed" >&2
    exit 1
fi
echo "all cases passed"
