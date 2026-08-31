#!/usr/bin/env bash
#
# Cases for site-from-tag.sh.
#
# The one that matters builds the situation the release pipeline actually produces: a tag whose tree
# still carries the previous release's pins, because the version was written inside the runner and
# never committed. Reading that tree straight out is what shipped the wrong installer, so the case
# seeds exactly that and asks what reaches the site.
#
# Builds its own repository. Every git command below is scoped to it with -C, and the inherited git
# environment is cleared first: a script that makes its own repository and is ever started from a
# hook takes refs and remotes from the repository being pushed while its working tree is the
# sandbox, and reports success the whole way.

unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PREFIX GIT_QUARANTINE_PATH
set -eu

here="$(cd "$(dirname "$0")" && pwd)"
script="$here/site-from-tag.sh"
repo="$(cd "$here/../.." && pwd)"
failures=0

check() {   # name, expected, actual
    if [ "$2" = "$3" ]; then
        echo "ok   - $1"
    else
        echo "FAIL - $1: expected '$2', got '$3'"
        failures=$((failures + 1))
    fi
}

refuses() {   # name, needle, repository to run in, tag to ask for
    local name="$1" needle="$2" where="$3" tag="$4"
    local out
    if out="$( cd "$where" && "$script" "$tag" "$(mktemp -d)" 2>&1 )"; then
        echo "FAIL - $name: it did not refuse"
        failures=$((failures + 1))
        return
    fi
    if printf '%s' "$out" | grep -q "$needle"; then
        echo "ok   - $name"
    else
        echo "FAIL - $name: refused without naming '$needle': $(printf '%s' "$out" | tail -1)"
        failures=$((failures + 1))
    fi
}

# A repository carrying all six pins, every one of them at $1.
make_repo() {   # version the committed tree pins
    local dir; dir="$(mktemp -d)/repo"; mkdir -p "$dir"
    mkdir -p "$dir/cli/src/main/java/io/tapstate/cli" \
             "$dir/control/mcp-server/src/main/resources" \
             "$dir/deploy/quickstart" "$dir/install"
    printf '<project><revision>%s</revision></project>\n' "$1" > "$dir/pom.xml"
    printf 'class Cli { String VERSION = "tapstate %s"; }\n' "$1" > "$dir/cli/src/main/java/io/tapstate/cli/Cli.java"
    printf 'spring.ai.mcp.server.version=%s\n' "$1" > "$dir/control/mcp-server/src/main/resources/application.properties"
    printf 'CLI_VERSION="%s"\n' "$1" > "$dir/deploy/quickstart/quickstart.sh"
    printf 'PINNED_VERSION="%s"\n' "$1" > "$dir/install/install.sh"
    printf 'services:\n  tapstate:\n    image: ghcr.io/tapstate/tapstate:%s\n' "$1" > "$dir/deploy/quickstart/docker-compose.yml"
    git -C "$dir" init --quiet
    git -C "$dir" -c user.email=t@t -c user.name=t add -A
    git -C "$dir" -c user.email=t@t -c user.name=t commit --quiet -m "pins at $1"
    echo "$dir"
}

# --- the case this script exists for -------------------------------------------------------------
# The tree says 0.3.0 and the tag says v0.4.0, which is every release the current pipeline cuts: the
# version is written in the runner and never committed, so the tag lands on the pre-bump tree.
work="$(make_repo 0.3.0)"
git -C "$work" tag v0.4.0
git -C "$work" tag v0.3.0   # the same commit under its own name, for the control group below
out="$(mktemp -d)"
( cd "$work" && "$script" v0.4.0 "$out" >/dev/null )
check "a tag whose tree still pins the previous release publishes the released version" \
      'PINNED_VERSION="0.4.0"' "$(cat "$out/install.sh")"
check "and the quickstart it publishes pins it too" \
      'CLI_VERSION="0.4.0"' "$(cat "$out/quickstart.sh")"

# The control group. Without it the case above is satisfied by a script that writes 0.4.0 no matter
# what it was handed, which would be just as wrong in the other direction.
out2="$(mktemp -d)"
( cd "$work" && "$script" v0.3.0 "$out2" >/dev/null )
check "an older tag publishes its own version, not the newest one" \
      'PINNED_VERSION="0.3.0"' "$(cat "$out2/install.sh" 2>/dev/null || echo missing)"

# A tag cut the old way, with the bump committed before tagging. Writing the version in is a no-op,
# and has to stay one -- both eras of tag are published by this one path.
old="$(make_repo 0.2.1)"
git -C "$old" tag v0.2.1
out3="$(mktemp -d)"
( cd "$old" && "$script" v0.2.1 "$out3" >/dev/null )
check "a tag that already pins its own version is unchanged" \
      'PINNED_VERSION="0.2.1"' "$(cat "$out3/install.sh")"

# --- refusals ------------------------------------------------------------------------------------
refuses "a tag that is not a version is refused" "does not name a release version" \
        "$work" connectors-preview

refuses "a two-part version is refused" "does not name a release version" \
        "$work" v0.4

refuses "a tag that does not exist is refused" "" \
        "$work" v9.9.9

# A tree too old to carry every pin. The first release predates one of the six files, and an
# installer whose pinned version could not be written is not published quietly.
young="$(make_repo 0.1.0)"
rm -f "$young/control/mcp-server/src/main/resources/application.properties"
git -C "$young" -c user.email=t@t -c user.name=t commit --quiet -am "before the mcp server existed"
git -C "$young" tag v0.1.0
refuses "a tag too old to carry every pin is refused, not published as-is" "too old to publish from" \
        "$young" v0.1.0

# --- the workflow actually calls it --------------------------------------------------------------
# A script nothing invokes is a script that cannot fail. This is the same registration check the
# other release scripts carry, and the reason all three latest-release lanes stopped keeping copies.
wf="$repo/.github/workflows/publish-install-site.yml"
if grep -q 'site-from-tag.sh' "$wf"; then
    echo "ok   - publish-install-site.yml calls site-from-tag.sh"
else
    echo "FAIL - publish-install-site.yml does not call site-from-tag.sh"
    failures=$((failures + 1))
fi
# shellcheck disable=SC2016  # the literal text being searched for contains $tag
if grep -qE '^\s*git show "\$tag:(install/install.sh|deploy/quickstart/quickstart.sh)"' "$wf"; then
    echo "FAIL - publish-install-site.yml still reads a published script straight from the tag"
    failures=$((failures + 1))
else
    echo "ok   - publish-install-site.yml keeps no copy of the old straight-from-the-tag read"
fi

rm -rf "$(dirname "$work")" "$(dirname "$old")" "$(dirname "$young")" "$out" "$out2" "$out3"

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed" >&2
    exit 1
fi
echo "all cases passed"
