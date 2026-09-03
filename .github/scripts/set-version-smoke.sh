#!/usr/bin/env bash
#
# Cases for set-version.sh. Each builds a throwaway tree holding the six files that pin a version, so
# a case can put one of them out of step -- which is the state the real tree is never in for long,
# and the only state worth testing.

set -eu

here="$(cd "$(dirname "$0")" && pwd)"
script="$here/set-version.sh"
repo="$(cd "$here/../.." && pwd)"
failures=0

check() {
    local name="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        echo "ok   - $name"
    else
        echo "FAIL - $name: expected '$expected', got '$actual'"
        failures=$((failures + 1))
    fi
}

# A tree pinned at $1 everywhere, except that $2 (a file path, optional) is left at $3.
tree_at() {
    local version="$1" stale_file="${2:-}" stale_version="${3:-}" dir
    dir="$(mktemp -d)"
    mkdir -p "$dir/cli/src/main/java/io/tapstate/cli" \
             "$dir/control/mcp-server/src/main/resources" \
             "$dir/deploy/quickstart" \
             "$dir/install"
    printf '<project>\n  <properties>\n    <revision>%s</revision>\n  </properties>\n</project>\n' "$version" > "$dir/pom.xml"
    printf 'class Cli {\n    static final String VERSION = "tapstate %s";\n}\n' "$version" > "$dir/cli/src/main/java/io/tapstate/cli/Cli.java"
    printf 'spring.application.name=tapstate-mcp\nspring.ai.mcp.server.version=%s\n' "$version" > "$dir/control/mcp-server/src/main/resources/application.properties"
    printf '#!/bin/sh\nCLI_VERSION="%s"\n' "$version" > "$dir/deploy/quickstart/quickstart.sh"
    printf '#!/bin/sh\nPINNED_VERSION="%s"\n' "$version" > "$dir/install/install.sh"
    printf 'services:\n  tapstate:\n    image: ghcr.io/tapstate/tapstate:%s\n' "$version" > "$dir/deploy/quickstart/docker-compose.yml"
    if [ -n "$stale_file" ]; then
        sed -i.bak "s/$version/$stale_version/" "$dir/$stale_file" && rm -f "$dir/$stale_file.bak"
    fi
    echo "$dir"
}

# Every version this tree still mentions, deduplicated. Six pins agreeing shows up as one line.
versions_in() {
    grep -rhoE '[0-9]+\.[0-9]+\.[0-9]+' "$1" | sort -u | tr '\n' ' ' | sed 's/ $//'
}

# Writing a version reaches all six. One line back means they agree; the assertion would read the
# same for five of six only if the sixth carried no version at all, which case 4 rules out.
dir="$(tree_at 0.3.0)"
( cd "$dir" && "$script" 0.4.0 >/dev/null )
check "writing a version reaches every pin" "0.4.0" "$(versions_in "$dir")"
rm -rf "$dir"

# No argument means "make the other five agree with <revision>". This is the form CI runs, and the
# pin it is seeded stale here is the one nothing else in the repository checks.
dir="$(tree_at 0.3.0 control/mcp-server/src/main/resources/application.properties 0.2.1)"
check "a stale pin is visible before the run" "0.2.1 0.3.0" "$(versions_in "$dir")"
( cd "$dir" && "$script" >/dev/null )
check "no argument brings a stale pin back to <revision>" "0.3.0" "$(versions_in "$dir")"
rm -rf "$dir"

# The control for the case above: on a tree that already agrees, the run must write nothing at all.
# CI reads its answer as `git diff --exit-code`, so a run that reformats a file it did not need to
# touch reports a version mismatch that is not one.
dir="$(tree_at 0.3.0)"
before="$(cd "$dir" && find . -type f -exec cksum {} + | sort)"
( cd "$dir" && "$script" >/dev/null )
after="$(cd "$dir" && find . -type f -exec cksum {} + | sort)"
check "a tree that already agrees is left untouched" "$before" "$after"
rm -rf "$dir"

# A version that is not three numbers has to stop the run. `v0.4.0` is the shape that gets typed, and
# it would otherwise be written into the image tag and the installer pin, where it is wrong in a way
# that only shows up when a user tries to install.
dir="$(tree_at 0.3.0)"
if ( cd "$dir" && "$script" v0.4.0 >/dev/null 2>&1 ); then
    echo "FAIL - a version that is not three numbers must refuse"
    failures=$((failures + 1))
else
    echo "ok   - a version that is not three numbers refuses"
fi
rm -rf "$dir"

# A pin that has been renamed away must stop the run rather than be skipped. This is the whole reason
# the script checks its own work: a substitution that matches nothing succeeds, so a renamed constant
# would drop out of the set silently and stay behind at the old version forever.
dir="$(tree_at 0.3.0)"
sed -i.bak 's/PINNED_VERSION/INSTALLER_VERSION/' "$dir/install/install.sh" && rm -f "$dir/install/install.sh.bak"
if ( cd "$dir" && "$script" 0.4.0 >/dev/null 2>&1 ); then
    echo "FAIL - a renamed pin must refuse, not be skipped"
    failures=$((failures + 1))
else
    echo "ok   - a renamed pin refuses"
fi
rm -rf "$dir"

# --- the write-back pull request carries every pin this script writes ----------------------------
# release.yml opens a pull request putting the released version onto the default branch, and it lists
# the paths that pull request may contain. That list and the pins below are the same set stated twice.
# A seventh pin added here and not there does not fail anything: the release run writes it, the pull
# request quietly leaves it out, and the default branch keeps one file at the old version -- which is
# exactly the single unwatched pin this script was written to end, back again by another route.
wf="$repo/.github/workflows/release.yml"
pinned="$(sed -n 's/^pin \([^ ]*\) .*/\1/p' "$here/set-version.sh" | sort)"
listed="$(sed -n '/add-paths: |/,/^          [a-z]/p' "$wf" | sed -n 's/^            \([^ ]*\)$/\1/p' | sort)"
if [ "$pinned" = "$listed" ]; then
    echo "ok   - the write-back pull request lists exactly the files this script pins"
else
    echo "FAIL - the write-back pull request and this script disagree about which files pin a version"
    diff <(printf '%s\n' "$pinned") <(printf '%s\n' "$listed") | sed 's/^/       /'
    failures=$((failures + 1))
fi

# Both steps, not one. A push or a pull request made with the default GITHUB_TOKEN triggers no
# workflow, so the required checks would be permanently absent; and giving the app token only to the
# pull request step leaves checkout's persisted credentials to push the branch, which produces the
# same absent checks and looks identical to the secret not being set.
if [ "$(grep -c 'steps.token.outputs.token' "$wf")" -ge 2 ]; then
    echo "ok   - the write-back gives its app token to checkout as well as to the pull request"
else
    echo "FAIL - the write-back does not give its app token to both checkout and the pull request"
    failures=$((failures + 1))
fi

# --list prints the pins and nothing else. The admission gate reads this to tell a pull request that
# only rewrites version pins from one that changes behaviour, so a pin that stops being listed would
# quietly put that pull request back in scope -- and the gate would refuse every release's write-back
# with a demand it cannot satisfy.
listed="$("$script" --list)"
if [ "$(printf '%s\n' "$listed" | wc -l | tr -d ' ')" = 6 ] \
    && printf '%s\n' "$listed" | grep -qx 'pom.xml' \
    && printf '%s\n' "$listed" | grep -qx 'cli/src/main/java/io/tapstate/cli/Cli.java'; then
    echo "ok   - --list prints the six pinned files"
else
    echo "FAIL - --list did not print the six pinned files, it printed: $listed"
    failures=$((failures + 1))
fi

# --list must not write. It is called from a gate on a checkout the gate does not own, and a mode
# that edited the tree there would rewrite six files as a side effect of being asked a question.
# Asked of a synthetic tree, like every case above: an earlier draft of this case copied the real
# repository instead, and in a linked worktree `.git` is a file pointing back at the real git
# directory -- so `git` inside the copy committed to the repository being tested. A case that builds
# its own tree cannot reach anything it did not create.
listing_dir="$(tree_at 9.9.9)"
before="$(versions_in "$listing_dir")"
( cd "$listing_dir" && "$script" --list >/dev/null )
check "--list leaves the tree untouched" "$before" "$(versions_in "$listing_dir")"

# The body the write-back opens its pull request with has to answer the sections the template check
# requires. It is written here, once, and no human sees it before it is posted -- so a missing
# section is not caught in review, it is caught by every release's write-back being red.
body_sections="$(awk '/^  write-back:/ { inside = 1 } inside && /^  [a-z][a-z0-9_-]*:[ \t]*$/ && !/write-back/ { inside = 0 } inside' "$wf")"
missing=""
for section in "## Linked issue" "## Live verification scenario" "### Release note"; do
    printf '%s\n' "$body_sections" | grep -qF "$section" || missing="$missing '$section'"
done
if [ -z "$missing" ]; then
    echo "ok   - the write-back body answers every section the template check requires"
else
    echo "FAIL - the write-back body is missing:$missing"
    failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
    echo "$failures case(s) failed" >&2
    exit 1
fi
echo "all cases passed"
