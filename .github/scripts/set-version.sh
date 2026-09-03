#!/usr/bin/env bash
#
# Write one release version into every place that pins one.
#
# Six files carry the version, and only the first is a source of truth. The release pipeline runs
# this before it builds, so the version enters once, as an argument, instead of being hand-edited in
# six places by whoever remembers all six.
#
#   pom.xml                                                    <revision>, the source of truth
#   cli/src/main/java/io/tapstate/cli/Cli.java                 what `tapstate --version` prints
#   control/mcp-server/src/main/resources/application.properties  what the MCP server reports
#   deploy/quickstart/quickstart.sh                            which CLI the quickstart installs
#   install/install.sh                                         which release the installer pins
#   deploy/quickstart/docker-compose.yml                       which server image the demo runs
#
# Run with no argument to make the other five agree with <revision> instead. That is the form CI
# runs, followed by `git diff --exit-code`: a pin that drifted comes back as a diff naming the file
# and the wrong value. Five of the six already had something watching them; the MCP one had nothing
# at all, and a wrong value there is invisible because nothing in the tree reads it.
#
# Checks its own work, pin by pin. A substitution that matches nothing exits zero, so a constant
# somebody renamed would drop out of the set without a word and sit at the old version release after
# release. Refusing is the only way that surfaces.
#
# Not build-time injection. The CLI constant has to survive into a native image, where neither a
# manifest nor a bundled resource is readable, so it stays a compile-time constant and this script
# is what keeps it honest.

set -eu

# Answering "which files pin the version" without writing anything. The list is produced by the same
# pin calls that do the writing, so a pin added or renamed is listed by construction. A second,
# hand-kept copy of the list would drift the first time one moved, and the admission gate that reads
# this would silently put the release's own write-back back in scope -- demanding an end-to-end case
# for a version constant, which is a demand no write-back can satisfy.
list_only=no
if [ "${1:-}" = "--list" ]; then
    list_only=yes
    version=0.0.0
fi

if [ "$list_only" = no ]; then
version="${1:-}"

if [ -z "$version" ]; then
    version="$(sed -n 's|.*<revision>\([^<]*\)</revision>.*|\1|p' pom.xml | head -1)"
    if [ -z "$version" ]; then
        echo "pom.xml carries no <revision> - is this the repository root?" >&2
        exit 1
    fi
fi

case "$version" in
    *[!0-9.]* | '' ) bad=yes ;;
    *) bad=no ;;
esac
if [ "$bad" = yes ] || [ "$(echo "$version" | tr -cd . | wc -c)" -ne 2 ]; then
    echo "'$version' is not a release version - expected three numbers, as in 0.4.0" >&2
    exit 1
fi
fi

# file, sed expression writing the version, and the text that proves it landed.
pin() {
    local file="$1" expression="$2" proof="$3"
    if [ "$list_only" = yes ]; then
        printf '%s\n' "$file"
        return 0
    fi
    if [ ! -f "$file" ]; then
        echo "$file is missing - it pins the version, so this is not a tree we can release from" >&2
        exit 1
    fi
    sed -i.bak "$expression" "$file"
    rm -f "$file.bak"
    if ! grep -qF "$proof" "$file"; then
        echo "$file: nothing matched the version pin - has it been renamed? Expected to find: $proof" >&2
        exit 1
    fi
}

pin pom.xml \
    "s|<revision>[^<]*</revision>|<revision>$version</revision>|" \
    "<revision>$version</revision>"

pin cli/src/main/java/io/tapstate/cli/Cli.java \
    "s|VERSION = \"tapstate [^\"]*\"|VERSION = \"tapstate $version\"|" \
    "VERSION = \"tapstate $version\""

pin control/mcp-server/src/main/resources/application.properties \
    "s|^spring.ai.mcp.server.version=.*|spring.ai.mcp.server.version=$version|" \
    "spring.ai.mcp.server.version=$version"

pin deploy/quickstart/quickstart.sh \
    "s|^CLI_VERSION=\".*\"|CLI_VERSION=\"$version\"|" \
    "CLI_VERSION=\"$version\""

pin install/install.sh \
    "s|^PINNED_VERSION=\".*\"|PINNED_VERSION=\"$version\"|" \
    "PINNED_VERSION=\"$version\""

pin deploy/quickstart/docker-compose.yml \
    "s|image: ghcr.io/tapstate/tapstate:.*|image: ghcr.io/tapstate/tapstate:$version|" \
    "image: ghcr.io/tapstate/tapstate:$version"

if [ "$list_only" = no ]; then
    echo "$version"
fi
