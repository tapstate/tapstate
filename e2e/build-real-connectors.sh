#!/usr/bin/env bash
#
# Builds the real connector jars this repository's real-connector witnesses drive, from the
# connectors' own source.
#
# Those jars are published nowhere a build can fetch them. This lane used to name a directory somebody
# had staged on the runner by hand, which is exactly why it could never be scheduled: a nightly cannot
# depend on a human having put files somewhere first. Building them here removes that arrangement - a
# run needs the upstream repository and the PDK repository to be reachable, and nothing else.
#
# A failure to clone or build stops the run rather than leaving the destination empty. The witnesses
# gate on jars resolving, and an empty destination reads to that gate as "no real-connector run was
# intended" - a green build that drove no connector at all, which is the outcome this lane exists to
# prevent.
#
# The connector source is taken from its default branch, not a pinned commit. Its PDK dependencies are
# snapshots, so pinning the commit would not buy a reproducible build anyway; following the branch
# means an upstream change that breaks the connectors surfaces on the next nightly rather than at the
# next release.
#
# The nightly lane runs this, and so can a developer who wants the real-connector witnesses locally:
#
#   e2e/build-real-connectors.sh /tmp/connectors
#   mvn -pl e2e -am verify -Dapi.version=1.44 -Dtapstate.e2e.connectors-dir=/tmp/connectors \
#     -Dit.test=PublishedExamplesIT
#
# A second argument names the directory to clone into, which has to be empty; it defaults to a fresh
# temporary directory.

set -euo pipefail

readonly SOURCE_REPOSITORY="https://github.com/tapdata/tapdata-connectors.git"

# Connector ids as the specifications name them, paired with the module that builds each one. The id
# is also the file-name prefix the witnesses resolve by, so it cannot be chosen freely here.
readonly CONNECTOR_IDS=(mysql mongodb)
readonly CONNECTOR_MODULES=(connectors/mysql-connector connectors/mongodb-connector)

destination="${1:?usage: build-real-connectors.sh <destination-directory>}"
workspace="${2:-$(mktemp -d)}"

mkdir -p "$destination"
destination="$(cd "$destination" && pwd)"

echo "Cloning the connector source into $workspace"
git clone --depth 1 --quiet "$SOURCE_REPOSITORY" "$workspace/tapdata-connectors"

modules="$(IFS=,; echo "${CONNECTOR_MODULES[*]}")"
echo "Building $modules"
mvn -B -f "$workspace/tapdata-connectors/pom.xml" -pl "$modules" -am -DskipTests package

# Stage one jar per connector, and insist on exactly one.
#
# Each module builds two jars: a thin one holding just its own classes, and the shaded runtime jar that
# carries its dependencies. Only the shaded one is loadable as a connector, and the two are told apart
# by the "-v" the shade plugin puts before the version. Matching on the connector id alone would take
# both, and the witnesses refuse an ambiguous match - so the selector has to be this specific.
#
# Staging the whole build output would break them the same way, on the sibling connectors that share a
# prefix: mysql-pxc, mongodb-atlas.
for index in "${!CONNECTOR_IDS[@]}"; do
    id="${CONNECTOR_IDS[$index]}"
    module="${CONNECTOR_MODULES[$index]}"
    artifact="$(basename "$module")"
    built=()
    while IFS= read -r jar; do built+=("$jar"); done < <(
        find "$workspace/tapdata-connectors/$module/target" -maxdepth 1 -name "$artifact-v*.jar" -type f | sort
    )
    if [ "${#built[@]}" -ne 1 ]; then
        echo "expected exactly one shaded $artifact jar in $module/target, found ${#built[@]}: ${built[*]-}" >&2
        exit 1
    fi
    cp "${built[0]}" "$destination/"
    echo "Staged $(basename "${built[0]}") for connector '$id'"
done

# The destination is what the witnesses are pointed at, so assert the property they depend on here
# rather than letting an ambiguous directory fail later as something that reads like a product bug.
for id in "${CONNECTOR_IDS[@]}"; do
    staged="$(find "$destination" -maxdepth 1 -name "$id*.jar" -type f | wc -l | tr -d ' ')"
    if [ "$staged" -ne 1 ]; then
        echo "$destination resolves $staged jars for '$id', and the witnesses require exactly one" >&2
        exit 1
    fi
done

echo "Connector jars staged in $destination"
