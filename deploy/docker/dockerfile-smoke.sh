#!/usr/bin/env bash
#
# Build-behavior smoke for the server image's boot-jar COPY.
#
# The image smoke next door asserts things about a *built* image; this one asserts the thing that
# decides *which* jar that image is built from. The Dockerfile names the boot jar with a wildcard:
#
#   COPY --chown=tapstate:tapstate app/target/app-*-boot.jar /opt/tapstate/tapstate.jar
#
# A wildcard is only safe while it resolves to exactly one file. Locally it often does not: app/target
# is not cleaned between revisions, so a working copy that has built more than once holds a jar per
# revision it has built, and the compose file for local development documents exactly that workflow.
# With two candidates the builder neither errors nor warns -- it copies them in turn over the same
# single destination, so the last one by name order is what the image ships. Name order is not
# intent: a leftover whose version string sorts above the current revision wins, and the image then
# carries a build that predates the change someone is about to verify against it. Nothing in the
# build output says so, and a verification run against that image reports the change as missing --
# indistinguishable from the change itself being broken.
#
# The case therefore builds the real Dockerfile against a synthetic context holding two boot jars --
# the one this checkout's revision produces, and a leftover that sorts above it -- and requires the
# ambiguity to be resolved rather than silently decided. Either resolution passes: refusing the build
# (a wildcard that must match exactly one file) or copying the intended jar by exact name. What fails
# is a build that exits 0 carrying the leftover.
#
# The context is synthetic on purpose: the Dockerfile reads nothing from the context but that glob, so
# the case needs no Maven build and never writes into the working tree. The repository's .dockerignore
# travels with it, since which jars reach the builder at all is that file's decision.
#
# Usage:
#   deploy/docker/dockerfile-smoke.sh
#
# Requires a Docker daemon. Exit 0 iff the ambiguous glob is resolved rather than silently decided.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DOCKERFILE="$REPO_ROOT/deploy/docker/Dockerfile"
TAG="tapstate:dockerfile-smoke-$$"
CTX="$(mktemp -d)"
trap 'docker rmi -f "$TAG" >/dev/null 2>&1 || true; rm -rf "$CTX"' EXIT

red()   { printf '\033[31m%s\033[0m\n' "$1"; }
green() { printf '\033[32m%s\033[0m\n' "$1"; }

# The intended jar is the one this checkout builds, so it is read from the same source of truth the
# build reads it from. The leftover carries a suffix rather than a bumped version: that keeps it
# sorting above the intended jar whatever the revision happens to be, without parsing the version.
VERSION="$(sed -n 's|.*<revision>\([^<]*\)</revision>.*|\1|p' "$REPO_ROOT/pom.xml" | head -1)"
[ -n "$VERSION" ] || { red "pom.xml carries no <revision> - is this the repository root?"; exit 2; }
STALE="$VERSION.1"

mkdir -p "$CTX/app/target"
cp "$REPO_ROOT/.dockerignore" "$CTX/.dockerignore"
printf 'intended-%s' "$VERSION" >"$CTX/app/target/app-$VERSION-boot.jar"
printf 'leftover-%s' "$STALE"   >"$CTX/app/target/app-$STALE-boot.jar"

echo "building $DOCKERFILE against a context holding app-$VERSION-boot.jar and app-$STALE-boot.jar"
if ! docker build -f "$DOCKERFILE" -t "$TAG" "$CTX" >"$CTX/build.log" 2>&1; then
    green "PASS: the build refused a glob matching two boot jars"
    exit 0
fi

SHIPPED="$(docker run --rm --entrypoint cat "$TAG" /opt/tapstate/tapstate.jar)"
if [ "$SHIPPED" = "intended-$VERSION" ]; then
    green "PASS: the build resolved the two candidates to app-$VERSION-boot.jar"
    exit 0
fi

red "FAIL: the build exited 0 and shipped '$SHIPPED', not 'intended-$VERSION'"
red "      app-$STALE-boot.jar won because it sorts last, and the build said nothing:"
sed -n 's/^/      /p' "$CTX/build.log" | grep -i -e 'COPY' -e 'warn' || true
exit 1
