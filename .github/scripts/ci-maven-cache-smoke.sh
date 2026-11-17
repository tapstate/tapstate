#!/usr/bin/env bash
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/cache/io/tapstate/core" "$scratch/cache/org/example"
printf stale > "$scratch/cache/io/tapstate/core/old.jar"
printf dependency > "$scratch/cache/org/example/cached.jar"
mkdir -p "$scratch/cache/io/tapdata/demo/1-SNAPSHOT"
printf old > "$scratch/cache/io/tapdata/demo/1-SNAPSHOT/demo.jar"
"$here/ci-maven-cache.sh" seed-stable "$scratch/cache" "$scratch/stable"
test -f "$scratch/stable/org/example/cached.jar"
test ! -e "$scratch/stable/io/tapdata/demo/1-SNAPSHOT"
"$here/ci-maven-cache.sh" seed "$scratch/cache" "$scratch/repository"
test -f "$scratch/repository/org/example/cached.jar"
test ! -e "$scratch/repository/io/tapstate"
mkdir -p "$scratch/repository/io/tapstate/core" "$scratch/repository/com/example"
printf built > "$scratch/repository/io/tapstate/core/new.jar"
printf fetched > "$scratch/repository/com/example/new.jar"
"$here/ci-maven-cache.sh" collect "$scratch/cache" "$scratch/repository"
test -f "$scratch/cache/com/example/new.jar"
test ! -e "$scratch/cache/io/tapstate/core/new.jar"
if "$here/ci-maven-cache.sh" seed "$scratch/cache" "$scratch/cache"; then
  echo 'same source and destination was accepted' >&2
  exit 1
fi
echo 'ci-maven-cache smoke: both directions exclude project artifacts'
