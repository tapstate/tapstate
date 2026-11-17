#!/usr/bin/env bash
# Share downloaded dependencies while keeping every install's project artifacts local.
set -euo pipefail
if [ "$#" -ne 3 ]; then
  echo 'usage: ci-maven-cache.sh seed|seed-stable|collect CACHE REPOSITORY' >&2
  exit 2
fi
mode="$1"
case "$mode" in seed|seed-stable|collect) ;; *) echo 'expected seed, seed-stable or collect' >&2; exit 2 ;; esac
mkdir -p "$2" "$3"
cache="$(cd "$2" && pwd -P)"
repository="$(cd "$3" && pwd -P)"
if [ "$cache" = "$repository" ]; then
  echo 'cache and build repository must be different directories' >&2
  exit 1
fi
source="$cache"
destination="$repository"
if [ "$mode" = collect ]; then
  source="$repository"
  destination="$cache"
fi
if [ "$mode" = seed-stable ]; then
  rsync -a --exclude='/io/tapstate/' --exclude='*-SNAPSHOT/' -- "$source/" "$destination/"
else
  rsync -a --exclude='/io/tapstate/' -- "$source/" "$destination/"
fi
