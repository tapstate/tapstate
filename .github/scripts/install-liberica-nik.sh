#!/usr/bin/env bash
# Install the exact release toolchain from Tapstate's verified copy.
set -euo pipefail

platform="${1:?usage: install-liberica-nik.sh <os-arch>}"
lock="$(dirname "$0")/../liberica-nik.lock.json"
tag="$(jq -er '.releaseTag' "$lock")"
file="$(jq -er --arg platform "$platform" '.platforms[$platform].file' "$lock")"
digest="$(jq -er --arg platform "$platform" '.platforms[$platform].sha256' "$lock")"
directory="$(jq -er '.directory' "$lock")"
install_dir="$(mktemp -d "${RUNNER_TEMP:?}/tapstate-nik.XXXXXX")"

cd "$install_dir"
trap 'rm -f -- "$file"' EXIT
curl --fail --silent --show-error --location --retry 3 --output "$file" \
  "https://github.com/tapstate/tapstate/releases/download/$tag/$file"
printf '%s  %s\n' "$digest" "$file" | shasum -a 256 -c -
tar -xzf "$file"

java_home="$install_dir/$directory"
if [[ "$platform" == darwin-* ]]; then
  java_home="$java_home/Contents/Home"
fi
"$java_home/bin/java" --version
"$java_home/bin/native-image" --version
printf 'JAVA_HOME=%s\nGRAALVM_HOME=%s\n' "$java_home" "$java_home" >> "${GITHUB_ENV:?}"
printf '%s\n' "$java_home/bin" >> "${GITHUB_PATH:?}"
