#!/usr/bin/env bash
# Put one exact Maven release first on PATH. Hosted runner images roll their
# preinstalled Maven forward machine by machine, and every job whose build
# inputs are compared must report the same toolchain.
set -euo pipefail
version=3.10.1
sha512=8af2fcc48ee6b8f8fc0b21054226e14404b88500df6483cdeb3ae603408f6de27b1397ac591a9fb0d98fee64f4f624fee1383efd25b8998786cda1598fb9ea39
root="${RUNNER_TEMP:?RUNNER_TEMP is required}/pinned-maven"
archive="$root/apache-maven-$version-bin.tar.gz"
mkdir -p "$root"
for attempt in 1 2 3; do
  if curl -fsSL --retry 3 -o "$archive" \
      "https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/$version/apache-maven-$version-bin.tar.gz" \
      && echo "$sha512  $archive" | sha512sum -c --quiet -; then
    break
  fi
  if [ "$attempt" -eq 3 ]; then
    echo "::error::Maven $version could not be downloaded and verified" >&2
    exit 1
  fi
  sleep $((attempt * 5))
done
tar -xzf "$archive" -C "$root"
echo "$root/apache-maven-$version/bin" >> "${GITHUB_PATH:?GITHUB_PATH is required}"
"$root/apache-maven-$version/bin/mvn" --version
