#!/usr/bin/env bash
set -euo pipefail

# Hosted runner image updates can change Maven between independent build producers.
version=3.9.16
sha512=831a8591fe20c8243b1dbe7d71e3244f31d1665b0804b2e825e38cbbe5ce0cafb8338851f90780735568773e0a6cd07bbec107cda0b896b008b861075358b6f6
maven_home="${RUNNER_TEMP:?}/apache-maven-$version"
archive="$maven_home-bin.tar.gz"

curl --fail --silent --show-error --location --retry 3 --connect-timeout 15 --max-time 120 \
    "https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/$version/apache-maven-$version-bin.tar.gz" \
    --output "$archive"
printf '%s  %s\n' "$sha512" "$archive" | sha512sum -c -
tar -xzf "$archive" -C "$RUNNER_TEMP"
printf '%s\n' "$maven_home/bin" >> "${GITHUB_PATH:?}"
printf 'MAVEN_HOME=%s\n' "$maven_home" >> "${GITHUB_ENV:?}"
"$maven_home/bin/mvn" --version
