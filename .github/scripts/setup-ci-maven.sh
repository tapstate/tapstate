#!/usr/bin/env bash
set -euo pipefail

distribution="$(sed -n 's|^distributionUrl=.*/apache-maven-\(.*\)-bin\.zip$|\1|p' .mvn/wrapper/maven-wrapper.properties)"
version_output="$(MAVEN_USER_HOME="$RUNNER_TEMP/tapstate-ci-maven" ./mvnw -B -Dstyle.color=never --version)"
maven_home="$(sed -n 's/^Maven home: //p' <<< "$version_output")"

if [[ -z "$distribution" || ! -x "$maven_home/bin/mvn" ]] ||
    ! grep -Fq "Apache Maven $distribution (" <<< "$version_output"; then
  echo '::error::Maven Wrapper did not resolve the pinned Maven distribution.'
  exit 1
fi

printf '%s\n' "$maven_home/bin" >> "$GITHUB_PATH"
printf 'MAVEN_HOME=%s\n' "$maven_home" >> "$GITHUB_ENV"
printf '%s\n' "$version_output"
