#!/usr/bin/env bash
set -euo pipefail

distribution="$(sed -n 's|^distributionUrl=.*/apache-maven-\(.*\)-bin\.zip$|\1|p' .mvn/wrapper/maven-wrapper.properties)"

wrapper_version() {
  MAVEN_USER_HOME="$RUNNER_TEMP/tapstate-ci-maven" \
    timeout --verbose --kill-after=5s 60s ./mvnw -B -Dstyle.color=never --version
}

if ! version_output="$(MVNW_REPOURL="${MVNW_REPOURL:-https://repository.apache.org/content/repositories/releases}" wrapper_version)"; then
  [[ -z "${MVNW_REPOURL:-}" ]] || exit 1
  echo '::warning::Apache releases Maven setup failed. Retrying with the pinned wrapper URL.' >&2
  version_output="$(wrapper_version)"
fi
maven_home="$(sed -n 's/^Maven home: //p' <<< "$version_output")"

if [[ -z "$distribution" || ! -x "$maven_home/bin/mvn" ]] ||
    ! grep -Fq "Apache Maven $distribution (" <<< "$version_output"; then
  echo '::error::Maven Wrapper did not resolve the pinned Maven distribution.'
  exit 1
fi

printf '%s\n' "$maven_home/bin" >> "$GITHUB_PATH"
printf 'MAVEN_HOME=%s\n' "$maven_home" >> "$GITHUB_ENV"
printf '%s\n' "$version_output"
