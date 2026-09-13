#!/usr/bin/env bash
# Exercise the build smoke's verdicts without a Docker daemon. The real Dockerfile build stays in
# the image-smoke job; these cases ensure infrastructure failures cannot stand in for its refusal.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH"' EXIT
mkdir -p "$SCRATCH/bin"
export SMOKE_VERSION
SMOKE_VERSION="$(sed -n 's|.*<revision>\([^<]*\)</revision>.*|\1|p' "$HERE/../../pom.xml" | head -1)"

cat > "$SCRATCH/bin/docker" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
    rmi) exit 0 ;;
    run)
        case "$SMOKE_MODE" in
            intended) printf 'intended-%s' "$SMOKE_VERSION" ;;
            stale) printf 'leftover-%s.1' "$SMOKE_VERSION" ;;
            *) echo 'unexpected container run' >&2; exit 2 ;;
        esac
        exit 0 ;;
    build) ;;
    *) echo 'unexpected docker command' >&2; exit 2 ;;
esac

case "$SMOKE_MODE" in
    daemon) echo 'Cannot connect to the Docker daemon' >&2 ;;
    pull) echo 'failed to resolve eclipse-temurin:21-jre: registry unavailable' >&2 ;;
    run-error) echo '#8 ERROR: process apt-get update exited with code 100' >&2 ;;
    source-only)
        # BuildKit echoes RUN source even when the command fails before emitting its diagnostic.
        echo '#8 [boot-jar 3/3] RUN echo "app/target/app-*-boot.jar matched 2 files in the build context; it must match exactly one:"; exit 1'
        printf '#8 [boot-jar 3/3] RUN echo "  app/target/app-%s-boot.jar"\n' "$SMOKE_VERSION"
        printf '#8 [boot-jar 3/3] RUN echo "  app/target/app-%s.1-boot.jar"\n' "$SMOKE_VERSION" ;;
    refusal|plain|wrong-count|missing-candidate)
        prefix='#8 0.123 '
        [ "$SMOKE_MODE" != plain ] || prefix=''
        count=2
        [ "$SMOKE_MODE" != wrong-count ] || count=3
        printf '%sapp/target/app-*-boot.jar matched %s files in the build context; it must match exactly one:\n' "$prefix" "$count"
        printf '%s  app/target/app-%s-boot.jar\n' "$prefix" "$SMOKE_VERSION"
        if [ "$SMOKE_MODE" != missing-candidate ]; then
            printf '%s  app/target/app-%s.1-boot.jar\n' "$prefix" "$SMOKE_VERSION"
        fi ;;
    intended|stale) exit 0 ;;
    *) echo 'unexpected smoke mode' >&2; exit 2 ;;
esac
exit 1
STUB
chmod +x "$SCRATCH/bin/docker"

passed=0
failed=0
expect() {
    local mode="$1" expected="$2" diagnostic="$3" status=0
    PATH="$SCRATCH/bin:$PATH" SMOKE_MODE="$mode" bash "$HERE/dockerfile-smoke.sh" \
        > "$SCRATCH/output" 2>&1 || status=$?
    if [ "$status" -eq "$expected" ] && grep -Fq "$diagnostic" "$SCRATCH/output" &&
        { [ "$expected" -eq 0 ] || ! grep -Fq 'PASS:' "$SCRATCH/output"; }; then
        echo "PASS: $mode"
        passed=$((passed + 1))
    else
        echo "FAIL: $mode (exit $status, expected $expected with '$diagnostic')"
        cat "$SCRATCH/output"
        failed=$((failed + 1))
    fi
}

expect daemon 1 'Cannot connect to the Docker daemon'
expect pull 1 'registry unavailable'
expect run-error 1 'apt-get update exited with code 100'
expect source-only 1 'build failed without the expected two-jar refusal'
expect wrong-count 1 'matched 3 files'
expect missing-candidate 1 'build failed without the expected two-jar refusal'
expect refusal 0 'PASS: the build refused a glob matching two boot jars'
expect plain 0 'PASS: the build refused a glob matching two boot jars'
expect intended 0 "PASS: the build resolved the two candidates to app-$SMOKE_VERSION-boot.jar"
expect stale 1 "shipped 'leftover-$SMOKE_VERSION.1'"

printf '\n%s passed, %s failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ]
