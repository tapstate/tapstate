#!/usr/bin/env bash
# Cover primary-election failure, bounded output, and best-effort diagnostics without a daemon.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH"' EXIT
mkdir -p "$SCRATCH/bin"

cat > "$SCRATCH/bin/docker" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
record() { printf '%s\n' "$1" >> "$IMAGE_SMOKE_FIXTURE_DIR/events"; }
unexpected() { record "unexpected: $*"; exit 99; }
case "$1" in
    image) [ "$2" = inspect ] || unexpected "$@" ;;
    network)
        case "$2" in
            create) record network-created ;;
            rm) record network-deleted; exit 24 ;;
            *) unexpected "$@" ;;
        esac ;;
    run)
        [[ "$*" == *'mongo:7.0 --replSet rs0 --bind_ip_all'* ]] || unexpected "$@"
        record mongo-started
        if [ "$IMAGE_SMOKE_FIXTURE_MODE" = run-failure ]; then
            echo 'fixture-run-error' >&2
            exit 125
        fi
        touch "$IMAGE_SMOKE_FIXTURE_DIR/mongo-exists" ;;
    exec)
        [ -f "$IMAGE_SMOKE_FIXTURE_DIR/mongo-exists" ] || unexpected exec-after-delete
        case "${!#}" in
            'db.runCommand({ ping: 1 }).ok') record ping; echo 1 ;;
            'db.hello().isWritablePrimary')
                record primary
                attempts=0
                if [ -f "$IMAGE_SMOKE_FIXTURE_DIR/attempts" ]; then
                    attempts="$(cat "$IMAGE_SMOKE_FIXTURE_DIR/attempts")"
                fi
                attempts=$((attempts + 1))
                printf '%s\n' "$attempts" > "$IMAGE_SMOKE_FIXTURE_DIR/attempts"
                printf 'early-readiness-error:' >&2
                printf '%32768s' '' | tr ' ' x >&2
                printf '\nprimary-readiness-error-%s: MongoServerSelectionError\n' "$attempts" >&2
                exit 17 ;;
            *rs.initiate*) record initiate ;;
            *) unexpected "$@" ;;
        esac ;;
    inspect|logs)
        record "$1"
        if [ ! -f "$IMAGE_SMOKE_FIXTURE_DIR/mongo-exists" ]; then
            echo 'fixture no longer exists' >&2
            exit 44
        fi
        if [ "$IMAGE_SMOKE_FIXTURE_MODE" = diagnostics-failure ]; then
            echo "fixture-$1-error" >&2
            exit 45
        fi
        if [ "$1" = inspect ]; then
            printf '{"Status":"exited","ExitCode":42}\n'
        else
            [[ "$*" =~ --tail(=|\ )[0-9]+ ]] || unexpected unbounded-logs
            printf 'early-mongo-log:'
            printf '%32768s' '' | tr ' ' x
            printf '\nfinal-mongo-log: fatal startup failure\n'
        fi ;;
    rm)
        record mongo-deleted
        rm -f "$IMAGE_SMOKE_FIXTURE_DIR/mongo-exists"
        # Cleanup failures must not hide the original readiness or docker run status.
        exit 23 ;;
    *) unexpected "$@" ;;
esac
STUB

cat > "$SCRATCH/bin/sleep" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
[ "$#" -eq 1 ] && [ "$1" = 1 ] || exit 99
printf 'sleep-1\n' >> "$IMAGE_SMOKE_FIXTURE_DIR/events"
STUB
chmod +x "$SCRATCH/bin/docker" "$SCRATCH/bin/sleep"

fail() {
    cat "$CASE_DIR/output" >&2
    cat "$CASE_DIR/events" >&2
    printf 'FAIL: %s: %s\n' "$mode" "$1" >&2
    exit 1
}

for mode in election diagnostics-failure run-failure; do
    CASE_DIR="$SCRATCH/$mode"
    mkdir -p "$CASE_DIR"
    status=0
    PATH="$SCRATCH/bin:$PATH" IMAGE_SMOKE_FIXTURE_DIR="$CASE_DIR" IMAGE_SMOKE_FIXTURE_MODE="$mode" \
        bash "$HERE/image-smoke.sh" tapstate:fixture-diagnostics-test > "$CASE_DIR/output" 2>&1 || status=$?
    if grep -q '^unexpected:' "$CASE_DIR/events"; then fail 'unexpected Docker command'; fi
    order="$(grep -E '^(inspect|logs|mongo-deleted|network-deleted)$' "$CASE_DIR/events")"
    [ "$order" = $'inspect\nlogs\nmongo-deleted\nnetwork-deleted' ] || fail 'diagnostics did not precede teardown'
    [ ! -f "$CASE_DIR/mongo-exists" ] || fail 'fixture survived teardown'
    if [ "$mode" = run-failure ]; then
        [ "$status" -eq 125 ] || fail "docker run status 125 became $status"
        grep -Fq fixture-run-error "$CASE_DIR/output" || fail 'original docker run error was lost'
        printf 'PASS: docker run failure preserves exit 125 and tears down after diagnostic errors\n'
        continue
    fi
    [ "$status" -eq 2 ] || fail "readiness status 2 became $status"
    grep -Fq 'mongo replica set did not elect a primary' "$CASE_DIR/output" || fail 'election failure was not reached'
    [ "$(grep -c '^ping$' "$CASE_DIR/events")" -eq 1 ] || fail 'startup did not succeed on its first probe'
    [ "$(grep -c '^initiate$' "$CASE_DIR/events")" -eq 1 ] || fail 'replica set was not initiated once'
    [ "$(grep -c '^primary$' "$CASE_DIR/events")" -eq 60 ] || fail 'primary probe budget changed'
    [ "$(grep -c '^sleep-1$' "$CASE_DIR/events")" -eq 60 ] || fail 'primary readiness delays changed'
    grep -Fq 'primary-readiness-error-60: MongoServerSelectionError' "$CASE_DIR/output" || fail 'last primary error was lost'
    if grep -Fq early-readiness-error "$CASE_DIR/output"; then fail 'readiness stderr was not bounded'; fi
    [ "$(wc -c < "$CASE_DIR/output")" -lt 25000 ] || fail 'diagnostic output exceeded its byte budget'
    if [ "$mode" = election ]; then
        grep -Fq exited "$CASE_DIR/output" || fail 'container state was lost'
        grep -Fq 42 "$CASE_DIR/output" || fail 'container exit code was lost'
        grep -Fq final-mongo-log "$CASE_DIR/output" || fail 'Mongo logs were lost'
        if grep -Fq early-mongo-log "$CASE_DIR/output"; then fail 'Mongo logs were not byte-bounded'; fi
    else
        grep -Fq fixture-inspect-error "$CASE_DIR/output" || fail 'inspect failure was not reported'
        grep -Fq fixture-logs-error "$CASE_DIR/output" || fail 'log failure was not reported'
    fi
    printf 'PASS: %s retains bounded primary diagnostics and exit 2 despite teardown failures\n' "$mode"
done
