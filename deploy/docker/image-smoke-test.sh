#!/usr/bin/env bash
# Exercise one Mongo startup failure through the real smoke script without a Docker daemon.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH"' EXIT
mkdir -p "$SCRATCH/bin"

cat > "$SCRATCH/bin/docker" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
record() { printf '%s\n' "$1" >> "$IMAGE_SMOKE_TEST_DIR/events"; }
unexpected() { record "unexpected: $*"; echo "unexpected fake Docker command: $*" >&2; exit 99; }
case "$1" in
    image)
        [ "$2" = inspect ] || unexpected "$@"
        exit 0 ;;
    network)
        case "$2" in
            create) record network-created ;;
            rm) record network-deleted ;;
            *) unexpected "$@" ;;
        esac
        exit 0 ;;
    run)
        [[ "$*" == *'mongo:7.0 --replSet rs0 --bind_ip_all'* ]] || unexpected "$@"
        while [ "$#" -gt 0 ]; do
            if [ "$1" = --name ]; then
                printf '%s\n' "$2" > "$IMAGE_SMOKE_TEST_DIR/mongo-name"
                break
            fi
            shift
        done
        [ -s "$IMAGE_SMOKE_TEST_DIR/mongo-name" ] || unexpected missing-mongo-name
        touch "$IMAGE_SMOKE_TEST_DIR/mongo-exists"
        record mongo-started
        echo fixture-container-id
        exit 0 ;;
    exec)
        [ "$2" = "$(cat "$IMAGE_SMOKE_TEST_DIR/mongo-name")" ] || unexpected "$@"
        [[ "$*" == *'db.runCommand({ ping: 1 }).ok'* ]] || unexpected "$@"
        [ -f "$IMAGE_SMOKE_TEST_DIR/mongo-exists" ] || unexpected exec-after-delete
        attempts=0
        if [ -f "$IMAGE_SMOKE_TEST_DIR/attempts" ]; then
            attempts="$(cat "$IMAGE_SMOKE_TEST_DIR/attempts")"
        fi
        attempts=$((attempts + 1))
        printf '%s\n' "$attempts" > "$IMAGE_SMOKE_TEST_DIR/attempts"
        record readiness-failed
        printf 'fixture-readiness-error-%s: MongoNetworkError: connect ECONNREFUSED 127.0.0.1:27017\n' "$attempts" >&2
        exit 17 ;;
    inspect|logs)
        [ "${!#}" = "$(cat "$IMAGE_SMOKE_TEST_DIR/mongo-name")" ] || unexpected "$@"
        # The control makes diagnostics unavailable after removal, just like the real fixture.
        [ -f "$IMAGE_SMOKE_TEST_DIR/mongo-exists" ] || unexpected diagnostics-after-delete
        if [ "$1" = inspect ]; then
            record inspect-before-delete
            printf '{"Status":"exited","Running":false,"ExitCode":42,"Error":"fixture startup failure"}\n'
        else
            record logs-before-delete
            tail_lines=""
            shift
            while [ "$#" -gt 0 ]; do
                case "$1" in
                    --tail) tail_lines="${2:-}"; shift ;;
                    --tail=*) tail_lines="${1#--tail=}" ;;
                esac
                shift
            done
            if [[ "$tail_lines" =~ ^[0-9]+$ ]] && [ "$tail_lines" -gt 0 ]; then
                record bounded-logs
            fi
            printf 'fixture-mongod-log-before-delete: fatal startup failure\n' >&2
        fi
        exit 0 ;;
    rm)
        [[ "$*" == *"$(cat "$IMAGE_SMOKE_TEST_DIR/mongo-name")"* ]] || unexpected "$@"
        record mongo-deleted
        rm -f "$IMAGE_SMOKE_TEST_DIR/mongo-exists"
        exit 0 ;;
    *) unexpected "$@" ;;
esac
STUB

cat > "$SCRATCH/bin/sleep" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
[ "$#" -eq 1 ] && [ "$1" = 1 ] || exit 99
# Skip wall-clock waiting while retaining every probe and its requested delay.
printf 'sleep-1\n' >> "$IMAGE_SMOKE_TEST_DIR/events"
STUB
chmod +x "$SCRATCH/bin/docker" "$SCRATCH/bin/sleep"

status=0
PATH="$SCRATCH/bin:$PATH" IMAGE_SMOKE_TEST_DIR="$SCRATCH" \
    bash "$HERE/image-smoke.sh" tapstate:fixture-diagnostics-test > "$SCRATCH/output" 2>&1 || status=$?

fail() {
    cat "$SCRATCH/output" >&2
    printf 'FAIL: %s\n' "$1" >&2
    exit 1
}

# Establish the reported path before asserting diagnostics: no build, server boot or setup failure.
[ "$status" -eq 2 ] || fail "smoke exited $status instead of the original startup failure code 2"
grep -Fq 'mongo did not accept connections' "$SCRATCH/output" || fail 'Mongo readiness failure was not reached'
if grep -q '^unexpected:' "$SCRATCH/events"; then
    cat "$SCRATCH/events" >&2
    fail 'the fake Docker boundary received an unexpected command'
fi
[ "$(cat "$SCRATCH/attempts")" -eq 60 ] || fail 'the original 60 readiness probes were not preserved'
[ "$(grep -c '^sleep-1$' "$SCRATCH/events")" -eq 60 ] || fail 'the original readiness delays were not preserved'
[ "$(grep -c '^mongo-started$' "$SCRATCH/events")" -eq 1 ] || fail 'the fixture was not started exactly once'
grep -qx mongo-deleted "$SCRATCH/events" || fail 'the fixture was not removed'
grep -qx network-deleted "$SCRATCH/events" || fail 'the fixture network was not removed'
[ ! -f "$SCRATCH/mongo-exists" ] || fail 'the fixture survived cleanup'

missing=()
if ! grep -Fq exited "$SCRATCH/output" || ! grep -Fq 42 "$SCRATCH/output"; then
    missing+=("container state/exit code")
fi
grep -Fq 'fixture-readiness-error-60: MongoNetworkError' "$SCRATCH/output" || missing+=("final readiness stderr")
grep -Fq 'fixture-mongod-log-before-delete' "$SCRATCH/output" || missing+=("Mongo logs")
grep -qx bounded-logs "$SCRATCH/events" || missing+=("bounded log capture")
deleted_line="$(grep -n '^mongo-deleted$' "$SCRATCH/events" | cut -d: -f1)"
for diagnostic in inspect-before-delete logs-before-delete; do
    diagnostic_line="$(grep -n "^$diagnostic$" "$SCRATCH/events" | cut -d: -f1 || true)"
    if [ -z "$diagnostic_line" ] || [ "$diagnostic_line" -ge "$deleted_line" ]; then
        missing+=("$diagnostic control")
    fi
done
if [ "${#missing[@]}" -gt 0 ]; then
    printf 'Observed: 60 failed probes, exit 2, fixture and network removed; Tapstate never started.\n' >&2
    fail "Mongo startup diagnostics missing before cleanup: ${missing[*]}"
fi
printf 'PASS: Mongo startup failure retains bounded diagnostics before deleting the fixture\n'
