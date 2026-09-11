#!/usr/bin/env bash
#
# One case, run on its own: a connector registration the server refused has to stop the quickstart
# where it was refused.
#
# What it reproduces. The quickstart drives every online verb through one piped REPL session and then
# reads that session's exit status:
#
#   repl_out="$(printf 'connect ...\nregister ../postgres-connector.jar\n...' | ./tapstate -w work 2>&1)" || repl_status=$?
#   ...
#   if [ "$repl_status" -ne 0 ]; then die "the CLI exited $repl_status before the pipeline was started; its output is above"; fi
#
# A REPL fed a script reports how the session ended, not whether every verb in it succeeded: a rejected
# verb prints its error and the loop reads the next line, so that status is 0 across a session in which
# nothing worked. The guard is not missing, it cannot see this class of failure at all. The run then
# continues into apply / discover-schema / start, and half a minute later reports an empty target and
# points at the server log -- for a failure that happened several steps earlier and was the server's
# only if the registration itself was.
#
# The fixture is built to that shape rather than to the odds of it. The stub CLI prints what was
# observed for a refused registration (`The server refused the connector registration.`) and then exits
# 0, which is what the real CLI does for any piped session -- a property of its read loop, not of any
# one refusal. The stub docker reports a healthy stack whose admin exists and a target with nothing in
# it, the shape a run reaches when the connectors were never registered. No real server is needed:
# what is under test is what the quickstart does with that output, and that output is a fixed sentence
# the CLI renders for any refusal it cannot read a code out of.
#
# What it asserts: the run must not go on to the row-count wait, must not report the refused
# registration as "the two engines were not assembled", and must not send the reader to the server log
# for a failure that was not the server's. It deliberately does not assert that the refusal names its
# own reason -- that depends on the response body the server answered with, which was never captured
# for the observed run, and it is a separate question from where the run stops.
#
# The stub CLI exits 0, which is the CLI's behaviour as it stands: this case holds the quickstart to
# recognising the failure in the output it already captures. A change to the CLI's session exit status
# would cover other scripted users too, and would still leave this case meaningful -- the quickstart
# would stop at the guard it already has instead of at the row count.
#
# Exit 0 iff the case passes.

set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
QUICKSTART_SH="$HERE/quickstart.sh"
INSTALL_SH="$REPO/install/install.sh"
for f in "$QUICKSTART_SH" "$INSTALL_SH" "$HERE/docker-compose.yml"; do
    [ -f "$f" ] || { printf 'missing %s\n' "$f" >&2; exit 2; }
done

WORK="$(mktemp -d)"
STUB="$(mktemp -d)"
SHIM="$(mktemp -d)"
DEMO="$WORK/tapstate-demo"
trap 'rm -rf "$WORK" "$STUB" "$SHIM"' EXIT INT TERM

# --- the release assets the script fetches, served over file:// -------------------------------------
# The real install.sh (the platform gate reuses its detection), the real compose file and the real seed
# SQL, so the run walks the paths it walks for a user. The connector jars are fetched from here too --
# this case never reaches a server, so their contents are what the script only downloads and uploads.
mkdir -p "$STUB/install" "$STUB/deploy/quickstart/mysql-init" \
         "$STUB/deploy/quickstart/postgres-init" "$STUB/connectors-preview"
cp "$INSTALL_SH"                    "$STUB/install/install.sh"
cp "$HERE/docker-compose.yml"       "$STUB/deploy/quickstart/docker-compose.yml"
cp "$HERE/mysql-init/01-orders.sql" "$STUB/deploy/quickstart/mysql-init/01-orders.sql"
cp "$HERE/postgres-init/01-shipments.sql" "$STUB/deploy/quickstart/postgres-init/01-shipments.sql"
for jar in mysql mongodb postgres; do
    printf 'not a real connector jar\n' > "$STUB/connectors-preview/$jar-connector.jar"
done

# --- the demo directory, with the CLI already in place ------------------------------------------------
# The CLI is placed rather than installed: the release payload it would come from is not what this case
# is about, and skipping the install keeps the run free of the installer's own network behaviour.
mkdir -p "$DEMO"
cp "$QUICKSTART_SH" "$DEMO/quickstart.sh"
cat > "$DEMO/tapstate" <<'CLI'
#!/bin/sh
# Stands in for the real CLI in a piped session: it reads the scripted session off stdin, prints what
# the verbs did, and exits 0. The status is the point -- an interactive session does not end because one
# command was rejected, so the real one answers 0 here too, whatever the verbs said.
cat >/dev/null
cat <<'OUT'
uploading postgres-connector.jar (20.0 MB)
  The server refused the connector registration.

error: connector.not-registered
  No connector 'postgres' is registered.
  Register the connector's artifact -- or place it in the connector seed directory -- before using it.

order_pipeline  running
OUT
exit 0
CLI
chmod +x "$DEMO/tapstate"

# --- the machine around it ---------------------------------------------------------------------------
# The platform the gate detects is a supported one, so the run reaches Docker. The stack answers as a
# healthy one: the server reports Health, the one-shot bootstrap reports State and ExitCode (a one-shot
# container never reports Health), and every count read out of the store is 0 -- the target of a run
# whose connectors were never registered.
printf '#!/bin/sh\ncase "$1" in -s) echo Linux ;; -m) echo x86_64 ;; *) echo unknown ;; esac\n' > "$SHIM/uname"
cat > "$SHIM/docker" <<'DOCK'
#!/bin/sh
case " $* " in
    *" logs "*)      echo 'bootstrap: first admin created'; exit 0 ;;
    *" bootstrap "*) echo '{"State":"exited","ExitCode":0}'; exit 0 ;;
esac
for a in "$@"; do
    [ "$a" = ps ]   && { echo '{"Health":"healthy"}'; exit 0; }
    [ "$a" = exec ] && { echo 0; exit 0; }
done
exit 0
DOCK
# The row-count wait polls 30 times at two seconds apiece. How long it waits is not what is under test
# -- only whether it is reached at all -- and a case that spends a minute proving it is reached is a
# case nobody runs.
printf '#!/bin/sh\nexit 0\n' > "$SHIM/sleep"
chmod +x "$SHIM/uname" "$SHIM/docker" "$SHIM/sleep"

# --- run ---------------------------------------------------------------------------------------------
# The telemetry endpoint is pointed at a port nothing listens on: the installer's default is the
# production one, and a case that runs it must not post an install into that count.
OUT="$(cd "$DEMO" && PATH="$SHIM:$PATH" \
    TAPSTATE_QUICKSTART_BASE_URL="file://$STUB" \
    TAPSTATE_CONNECTORS_URL="file://$STUB/connectors-preview" \
    TAPSTATE_TELEMETRY_URL="http://127.0.0.1:1/e" \
    TAPSTATE_QUICKSTART_POLL_SECONDS=0 \
    sh "$DEMO/quickstart.sh" 2>&1)"; RC=$?

fail() {
    printf 'FAIL  %s\n' "$1" >&2
    printf '\n--- the run said (exit %s) ---\n%s\n------------------------------\n' "$RC" "$OUT" >&2
    exit 1
}

[ "$RC" -ne 0 ] \
    || fail "a run whose connector registration was refused was reported as a success (exit 0)"
printf '%s\n' "$OUT" | grep -q 'waiting for the two engines to be assembled' \
    && fail "the run went on to the row-count wait after the registration was refused"
printf '%s\n' "$OUT" | grep -q 'were not assembled' \
    && fail "the refused registration was reported as an empty target (the two engines were not assembled)"
printf '%s\n' "$OUT" | grep -q 'inspect it with: docker compose logs server' \
    && fail "the refusal sent the reader to the server log, for a failure that was not the server's"

printf 'PASS  a refused connector registration stops the quickstart where it was refused\n'
