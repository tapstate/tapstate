#!/usr/bin/env bash
#
# Test harness for deploy/quickstart/quickstart.sh. Black-box, against local file:// stubs, with no
# network and no Docker. It covers the platform gate (reused from install.sh), the prepare phase (fetch
# the compose file, the CLI, and the connector jars; generate .env and the demo work/), the demo
# workspace itself, and idempotency. The live run phase (docker compose up + the online verbs) is out of
# scope here -- the live end-to-end test covers it -- so the script runs with TAPSTATE_QUICKSTART_PREPARE_ONLY=1,
# which stops before Docker. A fake `uname` (and, for the musl case, a fake `ldd`) placed first on PATH
# drives the platform each run sees. Exit 0 iff every check passes.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"          # deploy/quickstart -> repo root
QUICKSTART_SH="$HERE/quickstart.sh"
# Read the version off the script under test rather than restating it, so the release stub always serves
# exactly what the script will ask for and the two can never drift.
VERSION="$(sed -n 's/^CLI_VERSION="\(.*\)"$/\1/p' "$QUICKSTART_SH")"
[ -n "$VERSION" ] || { printf 'cannot read CLI_VERSION from %s\n' "$QUICKSTART_SH" >&2; exit 1; }

PASS=0; FAIL=0
ok()  { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }
sha256_of() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"; else shasum -a 256 "$1"; fi; }

# --- CLI release stub (what install.sh fetches): download/v<ver>/tapstate-<ver>-<platform>.tar.gz ----
CLI_STUB="$(mktemp -d)"
make_cli() {   # $1 = platform; the fake binary just echoes, so a run phase (if ever reached) won't hang
  d="$CLI_STUB/download/v$VERSION"; mkdir -p "$d"; stage="$(mktemp -d)"
  mkdir -p "$stage/tapstate-cli-$VERSION/bin" "$stage/tapstate-cli-$VERSION/libexec"
  printf '#!/bin/sh\necho "tapstate %s %s"\n' "$VERSION" "$1" > "$stage/tapstate-cli-$VERSION/bin/tapstate"
  chmod +x "$stage/tapstate-cli-$VERSION/bin/tapstate"
  printf '#!/bin/sh\necho mcp\n' > "$stage/tapstate-cli-$VERSION/libexec/tapstate-mcp"
  chmod +x "$stage/tapstate-cli-$VERSION/libexec/tapstate-mcp"
  echo license > "$stage/LICENSE"; echo notice > "$stage/NOTICE"
  a="tapstate-$VERSION-$1.tar.gz"; tar -czf "$d/$a" -C "$stage" "tapstate-cli-$VERSION" LICENSE NOTICE
  ( cd "$d" && sha256_of "$a" > "$a.sha256" ); rm -rf "$stage"
}
for p in darwin-arm64 darwin-x64 linux-x64 linux-arm64; do make_cli "$p"; done

# --- quickstart asset stub: the REAL repo files (install.sh, compose, mysql-init) + fake conn jars ---
# Serving the real files means the smoke also proves the script fetches assets that actually exist at the
# paths it expects, and that the generated demo matches the real seed schema.
QS_STUB="$(mktemp -d)"
mkdir -p "$QS_STUB/install" "$QS_STUB/deploy/quickstart/mysql-init" \
         "$QS_STUB/deploy/quickstart/postgres-init" "$QS_STUB/connectors-preview"
cp "$REPO/install/install.sh"                              "$QS_STUB/install/install.sh"
cp "$REPO/deploy/quickstart/docker-compose.yml"           "$QS_STUB/deploy/quickstart/docker-compose.yml"
cp "$REPO/deploy/quickstart/mysql-init/01-orders.sql"     "$QS_STUB/deploy/quickstart/mysql-init/01-orders.sql"
cp "$REPO/deploy/quickstart/postgres-init/01-shipments.sql" "$QS_STUB/deploy/quickstart/postgres-init/01-shipments.sql"
printf 'fake-mysql-connector-jar\n'   > "$QS_STUB/connectors-preview/mysql-connector.jar"
printf 'fake-mongodb-connector-jar\n' > "$QS_STUB/connectors-preview/mongodb-connector.jar"
printf 'fake-postgres-connector-jar\n' > "$QS_STUB/connectors-preview/postgres-connector.jar"

trap 'rm -rf "$CLI_STUB" "$QS_STUB"' EXIT

# Set to an executable to place a fake `curl` first on PATH for the next run_prepare, so a test can
# make a transfer die the way a dropped link does. Empty means the real curl runs.
CURL_SHIM=""

# Run quickstart.sh's prepare phase in a demo dir with a faked platform.
#   run_prepare OS ARCH MUSL(glibc|musl) [REUSE_DEMO_DIR]
# Sets RC, OUT, DEMO. With REUSE_DEMO_DIR the same directory is reused (for the idempotency check).
run_prepare() {
  local fos="$1" farch="$2" fmusl="$3" reuse="${4:-}" shim
  if [ -n "$reuse" ]; then
    DEMO="$reuse"
  else
    DEMO="$(mktemp -d)/tapstate-demo"; mkdir -p "$DEMO"; cp "$QUICKSTART_SH" "$DEMO/quickstart.sh"
  fi
  shim="$(mktemp -d)"
  cat > "$shim/uname" <<EOF
#!/bin/sh
case "\$1" in -s) echo "$fos" ;; -m) echo "$farch" ;; *) echo unknown ;; esac
EOF
  chmod +x "$shim/uname"
  if [ "$fmusl" = musl ]; then
    printf '#!/bin/sh\necho "musl libc (x86_64)"\n' > "$shim/ldd"; chmod +x "$shim/ldd"
  fi
  # A recording xattr, so a test can prove the quarantine strip fires on macOS and nowhere else.
  cat > "$shim/xattr" <<EOF
#!/bin/sh
echo "\$*" >> "$DEMO/.xattr-calls"
EOF
  chmod +x "$shim/xattr"
  [ -z "$CURL_SHIM" ] || { cp "$CURL_SHIM" "$shim/curl"; chmod +x "$shim/curl"; }
  OUT="$(cd "$DEMO" && PATH="$shim:$PATH" \
    TAPSTATE_VERSION="${PIN_VERSION-$VERSION}" \
    TAPSTATE_BASE_URL="file://$CLI_STUB" \
    TAPSTATE_QUICKSTART_BASE_URL="file://$QS_STUB" \
    TAPSTATE_CONNECTORS_URL="file://$QS_STUB/connectors-preview" \
    TAPSTATE_QUICKSTART_PREPARE_ONLY=1 \
    sh "$DEMO/quickstart.sh" 2>&1)"
  RC=$?
  rm -rf "$shim"
}

printf '\033[1mquickstart smoke — %s\033[0m\n' "$QUICKSTART_SH"

# --- platform gate: unsupported platforms fail before any fetch, demo dir left pristine --------------
# The gate reuses install.sh --print-platform. A refusal must exit non-zero, point the user elsewhere,
# and leave the demo directory holding nothing but the quickstart.sh they downloaded -- zero side effects.
neg_gate() {   # OS ARCH MUSL grep-for label
  run_prepare "$1" "$2" "$3"
  local residue; residue="$(find "$DEMO" -mindepth 1 ! -name quickstart.sh 2>/dev/null)"
  if [ "$RC" -ne 0 ] && printf '%s' "$OUT" | grep -qiE "$4" && [ -z "$residue" ]; then
    ok "$5"
  else
    bad "$5 (rc=$RC, residue='$residue'): $OUT"
  fi
}
neg_gate "MINGW64_NT-10.0" x86_64  glibc 'wsl|source'         "gate refuses Git Bash / MinGW, demo dir pristine"
neg_gate "Linux"           x86_64  musl  'musl'               "gate refuses musl libc, demo dir pristine"
neg_gate "Linux"           riscv64 glibc 'architecture|riscv' "gate refuses an unknown arch, demo dir pristine"

# --- prepare phase: a supported platform downloads everything the demo needs, with no build ----------
run_prepare Linux x86_64 glibc
PREP="$DEMO"
if [ "$RC" -eq 0 ]; then ok "prepare on a supported platform exits 0"; else bad "prepare exits 0 (rc=$RC): $OUT"; fi
have() { if [ -e "$PREP/$1" ]; then ok "$2"; else bad "$2 — missing $1"; fi; }
if [ -x "$PREP/tapstate" ]; then ok "installs the CLI in place as ./tapstate"; else bad "./tapstate not installed/executable: $OUT"; fi
have docker-compose.yml               "fetches the compose file into the demo dir"
have mysql-init/01-orders.sql         "fetches the demo seed SQL"
# Both seed dirs, because compose mounts both. A missing mount source is not an error Docker reports:
# it creates an empty directory and starts a database with no demo data, which surfaces much later as
# a pipeline that reads nothing.
have postgres-init/01-shipments.sql   "fetches the second engine's seed SQL"
have mysql-connector.jar              "fetches the mysql connector jar"
have mongodb-connector.jar            "fetches the mongodb connector jar"
# The connector seed dir must exist (compose bind-mounts it) but stay empty: registration goes through
# the CLI upload path, and an empty seed dir is the documented expected case.
if [ -d "$PREP/connectors" ] && [ -z "$(ls -A "$PREP/connectors" 2>/dev/null)" ]; then
  ok "creates an empty connectors/ seed dir (registration is via the CLI, not the seed)"
else
  bad "connectors/ seed dir missing or non-empty: $(ls -A "$PREP/connectors" 2>/dev/null)"
fi

# --- the pinned CLI version: it must resolve with TAPSTATE_VERSION unset, and match the build ---------
# Regression guard for a real defect. Every other check here passes TAPSTATE_VERSION explicitly, so the
# default path -- the only one a clean-machine user takes -- was never exercised. install.sh's default
# resolves the /releases/latest redirect, and GitHub fills that from full releases only; the CLI ships as
# a prerelease, so the lookup returned nothing and install.sh refused, stranding the quickstart at the
# CLI step. quickstart.sh now carries its own pin; an empty TAPSTATE_VERSION exercises that fallback
# exactly as an unset one does (both scripts test with :- / -n).
POM_VERSION="$(sed -n 's/.*<revision>\(.*\)<\/revision>.*/\1/p' "$REPO/pom.xml" | head -1)"
if [ "$VERSION" = "$POM_VERSION" ]; then
  ok "the pinned CLI version matches the build ($VERSION)"
else
  bad "pinned CLI version $VERSION does not match pom.xml revision $POM_VERSION — bump quickstart.sh"
fi

# run_prepare writes the shared RC/OUT/DEMO, and later checks still read the first run's; save and
# restore them so this extra run stays invisible to them.
saved_rc="$RC"; saved_out="$OUT"; saved_demo="$DEMO"
PIN_VERSION="" run_prepare Linux x86_64 glibc
if [ "$RC" -eq 0 ] && [ -x "$DEMO/tapstate" ]; then
  ok "with TAPSTATE_VERSION unset the CLI still installs, from the script's own pin"
else
  bad "with TAPSTATE_VERSION unset the CLI did not install (rc=$RC): $OUT"
fi
unset PIN_VERSION
RC="$saved_rc"; OUT="$saved_out"; DEMO="$saved_demo"

# --- .env: a random admin password, saved with tight perms and announced once ------------------------
# Replaces the shipped admin/admin default so a demo left running is not trivially reachable, and the
# password lives only in a user-readable-only file plus a single line of output -- never a CLI argument
# or shell history.
env_pw="$(sed -n 's/^TAPSTATE_ADMIN_PASSWORD=//p' "$PREP/.env" 2>/dev/null || true)"
if [ -n "$env_pw" ] && [ "$env_pw" != admin ] && [ "${#env_pw}" -ge 16 ]; then
  ok "generates a random admin password in .env (not the shipped default)"
else
  bad ".env password weak or absent: '$env_pw'"
fi
# ls -l is the portable way to read the mode string here (macOS find has no -printf).
# shellcheck disable=SC2012
perms="$(ls -l "$PREP/.env" 2>/dev/null | awk '{print $1}')"
case "$perms" in
  -rw-------*) ok "writes .env readable only by the user (600)" ;;
  *)          bad ".env perms = '$perms', want -rw-------" ;;
esac
count="$(printf '%s\n' "$OUT" | grep -Fc "$env_pw" || true)"
if [ "$count" = 1 ]; then ok "announces the password exactly once"; else bad "password printed $count times (want 1)"; fi
# two fresh runs get different passwords -- a randomness proxy that a fixed or predictable value fails
run_prepare Darwin arm64 glibc; pw_a="$(sed -n 's/^TAPSTATE_ADMIN_PASSWORD=//p' "$DEMO/.env" 2>/dev/null || true)"
run_prepare Darwin arm64 glibc; pw_b="$(sed -n 's/^TAPSTATE_ADMIN_PASSWORD=//p' "$DEMO/.env" 2>/dev/null || true)"
if [ -n "$pw_a" ] && [ "$pw_a" != "$pw_b" ]; then ok "each fresh run gets a different password"; else bad "passwords not distinct: '$pw_a' vs '$pw_b'"; fi

# --- the demo workspace is generated, uses in-network addresses, and honours the CEL constraint ------
WORK="$PREP/work"
have work/source/orders_db.tap.yml           "generates the first engine's source resource"
# The second engine's half. Its seed has been fetched and mounted since the compose file grew a
# postgres service, but nothing read it: the demo generated a mysql source, a mongo target and one
# pipeline. A seeded database no resource names is indistinguishable from one that is not there.
have work/source/fulfillment_db.tap.yml      "generates the second engine's source resource"
# One pipeline over both, not one per engine. Two pipelines put each engine's rows in a collection of
# its own, which is two syncs standing next to each other; the object this demo is about only exists
# because a single pipeline reads both.
have work/pipeline/order_pipeline.tap.yml    "generates one pipeline over both engines"
pcount="$(find "$PREP/work/pipeline" -name '*.tap.yml' 2>/dev/null | wc -l | tr -d ' ')"
if [ "$pcount" = 1 ]; then
  ok "generates exactly one pipeline, so the demo has one collection to look at"
else
  bad "generated $pcount pipelines (want 1): $(find "$PREP/work/pipeline" -name '*.tap.yml' 2>/dev/null)"
fi
# The managed store is deliberately NOT here. It is the deployment's, registered by the server at
# startup, and a demo that shipped it as a file to apply would be teaching the opposite -- that the
# store is one more thing an author owns and has to hand back.
if [ ! -e "$WORK/source/views.tap.yml" ]; then
  ok "does not generate the managed store: the deployment provides it"
else
  bad "the managed store is still generated as a workspace file: $(cat "$WORK/source/views.tap.yml")"
fi
# Addresses use compose service names: the connector runs inside the server container, so 127.0.0.1
# would point the server at itself, not at the databases.
if grep -q 'host: mysql' "$WORK/source/orders_db.tap.yml" 2>/dev/null && ! grep -q '127.0.0.1' "$WORK/source/orders_db.tap.yml" 2>/dev/null; then
  ok "source addresses mysql by its compose service name, not loopback"
else
  bad "source is not addressed by service name: $(cat "$WORK/source/orders_db.tap.yml" 2>/dev/null)"
fi
# The store the server seeds is derived from its own store URI, so that URI has to be reachable from
# inside the container for the derived one to be too. Checked on the compose file, which is where it is
# set: loopback here would point the server at itself and the derived views URI would inherit exactly
# that mistake, one indirection further from anyone looking for it.
if grep -q 'TAPSTATE_STORE_MONGO_URI:.*mongo:27017' "$PREP/docker-compose.yml" 2>/dev/null \
   && ! grep -q 'TAPSTATE_STORE_MONGO_URI:.*127.0.0.1' "$PREP/docker-compose.yml" 2>/dev/null; then
  ok "the server's store URI addresses mongo by its compose service name, not loopback"
else
  bad "server store URI is not addressed by service name: $(grep TAPSTATE_STORE_MONGO_URI "$PREP/docker-compose.yml" 2>/dev/null)"
fi
stale="$(grep -rl 'warehouse' "$WORK" 2>/dev/null || true)"
if [ -z "$stale" ]; then
  ok "no resource in the generated workspace still says warehouse"
else
  bad "warehouse survives the rename in: $stale"
fi
vcount="$(cat "$WORK"/source/*.tap.yml "$WORK"/pipeline/*.tap.yml 2>/dev/null | grep -c '^version: tapstate/v1' || true)"
if [ "$vcount" = 3 ]; then ok "all three generated resources declare version: tapstate/v1"; else bad "version lines = $vcount (want 3)"; fi

# The second source is addressed the same way the first is, and it reads changes rather than only a
# snapshot. Both matter to the live check this demo exists to make possible: a row inserted by hand
# after the stack is up only crosses if the tail is running.
if grep -q 'host: postgres' "$WORK/source/fulfillment_db.tap.yml" 2>/dev/null \
   && ! grep -q '127.0.0.1' "$WORK/source/fulfillment_db.tap.yml" 2>/dev/null; then
  ok "the second source addresses postgres by its compose service name, not loopback"
else
  bad "second source is not addressed by service name: $(cat "$WORK/source/fulfillment_db.tap.yml" 2>/dev/null)"
fi
# Both streams carry changes, not just one. With the two engines assembled into a single object, a
# source left on snapshot contributes its seeded rows and then goes quiet - and the object still looks
# complete, because the other side keeps it moving. Which side went quiet is invisible from outside.
if grep -q 'mode: cdc' "$WORK/source/orders_db.tap.yml" 2>/dev/null \
   && grep -q 'mode: cdc' "$WORK/source/fulfillment_db.tap.yml" 2>/dev/null \
   && grep -q 'read_mode: snapshot_and_cdc' "$WORK/pipeline/order_pipeline.tap.yml" 2>/dev/null; then
  ok "both engines read changes, not only a snapshot"
else
  bad "one of the two halves is snapshot-only, so a hand-inserted row would never cross"
fi
# HARD CONSTRAINT: the decimal `amount` column cannot pass through a CEL expression in this preview, so
# the demo pipeline must never name it -- it only ever passes through untouched.
if ! grep -q 'amount' "$WORK/pipeline/order_pipeline.tap.yml" 2>/dev/null \
   && ! grep -qE '=[^#]*after\.(id|amount)' "$WORK/pipeline/order_pipeline.tap.yml" 2>/dev/null; then
  ok "the decimal amount column never appears in the pipeline (no numeric column in any CEL)"
else
  bad "a numeric column leaked into the pipeline: $(grep -nE 'amount|after\.(id|amount)' "$WORK/pipeline/order_pipeline.tap.yml" 2>/dev/null)"
fi

# --- the shape the demo exists to show: two engines assembled into one object ------------------------
# Each of these is a way the pipeline could still apply, still move rows, and no longer be the demo.
PIPE="$WORK/pipeline/order_pipeline.tap.yml"
if grep -qE '^source: *\[ *orders_db *, *fulfillment_db *\]' "$PIPE" 2>/dev/null; then
  ok "one pipeline reads both engines, named in a single source list"
else
  bad "the pipeline does not read both engines: $(grep -n '^source:' "$PIPE" 2>/dev/null)"
fi
if grep -q 'type: nest' "$PIPE" 2>/dev/null && grep -q 'path: shipments' "$PIPE" 2>/dev/null; then
  ok "the second engine's rows are placed inside the first engine's object, not beside it"
else
  bad "no nest placing shipments under the order: $(cat "$PIPE" 2>/dev/null)"
fi
# arrayKey is what identifies an element already in the array. Without it an updated shipment appends a
# second copy instead of moving the one it belongs to -- which the demo's own CDC walk would show, and
# which no assertion about the pipeline applying could see.
if grep -q 'arrayKey' "$PIPE" 2>/dev/null; then
  ok "array elements are identified, so a changed shipment moves rather than duplicates"
else
  bad "the embed carries no arrayKey: $(grep -n 'embed' -A 2 "$PIPE" 2>/dev/null)"
fi
# No serve block, and a view instead. A serve block would write the result into somebody else's system;
# declaring a view is the instruction to materialize it in the store the deployment already runs. The
# demo's whole "no second client, no URI to configure" claim rests on which of the two is here.
if grep -q '^view:' "$PIPE" 2>/dev/null && ! grep -q '^serve:' "$PIPE" 2>/dev/null; then
  ok "the pipeline declares a view and names no target: materializing is the declaration"
else
  bad "the pipeline still names a target instead of declaring a view: $(cat "$PIPE" 2>/dev/null)"
fi
# The materialized document has to say what identifies it. Missing, the run is refused at actuation --
# well after validate has passed, which is exactly where a generated workspace must not send a reader.
if grep -q 'primary_key:' "$PIPE" 2>/dev/null; then
  ok "the view says what identifies a document, so materializing is not refused later"
else
  bad "the view carries no primary_key: $(cat "$PIPE" 2>/dev/null)"
fi

# --- idempotency: a re-run keeps generated state and does not re-download verified assets ------------
run_prepare Linux x86_64 glibc; RE="$DEMO"
pw_before="$(sed -n 's/^TAPSTATE_ADMIN_PASSWORD=//p' "$RE/.env")"
printf '# user-marker\n' >> "$RE/docker-compose.yml"   # a marker a re-download would erase
printf 'user-edit\n' > "$RE/work/marker"               # a user edit to the workspace
run_prepare Linux x86_64 glibc "$RE"                   # re-run over the same dir
pw_after="$(sed -n 's/^TAPSTATE_ADMIN_PASSWORD=//p' "$RE/.env")"
if [ "$RC" -eq 0 ] \
   && [ -n "$pw_before" ] && [ "$pw_before" = "$pw_after" ] \
   && [ -f "$RE/work/marker" ] \
   && grep -q '# user-marker' "$RE/docker-compose.yml"; then
  ok "a re-run keeps the password and workspace, and does not re-download assets"
else
  bad "re-run not idempotent (rc=$RC, pw_same=$([ "$pw_before" = "$pw_after" ] && echo y || echo n), work_marker=$([ -f "$RE/work/marker" ] && echo y || echo n), compose_marker=$(grep -q '# user-marker' "$RE/docker-compose.yml" && echo y || echo n))"
fi

# --- macOS quarantine: the strip fires on Darwin, and only there -------------------------------------
run_prepare Darwin arm64 glibc; DARWIN="$DEMO"
if [ -f "$DARWIN/.xattr-calls" ] && grep -q 'com.apple.quarantine' "$DARWIN/.xattr-calls" && grep -q 'tapstate' "$DARWIN/.xattr-calls"; then
  ok "strips the macOS quarantine attribute from the CLI on Darwin"
else
  bad "quarantine not stripped on Darwin: $(cat "$DARWIN/.xattr-calls" 2>/dev/null)"
fi
run_prepare Linux x86_64 glibc; LINUX="$DEMO"
if [ ! -f "$LINUX/.xattr-calls" ]; then
  ok "does not touch xattr on Linux (the strip is macOS-only)"
else
  bad "xattr called on Linux: $(cat "$LINUX/.xattr-calls" 2>/dev/null)"
fi

# --- run phase (fakes): the live stack is the end-to-end test's to prove; here fakes pin what must not -
# regress -- the password reaches the CLI over stdin (never an argument), the online verbs are driven,
# and teardown is printed. A recording ./tapstate replaces the stub after prepare; a fake docker reports
# the server healthy so the wait ends. curl stays real, so the gate's install.sh fetch still works.
run_phase_fakes() {
  run_prepare Linux x86_64 glibc            # prepare a dir (installs the stub CLI)
  RUN="$DEMO"
  RUN_PW="$(sed -n 's/^TAPSTATE_ADMIN_PASSWORD=//p' "$RUN/.env")"
  cat > "$RUN/tapstate" <<'CLI'
#!/bin/sh
printf '%s\n' "$*" >> .cli-argv
cat >> .cli-stdin
# A REPL prints its verbs' errors and still exits 0 -- an interactive session does not end because one
# command was rejected. FAKE_CLI_OUT lets a case reproduce that shape: output that says it failed, over
# an exit status that says it did not.
[ -n "${FAKE_CLI_OUT:-}" ] && printf '%s\n' "$FAKE_CLI_OUT"
exit 0
CLI
  chmod +x "$RUN/tapstate"
  local shim; shim="$(mktemp -d)"
  # the $1 is the fake uname's own argument -- it must stay literal.
  # shellcheck disable=SC2016
  printf '#!/bin/sh\ncase "$1" in -s) echo Linux ;; -m) echo x86_64 ;; *) echo unknown ;; esac\n' > "$shim/uname"
  cat > "$shim/docker" <<'DOCK'
#!/bin/sh
# `compose ps ... server` -> report healthy so the wait loop ends; `compose ps -a ... bootstrap` -> report
# the one-shot admin-creation container in whatever state the case asked for; `compose exec ... mongosh`
# (the snapshot count read) -> report the row count the case asked for, so a run that delivers and a run
# that delivers nothing can both be driven; every other subcommand no-ops.
#
# The two `ps` answers are deliberately different shapes. A server reports Health; a one-shot container
# reports State and ExitCode and never reports Health at all. A script that waited on the wrong one would
# read a field the other never publishes, which is exactly the confusion these fakes have to be able to
# expose rather than paper over.
# A brace inside ${VAR:-default} would close the expansion, so the default is set on its own line.
bs="${FAKE_BOOTSTRAP_PS:-}"
[ -n "$bs" ] || bs='{"State":"exited","ExitCode":0}'
# Which container is being asked about is decided before the subcommand is, because `ps` appears in the
# bootstrap query too -- answering on the subcommand alone would hand the server's Health line back for
# every query and quietly make the two indistinguishable.
# `logs` is answered before the container is looked at, because the bootstrap log query names the
# container too -- deciding on the container alone would hand a ps line back to a log query.
case " $* " in *" logs "*) echo "${FAKE_BOOTSTRAP_LOG:-bootstrap: first admin created}"; exit 0 ;; esac
case " $* " in *" bootstrap "*) echo "$bs"; exit 0 ;; esac
# The count is answered per collection rather than once for all of them. That is what lets a case drive
# "the first engine arrived and the second did not" - the shape a demo wired to only one of its two
# sources actually produces, and the one a check on a single total cannot tell apart from success.
for a in "$@"; do
  [ "$a" = ps ] && { echo '{"Health":"healthy"}'; exit 0; }
  [ "$a" = exec ] && {
    case "$*" in
      *shipments*) echo "${FAKE_SHIPMENT_ROWS:-6}" ;;
      *)           echo "${FAKE_TARGET_ROWS:-5}" ;;
    esac
    exit 0
  }
done
exit 0
DOCK
  chmod +x "$shim/uname" "$shim/docker"
  RUN_OUT="$(cd "${RUN_CWD:-$RUN}" && PATH="$shim:$PATH" \
    TAPSTATE_VERSION="$VERSION" TAPSTATE_BASE_URL="file://$CLI_STUB" \
    TAPSTATE_QUICKSTART_BASE_URL="file://$QS_STUB" TAPSTATE_CONNECTORS_URL="file://$QS_STUB/connectors-preview" \
    FAKE_TARGET_ROWS="${1:-5}" FAKE_SHIPMENT_ROWS="${2:-6}" \
    FAKE_BOOTSTRAP_PS="${FAKE_BOOTSTRAP_PS:-}" FAKE_CLI_OUT="${FAKE_CLI_OUT:-}" \
    FAKE_BOOTSTRAP_LOG="${FAKE_BOOTSTRAP_LOG:-}" \
    TAPSTATE_QUICKSTART_POLL_SECONDS=0 \
    sh "$RUN/quickstart.sh" 2>&1)"; RUN_RC=$?
  rm -rf "$shim"
}
run_phase_fakes
if [ "$RUN_RC" -eq 0 ] && [ -f "$RUN/.cli-argv" ] && ! grep -Fq "$RUN_PW" "$RUN/.cli-argv" && grep -Fq "$RUN_PW" "$RUN/.cli-stdin"; then
  ok "the admin password reaches the CLI over stdin, never as a command argument"
else
  bad "password handling (rc=$RUN_RC, argv=$(grep -Fq "$RUN_PW" "$RUN/.cli-argv" 2>/dev/null && echo LEAK || echo ok), stdin=$(grep -Fq "$RUN_PW" "$RUN/.cli-stdin" 2>/dev/null && echo ok || echo MISSING)): $RUN_OUT"
fi
# Failure branches print the driven command stream to say what went wrong -- and that stream contains
# the admin password by design (the assertion above requires it there). Print it redacted, always.
redacted_stdin() { sed "s/$RUN_PW/<redacted>/g" "$RUN/.cli-stdin" 2>/dev/null; }
# The second engine is driven as far as the first is. It no longer has a pipeline of its own -- there
# is one pipeline over both -- so what is left to check is its connector and its discovery, and the
# ordering check below then holds it to the same sequence as the first.
if grep -q 'register \.\./postgres-connector.jar' "$RUN/.cli-stdin" 2>/dev/null \
   && grep -q 'discover-schema fulfillment_db' "$RUN/.cli-stdin" 2>/dev/null \
   && grep -q 'apply source/fulfillment_db.tap.yml' "$RUN/.cli-stdin" 2>/dev/null; then
  ok "drives the second engine's connector, source and discovery too"
else
  bad "the second engine is not driven: $(redacted_stdin)"
fi
if grep -q 'register \.\./mysql-connector.jar' "$RUN/.cli-stdin" 2>/dev/null && grep -q '^apply' "$RUN/.cli-stdin" 2>/dev/null && grep -q 'start order_pipeline' "$RUN/.cli-stdin" 2>/dev/null; then
  ok "drives register / apply / start through the REPL"
else
  bad "online verbs not driven: $(redacted_stdin)"
fi
# Exactly one start. Two would mean the workspace grew a second pipeline again, which is the shape this
# demo moved away from -- and a stream that starts one pipeline and silently leaves another stopped
# looks identical from here to one that has only ever had one.
starts="$(grep -c '^start ' "$RUN/.cli-stdin" 2>/dev/null || true)"
if [ "$starts" = 1 ]; then
  ok "starts one pipeline, because there is one"
else
  bad "started $starts pipelines (want 1): $(grep '^start ' "$RUN/.cli-stdin" 2>/dev/null)"
fi
# The order, not just the presence. Each pipeline maps a row field, which the server refuses to apply
# until the source it reads has a discovered schema -- while the discovery needs the source applied.
# So the stream must apply the source alone, discover it, and only then apply the workspace. Presence
# checks matched the old broken stream just as happily; only the line order pins the fix.
#
# Both engines are held to it, separately. The second engine's pipeline maps a row field exactly like
# the first's, so it is under the same rule -- and checking only the first would pass a stream that
# discovers fulfillment_db after the workspace apply, which is the same defect wearing the other engine.
check_apply_order() {   # $1 = source id
  _src="$(grep -n "^apply source/$1.tap.yml\$" "$RUN/.cli-stdin" 2>/dev/null | head -1 | cut -d: -f1)"
  _disc="$(grep -n "^discover-schema $1\$" "$RUN/.cli-stdin" 2>/dev/null | head -1 | cut -d: -f1)"
  _full="$(grep -n '^apply$' "$RUN/.cli-stdin" 2>/dev/null | head -1 | cut -d: -f1)"
  if [ -n "$_src" ] && [ -n "$_disc" ] && [ -n "$_full" ] \
      && [ "$_src" -lt "$_disc" ] && [ "$_disc" -lt "$_full" ]; then
    ok "applies $1 alone, discovers it, then applies the workspace -- in that order"
  else
    bad "apply/discover ordering for $1 (source-apply=$_src discover=$_disc full-apply=$_full): $(redacted_stdin)"
  fi
}
check_apply_order orders_db
check_apply_order fulfillment_db
if printf '%s' "$RUN_OUT" | grep -q 'down -v' && printf '%s' "$RUN_OUT" | grep -qi 'images remain'; then
  ok "prints teardown on completion (down -v, images noted)"
else
  bad "no teardown printed: $RUN_OUT"
fi
# Run in place -- a saved script executed where it sits -- the directory is the user's and holds their
# files. Offering `rm -rf` on it is a command that destroys unrelated work, printed to someone who has
# just been told everything above was safe to copy. This run is exactly that shape.
if ! printf '%s' "$RUN_OUT" | grep -q 'rm -rf'; then
  ok "a directory the quickstart did not create is never offered for removal"
else
  bad "offered to rm -rf a directory it does not own: $(printf '%s' "$RUN_OUT" | grep 'rm -rf')"
fi
if printf '%s' "$RUN_OUT" | grep -q 'this directory is yours'; then
  ok "and says so, rather than leaving the teardown looking incomplete"
else
  bad "silently omitted the removal line instead of saying why: $RUN_OUT"
fi

# The other half, or the pair proves nothing: a script that never offers removal would pass both
# assertions above. Run from an empty directory, the quickstart makes tapstate-demo, owns it, and there
# the whole-directory removal is the correct advice.
saved_out="$RUN_OUT"; saved_rc="$RUN_RC"
OWNED="$(mktemp -d)"
RUN_CWD="$OWNED" run_phase_fakes
if printf '%s' "$RUN_OUT" | grep -q 'rm -rf tapstate-demo'; then
  ok "a directory the quickstart created is offered for removal by name"
else
  bad "own directory not offered for removal: $RUN_OUT"
fi
if [ -d "$OWNED/tapstate-demo" ]; then
  ok "and that directory is the one it made, not the caller's"
else
  bad "the run did not make its own directory under $OWNED"
fi
rm -rf "$OWNED"
RUN_OUT="$saved_out"; RUN_RC="$saved_rc"
# Telling someone to publish the port is the half of the advice that does not work on its own: the
# set registers its member under a container-internal name, so a driver that discovers the topology
# dials the host's own loopback and is refused. The note has to carry directConnection with it.
if printf '%s' "$RUN_OUT" | grep -q 'ports: \["127.0.0.1:27017:27017"\]' \
   && printf '%s' "$RUN_OUT" | grep -q 'directConnection=true' \
   && printf '%s' "$RUN_OUT" | grep -qi 'replicaSet=rs0'; then
  ok "says how to reach the store from the host, and why directConnection is required"
else
  bad "host access to the store is undocumented or incomplete: $RUN_OUT"
fi
# Stopping and destroying are different intentions and the demo has to offer both. Until it did, the
# only documented way out deleted the data, so a user who just wanted their laptop back had to guess
# -- and guessing wrong on this one is unrecoverable.
if printf '%s' "$RUN_OUT" | grep -q 'docker compose stop' \
   && printf '%s' "$RUN_OUT" | grep -qi 'keep its data'; then
  ok "offers a non-destructive stop, and says the data survives it"
else
  bad "no non-destructive stop offered: $RUN_OUT"
fi
# The warning has to sit on the destructive line itself. Asserting that the word appears somewhere in
# the output is satisfied by the CDC walkthrough's DELETE statement, several screens further up.
if printf '%s' "$RUN_OUT" | grep 'down -v' | grep -qi 'delet'; then
  ok "the destructive teardown says on its own line that it deletes data"
else
  bad "the down -v line does not say what it destroys: $(printf '%s' "$RUN_OUT" | grep 'down -v')"
fi
# The snapshot payoff is a real row count, printed with no user action, and it names both engines (the
# fake docker returns 5 orders and 6 shipments). Naming them is the point: one number for the pair
# would leave the reader unable to tell which engine produced it, and the demo's whole claim is that
# two of them did.
if printf '%s' "$RUN_OUT" | grep -q '5 orders from MySQL' \
   && printf '%s' "$RUN_OUT" | grep -q '6 of them, in one object per order'; then
  ok "prints what was assembled automatically (no user action), naming both engines"
else
  bad "assembly counts not printed: $RUN_OUT"
fi
# The CDC section walks all three operations -- consistent with a pipeline that no longer drops deletes.
# They are spread across the two engines on purpose now: the parent moves in MySQL, the array elements
# arrive and leave from PostgreSQL, and that spread is the demonstration. A walk that stayed on one
# engine would show three operations and none of the thing this demo is about.
if printf '%s' "$RUN_OUT" | grep -q 'INSERT INTO orders' \
   && printf '%s' "$RUN_OUT" | grep -q 'UPDATE orders' \
   && printf '%s' "$RUN_OUT" | grep -q 'DELETE FROM shipments'; then
  ok "the CDC section demonstrates insert, update and delete"
else
  bad "CDC section does not walk insert/update/delete: $RUN_OUT"
fi
# And it shows a change made in the second engine reaching the object the first engine roots. This is
# the whole reason the second engine is wired in, and the read it is verified by has to be the array
# inside that object -- a walk that inserted into PostgreSQL and then read a collection of its own
# would demonstrate two syncs, which is what this demo stopped being.
if printf '%s' "$RUN_OUT" | grep -q 'INSERT INTO shipments' \
   && printf '%s' "$RUN_OUT" | grep -q 'order_state.findOne({id:1})?.shipments?.length'; then
  ok "a change in the second engine is shown reaching the first engine's object"
else
  bad "no second-engine change demonstrated inside the assembled object: $RUN_OUT"
fi
# Every wait printed for the user to paste is bounded. An unbounded `until` returns in a second when
# the change arrives and hangs forever, in silence, when it does not -- and a recording of this demo
# structurally cannot show that, because on the recording machine it always returns. Counted rather
# than spot-checked: one unbounded loop among four is the whole defect.
unbounded="$(printf '%s' "$RUN_OUT" | grep -c 'until docker compose exec.*; do sleep 1; done' || true)"
loops="$(printf '%s' "$RUN_OUT" | grep -c '^  i=0; until docker compose exec' || true)"
if [ "$unbounded" = 0 ] && [ "$loops" -ge 1 ]; then
  ok "every wait a reader is told to paste is bounded ($loops of them), none can hang"
else
  bad "$unbounded unbounded wait(s) printed, $loops bounded: $(printf '%s' "$RUN_OUT" | grep 'until docker compose')"
fi
# A bound that gives up silently is the same trap wearing a timer. Each one has to say what it saw, so
# a reader can tell "nothing is arriving" from "this machine is slow", and where to look next.
if printf '%s' "$RUN_OUT" | grep -q 'docker compose logs --tail 50 server'; then
  ok "a wait that runs out says what it observed and where to look"
else
  bad "a wait can run out with nothing actionable printed: $RUN_OUT"
fi
# ...and it has to answer non-zero, or the same lines pasted into a script pass having waited out a
# change that never came. `false` rather than `exit`: the reader may be standing in that shell, and
# `exit` would close it. Counted against the loops so a bound left without one is not summed away.
# The $i here is the reader's shell variable in the printed text, not this script's -- single quotes.
# shellcheck disable=SC2016
gaveup="$(printf '%s' "$RUN_OUT" | grep -c 'done; \[ "$i" -lt 60 \] || false' || true)"
if [ "$gaveup" = "$loops" ] && [ "$loops" -ge 1 ]; then
  ok "every bounded wait answers non-zero when it gives up ($gaveup of $loops)"
else
  bad "$gaveup of $loops bounded waits answer non-zero when they give up"
fi
# The demo names the collection a reader should find, beside the command that lists it. The whole
# "no second client, no URI to configure" claim is about what they find when they look, so the name
# has to be in front of them rather than left to be recognised in a listing.
#
# Asserted on the name, not on the sentence around it. The sentence this once pinned said "one
# collection", which a real machine contradicts: a bare listing shows the two source tables as well,
# because it lists what every declared source holds. One collection in the *store* is the claim; the
# earlier wording made it sound like one line of output, and this assertion held that wording in
# place until somebody ran it by hand.
if printf '%s' "$RUN_OUT" | grep -q 'show collections' \
   && printf '%s' "$RUN_OUT" | grep -q 'views.order_state'; then
  ok "the demo points at exactly one collection, named"
else
  bad "the demo does not name the single collection a reader should find: $RUN_OUT"
fi

# --- the first admin exists before anyone tries to log in ---------------------------------------------
# The bootstrap sidecar depends_on the server being *healthy*, so it only starts at the moment the server
# reports healthy. Waiting on server health therefore proves the opposite of what it looks like it proves:
# it is the moment the admin is guaranteed NOT to exist yet. Observed as a real flake -- two runs of the
# same commit, one logged in fine and one drew control.auth-failed, after which every following verb
# reported cli.not-authenticated and the run died on the row count with the real cause scrolled off the top.
FAKE_BOOTSTRAP_PS='{"State":"running","ExitCode":0}' run_phase_fakes
unset FAKE_BOOTSTRAP_PS
if [ "$RUN_RC" -ne 0 ] && printf '%s' "$RUN_OUT" | grep -qi 'admin'; then
  ok "refuses to drive the verbs while the admin has not been created yet, and says so"
else
  bad "raced the bootstrap instead of waiting (rc=$RUN_RC): $RUN_OUT"
fi
# A bootstrap that ran and *failed* is a different condition from one still running, and it must not be
# waited out until the timeout: the container is gone, so waiting can only end one way.
FAKE_BOOTSTRAP_PS='{"State":"exited","ExitCode":1}' run_phase_fakes
unset FAKE_BOOTSTRAP_PS
if [ "$RUN_RC" -ne 0 ] && printf '%s' "$RUN_OUT" | grep -q 'docker compose logs bootstrap'; then
  ok "a bootstrap that exited non-zero fails the run and points at its log"
else
  bad "a failed bootstrap was not surfaced (rc=$RUN_RC): $RUN_OUT"
fi
# An authentication failure must be named where it happens. Without this the run still fails -- but it
# fails 30 seconds later on "did not reach the target (0 of 5 rows)", which sends the reader to the server
# log to investigate a pipeline that was never started. The discriminating part is that the run below
# delivers its rows: a check that merely required a non-zero exit would pass on the row count alone.
FAKE_CLI_OUT='error: control.auth-failed' run_phase_fakes
unset FAKE_CLI_OUT
if [ "$RUN_RC" -ne 0 ] && printf '%s' "$RUN_OUT" | grep -q 'could not log in' \
   && ! printf '%s' "$RUN_OUT" | grep -q 'did not reach the target'; then
  ok "an auth failure is diagnosed as an auth failure, not as an empty target"
else
  bad "auth failure not named at the point it happened (rc=$RUN_RC): $RUN_OUT"
fi

# An admin left over from an earlier run is the likely reason a demo whose password is generated cannot
# log in, and it is the one case the user can act on without reading anything. The bootstrap step reports
# success either way -- creating the admin and finding one already there are both "an admin exists" -- so
# a run against a stack whose volume outlived its .env fails at login with both facts true and filed in
# different places. Reported here as the remedy rather than as a log to go and read.
FAKE_CLI_OUT='error: control.auth-failed' \
  FAKE_BOOTSTRAP_LOG='bootstrap: an admin already exists, nothing to do' run_phase_fakes
unset FAKE_CLI_OUT FAKE_BOOTSTRAP_LOG
if [ "$RUN_RC" -ne 0 ] && printf '%s' "$RUN_OUT" | grep -q 'down -v'; then
  ok "an auth failure against a pre-existing admin names the reset that fixes it"
else
  bad "stale-admin auth failure did not name the remedy (rc=$RUN_RC): $RUN_OUT"
fi
# ...and the generic message is kept for an auth failure with no such history, which is a different
# situation with a different answer: wiping the volume there would destroy data to fix nothing.
FAKE_CLI_OUT='error: control.auth-failed' \
  FAKE_BOOTSTRAP_LOG='bootstrap: first admin created' run_phase_fakes
unset FAKE_CLI_OUT FAKE_BOOTSTRAP_LOG
if [ "$RUN_RC" -ne 0 ] && ! printf '%s' "$RUN_OUT" | grep -q 'down -v'; then
  ok "an auth failure with a freshly created admin does not prescribe wiping the volume"
else
  bad "the stale-admin remedy leaked into an unrelated auth failure (rc=$RUN_RC): $RUN_OUT"
fi

# A run whose online verbs did not take must fail, loudly and non-zero. The REPL is the reason this
# needs its own check: an interactive session does not end because one command was rejected, so it
# exits 0 whether register / apply / start succeeded or errored, and set -e sees nothing wrong. The
# row count is therefore the script's only evidence that a pipeline is actually moving data, and a run
# that reports an empty target while exiting 0 is the worst of both -- it reads as success everywhere
# a machine looks. The stack is deliberately left standing on the failure path: the server log is the
# next thing to read, and tearing the stack down would take it away.
run_phase_fakes 0
if [ "$RUN_RC" -ne 0 ] && printf '%s' "$RUN_OUT" | grep -q 'were not assembled'; then
  ok "fails non-zero when the pipeline delivers nothing to the target"
else
  bad "an empty target was reported as success (rc=$RUN_RC): $RUN_OUT"
fi
# And the half that is easy to leave unchecked: the first engine's rows arrive and the second engine's
# never do. That is not a hypothetical shape - it is exactly what a demo which fetches, registers and
# starts only the mysql half produces, and it was this script's own state until the second engine was
# wired in. A run verified on one total reports it as success, because that total is the one the
# working half fills.
run_phase_fakes 5 0
if [ "$RUN_RC" -ne 0 ] && printf '%s' "$RUN_OUT" | grep -q 'were not assembled'; then
  ok "fails non-zero when the first engine's orders arrive with nothing embedded in them"
else
  bad "a run missing the second engine was reported as success (rc=$RUN_RC): $RUN_OUT"
fi
# The same checks must not fire on a run that did deliver both: the failure paths above are worth
# nothing if they also reject the successful one.
run_phase_fakes 5 6
if [ "$RUN_RC" -eq 0 ] \
   && printf '%s' "$RUN_OUT" | grep -q '5 orders from MySQL' \
   && printf '%s' "$RUN_OUT" | grep -q '6 of them, in one object per order'; then
  ok "still succeeds when both engines' seeded rows are in the target"
else
  bad "a delivering run was rejected (rc=$RUN_RC): $RUN_OUT"
fi

# --- what the release serves: one pinned version, and a stack that pulls rather than builds ----------
# The demo directory a user lands in is not a checkout. Two things follow, and neither can be left as a
# step someone performs at release time -- an omitted step here does not fail the release, it fails the
# user, weeks later, on a machine nobody is watching.
#
# First, the assets the script fetches must come from the same release as the CLI it pins. Pointing the
# base at a branch would hand out a CLI frozen at one version alongside a compose file that keeps moving,
# and the mismatch would appear only on the user's machine. Deriving the base from the pin means the
# release tag is the single thing that decides, and the pin is already checked against the build above.
# Both patterns below are read as text, not evaluated: the point is that the source carries an
# unexpanded ${CLI_VERSION}, so a literal is what must be matched.
# shellcheck disable=SC2016
DEFAULT_QBASE="$(sed -n 's/^ *qbase="\${TAPSTATE_QUICKSTART_BASE_URL:-\(.*\)}"$/\1/p' "$QUICKSTART_SH")"
# shellcheck disable=SC2016
case "$DEFAULT_QBASE" in
  *'${CLI_VERSION}'*)
    ok "the default asset base is derived from the pinned CLI version, not a branch" ;;
  *)
    bad "default asset base '$DEFAULT_QBASE' is not derived from CLI_VERSION — a branch keeps moving after the release" ;;
esac

# The replica-set member address is the one thing in this file a client outside the container reads.
# Registered as localhost it points every such client at its own loopback, and the connection refused
# there reads like a local firewall problem rather than a name that means nothing here. Asserting the
# service name also keeps replicaSet= URIs working for anything a user adds to the compose network.
if grep -q "rs.initiate({" "$HERE/docker-compose.yml" \
   && grep "rs.initiate({" "$HERE/docker-compose.yml" | grep -q "host: 'mongo:27017'"; then
  ok "the replica set registers its member under the service name, not localhost"
else
  bad "replica-set member address: $(grep "rs.initiate({" "$HERE/docker-compose.yml" || echo '(no rs.initiate call found)')"
fi

# Second, the compose file must name a published image. A `build:` key is unusable from a demo directory:
# its context points into a repository that is not there. The source path keeps its build through an
# explicit override file instead, so the released stack and the development stack stop being the same
# file trying to be both.
COMPOSE="$HERE/docker-compose.yml"
COMPOSE_DEV="$HERE/docker-compose.dev.yml"
if grep -qE '^\s*build:' "$COMPOSE"; then
  bad "docker-compose.yml still carries a build: — a demo directory has no repository to build from"
else
  ok "the released compose file has no build: (a demo directory cannot build)"
fi
if grep -qE '^\s*image:\s*ghcr\.io/' "$COMPOSE"; then
  ok "the released compose file pins a published registry image"
else
  bad "docker-compose.yml does not pin a ghcr.io image: $(grep -nE '^\s*image:' "$COMPOSE" | tr '\n' ' ')"
fi
# The bundled state store is the official upstream image, pulled like any other. Tapstate does not
# redistribute a MongoDB binary, and pulling a stock image in a compose file is the ordinary way to depend
# on one -- packaging it into the distribution instead would be a redistribution decision, made silently,
# by whoever edited this file. Assert the store service names an unqualified upstream image (no registry
# host, so Docker Hub's official library) rather than something built or re-hosted here.
STORE_IMAGE="$(awk '/^  mongo:/{f=1} f&&/image:/{print $2; exit}' "$COMPOSE")"
case "$STORE_IMAGE" in
  mongo:*)
    ok "the state store is the official upstream image ($STORE_IMAGE), not one repackaged here" ;;
  "")
    bad "no image found for the mongo service in $COMPOSE" ;;
  *)
    bad "the state store image is not the upstream official one: $STORE_IMAGE" ;;
esac
# The image tag drifting from the build is the same defect as the CLI pin drifting, and gets the same guard.
COMPOSE_TAG="$(sed -n 's|.*image:.*ghcr\.io/[^:]*:\(.*\)|\1|p' "$COMPOSE" | sed 's/}$//; s/.*:-//')"
if [ "$COMPOSE_TAG" = "$POM_VERSION" ]; then
  ok "the compose image tag matches the build ($COMPOSE_TAG)"
else
  bad "compose image tag '$COMPOSE_TAG' does not match pom.xml revision $POM_VERSION"
fi
if [ -f "$COMPOSE_DEV" ] && grep -qE '^\s*build:' "$COMPOSE_DEV"; then
  ok "the development override re-adds the build for the from-source path"
else
  bad "docker-compose.dev.yml missing or carries no build: — the from-source path would have no way to build"
fi

# --- a compose fetched from a stale tree is pinned to this script's own version ----------------------
# The stub above serves the repository's own compose, which is in sync with the script, so nothing here
# could tell a script that pins from one that does not. Reality is the opposite: the script fetches this
# file from a release tag, and a tag's tree carries the PREVIOUS release's pins by construction -- the
# number is written inside the release runner, which never commits. So the case seeds exactly that and
# asks what reaches the demo directory.
#
# Shipped, this was a 0.4.1 CLI starting a 0.3.0 server. Nothing reported it: the file was present, the
# version it named existed, every container came up healthy, and the demo then could not log in.
STALE_STUB="$(mktemp -d)"
cp -R "$QS_STUB/." "$STALE_STUB/"
sed 's|image: ghcr.io/tapstate/tapstate:[^[:space:]]*|image: ghcr.io/tapstate/tapstate:0.0.1-stale|g' \
    "$QS_STUB/deploy/quickstart/docker-compose.yml" > "$STALE_STUB/deploy/quickstart/docker-compose.yml"
# The seed is checked before it is used. A substitution that quietly matched nothing would leave the
# stub in sync with the script, and the case would pass having tested the situation it exists to avoid.
if grep -q '0\.0\.1-stale' "$STALE_STUB/deploy/quickstart/docker-compose.yml"; then
  REAL_STUB="$QS_STUB"; QS_STUB="$STALE_STUB"
  run_prepare Linux x86_64 glibc
  QS_STUB="$REAL_STUB"
  if grep -q "image: ghcr.io/tapstate/tapstate:$VERSION" "$DEMO/docker-compose.yml" \
     && ! grep -q '0\.0\.1-stale' "$DEMO/docker-compose.yml"; then
    ok "a compose fetched from a stale tree is pinned to this script's version ($VERSION)"
  else
    bad "the fetched compose still names $(sed -n 's|.*image: ghcr.io/tapstate/tapstate:\([^[:space:]]*\).*|\1|p' "$DEMO/docker-compose.yml" | head -1) — the demo would run a server other than $VERSION"
  fi
else
  bad "could not seed a stale compose, so the pinning was never exercised"
fi
rm -rf "$STALE_STUB"

# --- the CLI's own words survive a CLI that fails ---------------------------------------------------
# Text, not behaviour: this smoke stops before Docker, and the line in question runs only against a live
# stack. It is checked at all because of what its absence looks like -- the script runs under `set -e`,
# and as a bare assignment `repl_out="$(... )"` ends the script AT that line when the CLI exits
# non-zero. The print and the diagnosis below it never run, and everything the CLI said, up to and
# including a stack trace, reaches the exit status and nowhere else. That shape was reported once as a
# CPU-specific fault and reproduced later on hardware where that could not be the cause.
if grep -qE '\|\| repl_status=\$\?' "$QUICKSTART_SH"; then
  ok "a failing CLI does not end the script before its output is printed"
else
  bad "the REPL capture is a bare assignment again — under set -e the CLI's output would be discarded"
fi
# And a failure the two named cases do not recognise still stops the run, rather than falling through to
# the row-count wait, which reports an empty target and sends the reader to a pipeline never started.
if grep -qE 'repl_status" -ne 0' "$QUICKSTART_SH"; then
  ok "a CLI failure the named cases do not match still stops the run"
else
  bad "nothing answers a non-zero CLI exit that is neither auth-failed nor not-authenticated"
fi

# --- a dropped transfer leaves no stump, and the run survives one -----------------------------------
# Reported from a from-scratch install of a published release: the postgres jar transfer dropped
# mid-flight three runs in a row, at three different offsets. Two things then went wrong, and the
# second is the expensive one. The transfer wrote in place, so the partial bytes landed at the real
# path; the guard below is `[ -f ... ] ||`, existence only, so every later run read the stump as
# "already have it" and never re-fetched it. The stack came up with a corrupt connector and the
# failure surfaced three steps downstream, naming a remedy that was the step that had just failed.
#
# The fake curl reproduces the drop rather than describing it: it writes a few bytes to wherever -o
# points and exits 18 (partial file), the code the real one gave, for the first $FAIL_TIMES attempts
# at that URL. Every other transfer is passed through to the real curl untouched.
REAL_CURL="$(command -v curl 2>/dev/null || true)"
DROP_DIR="$(mktemp -d)"
cat > "$DROP_DIR/curl" <<EOF
#!/bin/sh
out=""; url=""; prev=""
for a in "\$@"; do
  [ "\$prev" = "-o" ] && out="\$a"
  case "\$a" in http://*|https://*|file://*) url="\$a" ;; esac
  prev="\$a"
done
case "\$url" in
  *postgres-connector.jar)
    n=0; [ -f "$DROP_DIR/n" ] && n="\$(cat "$DROP_DIR/n")"
    if [ "\$n" -lt "\$(cat "$DROP_DIR/fail_times")" ]; then
      echo "\$((n + 1))" > "$DROP_DIR/n"
      [ -n "\$out" ] && printf 'fake-pos' > "\$out"
      echo "curl: (18) Transferred a partial file" >&2
      exit 18
    fi ;;
esac
exec "$REAL_CURL" "\$@"
EOF
chmod +x "$DROP_DIR/curl"
JAR_BYTES="$(wc -c < "$QS_STUB/connectors-preview/postgres-connector.jar" | tr -d ' ')"

if [ -z "$REAL_CURL" ]; then
  bad "no curl on PATH, so the dropped-transfer cases never ran"
else
  # One drop, then the link recovers -- which is what a flaky link does. The documented one-liner has
  # to survive this by itself: a user who must run it again is doing an undocumented manual step.
  CURL_SHIM="$DROP_DIR/curl"; echo 1 > "$DROP_DIR/fail_times"; rm -f "$DROP_DIR/n"
  run_prepare Linux x86_64 glibc
  DROP1="$DEMO"
  if [ "$RC" -eq 0 ] && [ "$(wc -c < "$DROP1/postgres-connector.jar" | tr -d ' ')" = "$JAR_BYTES" ]; then
    ok "a transfer that drops once is retried, and the run completes on its own"
  else
    bad "one dropped transfer ended the run (rc=$RC, jar=$(wc -c < "$DROP1/postgres-connector.jar" 2>/dev/null | tr -d ' ') of $JAR_BYTES bytes)"
  fi

  # A link that never recovers. The run must fail -- but it must not leave a partial file at the real
  # path, because that is what the next run would accept.
  CURL_SHIM="$DROP_DIR/curl"; echo 99 > "$DROP_DIR/fail_times"; rm -f "$DROP_DIR/n"
  run_prepare Linux x86_64 glibc
  DROP2="$DEMO"
  if [ "$RC" -ne 0 ] && [ ! -f "$DROP2/postgres-connector.jar" ]; then
    ok "a transfer that never completes leaves nothing at the final path"
  else
    bad "a stump survived a failed transfer (rc=$RC, $(wc -c < "$DROP2/postgres-connector.jar" 2>/dev/null | tr -d ' ') of $JAR_BYTES bytes at the real path)"
  fi

  # And the run after it recovers, which is the half the user actually hits: same directory, working
  # link, no manual deletion. Under the old guard the stump was read as "already have it" forever.
  CURL_SHIM=""
  run_prepare Linux x86_64 glibc "$DROP2"
  if [ "$RC" -eq 0 ] && [ "$(wc -c < "$DROP2/postgres-connector.jar" | tr -d ' ')" = "$JAR_BYTES" ]; then
    ok "re-running after a failed transfer re-fetches the jar instead of accepting the stump"
  else
    bad "the re-run did not recover the jar (rc=$RC, $(wc -c < "$DROP2/postgres-connector.jar" 2>/dev/null | tr -d ' ') of $JAR_BYTES bytes): $OUT"
  fi
  CURL_SHIM=""
fi
rm -rf "$DROP_DIR"

# --- summary ----------------------------------------------------------------------------------------
echo
printf '\033[1mquickstart smoke: %d passed, %d failed\033[0m\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
