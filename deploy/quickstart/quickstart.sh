#!/bin/sh
#
# tapstate quickstart — brings up the local demo from an empty directory: it downloads the compose stack,
# the platform's CLI, and the demo connector jars, generates a demo workspace and a .env with a random
# admin password, then starts the stack and runs a real MySQL -> MongoDB pipeline. Nothing is built.
#
# Usage, either form:
#
#   curl -sSL <base>/quickstart.sh | sh
#
# Piped, the script takes a directory of its own (./tapstate-demo) so everything it adds stays inside
# one removable directory. Download-then-run works the same and is the form to pick when you want to
# read the script first, re-run it, or inspect a failure -- the saved file marks the directory to
# work in, so nothing nests:
#
#   mkdir tapstate-demo && cd tapstate-demo
#   curl -fLO <base>/quickstart.sh
#   sh quickstart.sh
#
# It never uses sudo, never edits a shell rc, and installs the CLI in place (./tapstate, not on PATH), so
# `rm -rf` of this directory removes everything it added. An unsupported platform fails before anything is
# fetched, leaving this directory as you found it.
#
# Environment seams:
#   TAPSTATE_QUICKSTART_BASE_URL  where install.sh, the compose file, and the seed SQL are fetched from.
#   TAPSTATE_BASE_URL             CLI release base, passed through to install.sh.
#   TAPSTATE_VERSION              pin the CLI version (default: the pinned CLI_VERSION below).
#   TAPSTATE_CONNECTORS_URL       base URL for the demo connector jars.
#   TAPSTATE_QUICKSTART_PREPARE_ONLY  stop after preparing the directory, before Docker (used by tests).
#
# POSIX sh, no bashisms. All work is inside main().
set -eu

# The CLI release this quickstart installs. install.sh's own default resolves /releases/latest, and
# GitHub fills that only from full releases -- the CLI ships as a prerelease, so that lookup finds
# nothing and install.sh refuses, which would strand the quickstart at the CLI step on a clean machine.
# Pin it here instead, the same way the demo connector jars are pinned to a published tag. This must
# match the version in pom.xml; quickstart-smoke.sh fails the build when the two drift apart.
CLI_VERSION="0.7.0"

die() {
    step_close
    printf 'quickstart: %s\n' "$1" >&2
    exit 1
}

# Progress is one line per step. On a terminal the line shows the step in progress and is redrawn in
# place as finished; anywhere else (a log, CI, a test) both lines are printed, so nothing depends on a
# cursor that is not there. What each step ran, and everything it printed, goes to quickstart.log: the
# raw CLI session is what a failure needs and what a first reader does not, so it is printed only when
# a step fails.
step_open=""
step_begin() {   # $1 = what is happening, as a phrase
    step_open="$1"
    if [ -t 1 ]; then printf '  ... %s ' "$1"; else printf '  ... %s\n' "$1"; fi
    [ -z "${log:-}" ] || printf '\n== %s\n' "$1" >> "$log"
}
step_tick() {
    if [ -t 1 ]; then printf '.'; fi
}
step_done() {    # $1 = what is now true
    if [ -t 1 ] && [ -n "$step_open" ]; then printf '\r\033[K'; fi
    step_open=""
    printf '  ✓ %s\n' "$1"
}
# Ends an unfinished step line, so whatever is printed next starts on a line of its own.
step_close() {
    if [ -t 1 ] && [ -n "${step_open:-}" ]; then printf '\n'; fi
    step_open=""
}

# $1 quoted for a POSIX shell when it needs it, so a printed `cd <dir>` can be pasted as it stands.
sh_quote() {
    case "$1" in
        *[!A-Za-z0-9_./-]*) printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")" ;;
        *) printf '%s' "$1" ;;
    esac
}

# Download $1 to the file $2 with whichever of curl / wget is present.
#
# The transfer lands in $2.part and is moved into place only once it has completed, so an interrupted
# one leaves nothing at the real path. That matters more than it reads: callers guard on existence
# alone, so a partial file sitting at the real path is read as "already have it" by every later run,
# and the corrupt result surfaces steps downstream with nothing naming the cause.
#
# Attempts resume the partial rather than restarting it -- a link that drops once on a 50 MB download
# tends to drop again, and restarting from zero each time can make no progress at all. The last
# attempt starts clean instead, which is the only way past a server that cannot serve ranges, or a
# .part that is already complete because a run was killed between the transfer and the move.
#
# It reports and returns non-zero rather than exiting, so the caller decides: under `set -e` an
# unguarded call ends the run exactly as it did before, and a caller that can do without the file
# keeps its own `|| ...`.
fetch() {
    _part="$2.part"
    _try=0
    # Two failures this download used to have, and they are the same failure wearing different
    # clothes: it said nothing while it worked, and it said nothing while it was dead.
    #
    # Silence was the whole user experience of the first step anyone takes with this product. The
    # binary is tens of megabytes; on an ordinary connection that is a minute or more of a terminal
    # that has printed nothing since the disclosure line, and the reader's only move is to guess
    # whether it is working. Measured: someone waited two minutes and then asked whether it had hung.
    # It had not.
    #
    # And there was no timeout of any kind, so a transfer that stalled outright hung forever and
    # looked EXACTLY like one that was merely slow. Two states that want opposite reactions, rendered
    # identically.
    #
    # So: show progress where a person is watching, and fail a dead transfer rather than hanging on
    # it. The speed floor is deliberately far below any working connection -- 1 KB/s averaged over 15
    # seconds -- because the job is to end a transfer that is not moving, NOT to punish a slow one.
    # A flat --max-time would do the opposite and cut off exactly the slow-but-working case that
    # needs the patience.
    #
    # That floor is curl's. wget has no minimum-rate option, so its path is an idle timeout instead: it
    # ends a transfer that has delivered nothing at all for the same window. A server trickling a byte
    # at a time would keep it alive; a connection that has died would not, and a dead one is what this
    # exists to end. Its retries are capped at curl's three, because wget's own default is twenty, and
    # twenty read timeouts per attempt is a stall that ends long after the reader has given up on it.
    #
    # Progress goes to stderr and only when stderr is a terminal. The quickstart drops this script's
    # stdout but shows its stderr, so a person piping the one-liner sees the bar; a log, a CI run and
    # anything reading the output stay byte-for-byte as quiet as before.
    #
    # wget's bar is --show-progress, which GNU wget only grew in 1.16; an older one (RHEL/CentOS 7
    # ships 1.14) rejects the flag and fails every attempt. It is asked for only when wget lists it,
    # so a wget that cannot draw the bar still downloads, quietly, as it did before.
    if [ -t 2 ]; then
        _curl_out="--progress-bar"; _wget_out="-q"
        if wget --help 2>&1 | grep -q -- '--show-progress'; then
            _wget_out="-q --show-progress"
        fi
    else
        _curl_out="-sS"; _wget_out="-q"
    fi
    # A local, not an environment variable: the smoke lifts this function out with sed and rewrites
    # the number so its case runs in seconds instead of minutes. Making it settable from outside would
    # put a knob on the shipped installer that exists only for a test.
    _stall_secs=15
    while [ "$_try" -lt 3 ]; do
        _try=$((_try + 1))
        [ "$_try" -lt 3 ] || rm -f "$_part"
        if command -v curl >/dev/null 2>&1; then
            # shellcheck disable=SC2086  # deliberately split: one flag or two, chosen just above
            curl -fL $_curl_out --retry 2 -C - \
                 --connect-timeout 20 --speed-limit 1024 --speed-time "$_stall_secs" \
                 "$1" -o "$_part" && { mv -f "$_part" "$2"; return 0; }
        elif command -v wget >/dev/null 2>&1; then
            # shellcheck disable=SC2086  # same
            wget $_wget_out -c --tries=3 --connect-timeout=20 --read-timeout="$_stall_secs" \
                 "$1" -O "$_part" && { mv -f "$_part" "$2"; return 0; }
        else
            die "neither curl nor wget is available to download $1."
        fi
    done
    printf 'quickstart: could not download %s -- %s attempts, the last from scratch.\n' "$1" "$_try" >&2
    # Only when there is one. A transfer that never opened leaves no .part, and promising a
    # resume of a file that is not there sends the reader looking for it.
    [ ! -f "$_part" ] || printf 'quickstart: what did arrive is kept at %s, so a later run resumes it.\n' "$_part" >&2
    return 1
}

# One number read out of the managed store, as a plain integer. A read that fails, or answers anything
# that is not a number, counts as zero: these are polled while the stack is still settling, and a
# half-started mongosh has to read as "not there yet" rather than abort the run.
read_number() {   # $1 = a mongosh expression evaluating to a number
    _c="$(docker compose exec -T mongo mongosh --quiet \
        'mongodb://mongo:27017/views?directConnection=true' \
        --eval "$1" 2>/dev/null | tr -d '[:space:]')"
    case "$_c" in ''|*[!0-9]*) _c=0 ;; esac
    printf '%s' "$_c"
}

# How many orders have been assembled and written.
count_orders() {
    read_number 'db.order_state.countDocuments()'
}

# How many shipments are sitting inside those orders, added up across every document.
#
# This is the reading that sees the second engine, and counting documents is not: an assembly wired to
# MySQL alone still writes five orders, each with an empty array, and a check on the document count
# calls that a success. What only the second engine can produce is elements inside the arrays, so that
# is what is counted. An order with no shipments has no array at all, hence the coalesce - a missing
# field is zero elements, not an error that reads as zero rows arrived.
count_embedded_shipments() {
    # The $-prefixed words here are Mongo aggregation operators and a field path, not shell expansions,
    # so the quoting is single on purpose.
    # shellcheck disable=SC2016
    read_number 'db.order_state.aggregate([{$group:{_id:null,n:{$sum:{$size:{$ifNull:["$shipments",[]]}}}}}]).toArray()[0]?.n ?? 0'
}

# Generate the demo workspace: two sources on different engines, and the one pipeline that assembles
# them into a single object. The addresses are compose service names because the connector runs inside
# the server container, where loopback is the server itself.
#
# There is no target here, and that is the point of the shape rather than an omission: declaring a view
# is the whole instruction to materialize it, so the pipeline names nowhere to write and the assembled
# documents land in the managed store the deployment already runs. A serve block would say where to
# write, which is a different thing - an exit into somebody else's system, not the query face.
#
# The two halves cannot be joined by either database: orders live in MySQL, shipments in PostgreSQL,
# and neither engine can see the other's table. That is what makes this worth demonstrating - it is not
# a view and not a join, and no single SQL statement anywhere can produce it.
generate_workspace() {
    mkdir -p work/source work/pipeline
    cat > work/source/orders_db.tap.yml <<'YAML'
version: tapstate/v1
kind: source
id: orders_db
connector: mysql
config: { host: mysql, port: 3306, database: appdb, username: root, password: secret }
mode: cdc
tables: [ orders ]
YAML
    # The second engine's source. Two settings are spelled differently from the MySQL source above, and
    # both are this connector's own spelling rather than a choice: the account is `user` where MySQL says
    # `username`, and a table is addressed by schema as well as by database. `mode: cdc` is what makes a
    # row inserted after the stack is up cross at all - a snapshot-only source would carry the seeded
    # rows and then nothing, which reads from outside as a demo that works.
    cat > work/source/fulfillment_db.tap.yml <<'YAML'
version: tapstate/v1
kind: source
id: fulfillment_db
connector: postgres
config: { host: postgres, port: 5432, database: appdb, schema: public, user: postgres, password: secret }
mode: cdc
tables: [ shipments ]
YAML
    # One pipeline over both sources. `from:` names each side under a short alias; the root is the order,
    # and each shipment is placed into an array on the order it belongs to.
    #
    # Three settings here are load-bearing and none is decoration. `arrayKey` is how an element inside
    # the array is identified, so a shipment that is later updated or deleted moves the element it
    # belongs to rather than appending a second one. `primary_key` is how the materialized document is
    # addressed, and the run refuses without it. And `read_mode: snapshot_and_cdc` is what makes both
    # halves live: the seeded rows arrive by snapshot, and everything typed afterwards - into either
    # engine - arrives by change stream.
    cat > work/pipeline/order_pipeline.tap.yml <<'YAML'
version: tapstate/v1
kind: pipeline
id: order_pipeline
source: [ orders_db, fulfillment_db ]
settings: { read_mode: snapshot_and_cdc }
transforms:
  - id: assemble
    type: nest
    from: { order: orders, shipment: shipments }
    root:
      from: order
      key: [ id ]
      embed:
        - { from: shipment, on: { order_id: id }, as: array, path: shipments, arrayKey: [ id ] }
view:
  id: order_state
  from: assemble
  primary_key: id
YAML
}

# Wire an AI agent to this stack: a write-scoped machine token, and an MCP client configuration in
# tapstate-mcp.json that starts the sidecar with it. The agent then creates sources and pipelines itself.
#
# Optional by design. The demo has already succeeded when this runs, so nothing here fails the run: a
# step that cannot complete leaves agent_note saying why, and the browser and CLI paths stand on their
# own. The file name is the demo's own rather than .mcp.json, because a saved script run in place sits in
# the user's directory, and that name may already hold their configuration for something else.
#
# The sidecar ships as a jar unless a native one sits beside the CLI. A local Java 21 (or the native
# sidecar) runs it through `tapstate mcp`, and JAVA_HOME is written into the configuration because an
# agent launched from a desktop app does not inherit this shell's PATH. Without either, it runs on the
# server image's own Java, inside the server container's network namespace: the sidecar refuses plain
# HTTP to anything but loopback, and that namespace is where the server is loopback.
write_agent_config() {
    agent_config=tapstate-mcp.json
    agent_note=""
    server_url=http://127.0.0.1:8080
    # A token from an earlier run is kept while the server still accepts it. One it no longer accepts --
    # the store was wiped with down -v -- is replaced, rather than left to fail inside the agent.
    if [ -f "$agent_config" ]; then
        agent_token="$(sed -n 's/.*"TAPSTATE_TOKEN": *"\([^"]*\)".*/\1/p' "$agent_config" | head -n 1)"
        if [ -n "$agent_token" ] \
           && TAPSTATE_TOKEN="$agent_token" ./tapstate -c "$server_url" ls >/dev/null 2>&1; then
            return 0
        fi
    fi

    # The password travels in the environment, as everywhere else here, never as an argument.
    token_json="$(TAPSTATE_PASSWORD="$admin_pw" ./tapstate -c "$server_url" -u admin \
        token create --scope write -o json 2>/dev/null)" || token_json=""
    agent_token="$(printf '%s' "$token_json" | tr -d '\n' \
        | sed -n 's/.*"token" *: *"\([A-Za-z0-9._~+\/=-]*\)".*/\1/p')"
    if [ -z "$agent_token" ]; then
        agent_note="no agent token could be created; make one yourself with: TAPSTATE_PASSWORD=<.env password> ./tapstate -c $server_url -u admin token create --scope write"
        return 0
    fi

    # Every path below is written into JSON unescaped, so one that would need escaping is refused here.
    case "$PWD" in *\"*|*\\*)
        agent_note="this directory's path contains a quote or a backslash, so no agent configuration was written"
        return 0 ;;
    esac
    cli_link="$(readlink ./tapstate 2>/dev/null || true)"
    case "$cli_link" in
        "") cli_bin_dir="$PWD" ;;
        /*) cli_bin_dir="$(dirname "$cli_link")" ;;
        *)  cli_bin_dir="$PWD/$(dirname "$cli_link")" ;;
    esac
    libexec_dir=""
    if [ -d "$cli_bin_dir/../libexec" ]; then libexec_dir="$(cd "$cli_bin_dir/../libexec" && pwd -P)"; fi

    if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
        java_bin="$JAVA_HOME/bin/java"
    else
        java_bin="$(command -v java 2>/dev/null || true)"
    fi
    java_major=""; java_home=""
    if [ -n "$java_bin" ]; then
        java_props="$("$java_bin" -XshowSettings:properties -version 2>&1 || true)"
        java_major="$(printf '%s\n' "$java_props" | sed -n 's/^ *java\.specification\.version = \([0-9][0-9]*\).*/\1/p' | head -n 1)"
        java_home="$(printf '%s\n' "$java_props" | sed -n 's/^ *java\.home = //p' | head -n 1)"
    fi
    case "$java_home" in *\"*|*\\*) java_home="" ;; esac

    if [ -n "$libexec_dir" ] && [ -x "$libexec_dir/tapstate-mcp" ]; then
        agent_env=""
        agent_runs="It runs \`./tapstate mcp\`, which needs nothing else installed."
    elif [ -n "$java_major" ] && [ "$java_major" -ge 21 ] && [ -x "$java_home/bin/java" ]; then
        agent_env=",
        \"JAVA_HOME\": \"$java_home\""
        agent_runs="It runs \`./tapstate mcp\` on the Java 21 at \`$java_home\`, which is named in the file so an agent started from a desktop app finds it too."
    else
        agent_env=docker
        agent_runs="No Java 21 was found on this machine, so it runs inside Docker, beside the demo's server: Docker has to be running whenever the agent uses it."
    fi

    if [ "$agent_env" != docker ]; then
        agent_command="$PWD/tapstate"
        agent_args="\"mcp\", \"--server\", \"$server_url\", \"--allow-write\""
    else
        docker_bin="$(command -v docker)"
        server_container="$(docker compose ps --format '{{.Name}}' server 2>/dev/null | head -n 1)"
        case "$server_container" in
            ""|*[!A-Za-z0-9_.-]*)
                agent_note="the server container could not be named, so no agent configuration was written"
                return 0 ;;
        esac
        case "$libexec_dir" in *\"*|*\\*) libexec_dir="" ;; esac
        if [ -z "$libexec_dir" ] || [ ! -f "$libexec_dir/tapstate-mcp.jar" ]; then
            agent_note="the MCP sidecar was not found beside the CLI, so no agent configuration was written"
            return 0
        fi
        agent_command="$docker_bin"
        agent_args="\"run\", \"-i\", \"--rm\", \"--network\", \"container:$server_container\",
        \"-e\", \"TAPSTATE_TOKEN\", \"-v\", \"$libexec_dir:/opt/tapstate-mcp:ro\",
        \"--entrypoint\", \"java\", \"ghcr.io/tapstate/tapstate:${CLI_VERSION}\",
        \"-jar\", \"/opt/tapstate-mcp/tapstate-mcp.jar\", \"--server\", \"$server_url\", \"--allow-write\""
        agent_env=""
    fi

    # The token is a credential, so the file is created readable by this user alone, like .env.
    (
        umask 077
        cat > "$agent_config.tmp" <<EOF
{
  "mcpServers": {
    "tapstate": {
      "command": "$agent_command",
      "args": [$agent_args],
      "env": {
        "TAPSTATE_TOKEN": "$agent_token"$agent_env
      }
    }
  }
}
EOF
    ) && mv "$agent_config.tmp" "$agent_config" \
        || agent_note="the agent configuration could not be written to $agent_config"
}

# The files below are written into the demo directory and rewritten by every run, so they always match
# the stack beside them. A saved script run in place works in the user's own directory, where a file of
# the same name may be theirs: only a file carrying this marker, or no file at all, is ever written.
generated_marker="written by the Tapstate quickstart"
write_generated() {   # $1 = path; the content arrives on stdin
    if [ -f "$1" ] && ! grep -q "$generated_marker" "$1"; then
        cat > /dev/null
        printf 'quickstart: %s is not one this script wrote, so it was left alone\n' "$1" >&2
        return 0
    fi
    cat > "$1.tmp" && mv "$1.tmp" "$1"
}

# try-cdc.sh: change-data-capture, seen from the outside. Four changes to the two source databases, one
# at a time, each followed by the document it should have moved.
#
# Every wait is bounded, and that is not tidiness. Somebody the script will never hear from again runs
# this, and an unbounded wait returns in a second when the change arrives and hangs forever, silently,
# when it does not. The bound is taken for the slowest healthy machine rather than the fastest -- a
# change crosses in about a second -- so a normal run is never reported as a failure; when it runs out
# it says what was actually observed, which is what separates "nothing is arriving" from "this machine
# is slow", and it exits non-zero.
write_try_cdc() {
    write_generated try-cdc.sh <<'SH'
#!/bin/sh
# try-cdc.sh -- written by the Tapstate quickstart; a re-run of the quickstart rewrites it.
#
# Makes four changes to the demo's source databases, one at a time, and after each one prints the
# document in MongoDB that it should have changed. Nothing is re-run in between: the pipeline carries
# each change across on its own. Run it again to repeat it; it first undoes what an earlier run changed.
set -eu
cd "$(dirname "$0")"

wait_secs=60
uri='mongodb://mongo:27017/views?directConnection=true'
mongo() { docker compose exec -T mongo mongosh --quiet "$uri" --eval "$1"; }
postgres() { docker compose exec -T postgres psql -q -U postgres -d appdb -c "$1" > /dev/null; }
mysql() { docker compose exec -T -e MYSQL_PWD=secret mysql mysql -uroot appdb -e "$1"; }

# Waits until $1 (a mongosh expression) is true. On timeout, prints $2 followed by the value of $3, and
# where to look next, then exits non-zero.
await() {
    i=0
    until mongo "quit(($1) ? 0 : 1)" > /dev/null 2>&1; do
        i=$((i + 1))
        if [ "$i" -ge "$wait_secs" ]; then
            echo "    not there after ${wait_secs}s; $2 $(mongo "print($3)" 2> /dev/null || echo '(unreadable)')." >&2
            echo "    Look at: docker compose logs --tail 50 server" >&2
            exit 1
        fi
        sleep 1
    done
}
show() { mongo "printjson(db.order_state.findOne({id: $1}, {_id: 0}))" | sed 's/^/    /'; }

order1_shipments='db.order_state.findOne({id: 1})?.shipments?.length ?? 0'

# The seeded state is order 1 owned by alice with two shipments, and no order 6.
if ! mongo "quit(($order1_shipments) === 2 && db.order_state.findOne({id: 1})?.customer === 'alice' && !db.order_state.countDocuments({id: 6}) ? 0 : 1)" > /dev/null 2>&1; then
    echo "Undoing the changes from an earlier run first."
    postgres "DELETE FROM shipments WHERE id = 7;"
    mysql "DELETE FROM orders WHERE id = 6; UPDATE orders SET customer = 'alice' WHERE id = 1;"
    await "($order1_shipments) === 2 && db.order_state.findOne({id: 1})?.customer === 'alice' && !db.order_state.countDocuments({id: 6})" \
        'order 1 holds' "JSON.stringify(db.order_state.findOne({id: 1}, {_id: 0}))"
fi

echo
echo "1/4  PostgreSQL: add a shipment to order 1, which has two."
postgres "INSERT INTO shipments VALUES (7, 1, 'ups', 'pending');"
await "($order1_shipments) >= 3" 'order 1 holds this many shipments:' "$order1_shipments"
echo "     Order 1 in MongoDB now has three:"
show 1

echo
echo "2/4  MySQL: rename the customer on order 1, from alice to alicia."
mysql "UPDATE orders SET customer = 'alicia' WHERE id = 1;"
await "db.order_state.findOne({id: 1})?.customer === 'alicia' && ($order1_shipments) >= 3" \
    'order 1 reads' "JSON.stringify(db.order_state.findOne({id: 1}, {_id: 0}))"
echo "     The customer changed, and the shipments from PostgreSQL stayed where they were:"
show 1

echo
echo "3/4  PostgreSQL: delete that shipment again."
postgres "DELETE FROM shipments WHERE id = 7;"
await "($order1_shipments) <= 2" 'order 1 still holds this many shipments:' "$order1_shipments"
echo "     Order 1 is back to two:"
show 1

echo
echo "4/4  MySQL: add order 6, which has no shipments yet."
mysql "INSERT INTO orders VALUES (6, 'frank', 60.00);"
await "db.order_state.countDocuments({id: 6}) === 1" 'MongoDB holds this many orders:' 'db.order_state.countDocuments()'
echo "     A new document, with no shipments in it:"
show 6

echo
echo "Each change reached MongoDB on its own, from either database. Run ./try-cdc.sh again to repeat it."
SH
    chmod +x try-cdc.sh
}

# NEXT.md: what a reader looks for after the first screen, and not on it. Every command starts with a cd
# into the demo directory, because the file is read from wherever the reader happens to be.
write_next_md() {
    fence='```'
    if [ "${demo_dir_is_ours:-no}" = yes ]; then
        # Whole-directory removal is only ever offered for a directory this script made.
        removal="Then remove the folder, with the CLI, the connector jars, the workspace, \`.env\` and the
agent configuration in it:

${fence}sh
rm -rf $here
${fence}"
    else
        # Run in place, the directory is the user's and holds their files, so \`rm -rf\` on it would
        # destroy unrelated work. Say what was added instead.
        removal="This folder is yours, so nothing here removes it. What the quickstart added to it:

    tapstate tap versions/ connectors/ *-connector.jar mysql-init/ postgres-init/ work/ .env
    docker-compose.yml tapstate-mcp.json try-cdc.sh NEXT.md AGENT-SETUP.md quickstart.log"
    fi
    write_generated NEXT.md <<EOF
<!-- $generated_marker; a re-run of the quickstart rewrites it -->
# Your Tapstate demo: what next

The demo lives in \`$PWD\`. Every command below starts with a \`cd\` into it, so it works from any
terminal.

## Look at it from the CLI

${fence}sh
${cd_here}./tapstate -w work
${fence}

Then, at the CLI's prompt (the admin password is in \`.env\`):

${fence}
connect http://127.0.0.1:8080
login admin
show collections                     three: the two sources, and views.order_state
views.order_state.find({id:1})       one order, with its shipments inside it
watch views.order_state {id:1}       the same document, redrawn as it changes
status order_pipeline --watch        the pipeline behind it
${fence}

## Open the web app from another computer

The server answers on this machine only. From another computer, keep this open:

${fence}sh
ssh -N -L 127.0.0.1:18080:127.0.0.1:8080 <you>@<this host>
${fence}

and visit http://127.0.0.1:18080 instead.

## See change-data-capture

${fence}sh
${cd_here}./try-cdc.sh
${fence}

It adds a shipment in PostgreSQL, renames a customer in MySQL, removes the shipment, and adds an order,
printing the MongoDB document after each change. It is a plain shell script: read it to see the SQL it
runs, and copy those lines to make changes of your own.

## Connect your own tools to MongoDB

The store's port is not published, so nothing on your machine can reach it by default. To point a GUI
or a driver at it, add \`ports: ["127.0.0.1:27017:27017"]\` to the \`mongo\` service in
\`docker-compose.yml\`, run \`${cd_here}docker compose up -d\`, and connect with:

    mongodb://127.0.0.1:27017/views?directConnection=true

Keep \`directConnection=true\`. This is a one-member replica set that registers its member under a name
that means something only inside the container, so a URI carrying \`replicaSet=rs0\` makes the driver
discover that name and dial an address on your machine where nothing is listening.

## Pause and resume

${fence}sh
${cd_here}docker compose stop      # stop the demo and keep its data
${cd_here}docker compose start     # pick it up again
${fence}

## Delete it

This is not reversible:

${fence}sh
${cd_here}docker compose down -v    # stop the demo and delete its data
${fence}

$removal

The pulled images remain; remove them with \`docker image rm <image>\`.
EOF
}

# AGENT-SETUP.md: the agent path in full, beside the configuration it describes. Written only when that
# configuration was, so it never walks a reader through a file that is not there. The name is not
# AGENT.md on purpose: some agents read a file of that name as instructions addressed to them.
write_agent_md() {
    [ -z "${agent_note:-}" ] || return 0
    fence='```'
    write_generated AGENT-SETUP.md <<EOF
<!-- $generated_marker; a re-run of the quickstart rewrites it -->
# Use an AI agent with this demo

Connected to Tapstate over MCP, an AI agent can do what you would do in the browser or the CLI: create
sources, write and check pipelines, start and stop them, and read their status and their data. You
describe what you want; the agent does the work and tells you what it did.

## 1. Install the Tapstate skills first

${fence}sh
npx --yes skills add tapstate/tapstate-skills
${fence}

The skills teach the agent how Tapstate pipelines are written. Without them, an agent can write a
pipeline that passes validation and runs, but does not do what you asked -- and still report success.

## 2. Connect the agent

\`$PWD/$agent_config\` starts Tapstate's MCP server with a token for this demo.
${agent_runs:-}

- **Claude Code:** run

  ${fence}sh
  ${cd_here}claude --mcp-config $agent_config
  ${fence}

- **Cursor, Claude Desktop, and other clients configured with an \`mcpServers\` JSON block:** copy the
  \`"tapstate"\` entry from \`$agent_config\` into the client's MCP settings (for Cursor,
  \`~/.cursor/mcp.json\`; for Claude Desktop, Settings > Developer > Edit Config), then restart the client.
- **Codex:** add an \`[mcp_servers.tapstate]\` table to \`~/.codex/config.toml\` with the same \`command\`,
  \`args\` and \`env\` as the entry in \`$agent_config\`.

Every path in the entry is absolute, so it works from any folder on this machine.

## 3. Ask it something

- "Create a pipeline that keeps a live copy of the shipments in fulfillment_db with only their id,
  carrier and status, start it, and tell me its status."
- "Show me order 1 in views.order_state."
- "Is order_pipeline healthy? Explain how far behind it is."
- "Stop the pipeline you created, and delete it."

Whatever the agent creates also shows up in the browser, at http://127.0.0.1:8080.

## The token

The token in \`$agent_config\` is a write token for this demo's server: whoever holds it can create,
change, start, stop and delete sources and pipelines there. It is kept in that file only, readable by
you alone, and the quickstart never prints it.

To revoke it, list the tokens and revoke the agent's by its id:

${fence}sh
${cd_here}TAPSTATE_PASSWORD=<the password in .env> ./tapstate -c http://127.0.0.1:8080 -u admin token list
${cd_here}TAPSTATE_PASSWORD=<the password in .env> ./tapstate -c http://127.0.0.1:8080 -u admin token revoke <id>
${fence}

Deleting the demo with \`docker compose down -v\` invalidates it too; the next quickstart run writes a
new one.
EOF
}

# The closing message. It is the first screen a reader acts on, so it carries only the next moves and
# names the file that holds everything else. Every command on it runs in the demo directory, which the
# last line names, because a piped run made that directory itself and the reader's shell is not in it.
print_next_steps() {
    # The agent block names the files only when they exist; otherwise it says why they do not, so the
    # option is never advertised over a configuration that was never written.
    if [ -z "${agent_note:-}" ]; then
        agent_lines="    You describe what you want, and the agent creates and runs the sources and pipelines.
    1. Teach it Tapstate:  npx --yes skills add tapstate/tapstate-skills
    2. Connect it to this demo:
         Claude Code:     claude --mcp-config $agent_config
         another agent:   copy the \"tapstate\" entry from $agent_config into its MCP settings
    3. Ask it, for example:
         \"Create a pipeline that keeps a live copy of the shipments in fulfillment_db with
          only their id, carrier and status, start it, and tell me its status.\"
    Full walkthrough, with setup for other agents: AGENT-SETUP.md in this folder"
    else
        agent_lines="    Not set up: $agent_note"
    fi
    cat <<EOF

quickstart: done. The demo is running.

What you have: one MongoDB document per order, built from two databases -- the orders are in MySQL,
their shipments are in PostgreSQL -- and kept up to date as either one changes.

Three ways to use it:

  In the browser
    Open http://127.0.0.1:8080 and sign in as admin. The password is in .env.

  With an AI agent (Claude Code, Cursor, ...)
$agent_lines

  From the CLI
    ./tapstate -w work, then at its prompt: connect http://127.0.0.1:8080 ; login admin

See it stay in sync
  Run ./try-cdc.sh. It makes four small changes to the source databases, one at a time:
  it adds a shipment in PostgreSQL, renames a customer in MySQL, removes the shipment again,
  and adds a new order. After each change it prints the combined document from MongoDB, so
  you can see it update a moment later without anything being re-run.

Stop, clean up, or connect your own tools
  NEXT.md in this folder has the commands to pause and resume the demo, to delete it and its
  data, and to open the MongoDB store in a GUI on your machine.

If something looks wrong
  The full output of every setup step is in quickstart.log.

All commands above run from $here:
  cd $here
EOF
}

main() {
    # The piped form has no saved file and no stack beside it, so it takes a directory of its own --
    # everything this script adds must stay inside one removable directory. Either marker file says
    # "work here": the saved script is the download-then-run form, the compose file is a re-run of an
    # earlier one (piped re-runs land back in the same directory rather than nesting a second).
    # Whether this directory is ours decides what the teardown may offer. Working in place is a
    # supported form -- a saved script run where it sits -- and there the directory is the user's, with
    # their files in it.
    demo_dir_is_ours=no
    if [ ! -f ./quickstart.sh ] && [ ! -f ./docker-compose.yml ]; then
        mkdir -p tapstate-demo
        cd tapstate-demo
        demo_dir_is_ours=yes
        printf 'quickstart: working in %s\n' "$PWD"
    fi
    # Every command this script hands the reader runs in this directory, and a piped run made it, so the
    # reader's shell is not in it. A hint is therefore printed with the cd that makes it work as pasted.
    here="$(sh_quote "$PWD")"
    cd_here="cd $here && "

    # The whole product runs as a compose stack, so a machine without Docker is refused here, before
    # anything is downloaded -- an actionable sentence beats "docker: command not found" three
    # downloads later. The prepare-only test seam deliberately skips this: it exists to stop before
    # Docker, so it must not require it. The CLI alone needs neither; say where to get it.
    if [ -z "${TAPSTATE_QUICKSTART_PREPARE_ONLY:-}" ]; then
        command -v docker >/dev/null 2>&1 \
            || die "Docker is required to run the stack. Install Docker with the Compose v2 plugin, or install only the offline CLI:  curl -sSL https://install.tapstate.dev/cli | sh"
        docker compose version >/dev/null 2>&1 \
            || die "Docker is present but the Compose v2 plugin is not ('docker compose version' failed). Update Docker, or install only the offline CLI:  curl -sSL https://install.tapstate.dev/cli | sh"
    fi

    # Say what is about to happen before any of it does. A first reader does not expect one command to
    # start four databases' worth of containers, and a long quiet run they were not told about reads as
    # one that has gone wrong.
    cat <<'EOF'
quickstart: this sets up a local Tapstate demo in Docker: MySQL and PostgreSQL with sample orders and
            shipments, MongoDB, the Tapstate server, and one live pipeline that combines the two
            databases into MongoDB. The first run takes a few minutes while the downloads finish.
EOF

    # Where the stack's assets come from: the same release the CLI is pinned to, derived from that pin
    # rather than named separately. A branch would keep moving after the release, handing a later user a
    # CLI frozen at one version beside a compose file from another -- a mismatch that shows up only on
    # their machine. Deriving it means the release tag decides both, and there is no step to remember.
    qbase="${TAPSTATE_QUICKSTART_BASE_URL:-https://raw.githubusercontent.com/tapstate/tapstate/v${CLI_VERSION}}"

    work="$(mktemp -d)"
    trap 'rm -rf "$work"' EXIT INT TERM

    # Platform gate: fetch install.sh into the throwaway work area and reuse its detection. This refuses
    # an unsupported platform (Windows shell, musl, unknown OS/arch) before anything is written into the
    # demo directory, and shares one copy of the uname mapping rather than duplicating it here.
    fetch "${qbase}/install/install.sh" "$work/install.sh"
    if ! platform="$(sh "$work/install.sh" --print-platform)"; then
        exit 1   # install.sh already said why (musl / Windows / unknown), pointing to WSL or source
    fi

    # Fetch the stack into this directory, each asset only if absent, so a re-run neither re-downloads a
    # verified asset nor overwrites an edit the user made to it. The seed dir is created empty on purpose:
    # a registered jar's bytes live in the store, and the demo registers over the CLI upload path, so the
    # seed stays the documented empty convenience rather than the route registration depends on.
    #
    # Both seed directories are fetched, not just the one the demo pipeline reads. The compose file
    # mounts each of them, and a missing mount source is not an error Docker reports - it creates an
    # empty directory and starts a database with no demo data in it, which then fails much later as a
    # pipeline that reads nothing.
    mkdir -p mysql-init postgres-init connectors
    # Fetched from the tag, then pinned to this script's own version. A release tag's tree carries the
    # previous release's pins by construction: the number is written into the tree inside the release
    # runner, which never commits, so the tag lands on the commit CI validated and that tree still says
    # the release before this one. The two files the install site serves are rewritten on the way out,
    # which is why the CLI installs correctly -- everything fetched from the tag afterwards is not, and
    # the compose file is the one of those that carries a version.
    #
    # Left as it shipped, the one-liner brings up the previous release's server under this release's
    # CLI. Measured on 0.4.1: a 0.4.1 CLI against a 0.3.0 server, whose discovery endpoint the newer
    # CLI cannot use, so the demo could not log in at all.
    #
    # An existing file is never rewritten -- a re-run keeps the stack it already has, and a compose the
    # user edited is theirs.
    if [ ! -f ./docker-compose.yml ]; then
        fetch "${qbase}/deploy/quickstart/docker-compose.yml" ./docker-compose.yml
        sed "s|image: ghcr.io/tapstate/tapstate:[^[:space:]]*|image: ghcr.io/tapstate/tapstate:${CLI_VERSION}|g" \
            ./docker-compose.yml > ./docker-compose.yml.pinned \
            && mv ./docker-compose.yml.pinned ./docker-compose.yml
        # The substitution is checked, not assumed. A compose file that writes the image reference any
        # other way leaves this a silent no-op, and a silent no-op here is the whole defect coming back
        # -- with a stack that starts, reports healthy, and runs the wrong server.
        left="$(grep -c "image: ghcr.io/tapstate/tapstate:${CLI_VERSION}" ./docker-compose.yml || true)"
        other="$(grep -c "image: ghcr.io/tapstate/tapstate:" ./docker-compose.yml || true)"
        if [ "$left" = 0 ] || [ "$left" != "$other" ]; then
            die "the compose file does not pin the server image the way this script expects; it would have started a server other than ${CLI_VERSION}"
        fi
    fi
    [ -f ./mysql-init/01-orders.sql ]        || fetch "${qbase}/deploy/quickstart/mysql-init/01-orders.sql" ./mysql-init/01-orders.sql
    [ -f ./postgres-init/01-shipments.sql ]  || fetch "${qbase}/deploy/quickstart/postgres-init/01-shipments.sql" ./postgres-init/01-shipments.sql

    # Install the CLI in place as ./tapstate, reusing install.sh wholesale (download, checksum, atomic
    # place). TAPSTATE_INSTALL_DIR here is the seam that keeps it out of PATH: `rm -rf` of this directory
    # removes it. install.sh's own stdout (a PATH hint that does not apply in place) is dropped; its
    # errors still surface and abort under set -e.
    if [ ! -x ./tapstate ]; then
        # TAPSTATE_ENTRYPOINT tells the install event which of the two front doors this was, so the
        # two paths can be compared. stdout stays dropped; the installer's disclosure is on stderr and
        # still reaches the user.
        TAPSTATE_INSTALL_DIR="$PWD" TAPSTATE_VERSION="${TAPSTATE_VERSION:-$CLI_VERSION}" \
            TAPSTATE_ENTRYPOINT=quickstart \
            sh "$work/install.sh" >/dev/null
        # A binary fetched by a browser carries macOS's quarantine attribute, which blocks it from running
        # until cleared; install.sh's atomic move preserves it. Strip it -- only on macOS, only if xattr
        # is present, and tolerating the case where the attribute was never set.
        case "$platform" in
            darwin-*) if command -v xattr >/dev/null 2>&1; then xattr -d com.apple.quarantine ./tapstate 2>/dev/null || true; fi ;;
        esac
    fi

    # The demo connector jars. These three are what this release registers, and they are fetched so the
    # demo runs without the user choosing. They sit outside connectors/ so the seed dir stays empty.
    #
    # The postgres one is fetched for the same reason its seed is: the compose file has run a postgres
    # service with a seeded shipments table since the second engine was added, and a demo that fetches
    # only two jars can never read it. The half that is missing then is precisely the interesting one -
    # a row a user types into the second engine after the stack is up.
    cbase="${TAPSTATE_CONNECTORS_URL:-${TAPSTATE_BASE_URL:-https://github.com/tapstate/tapstate/releases}/download/connectors-preview}"
    [ -f ./mysql-connector.jar ]    || fetch "${cbase}/mysql-connector.jar" ./mysql-connector.jar
    [ -f ./mongodb-connector.jar ]  || fetch "${cbase}/mongodb-connector.jar" ./mongodb-connector.jar
    [ -f ./postgres-connector.jar ] || fetch "${cbase}/postgres-connector.jar" ./postgres-connector.jar

    # A random admin password replaces the shipped admin/admin default so a stack left running is not
    # trivially reachable. It is written only to .env (readable by this user alone) and announced once
    # here -- never passed as a CLI argument, so it stays out of the process table and shell history. A
    # re-run keeps the existing .env: regenerating it would lock the user out of the admin already
    # bootstrapped against the old password.
    if [ ! -f .env ]; then
        admin_pw="$(LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 24)"
        printf 'TAPSTATE_ADMIN_USER=admin\nTAPSTATE_ADMIN_PASSWORD=%s\n' "$admin_pw" > .env
        chmod 600 .env
        printf 'quickstart: generated a random admin password, saved to .env: %s\n' "$admin_pw"
    fi

    # Generate the demo workspace, unless one is already here: a re-run must not clobber edits the user
    # made to their resources.
    if [ ! -d work ]; then
        generate_workspace
    fi

    if [ -n "${TAPSTATE_QUICKSTART_PREPARE_ONLY:-}" ]; then
        return
    fi

    # Everything a step runs is written here; the reader sees one line per step instead.
    log="$PWD/quickstart.log"
    : > "$log"

    # Bring up the stack. The compose file pins the published image, so this pulls rather than builds.
    #
    # A first run spends most of its time downloading images, and that is shown: the fetches above draw
    # their progress for the same reason, so a long download never looks like a hang. Only when an image
    # is missing, though -- a pull also asks the registry about images already here, and a re-run must
    # still start offline. A compose too old to list its images falls back to the plain, visible `up`.
    if images="$(docker compose config --images 2>/dev/null)" && [ -n "$images" ]; then
        missing=""
        for image in $images; do
            docker image inspect "$image" >/dev/null 2>&1 || missing=yes
        done
        if [ -n "$missing" ]; then
            echo 'quickstart: downloading the container images (first run only)'
            docker compose pull \
                || die "the container images could not be downloaded; check the network and run this again"
        fi
        step_begin "starting the stack"
        if ! docker compose up -d >> "$log" 2>&1; then
            step_close
            tail -n 20 "$log" >&2
            die "the stack did not start; the last lines of quickstart.log are above"
        fi
    else
        docker compose up -d
        step_begin "starting the stack"
    fi

    # Both waits below poll on this interval. It is a knob only so the script's own test suite can drive
    # the waiting paths without spending a minute on each; a run that does not set it waits the same two
    # seconds it always has.
    poll="${TAPSTATE_QUICKSTART_POLL_SECONDS:-2}"

    # Wait until the server container reports healthy -- its image carries the /healthz healthcheck -- so
    # the online verbs are not driven before the server can answer.
    i=0
    while [ "$i" -lt 90 ]; do
        if docker compose ps --format json server 2>/dev/null | grep -q '"Health":"healthy"'; then
            break
        fi
        i=$((i + 1)); step_tick; sleep "$poll"
    done
    docker compose ps --format json server 2>/dev/null | grep -q '"Health":"healthy"' \
        || die "the server did not become healthy in time; inspect it with: ${cd_here}docker compose logs server"

    # Then wait for the first admin to actually exist. Server health is not that moment -- it is the
    # moment before it: the bootstrap sidecar declares depends_on the server being healthy, so health is
    # precisely when that container is cleared to start its one POST. Driving `login` off the health
    # check alone is a race with a one-request container, and it is a race this has lost in the wild.
    #
    # A one-shot container reports State and ExitCode, never Health, so both are checked: `exited` alone
    # would accept a bootstrap that ran and failed. A non-zero exit is reported immediately rather than
    # waited out -- the container is gone, so no amount of further waiting changes the answer.
    i=0
    while [ "$i" -lt 60 ]; do
        bootstrap_ps="$(docker compose ps -a --format json bootstrap 2>/dev/null)"
        if printf '%s' "$bootstrap_ps" | grep -q '"State":"exited"'; then
            printf '%s' "$bootstrap_ps" | grep -q '"ExitCode":0' \
                || die "the first admin could not be created; inspect it with: ${cd_here}docker compose logs bootstrap"
            break
        fi
        i=$((i + 1)); step_tick; sleep "$poll"
    done
    printf '%s' "$bootstrap_ps" | grep -q '"State":"exited"' \
        || die "the first admin was not created in time; inspect it with: ${cd_here}docker compose logs bootstrap"
    step_done "stack running: MySQL, PostgreSQL, MongoDB and the Tapstate server"

    # Drive the online verbs through the REPL, feeding the password on stdin (the login prompt reads the
    # next line) so it is never a process argument or a shell-history entry. Workspace paths resolve
    # against work/, so the jars beside it are ../<jar>.
    #
    # Each capture source is applied on its own first, then discovered, then everything is applied. Both
    # pipelines map a row field, and an expression that reads row fields is refused until the source it
    # reads has a discovered schema -- while a discovery, in turn, needs the source to exist on the
    # server. One apply for all of it therefore cannot succeed in either order: the batch is refused
    # whole, which leaves the sources unapplied and the discoveries with nothing to look at. The second
    # engine is under the same rule as the first, not exempt from it -- its pipeline maps a row field too.
    #
    # Applying the source twice is free; the second apply reports it unchanged.
    #
    # The REPL's output is captured so a failed login can be named here, and so a failed verb is seen at
    # all. Neither can be left to the row count below: that check fires half a minute later and says "the
    # target is empty", which sends the reader to the server log to investigate a pipeline that was never
    # started. Authentication is also the one failure that cascades -- every verb after it reports
    # cli.not-authenticated, so the real cause ends up at the top of a screen of consequences.
    admin_pw="$(sed -n 's/^TAPSTATE_ADMIN_PASSWORD=//p' .env)"
    step_begin "registering the connectors, creating the two sources, starting the pipeline"
    repl_status=0
    repl_out="$(printf 'connect http://127.0.0.1:8080\nlogin admin\n%s\nregister ../mysql-connector.jar\nregister ../mongodb-connector.jar\nregister ../postgres-connector.jar\napply source/orders_db.tap.yml\napply source/fulfillment_db.tap.yml\ndiscover-schema orders_db\ndiscover-schema fulfillment_db\napply\nstart order_pipeline\nexit\n' "$admin_pw" \
        | ./tapstate -w work 2>&1)" || repl_status=$?
    # The status is taken beside the output rather than being left to `set -e`. As the bare assignment
    # it was, a CLI that exits non-zero ends the script AT this line: the print below never runs, the
    # `case` below it never runs, and everything the CLI said -- a refusal, a stack trace, ten frames of
    # it -- reaches the exit code and nowhere else. The one report of that shape blamed the CPU, and the
    # same silence was reproduced later on hardware where that could not be the cause.
    #
    # The session goes to the log either way, and to the screen only when the run stops because of it:
    # a session that worked is a screen of prompts and checksums that says nothing a reader needs, and
    # one that did not is the first thing they need.
    printf '%s\n' "$repl_out" >> "$log"
    show_session() { step_close; printf '%s\n' "$repl_out"; }
    case "$repl_out" in
        *control.auth-failed*|*cli.not-authenticated*)
            # Ask the bootstrap what it actually did. "Created it" and "found one already there" are both
            # successes to that step -- an admin exists either way, which is what makes a re-run safe --
            # but only the second can explain a password that does not work: the store outlived the .env
            # the admin was made from. The two failures need different answers, and prescribing a volume
            # wipe for the wrong one destroys a user's data to fix nothing.
            bootstrap_said="$(docker compose logs --no-log-prefix --tail 1 bootstrap 2>/dev/null || true)"
            case "$bootstrap_said" in
                *"already exists"*)
                    # The rerun half of this advice has to match how the script was started. The piped
                    # form saves no copy of itself, so naming quickstart.sh there sends the user to a
                    # file that is not present -- after the volume wipe has already happened, which is
                    # the worst possible moment to hand someone a command that cannot work.
                    if [ -f ./quickstart.sh ]; then
                        again="sh quickstart.sh"
                    else
                        again="curl -sSL https://install.tapstate.dev/demo | sh"
                    fi
                    show_session
                    die "the CLI could not log in: this stack already had an admin from an earlier run, and the password in .env is not the one it was created with. Start clean with: ${cd_here}docker compose down -v && $again" ;;
                *)
                    show_session
                    die "the CLI could not log in, so no verb after it ran; inspect it with: ${cd_here}docker compose logs bootstrap" ;;
            esac ;;
    esac
    # Anything else the CLI failed on. Two guards, because what one can see the other cannot.
    #
    # A CLI that died -- a crash, a kill -- is what the exit status is for, and the failure it names is
    # the process, not the verb: the session ended, so nothing after it ran at all.
    if [ "$repl_status" -ne 0 ]; then
        show_session
        die "the CLI exited $repl_status before the pipeline was started; its output is above"
    fi
    # A verb that failed is what the exit status cannot see, and it is the case a session produces: a
    # rejected line is printed and the read loop takes the next one, so a session in which nothing worked
    # ends exactly the way a healthy one does -- 0. That is how a refused connector registration reached
    # the row-count wait, which then reported an empty target and sent the reader to the server log, half
    # a minute later, for a failure that was neither there nor the server's.
    #
    # The verbs' own output carries what the status cannot, in one of two shapes. A failure the CLI can
    # name is a coded diagnostic and opens with `error: <code>`, the header every face of the CLI renders
    # failures with. A refusal whose response body carried no code has none to open with, and arrives as
    # the client's own sentence for it -- `The server refused ...` -- under the same indent a message
    # gets, with no header at all. That is the shape the reported registration refusal took, so a guard
    # reading the header alone would have let exactly that one through. Either shape means a verb the run
    # depends on did not take, and the run stops at it rather than half a minute later at the row count.
    #
    # START is intentionally refused for a pipeline already running on a re-run.
    # Remove only that adjacent code/message pair from the guard input, preserving
    # every other diagnostic (including another illegal transition). Keep the full
    # original output above so the exception never hides what the CLI actually said.
    # These are text renderings, not a machine-readable CLI status contract.
    verb_failures="$(printf '%s\n' "$repl_out" | awk '
        /error: lifecycle[.]illegal-transition$/ {
            if (pending != "") print pending
            pending = $0
            next
        }
        pending != "" {
            if ($0 == "  Cannot start a pipeline in state RUNNING.") {
                pending = ""
                next
            }
            print pending
            pending = ""
        }
        { print }
        END { if (pending != "") print pending }
    ')"
    if printf '%s\n' "$verb_failures" | grep -q -e 'error: ' -e 'The server refused ' -e 'request failed: '; then
        show_session
        die "a verb failed during pipeline setup; the CLI's output is above"
    fi
    step_done "connectors registered: MySQL, PostgreSQL, MongoDB"
    step_done "sources created: orders_db (MySQL), fulfillment_db (PostgreSQL)"
    step_done "pipeline order_pipeline running"

    # Snapshot verification, printed automatically: the demo's payoff is a real row count in the target,
    # not an "it should have worked". A fresh snapshot of the seeded rows is quick, but the read still
    # retries so a slow first run is not misreported as an empty target.
    step_begin "waiting for the two databases to be combined in MongoDB"
    seeded_orders=5      # rows the demo seed puts in MySQL
    seeded_shipments=6   # rows the demo seed puts in PostgreSQL, spread unevenly across four orders
    # Both halves are waited on, and both are named if the wait runs out. The second number is the one
    # that sees the second engine: an assembly wired to MySQL alone writes all five orders, each with an
    # empty array, so a check on the document count alone reports that as a success - and the half left
    # out would be the second engine, which is the whole reason there is a second one.
    orders=0; shipments=0; i=0
    while [ "$i" -lt 30 ]; do
        orders="$(count_orders)"
        shipments="$(count_embedded_shipments)"
        [ "$orders" -ge "$seeded_orders" ] && [ "$shipments" -ge "$seeded_shipments" ] && break
        i=$((i + 1)); step_tick; sleep 2
    done

    # Falling out of that loop short is a failed run, and it has to be said with a non-zero exit. The
    # REPL above cannot say it: an interactive session does not end because one command was rejected,
    # so it exits 0 whether the verbs took or errored, and set -e sees nothing wrong. This count is the
    # only evidence the script has that a pipeline is moving data. The stack is left standing rather
    # than torn down -- the server log is the next thing to read, and a teardown would take it along.
    { [ "$orders" -ge "$seeded_orders" ] && [ "$shipments" -ge "$seeded_shipments" ]; } \
        || die "the two engines were not assembled (orders $orders of $seeded_orders, embedded shipments $shipments of $seeded_shipments); inspect it with: ${cd_here}docker compose logs server"
    step_done "$orders orders from MySQL, each with its shipments from PostgreSQL ($shipments in all), one MongoDB document per order"

    write_agent_config
    write_try_cdc
    write_next_md
    write_agent_md
    print_next_steps
}

main "$@"
