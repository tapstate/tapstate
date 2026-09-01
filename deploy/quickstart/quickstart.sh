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
CLI_VERSION="0.3.0"

die() {
    printf 'quickstart: %s\n' "$1" >&2
    exit 1
}

# Download $1 to the file $2 with whichever of curl / wget is present.
fetch() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$1" -o "$2"
    elif command -v wget >/dev/null 2>&1; then
        wget -q "$1" -O "$2"
    else
        die "neither curl nor wget is available to download $1."
    fi
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

# Closing instructions: how to watch the assembled object, exercise CDC on both engines, and remove
# everything. The teardown is printed because "back to a clean machine" is only honest if the images are
# called out too.
#
# Every wait printed here is bounded, and that is not tidiness. These lines are copied and pasted by
# somebody the script will never hear from again: an unbounded `until` returns in a second when the
# change arrives and hangs forever, silently, when it does not - and a recording of this demo cannot
# show that, because on the machine doing the recording it always returns. The bound is generous, taken
# for the slowest healthy machine rather than the fastest, so a normal run is never reported as a
# failure; when it does run out it says what was actually observed, which is what separates "nothing is
# arriving" from "this machine is slow": a count still at its starting value is the first, a count that
# moved and did not finish is the second.
print_next_steps() {
    demo_dir="$(basename "$PWD")"
    # Whole-directory removal is only ever offered for a directory this script made. Run in place --
    # a saved script executed where it sits -- the directory is the user's and holds their files, so
    # printing `rm -rf` on it puts a command that destroys unrelated work in front of someone who has
    # been told, correctly, that everything above was safe to copy.
    if [ "${demo_dir_is_ours:-no}" = yes ]; then
        removal_line="  cd .. && rm -rf $demo_dir  remove this directory (CLI, jars, workspace, .env)"
    else
        removal_line="  this directory is yours, so nothing here removes it. What the quickstart added:
    tapstate tap versions/ connectors/ *-connector.jar mysql-init/ postgres-init/ work/ .env docker-compose.yml"
    fi
    uri="mongodb://mongo:27017/views?directConnection=true"
    # How long a pasted wait keeps trying. Taken for the slowest healthy machine, not the fastest: a
    # single change crosses in about a second here, so a minute is not a guess at the duration but a
    # ceiling far above it - small enough that nobody sits in front of a hung terminal, large enough
    # that a loaded laptop is never told its working demo failed.
    cdc_wait_seconds=60
    cat <<EOF
quickstart: pipeline started. The stack is running.

What you have: one object per order, assembled out of two different databases. The orders are in
MySQL, their shipments are in PostgreSQL, and neither engine can see the other's table -- so this is
not a view and not a join. Tapstate keeps it fresh from both sides at once.

Look at it (from this directory):
  ./tapstate -w work        then: connect http://127.0.0.1:8080 ; login admin
  then, at the prompt:
    show collections                      three: the two sources, and views.order_state
    views.order_state.find({id:1})         one order, with its shipments inside it
    watch views.order_state {id:1}         the same object, redrawn as it changes
    status order_pipeline --watch          the pipeline behind it

See change-data-capture: change either database, watch the same object follow. Each wait below gives
up after ${cdc_wait_seconds}s, says what it saw, and answers non-zero -- so pasted into a script it
fails rather than passing quietly. It ends in \`false\` and not \`exit\` on purpose: \`exit\` would close
the shell you are standing in.

  # add a shipment in PostgreSQL -- it joins the array on order 1, which already has two
  docker compose exec postgres psql -U postgres -d appdb -c "INSERT INTO shipments VALUES (7,1,'ups','pending');"
  i=0; until docker compose exec -T mongo mongosh --quiet "$uri" --eval 'quit((db.order_state.findOne({id:1})?.shipments?.length ?? 0) >= 3 ? 0 : 1)'; do
    i=\$((i+1)); [ "\$i" -lt ${cdc_wait_seconds} ] || { echo "not assembled after ${cdc_wait_seconds}s; order 1 now holds \$(docker compose exec -T mongo mongosh --quiet "$uri" --eval 'print(db.order_state.findOne({id:1})?.shipments?.length ?? 0)') shipments (it started with 2). Look at: docker compose logs --tail 50 server"; break; }
    sleep 1
  done; [ "\$i" -lt ${cdc_wait_seconds} ] || false
  docker compose exec mongo mongosh --quiet "$uri" --eval 'db.order_state.find({id:1}).pretty()'

  # change the order itself in MySQL -- the parent column moves, the array stays where it is
  docker compose exec mysql mysql -uroot -psecret appdb -e "UPDATE orders SET customer='alicia' WHERE id=1;"
  i=0; until docker compose exec -T mongo mongosh --quiet "$uri" --eval 'quit(db.order_state.findOne({id:1})?.customer=="alicia"?0:1)'; do
    i=\$((i+1)); [ "\$i" -lt ${cdc_wait_seconds} ] || { echo "not updated after ${cdc_wait_seconds}s; order 1 still reads \$(docker compose exec -T mongo mongosh --quiet "$uri" --eval 'print(db.order_state.findOne({id:1})?.customer ?? "nothing")'). Look at: docker compose logs --tail 50 server"; break; }
    sleep 1
  done; [ "\$i" -lt ${cdc_wait_seconds} ] || false
  docker compose exec mongo mongosh --quiet "$uri" --eval 'db.order_state.find({id:1}).pretty()'

  # remove that shipment again -- the array shrinks back, in the same object
  docker compose exec postgres psql -U postgres -d appdb -c "DELETE FROM shipments WHERE id=7;"
  i=0; until docker compose exec -T mongo mongosh --quiet "$uri" --eval 'quit((db.order_state.findOne({id:1})?.shipments?.length ?? 0) <= 2 ? 0 : 1)'; do
    i=\$((i+1)); [ "\$i" -lt ${cdc_wait_seconds} ] || { echo "still there after ${cdc_wait_seconds}s; order 1 holds \$(docker compose exec -T mongo mongosh --quiet "$uri" --eval 'print(db.order_state.findOne({id:1})?.shipments?.length ?? 0)') shipments. Look at: docker compose logs --tail 50 server"; break; }
    sleep 1
  done; [ "\$i" -lt ${cdc_wait_seconds} ] || false

  # and a whole new order in MySQL, which arrives with no shipments yet
  docker compose exec mysql mysql -uroot -psecret appdb -e "INSERT INTO orders VALUES (6,'frank',60.00);"
  i=0; until docker compose exec -T mongo mongosh --quiet "$uri" --eval 'quit(db.order_state.countDocuments({id:6})?0:1)'; do
    i=\$((i+1)); [ "\$i" -lt ${cdc_wait_seconds} ] || { echo "order 6 did not arrive after ${cdc_wait_seconds}s; the view holds \$(docker compose exec -T mongo mongosh --quiet "$uri" --eval 'print(db.order_state.countDocuments())') orders (it started with 5). Look at: docker compose logs --tail 50 server"; break; }
    sleep 1
  done; [ "\$i" -lt ${cdc_wait_seconds} ] || false
  docker compose exec mongo mongosh --quiet "$uri" --eval 'db.order_state.find({id:6}).pretty()'

Reach the store from your own machine (optional -- the reads above go through the container):
  The store's port is not published, so nothing on your host can see it by default. To point a GUI or
  a driver at it, add  ports: ["127.0.0.1:27017:27017"]  to the mongo service in docker-compose.yml,
  re-run docker compose up -d, and connect with:
    mongodb://127.0.0.1:27017/views?directConnection=true
  Keep directConnection=true. This is a one-member replica set that registers its member under a name
  meaning something only inside the container, so a URI carrying replicaSet=rs0 makes the driver
  discover that name and dial an address on your own machine where nothing is listening.

Stop, and pick it up later (run in this directory):
  docker compose stop        stop the stack and keep its data (docker compose start resumes it)

Tear down -- this one is not reversible (run in this directory):
  docker compose down -v     stop the stack and delete its data (a re-run re-registers the connectors)
$removal_line
The pulled images remain; remove them with:  docker image rm <image>
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
    [ -f ./docker-compose.yml ]              || fetch "${qbase}/deploy/quickstart/docker-compose.yml" ./docker-compose.yml
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

    # Bring up the stack. The compose file pins the published image, so this pulls rather than builds.
    docker compose up -d

    # Both waits below poll on this interval. It is a knob only so the script's own test suite can drive
    # the waiting paths without spending a minute on each; a run that does not set it waits the same two
    # seconds it always has.
    poll="${TAPSTATE_QUICKSTART_POLL_SECONDS:-2}"

    # Wait until the server container reports healthy -- its image carries the /healthz healthcheck -- so
    # the online verbs are not driven before the server can answer.
    printf 'quickstart: waiting for the stack to become healthy'
    i=0
    while [ "$i" -lt 90 ]; do
        if docker compose ps --format json server 2>/dev/null | grep -q '"Health":"healthy"'; then
            break
        fi
        i=$((i + 1)); printf '.'; sleep "$poll"
    done
    printf '\n'
    docker compose ps --format json server 2>/dev/null | grep -q '"Health":"healthy"' \
        || die "the server did not become healthy in time; inspect it with: docker compose logs server"

    # Then wait for the first admin to actually exist. Server health is not that moment -- it is the
    # moment before it: the bootstrap sidecar declares depends_on the server being healthy, so health is
    # precisely when that container is cleared to start its one POST. Driving `login` off the health
    # check alone is a race with a one-request container, and it is a race this has lost in the wild.
    #
    # A one-shot container reports State and ExitCode, never Health, so both are checked: `exited` alone
    # would accept a bootstrap that ran and failed. A non-zero exit is reported immediately rather than
    # waited out -- the container is gone, so no amount of further waiting changes the answer.
    printf 'quickstart: waiting for the first admin to be created'
    i=0
    while [ "$i" -lt 60 ]; do
        bootstrap_ps="$(docker compose ps -a --format json bootstrap 2>/dev/null)"
        if printf '%s' "$bootstrap_ps" | grep -q '"State":"exited"'; then
            printf '%s' "$bootstrap_ps" | grep -q '"ExitCode":0' \
                || { printf '\n'; die "the first admin could not be created; inspect it with: docker compose logs bootstrap"; }
            break
        fi
        i=$((i + 1)); printf '.'; sleep "$poll"
    done
    printf '\n'
    printf '%s' "$bootstrap_ps" | grep -q '"State":"exited"' \
        || die "the first admin was not created in time; inspect it with: docker compose logs bootstrap"

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
    # The REPL's output is captured so a failed login can be named here. It cannot be left to the row
    # count below: that check fires half a minute later and says "the target is empty", which sends the
    # reader to the server log to investigate a pipeline that was never started. Authentication is also
    # the one failure that cascades -- every verb after it reports cli.not-authenticated, so the real
    # cause ends up at the top of a screen of consequences.
    admin_pw="$(sed -n 's/^TAPSTATE_ADMIN_PASSWORD=//p' .env)"
    repl_out="$(printf 'connect http://127.0.0.1:8080\nlogin admin\n%s\nregister ../mysql-connector.jar\nregister ../mongodb-connector.jar\nregister ../postgres-connector.jar\napply source/orders_db.tap.yml\napply source/fulfillment_db.tap.yml\ndiscover-schema orders_db\ndiscover-schema fulfillment_db\napply\nstart order_pipeline\nexit\n' "$admin_pw" \
        | ./tapstate -w work 2>&1)"
    printf '%s\n' "$repl_out"
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
                        again="curl -sSL https://install.tapstate.dev | sh"
                    fi
                    die "the CLI could not log in: this stack already had an admin from an earlier run, and the password in .env is not the one it was created with. Start clean with: docker compose down -v && $again" ;;
                *)
                    die "the CLI could not log in, so no verb after it ran; inspect it with: docker compose logs bootstrap" ;;
            esac ;;
    esac

    # Snapshot verification, printed automatically: the demo's payoff is a real row count in the target,
    # not an "it should have worked". A fresh snapshot of the seeded rows is quick, but the read still
    # retries so a slow first run is not misreported as an empty target.
    echo 'quickstart: waiting for the two engines to be assembled into one object'
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
        i=$((i + 1)); sleep 2
    done

    # Falling out of that loop short is a failed run, and it has to be said with a non-zero exit. The
    # REPL above cannot say it: an interactive session does not end because one command was rejected,
    # so it exits 0 whether the verbs took or errored, and set -e sees nothing wrong. This count is the
    # only evidence the script has that a pipeline is moving data. The stack is left standing rather
    # than torn down -- the server log is the next thing to read, and a teardown would take it along.
    { [ "$orders" -ge "$seeded_orders" ] && [ "$shipments" -ge "$seeded_shipments" ]; } \
        || die "the two engines were not assembled (orders $orders of $seeded_orders, embedded shipments $shipments of $seeded_shipments); inspect it with: docker compose logs server"
    printf 'quickstart: %s orders from MySQL, each carrying its shipments from PostgreSQL -- %s of them, in one object per order\n' \
        "$orders" "$shipments"

    print_next_steps
}

main "$@"
