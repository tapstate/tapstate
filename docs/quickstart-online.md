---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/quickstart-online
---

# Quick start: the online runtime (preview)

> **Preview / POC.** Tapstate's runtime is an early slice: a single-node, in-memory
> engine that executes your `.tap.yml` resources as live pipelines. It is enough to
> run a real end-to-end sync, but it is **not** production-hardened — see
> [Limitations](#limitations) before you rely on it. The offline authoring CLI is
> covered in the [main README](../README.md); this page is the runtime.
>
> **Recommended platform.** Docker with the Compose v2 plugin, plus the system versions
> the release you install names in its notes: a macOS version, and a glibc version on
> Linux. Both are measured from the binaries themselves and follow the machines that
> built them, so an older system may refuse to launch them — check yours with
> `sw_vers -productVersion` or `ldd --version`. You are not blocked from trying: the
> installer names what the build expects and continues, and whether it runs from there
> is yours to own.

What you'll do: bring up a Docker Compose stack — databases, the server, and the
first-admin bootstrap all seeded and started together — then drive two sources through
one pipeline from the CLI, so that rows from two different database engines are assembled
into a single object and kept fresh: snapshot first, then live change-data-capture (CDC).

The worked example is **MySQL + PostgreSQL → one materialized object**. An order lives in
MySQL, its shipments live in PostgreSQL, and neither database can see the other — so no
view and no join can produce the result. The runtime itself is connector-agnostic (every
connector is loaded through the same plugin interface), but this release **registers
MySQL, PostgreSQL and MongoDB only**: they are the connectors it supports end to end
today, and registering any other one is refused. The set grows as connectors are
certified.

## The one-command demo

`quickstart.sh` runs everything on this page for you: it makes itself a
`tapstate-demo` directory, fetches the stack, installs the CLI in place, generates
the demo workspace, brings the stack up, and runs the pipeline — then prints the
target row count and the commands to drive CDC and tear down:

```sh
curl -sSL https://install.tapstate.dev | sh
```

To read the script before running it, download it into a directory of your own
first — it then works right there:

```sh
mkdir tapstate-demo && cd tapstate-demo
curl -sSL https://install.tapstate.dev -o quickstart.sh
sh quickstart.sh
```

The rest of this page is that same flow by hand. Run it when you want to see the
online verbs the script drives, or to point the pipeline at your own databases.

> **Preview.** The script installs a released CLI binary and pulls a published
> server image; the version is pinned in the script, so the same script always
> installs the same stack.

## Prerequisites

- **Docker** with the **Compose v2** plugin (`docker compose version`). The stack is
  a single-node local demo: databases, server, and first-admin bootstrap come up
  together, on the loopback interface only.
- **The system version named in the release notes** — a macOS version on a Mac, a glibc
  version on Linux. A recommendation, not a gate; see the note at the top of this page.
- **JDK 21** (`java -version`) — only for the dev overlay below, which builds the
  server image from this checkout. The CLI needs no toolchain at all: it is installed
  as a native binary in step 3, and GraalVM is only for building it from source.

## 1. Get the stack

In this preview the compose file, the server image, and the CLI all come from the
source tree, so clone the repository and move into the quickstart directory:

```sh
git clone https://github.com/tapstate/tapstate.git
cd tapstate/deploy/quickstart
export COMPOSE_FILE=docker-compose.yml:docker-compose.dev.yml
```

Everything below runs from here — this is where the compose file lives, so
`docker compose …` finds it, and the jars and workspace you create sit alongside it.

`docker-compose.yml` on its own names the published server image and never builds,
because that is the file a user downloads into an empty directory where there is no
source tree. The `COMPOSE_FILE` line above adds `docker-compose.dev.yml`, which
builds the server from this checkout instead; every `docker compose …` below then
picks up both without repeating them. Set it once per shell — a new terminal needs
it again.

> **Forgetting it fails silently, and the symptom points at the wrong thing.** Without
> `COMPOSE_FILE`, `docker compose` reads `docker-compose.yml` alone: the stack comes up
> on the *published* server image, and every change in your checkout is simply absent.
> Nothing reports a missing variable — you see a product that behaves like an older
> release, so the natural next move is to go debug the code you just changed. Ask the
> stack which image it will actually run:
>
> ```sh
> docker compose config --format json \
>   | python3 -c 'import sys,json;print(json.load(sys.stdin)["services"]["server"]["image"])'
> ```
>
> `tapstate:dev` is the image built from this checkout; a `ghcr.io/...` one is a published
> release. Check this first whenever a change you know you made appears not to be there.

## 2. Bring up the stack

> **Preview.** With the development override in play, the server image is built
> locally on first run from a repackaged jar, so build that jar once:
> ```sh
> ( cd ../.. && mvn -pl app -am -DskipTests package )    # -> app/target/app-<version>-boot.jar
> ```
> Drop the override once the image is published and this becomes a plain pull.

Start everything:

```sh
docker compose up -d
```

That brings up four services:

- **MongoDB** as a single-member replica set (Tapstate's state store needs one),
  initiated automatically by its healthcheck.
- **MySQL** seeded with a demo `orders` table of five rows, with row-based binary
  logging on so CDC has a binlog to tail.
- the **server** (`--role=all`), published on **`127.0.0.1:8080`** only — an
  unauthenticated first run must not be reachable from other machines.
- a one-shot **bootstrap** that creates the first admin over the server's loopback,
  then exits.

The first admin defaults to **`admin` / `admin`**, which is fine for this
loopback-only demo. To set your own password, copy `.env.example` to `.env` and edit
`TAPSTATE_ADMIN_PASSWORD` before bringing the stack up.

Wait for the server to report healthy (first run also builds the image, so allow up
to a minute):

```sh
docker compose ps          # the "server" row should read "healthy"
```

### The server's address

This guide addresses the server as `http://127.0.0.1:8080` throughout. Export it once and
the shell commands below follow it:

```sh
export TAPSTATE_URL=http://127.0.0.1:8080
```

If 8080 is taken on your machine, move it in one place and use your port above:

- **the compose stack** - the published port in `deploy/quickstart/docker-compose.yml`,
  the left-hand side of `127.0.0.1:8080:8080`.
- **a server you run yourself** - the `SERVER_PORT` environment variable, see
  [Alternative: build and run the server from source](#alternative-build-and-run-the-server-from-source).

Two places cannot read a shell variable and so spell the address out: what you type at the
REPL prompt, and the MCP JSON configuration. Substitute your port there by hand.

## 3. Get the CLI

Install it right here in the demo directory — the same installer as a permanent
install, pointed at `.` so that deleting the directory later removes everything:

```sh
curl -sSL https://install.tapstate.dev/cli | TAPSTATE_INSTALL_DIR=. sh
```

(Building from source still works — `mvn -Pnative -pl cli -am -DskipTests package`
in the repository, needs GraalVM for JDK 21 — but nothing in this walkthrough
requires it.)

The source build also produces the MCP sidecar used by `tapstate mcp`; the
relocatable bundle keeps the launcher and sidecar together when installed.

The CLI drives both the offline authoring loop and the online verbs below. It runs
on the host and talks to the server over HTTP; put the bundle's `bin/tapstate` on
your `PATH`, or keep it here as `./tapstate-cli/bin/tapstate` as above. The sibling
`libexec` artifact is loaded only by `tapstate mcp`; every other CLI command stays
independent of Spring. Do not expose `cli/target/tapstate` directly when MCP is
needed: a global symlink must point at the bundle's `bin/tapstate`, or the launcher
cannot find the sidecar.

## 4. Get the connector jars

This walkthrough needs a MySQL, a PostgreSQL and a MongoDB connector. Prebuilt jars are
published as release assets — download them next to the compose file:

```sh
base=https://github.com/tapstate/tapstate/releases/download/connectors-preview
curl -fL -O "$base/mysql-connector.jar"
curl -fL -O "$base/postgres-connector.jar"
curl -fL -O "$base/mongodb-connector.jar"
```

These three are what this release registers, and they are published so this page runs
without building the connector repositories first. A jar declaring any other connector
is refused with `connector.not-official`, whether it is uploaded with `register` or
staged in the seed directory. They are shaded and carry their own drivers on an
isolated loader; `mysql-connector.jar` bundles Oracle MySQL Connector/J under GPL-2.0
with the Universal FOSS Exception (see [`NOTICE`](../NOTICE)).

## 5. Author the resources

A workspace is a folder partitioned by resource kind. Create three resources — one read
source per engine, and the single pipeline that assembles them:

```sh
mkdir -p work/source work/pipeline
```

The commands below name this workspace explicitly — the verbs take it as an argument
(`tapstate validate work`) and the REPL takes it as a flag (`tapstate -w work`).
Unnamed, the CLI falls back to its default workspace, `tap-work`, and finds nothing.
(`TAPSTATE_WORKDIR=work` in the environment does the same job for both.)

The connector configs address the databases by their **compose service names**
(`mysql`, `postgres`): the connector runs inside the server container, where those names
resolve and loopback is the server itself.

**There is no target resource here, and that is the shape rather than an omission.**
Declaring a `view` is the whole instruction to materialize it, into the managed state
store the deployment already runs. You never supply a store URI and never apply a store
of your own.

`work/source/orders_db.tap.yml` — the orders service's database (the demo MySQL):

```yaml
version: tapstate/v1
kind: source
id: orders_db
connector: mysql
config: { host: mysql, port: 3306, database: appdb, username: root, password: secret }
mode: cdc
tables: [ orders ]
```

`work/source/fulfillment_db.tap.yml` — the fulfillment service's database, on a
different engine (the demo PostgreSQL). Two settings are spelled differently from the
MySQL source above, and both are this connector's own spelling rather than a choice: the
account is `user` where MySQL says `username`, and a table is addressed by schema as well
as by database.

```yaml
version: tapstate/v1
kind: source
id: fulfillment_db
connector: postgres
config: { host: postgres, port: 5432, database: appdb, schema: public, user: postgres, password: secret }
mode: cdc
tables: [ shipments ]
```

`work/pipeline/order_pipeline.tap.yml` — one pipeline over both engines:

```yaml
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
```

**An order and its shipments live in two different databases, and neither can see the
other.** There is no SQL view and no join that could produce this document: the tables
are in separate engines, so an application would have to read both and stitch the result
itself, on every request. `nest` declares the stitch once, and Tapstate keeps the result
materialized and fresh as either side changes.

Reading the `nest` step:

| Key | What it says |
|---|---|
| `from` | short aliases for the tables this step reads — `order` is the MySQL table, `shipment` the PostgreSQL one |
| `root.from` / `root.key` | which side is the document, and the column that identifies it |
| `embed[].on` | `{ <child column>: <parent column> }` — how a shipment finds its order |
| `embed[].as` / `path` | placed as an array, at the field `shipments` on the order |
| `embed[].arrayKey` | what identifies an element already in that array, so a changed shipment moves the element it belongs to instead of appending a second copy |

> **A PostgreSQL table you embed needs `REPLICA IDENTITY FULL`.** By default PostgreSQL publishes only
> the primary key when a row is updated or deleted, and a key alone does not say which parent the row was
> hanging under — so a deleted shipment would leave the array it was in unchanged, with every other
> reading healthy. The demo's seed sets it; do the same on your own tables:
>
> ```sql
> ALTER TABLE shipments REPLICA IDENTITY FULL;
> ```
>
> MySQL needs nothing here: its binary log carries the whole previous row already.

Two settings outside the step matter as much:

- **`mode: cdc` on both sources.** A source left on a snapshot contributes its seeded
  rows and then goes quiet — and the assembled object still looks complete, because the
  other side keeps it moving. Which side went silent is not visible from outside.
- **`read_mode: snapshot_and_cdc`** on the pipeline: the seeded rows arrive by snapshot,
  everything typed afterwards arrives by change stream.

> Declaring a `view` is the whole instruction to materialize it: the rows land in the
> managed state store as the collection the view names — `views.order_state` here —
> and no `serve` block is needed. `view.from` names the **last** step you want
> materialized (`assemble` here); it takes a transform `id` or a concrete resource,
> **not** a regex such as `/.*/`. `primary_key` is the column documents are addressed
> by, and the collection is indexed uniquely on it — without it the run is refused when
> it goes to materialize, which is after `validate` has already passed.
>
> A `serve` block still exists, and is what you write to deliver to a system outside
> Tapstate. The two are independent: a pipeline may carry either, or both.

> **Numeric columns cannot be used in CEL expressions in this preview.** Row values
> reach CEL as the connector produced them, so an `int` or `decimal` column matches no
> CEL overload — `=string(after.amount)` and `=after.id > 2` both compile offline and
> then fail at runtime with `No matching overload`. This pipeline names no columns in
> expressions at all, so it does not meet the limit; a `map` step you add later would.

Validate offline before going online (no server needed):

```sh
./tapstate-cli/bin/tapstate validate work       # expects: valid: 3 resources in work
```

## 6. Save a context, sign in, and run

Create a named context once, bind it to this workspace, and sign in from the same
session. The password prompt is masked. The CLI does not accept a password option: use the prompt or
`TAPSTATE_PASSWORD` for non-interactive use.

```console
$ ./tapstate-cli/bin/tapstate -w work
tapstate(offline:work)> :ctx
Context action: Create a context
Context name: local
Server URL: http://127.0.0.1:8080
Verify TLS [Y]:
Bind local to /.../work [Y]:
created context local
bound local to /.../work
tapstate(offline:work)> auth login admin
Password:                       # the admin password from step 2 (not echoed)
signed in as admin (read, write, admin)
tapstate(admin@127.0.0.1:8080)> register ../mysql-connector.jar
tapstate(admin@127.0.0.1:8080)> register ../postgres-connector.jar
tapstate(admin@127.0.0.1:8080)> register ../mongodb-connector.jar
tapstate(admin@127.0.0.1:8080)> apply source/orders_db.tap.yml
tapstate(admin@127.0.0.1:8080)> apply source/fulfillment_db.tap.yml
tapstate(admin@127.0.0.1:8080)> discover-schema orders_db
tapstate(admin@127.0.0.1:8080)> discover-schema fulfillment_db
tapstate(admin@127.0.0.1:8080)> apply
tapstate(admin@127.0.0.1:8080)> start order_pipeline
```

The context stores the server target and workspace binding. The CLI stores a revocable
opaque session separately, never the password or access token. After a process restart,
the workspace binding selects `local` and the first online command resumes that session:

```console
$ ./tapstate-cli/bin/tapstate -w work
tapstate(offline:work)> ls pipeline
resumed admin@local
```

Use `./tapstate-cli/bin/tapstate auth status --context local` to inspect the saved
session and `./tapstate-cli/bin/tapstate auth logout --context local` to revoke it and
remove the local cache. `connect` remains a temporary diagnostic connection: a later
`connect` and `login` change only the current REPL process and do not update a context or
save a session.

For automation, pass a machine token at launch with `--token TOKEN` or
`TAPSTATE_TOKEN`. It wins over a cached human session for this process, is never read
from or written to the human-session cache, and is not printed by CLI diagnostics. The
CLI performs anonymous server discovery before attaching the bearer. Pair it with a
temporary target when no context is selected:

```console
$ TAPSTATE_TOKEN=... ./tapstate-cli/bin/tapstate --connect http://127.0.0.1:8080 ls pipeline
```

In an interactive process, `auth status` reports that the machine token is selected and
`auth logout` (or bare `logout`) only clears that in-process token; neither action
requires a context or contacts the server. `auth login` is intentionally unavailable
while a machine token is selected.

- **`register`** uploads a connector jar to the server (content-addressed and
  idempotent; re-registering the same jar is a no-op). Its paths resolve against the
  workspace root — `work/` here — which is why the jars beside it are reached as
  `../mysql-connector.jar`. An absolute path works too, as does naming a directory:
  `register ..` uploads every `*.jar` under it as one batch.
- **`apply`** with no argument applies the whole workspace as one batch. The batch is
  the reference closure — a pipeline and the sources it names must be applied
  together, so apply the workspace, not one file at a time.
- **Each capture source is applied on its own first**, which is the one exception to
  that. A discovery has to be asked for before the pipeline is applied, and a discovery
  needs the source to already exist on the server — so a single batch carrying both
  cannot succeed in either order. Applying a source twice costs nothing; the second
  apply reports it unchanged.
- **`discover-schema orders_db`** reads the source schema and derives the target model
  and primary key. Run it **after** that source is applied and **before** the apply that
  carries the pipeline — and run it **once per source**, so `fulfillment_db` needs its
  own. The second engine is not exempt; it is half the document.
- **`start order_pipeline`** submits the pipeline: it reads the current rows of both
  engines (snapshot), then tails both change streams. **One start, because there is one
  pipeline** — the assembly is what makes it one, and two pipelines would be two
  collections standing next to each other rather than one object.

## AI-driven alternative: run the pipeline through MCP

The local MCP sidecar lets an MCP-capable coding agent perform the online part of
the same workflow. The sidecar is a foreground stdio process. It does not contain a
model, start a Tapstate Server, or access the state store directly; every tool call
uses the Server's authenticated HTTP control API.

Register connector jars and create a revocable machine token from an authenticated
CLI session first:

```console
tapstate(admin@127.0.0.1:8080)> register ../mysql-connector.jar
tapstate(admin@127.0.0.1:8080)> register ../postgres-connector.jar
tapstate(admin@127.0.0.1:8080)> register ../mongodb-connector.jar
tapstate(admin@127.0.0.1:8080)> token create --scope write
created <token-id> WRITE
token <one-time-token>
```

The bearer value is shown only once. Inject it into the MCP process environment; do
not put it in command arguments:

```json
{
  "mcpServers": {
    "tapstate": {
      "command": "/absolute/path/to/tapstate",
      "args": ["mcp", "--server", "http://127.0.0.1:8080", "--allow-write"],
      "env": {
        "TAPSTATE_TOKEN": "<one-time-token>",
        "MYSQL_PASSWORD": "secret"
      }
    }
  }
}
```

`--server` wins over `TAPSTATE_SERVER_URL`; the final default is
`http://127.0.0.1:8080`. There is intentionally no `--token` option. Without
`--allow-write`, the sidecar exposes exactly the 10 read tools. With it, five write
tools are added, but the Server still enforces the token scope. A read token cannot
write even when the tools are locally visible.

An agent should use this sequence:

1. Call `connector_list`, then `connector_get` for each required connector.
2. Build each Source envelope from the DSL semantics and build its `config` only
   from the complete live connector spec returned by `connector_get`.
3. Call `source_draft` to validate the structured Source view and render canonical
   YAML without persistence, then call `connection_test` and
   `connection_discover_schema`. Source config may contain `${NAME}` or
   `${var:NAME:default}` references; the sidecar expands them only inside `config`
   immediately before the HTTP request.
4. Author the complete `tapstate/v1` workspace and send every resource as a YAML
   draft to `artifact_validate`. Fix all diagnostics before `artifact_apply`.
5. Call `pipeline_start`, then use `pipeline_status`, `pipeline_metrics`,
   `pipeline_snapshot`, and `pipeline_logs` until the expected state and data are
   visible. Finish with `pipeline_stop`.

`source_draft` refuses to guess connector fields. If the connector is bundled-only,
its runtime is unavailable, or the live response has no complete spec and content
hash, the call fails before rendering the Source YAML. It does not create an artifact
or audit record. Secret values are returned only in redacted Source views; the MCP
tool result exposes configured secret field names, not their values.

Revoke the credential when the automation no longer needs it:

```console
tapstate(admin@127.0.0.1:8080)> token revoke <token-id>
```

Revocation is immediate. The next MCP request using that token is rejected by the
Server. Closing the MCP host's stdin stops the foreground sidecar; there is no MCP
tool for stopping its own process.

## 7. Observe and verify

```console
tapstate(admin@127.0.0.1:8080)> status order_pipeline --watch    # live state; Ctrl-C to stop
tapstate(admin@127.0.0.1:8080)> metrics order_pipeline           # recordCount / errorCount / per-table offset
tapstate(admin@127.0.0.1:8080)> logs order_pipeline              # node-local operational log tail
```

- The read faces lag the write verbs: they report observed state, which converges to
  what you asked for rather than changing with the command. Immediately after `start`
  the first `status`/`metrics` may report no observation yet, and a `status` right
  after `stop` can still say `running`. Use `--watch`, or retry after a second.
- `metrics` is the signal for progress: `recordCount` climbing, `errorCount` at 0.
- **Metric names are unstable in this preview.** They may be renamed as the metric model
  settles, so treat them as something to read, not something to build on: a dashboard or
  an alert wired to these names will need revisiting. The `metrics` output says so too.
  The lifecycle state in `status` is not affected — that one is a stable contract.

Verify the objects landed, straight from the store — `mongosh` runs inside the Mongo
container, so no client is needed on the host:

```sh
# five orders, one document each
docker compose exec mongo mongosh --quiet \
  "mongodb://mongo:27017/views?directConnection=true" \
  --eval "db.order_state.countDocuments()"    # should reach 5

# and the second engine inside them: six shipments, spread over four of those orders
docker compose exec mongo mongosh --quiet \
  "mongodb://mongo:27017/views?directConnection=true" \
  --eval 'db.order_state.find({}, {id:1, customer:1, "shipments.carrier":1}).pretty()'
```

**Count the array elements, not the documents, when you want to know the second engine
arrived.** An assembly wired to MySQL alone still writes all five orders — each with an
empty array — so a document count of 5 is satisfied by a demo that is only half working.
Order 5 has no shipments at all, which is deliberate: an implementation that invents an
empty array for it, or that drops it, is wrong in a way a uniform seeding would hide.

To point a GUI or a driver at the store from your own machine instead, publish the
port first — the stack does not, so nothing on the host can see it by default. Add
`ports: ["127.0.0.1:27017:27017"]` to the `mongo` service in `docker-compose.yml`,
re-run `docker compose up -d`, and connect with:

```text
mongodb://127.0.0.1:27017/views?directConnection=true
```

`directConnection=true` is not optional here. The stack runs a one-member replica set
whose member is registered under a name that resolves only inside the container, so a
URI carrying `replicaSet=rs0` makes the driver discover that name and dial an address
on your own machine where nothing is listening — the connection is refused against
your own loopback, which reads like a firewall problem and is not one.

## 8. Exercise change-data-capture

Change either database and watch the same object follow. This is the part worth doing by
hand: the two halves are in engines that cannot see each other, and both of them reach one
document.

Each wait below gives up after 60 tries, says what it saw, and answers non-zero. That matters more
than it looks: an unbounded wait returns in a second when the change arrives and hangs forever, in
silence, when it does not — and a bounded one that gave up quietly would pass in any script that
pasted it. The give-up is `false` rather than `exit` so that a shell you are standing in survives it.

```sh
uri="mongodb://mongo:27017/views?directConnection=true"

# a shipment added in PostgreSQL joins the array on order 1, which already has two
docker compose exec postgres psql -U postgres -d appdb -c "INSERT INTO shipments VALUES (7,1,'ups','pending');"
i=0; until docker compose exec -T mongo mongosh --quiet "$uri" --eval 'quit((db.order_state.findOne({id:1})?.shipments?.length ?? 0) >= 3 ? 0 : 1)'; do
  i=$((i+1)); [ "$i" -lt 60 ] || { echo "not assembled after 60s; order 1 holds $(docker compose exec -T mongo mongosh --quiet "$uri" --eval 'print(db.order_state.findOne({id:1})?.shipments?.length ?? 0)') shipments (it started with 2). Look at: docker compose logs --tail 50 server"; break; }
  sleep 1
done; [ "$i" -lt 60 ] || false
docker compose exec mongo mongosh --quiet "$uri" --eval 'db.order_state.find({id:1}).pretty()'

# the order itself changes in MySQL: the parent column moves, the array stays put
docker compose exec mysql mysql -uroot -psecret appdb -e "UPDATE orders SET customer='alicia' WHERE id=1;"
i=0; until docker compose exec -T mongo mongosh --quiet "$uri" --eval 'quit(db.order_state.findOne({id:1})?.customer=="alicia"?0:1)'; do
  i=$((i+1)); [ "$i" -lt 60 ] || { echo "not updated after 60s; order 1 still reads $(docker compose exec -T mongo mongosh --quiet "$uri" --eval 'print(db.order_state.findOne({id:1})?.customer ?? "nothing")'). Look at: docker compose logs --tail 50 server"; break; }
  sleep 1
done; [ "$i" -lt 60 ] || false

# and removing that shipment shrinks the array back, in the same document
docker compose exec postgres psql -U postgres -d appdb -c "DELETE FROM shipments WHERE id=7;"
i=0; until docker compose exec -T mongo mongosh --quiet "$uri" --eval 'quit((db.order_state.findOne({id:1})?.shipments?.length ?? 0) <= 2 ? 0 : 1)'; do
  i=$((i+1)); [ "$i" -lt 60 ] || { echo "still there after 60s; order 1 holds $(docker compose exec -T mongo mongosh --quiet "$uri" --eval 'print(db.order_state.findOne({id:1})?.shipments?.length ?? 0)') shipments. Look at: docker compose logs --tail 50 server"; break; }
  sleep 1
done; [ "$i" -lt 60 ] || false
```

A new order inserted into MySQL arrives with no shipments yet, and gains them as the
fulfillment side catches up — which is the ordinary case, not an edge one: the two engines
are independent and neither waits for the other.

## 9. Tear down

In the REPL:

```console
tapstate(admin@127.0.0.1:8080)> stop order_pipeline
tapstate(admin@127.0.0.1:8080)> exit
```

Then stop the stack and delete its data:

```sh
docker compose down -v     # stops every service and drops the named volumes
```

`down -v` discards the store, so a re-run re-registers the connectors from scratch.
The pulled/built images remain — remove them with `docker image rm <image>` if you
want the machine back exactly as it was. The jars, `work/`, and `.env` you created
here are just files; delete them as usual.

## Alternative: build and run the server from source

Prefer to run the server process directly on the host — to attach a debugger, or to
iterate on server code — rather than in a container? The flow is the same; only how
the server and databases are hosted changes.

1. **Build** the server jar and the CLI:

   ```sh
   mvn -DskipTests install          # -> app/target/app-<version>-boot.jar (the runtime server)
   mvn -Pnative -pl cli -am -DskipTests package   # -> cli/target/tapstate
   ```

2. **Databases on the host.** The compose stack publishes no host ports for MongoDB
   and MySQL, so run your own with ports exposed (or point at databases you already
   have). MongoDB must be a single-node replica set advertising `127.0.0.1`; MySQL
   needs row-based binary logging for CDC:

   ```sh
   docker run -d --name tapstate-mongo -p 27017:27017 mongo:7 --replSet rs0
   until docker exec tapstate-mongo mongosh --quiet --eval 'db.runCommand({ping:1})' >/dev/null 2>&1; do sleep 2; done
   docker exec tapstate-mongo mongosh --quiet --eval \
     "rs.initiate({_id:'rs0',members:[{_id:0,host:'127.0.0.1:27017'}]})"

   docker run -d --name tapstate-mysql -e MYSQL_ROOT_PASSWORD=secret -e MYSQL_DATABASE=appdb \
     -p 3306:3306 mysql:8.0 \
     --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --gtid-mode=ON --enforce-gtid-consistency=ON
   until docker exec tapstate-mysql mysqladmin ping -uroot -psecret --silent 2>/dev/null; do sleep 2; done
   docker exec -i tapstate-mysql mysql -uroot -psecret appdb < deploy/quickstart/mysql-init/01-orders.sql

   docker run -d --name tapstate-postgres -e POSTGRES_PASSWORD=secret -e POSTGRES_DB=appdb \
     -p 5432:5432 postgres:16 -c wal_level=logical
   until docker exec tapstate-postgres pg_isready -U postgres >/dev/null 2>&1; do sleep 2; done
   docker exec -i tapstate-postgres psql -U postgres -d appdb < deploy/quickstart/postgres-init/01-shipments.sql
   ```

   The seeding lines use the **same** files the compose stack does — one sample, not two
   that drift. Two settings are not optional: `wal_level=logical` cannot be added after the
   fact, and the seed file's `REPLICA IDENTITY FULL` is what lets a deleted shipment leave
   the array it was in (see the note in [step 5](#5-author-the-resources)).

3. **Start the server** on the host with JDK 21, pointing it at your Mongo:

   ```sh
   mkdir -p ./plugins       # a writable cache the server unpacks registered connectors into
   java -jar app/target/app-<version>-boot.jar --role=all \
     --tapstate.store.mongo.uri="mongodb://127.0.0.1:27017/tapstate?replicaSet=rs0" \
     --tapstate.connectors.plugins-dir=./plugins
   ```

   It listens on port **8080**. If that port is taken, set `SERVER_PORT` to another one
   before starting it, and point the rest of this section at the same value:

   ```sh
   export TAPSTATE_URL=http://127.0.0.1:8080     # match SERVER_PORT if you set one
   ```

   A Hazelcast `--add-opens` warning at startup is harmless.

4. **First admin.** There is no bootstrap sidecar here, so create the first user with
   a one-time, localhost-only `curl` (a `204 No Content` means success):

   ```sh
   curl -X POST "$TAPSTATE_URL/auth/bootstrap" \
     -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin123"}'
   ```

5. **Resources.** Use the same three resources as [step 5](#5-author-the-resources),
   with one change: the server now runs on the host, not in the compose network, so
   the connectors address the databases by their host ports instead of the compose
   service names — `config: { host: 127.0.0.1, port: 3306, … }` in `orders_db` and
   `config: { host: 127.0.0.1, port: 5432, … }` in `fulfillment_db`. Both of them, not one:
   a source left pointing at a compose service name resolves to nothing from the host, and
   the pipeline names every source it reads. The pipeline itself needs no change — the
   managed store it materializes into is addressed by the server, through the
   `--tapstate.store.mongo.uri` you passed in step 3, not by a resource here.

6. **Online verbs, observe, CDC** are identical to steps 6–8, except you reach the
   databases with your own client (`docker exec tapstate-mysql …` /
   `docker exec tapstate-mongo mongosh "mongodb://127.0.0.1:27017/views" …`)
   rather than `docker compose exec`.

Tear down with `docker rm -f tapstate-mysql tapstate-postgres tapstate-mongo` and `Ctrl-C` in the
server's terminal.

## Limitations

This runtime is a preview. Known constraints in this slice:

- **The bundled store is not a security boundary.** The `mongo` service runs without
  `--auth`, and the server's control-plane data — users, tokens, audit, connection
  configuration, applied artifacts — shares that instance with everything your
  pipelines write. A holder of a Tapstate token can point an ordinary `kind: source`
  at the `tapstate` database and read it: valid DSL, not a bypass. The container
  publishes no host port, so the threshold is a token rather than network reach. Do
  not put data in this deployment that its own users should not see; isolating the two
  needs authentication or a second instance, and this preview has neither.
- **Single node, in-memory.** No multi-node HA. A server restart does **not** resume
  from a persisted offset — it replays from the source (idempotent upsert absorbs the
  overlap). Durable resume / exactly-once are not in this preview.
- **Preview builds.** Until the first release, the server image is assembled locally
  and the CLI is built from source; a published image and a CLI installer remove
  those steps.
- **`logs` is thin.** The per-pipeline `logs` face is a node-local operational tail
  and is often sparse; full runtime detail is in the server process log.
- **No CLI bootstrap verb.** The compose stack creates the first admin for you; on
  the from-source path it is the `curl` above.
- **Temporary connections and machine tokens are process-scoped.** A persistent human
  session is created only by `auth login` against a named context; `connect`,
  `--connect`, `--token`, and `TAPSTATE_TOKEN` never create or update that cache.
