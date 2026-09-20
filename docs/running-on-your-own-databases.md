---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/running-on-your-own-databases
---

# Running the server against databases you started yourself

The [quickstart](quickstart-online.md) brings up databases, the server and the first admin together,
which is the fastest way to see the product work. This page is the other shape: **you already have a
MySQL and a MongoDB, and you want the server to be an ordinary process on your machine** pointed at
them. It is the shape to use when the databases are ones you care about, when you want the server's
log in your own terminal, or when you are attaching a debugger to it.

Only the parts that differ are here. Getting connector jars, registering them and creating the first
admin are the same either way and are covered by the quickstart - do those once, from there.

## What the server needs

| | |
|---|---|
| **MongoDB, as a replica set** | The control plane stores its own state here, and it uses transactions - so a standalone `mongod` is not enough. A single-member set is fine. |
| **A database of its own** | Point the server at a database that is not your data. The examples below use `tapstate`. |
| **MySQL with binlog** | Only if you are capturing from it. The connector reads the binlog, so `binlog_format=ROW` and a user that may read it. |

The databases do not have to be in containers, and the server does not have to be on the same host as
either. What matters is that the addresses you give below are reachable **from the server**, because
that is where a connector runs.

## Start it

```sh
java -jar app-<version>-boot.jar \
  --role=all \
  --tapstate.store.mongo.enabled=true \
  --tapstate.store.mongo.uri="mongodb://127.0.0.1:27018/tapstate?replicaSet=rs0" \
  --tapstate.store.mongo.server-selection-timeout=5s \
  --tapstate.connectors.plugins-dir=/path/to/plugins
```

What each one is for:

- `--role=all` runs every role in one process. It is the single-process form; splitting roles across
  processes is a deployment choice, not something this page needs.
- `--tapstate.store.mongo.uri` is **where the control plane keeps its own state** - pipelines,
  schemas, users, operator state. It is not where your data goes; that is a source you declare later.
  Give it its own database name.
- `--tapstate.store.mongo.server-selection-timeout` bounds how long a wrong address takes to fail.
  Without it an unreachable Mongo looks like a slow start rather than a mistake.
- `--tapstate.connectors.plugins-dir` is where registered connector jars are unpacked. Point it at a
  directory that survives a restart and the connectors registered once stay registered.

It is up in a few seconds. Check it, and check it the right way. The server binds 8080
unless `SERVER_PORT` says otherwise; export its address once and the commands below follow
it:

```sh
export TAPSTATE_URL=http://127.0.0.1:8080              # match SERVER_PORT if you set one
curl -s --noproxy '*' "$TAPSTATE_URL/healthz"           # -> ok
```

> **`--noproxy '*'` is not decoration.** `curl` does not bypass a proxy for loopback addresses. With
> `http_proxy` set - which it often is - a request to `127.0.0.1:8080` goes to the proxy, and the
> proxy answers for it. A stopped server then reads as a `503` rather than as a refused connection,
> which is a confusing way to spend twenty minutes.

Then point the CLI at it and carry on with any tutorial:

```sh
tapstate -c "$TAPSTATE_URL" -u admin ls
```

## Addresses are resolved from the server, not from you

A connector runs inside the server, so `host:` and a Mongo URI in a source are resolved from
wherever the server is. Two cases catch people out:

- **Server on your machine, databases in containers.** Use the ports you published:
  `host: 127.0.0.1, port: 3307`, not the container name. A container name means nothing to a process
  outside the container network.
- **Server in the same compose network as the databases.** Use the service names - `host: mysql` -
  because that is what resolves there, and the published port is irrelevant.

Getting this wrong shows up as a connection failure at `discover-schema`, which is the first command
that actually reaches the database.

## When you start before the source schema has been discovered

`discover-schema` is a command you run, not something `start` does for you, and forgetting it is the
common first mistake on your own databases. `start` accepts the pipeline - nothing is wrong with the
definition - and the run then fails on the next convergence pass, so `status` is where you see it:

```
order_pipeline  failed
reason: actuation.source-schema-not-discovered
  Source `src_orders` needs a discovered schema before its tables can be selected.
why: the run failed, and said why: actuation.source-schema-not-discovered
  read       status.failure = actuation.source-schema-not-discovered
  next       tapstate logs order_pipeline
moving     not known -- the run failed
```

Run `discover-schema` for the source it names, then start again.

This refusal also applies when the pipeline's only output is a view over literally named tables. No
view rows are materialized until the source schema has been discovered, because that schema supplies
the columns and identity of the collection the view writes.

## When a pipeline says `running` but nothing arrives

`status` reports the pipeline's last published state, and then answers this question directly. A
pipeline whose plan cannot be built - a nest tree that is refused, for instance - fails while
reconciling, retries on the next tick, and keeps reporting `running`, because nothing has seen its
job die and reporting it as failed would be a guess. What `status` adds is that it says so:

```
order_pipeline  running
why: the server keeps failing to bring this pipeline up: 12 passes in a row have thrown
  read       metrics.reconcileFailuresInARow = 12
  read       status.state = running
  next       read the server's own log -- the reason is printed there once per pass
  cannot say whether the job itself is still alive: nothing here has seen it die, so the state
             stays running rather than being guessed into a failure
moving     not known -- no records counter is published (no live job)
lag        not published
```

The reason itself is in the server's log, once per tick:

```
io.tapstate.core.common.TapstateException: nest.embed-target-not-parent-key {embedPath=lines, ...}
```

So the status tells you to read the log, and how long it has been failing; the log tells you what
failed. Having the server's output in your own terminal is one of the reasons to run it this way.

## When a source refuses the connection

The connector is the only thing that knows why a database said no: the password, the permission, the
database that is not there. From outside it, the server can say only that the read failed and name the
exception class. So what the connector says about it is written into that pipeline's own tail:

```
tapstate(admin@127.0.0.1:8080)> logs order_pipeline
2026-09-15T08:21:04.318Z  ERROR  password authentication failed for user "orders_reader"
2026-09-15T08:21:04.502Z  WARN   Pipeline order_pipeline entered FAILED [connector.capture-failed]: its data-plane job died
```

The first line is the connector's; the second is the server's own account of the same event. You want
the first one - it names the thing to go and fix.

Two limits worth knowing before you go looking for a line that is not there. A connector's routine
progress chatter is deliberately kept out of this tail: it is a bounded window of recent lines, and
chatter would push the one line you came for out of it (raise the server's log level if you want it).
And a line can still arrive under no pipeline at all. What a connector says through the log it is
driven with is filed against its pipeline whatever thread writes it - a reconnect loop's error line is
in the tail. But the connector contract also carries a shared, process-wide channel that names no
pipeline of its own, and the driver a connector bundles may log through its own logger from a thread of
its own - a connection-pool monitor, a background reaper. Those reach the server's console filed under
no pipeline, because at that point nothing can say which run they belonged to. If the tail is quiet,
the server's own output is the next place to look.
