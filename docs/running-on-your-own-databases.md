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

## When a pipeline says `running` but nothing arrives

`status` reports the pipeline's last published state, and then answers this question directly. A
pipeline whose plan cannot be built - a nest tree that is refused, for instance - fails while
reconciling, retries on the next tick, and keeps reporting `running`, because nothing has seen its
job die and reporting it as failed would be a guess. What `status` adds is that it says so:

```
order_pipeline  running
why: the server keeps failing to bring this pipeline up: 12 passes in a row have thrown
  read       metrics.errorCount = 12
  read       status.state = running
  next       read the server's own log -- the reason is printed there once per pass
  cannot say whether the job itself is still alive: nothing here has seen it die, so the state
             stays running rather than being guessed into a failure
```

The reason itself is in the server's log, once per tick:

```
io.tapstate.core.common.TapstateException: nest.embed-target-not-parent-key {embedPath=lines, ...}
```

So the status tells you to read the log, and how long it has been failing; the log tells you what
failed. Having the server's output in your own terminal is one of the reasons to run it this way.
