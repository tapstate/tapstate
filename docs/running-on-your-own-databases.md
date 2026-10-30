---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/running-on-your-own-databases
---

# Running the server against databases you started yourself

Use this path when your databases already exist and the server runs as a process on your own
host. Complete [storage setup and first-administrator creation](storage/README.md) before using
the Pipeline tutorials. The Configuration, Durable cache and Application data databases must
have different names; the OP setup form defaults to `tapstate_config`, `tapstate_cache` and
`tapstate_views`. Keep them separate from the source databases you capture.

## Prerequisites and startup

- Configuration and Durable cache need MongoDB replica-set transactions. A single-member
  replica set is enough for a local installation; a standalone `mongod` is not.
- Use JDK 21, at least 4 GiB of JVM heap for the normal server, persistent installation
  files, and the packaged verified connector JARs. Sources such as MySQL also need their own
  capture permissions and CDC settings.
- Check reachability from the server. For TLS, Atlas egress access lists, credentials and custom
  CA support, follow the [connection requirements](storage/README.md#tls-and-atlas).

From the server distribution, initialize a new owner-only directory once and start with it:

```sh
./bin/tapstate-server installation --initialize \
  --tapstate.installation.directory=/var/lib/tapstate/installation
./bin/tapstate-server --role=all \
  --tapstate.installation.directory=/var/lib/tapstate/installation
```

Use the dedicated management address in `stage.json` to test/save three-layer storage. After
**Restart required**, restart with the same directory, run `installation --authorize`, and
create the first administrator on that management channel. Then use the normal service address
for ordinary login. The full [OP procedure](storage/README.md#install-op-and-create-its-first-administrator)
also covers a remote SSH tunnel, expired credentials and interrupted setup.

The public service defaults to port 8080; retain your deployment's port and TLS configuration.
A process health response is not a storage-readiness or Pipeline-result check. Once setup and
normal authentication are complete, inspect storage separately:

```sh
tapstate storage status
```

Do not use `tapstate.store.mongo.operator-state-database` to choose storage in this layout.
The cache and default target are selected by the persisted setup binding. Editing a startup URI
or creating a new installation directory does not migrate an existing installation.

## Give one Nest its own cache database

A Nest can name an additional database on the same bound cache deployment:

```yaml
transforms:
  - id: order_doc
    type: nest
    state:
      database: order_doc_state
    from: { o: orders, i: order_items }
    root:
      from: o
      key: [id]
      embed:
        - { from: i, on: { order_id: id }, as: array, path: items }
```

Without `state.database`, the Nest uses the bound Durable cache database. An override is a
name only, not another URI, credential or pool. The cache credential must have the required
permissions on the selected database, whose ownership and layout must pass controlled preparation.
Configuration, Application data and system databases are not valid destinations. Other operators
keep their own placement; necessary dead-letter facts remain protected in Configuration.

This section describes selecting placement for a new Nest. It does not supply a migration path
for an existing one. Changing the name does not copy state or authorize a new empty destination.
There is no supported manual copy of old `operator_state` records or continuation from an old
checkpoint against an empty database. Keep the original placement and backup and investigate
any placement refusal with the deployment maintainer. Do not manufacture cache loss to obtain
recovery permission. The [pending Pipeline recovery flow](storage/README.md#rebuild-a-pending-pipeline)
applies only when the runtime has retained the corresponding cache-replacement recovery record.

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
