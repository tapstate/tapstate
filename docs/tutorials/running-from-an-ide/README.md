---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/tutorials/running-from-an-ide
---

# Running the server and CLI from an IDE

The [online quickstart](../../quickstart-online.md) runs a released image out of compose. This page
runs **the code you are editing**: the server started from IntelliJ IDEA, and the CLI driving it from
a terminal, against a MongoDB you supply yourself - so a change to a source file is one click away
from a running server.

It stops at the point where the CLI is connected and authenticated. That is deliberately short of
doing anything with data - from there any tutorial applies, and
[assembling one document out of many tables](../nest-document-assembly/) is the one to read next.

Time: about 20 minutes. You need a checkout, JDK 21, Docker, and IntelliJ IDEA.

## 1. What you are going to run

| Process | Module | Main class | What it is |
|---|---|---|---|
| server | `app` | `io.tapstate.app.Bootstrap` | The Spring Boot service. Embeds Hazelcast (5701) and Tomcat (8080). Connectors run inside it. |
| CLI | `cli` | `io.tapstate.cli.Cli` | picocli and JLine. Authors YAML offline; drives the server once connected. |

The CLI's native binary (`-Pnative`) matters only when cutting a release. For development, run the thin
jar from a terminal or the main class from the IDE, and skip native-image entirely.

**One database is required and one is not.** MongoDB is the server's own state store - schemas, users,
operator state, checkpoints - and the server does not start without it. A *source* database is a
tutorial's business, not the server's, so nothing here needs MySQL.

## 2. Install the reactor once, from a terminal

Suggested rather than required, before opening the IDE. `-pl … -am package` works on a fresh checkout,
but installing once makes every later single-module build much faster.

```sh
# Maven defaults to whichever JDK it finds; the repository targets 21, so say so explicitly
export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
cd <your-checkout>/tapstate
mvn -DskipTests install
```

### What goes wrong here

**Do not let the checkout move while it is building.** The version is a single `${revision}` property
and every module imports `io.tapstate:bom` with `<scope>import</scope>`. The reactor reads the version
once at startup, while the flatten plugin re-reads the pom on disk per module. If someone pulls the
checkout to a different version mid-build, you get this:

```
Non-resolvable import POM: io.tapstate:bom:pom:0.2.1 (absent)
'dependencies.dependency.version' for org.junit.jupiter:junit-jupiter:jar is missing.
```

It reads as though some pom forgot a version number, and it has nothing to do with the module it
names: the reactor built as one version, flatten later read another, and went looking for a bom that
did not exist at the time. **The test is whether `Reactor Summary for Tapstate <version>` at the top
of the log matches `<revision>` in the pom.** If they disagree, that is the cause, and rerunning is
the fix. Working in your own worktree avoids it.

**Do not read Maven's outcome from an exit code that went through a pipe.** `mvn … | tail` reports the
status of the last stage of the pipeline, not Maven's, so `BUILD FAILURE` arrives as success. `-q` is
worse: it swallows the `Tests run:` lines and the `BUILD` line together, so a failed run and a
successful one produce the same log. The only signal is `[INFO] BUILD SUCCESS` in the output.

## 3. Import into IDEA

- **Open** and select `tapstate/pom.xml`, then *Open as Project*. Select the pom, not the directory.
- **Project Structure -> Project SDK**: JDK 21, language level 21. The repository targets 21; on 17 it
  fails immediately in `core-model`.
- **Settings -> Build Tools -> Maven -> Runner -> JRE**: 21 as well. This is a separate setting from
  the Project SDK, and setting only one of them is not enough.
- Reload all Maven projects from the Maven panel and let the index finish.

If the Maven panel reports `io.tapstate:bom` unresolved after that, go back and finish step 2. It is
not an IDE problem.

## 4. Start MongoDB

It must be a **replica set**, not a standalone: checkpointing compares and swaps inside a
multi-document transaction, and MongoDB offers transactions only on a replica set.

```sh
docker run -d --name tapstate-dev-mongo -p 27117:27017 \
  mongo:7.0 --replSet rs0 --bind_ip_all

# initiate it, and wait until a primary has actually been elected
docker exec tapstate-dev-mongo mongosh --quiet --eval \
  "try { rs.status().ok } catch (e) { rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]}) };
   if (!db.hello().isWritablePrimary) { quit(1) }"
```

### Why 27117, and why `directConnection=true`

`deploy/local-mongo/docker-compose.yml` publishes 27017 and would otherwise be a single
`docker compose up -d`. On a machine that already runs a native `mongod` there, that gives you two
problems, and **the second one survives changing the port**: with `replicaSet=rs0` in the URI the
driver performs topology discovery and connects to whatever address the replica set configuration
registers - `localhost:27017` - which is the other cluster, under a different replica set name.

Publishing 27117 *and* using `directConnection=true` avoids both. A direct connection skips discovery
entirely, so the registered address never participates. That is safe for a single-member set; what you
give up is failover awareness, which local development does not need.

## 5. The server run configuration

*Run -> Edit Configurations -> + -> **Application***, field by field:

| Field | Value |
|---|---|
| Name | `tapstate-server` |
| Module / classpath | `app` |
| Main class | `io.tapstate.app.Bootstrap` |
| Program arguments | `--role=all` |
| VM options | `--add-opens java.base/java.lang=ALL-UNNAMED` |
| Working directory | an empty directory of your own, for example `~/tapstate-run` |
| Environment variables | `TAPSTATE_STORE_MONGO_URI=mongodb://localhost:27117/tapstate?directConnection=true` |

### The VM option is not optional, and leaving it out fails somewhere else entirely

Without it the server starts, `/healthz` answers, and everything looks correct - until the first
`discover-schema`:

```
error: connector.discover-failed
  Connector 'mysql' could not discover its schema: ExceptionInInitializerError
  <- CodeGenerationException: InaccessibleObjectException--> Unable to make protected final
  java.lang.Class java.lang.ClassLoader.defineClass(...) accessible:
  module java.base does not "opens java.lang" to unnamed module
```

PDK connectors define classes through cglib, which needs `java.base/java.lang` opened. That argument
lives in the boot jar's manifest (`Add-Opens: java.base/java.lang`), and only Spring Boot's
`JarLauncher` applies it - which means only under `java -jar`. Running the main class directly does not
go through `JarLauncher`, so the argument is simply absent. Docker is fine, `java -jar` is fine, the
IDE is not: this one belongs to running from an IDE and nowhere else.

### Why the working directory matters

Two settings default to paths *relative to it*: `tapstate.connectors.seed-dir` (`connectors`, swept
once at startup with every jar in it registered if absent) and `tapstate.connectors.plugins-dir`
(`plugins`, an on-disk cache of resolved artifacts addressed by content hash). So stage the connector
jars before starting:

```sh
mkdir -p ~/tapstate-run/connectors
cp mysql-connector-*.jar mongodb-connector-*.jar ~/tapstate-run/connectors/
```

An empty seed directory is a valid deployment: the server starts with nothing registered, which is
enough to reach the end of this page. You need the jars for any tutorial that moves data.
`scripts/build-real-connectors.sh /tmp/connectors` builds them from the connector sources. This release
accepts `mysql` and `mongodb`; another id is refused with `connector.not-official`.

That build wants a JDK 17, not the 21 the rest of this page uses: some connectors pin a Lombok that
JDK 21 breaks. Point `TAPSTATE_CONNECTOR_JAVA_HOME` at one - the script refuses up front rather than
failing deep in a long build, and says the same thing.

### If 8080 is already taken

Add `SERVER_PORT=8081` to the same Environment variables field. The two ports behave differently and
it is worth knowing which is which: Hazelcast notices 5701 is occupied, moves to 5702 and says so in
the log, while **Tomcat does not** - without `SERVER_PORT` it fails to start. Once you change it, the
`curl` below and the CLI's `-c` have to change with it.

It is up when the log reads `Started Bootstrap in N seconds`, preceded by one `Seeded connector …`
line per jar you staged.

## 6. Create the first admin

A fresh store holds no accounts. The endpoint that creates the first one **accepts loopback only**, so
an instance that has not been given a password yet cannot be claimed over the network.

```sh
curl -s --noproxy '*' -o /dev/null -w '%{http_code}\n' \
  -X POST http://127.0.0.1:8080/auth/bootstrap \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}'
204
```

`204` means created; `409` means an admin already exists, which is also success. These credentials are
fine for an instance reachable only from your own loopback interface and nowhere else.

### `--noproxy '*'` is not decoration

`curl` does not bypass a proxy for loopback addresses. With `http_proxy` set - which it often is - the
proxy answers on the server's behalf, so a server that is not listening can come back as a `503`, or
even as a success. Every loopback `curl` here carries the flag. The JVM does not read those variables,
so the server itself is unaffected.

The health path is `/healthz`. `/actuator/health` returns 404.

## 7. Run the CLI

The CLI is an ordinary command-line program, and **a terminal is the better place to run it**. The
install in step 2 already left a runnable thin jar behind - the pom calls it exactly that, a *runnable
thin jar for manual verification* - with its dependencies in `target/lib` beside it:

```sh
cd <your-checkout>/tapstate
java -jar cli/target/cli-0.2.1.jar --version        # -> tapstate 0.2.1
TAPSTATE_PASSWORD=admin java -jar cli/target/cli-0.2.1.jar -w ./work -c 127.0.0.1:8080 -u admin
```

With no subcommand it opens a REPL - one session holding a workspace and a connection. With a
subcommand it runs that verb and exits, which is the form for scripts. The verbs are identical in both.

A terminal hands it a real TTY, so **Tab completion works with nothing to configure** - and completion
is the best part of this CLI, covering verbs, field paths and file paths.

### Running it from the IDE instead, and what that costs

Worth a run configuration when you want to step through CLI code in a debugger, and not otherwise.
*Run -> Edit Configurations -> + -> **Application***:

| Field | Value |
|---|---|
| Name | `tapstate-cli` |
| Module / classpath | `cli` |
| Main class | `io.tapstate.cli.Cli` |
| Program arguments | `-w ./work -c 127.0.0.1:8080 -u admin` |
| Environment variables | `TAPSTATE_PASSWORD=admin` |
| Working directory | the parent of the directory holding your `.tap.yml` files |

The IDE console is not a TTY, so JLine falls back to a dumb terminal and Tab completion is gone.
*Modify options -> Emulate terminal in output console* is what restores it **where that option is
offered** - it is not present on every configuration type or IDE version, and it is not worth hunting
for: if it is not there, run the CLI in a terminal and keep the IDE for the server. Nothing else about
the CLI behaves differently between the two.

### There are three rules for giving a workspace, and `--help` describes one

The arguments above carry no subcommand, so that is the session form and `-w` is accepted. Add a
subcommand to the same line and the rule changes:

| Written as | Result |
|---|---|
| `tapstate -w DIR` | session form, accepted |
| `tapstate -w DIR ls` | `Unknown options: '-w'` |
| `tapstate ls -w DIR` | offline verbs take `-w` themselves |
| `tapstate … apply -w DIR` | connected verbs have no `-w`, only a positional argument |
| `tapstate … apply DIR` | accepted |
| `TAPSTATE_WORKDIR=DIR tapstate … apply` | accepted by both kinds |

`-c` and `-u` before a subcommand are fine; `-w` is the only one that is not. Set
`TAPSTATE_PASSWORD` when a one-line launch needs a password. `--help` lists the launch options, so
following the help walks straight into it:

```
$ tapstate ... apply -w ./work
apply: unknown option '-w' (usage: apply [<path>] [--if-match <hash>])
```

`TAPSTATE_WORKDIR` is the one form both kinds of verb accept, and the one rule to remember if you would
rather not remember three.

## You are connected

The prompt names who and where: `tapstate(admin@127.0.0.1:8080)>`. Two things worth doing from here:

- [Assembling one document out of many tables](../nest-document-assembly/) - build a live document out
  of nine tables. It needs the MySQL source and the connector jars mentioned above.
- `tapstate validate ./work` - check a workspace without a server at all.

## Troubleshooting

| What you see | What it actually is |
|---|---|
| `Non-resolvable import POM: io.tapstate:bom` | The checkout moved to another version mid-build. Compare `Reactor Summary for Tapstate <version>` against `<revision>` and rerun. |
| `connector.discover-failed` with `InaccessibleObjectException` | The server's VM options are missing `--add-opens java.base/java.lang=ALL-UNNAMED`. |
| `/actuator/health` returns 404 | Wrong path. It is `/healthz`. |
| The server cannot reach its store on startup | Mongo is not a replica set, or the URI says `replicaSet=rs0` while the address it discovers belongs to a different set. Use `directConnection=true`. |
| Port 27017 already in use | Something else already serves it. Publish another port rather than stopping it, and connect directly. |
| Tomcat fails to start on 8080 | Occupied, and Tomcat does not move by itself. Set `SERVER_PORT`. |
| `apply` reports `unknown option '-w'` | Connected verbs take a positional path: `apply ./work`, or set `TAPSTATE_WORKDIR`. |
| Tab completion does nothing in the IDE console | The IDE console is not a TTY. *Emulate terminal in output console* restores it where that option is offered; where it is not, run the CLI in a terminal, which needs no configuration for it. |
| `curl` to 127.0.0.1 returns 503 or an implausible success | An HTTP proxy answered instead. Add `--noproxy '*'`. |

## Tearing down

```sh
docker rm -f tapstate-dev-mongo
```

By name. Not `docker container prune` or `docker volume prune`: on a machine anyone else is using,
those take other people's containers and volumes with them, and they do so without asking.
