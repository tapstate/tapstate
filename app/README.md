# app — service assembly root

The single deliverable that brings the whole platform up in one process (`--role=all`). It is the
only module permitted to depend on the adapters ring: the assembly root is where the adapter bridges
are bound into the runtime. It runs on the JVM (Spring Boot), packaged as a fat-jar and a `tar.gz`
distribution (`bin/` launcher + `conf/` external config + `lib/` jar).

## Server timezone

The server defaults to the canonical `UTC` timezone before initializing logging, Spring or
connectors. The same bootstrap applies to direct JAR launches, the distribution launcher, and
on-premises or Cloud containers. The host's timezone does not select the server default.

An explicit JVM `-Duser.timezone=...` option takes precedence over the server default, including
one supplied through `JAVA_TOOL_OPTIONS`, `JDK_JAVA_OPTIONS`, or `_JAVA_OPTIONS`. The JVM's option
order is preserved: command-line options override `JAVA_TOOL_OPTIONS` and `JDK_JAVA_OPTIONS`, while
`_JAVA_OPTIONS` is applied after the command line and can override it. Within each source the last
option wins. Check these environment variables if the effective timezone differs from the command.
For example (with no conflicting `_JAVA_OPTIONS`):

```sh
java -Duser.timezone=Asia/Tokyo -jar app-0.7.0-boot.jar
JAVA_TOOL_OPTIONS=-Duser.timezone=Asia/Tokyo bin/tapstate-server
```

An empty timezone option uses the server default. Explicit aliases such as `Etc/UTC` are preserved;
they are not rewritten to `UTC`. Some older Oracle servers reject `Etc/UTC` with `ORA-01882`, so pin
`-Duser.timezone=UTC` when canonical UTC is required. Avoid trailing whitespace in the value.

Startup logs report the effective timezone and whether it came from a JVM option or the default,
once in both the console and the rolling log file after logging is configured and before the
application context is refreshed. If the JDK resolves a requested ID differently (an unknown name
such as `Asia/Tokio`, whitespace, or a normalized custom offset), the report includes the requested
value and the resolved value; it retains the JDK's resolution behavior. Merely loading the assembly
class in an embedded application or a test does not change that process's timezone.

Source-specific timezone settings retain their connector-defined meaning; this default does not change connector parameters. The separate CLI
client continues to use local time for its local displays.

## Operational logging

The server has two, deliberately separate, ways of saying something went wrong:

- **Operational logs** — the operator-facing diagnostic stream (process health, lifecycle,
  troubleshooting). Free-form English text, written to the console and a rolling file. This is for
  whoever runs the process.
- **Coded errors** — user-facing, diagnosable errors carried as stable symbolic codes, rendered from
  a message catalog per locale. This is a contract with the user and with tooling.

The two never mix: a code is not logged as the framework's contract, and a free-text log line never
becomes a code. Programmer bugs (invariant violations) stay bare crashes with a stack, not laundered
into either channel.

Logging is configured in [`logback-spring.xml`](src/main/resources/logback-spring.xml): console +
rolling file, with three reserved MDC attribution slots — `role`, `component`, `pipeline_id` —
spliced into the format. The slots are reserved now and render empty until later runtime work
populates them, so the format never has to change to gain them.

Operators tune it from `conf/application.properties` (or the environment); see the `tapstate.log.*`
keys there for the file location and the size/time rolling and retention caps, and Spring Boot's
`logging.level.*` keys for levels.
