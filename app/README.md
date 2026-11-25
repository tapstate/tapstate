# app — service assembly root

The single deliverable that brings the whole platform up in one process (`--role=all`). It is the
only module permitted to depend on the adapters ring: the assembly root is where the adapter bridges
are bound into the runtime. It runs on the JVM (Spring Boot), packaged as a fat-jar and a `tar.gz`
distribution (`bin/` launcher + `conf/` external config + `lib/` jar).

## Installation and recovery

Follow the [storage operator guide](../docs/storage/README.md) for OP installation, Cloud storage
setup, TLS, original-operation recovery and Configuration/keyring backups. Start an OP server with
its retained `tapstate.installation.directory`; the launcher forwards the `installation` owner
command and supplies the required JVM module-opening option.

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

When INFO is enabled for the `io.tapstate.app.Bootstrap` logger, startup logs report the effective
timezone and whether it came from a JVM option or the default, once in both the console and the
rolling log file after logging is configured and before the application context is refreshed.
If the root level is WARN or higher, enable this logger separately to retain the report:

```properties
logging.level.io.tapstate.app.Bootstrap=INFO
```

Filtering out the report does not affect timezone selection: the UTC default and explicit JVM
timezone options still apply. If the JDK resolves a requested ID differently (an unknown name
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


### Cloud HTTP request diagnostics

The `io.tapstate.diagnostics.http` logger emits one JSON summary at DEBUG for the initial
synchronous `/api/` or `/auth/` dispatch. Ordinary JAR and server defaults keep it at INFO.
Enable it at startup with `logging.level.io.tapstate.diagnostics.http=DEBUG`, the command-line
option `--logging.level.io.tapstate.diagnostics.http=DEBUG`, or the environment variable
`LOGGING_LEVEL_IO_TAPSTATE_DIAGNOSTICS_HTTP=DEBUG`. Set INFO and restart to disable it.
There is no runtime log-level administration endpoint in the default distribution.

The diagnostic Cloud image on this branch explicitly sets that environment variable to DEBUG,
so its existing build and deployment workflow needs no extra inputs. Deploying that image enables
the probe; replacing it with the original image disables it. This image is intended for a short
diagnostic run. It does not turn on root DEBUG or Mongo command-body logging.

Search console or rolling-file logs for `"event":"http_diag"`. Match `request_id` to the
browser response's `X-Request-ID`. A summary includes method, path without query parameters,
status, `app_dispatch_ms`, optional thread CPU, phase calls/inclusive durations, and Mongo
counts/driver durations grouped by database, collection, command and current phase.
Pool checkout counts, failures and elapsed time include waiting for a usable connection; they are
reported separately from commands and must not be relabelled as database server execution.
SDK phases classify exchange, JWT verification, egress lookup, callback/status and configuration
operations; `provider_failures` counts caught provider failures without their messages.

Phase durations are inclusive and may overlap; do not add authentication, context, SDK and
Mongo totals as if they were disjoint. Mongo timing is the driver's elapsed time, not server CPU.
`app_dispatch_ms` excludes time before the servlet filter (such as ingress or executor queues),
and async requests only describe their initial dispatch (`async_started=true`). Work on other
threads is not silently assigned to the request. `continuation_wait` measures a synchronous wait
for authentication continuation, even when its background commands have no request scope.

No cookies, authorization headers, request/response bodies, Mongo predicates, connection URIs,
or SDK payloads are collected. The probe does not install the byte-size observer or capture
per-command stack traces. Group details are bounded; overflow is aggregated as `other` while
total counts remain complete. `pending_commands`, `unmatched_completions` and `unclosed_stages`
qualify capture completeness. Database and collection identifiers are operational metadata.

For a capture, use the same image and data, reproduce each slow page a few times, and keep the
`http_diag` lines plus their nearby errors and timestamps. Record the image/source revision and
browser paths/timings. Compare browser first-byte waiting with the matching application dispatch;
a difference alone does not distinguish network, ingress and pre-filter queueing.
