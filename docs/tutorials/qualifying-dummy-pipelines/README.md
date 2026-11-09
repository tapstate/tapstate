---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/tutorials/qualifying-dummy-pipelines
---

# Qualifying Dummy pipelines on a local OP node

Use the internal PDK Dummy connector to qualify synthetic inputs and sink acknowledgement
on a disposable single-node on-prem (OP) installation. The CLI exercise runs a finite
32-row snapshot followed by a separate live CDC run. Both ends use the published Dummy
JAR. The integration witnesses below cover row content, repeatability, timing, and rate limits.

## Prepare the installation and verify the fixture

Use a server build from this checkout and its matching CLI. Follow the
[OP installation procedure](../../storage/README.md#install-op-and-create-its-first-administrator)
with isolated Configuration, Durable cache, and Application data databases. The Dummy
connections themselves generate and consume rows without a source or target database.
Keep the normal packaged seed connectors, including MongoDB for OP storage setup.

Download `dummy-connector.jar` from the
[connector preview release](https://github.com/tapstate/tapstate/releases/tag/connectors-preview).
From the repository root, verify the download and the three files listed in
[`fixture.json`](fixture.json). The [connector lock](../../../deploy/cloud/connectors.lock.json)
owns the upstream revision, artifact size, digest, and PDK API version.

```sh
dummy_jar=/absolute/path/to/dummy-connector.jar
sample=docs/tutorials/qualifying-dummy-pipelines
jq -r '.files | to_entries[] | "\(.value)  \(.key)"' "$sample/fixture.json" |
  (cd "$sample" && shasum -a 256 -c -)
jq -r --arg file "$dummy_jar" '.connectors[] | select(.id == "dummy") | "\(.sha256)  \($file)"' \
  deploy/cloud/connectors.lock.json | shasum -a 256 -c -
work=$(mktemp -d)
cp "$sample"/*.tap.yml "$work/"
```

Continue once all four checks report `OK`. Add the verified JAR to the OP server's
configured connector seed directory (`tapstate.connectors.seed-dir`), then start the
server with that directory. Startup registers Dummy. If discovery cannot resolve
`dummy`, check the seed path and server startup log before applying the pipeline.

## Run the snapshot

Launch `tapstate -w "$work"`, bind a local context to the workspace with `:ctx`, and
sign in with `auth login <username>` as in the
[online CLI quickstart](../../quickstart-online.md#6-save-a-context-sign-in-and-run).
Use an account with permission to apply resources, discover schemas, and start and
stop pipelines. Enter these commands in that session.

```text
apply src_dummy.tap.yml
apply tgt_dummy.tap.yml
discover-schema src_dummy -o json
discover-schema tgt_dummy -o json
apply
start dummy_op_smoke
status dummy_op_smoke
metrics dummy_op_smoke
```

Both discoveries should report `dummy_orders` with a `serial` field `id` and
`primaryKey: ["id"]`. The start check can warn that it cannot browse rows on Dummy
and continue. Within 30 seconds of start, repeat `status` until it reports `completed`
and the table reports `LANDED`. Verify `snapshot.rows.read.dummy_orders` is 32
and there are no positive `errors.*` counters. The completed job retains the snapshot
count. Its live `records.out` counter expires with the job. Use the content witness
below to verify the keys and the real sink callback count.

## Run bounded live CDC

In the copied `pipeline.tap.yml`, change `id` to `dummy_op_cdc` and `read_mode` to
`cdc_only`. The separate pipeline ID gives CDC its own lifecycle and position.

```text
apply
start dummy_op_cdc
status dummy_op_cdc
metrics dummy_op_cdc
```

While status is `running`, allow up to 30 seconds for a positive `records.out` and a
`targetAckedPosition.dummy_orders` entry. The CLI can show the position as an opaque
fingerprint. Record the output count, then repeat `metrics dummy_op_cdc` for up to
15 seconds until the count increases. Check for positive `errors.*` counters in
both readings. The source is configured for one INSERT per 1000 ms. Batching and
Metrics publication can make the output count advance in steps.

Stop the pipeline even if a check times out. If a check fails, keep the output and
inspect `logs dummy_op_cdc`.

```text
stop dummy_op_cdc --keep-state
status dummy_op_cdc
```

Confirm `stopped` within 30 seconds. Record the operator, UTC time, server and CLI
builds, JAR and YAML digests, discovery output, snapshot count, the two CDC readings,
and the final status. Use a fresh disposable installation for a clean rerun with
the same IDs.

## Reproduce the qualification witnesses

The [real-process integration test](../../../e2e/src/test/java/io/tapstate/e2e/RealDummyPipelineIT.java)
runs the supplied YAML through the CLI against an isolated OP process. The other
methods exercise specific content and timing boundaries through the normal runtime.
Use JDK 21, Maven, a running Docker daemon, and the verified Dummy JAR. From the
repository root, select one method per run. Check that Failsafe ran the selected
method with zero failures and zero skips.

```sh
case_method=publishedDummyRunsThroughTheCli
mvn -B -ntp -pl e2e -am -Dtest=NoUnitTestsInThisPass \
  -Dsurefire.failIfNoSpecifiedTests=false \
  "-Dit.test=RealDummyPipelineIT#$case_method" -Dfailsafe.failIfNoSpecifiedTests=false \
  -Dapi.version=1.44 "-Dtapstate.dummy.jar=$dummy_jar" verify
```

For the MongoDB backlog method also pass `-Dtapstate.e2e.connectors-dir=/absolute/path/to/connectors`.
Put the release's `mongodb-connector.jar` in that directory and verify it against
the `mongodb` entry in the same connector lock. Record both verified artifact
identities and the server build with the results.

| Method | Fixture and boundary |
|---|---|
| `observesContentAndRealConsumptionSeparately` | Snapshot-only, CDC-only, and snapshot followed by CDC. Compare the expected keys, operations, and row images with the events at the real Dummy `writeRecord` entry. Independently check the connector's consumption callbacks. A same-count wrong-key control fails the content assertion. For live latency, pair the PDK source event timestamp with the real target acknowledgement. The interval includes source batching and downstream waiting. |
| `fixedDummyInputsRepeatAcrossIndependentRuns` | In two independent runs, A produces keys 1, 2, 3 and B produces 2, 3, 4. Verify the common keys 2 and 3 and the unmatched keys 1 and 4 against the confirmed outputs. The payload is the eight ASCII bytes of `fixed123` plus a separate numeric key field. UUID values varied between runs. Live event timestamps are generated afresh, and arrival order across keys can vary. |
| `replaysTheSamePrewrittenMongoBatchThroughRealDummy` | MongoDB 7.0.43 replica set with collection pre-images and a majority transaction. Prewrite an INSERT, UPDATE, and DELETE for key 2, sharing a cluster timestamp with distinct resume tokens. The same-table key 1 control precedes that second. Run CDC twice with direct capture (`srs.enabled: false`) from the batch's UTC second. Verify the exact ordered events, before and after values, final DELETE acknowledgement, and a recorded target position. Measure start request to the last acknowledgement. Record prewrite time separately. |
| `qualifiesSourceAndSinkLimitsWithoutAnObserver` | Real Dummy JAR on both ends, direct INSERT-only CDC. Measure the bounded windows described below with the write observer off. |

Use the observer for small content checks. Use the unchanged published JAR and
normal runtime counters for rate measurements. A callback confirms Dummy consumption.
It supplies no measurement of a database sink's persistence or write cost.

In one two-event live run, the engine recorded 4395 ms of cumulative delivery
time. The paired event timestamps and callbacks gave 4389 ms, with 3995 ms already
elapsed when PDK emitted the events. In the separate MongoDB fixture, prewriting
took 6 ms and the two runs reached the last acknowledgement 4136 ms and 4137 ms
after start. These small samples qualify the timing boundaries.

## Rate measurements and scenario handoff

The 2026-11-09 qualification used OP 0.7.0 on macOS arm64 with JDK 21.0.11 and the
locked Dummy artifact. The source PDK batch limit was 1000 rows with a 250 ms flush.
The sink had one writer with batches of up to eight rows. A quota of zero disables
that connector limiter. The first sample followed positive acknowledgement and the
second followed at least three seconds with fresh confirmed output.

Source emission counts rows in recorded native capture batches, excluding rows
still in the PDK buffer. `records.in` counts executor intake, `recordCount` counts
rows driven to the target, and `records.out` counts rows confirmed through the
write callback. Divide each counter delta by the elapsed window. Compute the change
in unconfirmed executor intake as the `records.in` delta minus the `records.out`
delta. The sink processor stage timer measures processor calls. Use the delivery
histogram for source-to-acknowledgement timing.

| Source quota | Sink quota | Window | Emitted / intake / driven / confirmed rows per second | Change in unconfirmed rows | Mean source to ack |
|---|---|---:|---:|---:|---:|
| 10/s | Unlimited | 4.594 s | 8.71 / 10.88 / 10.88 / 10.88 | 0 | 370.7 ms |
| Unlimited | Unlimited | 4.893 s | 408.75 / 0.00 / 209.28 / 235.44 | -1152 | 5476.7 ms |
| 10/s | 1 per 250 ms | 5.178 s | 9.66 / 9.66 / 1.93 / 3.09 | +34 | 6428.1 ms |

All three runs stopped with zero errors. The unrestricted source emitted 9000 rows
during warm-up, and executor intake was already at 8192 at the first reading. The
sink continued to confirm that queued work during the window. These short windows
qualify the configured limits and expose queueing and batching. Steady-state
capacity and per-operator cost require separate measurements.

Carry the fixture identity, runtime revision, connector digests, input types and
sizes, key distribution, quotas, capture mode, batching, writer count, and stop
condition into each performance case. For snapshot throughput, fix the finite row
cohort and measure through the final target confirmation. For join and nest, use
the qualified matching and unmatched keys to specify expected outputs before
measuring. Keep live event-to-acknowledgement latency separate from catch-up time
over prewritten history, and retain warm-up counts and measurement windows with
each result.
