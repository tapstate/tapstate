---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/guides/observe-a-pipeline
---

# Observe a pipeline

Tapstate exposes three views of pipeline activity:

- current observation reads answer what the server last saw;
- bounded history answers how target-acknowledged output and lag changed over time;
- retained events show best-effort lifecycle, failure, recovery, and telemetry boundaries.

The server also exposes one shared explanation of the current observation. Use it instead of
reimplementing diagnosis from status, metrics, and snapshot responses in each client.

All of these operations require an authenticated credential with read scope. They do not change the
pipeline. Observability GET responses, including coded and authentication errors, carry
`Cache-Control: no-store`. Keep them out of persistent HTTP or local caches.

## Choose the read that answers your question

| Question | REST | CLI | MCP |
|---|---|---|---|
| What lifecycle state was last observed? | `GET /api/pipelines/{id}/status` | `status <id>` | `pipeline_status` |
| What are the current counters and positions? | `GET /api/pipelines/{id}/metrics` | `metrics <id>` | `pipeline_metrics` |
| How far has the initial snapshot loaded? | `GET /api/pipelines/{id}/snapshot` | `snapshot <id>` | `pipeline_snapshot` |
| What did this node log for the pipeline? | `GET /api/pipelines/{id}/logs` | `logs <id>` | `pipeline_logs` |
| How did output rate and selected table lag change? | `GET /api/pipelines/{id}/metrics/history` | `metrics <id> --from ... --to ...` | `pipeline_metrics_history` |
| Which retained lifecycle or telemetry events occurred? | `GET /api/pipelines/{id}/events` | `events <id> --from ... --to ...` | `pipeline_events` |
| Why does the latest observation look this way? | `GET /api/pipelines/{id}/explain` | `explain <id>` or `status <id>` | `pipeline_explain` |

`pipeline.explain` requires a current observation. A newly started pipeline may return
`monitor.no-observation` until its first observation is published. History is independent of the
latest observation and can still return retained samples for a stopped pipeline. Events also do not
require a current observation: an existing pipeline can have retained events while its current reads
are pending.

Check the deployed server's version and supported operation set before enabling these views. Earlier
builds may provide history and explain without events or scoped logs. The examples and schemas in this
guide do not establish what an older deployment exposes. Treat an unsupported operation as an
unavailable capability, never as a successful empty event page. MCP clients can inspect `tools/list`;
direct REST clients use `/version` and the compatibility information for the installed build, not an
unrelated 404 as capability discovery.

The current read faces are separate requests, not one transactional snapshot. If you need a diagnostic
conclusion, read `pipeline.explain`; do not join the other responses and copy its rules.

## Keep lifecycle boundaries visible

The backend distinguishes a resource's incarnation from each physical execution. Editing or applying
the same pipeline preserves its incarnation; deleting and recreating the id creates a new one. A new
start, restart, rebuilding resume, or ownership takeover changes execution. These identities belong to
the backend's storage and dispatch envelope; events, history, and logs do not expose identity selectors
for clients to reconstruct.

| Action | Current observation and counters | Retained history, events, and local logs |
|---|---|---|
| Pause | Keep the observation and publish `PAUSED` freshness. Preserve cumulative counters and their start. Quiet measurements remain absent. | Keep history and the bounded log tail; do not invent samples or zeros for the pause. |
| Resume without rebuilding | Keep incarnation and execution; continue cumulative values and counter start. | Continue sampling at real intervals. |
| Resume with a rebuilt job | Keep incarnation, take a new execution, and carry the known counter/histogram baseline and start. Gauges are measured again. | History can begin a new segment to avoid differences across physical executions; that boundary alone is not a counter reset. |
| Stop | Keep the final `STOPPED` observation; do not keep advancing old execution counters. | Keep history and events until retention expires, and keep the bounded local log tail. |
| Start or restart after stop | Use a fresh counter start. Until the new execution publishes, current reads return 404 `monitor.no-observation` rather than the previous run's numbers. | Keep earlier executions of the same incarnation within retention; counter reset is expressed by counter start and `COUNTER_RESET`. |
| Delete and recreate the id | New scoped reads belong to the new incarnation; old scoped data cannot become its current observation. | Clear client state for that id. Cleanup of the old identity is conditional and best effort; physical residue may remain until TTL, capacity/LRU, or a bounded janitor reclaims it. |

An extreme failure can lose an unsaved statistics tail. An unknown continuation baseline stays unknown;
it is not repaired with a fabricated zero or a per-record statistics transaction. Legacy history written
before identity envelopes remains readable under its original pipeline-id semantics and ages out within
the history retention window. Once a current resource has scoped identity, legacy latest data cannot
stand in for its missing current observation.

Physical cleanup is not a transaction across observation, history, event, log, and remote exporter
stores. A delayed cleanup uses the old identity so it cannot remove the new resource's telemetry.
History, events, and rollups use the bounded retention policy; local logs use bounded tail capacity/LRU.
Cleanup failure is an operational diagnostic and does not undo an already completed artifact deletion.
Remote exporter retention has the separate boundary described below.

After deletion, a valid events request returns 404 `lifecycle.unknown-pipeline`, including when old
events remain physically stored. An earlier cursor cannot authorize a read of the removed identity.
Recreating the same id exposes only the new incarnation's scoped events. Diagnose incomplete cleanup
with the process cleanup metrics and local logs. Without external metrics or centralized logs, process
exit can lose those local diagnostic signals. A later successful cleanup does not establish that all
earlier residue was reclaimed.

## Read performance and telemetry health facts

The current metrics response carries a flat numeric `metrics` map and typed `facts` with units, attributes,
and each point's measurement time. A missing measurement means it was quiet or unavailable. Do not fill it
with zero. When a per-table, code, chain, or namespace series exceeds its budget, an
`otel.metric.overflow=true` point keeps the folded value; the dimension name is no longer available for
that point. The exporter also applies a 10,000-series ceiling per instrument across the process.
The complete types, units, attribute budgets, measurement boundaries, and absence rules are maintained
in the [metric inventory](metric-inventory.md).

| Reading | First check when it worsens |
|---|---|
| `tapstate.pipeline.queue.depth`, `.capacity`, `.high_water` | Compare depth with capacity, then inspect the stage durations and sink pending batches. High-water is the highest collected Jet sample, so a shorter peak between scrapes may not appear. |
| `tapstate.pipeline.stage.queue.depth`, `.capacity`, `.high_water` | Compare the fixed stage's input queue with its active work and duration, then check downstream sink pending/limit. The sampled peak belongs to the current job/execution and resets on replacement; quiet queues stay absent until occupancy is observed. Missing or mismatched processor/member collections are unknown. Framework pass-through queues can still contribute to the separate pipeline total. |
| `tapstate.pipeline.stage.output.refused` and `.retry.duration` | Compare actual refused offers and completed retry intervals with the stage's input queue, sink pending/limit, and delivery duration. Retry time includes callback quota, scheduler delay, backoff, and blocking waits; these facts alone cannot identify downstream-full pressure. Quiet or incomplete output accounts remain absent. |
| `tapstate.pipeline.process.active` and `tapstate.pipeline.work.active` | Compare active business units with queue depth, stage duration, and sink pending batches. Both are gauges in `{work}`. The first has pipeline id and `stage=source/transform/nest/join/sink`; the pipeline total has only pipeline id and requires a complete reading from the current job/execution. Missing points remain unknown. These gauges do not measure CPU busy time or executor saturation. |
| `tapstate.pipeline.sink.batch.pending`, `.limit`, `.write.duration`, and `tapstate.pipeline.sink.backpressure.duration` | A full pending limit with increasing write time points toward target delivery. Compare target-acknowledged `records.out` with issued batches; an issued batch is not an acknowledgement. |
| `tapstate.pipeline.nest.cold_layer.over_threshold` and `nestStateColdLayerOverThreshold.<namespace>` | A measured value of 1 means at least 100 state accesses in the decision window and at least half served from the cold layer. Compare access/backfill deltas, in-memory entries, stored entries, and backfill time before changing state memory. A quiet window has no threshold fact; it is not a measured 0. |
| `tapstate.pipeline.state.store.operation.count`, `.duration.sum`, `.payload.bytes`, and `.serialization.count/bytes` | Compare changes between samples by compiled state namespace and the fixed operation, outcome, or codec. Rising load calls and duration point to cold-layer work; compare actual payload and Java serialization bytes before blaming network transfer. These job counters are absent when the cold store is unwired, the job is quiet, or a member's current reading is missing. They never include state keys or row values as labels. |
| `tapstate.process.nest.stored_count.queued`, `.active`, `.failed`, `.rejected`, `.duration.sum` | Inspect Mongo namespace size and the `_id` range index when counts queue or fail. Stored counts are sampled by two bounded background workers: refresh starts after 15 seconds, a result expires after 30 seconds, and pending or expired results are absent. The typed `nest.stored` point keeps its actual count time; the flat value can be up to 30 seconds old. |
| `tapstate.process.lifecycle.pipelines.pending`, `.queue.high_water`, `.capacity.refused`, `.work.duration` | Check whether start or stop work is occupying the four default lifecycle slots. `START_CAPACITY` and `STOP_CAPACITY` in explain describe a wait, not a failed data job. |
| `tapstate.process.telemetry.degraded`, `.queue.depth`, `.dropped`, `.write.failure`, `.last_success.age` | Filter by the fixed `sink=latest/history/event/export` label. A failing observation store leaves the last successful document and its old `observedAt` in the API. Check local logs and the affected sink before interpreting an unchanged graph as an idle pipeline. |
| `tapstate.process.telemetry.breaker.state`, `.recovered` | The state gauge is 0 when closed, 1 while open, and 2 during the single recovery probe. The recovery counter counts transitions back to closed after a write succeeds and has not been marked timed out, including a write admitted before the breaker opened. A failed probe or a success while already closed does not increment it. Both have only the four fixed sink labels. Check the affected store or exporter and its timeout logs; waiting for cooldown alone does not prove recovery. |
| `tapstate.process.telemetry.gap.open`, `.opened`, `.closed` | These readings have only `sink=history/event`. A history gap opens when a due sample is lost and closes after a successful sample in the same execution; replacing that execution or forgetting a deleted pipeline removes its open gap without claiming recovery. An event gap closes only after its marker is persisted. Check the affected sink and the history gaps or event page's known gaps; the process counters do not provide an audit log. |
| `tapstate.process.memory.rss` | Compare resident bytes with the same host's heap and workload windows. Linux reads `/proc/self/smaps_rollup` every 15 seconds on a separate worker; a sample older than 30 seconds or an unavailable source is absent. Other platforms do not emit this fact. |
| `tapstate.process.jvm.gc.pause.observed.duration.sum` | Compare counter deltas with delivery latency and heap pressure. It sums observed JFR `jdk.GCPhasePause` durations in nanoseconds from the metrics stream's start. It is absent before the first observed pause and after stream failure or known JFR data loss; it is not a guaranteed lifetime total. |
| `tapstate.process.metrics.overflow.instruments`, `.series` | Count distinct instruments and exported aggregate series that currently carry `otel.metric.overflow=true`. They are absent when nothing is folded and have no labels. A series count describes visible overflow aggregates, not the number of original input series folded into them; inspect budgets and the affected instrument before increasing a limit. |

Process facts are emitted only when Prometheus or OTLP export is configured; they have no pipeline or row
labels. Connector call duration includes its synchronous callback. Connector-internal retries and pool
occupancy have no general measurement yet. JVM GC collection time is not an exact stop-the-world pause;
do not substitute it for the JFR pause reading. Jet executor saturation has no trustworthy built-in
measurement; use job queue depth, stage duration, and sink backpressure to locate pressure instead.
Do not substitute committed heap for RSS when comparing workloads.

Active work counts the existing timed business units. A transform covers its synchronous
`port.transform` call and releases active work before the later outbox drain. A sink covers its processor
drain, excluding time waiting on an asynchronous target future; use pending batches for that wait.
Empty source polls produce no duration sample and release active work. Points keep the real Jet
collection timestamp. Quiet, unwired, partial, and mixed-job readings are absent. Expected staged
vertices are registered by DAG compilation; the total is not guessed from an unknown graph.

Scoped latest-write, history-sample, and local-export-offer failures can produce a
`TELEMETRY_DEGRADED` event. Repeated failures stay in one episode per pipeline and sink. A successful
attempt that began after the latest observed loss can produce `TELEMETRY_RESTORED`; history also waits
until that execution's sampling gap closes. An older blocked write finishing does not prove recovery.
Local export-offer recovery does not claim that an OTLP collector received the data; use the exporter's
asynchronous completion facts for that boundary. Event-store loss continues to persist its gap marker
before reporting recovery. Legacy frames without execution identity do not invent scoped events.
The event trail remains best effort, so a missing boundary event cannot prove that no outage happened.

Deleting a pipeline removes its old current points from the local exporter, including when the same id
is recreated before another sweep. Exported point labels contain the pipeline id but no incarnation or
execution selector. A remote Prometheus or OTLP backend may retain older points until its own retention
expires; do not use that remote history to decide which incarnation a current store-backed read belongs to.

## Configure observation work and retention

These startup settings use the shipped defaults below. Change one setting at a time and compare the
corresponding queue, duration, failure, and resource facts on the same workload.

| Property | Default | What it controls |
|---|---|---|
| `tapstate.lifecycle.max-concurrency` | `4` | Shared lifecycle coordination slots; one pipeline still has at most one active intent |
| `tapstate.lifecycle.queue-capacity` | `64` | Bounded accepted lifecycle intent queue; later convergence passes retry refused admission |
| `tapstate.observability.janitor.batch-size` | `16` | Maximum documents examined by each bounded cold cleanup phase in a pass |
| `tapstate.observability.janitor.interval` | `PT1M` | Delay between cold janitor passes |
| `tapstate.metrics.history.sample-interval` | `PT1M` | Minimum interval between retained raw movement samples |
| `tapstate.metrics.history.retention` | `P15D` | Time-based raw/rollup retention and query clipping; shorter retention also shortens the available window |
| `tapstate.metrics.history.rollup-read-enabled` | `true` | Allows valid rollups on reads; `false` selects raw aggregation for a same-build diagnostic comparison |

Worker and queue counts must be positive. A larger lifecycle pool does not change pipeline processing
parallelism or make a slow target finish sooner. Inspect the verb's work duration and capacity wait
before changing admission limits. Reduce observation cost only after measuring query fallback and
sampling gaps alongside the resource change.

Telemetry dispatch currently has four latest workers and one worker for each wired history, event, and
local-export sink, with a queue budget of 64 for each pool. Its write watchdog marks timed-out work after
the five-second deadline and uses a one-second breaker cooldown. A driver call that cannot be interrupted
can continue to occupy its bounded slot; a timeout does not prove that remote IO stopped. These are fixed
budgets in this build. Collector push completion has its own export health boundary.

## Latest storage and upgrades

Each pipeline has one logical latest observation. Mongo stores its current pointer in a bounded
manifest and keeps payloads up to 512 KiB inline. Larger payloads stream into immutable chunks of at
most 1 MiB; every physical BSON document stays below the 12 MiB evolution budget. A large connector
position or failure detail is retained in full. History and events remain separate expiring documents.

Chunk writes and manifest transitions require majority and journal acknowledgement. A chunked publish
holds one renewable publication lease, writes all chunks, then advances the current pointer atomically.
An interrupted or stale publish leaves the last committed observation and its original `observedAt`
visible. Normal advancing inline writes use one conditional Mongo update. The first format transition
checks and fences any legacy latest document in a bounded transaction; an incomplete transition keeps
that legacy value readable under the existing identity checks.
Once scoped current authority is committed, removing its descriptor does not revive the old value.
Compatibility writes remain behind that authority. Legacy deletion and residue tracking commit
together, so an empty unowned manifest can be removed after its legacy residue is gone.

Readers verify the committed payload's version, order, byte count and digests before returning it.
Missing or corrupt committed chunks produce `io.document-unreadable`; they do not expose a partial
observation or substitute an older legacy value. Reads have a 10-second total deadline with at most
one retry. The cold janitor keeps current and pending chunks, retires unreferenced chunks, and waits
15 seconds before conditionally deleting them. Its failure delays space reclamation and appears in
process health; it does not change which incarnation owns a read.

This representation installs system-data version 14 at startup. Stop the previous binaries before
upgrading: binaries supporting an older data version refuse the migrated store before serving requests
or joining the cluster. Keep an upgrade backup for recovery; the first new observation is independent
of this startup version boundary.

## What history measures

History contains samples, not stored rates. By default the server samples no more often than once per
minute, when convergence publishes an observation, and keeps samples for 15 days. Both intervals are
configurable. A stopped pipeline does not produce an artificial grid of zero-valued samples, so a time
range may contain gaps.

The history projection deliberately exposes only:

- pipeline-level `records.out`, the number of records acknowledged by the target;
- pipeline-level `bytes.out`, the payload bytes acknowledged by the target;
- target-acknowledged lag for explicitly selected tables, in seconds.

Rates are calculated at query time from adjacent samples and their real `observedAt` values. Each
sample records when its outbound counters started. A changed counter start or a decreased outbound
counter begins a `COUNTER_RESET` segment, so a restart never becomes a negative rate or a spike across
runs. A sufficiently long interval begins a `GAP` segment and is never interpolated.

Inbound counters are not returned as rates. Retained samples do not carry an independent start for
each inbound series, so applying the outbound start to them would falsely describe a reset-aware rate.
Do not infer additional history fields from names currently visible on `pipeline.metrics`; those names
remain preview surface and may change.

Lag is the age of the last event that the target acknowledged for that table. It is not source backlog,
and it cannot say whether the source has more changes waiting.

## Window and resolution

`from` and `to` are required RFC 3339 timestamps with `Z` or a UTC offset. The window is half-open:
`[from,to)`. It must be no longer than 15 days.

The server clips the first page to both the current retention boundary and server time:

- `effectiveFrom` is the later of `from` and `retentionCutoff`;
- `effectiveTo` is the earlier of `to` and server time.

Those values are frozen for the cursor walk. If the effective window contains no retained samples, the
request succeeds with `status: "NO_RETAINED_SAMPLES"`, empty arrays, and `nextCursor: null`. That does
not prove that the pipeline never ran or that samples expired; it says only that the retained window is
empty now.

`resolution` defaults to `auto`:

| Requested span | `effectiveResolution` |
|---|---|
| up to 1 hour | `PT1M` raw minute-class samples |
| up to 6 hours | `PT5M` |
| up to 1 day | `PT30M` |
| up to 3 days | `PT1H` |
| up to 7 days | `PT3H` |
| up to 15 days | `PT6H` |

REST and MCP also accept explicit `raw`, `PT5M`, `PT30M`, `PT1H`, `PT3H`, or `PT6H`.
The CLI uses the aliases `raw`, `5m`, `30m`, `1h`, `3h`, and `6h`.
`PT1M` identifies the raw minute-class mode; it does not promise that samples are exactly 60 seconds
apart.

Aggregate buckets are aligned to UTC. A first or last partial bucket carries its actual
`intervalStart` and `intervalEnd`; contributions outside the effective window are not included.
For an output rate:

- `delta` is the counter contribution in that interval, in records or bytes;
- `averageRate` is that contribution divided by the measured interval, in records/s or bytes/s;
- `maxRate` is the highest valid adjacent-sample rate contributing to it, in the same per-second unit.

For table lag, `last` is the last real reading in the bucket and `max` is its peak. Missing metrics are
omitted rather than filled with zero. `unavailable` names a requested series for which this page has no
usable value.

Raw responses are also derived intervals, not stored counter documents. The server may read one sample
before `from` so it can calculate the first in-window rate. That first point can therefore have an
`intervalStart` before `from`; plot it at its in-window `intervalEnd`. Without a valid predecessor, the
rate field is absent and any real lag reading remains available.

For larger resolutions, the server may use disposable 5m, 30m, 1h, 3h, and 6h rollup buckets. Raw samples
remain the source of truth. A complete valid bucket avoids a raw read; missing, expired, partial, or
fallback-marked buckets descend to a finer level or raw for only the affected interval. A late raw sample
may differ from a cached result for up to five minutes from the cache's first input read. After that,
the query must descend or refresh; it cannot serve the expired bucket as if it were current. Rollups add
about 26% as many retained documents as one-minute raw history over a fully populated 15-day window.
This is a document-count ratio; bucket fragments and indexes can make the byte and disk cost higher.
The five layers add at most 5,580 bucket documents per pipeline to 21,600 one-minute raw documents in
that window. Coarser buckets inherit the earliest input deadline, so cascading layers cannot extend
stale data by another five minutes each. A partial bucket is recomputed for the actual interval; the
server does not prorate a whole bucket's peak or delta. Rollups are produced by bounded background
batches, not by synchronously writing all five layers on every raw append.
For a same-build diagnostic comparison, set `tapstate.metrics.history.rollup-read-enabled=false` at
startup. The same request resolution then aggregates raw samples without reading the rollup cache; the
default is `true`. This switch does not remove cached documents or stop the background worker, and a raw
query remains subject to its normal scan and output budgets.

## Segments and gaps

Every segment has one `startReason`:

| Reason | Meaning |
|---|---|
| `WINDOW_START` | First segment of a new query |
| `CONTINUATION` | First segment on a later page |
| `COUNTER_RESET` | Outbound counter start changed or the counter decreased |
| `GAP` | Samples were too far apart to connect |

Only `COUNTER_RESET` means reset. Do not treat every new segment as one, and never join across a `GAP`.
The top-level `gaps` array gives explicit `SAMPLE_GAP` intervals related to
the current page. Merge pages before claiming you have the complete set of gaps for a window.

Points, segments, gaps, and selected table lag are ordered. Samples with the same observation timestamp
retain server order; a client must not deduplicate them by timestamp.

## Pagination, limits, and consistency

`limit` defaults to 240 and accepts 1 through 1000. It counts output points across all segments in one
response page, not stored samples and not points per series. `table` is an exact, case-sensitive selector;
repeat it for up to 20 unique tables. Omitting it returns only the two pipeline output-rate series and no
per-table lag.

The server enforces independent bounds:

- up to 1000 output points per response page;
- up to 1024 raw samples in one internal store read;
- up to 25,000 scanned raw documents in one request;
- two output-rate series plus at most 20 selected lag series.

Crossing a runtime bound returns `monitor.query-budget-exceeded`; narrow the time range or select fewer
tables. Lowering the output limit creates more response pages but does not raise the raw scan budget.

`nextCursor` is always present and is `null` on the last page. When it is non-null, repeat the pipeline
id, `from`, `to`, `resolution`, `limit`, and every table selector unchanged, and add that cursor. A
token is opaque, query-bound, and valid for 10 minutes. It
does not hold a Mongo cursor, transaction, or other server-side session. Changing the query, modifying the
token, or using a token from another pipeline returns `monitor.invalid-cursor`; an expired token returns
`monitor.cursor-expired` with HTTP 410. Start a new first-page query in either case.

History declares `consistency: "EVENTUAL"`. Keyset pagination does not repeat or skip records visible in
the ordered walk at each page boundary, but the complete walk is not a database snapshot. A late sample
inserted before the cursor is not revisited, and retention can remove an unread sample between pages.
Start a new query to refresh the graph. Do not use a cursor walk as an audit export.

## Read history through REST

URL-encode the pipeline id when placing it in the path.

```sh
curl -sS --get "$TAPSTATE_URL/api/pipelines/order_pipeline/metrics/history" \
  -H "Authorization: Bearer $TAPSTATE_TOKEN" \
  --data-urlencode 'from=2026-09-20T10:00:00Z' \
  --data-urlencode 'to=2026-09-20T11:00:00Z' \
  --data-urlencode 'resolution=auto' \
  --data-urlencode 'limit=240' \
  --data-urlencode 'table=public.orders'
```

Follow a page by making the same request and adding:

```sh
  --data-urlencode 'cursor=<nextCursor>'
```

Read the shared explanation separately:

```sh
curl -sS "$TAPSTATE_URL/api/pipelines/order_pipeline/explain" \
  -H "Authorization: Bearer $TAPSTATE_TOKEN"
```

Successful response examples are versioned with the implementation under
[`control/rest-api/src/test/resources/golden/observability/`](../../control/rest-api/src/test/resources/golden/observability/).
The package manifest records the contract version, first supported product version, and first backend
revision. Direct REST has no `/api/schema`, `/v3/api-docs`, or capability-discovery endpoint; use
`/version` together with that compatibility record instead of interpreting an unrelated 404 as feature
discovery.

## Read history through the CLI

In a connected session:

```console
tapstate(admin@127.0.0.1:8080)> metrics order_pipeline --from 2026-09-20T10:00:00Z --to 2026-09-20T11:00:00Z --resolution 5m --limit 240 --table public.orders
tapstate(admin@127.0.0.1:8080)> metrics order_pipeline --from 2026-09-20T10:00:00Z --to 2026-09-20T11:00:00Z --resolution 5m --limit 240 --table public.orders --cursor <nextCursor>
tapstate(admin@127.0.0.1:8080)> explain order_pipeline
```

`metrics <id>` without `--from` and `--to` keeps its current-snapshot behavior. A history query requires
both range options. The CLI prints the effective range, segments, rates, lag, gaps, unavailable series,
retention cutoff, consistency, and continuation cursor.

In a connected session, or when `-c` selects a server explicitly, `explain <id>` is the runtime
explanation. Without a server target, the existing offline `explain` command remains the DSL field
manual.

`status <id>` renders the same server-owned explanation below the lifecycle state. It may also make a
separate metrics read for the current movement line; that extra reading does not change the explanation
or turn the two calls into one snapshot.

## Read history through MCP

The MCP tools are mechanical names for the same operations. `pipeline_metrics_history` accepts:

```json
{
  "id": "order_pipeline",
  "from": "2026-09-20T10:00:00Z",
  "to": "2026-09-20T11:00:00Z",
  "resolution": "PT5M",
  "limit": 240,
  "table": ["public.orders"]
}
```

Supply `cursor` with every other argument unchanged for the next page. MCP uses the REST resolution
values, not the CLI aliases. `pipeline_explain` accepts only `{"id":"order_pipeline"}`. Both tools
return the same structured JSON fields and coded errors as REST; an agent does not need a separate
diagnostic or rate algorithm.

## Read retained events

Use `pipeline.events` for low-frequency state changes, coded failures, execution restart/recovery,
telemetry degraded/restored boundaries, cleanup diagnostics, and telemetry-gap markers. The event
trail is retained for 15 days by default and is always `completeness: "BEST_EFFORT"`. It has no durable
outbox or audit-level completeness guarantee. An empty page, no `FAILURE` event, or a completed cursor
walk cannot prove that an event never happened.

`CLEANUP_INCOMPLETE` remains an event kind. A cleanup failure for a deleted incarnation may be retained
until TTL, but this does not provide a deleted-resource query route. The deleted pipeline's events API
returns `lifecycle.unknown-pipeline`; a recreated pipeline cannot inherit that event. Process cleanup
health and logs provide the operational signal.

The REST request has required `from` and `to` RFC 3339 timestamps with an offset, with `from < to` and
a maximum 15-day half-open range `[from,to)`. `limit` defaults to 100 and accepts 1 through 500. It
counts events in this page; gap markers count as events too. There is no event `resolution`, table
selector, or execution selector.

```sh
curl -sS --get "$TAPSTATE_URL/api/pipelines/orders/events" \
  -H "Authorization: Bearer $TAPSTATE_TOKEN" \
  --data-urlencode 'from=2026-09-20T10:00:00Z' \
  --data-urlencode 'to=2026-09-20T11:00:00Z' \
  --data-urlencode 'limit=100'
```

The CLI accepts paired `--from` and `--to`, plus optional `--limit` and `--cursor`:

```console
tapstate(admin@127.0.0.1:8080)> events orders --from 2026-09-20T10:00:00Z --to 2026-09-20T11:00:00Z --limit 100
tapstate(admin@127.0.0.1:8080)> events orders --from 2026-09-20T10:00:00Z --to 2026-09-20T11:00:00Z --limit 100 --cursor <nextCursor>
```

`pipeline_events` takes the same arguments as REST, with the pipeline path value named `id`:

```json
{
  "id": "orders",
  "from": "2026-09-20T10:00:00Z",
  "to": "2026-09-20T11:00:00Z",
  "limit": 100
}
```

Events are ordered by `(occurredAt,id)` ascending. The event id is opaque: use it for deduplication,
not as a run id or a value to parse. Keep server order, including events with equal timestamps.
The server normalizes times to UTC and clips the first page to retention and server time. Its
`effectiveFrom`, `effectiveTo`, and `retentionCutoff` stay frozen for the cursor walk. A window with no
effective overlap still succeeds with empty arrays and equal effective bounds.

`nextCursor` is always present; only `null` means there is no next page. For another page, repeat the
original pipeline, `from`, `to`, and `limit` unchanged and add the returned `cursor`. The opaque token
is bound to that operation, query, and current resource; it expires after 10 minutes. A changed query
or invalid token returns `monitor.invalid-cursor`; expiry returns HTTP 410 `monitor.cursor-expired`.
Start a new first-page query instead of splicing a new walk onto the old one. Pagination is eventual,
not a database snapshot: late events before a passed cursor and events removed by retention are not
revisited. Refresh with a new query.

Each event has `id`, `occurredAt`, `kind`, and `message`. Optional `beforeState`/`afterState` use only
`NEW`, `RUNNING`, `PAUSED`, `STOPPED`, `COMPLETED`, and `FAILED`. The event kinds are
`STATE_CHANGED`, `FAILURE`, `EXECUTION_RESTARTED`, `EXECUTION_RECOVERED`, `TELEMETRY_DEGRADED`,
`TELEMETRY_RESTORED`, `CLEANUP_INCOMPLETE`, and `TELEMETRY_GAP`. A failure preserves its
`{code,params,message}`; use code and named parameters for machine decisions, not rendered text.
An optional `reason` is display text, not another state or decision vocabulary.

`knownGaps` is strictly the projection of `TELEMETRY_GAP` markers in this page. It performs no extra
whole-window gap query. Merge only pages already fetched and deduplicate gaps by `eventId`; reasons
are the fixed `QUEUE_FULL`, `WRITE_FAILURE`, and `SHUTDOWN` values. A marker is selected by its own
`occurredAt`, while the gap's `from`/`to` retain their real boundaries. A marker outside the request
window is absent even if its gap overlaps that window. `knownGaps: []` does not establish completeness,
and event gaps do not replace the rate-history `gaps` used to break a graph line.

### Event response examples

These three independent fixtures use the request above. They are copied from the
[event goldens](../../control/rest-api/src/test/resources/golden/observability-events/) and validated
against the [event OpenAPI schema](../../control/rest-api/src/test/resources/golden/observability-events/pipeline-events.openapi.json).

A retained failure followed by recovery:

```json
{
  "pipelineId": "orders",
  "from": "2026-09-20T10:00:00Z",
  "to": "2026-09-20T11:00:00Z",
  "effectiveFrom": "2026-09-20T10:00:00Z",
  "effectiveTo": "2026-09-20T11:00:00Z",
  "retentionCutoff": "2026-09-05T11:00:00Z",
  "completeness": "BEST_EFFORT",
  "events": [
    {
      "id": "ev-a7",
      "occurredAt": "2026-09-20T10:10:00Z",
      "kind": "FAILURE",
      "message": "Pipeline orders stopped because its job failed: sink refused the batch.",
      "beforeState": "RUNNING",
      "afterState": "FAILED",
      "failure": {
        "code": "engine.job-failed",
        "params": {"pipeline": "orders", "cause": "sink refused the batch"},
        "message": "Pipeline orders stopped because its job failed: sink refused the batch."
      }
    },
    {
      "id": "ev-b2",
      "occurredAt": "2026-09-20T10:12:00Z",
      "kind": "EXECUTION_RECOVERED",
      "message": "Pipeline execution recovered.",
      "beforeState": "FAILED",
      "afterState": "RUNNING"
    }
  ],
  "knownGaps": [],
  "nextCursor": null
}
```

A page containing a known loss marker; its gap begins before the requested window:

```json
{
  "pipelineId": "orders",
  "from": "2026-09-20T10:00:00Z",
  "to": "2026-09-20T11:00:00Z",
  "effectiveFrom": "2026-09-20T10:00:00Z",
  "effectiveTo": "2026-09-20T11:00:00Z",
  "retentionCutoff": "2026-09-05T11:00:00Z",
  "completeness": "BEST_EFFORT",
  "events": [
    {
      "id": "ev-g4",
      "occurredAt": "2026-09-20T10:20:00Z",
      "kind": "TELEMETRY_GAP",
      "message": "Some pipeline events could not be recorded."
    }
  ],
  "knownGaps": [
    {
      "eventId": "ev-g4",
      "from": "2026-09-20T09:58:00Z",
      "to": "2026-09-20T10:19:00Z",
      "reasons": ["QUEUE_FULL", "WRITE_FAILURE"]
    }
  ],
  "nextCursor": null
}
```

A valid range with no retained events:

```json
{
  "pipelineId": "orders",
  "from": "2026-09-20T10:00:00Z",
  "to": "2026-09-20T11:00:00Z",
  "effectiveFrom": "2026-09-20T10:00:00Z",
  "effectiveTo": "2026-09-20T11:00:00Z",
  "retentionCutoff": "2026-09-05T11:00:00Z",
  "completeness": "BEST_EFFORT",
  "events": [],
  "knownGaps": [],
  "nextCursor": null
}
```

An empty event response has no history `NO_RETAINED_SAMPLES` status and is not a
`monitor.no-observation` error.

## Read node-local logs

`GET /api/pipelines/{id}/logs` accepts optional positive `limit` and
`scope=current|incarnation`, defaulting to `current`:

```sh
curl -sS --get "$TAPSTATE_URL/api/pipelines/orders/logs" \
  -H "Authorization: Bearer $TAPSTATE_TOKEN" \
  --data-urlencode 'limit=100' \
  --data-urlencode 'scope=incarnation'
```

The CLI accepts `logs <id> [--scope current|incarnation]`. MCP `pipeline_logs` accepts `id`, optional
`scope`, and optional `limit` (its adapter clamps a supplied limit to 1 through 200):

```console
tapstate(admin@127.0.0.1:8080)> logs orders --scope incarnation
```

```json
{"id":"orders","scope":"incarnation","limit":100}
```

`current` reads this node's retained lines for the current execution. `incarnation` reads this node's
retained lines across executions of the current resource. Both resolve identity on the backend;
neither accepts an execution selector or includes an older resource recreated under the same id.
The response shape stays `{pipelineId,lines:[{timestampMillis,level,message}]}`, with epoch-millisecond
timestamps and lines ordered oldest to newest. `limit` selects from the bounded local tail; it does
not use the event API's 100/500 pagination limits.

No retained lines on this node, or a pipeline unknown here, retains the existing HTTP 200 empty
`lines` behavior. It is not current-observation pending. Local logs are not a cross-node collection or
permanent history; use an external log backend when that retention or routing is required.

## Interpret an explanation

An explanation reads one observation once and applies a fixed first-match order:

1. `OBSERVATION_STALE` when the observation is at least 30 seconds old.
2. `CODED_FAILURE`, preserving the original failure code, parameters, and rendered message as evidence.
3. `RECONCILE_FAILURES` while a new or running pipeline repeatedly fails to converge.
4. `NO_MOVEMENT` while a new or running pipeline has driven and loaded no rows.
5. `FRONTIER_STALLED` when a chain reports a stalled frontier.
6. `NO_MATCH` when none of those conclusions is supported.

The lifecycle `state` is returned unchanged. `freshness` is independently `FRESH`, `STALE`, or
`UNKNOWN`; it is not a lifecycle state. When observation time is unavailable, `observedAt` and
`observedAgeMillis` are both absent.

Treat `evidence[].value` as JSON-typed data, not text to parse out of `message`. `cannotSay` names facts
the available observation cannot establish. In particular, `NO_MATCH` is not a healthy verdict and
always has a non-empty `cannotSay` list. `next`, when present, is an operator suggestion rather than
authorization to perform an action. An optional `pending` field is reserved for a server-provided
capacity or lifecycle wait reason; its absence does not prove that no wait exists.

## Coded errors and operator response

Every coded REST error is `{code, params, message}` and carries `Cache-Control: no-store`.

| HTTP | Code | Response |
|---|---|---|
| 400 | `control.malformed-request` | Correct timestamps, range, resolution, limit, selectors, or log scope. |
| 400 | `monitor.invalid-cursor` | Repeat the exact query or start a new first page. |
| 400 | `monitor.query-budget-exceeded` | Narrow the range or select fewer table series. |
| 401/403 | `control.unauthenticated` / `control.forbidden` | Supply a valid read-scoped credential. |
| 404 | `lifecycle.unknown-pipeline` | Check the id and apply the pipeline first. |
| 404 | `monitor.no-observation` | The pipeline exists; wait for its first observation and retry current reads. |
| 410 | `monitor.cursor-expired` | Start a new history or events query; do not resume the old walk. |

A 200 `NO_RETAINED_SAMPLES` history response is different from every error above. So is an absent
metric in an otherwise valid page, or a 200 empty `BEST_EFFORT` event page. Keep those states distinct
in dashboards and automation. A current read's 404 `monitor.no-observation` is pending for an existing
resource; a successful response with an old `observedAt` is stale data. A fresh HTTP response does not
make the underlying observation fresh.

## Maintain client request and cache state

Use one in-flight request per active view/query, pause hidden views, and back off after failed polls.
Give each resource/query selection a client request version. Increment it on a resource switch,
query-argument change, or lifecycle mutation; apply a response only when it still matches the current
version. Cancel older requests where possible and ignore their late responses. An old successful
request must not repopulate numbers or events after a later restart, deletion, or recreation.

After delete/recreate of an id, discard its latest/history/events/logs data, pagination cursors, and
selection state. After an accepted start/restart, stop showing the prior execution's current numbers
while the new read is pending. These are client actions on public lifecycle/query state; they do not
require parsing backend incarnation or execution identifiers.

For live displays, poll current reads and `pipeline.explain` as independent observations. Refresh
history or events with a new first page and replace the displayed window after the refresh completes.
Do not permanently append only the newest points or extend a completed cursor walk. Respect
`no-store`, keep gaps as gaps, and leave an absent value unknown. Unsupported capabilities, request
failures, pending observations, stale observations, and legitimate empty windows remain distinct.

Performance facts support a measured diagnosis; this observability surface does not automatically tune
parallelism, batch sizes, or caches. Benchmark results describe their named machine, JDK, connectors,
workload, and configuration. They are not a throughput or latency SLO across hardware.
