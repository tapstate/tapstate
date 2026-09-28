---
status: engineering-draft
publication: handoff
---

# Performance and telemetry metric inventory

This inventory describes the facts produced by the corresponding implementation, rather than a promise
that a deployed server supports every reading. Follow [Observe a pipeline](README.md) for queries and
operator actions. Keep this table with changes to the fact producers, their measurement boundaries,
`CardinalityBudget`, and the focused measurement tests.

Pipeline facts use the existing metrics response and the configured exporter. Process facts are available
only through a configured Prometheus or OTLP exporter and local diagnostic logs; there is no process REST
endpoint. Process facts never acquire a pipeline, member, job, bucket, row key, or data-value label. Node
identity belongs to the export resource. Incarnation and execution identity remain internal.

## Types, time, and absence

`C` means a monotonic counter with an explicit start time, `G` a gauge, and `H` a histogram. Durations in a
histogram retain count, sum, and registered buckets, rather than a precomputed average or percentile.
Counter rates and percentiles are computed by the reader. A `.duration.max` gauge is the largest completed
measurement since its local account began; it is not a histogram or a counter. Read every point's own
measurement time instead of assigning the whole observation's timestamp to cached or collected readings.

An unwired or unavailable source is absent. Quiet distributions and quantities with no observations can
also be absent. A wired queue, initialized counter, or measured Boolean may legitimately be zero; that
does not authorize a client to fill an absent measurement with zero. A counter's start is the data job,
worker, sampler, JVM, or export stream that owns it, as identified below.

## Pipeline facts

All rows include `tapstate.pipeline.id`. The attribute sets listed below are the additional attributes.
Open-dimension budgets apply independently to each pipeline and instrument. Closed sets do not grow with
records. An overflow point preserves the instrument's aggregate and has `otel.metric.overflow=true`.

| Instrument | Type / unit | Measurement boundary | Additional attributes / budget | First action |
|---|---|---|---|---|
| `tapstate.pipeline.records` | C / `{record}` | Successful source handoff (`in`) or durable target acknowledgement (`out`); repeated work is counted again | `tapstate.table.id`: 1,000; `direction=in,out`; source `op=insert,update,delete,read,ddl,other` | Compare source work with target ACK; do not substitute issued batches for ACK |
| `tapstate.pipeline.bytes` | C / `By` | Logical serialized payload at the corresponding in/out boundary, not compressed network bytes | `tapstate.table.id`: 1,000; `direction` | Compare payload growth with records and serialization cost |
| `tapstate.pipeline.lag` | G / `s` | Age of the latest target-acknowledged source event, per table; an idle source can make this age rise | `tapstate.table.id`: 1,000; highest-value overflow | Compare source activity, target ACK, and gaps before diagnosing backlog |
| `tapstate.pipeline.record.delivery.duration` | H / `s` | Completed target delivery measured from the source event time; start follows the current counting job | `tapstate.table.id`: 1,000 | Compare delivery distribution with stage and target-write distributions |
| `tapstate.pipeline.process.duration` | H / `s` | Completed timed source read/project, transform call, join/nest drain, or sink drain; stage units differ | `stage=source,transform,join,nest,sink`: 5 | Identify the expensive stage, then inspect its queue or downstream sink |
| `tapstate.pipeline.process.active` | G / `{work}` | Processor slots currently inside those timed business units at one complete current-job collection | Fixed `stage`: 5 | Compare active work with stage duration and pipeline queue pressure |
| `tapstate.pipeline.work.active` | G / `{work}` | Total active business slots only after every expected staged vertex is represented in the same execution collection | None: 1 | Distinguish synchronous stage work from pending asynchronous target batches |
| `tapstate.pipeline.queue.depth`, `tapstate.pipeline.queue.capacity`, `tapstate.pipeline.queue.high_water` | G / `{item}` | Paired built-in Jet input queue size/capacity for the current job; high-water is the highest collected sample | None: 1 each; highest-value high-water overflow | Compare sampled depth with capacity and sink pending limits |
| `tapstate.pipeline.stage.queue.depth`, `tapstate.pipeline.stage.queue.capacity`, `tapstate.pipeline.stage.queue.high_water` | G / `{item}` | Complete same-collection business-processor input queues summed by stage; peak is the highest collected stage sum in this job/execution | Fixed `stage`: 5 each; added depth/capacity, highest-value high-water overflow | Compare the stage's input occupancy with active work, duration, and downstream sink pending limits |
| `tapstate.pipeline.stage.output.refused` | C / `{offer}` | Actual ordinary data, frontier, control, or watermark outbox offers returning false; starts with the current stage's processor accounts | Fixed `stage`: 5 | Compare refusals with stage input queues and downstream sink pending limits; quota yields also count |
| `tapstate.pipeline.stage.output.retry.duration` | H / `s` | First refused ordinary offer to the next accepted ordinary offer on that processor; only completed intervals | Fixed `stage`: 5; registered process-duration buckets | Compare completed retry time with refusals and delivery latency; the interval includes scheduling, backoff, and blocking waits |
| `tapstate.pipeline.sink.batch.issued` | C / `{batch}` | Actual batch handoff, not completion | None: 1 | Compare with completed output and pending batches |
| `tapstate.pipeline.sink.batch.records` | C / `{record}` | Records handed to issued batches | None: 1 | Compare batch size and durable output |
| `tapstate.pipeline.sink.batch.records.max` | G / `{record}` | Largest issued batch in this job | None: 1; highest-value overflow | Check whether batching reaches the configured size |
| `tapstate.pipeline.sink.batch.pending`, `tapstate.pipeline.sink.batch.limit` | G / `{batch}` | Pending delivery futures and the corresponding finite in-flight limit | None: 1 each | Inspect target latency when pending reaches the limit |
| `tapstate.pipeline.sink.backpressured` | G / `{sink}` | Sink processors currently prevented from accepting more work by their in-flight bound | None: 1 | Check pending batches and target completion |
| `tapstate.pipeline.sink.batch.write.duration` | H / `s` | Writer handoff to actual future completion, including synchronous preparation; late reap does not extend it | None: 1 | Separate target completion time from stage drain time |
| `tapstate.pipeline.sink.backpressure.duration` | H / `s` | Completed intervals in which the sink's pending limit prevented acceptance | None: 1 | Compare with batch-write time and configured pending limit |
| `tapstate.pipeline.state.store.operation.count` | C / `{operation}` | Completed or thrown Join/Nest cold-store call; one baseline per current job and member | `tapstate.state.namespace`: 1,000; `state.operation=load,load_all,save,delete`; `state.outcome=success,failure` | Compare call counts with backfills and write activity |
| `tapstate.pipeline.state.store.operation.duration.sum` | C / `ns` | Elapsed cold-store call time, including blocking waits | Namespace: 1,000; fixed `state.operation` | Compare duration deltas with call deltas and Mongo query plans |
| `tapstate.pipeline.state.store.operation.payload.bytes` | C / `By` | Actual byte-array payload handled by the cold store; excludes command envelopes and wire compression | Namespace: 1,000; fixed `state.operation` | Check payload and cold-layer traffic before changing parallelism |
| `tapstate.pipeline.state.store.serialization.count` | C / `{serialization}` | Completed Java encoding or decoding | Namespace: 1,000; `state.codec=encode,decode` | Compare repeated encoding work with state calls |
| `tapstate.pipeline.state.store.serialization.bytes` | C / `By` | Bytes handled by those completed codecs | Namespace: 1,000; fixed `state.codec` | Compare codec traffic with cold-store payload |
| `tapstate.pipeline.nest.cold_layer.over_threshold` | G / `1` | Last qualifying decision window: at least 100 accesses and at least half served from cold state | `tapstate.nest.namespace`: 1,000; highest-value overflow | Inspect backfill/access deltas and hot-state budget |

Stage active work is sampled work occupancy, not CPU utilization or executor saturation. A transform's
subsequent outbox drain is outside its synchronous call. A sink waiting on an asynchronous target future
is represented by pending batches, rather than an active processor slot. Empty source polls do not add
duration. Quiet, unwired, missing-processor, mixed-execution, or incomplete stage accounts are absent.
State-store totals likewise require every current member's complete reading from the same job execution;
one surviving member's cost is not a pipeline total.

Stage queue peaks reset with the job or physical execution. An entirely quiet queue is absent until
occupancy has been observed; a later empty queue can report measured depth zero beside its retained
sampled peak. Cancelled, superseded, missing, or older same-scope collections cannot restore old peaks.
Each point keeps the actual stage collection time. These are input queues of declared business
processors; pass-through framework queues can remain part of the separate pipeline total.

Output refusals and retry intervals cover owned source, transform, join, and nest processors. The sink's
pending-batch and backpressure facts retain their separate boundary. Each actual outbox offer is observed:
a flat-map retry that accepts an earlier item and refuses a later one closes one interval and opens
another. Repeated refusals do not restart an open interval. Snapshot persistence offers are excluded;
cancellation discards an unfinished interval instead of reporting a successful retry. Histogram sum,
count, and all buckets come from one processor snapshot, and pipeline stage totals require the complete
current execution's business-processor collection. Quiet or incomplete output accounts are absent,
without hiding independently measured active work or queues. After a refusal, its measured counter can
appear before any retry completes. These facts cannot distinguish a full downstream queue from Jet's
callback quota or scheduler delay, and they do not measure executor saturation.

## Process facts

Closed process attributes and their point limits are: `sink=latest,history,event,export` (4),
`verb=start,pause,resume,stop` (4), `resolution=5m,30m,1h,3h,6h` (5), and
`call=snapshot_read,sink_write` with `outcome=success,failure` (4 combinations). Label-free instruments have
one point. These local worker counters start with their worker or sampler; collection never increments
them merely because Prometheus scraped them.

| Instrument | Type / unit | Measurement boundary and attributes | First action |
|---|---|---|---|
| `tapstate.process.lifecycle.slots.active` | G / `{slot}` | Current actuator work occupying the shared lifecycle slots; no labels | Compare with configured concurrency and work duration |
| `tapstate.process.lifecycle.pipelines.pending`, `tapstate.process.lifecycle.queue.depth`, `tapstate.process.lifecycle.queue.high_water` | G / `{pipeline}` | Accepted or capacity-refused pending pipelines, queued intents, and maximum queue depth; no labels | Read explain capacity reasons and distinguish queueing from job failure |
| `tapstate.process.lifecycle.intent.coalesced`, `tapstate.process.lifecycle.intent.cancelled` | C / `{intent}` | Superseded and cancelled lifecycle intents; no labels | Inspect rapid intent changes before increasing concurrency |
| `tapstate.process.lifecycle.capacity.refused` | C / `{offer}` | Work offers refused by the bounded admission queue; no labels | Check slow work and admission capacity |
| `tapstate.process.lifecycle.capacity.wait.duration` | H / `s` | Completed admission waits; no labels | Compare queue time with actuator work time |
| `tapstate.process.lifecycle.work.duration` | H / `s` | Completed actuator work by fixed `verb`; up to 4 points | Identify slow validation, submission, pause/resume, or teardown |
| `tapstate.process.telemetry.queue.depth`, `tapstate.process.telemetry.queue.high_water`, `tapstate.process.telemetry.in_flight` | G / `{task}` | Current or peak accepted work by wired `sink`; up to 4 points | Inspect that sink's failures, deadline logs, and last-success age |
| `tapstate.process.telemetry.coalesced`, `tapstate.process.telemetry.dropped` | C / `{frame}` | Latest frames merged or telemetry work lost at the dispatcher boundary, by `sink` | Expect latest-state coalescing; inspect explicit history/event gaps for loss |
| `tapstate.process.telemetry.write.success`, `tapstate.process.telemetry.write.failure`, `tapstate.process.telemetry.write.timeout` | C / `{write}` | Actual successful accepted writes, failures, or watchdog timeouts, by `sink`; stale no-ops are not successes | Check local logs and the affected backend |
| `tapstate.process.telemetry.write.duration.max` | G / `ms` | Largest completed local write duration by `sink` | Check store/exporter latency; this is not p99 |
| `tapstate.process.telemetry.last_success.age` | G / `ms` | Age of a completed successful local write, when one exists, by `sink` | Compare with observation freshness and degraded state |
| `tapstate.process.telemetry.degraded` | G / `1` | Local sink loss not superseded by a qualifying successful attempt, by `sink` | Diagnose the sink; an older blocked completion does not establish recovery |
| `tapstate.process.telemetry.breaker.state` | G / `1` | Current closed/open/probe state encoded as 0/1/2, by `sink` | Check the failed backend and deadline before interpreting cooldown |
| `tapstate.process.telemetry.breaker.recovered` | C / `{recovery}` | Actual open-to-closed transitions after successful non-timeout work, by `sink` | Distinguish successful recovery from another failed probe |
| `tapstate.process.telemetry.gap.open` | G / `{gap}` | Current bounded open intervals for `sink=history,event`; at most 2 points | Inspect failed due samples or event marker persistence |
| `tapstate.process.telemetry.gap.opened`, `tapstate.process.telemetry.gap.closed` | C / `{gap}` | Real gap transitions, starting with their sampler/account; execution replacement is not recovery | Compare persisted event markers or retained history samples |
| `tapstate.process.telemetry.restoration.pending` | G / `{event}` | Event restorations waiting for their gap marker, `sink=event`; 1 point | Restore event storage; marker persistence precedes restored |
| `tapstate.process.telemetry.cleanup.failure`, `tapstate.process.telemetry.cleanup.rejected` | C / `{cleanup}` | Failed or rejected best-effort cleanup work; no labels | Inspect cleanup logs and old-resource residue without touching a recreated resource |
| `tapstate.process.telemetry.cleanup.degraded` | G / `1` | Known incomplete cleanup; an unrelated later success does not clear it; no labels | Check the conditionally scoped cleanup and TTL/janitor bounds |
| `tapstate.process.otlp.export.success`, `tapstate.process.otlp.export.failure` | C / `{export}` | Asynchronous collector export completion; an offer to the local exporter is not completion; no labels | Inspect collector endpoint, transport, and export logs |
| `tapstate.process.otlp.export.duration.max` | G / `ms` | Largest elapsed completed collector push; no labels | Compare collector completion latency with export interval/deadline |
| `tapstate.process.otlp.export.last_success.age` | G / `ms` | Age of an actual successful collector push, absent before success; no labels | Check an unreachable collector even when local offers succeed |
| `tapstate.process.otlp.export.degraded` | G / `1` | Result of the latest completed export attempt by admission order; old completion cannot clear a newer failure; no labels | Inspect collector failure rather than classifying the pipeline as failed |
| `tapstate.process.rollup.bucket.computed`, `tapstate.process.rollup.bucket.retried`, `tapstate.process.rollup.bucket.failed` | C / `{bucket}` | Completed, retried, or failed cache builds by fixed `resolution`; up to 5 points | Inspect owner eligibility, raw retention, and batch failures |
| `tapstate.process.rollup.build.raw_fallback` | C / `{bucket}` | Cache builds that read raw instead of valid finer rollups, by fixed `resolution`; up to 5 points | Inspect finer-input expiry before changing worker capacity |
| `tapstate.process.rollup.closed_through.age` | G / `ms` | Age of the last continuously closed bucket by `resolution`; not a freshness guarantee | Check cache validity and fallback separately |
| `tapstate.process.rollup.batch.duration.max`, `tapstate.process.rollup.batch.in_flight.age` | G / `ms` | Completed batch peak or current in-flight age; no labels | Distinguish slow completed batches from stuck work |
| `tapstate.process.rollup.degraded` | G / `1` | Worker build health; no labels | Check failures while continuing to serve bounded fallback |
| `tapstate.process.rollup.query.raw_fallback`, `tapstate.process.rollup.query.full_raw_fallback` | C / `{query}` | Returned queries using some or all raw data in place of a rollup, by `resolution` | Inspect missing, expired, partial, or marked-finer buckets |
| `tapstate.process.rollup.query.bucket.down_drilled` | C / `{bucket}` | Buckets that required finer data in those returned queries, by `resolution` | Verify the necessary ranges and query scan budget |
| `tapstate.process.observation_janitor.scanned`, `tapstate.process.observation_janitor.deleted` | C / `{document}` | Examined or conditionally removed documents; no labels | Compare scans/deletes without weakening owner/token/time guards |
| `tapstate.process.observation_janitor.failure` | C / `{batch}` | Failed bounded cold cleanup batches; no labels | Inspect Mongo errors and current batch phase |
| `tapstate.process.observation_janitor.batch.duration.max`, `tapstate.process.observation_janitor.last_success.age` | G / `ms` | Completed cleanup duration peak or age of successful cleanup, when known; no labels | Inspect batch latency and stalled progress |
| `tapstate.process.observation_janitor.degraded` | G / `1` | Known failed cleanup phase; another phase's success is insufficient; no labels | Inspect each cleanup phase before claiming recovery |
| `tapstate.process.connector.external.call.count` | C / `{call}` | Completed synchronous PDK snapshot-read or sink-write calls including callbacks, by `call/outcome` | Identify member-local read/write failures |
| `tapstate.process.connector.external.call.duration` | H / `s` | Elapsed time of those calls, by `call/outcome`; not long-running stream duration | Compare target batch completion and shared source-read cost |
| `tapstate.process.nest.stored_count.completed`, `tapstate.process.nest.stored_count.failed`, `tapstate.process.nest.stored_count.rejected` | C / `{query}` | Completed, failed, or refused namespace counts by two bounded background workers; no labels | Inspect namespace size, Mongo latency, and range index |
| `tapstate.process.nest.stored_count.duration.sum` | C / `ns` | Total elapsed completed count-command time; no labels | Compare time deltas with completed/failed command deltas |
| `tapstate.process.nest.stored_count.queued`, `tapstate.process.nest.stored_count.active` | G / `{query}` | Current queued or running count requests; no labels | Check worker pressure and refresh frequency |
| `tapstate.process.cpu.time` | C / `ns` | JVM-reported process CPU time from process start; no labels | Compare CPU deltas with the same workload window |
| `tapstate.process.cpu.load` | G / `%` | Supported JVM process CPU load rounded to an integer percentage; no labels | Compare queue and stage pressure; this is not executor saturation |
| `tapstate.process.jvm.heap.used`, `tapstate.process.jvm.heap.committed` | G / `By` | JVM heap management readings; no labels | Compare heap pressure and observed pauses; committed heap is not RSS |
| `tapstate.process.memory.rss` | G / `By` | Linux `smaps_rollup` RSS sampled every 15 s; original sample time; expires after 30 s; no labels | Compare resident bytes on the same host and workload |
| `tapstate.process.jvm.gc.collections` | C / `{collection}` | Sum of supported collector MXBean counts from process start; no labels | Compare collections and heap behavior |
| `tapstate.process.jvm.gc.collection.time` | C / `ms` | Sum of supported MXBean collection elapsed times; no labels | Treat this as collection time, rather than exact stop-the-world pause |
| `tapstate.process.jvm.gc.pause.observed.duration.sum` | C / `ns` | Observed JFR `GCPhasePause` durations from stream start; absent before first pause or after known loss/failure; no labels | Compare deltas with latency; the bounded stream is not a lossless lifetime account |
| `tapstate.process.metrics.overflow.instruments`, `tapstate.process.metrics.overflow.series` | G / `{instrument}`, `{series}` | Distinct names and visible aggregate points currently folded by export; no labels | Inspect dimensions and budgets; these counts do not reveal the number of original series |

## Bounds and unsupported readings

The pipeline fold preserves totals, fixed dimensions, and histogram buckets. It removes only the open
dimension from excess points; gauges use their declared added or highest-value rule. The exporter applies
a separate limit of 10,000 visible points per instrument across pipelines and preserves overflow totals.
Even one pipeline at its 1,000-table budget can exceed that export bound: six operations in both
directions produce up to 12,000 record points. A table budget is therefore not an exemption from export
folding. The export backstop uses one aggregate carrying only the overflow marker, so those excess points
also lose their pipeline and operation labels. These limits bound series, not all payload bytes or total
storage IO.

Connector-internal retries, independent flushes, and connection-pool active/waiting/ceiling have no
generic trustworthy source and remain absent. A shared physical snapshot call is not copied into every
subscribing pipeline's counters. Member-local connector facts cannot apportion that shared cost by
pipeline. Use pipeline sink batch duration/pending/limit for the owning target path.

Full cooperative/blocking executor saturation is unimplemented: tasklet counts, idle iterations, and a
blocking cached pool's worker count do not supply busy time, runnable waiting, or finite pool capacity.
Stage active work and backpressure remain accurately named pressure evidence. No automatic tuning or
cross-hardware throughput/latency SLO is implied by these facts.
