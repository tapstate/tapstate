---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/guides/observe-a-pipeline
---

# Observe a pipeline

Tapstate exposes two views of pipeline activity:

- current observation reads answer what the server last saw;
- bounded history answers how target-acknowledged output and lag changed over time.

The server also exposes one shared explanation of the current observation. Use it instead of
reimplementing diagnosis from status, metrics, and snapshot responses in each client.

All of these operations require an authenticated credential with read scope. They do not change the
pipeline. The history and explanation responses, including coded errors, carry
`Cache-Control: no-store`.

## Choose the read that answers your question

| Question | REST | CLI | MCP |
|---|---|---|---|
| What lifecycle state was last observed? | `GET /api/pipelines/{id}/status` | `status <id>` | `pipeline_status` |
| What are the current counters and positions? | `GET /api/pipelines/{id}/metrics` | `metrics <id>` | `pipeline_metrics` |
| How far has the initial snapshot loaded? | `GET /api/pipelines/{id}/snapshot` | `snapshot <id>` | `pipeline_snapshot` |
| What did this node log for the pipeline? | `GET /api/pipelines/{id}/logs` | `logs <id>` | `pipeline_logs` |
| How did output rate and selected table lag change? | `GET /api/pipelines/{id}/metrics/history` | `metrics <id> --from ... --to ...` | `pipeline_metrics_history` |
| Why does the latest observation look this way? | `GET /api/pipelines/{id}/explain` | `explain <id>` or `status <id>` | `pipeline_explain` |
| How wide does each node of the current run run, and why? | `plan` in `GET /api/pipelines/{id}/status` or `.../explain` | `explain <id>` or `status <id>` | `pipeline_status` or `pipeline_explain` |

The snapshot read shows progress for the pipeline's initial load. A replacement run keeps a table's
confirmed progress even when it skips reading that table again. Once the target confirms the load,
the face shows 100% and uses the durably recorded row count when available; an older load without
that count falls back to the last discovery's estimate. Before confirmation, `rowsTotal` is only
that estimate and can lag a growing table. The current run's own snapshot read count is available as
`snapshot.rows.read.<table>` on the metrics face.

`pipeline.explain` requires a current observation. A newly started pipeline may return
`monitor.no-observation` until its first observation is published. History is independent of the
latest observation and can still return retained samples for a stopped pipeline.

The current read faces are separate requests, not one transactional snapshot. If you need a diagnostic
conclusion, read `pipeline.explain`; do not join the other responses and copy its rules.

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

## Read how wide a run is

`status` and `explain` both carry an optional `plan`: the plan the pipeline's current run was submitted
on, written down when the run was submitted and replaced by the next run's. The two faces send it in
the same shape. It is absent when no run has one recorded, for example after the pipeline was stopped.
No explanation rule reads it; it answers beside the diagnosis.

| Field | Meaning |
|---|---|
| `claimGeneration`, `executionGeneration`, `topologyRevision` | Which run the plan belongs to. Absent where nothing fences the run, such as a server that is not a cluster member |
| `members` | The members the widths were worked out for, by stable id |
| `plannedAt` | When the run was planned |
| `nodes[].requested`, `nodes[].requestedOrigin` | The target total the node was given, and whether its author wrote it (`explicit`) or it is the default for the node's kind (`node-default`) |
| `nodes[].scope` | `total-one`: one processor for the whole cluster. `native`: the same number of processors on every member |
| `nodes[].memberCount`, `nodes[].computedLocal`, `nodes[].effective` | The member count and per-member count the width was worked out for, and the processors that makes in total. `computedLocal` is absent for `total-one` |
| `nodes[].reasons` | Stable ids for why the width is what it is: `requested-one`, `source-reads-not-split`, `single-target-keyless`, `key-not-derivable`, `rounded-up`, `rounded-down`, or `budget:<name>` |
| `nodes[].batch` | `maxRecords` and `maxWaitMillis`: the batch the node takes its input in |
| `nodes[].resources` | Sinks only: `writers`; `connectorMode` (`isolated`: a connector per writer, `shared`: one per member, used only for an artifact certified to be shared) and `connectorInstances`; `bufferedRecords`, two batches per writer; and `edgeQueueRecords`, a full queue from every processor sending into the sink to every processor it takes its input on. These are upper bounds worked out before anything opens. A connector's own connection pool is sized inside the connector and is not counted |

The status watch stream does not carry the plan. Read `status` again after a restart to see the new
run's plan.

## Coded errors and operator response

Every coded REST error is `{code, params, message}` and carries `Cache-Control: no-store`.

| HTTP | Code | Response |
|---|---|---|
| 400 | `control.malformed-request` | Correct timestamps, range, resolution, limit, or selectors. |
| 400 | `monitor.invalid-cursor` | Repeat the exact query or start a new first page. |
| 400 | `monitor.query-budget-exceeded` | Narrow the range or select fewer table series. |
| 401/403 | `control.unauthenticated` / `control.forbidden` | Supply a valid read-scoped credential. |
| 404 | `lifecycle.unknown-pipeline` | Check the id and apply the pipeline first. |
| 404 | `monitor.no-observation` | The pipeline exists; wait for its first observation and retry current reads. |
| 410 | `monitor.cursor-expired` | Start a new history query; do not resume the old walk. |

A 200 `NO_RETAINED_SAMPLES` history response is different from every error above. So is an absent
metric in an otherwise valid page. Keep those states distinct in dashboards and automation.

For live displays, poll current reads and `pipeline.explain` as independent observations. Refresh
history by fetching a new first page instead of caching or extending a completed cursor walk. Respect
`no-store`, keep gaps as gaps, and leave an absent value unknown. These rules prevent a stopped
publisher, reset counter, expired sample, and measured zero from collapsing into the same graph.
