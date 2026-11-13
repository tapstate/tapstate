---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/reference/transforms
---

# Keeping one row per group current

An `aggregate` step groups the rows of one input and keeps one output row per group, with fields
computed over the group's rows: a count, a sum, a maximum, the latest value. It is a `GROUP BY` that
stays current as the source changes. A row that changes group leaves the old one and joins the new one,
and a group whose last row leaves is deleted from the target.

```yaml
version: tapstate/v1
kind: pipeline
id: sales_by_region
source: src_orders
transforms:
  - id: sales_by_region_day
    from: [ orders ]
    type: aggregate
    key: [ id ]
    where: "=after.status != 'cancelled'"
    groupBy:
      region: $region
      day: "=after.created_day"
    fields:
      order_count: { count: true }
      revenue: { sum: $amount, default: 0 }
      max_order: { max: $amount }
      latest_status: { last: { value: $status, by: $updated_at } }
serve:
  from: sales_by_region_day
  sync: [ { id: dw, source: tgt_sales, write_mode: upsert, on_full_load: clear } ]
```

The target holds one row per `(region, day)` with those four fields, keyed by `region` and `day`.

## The step

| Key | Meaning |
|---|---|
| `from` | One input: a source table, or a `filter`, `map` or `union` step over source tables. |
| `key` | The columns that identify one input row. Absent uses the input's primary key; required when it has none. |
| `where` | A predicate over the input row. A row that fails it is in no group, and a row that stops passing it leaves its group. |
| `groupBy` | Output key fields by name, each a `$column` or an `=expression` over the input row. Declared order is the order of the output key. Absent or empty gives one group holding every row. |
| `fields` | Output fields by name, each one function over the group's rows. |

Expressions read the input row as `after.<column>`, as every row expression does.

A step with no `groupBy` keeps a single row, the total over the whole input. Every change to the input
updates that one row, so it is the slowest shape to keep current; see [what it costs](#what-it-costs).

### `where:` or a `filter` step upstream

Both keep rows out of a group. They differ in what they need from the source.

- `where:` is evaluated by the step, which remembers which group every row is in. A row that stops
  matching is taken out of its group whatever the source sends.
- A `filter` step upstream has to tell the step that a row stopped matching, and it can only do that
  when the source sends the row as it was before the update. On a source that does not guarantee this,
  an aggregate step reading a `filter` is refused with `aggregate.upstream-filter-unsafe`.

Use `where:` unless the filter is shared with other consumers.

## The functions

The functions, `filter:`, `type:`, `default:`, the null rules and the numeric rules are those of
[rollup fields](../nest/rollup-fields.md), unchanged. In short:

| Function | Argument | Empty group |
|---|---|---|
| `count` | `true` or an expression (its non-null values) | `0` |
| `count_distinct` | an expression | `0` |
| `sum`, `avg` | a number | null, or `default:` |
| `min`, `max` | a number, string or temporal | null |
| `first`, `last` | `{ value: <expr>, by: <expr> or [<expr>, ...] }` | null |

A group only exists while it holds at least one row, so "empty" here means a field whose `filter:` or
null arguments left it nothing to compute over. Ties in `first` and `last` that survive every `by`
expression are decided by the input row's key.

Numbers are exact or refused. A sum over a 64-bit integer stays a 64-bit integer, a sum over a decimal
keeps its scale, and nothing is rounded except `avg`, whose scale is part of its type. A column whose
numeric type nothing resolved cannot be summed or averaged and is refused at apply with
`aggregate.type-unknown`.

## What reaches the target

Each group is one row, written as an upsert on the `groupBy` names and deleted when its last row leaves.
A change to one input row sends the rows of the groups it touched and nothing else.

Within a group, every field of a row was computed over the same set of input rows. Across groups there
is no such promise: when a row moves from one group to another, the target may briefly hold the row in
both or in neither. Totals added up across groups can therefore be off by a moving row for a moment, and
are right again once both groups have been written.

### The target must be cleared on a full load

A target that reads an aggregate step must be set to `on_full_load: clear` (or `fail`). A full load
rebuilds the step's state from nothing, and a target that kept its old rows would keep groups that no
longer exist. The default policy keeps rows, so it is refused at apply with
`aggregate.rebuild-needs-clear`.

### Changing the step

The step's state is valid only for the definition that built it. After a change to `from`, `key`,
`where`, `groupBy` or `fields`, the step refuses to start on the old state with
`aggregate.definition-changed`, naming what changed. Rebuild the pipeline from a full load, which clears
the state and the target, or restore the previous definition.

## A group that cannot be given exactly

Some results cannot be computed exactly on some data: a sum that no longer fits its type
(`aggregate.overflow`), a value whose type is not the one its column declared
(`aggregate.value-type-mismatch`), a result `type:` cannot hold (`aggregate.result-not-convertible`).
The step never writes a wrong number. It keeps the group's row at the last values that were exact and
marks it:

```json
{
  "region": "EU", "day": "2026-03-01",
  "order_count": 41, "revenue": "18200.00",
  "__tap": { "aggregate": { "sales_by_region_day": {
    "status": "failed",
    "code": "aggregate.overflow",
    "failed_since": "2026-03-01T10:15:00Z",
    "last_good_at": "2026-03-01T10:14:58Z"
  } } }
}
```

A row without the marker is current. A row with it is stale since `last_good_at`; a group that never had
exact values carries nulls. The other groups are not affected, and the group recovers by itself: the
first change after which every field can be given again writes a normal row, without the marker.

The number of such groups is reported per step (see [what it reports](#what-it-reports)).

## What it keeps

The step recovers by replaying the source, so it cannot keep running totals alone: a replayed row would
be counted twice. It stores one contribution per input row and computes every field from those.

For each input row it keeps:

- one entry recording which group the row is in;
- one entry on the group's side holding what the row contributes;
- one more entry for each field that has to keep its inputs: `min`, `max`, `first`, `last` and
  `count_distinct`. A retracted maximum cannot be undone from a total.

That is **2 + m entries per input row**, where m is the number of such fields, plus one entry per group.
The example above keeps 4 per row. A row that fails `where:` is in no group but is still remembered, with
one entry, so that the step knows it has seen it.

**The state holds the input's values**: the group names, the summed amounts, the values ordered for
`max` and `last`. Treat the state store as you treat the source data.

A row that was deleted, or that left a group, is remembered until the source can no longer deliver the
change that removed it a second time, and is then forgotten.

### What it costs

- Applying a pipeline states the cost before anything runs: `aggregate.state-estimate` gives the entries
  per row and the total for the rows the inputs were discovered to hold. When an input reports no row
  count, `aggregate.state-estimate-unsized` gives the cost per row and no total.
- `explain <pipeline>` shows the same estimate beside what the running step last counted. It answers
  once the pipeline is running.
- All changes to one group are applied by one worker, in order. Many changes to the same group in a
  short time are folded into one write, but a step with very few groups (or none) cannot spread its work.

Measured throughput and state size are not published yet.

## What it reports

Per step, in the pipeline's status and metrics. Nothing is reported per group.

| Status key | Metric | Meaning |
|---|---|---|
| `aggregateRows.<step>` | `tapstate.pipeline.aggregate.rows` | input rows that are in a group |
| `aggregateGroups.<step>` | `tapstate.pipeline.aggregate.groups` | groups |
| `aggregateStateEntries.<step>` | `tapstate.pipeline.aggregate.state.entries` | every entry the step stores |
| `aggregateTombstones.<step>` | `tapstate.pipeline.aggregate.tombstones` | records kept only until a second delivery is impossible |
| `aggregateLargestGroupRows.<step>` | `tapstate.pipeline.aggregate.group.rows.largest` | rows in the largest group |
| `aggregateFailedGroups.<step>` | `tapstate.pipeline.aggregate.groups.failed` | groups held at their last good values |
| `aggregateInputs.<step>` | `tapstate.pipeline.aggregate.inputs` | input events applied since the step started |
| `aggregateStateWrites.<step>` | `tapstate.pipeline.aggregate.state.writes` | commits to state since the step started |
| `aggregateOrderConflicts.<step>` | `tapstate.pipeline.aggregate.order_conflicts` | input events not applied since the step started |

The first six are counted by a pass over the step's state and are as of the last pass that finished. A
pass reads about 4,000 entries a second, so a state of five million entries is counted in about twenty
minutes, and a pass starts over when the step restarts. Until the first one finishes they are absent.
Absent means not measured yet, not zero. `aggregateOrderConflicts` is absent until there is one.

Inputs divided by state writes says how many changes one write absorbs. A number of tombstones that only
grows says the source's replay position is not advancing.

## Check and repair

Because every contribution is stored, a group's fields can be recomputed from its stored rows and
compared with what the step holds.

```text
aggregate <pipeline-id> <step>                                  check every group
aggregate <pipeline-id> <step> --group region=EU,day=2026-03-01 check one group
aggregate <pipeline-id> <step> --group region=EU,day=2026-03-01 --repair
```

`--group` names every `groupBy` field; a name with no `=value` is the null of that column. A date is
written as `2026-03-01`. When the step holds no group of that name the answer says so, and a repair of
it writes and sends nothing. Over HTTP the
same two actions are `POST /api/pipelines/{id}/aggregates/{step}:check` and `:repair`, with an optional
body `{ "group": { "<name>": "<value>" }, "after": "<cursor>" }`.

- A **check** changes nothing. It reports each group whose stored fields differ from what its stored
  rows give (`aggregate.total-mismatch`), by field.
- A **repair** writes the group's fields again from its stored rows, in one commit, and sends the group
  to the target again. It requires write access and is audited.
- A group too large to recompute in memory is reported as too large to decide, and nothing is claimed or
  changed for it.
- The step answers between two of its own updates, so the pipeline keeps running. It must be running:
  otherwise the answer is `aggregate.step-not-running`.

**What a check does not prove.** It compares the step's fields with the rows *the step holds*. It reads
neither the source nor the target. A clean check says the fields are consistent with the stored rows; it
does not say the stored rows are the source's rows, or that the target holds what the step sent. To
compare with the source, run the `GROUP BY` there and compare with the target.

A check of every group lists groups by their stored fields. A group whose stored fields were lost
altogether is found only by a check that names it.

## Refusals

| Code | When |
|---|---|
| `aggregate.input-not-supported` | `from` is not a source table or a `filter`, `map` or `union` over source tables |
| `aggregate.upstream-filter-unsafe` | reads a `filter` step on a source without guaranteed before-images |
| `aggregate.key-required` | the input has no primary key and the step names no `key:` |
| `aggregate.name-conflict` | a name is used for both a group and a field |
| `aggregate.expression-invalid` | `where`, a `groupBy` expression, an argument, a filter or a default does not check |
| `aggregate.type-unknown` | `sum` or `avg` over a column whose numeric type nothing resolved |
| `aggregate.rebuild-needs-clear` | a target reading the step keeps its rows on a full load |
| `aggregate.definition-changed` | the stored state was built by a different definition |

## Known limits

- One input. A nest or a join cannot be the input of an aggregate step.
- A pipeline with an aggregate step cannot be previewed: there is no state behind a preview.
- A decimal column cannot be compared with a literal in `where:` or `filter:` yet
  (`=after.amount >= 1000` over a decimal is refused at apply).
- No `HAVING`, top-N, array or string aggregation, or windows. The SQL `GROUP BY` spelling is not
  available yet.
- Embedding a group into a nest document has no end-to-end case yet; do not rely on it.
- The step is correct on one cluster member. Running it across several members is not supported yet.
- Throughput and state size on large inputs have not been measured.
