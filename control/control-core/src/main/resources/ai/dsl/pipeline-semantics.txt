# Pipeline Semantics

Use the vendored schema for exact YAML shapes. Use this reference for dataflow
and execution meaning, then consult `runtime-support.md` before making a run
claim.

## Contents

- Read settings
- Wiring forms
- Reusable `use`
- Transform bodies
- CEL envelope
- Serve
- View

## Read settings

Pipeline `settings` apply to the task, not to a source:

| Field | Semantic contract |
|---|---|
| `error_policy` | Requested record-error behavior: `fail`, `skip`, or `dead_letter`. |
| `batch_size` | Requested records per batch. |
| `parallelism` | Requested parallel workers. |
| `schedule` | Cron-style schedule, legal only for a bounded read. |
| `read_mode` | `snapshot_and_cdc`, `cdc_only`, or `snapshot_only`; meaningful for CDC sources. |
| `start_from` | Cursor for an incremental tail: `earliest`, `latest`, or an ISO-8601 instant. |

`read_mode` defaults in the grammar to `snapshot_and_cdc`. `snapshot_only` is
invalid for a pure stream. `schedule` requires a bounded read. `start_from`
requires an incremental tail and therefore cannot accompany a bounded-only
read. Runtime defaults and restrictions may be narrower than these grammar
rules.

## Wiring forms

Streaming transforms (`filter`, `map`, `js`, and `union`) use scalar or list
`from` wiring. The normalized meaning is always a non-empty list:

```yaml
from: orders
```

```yaml
from: [orders, returns]
```

`nest` and `join` require an explicit alias map. Body fields refer to aliases,
not directly to workspace resources:

```yaml
from:
  customer: customers
  order: orders
```

The table names in these Pipeline `from` clauses are the Pipeline's selection
for this synchronization. Keep the Source reusable by omitting `source.tables`
unless the user explicitly requires a Source-level allowlist; a Source-level
allowlist constrains every Pipeline that references that Source.

For streaming steps after the first, omitted `from` means the preceding step.
An omitted `from` on an inline view or serve block also means the preceding
transform, or the view for a serve block that follows one. Prefer explicit
wiring when editing or when a future reorder could change the natural source.

## Reusable `use`

A reusable definition separates logic from wiring:

```yaml
version: tapstate/v1
kind: transform
id: public_rows
type: filter
expr: "op != 'd'"
```

```yaml
transforms:
  - id: current_rows
    use: public_rows
    from: orders
```

The same rule applies to view and serve definitions: definitions never carry
`from`; Pipeline use sites always do. Scalar entries are sugar for a use
reference with natural-order wiring, but the first transform must provide an
explicit upstream.

## Transform bodies

### Filter

`filter.expr` is a CEL boolean predicate over the event envelope. Row events
whose result is false are dropped. Non-row DDL events bypass the filter so a
predicate cannot silently swallow schema evolution.

### Map

`map.fields` is an ordered mapping of output names to field rules:

| Authored value | Meaning |
|---|---|
| `$field` | Rename `field` to the output name and consume the old name. |
| `false` | Drop the same-named field. |
| `=expression` | Compute the output with CEL. The leading `=` is mandatory for a computed map rule. |
| Any other YAML value | Add or replace the output with that literal value. |

Rules run in declaration order. Declared outputs come first, followed by
unlisted input fields in input order. An output wins over a pass-through field
with the same name. A rename consumes its source name. A rename whose source is
missing produces nothing and does not suppress an unrelated same-named input.
Map applies to the `after` row image; events with no `after` image, including
deletes and DDL, pass through unchanged.

### JavaScript

`js.script` is the GraalVM escape hatch and sees all events, including DDL. The
script declares `process(record, ctx)` and may declare `filter(record)`.
`record` has the same `op`, `ts`, `src`, `before`, `after`, and `schema` fields as
the envelope. `ctx.emit(record)` fans out in call order and `ctx.log(message)`
logs; there is no state or lookup surface. Emitted records are followed by the
non-null return value. Return null with no emissions to drop the event. Use JavaScript
only when filter and map cannot express the change.

### Union

`union` explicitly merges multiple upstream streams. It has no body fields; its
multi-item list `from` defines the fan-in.

### Nest

`nest` materializes related streams into documents. Its alias-map `from` names
the available streams. `root.from` selects the parent alias; `root.key` defines
the parent upsert key; `root.mode` declares its write shape. Each `embed`:

- selects a child alias with `from`;
- maps child fields to parent fields through `on`;
- chooses `array`, `object`, or `flat` with `as`;
- writes array/object at `path`; flat must omit both `path` and `arrayKey` and merges one row into the parent;
- may define `key`, `arrayKey` for arrays, `ignoreUpdates`, and `trackKeyChanges`;
- may recursively contain more `embed` entries.

Flat supports one-to-one children and many-to-one shared referenced rows. Several flat siblings may
merge into one parent when their output fields are disjoint. A second live row under one parent is a
one-to-many relationship and stops with `nest.flat-cardinality-violation`; use `array` for that shape.
Fields may not overlap the parent, another flat child, or an object/array path. Discovered models catch
known conflicts before start and actual rows catch dynamic or model-less conflicts at runtime with
`nest.flat-field-conflict`. Rename or drop fields in an upstream map, retaining any join/key fields until
after Nest has used them.

`primary_key`, `order: main_first|sub_first`, capacity settings, and key-tracking switches provide
additional stateful materialization controls.

### Join

`join` materializes a flat wide table. Its alias-map keys become SQL relation
names. `engine` selects the query engine and `sql` defines the result.

## CEL envelope

Envelope-rooted filter, computed map, and push-format expressions bind:

| Binding | Type and meaning |
|---|---|
| `op` | String operation symbol: `i`, `u`, `d`, `r`, or `ddl`. |
| `ts` | Integer event timestamp. |
| `src` | String source stream name. |
| `before` | String-keyed dynamic map; empty when absent. |
| `after` | String-keyed dynamic map; empty when absent. |
| `schema` | String-keyed dynamic map; empty when absent. |

The compile environment includes the CEL standard library, standard macros
such as `has`, `exists`, `all`, `map`, and `filter`, plus declared `now()`.
Offline field types remain dynamic, so validation catches unknown envelope names
and syntax but cannot prove every field access or overload will evaluate.
`tables[].filter` is different: it is bound to discovered source columns and is
deferred rather than compiled offline.

## Serve

An inline Pipeline serve block or reusable `serve` body can declare three
surfaces:

- `sync`: table-model writes. `source` names a target connection supplier;
  `write_mode` is `upsert` or `append`; `ddl` is `apply`, `ignore`, or `fail`.
  Rename precedence is explicit `rename.map`, otherwise case conversion, then
  literal prefix and suffix. A sync `id` is required when a query backend names
  it.
- `query`: pull endpoints of type `rest`, `graphql`, or `mcp`. With no backend,
  the query is parallel egress from a view store; `backend` names a sync element
  when the sink serves the API.
- `push`: event-stream egress. `source` names a target connection supplier;
  `topic` selects the destination. `format` is either a whole-payload CEL value
  or an ordered field-rule map. Do not add a connector `type` field.

## View

A view is a materialized, queryable output. Inline views add `from`; reusable
view definitions omit it. `primary_key` identifies records. `storage` can
declare hot memory (`ttl`), warm database (`collection`, `indexes`), and cold
lake (`partition_by`) tiers. `schema.enforce` requests strict enforcement and
`schema.evolution` declares the evolution policy.
