---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/nest/semantics
---

# What a nest view is, and how to read one

This page is the reference for what a `nest` step promises, in plain words, and for the three commands that
show you what one will do and is doing. Read it once before running a nest on your own data; come back to it when a
document looks wrong.

## What a view is

A `nest` step is a **view**: a set of documents, one per row of the root table, each assembled from that
row and the rows of other tables that belong under it. It is not a query you re-run. Once it has loaded,
every insert, update and delete on any table feeding it changes the documents it affects, and the target
is kept equal to what the view says.

```yaml
- id: order_doc
  type: nest
  from: { o: orders, i: order_items, c: customers }
  root:
    from: o
    key: [ id ]                     # one document per order
    embed:
      - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
      - { from: c, on: { id: customer_id }, as: object, path: customer }
```

`o`, `i` and `c` are aliases you choose: `from:` names each table the step reads once, and the rest of the
step refers to it by that alias. Any short name works; there is no naming rule.

The **root key** (`id` above) is the document's identity. It is what the target is written by and what
`nest inspect` asks for.

## What it promises

These hold whatever order the rows arrive in, across restarts, and while a load is still running.

1. **The latest change to a row wins, and only the latest.** Every row carries the position its source
   gave it. A change older than what a document already holds for that row is ignored, so a replay after a
   restart, a snapshot row arriving after a live update, and a late duplicate all leave the same document.
2. **A document goes out whole.** What the target receives is the entire document, never a patch. A change
   to any row under a root rewrites that root in full; [Throughput limits](throughput-limits.md) is the
   arithmetic for what that costs.
3. **No document exists without its root row.** Rows that arrive before their root row are held, in their
   right place, and nothing is sent. The document appears when the root row arrives. When the root row is
   deleted the document is deleted from the target; what was under it is kept, so a root row that comes back
   brings its children back with it.
4. **A row that arrives before its parent waits for it.** A line whose order has not been read yet is held
   under that order's key, not dropped and not shown anywhere. It is placed the moment the order arrives.
   A row whose parent never comes stays held; `nest inspect` on the parent's key shows it.
5. **A deletion stays deleted.** Deleting a row leaves a record of the deletion behind, so an older insert
   replayed after a restart cannot bring it back. The record is dropped once no replay can reach it.
6. **Nothing is lost across a restart.** The position the pipeline resumes from never moves past a change
   that is neither in a sent document nor kept in the step's own durable state. A row waiting for its parent
   is kept in that state, so the resume position can move past it: it survives the restart and is placed
   when the parent arrives.

## The two shapes

Each embed is one of two shapes, chosen with `as:`.

| `as:` | The field holds | With no matching row |
|---|---|---|
| `array` | every matching row, keyed by `arrayKey` | an empty array |
| `object` | one row | the field is left out |

A third shape, `flat`, merges one row's fields into its parent instead of under a field: with `as: flat`
the example's order would carry `name` at its top level, not `customer.name`. Use it when the documents'
readers expect one level of fields, for example when replacing a denormalized table; keep `object` when
the fields should stay apart, because a flat field may not share a name with any field already in the
parent, and a second matching row stops the run. It has a page of its own: [Flat embeds](flat-embeds.md).

Independently of shape, an embed joins in one of two **directions**, read from which side of `on:`
identifies a row:

- **gathered** - the child row carries its parent's key (`order_items.order_id` points at `orders.id`). The
  child is placed under the one parent it names.
- **referenced** - the parent row carries the child's key (`orders.customer_id` points at `customers.id`).
  The child row is shared: one customer under many orders. Changing that customer rewrites every order that
  points at it.

With the example's rows, orders 1 and 2 both placed by customer 7, the two directions produce:

```text
orders:       { id: 1, customer_id: 7 }   { id: 2, customer_id: 7 }
order_items:  { id: 10, order_id: 1 }     { id: 11, order_id: 1 }     { id: 12, order_id: 2 }
customers:    { id: 7, name: "Ann" }
```

```json
{ "id": 1, "customer_id": 7,
  "items":    [ { "id": 10, "order_id": 1 }, { "id": 11, "order_id": 1 } ],
  "customer": { "id": 7, "name": "Ann" } }

{ "id": 2, "customer_id": 7,
  "items":    [ { "id": 12, "order_id": 2 } ],
  "customer": { "id": 7, "name": "Ann" } }
```

`items` is gathered: each item lands under the one order it names, and appears once. `customer` is
referenced: the same customer row is copied into both orders, so renaming Ann rewrites both documents.

## Limits

A nest refuses to hold some things rather than absorbing them silently. When one is reached the run stops
with a code that names the key and the limit, so you can tell a data problem from a sizing problem.

| Code | What reached the limit | Usually means | What to do |
|---|---|---|---|
| `nest.root-fanout-limit-exceeded` | elements in one document | the data really is this wide, or the embed is too broad | narrow the embed with a `filter` step before the nest, or raise `max_elements_per_document` if documents this large are what you want |
| `nest.reference-fanout-limit-exceeded` | documents pointing at one shared row | one edit to that row would rewrite too many documents | stop copying the shared row into every document: drop the embed and keep only its key (`customer_id`), or make the shared table the root and gather the others under it; raise the limit only if every one of those rewrites is what you want |
| `nest.pending-limit-exceeded` | changes held for a parent that has not arrived | parents are very late, or a reference is broken for good | `nest inspect` the parent key the error names; if that parent will never exist, filter its children out before the nest; if it is only late, raise the limit |
| `nest.tombstone-limit-exceeded` | records of deletions kept in one document | the resume position is not advancing | find what holds the resume position back, usually a target that is not confirming writes |
| `nest.flat-cardinality-violation` | live rows matched by a `flat` embed | the relationship is one-to-many | use `as: array` |

Changing `as:` does not change a reference's fan-out: the direction comes from `on:`, and a shared row is
copied into every document that points at it whatever the shape.

## Before it runs: `explain` on a nest step

```text
tapstate explain <pipeline> <step>
```

Compiles the step as a start would and prints what compiling it decided, then reads a small sample of
each table it embeds (200 rows and the table's row count, through the same read-only probes the data
browser uses) and warns about the shapes that would fail the run later. It writes nothing and needs no
running pipeline.

```text
order_doc  step order_doc  root o  key id
  at most 20000 elements per document
  build: not available: this version has no build phase, so the whole load runs through change assembly, one row at a time
  embeds:
    items  from i  as array  owned, gathered: each row names its one parent  on order_id = parent id
      sample: shop.order_items, 1000 rows; at most 4 under one parent in 200 rows read; about 3.3 per document
    customer  from c  as object  shared, referenced: the parent names the row, at most 100000 documents per row  on id = parent customer_id
      sample: shop.customers, 40 rows; at most 9 parent(s) on one row in 200 rows read; about 7.5 documents per row
  warnings: none from the sample
```

- **owned / shared** - whether each row belongs to the one document it names, or is a row many documents
  point at. The direction (gathered or referenced, above) is the one the compiler resolved from `on:`.
- **at most ... per document / per row** - the limits the run will be failed at, as this deployment and the
  nest's own `max_elements_per_document` set them.
- **sample** - where the embed was read from and what the sample counted. A table the data browser cannot
  read, an alias that names an upstream step or a pattern, or a table more than one source could hold, prints
  `not sampled` with the reason; qualify an ambiguous table as `source.table` in `from:`.

A sample is a sample: no warning means the 200 rows read raised none, not that none can happen. The
warnings, each naming the embed, the count it was estimated from and what to change:

| Warning | Raised when |
|---|---|
| `monitor.nest-object-key-not-unique` | an `object` or `flat` embed matched more than one sampled row under one parent; at run time `object` shows only the row changed most recently, and `flat` stops the run |
| `monitor.nest-near-element-limit` | an `array` embed is estimated at 80% or more of the per-document element limit |
| `monitor.nest-document-near-element-limit` | no embed is near the limit alone, but the owned embeds together (siblings added, nested arrays multiplied) are estimated at 80% or more of it |
| `monitor.nest-reference-widely-shared` | one shared row is estimated to be pointed at by 80% or more of the per-row limit |

An estimate is the larger of the most found under one parent in the sample and the average from the two
tables' row counts.

## Reading what a step is doing: `nest progress`

```text
tapstate nest progress <pipeline> [<step>]
```

One block per nest step: one sentence saying what the step is doing and, when it is held, which tables
hold it; how many documents it has sent against the root table's estimate; and one row per table the step
reads, with that table's load stage and rows read. A number the pipeline has not reported prints as `-`,
never as `0`.

```text
order_doc  running  initial load: snapshotting

step order_doc: loading; waiting for orders to be read
  documents sent: - of about 300 orders rows
  TABLE        PATH    LOAD     READ  ESTIMATE
  orders       (root)  pending  -     300
  order_items  items   landing  1000  1000
```

The sentence is the line to read first:

| It says | Meaning |
|---|---|
| `loading; waiting for <tables> to be read` | those tables are still being read; embeds are named before the root |
| `every table read; waiting for the target to confirm <tables>` | everything is read; the target has not confirmed these yet |
| `load complete; applying changes as they arrive` | the load is done and the view is being kept current |
| `load complete; the pipeline has finished` | a snapshot-only pipeline read and landed everything and stopped |
| `no load reported for some or all of its tables` | the pipeline has not reported a load for a table the step reads, so whether it is read through cannot be said |
| `not moving; the pipeline is <state>` | the pipeline is stopped, paused or failed |

`nest progress` reads what the pipeline last published, so it works while the pipeline is stopped.

## Reading one document: `nest inspect`

```text
tapstate nest inspect <pipeline> <step> <root key>...
```

Give one value per root key column, in order. It reads what the step **holds** for that key, not what the
target shows, so it also answers for a key the target has no document for. The pipeline must be running.

```text
order_doc  step order_doc  root key id=1
  root row: present, order 1:3
  parts:
    PATH      KEY   ORDER  STATE
    items     [1]   1:4    in document
    items     [2]   1:5    in document
  document, as the step last rendered it:
    {
      "id": 1,
      "customer_id": 1,
      "items": [ ... ],
      "customer": { "id": 1, "name": "Ann" }
    }
```

- **root row** - `present` with its order; `not arrived`, when only rows under it have come; or `deleted`.
- **parts** - every row held under the root, at every depth, with the position it was taken at (`epoch:seq`;
  higher wins; `epoch:snapshot` for a row the initial load read, which is below every change). `deleted` marks a record of a deletion; `held` marks a row under a root that is absent.
- **waiting for a parent row** - rows held under a deeper parent that has not arrived, and how many.
- **references with no row** - shared rows the document points at that were not found.
- **document** - as the step last rendered it when nothing is unsent, otherwise as it will next render it,
  with the count of changes not yet sent. While the root row is absent there is no document. This is what the
  step hands the target, not what the target stores: a target that writes by another key keeps its own `_id`,
  and a converted value is shown as the value it carries, a date as ISO-8601 text.

A line pointing at order 99, which does not exist, shows up like this:

```text
order_doc  step order_doc  root key id=99
  root row: not arrived; what is below waits for it
  parts:
    PATH   KEY  ORDER  STATE
    items  [7]  1:6    held
  document: none while the root row is absent; 1 change(s) held
```

The key is typed text and the state is typed, so `17` is tried as a whole number, a decimal and text, in
that order. A key column of another type, such as a date, cannot be named from the command line.

The output is a rendered view with a version of its own (`version` in the API answer), not the stored
state, so it stays the same when the way state is stored changes.

| Refusal | Meaning |
|---|---|
| `monitor.no-nest-step` | the pipeline has no nest step |
| `monitor.unknown-nest-step` | the step named is not a nest step; the message lists the ones there are |
| `monitor.nest-not-running` | the pipeline is not running on the node you asked |
| `monitor.nest-root-key-width` | the number of key values does not match the root key's columns |
| `monitor.nest-root-key-not-held` | nothing has reached the step under that key yet |
| `monitor.nest-root-key-not-resolved` | nothing was found, but the key has too many numeric columns for every way it may be stored to be tried, so it may still be held |

## Metrics

The counters a nest step publishes (documents sent, state writes, evictions, and how often state had to be
read back from the store) are listed with the other pipeline metrics in
[Observe a pipeline](../observability/README.md#nest-state-metrics).
