---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/nest/semantics
---

# What a nest view is, and how to read one

This page is the reference for what a `nest` step promises, in plain words, and for the two commands that
show you what one is doing. Read it once before running a nest on your own data; come back to it when a
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
6. **Nothing is shown before it is safe to resume from.** The position the pipeline resumes from after a
   restart never moves past a change that is held and not yet in a sent document.

## The two shapes

Each embed is one of two shapes, chosen with `as:`.

| `as:` | The field holds | With no matching row |
|---|---|---|
| `array` | every matching row, keyed by `arrayKey` | an empty array |
| `object` | one row | the field is left out |

A third shape, `flat`, merges one row's fields into its parent instead of under a field; it has a page of
its own: [Flat embeds](flat-embeds.md).

Independently of shape, an embed joins in one of two **directions**, read from which side of `on:`
identifies a row:

- **gathered** - the child row carries its parent's key (`order_items.order_id` points at `orders.id`). The
  child is placed under the one parent it names.
- **referenced** - the parent row carries the child's key (`orders.customer_id` points at `customers.id`).
  The child row is shared: one customer under many orders. Changing that customer rewrites every order that
  points at it.

## Limits

A nest refuses to hold some things rather than absorbing them silently. When one is reached the run stops
with a code that names the key and the limit, so you can tell a data problem from a sizing problem.

| Code | What reached the limit | Usually means |
|---|---|---|
| `nest.root-fanout-limit-exceeded` | elements in one document | the data really is this wide, or the embed is too broad |
| `nest.reference-fanout-limit-exceeded` | documents pointing at one shared row | one edit would rewrite too many documents; embed from the other side |
| `nest.pending-limit-exceeded` | changes held for a parent that has not arrived | parents are very late, or a reference is broken for good |
| `nest.tombstone-limit-exceeded` | records of deletions kept in one document | the resume position is not advancing; find what holds it back |
| `nest.flat-cardinality-violation` | live rows matched by a `flat` embed | the relationship is one-to-many; use `as: array` |

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
  document, as last sent:
    {
      "id": 1,
      "customer_id": 1,
      "items": [ ... ],
      "customer": { "id": 1, "name": "Ann" }
    }
```

- **root row** - `present` with its order; `not arrived`, when only rows under it have come; or `deleted`.
- **parts** - every row held under the root, at every depth, with the position it was taken at (`epoch:seq`;
  higher wins). `deleted` marks a record of a deletion; `held` marks a row under a root that is absent.
- **waiting for a parent row** - rows held under a deeper parent that has not arrived, and how many.
- **references with no row** - shared rows the document points at that were not found.
- **document** - as last sent when nothing is unsent, otherwise as it will next be sent, with the count of
  changes not yet sent. While the root row is absent there is no document.

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

## Metrics

Each nest state namespace (one per level of each nest step) publishes these, with the namespace as the
`tapstate.nest.namespace` attribute. They count from when the run began; a restart starts a new count.

| Metric | Type | What it counts |
|---|---|---|
| `tapstate.pipeline.nest.roots.rendered` | counter | whole documents sent by a step's root level |
| `tapstate.pipeline.state.commit.groups` | counter | durable writes of the namespace's state |
| `tapstate.pipeline.state.commit.keys` | histogram | keys per durable write |
| `tapstate.pipeline.state.commit.round_trips` | histogram | store round trips per durable write |
| `tapstate.pipeline.state.commit.duration` | histogram | seconds per durable write |
| `tapstate.pipeline.state.evictions` | counter | entries let go of from memory, to be read back when next needed |

The share of reads that had to go to the store is `tapstate.pipeline.nest.cold.keys` divided by
`tapstate.pipeline.nest.key.accesses` over the same window.
