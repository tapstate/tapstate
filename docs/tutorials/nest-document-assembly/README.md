---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/tutorials/nest-document-assembly
---

# Assembling one document out of many tables

A relational shop keeps an order in nine tables. An application that wants to show one order wants
one document. A `nest` transform is how Tapstate turns the first into the second and then keeps it
that way: every insert, update and delete on any of those tables lands in the document a second
later, without anyone re-running a query.

This tutorial builds that document from scratch, and then builds two more from the same nine tables -
one rooted at the customer, one at the product - because **which root you choose is the decision that
shapes everything else**. Along the way you will learn which shapes a nest can express and which it
cannot, which is the part that is hard to guess and quiet when you get it wrong.

Time: about 30 minutes. It assumes you have been through the
[online quickstart](../../quickstart-online.md) once, so the stack is up, the CLI is installed, and the
MySQL and MongoDB connectors are registered.

## 1. Load the shop

[`shop.sql`](shop.sql) creates nine tables and about two thousand rows. Nothing in it is random, so
your row counts are the ones quoted here.

On the compose stack from the quickstart, the databases publish no host ports, so load it through the
container:

```sh
docker compose exec -T mysql mysql -uroot -psecret < shop.sql
```

Running your own MySQL with a published port instead, use your client directly:

```sh
mysql -h 127.0.0.1 -P 3306 -u root -p < shop.sql
```

```
customers 12   products 20   orders 300   order_invoices 150   order_items 600
item_options 400   payments 180   shipments 86   shipment_events 247
```

The relationships, which are the whole subject of this tutorial:

```
customers ──< orders ──< order_items ──< item_options
                 │          │
                 │          └──> products        (the line points AT a product)
                 ├──< order_invoices  (at most one per order)
                 ├──< payments
                 └──< shipments ──< shipment_events
```

`──<` is one-to-many, `──>` is many-to-one. Read those arrows carefully; section 4 is entirely about
the difference between them.

## 2. Start with two tables

Author a source per group of tables, a target, and a pipeline. A source reads one or more tables; the
target is a `source` too, because a connection is a connection.

> **Addresses depend on where the server runs.** A connector runs inside the server, so it reaches the
> databases from there. On the compose stack that means the service names - `host: mysql` and
> `mongodb://mongo:27017/...`. With the server running on your own machine it means whatever host and
> port you published. The files below use the second form; swap the two lines if you are on compose.

`work/source/shop_core.tap.yml`:

```yaml
version: tapstate/v1
kind: source
id: shop_core
connector: mysql
config: { host: 127.0.0.1, port: 3306, database: shop, username: root, password: secret }
mode: cdc
tables: [ orders, order_invoices ]
```

`work/source/shop_lines.tap.yml` is the same with `tables: [ order_items, item_options ]`, and
`work/source/shop_fulfil.tap.yml` with `tables: [ payments, shipments, shipment_events ]`.

`work/source/shop_target.tap.yml`:

```yaml
version: tapstate/v1
kind: source
id: shop_target
connector: mongodb
config: { isUri: true, uri: "mongodb://127.0.0.1:27017/shopdocs?directConnection=true" }
```

Now the smallest nest that does anything - an order with its lines:

`work/pipeline/order_doc.tap.yml`:

```yaml
version: tapstate/v1
kind: pipeline
id: order_doc
source: [ shop_core, shop_lines ]
settings: { read_mode: snapshot_and_cdc }
transforms:
  - id: doc
    type: nest
    # Optional. Without this block, the deployment-wide operator-state database is used.
    state: { database: order_doc_state }
    from: { o: orders, i: order_items }
    root:
      from: o                    # the document is an order
      key: [ id ]                # identified by orders.id
      embed:
        - from: i                # lines go inside it
          on: { order_id: id }   # order_items.order_id matches orders.id
          as: array
          path: items            # under the field "items"
          arrayKey: [ id ]       # each element is identified by order_items.id
serve:
  from: doc
  sync:
    - source: shop_target
```

Validate, apply, discover, start:

```sh
tapstate validate work
tapstate -w work                       # then, in the session:
#   connect http://127.0.0.1:8080
#   login admin
#   apply
#   discover-schema shop_core
#   discover-schema shop_lines
#   apply                              # again: see the note below
#   start order_doc
#   status order_doc                   # must read: running
```

> **Discover before the second apply.** A nest sizes its levels from the row counts of the tables
> feeding them. Applied before discovery, those counts are unknown and you get
> `nest.capacity-estimate-incomplete` telling you the estimate is a floor. Discover both sources, apply
> once more, and the warning goes away.

> **`start` returning `running` is not proof it runs.** A pipeline whose configuration is wrong can
> still be accepted and then fail seconds later. Always read `status` afterwards; a failed pipeline
> prints its reason there.

Look at what landed - again through the container on the compose stack
(`docker compose exec mongo mongosh ...`), or straight from your machine if the port is published:

```sh
mongosh "mongodb://127.0.0.1:27017/shopdocs?directConnection=true"
```

```js
db.orders.countDocuments()               // 300
db.orders.findOne({ id: 70 })            // an order with an "items" array
```

## 3. Grow the tree

Everything else is more of the same shape. Add the remaining branches, and note that two of them
have children of their own:

```yaml
    root:
      from: o
      key: [ id ]
      embed:
        - from: inv                       # one-to-one: embedded as an object, not an array
          on: { order_id: id }
          as: object
          path: invoice

        - from: i
          on: { order_id: id }
          as: array
          path: items
          arrayKey: [ id ]
          trackKeyChanges: true
          embed:
            - from: opt                   # a third level
              on: { item_id: id }
              as: array
              path: options
              arrayKey: [ id ]

        - from: pay
          on: { order_id: id }
          as: array
          path: payments
          arrayKey: [ id ]

        - from: sh
          on: { order_id: id }
          as: array
          path: shipments
          arrayKey: [ id ]
          embed:
            - from: ev                    # a third level on the other leg
              on: { shipment_id: id }
              as: array
              path: events
              arrayKey: [ id ]
```

with the aliases declared once at the top of the transform:

```yaml
    from:
      o: orders
      inv: order_invoices
      i: order_items
      opt: item_options
      pay: payments
      sh: shipments
      ev: shipment_events
```

Restart the pipeline and count every branch against the source:

| Source table | Rows | In the documents | Count |
|---|---|---|---|
| `orders` | 300 | documents | 300 |
| `order_invoices` | 150 | documents with an `invoice` | 150 |
| `order_items` | 600 | `items` elements | 600 |
| `item_options` | 400 | `items[].options` elements | 400 |
| `payments` | 180 | `payments` elements | 180 |
| `shipments` | 86 | `shipments` elements | 86 |
| `shipment_events` | 247 | `shipments[].events` elements | 247 |

```js
db.orders.aggregate([
  { $project: {
      it:  { $size: { $ifNull: ["$items", []] } },
      inv: { $cond: [{ $ifNull: ["$invoice", false] }, 1, 0] },
      opt: { $sum: { $map: { input: { $ifNull: ["$items", []] }, as: "x",
                             in: { $size: { $ifNull: ["$$x.options", []] } } } } } } },
  { $group: { _id: null, docs: { $sum: 1 }, invoices: { $sum: "$inv" },
              items: { $sum: "$it" }, options: { $sum: "$opt" } } }
])
```

### Two shapes of "nothing"

An order with no payments and an order that has not been shipped both look empty, but they are not
the same emptiness, and the document says which is which:

- an **array** branch with no children is an **empty array**: `payments: []`
- an **object** branch with nothing to hold **is not there at all** - the key is absent, not `null`

```js
db.orders.findOne({ id: 4 })     // status "new": "invoice" in doc === false, shipments: []
```

### A third shape: flat fields

An `object` keeps the child row under one field. A `flat` embed contributes that row's fields directly
to its parent and therefore has no `path` or `arrayKey`:

```yaml
        - from: invoice_fields
          on: { invoice_order_id: id }
          as: flat
          key: [invoice_row_id]
```

The child join and identity fields still have to reach Nest. Rename them before the nest so they do not
claim the order's own field names, then drop the internal names after assembly. The complete pattern,
including two flat child tables and the many-to-one direction, is in
[Flat embeds](../../nest/flat-embeds.md).

If two live invoice rows name one order, the run stops with `nest.flat-cardinality-violation`. If a flat
field overlaps the order, another flat child, or a nested path, model discovery rejects it before start
when possible; an actual row catches anything the model could not see and stops with
`nest.flat-field-conflict`.

## 4. The rule that decides relationship direction

Every `on` pair can point in either direction, and the declared or discovered keys decide which:

> **If the parent side is the parent's key, child rows belong to that parent. If the child side is the
> child's key, the parent points at one shared row.**

In `on: { order_id: id }` the left side is a field of the child and the right side is a field of the
parent - and that right side **is the level's identity**. It is not "the column to look things up by";
it is the declaration of what this level is keyed on. The engine partitions the level by it and routes
every child event to the parent that owns that key.

The reverse form names the child's own identity on the left. For example, an order carrying
`customer_id` can point at one customer row:

```yaml
        - from: c
          on: { id: customer_id }        # customers.id is the child's own key
          as: object
          path: customer
```

That customer does not redefine the order level's identity; it is a referenced row and may be shared by
many orders. Grouped siblings still have to agree on the one parent identity they carry.

If neither side of `on` identifies its row, the direction cannot be inferred and the tree is refused.
For example, `order_items.product_id` is not the line's key and `products.category` is not the product's
key:

```yaml
            - from: p
              on: { category: product_id }
              as: object
              path: product
```

Starting the pipeline fails with a diagnostic such as:

```
nest.embed-target-not-parent-key {embedPath=lines.product, fields=product_id, parentKey=id}
```

Declare the actual row identity with `key` when discovery cannot provide it. Do not change `on` merely to
silence the error: its two sides define which updates and deletes reach which documents.

### The other direction is a shared referenced row

When the child side of `on` is the child's own key, the direction reverses: the parent points at one
shared row. This is a many-to-one relationship, and Nest keeps one copy of the shared row while fanning
its changes out to every document that refers to it. A flat customer on an order is one example:

```yaml
        - from: customer_fields
          on: { customer_key: customer_id }
          as: flat
          key: [ customer_key ]
```

Here `customer_key` identifies the customer row and `orders.customer_id` points at it. One customer may
serve many order documents; each order still names only one customer. Updating or deleting that customer
redraws every referring order. The field-collision rules are unchanged, so the upstream map gives the
customer fields distinct public names and the internal key is dropped after Nest.

The unsupported direction is one parent matching several rows under `as: flat`. That is an array shape,
not a reference. It is detected from live rows and stops rather than selecting whichever row arrived
first.

## 5. Turn the tree around: a customer document

The rule also tells you what to do when the document you want is the other way up. A customer's
orders point at the customer, so with the **customer as the root** that same relationship runs the
right way and everything below comes along.

This is a second pipeline - `id: customer_doc`, reading the same sources plus one carrying
`customers`, writing to the same target - and only its transform differs:

```yaml
    root:
      from: c
      key: [ id ]
      embed:
        - from: o
          on: { customer_id: id }        # orders.customer_id matches customers.id
          as: array
          path: orders
          arrayKey: [ id ]
          trackKeyChanges: true
          embed:
            - from: inv
              on: { order_id: id }
              as: object
              path: invoice
            - from: i
              on: { order_id: id }
              as: array
              path: items
              arrayKey: [ id ]
              embed:
                - from: opt              # a fourth level
                  on: { item_id: id }
                  as: array
                  path: options
                  arrayKey: [ id ]
            - from: pay
              on: { order_id: id }
              as: array
              path: payments
              arrayKey: [ id ]
            - from: sh
              on: { order_id: id }
              as: array
              path: shipments
              arrayKey: [ id ]
              embed:
                - from: ev
                  on: { shipment_id: id }
                  as: array
                  path: events
                  arrayKey: [ id ]
```

Twelve documents now hold all 300 orders, all 600 lines, all 400 options, all 180 payments, all 86
parcels and all 247 events - four levels deep, and each customer's own columns sit at the top of their
document.

## 6. The same move again: a product document

Section 4 ended with a product that could not be pulled into a line. Root a document at `products`
instead and the same relationship reads the right way round - a line carries `product_id`, which is
the product's identity, so it routes to the product that owns it:

A third pipeline, `id: product_doc`:

```yaml
transforms:
  - id: product_view
    type: nest
    from: { p: products, i: order_items }
    root:
      from: p
      key: [ id ]
      embed:
        - from: i
          on: { product_id: id }     # order_items.product_id matches products.id
          as: array
          path: lines
          arrayKey: [ id ]
```

Twenty documents, one per product, holding all 600 lines between them - every line filed under the
product it refers to. The relationship that could not be expressed in one direction is ordinary in
the other, and this is the general shape of the answer: **a nest document is rooted at the "one" side
of every relationship it contains.** Which document you need decides which root you pick, and a
dataset usually supports more than one.

## 7. Watch it follow the source

The point of a nest is that the document stays right without being rebuilt. Change the source and
watch, one statement at a time:

```sql
-- a new option, three levels down
INSERT INTO item_options (id, item_id, name, value) VALUES (99001, 701, 'engraving', 'ACME');

-- an update in place
UPDATE item_options SET value = 'ACME-2' WHERE id = 99001;

-- a delete leaves the array
DELETE FROM item_options WHERE id = 99001;

-- a one-to-one branch appearing out of nothing: the "invoice" key shows up on an order that had none
INSERT INTO order_invoices (id, order_id, invoice_no, issued_at, net_amount, tax_amount, settled)
VALUES (99004, 4, 'INV-2025-99004', NOW(), 100.00, 19.00, 0);

-- a line moving to another order: it must MOVE, not be copied
UPDATE order_items SET order_id = 71 WHERE id = 702;
```

That last one is worth checking properly. Count the elements before and after:

```js
db.orders.aggregate([{ $project: { c: { $size: { $ifNull: ["$items", []] } } } },
                     { $group: { _id: null, items: { $sum: "$c" } } }])
```

The total must be unchanged. If it grew by one, the line was copied instead of moved, which means the
branch it moved within does not have `trackKeyChanges: true`. Editing a key is off by default - a tree
that never edits its keys pays nothing for it. See
[nest structural key changes](../../nest/structural-key-changes.md) for what the switch costs and what it
needs from the source.

## 8. Where the assembled state lives

A nest holds partly assembled documents between events. That state is in memory up to the budget you
set with `entries_in_memory`, and written through to MongoDB behind it, in a database of its own. The
pipeline above selects `order_doc_state`; without its `state` block the default is `tapstate_nest`:

```js
use order_doc_state
db.operator_state.aggregate([{ $group: { _id: "$_id.ns", n: { $sum: 1 } } }])
// nest.<pipeline>.<transform>.$root : one entry per root
```

Each pipeline gets its own namespaces, so two pipelines never read each other's state. Entries are
opaque - the readable view is the metrics:

```sh
tapstate -c http://127.0.0.1:8080 -u admin metrics order_doc
```

| Reading | Means |
|---|---|
| `nestStateEntries.<ns>` | how many keys this level holds in memory right now - not how many there are; past the memory budget the rest live behind memory and still belong to the level |
| `nestStateStored.<ns>` | how many keys this level holds in total, the resident ones included - every write reaches the layer behind memory, so this is the whole; absent, not 0, when the run has no such layer to ask. Entries over stored is the share served from memory |
| `nestStateBackfills.<ns>` | reads that had to go behind memory - the one that tells you the cold layer is really in use |
| `nestStatePendingHighWater.<ns>` | the deepest one key's pending queue has ever got; a high-water mark, it does not fall back |

Two things to know: the state is dropped when the pipeline stops, so read these while it runs; and
this is not the control store (`tapstate`) or your target. A deployment can choose another default with
`tapstate.store.mongo.operator-state-database`, and one Nest can override it with
`state: { database: ... }`. Changing either name selects a different state database; it does not copy
the old one. For an existing run, follow the stop, copy, verify, restart, and rollback procedure in
[running on your own databases](../../running-on-your-own-databases.md#move-an-existing-nest-state-database),
or clear the pipeline state and perform a full replay.

## 9. Clean up

```sh
# in the CLI session, one per pipeline you started
stop order_doc
stop customer_doc
stop product_doc
```

```js
db.getSiblingDB("shopdocs").dropDatabase()
```

The shop database is a plain MySQL database - `DROP DATABASE shop;` when you are done with it.

## What to read next

- [Nest structural key changes](../../nest/structural-key-changes.md) - what happens when a key is
  edited, and what tracking it costs.
- [Nest throughput limits](../../nest/throughput-limits.md) - what a nest does when one root takes far
  more changes than the others.
