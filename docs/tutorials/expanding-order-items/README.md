---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/tutorials/expanding-order-items
---

# Expanding order items from MySQL into MongoDB

Start with two MySQL orders and produce four MongoDB rows, then delete one parent
and watch its three expanded rows disappear. Use a server and CLI built from a
revision containing `unwind`, a MySQL source with `binlog_format=ROW` and
`binlog_row_image=FULL`, and a MongoDB target. Follow
[the local server setup](../../running-on-your-own-databases.md) to connect,
create an admin and register the MySQL and MongoDB connectors first.

This example constructs arrays in a `map` from a scalar source revision before
expanding them. It does not assume MySQL JSON text is decoded into a list by a
connector. The map evaluates both current and previous images, so source CDC
changes the list presented to unwind.

## Create the workspace and data

Use a new source database and an empty target database for this exercise. Load
[seed.sql](seed.sql) into the source database with your MySQL client. From this
tutorial directory, create a fresh workspace:

```sh
mkdir work
cp src_mysql.tap.yml tgt_mongo.tap.yml pipeline.tap.yml work/
```

Edit `work/src_mysql.tap.yml` with your source host, port, database, username and
password. Edit `work/tgt_mongo.tap.yml` with the target MongoDB URI. The checked-in
values are local examples, not a database or account created for you. Addresses
must be reachable from the server, and the URI should name the empty target
database. Keep connection settings in your local workspace.

The [pipeline](pipeline.tap.yml) maps revision 0 to three elements (`a`, `b`, `c`)
and revision 1 to one (`d`). `unwind` lifts each element's `sku` to a top-level
column and gives the target the composite key `(id, sku)`.

```sh
tapstate validate work
tapstate -w work
```

In the CLI session:

```text
connect http://127.0.0.1:8080
login admin
apply
discover-schema src_mysql
start unwind_snapshot
status unwind_snapshot
```

Use the server address and account from your setup. In a MongoDB shell connected
to the target database, wait for four rows, then inspect both identity and data:

```javascript
db.orders.countDocuments({})
db.orders.find({}, {_id: 0, id: 1, customer: 1, sku: 1, items: 1}).sort({id: 1, sku: 1})
```

The count is 4: `(1,a)`, `(1,b)`, `(1,c)` all retain `customer: "ada"`, and
`(2,d)` retains `customer: "lin"`. Each `items` value is one document, not an array.
A `running` status alone is insufficient; verify these actual target rows.

## Delete the parent

In MySQL:

```sql
DELETE FROM orders WHERE id = 1;
```

Wait for MongoDB to report both:

```javascript
db.orders.countDocuments({})       // 1
db.orders.countDocuments({id: 1})  // 0
```

The remaining row is `(2,d)`. Inspect the pipeline log if the counts do not
converge; an incomplete source before image is refused rather than guessed.
Stop the exercise pipeline in the CLI with `stop unwind_snapshot` when finished.
Record the source commit, connector versions, machine, date, and observed rows
when using this walkthrough as a manual verification.

See [unwind semantics and limits](../../unwind/) for duplicate keys, generated
column names, empty arrays, and write-mode constraints. Keep parent key names
unchanged through maps; this walkthrough does not exercise key-column renaming.
