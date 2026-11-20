---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/nest/scripts-after-a-nest
---

# Reshaping an assembled document with a script

A `js` step placed after a `nest` runs once for every document the nest writes, and it sees the
whole document: the root's own columns and every embedded array, as they stand after the change that
caused the write. That makes it the place for the reshaping the declarative steps do not express:

- put an array in a chosen order;
- join an array's values into one string;
- keep only the newest element of each kind in an array;
- copy a value from one part of the document into each element of an array.

Totals over an embed belong in [rollup fields](rollup-fields.md) instead, which are typed and exact.
Use a script for shape, and a rollup for arithmetic you need to be exact (see
[Money and other exact decimals](#money-and-other-exact-decimals)).

## An example

An order document with its lines, sorted by `sku` newest first, with the skus joined into one field
and a line count beside them:

```yaml
transforms:
  - id: order_doc
    type: nest
    from: { o: orders, i: order_items }
    root:
      from: o
      key: [ id ]
      embed:
        - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
  - id: order_shaped
    type: js
    from: order_doc
    script: |
      function process(record, ctx) {
        var doc = record.after;
        if (doc) {
          var items = doc.items || [];
          items.sort(function (a, b) { return a.sku < b.sku ? 1 : a.sku > b.sku ? -1 : 0; });
          doc.items = items;
          doc.skus = items.map(function (item) { return item.sku; }).join(',');
          doc.item_count = items.length;
        }
        return record;
      }
serve:
  from: order_shaped
  sync:
    - source: shop_target
```

The end-to-end case `a-script-reshapes-the-assembled-document` runs this shape against MySQL and
MongoDB, including a child insert after the first load and a parent delete.

## What the script is given, and when

- **Every write, whole.** The nest sends a document as one unit, so any change to the root or to any
  row embedded in it reaches the script as the complete new document in `record.after`. The script
  never has to merge a change into an earlier result: it computes from the document it holds, and its
  output replaces the previous one.
- **Deletes.** When the root row is deleted, the record arrives with no `after` (`record.after` is
  `null`). Return the record unchanged and the document leaves the target. Guard every read of
  `record.after` for this case, as the example does.
- **An empty array may be absent.** An array whose last element was removed can be missing from the
  document rather than empty, so read it as `doc.items || []`.
- **No state between calls.** A script holds nothing from one document to the next, and it cannot look
  up other rows. Everything it needs has to be embedded in the document it is given.
- **No checks before it runs.** Unlike `map`, `filter` and rollup fields, a script's field names and
  types are not checked when the pipeline is validated or applied. A mistyped field name reads as
  `undefined` at run time, and a thrown error stops the pipeline with the script's message.

## Cost

A script runs on every document the nest writes, so its cost is the nest's write rate times the size of
the documents. Sorting or joining a few dozen elements is cheap; walking very large arrays on every
write is not. [Throughput limits](throughput-limits.md) has the arithmetic for the write rate itself.

An array embedded only to feed the script stays in the target document unless the script removes it
(`delete doc.items`).

## Money and other exact decimals

A script works in JavaScript numbers, which are binary floating point. An exact decimal from the
source, such as a MongoDB `Decimal128` or a SQL `DECIMAL(p, s)` column, is handed to the script as the
nearest JavaScript number. If the script leaves that field untouched, the original exact value is
written out unchanged. If the script computes with it, the arithmetic is floating point:

```js
0.10 + 0.20        // 0.30000000000000004
```

and whatever the script writes back is written as a floating-point number, not as a decimal.

For money, compute in integer minor units and convert once at the end:

```js
var cents = 0;
(doc.CreditActivity || []).forEach(function (row) {
  if (row.SiteID == 1 && row.FrontMoney != null && row.FrontMoney > 0) {
    cents += Math.round(row.FrontMoney * 100);
  }
});
doc.frontMoney = cents / 100;
```

`Math.round(value * 100)` recovers the exact cents of any amount with two decimal places, as long as
the total stays below about 90 trillion units (2^53 cents). The result written back is still a
floating-point number: how a `DECIMAL` target column stores it is up to the target.

When an amount has more significant digits than a floating-point number holds (about 15), or when the
target needs the exact decimal type, do the arithmetic in a [rollup field](rollup-fields.md), which sums
decimals exactly. A decimal too large or too small to become a finite, nonzero JavaScript number stops
the pipeline with a coded error rather than reaching the script as infinity or zero.
