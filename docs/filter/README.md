---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/reference/transforms
---

# Keeping matching rows in a view

A `filter` evaluates a CEL expression over each event. A predicate over `after`
can keep a live subset of source rows in a keyed view:

```yaml
version: tapstate/v1
kind: pipeline
id: in_transit_shipments_pipeline
source:
  - id: fulfillment_db
transforms:
  - id: in_transit_only
    type: filter
    from: shipments
    expr: "after.status == 'in_transit'"
view:
  id: in_transit_shipments
  from: in_transit_only
  primary_key: id
```

Matching snapshot rows and inserts pass through. Matching updates also pass
through, including updates that bring a previously excluded row into the view.
When an update stops matching, the filter evaluates the previous row as `after`.
If that row matched, it sends a delete containing the previous row and its key,
so the old view row is removed. An update whose previous and current rows both
fail the predicate produces no row event.

Source deletes keep their original envelope bindings: the removed row is in
`before`, and `after` is empty during evaluation. To include matching source
deletes as well as updates, use:

```yaml
expr: "op == 'd' ? before.status == 'in_transit' : after.status == 'in_transit'"
```

DDL events bypass the predicate.

The original `op`, `before`, `src`, `ts`, and `schema` bindings remain available
during predicate evaluation. Explicit operation predicates still apply:
`op != 'd'` intentionally suppresses source deletes, and `op == 'r'` admits
only snapshot reads. Such predicates do not describe a complete live copy of
the source.

Updates and deletes need the earlier values read by the predicate. Configure
the connector to provide full before images; for PostgreSQL:

```sql
ALTER TABLE shipments REPLICA IDENTITY FULL;
```

If predicate evaluation reads a missing before-image field, it reports the
existing `transform.expression-failed` diagnostic. The filter does not fetch
old rows from the source or reconstruct missing values.

## Ignoring an update that carries an older timestamp

Two DATETIME, DATE or TIME values of the same type can be ordered with `<`,
`<=`, `>` and `>=`. A filter can use that to ignore an update whose
last-updated column is older than the row it replaces, so a late write from
an upstream system does not overwrite a newer version:

```yaml
expr: "op != 'u' || (after != before && after.updated_at >= before.updated_at)"
```

The `after != before` part matters. Without it, a stale update stops
matching, the filter evaluates the previous row as `after`, that row matches,
and the filter sends a delete: the row leaves the view instead of keeping its
newer version. With it, the previous row does not match either, so the stale
update produces no row event. An update that changes nothing is dropped too.

Mixed operands, such as a DATETIME against a DATE, a string or a number, are
refused at validation, and so is arithmetic on these types. A null operand
fails evaluation with `transform.expression-failed`; guard a nullable column
with `has()` or a null check. The filter judges each update against the row it
replaces, so a stale value that a partial update has already merged into the
source row travels on with the next newer update.

## Comparing with today

`now()` is the moment the row is processed, and `today(zone)` is that moment's
calendar day in a time zone. A DATE can move by whole days, months or years
with `addDays(n)`, `addMonths(n)` and `addYears(n)`, and a DATETIME gives the
day it falls on with `toDate(zone)`. These keep only comps issued in the last
five years, by Las Vegas days:

```yaml
expr: "after.IssueDate > today('America/Los_Angeles').addYears(-5)"
```

```yaml
expr: "after.IssuedAt.toDate('America/Los_Angeles') > today('America/Los_Angeles').addYears(-5)"
```

| Function | Returns | Notes |
| --- | --- | --- |
| `now()` | timestamp | Read once per evaluation: every call in one expression sees the same moment. A computed field holding it is written as a date and time with an offset. |
| `today(zone)` | DATE | The zone is required. Use a region id such as `America/Los_Angeles` or a fixed offset such as `+08:00`; an abbreviation such as `PST` is refused. |
| `date.addDays(n)`, `date.addMonths(n)`, `date.addYears(n)` | DATE | A day past the end of a shorter month lands on its last day, so 29 February plus one year is 28 February. |
| `datetime.toDate(zone)` | DATE | A date and time without an offset already is a wall-clock reading and keeps its own day; one with an offset or an instant takes the day in `zone`. |

A literal zone that names no zone is refused when the pipeline is applied. A
zone read from the row is judged when the row is processed, and one that names
no zone fails with `transform.expression-failed`.

### The answer is fixed when the row is processed

Tapstate recomputes a value when the rows it reads change, never because the
clock moved. A row that passed `IssueDate > today(...).addYears(-5)` stays in
the view after its issue date ages past five years, until the row itself
changes again. A field computed with `today()` keeps the day it was computed
on.

So use the clock to decide what to do with a row as it arrives, such as
dropping a comp that is already too old. For a value that has to follow the
calendar on its own, such as "the player is in house today", keep the fact in
the document and compare it with today where it is read:

- Keep the date: a rollup such as `LastAccountingDate: { max: $AccountingDate }`
  on the player document changes whenever an accounting day is added or
  removed.
- Compare it where it is read: a formula field in the CRM, a database view, or
  the query itself, for example `LastAccountingDate == today` evaluated by the
  reader.
- Remove rows that age out with the target's own retention, such as a MongoDB
  TTL index on the issue date.

Rollup fields and aggregate steps do not offer the clock functions, so a
rollup filter cannot silently depend on the day it was computed.
