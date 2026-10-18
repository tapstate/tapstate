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
