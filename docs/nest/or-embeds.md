---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/nest/or-embeds
---

# Embeds that match on one column or another

Some rows name the row they point at in more than one way. A wallet transaction that came from
one system carries a location code; one that came from another carries a hub location id; some
carry both. `orOn` lists the other ways, so one embed finds the location whichever way the
transaction names it:

```yaml
- id: wallet_doc
  type: nest
  from: { tran: wallet_tran, loc: location }
  root:
    from: tran
    key: [tran_id]
    embed:
      - from: loc
        on: { locn_code: locn_code }
        orOn:
          - { hub_locn_id: hub_locn_id }
        as: object
        path: location
```

`on` is the first alternative and each map under `orOn` is another, written the same way as `on`:
the embedded row's field on the left, the parent's field on the right. An alternative may match on
several fields at once.

Without `orOn` the same document takes two embeds, one per column, and a `map` step that picks
one of the two and drops the other. `orOn` gives the same result in one embed.

## Which row wins

Alternatives are tried in the order written, and the parent embeds the row found by the first
alternative that has a live match. In the example a transaction whose code matches a location
embeds that location even if its hub id names a different one; a transaction whose code matches
nothing falls back to its hub id. If no alternative matches, the embed is absent (`object`), an
empty array (`array`), or contributes no fields (`flat`), exactly as for an embed with only `on`.

A NULL in an alternative's fields matches nothing, on either side. A transaction without a hub
id never matches a location without a hub id.

## Under change

Each alternative keeps its own index of which documents point at which rows, so any change
redraws exactly the documents it affects:

- a location inserted with a code that a transaction names switches that transaction from its
  hub id match to the code match;
- deleting that location falls back to the hub id match;
- editing a location's code or hub id moves the documents that matched the old value. This needs
  the source to send before images on update, as described in
  [Structural key changes](structural-key-changes.md);
- editing the transaction's own code or hub id re-points it.

## Limits

- `orOn` is for embeds that point at one shared row (many to one), such as a location looked up
  by code or by hub id. Every alternative must find at most one row, the same rule as any such
  embed. An embed that gathers several rows under its parent matches on one condition; an
  alternative that matches on the parent's own key is refused with `nest.or-on-not-a-reference`.
- `orOn` must hold at least one alternative, and each alternative at least one field.
- Row-level validation of a pipeline does not yet cover an embed with `orOn`, and says so.
- The join step has the same feature in SQL: `LEFT JOIN … ON a = b OR c = d`. See
  [Join: match on one column or another](../join/README.md#match-on-one-column-or-another).
