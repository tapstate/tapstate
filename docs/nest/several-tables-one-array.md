---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/nest/several-tables-one-array
---

# Several tables in one array

Sometimes one array in a document is fed by more than one table:

- **Different tables, one list.** A player's points, comps and awards live in three tables, and the
  document wants them as one list of movements.
- **One table split into shards.** A player's sessions live in `PlayerSession0` to `PlayerSession9`,
  and the document wants them as one list of sessions.

An embed reads one stream. So you merge the tables into one stream before the `nest` and embed that
stream. There are three things to get right: say which table each row came from, merge the streams,
and use the table tag together with the row's id as the element's `arrayKey`.

## Different tables: tag with `map`, merge with `union`

```yaml
version: tapstate/v1
kind: pipeline
id: player_ledger
source: [ ledger_db ]
transforms:
  # 1. Each table's rows say which table they came from.
  - id: point_moves
    type: map
    from: points
    fields: { kind: points }
  - id: comp_moves
    type: map
    from: comps
    fields: { kind: comps }
  - id: award_moves
    type: map
    from: awards
    fields: { kind: awards }

  # 2. Three tables, one stream.
  - id: movements
    type: union
    from: [ point_moves, comp_moves, award_moves ]

  # 3. The tag and the id together identify an element.
  - id: player_doc
    type: nest
    from: { p: players, m: movements }
    root:
      from: p
      key: [ id ]
      embed:
        - from: m
          on: { player_id: id }
          as: array
          path: item_movement
          arrayKey: [ kind, id ]
```

The `map` steps are also where each table's rows are brought into one shape, if they differ. A
computed field can hold a formula that only fits one table, for example
`amount: "=after.GamePts + after.BasePts - after.ExpirePts"` on the points table alone.

## One table in shards: read them by pattern, tag with the table name

A `map` step can read every shard with one pattern. In an expression, `src` is the name of the table
the row came from, so one step both merges the shards and tags each row:

```yaml
  - id: sessions
    type: map
    from: "/^PlayerSession[0-9]$/"
    fields: { shard: "=src" }

  - id: player_doc
    type: nest
    from: { p: players, s: sessions }
    root:
      from: p
      key: [ id ]
      embed:
        - from: s
          on: { player_id: id }
          as: array
          path: sessions
          arrayKey: [ shard, SessionID ]
```

The source has to select the shards as well, for example `tables: [ players, "/^PlayerSession[0-9]$/" ]`.
Quote a pattern in YAML; inside `[ ... ]` the brackets of `[0-9]` are otherwise read as YAML.

If the session id is unique across all shards, the tag is not needed for identity, but it costs
nothing and keeps the document correct if that ever stops being true.

## Why `arrayKey` is required here

An embed over a source table can fall back on that table's primary key to tell its elements apart. An
embed over a step's output, such as a `union`, has no table key to fall back on, so it must declare
`arrayKey`. Leaving it out is refused when the pipeline starts, with `nest.array-key-unresolvable`.

If the merged level has embeds of its own beneath it, also declare `key` with the same fields: that is
what the children below it are filed under.

**Leaving the tag out of the key is not reported.** If points and comps both have a row with id 1 and
the `arrayKey` is only `[ id ]`, the two rows are the same element as far as the nest can tell: the array
holds whichever was written last, and a delete in one table removes the row of the other. Nothing
fails, and the document is silently wrong. Always include the tag unless the ids are unique across
every table you merge.

## Updates, deletes and moves

With the tag in `arrayKey`, every change reaches exactly its own element:

- an update to a row in one table changes that element only, even when other tables have a row with
  the same id;
- a delete removes that element only;
- a row inserted while the pipeline runs joins its parent's array.

A row that changes which parent it belongs to (an edited `player_id`) needs `trackKeyChanges: true`
on the embed, as for any embed, and a source that supplies before images. See
[Structural key changes](structural-key-changes.md).

## Limits

- **All elements of one parent are held together.** A merged array is one embed level, so the nest's
  per-document element limit (`max_elements_per_document` on the `nest` step) counts the rows of all
  merged tables together. A parent with many rows across many tables reaches it sooner.
- **Every change rewrites the whole document.** A new session row sends the whole player document
  again, with every movement and session in it. See [Throughput limits](throughput-limits.md) for
  the arithmetic on your own row counts.
- **One shape per array.** Elements from different tables sit in one array. Fields one table has and
  another does not are simply absent on the other table's elements. Use the `map` steps to give
  them the same field names where a consumer expects it.
