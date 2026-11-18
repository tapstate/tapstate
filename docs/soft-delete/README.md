---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/reference/transforms
---

# Soft delete: keeping deleted rows, marked

By default a delete at the source removes the row from the target, and a deleted child row leaves the
array of the document it was embedded in. Some consumers need the opposite: the row stays, carrying a
flag that says it was deleted, so downstream systems can see the deletion and keep the history.

A `map` step does this when it names a `soft_delete` column:

```yaml
transforms:
  - id: live_players
    type: map
    from: [ players ]
    soft_delete: _is_deleted
```

What the step then does to each event:

| Event reaching the step | What leaves it |
|---|---|
| insert, snapshot read | the row, with `_is_deleted: false` |
| update | the row before and after, both with `_is_deleted: false` |
| delete | an **update** of the row as it last was, with `_is_deleted: true` |
| schema change | unchanged |

So the target never receives a delete from this step. A row deleted and later inserted again under the
same key reads as live again, because the insert carries `_is_deleted: false`.

`soft_delete` may sit beside `fields`, and the field rules run on the deleted row exactly as they would
on a live one, so the marked row has the same projected shape as every row before it. With
`soft_delete` set, `fields` may be left out. The column named by `soft_delete` may not also be declared
under `fields`.

## Choosing where it applies

The setting belongs to the step, so it applies exactly where the step sits in the pipeline:

- **For one target only.** Put the step just before what that target reads (`serve.from`, or a view's
  `from`). Other targets reading the step above it still see real deletes.
- **For the documents a nest assembles.** Put the step on the stream that feeds the nest root. A deleted
  root row then stays in the nest as a row marked deleted, the document is kept with `_is_deleted: true`
  at its top level, and its embedded children stay embedded.
- **For children inside a document.** Put the step on the stream that feeds an embed. A deleted child row
  stays in its array as an element with `_is_deleted: true`. Each embed decides for itself: an embed fed
  directly by its table still loses elements on delete.

A complete nest with both its root and one child stream marked:

```yaml
transforms:
  - id: player_rows
    type: map
    from: [ players ]
    soft_delete: _is_deleted
  - id: email_rows
    type: map
    from: [ player_emails ]
    soft_delete: _is_deleted
  - id: player_doc
    type: nest
    from: { p: player_rows, e: email_rows, ph: player_phones }
    root:
      from: p
      key: [ player_id ]
      embed:
        - { from: e, on: { player_id: player_id }, as: array, path: emails, arrayKey: [ email_id ] }
        - { from: ph, on: { player_id: player_id }, as: array, path: phones, arrayKey: [ phone_id ] }
```

Here a deleted e-mail stays in `emails` marked deleted, a deleted phone leaves `phones`, and a deleted
player keeps its document marked deleted.

The documents are still written to the root table's name and upserted by the root's key when the root
is fed through maps and filters, as here. The key may be written in a name a map gave the column, and a
root or embed that leaves its key out takes the table's key under those names. A map that writes a
literal or computed value over a key column ends that inference, since the column no longer tells rows
apart; declare the key yourself in that case. The target's columns follow the maps too, so it declares
the marker column alongside the ones the rows carry. A root fed through any
other step (a `js` step, a union) loses that: the target is named after the step and has no key to
upsert by.

When a source cascades a parent delete to its children, each child delete arrives as its own event, so
the children are marked too if their stream passes through a soft-delete step. Marking a document's
children when only the parent row is deleted is not done.

## What it requires from the source

**A whole row on delete.** The update a delete becomes holds only what the delete carried. A source that
sends the full earlier row on a delete gives a full row marked deleted. One that sends only the
identifying columns gives just those columns and the marker:

- written straight to a target that applies an update field by field, the rest of the target row is kept;
- fed into a nest, the element or root row is replaced by the key and the marker, and its other columns
  are lost from the document.

So a soft-delete step feeding a nest needs before images, the same setting
[structural key changes](../nest/structural-key-changes.md#what-it-requires-from-the-source) need:
`binlog_row_image=FULL` on MySQL and MariaDB, `REPLICA IDENTITY FULL` on PostgreSQL. SQL Server change
data capture and Oracle with supplemental logging of all columns send whole rows.

## Known limits

- **A fresh full load has no deleted rows to mark.** A snapshot reads only the rows that exist, so a
  pipeline started over from a snapshot carries no row deleted before it started. Rows already marked in
  the target stay where they are, since a full load keeps existing target rows unless `on_full_load` says
  otherwise.
- **Deleted rows are kept, not aged out.** Nothing removes a marked row later. Clearing old marked rows
  is a job for the target.
