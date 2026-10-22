---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs
---

# Expanding an array into rows

An `unwind` transform turns one parent row into one row per array element. Each
output keeps the parent's fields and replaces the array field with one element.
Updates add and remove corresponding target rows; deleting the parent removes
all the rows it produced. The target must use `write_mode: upsert`.

```yaml
transforms:
  - id: lines
    from: [orders]
    type: unwind
    path: items
    element_key: sku
    include_array_index: item_no
    element_type: json
```

Given `{id: 10, items: [{sku: "a"}, {sku: "b"}]}`, this produces:

```json
{"id": 10, "items": {"sku": "a"}, "sku": "a", "item_no": 0}
{"id": 10, "items": {"sku": "b"}, "sku": "b", "item_no": 1}
```

`element_key: sku` copies the element's `sku` into a **top-level column named
`sku`**. The target key is the parent's key plus that column: `(id, sku)` here.
If both locator options are present, `element_key` supplies identity and
`include_array_index` only records order. To expand scalar elements, omit
`element_key` and use `include_array_index`; the key becomes `(id, item_no)`.
At least one locator is required. Generated column names must differ from parent
column names, the array path, and each other. Assembly refuses a known collision with
`dsl.unwind-column-already-exists` before the job runs. If an extra field absent from
the discovered model appears in a row, the same check stops expansion before that
row can overwrite it. Choose another generated name or rename the parent field in
an earlier `map`.

| Option | Meaning |
|---|---|
| `path` | Required array field name. Expands one level; dotted paths, wildcards and recursive expansion are not supported. |
| `element_key` | Identity field inside each element. Also creates a top-level column of the same name. |
| `include_array_index` | Adds a zero-based ordinal column. Supplies element identity when `element_key` is absent. |
| `preserve_null_and_empty_arrays` | Defaults to `false`: null, missing and empty arrays produce no rows. `true` retains one row with the expanded field and locator columns set to null. |
| `source_id_field` | Optional top-level name for a preserved parent identity when the target requires an independent generated identity. Defaults to `_unwind_source_id`. It adds no field when automatic separation is unnecessary. |
| `element_type` | Optional declared type of the expanded column, such as `json` for objects. Without it the type is unresolved and the connector infers it. It does not convert values. |

A non-array value is treated as one element. This includes a string containing
JSON text: decode text into an array before expansion. Output order is not
guaranteed; use the ordinal column when order matters. A second unwind inherits
the first expansion's key and adds its own locator.

## Target identity checks

The logical key of an expanded row combines the parent key and the element
locator. Some targets also enforce an independent identity constraint. For example,
MongoDB requires `_id` alone to be unique, even when a unique compound index exists
on `(_id, item_no)`. Copying the same parent `_id` into two expanded documents then
fails with a duplicate-key error. MongoDB's native `$unwind` preserves `_id`; it does
not resolve that target constraint.

Tapstate checks each destination separately before preview and START. Automatic
separation applies only when all of the following hold:

- An unwind expanded the row key, and the target's mandatory identity is inherited
  from the repeated parent key.
- The target can generate an independent identity when that field is omitted.
- The original identity can be preserved without overwriting an existing field.
- The complete expanded logical key can reliably match updates, deletes and
  replayed events, backed by a full unique index.
- Other target unique constraints remain compatible with every expanded row.

For an eligible MongoDB target, a logical row such as
`{"_id":"order-1","items":"a","item_no":0}` is written as
`{"_unwind_source_id":"order-1","items":"a","item_no":0}`. MongoDB generates a
new `_id`. Matching uses `(_unwind_source_id, item_no)`, so replay updates the same
row instead of creating another one. The original value and type are preserved,
including BSON ObjectId values. The same projection applies to before images,
after images, deletes and the target model.

This decision is based on constraints, not just a database name. An ordinary
composite-key target keeps its fields unchanged. An existing PostgreSQL or MySQL
table with an independently generated identity can also qualify, provided its
schema already contains the preserved field and a non-null, complete unique
matching key. Tapstate does not add missing columns to an existing relational
table, remove business constraints, or rename unrelated unique fields to make a
write succeed. Partial, expression, prefix, sparse or non-simple-collation unique
indexes do not establish the required guarantee and block automatic separation.
MySQL character-collated unique keys are conservatively refused, including binary
collations with unproven padding semantics; numeric and binary key columns are
supported. PostgreSQL nondeterministic collations are refused. A compatible
composite primary key does not exempt conflicting secondary business indexes.
Existing targets whose connectors cannot expose exact constraint metadata are
refused until that metadata can be verified.

For MongoDB, exact index-option inspection uses the connector's read-only
`$indexStats` command, which requires the `indexStats` privilege on the target
collection. The connector's general `listIndexes` result does not expose all the
options required for this check. If exact metadata or permission is unavailable,
START is blocked with `connector.target-identity-check-failed`; it is never reported
as "no change needed". Tapstate does not grant privileges. A required logical
index may be prepared for a new or empty collection, or when the existing full-load
policy explicitly clears the collection. Preparation is followed by another exact
index check before writes begin. A populated collection without the matching index
requires an explicit migration or a new destination.

### Configuration and preview

The Wizard's unwind advanced options expose **Source identity field**, initialized
to `_unwind_source_id`, with a restore-default action. For example:

```yaml
- id: lines
  from: [orders]
  type: unwind
  path: items
  include_array_index: item_no
  source_id_field: original_order_id
```

Use a nonblank top-level name without dots, a leading `$`, or a NUL character.
A collision with a source field is shown beside the setting and blocks START;
a collision detected only in a runtime row also fails before overwriting data.
With successive unwinds, an omitted setting inherits the upstream choice; an explicit setting overrides
it for downstream targets. The full matching key includes every locator.

The target panel reports **No change needed**, **Automatic**, or **Action required**
for each destination. An automatic result shows the original and preserved field,
the complete matching key, and which physical field is generated on write. The
**Logical operator result** preview retains the original logical fields; **Target
write result** shows the projected fields. A generated physical identity is
annotated as generated on write and is not fabricated in preview.

Changing the candidate invalidates the previous check. START performs fresh checks
against the saved configuration and current target metadata. Automatic handling
requires no confirmation dialog or additional mapping step. Do not change
`source_id_field` for a populated destination as a hot migration: existing rows are
not rewritten, and an incompatible existing matching index blocks the new mapping.
Use a new destination or explicitly migrate and validate existing data first.

## Changes and deletion

Snapshot rows and inserts expand the current row. Deletes expand the previous row
into deletes. Updates compare expanded **write keys** from both images, delete
keys that disappeared, and write current rows. This handles a parent key change
or removal of an element from the middle of an indexed array.

The source must provide a complete previous image for updates and deletes. The
runtime requires every parent key column and at least one other column. A missing,
empty or key-only previous image stops with
`transform.unwind-needs-a-complete-before-image`. For MySQL CDC, configure full
binlog row images. For MongoDB CDC, enable source collection pre-images.
A full document without the optional array field is allowed when it has another
non-key field. A document consisting only of its key cannot be distinguished from
a partial image and is refused.

Duplicate element keys still emit all rows. The log reports
`transform.unwind-rows-share-a-key` at WARNING severity, and the target may keep
only the last row for that key. The warning is not an `error_count` increment.
Choose element keys unique within each parent.

A declaration with neither locator fails validation with
`dsl.unwind-needs-an-element-key`. An append target fails validation with
`dsl.unwind-needs-an-upsert-target`: appending a delete would add a row instead of
removing the old one. One-time expansion into an append target is also unsupported.

A `map` before or after `unwind` can rename parent key columns; their identity follows
those renames into the target key. A following `map` can also rename the element key
column. Projection applies to both previous and current images, including deletes.
Keep the parent and element key values available: dropping or replacing them does
not preserve row identity.

Run the [MySQL to MongoDB walkthrough](../tutorials/expanding-order-items/) to see snapshot expansion and parent deletion.
