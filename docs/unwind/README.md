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
| `element_type` | Optional declared type of the expanded column, such as `json` for objects. Without it the type is unresolved and the connector infers it. It does not convert values. |

A non-array value is treated as one element. This includes a string containing
JSON text: decode text into an array before expansion. Output order is not
guaranteed; use the ordinal column when order matters. A second unwind inherits
the first expansion's key and adds its own locator.

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
