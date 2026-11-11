---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/reference/targets
---

# The reserved row field `__tap`

Tapstate keeps everything it adds to a row under one top-level field, `__tap`. Nothing else in
a row is Tapstate's, and no customer field can be mistaken for it. This page says what can
appear under the field in a target, how to keep it out of a target entirely, and what happens
when a source table already has a column with that name.

## What reaches a target

Rows are checked at one place, where they are written to a target. Whatever a step did
upstream, a row leaves in one of two shapes:

| Under `__tap` | Reaches the target |
|---|---|
| bookkeeping Tapstate uses to do its own work | never |
| a marker meant for whoever reads the target, such as "this total could not be refreshed" | yes, unless the target opts out |

A row with nothing to deliver has no `__tap` field at all. A marker is addressed by the
feature that wrote it and, where a pipeline can hold several, by the step's id, for example
`__tap.aggregate.<step id>`. The page for each feature says what its marker contains.

Row expressions cannot read or write the field: an expression naming `after.__tap` is refused
when the pipeline is validated, so no pipeline logic can come to depend on it.

## Keeping the field out of a target

A target with a strict schema may not accept a field it did not declare. Each `serve.sync`
element takes `internal_fields`:

```yaml
version: tapstate/v1
kind: serve
id: reporting_db
sync:
  - id: order_totals
    source: order_totals_by_customer
    internal_fields: strip
```

| Value | Effect |
|---|---|
| `deliver` (default) | markers are written to the target under `__tap` |
| `strip` | the target never receives `__tap`; a consumer reading that target cannot tell a marked row from an unmarked one |

The option applies to the whole target. It does not select one feature's markers.

## A source column that is already called `__tap`

The pipeline still runs. The column is renamed where it enters Tapstate, so every step and
every target sees one name:

| Situation | What happens |
|---|---|
| a source table has a column `__tap` | it is read as `__tap_src`; applying the pipeline reports the warning `schema.reserved-field-renamed`, naming the source, the table, the column and the new name |
| the column is part of the table's key | values are unchanged; the key is known by the new name, and the pipeline refers to it by that name |
| the table also has a column `__tap_src` | refused with `schema.reserved-field-rename-conflict`: two columns cannot be read under one name |
| a `map` or `format` step declares an output field `__tap` | refused with `dsl.reserved-field-declared`; choose another name |

Only the top-level column name is reserved. A field called `__tap` inside a nested value or a
sub-document is ordinary data and is delivered as it is.
