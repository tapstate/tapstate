---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/nest/rollup-fields
---

# Rollup fields

A rollup field is a value computed over the rows a nest gathers under one embed and written on the
parent, beside them: an order's item count, a transaction's total, a player's latest rating.

```yaml
- from: i
  on: { order_id: id }
  as: array
  path: items
  arrayKey: [ id ]
  rollup:
    item_count: { count: true }
    total_qty:  { sum: $qty, default: 0 }
```

The order document now carries `items`, `item_count` and `total_qty`. They are computed in the same
render that writes `items`, so the array and its totals always leave in the same document: no reader
ever sees one without the other. Every insert, update and delete on the rows they read, a change to a
row they reference, an element moving to another parent, and a restart all leave them right, because
they are not kept anywhere: each render recomputes them from the live elements.

## Declaring them

`rollup:` is a map from field name to exactly one function and its argument:

```yaml
rollup:
  <field>: { <function>: <argument>, filter: <predicate>, type: <type>, default: <value> }
```

It is allowed on an `as: array` embed and on an `as: rollup` embed. Each field is written on the
**parent** of the embed: a rollup on a top-level embed lands on the document, one on a nested embed lands
on each element of the level above.

`as: rollup` keeps a level's rows only to compute its rollup fields and renders no array. It takes
`arrayKey` like an array and no `path`, and it must declare at least one rollup field:

```yaml
- from: r
  on: { trans_id: id }
  as: rollup
  arrayKey: [ trans_id, rating_type ]
  rollup:
    latest_score: { last: { value: $score, by: [ $model_date, $rating_type ] } }
```

### Arguments read the element as it is rendered

An argument, ordering or filter is a row expression over one element, written the way every row
expression is written: `$column` for a bare read, or `=` followed by an expression that reads the
element as `after`.

| You write | It reads |
|---|---|
| `$amount` | the element's `amount` |
| `=after.amount * after.prize_definition.prize_value` | the element's `amount` times a column of the row it references |
| `=has(after.prize_definition)` | whether the element's reference resolved |
| `$line_total` | a rollup field the element's own child level wrote on it |

The element is seen exactly as it will appear in the document: its own columns, the fields its flat
embeds merge in, the objects and arrays its references resolve to, and the rollup fields of its own child
levels (those are computed first, so a total can sum line totals). It never sees its siblings, its parent
or another document.

Expressions are checked when the pipeline is applied, against the discovered models of the level's table
and of every table it references. A misspelt column of a referenced row is refused there, not read as an
absent value forever after. The element's own columns follow the rule every row expression follows: a
column a sampled model has not seen yet is allowed, untyped.

## The functions

| Function | Argument | Result | Empty level |
|---|---|---|---|
| `count` | `true` (every element that passes the filter) or an expression (its non-null values) | 64-bit integer | `0` |
| `count_distinct` | an expression | 64-bit integer | `0` |
| `sum` | a number | see [numbers](#numbers-are-exact-or-refused) | null |
| `avg` | a number | see [numbers](#numbers-are-exact-or-refused) | null |
| `min`, `max` | a number, string or temporal | the argument's type | null |
| `first`, `last` | `{ value: <expr>, by: <expr> or [<expr>, ...] }` | the value's type | null |

Nulls follow SQL: a null argument is skipped, so a level whose arguments are all null is an empty level.
Every result depends only on which elements are live, never on the order they arrived in or on how many
times the document was rendered.

### Comparing values

`min`, `max`, `count_distinct`, `first` and `last` compare values the same way everywhere:

- numbers by value: `2.50` and `2.5` are equal;
- strings by Unicode code point, not by the source database's collation;
- temporals by instant.

### `first` and `last`

`first` picks the element with the smallest `by`, `last` the one with the greatest, and the field is
that element's `value`.

| Situation | What happens |
|---|---|
| `by` is a list | compared left to right; a later expression only breaks ties of the earlier ones |
| an element's `by` (any part of it) is null | the element is not a candidate |
| the chosen element's `value` is null | the field is null. It does **not** fall back to the next element; write `filter: "=has(after.score)"` to skip such elements |
| a tie survives every `by` expression | the element key (`arrayKey`, in declared order) decides: smallest for `first`, greatest for `last`. This is a deterministic fallback, not a business rule; put the business tie-breaker in `by` |
| the winner is deleted or its `by` changes | the next render picks the new winner from the live elements |

## `filter:`

A predicate over the element. An element contributes to that field only while the predicate holds, so an
update that moves an element in or out of it changes the field. Each field has its own filter; one level
can carry several buckets:

```yaml
rollup:
  qty:        { sum: $amount, default: 0 }
  reward_qty: { sum: $amount, filter: "=after.prize_definition.prize_code == 'reward-points'", default: 0 }
```

## Numbers are exact or refused

A rollup is computed exactly in a type fixed when the pipeline is applied, from the argument's declared
type. A value that does not fit stops the document with a code; nothing is silently rounded, truncated or
narrowed.

| Argument | `sum` | `avg` |
|---|---|---|
| 64-bit integer | 64-bit integer; overflow is `nest.rollup-overflow` | decimal, scale 6, rounded half-even |
| decimal `(p, s)` | decimal `(38, s)`, exact; more than 38 digits is `nest.rollup-overflow` | decimal `(38, max(s, 6))`, rounded half-even |
| double | double, added in element-key order so every render gives the same bits | double |
| unknown | refused at apply: `nest.rollup-type-unknown` | refused at apply |

- `avg` is the only rounding, and its scale and rounding mode are part of its type.
- The types come from discovery. A column nothing typed - an undiscovered source, a schemaless field, the
  output of a `map` step - cannot be summed or averaged; `count`, `min`, `max`, `first` and `last` still
  work on it.
- A value whose type differs from its column's declared type (a double in a decimal column) stops the
  document with `nest.rollup-value-type-mismatch` rather than being coerced.
- Arithmetic on decimals inside an expression is not available yet: `=after.qty * after.unit_price` over
  a decimal is refused at apply. Summing a single decimal column (`$unit_price`) is exact.

### `type:`

`type:` converts the result, exactly or not at all: `int64`, `decimal` or `double`.

- `decimal` to `int64` succeeds only for an integral value that fits 64 bits; `3.75` is refused with
  `nest.rollup-result-not-convertible`, `3.00` renders `3`.
- `int64` to `decimal` always succeeds.
- `double` is allowed because you asked for it; it rounds as binary floating point rounds.

## `default:`

The value written when no element contributed - an empty level, or one where the filter or nulls left
nothing. It is typed against the field's final type: it must already be of that type, except that an
integer becomes a decimal at the result's scale (`default: 0` on a `decimal(38,2)` sum writes `0.00`). A
default of the wrong type is refused at apply.

Without a default, an empty `sum` is null - the SQL answer. A `count` is `0` either way.

## A missing reference makes the field null, never partial

A field whose argument or filter reads a reference is computed only when every live element's reference
resolved. It reuses what a nest already does with references:

| Reference state | What the nest does | The rollup field |
|---|---|---|
| named, but the referenced table is still loading | holds the whole document back until the row arrives | nothing is written, so no partial total exists |
| the referenced table has loaded and the row is not there, or it was deleted | writes the document without the object | **null**, and its `default:` does **not** replace it |
| the row arrives later, including after a restart | re-renders the document | the full value |

An element whose reference columns are all null names no row, so it is not "missing" one: its product is
null and is skipped, like any null argument.

**Do not read this null as 0.** It means "incomplete": a total that would have left out an element. A
field that should tolerate the gap says so in its own expression:

```yaml
amount: { sum: "=has(after.prize_definition) ? after.amount * after.prize_definition.prize_value : 0" }
```

`explain <pipeline>` in a connected session (`rollups` in `GET /api/pipelines/{id}/explain`) lists every
rollup field with the level it lands on, its function, its filter, and the references that make it null
while they are missing.

## Refusals

| Code | When | Instead |
|---|---|---|
| `dsl.rollup-function-unknown` | a field names no function, an unknown one, or more than one | one of `count`, `count_distinct`, `sum`, `avg`, `min`, `max`, `first`, `last` |
| `dsl.rollup-on-single-row-embed` | `rollup:` on `as: object` or `as: flat` | read the row's columns directly |
| `dsl.rollup-level-without-fields` | `as: rollup` with no `rollup:` | add the fields, or `as: array` to show the rows |
| `nest.rollup-on-reference-embed` | `rollup:` on an embed its parent points at (it holds one row) | read the referenced columns from the level above: `=after.<path>.<column>` |
| `nest.rollup-field-conflict` | the name is already a column, an embed path, a flat field or another rollup field on that level | rename it; these share one namespace per level |
| `nest.rollup-expression-invalid` | an argument, `by`, filter, `type:` or default does not check | the message names the part and why |
| `nest.rollup-type-unknown` | a `sum` or `avg` over a value with no discovered numeric type | discover the source, or compute over a typed column |
| `nest.rollup-overflow` | a sum past 64 bits or 38 digits, at render | check for an outlier; such a total cannot be a rollup field |
| `nest.rollup-value-type-mismatch` | a value of another type than its column, at render | fix the row or rediscover the source |
| `nest.rollup-result-not-convertible` | a `type:` conversion that would not be exact, at render | drop `type:` or convert to `decimal` |
| `nest.rollup-evaluation-failed` | an expression failed on an element, at render | check the element's values against what the expression expects |

The render-time refusals stop the document's pipeline and name the document; the field is never written
with a guessed value.

## What it costs

Rollup fields add no state and no operator. Every send of a document walks each rollup level's live
elements once, folding all of that level's fields in the same walk, so the cost grows with the number of
elements per document, not with the number of fields. An `as: rollup` level holds the same state as the
same level `as: array`: its elements are stored as whole rows.

The send itself is the whole document, as for every nest ([throughput limits](throughput-limits.md)): a
level with many elements is rewritten and re-folded on every change to any of them.

### How wide a rollup level can be

Measured on a 2-CPU / 8 GB engine with one shared MongoDB, one level of five-column rows:

| Elements per document | Render per send, 1 field / 5 fields | Changes per second one document sustains |
|---:|---|---|
| 10 | 0.04 / 0.15 ms | as many as a plain array (the engine's own ceiling) |
| 100 | 0.09 / 0.33 ms | as many as a plain array |
| 1,000 | 0.4 / 1.6 ms | as many as a plain array |
| 10,000 | 4.5 / 17 ms | fewer than 25, where the same level as a plain array sustains 50 |

**Keep a rollup level to about 1,000 elements per document.** Up to there rollup fields cost nothing a
nest's own throughput would notice; at 10,000 the document falls behind at a few dozen changes a second.
Beyond 1,000 elements, compute the value in the target or wait for the cross-row step.

Independent of rollups, a nest level also meets these limits as it widens: a snapshot that reads more than
10,000 children before their parent row stops with `nest.pending-limit-exceeded`; a document holds 20,000
elements by default (`max_elements_per_document`); and a root's stored state, about 300 bytes per element at
this row width, must fit one MongoDB document, which it outgrows just under 50,000 elements.

## When to use something else

A rollup field summarizes rows that belong in the document anyway. It is the wrong tool when:

- the group is not a document - a total by region or by day;
- the rows per document are very many - a player's total over hundreds of thousands of sessions, which
  would rewrite and re-fold that whole set on every change.

Those are cross-row aggregations. They are not available as a step yet; until they are, compute them in
the target or in a `js` step.

## Known limits

| Limit | What you run into |
|---|---|
| No arithmetic on decimals inside expressions | `=after.qty * after.unit_price` over decimals is refused at apply |
| `sum` and `avg` need a discovered type | a rollup over a `map` step's output or an undiscovered source can count, but not sum |
| Recompute on every send | a document with very many elements per rollup level pays a full walk per send; keep it to about 1,000 |
| Changing a level from `as: array` to `as: rollup` changes its state identity | the level's stored rows are not carried over; plan such a change as a rebuild |
| No time-driven re-render | a filter that compares with the current time is right only as of the last change to the document |
| Data validation does not check rollup fields | validating a nest that declares rollup fields is refused rather than compared |
