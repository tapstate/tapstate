---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/connectors/value-shapes
---

# What a connector's own types look like in Tapstate

A database has types Tapstate has no name of its own for: a MongoDB `ObjectId`, a binary column, a
regular expression stored as a value. Its connector ships a conversion for each of those, and
Tapstate now runs them.

**What changed for you, in one sentence:** values from a connector's own types are read as the
database itself spells them - an `ObjectId` is the hexadecimal string you know it by, a binary column
is base64 - the browse face and the change stream agree on every one of them, and a value written to
a target of the same kind arrives as that type rather than as text.

Before this, the same value could read one way and follow another: an `_id` was the hex string in a
change and a `{date, timestamp}` document in a read, so the same row looked like two rows depending
on which face you opened. Nothing was lost on the wire; the two faces spelled it differently.

## What you see, per type

For MongoDB, the seven types its connector declares a conversion for:

| In the database | How Tapstate reads it |
|---|---|
| `ObjectId` | its hexadecimal string - `6a9d6c2d533d296b4fa378fe` |
| `Binary` | base64 - `SGVsbG8gVGFwc3RhdGU=`, the same text `mongosh` prints |
| `Code`, `Symbol` | the text it holds |
| regular expression | `/pattern/flags` |
| `Decimal128` | the exact decimal value, with all significant digits - **see the special-value limit below** |
| BSON timestamp | its seconds-based instant - **see the counter limit below** |

Everything else - integers, floating point numbers, exact decimals, text, booleans, dates - is
untouched by any of this and reads as it always did.

## Writing to a target of the same kind

A value that arrives as one of those types is written back as that type, not as the text it travelled
as. An `ObjectId` primary key read from one MongoDB and written to another is an `ObjectId` there,
and a binary column is binary, byte for byte.

This works because the row carries the name the source's schema gave the column, and the target's own
connector rebuilds from that. What follows from it is visible rather than silent:

- **A target of a different kind gets the value as it reads above** - a hex string, base64 - because
  it has no such type to rebuild into. That is the right answer for it.
- **Where the schema names the value, its word is what the target rebuilds from.** It names a field
  inside a document by its path, so a key nested one level down is restored like a top-level one, and
  what it says there is used even where the value's own type would suggest something else.
- **Where it names nothing, the value's own type answers instead.** Two places it commonly names
  nothing for: an array's elements, which are positional and may each be a different type, so "the
  type of this array's elements" is not something a schema can state at all; and a field - at any
  depth, including a top-level one - that discovery never described, because a schemaless source's
  field map is the fields it met in the documents it sampled rather than a census of the collection.
  Neither absence says anything about the type, so both are answered the same way: from what your
  source's schema calls that type elsewhere in the same row. An `ObjectId` inside an array, or in a
  field that first appeared after discovery ran, is written back as an `ObjectId` because the
  collection's `_id` is one and the schema names it. Answering only the array would let one document
  land two ways, with the better-described place getting the worse answer.
- **That reading is taken one document at a time**, off the values that document itself carries.
  Where the document holds no such column - the schema names none, or the one it names is absent or
  null in that document - or where it holds two columns of that type the schema spells differently,
  the value arrives as its text form. Both readings are per document, the second one included: a
  document that carries only one of two differently spelled columns does not show the ambiguity and
  is restored from the spelling it carries. So one collection can land with these values restored in
  some documents and as text in others; either way it is visible in the target rather than silently
  the wrong type. An `_id` is always there, which is why an `ObjectId` is the reliable case.

## Limits worth knowing before you rely on this

The limits that remain come from the portable value the connector returns:

- **A `Decimal128` special value is not an exact decimal.** `NaN`, `Infinity`, `-Infinity` and
  negative zero have no exact decimal form at all, so the exactness above does not reach them: each
  keeps the double the connector's own conversion produced. Ordinary finite values - every value a
  column of money or quantity holds - are unaffected.
- **An exact decimal is matched as the number it is.** A `Decimal128` column used to key an embedded
  document does so as its exact value: two spellings of one number - `NumberDecimal("10.50")` and
  `NumberDecimal("10.5")` - are one key, and a decimal key no longer lands on the same key as a plain
  `DOUBLE` column in another source the way the rounded value did. An upgrade consequence follows from
  the same change: a pipeline already running with a nest or a join keyed on such a column files its
  state under a new name from the first change after the upgrade, so what it assembled before is not
  found again. Recreate such a pipeline rather than upgrading it in place.
- **A `js` transform reads an exact decimal as an object, not as a number.** `r.after.amount * 1.1`
  is `NaN` there and `r.after.amount > 100` is `false`, with nothing thrown and nothing logged. This
  is how every exact decimal column has always reached a script - a relational `NUMERIC` one
  included - and a `Decimal128` column now reaches it the same way instead of as a rounded double.
  The `filter` and `map` ports are unaffected: they refuse arithmetic on a decimal while the pipeline
  is being validated, rather than answering something wrong at run time.
- **A BSON timestamp's counter is not represented.** Its seconds field reads as the corresponding
  instant, but the per-second ordering counter has no counterpart in the portable date-time value and
  is not carried. A BSON timestamp is an internal replication type and is rare in application data;
  an ordinary date column is a different type and is not affected.

Each is tracked separately.
