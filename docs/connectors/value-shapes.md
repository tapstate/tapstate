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
| `Decimal128` | a number - **see the limit below** |
| BSON timestamp | a timestamp - **see the limit below** |

Everything else - integers, floating point numbers, exact decimals, text, booleans, dates - is
untouched by any of this and reads as it always did.

## Writing to a target of the same kind

A value that arrives as one of those types is written back as that type, not as the text it travelled
as. An `ObjectId` primary key read from one MongoDB and written to another is an `ObjectId` there,
and a binary column is binary, byte for byte.

This works because the row carries the name the source's schema gave the column, and the target's own
connector rebuilds from that. Two consequences follow, and both are visible rather than silent:

- **A target of a different kind gets the value as it reads above** - a hex string, base64 - because
  it has no such type to rebuild into. That is the right answer for it.
- **The schema has to name the column.** It names a field inside a document by its path, so a key
  nested one level down is restored like a top-level one. It names an array and stops, so a value
  inside an array arrives as its text form. There is no way to state "the type of this array's
  elements" in a schema, which is why this one is a limit rather than an oversight.

## Limits worth knowing before you rely on this

Both come from the connector's own conversion, which Tapstate applies as written rather than
second-guessing:

- **`Decimal128` loses precision.** The conversion produces a double, so 34 significant digits become
  about 15 to 17. A column you keep exact decimals in - money, most often - is affected. Read it
  through a target that stores it as a decimal and compare before you depend on it.
- **A BSON timestamp reads as the wrong instant**, not merely a rounded one: the conversion reads the
  seconds field as if it were milliseconds, so a 2026 timestamp reads as January 1970. A BSON
  timestamp is an internal replication type and is rare in application data; an ordinary date column
  is a different type and is not affected.

Each is tracked as its own issue.
