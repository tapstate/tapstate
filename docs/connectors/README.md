---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/connectors
---

# Connectors

A connector becomes known to Tapstate along one of two paths, and they answer to different rules.

**The bundled catalog** is a set of generated rows shipped inside the binary: one row per connector,
holding its configuration fields, the modes it supports and whether it can be written to. Everything
that validates a workspace offline reads it - `tapstate validate`, and the validation status
`tapstate desc` and `tapstate explain` report - as do the `new` wizard and `-c` tab completion. It is
regenerated from an upstream repository on a schedule, and adding to it means running that
regeneration.

**Registering with a running server** is the other path: `tapstate register <path>` uploads a
connector artifact, the server reads the connector's own declarations back out of it, and
`tapstate connectors` lists what a given server ended up with. This path accepts only the connectors
this release officially supports, so it is not a way to widen what the catalog offers.

The two do not have to agree, and where they disagree the release is the honest one: a row can sit in
the bundled catalog describing a connector that this release will not install.

| Page | Answers |
|---|---|
| [Refreshing the catalog](refreshing-the-catalog.md) | Something moved upstream, or you changed a connector. **What has to be rebuilt, which of the two lanes carries it, and how to run one by hand.** Also: how to read the diff a rebuild produces, and what to look at on a pull request one of the lanes opened. |
| [Declaring modes](declaring-modes.md) | Derivation says a CSV file does CDC and a message queue does nothing. **The overlay** - this repository's own declaration that outranks it: what justifies an entry, how to add one, and why an empty one is refused rather than accepted. |
| [Value shapes](value-shapes.md) | A database has types Tapstate has no name for - an `ObjectId`, a binary column. **What you see when you read one, what a target of the same kind stores, and the two conversions that are known to be lossy.** |
