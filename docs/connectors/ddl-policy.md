---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/connectors/ddl-policy
---

# DDL policies

A direct Oracle or Db2 synchronization to MongoDB applies column changes in the same order as
source rows. With `ddl: apply`, a renamed column moves its existing document values, a dropped
column is removed from existing documents, and an added column fills missing fields with its
literal default or `null`. Existing document identities, unrelated target-only fields, and existing values
are retained. This does not require another source snapshot.

The target change is acknowledged before any following row can advance the consumer's progress.
Replaying an already acknowledged column change is safe: ADD fills only absent fields, RENAME
moves only fields that exist, and DROP removes only fields that exist. The command is retained
in the durable change log so it survives engine restarts.

`ddl: ignore` preserves existing target documents and delivers subsequent source rows with their
new shape. `ddl: fail` stops delivery at the DDL boundary. These policies do not prevent the
source connector from updating the model used to decode later rows.

MongoDB has no SQL column type or nullability constraint to alter; non-key type and nullability
changes update source decoding while preserving existing target values. A physical source table
rename retains the selected logical stream and its configured target collection.

Applying field mutations requires a direct row shape. A view, field transform, primary-key change,
unsupported target connector, unsupported default expression, or old log record without a typed
DDL command produces an explicit error before the affected batch writes data. Literal numeric,
boolean, and variable-width string defaults are supported; source expressions such as `SYSDATE`
require a source-coerced value and are refused. SQL text is never submitted to MongoDB as a command.
