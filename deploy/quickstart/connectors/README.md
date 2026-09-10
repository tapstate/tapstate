# Connector seed directory (optional)

This directory is bind-mounted into the server as its connector seed directory. Drop connector
`*.jar` files here **before** starting the stack and each one is registered once, at server
startup, through the same register-if-absent path `tapstate register` uses. It is a convenience
for staging jars offline or in bulk.

This preview certifies the following database kinds, with certification scoped by direction:

| Database | Connector kind | Certified use |
|---|---|---|
| MySQL | `mysql` | Read and write |
| PostgreSQL | `postgres` | Read and write |
| MongoDB | `mongodb` | Read and write |
| Oracle | `oracle` | Source only |
| SQL Server | `sqlserver` | Source only |

The catalog may report write capability for Oracle and SQL Server; those write paths
are not certified in this preview. The default accepted set contains 16 connector ids
across these five database kinds, including existing managed variants of MySQL,
PostgreSQL and MongoDB. Those managed variants have not been live-verified individually.
Other managed variants of Oracle and SQL Server are outside the default accepted set.

`tapstate.connectors.also-accept-ids` lets an operator accept additional connector ids
on this server. Configuring it puts that server outside the supported configuration;
acceptance does not certify the added connectors. The setting is empty by default and
is not configured in release or quickstart artifacts. A `connector.not-official` refusal
reports the server's actual accepted set, including any additional ids configured there.
Registration through an upload and registration through the seed directory use the same
acceptance check.

Oracle connector bytes, including the bundled `ojdbc8` driver under the Oracle Free Use
Terms, are excluded from versioned releases, `connectors-preview`, and quickstart.
They are retained only as CI artifacts for 7 days. This distribution boundary does not
establish a license for the upstream enterprise connector repository, which has no
LICENSE file.

The Oracle Free 23 source example selects `autoLog: false` and uses schema, table
and column identifiers no longer than 30 characters. The automatic miner requests
`CONTINUOUS_MINE`, which that database no longer supports. Long schema identifiers
can pass snapshot reads while Oracle LogMiner marks their changes unsupported;
Tapstate does not yet reject that configuration before starting.

A refusal is reported for that jar alone and the seed sweep carries on with the rest.

It is **not** how a connector is normally registered, and it is **not** a precondition for
registration. The usual path is:

```
tapstate register <path-to-jar>
```

which uploads the jar's bytes to the running server over HTTP -- no mount, and nothing in this
directory, is involved. Registering that way works whether or not this directory exists or holds
anything. **Leaving this directory empty is the expected case.**

Notes:

- The mount is read-only: files here are read, never written. The registered bytes live in the
  store (Mongo), not here, so removing a jar after it has been registered does not unregister it.
- Only `*.jar` is swept; this README is ignored.
- Both routes -- a jar swept from here and a jar uploaded by `tapstate register` -- reach the
  identical content-hash register-if-absent path, and are held to the same accepted connector set,
  so the same jar registered either way is one registration, not two.
