# Connector seed directory (optional)

This directory is bind-mounted into the server as its connector seed directory. Drop connector
`*.jar` files here **before** starting the stack and each one is registered once, at server
startup, through the same register-if-absent path `tapstate register` uses. It is a convenience
for staging jars offline or in bulk.

This preview certifies the following database kinds, with certification scoped by direction:

| Database | Connector kind | Certified use |
|---|---|---|
| MySQL | `mysql` | Read |
| PostgreSQL | `postgres` | Read |
| MongoDB | `mongodb` | Read and write |
| Oracle | `oracle` | Read |
| SQL Server | `sqlserver` | Read |

A `serve.sync` element in the cloud deployment profile installs only onto MongoDB Atlas.
On-prem deployments may write to any catalog connector marked sink-capable; private connectors
outside the catalog remain the operator's responsibility. This deployment allowance is not a
certification claim: this preview certifies MongoDB write support only. Applying a cloud pipeline
whose sync names another connector is refused; reading through it is unaffected.

Db2 is accepted as a source-only **preview**, outside the certified table: no release lane
reads a live Db2 database. Snapshot, change capture of inserts, updates and deletes, and
continuation across a server restart were verified by hand against Db2 LUW 11.5.5 in both
change-capture modes below.
Db2 is supported as a source only. Its connector can write, but its catalog row is not
sink-capable, so a `serve.sync` naming a `db2` connection is refused on-prem as well as in
the cloud profile. Change capture needs archive logging on the database and
`DATA CAPTURE CHANGES` on each captured table. With `useNativeMiner: true` the connector reads
the log in-process; that needs the server on Linux x86_64, running as root at least for the
first Db2 connection, and the IBM Db2 runtime client prepared once on that host with
`db2-native-runtime-setup.sh` from the same release. Otherwise it reads from a raw log server
at `rawLogServerHost` and `rawLogServerPort`; that mode is experimental and not supported for
production use.

Reads are verified on Oracle Free 23 and SQL Server 2022, and across MySQL, PostgreSQL and
MongoDB, with snapshot and CDC inserts, updates and deletes.
Decimal validation includes a persisted MySQL DECIMAL(18,4) model, large values,
negative fractions and CDC updates. This is not an exhaustive cross-version or
all-data-type matrix. The default accepted set contains 17 connector ids: the Db2 preview,
and 16 across these five database kinds, including existing managed variants of MySQL,
PostgreSQL and MongoDB. Those managed variants have not been live-verified individually.
Other managed variants of Oracle, SQL Server and Db2 are outside the default accepted set.

`tapstate.connectors.also-accept-ids` lets an operator accept additional connector ids
on this server. Configuring it puts that server outside the supported configuration;
acceptance does not certify the added connectors. The setting is empty by default and
is not configured in release or quickstart artifacts. A `connector.not-official` refusal
reports the server's actual accepted set, including any additional ids configured there.
Registration through an upload and registration through the seed directory use the same
acceptance check.

Numeric source attributes, including precision, scale and value bounds, are preserved
through schema storage and target preparation. Decimal columns whose metadata was
stored by an older build need schema rediscovery before automatic target creation.
Missing or inconsistent decimal metadata is refused before writing; computed decimal
outputs without a declared numeric domain cannot be auto-created safely.

Oracle, SQL Server and Db2 connector jars are separate assets on the floating
`connectors-preview` release. They remain outside versioned Tapstate releases and this
three-database quickstart does not fetch them automatically. An authenticated CLI can
download and register each one explicitly:

```
tapstate register oracle
tapstate register sqlserver
tapstate register db2
```

The Oracle jar bundles `ojdbc8`, `orai18n`, and `xdb` 21.5.0.0 under the
Oracle Free Use Terms; the SQL Server jar bundles Microsoft JDBC Driver 12.2.0 under the MIT License;
the Db2 jar bundles IBM Data Server Driver for JDBC and SQLJ 4.25.13 under
IBM's International Program License Agreement.
Those dependency terms govern only the bundled drivers and do not change Tapstate's Apache-2.0
license. The Oracle, SQL Server and Db2 implementations are paid connector implementations; their use
remains subject to the applicable Tapdata agreement.
The upstream enterprise connector repository has no LICENSE file; publishing these binary
assets does not relicense that source repository.

The Oracle Free 23 source example selects `autoLog: false` and uses schema, table
and column identifiers no longer than 30 characters. The automatic miner requests
`CONTINUOUS_MINE`, which that database no longer supports. Long schema identifiers
can pass snapshot reads while Oracle LogMiner marks their changes unsupported;
Tapstate does not yet reject that configuration before starting.

A refusal is reported for that jar alone and the seed sweep carries on with the rest.

It is **not** how a connector is normally registered, and it is **not** a precondition for
registration. The usual path is:

```
tapstate register <path-to-jar-or-published-id>
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
