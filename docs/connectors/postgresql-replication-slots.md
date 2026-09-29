---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/connectors/postgresql-replication-slots
---

# PostgreSQL replication slots

Reading changes from PostgreSQL goes through a **logical replication slot** that the `postgres` connector
creates on your database the first time a pipeline reads it. Its name starts with `tapdata_cdc_`.
PostgreSQL keeps every write-ahead log (WAL) segment the slot has not confirmed, so how far the slot is
confirmed decides how much log your database holds on to. This page says when Tapstate moves it, when it
holds it back, and when it drops it.

## The slot follows your pipelines

- **A change is confirmed once it has landed.** The slot moves past a change only after every pipeline
  that reads that change's table from this source has written it to its target. A change one pipeline has
  not landed yet holds the slot before it, however far other pipelines reading other tables have got -
  so a restart is always sent every change that has not landed somewhere.
- **A quiet source still moves on.** When the tables you capture stop changing but the database keeps
  writing elsewhere, the slot follows the head of the log rather than staying at the last change.
- **Expect it within about half a minute.** Tapstate reads the confirmed position every few seconds, hands
  it to the connector at most every few seconds, and PostgreSQL shows it once the reader next reports back.
  `pg_stat_replication.flush_lag` stays in seconds while pipelines keep up.
- **One slot per source.** Every pipeline reading the same PostgreSQL source shares one slot, whichever of
  them happens to start the stream - after a restart, after a failover, after the first one is removed. A
  pipeline that reads the source directly, with `srs.enabled: false`, has a slot of its own.

To watch it on the source:

```sql
SELECT slot_name, active, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
FROM pg_replication_slots
WHERE slot_name LIKE 'tapdata_cdc_%';
```

## Which sources this applies to

The `postgres` connector, and connectors built on it, confirm positions when they read through a logical
decoding plugin: `pgoutput` (the default), `wal2json` or `decoderbufs`, set with `logPluginName`. With
`walminer` or the `physical` plugin the connector does not confirm anything, and the slot stays where it
was created, as it did in earlier versions. Other source databases are not affected.

## A paused pipeline holds the log

A pipeline that is paused, or stopped with its state kept (`stop <pipeline> --keep-state`), still owes
every change after its position, so the slot waits for it - and so does every other pipeline sharing the
slot, since the slot cannot move past what one of them has not landed. PostgreSQL keeps WAL for as long as
that lasts.

Bound it on the source with `max_slot_wal_keep_size`. Past that limit PostgreSQL invalidates the slot;
the pipelines reading through it cannot resume from where they were, and have to be rerun with
`restart <pipeline> --rerun`.

## Rewinding a pipeline, and `keepWalHours`

Once a position is confirmed, PostgreSQL will not send the changes before it again. A request to start
earlier is not refused: the stream starts at the confirmed position, without an error, and whatever lay
before it is not read.

So writing an earlier position back with `position <pipeline> -f <file>` re-reads changes only as far
back as the slot has not yet confirmed. If you need a window to rewind into, set `keepWalHours` on the
source (hours, default `0`): the connector then confirms positions that many hours late, the slot trails
your pipelines by that much, and the source keeps that much more WAL.

## Clearing a pipeline's state drops the slot

Clearing a pipeline's state - `stop <pipeline>` without `--keep-state`, or `restart <pipeline> --rerun` -
drops the slot **when no other pipeline reads the same source**. Clearing one of several pipelines that
share the source leaves the slot to the others, and a stop that keeps the state keeps it.

- The connector drops the slot only while `autoClearSlot` is on, which it is by default. With it off, the
  slot stays after clearing and is yours to drop.
- A slot you named yourself with `customSlotName` is dropped the same way when `autoClearSlot` is on.
  Turn `autoClearSlot` off to keep it.
- If the source cannot be reached at that moment, the clearing still completes, and the server logs a
  warning, `connector.release-failed`, naming the slot. Drop it on the source once nothing is using it:

  ```sql
  SELECT pg_drop_replication_slot('<slot_name>');
  ```

The stop confirmation lists the slot among what a clearing takes.

## Upgrading from an earlier version

**A pipeline reading several tables may stop at its first start** with `capture.shared-position-unverified`.
An earlier version recorded one position for all of a source's tables without proof that every table's
changes before it had landed. Either rerun it with `restart <pipeline> --rerun`, which reads the source
again, or set a position you have checked with `position <pipeline> -f <file>` and start it.

**Earlier versions left slots behind.** They never dropped a slot when a pipeline's state was cleared, and
pipelines sharing a source could each create one. After the upgrade a source keeps using one slot; the
others stay until you drop them. To find them, start every pipeline that reads the database, then list the
Tapstate slots nobody holds:

```sql
SELECT slot_name, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
FROM pg_replication_slots
WHERE slot_name LIKE 'tapdata_cdc_%' AND NOT active;
```

With every pipeline on that database running, an inactive one is not in use, and
`pg_drop_replication_slot` removes it. A pipeline stopped with its state kept needs its slot even though
the slot is inactive, which is why every pipeline is started first.
