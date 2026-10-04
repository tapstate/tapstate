---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/connectors/postgresql-replication-slots
---

# PostgreSQL replication slots

Reading changes from PostgreSQL goes through a **logical replication slot** that the `postgres` connector
creates on your database the first time a pipeline reads it. Its name starts with `tapdata_cdc_`.
PostgreSQL keeps every write-ahead log (WAL) segment the slot has not confirmed, so how far the slot is
confirmed decides how much log your database holds on to. This page says when Tapstate moves it, what
holds it back, and when it drops it.

## The slot follows Tapstate's change log

Pipelines read a source through a shared change log by default. Every change read from the source is
written into that log, in Tapstate's metadata store, before any pipeline is handed it.

- **A change is confirmed once it is in the log.** The slot moves past a change as soon as the change has
  been written into Tapstate's change log, whether or not every pipeline has written it to its target yet.
  A pipeline that has not landed it reads it from the log, not from your database.
- **A quiet source still moves on.** When the tables you capture stop changing but the database keeps
  writing elsewhere, the slot follows the head of the log rather than staying at the last change.
- **Expect it within about half a minute.** Tapstate reads the confirmed position every few seconds, hands
  it to the connector at most every few seconds, and PostgreSQL shows it once the reader next reports back.
  `pg_stat_replication.flush_lag` stays in seconds while the source is being read.
- **One slot per source.** Every pipeline reading the same PostgreSQL source through the change log shares
  one slot, whichever server in the cluster is reading it.

To watch it on the source:

```sql
SELECT slot_name, active, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
FROM pg_replication_slots
WHERE slot_name LIKE 'tapdata_cdc_%';
```

## A paused pipeline does not hold the source

A pipeline that is paused, slow, or stopped with its state kept (`stop <pipeline> --keep-state`) still owes
every change after its position, but it owes them to Tapstate's change log, not to your database. The slot
goes on moving with the log, the other pipelines go on reading, and the paused pipeline catches up from the
log when it runs again. What it has not read takes up room in Tapstate's metadata store instead of your
database's WAL.

## Tapstate's metadata store holds changes your database has let go of

Once a change is confirmed, PostgreSQL may recycle the WAL that held it, and the only copy of a change a
pipeline has not landed yet is in Tapstate's change log. So while pipelines are running:

- **Do not delete Tapstate's metadata store**, or drop its collections, to "reset" a pipeline. Clear a
  pipeline's state with `stop <pipeline>` or `restart <pipeline> --rerun` instead.
- **Do not restore it from an older backup.** A restored store is missing the changes written into the log
  since the backup, and the source no longer has them.

If the log no longer has a change a pipeline still needs, the pipeline does not skip it: it stops with
`capture.recovery-log-gap`. Rerun it with `restart <pipeline> --rerun`, which reads the source again.

## Pipelines that read the source directly

A pipeline whose source sets `srs.enabled: false` reads the source directly, without the change log, through
a slot of its own. There is no log to replay from, so its slot moves past a change only once the pipeline
has written that change to its target. Through quiet periods its slot still follows the head of the log.

Such a pipeline does hold your database's WAL while it is paused, stopped with its state kept, or unable to
write to its target. Bound it on the source with `max_slot_wal_keep_size`. Past that limit PostgreSQL
invalidates the slot, the pipeline cannot resume from where it was, and it has to be rerun with
`restart <pipeline> --rerun`.

## Which sources this applies to

The `postgres` connector, and connectors built on it, confirm positions when they read through a logical
decoding plugin: `pgoutput` (the default), `wal2json` or `decoderbufs`, set with `logPluginName`. With
`walminer` or the `physical` plugin the connector does not confirm anything, and the slot stays where it
was created, as it did in earlier versions. Other source databases are not affected.

## Rewinding a pipeline, and `keepWalHours`

Once a position is confirmed, PostgreSQL will not send the changes before it again. A request to start
earlier is not refused: the stream starts at the confirmed position, without an error, and whatever lay
before it is not read.

So writing an earlier position back with `position <pipeline> -f <file>` re-reads changes only as far
back as the slot has not yet confirmed. If you need a window to rewind into, set `keepWalHours` on the
source (hours, default `0`): the connector then confirms positions that many hours late, the slot trails
Tapstate by that much, and the source keeps that much more WAL.

## Clearing a pipeline's state drops the slot

Clearing a pipeline's state - `stop <pipeline>` without `--keep-state`, or `restart <pipeline> --rerun` -
drops the slot it read through **once no other pipeline reads through that slot**. Clearing one of several
pipelines that share the change log leaves the shared slot to the others; clearing a pipeline that reads its
source directly drops its own slot, whatever else reads the source. Nothing else drops a slot: a stop that
keeps the state, pausing, resuming and a plain `restart` all leave it where it is, so the pipeline carries on
from it.

- The connector drops the slot only while `autoClearSlot` is on, which it is by default. With it off, the
  slot stays after clearing and is yours to drop.
- A slot you named yourself with `customSlotName` is dropped the same way when `autoClearSlot` is on.
  Turn `autoClearSlot` off to keep it.
- A pipeline that only loads its source (`read_mode: snapshot_only`) still creates a slot when its load
  starts, reads nothing through it afterwards, and clearing it does not drop that slot yet. Drop it by hand
  as below once the load is done.
- If the source cannot be reached at that moment, or does not answer within a minute, the clearing still
  completes, and the server logs a warning, `connector.release-failed`, naming the source and the slot.
  Drop the slot on the source once nothing is using it:

  ```sql
  SELECT pg_drop_replication_slot('<slot_name>');
  ```

The stop confirmation lists the slot among what a clearing takes.

## Upgrading from an earlier version

**Earlier versions left slots behind.** They never confirmed a position, so their slots held all the WAL
since they were created; they never dropped a slot when a pipeline's state was cleared; and pipelines
sharing a source could each create one. After the upgrade a source keeps using one slot; the others stay
until you drop them. To find them, start every pipeline that reads the database, then list the Tapstate
slots nobody holds:

```sql
SELECT slot_name, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
FROM pg_replication_slots
WHERE slot_name LIKE 'tapdata_cdc_%' AND NOT active;
```

With every pipeline on that database running, an inactive one is not in use, and
`pg_drop_replication_slot` removes it. A pipeline stopped with its state kept needs its slot even though
the slot is inactive, which is why every pipeline is started first.

A pipeline whose recorded position an earlier version wrote without enough to resume from safely stops at
its first start with `capture.recovery-progress-unproven`, and keeps its state for you to look at. Rerun it
with `restart <pipeline> --rerun`.
