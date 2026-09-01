package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The bounded snapshot phase of a capture. Reads the current rows once and passes them straight to the
 * downstream stage — every event is a snapshot read (op {@code r}) and none is buffered in the change
 * ring: the snapshot is absorbed by the idempotent sink, not replicated into the volatile SRS ring, and
 * no source replica is materialized.
 */
public final class SnapshotPhase {

    private SnapshotPhase() {
    }

    /**
     * Runs the snapshot phase over the chain's selected {@code tables}: records the cdc-start position for
     * the chain, drains the bounded snapshot read straight to {@code sink} with every row stamped with the
     * generation this snapshot belongs to, then marks each of those tables' snapshot complete. Returns the
     * number of events passed through.
     *
     * <p>The rows are ordered even though they have no position in the change stream: they carry the
     * reserved snapshot sequence, which places all of them before every change of the same generation, so
     * a change can never be overwritten by the snapshot value of the row it changed.
     *
     * <p>A snapshot that is starting takes {@code ringEpoch} and the seam its own batch sampled; one that
     * is resuming takes the generation and the seam already recorded, as a pair — see
     * {@link #resumedSnapshot}.
     *
     * <p>The two marks bracket the drain and answer different questions. The cdc-start position — the
     * seam this snapshot began at, sampled at the source before its first row — is recorded before the
     * batch drains, so the cdc tail that follows resumes from before the snapshot and the idempotent sink
     * absorbs the overlap; no change made while the snapshot runs is missed. A batch that reports no seam
     * stops the run with a code rather than letting the caller pick a start of its own.
     * Its presence therefore means the snapshot has <em>started</em>.
     * The completion mark is written only after the drain returns, so its presence means the table has been
     * read to exhaustion. A drain that fails partway marks nothing: an aborted snapshot is not a completed
     * one, and a reader treating a partial drain as exhausted would conclude rows are absent from the source
     * that were merely never read. Events are passed through one by one, never buffered in the change ring,
     * and the batch is always closed.
     *
     * <p>Seeding the chain's meta record is a separate lifecycle step; recording the cdc-start position on
     * an unseeded chain is a caller ordering error surfaced by the store.
     */
    public static long run(
            CapturePort port,
            CaptureConfig config,
            String miningChainId,
            List<String> tables,
            long ringEpoch,
            SrsMetaStore meta,
            Consumer<Envelope> sink) {
        Objects.requireNonNull(port, "port");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(tables, "tables");
        Objects.requireNonNull(meta, "meta");
        Objects.requireNonNull(sink, "sink");
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("a snapshot over a chain must name the tables it reads");
        }

        Optional<SrsMeta> resumed = resumedSnapshot(meta, miningChainId, tables);
        long epoch = resumed.map(SrsMeta::snapshotEpoch).orElse(ringEpoch);
        SourceOrder order = SourceOrder.snapshotRow(epoch);
        long count = 0;
        try (CaptureBatch batch = port.snapshot(config)) {
            // The seam comes from the batch, which sampled it at the source before reading its first row.
            // A source that reports none leaves the tail nothing to join to, and the run stops here rather
            // than proceeding: a snapshot whose tail then begins wherever it likes drops every change made
            // while the snapshot ran, with nothing thrown and nothing logged.
            SourcePosition seam = batch.seam().orElseThrow(() -> new TapstateException(
                    CaptureError.SNAPSHOT_REPORTS_NO_SEAM, Map.of("chain", miningChainId), null));
            // A resume rewrites the pair it read back, unchanged. Both halves come from the same record and
            // the same question, so one of them moving on its own is the state that has no meaning: rows
            // pinned to a generation whose seam is somewhere else.
            String start = resumed.map(SrsMeta::cdcStartPosition).orElse(seam.token());
            meta.setCdcStart(miningChainId, start, epoch);
            while (batch.hasNext()) {
                sink.accept(batch.next().withOrder(order));
                count++;
            }
        }
        // One drain reads every selected table to exhaustion, so each of them is marked - marking only the
        // first would leave the rest looking un-drained and pin a later re-mine to this run's generation.
        for (String table : tables) {
            meta.markSnapshotComplete(miningChainId, table);
        }
        return count;
    }

    /**
     * The recorded snapshot this run is resuming, or empty when whatever runs now is a snapshot of its own.
     *
     * <p>A resuming run reuses two things and reuses them together: the generation its rows are pinned to,
     * and the seam its tail joins at. One call wrote them and one act is described by them — the seam says
     * where the changes this snapshot does not cover begin, the generation says where its rows sit against
     * those changes — so a run that took one from the record and one from itself would be describing an act
     * that never happened.
     *
     * <p>A snapshot outlives the ring it started under. Every restart rebuilds the ring and opens a new
     * generation, and a higher generation wins every comparison — so a rerun whose rows took the generation
     * running now would beat every change the earlier one had already applied and roll each of those rows
     * back to its snapshot value. It would happen silently, and only while the snapshot runs. Keeping the
     * generation makes that reversal impossible rather than unlikely: the rerun's rows sit below every
     * change of their own generation and below every generation after it.
     *
     * <p>Keeping the seam closes the matching hole on the tail's side. The rerun's batch samples a seam of
     * its own, later than the recorded one, and taking that would move where the tail begins forward over
     * the span between the two. Nothing else covers that span: the rerun re-reads the rows that are there
     * now, so a row deleted since the recorded seam is in neither the re-read nor a tail that starts after
     * the delete, and it stays in the target for good — nothing thrown, nothing logged. Starting at the
     * recorded seam replays the span instead, which the idempotent sink absorbs.
     *
     * <p>Resuming is "a seam was recorded and some selected table has not drained". A run whose tables are
     * all marked drained means whatever runs now is a new snapshot — a re-mine — and a new baseline of truth
     * is entitled to beat what came before, so it takes the current generation and the seam it sampled
     * itself; reusing the recorded seam there would replay everything since that run on every re-mine, for
     * ever. One table of the selection still un-drained keeps the whole run pinned: the rows of its drained
     * siblings are re-read by the same drain, and letting those take the running generation is exactly the
     * silent roll-back this pins against.
     */
    private static Optional<SrsMeta> resumedSnapshot(
            SrsMetaStore meta, String miningChainId, List<String> tables) {
        return meta.read(miningChainId)
                .filter(record -> record.snapshotEpoch() != 0L)
                .filter(record -> !record.snapshotCompletedTables().containsAll(tables));
    }

    /**
     * Drains the bounded snapshot read straight to {@code sink}, returning the number of events passed
     * through. Pure pass-through: it records no cdc-start position and touches no meta record — the path a
     * {@code snapshot_only} or srs-disabled read takes, where there is no shared chain a cdc tail resumes
     * against. Events go one by one, never buffered in the change ring, and the batch is always closed.
     */
    public static long drain(CapturePort port, CaptureConfig config, Consumer<Envelope> sink) {
        Objects.requireNonNull(port, "port");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(sink, "sink");

        long count = 0;
        try (CaptureBatch batch = port.snapshot(config)) {
            while (batch.hasNext()) {
                sink.accept(batch.next());
                count++;
            }
        }
        return count;
    }
}
