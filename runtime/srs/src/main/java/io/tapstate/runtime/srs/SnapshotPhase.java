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
     * the chain, then reads each table it still owes with a bounded read of its own, straight to
     * {@code sink} with every row stamped with the generation this snapshot belongs to. Returns the number
     * of events passed through.
     *
     * <p>A table at a time, and only the tables still owed. Which those are is read from this pipeline's
     * own record on the chain: a table is owed until this pipeline's sink is recorded as having written
     * it. Another pipeline on the same chain having finished it says nothing -- it wrote to its target,
     * not to this one. That is a different question from
     * whether it was read, and only the first is safe to skip on -- a table read and never written, skipped
     * on the way back, leaves every one of its rows that has not changed since absent from the target for
     * good, because the tail only replays what changed after the seam.
     *
     * <p>Nothing here marks a table written. That mark is the sink's to make, when its frontier confirms
     * that table's rows; a reader that also made it would be a second voice on the one question this phase
     * cannot answer, and the two would disagree in the direction that loses rows.
     *
     * <p>The rows are ordered even though they have no position in the change stream: they carry the
     * reserved snapshot sequence, which places all of them before every change of the same generation, so
     * a change can never be overwritten by the snapshot value of the row it changed.
     *
     * <p>A snapshot that is starting takes {@code ringEpoch} and the seam its own batch sampled; one that
     * is resuming takes the generation and the seam already recorded, as a pair — see
     * {@link #resumedSnapshot}.
     *
     * <p>The cdc-start position is the one thing this phase writes. It — the
     * seam this snapshot began at, sampled at the source before its first row — is recorded before the
     * first table drains, so the cdc tail that follows resumes from before the snapshot and the idempotent
     * sink absorbs the overlap; no change made while the snapshot runs is missed. A batch that reports no
     * seam stops the run with a code rather than letting the caller pick a start of its own.
     * Its presence therefore means the snapshot has <em>started</em>.
     * A read that fails partway stops the run there: the tables after it are not read, and none of them --
     * nor the one that failed -- is recorded as anything. Events are passed through one by one, never
     * buffered in the change ring, and every batch is always closed.
     *
     * <p>One seam covers the whole round, taken from the first read of it and never replaced. Each bounded
     * read samples a seam of its own, later than the one before, and letting a later table's seam move
     * where the tail begins would leave the span between the two covered by nothing: the tables already
     * read were read before it, and the tail starts after it, so a row deleted in between is in neither and
     * stays in the target for good — nothing thrown, nothing logged.
     *
     * <p>Seeding the chain's meta record is a separate lifecycle step; recording the cdc-start position on
     * an unseeded chain is a caller ordering error surfaced by the store.
     */
    public static long run(
            CapturePort port,
            CaptureConfig config,
            String miningChainId,
            String pipelineId,
            List<String> tables,
            long ringEpoch,
            SrsMetaStore meta,
            Consumer<Envelope> sink) {
        Objects.requireNonNull(port, "port");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(tables, "tables");
        Objects.requireNonNull(meta, "meta");
        Objects.requireNonNull(sink, "sink");
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("a snapshot over a chain must name the tables it reads");
        }

        Optional<SrsMeta> record = meta.read(miningChainId);
        List<String> owed = stillOwed(record, pipelineId, tables);
        Optional<SrsMeta> resumed = resumedSnapshot(record, owed);
        long epoch = resumed.map(SrsMeta::snapshotEpoch).orElse(ringEpoch);
        SourceOrder order = SourceOrder.snapshotRow(epoch);
        // A resume rewrites the pair it read back, unchanged. Both halves come from the same record and the
        // same question, so one of them moving on its own is the state that has no meaning: rows pinned to
        // a generation whose seam is somewhere else.
        String resumedStart = resumed.map(SrsMeta::cdcStartPosition).orElse(null);
        boolean seamRecorded = false;
        long count = 0;
        for (String table : owed) {
            try (CaptureBatch batch = port.snapshot(readOf(config, table))) {
                // The seam comes from the batch, which sampled it at the source before reading its first
                // row. A source that reports none leaves the tail nothing to join to, and the run stops
                // here rather than proceeding: a snapshot whose tail then begins wherever it likes drops
                // every change made while the snapshot ran, with nothing thrown and nothing logged.
                SourcePosition seam = batch.seam().orElseThrow(() -> new TapstateException(
                        CaptureError.SNAPSHOT_REPORTS_NO_SEAM, Map.of("chain", miningChainId), null));
                if (!seamRecorded) {
                    meta.setCdcStart(miningChainId, resumedStart != null ? resumedStart : seam.token(),
                            epoch);
                    seamRecorded = true;
                }
                while (batch.hasNext()) {
                    sink.accept(batch.next().withOrder(order));
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * The selected tables this run still owes a read: the ones this pipeline's own record does not show as
     * written. A table is owed until this pipeline's sink has confirmed it, however far its read got --
     * reading is not writing, and the only one of the two that is safe to skip a table on is the second.
     *
     * <p>This is also what a hold has to be resumed against, which is why it is not private. The rows a
     * load has read but not delivered live nowhere durable, so the tables this reports are exactly the
     * ones a rebuild would read again -- one reckoning for both questions, because two of them would
     * eventually disagree and the disagreement is silent either way round.
     *
     * <p>Read against the pipeline and not the chain. A chain is keyed by the source connection and
     * excludes the table subset, so pipelines reading one database share a chain by construction while
     * writing to targets of their own -- and a chain-level reading hands a pipeline new to the chain the
     * answer another pipeline produced. It then owes nothing, enters no snapshot phase at all, and its
     * target keeps none of the rows that were there before it started, with the run healthy and nothing
     * logged.
     */
    public static List<String> stillOwed(
            Optional<SrsMeta> record, String pipelineId, List<String> tables) {
        List<String> written =
                record.map(stored -> stored.snapshotCompletedTables(pipelineId)).orElse(List.of());
        return tables.stream().filter(table -> !written.contains(table)).toList();
    }

    /**
     * The bounded read that covers {@code table} alone, on the connection the whole capture runs on. The
     * selection is the only thing that narrows: everything a connector needs to open the source is what the
     * capture was configured with, and a read that changed any of it would be reading somewhere else.
     */
    private static CaptureConfig readOf(CaptureConfig config, String table) {
        return new CaptureConfig(config.connectorId(), config.settings(), List.of(table));
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
     * <p>Resuming is "a seam was recorded and some selected table is still owed". A run that owes none
     * means whatever runs now is a new snapshot — a re-mine — and a new baseline of truth is entitled to
     * beat what came before, so it takes the current generation and the seam it sampled itself; reusing the
     * recorded seam there would replay everything since that run on every re-mine, for ever.
     */
    private static Optional<SrsMeta> resumedSnapshot(Optional<SrsMeta> record, List<String> owed) {
        return record
                .filter(stored -> stored.snapshotEpoch() != 0L)
                .filter(stored -> !owed.isEmpty());
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
