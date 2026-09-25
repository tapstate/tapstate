package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;

/**
 * The bounded snapshot phase of a capture. Reads the current rows once and passes them straight to the
 * downstream stage — every event is a snapshot read (op {@code r}) and none is buffered in the change
 * ring: the snapshot is absorbed by the idempotent sink, not replicated into the volatile SRS ring, and
 * no source replica is materialized.
 *
 * <p>It comes in two halves, {@link #open} and {@link Load#read}, because the two are waited for by
 * different things: the first settles where the tail will join and has to be done before the job taking
 * the rows is assembled, while the second passes the rows on no faster than that job takes them.
 */
public final class SnapshotPhase {

    private SnapshotPhase() {
    }

    /**
     * Runs the snapshot phase over the chain's selected {@code tables}: records the cdc-start position for
     * the chain, then reads each table it still owes with a bounded read of its own, straight to
     * {@code sink} with every row stamped with the generation this snapshot belongs to. Returns how many
     * events it passed through, and the seam the tail that follows it has to join at.
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
     * seam this snapshot began at, sampled at the source before its first row — is settled before the
     * first table drains, so the cdc tail that follows resumes from before the snapshot and the idempotent
     * sink absorbs the overlap; no change made while the snapshot runs is missed. It goes into this
     * pipeline's record, because every pipeline on a shared chain runs a load of its own. A batch that
     * reports no seam stops the run with a code rather than letting the caller pick a start of its own.
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
    public static Outcome run(
            CapturePort port,
            CaptureConfig config,
            String miningChainId,
            String pipelineId,
            List<String> tables,
            long ringEpoch,
            SrsMetaStore meta,
            Consumer<Envelope> sink) {
        Objects.requireNonNull(sink, "sink");
        try (Load load = open(port, config, miningChainId, pipelineId, tables, ringEpoch, meta)) {
            long rows = load.read(sink, table -> { });
            return new Outcome(rows, load.tailSeam());
        }
    }

    /**
     * The half of {@link #run} that has to happen before the job taking the rows exists: works out which
     * tables this run still owes and, where one is owed, opens the read of the first of them and records
     * the seam its batch sampled as this pipeline's cdc-start position. No row is taken here; {@link
     * Load#read} takes them, a table at a time, on a thread that can afford to wait for room to put them.
     *
     * <p>Split along this line because of who waits for each half. The seam and its record are read while
     * the job is assembled, and a source that cannot be opened is best refused by the start that asked for
     * it. The rows are the load itself: they arrive no faster than the slowest step downstream takes them,
     * which is hours for a large load behind a slow step, and only a job that is already running takes any.
     */
    public static Load open(
            CapturePort port,
            CaptureConfig config,
            String miningChainId,
            String pipelineId,
            List<String> tables,
            long ringEpoch,
            SrsMetaStore meta) {
        Objects.requireNonNull(port, "port");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(tables, "tables");
        Objects.requireNonNull(meta, "meta");
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("a snapshot over a chain must name the tables it reads");
        }

        Optional<SrsMeta> record = meta.read(miningChainId);
        List<String> owed = stillOwed(record, pipelineId, tables);
        Optional<ConsumerOffset> resumed = resumedSnapshot(record, pipelineId, owed);
        long epoch = resumed.map(ConsumerOffset::snapshotEpoch).orElse(ringEpoch);
        SourceOrder order = SourceOrder.snapshotRow(epoch);
        if (owed.isEmpty()) {
            return new Load(port, config, miningChainId, tables, owed, order, null, null, false);
        }
        // A resume reuses the pair it read back. Both halves come from the same record and the same
        // question, so one of them moving on its own is the state that has no meaning: rows pinned to a
        // generation whose seam is somewhere else.
        String resumedStart = resumed.map(ConsumerOffset::cdcStartPosition).orElse(null);
        CaptureBatch first = port.snapshot(readOf(config, owed.getFirst()));
        String tailSeam;
        try {
            SourcePosition seam = seamOf(first, miningChainId);
            tailSeam = resumedStart != null ? resumedStart : seam.token();
            // A resume writes back the pair it read, unchanged; a new load writes the pair it sampled. Both
            // are scoped to this pipeline, so neither can move another pipeline's tail or generation.
            meta.setCdcStart(miningChainId, pipelineId, tailSeam, epoch);
        } catch (RuntimeException | Error failure) {
            first.close();
            throw failure;
        }
        return new Load(port, config, miningChainId, tables, owed, order, tailSeam, first, false);
    }

    /**
     * The seam a batch sampled at the source before reading its first row. A source that reports none
     * leaves the tail nothing to join to, and the run stops here rather than proceeding: a snapshot whose
     * tail then begins wherever it likes drops every change made while the snapshot ran, with nothing
     * thrown and nothing logged.
     */
    private static SourcePosition seamOf(CaptureBatch batch, String miningChainId) {
        return batch.seam().orElseThrow(() -> new TapstateException(
                CaptureError.SNAPSHOT_REPORTS_NO_SEAM, Map.of("chain", miningChainId), null));
    }

    /**
     * What one run of the phase produced: how many rows it passed through, and the seam the tail that
     * follows it has to join at -- null when the run read nothing, because a run that owes no table
     * samples no seam and starts no snapshot the tail has to cover.
     *
     * <p>The seam is handed back rather than left to be read off the chain's record, and that is the whole
     * point of it being here. It is the seam sampled by this invocation, so the tail can start without a
     * second store read and without racing a later update to the same pipeline's durable record.
     */
    public record Outcome(long rows, String tailSeam) {
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
        return new CaptureConfig(config.connectorId(), config.settings(), List.of(table), config.node());
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
     * <p>Resuming is "this pipeline recorded a seam here, and some selected table is still owed" — all
     * three, and the first of them is what keeps one pipeline's snapshot from being read as another's. A
     * run that owes none
     * means whatever runs now is a new snapshot — a re-mine — and a new baseline of truth is entitled to
     * beat what came before, so it takes the current generation and the seam it sampled itself; reusing the
     * recorded seam there would replay everything since that run on every re-mine, for ever.
     */
    private static Optional<ConsumerOffset> resumedSnapshot(
            Optional<SrsMeta> record, String pipelineId, List<String> owed) {
        if (owed.isEmpty()) {
            return Optional.empty();
        }
        return record
                .flatMap(stored -> stored.consumerOffset(pipelineId))
                .filter(offset -> offset.cdcStartPosition() != null)
                .filter(offset -> offset.snapshotEpoch() != 0L);
    }

    /**
     * Drains the bounded snapshot read straight to {@code sink}, returning the number of events passed
     * through. It records no cdc-start position and touches no meta record — the path a
     * {@code snapshot_only} read takes, where there is no change chain a tail resumes against. Every row is
     * stamped with the generation assigned to this bounded run before it leaves: a stateful node needs an
     * order even where there will never be a later change, and a later run needs a higher generation to beat
     * state the earlier one left behind. Events go one by one, never buffered in the change ring, and the
     * batch is always closed.
     */
    public static long drain(
            CapturePort port, CaptureConfig config, long snapshotEpoch, Consumer<Envelope> sink) {
        Objects.requireNonNull(sink, "sink");
        try (Load load = openChainless(port, config, snapshotEpoch)) {
            return load.read(sink, table -> { });
        }
    }

    /**
     * The half of {@link #drain} that opens its read: the batch over every selected stream, not yet read
     * from. {@link Load#read} takes the rows, and says every selected table is loaded once the batch is
     * through -- one read covers them all, so no table is through before the last row of the batch is.
     */
    public static Load openChainless(CapturePort port, CaptureConfig config, long snapshotEpoch) {
        Objects.requireNonNull(port, "port");
        Objects.requireNonNull(config, "config");
        if (snapshotEpoch < 1) {
            throw new IllegalArgumentException(
                    "a chainless snapshot generation must be positive, got " + snapshotEpoch);
        }
        SourceOrder order = SourceOrder.snapshotRow(snapshotEpoch);
        CaptureBatch batch = port.snapshot(config);
        return new Load(port, config, null, config.streams(), config.streams(), order, null, batch, true);
    }

    /**
     * One run's load, opened and not yet read: which tables it owes, the batch of the first of them already
     * open, and the order every row is stamped with.
     *
     * <p>Reading and abandoning are both done through it, from different threads. Closing it while a read
     * is under way closes the batch that read is taking rows from, and whatever that batch does next, the
     * read ends with a {@link CancellationException} and reports no further table as loaded: a table whose
     * read was cut short is not a table whose rows were all handed over, and saying otherwise is what would
     * let a load that lost its tail be recorded as written.
     */
    public static final class Load implements AutoCloseable {

        private final CapturePort port;
        private final CaptureConfig config;
        private final String miningChainId;
        private final List<String> tables;
        private final List<String> owed;
        private final SourceOrder order;
        private final String tailSeam;
        private final boolean oneBatch;
        private CaptureBatch open;
        private boolean closed;

        private Load(CapturePort port, CaptureConfig config, String miningChainId, List<String> tables,
                List<String> owed, SourceOrder order, String tailSeam, CaptureBatch open, boolean oneBatch) {
            this.port = port;
            this.config = config;
            this.miningChainId = miningChainId;
            this.tables = List.copyOf(tables);
            this.owed = List.copyOf(owed);
            this.order = order;
            this.tailSeam = tailSeam;
            this.open = open;
            this.oneBatch = oneBatch;
        }

        /** The seam the tail following this load joins at; null where the load reads nothing. */
        public String tailSeam() {
            return tailSeam;
        }

        /** Whether this load has any row to read at all -- whether any selected table is still owed. */
        public boolean readsAnything() {
            return !owed.isEmpty();
        }

        /**
         * Reads every row this load owes into {@code sink}, a table at a time, and tells {@code loaded} about
         * each table once its last row is in -- then about every selected table it owed nothing for, which
         * has nothing left to hand over. Returns how many rows it passed on.
         *
         * <p>A failure stops the read where it happened: the tables after it are not read, and neither they
         * nor the one that failed are reported. Every batch is closed, whether or not it was read through.
         */
        public long read(Consumer<Envelope> sink, Consumer<String> loaded) {
            Objects.requireNonNull(sink, "sink");
            Objects.requireNonNull(loaded, "loaded");
            long count = 0;
            Set<String> reported = new LinkedHashSet<>();
            if (oneBatch) {
                count += readOpenBatch(sink);
            } else {
                for (int i = 0; i < owed.size(); i++) {
                    String table = owed.get(i);
                    if (i > 0) {
                        openBatchOf(table);
                    }
                    count += readOpenBatch(sink);
                    report(table, loaded, reported);
                }
            }
            for (String table : tables) {
                if (!reported.contains(table)) {
                    report(table, loaded, reported);
                }
            }
            return count;
        }

        /** Abandons the load: closes the batch being read, if any, and keeps any later one from opening. */
        @Override
        public void close() {
            CaptureBatch batch;
            synchronized (this) {
                closed = true;
                batch = open;
                open = null;
            }
            if (batch != null) {
                batch.close();
            }
        }

        private long readOpenBatch(Consumer<Envelope> sink) {
            CaptureBatch batch;
            synchronized (this) {
                batch = open;
            }
            requireOpen();
            long count = 0;
            try {
                while (batch.hasNext()) {
                    sink.accept(batch.next().withOrder(order));
                    count++;
                }
            } finally {
                synchronized (this) {
                    if (open == batch) {
                        open = null;
                    }
                }
                batch.close();
            }
            return count;
        }

        /**
         * Opens the read of a table after the first. Its seam is required as the first one was -- a source
         * that reports none for one table reports none it can be trusted for -- but it is not recorded: one
         * seam covers the whole round, and the first read took it.
         *
         * <p>A load abandoned already opens nothing more. Asked first, because opening a read is a connection
         * to the source; the check after it is the one that decides, and this only spares a load that is
         * over the cost of a read it would close again at once.
         */
        private void openBatchOf(String table) {
            requireOpen();
            CaptureBatch batch = port.snapshot(readOf(config, table));
            try {
                seamOf(batch, miningChainId);
            } catch (RuntimeException | Error failure) {
                batch.close();
                throw failure;
            }
            synchronized (this) {
                if (!closed) {
                    open = batch;
                    return;
                }
            }
            batch.close();
            throw abandoned();
        }

        /**
         * Tells {@code loaded} that {@code table} is through -- unless the load was abandoned. A batch closed
         * under the read may simply have stopped handing rows over, and then what came out of it is a prefix
         * of the table rather than the table.
         */
        private void report(String table, Consumer<String> loaded, Set<String> reported) {
            requireOpen();
            loaded.accept(table);
            reported.add(table);
        }

        private synchronized void requireOpen() {
            if (closed) {
                throw abandoned();
            }
        }

        private static CancellationException abandoned() {
            return new CancellationException("the load was abandoned before it was read through");
        }
    }
}
