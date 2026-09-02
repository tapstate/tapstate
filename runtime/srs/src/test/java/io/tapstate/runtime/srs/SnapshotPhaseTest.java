package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The snapshot phase drains the bounded snapshot read (op {@code r}) straight to the downstream stage,
 * recording the cdc-start position at the seam before it drains, and never buffering a snapshot event in
 * the change ring. Its collaborators are faked here: a port that yields a fixed batch, a meta store that
 * records the cdc-start position, and a capturing downstream sink.
 */
class SnapshotPhaseTest {

    private static CaptureConfig config() {
        return new CaptureConfig("mysql", Map.of(), List.of("orders"));
    }

    /** A read whose source selects two streams, which one bounded batch covers together. */
    private static CaptureConfig multiTableConfig() {
        return new CaptureConfig("mysql", Map.of(), List.of("orders", "customers"));
    }

    /** A read whose source selects three streams. */
    private static CaptureConfig threeTableConfig() {
        return new CaptureConfig("mysql", Map.of(), List.of("orders", "customers", "invoices"));
    }

    private static Envelope row(int id) {
        return row("orders", id);
    }

    private static Envelope row(String src, int id) {
        return Envelope.read(id, src, Map.of("id", id), Map.of());
    }

    @Test
    void drainsTheSnapshotBatchStraightToTheSinkInOrder() {
        List<Envelope> rows = List.of(row(1), row(2), row(3));
        FakePort port = new FakePort(new FakeBatch(rows, "p0"));
        List<Envelope> sink = new ArrayList<>();

        long count = SnapshotPhase.run(
                port, config(), "chain", List.of("orders"),
                1L, new RecordingMeta(new ArrayList<>()), sink::add);

        // Straight through in batch order, each row stamped with the generation and otherwise untouched.
        assertThat(sink).containsExactlyElementsOf(
                rows.stream().map(r -> r.withOrder(SourceOrder.snapshotRow(1L))).toList());
        assertThat(count).isEqualTo(3);
    }

    @Test
    void drainPassesTheSnapshotBatchStraightToTheSinkWithoutRecordingACdcStart() {
        List<Envelope> rows = List.of(row(1), row(2), row(3));
        FakePort port = new FakePort(new FakeBatch(rows, "p0"));
        List<Envelope> sink = new ArrayList<>();

        // drain is the pure pass-through with no meta collaborator: it never records a cdc-start position.
        // It is the path a snapshot_only or srs-disabled read takes, where there is no shared chain a cdc
        // tail would resume against, so there is nothing to position.
        long count = SnapshotPhase.drain(port, config(), sink::add);

        assertThat(sink).containsExactlyElementsOf(rows);
        assertThat(count).isEqualTo(3);
    }

    @Test
    void drainClosesTheSnapshotBatch() {
        FakeBatch batch = new FakeBatch(List.of(row(1)), "p0");

        SnapshotPhase.drain(new FakePort(batch), config(), e -> {});

        assertThat(batch.closed).isTrue();
    }

    @Test
    void closesTheSnapshotBatchAfterDraining() {
        FakeBatch batch = new FakeBatch(List.of(row(1)), "p0");

        SnapshotPhase.run(
                new FakePort(batch), config(), "chain", List.of("orders"),
                1L, new RecordingMeta(new ArrayList<>()), e -> {});

        assertThat(batch.closed).isTrue();
    }

    @Test
    void recordsTheCdcStartPositionBeforeDrainingSoTheCdcTailMissesNoChange() {
        List<String> trace = new ArrayList<>();
        RecordingMeta meta = new RecordingMeta(trace);
        Consumer<Envelope> sink = e -> trace.add("event");

        SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1), row(2)), "binlog.000042:1024")), config(), "chain", List.of("orders"),
                1L, meta, sink);

        // The cdc-start position is the source log position sampled at snapshot start: recorded before the
        // snapshot drains, so the cdc tail resumes from before the snapshot and the idempotent sink absorbs
        // the overlap -- a change made during the snapshot is never missed.
        assertThat(meta.cdcStart).isEqualTo("binlog.000042:1024");
        assertThat(trace).startsWith("cdc-start", "event", "event");
    }

    /**
     * A batch that reports no seam stops the run with a code, rather than the caller choosing a start.
     *
     * <p>The alternative is what makes this worth a test: carrying on and beginning the tail at the
     * source's present moment compiles, runs, and reports a healthy pipeline, while every change made
     * during the snapshot falls between the two and is never delivered. Nothing throws and nothing logs.
     */
    @Test
    void refusesToSnapshotWhenTheBatchReportsNoSeamForItsTailToJoinAt() {
        RecordingMeta meta = new RecordingMeta(new ArrayList<>());

        assertThatThrownBy(() -> SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1)))), config(), "chain", List.of("orders"),
                1L, meta, e -> { }))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class)
                .extracting(e -> ((io.tapstate.core.common.TapstateException) e).code())
                .isEqualTo(CaptureError.SNAPSHOT_REPORTS_NO_SEAM);

        assertThat(meta.cdcStart).as("nothing was recorded for a seam that does not exist").isNull();
    }

    @Test
    void stampsEverySnapshotRowWithTheGenerationTheSnapshotBeganIn() {
        List<Envelope> sink = new ArrayList<>();

        SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1), row(2)), "p0")), config(), "chain", List.of("orders"),
                4L, new RecordingMeta(new ArrayList<>()), sink::add);

        // A snapshot row has no position in the change stream, so nothing else can say where it sits
        // against the changes that follow. Every row of one snapshot shares the reserved sequence, which
        // puts all of them before every change of their generation.
        assertThat(sink).extracting(event -> event.position().order())
                .containsOnly(SourceOrder.snapshotRow(4L));
        assertThat(sink).extracting(event -> event.position().token()).containsOnlyNulls();
    }

    @Test
    void recordsTheSeamPositionTogetherWithTheGenerationItsRowsArePinnedTo() {
        RecordingMeta meta = new RecordingMeta(new ArrayList<>());

        SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1)), "binlog.000042:1024")), config(), "chain", List.of("orders"),
                4L, meta, e -> { });

        assertThat(meta.cdcStart).isEqualTo("binlog.000042:1024");
        assertThat(meta.pinnedEpoch).isEqualTo(4L);
    }

    @Test
    void aSnapshotThatNeverDrainedKeepsItsGenerationWhenItRerunsUnderANewRing() {
        // The chain recorded a seam under generation 1 and this table never finished draining, so the ring
        // that is running now is a rebuild -- generation 2. The rerun's rows must stay on generation 1.
        SrsMeta interrupted = new SrsMeta("chain", null, List.of(), "binlog.000042:1024", List.of(), null,
                List.of(), 2L, 1L);
        RecordingMeta meta = new RecordingMeta(new ArrayList<>(), interrupted);
        List<Envelope> sink = new ArrayList<>();

        SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1)), "binlog.000042:1024")), config(), "chain", List.of("orders"),
                2L, meta, sink::add);

        // This is the whole reason the two generations are kept apart. Rows of a rerun that took the
        // current generation would beat every change generation 1 already applied and roll each of those
        // rows back to its snapshot value -- silently, and only while the snapshot is running.
        assertThat(sink).extracting(event -> event.position().order()).containsOnly(SourceOrder.snapshotRow(1L));
        assertThat(meta.pinnedEpoch).isEqualTo(1L);
    }

    @Test
    void aSnapshotThatNeverDrainedResumesFromTheSeamItRecordedRatherThanTheOneSampledNow() {
        // The same interruption as above, but the source has moved on: the batch this rerun opens samples
        // a later seam than the one the interrupted run recorded.
        SrsMeta interrupted = new SrsMeta("chain", null, List.of(), "binlog.000042:1024", List.of(), null,
                List.of(), 2L, 1L);
        RecordingMeta meta = new RecordingMeta(new ArrayList<>(), interrupted);

        SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1)), "binlog.000099:1")), config(), "chain",
                List.of("orders"), 2L, meta, e -> { });

        // Overwriting the recorded seam with this run's would move where the tail begins forward over the
        // span between the two, and nothing else covers that span: the rerun's snapshot re-reads the rows
        // that are there now, so a row deleted since the first seam is in neither the re-read nor a tail
        // that starts after the delete. It stays in the target for good, with nothing thrown and nothing
        // logged. The seam and the generation are one recorded pair, and a resume reuses both or neither.
        assertThat(meta.cdcStart).isEqualTo("binlog.000042:1024");
        assertThat(meta.pinnedEpoch).isEqualTo(1L);
    }

    @Test
    void readsNothingWhenEverySelectedTableIsAlreadyRecordedAsWritten() {
        // Same recorded seam, and the one selected table is recorded as written.
        SrsMeta written = new SrsMeta("chain", null, List.of(), "binlog.000042:1024", List.of(), null,
                List.of("orders"), 2L, 1L);
        RecordingMeta meta = new RecordingMeta(new ArrayList<>(), written);
        FakePort port = new FakePort(new FakeBatch(List.of(row(1)), "binlog.000099:1"));
        List<Envelope> sink = new ArrayList<>();

        long count = SnapshotPhase.run(port, config(), "chain", List.of("orders"), 2L, meta, sink::add);

        // Nothing is owed, so nothing is read and nothing is written down -- not even a seam. Reading the
        // table again here would be a re-mine, and a re-mine is not something a resume decides to do on its
        // own: it starts by clearing what the record says was written, which is a different verb with its
        // own confirmation. Sampling a seam here would also move where the tail begins, over a span nothing
        // else covers.
        assertThat(count).isZero();
        assertThat(port.asked).isEmpty();
        assertThat(sink).isEmpty();
        assertThat(meta.cdcStart).isNull();
    }

    @Test
    void aChainThatRecordedNoSeamHasNoSnapshotToResumeAndTakesTheGenerationRunningNow() {
        SrsMeta neverSnapshotted = new SrsMeta("chain", null, List.of(), null, List.of(), null,
                List.of(), 2L, 0L);
        RecordingMeta meta = new RecordingMeta(new ArrayList<>(), neverSnapshotted);
        List<Envelope> sink = new ArrayList<>();

        SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1)), "p0")), config(), "chain", List.of("orders"),
                2L, meta, sink::add);

        assertThat(sink).extracting(event -> event.position().order()).containsOnly(SourceOrder.snapshotRow(2L));
    }

    @Test
    void writesTheCdcStartAndNoCompletionMarkOfItsOwn() {
        List<String> trace = new ArrayList<>();
        RecordingMeta meta = new RecordingMeta(trace);
        Consumer<Envelope> sink = e -> trace.add("event");

        SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1), row(2)), "binlog.000042:1024")), config(), "chain", List.of("orders"),
                1L, meta, sink);

        // The cdc-start position is the one thing this phase writes, and it goes down before the drain: it
        // means the snapshot has started, and must precede it or a change made while the snapshot runs
        // would be missed. Whether a table finished is a question about the sink, and this phase does not
        // answer it -- a table read and never written would otherwise look done to whoever resumes, and be
        // skipped.
        assertThat(trace).containsExactly("cdc-start", "event", "event");
        assertThat(meta.completed).isEmpty();
    }

    @Test
    void readsEverySelectedTableThatIsStillOwed() {
        FakePort port = new FakePort(Map.of(
                "orders", new FakeBatch(List.of(row("orders", 1)), "p0"),
                "customers", new FakeBatch(List.of(row("customers", 1)), "p0")));
        RecordingMeta meta = new RecordingMeta(new ArrayList<>());

        SnapshotPhase.run(port, multiTableConfig(), "chain",
                List.of("orders", "customers"), 1L, meta, e -> { });

        // Nothing in the record shows either table as written, so both are owed and both are read -- one
        // bounded read each, in selection order.
        assertThat(port.asked).containsExactly(List.of("orders"), List.of("customers"));
        assertThat(meta.completed).isEmpty();
    }

    @Test
    void readsEachSelectedTableOnItsOwnSoAnInterruptionStopsAtTheTableItWasOn() {
        FakePort port = new FakePort(Map.of(
                "orders", new FakeBatch(List.of(row("orders", 1)), "p0"),
                "customers", new FakeBatch(List.of(row("customers", 1)), "p0"),
                "invoices", new FakeBatch(List.of(row("invoices", 1)), "p0")));
        RecordingMeta meta = new RecordingMeta(new ArrayList<>());
        Consumer<Envelope> failingOnCustomers = event -> {
            if ("customers".equals(event.src())) {
                throw new IllegalStateException("sink down");
            }
        };

        assertThatThrownBy(() -> SnapshotPhase.run(
                port, threeTableConfig(), "chain", List.of("orders", "customers", "invoices"),
                1L, meta, failingOnCustomers))
                .isInstanceOf(IllegalStateException.class);

        // One bounded read per table is what lets an interrupted load keep what it finished: the read stops
        // at the table it was on and the ones after it are never opened, so what the sink has already
        // confirmed stays confirmed and a resume re-reads only what is still owed. A single read over the
        // whole selection cannot say that -- it fails as a unit, so a load interrupted at the fifty-first
        // table of a hundred resumes by reading all hundred again. Nothing is marked here either way:
        // reading is not writing.
        assertThat(port.asked).containsExactly(List.of("orders"), List.of("customers"));
        assertThat(meta.completed).isEmpty();
    }

    @Test
    void recordsTheSeamOfTheRoundOnceRatherThanTheOneEachTableSamples() {
        List<String> trace = new ArrayList<>();
        FakePort port = new FakePort(Map.of(
                "orders", new FakeBatch(List.of(row("orders", 1)), "binlog.000042:1024"),
                "customers", new FakeBatch(List.of(row("customers", 1)), "binlog.000042:9999")));
        RecordingMeta meta = new RecordingMeta(trace);

        SnapshotPhase.run(port, multiTableConfig(), "chain", List.of("orders", "customers"),
                1L, meta, e -> { });

        // Every table of one round joins the tail at the same seam. Each bounded read samples one of its
        // own, later than the last, and letting a later table's seam move where the tail begins would leave
        // the span between the two covered by nothing: the earlier tables were read before it and the tail
        // starts after it, so a row deleted in between is in neither, and it stays in the target for good.
        assertThat(port.asked).containsExactly(List.of("orders"), List.of("customers"));
        assertThat(meta.cdcStart).isEqualTo("binlog.000042:1024");
        assertThat(trace).containsExactly("cdc-start");
    }

    @Test
    void oneTableOfTheSelectionStillUndrainedKeepsTheGenerationForAllOfThem() {
        // The seam was recorded under generation 1 and the ring running now is a rebuild -- generation 2.
        // One of the two selected tables drained before the interruption; the other did not.
        SrsMeta interrupted = new SrsMeta("chain", null, List.of(), "binlog.000042:1024", List.of(), null,
                List.of("orders"), 2L, 1L);
        RecordingMeta meta = new RecordingMeta(new ArrayList<>(), interrupted);
        List<Envelope> sink = new ArrayList<>();

        SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1)), "binlog.000042:1024")), multiTableConfig(), "chain",
                List.of("orders", "customers"), 2L, meta, sink::add);

        // The whole selection is re-read by the one drain, the already-drained table included. Judging the
        // resume on that table alone would call this a fresh re-mine and stamp its rows with the generation
        // running now -- which beats every change generation 1 already applied to them and rolls each of
        // those rows back to its snapshot value, silently, exactly what pinning exists to prevent.
        assertThat(meta.pinnedEpoch).isEqualTo(1L);
        assertThat(sink).extracting(event -> event.position().order()).containsOnly(SourceOrder.snapshotRow(1L));
    }

    @Test
    void readsOnlyTheTablesTheRecordDoesNotShowAsWritten() {
        // The seam was recorded under generation 1 and the ring running now is a rebuild -- generation 2.
        // One of the two selected tables is recorded as written; the other is not.
        SrsMeta interrupted = new SrsMeta("chain", null, List.of(), "binlog.000042:1024", List.of(), null,
                List.of("orders"), 2L, 1L);
        FakePort port = new FakePort(Map.of(
                "orders", new FakeBatch(List.of(row("orders", 1)), "binlog.000042:1024"),
                "customers", new FakeBatch(List.of(row("customers", 1)), "binlog.000042:1024")));
        RecordingMeta meta = new RecordingMeta(new ArrayList<>(), interrupted);
        List<Envelope> sink = new ArrayList<>();

        long count = SnapshotPhase.run(port, multiTableConfig(), "chain",
                List.of("orders", "customers"), 2L, meta, sink::add);

        // Not redoing the load is the whole of what resuming means: a table already written is not read
        // again. What the one still owed emits is pinned to the generation the snapshot began in rather
        // than the one running now -- a higher generation beats every change already applied to those rows
        // and rolls each of them back to its snapshot value, silently.
        assertThat(port.asked).containsExactly(List.of("customers"));
        assertThat(count).isEqualTo(1);
        assertThat(meta.pinnedEpoch).isEqualTo(1L);
        assertThat(sink).extracting(event -> event.position().order())
                .containsOnly(SourceOrder.snapshotRow(1L));
    }

    @Test
    void doesNotMarkTheSnapshotCompleteWhenTheDrainFailsPartway() {
        List<Envelope> drained = new ArrayList<>();
        Consumer<Envelope> failingOnSecond = e -> {
            if (!drained.isEmpty()) {
                throw new IllegalStateException("sink down");
            }
            drained.add(e);
        };
        RecordingMeta meta = new RecordingMeta(new ArrayList<>());

        assertThatThrownBy(() -> SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1), row(2)), "p0")), config(), "chain", List.of("orders"),
                1L, meta, failingOnSecond))
                .isInstanceOf(IllegalStateException.class);

        // The cdc-start position, already written, is what a run that got half way leaves behind -- and it
        // is what the tail of the next attempt joins at, so the span this aborted read covered is replayed
        // rather than skipped.
        assertThat(meta.completed).isEmpty();
        assertThat(meta.cdcStart).isEqualTo("p0");
    }

    @Test
    void closesTheSnapshotBatchEvenWhenTheSinkThrows() {
        FakeBatch batch = new FakeBatch(List.of(row(1)), "p0");
        Consumer<Envelope> failing = e -> {
            throw new IllegalStateException("sink down");
        };

        assertThatThrownBy(() -> SnapshotPhase.run(
                new FakePort(batch), config(), "chain", List.of("orders"),
                1L, new RecordingMeta(new ArrayList<>()), failing))
                .isInstanceOf(IllegalStateException.class);

        // try-with-resources releases the source even when the drain fails partway.
        assertThat(batch.closed).isTrue();
    }

    @Test
    void rejectsANullSinkBeforeTouchingTheStore() {
        List<String> trace = new ArrayList<>();

        assertThatThrownBy(() -> SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(), "p0")), config(), "chain", List.of("orders"),
                1L, new RecordingMeta(trace), null))
                .isInstanceOf(NullPointerException.class);

        // Args are validated up front: a null sink fails fast without first recording the cdc-start position.
        assertThat(trace).isEmpty();
    }

    /** A bounded snapshot batch over a fixed list of events; records whether it was closed. */
    private static final class FakeBatch implements CaptureBatch {
        private final Iterator<Envelope> events;
        private final String seam;
        boolean closed;

        /** A batch that reports the seam its source sampled before the read -- the usual case. */
        FakeBatch(List<Envelope> events, String seam) {
            this.events = events.iterator();
            this.seam = seam;
        }

        /** A batch whose source named no seam, so the tail has nothing to join to. */
        FakeBatch(List<Envelope> events) {
            this(events, null);
        }

        @Override
        public boolean hasNext() {
            return events.hasNext();
        }

        @Override
        public Envelope next() {
            return events.next();
        }

        @Override
        public Optional<SourcePosition> seam() {
            return Optional.ofNullable(seam).map(SourcePosition::new);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /**
     * A capture port that yields a snapshot batch per read, and records the stream selection of every read
     * it was asked for. Either one fixed batch whatever the selection, or one batch per stream name; the
     * streaming and discovery reads are unused here.
     */
    private static final class FakePort implements CapturePort {
        private final FakeBatch batch;
        private final Map<String, FakeBatch> byTable;
        final List<List<String>> asked = new ArrayList<>();

        FakePort(FakeBatch batch) {
            this.batch = batch;
            this.byTable = null;
        }

        FakePort(Map<String, FakeBatch> byTable) {
            this.batch = null;
            this.byTable = byTable;
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            asked.add(config.streams());
            return byTable == null ? batch : byTable.get(config.streams().getFirst());
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectionReport testConnection(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DiscoveredSchema discoverSchema(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A meta store that records the two marks the snapshot phase writes -- the cdc-start position and the
     * per-table completion -- in call order; the other facets are unused in the snapshot phase.
     */
    private static final class RecordingMeta implements SrsMetaStore {
        @Override
        public java.util.List<String> miningChainIdsWithConsumer(String pipelineId) {
            throw new UnsupportedOperationException("consumer detachment is not exercised by this double");
        }

        @Override
        public void dropChain(String miningChainId) {
            throw new UnsupportedOperationException(
                    "chain removal is not exercised by this double");
        }

        @Override
        public void detachConsumer(String miningChainId, String pipelineId) {
            throw new UnsupportedOperationException("consumer detachment is not exercised by this double");
        }

        private final List<String> trace;
        private final SrsMeta stored;
        final List<String> completed = new ArrayList<>();
        String cdcStart;
        long pinnedEpoch;

        RecordingMeta(List<String> trace) {
            this(trace, new SrsMeta("chain", null, List.of(), null, List.of(), null));
        }

        RecordingMeta(List<String> trace, SrsMeta stored) {
            this.trace = trace;
            this.stored = stored;
        }

        @Override
        public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
            this.cdcStart = cdcStartPosition;
            this.pinnedEpoch = snapshotEpoch;
            trace.add("cdc-start");
        }

        @Override
        public long openEpoch(String miningChainId) {
            // A snapshot reads the generation it runs under; opening one is the coordinator's job.
            throw new UnsupportedOperationException();
        }

        @Override
        public void markSnapshotComplete(String miningChainId, String table) {
            completed.add(miningChainId + "/" + table);
            trace.add("snapshot-complete");
        }

        @Override
        public Optional<SrsMeta> read(String miningChainId) {
            return Optional.of(stored);
        }

        @Override
        public void create(String miningChainId, String retention) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            throw new UnsupportedOperationException();
        }
    }
}
