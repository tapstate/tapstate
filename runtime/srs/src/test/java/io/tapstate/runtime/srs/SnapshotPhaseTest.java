package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
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

    private static Envelope row(int id) {
        return Envelope.read(id, "orders", Map.of("id", id), Map.of());
    }

    @Test
    void drainsTheSnapshotBatchStraightToTheSinkInOrder() {
        List<Envelope> rows = List.of(row(1), row(2), row(3));
        FakePort port = new FakePort(new FakeBatch(rows));
        List<Envelope> sink = new ArrayList<>();

        long count = SnapshotPhase.run(
                port, config(), "chain", "orders", new SourcePosition("p0"),
                1L, new RecordingMeta(new ArrayList<>()), sink::add);

        // Straight through in batch order, each row stamped with the generation and otherwise untouched.
        assertThat(sink).containsExactlyElementsOf(
                rows.stream().map(r -> r.withOrder(SourceOrder.snapshotRow(1L))).toList());
        assertThat(count).isEqualTo(3);
    }

    @Test
    void drainPassesTheSnapshotBatchStraightToTheSinkWithoutRecordingACdcStart() {
        List<Envelope> rows = List.of(row(1), row(2), row(3));
        FakePort port = new FakePort(new FakeBatch(rows));
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
        FakeBatch batch = new FakeBatch(List.of(row(1)));

        SnapshotPhase.drain(new FakePort(batch), config(), e -> {});

        assertThat(batch.closed).isTrue();
    }

    @Test
    void closesTheSnapshotBatchAfterDraining() {
        FakeBatch batch = new FakeBatch(List.of(row(1)));

        SnapshotPhase.run(
                new FakePort(batch), config(), "chain", "orders", new SourcePosition("p0"),
                1L, new RecordingMeta(new ArrayList<>()), e -> {});

        assertThat(batch.closed).isTrue();
    }

    @Test
    void recordsTheCdcStartPositionBeforeDrainingSoTheCdcTailMissesNoChange() {
        List<String> trace = new ArrayList<>();
        RecordingMeta meta = new RecordingMeta(trace);
        Consumer<Envelope> sink = e -> trace.add("event");

        SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1), row(2)))), config(), "chain", "orders",
                new SourcePosition("binlog.000042:1024"), 1L, meta, sink);

        // The cdc-start position is the source log position sampled at snapshot start: recorded before the
        // snapshot drains, so the cdc tail resumes from before the snapshot and the idempotent sink absorbs
        // the overlap -- a change made during the snapshot is never missed.
        assertThat(meta.cdcStart).isEqualTo("binlog.000042:1024");
        assertThat(trace).startsWith("cdc-start", "event", "event");
    }

    @Test
    void stampsEverySnapshotRowWithTheGenerationTheSnapshotBeganIn() {
        List<Envelope> sink = new ArrayList<>();

        SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1), row(2)))), config(), "chain", "orders",
                new SourcePosition("p0"), 4L, new RecordingMeta(new ArrayList<>()), sink::add);

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
                new FakePort(new FakeBatch(List.of(row(1)))), config(), "chain", "orders",
                new SourcePosition("binlog.000042:1024"), 4L, meta, e -> { });

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
                new FakePort(new FakeBatch(List.of(row(1)))), config(), "chain", "orders",
                new SourcePosition("binlog.000042:1024"), 2L, meta, sink::add);

        // This is the whole reason the two generations are kept apart. Rows of a rerun that took the
        // current generation would beat every change generation 1 already applied and roll each of those
        // rows back to its snapshot value -- silently, and only while the snapshot is running.
        assertThat(sink).extracting(event -> event.position().order()).containsOnly(SourceOrder.snapshotRow(1L));
        assertThat(meta.pinnedEpoch).isEqualTo(1L);
    }

    @Test
    void aSnapshotOfATableThatAlreadyDrainedTakesTheGenerationRunningNow() {
        // Same recorded seam, but this table drained: whatever runs now is a new snapshot -- a re-mine --
        // and it is a new baseline of truth, so it is entitled to beat what came before.
        SrsMeta drained = new SrsMeta("chain", null, List.of(), "binlog.000042:1024", List.of(), null,
                List.of("orders"), 2L, 1L);
        RecordingMeta meta = new RecordingMeta(new ArrayList<>(), drained);
        List<Envelope> sink = new ArrayList<>();

        SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1)))), config(), "chain", "orders",
                new SourcePosition("binlog.000099:1"), 2L, meta, sink::add);

        assertThat(sink).extracting(event -> event.position().order()).containsOnly(SourceOrder.snapshotRow(2L));
        assertThat(meta.pinnedEpoch).isEqualTo(2L);
    }

    @Test
    void aChainThatRecordedNoSeamHasNoSnapshotToResumeAndTakesTheGenerationRunningNow() {
        SrsMeta neverSnapshotted = new SrsMeta("chain", null, List.of(), null, List.of(), null,
                List.of(), 2L, 0L);
        RecordingMeta meta = new RecordingMeta(new ArrayList<>(), neverSnapshotted);
        List<Envelope> sink = new ArrayList<>();

        SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1)))), config(), "chain", "orders",
                new SourcePosition("p0"), 2L, meta, sink::add);

        assertThat(sink).extracting(event -> event.position().order()).containsOnly(SourceOrder.snapshotRow(2L));
    }

    @Test
    void marksTheTableSnapshotCompleteOnlyAfterTheDrainHasFinished() {
        List<String> trace = new ArrayList<>();
        RecordingMeta meta = new RecordingMeta(trace);
        Consumer<Envelope> sink = e -> trace.add("event");

        SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of(row(1), row(2)))), config(), "chain", "orders",
                new SourcePosition("binlog.000042:1024"), 1L, meta, sink);

        // The two marks answer different questions and must not be conflated: the cdc-start position is
        // written before the drain (it means the snapshot has started, and must precede it or a change made
        // during the snapshot would be missed), while the completion mark is written after the drain
        // returns. A reader asking "has this table's snapshot finished?" can only be answered by the
        // second: the presence of a cdc-start position means started, never finished.
        assertThat(trace).containsExactly("cdc-start", "event", "event", "snapshot-complete");
        assertThat(meta.completed).containsExactly("chain/orders");
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
                new FakePort(new FakeBatch(List.of(row(1), row(2)))), config(), "chain", "orders",
                new SourcePosition("p0"), 1L, meta, failingOnSecond))
                .isInstanceOf(IllegalStateException.class);

        // An aborted snapshot is not a completed one. The mark is what downstream reads as "every row of
        // this table has been through", so marking a partial drain would assert the table is exhausted
        // when rows are still missing -- and the cdc-start position, already written, would be the only
        // trace left of a run that got half way.
        assertThat(meta.completed).isEmpty();
        assertThat(meta.cdcStart).isEqualTo("p0");
    }

    @Test
    void closesTheSnapshotBatchEvenWhenTheSinkThrows() {
        FakeBatch batch = new FakeBatch(List.of(row(1)));
        Consumer<Envelope> failing = e -> {
            throw new IllegalStateException("sink down");
        };

        assertThatThrownBy(() -> SnapshotPhase.run(
                new FakePort(batch), config(), "chain", "orders", new SourcePosition("p0"),
                1L, new RecordingMeta(new ArrayList<>()), failing))
                .isInstanceOf(IllegalStateException.class);

        // try-with-resources releases the source even when the drain fails partway.
        assertThat(batch.closed).isTrue();
    }

    @Test
    void rejectsANullSinkBeforeTouchingTheStore() {
        List<String> trace = new ArrayList<>();

        assertThatThrownBy(() -> SnapshotPhase.run(
                new FakePort(new FakeBatch(List.of())), config(), "chain", "orders",
                new SourcePosition("p0"), 1L, new RecordingMeta(trace), null))
                .isInstanceOf(NullPointerException.class);

        // Args are validated up front: a null sink fails fast without first recording the cdc-start position.
        assertThat(trace).isEmpty();
    }

    /** A bounded snapshot batch over a fixed list of events; records whether it was closed. */
    private static final class FakeBatch implements CaptureBatch {
        private final Iterator<Envelope> events;
        boolean closed;

        FakeBatch(List<Envelope> events) {
            this.events = events.iterator();
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
        public void close() {
            closed = true;
        }
    }

    /** A capture port that yields one fixed snapshot batch; the streaming and discovery reads are unused here. */
    private static final class FakePort implements CapturePort {
        private final FakeBatch batch;

        FakePort(FakeBatch batch) {
            this.batch = batch;
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            return batch;
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureListener listener) {
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
        public void advanceSourceReadOffset(String miningChainId, String sourceReadOffset) {
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
