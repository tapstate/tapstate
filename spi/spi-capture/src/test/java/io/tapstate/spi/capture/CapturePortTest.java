package io.tapstate.spi.capture;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The capture port seen through a stub implementation: proves the contract is implementable and
 * usable, and pins the read-side boundary (snapshot yields {@code r}; cdc yields row/ddl mutations)
 * plus the close semantics the port documents (idempotent, may close before draining).
 *
 * <p>It also pins the position surface: a cdc stream is told where to begin, and a snapshot batch hands
 * out the seam its change tail has to resume from. Both used to sit outside these signatures, which left
 * the caller synthesizing a position the source had never reported.
 */
class CapturePortTest {

    private static final CaptureConfig CONFIG = new CaptureConfig("mysql", Map.of(), List.of("orders"));

    @Test
    void snapshotYieldsACloseableIteratorOfReadEnvelopes() {
        StubCapture capture = new StubCapture();

        List<Op> ops = new ArrayList<>();
        try (CaptureBatch batch = capture.snapshot(CONFIG)) {
            while (batch.hasNext()) {
                ops.add(batch.next().op());
            }
        }

        assertThat(ops).containsExactly(Op.READ, Op.READ);
        assertThat(capture.lastBatch.closed).isTrue();
    }

    @Test
    void snapshotBatchCloseIsIdempotent() {
        StubCapture capture = new StubCapture();

        CaptureBatch batch = capture.snapshot(CONFIG);
        batch.close();
        batch.close();

        // the underlying source is released once, not once per close() call
        assertThat(capture.lastBatch.releaseRuns).isEqualTo(1);
    }

    @Test
    void snapshotBatchMayBeClosedBeforeItIsDrained() {
        StubCapture capture = new StubCapture();

        CaptureBatch batch = capture.snapshot(CONFIG);
        batch.close(); // closed while elements are still pending

        assertThat(capture.lastBatch.closed).isTrue();
        assertThat(capture.lastBatch.releaseRuns).isEqualTo(1);
    }

    @Test
    void snapshotHandsOutTheSeamItsChangeTailResumesFrom() {
        StubCapture capture = new StubCapture();

        try (CaptureBatch batch = capture.snapshot(CONFIG)) {
            // Sampled when the batch opened, before its first row was read: a change made while the
            // snapshot runs falls after this and is re-delivered rather than missed.
            assertThat(batch.seam()).contains(new SourcePosition("binlog.000042:1000"));
        }
    }

    @Test
    void aSourceWithNoChangeStreamReportsNoSeam() {
        try (CaptureBatch batch = new SnapshotOnlyCapture().snapshot(CONFIG)) {
            assertThat(batch.seam()).isEmpty();
        }
    }

    @Test
    void cdcDeliversMutationEventsToTheListenerAndReturnsAClosableSubscription() {
        StubCapture capture = new StubCapture();
        List<Op> delivered = new ArrayList<>();

        Subscription subscription =
                capture.cdc(CONFIG, CaptureStart.present(), event -> delivered.add(event.op()));

        assertThat(delivered).containsExactly(Op.INSERT, Op.UPDATE, Op.DELETE, Op.DDL);

        subscription.close();
        subscription.close();
        // idempotent: two close() calls stop the stream once
        assertThat(capture.lastSubscription.stopRuns).isEqualTo(1);
    }

    @Test
    void cdcIsToldWhereToBeginAndTheTwoStartsReachThePortAsThemselves() {
        StubCapture capture = new StubCapture();

        capture.cdc(CONFIG, CaptureStart.resume(new SourcePosition("gtid:9-17")), event -> { });
        assertThat(capture.lastStart).isEqualTo(CaptureStart.resume(new SourcePosition("gtid:9-17")));

        capture.cdc(CONFIG, CaptureStart.present(), event -> { });
        assertThat(capture.lastStart)
                .as("beginning at the present is what the caller asked for, not what an omitted "
                        + "position decayed into inside the port")
                .isEqualTo(CaptureStart.present());
    }

    @Test
    void testConnectionReturnsSchemaAndSample() {
        ConnectionReport report = new StubCapture().testConnection(CONFIG);

        assertThat(report.schema().tables()).extracting(TableSchema::name).containsExactly("orders");
        assertThat(report.sample()).singleElement().satisfies(e -> assertThat(e.op()).isEqualTo(Op.READ));
    }

    @Test
    void discoverSchemaReturnsTheTableModel() {
        DiscoveredSchema schema = new StubCapture().discoverSchema(CONFIG);

        assertThat(schema.tables()).extracting(TableSchema::name).containsExactly("orders");
        assertThat(schema.tables().get(0).fields()).extracting(FieldSchema::name).containsExactly("id");
    }

    @Test
    void captureListenerIsAFunctionalInterface() {
        List<Envelope> collected = new ArrayList<>();
        CaptureListener listener = collected::add;

        listener.onEvent(Envelope.insert(1L, "orders", Map.of("id", 1), null));

        assertThat(collected).hasSize(1);
    }

    private static final DiscoveredSchema SCHEMA =
            new DiscoveredSchema(List.of(new TableSchema("orders", List.of(new FieldSchema("id", "long")))));

    /** A minimal in-memory capture, enough to exercise the four port methods. */
    private static final class StubCapture implements CapturePort {

        RecordingBatch lastBatch;
        RecordingSubscription lastSubscription;
        CaptureStart lastStart;

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            lastBatch = new RecordingBatch(List.of(
                    Envelope.read(1L, "orders", Map.of("id", 1), null),
                    Envelope.read(2L, "orders", Map.of("id", 2), null)).iterator(),
                    Optional.of(new SourcePosition("binlog.000042:1000")));
            return lastBatch;
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            lastStart = start;
            listener.onEvent(Envelope.insert(10L, "orders", Map.of("id", 1), null));
            listener.onEvent(Envelope.update(11L, "orders", Map.of("id", 1), Map.of("id", 1, "n", 2), null));
            listener.onEvent(Envelope.delete(12L, "orders", Map.of("id", 1), null));
            listener.onEvent(Envelope.ddl(13L, "orders", Map.of("added", "n")));
            lastSubscription = new RecordingSubscription();
            return lastSubscription;
        }

        @Override
        public ConnectionReport testConnection(CaptureConfig config) {
            return new ConnectionReport(SCHEMA, List.of(Envelope.read(1L, "orders", Map.of("id", 1), null)));
        }

        @Override
        public DiscoveredSchema discoverSchema(CaptureConfig config) {
            return SCHEMA;
        }
    }

    /** A source with no change stream at all: it reads rows and there is nothing to stitch a tail to. */
    private static final class SnapshotOnlyCapture implements CapturePort {

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            return new RecordingBatch(
                    List.of(Envelope.read(1L, "orders", Map.of("id", 1), null)).iterator(), Optional.empty());
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            throw new UnsupportedOperationException("this source has no change stream");
        }

        @Override
        public ConnectionReport testConnection(CaptureConfig config) {
            return new ConnectionReport(SCHEMA, List.of());
        }

        @Override
        public DiscoveredSchema discoverSchema(CaptureConfig config) {
            return SCHEMA;
        }
    }

    /** Models an idempotent close: the underlying source is released at most once. */
    private static final class RecordingBatch implements CaptureBatch {
        private final Iterator<Envelope> delegate;
        private final Optional<SourcePosition> seam;
        boolean closed;
        int releaseRuns;

        RecordingBatch(Iterator<Envelope> delegate, Optional<SourcePosition> seam) {
            this.delegate = delegate;
            this.seam = seam;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public Envelope next() {
            return delegate.next();
        }

        @Override
        public Optional<SourcePosition> seam() {
            return seam;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            releaseRuns++;
        }
    }

    /** Models an idempotent close: the stream is stopped at most once. */
    private static final class RecordingSubscription implements Subscription {
        boolean stopped;
        int stopRuns;

        @Override
        public void close() {
            if (stopped) {
                return;
            }
            stopped = true;
            stopRuns++;
        }
    }
}
