package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/**
 * A sink writes a stream's rows in batches that close on count or on time: at most {@code max_records} rows,
 * gone as soon as they can be written when the wait is nothing, gone once the oldest has waited {@code
 * max_wait} otherwise - and never by sleeping. A batch holds one stream's rows, the streams take turns, and at
 * most one batch's worth waits in the sink across all of them.
 *
 * <p>A bound, and word that a chain got past positions with nothing to deliver, each wait for the rows taken in
 * before them to land - and only those: they neither cut a batch short, which would leave a wait that never
 * elapses in a pipeline whose source bounds every read, nor wait on rows taken in after them.
 */
class ASinkBatchesByCountOrByTimeTest {

    private static final ChainAxes AXES = ChainAxes.assign(List.of("orders", "lines"));
    private static final long WAIT_MILLIS = 50;

    private final AtomicLong nanos = new AtomicLong();

    @Test
    void withNoWaitEveryRowAlreadyThereGoesInOneBatch() throws Exception {
        RecordingWriter writer = new RecordingWriter();
        SinkProcessor sink = sink(writer, null, null, 1024, 0);

        sink.process(0, inbox(row("orders", 1), row("orders", 2), row("orders", 3)));

        assertThat(writer.batches).as("one call for the rows that were there, not one per row")
                .containsExactly(List.of(1, 2, 3));
    }

    @Test
    void withNoWaitTheRowsArrivingDuringAWriteGoTogetherAfterIt() throws Exception {
        ManualWriter writer = new ManualWriter();
        SinkProcessor sink = sink(writer, null, null, 1024, 0);

        sink.process(0, inbox(row("orders", 1)));
        sink.process(0, inbox(row("orders", 2), row("orders", 3), row("orders", 4)));
        assertThat(writer.batches).as("one write out at a time").containsExactly(List.of(1));

        writer.completeAll();
        sink.tryProcess();

        assertThat(writer.batches).containsExactly(List.of(1), List.of(2, 3, 4));
    }

    @Test
    void withAWaitTheRowsGoOnceTheOldestHasWaitedItOut() throws Exception {
        RecordingWriter writer = new RecordingWriter();
        SinkProcessor sink = sink(writer, null, null, 1024, WAIT_MILLIS);

        sink.process(0, inbox(row("orders", 1), row("orders", 2)));
        elapse(WAIT_MILLIS - 1);
        sink.tryProcess();
        assertThat(writer.batches).as("before the wait is up").isEmpty();

        elapse(1);
        sink.tryProcess();
        assertThat(writer.batches).containsExactly(List.of(1, 2));
    }

    @Test
    void aFullBatchGoesWithoutWaiting() throws Exception {
        RecordingWriter writer = new RecordingWriter();
        SinkProcessor sink = sink(writer, null, null, 3, WAIT_MILLIS);

        sink.process(0, inbox(row("orders", 1), row("orders", 2), row("orders", 3), row("orders", 4)));
        assertThat(writer.batches).as("full at once; the fourth waits").containsExactly(List.of(1, 2, 3));

        elapse(WAIT_MILLIS);
        sink.tryProcess();
        assertThat(writer.batches).containsExactly(List.of(1, 2, 3), List.of(4));
    }

    @Test
    void theEndOfInputSendsWhateverIsWaiting() throws Exception {
        RecordingWriter writer = new RecordingWriter();
        SinkProcessor sink = sink(writer, null, null, 1024, WAIT_MILLIS);

        sink.process(0, inbox(row("orders", 1)));

        assertThat(sink.complete()).as("done once the last rows are written, not before").isTrue();
        assertThat(writer.batches).containsExactly(List.of(1));
    }

    @Test
    void aWaitIsNeverSpentAsleep() throws Exception {
        RecordingWriter writer = new RecordingWriter();
        SinkProcessor sink = new SinkProcessor(writer, null, null, 1, 1024, FrontierGauge.none(),
                DeliveryGauge.none(), System::currentTimeMillis, null, true, null, TimeUnit.SECONDS.toMillis(30),
                System::nanoTime);
        sink.init(new TestOutbox(new int[] {}, 128), new TestProcessorContext());

        long started = System.nanoTime();
        sink.process(0, inbox(row("orders", 1)));
        sink.tryProcess();
        long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        assertThat(tookMillis).as("a thirty-second wait held without holding the thread").isLessThan(5_000);
        assertThat(writer.batches).isEmpty();
    }

    @Test
    void aBatchHoldsOneStreamAndTheStreamsTakeTurns() throws Exception {
        ManualWriter writer = new ManualWriter();
        SinkProcessor sink = sink(writer, null, null, 4, 0);

        sink.process(0, inbox(row("hot", 1), row("hot", 2), row("cold", 1), row("hot", 3)));
        sink.process(0, inbox(row("hot", 4), row("hot", 5)));
        writer.completeAll();
        sink.tryProcess();
        writer.completeAll();
        sink.tryProcess();

        assertThat(writer.streams)
                .as("each batch one stream's rows; the quiet stream's turn comes before the busy one's next")
                .containsExactly("hot", "cold", "hot");
        assertThat(writer.batches).containsExactly(List.of(1, 2, 3), List.of(1), List.of(4, 5));
    }

    @Test
    void atMostOneBatchWaitsInTheSinkAndTheRestStaysUpstream() throws Exception {
        ManualWriter writer = new ManualWriter();
        SinkProcessor sink = sink(writer, null, null, 3, 0);
        TestInbox inbox = inbox();
        for (int id = 1; id <= 10; id++) {
            inbox.add(row(id % 2 == 0 ? "orders" : "lines", id));
        }

        sink.process(0, inbox);

        assertThat(writer.batches).as("the one write out").hasSize(1);
        assertThat(writer.batches.get(0).size() + inbox.size())
                .as("rows being written and rows left upstream: all but at most one batch's worth")
                .isGreaterThanOrEqualTo(10 - 3);
    }

    @Test
    void aBoundWaitsForTheRowsBeforeItAndForNoOthers() throws Exception {
        ManualWriter writer = new ManualWriter();
        RecordingAck ack = new RecordingAck();
        SinkProcessor sink = sink(writer, ack, new ContiguousPrefix(AXES), 1024, WAIT_MILLIS);

        sink.process(0, inbox(at("orders", 1)));
        sink.tryProcessWatermark(boundAt("orders", 1));
        assertThat(writer.batches).as("a bound does not cut the wait short").isEmpty();

        elapse(WAIT_MILLIS / 2);
        sink.process(0, inbox(at("lines", 1)));
        elapse(WAIT_MILLIS / 2);
        sink.tryProcess();
        assertThat(ack.calls).as("the row before the bound is still being written").isEmpty();

        writer.completeAll();
        sink.tryProcess();

        assertThat(ack.calls).as("landed as soon as the row before it did, while a later row still waits")
                .containsExactly("orders=o1");
        assertThat(writer.batches).containsExactly(List.of(1));
    }

    @Test
    void wordThatAChainGotPastPositionsWaitsForTheRowsBeforeIt() throws Exception {
        ManualWriter writer = new ManualWriter();
        RecordingAck ack = new RecordingAck();
        SinkProcessor sink = sink(writer, ack, new ContiguousPrefix(AXES), 1024, WAIT_MILLIS);

        sink.process(0, inbox(at("orders", 1),
                new SettledPositions(Map.of("orders", new ChainPosition(new SourceOrder(1, 2), "o2")))));
        elapse(WAIT_MILLIS);
        sink.tryProcess();
        assertThat(ack.calls).as("the row before the word is still being written").isEmpty();

        writer.completeAll();
        sink.tryProcess();

        assertThat(ack.calls).as("the word closes the row before it once that row has landed")
                .containsExactly("orders=o1");
    }

    @Test
    void aMoveReservesItsPlaceWhileThePreviousBatchIsStillWriting() throws Exception {
        var writer = new MoveWriter();
        SinkProcessor sink = sink(writer, null, null, 32, WAIT_MILLIS);
        var input = inbox(row("orders", 1), row("orders", 2), row("orders", 3),
                row("orders", 4), row("orders", 5));
        sink.process(0, input);
        assertThat(writer.notices).containsExactly("queued");
        assertThat(writer.batches).containsExactly(List.of(1, 2));

        writer.completeAll();
        sink.tryProcess();
        sink.process(0, input);
        assertThat(writer.batches).containsExactly(List.of(1, 2), List.of(3));
        assertThat(writer.notices).containsExactly("queued");

        writer.completeAll();
        elapse(WAIT_MILLIS);
        sink.tryProcess();
        assertThat(writer.notices).containsExactly("queued", "written");
        assertThat(writer.batches).containsExactly(List.of(1, 2), List.of(3), List.of(4, 5));
    }

    @Test
    void aReservedEffectSharesItsBatchOnlyWithProvenIndependentRows() throws Exception {
        var writer = new IndependentWriter();
        SinkProcessor sink = sink(writer, null, null, 32, WAIT_MILLIS);
        var input = inbox(row("orders", 1), row("orders", 3), row("orders", 2),
                row("orders", 3), row("orders", 4));
        sink.process(0, input);
        assertThat(writer.batches).containsExactly(List.of(1, 3, 2));
        assertThat(writer.notices).containsExactly("queued", "queued");
        writer.pending.getFirst().complete(new WriteResult(3));
        sink.tryProcess();
        assertThat(writer.batches).containsExactly(List.of(1, 3, 2), List.of(3, 4));
        assertThat(writer.notices).containsExactly("queued", "queued", "written");
        writer.pending.getLast().complete(new WriteResult(1));
        assertThatThrownBy(sink::tryProcess).isInstanceOf(RuntimeException.class);
        assertThat(writer.notices).containsExactly("queued", "queued", "written");
    }

    @Test
    void twoOrdinaryWritesAcrossAReservationCannotBeReorderedInOneBulk() throws Exception {
        var revisions = new HashMap<Object, Object>();
        var writer = new IndependentWriter() {
            @Override
            public CompletionStage<WriteResult> write(List<Envelope> rows) {
                return super.write(rows).thenApply(result -> {
                    for (Envelope row : rows.reversed()) {
                        revisions.put(row.after().get("id"), row.after().get("revision"));
                    }
                    return result;
                });
            }
        };
        SinkProcessor sink = sink(writer, null, null, 32, 0);
        var before = Envelope.insert(1, "orders", Map.of("id", 2, "revision", 1), null);
        var after = Envelope.insert(3, "orders", Map.of("id", 2, "revision", 2), null);
        sink.process(0, inbox(before, row("orders", 3), after));
        writer.pending.getFirst().complete(new WriteResult(writer.batches.getFirst().size()));
        sink.tryProcess();
        writer.completeAll();
        sink.tryProcess();
        assertThat(revisions.get(2)).isEqualTo(2);
        assertThat(writer.batches).containsExactly(List.of(2, 3), List.of(2));
    }

    private static class IndependentWriter extends MoveWriter {
        @Override
        public Predicate<Envelope> batchAdmission(Envelope reserved) {
            var keys = new HashSet<Object>();
            keys.add(reserved.after().get("id"));
            return other -> keys.add(other.after().get("id"));
        }
    }

    @Test
    void aFailedOrUnconfirmedEffectDoesNotReleaseAWriteReceipt() throws Exception {
        for (boolean failed : List.of(true, false)) {
            var writer = new MoveWriter();
            SinkProcessor sink = sink(writer, null, null, 32, 0);
            sink.process(0, inbox(row("orders", 3), row("orders", 4)));
            if (failed) {
                writer.pending.getFirst().completeExceptionally(new IllegalStateException("disconnected"));
            } else {
                writer.pending.getFirst().complete(new WriteResult(0));
            }
            if (failed) {
                assertThatThrownBy(sink::tryProcess).hasMessage("disconnected");
            } else {
                assertThatThrownBy(sink::tryProcess).isInstanceOfSatisfying(
                        TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(EngineError.KEY_MOVE_WRITE_UNCONFIRMED));
            }
            assertThat(writer.notices).containsExactly("queued");
            assertThat(writer.batches).containsExactly(List.of(3));
        }
    }

    private static class MoveWriter extends ManualWriter implements KeyMoveSinkWriter {
        final List<String> notices = new ArrayList<>();

        @Override
        public KeyMoveHandoffs.Receipt receiptFor(Envelope row) {
            if (!Integer.valueOf(3).equals(row.after().get("id"))) return null;
            return new KeyMoveHandoffs.Receipt() {
                public void queued() { notices.add("queued"); }
                public void written() { notices.add("written"); }
            };
        }
    }

    private SinkProcessor sink(SinkWriter writer, SinkAck ack, SinkFrontier frontier, int maxRecords,
            long maxWaitMillis) throws Exception {
        SinkProcessor sink = new SinkProcessor(writer, ack, frontier, 1, maxRecords, FrontierGauge.none(),
                DeliveryGauge.none(), System::currentTimeMillis, null, true, null, maxWaitMillis, nanos::get);
        sink.init(new TestOutbox(new int[] {}, 128), new TestProcessorContext());
        return sink;
    }

    private void elapse(long millis) {
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
    }

    private static TestInbox inbox(Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(items));
        return inbox;
    }

    private static Envelope row(String stream, int id) {
        return Envelope.insert(id, stream, Map.of("id", id), null);
    }

    /** A row of {@code chain}'s own stream at position {@code seq}, whose token spells the chain's initial. */
    private static Envelope at(String chain, int seq) {
        return Envelope.insert(seq, chain, Map.of("id", seq), null)
                .withPosition(new ChainPosition(new SourceOrder(1, seq), chain.charAt(0) + String.valueOf(seq)));
    }

    private static Watermark boundAt(String chain, int seq) {
        return new Watermark(FrontierOrders.pack(chain, new SourceOrder(1, seq)), AXES.axisOf(chain));
    }

    /** Records the ids of each batch it is handed, and settles at once. */
    private static final class RecordingWriter implements SinkWriter {
        private final List<List<Object>> batches = new ArrayList<>();

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            batches.add(records.stream().map(record -> record.after().get("id")).toList());
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }

    /** Records each batch's ids and stream, and settles only when the test says so. */
    private static class ManualWriter implements SinkWriter {
        final List<List<Object>> batches = new ArrayList<>();
        private final List<String> streams = new ArrayList<>();
        final List<CompletableFuture<WriteResult>> pending = new ArrayList<>();

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            batches.add(records.stream().map(record -> record.after().get("id")).toList());
            streams.add(records.get(0).src());
            CompletableFuture<WriteResult> future = new CompletableFuture<>();
            pending.add(future);
            return future;
        }

        void completeAll() {
            pending.forEach(future -> future.complete(new WriteResult(1)));
        }

        @Override
        public void close() {
        }
    }

    /** Records every position the sink advances a chain to, as {@code chain=token}. */
    private static final class RecordingAck implements SinkAck {
        private final List<String> calls = new ArrayList<>();

        @Override
        public void advance(String chain, ChainPosition position) {
            calls.add(chain + "=" + position.token());
        }
    }
}
