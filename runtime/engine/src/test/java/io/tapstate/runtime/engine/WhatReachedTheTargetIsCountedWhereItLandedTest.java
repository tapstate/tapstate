package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

/**
 * Where rows delivered to a target are counted, and where they are not. The boundary is the whole of what
 * these cover: the same rows counted when the writer is handed them and counted when the write is known to
 * have succeeded give different numbers, and both readings look equally plausible on a healthy run. They
 * come apart exactly when somebody is looking — while writes are piling up unacknowledged, or after one
 * failed — which is why the boundary is written down rather than left to whoever wires the counter.
 *
 * <p>The recency reading is here for the same reason. What the sink reports is the newest event time it
 * has settled, never how far behind that is: the distance keeps growing while nothing arrives, so a
 * distance worked out here would be frozen at the last settle and would read as healthy for exactly as
 * long as a pipeline stayed stalled.
 */
class WhatReachedTheTargetIsCountedWhereItLandedTest {

    @Test
    void counts_nothing_until_the_write_settles() throws Exception {
        RecordingDelivery delivery = new RecordingDelivery();
        ManualWriter writer = new ManualWriter();
        SinkProcessor processor = init(writer, delivery);

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(row("orders", 1L), row("orders", 2L)));
        processor.process(0, inbox);

        // Handed to the writer and not yet written. A count taken here runs ahead of the target by
        // whatever is in flight, and the write can still fail.
        assertThat(delivery.rows).isEmpty();

        writer.completeAll();
        drain(processor);

        assertThat(delivery.latestRows()).isEqualTo(Map.of("orders", Map.of("i", 2L)));
    }

    @Test
    void counts_nothing_for_a_write_that_failed() throws Exception {
        RecordingDelivery delivery = new RecordingDelivery();
        RuntimeException boom = new IllegalStateException("target refused the batch");
        SinkProcessor processor = init(new FailingWriter(boom), delivery);

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(row("orders", 1L)));
        processor.process(0, inbox);

        assertThatThrownBy(processor::complete).isSameAs(boom);

        // The rows never reached the target, so nothing counted them. Counting on hand-off would have
        // reported them delivered, and a pipeline whose every write fails would read as one moving data.
        assertThat(delivery.rows).isEmpty();
        assertThat(delivery.eventTimes).isEmpty();
    }

    @Test
    void breaks_the_count_out_by_table_and_by_the_source_operation() throws Exception {
        RecordingDelivery delivery = new RecordingDelivery();
        SinkProcessor processor = init(new ImmediateWriter(), delivery);

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(row("orders", 1L), update("orders", 2L), row("items", 3L),
                delete("items", 4L), delete("items", 5L)));
        processor.process(0, inbox);
        drain(processor);

        assertThat(delivery.latestRows()).isEqualTo(Map.of(
                "orders", Map.of("i", 1L, "u", 1L),
                "items", Map.of("i", 1L, "d", 2L)));
    }

    @Test
    void keeps_a_running_total_across_batches() throws Exception {
        RecordingDelivery delivery = new RecordingDelivery();
        SinkProcessor processor = init(new ImmediateWriter(), delivery);

        pump(processor, row("orders", 1L));
        pump(processor, row("orders", 2L), row("orders", 3L));

        // Cumulative rather than per-batch: a rate over any window the reader chooses is a subtraction of
        // two totals, and a delta only ever answers for the window whoever published it happened to pick.
        assertThat(delivery.latestRows()).isEqualTo(Map.of("orders", Map.of("i", 3L)));
    }

    @Test
    void reports_the_newest_event_time_of_a_table_not_the_last_one_it_saw() throws Exception {
        RecordingDelivery delivery = new RecordingDelivery();
        SinkProcessor processor = init(new ImmediateWriter(), delivery);

        // Out of order on purpose. Rows reach a sink in arrival order, which is the order a source
        // produced them, and that is not their event time: a snapshot read of an old row arrives after a
        // change to a new one every time a load and a tail meet.
        pump(processor, row("orders", 900L), row("orders", 100L));

        assertThat(delivery.latestEventTimes()).isEqualTo(Map.of("orders", 900L));
    }

    @Test
    void keeps_each_table_recency_apart() throws Exception {
        RecordingDelivery delivery = new RecordingDelivery();
        SinkProcessor processor = init(new ImmediateWriter(), delivery);

        pump(processor, row("orders", 900L), row("items", 100L));

        // One number over both tables would be the newer of them, and the table that has stopped moving is
        // the one nobody would then see.
        assertThat(delivery.latestEventTimes()).isEqualTo(Map.of("orders", 900L, "items", 100L));
    }

    @Test
    void reports_nothing_at_all_for_a_sink_that_has_settled_nothing() throws Exception {
        RecordingDelivery delivery = new RecordingDelivery();
        SinkProcessor processor = init(new ImmediateWriter(), delivery);

        drain(processor);

        // Absent, not present at zero. A table with nothing delivered yet and a sink whose counting is not
        // wired are different states, and a published zero spells them the same way.
        assertThat(delivery.rows).isEmpty();
        assertThat(delivery.eventTimes).isEmpty();
    }

    @Test
    void reports_nothing_for_a_batch_that_settled_without_a_single_row() throws Exception {
        RecordingDelivery delivery = new RecordingDelivery();
        SinkProcessor processor = init(new ImmediateWriter(), delivery);

        // A drain of nothing but word that a chain got past some changes with nothing to deliver for them.
        // That is the ordinary shape on a stream whose changes are absorbed upstream, not an edge case:
        // the write settles at once because there is nothing to write, so this path runs on every such
        // drain of every such pipeline.
        TestInbox inbox = new TestInbox();
        inbox.add(new SettledPositions(
                Map.of("orders", new ChainPosition(new SourceOrder(1, 7), "p7"))));
        processor.process(0, inbox);
        drain(processor);

        // Nothing reported at all, rather than a reading of nothing. Downstream both are a map, and one of
        // them says "this sink has delivered no rows" while the other says "no sink has said anything yet"
        // -- and a pipeline filtering everything out is the case where telling them apart is the answer.
        assertThat(delivery.rows).isEmpty();
        assertThat(delivery.eventTimes).isEmpty();
    }

    @Test
    void reports_what_the_totals_accumulate_from_every_time_it_reports_them() throws Exception {
        RecordingDelivery delivery = new RecordingDelivery();
        SinkProcessor processor = init(new ImmediateWriter(), delivery);

        pump(processor, row("orders", 1L));
        pump(processor, row("orders", 2L));

        // One start, published with every reading of the totals rather than once beside them. A running
        // total is readable only against what it accumulates from, and the two arriving by different
        // routes is how a consumer comes to hold a total from this execution against a start from the
        // one before it.
        assertThat(delivery.starts).hasSameSizeAs(delivery.rows);
        assertThat(delivery.starts).containsOnly(delivery.starts.get(0));
        assertThat(delivery.starts.get(0)).isPositive();
    }

    @Test
    void does_not_say_what_it_counts_from_before_it_has_counted_anything() throws Exception {
        RecordingDelivery delivery = new RecordingDelivery();
        SinkProcessor processor = init(new ImmediateWriter(), delivery);

        drain(processor);

        // The start travels with the totals, so a sink with no totals publishes neither. A start on its
        // own would be a stream that exists with nothing in it, which is not the same as a sink that has
        // not delivered.
        assertThat(delivery.starts).isEmpty();
    }

    @Test
    void drives_by_hand_without_a_job_rather_than_failing_for_want_of_one() throws Exception {
        // The gauge a real sink is given, on a processor with no job behind it - which is how every case
        // above drives one, and how a sink's behaviour is pinned at all. Its readings go into statistics
        // the job collects, and asking for a handle where there is no job fails outright and takes the
        // sink down with it. So the readings go nowhere instead; a sink that could not be driven by hand
        // would be a sink nothing could pin.
        SinkProcessor processor = new SinkProcessor(new ImmediateWriter(), null, null, 1, 1024,
                FrontierGauge.none(), new JetDeliveryGauge());
        processor.init(new TestOutbox(new int[] {}, 128), new TestProcessorContext());

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(row("orders", 1L)));
        processor.process(0, inbox);

        assertThatCode(() -> drain(processor)).doesNotThrowAnyException();
    }

    /** A row of {@code table} whose event time is {@code ts}. */
    private static Envelope row(String table, long ts) {
        return Envelope.insert(ts, table, Map.of("id", ts), null);
    }

    private static Envelope update(String table, long ts) {
        return Envelope.update(ts, table, Map.of("id", ts), Map.of("id", ts), null);
    }

    private static Envelope delete(String table, long ts) {
        return Envelope.delete(ts, table, Map.of("id", ts), null);
    }

    private static SinkProcessor init(SinkWriter writer, DeliveryGauge delivery) throws Exception {
        SinkProcessor processor =
                new SinkProcessor(writer, null, null, 1, 1024, FrontierGauge.none(), delivery);
        processor.init(new TestOutbox(new int[] {}, 128), new TestProcessorContext());
        return processor;
    }

    private static void pump(SinkProcessor processor, Envelope... events) {
        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(events));
        processor.process(0, inbox);
        drain(processor);
    }

    private static void drain(SinkProcessor processor) {
        for (int i = 0; i < 10_000; i++) {
            if (processor.complete()) {
                return;
            }
        }
        throw new AssertionError("processor did not complete");
    }

    /** Keeps every reading handed to it, so a test can assert both what was reported and when. */
    private static final class RecordingDelivery implements DeliveryGauge {
        private final List<Map<String, Map<String, Long>>> rows = new ArrayList<>();
        private final List<Map<String, Long>> eventTimes = new ArrayList<>();
        private final List<Long> starts = new ArrayList<>();

        @Override
        public void delivered(Map<String, Map<String, Long>> rowsByTableAndOp) {
            Map<String, Map<String, Long>> copy = new LinkedHashMap<>();
            rowsByTableAndOp.forEach((table, byOp) -> copy.put(table, Map.copyOf(byOp)));
            rows.add(copy);
        }

        @Override
        public void reached(Map<String, Long> newestEventTimeByTable) {
            eventTimes.add(Map.copyOf(newestEventTimeByTable));
        }

        @Override
        public void countingSince(long epochMillis) {
            starts.add(epochMillis);
        }

        Map<String, Map<String, Long>> latestRows() {
            return rows.get(rows.size() - 1);
        }

        Map<String, Long> latestEventTimes() {
            return eventTimes.get(eventTimes.size() - 1);
        }
    }

    /** Settles every write at once. */
    private static final class ImmediateWriter implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }

    /** Hands out futures the test settles by hand. */
    private static final class ManualWriter implements SinkWriter {
        private final List<CompletableFuture<WriteResult>> pending = new ArrayList<>();

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            CompletableFuture<WriteResult> future = new CompletableFuture<>();
            pending.add(future);
            return future;
        }

        void completeAll() {
            for (CompletableFuture<WriteResult> future : pending) {
                if (!future.isDone()) {
                    future.complete(new WriteResult(1));
                }
            }
        }

        @Override
        public void close() {
        }
    }

    /** Fails every write with the given cause. */
    private static final class FailingWriter implements SinkWriter {
        private final RuntimeException cause;

        FailingWriter(RuntimeException cause) {
            this.cause = cause;
        }

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            return CompletableFuture.failedFuture(cause);
        }

        @Override
        public void close() {
        }
    }
}
