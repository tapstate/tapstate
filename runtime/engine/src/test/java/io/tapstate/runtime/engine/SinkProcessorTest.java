package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestSupport;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The generic async sink adapter that drives a {@link SinkWriter} from a Jet vertex: it batches
 * inbound events, keeps a bounded number of writes in flight (the backpressure the port contract
 * hands to the runtime), completes only once every write has settled, and closes the writer once.
 * The write intent (mode / ddl) lives in the writer, not here — this adapter is connector-agnostic.
 */
class SinkProcessorTest {

    private static final ChainAxes AXES = ChainAxes.assign(List.of("orders", "lines"));

    private static Envelope event(int id) {
        return Envelope.insert(id, "orders", Map.of("id", id), null);
    }

    /**
     * An event on chain {@code src} carrying source position {@code pos}, ordered by the numeric suffix of
     * that token - the order the engine assigned as it read, which is what ranks positions now that no
     * connector supplies an order over its own tokens.
     */
    private static Envelope at(String src, String pos) {
        return Envelope.insert(1L, src, Map.of("id", pos), null)
                .withSrcPos(pos)
                .withOrder(new SourceOrder(1, Integer.parseInt(pos.replaceAll("\\D+", ""))));
    }

    /** A sink is terminal: it consumes every event and emits nothing, across all Jet run scenarios. */
    @Test
    void consumes_all_input_and_emits_nothing() {
        TestSupport.verifyProcessor(() -> new SinkProcessor(new RecordingWriter(), 4, 1024))
                .input(List.of(event(1), event(2), event(3)))
                .expectOutput(List.of());
    }

    @Test
    void writes_every_inbound_event_to_the_writer() throws Exception {
        RecordingWriter writer = new RecordingWriter();
        SinkProcessor processor = init(new SinkProcessor(writer, 4, 1024));

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(event(1), event(2), event(3)));
        processor.process(0, inbox);
        assertThat(inbox.isEmpty()).isTrue();
        drain(processor);

        assertThat(writer.batches.stream().flatMap(List::stream))
                .containsExactly(event(1), event(2), event(3));
    }

    @Test
    void never_writes_a_batch_larger_than_the_max_batch_size() throws Exception {
        RecordingWriter writer = new RecordingWriter();
        SinkProcessor processor = init(new SinkProcessor(writer, 8, 2));

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(event(1), event(2), event(3), event(4), event(5)));
        processor.process(0, inbox);
        drain(processor);

        assertThat(writer.batches).allSatisfy(batch -> assertThat(batch.size()).isLessThanOrEqualTo(2));
        assertThat(writer.batches.stream().flatMap(List::stream)).hasSize(5);
    }

    @Test
    void bounds_the_number_of_in_flight_writes() throws Exception {
        ManualWriter writer = new ManualWriter();
        // one event per write, at most two writes outstanding at a time.
        SinkProcessor processor = init(new SinkProcessor(writer, 2, 1));

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(event(1), event(2), event(3), event(4), event(5)));

        processor.process(0, inbox);
        // saturated at two in flight; the remaining three events stay in the inbox for backpressure.
        assertThat(writer.issued()).isEqualTo(2);
        assertThat(inbox).hasSize(3);
        // two writes are still pending, so the processor must not report itself done: a premature
        // complete() would let Jet close the writer and abandon an unsettled write.
        assertThat(processor.complete()).isFalse();

        writer.completeOldest();
        processor.process(0, inbox);
        assertThat(writer.issued()).isEqualTo(3);
        assertThat(inbox).hasSize(2);
        assertThat(processor.complete()).isFalse();

        writer.completeAll();
        processor.process(0, inbox);
        assertThat(inbox.isEmpty()).isTrue();
        writer.completeAll();
        drain(processor);
        assertThat(writer.issued()).isEqualTo(5);
    }

    @Test
    void at_the_single_in_flight_bound_a_batch_settles_before_the_next_is_issued() throws Exception {
        ManualWriter writer = new ManualWriter();
        // one write in flight is the production default: it is what keeps a key's events in order.
        SinkProcessor processor = init(new SinkProcessor(writer, 1, 1));

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(event(1), event(2), event(3)));

        processor.process(0, inbox);
        // exactly one write may be outstanding; the rest wait in the inbox, so a later same-key
        // event can never reach the target before an earlier one it depends on.
        assertThat(writer.issued()).isEqualTo(1);
        assertThat(inbox).hasSize(2);

        writer.completeAll();
        processor.process(0, inbox);
        assertThat(writer.issued()).isEqualTo(2);
        assertThat(inbox).hasSize(1);

        writer.completeAll();
        processor.process(0, inbox);
        assertThat(writer.issued()).isEqualTo(3);
        assertThat(inbox.isEmpty()).isTrue();
        writer.completeAll();
        drain(processor);
    }

    @Test
    void closes_the_writer_exactly_once_even_when_close_is_called_twice() throws Exception {
        RecordingWriter writer = new RecordingWriter();
        SinkProcessor processor = init(new SinkProcessor(writer, 4, 1024));
        processor.close();
        processor.close();
        assertThat(writer.closes.get()).isEqualTo(1);
    }

    @Test
    void a_failed_write_propagates_the_cause_not_the_completion_wrapper() throws Exception {
        SinkFailure boom = new SinkFailure("target refused the batch");
        SinkProcessor processor = init(new SinkProcessor(new FailingWriter(boom), 4, 1024));

        TestInbox inbox = new TestInbox();
        inbox.add(event(1));

        assertThatThrownBy(() -> {
            processor.process(0, inbox);
            drain(processor);
        }).isSameAs(boom);
    }

    @Test
    void surfaces_a_failed_write_while_idle_without_further_input_or_completion() throws Exception {
        // The sink reads from a streaming source that never completes, so complete() is never called; and a
        // batch that fails may be the last one, so no later process() call arrives to reap it either. Left to
        // those two alone, a failed write would sit unsurfaced and the job would stay RUNNING while moving no
        // data - an error hiding behind a healthy-looking state, the exact thing this must not do. Jet's idle
        // hook is what closes that gap: it runs when the inbox is empty, so the failure is surfaced there.
        SinkFailure boom = new SinkFailure("target refused the batch");
        SinkProcessor processor = init(new SinkProcessor(new FailingWriter(boom), 4, 1024));

        TestInbox inbox = new TestInbox();
        inbox.add(event(1));
        processor.process(0, inbox);

        assertThatThrownBy(() -> {
            for (int i = 0; i < 10_000; i++) {
                processor.tryProcess();
            }
        }).isSameAs(boom);
    }

    // ---- sink-ack contiguous acked-prefix watermark -------------------------------------------

    @Test
    void refuses_a_watermark_hook_with_more_than_one_write_in_flight() {
        // The contiguous prefix is only correct if batches settle in issue order, which needs one write
        // in flight; a hook with a higher bound is a wiring error and fails fast at construction.
        assertThatThrownBy(
                () -> new SinkProcessor(new RecordingWriter(), new RecordingAck(), new ContiguousPrefix(), 2, 1024))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxInFlight");
    }

    @Test
    void does_not_advance_the_watermark_on_the_first_settled_position() throws Exception {
        RecordingAck ack = new RecordingAck();
        SinkProcessor processor = init(new SinkProcessor(new RecordingWriter(), ack, new ContiguousPrefix(), 1, 1));

        TestInbox inbox = new TestInbox();
        inbox.add(at("orders", "p1"));
        pump(processor, inbox);

        // p1 is the open position; nothing higher has settled, so it is not yet a closed acked prefix.
        assertThat(ack.calls).isEmpty();
    }

    @Test
    void advances_a_position_only_once_a_strictly_higher_one_has_settled() throws Exception {
        RecordingAck ack = new RecordingAck();
        // one event per batch, so each position settles in its own batch, in order.
        SinkProcessor processor = init(new SinkProcessor(new RecordingWriter(), ack, new ContiguousPrefix(), 1, 1));

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(at("orders", "p1"), at("orders", "p2"), at("orders", "p3")));
        pump(processor, inbox);

        // p1 acks when p2 settles, p2 acks when p3 settles; p3 stays open (the lag-by-one prefix).
        assertThat(ack.calls).containsExactly("orders=p1", "orders=p2");
    }

    @Test
    void holds_a_fan_out_position_that_spans_batches_until_a_higher_one_settles() throws Exception {
        RecordingAck ack = new RecordingAck();
        SinkProcessor processor = init(new SinkProcessor(new RecordingWriter(), ack, new ContiguousPrefix(), 1, 1));

        TestInbox inbox = new TestInbox();
        // p1 appears in two separate batches (a fan-out split across a batch boundary), then p2.
        inbox.addAll(List.of(at("orders", "p1"), at("orders", "p1"), at("orders", "p2")));
        pump(processor, inbox);

        // the second p1 batch must not advance p1 — a later sibling could still be unwritten. Only p2,
        // strictly higher, proves every p1 output is in and closes the p1 prefix.
        assertThat(ack.calls).containsExactly("orders=p1");
    }

    @Test
    void advances_to_the_within_batch_max_when_a_batch_holds_several_positions() throws Exception {
        RecordingAck ack = new RecordingAck();
        // three positions per batch, so the per-batch reduction must pick the batch's highest position,
        // not the first, at the production-realistic batch size (every other test pins batch size to one).
        SinkProcessor processor = init(new SinkProcessor(new RecordingWriter(), ack, new ContiguousPrefix(), 1, 3));

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(
                at("orders", "p1"), at("orders", "p2"), at("orders", "p3"),
                at("orders", "p4"), at("orders", "p5"), at("orders", "p6")));
        pump(processor, inbox);

        // batch [p1,p2,p3] opens at p3 (its max, not p1); batch [p4,p5,p6] closes it -> ack reaches p3.
        assertThat(ack.calls).containsExactly("orders=p3");
    }

    @Test
    void reduces_each_chain_to_its_own_position_within_a_multi_chain_batch() throws Exception {
        RecordingAck ack = new RecordingAck();
        SinkProcessor processor = init(new SinkProcessor(new RecordingWriter(), ack, new ContiguousPrefix(), 1, 3));

        TestInbox inbox = new TestInbox();
        // one batch interleaves two chains; within it chain a's position is a2 (its max), chain b's is b1.
        inbox.addAll(List.of(at("a", "a1"), at("b", "b1"), at("a", "a2"), at("a", "a3"), at("b", "b2")));
        pump(processor, inbox);

        // batch [a1,b1,a2] opens a at a2 and b at b1; batch [a3,b2] closes both -> a=a2, b=b1 independently.
        assertThat(ack.calls).containsExactlyInAnyOrder("a=a2", "b=b1");
    }

    @Test
    void tracks_each_chain_independently() throws Exception {
        RecordingAck ack = new RecordingAck();
        SinkProcessor processor = init(new SinkProcessor(new RecordingWriter(), ack, new ContiguousPrefix(), 1, 1));

        TestInbox inbox = new TestInbox();
        // two single-table chains merged (a union): each advances on its own higher position.
        inbox.addAll(List.of(at("a", "a1"), at("b", "b1"), at("a", "a2"), at("b", "b2")));
        pump(processor, inbox);

        assertThat(ack.calls).containsExactly("a=a1", "b=b1");
    }

    @Test
    void skips_events_that_carry_no_source_position() throws Exception {
        RecordingAck ack = new RecordingAck();
        SinkProcessor processor = init(new SinkProcessor(new RecordingWriter(), ack, new ContiguousPrefix(), 1, 1));

        TestInbox inbox = new TestInbox();
        // a snapshot / synthetic event carries no position and must not move the watermark.
        inbox.addAll(List.of(at("orders", "p1"), event(2), at("orders", "p2")));
        pump(processor, inbox);

        assertThat(ack.calls).containsExactly("orders=p1");
    }

    @Test
    void never_advances_past_an_unsettled_write() throws Exception {
        RecordingAck ack = new RecordingAck();
        ManualWriter writer = new ManualWriter();
        SinkProcessor processor = init(new SinkProcessor(writer, ack, new ContiguousPrefix(), 1, 1));

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(at("orders", "p1"), at("orders", "p2")));

        processor.process(0, inbox);
        assertThat(ack.calls).isEmpty(); // p1 issued but not settled
        assertThat(processor.complete()).isFalse();

        writer.completeAll(); // p1 settles
        processor.process(0, inbox);
        assertThat(ack.calls).isEmpty(); // p1 is open, p2 not yet settled -> nothing to advance
        assertThat(processor.complete()).isFalse();

        writer.completeAll(); // p2 settles
        drain(processor);
        assertThat(ack.calls).containsExactly("orders=p1"); // p2 closes p1; p2 stays open
    }

    @Test
    void does_not_advance_when_a_write_fails() throws Exception {
        RecordingAck ack = new RecordingAck();
        SinkFailure boom = new SinkFailure("target refused the batch");
        // the first write (p1) succeeds and opens p1; the second (p2) fails.
        SinkProcessor processor = init(new SinkProcessor(new FailOnNthWriter(2, boom), ack, new ContiguousPrefix(), 1, 1));

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(at("orders", "p1"), at("orders", "p2")));

        assertThatThrownBy(() -> pump(processor, inbox)).isSameAs(boom);
        // p2's write failed, so it must not settle and close p1: nothing is durably acked.
        assertThat(ack.calls).isEmpty();
    }

    // ---- the shape of frontier is settled while the graph is compiled -------------------------

    @Test
    void a_sink_fed_by_one_chain_in_order_discards_a_bound_rather_than_acting_on_it() throws Exception {
        RecordingAck ack = new RecordingAck();
        SinkProcessor processor = init(new SinkProcessor(new RecordingWriter(), ack, new ContiguousPrefix(), 1, 1));

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(at("orders", "p1"), at("orders", "p2")));
        pump(processor, inbox);
        processor.tryProcessWatermark(new Watermark(Long.MAX_VALUE - 1, (byte) 1));
        drain(processor);

        // The source stamps bounds whatever the graph does with them, so they reach this sink too. Acting
        // on one here would advance past p2, which is still open: the settled prefix is the whole answer.
        assertThat(ack.calls).containsExactly("orders=p1");
    }

    @Test
    void a_sink_fed_by_an_assembly_advances_only_what_the_combined_bound_covers() throws Exception {
        RecordingAck ack = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);
        SinkProcessor processor = init(new SinkProcessor(new RecordingWriter(), ack, floor, 1, 1));

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(at("orders", "p1"), at("orders", "p2"), at("orders", "p3")));
        pump(processor, inbox);
        assertThat(ack.calls).isEmpty();

        processor.tryProcessWatermark(boundAt("orders", 2));
        drain(processor);

        assertThat(ack.calls).containsExactly("orders=p2");
    }

    @Test
    void a_sink_fed_by_an_assembly_takes_no_notice_of_a_bound_that_arrived_on_one_edge() throws Exception {
        RecordingAck ack = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);
        SinkProcessor processor = init(new SinkProcessor(new RecordingWriter(), ack, floor, 1, 1));

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(at("orders", "p1"), at("orders", "p2")));
        pump(processor, inbox);
        processor.tryProcessWatermark(0, boundAt("orders", 2));
        drain(processor);

        // The per-edge variant carries one edge's promise; data it covers may be sitting on another edge.
        // Answering true is what lets the engine combine the edges and call the variant that is safe.
        assertThat(ack.calls).isEmpty();
    }

    @Test
    void a_sink_fed_by_one_chain_advances_the_same_sequence_whether_or_not_bounds_arrive() throws Exception {
        List<Envelope> changes =
                List.of(at("orders", "p1"), at("orders", "p2"), at("orders", "p3"), at("orders", "p4"));

        RecordingAck withoutBounds = new RecordingAck();
        SinkProcessor bare =
                init(new SinkProcessor(new RecordingWriter(), withoutBounds, new ContiguousPrefix(), 1, 1));
        TestInbox inbox = new TestInbox();
        inbox.addAll(changes);
        pump(bare, inbox);

        RecordingAck withBounds = new RecordingAck();
        SinkProcessor bounded =
                init(new SinkProcessor(new RecordingWriter(), withBounds, new ContiguousPrefix(), 1, 1));
        // Bounds a sink fed by an assembly would act on: one beneath everything settled, which would hold
        // that sink where it is, and one above, which would carry it to the top. Neither may show here.
        bounded.tryProcessWatermark(boundAt("orders", 1));
        TestInbox bothInbox = new TestInbox();
        bothInbox.addAll(changes);
        pump(bounded, bothInbox);
        bounded.tryProcessWatermark(boundAt("orders", 9));
        drain(bounded);

        // Bounds are stamped at the source whatever the graph does with them, so a linear pipeline gets
        // them whether or not it has any use for them - there is no longer a way to run one without. What
        // keeps that from being a change in behaviour is that this frontier discards them, and the only
        // way to say so is that the two sequences are the same one, position for position.
        assertThat(withBounds.calls).isEqualTo(withoutBounds.calls);
        assertThat(withBounds.calls).containsExactly("orders=p1", "orders=p2", "orders=p3");
    }

    @Test
    void reports_how_far_the_frontier_trails_its_bound_each_time_a_bound_arrives() throws Exception {
        RecordingGauge gauge = new RecordingGauge();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);
        SinkProcessor processor =
                init(new SinkProcessor(new RecordingWriter(), new RecordingAck(), floor, 1, 1, gauge));

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(at("orders", "p1")));
        pump(processor, inbox);
        processor.tryProcessWatermark(boundAt("orders", 1));
        processor.tryProcessWatermark(boundAt("orders", 20));

        // A bound is the only thing that moves while a chain is starved of positions, so a reading taken
        // only when something settles would go quiet exactly when the number starts to matter.
        assertThat(gauge.reported).last().isEqualTo(Map.of("orders", 19L));
    }

    @Test
    void reports_a_frontier_that_caught_up_with_its_bound_as_trailing_by_nothing() throws Exception {
        RecordingGauge gauge = new RecordingGauge();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);
        SinkProcessor processor =
                init(new SinkProcessor(new RecordingWriter(), new RecordingAck(), floor, 1, 1, gauge));

        processor.tryProcessWatermark(boundAt("orders", 3));
        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(at("orders", "p1"), at("orders", "p2"), at("orders", "p3")));
        pump(processor, inbox);

        // The batch settles after the bound that covers it, so the reading has to be taken when a write
        // settles too - otherwise the last one before a lull reports the trailing distance forever.
        assertThat(gauge.reported).last().isEqualTo(Map.of("orders", 0L));
    }

    @Test
    void reports_nothing_for_a_sink_fed_by_one_chain_in_its_own_order() throws Exception {
        RecordingGauge gauge = new RecordingGauge();
        SinkProcessor processor = init(new SinkProcessor(
                new RecordingWriter(), new RecordingAck(), new ContiguousPrefix(), 1, 1, gauge));

        TestInbox inbox = new TestInbox();
        inbox.addAll(List.of(at("orders", "p1"), at("orders", "p2")));
        pump(processor, inbox);
        processor.tryProcessWatermark(boundAt("orders", 2));

        // Nothing runs ahead of that frontier to trail, so there is no distance rather than a zero one.
        // Readings were taken, and every one of them was empty: without the first half this passes just as
        // well on a sink that never reports at all, which is a different claim and not the one made here.
        assertThat(gauge.reported).isNotEmpty().allMatch(Map::isEmpty);
    }

    /** The bound the engine combined across every input queue for {@code chain}, at ring sequence {@code seq}. */
    private static Watermark boundAt(String chain, int seq) {
        return new Watermark(FrontierOrders.pack(chain, new SourceOrder(1, seq)), AXES.axisOf(chain));
    }

    /** Drives process() until the inbox drains (reaping and issuing one batch per call), then completes. */
    private static void pump(SinkProcessor processor, TestInbox inbox) {
        for (int i = 0; i < 10_000 && !inbox.isEmpty(); i++) {
            processor.process(0, inbox);
        }
        drain(processor);
    }

    /** Initialises a processor with a throwaway outbox and context; the sink has no outbound edge. */
    private static SinkProcessor init(SinkProcessor processor) throws Exception {
        processor.init(new TestOutbox(new int[] {}, 128), new TestProcessorContext());
        return processor;
    }

    /** Cooperatively drives complete() to termination (its writes settle in the background). */
    private static void drain(SinkProcessor processor) {
        for (int i = 0; i < 10_000; i++) {
            if (processor.complete()) {
                return;
            }
        }
        throw new AssertionError("processor did not complete");
    }

    /** Records every batch it is handed and completes synchronously. */
    private static final class RecordingWriter implements SinkWriter {
        private final List<List<Envelope>> batches = new CopyOnWriteArrayList<>();
        private final AtomicInteger closes = new AtomicInteger();

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            batches.add(List.copyOf(records));
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    /** Hands out futures the test completes by hand, to observe the in-flight bound. */
    private static final class ManualWriter implements SinkWriter {
        private final List<CompletableFuture<WriteResult>> pending = new ArrayList<>();

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            CompletableFuture<WriteResult> future = new CompletableFuture<>();
            pending.add(future);
            return future;
        }

        int issued() {
            return pending.size();
        }

        void completeOldest() {
            for (CompletableFuture<WriteResult> f : pending) {
                if (!f.isDone()) {
                    f.complete(new WriteResult(1));
                    return;
                }
            }
        }

        void completeAll() {
            for (CompletableFuture<WriteResult> f : pending) {
                if (!f.isDone()) {
                    f.complete(new WriteResult(1));
                }
            }
        }

        @Override
        public void close() {
        }
    }

    /** Records every {@code advance(chain, position)} call as {@code "chain=token"}, in order. */
    private static final class RecordingAck implements SinkAck {
        private final List<String> calls = new ArrayList<>();

        @Override
        public void advance(String chain, ChainPosition position) {
            calls.add(chain + "=" + position.token());
        }
    }

    /** Records every reading of how far each chain's frontier trails its bound, in the order taken. */
    private static final class RecordingGauge implements FrontierGauge {
        private final List<Map<String, Long>> reported = new ArrayList<>();

        @Override
        public void trailing(Map<String, Long> gapsByChain) {
            reported.add(Map.copyOf(gapsByChain));
        }
    }

    /** Succeeds every write but the {@code failAt}-th (1-based), which fails with a given cause. */
    private static final class FailOnNthWriter implements SinkWriter {
        private final int failAt;
        private final RuntimeException cause;
        private int n;

        FailOnNthWriter(int failAt, RuntimeException cause) {
            this.failAt = failAt;
            this.cause = cause;
        }

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            if (++n == failAt) {
                return CompletableFuture.failedFuture(cause);
            }
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }

    /** Fails every write with a given cause, to check the cause propagates unwrapped. */
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

    private static final class SinkFailure extends RuntimeException {
        SinkFailure(String message) {
            super(message);
        }
    }
}
