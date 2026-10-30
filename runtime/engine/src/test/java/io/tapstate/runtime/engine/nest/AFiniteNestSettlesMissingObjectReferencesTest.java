package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.runtime.engine.ChainAxes;
import io.tapstate.runtime.engine.FrontierOrders;
import io.tapstate.runtime.engine.ReplayFloor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class AFiniteNestSettlesMissingObjectReferencesTest {

    private static final String ORDERS = "orders";

    @Test
    void idleTurnsDoNotSettleAnObjectWhileItsInputMayStillArrive() throws Exception {
        Run run = run(EmbedAs.OBJECT, NestSendPolicy.everyChange(), false);
        run.order(1, "missing");
        assertThat(run.assembler.tryProcess()).isTrue();

        assertThat(run.drain()).isEmpty();
        assertThat(run.assembler.complete()).isTrue();
        assertThat(run.drain()).extracting(Envelope::after)
                .containsExactly(row("order_id", 1, "cust_ref", "missing"));
    }

    @Test
    void normalCompletionFlushesAMissingObjectFromAnOpenWindowOnlyOnce() throws Exception {
        Run run = run(EmbedAs.OBJECT, NestSendPolicy.within(1_000), false);
        run.customers.save(List.of("known"), row("customer_id", "known", "name", "Ada"));
        run.order(1, "known");
        assertThat(run.drain()).extracting(Envelope::after)
                .containsExactly(row("order_id", 1, "cust_ref", "known",
                        "customer", row("customer_id", "known", "name", "Ada")));

        run.order(2, "missing");
        assertThat(run.drain()).isEmpty();
        assertThat(run.assembler.complete()).isTrue();
        assertThat(run.drain()).extracting(Envelope::after)
                .containsExactly(row("order_id", 1, "cust_ref", "missing"));
        assertThat(run.assembler.complete()).isTrue();
        assertThat(run.drain()).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(value = EmbedAs.class, names = {"ARRAY", "FLAT"})
    void normalCompletionDoesNotSettleOtherReferenceShapes(EmbedAs shape) throws Exception {
        Run run = run(shape, NestSendPolicy.everyChange(), false);
        run.order(1, "missing");

        assertThat(run.assembler.complete()).isTrue();
        assertThat(run.drain()).isEmpty();
    }

    @Test
    void aBoundedPreviewStillSettlesAnArrayMissingFromItsSample() throws Exception {
        Run run = run(EmbedAs.ARRAY, NestSendPolicy.everyChange(), true);
        run.order(1, "missing");

        assertThat(run.assembler.complete()).isTrue();
        assertThat(run.drain()).extracting(Envelope::after)
                .containsExactly(row("order_id", 1, "cust_ref", "missing", "customer", List.of()));
    }

    @Test
    void completionSettlesAtMostOneBoundedBatchBeforeYielding() throws Exception {
        Run run = run(EmbedAs.OBJECT, NestSendPolicy.everyChange(), false, 4_096, () -> 0L, false);
        int roots = 2 * DrainFolding.MAX_KEYS_HELD + 1;
        for (int key = 1; key <= roots; key++) {
            run.orderFor(key, key, "missing");
        }
        List<Envelope> documents = new ArrayList<>();
        for (int batch = 0; batch < 3; batch++) {
            assertThat(run.assembler.complete()).isEqualTo(batch == 2);
            List<Envelope> emitted = run.drain();
            assertThat(emitted).hasSize(batch < 2 ? DrainFolding.MAX_KEYS_HELD : 1);
            documents.addAll(emitted);
        }
        assertThat(documents).extracting(document -> document.after().get("order_id"))
                .hasSize(roots).doesNotHaveDuplicates();
    }

    @Test
    void backpressureRetriesTheQueuedDocumentsWithoutRepeatingTheSettlement() throws Exception {
        Run run = run(EmbedAs.OBJECT, NestSendPolicy.everyChange(), false, 1, () -> 0L, false);
        run.orderFor(1, 1, "missing");
        run.orderFor(2, 2, "missing");

        assertThat(run.assembler.complete()).isFalse();
        List<Envelope> documents = new ArrayList<>(run.drain());
        assertThat(run.assembler.complete()).isTrue();
        documents.addAll(run.drain());
        assertThat(documents).extracting(document -> document.after().get("order_id"))
                .containsExactly(1, 2);
    }

    @Test
    void eachBatchFinishesItsFoldedAndDirectOutputBeforeTheNextBatchIsLoaded() throws Exception {
        Run run = run(EmbedAs.OBJECT, NestSendPolicy.within(1_000), false, 1, () -> 0L, false);
        run.customers.save(List.of("known"), row("customer_id", "known", "name", "Ada"));
        int roots = 2 * DrainFolding.MAX_KEYS_HELD + 1;
        for (int key = 2; key <= roots; key += 2) {
            run.orderFor(key, key, "known");
            assertThat(run.drain()).hasSize(1);
        }
        for (int key = 1; key <= roots; key++) {
            run.orderFor(roots + key, key, "missing");
        }

        List<Envelope> documents = new ArrayList<>();
        boolean completed = false;
        for (int attempt = 0; attempt < 3 * roots && !completed; attempt++) {
            completed = run.assembler.complete();
            documents.addAll(run.drain());
        }
        assertThat(completed).isTrue();
        assertThat(documents).extracting(document -> document.after().get("order_id"))
                .hasSize(roots).doesNotHaveDuplicates();
        assertThat(documents.subList(0, DrainFolding.MAX_KEYS_HELD))
                .allSatisfy(document -> assertThat((Integer) document.after().get("order_id"))
                        .isLessThanOrEqualTo(DrainFolding.MAX_KEYS_HELD));
    }

    @Test
    void theFinalBoundFollowsTheCompletedDocumentAndIsRetriedWhenRefused() throws Exception {
        AtomicLong now = new AtomicLong();
        Run run = run(EmbedAs.OBJECT, NestSendPolicy.within(1_000), false, 1, now::get, true);
        run.customers.save(List.of("known"), row("customer_id", "known", "name", "Ada"));
        run.order(1, "known");
        assertThat(run.drain()).hasSize(1);
        run.order(2, "known");
        run.order(3, "missing");
        long finalBound = FrontierOrders.pack(ORDERS, at(900));
        assertThat(run.assembler.tryProcessWatermark(run.topology.assembler().inboundFor(List.of()).ordinal(),
                new Watermark(finalBound, run.axes.axisOf(ORDERS)))).isTrue();
        assertThat(run.drainItems()).singleElement().isInstanceOfSatisfying(Watermark.class,
                bound -> assertThat(bound.timestamp()).isEqualTo(FrontierOrders.pack(ORDERS, at(2)) - 1));

        now.set(1_001);
        assertThat(run.assembler.complete()).isFalse();
        assertThat(run.drainItems()).singleElement().isInstanceOfSatisfying(Envelope.class,
                document -> assertThat(document.after()).isEqualTo(row("order_id", 1, "cust_ref", "missing")));
        assertThat(run.assembler.complete()).isTrue();
        assertThat(run.drainItems()).singleElement().isInstanceOfSatisfying(Watermark.class,
                bound -> assertThat(bound.timestamp()).isEqualTo(finalBound));
    }

    private static Run run(EmbedAs shape, NestSendPolicy sending, boolean preview) throws Exception {
        return run(shape, sending, preview, 128, () -> 0L, false);
    }

    private static Run run(EmbedAs shape, NestSendPolicy sending, boolean preview,
            int capacity, NestClock clock, boolean propagateBounds) throws Exception {
        NestTopology topology = NestTopology.compile("p", "doc",
                nest("order", List.of("order_id"),
                        embed("customer", "customer_id", "cust_ref", shape,
                                shape == EmbedAs.FLAT ? null : "customer", null)), tables());
        HeapNestStore<Map<String, Object>> customers = new HeapNestStore<>();
        ChainAxes axes = propagateBounds ? ChainAxes.assign(List.of(ORDERS)) : null;
        Map<Integer, List<String>> chains = propagateBounds
                ? Map.of(topology.assembler().inboundFor(List.of()).ordinal(), List.of(ORDERS)) : null;
        AssemblerProcessor assembler = new AssemblerProcessor(
                topology.assembler(), topology.slots(), new HeapNestStore<>(), "doc", axes, chains,
                ReplayFloor.NONE, NestSettings.defaults(), clock, sending,
                new HeapNestStore<>(), (from, released) -> {
                    throw new AssertionError("unexpected dead letter: " + released);
                }, Map.of(topology.lookups().getFirst().mapName(), customers), preview);
        TestOutbox outbox = new TestOutbox(capacity);
        assembler.init(outbox, new TestProcessorContext());
        return new Run(assembler, topology, customers, outbox, axes);
    }

    private record Run(AssemblerProcessor assembler, NestTopology topology,
            HeapNestStore<Map<String, Object>> customers, TestOutbox outbox, ChainAxes axes) {

        void order(long sequence, String customer) {
            orderFor(sequence, 1, customer);
        }

        void orderFor(long sequence, int key, String customer) {
            TestInbox inbox = new TestInbox();
            inbox.queue().add(Envelope.insert(sequence, "orders",
                    row("order_id", key, "cust_ref", customer), null).withOrder(at(sequence))
                    .withPositions(Map.of(ORDERS, new ChainPosition(at(sequence), "row-" + sequence))));
            assembler.process(topology.assembler().inboundFor(List.of()).ordinal(), inbox);
            assertThat(inbox).isEmpty();
        }

        List<Envelope> drain() {
            return drainItems().stream().map(Envelope.class::cast).toList();
        }

        List<Object> drainItems() {
            List<Object> emitted = new ArrayList<>();
            outbox.drainQueueAndReset(0, emitted, false);
            return emitted;
        }
    }
}
