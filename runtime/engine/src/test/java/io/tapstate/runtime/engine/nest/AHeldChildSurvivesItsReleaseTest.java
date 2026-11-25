package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.EmbedAs;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static io.tapstate.runtime.engine.nest.NestTreeFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

class AHeldChildSurvivesItsReleaseTest {
    private static final NestVertex VERTEX = NestTopology.compile("p", "doc",
            nest("customer", List.of("customer_id"),
                    embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                            embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id")))),
            tables()).vertexAt(List.of("policies"));

    @Test void aRestartAfterBackpressureStillDeliversTheDurablyHeldChild() throws Exception {
        var store = new HeapNestStore<ResolverState>();
        var out = new TestOutbox(1);
        var first = new ResolverProcessor(VERTEX, store, (from, child) -> {});
        first.init(out, new TestProcessorContext());
        var parentFields = Map.<String,Object>of("policy_id", "P1", "customer_id", "C1", "policy_no", "PN1");
        feed(first, 0, Envelope.delete(1, "policy", parentFields, null).withOrder(new SourceOrder(1, 1)));
        out.drainQueueAndReset(0, new ArrayList<>(), false);
        feed(first, 1, Envelope.insert(3, "claim", Map.of("claim_id", "K1", "policy_id", "P1", "value", 1), null)
                .withOrder(new SourceOrder(1, 3)));
        assertThat(store.load(List.of("P1")).pending()).isEqualTo(1);
        var parent = Envelope.insert(2, "policy", parentFields, null).withOrder(new SourceOrder(1, 2));
        feed(first, 0, parent);
        var delivered = new ArrayList<Object>();
        out.drainQueueAndReset(0, delivered, false);
        assertThat(delivered).hasSize(1);
        assertThat(((KeyedElement) delivered.getFirst()).element().ref().pathId()).containsExactly("policies");

        // Discard the processor and its unsent output; only the serialized state survives.
        store.save(List.of("P1"), roundTrip(store.load(List.of("P1"))));
        var restartedOut = new TestOutbox(16);
        var restarted = new ResolverProcessor(VERTEX, store, (from, child) -> {});
        restarted.init(restartedOut, new TestProcessorContext());
        feed(restarted, 0, parent);
        var recovered = new ArrayList<Object>();
        restartedOut.drainQueueAndReset(0, recovered, false);
        assertThat(recovered).anySatisfy(item -> assertThat(((KeyedElement) item).element().fields()).containsEntry("value", 1));
    }

    @Test void aBinaryRowReplayDoesNotSpendAnotherPendingSlotAfterDeserialization() throws Exception {
        var state = new ResolverState();
        state.deleteMapping(new SourceOrder(1, 1));
        var child = new NestElement(new ElementRef(List.of("policies", "claims"), List.of("P1"), List.of("K1"), null),
                Map.of("claim_id", "K1", "payload", new byte[]{1, 2}), new SourceOrder(1, 3), Map.of());
        state.resolve(child, 1);
        var restored = roundTrip(state);
        restored.resolve(child, 2);
        assertThat(restored.pending()).isEqualTo(1);
    }

    @Test void differentChangesAtOneOrderStillHaveTheirOwnPendingSlots() {
        var state = new ResolverState();
        var order = new SourceOrder(1, 3);
        var first = new ElementRef(List.of("policies", "claims"), List.of("P1"), List.of("K1"), null);
        var second = new ElementRef(List.of("policies", "claims"), List.of("P1"), List.of("K2"), null);
        var fields = Map.<String,Object>of("value", 1);
        state.resolve(new NestElement(first, fields, order, Map.of()), 1);
        state.resolve(new NestElement(second, fields, order, Map.of()), 2);
        state.resolve(new NestElement(first, null, order, Map.of()), 3);
        state.resolve(new NestElement(first, fields, order,
                Map.of("another-chain", new io.tapstate.core.event.ChainPosition(order, "opaque"))), 4);
        assertThat(state.pending()).isEqualTo(4);
    }

    @Test void onlyAFloorPastTheOriginalHandoffBarrierReclaimsItsDurableChildren() throws Exception {
        var store = new HeapNestStore<ResolverState>();
        var floors = new java.util.HashMap<String,SourceOrder>();
        floors.put("policy", new SourceOrder(1, 20));
        var clock = new java.util.concurrent.atomic.AtomicLong();
        var axes = io.tapstate.runtime.engine.ChainAxes.assign(List.of("policy", "claim"));
        var processor = new ResolverProcessor(VERTEX, store, (from, child) -> {}, axes,
                Map.of(0, List.of("policy"), 1, List.of("claim")),
                chain -> java.util.Optional.ofNullable(floors.get(chain)), clock::get);
        var out = new TestOutbox(32); processor.init(out, new TestProcessorContext());
        processor.tryProcessWatermark(0, new com.hazelcast.jet.core.Watermark(
                io.tapstate.runtime.engine.FrontierOrders.pack("policy", new SourceOrder(1, 50)), axes.axisOf("policy")));
        out.drainQueueAndReset(0, new ArrayList<>(), false);
        var fields = Map.<String,Object>of("policy_id", "P1", "customer_id", "C1", "policy_no", "PN1");
        feed(processor, 0, Envelope.delete(1, "policy", fields, null).withOrder(new SourceOrder(1, 1)));
        feed(processor, 1, Envelope.insert(3, "claim", Map.of("claim_id", "K1", "policy_id", "P1"), null)
                .withOrder(new SourceOrder(1, 3)));
        feed(processor, 0, Envelope.insert(2, "policy", fields, null).withOrder(new SourceOrder(1, 2)));
        assertThat(store.load(List.of("P1")).releases()).hasSize(1);
        assertThat(store.load(List.of("P1")).pending()).isEqualTo(1);
        floors.put("policy", new SourceOrder(1, 50));
        feed(processor, 1, Envelope.insert(4, "claim", Map.of("claim_id", "K2", "policy_id", "P1"), null)
                .withOrder(new SourceOrder(1, 4)));
        clock.set(1000); processor.tryProcess();
        assertThat(store.load(List.of("P1")).releases()).hasSize(1);
        floors.put("policy", new SourceOrder(1, 51));
        feed(processor, 1, Envelope.insert(5, "claim", Map.of("claim_id", "K3", "policy_id", "P1"), null)
                .withOrder(new SourceOrder(1, 5)));
        clock.set(2001); processor.tryProcess();
        assertThat(store.load(List.of("P1")).releases()).isEmpty();
        assertThat(store.load(List.of("P1")).pending()).isZero();
    }

    @Test void aFailedReleasePreparationKeepsItsPreviouslyDurableWaiting() throws Exception {
        var store = new HeapNestStore<ResolverState>();
        var processor = new ResolverProcessor(VERTEX, store, (from, child) -> {}, chain -> {
            throw new IllegalStateException("floor read refused");
        });
        var out = new TestOutbox(16); processor.init(out, new TestProcessorContext());
        feed(processor, 1, Envelope.insert(1, "claim", Map.of("claim_id", "K1", "policy_id", "P1"), null)
                .withOrder(new SourceOrder(1, 1)));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> feed(processor, 0,
                Envelope.insert(2, "policy", Map.of("policy_id", "P1", "customer_id", "C1", "policy_no", "PN1"), null)
                        .withOrder(new SourceOrder(1, 2)))).isInstanceOf(IllegalStateException.class);
        assertThat(store.load(List.of("P1")).pending()).isEqualTo(1);
    }

    @Test void ordinaryChildrenDoNotReadTheDurableFloorOncePerChange() throws Exception {
        var store = new HeapNestStore<ResolverState>(); var reads = new java.util.concurrent.atomic.AtomicInteger();
        var processor = new ResolverProcessor(VERTEX, store, (from, child) -> {}, chain -> {
            reads.incrementAndGet(); return java.util.Optional.empty();
        });
        var out = new TestOutbox(128); processor.init(out, new TestProcessorContext());
        feed(processor, 1, Envelope.insert(1, "claim", Map.of("claim_id", "K1", "policy_id", "P1"), null)
                .withOrder(new SourceOrder(1, 1)));
        feed(processor, 0, Envelope.insert(2, "policy", Map.of("policy_id", "P1", "customer_id", "C1", "policy_no", "PN1"), null)
                .withOrder(new SourceOrder(1, 2)));
        int preparedReads = reads.get();
        for (int i = 3; i < 20; i++) feed(processor, 1, Envelope.insert(i, "claim",
                Map.of("claim_id", "K" + i, "policy_id", "P1"), null).withOrder(new SourceOrder(1, i)));
        assertThat(reads.get()).isEqualTo(preparedReads);
    }

    private static void feed(ResolverProcessor processor, int ordinal, Object item) {
        var inbox = new TestInbox(); inbox.queue().add(item); processor.process(ordinal, inbox);
    }

    private static ResolverState roundTrip(ResolverState state) throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var out = new ObjectOutputStream(bytes)) { out.writeObject(state); }
        try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (ResolverState) in.readObject();
        }
    }
}
